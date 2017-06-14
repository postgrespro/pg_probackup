/*-------------------------------------------------------------------------
 *
 * data.c: utils to parse and backup data pages
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "libpq/pqsignal.h"
#include "storage/block.h"
#include "storage/bufpage.h"
#include "storage/checksum_impl.h"
#include <common/pg_lzcompress.h>
#include <zlib.h>

static size_t zlib_compress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
	uLongf compressed_size = dst_size;
	int rc = compress2(dst, &compressed_size, src, src_size, compress_level);
	return rc == Z_OK ? compressed_size : rc;
}

static size_t zlib_decompress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
	uLongf dest_len = dst_size;
	int rc = uncompress(dst, &dest_len, src, src_size);
	return rc == Z_OK ? dest_len : rc;
}

static size_t
do_compress(void* dst, size_t dst_size, void const* src, size_t src_size, CompressAlg alg)
{
	switch (alg)
	{
		case NONE_COMPRESS:
		case NOT_DEFINED_COMPRESS:
			return -1;
		case ZLIB_COMPRESS:
			return zlib_compress(dst, dst_size, src, src_size);
		case PGLZ_COMPRESS:
			return pglz_compress(src, src_size, dst, PGLZ_strategy_always);
	}

	return -1;
}

static size_t
do_decompress(void* dst, size_t dst_size, void const* src, size_t src_size, CompressAlg alg)
{
	switch (alg)
	{
		case NONE_COMPRESS:
		case NOT_DEFINED_COMPRESS:
			return -1;
		case ZLIB_COMPRESS:
			return zlib_decompress(dst, dst_size, src, src_size);
		case PGLZ_COMPRESS:
			return pglz_decompress(src, src_size, dst, dst_size);
	}

	return -1;
}



typedef struct BackupPageHeader
{
	BlockNumber	block;			/* block number */
	int32		compressed_size;
} BackupPageHeader;

/* Verify page's header */
static bool
parse_page(const DataPage *page, XLogRecPtr *lsn)
{
	const PageHeaderData *page_data = &page->page_data;

	/* Get lsn from page header */
	*lsn = PageXLogRecPtrGet(page_data->pd_lsn);

	if (PageGetPageSize(page_data) == BLCKSZ &&
		PageGetPageLayoutVersion(page_data) == PG_PAGE_LAYOUT_VERSION &&
		(page_data->pd_flags & ~PD_VALID_FLAG_BITS) == 0 &&
		page_data->pd_lower >= SizeOfPageHeaderData &&
		page_data->pd_lower <= page_data->pd_upper &&
		page_data->pd_upper <= page_data->pd_special &&
		page_data->pd_special <= BLCKSZ &&
		page_data->pd_special == MAXALIGN(page_data->pd_special))
		return true;

	return false;
}

/*
 * Backup the specified block from a file of a relation.
 * Verify page header and checksum of the page and write it
 * to the backup file.
 */
static void
backup_data_page(pgFile *file, XLogRecPtr prev_backup_start_lsn,
				 BlockNumber blknum, BlockNumber nblocks,
				 FILE *in, FILE *out,
				 pg_crc32 *crc, int *n_skipped)
{
	BackupPageHeader	header;
	off_t				offset;
	DataPage			page; /* used as read buffer */
	DataPage			compressed_page; /* used as read buffer */
	size_t				write_buffer_size;
	/* maximum size of write buffer */
	char				write_buffer[BLCKSZ+sizeof(header)];
	size_t				read_len = 0;
	XLogRecPtr			page_lsn;
	int					try_checksum = 100;
	bool				is_zero_page = false;

	header.block = blknum;
	offset = blknum * BLCKSZ;

	while(try_checksum--)
	{
		if (fseek(in, offset, SEEK_SET) != 0)
			elog(ERROR, "File: %s, could not seek to block %u: %s",
				 file->path, blknum, strerror(errno));

		read_len = fread(&page, 1, BLCKSZ, in);

		if (read_len != BLCKSZ)
		{
			elog(ERROR, "File: %s, invalid block size of block %u : %lu",
				 file->path, blknum, read_len);
		}

		/*
		 * If we found page with invalid header, at first check if it is zeroed,
		 * which is valid state for page. If it is not, read it and check header
		 * again, because it's possible that we've read a partly flushed page.
		 * If after several attempts page header is still invalid, throw an error.
		 * The same idea is applied to checksum verification.
		 */
		if (!parse_page(&page, &page_lsn))
		{
			int i;
			/* Check if the page is zeroed. */
			for(i = 0; i < BLCKSZ && page.data[i] == 0; i++);
			if (i == BLCKSZ)
			{
				is_zero_page = true;
				try_checksum = 0;
				elog(LOG, "File: %s blknum %u, empty page", file->path, blknum);
			}

			/*
			 * If page is not completely empty and we couldn't parse it,
			 * try again several times. If it didn't help, throw error
			 */
			if (!is_zero_page)
			{
				/* Try to read and verify this page again several times. */
				if (try_checksum)
				{
					elog(WARNING, "File: %s blknum %u have wrong page header, try again",
						 file->path, blknum);
					usleep(100);
					continue;
				}
				else
					elog(ERROR, "File: %s blknum %u have wrong page header.", file->path, blknum);
			}
		}

		/* If the page hasn't changed since previous backup, don't backup it. */
		if (!XLogRecPtrIsInvalid(prev_backup_start_lsn)
			&& !XLogRecPtrIsInvalid(page_lsn)
			&& page_lsn < prev_backup_start_lsn)
		{
			*n_skipped += 1;
			return;
		}

		/* Verify checksum */
		if(current.checksum_version && !is_zero_page)
		{
			/*
			 * If checksum is wrong, sleep a bit and then try again
			 * several times. If it didn't help, throw error
			 */
			if (pg_checksum_page(page.data, file->segno * RELSEG_SIZE + blknum) != ((PageHeader) page.data)->pd_checksum)
			{
				if (try_checksum)
				{
					elog(WARNING, "File: %s blknum %u have wrong checksum, try again",
						 file->path, blknum);
					usleep(100);
				}
				else
					elog(ERROR, "File: %s blknum %u have wrong checksum.",
									file->path, blknum);
			}
		}
	}

	file->read_size += read_len;

	header.compressed_size = do_compress(compressed_page.data, sizeof(compressed_page.data),
										 page.data, sizeof(page.data), compress_alg);

	file->compress_alg = compress_alg;

	Assert (header.compressed_size <= BLCKSZ);
	write_buffer_size = sizeof(header);

	if (header.compressed_size > 0)
	{
		memcpy(write_buffer, &header, sizeof(header));
		memcpy(write_buffer + sizeof(header), compressed_page.data, header.compressed_size);
		write_buffer_size += MAXALIGN(header.compressed_size);
	}
	else
	{
		header.compressed_size = BLCKSZ;
		memcpy(write_buffer, &header, sizeof(header));
		memcpy(write_buffer + sizeof(header), page.data, BLCKSZ);
		write_buffer_size += header.compressed_size;
	}

	/* Update CRC */
	COMP_CRC32C(*crc, &write_buffer, write_buffer_size);

	/* write data page */
	if(fwrite(write_buffer, 1, write_buffer_size, out) != write_buffer_size)
	{
		int errno_tmp = errno;
		fclose(in);
		fclose(out);
		elog(ERROR, "File: %s, cannot write backup at block %u : %s",
				file->path, blknum, strerror(errno_tmp));
	}

	file->write_size += write_buffer_size;
}

/*
 * Backup data file in the from_root directory to the to_root directory with
 * same relative path. If prev_backup_start_lsn is not NULL, only pages with
 * higher lsn will be copied.
 * Not just copy file, but read it block by block (use bitmap in case of
 * incremental backup), validate checksum, optionally compress and write to
 * backup with special header.
 */
bool
backup_data_file(const char *from_root, const char *to_root,
				 pgFile *file, XLogRecPtr prev_backup_start_lsn)
{
	char			to_path[MAXPGPATH];
	FILE			*in;
	FILE			*out;
	BlockNumber		blknum = 0;
	BlockNumber		nblocks = 0;
	int n_blocks_skipped = 0;
	int n_blocks_read = 0;

	/* reset size summary */
	file->read_size = 0;
	file->write_size = 0;
	INIT_CRC32C(file->crc);

	/* open backup mode file for read */
	in = fopen(file->path, "r");
	if (in == NULL)
	{
		FIN_CRC32C(file->crc);

		/*
		 * If file is not found, this is not en error.
		 * It could have been deleted by concurrent postgres transaction.
		 */
		if (errno == ENOENT)
		{
			elog(LOG, "File \"%s\" is not found", file->path);
			return false;
		}

		elog(ERROR, "cannot open file \"%s\": %s",
			 file->path, strerror(errno));
	}

	if (file->size % BLCKSZ != 0)
	{
		fclose(in);
		elog(ERROR, "File: %s, invalid file size %lu", file->path, file->size);
	}

	/*
	 * Compute expected number of blocks in the file.
	 * NOTE This is a normal situation, if the file size has changed
	 * since the moment we computed it.
	 */
	nblocks = file->size/BLCKSZ;

	/* open backup file for write  */
	join_path_components(to_path, to_root, file->path + strlen(from_root) + 1);
	out = fopen(to_path, "w");
	if (out == NULL)
	{
		int errno_tmp = errno;
		fclose(in);
		elog(ERROR, "cannot open backup file \"%s\": %s",
			 to_path, strerror(errno_tmp));
	}

	/*
	 * Read each page, verify checksum and write it to backup.
	 * If page map is not empty we scan only changed blocks, otherwise
	 * backup all pages of the relation.
	 */
	if (file->pagemap.bitmapsize == 0)
	{
		for (blknum = 0; blknum < nblocks; blknum++)
		{
			backup_data_page(file, prev_backup_start_lsn, blknum,
							 nblocks, in, out, &(file->crc), &n_blocks_skipped);
			n_blocks_read++;
		}
	}
	else
	{
		datapagemap_iterator_t *iter;
		iter = datapagemap_iterate(&file->pagemap);
		while (datapagemap_next(iter, &blknum))
		{
			backup_data_page(file, prev_backup_start_lsn, blknum,
							 nblocks, in, out, &(file->crc), &n_blocks_skipped);
			n_blocks_read++;
		}

		pg_free(file->pagemap.bitmap);
		pg_free(iter);
	}

	/* update file permission */
	if (chmod(to_path, FILE_PERMISSION) == -1)
	{
		int errno_tmp = errno;
		fclose(in);
		fclose(out);
		elog(ERROR, "cannot change mode of \"%s\": %s", file->path,
			 strerror(errno_tmp));
	}

	fclose(in);
	fclose(out);

	FIN_CRC32C(file->crc);

	/*
	 * If we have pagemap then file can't be a zero size.
	 * Otherwise, we will clear the last file.
	 */
	if (n_blocks_read != 0 && n_blocks_read == n_blocks_skipped)
	{
		if (remove(to_path) == -1)
			elog(ERROR, "cannot remove file \"%s\": %s", to_path,
				 strerror(errno));
		return false;
	}

	return true;
}

/*
 * Restore compressed file that was backed up partly.
 */
static void
restore_file_partly(const char *from_root,const char *to_root, pgFile *file)
{
	FILE	   *in;
	FILE	   *out;
	size_t		read_len = 0;
	int			errno_tmp;
	struct stat	st;
	char		to_path[MAXPGPATH];
	char		buf[BLCKSZ];
	size_t write_size = 0;

	join_path_components(to_path, to_root, file->path + strlen(from_root) + 1);
	/* open backup mode file for read */
	in = fopen(file->path, "r");
	if (in == NULL)
	{
		elog(ERROR, "cannot open backup file \"%s\": %s", file->path,
			strerror(errno));
	}
	out = fopen(to_path, "r+");

	/* stat source file to change mode of destination file */
	if (fstat(fileno(in), &st) == -1)
	{
		fclose(in);
		fclose(out);
		elog(ERROR, "cannot stat \"%s\": %s", file->path,
			 strerror(errno));
	}

	if (fseek(out, 0, SEEK_END) < 0)
		elog(ERROR, "cannot seek END of \"%s\": %s",
				to_path, strerror(errno));

	/* copy everything from backup to the end of the file */
	for (;;)
	{
		if ((read_len = fread(buf, 1, sizeof(buf), in)) != sizeof(buf))
			break;

		if (fwrite(buf, 1, read_len, out) != read_len)
		{
			errno_tmp = errno;
			/* oops */
			fclose(in);
			fclose(out);
			elog(ERROR, "cannot write to \"%s\": %s", to_path,
				 strerror(errno_tmp));
		}
		write_size += read_len;
	}

	errno_tmp = errno;
	if (!feof(in))
	{
		fclose(in);
		fclose(out);
		elog(ERROR, "cannot read backup mode file \"%s\": %s",
			 file->path, strerror(errno_tmp));
	}

	/* copy odd part. */
	if (read_len > 0)
	{
		if (fwrite(buf, 1, read_len, out) != read_len)
		{
			errno_tmp = errno;
			/* oops */
			fclose(in);
			fclose(out);
			elog(ERROR, "cannot write to \"%s\": %s", to_path,
				 strerror(errno_tmp));
		}

		write_size += read_len;
	}


	/* update file permission */
	if (chmod(to_path, file->mode) == -1)
	{
		int errno_tmp = errno;

		fclose(in);
		fclose(out);
		elog(ERROR, "cannot change mode of \"%s\": %s", to_path,
			strerror(errno_tmp));
	}

	fclose(in);
	fclose(out);
}

void
restore_compressed_file(const char *from_root,
						const char *to_root,
						pgFile *file)
{
	if (!file->is_partial_copy)
		copy_file(from_root, to_root, file);
	else
		restore_file_partly(from_root, to_root, file);
}

/*
 * Restore files in the from_root directory to the to_root directory with
 * same relative path.
 */
void
restore_data_file(const char *from_root,
				  const char *to_root,
				  pgFile *file,
				  pgBackup *backup)
{
	char				to_path[MAXPGPATH];
	FILE			   *in;
	FILE			   *out;
	BackupPageHeader	header;
	BlockNumber			blknum;

	/* open backup mode file for read */
	in = fopen(file->path, "r");
	if (in == NULL)
	{
		elog(ERROR, "cannot open backup file \"%s\": %s", file->path,
			 strerror(errno));
	}

	/*
	 * Open backup file for write. 	We use "r+" at first to overwrite only
	 * modified pages for differential restore. If the file is not exists,
	 * re-open it with "w" to create an empty file.
	 */
	join_path_components(to_path, to_root, file->path + strlen(from_root) + 1);
	out = fopen(to_path, "r+");
	if (out == NULL && errno == ENOENT)
		out = fopen(to_path, "w");
	if (out == NULL)
	{
		int errno_tmp = errno;
		fclose(in);
		elog(ERROR, "cannot open restore target file \"%s\": %s",
			 to_path, strerror(errno_tmp));
	}

	for (blknum = 0; ; blknum++)
	{
		size_t		read_len;
		DataPage	compressed_page; /* used as read buffer */
		DataPage	page;

		/* read BackupPageHeader */
		read_len = fread(&header, 1, sizeof(header), in);
		if (read_len != sizeof(header))
		{
			int errno_tmp = errno;
			if (read_len == 0 && feof(in))
				break;		/* EOF found */
			else if (read_len != 0 && feof(in))
				elog(ERROR,
					 "odd size page found at block %u of \"%s\"",
					 blknum, file->path);
			else
				elog(ERROR, "cannot read header of block %u of \"%s\": %s",
					 blknum, file->path, strerror(errno_tmp));
		}

		if (header.block < blknum)
			elog(ERROR, "backup is broken at block %u", blknum);

		Assert(header.compressed_size <= BLCKSZ);

		read_len = fread(compressed_page.data, 1,
			MAXALIGN(header.compressed_size), in);
		if (read_len != MAXALIGN(header.compressed_size))
			elog(ERROR, "cannot read block %u of \"%s\" read %lu of %d",
				 blknum, file->path, read_len, header.compressed_size);

		if (header.compressed_size < BLCKSZ)
		{
			size_t uncompressed_size = 0;

			uncompressed_size = do_decompress(page.data, BLCKSZ,
											  compressed_page.data,
											  header.compressed_size, file->compress_alg);

			if (uncompressed_size != BLCKSZ)
				elog(ERROR, "page uncompressed to %ld bytes. != BLCKSZ", uncompressed_size);
		}

		/*
		 * Seek and write the restored page.
		 */
		blknum = header.block;
		if (fseek(out, blknum * BLCKSZ, SEEK_SET) < 0)
			elog(ERROR, "cannot seek block %u of \"%s\": %s",
				 blknum, to_path, strerror(errno));

		if (header.compressed_size < BLCKSZ)
		{
			if (fwrite(page.data, 1, BLCKSZ, out) != BLCKSZ)
				elog(ERROR, "cannot write block %u of \"%s\": %s",
					blknum, file->path, strerror(errno));
		}
		else
		{
			/* if page wasn't compressed, we've read full block */
			if (fwrite(compressed_page.data, 1, BLCKSZ, out) != BLCKSZ)
				elog(ERROR, "cannot write block %u of \"%s\": %s",
					blknum, file->path, strerror(errno));
		}
	}

	/* update file permission */
	if (chmod(to_path, file->mode) == -1)
	{
		int errno_tmp = errno;

		fclose(in);
		fclose(out);
		elog(ERROR, "cannot change mode of \"%s\": %s", to_path,
			 strerror(errno_tmp));
	}

	fclose(in);
	fclose(out);
}

/*
 * Copy file to backup.
 * We do not apply compression to these files, because
 * it is either small control file or already compressed cfs file.
 */
bool
copy_file(const char *from_root, const char *to_root, pgFile *file)
{
	char		to_path[MAXPGPATH];
	FILE	   *in;
	FILE	   *out;
	size_t		read_len = 0;
	int			errno_tmp;
	char		buf[BLCKSZ];
	struct stat	st;
	pg_crc32	crc;

	INIT_CRC32C(crc);

	/* reset size summary */
	file->read_size = 0;
	file->write_size = 0;

	/* open backup mode file for read */
	in = fopen(file->path, "r");
	if (in == NULL)
	{
		FIN_CRC32C(crc);
		file->crc = crc;

		/* maybe deleted, it's not error */
		if (errno == ENOENT)
			return false;

		elog(ERROR, "cannot open source file \"%s\": %s", file->path,
			 strerror(errno));
	}

	/* open backup file for write  */
	join_path_components(to_path, to_root, file->path + strlen(from_root) + 1);
	out = fopen(to_path, "w");
	if (out == NULL)
	{
		int errno_tmp = errno;
		fclose(in);
		elog(ERROR, "cannot open destination file \"%s\": %s",
			 to_path, strerror(errno_tmp));
	}

	/* stat source file to change mode of destination file */
	if (fstat(fileno(in), &st) == -1)
	{
		fclose(in);
		fclose(out);
		elog(ERROR, "cannot stat \"%s\": %s", file->path,
			 strerror(errno));
	}

	/* copy content and calc CRC */
	for (;;)
	{
		read_len = 0;

		if ((read_len = fread(buf, 1, sizeof(buf), in)) != sizeof(buf))
			break;

		if (fwrite(buf, 1, read_len, out) != read_len)
		{
			errno_tmp = errno;
			/* oops */
			fclose(in);
			fclose(out);
			elog(ERROR, "cannot write to \"%s\": %s", to_path,
				 strerror(errno_tmp));
		}
		/* update CRC */
		COMP_CRC32C(crc, buf, read_len);

		file->read_size += read_len;
	}

	errno_tmp = errno;
	if (!feof(in))
	{
		fclose(in);
		fclose(out);
		elog(ERROR, "cannot read backup mode file \"%s\": %s",
			 file->path, strerror(errno_tmp));
	}

	/* copy odd part. */
	if (read_len > 0)
	{
		if (fwrite(buf, 1, read_len, out) != read_len)
		{
			errno_tmp = errno;
			/* oops */
			fclose(in);
			fclose(out);
			elog(ERROR, "cannot write to \"%s\": %s", to_path,
				 strerror(errno_tmp));
		}
		/* update CRC */
		COMP_CRC32C(crc, buf, read_len);

		file->read_size += read_len;
	}

	file->write_size = file->read_size;
	/* finish CRC calculation and store into pgFile */
	FIN_CRC32C(crc);
	file->crc = crc;

	/* update file permission */
	if (chmod(to_path, st.st_mode) == -1)
	{
		errno_tmp = errno;
		fclose(in);
		fclose(out);
		elog(ERROR, "cannot change mode of \"%s\": %s", to_path,
			 strerror(errno_tmp));
	}

	fclose(in);
	fclose(out);

	return true;
}

/* Almost like copy file, except the fact we don't calculate checksum */
void
copy_wal_file(const char *from_path, const char *to_path)
{
	FILE	   *in;
	FILE	   *out;
	size_t		read_len = 0;
	int			errno_tmp;
	char		buf[XLOG_BLCKSZ];
	struct stat	st;

	/* open file for read */
	in = fopen(from_path, "r");
	if (in == NULL)
	{
		/* maybe deleted, it's not error */
		if (errno == ENOENT)
			elog(ERROR, "cannot open source WAL file \"%s\": %s", from_path,
			 strerror(errno));
	}

	/* open backup file for write  */
	out = fopen(to_path, "w");
	if (out == NULL)
	{
		int errno_tmp = errno;
		fclose(in);
		elog(ERROR, "cannot open destination file \"%s\": %s",
			 to_path, strerror(errno_tmp));
	}

	/* stat source file to change mode of destination file */
	if (fstat(fileno(in), &st) == -1)
	{
		fclose(in);
		fclose(out);
		elog(ERROR, "cannot stat \"%s\": %s", from_path,
			 strerror(errno));
	}

	if (st.st_size > XLOG_SEG_SIZE)
		elog(ERROR, "Unexpected wal file size %s : %ld", from_path, st.st_size);

	/* copy content */
	for (;;)
	{
		if ((read_len = fread(buf, 1, sizeof(buf), in)) != sizeof(buf))
			break;

		if (fwrite(buf, 1, read_len, out) != read_len)
		{
			errno_tmp = errno;
			/* oops */
			fclose(in);
			fclose(out);
			elog(ERROR, "cannot write to \"%s\": %s", to_path,
				 strerror(errno_tmp));
		}
	}

	errno_tmp = errno;
	if (!feof(in))
	{
		fclose(in);
		fclose(out);
		elog(ERROR, "cannot read backup mode file \"%s\": %s",
			 from_path, strerror(errno_tmp));
	}

	/* copy odd part */
	if (read_len > 0)
	{
		if (fwrite(buf, 1, read_len, out) != read_len)
		{
			errno_tmp = errno;
			/* oops */
			fclose(in);
			fclose(out);
			elog(ERROR, "cannot write to \"%s\": %s", to_path,
				 strerror(errno_tmp));
		}
	}


	/* update file permission. */
	if (chmod(to_path, st.st_mode) == -1)
	{
		errno_tmp = errno;
		fclose(in);
		fclose(out);
		elog(ERROR, "cannot change mode of \"%s\": %s", to_path,
			 strerror(errno_tmp));
	}

	fclose(in);
	fclose(out);

}

/*
 * Save part of the file into backup.
 * skip_size - size of the file in previous backup. We can skip it
 *			   and copy just remaining part of the file
 */
bool
copy_file_partly(const char *from_root, const char *to_root,
				 pgFile *file, size_t skip_size)
{
	char		to_path[MAXPGPATH];
	FILE	   *in;
	FILE	   *out;
	size_t		read_len = 0;
	int			errno_tmp;
	struct stat	st;
	char		buf[BLCKSZ];

	/* reset size summary */
	file->read_size = 0;
	file->write_size = 0;

	/* open backup mode file for read */
	in = fopen(file->path, "r");
	if (in == NULL)
	{
		/* maybe deleted, it's not error */
		if (errno == ENOENT)
			return false;

		elog(ERROR, "cannot open source file \"%s\": %s", file->path,
			 strerror(errno));
	}

	/* open backup file for write  */
	join_path_components(to_path, to_root, file->path + strlen(from_root) + 1);

	out = fopen(to_path, "w");
	if (out == NULL)
	{
		int errno_tmp = errno;
		fclose(in);
		elog(ERROR, "cannot open destination file \"%s\": %s",
			 to_path, strerror(errno_tmp));
	}

	/* stat source file to change mode of destination file */
	if (fstat(fileno(in), &st) == -1)
	{
		fclose(in);
		fclose(out);
		elog(ERROR, "cannot stat \"%s\": %s", file->path,
			 strerror(errno));
	}

	if (fseek(in, skip_size, SEEK_SET) < 0)
		elog(ERROR, "cannot seek %lu of \"%s\": %s",
				skip_size, file->path, strerror(errno));

	/*
	 * copy content
	 * NOTE: Now CRC is not computed for compressed files now.
	 */
	for (;;)
	{
		if ((read_len = fread(buf, 1, sizeof(buf), in)) != sizeof(buf))
			break;

		if (fwrite(buf, 1, read_len, out) != read_len)
		{
			errno_tmp = errno;
			/* oops */
			fclose(in);
			fclose(out);
			elog(ERROR, "cannot write to \"%s\": %s", to_path,
				 strerror(errno_tmp));
		}

		file->write_size += sizeof(buf);
		file->read_size += sizeof(buf);
	}

	errno_tmp = errno;
	if (!feof(in))
	{
		fclose(in);
		fclose(out);
		elog(ERROR, "cannot read backup mode file \"%s\": %s",
			 file->path, strerror(errno_tmp));
	}

	/* copy odd part. */
	if (read_len > 0)
	{
		if (fwrite(buf, 1, read_len, out) != read_len)
		{
			errno_tmp = errno;
			/* oops */
			fclose(in);
			fclose(out);
			elog(ERROR, "cannot write to \"%s\": %s", to_path,
				 strerror(errno_tmp));
		}

		file->write_size += read_len;
		file->read_size += read_len;
	}

	/* update file permission */
	if (chmod(to_path, st.st_mode) == -1)
	{
		errno_tmp = errno;
		fclose(in);
		fclose(out);
		elog(ERROR, "cannot change mode of \"%s\": %s", to_path,
			 strerror(errno_tmp));
	}

	/* add meta information needed for recovery */
	file->is_partial_copy = true;

	fclose(in);
	fclose(out);

	return true;
}


/*
 * Calculate checksum of various files which are not copied from PGDATA,
 * but created in process of backup, such as stream XLOG files,
 * PG_TABLESPACE_MAP_FILE and PG_BACKUP_LABEL_FILE.
 */
bool
calc_file_checksum(pgFile *file)
{
	FILE	   *in;
	size_t		read_len = 0;
	int			errno_tmp;
	char		buf[BLCKSZ];
	struct stat	st;
	pg_crc32	crc;

	INIT_CRC32C(crc);

	/* reset size summary */
	file->read_size = 0;
	file->write_size = 0;

	/* open backup mode file for read */
	in = fopen(file->path, "r");
	if (in == NULL)
	{
		FIN_CRC32C(crc);
		file->crc = crc;

		/* maybe deleted, it's not error */
		if (errno == ENOENT)
			return false;

		elog(ERROR, "cannot open source file \"%s\": %s", file->path,
			 strerror(errno));
	}

	/* stat source file to change mode of destination file */
	if (fstat(fileno(in), &st) == -1)
	{
		fclose(in);
		elog(ERROR, "cannot stat \"%s\": %s", file->path,
			 strerror(errno));
	}

	for (;;)
	{
		read_len = fread(buf, 1, sizeof(buf), in);

		if(read_len == 0)
			break;

		/* update CRC */
		COMP_CRC32C(crc, buf, read_len);

		file->write_size += read_len;
		file->read_size += read_len;
	}

	errno_tmp = errno;
	if (!feof(in))
	{
		fclose(in);
		elog(ERROR, "cannot read backup mode file \"%s\": %s",
			 file->path, strerror(errno_tmp));
	}

	/* finish CRC calculation and store into pgFile */
	FIN_CRC32C(crc);
	file->crc = crc;

	fclose(in);

	return true;
}
