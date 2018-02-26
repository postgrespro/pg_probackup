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

#ifdef HAVE_LIBZ
#include <zlib.h>
#endif

#ifdef HAVE_LIBZ
/* Implementation of zlib compression method */
static size_t zlib_compress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
	uLongf compressed_size = dst_size;
	int rc = compress2(dst, &compressed_size, src, src_size, compress_level);
	return rc == Z_OK ? compressed_size : rc;
}

/* Implementation of zlib compression method */
static size_t zlib_decompress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
	uLongf dest_len = dst_size;
	int rc = uncompress(dst, &dest_len, src, src_size);
	return rc == Z_OK ? dest_len : rc;
}
#endif

/*
 * Compresses source into dest using algorithm. Returns the number of bytes
 * written in the destination buffer, or -1 if compression fails.
 */
static size_t
do_compress(void* dst, size_t dst_size, void const* src, size_t src_size, CompressAlg alg)
{
	switch (alg)
	{
		case NONE_COMPRESS:
		case NOT_DEFINED_COMPRESS:
			return -1;
#ifdef HAVE_LIBZ
		case ZLIB_COMPRESS:
			return zlib_compress(dst, dst_size, src, src_size);
#endif
		case PGLZ_COMPRESS:
			return pglz_compress(src, src_size, dst, PGLZ_strategy_always);
	}

	return -1;
}

/*
 * Decompresses source into dest using algorithm. Returns the number of bytes
 * decompressed in the destination buffer, or -1 if decompression fails.
 */
static size_t
do_decompress(void* dst, size_t dst_size, void const* src, size_t src_size, CompressAlg alg)
{
	switch (alg)
	{
		case NONE_COMPRESS:
		case NOT_DEFINED_COMPRESS:
			return -1;
#ifdef HAVE_LIBZ
		case ZLIB_COMPRESS:
			return zlib_decompress(dst, dst_size, src, src_size);
#endif
		case PGLZ_COMPRESS:
			return pglz_decompress(src, src_size, dst, dst_size);
	}

	return -1;
}

/*
 * When copying datafiles to backup we validate and compress them block
 * by block. Thus special header is required for each data block.
 */
typedef struct BackupPageHeader
{
	BlockNumber	block;			/* block number */
	int32		compressed_size;
} BackupPageHeader;

/* Special value for compressed_size field */
#define PageIsTruncated -2

/* Verify page's header */
static bool
parse_page(Page page, XLogRecPtr *lsn)
{
	PageHeader	phdr = (PageHeader) page;

	/* Get lsn from page header */
	*lsn = PageXLogRecPtrGet(phdr->pd_lsn);

	if (PageGetPageSize(phdr) == BLCKSZ &&
		PageGetPageLayoutVersion(phdr) == PG_PAGE_LAYOUT_VERSION &&
		(phdr->pd_flags & ~PD_VALID_FLAG_BITS) == 0 &&
		phdr->pd_lower >= SizeOfPageHeaderData &&
		phdr->pd_lower <= phdr->pd_upper &&
		phdr->pd_upper <= phdr->pd_special &&
		phdr->pd_special <= BLCKSZ &&
		phdr->pd_special == MAXALIGN(phdr->pd_special))
		return true;

	return false;
}

/* Read one page from file directly accessing disk
 * return value:
 * 0  - if the page is not found
 * 1  - if the page is found and valid
 * -1 - if the page is found but invalid
 */
static int
read_page_from_file(pgFile *file, BlockNumber blknum,
					FILE *in, Page page, XLogRecPtr *page_lsn)
{
	off_t				offset = blknum*BLCKSZ;
	size_t				read_len = 0;

	/* read the block */
	if (fseek(in, offset, SEEK_SET) != 0)
		elog(ERROR, "File: %s, could not seek to block %u: %s",
				file->path, blknum, strerror(errno));

	read_len = fread(page, 1, BLCKSZ, in);

	if (read_len != BLCKSZ)
	{
		/* The block could have been truncated. It is fine. */
		if (read_len == 0)
		{
			elog(LOG, "File %s, block %u, file was truncated",
					file->path, blknum);
			return 0;
		}
		else
			elog(WARNING, "File: %s, block %u, expected block size %lu,"
					  "but read %d, try again",
					   file->path, blknum, read_len, BLCKSZ);
	}

	/*
	 * If we found page with invalid header, at first check if it is zeroed,
	 * which is a valid state for page. If it is not, read it and check header
	 * again, because it's possible that we've read a partly flushed page.
	 * If after several attempts page header is still invalid, throw an error.
	 * The same idea is applied to checksum verification.
	 */
	if (!parse_page(page, page_lsn))
	{
		int i;
		/* Check if the page is zeroed. */
		for(i = 0; i < BLCKSZ && page[i] == 0; i++);

		/* Page is zeroed. No need to check header and checksum. */
		if (i == BLCKSZ)
		{
			elog(LOG, "File: %s blknum %u, empty page", file->path, blknum);
			return 1;
		}

		/*
		 * If page is not completely empty and we couldn't parse it,
		 * try again several times. If it didn't help, throw error
		 */
		elog(LOG, "File: %s blknum %u have wrong page header, try again",
					   file->path, blknum);
		return -1;
	}

	/* Verify checksum */
	if(current.checksum_version)
	{
		/*
		 * If checksum is wrong, sleep a bit and then try again
		 * several times. If it didn't help, throw error
		 */
		if (pg_checksum_page(page, file->segno * RELSEG_SIZE + blknum)
			!= ((PageHeader) page)->pd_checksum)
		{
			elog(WARNING, "File: %s blknum %u have wrong checksum, try again",
						   file->path, blknum);
			return -1;
		}
		else
		{
			/* page header and checksum are correct */
			return 1;
		}
	}
	else
	{
		/* page header is correct and checksum check is disabled */
		return 1;
	}
}

/*
 * Backup the specified block from a file of a relation.
 * Verify page header and checksum of the page and write it
 * to the backup file.
 */
static void
backup_data_page(backup_files_args *arguments, 
				 pgFile *file, XLogRecPtr prev_backup_start_lsn,
				 BlockNumber blknum, BlockNumber nblocks,
				 FILE *in, FILE *out,
				 pg_crc32 *crc, int *n_skipped,
				 BackupMode backup_mode)
{
	BackupPageHeader	header;
	Page 				page = malloc(BLCKSZ);
	Page 				compressed_page = NULL;
	XLogRecPtr			page_lsn = 0;
	size_t				write_buffer_size;
	char				write_buffer[BLCKSZ+sizeof(header)];

	int					try_again = 100;
	bool				page_is_valid = false;
	BlockNumber absolute_blknum = file->segno * RELSEG_SIZE + blknum;

	header.block = blknum;
	header.compressed_size = 0;

	/*
	 * Read the page and verify its header and checksum.
	 * Under high write load it's possible that we've read partly
	 * flushed page, so try several times before throwing an error.
	 */
	if (backup_mode != BACKUP_MODE_DIFF_PTRACK)
	{
		while(!page_is_valid && try_again)
		{
			int result = read_page_from_file(file, blknum,
											 in, page, &page_lsn);

			try_again--;
			if (result == 0)
			{
				/* This block was truncated.*/
				header.compressed_size = PageIsTruncated;
				/* Page is not actually valid, but it is absent
				 * and we're not going to reread it or validate */
				page_is_valid = true;
			}

			if (result == 1)
				page_is_valid = true;

			/*
			 * If ptrack support is available use it to get invalid block
			 * instead of rereading it 99 times
			 */
			//elog(WARNING, "Checksum_Version: %i", current.checksum_version ? 1 : 0);

			if (result == -1 && is_ptrack_support)
			{
				elog(WARNING, "File %s, block %u, try to fetch via SQL",
					file->path, blknum);
				break;
			}
		}
		/*
		 * If page is not valid after 100 attempts to read it
		 * throw an error.
		 */
		if(!page_is_valid && !is_ptrack_support)
			elog(ERROR, "Data file checksum mismatch. Canceling backup");
	}

	if (backup_mode == BACKUP_MODE_DIFF_PTRACK || (!page_is_valid && is_ptrack_support))
	{
		size_t page_size = 0;

		free(page);
		page = NULL;
		page = (Page) pg_ptrack_get_block(arguments, file->dbOid, file->tblspcOid,
										  file->relOid, absolute_blknum, &page_size);

		if (page == NULL)
		{
			/* This block was truncated.*/
			header.compressed_size = PageIsTruncated;
		}
		else if (page_size != BLCKSZ)
		{
			elog(ERROR, "File: %s, block %u, expected block size %lu, but read %d",
					   file->path, absolute_blknum, page_size, BLCKSZ);
		}
		else
		{
			/*
			 * We must set checksum here, because it is outdated
			 * in the block recieved from shared buffers.
			 */
			if (is_checksum_enabled)
				((PageHeader) page)->pd_checksum = pg_checksum_page(page, absolute_blknum);
		}
		/* get lsn from page */
		if (!parse_page(page, &page_lsn))
			elog(ERROR, "Cannot parse page after pg_ptrack_get_block. "
							"Possible risk of a memory corruption");

	}

	if (backup_mode == BACKUP_MODE_DIFF_DELTA &&
				header.compressed_size != PageIsTruncated &&
								page_lsn < prev_backup_start_lsn)
	{
		elog(VERBOSE, "Skipping blknum: %u in file: %s", blknum, file->path);
		free(page);
		return;
	}

	if (header.compressed_size != PageIsTruncated)
	{
		file->read_size += BLCKSZ;

		compressed_page = malloc(BLCKSZ);
		header.compressed_size = do_compress(compressed_page, BLCKSZ,
											page, BLCKSZ, compress_alg);

		file->compress_alg = compress_alg;

		Assert (header.compressed_size <= BLCKSZ);
	}

	write_buffer_size = sizeof(header);

	/*
	 * The page was truncated. Write only header
	 * to know that we must truncate restored file
	 */
	if (header.compressed_size == PageIsTruncated)
	{
		memcpy(write_buffer, &header, sizeof(header));
	}
	/* The page compression failed. Write it as is. */
	else if (header.compressed_size == -1)
	{
		header.compressed_size = BLCKSZ;
		memcpy(write_buffer, &header, sizeof(header));
		memcpy(write_buffer + sizeof(header), page, BLCKSZ);
		write_buffer_size += header.compressed_size;
	}
	/* The page was successfully compressed */
	else if (header.compressed_size > 0)
	{
		memcpy(write_buffer, &header, sizeof(header));
		memcpy(write_buffer + sizeof(header), compressed_page, header.compressed_size);
		write_buffer_size += MAXALIGN(header.compressed_size);
	}

	/* elog(VERBOSE, "backup blkno %u, compressed_size %d write_buffer_size %ld",
				  blknum, header.compressed_size, write_buffer_size); */

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

	if (page != NULL)
		free(page);
	if (compressed_page != NULL)
		free(compressed_page);
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
backup_data_file(backup_files_args* arguments,
				 const char *from_root, const char *to_root,
				 pgFile *file, XLogRecPtr prev_backup_start_lsn,
				 BackupMode backup_mode)
{
	char			to_path[MAXPGPATH];
	FILE			*in;
	FILE			*out;
	BlockNumber		blknum = 0;
	BlockNumber		nblocks = 0;
	int n_blocks_skipped = 0;
	int n_blocks_read = 0;

	if ((backup_mode == BACKUP_MODE_DIFF_PAGE ||
		backup_mode == BACKUP_MODE_DIFF_PTRACK) &&
		file->pagemap.bitmapsize == PageBitmapIsEmpty)
	{
		/*
		 * There are no changed blocks since last backup. We want make
		 * incremental backup, so we should exit.
		 */
		elog(VERBOSE, "Skipping the file because it didn`t changed: %s", file->path);
		return false;
	}

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
	 * If page map is empty backup all pages of the relation.
	 */
	if (file->pagemap.bitmapsize == PageBitmapIsEmpty
		|| file->pagemap.bitmapsize == PageBitmapIsAbsent)
	{
		for (blknum = 0; blknum < nblocks; blknum++)
		{
			backup_data_page(arguments, file, prev_backup_start_lsn, blknum,
							 nblocks, in, out, &(file->crc),
							 &n_blocks_skipped, backup_mode);
			n_blocks_read++;
		}
		if (backup_mode == BACKUP_MODE_DIFF_DELTA)
			file->n_blocks = n_blocks_read;
	}
	/* If page map is not empty we scan only changed blocks, */
	else
	{
		datapagemap_iterator_t *iter;
		iter = datapagemap_iterate(&file->pagemap);
		while (datapagemap_next(iter, &blknum))
		{
			backup_data_page(arguments, file, prev_backup_start_lsn, blknum,
							 nblocks, in, out, &(file->crc),
							 &n_blocks_skipped, backup_mode);
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

	if (fflush(out) != 0 ||
		fsync(fileno(out)) != 0 ||
		fclose(out))
		elog(ERROR, "cannot write backup file \"%s\": %s",
			 to_path, strerror(errno));
	fclose(in);

	FIN_CRC32C(file->crc);

	/*
	 * If we have pagemap then file in the backup can't be a zero size.
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
	size_t              file_size;

	/* open backup mode file for read */
	in = fopen(file->path, "r");
	if (in == NULL)
	{
		elog(ERROR, "cannot open backup file \"%s\": %s", file->path,
			 strerror(errno));
	}

	/*
	 * Open backup file for write. 	We use "r+" at first to overwrite only
	 * modified pages for differential restore. If the file does not exist,
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
			elog(ERROR, "backup is broken at file->path %s block %u",file->path, blknum);

		if (header.compressed_size == PageIsTruncated)
		{
			/*
			 * Backup contains information that this block was truncated.
			 * Truncate file to this length.
			 */
			if (ftruncate(fileno(out), header.block * BLCKSZ) != 0)
				elog(ERROR, "cannot truncate \"%s\": %s",
					 file->path, strerror(errno));
			elog(VERBOSE, "truncate file %s to block %u", file->path, header.block);
			break;
		}

		Assert(header.compressed_size <= BLCKSZ);

		read_len = fread(compressed_page.data, 1,
			MAXALIGN(header.compressed_size), in);
		if (read_len != MAXALIGN(header.compressed_size))
			elog(ERROR, "cannot read block %u of \"%s\" read %lu of %d",
				blknum, file->path, read_len, header.compressed_size);

		if (header.compressed_size != BLCKSZ)
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

    /*
     * DELTA backup has no knowledge about truncated blocks as PAGE or PTRACK do
     * but knows file size at the time of backup.
     * So when restoring file from delta backup we, knowning it`s size at
     * a time of a backup, can truncate file to this size.
     */

	if (backup->backup_mode == BACKUP_MODE_DIFF_DELTA)
	{
		/* get file current size */
		fseek(out, 0, SEEK_END);
		file_size = ftell(out);

		if (file_size > file->n_blocks * BLCKSZ)
		{
			/*
			 * Truncate file to this length.
			 */
			if (ftruncate(fileno(out), file->n_blocks * BLCKSZ) != 0)
				elog(ERROR, "cannot truncate \"%s\": %s",
					 file->path, strerror(errno));
			elog(INFO, "Delta truncate file %s to block %u", file->path, file->n_blocks);
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

	if (fflush(out) != 0 ||
		fsync(fileno(out)) != 0 ||
		fclose(out))
		elog(ERROR, "cannot write \"%s\": %s", to_path, strerror(errno));
	fclose(in);
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

	if (fflush(out) != 0 ||
		fsync(fileno(out)) != 0 ||
		fclose(out))
		elog(ERROR, "cannot write \"%s\": %s", to_path, strerror(errno));
	fclose(in);

	return true;
}

#ifdef HAVE_LIBZ
/*
 * Show error during work with compressed file
 */
static const char *
get_gz_error(gzFile gzf, int errnum)
{
	int			gz_errnum;
	const char *errmsg;

	errmsg = gzerror(gzf, &gz_errnum);
	if (gz_errnum == Z_ERRNO)
		return strerror(errnum);
	else
		return errmsg;
}
#endif

/*
 * Copy file attributes
 */
static void
copy_meta(const char *from_path, const char *to_path, bool unlink_on_error)
{
	struct stat st;

	if (stat(from_path, &st) == -1)
	{
		if (unlink_on_error)
			unlink(to_path);
		elog(ERROR, "Cannot stat file \"%s\": %s",
			 from_path, strerror(errno));
	}

	if (chmod(to_path, st.st_mode) == -1)
	{
		if (unlink_on_error)
			unlink(to_path);
		elog(ERROR, "Cannot change mode of file \"%s\": %s",
			 to_path, strerror(errno));
	}
}

/*
 * Copy WAL segment from pgdata to archive catalog with possible compression.
 */
void
push_wal_file(const char *from_path, const char *to_path, bool is_compress,
			  bool overwrite)
{
	FILE	   *in = NULL;
	FILE	   *out;
	char		buf[XLOG_BLCKSZ];
	const char *to_path_p = to_path;
	char		to_path_temp[MAXPGPATH];
	int			errno_temp;

#ifdef HAVE_LIBZ
	char		gz_to_path[MAXPGPATH];
	gzFile		gz_out = NULL;
#endif

	/* open file for read */
	in = fopen(from_path, "r");
	if (in == NULL)
		elog(ERROR, "Cannot open source WAL file \"%s\": %s", from_path,
			 strerror(errno));

	/* open backup file for write  */
#ifdef HAVE_LIBZ
	if (is_compress)
	{
		snprintf(gz_to_path, sizeof(gz_to_path), "%s.gz", to_path);

		if (!overwrite && fileExists(gz_to_path))
			elog(ERROR, "WAL segment \"%s\" already exists.", gz_to_path);

		snprintf(to_path_temp, sizeof(to_path_temp), "%s.partial", gz_to_path);

		gz_out = gzopen(to_path_temp, "wb");
		if (gzsetparams(gz_out, compress_level, Z_DEFAULT_STRATEGY) != Z_OK)
			elog(ERROR, "Cannot set compression level %d to file \"%s\": %s",
				 compress_level, to_path_temp, get_gz_error(gz_out, errno));

		to_path_p = gz_to_path;
	}
	else
#endif
	{
		if (!overwrite && fileExists(to_path))
			elog(ERROR, "WAL segment \"%s\" already exists.", to_path);

		snprintf(to_path_temp, sizeof(to_path_temp), "%s.partial", to_path);

		out = fopen(to_path_temp, "w");
		if (out == NULL)
			elog(ERROR, "Cannot open destination WAL file \"%s\": %s",
				 to_path_temp, strerror(errno));
	}

	/* copy content */
	for (;;)
	{
		size_t		read_len = 0;

		read_len = fread(buf, 1, sizeof(buf), in);

		if (ferror(in))
		{
			errno_temp = errno;
			unlink(to_path_temp);
			elog(ERROR,
				 "Cannot read source WAL file \"%s\": %s",
				 from_path, strerror(errno_temp));
		}

		if (read_len > 0)
		{
#ifdef HAVE_LIBZ
			if (is_compress)
			{
				if (gzwrite(gz_out, buf, read_len) != read_len)
				{
					errno_temp = errno;
					unlink(to_path_temp);
					elog(ERROR, "Cannot write to compressed WAL file \"%s\": %s",
						 to_path_temp, get_gz_error(gz_out, errno_temp));
				}
			}
			else
#endif
			{
				if (fwrite(buf, 1, read_len, out) != read_len)
				{
					errno_temp = errno;
					unlink(to_path_temp);
					elog(ERROR, "Cannot write to WAL file \"%s\": %s",
						 to_path_temp, strerror(errno_temp));
				}
			}
		}

		if (feof(in) || read_len == 0)
			break;
	}

#ifdef HAVE_LIBZ
	if (is_compress)
	{
		if (gzclose(gz_out) != 0)
		{
			errno_temp = errno;
			unlink(to_path_temp);
			elog(ERROR, "Cannot close compressed WAL file \"%s\": %s",
				 to_path_temp, get_gz_error(gz_out, errno_temp));
		}
	}
	else
#endif
	{
		if (fflush(out) != 0 ||
			fsync(fileno(out)) != 0 ||
			fclose(out))
		{
			errno_temp = errno;
			unlink(to_path_temp);
			elog(ERROR, "Cannot write WAL file \"%s\": %s",
				 to_path_temp, strerror(errno_temp));
		}
	}

	if (fclose(in))
	{
		errno_temp = errno;
		unlink(to_path_temp);
		elog(ERROR, "Cannot close source WAL file \"%s\": %s",
			 from_path, strerror(errno_temp));
	}

	/* update file permission. */
	copy_meta(from_path, to_path_temp, true);

	if (rename(to_path_temp, to_path_p) < 0)
	{
		errno_temp = errno;
		unlink(to_path_temp);
		elog(ERROR, "Cannot rename WAL file \"%s\" to \"%s\": %s",
			 to_path_temp, to_path_p, strerror(errno_temp));
	}

#ifdef HAVE_LIBZ
	if (is_compress)
		elog(INFO, "WAL file compressed to \"%s\"", gz_to_path);
#endif
}

/*
 * Copy WAL segment from archive catalog to pgdata with possible decompression.
 */
void
get_wal_file(const char *from_path, const char *to_path)
{
	FILE	   *in = NULL;
	FILE	   *out;
	char		buf[XLOG_BLCKSZ];
	const char *from_path_p = from_path;
	char		to_path_temp[MAXPGPATH];
	int			errno_temp;
	bool		is_decompress = false;

#ifdef HAVE_LIBZ
	char		gz_from_path[MAXPGPATH];
	gzFile		gz_in = NULL;
#endif

	/* open file for read */
	in = fopen(from_path, "r");
	if (in == NULL)
	{
#ifdef HAVE_LIBZ
		/*
		 * Maybe we need to decompress the file. Check it with .gz
		 * extension.
		 */
		snprintf(gz_from_path, sizeof(gz_from_path), "%s.gz", from_path);
		gz_in = gzopen(gz_from_path, "rb");
		if (gz_in == NULL)
		{
			if (errno == ENOENT)
			{
				/* There is no compressed file too, raise an error below */
			}
			/* Cannot open compressed file for some reason */
			else
				elog(ERROR, "Cannot open compressed WAL file \"%s\": %s",
					 gz_from_path, strerror(errno));
		}
		else
		{
			/* Found compressed file */
			is_decompress = true;
			from_path_p = gz_from_path;
		}
#endif
		/* Didn't find compressed file */
		if (!is_decompress)
			elog(ERROR, "Cannot open source WAL file \"%s\": %s",
				 from_path, strerror(errno));
	}

	/* open backup file for write  */
	snprintf(to_path_temp, sizeof(to_path_temp), "%s.partial", to_path);

	out = fopen(to_path_temp, "w");
	if (out == NULL)
		elog(ERROR, "Cannot open destination WAL file \"%s\": %s",
			 to_path_temp, strerror(errno));

	/* copy content */
	for (;;)
	{
		size_t		read_len = 0;

#ifdef HAVE_LIBZ
		if (is_decompress)
		{
			read_len = gzread(gz_in, buf, sizeof(buf));
			if (read_len != sizeof(buf) && !gzeof(gz_in))
			{
				errno_temp = errno;
				unlink(to_path_temp);
				elog(ERROR, "Cannot read compressed WAL file \"%s\": %s",
					 gz_from_path, get_gz_error(gz_in, errno_temp));
			}
		}
		else
#endif
		{
			read_len = fread(buf, 1, sizeof(buf), in);
			if (ferror(in))
			{
				errno_temp = errno;
				unlink(to_path_temp);
				elog(ERROR, "Cannot read source WAL file \"%s\": %s",
					 from_path, strerror(errno_temp));
			}
		}

		if (read_len > 0)
		{
			if (fwrite(buf, 1, read_len, out) != read_len)
			{
				errno_temp = errno;
				unlink(to_path_temp);
				elog(ERROR, "Cannot write to WAL file \"%s\": %s", to_path_temp,
					 strerror(errno_temp));
			}
		}

		/* Check for EOF */
#ifdef HAVE_LIBZ
		if (is_decompress)
		{
			if (gzeof(gz_in) || read_len == 0)
				break;
		}
		else
#endif
		{
			if (feof(in) || read_len == 0)
				break;
		}
	}

	if (fflush(out) != 0 ||
		fsync(fileno(out)) != 0 ||
		fclose(out))
	{
		errno_temp = errno;
		unlink(to_path_temp);
		elog(ERROR, "Cannot write WAL file \"%s\": %s",
			 to_path_temp, strerror(errno_temp));
	}

#ifdef HAVE_LIBZ
	if (is_decompress)
	{
		if (gzclose(gz_in) != 0)
		{
			errno_temp = errno;
			unlink(to_path_temp);
			elog(ERROR, "Cannot close compressed WAL file \"%s\": %s",
				 gz_from_path, get_gz_error(gz_in, errno_temp));
		}
	}
	else
#endif
	{
		if (fclose(in))
		{
			errno_temp = errno;
			unlink(to_path_temp);
			elog(ERROR, "Cannot close source WAL file \"%s\": %s",
				 from_path, strerror(errno_temp));
		}
	}

	/* update file permission. */
	copy_meta(from_path_p, to_path_temp, true);

	if (rename(to_path_temp, to_path) < 0)
	{
		errno_temp = errno;
		unlink(to_path_temp);
		elog(ERROR, "Cannot rename WAL file \"%s\" to \"%s\": %s",
			 to_path_temp, to_path, strerror(errno_temp));
	}

#ifdef HAVE_LIBZ
	if (is_decompress)
		elog(INFO, "WAL file decompressed from \"%s\"", gz_from_path);
#endif
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

	Assert(S_ISREG(file->mode));
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
