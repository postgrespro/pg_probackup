/*-------------------------------------------------------------------------
 *
 * data.c: utils to parse and backup data pages
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include "storage/checksum.h"
#include "storage/checksum_impl.h"
#include <common/pg_lzcompress.h>
#include "utils/file.h"

#include <unistd.h>
#include <sys/stat.h>

#ifdef HAVE_LIBZ
#include <zlib.h>
#endif

#include "utils/thread.h"

/* Union to ease operations on relation pages */
typedef union DataPage
{
	PageHeaderData page_data;
	char		data[BLCKSZ];
} DataPage;

static bool
fileEqualCRC(const char *path1, const char *path2, bool path2_is_compressed);

#ifdef HAVE_LIBZ
/* Implementation of zlib compression method */
static int32
zlib_compress(void *dst, size_t dst_size, void const *src, size_t src_size,
			  int level)
{
	uLongf		compressed_size = dst_size;
	int			rc = compress2(dst, &compressed_size, src, src_size,
							   level);

	return rc == Z_OK ? compressed_size : rc;
}

/* Implementation of zlib compression method */
static int32
zlib_decompress(void *dst, size_t dst_size, void const *src, size_t src_size)
{
	uLongf		dest_len = dst_size;
	int			rc = uncompress(dst, &dest_len, src, src_size);

	return rc == Z_OK ? dest_len : rc;
}
#endif

/*
 * Compresses source into dest using algorithm. Returns the number of bytes
 * written in the destination buffer, or -1 if compression fails.
 */
int32
do_compress(void* dst, size_t dst_size, void const* src, size_t src_size,
			CompressAlg alg, int level, const char **errormsg)
{
	switch (alg)
	{
		case NONE_COMPRESS:
		case NOT_DEFINED_COMPRESS:
			return -1;
#ifdef HAVE_LIBZ
		case ZLIB_COMPRESS:
			{
				int32		ret;
				ret = zlib_compress(dst, dst_size, src, src_size, level);
				if (ret < Z_OK && errormsg)
					*errormsg = zError(ret);
				return ret;
			}
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
static int32
do_decompress(void* dst, size_t dst_size, void const* src, size_t src_size,
			  CompressAlg alg, const char **errormsg)
{
	switch (alg)
	{
		case NONE_COMPRESS:
		case NOT_DEFINED_COMPRESS:
		    if (errormsg)
				*errormsg = "Invalid compression algorithm";
			return -1;
#ifdef HAVE_LIBZ
		case ZLIB_COMPRESS:
			{
				int32		ret;
				ret = zlib_decompress(dst, dst_size, src, src_size);
				if (ret < Z_OK && errormsg)
					*errormsg = zError(ret);
				return ret;
			}
#endif
		case PGLZ_COMPRESS:
			return pglz_decompress(src, src_size, dst, dst_size);
	}

	return -1;
}


#define ZLIB_MAGIC 0x78

/*
 * Before version 2.0.23 there was a bug in pro_backup that pages which compressed
 * size is exactly the same as original size are not treated as compressed.
 * This check tries to detect and decompress such pages.
 * There is no 100% criteria to determine whether page is compressed or not.
 * But at least we will do this check only for pages which will no pass validation step.
 */
static bool
page_may_be_compressed(Page page, CompressAlg alg, uint32 backup_version)
{
	PageHeader	phdr;

	phdr = (PageHeader) page;

	/* First check if page header is valid (it seems to be fast enough check) */
	if (!(PageGetPageSize(phdr) == BLCKSZ &&
		  PageGetPageLayoutVersion(phdr) == PG_PAGE_LAYOUT_VERSION &&
		  (phdr->pd_flags & ~PD_VALID_FLAG_BITS) == 0 &&
		  phdr->pd_lower >= SizeOfPageHeaderData &&
		  phdr->pd_lower <= phdr->pd_upper &&
		  phdr->pd_upper <= phdr->pd_special &&
		  phdr->pd_special <= BLCKSZ &&
		  phdr->pd_special == MAXALIGN(phdr->pd_special)))
	{
		/* ... end only if it is invalid, then do more checks */
		if (backup_version >= 20023)
		{
			/* Versions 2.0.23 and higher don't have such bug */
			return false;
		}
#ifdef HAVE_LIBZ
		/* For zlib we can check page magic:
		 * https://stackoverflow.com/questions/9050260/what-does-a-zlib-header-look-like
		 */
		if (alg == ZLIB_COMPRESS && *(char*)page != ZLIB_MAGIC)
		{
			return false;
		}
#endif
		/* otherwize let's try to decompress the page */
		return true;
	}
	return false;
}

/* Verify page's header */
bool
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
	off_t		offset = blknum * BLCKSZ;
	ssize_t		read_len = 0;

	/* read the block */
	read_len = fio_pread(in, page, offset);

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
		{
			elog(WARNING, "File: %s, block %u, expected block size %u,"
					  "but read %zu, try again",
					   file->path, blknum, BLCKSZ, read_len);
			return -1;
		}
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
	if (current.checksum_version)
	{
		BlockNumber blkno = file->segno * RELSEG_SIZE + blknum;
		/*
		 * If checksum is wrong, sleep a bit and then try again
		 * several times. If it didn't help, throw error
		 */
		if (pg_checksum_page(page, blkno) != ((PageHeader) page)->pd_checksum)
		{
			elog(LOG, "File: %s blknum %u have wrong checksum, try again",
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
 * Retrieves a page taking the backup mode into account
 * and writes it into argument "page". Argument "page"
 * should be a pointer to allocated BLCKSZ of bytes.
 *
 * Prints appropriate warnings/errors/etc into log.
 * Returns 0 if page was successfully retrieved
 *         SkipCurrentPage(-3) if we need to skip this page
 *         PageIsTruncated(-2) if the page was truncated
 *         PageIsCorrupted(-4) if the page check mismatch
 */
static int32
prepare_page(backup_files_arg *arguments,
			 pgFile *file, XLogRecPtr prev_backup_start_lsn,
			 BlockNumber blknum, BlockNumber nblocks,
			 FILE *in, BlockNumber *n_skipped,
			 BackupMode backup_mode,
			 Page page,
			 bool strict)
{
	XLogRecPtr	page_lsn = 0;
	int			try_again = 100;
	bool		page_is_valid = false;
	bool		page_is_truncated = false;
	BlockNumber absolute_blknum = file->segno * RELSEG_SIZE + blknum;

	/* check for interrupt */
	if (interrupted || thread_interrupted)
		elog(ERROR, "Interrupted during page reading");

	/*
	 * Read the page and verify its header and checksum.
	 * Under high write load it's possible that we've read partly
	 * flushed page, so try several times before throwing an error.
	 */
	if (backup_mode != BACKUP_MODE_DIFF_PTRACK)
	{
		while(!page_is_valid && try_again)
		{
			int result = read_page_from_file(file, blknum, in, page, &page_lsn);

			try_again--;
			if (result == 0)
			{
				/* This block was truncated.*/
				page_is_truncated = true;
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

			if (result == -1 && is_ptrack_support && strict)
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

		if (!page_is_valid &&
			((strict && !is_ptrack_support) || !strict))
		{
			/* show this message for checkdb or backup without ptrack support */
			elog(WARNING, "CORRUPTION in file %s, block %u",
						file->path, blknum);
		}

		/* Backup with invalid block and without ptrack support must throw error */
		if (!page_is_valid && strict && !is_ptrack_support)
				elog(ERROR, "Data file corruption. Canceling backup");

		/* Checkdb not going futher */
		if (!strict)
		{
			if (page_is_valid)
				return 0;
			else
				return PageIsCorrupted;
		}
	}

	if (backup_mode == BACKUP_MODE_DIFF_PTRACK || (!page_is_valid && is_ptrack_support))
	{
		size_t page_size = 0;
		Page ptrack_page = NULL;
		ptrack_page = (Page) pg_ptrack_get_block(arguments, file->dbOid, file->tblspcOid,
										  file->relOid, absolute_blknum, &page_size);

		if (ptrack_page == NULL)
		{
			/* This block was truncated.*/
			page_is_truncated = true;
		}
		else if (page_size != BLCKSZ)
		{
			free(ptrack_page);
			elog(ERROR, "File: %s, block %u, expected block size %d, but read %zu",
					   file->path, absolute_blknum, BLCKSZ, page_size);
		}
		else
		{
			/*
			 * We need to copy the page that was successfully
			 * retreieved from ptrack into our output "page" parameter.
			 * We must set checksum here, because it is outdated
			 * in the block recieved from shared buffers.
			 */
			memcpy(page, ptrack_page, BLCKSZ);
			free(ptrack_page);
			if (current.checksum_version)
				((PageHeader) page)->pd_checksum = pg_checksum_page(page, absolute_blknum);
		}
		/* get lsn from page, provided by pg_ptrack_get_block() */
		if (backup_mode == BACKUP_MODE_DIFF_DELTA &&
			file->exists_in_prev &&
			!page_is_truncated &&
			!parse_page(page, &page_lsn))
				elog(ERROR, "Cannot parse page after pg_ptrack_get_block. "
								"Possible risk of a memory corruption");

	}

	/* Nullified pages must be copied by DELTA backup, just to be safe */
	if (backup_mode == BACKUP_MODE_DIFF_DELTA &&
		file->exists_in_prev &&
		!page_is_truncated &&
		page_lsn &&
		page_lsn < prev_backup_start_lsn)
	{
		elog(VERBOSE, "Skipping blknum: %u in file: %s", blknum, file->path);
		(*n_skipped)++;
		return SkipCurrentPage;
	}

	if (page_is_truncated)
		return PageIsTruncated;

	return 0;
}

static void
compress_and_backup_page(pgFile *file, BlockNumber blknum,
						FILE *in, FILE *out, pg_crc32 *crc,
						int page_state, Page page,
						CompressAlg calg, int clevel)
{
	BackupPageHeader header;
	size_t		write_buffer_size = sizeof(header);
	char		write_buffer[BLCKSZ+sizeof(header)];
	char		compressed_page[BLCKSZ*2]; /* compressed page may require more space than uncompressed */

	if(page_state == SkipCurrentPage)
		return;

	header.block = blknum;
	header.compressed_size = page_state;

	if(page_state == PageIsTruncated)
	{
		/*
		* The page was truncated. Write only header
		* to know that we must truncate restored file
		*/
		memcpy(write_buffer, &header, sizeof(header));
	}
	else
	{
		const char *errormsg = NULL;

		/* The page was not truncated, so we need to compress it */
		header.compressed_size = do_compress(compressed_page, sizeof(compressed_page),
											 page, BLCKSZ, calg, clevel,
											 &errormsg);
		/* Something went wrong and errormsg was assigned, throw a warning */
		if (header.compressed_size < 0 && errormsg != NULL)
			elog(WARNING, "An error occured during compressing block %u of file \"%s\": %s",
				 blknum, file->path, errormsg);

		file->compress_alg = calg;
		file->read_size += BLCKSZ;

		/* The page was successfully compressed. */
		if (header.compressed_size > 0 && header.compressed_size < BLCKSZ)
		{
			memcpy(write_buffer, &header, sizeof(header));
			memcpy(write_buffer + sizeof(header),
				   compressed_page, header.compressed_size);
			write_buffer_size += MAXALIGN(header.compressed_size);
		}
		/* Nonpositive value means that compression failed. Write it as is. */
		else
		{
			header.compressed_size = BLCKSZ;
			memcpy(write_buffer, &header, sizeof(header));
			memcpy(write_buffer + sizeof(header), page, BLCKSZ);
			write_buffer_size += header.compressed_size;
		}
	}

	/* elog(VERBOSE, "backup blkno %u, compressed_size %d write_buffer_size %ld",
				  blknum, header.compressed_size, write_buffer_size); */

	/* Update CRC */
	COMP_FILE_CRC32(true, *crc, write_buffer, write_buffer_size);

	/* write data page */
	if (fio_fwrite(out, write_buffer, write_buffer_size) != write_buffer_size)
	{
		int			errno_tmp = errno;

		fclose(in);
		fio_fclose(out);
		elog(ERROR, "File: %s, cannot write backup at block %u: %s",
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
backup_data_file(backup_files_arg* arguments,
				 const char *to_path, pgFile *file,
				 XLogRecPtr prev_backup_start_lsn, BackupMode backup_mode,
				 CompressAlg calg, int clevel)
{
	FILE		*in;
	FILE		*out;
	BlockNumber	blknum = 0;
	BlockNumber	nblocks = 0;
	BlockNumber	n_blocks_skipped = 0;
	BlockNumber	n_blocks_read = 0;
	int			page_state;
	char		curr_page[BLCKSZ];

	/*
	 * Skip unchanged file only if it exists in previous backup.
	 * This way we can correctly handle null-sized files which are
	 * not tracked by pagemap and thus always marked as unchanged.
	 */
	if ((backup_mode == BACKUP_MODE_DIFF_PAGE ||
		backup_mode == BACKUP_MODE_DIFF_PTRACK) &&
		file->pagemap.bitmapsize == PageBitmapIsEmpty &&
		file->exists_in_prev && !file->pagemap_isabsent)
	{
		/*
		 * There are no changed blocks since last backup. We want make
		 * incremental backup, so we should exit.
		 */
		elog(VERBOSE, "Skipping the unchanged file: %s", file->path);
		return false;
	}

	/* reset size summary */
	file->read_size = 0;
	file->write_size = 0;
	INIT_FILE_CRC32(true, file->crc);

	/* open backup mode file for read */
	in = fio_fopen(file->path, PG_BINARY_R, FIO_DB_HOST);
	if (in == NULL)
	{
		FIN_FILE_CRC32(true, file->crc);

		/*
		 * If file is not found, this is not en error.
		 * It could have been deleted by concurrent postgres transaction.
		 */
		if (errno == ENOENT)
		{
			elog(LOG, "File \"%s\" is not found", file->path);
			file->write_size = FILE_NOT_FOUND;
			return false;
		}

		elog(ERROR, "cannot open file \"%s\": %s",
			 file->path, strerror(errno));
	}

	if (file->size % BLCKSZ != 0)
	{
		fio_fclose(in);
		elog(WARNING, "File: %s, invalid file size %zu", file->path, file->size);
	}

	/*
	 * Compute expected number of blocks in the file.
	 * NOTE This is a normal situation, if the file size has changed
	 * since the moment we computed it.
	 */
	nblocks = file->size/BLCKSZ;

	/* open backup file for write  */
	out = fio_fopen(to_path, PG_BINARY_W, FIO_BACKUP_HOST);
	if (out == NULL)
	{
		int errno_tmp = errno;
		fio_fclose(in);
		elog(ERROR, "cannot open backup file \"%s\": %s",
			 to_path, strerror(errno_tmp));
	}

	/*
	 * Read each page, verify checksum and write it to backup.
	 * If page map is empty or file is not present in previous backup
	 * backup all pages of the relation.
	 *
	 * We will enter here if backup_mode is FULL or DELTA.
	 */
	if (file->pagemap.bitmapsize == PageBitmapIsEmpty ||
		file->pagemap_isabsent || !file->exists_in_prev)
	{
		if (backup_mode != BACKUP_MODE_DIFF_PTRACK && fio_is_remote_file(in))
		{
			int rc = fio_send_pages(in, out, file,
									backup_mode == BACKUP_MODE_DIFF_DELTA && file->exists_in_prev ? prev_backup_start_lsn : InvalidXLogRecPtr,
									&n_blocks_skipped, calg, clevel);
			if (rc == PAGE_CHECKSUM_MISMATCH && is_ptrack_support)
				goto RetryUsingPtrack;
			if (rc < 0)
				elog(ERROR, "Failed to read file %s: %s",
					 file->path, rc == PAGE_CHECKSUM_MISMATCH ? "data file checksum mismatch" : strerror(-rc));
			n_blocks_read = rc;
		}
		else
		{
		  RetryUsingPtrack:
			for (blknum = 0; blknum < nblocks; blknum++)
			{
				page_state = prepare_page(arguments, file, prev_backup_start_lsn,
										  blknum, nblocks, in, &n_blocks_skipped,
										  backup_mode, curr_page, true);
				compress_and_backup_page(file, blknum, in, out, &(file->crc),
										  page_state, curr_page, calg, clevel);
				n_blocks_read++;
				if (page_state == PageIsTruncated)
					break;
			}
		}
		if (backup_mode == BACKUP_MODE_DIFF_DELTA)
			file->n_blocks = n_blocks_read;
	}
	/*
	 * If page map is not empty we scan only changed blocks.
	 *
	 * We will enter here if backup_mode is PAGE or PTRACK.
	 */
	else
	{
		datapagemap_iterator_t *iter;
		iter = datapagemap_iterate(&file->pagemap);
		while (datapagemap_next(iter, &blknum))
		{
			page_state = prepare_page(arguments, file, prev_backup_start_lsn,
									  blknum, nblocks, in, &n_blocks_skipped,
									  backup_mode, curr_page, true);
			compress_and_backup_page(file, blknum, in, out, &(file->crc),
									  page_state, curr_page, calg, clevel);
			n_blocks_read++;
			if (page_state == PageIsTruncated)
				break;
		}

		pg_free(file->pagemap.bitmap);
		pg_free(iter);
	}

	/* update file permission */
	if (fio_chmod(to_path, FILE_PERMISSION, FIO_BACKUP_HOST) == -1)
	{
		int errno_tmp = errno;
		fio_fclose(in);
		fio_fclose(out);
		elog(ERROR, "cannot change mode of \"%s\": %s", file->path,
			 strerror(errno_tmp));
	}

	if (fio_fflush(out) != 0 ||
		fio_fclose(out))
		elog(ERROR, "cannot write backup file \"%s\": %s",
			 to_path, strerror(errno));
	fio_fclose(in);

	FIN_FILE_CRC32(true, file->crc);

	/*
	 * If we have pagemap then file in the backup can't be a zero size.
	 * Otherwise, we will clear the last file.
	 */
	if (n_blocks_read != 0 && n_blocks_read == n_blocks_skipped)
	{
		if (fio_unlink(to_path, FIO_BACKUP_HOST) == -1)
			elog(ERROR, "cannot remove file \"%s\": %s", to_path,
				 strerror(errno));
		return false;
	}

	return true;
}

/*
 * Restore files in the from_root directory to the to_root directory with
 * same relative path.
 *
 * If write_header is true then we add header to each restored block, currently
 * it is used for MERGE command.
 */
void
restore_data_file(const char *to_path, pgFile *file, bool allow_truncate,
				  bool write_header, uint32 backup_version)
{
	FILE	   *in = NULL;
	FILE	   *out = NULL;
	BackupPageHeader header;
	BlockNumber	blknum = 0,
				truncate_from = 0;
	bool		need_truncate = false;

	/* BYTES_INVALID allowed only in case of restoring file from DELTA backup */
	if (file->write_size != BYTES_INVALID)
	{
		/* open backup mode file for read */
		in = fopen(file->path, PG_BINARY_R);
		if (in == NULL)
		{
			elog(ERROR, "Cannot open backup file \"%s\": %s", file->path,
				 strerror(errno));
		}
	}

	/*
	 * Open backup file for write. 	We use "r+" at first to overwrite only
	 * modified pages for differential restore. If the file does not exist,
	 * re-open it with "w" to create an empty file.
	 */
	out = fio_fopen(to_path, PG_BINARY_R "+", FIO_DB_HOST);
	if (out == NULL)
	{
		int errno_tmp = errno;
		fclose(in);
		elog(ERROR, "Cannot open restore target file \"%s\": %s",
			 to_path, strerror(errno_tmp));
	}

	while (true)
	{
		off_t		write_pos;
		size_t		read_len;
		DataPage	compressed_page; /* used as read buffer */
		DataPage	page;
		int32		uncompressed_size = 0;

		/* File didn`t changed. Nothig to copy */
		if (file->write_size == BYTES_INVALID)
			break;

		/*
		 * We need to truncate result file if data file in a incremental backup
		 * less than data file in a full backup. We know it thanks to n_blocks.
		 *
		 * It may be equal to -1, then we don't want to truncate the result
		 * file.
		 */
		if (file->n_blocks != BLOCKNUM_INVALID &&
			(blknum + 1) > file->n_blocks)
		{
			truncate_from = blknum;
			need_truncate = true;
			break;
		}

		/* read BackupPageHeader */
		read_len = fread(&header, 1, sizeof(header), in);
		if (read_len != sizeof(header))
		{
			int errno_tmp = errno;
			if (read_len == 0 && feof(in))
				break;		/* EOF found */
			else if (read_len != 0 && feof(in))
				elog(ERROR,
					 "Odd size page found at block %u of \"%s\"",
					 blknum, file->path);
			else
				elog(ERROR, "Cannot read header of block %u of \"%s\": %s",
					 blknum, file->path, strerror(errno_tmp));
		}

		if (header.block == 0 && header.compressed_size == 0)
		{
			elog(VERBOSE, "Skip empty block of \"%s\"", file->path);
			continue;
		}

		if (header.block < blknum)
			elog(ERROR, "Backup is broken at block %u of \"%s\"",
				 blknum, file->path);

		blknum = header.block;

		if (header.compressed_size == PageIsTruncated)
		{
			/*
			 * Backup contains information that this block was truncated.
			 * We need to truncate file to this length.
			 */
			truncate_from = blknum;
			need_truncate = true;
			break;
		}

		Assert(header.compressed_size <= BLCKSZ);

		/* read a page from file */
		read_len = fread(compressed_page.data, 1,
			MAXALIGN(header.compressed_size), in);
		if (read_len != MAXALIGN(header.compressed_size))
			elog(ERROR, "Cannot read block %u of \"%s\" read %zu of %d",
				blknum, file->path, read_len, header.compressed_size);

		/*
		 * if page size is smaller than BLCKSZ, decompress the page.
		 * BUGFIX for versions < 2.0.23: if page size is equal to BLCKSZ.
		 * we have to check, whether it is compressed or not using
		 * page_may_be_compressed() function.
		 */
		if (header.compressed_size != BLCKSZ
			|| page_may_be_compressed(compressed_page.data, file->compress_alg,
									  backup_version))
		{
			const char *errormsg = NULL;

			uncompressed_size = do_decompress(page.data, BLCKSZ,
											  compressed_page.data,
											  header.compressed_size,
											  file->compress_alg, &errormsg);
			if (uncompressed_size < 0 && errormsg != NULL)
				elog(WARNING, "An error occured during decompressing block %u of file \"%s\": %s",
					 blknum, file->path, errormsg);

			if (uncompressed_size != BLCKSZ)
				elog(ERROR, "Page of file \"%s\" uncompressed to %d bytes. != BLCKSZ",
					 file->path, uncompressed_size);
		}

		write_pos = (write_header) ? blknum * (BLCKSZ + sizeof(header)) :
									 blknum * BLCKSZ;

		/*
		 * Seek and write the restored page.
		 */
		if (fio_fseek(out, write_pos) < 0)
			elog(ERROR, "Cannot seek block %u of \"%s\": %s",
				 blknum, to_path, strerror(errno));

		if (write_header)
		{
			/* We uncompressed the page, so its size is BLCKSZ */
			header.compressed_size = BLCKSZ;
			if (fio_fwrite(out, &header, sizeof(header)) != sizeof(header))
				elog(ERROR, "Cannot write header of block %u of \"%s\": %s",
					 blknum, file->path, strerror(errno));
		}

		/* if we uncompressed the page - write page.data,
		 * if page wasn't compressed -
		 * write what we've read - compressed_page.data
		 */
		if (uncompressed_size == BLCKSZ)
		{
			if (fio_fwrite(out, page.data, BLCKSZ) != BLCKSZ)
				elog(ERROR, "Cannot write block %u of \"%s\": %s",
					 blknum, file->path, strerror(errno));
		}
		else
		{
			if (fio_fwrite(out, compressed_page.data, BLCKSZ) != BLCKSZ)
				elog(ERROR, "Cannot write block %u of \"%s\": %s",
					 blknum, file->path, strerror(errno));
		}
	}

	/*
	 * DELTA backup have no knowledge about truncated blocks as PAGE or PTRACK do
	 * But during DELTA backup we read every file in PGDATA and thus DELTA backup
	 * knows exact size of every file at the time of backup.
	 * So when restoring file from DELTA backup we, knowning it`s size at
	 * a time of a backup, can truncate file to this size.
	 */
	if (allow_truncate && file->n_blocks != BLOCKNUM_INVALID && !need_truncate)
	{
		struct stat st;
		if (fio_ffstat(out, &st) == 0 && st.st_size > file->n_blocks * BLCKSZ)
		{
			truncate_from = file->n_blocks;
			need_truncate = true;
		}
	}

	if (need_truncate)
	{
		off_t		write_pos;

		write_pos = (write_header) ? truncate_from * (BLCKSZ + sizeof(header)) :
									 truncate_from * BLCKSZ;

		/*
		 * Truncate file to this length.
		 */
		if (fio_ftruncate(out, write_pos) != 0)
			elog(ERROR, "Cannot truncate \"%s\": %s",
				 file->path, strerror(errno));
		elog(VERBOSE, "Delta truncate file %s to block %u",
			 file->path, truncate_from);
	}

	/* update file permission */
	if (fio_chmod(to_path, file->mode, FIO_DB_HOST) == -1)
	{
		int errno_tmp = errno;

		if (in)
			fclose(in);
		fio_fclose(out);
		elog(ERROR, "Cannot change mode of \"%s\": %s", to_path,
			 strerror(errno_tmp));
	}

	if (fio_fflush(out) != 0 ||
		fio_fclose(out))
		elog(ERROR, "Cannot write \"%s\": %s", to_path, strerror(errno));

	if (in)
		fclose(in);
}

/*
 * Copy file to backup.
 * We do not apply compression to these files, because
 * it is either small control file or already compressed cfs file.
 */
bool
copy_file(const char *from_root, fio_location from_location,
		  const char *to_root, fio_location to_location, pgFile *file)
{
	char		to_path[MAXPGPATH];
	FILE	   *in;
	FILE	   *out;
	size_t		read_len = 0;
	int			errno_tmp;
	char		buf[BLCKSZ];
	struct stat	st;
	pg_crc32	crc;

	INIT_FILE_CRC32(true, crc);

	/* reset size summary */
	file->read_size = 0;
	file->write_size = 0;

	/* open backup mode file for read */
	in = fio_fopen(file->path, PG_BINARY_R, from_location);
	if (in == NULL)
	{
		FIN_FILE_CRC32(true, crc);
		file->crc = crc;

		/* maybe deleted, it's not error */
		if (errno == ENOENT)
		{
			elog(LOG, "File \"%s\" is not found", file->path);
			file->write_size = FILE_NOT_FOUND;
			return false;
		}

		elog(ERROR, "cannot open source file \"%s\": %s", file->path,
			 strerror(errno));
	}

	/* open backup file for write  */
	join_path_components(to_path, to_root, file->path + strlen(from_root) + 1);
	out = fio_fopen(to_path, PG_BINARY_W, to_location);
	if (out == NULL)
	{
		int errno_tmp = errno;
		fio_fclose(in);
		elog(ERROR, "cannot open destination file \"%s\": %s",
			 to_path, strerror(errno_tmp));
	}

	/* stat source file to change mode of destination file */
	if (fio_ffstat(in, &st) == -1)
	{
		fio_fclose(in);
		fio_fclose(out);
		elog(ERROR, "cannot stat \"%s\": %s", file->path,
			 strerror(errno));
	}

	/* copy content and calc CRC */
	for (;;)
	{
		read_len = 0;

		if ((read_len = fio_fread(in, buf, sizeof(buf))) != sizeof(buf))
			break;

		if (fio_fwrite(out, buf, read_len) != read_len)
		{
			errno_tmp = errno;
			/* oops */
			fio_fclose(in);
			fio_fclose(out);
			elog(ERROR, "cannot write to \"%s\": %s", to_path,
				 strerror(errno_tmp));
		}
		/* update CRC */
		COMP_FILE_CRC32(true, crc, buf, read_len);

		file->read_size += read_len;
	}

	errno_tmp = errno;
	if (read_len < 0)
	{
		fio_fclose(in);
		fio_fclose(out);
		elog(ERROR, "cannot read backup mode file \"%s\": %s",
			 file->path, strerror(errno_tmp));
	}

	/* copy odd part. */
	if (read_len > 0)
	{
		if (fio_fwrite(out, buf, read_len) != read_len)
		{
			errno_tmp = errno;
			/* oops */
			fio_fclose(in);
			fio_fclose(out);
			elog(ERROR, "cannot write to \"%s\": %s", to_path,
				 strerror(errno_tmp));
		}
		/* update CRC */
		COMP_FILE_CRC32(true, crc, buf, read_len);

		file->read_size += read_len;
	}

	file->write_size = (int64) file->read_size;
	/* finish CRC calculation and store into pgFile */
	FIN_FILE_CRC32(true, crc);
	file->crc = crc;

	/* update file permission */
	if (fio_chmod(to_path, st.st_mode, to_location) == -1)
	{
		errno_tmp = errno;
		fio_fclose(in);
		fio_fclose(out);
		elog(ERROR, "cannot change mode of \"%s\": %s", to_path,
			 strerror(errno_tmp));
	}

	if (fio_fflush(out) != 0 ||
		fio_fclose(out))
		elog(ERROR, "cannot write \"%s\": %s", to_path, strerror(errno));
	fio_fclose(in);

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

	errmsg = fio_gzerror(gzf, &gz_errnum);
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
copy_meta(const char *from_path, fio_location from_location, const char *to_path, fio_location to_location,  bool unlink_on_error)
{
	struct stat st;

	if (fio_stat(from_path, &st, true, from_location) == -1)
	{
		if (unlink_on_error)
			fio_unlink(to_path, to_location);
		elog(ERROR, "Cannot stat file \"%s\": %s",
			 from_path, strerror(errno));
	}

	if (fio_chmod(to_path, st.st_mode, to_location) == -1)
	{
		if (unlink_on_error)
			fio_unlink(to_path, to_location);
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
	int			out = -1;
	char		buf[XLOG_BLCKSZ];
	const char *to_path_p;
	char		to_path_temp[MAXPGPATH];
	int			errno_temp;

#ifdef HAVE_LIBZ
	char		gz_to_path[MAXPGPATH];
	gzFile		gz_out = NULL;

	if (is_compress)
	{
		snprintf(gz_to_path, sizeof(gz_to_path), "%s.gz", to_path);
		to_path_p = gz_to_path;
	}
	else
#endif
		to_path_p = to_path;

	/* open file for read */
	in = fio_fopen(from_path, PG_BINARY_R, FIO_DB_HOST);
	if (in == NULL)
		elog(ERROR, "Cannot open source WAL file \"%s\": %s", from_path,
			 strerror(errno));

	/* Check if possible to skip copying */
	if (fileExists(to_path_p, FIO_BACKUP_HOST))
	{
		if (fileEqualCRC(from_path, to_path_p, is_compress))
			return;
			/* Do not copy and do not rise error. Just quit as normal. */
		else if (!overwrite)
			elog(ERROR, "WAL segment \"%s\" already exists.", to_path_p);
	}

	/* open backup file for write  */
#ifdef HAVE_LIBZ
	if (is_compress)
	{
		snprintf(to_path_temp, sizeof(to_path_temp), "%s.partial", gz_to_path);

		gz_out = fio_gzopen(to_path_temp, PG_BINARY_W, instance_config.compress_level, FIO_BACKUP_HOST);
		if (gz_out == NULL)
			elog(ERROR, "Cannot open destination temporary WAL file \"%s\": %s",
				 to_path_temp, strerror(errno));
	}
	else
#endif
	{
		snprintf(to_path_temp, sizeof(to_path_temp), "%s.partial", to_path);

		out = fio_open(to_path_temp, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, FIO_BACKUP_HOST);
		if (out < 0)
			elog(ERROR, "Cannot open destination temporary WAL file \"%s\": %s",
				 to_path_temp, strerror(errno));
	}

	/* copy content */
	for (;;)
	{
		ssize_t		read_len = 0;

		read_len = fio_fread(in, buf, sizeof(buf));

		if (read_len < 0)
		{
			errno_temp = errno;
			fio_unlink(to_path_temp, FIO_BACKUP_HOST);
			elog(ERROR,
				 "Cannot read source WAL file \"%s\": %s",
				 from_path, strerror(errno_temp));
		}

		if (read_len > 0)
		{
#ifdef HAVE_LIBZ
			if (is_compress)
			{
				if (fio_gzwrite(gz_out, buf, read_len) != read_len)
				{
					errno_temp = errno;
					fio_unlink(to_path_temp, FIO_BACKUP_HOST);
					elog(ERROR, "Cannot write to compressed WAL file \"%s\": %s",
						 to_path_temp, get_gz_error(gz_out, errno_temp));
				}
			}
			else
#endif
			{
				if (fio_write(out, buf, read_len) != read_len)
				{
					errno_temp = errno;
					fio_unlink(to_path_temp, FIO_BACKUP_HOST);
					elog(ERROR, "Cannot write to WAL file \"%s\": %s",
						 to_path_temp, strerror(errno_temp));
				}
			}
		}

		if (read_len == 0)
			break;
	}

#ifdef HAVE_LIBZ
	if (is_compress)
	{
		if (fio_gzclose(gz_out) != 0)
		{
			errno_temp = errno;
			fio_unlink(to_path_temp, FIO_BACKUP_HOST);
			elog(ERROR, "Cannot close compressed WAL file \"%s\": %s",
				 to_path_temp, get_gz_error(gz_out, errno_temp));
		}
	}
	else
#endif
	{
		if (fio_flush(out) != 0 || fio_close(out) != 0)
		{
			errno_temp = errno;
			fio_unlink(to_path_temp, FIO_BACKUP_HOST);
			elog(ERROR, "Cannot write WAL file \"%s\": %s",
				 to_path_temp, strerror(errno_temp));
		}
	}

	if (fio_fclose(in))
	{
		errno_temp = errno;
		fio_unlink(to_path_temp, FIO_BACKUP_HOST);
		elog(ERROR, "Cannot close source WAL file \"%s\": %s",
			 from_path, strerror(errno_temp));
	}

	/* update file permission. */
	copy_meta(from_path, FIO_DB_HOST, to_path_temp, FIO_BACKUP_HOST, true);

	if (fio_rename(to_path_temp, to_path_p, FIO_BACKUP_HOST) < 0)
	{
		errno_temp = errno;
		fio_unlink(to_path_temp, FIO_BACKUP_HOST);
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
	int			out;
	char		buf[XLOG_BLCKSZ];
	const char *from_path_p = from_path;
	char		to_path_temp[MAXPGPATH];
	int			errno_temp;
	bool		is_decompress = false;

#ifdef HAVE_LIBZ
	char		gz_from_path[MAXPGPATH];
	gzFile		gz_in = NULL;
#endif

	/* First check source file for existance */
	if (fio_access(from_path, F_OK, FIO_BACKUP_HOST) != 0)
	{
#ifdef HAVE_LIBZ
		/*
		 * Maybe we need to decompress the file. Check it with .gz
		 * extension.
		 */
		snprintf(gz_from_path, sizeof(gz_from_path), "%s.gz", from_path);
		if (fio_access(gz_from_path, F_OK, FIO_BACKUP_HOST) == 0)
		{
			/* Found compressed file */
			is_decompress = true;
			from_path_p = gz_from_path;
		}
#endif
		/* Didn't find compressed file */
		if (!is_decompress)
			elog(ERROR, "Source WAL file \"%s\" doesn't exist",
				 from_path);
	}

	/* open file for read */
	if (!is_decompress)
	{
		in = fio_fopen(from_path, PG_BINARY_R, FIO_BACKUP_HOST);
		if (in == NULL)
			elog(ERROR, "Cannot open source WAL file \"%s\": %s",
					from_path, strerror(errno));
	}
#ifdef HAVE_LIBZ
	else
	{
		gz_in = fio_gzopen(gz_from_path, PG_BINARY_R, Z_DEFAULT_COMPRESSION,
						   FIO_BACKUP_HOST);
		if (gz_in == NULL)
			elog(ERROR, "Cannot open compressed WAL file \"%s\": %s",
					 gz_from_path, strerror(errno));
	}
#endif

	/* open backup file for write  */
	snprintf(to_path_temp, sizeof(to_path_temp), "%s.partial", to_path);

	out = fio_open(to_path_temp, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, FIO_DB_HOST);
	if (out < 0)
		elog(ERROR, "Cannot open destination temporary WAL file \"%s\": %s",
				to_path_temp, strerror(errno));

	/* copy content */
	for (;;)
	{
		int read_len = 0;

#ifdef HAVE_LIBZ
		if (is_decompress)
		{
			read_len = fio_gzread(gz_in, buf, sizeof(buf));
			if (read_len <= 0 && !fio_gzeof(gz_in))
			{
				errno_temp = errno;
				fio_unlink(to_path_temp, FIO_DB_HOST);
				elog(ERROR, "Cannot read compressed WAL file \"%s\": %s",
					 gz_from_path, get_gz_error(gz_in, errno_temp));
			}
		}
		else
#endif
		{
			read_len = fio_fread(in, buf, sizeof(buf));
			if (ferror(in))
			{
				errno_temp = errno;
				fio_unlink(to_path_temp, FIO_DB_HOST);
				elog(ERROR, "Cannot read source WAL file \"%s\": %s",
					 from_path, strerror(errno_temp));
			}
		}

		if (read_len > 0)
		{
			if (fio_write(out, buf, read_len) != read_len)
			{
				errno_temp = errno;
				fio_unlink(to_path_temp, FIO_DB_HOST);
				elog(ERROR, "Cannot write to WAL file \"%s\": %s", to_path_temp,
					 strerror(errno_temp));
			}
		}

		/* Check for EOF */
#ifdef HAVE_LIBZ
		if (is_decompress)
		{
			if (fio_gzeof(gz_in) || read_len == 0)
				break;
		}
		else
#endif
		{
			if (/* feof(in) || */ read_len == 0)
				break;
		}
	}

	if (fio_flush(out) != 0 || fio_close(out) != 0)
	{
		errno_temp = errno;
		fio_unlink(to_path_temp, FIO_DB_HOST);
		elog(ERROR, "Cannot write WAL file \"%s\": %s",
			 to_path_temp, strerror(errno_temp));
	}

#ifdef HAVE_LIBZ
	if (is_decompress)
	{
		if (fio_gzclose(gz_in) != 0)
		{
			errno_temp = errno;
			fio_unlink(to_path_temp, FIO_DB_HOST);
			elog(ERROR, "Cannot close compressed WAL file \"%s\": %s",
				 gz_from_path, get_gz_error(gz_in, errno_temp));
		}
	}
	else
#endif
	{
		if (fio_fclose(in))
		{
			errno_temp = errno;
			fio_unlink(to_path_temp, FIO_DB_HOST);
			elog(ERROR, "Cannot close source WAL file \"%s\": %s",
				 from_path, strerror(errno_temp));
		}
	}

	/* update file permission. */
	copy_meta(from_path_p, FIO_BACKUP_HOST, to_path_temp, FIO_DB_HOST, true);

	if (fio_rename(to_path_temp, to_path, FIO_DB_HOST) < 0)
	{
		errno_temp = errno;
		fio_unlink(to_path_temp, FIO_DB_HOST);
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
void
calc_file_checksum(pgFile *file, fio_location location)
{
	Assert(S_ISREG(file->mode));

	file->crc = pgFileGetCRC(file->path, true, false, &file->read_size, location);
	file->write_size = file->read_size;
}

/*
 * Validate given page.
 *
 * Returns value:
 * 0  - if the page is not found
 * 1  - if the page is found and valid
 * -1 - if the page is found but invalid
 */
#define PAGE_IS_NOT_FOUND 0
#define PAGE_IS_FOUND_AND_VALID 1
#define PAGE_IS_FOUND_AND_NOT_VALID -1
static int
validate_one_page(Page page, pgFile *file,
				  BlockNumber blknum, XLogRecPtr stop_lsn,
				  uint32 checksum_version)
{
	PageHeader	phdr;
	XLogRecPtr	lsn;

	/* new level of paranoia */
	if (page == NULL)
	{
		elog(LOG, "File \"%s\", block %u, page is NULL", file->path, blknum);
		return PAGE_IS_NOT_FOUND;
	}

	phdr = (PageHeader) page;

	if (PageIsNew(page))
	{
		int			i;

		/* Check if the page is zeroed. */
		for(i = 0; i < BLCKSZ && page[i] == 0; i++);

		if (i == BLCKSZ)
		{
			elog(LOG, "File: %s blknum %u, page is New, empty zeroed page",
				 file->path, blknum);
			return PAGE_IS_FOUND_AND_VALID;
		}
		else
		{
			elog(WARNING, "File: %s blknum %u, page is New, but not zeroed",
				 file->path, blknum);
		}

		/* Page is zeroed. No sense in checking header and checksum. */
		return PAGE_IS_FOUND_AND_VALID;
	}

	/* Verify checksum */
	if (checksum_version)
	{
		/* Checksums are enabled, so check them. */
		if (!(pg_checksum_page(page, file->segno * RELSEG_SIZE + blknum)
			== ((PageHeader) page)->pd_checksum))
		{
			elog(WARNING, "File: %s blknum %u have wrong checksum",
				 file->path, blknum);
			return PAGE_IS_FOUND_AND_NOT_VALID;
		}
	}

	/* Check page for the sights of insanity.
	 * TODO: We should give more information about what exactly is looking "wrong"
	 */
	if (!(PageGetPageSize(phdr) == BLCKSZ &&
		PageGetPageLayoutVersion(phdr) == PG_PAGE_LAYOUT_VERSION &&
		(phdr->pd_flags & ~PD_VALID_FLAG_BITS) == 0 &&
		phdr->pd_lower >= SizeOfPageHeaderData &&
		phdr->pd_lower <= phdr->pd_upper &&
		phdr->pd_upper <= phdr->pd_special &&
		phdr->pd_special <= BLCKSZ &&
		phdr->pd_special == MAXALIGN(phdr->pd_special)))
	{
		/* Page does not looking good */
		elog(WARNING, "Page header is looking insane: %s, block %i",
			file->path, blknum);
		return PAGE_IS_FOUND_AND_NOT_VALID;
	}

	/* At this point page header is sane, if checksums are enabled - the`re ok.
	 * Check that page is not from future.
	 */
	if (stop_lsn > 0)
	{
		/* Get lsn from page header. Ensure that page is from our time. */
		lsn = PageXLogRecPtrGet(phdr->pd_lsn);

		if (lsn > stop_lsn)
		{
			elog(WARNING, "File: %s, block %u, checksum is %s. "
						  "Page is from future: pageLSN %X/%X stopLSN %X/%X",
				file->path, blknum, checksum_version ? "correct" : "not enabled",
				(uint32) (lsn >> 32), (uint32) lsn,
				(uint32) (stop_lsn >> 32), (uint32) stop_lsn);
			return PAGE_IS_FOUND_AND_NOT_VALID;
		}
	}

	return PAGE_IS_FOUND_AND_VALID;
}

/*
 * Valiate pages of datafile in PGDATA one by one.
 *
 * returns true if the file is valid
 * also returns true if the file was not found
 */
bool
check_data_file(backup_files_arg* arguments,
				pgFile *file)
{
	FILE		*in;
	BlockNumber	blknum = 0;
	BlockNumber	nblocks = 0;
	BlockNumber n_blocks_skipped = 0;
	int			page_state;
	char		curr_page[BLCKSZ];
	bool 		is_valid = true;

	in = fopen(file->path, PG_BINARY_R);
	if (in == NULL)
	{
		/*
		 * If file is not found, this is not en error.
		 * It could have been deleted by concurrent postgres transaction.
		 */
		if (errno == ENOENT)
		{
			elog(LOG, "File \"%s\" is not found", file->path);
			return true;
		}

		elog(WARNING, "cannot open file \"%s\": %s",
			 file->path, strerror(errno));
		return false;
	}

	if (file->size % BLCKSZ != 0)
	{
		fclose(in);
		elog(WARNING, "File: %s, invalid file size %zu", file->path, file->size);
	}

	/*
	 * Compute expected number of blocks in the file.
	 * NOTE This is a normal situation, if the file size has changed
	 * since the moment we computed it.
	 */
	nblocks = file->size/BLCKSZ;

	for (blknum = 0; blknum < nblocks; blknum++)
	{
		page_state = prepare_page(arguments, file, InvalidXLogRecPtr,
									blknum, nblocks, in, &n_blocks_skipped,
									BACKUP_MODE_FULL, curr_page, false);

		if (page_state == PageIsTruncated)
			break;

		if (page_state == PageIsCorrupted)
		{
			/* Page is corrupted, no need to elog about it,
			 * prepare_page() already done that
			 */
			is_valid = false;
			continue;
		}

		/* At this point page is found and its checksum is ok, if any
		 * but could be 'insane'
		 * TODO: between prepare_page and validate_one_page we
		 * compute and compare checksum twice, it`s ineffective
		 */
		if (validate_one_page(curr_page, file, blknum,
								  InvalidXLogRecPtr,
								  0) == PAGE_IS_FOUND_AND_NOT_VALID)
		{
			/* Page is corrupted */
			is_valid = false;
		}
	}

	fclose(in);
	return is_valid;
}

/* Valiate pages of datafile in backup one by one */
bool
check_file_pages(pgFile *file, XLogRecPtr stop_lsn, uint32 checksum_version,
				 uint32 backup_version)
{
	size_t		read_len = 0;
	bool		is_valid = true;
	FILE		*in;
	pg_crc32	crc;
	bool		use_crc32c = backup_version <= 20021 || backup_version >= 20025;

	elog(VERBOSE, "Validate relation blocks for file %s", file->path);

	in = fopen(file->path, PG_BINARY_R);
	if (in == NULL)
	{
		if (errno == ENOENT)
		{
			elog(WARNING, "File \"%s\" is not found", file->path);
			return false;
		}

		elog(ERROR, "Cannot open file \"%s\": %s",
			 file->path, strerror(errno));
	}

	/* calc CRC of backup file */
	INIT_FILE_CRC32(use_crc32c, crc);

	/* read and validate pages one by one */
	while (true)
	{
		DataPage	compressed_page; /* used as read buffer */
		DataPage	page;
		BackupPageHeader header;
		BlockNumber blknum = 0;

		if (interrupted || thread_interrupted)
			elog(ERROR, "Interrupted during data file validation");

		/* read BackupPageHeader */
		read_len = fread(&header, 1, sizeof(header), in);
		if (read_len != sizeof(header))
		{
			int			errno_tmp = errno;
			if (read_len == 0 && feof(in))
				break;		/* EOF found */
			else if (read_len != 0 && feof(in))
				elog(WARNING,
					 "Odd size page found at block %u of \"%s\"",
					 blknum, file->path);
			else
				elog(WARNING, "Cannot read header of block %u of \"%s\": %s",
					 blknum, file->path, strerror(errno_tmp));
			return false;
		}

		COMP_FILE_CRC32(use_crc32c, crc, &header, read_len);

		if (header.block == 0 && header.compressed_size == 0)
		{
			elog(VERBOSE, "Skip empty block of \"%s\"", file->path);
			continue;
		}

		if (header.block < blknum)
		{
			elog(WARNING, "Backup is broken at block %u of \"%s\"",
				 blknum, file->path);
			return false;
		}

		blknum = header.block;

		if (header.compressed_size == PageIsTruncated)
		{
			elog(LOG, "Block %u of \"%s\" is truncated",
				 blknum, file->path);
			continue;
		}

		Assert(header.compressed_size <= BLCKSZ);

		read_len = fread(compressed_page.data, 1,
			MAXALIGN(header.compressed_size), in);
		if (read_len != MAXALIGN(header.compressed_size))
		{
			elog(WARNING, "Cannot read block %u of \"%s\" read %zu of %d",
				blknum, file->path, read_len, header.compressed_size);
			return false;
		}

		COMP_FILE_CRC32(use_crc32c, crc, compressed_page.data, read_len);

		if (header.compressed_size != BLCKSZ
			|| page_may_be_compressed(compressed_page.data, file->compress_alg,
									  backup_version))
		{
			int32		uncompressed_size = 0;
			const char *errormsg = NULL;

			uncompressed_size = do_decompress(page.data, BLCKSZ,
											  compressed_page.data,
											  header.compressed_size,
											  file->compress_alg,
											  &errormsg);
			if (uncompressed_size < 0 && errormsg != NULL)
				elog(WARNING, "An error occured during decompressing block %u of file \"%s\": %s",
					 blknum, file->path, errormsg);

			if (uncompressed_size != BLCKSZ)
			{
				if (header.compressed_size == BLCKSZ)
				{
					is_valid = false;
					continue;
				}
				elog(WARNING, "Page of file \"%s\" uncompressed to %d bytes. != BLCKSZ",
					 file->path, uncompressed_size);
				return false;
			}

			if (validate_one_page(page.data, file, blknum,
								  stop_lsn, checksum_version) == PAGE_IS_FOUND_AND_NOT_VALID)
				is_valid = false;
		}
		else
		{
			if (validate_one_page(compressed_page.data, file, blknum,
				stop_lsn, checksum_version) == PAGE_IS_FOUND_AND_NOT_VALID)
				is_valid = false;
		}
	}

	FIN_FILE_CRC32(use_crc32c, crc);
	fclose(in);

	if (crc != file->crc)
	{
		elog(WARNING, "Invalid CRC of backup file \"%s\": %X. Expected %X",
				file->path, file->crc, crc);
		is_valid = false;
	}

	return is_valid;
}

static bool
fileEqualCRC(const char *path1, const char *path2, bool path2_is_compressed)
{
	pg_crc32	crc1;
	pg_crc32	crc2;

	/* Get checksum of backup file */
#ifdef HAVE_LIBZ
	if (path2_is_compressed)
	{
		char 		buf [1024];
		gzFile		gz_in = NULL;

		INIT_FILE_CRC32(true, crc2);
		gz_in = fio_gzopen(path2, PG_BINARY_R, Z_DEFAULT_COMPRESSION, FIO_BACKUP_HOST);
		if (gz_in == NULL)
			/* File cannot be read */
			elog(ERROR,
					 "Cannot compare WAL file \"%s\" with compressed \"%s\"",
					 path1, path2);

		for (;;)
		{
			int read_len = fio_gzread(gz_in, buf, sizeof(buf));
			if (read_len <= 0 && !fio_gzeof(gz_in))
			{
				/* An error occurred while reading the file */
				elog(WARNING,
					 "Cannot compare WAL file \"%s\" with compressed \"%s\": %d",
					 path1, path2, read_len);
				return false;
			}
			COMP_FILE_CRC32(true, crc2, buf, read_len);
			if (fio_gzeof(gz_in) || read_len == 0)
				break;
		}
		FIN_FILE_CRC32(true, crc2);

		if (fio_gzclose(gz_in) != 0)
			elog(ERROR, "Cannot close compressed WAL file \"%s\": %s",
				 path2, get_gz_error(gz_in, errno));
	}
	else
#endif
	{
		crc2 = pgFileGetCRC(path2, true, true, NULL, FIO_BACKUP_HOST);
	}

	/* Get checksum of original file */
	crc1 = pgFileGetCRC(path1, true, true, NULL, FIO_DB_HOST);

	return EQ_CRC32C(crc1, crc2);
}
