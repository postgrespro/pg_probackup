/*-------------------------------------------------------------------------
 *
 * data.c: utils to parse and backup data pages
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2019, Postgres Professional
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
int32
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

#if PG_VERSION_NUM >= 120000
			return pglz_decompress(src, src_size, dst, dst_size, true);
#else
			return pglz_decompress(src, src_size, dst, dst_size);
#endif
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
	//	  PageGetPageLayoutVersion(phdr) == PG_PAGE_LAYOUT_VERSION &&
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
		/* otherwise let's try to decompress the page */
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
	//	PageGetPageLayoutVersion(phdr) == PG_PAGE_LAYOUT_VERSION &&
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
 * 2  - if the page is found but zeroed
 * 1  - if the page is found and valid
 * 0  - if the page is not found, probably truncated
 * -1 - if the page is found but read size is not multiple of BLKSIZE
 * -2 - if the page is found but page header is "insane"
 * -3 - if the page is found but page checksumm is wrong
 * -4 - something went wrong, check errno
 *
 */
static int
read_page_from_file(pgFile *file, BlockNumber blknum,
					FILE *in, Page page, XLogRecPtr *page_lsn,
					uint32 checksum_version)
{
	off_t		offset = blknum * BLCKSZ;
	ssize_t		read_len = 0;

	/* read the block */
	read_len = fio_pread(in, page, offset);

	if (read_len != BLCKSZ)
	{

		/* The block could have been truncated. It is fine. */
		if (read_len == 0)
			return 0;
		else if (read_len > 0)
			return -1;
		else
			return -4;
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
		for (i = 0; i < BLCKSZ && page[i] == 0; i++);

		/* Page is zeroed. No need to check header and checksum. */
		if (i == BLCKSZ)
			return 2;

		return -2;
	}

	/* Verify checksum */
	if (checksum_version)
	{
		BlockNumber blkno = file->segno * RELSEG_SIZE + blknum;
		/*
		 * If checksum is wrong, sleep a bit and then try again
		 * several times. If it didn't help, throw error
		 */
		if (pg_checksum_page(page, blkno) != ((PageHeader) page)->pd_checksum)
			return -3;
		else
			/* page header and checksum are correct */
			return 1;
	}
	else
		/* page header is correct and checksum check is disabled */
		return 1;
}

/*
 * Retrieves a page taking the backup mode into account
 * and writes it into argument "page". Argument "page"
 * should be a pointer to allocated BLCKSZ of bytes.
 *
 * Prints appropriate warnings/errors/etc into log.
 * Returns:
 *                 PageIsOk(0) if page was successfully retrieved
 *         PageIsTruncated(-2) if the page was truncated
 *         SkipCurrentPage(-3) if we need to skip this page
 *         PageIsCorrupted(-4) if the page check mismatch
 */
static int32
prepare_page(ConnectionArgs *conn_arg,
			 pgFile *file, XLogRecPtr prev_backup_start_lsn,
			 BlockNumber blknum, BlockNumber nblocks,
			 FILE *in, BackupMode backup_mode,
			 Page page, bool strict,
			 uint32 checksum_version,
			 int ptrack_version_num,
			 const char *ptrack_schema,
			 const char *from_fullpath)
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
	if (backup_mode != BACKUP_MODE_DIFF_PTRACK || ptrack_version_num >= 20)
	{
		while (!page_is_valid && try_again)
		{
			int result = read_page_from_file(file, blknum, in, page,
											 &page_lsn, checksum_version);

			switch (result)
			{
				case 2:
					elog(VERBOSE, "File: \"%s\" blknum %u, empty page", from_fullpath, blknum);
					return PageIsOk;

				case 1:
					page_is_valid = true;
					break;

				case 0:
					/* This block was truncated.*/
					page_is_truncated = true;
					/* Page is not actually valid, but it is absent
					 * and we're not going to reread it or validate */
					page_is_valid = true;

					elog(VERBOSE, "File \"%s\", block %u, file was truncated",
						from_fullpath, blknum);
					break;

				case -1:
					elog(WARNING, "File: \"%s\", block %u, partial read, try again",
							from_fullpath, blknum);
					break;

				case -2:
					elog(LOG, "File: \"%s\" blknum %u have wrong page header, try again",
							from_fullpath, blknum);
					break;

				case -3:
					elog(LOG, "File: \"%s\" blknum %u have wrong checksum, try again",
							from_fullpath, blknum);
					break;

				case -4:
					elog(LOG, "File: \"%s\" access error: %s",
							from_fullpath, strerror(errno));
					break;
			}

			/*
			 * If ptrack support is available use it to get invalid block
			 * instead of rereading it 99 times
			 */

			if (result < 0 && strict && ptrack_version_num > 0)
			{
				elog(WARNING, "File \"%s\", block %u, try to fetch via shared buffer",
					from_fullpath, blknum);
				break;
			}

			try_again--;
		}
		/*
		 * If page is not valid after 100 attempts to read it
		 * throw an error.
		 */

		if (!page_is_valid &&
			((strict && ptrack_version_num == 0) || !strict))
		{
			/* show this message for checkdb, merge or backup without ptrack support */
			elog(WARNING, "Corruption detected in file \"%s\", block %u",
						from_fullpath, blknum);
		}

		/* Backup with invalid block and without ptrack support must throw error */
		if (!page_is_valid && strict && ptrack_version_num == 0)
				elog(ERROR, "Data file corruption, canceling backup");

		/* Checkdb not going futher */
		if (!strict)
		{
			if (page_is_valid)
				return PageIsOk;
			else
				return PageIsCorrupted;
		}
	}

	/* Get page via ptrack interface from PostgreSQL shared buffer.
	 * We do this in following cases:
	 * 1. PTRACK backup of 1.x versions
	 * 2. During backup, regardless of backup mode, of PostgreSQL instance
	 *    with ptrack support we encountered invalid page.
	 */
	if ((backup_mode == BACKUP_MODE_DIFF_PTRACK
		&& (ptrack_version_num >= 15 && ptrack_version_num < 20))
			|| !page_is_valid)
	{
		size_t page_size = 0;
		Page ptrack_page = NULL;
		ptrack_page = (Page) pg_ptrack_get_block(conn_arg, file->dbOid, file->tblspcOid,
										  file->relOid, absolute_blknum, &page_size,
										  ptrack_version_num, ptrack_schema);

		if (ptrack_page == NULL)
		{
			/* This block was truncated.*/
			page_is_truncated = true;
		}
		else if (page_size != BLCKSZ)
		{
			free(ptrack_page);
			elog(ERROR, "File: \"%s\", block %u, expected block size %d, but read %zu",
					   from_fullpath, absolute_blknum, BLCKSZ, page_size);
		}
		else
		{
			/*
			 * We need to copy the page that was successfully
			 * retrieved from ptrack into our output "page" parameter.
			 * We must set checksum here, because it is outdated
			 * in the block recieved from shared buffers.
			 */
			memcpy(page, ptrack_page, BLCKSZ);
			free(ptrack_page);
			if (checksum_version)
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

	if (page_is_truncated)
		return PageIsTruncated;

	/*
	 * Skip page if page lsn is less than START_LSN of parent backup.
	 * Nullified pages must be copied by DELTA backup, just to be safe.
	 */
	if (backup_mode == BACKUP_MODE_DIFF_DELTA &&
		file->exists_in_prev &&
		page_lsn &&
		page_lsn < prev_backup_start_lsn)
	{
		elog(VERBOSE, "Skipping blknum %u in file: \"%s\"", blknum, from_fullpath);
		return SkipCurrentPage;
	}

	return PageIsOk;
}

static void
compress_and_backup_page(pgFile *file, BlockNumber blknum,
						FILE *in, FILE *out, pg_crc32 *crc,
						int page_state, Page page,
						CompressAlg calg, int clevel,
						const char *from_fullpath, const char *to_fullpath)
{
	BackupPageHeader header;
	size_t		write_buffer_size = sizeof(header);
	char		write_buffer[BLCKSZ+sizeof(header)];
	char		compressed_page[BLCKSZ*2]; /* compressed page may require more space than uncompressed */
	const char *errormsg = NULL;

	header.block = blknum;
	header.compressed_size = page_state;


	/* The page was not truncated, so we need to compress it */
	header.compressed_size = do_compress(compressed_page, sizeof(compressed_page),
										 page, BLCKSZ, calg, clevel,
										 &errormsg);
	/* Something went wrong and errormsg was assigned, throw a warning */
	if (header.compressed_size < 0 && errormsg != NULL)
		elog(WARNING, "An error occured during compressing block %u of file \"%s\": %s",
			 blknum, from_fullpath, errormsg);

	file->compress_alg = calg;

	/* The page was successfully compressed. */
	if (header.compressed_size > 0 && header.compressed_size < BLCKSZ)
	{
		memcpy(write_buffer, &header, sizeof(header));
		memcpy(write_buffer + sizeof(header),
			   compressed_page, header.compressed_size);
		write_buffer_size += MAXALIGN(header.compressed_size);
	}
	/* Non-positive value means that compression failed. Write it as is. */
	else
	{
		header.compressed_size = BLCKSZ;
		memcpy(write_buffer, &header, sizeof(header));
		memcpy(write_buffer + sizeof(header), page, BLCKSZ);
		write_buffer_size += header.compressed_size;
	}

	/* Update CRC */
	COMP_FILE_CRC32(true, *crc, write_buffer, write_buffer_size);

	/* write data page */
	if (fio_fwrite(out, write_buffer, write_buffer_size) != write_buffer_size)
		elog(ERROR, "File: \"%s\", cannot write at block %u: %s",
			 to_fullpath, blknum, strerror(errno));

	file->write_size += write_buffer_size;
	file->uncompressed_size += BLCKSZ;
}

/*
 * Backup data file in the from_root directory to the to_root directory with
 * same relative path. If prev_backup_start_lsn is not NULL, only pages with
 * higher lsn will be copied.
 * Not just copy file, but read it block by block (use bitmap in case of
 * incremental backup), validate checksum, optionally compress and write to
 * backup with special header.
 */
void
backup_data_file(ConnectionArgs* conn_arg, pgFile *file,
				 const char *from_fullpath, const char *to_fullpath,
				 XLogRecPtr prev_backup_start_lsn, BackupMode backup_mode,
				 CompressAlg calg, int clevel, uint32 checksum_version,
				 int ptrack_version_num, const char *ptrack_schema, bool missing_ok)
{
	FILE		*in;
	FILE		*out;
	BlockNumber	blknum = 0;
	BlockNumber	nblocks = 0;		/* number of blocks in file */
	BlockNumber	n_blocks_skipped = 0;
	BlockNumber	n_blocks_read = 0;  /* number of blocks actually readed
									 * TODO: we should report them */
	int			page_state;
	char		curr_page[BLCKSZ];

	/* stdio buffers */
	char 		in_buffer[STDIO_BUFSIZE];
	char 		out_buffer[STDIO_BUFSIZE];

	/* sanity */
	if (file->size % BLCKSZ != 0)
		elog(WARNING, "File: \"%s\", invalid file size %zu", from_fullpath, file->size);

	/*
	 * Compute expected number of blocks in the file.
	 * NOTE This is a normal situation, if the file size has changed
	 * since the moment we computed it.
	 */
	nblocks = file->size/BLCKSZ;

	/* set n_blocks for a file */
	file->n_blocks = nblocks;

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
		 * There are no changed blocks since last backup. We want to make
		 * incremental backup, so we should exit.
		 */
		file->write_size = BYTES_INVALID;
		return;
	}

	/* reset size summary */
	file->read_size = 0;
	file->write_size = 0;
	file->uncompressed_size = 0;
	INIT_FILE_CRC32(true, file->crc);

	/* open source file for read */
	in = fio_fopen(from_fullpath, PG_BINARY_R, FIO_DB_HOST);
	if (in == NULL)
	{
		FIN_FILE_CRC32(true, file->crc);

		/*
		 * If file is not found, this is not en error.
		 * It could have been deleted by concurrent postgres transaction.
		 */
		if (errno == ENOENT)
		{
			if (missing_ok)
			{
				elog(LOG, "File \"%s\" is not found", from_fullpath);
				file->write_size = FILE_NOT_FOUND;
				return;
			}
			else
				elog(ERROR, "File \"%s\" is not found", from_fullpath);
		}

		/* In all other cases throw an error */
		elog(ERROR, "Cannot open file \"%s\": %s",
			 from_fullpath, strerror(errno));
	}

	if (!fio_is_remote_file(in))
		setbuf(in, in_buffer);

	/* open backup file for write  */
	out = fopen(to_fullpath, PG_BINARY_W);
	if (out == NULL)
		elog(ERROR, "Cannot open backup file \"%s\": %s",
			 to_fullpath, strerror(errno));

	setbuf(out, out_buffer);

	/* update file permission */
	if (chmod(to_fullpath, FILE_PERMISSION) == -1)
		elog(ERROR, "Cannot change mode of \"%s\": %s", to_fullpath,
			 strerror(errno));

	/*
	 * Read each page, verify checksum and write it to backup.
	 * If page map is empty or file is not present in previous backup
	 * backup all pages of the relation.
	 *
	 * Usually enter here if backup_mode is FULL or DELTA.
	 * Also in some cases even PAGE backup is going here,
	 * becase not all data files are logged into WAL,
	 * for example CREATE DATABASE.
	 * Such files should be fully copied.

	 * In PTRACK 1.x there was a problem
	 * of data files with missing _ptrack map.
	 * Such files should be fully copied.
	 */
	if (file->pagemap.bitmapsize == PageBitmapIsEmpty ||
		file->pagemap_isabsent || !file->exists_in_prev)
	{
		/* remote FULL and DELTA */
		if (fio_is_remote_file(in))
		{
			int rc = fio_send_pages(in, out, file,
									backup_mode == BACKUP_MODE_DIFF_DELTA &&
									file->exists_in_prev ? prev_backup_start_lsn : InvalidXLogRecPtr,
									&n_blocks_skipped, calg, clevel);

			if (rc == PAGE_CHECKSUM_MISMATCH && ptrack_version_num >= 15)
				 /* only ptrack versions 1.5, 1.6, 1.7 and 2.x support this functionality */
				goto RetryUsingPtrack;
			if (rc < 0)
				elog(ERROR, "Failed to read file \"%s\": %s",
					 from_fullpath,
					 rc == PAGE_CHECKSUM_MISMATCH ? "data file checksum mismatch" : strerror(-rc));

			/* TODO: check that fio_send_pages ain`t lying about number of readed blocks */
			n_blocks_read = rc;

			file->read_size = n_blocks_read * BLCKSZ;
			file->uncompressed_size = (n_blocks_read - n_blocks_skipped)*BLCKSZ;
		}
		else
		{
			/* local FULL and DELTA */
		  RetryUsingPtrack:
			for (blknum = 0; blknum < nblocks; blknum++)
			{
				page_state = prepare_page(conn_arg, file, prev_backup_start_lsn,
											  blknum, nblocks, in, backup_mode,
											  curr_page, true, checksum_version,
											  ptrack_version_num, ptrack_schema,
											  from_fullpath);

				if (page_state == PageIsTruncated)
					break;

				else if (page_state == SkipCurrentPage)
					n_blocks_skipped++;

				else if (page_state == PageIsOk)
					compress_and_backup_page(file, blknum, in, out, &(file->crc),
												page_state, curr_page, calg, clevel,
												from_fullpath, to_fullpath);
				else
					elog(ERROR, "Invalid page state: %i, file: %s, blknum %i",
						page_state, file->rel_path, blknum);

				n_blocks_read++;
				file->read_size += BLCKSZ;
			}
		}
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
			page_state = prepare_page(conn_arg, file, prev_backup_start_lsn,
											  blknum, nblocks, in, backup_mode,
											  curr_page, true, checksum_version,
											  ptrack_version_num, ptrack_schema,
											  from_fullpath);

			if (page_state == PageIsTruncated)
				break;

			/* TODO: PAGE and PTRACK should never get SkipCurrentPage */
			else if (page_state == SkipCurrentPage)
				n_blocks_skipped++;

			else if (page_state == PageIsOk)
				compress_and_backup_page(file, blknum, in, out, &(file->crc),
											page_state, curr_page, calg, clevel,
											from_fullpath, to_fullpath);
			else
				elog(ERROR, "Invalid page state: %i, file: %s, blknum %i",
							page_state, file->rel_path, blknum);

			n_blocks_read++;
			file->read_size += BLCKSZ;
		}

		pg_free(file->pagemap.bitmap);
		pg_free(iter);
	}

	if (fclose(out))
		elog(ERROR, "Cannot close the backup file \"%s\": %s",
			 to_fullpath, strerror(errno));

	fio_fclose(in);

	FIN_FILE_CRC32(true, file->crc);

	/* Determine that file didn`t changed in case of incremental backup */
	if (backup_mode != BACKUP_MODE_FULL &&
		file->exists_in_prev &&
		file->write_size == 0 &&
		file->n_blocks > 0)
	{
		file->write_size = BYTES_INVALID;
	}

	/*
	 * No point in storing empty files.
	 */
	if (file->write_size <= 0)
	{
		if (unlink(to_fullpath) == -1)
			elog(ERROR, "Cannot remove file \"%s\": %s", to_fullpath,
				 strerror(errno));
	}
}

/*
 * Backup non data file
 * We do not apply compression to this file.
 * If file exists in previous backup, then compare checksums
 * and make a decision about copying or skiping the file.
 */
void
backup_non_data_file(pgFile *file, pgFile *prev_file,
				 const char *from_fullpath, const char *to_fullpath,
				 BackupMode backup_mode, time_t parent_backup_time,
				 bool missing_ok)
{
	/* special treatment for global/pg_control */
	if (file->external_dir_num == 0 && strcmp(file->rel_path, XLOG_CONTROL_FILE) == 0)
		return copy_pgcontrol_file(from_fullpath, FIO_DB_HOST,
							to_fullpath, FIO_BACKUP_HOST, file);

	/*
	 * If non-data file exists in previous backup
	 * and its mtime is less than parent backup start time ... */
	if (prev_file && file->exists_in_prev &&
		file->mtime <= parent_backup_time)
	{

		file->crc = fio_get_crc32(from_fullpath, FIO_DB_HOST, false);

		/* ...and checksum is the same... */
		if (EQ_TRADITIONAL_CRC32(file->crc, prev_file->crc))
		{
			file->write_size = BYTES_INVALID;
			return; /* ...skip copying file. */
		}
	}

	backup_non_data_file_internal(from_fullpath, FIO_DB_HOST,
								  to_fullpath, file, true);
}

/*
 * Iterate over parent backup chain and lookup given destination file in
 * filelist of every chain member starting with FULL backup.
 * Apply changed blocks to destination file from every backup in parent chain.
 */
size_t
restore_data_file(parray *parent_chain, pgFile *dest_file, FILE *out, const char *to_fullpath)
{
	int i;
	size_t total_write_len = 0;
	char 		buffer[STDIO_BUFSIZE];

	for (i = parray_num(parent_chain) - 1; i >= 0; i--)
	{
		char		from_root[MAXPGPATH];
		char		from_fullpath[MAXPGPATH];
		FILE		*in = NULL;

		pgFile	   **res_file = NULL;
		pgFile	   *tmp_file = NULL;

		pgBackup   *backup = (pgBackup *) parray_get(parent_chain, i);

		/* lookup file in intermediate backup */
		res_file =  parray_bsearch(backup->files, dest_file, pgFileCompareRelPathWithExternal);
		tmp_file = (res_file) ? *res_file : NULL;

		/* Destination file is not exists yet at this moment */
		if (tmp_file == NULL)
			continue;

		/*
		 * Skip file if it haven't changed since previous backup
		 * and thus was not backed up.
		 */
		if (tmp_file->write_size == BYTES_INVALID)
			continue;

		if (tmp_file->write_size == 0)
			continue;

		/*
		 * At this point we are sure, that something is going to be copied
		 * Open source file.
		 */
		join_path_components(from_root, backup->root_dir, DATABASE_DIR);
		join_path_components(from_fullpath, from_root, tmp_file->rel_path);

		in = fopen(from_fullpath, PG_BINARY_R);
		if (in == NULL)
			elog(ERROR, "Cannot open backup file \"%s\": %s", from_fullpath,
				 strerror(errno));

		setbuf(in, buffer);

		/*
		 * Restore the file.
		 * Datafiles are backed up block by block and every block
		 * have BackupPageHeader with meta information, so we cannot just
		 * copy the file from backup.
		 */
		total_write_len += restore_data_file_internal(in, out, tmp_file,
					  parse_program_version(backup->program_version),
					  from_fullpath, to_fullpath, dest_file->n_blocks);

		if (fclose(in) != 0)
			elog(ERROR, "Cannot close file \"%s\": %s", from_fullpath,
				strerror(errno));
	}
	return total_write_len;
}

size_t
restore_data_file_internal(FILE *in, FILE *out, pgFile *file, uint32 backup_version,
					  const char *from_fullpath, const char *to_fullpath, int nblocks)
{
	BackupPageHeader header;
	BlockNumber	blknum = 0;
	size_t	write_len = 0;

	for (;;)
	{
		off_t		write_pos;
		size_t		read_len;
		DataPage	page;
		int32		compressed_size = 0;
		bool		is_compressed = false;

		/* check for interrupt */
		if (interrupted || thread_interrupted)
			elog(ERROR, "Interrupted during data file restore");

		/* read BackupPageHeader */
		read_len = fread(&header, 1, sizeof(header), in);

		if (read_len != sizeof(header))
		{
			int errno_tmp = errno;
			if (read_len == 0 && feof(in))
				break;		/* EOF found */
			else if (read_len != 0 && feof(in))
				elog(ERROR, "Odd size page found at block %u of \"%s\"",
					 blknum, from_fullpath);
			else
				elog(ERROR, "Cannot read header of block %u of \"%s\": %s",
					 blknum, from_fullpath, strerror(errno_tmp));
		}

		/* Consider empty blockm. wtf empty block ? */
		if (header.block == 0 && header.compressed_size == 0)
		{
			elog(VERBOSE, "Skip empty block of \"%s\"", from_fullpath);
			continue;
		}

		/* sanity? */
		if (header.block < blknum)
			elog(ERROR, "Backup is broken at block %u of \"%s\"",
				 blknum, from_fullpath);

		blknum = header.block;

		/*
		 * Backupward compatibility kludge: in the good old days
		 * n_blocks attribute was available only in DELTA backups.
		 * File truncate in PAGE and PTRACK happened on the fly when
		 * special value PageIsTruncated is encountered.
		 * It was inefficient.
		 *
		 * Nowadays every backup type has n_blocks, so instead
		 * writing and then truncating redundant data, writing
		 * is not happening in the first place.
		 * TODO: remove in 3.0.0
		 */
		compressed_size = header.compressed_size;

		if (compressed_size == PageIsTruncated)
		{
			/*
			 * Block header contains information that this block was truncated.
			 * We need to truncate file to this length.
			 */

			elog(VERBOSE, "Truncate file \"%s\" to block %u", to_fullpath, header.block);

			/* To correctly truncate file, we must first flush STDIO buffers */
			if (fio_fflush(out) != 0)
				elog(ERROR, "Cannot flush file \"%s\": %s", to_fullpath, strerror(errno));

			/* Set position to the start of file */
			if (fio_fseek(out, 0) < 0)
				elog(ERROR, "Cannot seek to the start of file \"%s\": %s", to_fullpath, strerror(errno));

			if (fio_ftruncate(out, header.block * BLCKSZ) != 0)
				elog(ERROR, "Cannot truncate file \"%s\": %s", to_fullpath, strerror(errno));

			break;
		}

		/* no point in writing redundant data */
		if (nblocks > 0 && blknum >= nblocks)
			break;

		if (compressed_size > BLCKSZ)
			elog(ERROR, "Size of a blknum %i exceed BLCKSZ", blknum);

		/* read a page from file */
		read_len = fread(page.data, 1, MAXALIGN(compressed_size), in);

		if (read_len != MAXALIGN(compressed_size))
			elog(ERROR, "Cannot read block %u of \"%s\", read %zu of %d",
				blknum, from_fullpath, read_len, compressed_size);

		/*
		 * if page size is smaller than BLCKSZ, decompress the page.
		 * BUGFIX for versions < 2.0.23: if page size is equal to BLCKSZ.
		 * we have to check, whether it is compressed or not using
		 * page_may_be_compressed() function.
		 */
		if (header.compressed_size != BLCKSZ
			|| page_may_be_compressed(page.data, file->compress_alg,
									  backup_version))
		{
			is_compressed = true;
		}

		write_pos = blknum * BLCKSZ;

		/*
		 * Seek and write the restored page.
		 */
		if (fio_fseek(out, write_pos) < 0)
			elog(ERROR, "Cannot seek block %u of \"%s\": %s",
				 blknum, to_fullpath, strerror(errno));

		/* If page is compressed and restore is in remote mode, send compressed
		 * page to the remote side.
		 */
		if (is_compressed)
		{
			ssize_t rc;
			rc = fio_fwrite_compressed(out, page.data, compressed_size, file->compress_alg);

			if (!fio_is_remote_file(out) && rc != BLCKSZ)
				elog(ERROR, "Cannot write block %u of \"%s\": %s, size: %u",
					 blknum, to_fullpath, strerror(errno), compressed_size);
		}
		else
		{
			if (fio_fwrite(out, page.data, BLCKSZ) != BLCKSZ)
				elog(ERROR, "Cannot write block %u of \"%s\": %s",
					 blknum, to_fullpath, strerror(errno));
		}

		write_len += BLCKSZ;
	}

	elog(VERBOSE, "Copied file \"%s\": %lu bytes", from_fullpath, write_len);
	return write_len;
}

/*
 * Copy file to backup.
 * We do not apply compression to these files, because
 * it is either small control file or already compressed cfs file.
 */
void
restore_non_data_file_internal(FILE *in, FILE *out, pgFile *file,
					  const char *from_fullpath, const char *to_fullpath)
{
	ssize_t		read_len = 0;
	char		buf[STDIO_BUFSIZE]; /* 64kB buffer */

	/* copy content */
	for (;;)
	{
		read_len = 0;

		/* check for interrupt */
		if (interrupted || thread_interrupted)
			elog(ERROR, "Interrupted during non-data file restore");

		read_len = fread(buf, 1, sizeof(buf), in);

		if (read_len == 0 && feof(in))
			break;

		if (read_len < 0)
			elog(ERROR, "Cannot read backup file \"%s\": %s",
				 from_fullpath, strerror(errno));

		if (fio_fwrite(out, buf, read_len) != read_len)
			elog(ERROR, "Cannot write to \"%s\": %s", to_fullpath,
				 strerror(errno));
	}

	elog(VERBOSE, "Copied file \"%s\": %lu bytes", from_fullpath, file->write_size);
}

size_t
restore_non_data_file(parray *parent_chain, pgBackup *dest_backup,
					  pgFile *dest_file, FILE *out, const char *to_fullpath)
{
	int			i;
	char		from_root[MAXPGPATH];
	char		from_fullpath[MAXPGPATH];
	FILE		*in = NULL;

	pgFile		*tmp_file = NULL;
	pgBackup	*tmp_backup = NULL;
	char 		buffer[STDIO_BUFSIZE];

	/* Check if full copy of destination file is available in destination backup */
	if (dest_file->write_size > 0)
	{
		tmp_file = dest_file;
		tmp_backup = dest_backup;
	}
	else
	{
		/*
		 * Iterate over parent chain starting from direct parent of destination
		 * backup to oldest backup in chain, and look for the first
		 * full copy of destination file.
		 * Full copy is latest possible destination file with size equal or
		 * greater than zero.
		 */
		for (i = 1; i < parray_num(parent_chain); i++)
		{
			pgFile	   **res_file = NULL;

			tmp_backup = (pgBackup *) parray_get(parent_chain, i);

			/* lookup file in intermediate backup */
			res_file =  parray_bsearch(tmp_backup->files, dest_file, pgFileCompareRelPathWithExternal);
			tmp_file = (res_file) ? *res_file : NULL;

			/*
			 * It should not be possible not to find destination file in intermediate
			 * backup, without encountering full copy first.
			 */
			if (!tmp_file)
			{
				elog(ERROR, "Failed to locate non-data file \"%s\" in backup %s",
					dest_file->rel_path, base36enc(tmp_backup->start_time));
				continue;
			}

			/* Full copy is found and it is null sized, nothing to do here */
			if (tmp_file->write_size == 0)
				return 0;

			/* Full copy is found */
			if (tmp_file->write_size > 0)
				break;
		}
	}

	/* sanity */
	if (!tmp_backup)
		elog(ERROR, "Failed to found a backup containing full copy of non-data file \"%s\"",
			to_fullpath);

	if (!tmp_file)
		elog(ERROR, "Failed to locate a full copy of non-data file \"%s\"", to_fullpath);

	if (tmp_file->external_dir_num == 0)
		join_path_components(from_root, tmp_backup->root_dir, DATABASE_DIR);
	else
	{
		char		external_prefix[MAXPGPATH];

		join_path_components(external_prefix, tmp_backup->root_dir, EXTERNAL_DIR);
		makeExternalDirPathByNum(from_root, external_prefix, tmp_file->external_dir_num);
	}

	join_path_components(from_fullpath, from_root, dest_file->rel_path);

	in = fopen(from_fullpath, PG_BINARY_R);
	if (in == NULL)
		elog(ERROR, "Cannot open backup file \"%s\": %s", from_fullpath,
			 strerror(errno));

	setbuf(in, buffer);

	/* do actual work */
	restore_non_data_file_internal(in, out, tmp_file, from_fullpath, to_fullpath);

	if (fclose(in) != 0)
		elog(ERROR, "Cannot close file \"%s\": %s", from_fullpath,
			strerror(errno));

	return tmp_file->write_size;
}

/*
 * Copy file to backup.
 * We do not apply compression to these files, because
 * it is either small control file or already compressed cfs file.
 */
void
backup_non_data_file_internal(const char *from_fullpath,
							fio_location from_location,
							const char *to_fullpath, pgFile *file,
							bool missing_ok)
{
	FILE	   *in;
	FILE	   *out;
	ssize_t		read_len = 0;
	char		buf[STDIO_BUFSIZE]; /* 64kB buffer */
	pg_crc32	crc;

	INIT_FILE_CRC32(true, crc);

	/* reset size summary */
	file->read_size = 0;
	file->write_size = 0;
	file->uncompressed_size = 0;

	/* open source file for read */
	in = fio_fopen(from_fullpath, PG_BINARY_R, from_location);
	if (in == NULL)
	{
		FIN_FILE_CRC32(true, crc);
		file->crc = crc;

		/* maybe deleted, it's not error in case of backup */
		if (errno == ENOENT)
		{
			if (missing_ok)
			{
				elog(LOG, "File \"%s\" is not found", from_fullpath);
				file->write_size = FILE_NOT_FOUND;
				return;
			}
			else
				elog(ERROR, "File \"%s\" is not found", from_fullpath);
		}

		elog(ERROR, "Cannot open source file \"%s\": %s", from_fullpath,
			 strerror(errno));
	}

	/* open backup file for write  */
	out = fopen(to_fullpath, PG_BINARY_W);
	if (out == NULL)
		elog(ERROR, "Cannot open destination file \"%s\": %s",
			 to_fullpath, strerror(errno));

	/* update file permission */
	if (chmod(to_fullpath, file->mode) == -1)
		elog(ERROR, "Cannot change mode of \"%s\": %s", to_fullpath,
			 strerror(errno));

	/* copy content and calc CRC */
	for (;;)
	{
		read_len = fio_fread(in, buf, sizeof(buf));

		if (read_len == 0)
			break;

		if (read_len < 0)
			elog(ERROR, "Cannot read from source file \"%s\": %s",
				 from_fullpath, strerror(errno));

		if (fwrite(buf, 1, read_len, out) != read_len)
			elog(ERROR, "Cannot write to \"%s\": %s", to_fullpath,
				 strerror(errno));

		/* update CRC */
		COMP_FILE_CRC32(true, crc, buf, read_len);

		file->read_size += read_len;
	}

	file->write_size = (int64) file->read_size;

	if (file->write_size > 0)
		file->uncompressed_size = file->write_size;
	/* finish CRC calculation and store into pgFile */
	FIN_FILE_CRC32(true, crc);
	file->crc = crc;

	if (fclose(out))
		elog(ERROR, "Cannot write \"%s\": %s", to_fullpath, strerror(errno));
	fio_fclose(in);
}

/*
 * Create empty file, used for partial restore
 */
bool
create_empty_file(fio_location from_location, const char *to_root,
		  fio_location to_location, pgFile *file)
{
	char		to_path[MAXPGPATH];
	FILE	   *out;

	/* open file for write  */
	join_path_components(to_path, to_root, file->rel_path);
	out = fio_fopen(to_path, PG_BINARY_W, to_location);

	if (out == NULL)
		elog(ERROR, "Cannot open destination file \"%s\": %s",
			 to_path, strerror(errno));

	/* update file permission */
	if (fio_chmod(to_path, file->mode, to_location) == -1)
		elog(ERROR, "Cannot change mode of \"%s\": %s", to_path,
			 strerror(errno));

	if (fio_fclose(out))
		elog(ERROR, "Cannot close \"%s\": %s", to_path, strerror(errno));

	return true;
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
	//	PageGetPageLayoutVersion(phdr) == PG_PAGE_LAYOUT_VERSION &&
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
check_data_file(ConnectionArgs *arguments, pgFile *file,
				const char *from_fullpath, uint32 checksum_version)
{
	FILE		*in;
	BlockNumber	blknum = 0;
	BlockNumber	nblocks = 0;
	int			page_state;
	char		curr_page[BLCKSZ];
	bool 		is_valid = true;

	in = fopen(from_fullpath, PG_BINARY_R);
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

		elog(WARNING, "Cannot open file \"%s\": %s",
					from_fullpath, strerror(errno));
		return false;
	}

	if (file->size % BLCKSZ != 0)
		elog(WARNING, "File: \"%s\", invalid file size %zu", file->path, file->size);

	/*
	 * Compute expected number of blocks in the file.
	 * NOTE This is a normal situation, if the file size has changed
	 * since the moment we computed it.
	 */
	nblocks = file->size/BLCKSZ;

	for (blknum = 0; blknum < nblocks; blknum++)
	{

		page_state = prepare_page(NULL, file, InvalidXLogRecPtr,
									blknum, nblocks, in, BACKUP_MODE_FULL,
									curr_page, false, checksum_version,
									0, NULL, from_fullpath);

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

	elog(VERBOSE, "Validate relation blocks for file \"%s\"", file->path);

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
				file->path, crc, file->crc);
		is_valid = false;
	}

	return is_valid;
}
