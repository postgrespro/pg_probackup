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
typedef struct DataPage
{
	BackupPageHeader bph;
	char		data[BLCKSZ];
} DataPage;

static bool get_page_header(FILE *in, const char *fullpath, BackupPageHeader* bph,
							pg_crc32 *crc, bool use_crc32c);

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

/* We know that header is invalid, store specific
 * details in errormsg.
 */
void
get_header_errormsg(Page page, char **errormsg)
{
	PageHeader  phdr = (PageHeader) page;
	*errormsg = pgut_malloc(ERRMSG_MAX_LEN);

	if (PageGetPageSize(phdr) != BLCKSZ)
		snprintf(*errormsg, ERRMSG_MAX_LEN, "page header invalid, "
				"page size %lu is not equal to block size %u",
				PageGetPageSize(phdr), BLCKSZ);

	else if (phdr->pd_lower < SizeOfPageHeaderData)
		snprintf(*errormsg, ERRMSG_MAX_LEN, "page header invalid, "
				"pd_lower %i is less than page header size %lu",
				phdr->pd_lower, SizeOfPageHeaderData);

	else if (phdr->pd_lower > phdr->pd_upper)
		snprintf(*errormsg, ERRMSG_MAX_LEN, "page header invalid, "
				"pd_lower %u is greater than pd_upper %u",
				phdr->pd_lower, phdr->pd_upper);

	else if (phdr->pd_upper > phdr->pd_special)
		snprintf(*errormsg, ERRMSG_MAX_LEN, "page header invalid, "
				"pd_upper %u is greater than pd_special %u",
				phdr->pd_upper, phdr->pd_special);

	else if (phdr->pd_special > BLCKSZ)
		snprintf(*errormsg, ERRMSG_MAX_LEN, "page header invalid, "
				"pd_special %u is greater than block size %u",
				phdr->pd_special, BLCKSZ);

	else if (phdr->pd_special != MAXALIGN(phdr->pd_special))
		snprintf(*errormsg, ERRMSG_MAX_LEN, "page header invalid, "
				"pd_special %i is misaligned, expected %lu",
				phdr->pd_special, MAXALIGN(phdr->pd_special));

	else if (phdr->pd_flags & ~PD_VALID_FLAG_BITS)
		snprintf(*errormsg, ERRMSG_MAX_LEN, "page header invalid, "
				"pd_flags mask contain illegal bits");

	else
		snprintf(*errormsg, ERRMSG_MAX_LEN, "page header invalid");
}

/* We know that checksumms are mismatched, store specific
 * details in errormsg.
 */
void
get_checksum_errormsg(Page page, char **errormsg, BlockNumber absolute_blkno)
{
	PageHeader	phdr = (PageHeader) page;
	*errormsg = pgut_malloc(ERRMSG_MAX_LEN);

	snprintf(*errormsg, ERRMSG_MAX_LEN,
			 "page verification failed, "
			 "calculated checksum %u but expected %u",
			 phdr->pd_checksum,
			 pg_checksum_page(page, absolute_blkno));
}

/*
 * Retrieves a page taking the backup mode into account
 * and writes it into argument "page". Argument "page"
 * should be a pointer to allocated BLCKSZ of bytes.
 *
 * Prints appropriate warnings/errors/etc into log.
 * Returns:
 *                 PageIsOk(0) if page was successfully retrieved
 *         PageIsTruncated(-1) if the page was truncated
 *         SkipCurrentPage(-2) if we need to skip this page,
 *                                only used for DELTA backup
 *         PageIsCorrupted(-3) if the page checksum mismatch
 *                                or header corruption,
 *                                only used for checkdb
 *                                TODO: probably we should always
 *                                      return it to the caller
 */
static int32
prepare_page(ConnectionArgs *conn_arg,
			 pgFile *file, XLogRecPtr prev_backup_start_lsn,
			 BlockNumber blknum, FILE *in,
			 BackupMode backup_mode,
			 Page page, bool strict,
			 uint32 checksum_version,
			 int ptrack_version_num,
			 const char *ptrack_schema,
			 const char *from_fullpath,
			 PageState *page_st)
{
	int			try_again = PAGE_READ_ATTEMPTS;
	bool		page_is_valid = false;
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
		int rc = 0;
		while (!page_is_valid && try_again--)
		{
			/* read the block */
			int read_len = fio_pread(in, page, blknum * BLCKSZ);

			/* The block could have been truncated. It is fine. */
			if (read_len == 0)
			{
				elog(VERBOSE, "Cannot read block %u of \"%s\": "
							"block truncated", blknum, from_fullpath);
				return PageIsTruncated;
			}
			else if (read_len < 0)
				elog(ERROR, "Cannot read block %u of \"%s\": %s",
						blknum, from_fullpath, strerror(errno));
			else if (read_len != BLCKSZ)
				elog(WARNING, "Cannot read block %u of \"%s\": "
								"read %i of %d, try again",
						blknum, from_fullpath, read_len, BLCKSZ);
			else
			{
				/* We have BLCKSZ of raw data, validate it */
				rc = validate_one_page(page, absolute_blknum,
									   InvalidXLogRecPtr, page_st,
									   checksum_version);
				switch (rc)
				{
					case PAGE_IS_ZEROED:
						elog(VERBOSE, "File: \"%s\" blknum %u, empty page", from_fullpath, blknum);
						return PageIsOk;

					case PAGE_IS_VALID:
						/* in DELTA mode we must compare lsn */
						if (backup_mode == BACKUP_MODE_DIFF_DELTA)
							page_is_valid = true;
						else
							return PageIsOk;
						break;

					case PAGE_HEADER_IS_INVALID:
						elog(VERBOSE, "File: \"%s\" blknum %u have wrong page header, try again",
								from_fullpath, blknum);
						break;

					case PAGE_CHECKSUM_MISMATCH:
						elog(VERBOSE, "File: \"%s\" blknum %u have wrong checksum, try again",
								from_fullpath, blknum);
						break;
					default:
						Assert(false);
				}
			}
		}

		/*
		 * If page is not valid after 100 attempts to read it
		 * throw an error.
		 */
		if (!page_is_valid)
		{
			int elevel = ERROR;
			char *errormsg = NULL;

			/* Get the details of corruption */
			if (rc == PAGE_HEADER_IS_INVALID)
				get_header_errormsg(page, &errormsg);
			else if (rc == PAGE_CHECKSUM_MISMATCH)
				get_checksum_errormsg(page, &errormsg,
									  file->segno * RELSEG_SIZE + blknum);

			/* Error out in case of merge or backup without ptrack support;
			 * issue warning in case of checkdb or backup with ptrack support
			 */
			if (!strict)
				elevel = WARNING;

			if (errormsg)
				elog(elevel, "Corruption detected in file \"%s\", block %u: %s",
								from_fullpath, blknum, errormsg);
			else
				elog(elevel, "Corruption detected in file \"%s\", block %u",
								from_fullpath, blknum);

			pg_free(errormsg);
			return PageIsCorrupted;
		}

		/* Checkdb not going futher */
		if (!strict)
			return PageIsOk;
	}

	/*
	 * Get page via ptrack interface from PostgreSQL shared buffer.
	 * We do this only in the cases of PTRACK 1.x versions backup
	 */
	if (backup_mode == BACKUP_MODE_DIFF_PTRACK
		&& (ptrack_version_num >= 15 && ptrack_version_num < 20))
	{
		int rc = 0;
		size_t page_size = 0;
		Page ptrack_page = NULL;
		ptrack_page = (Page) pg_ptrack_get_block(conn_arg, file->dbOid, file->tblspcOid,
										  file->relOid, absolute_blknum, &page_size,
										  ptrack_version_num, ptrack_schema);

		if (ptrack_page == NULL)
			/* This block was truncated.*/
			return PageIsTruncated;

		if (page_size != BLCKSZ)
			elog(ERROR, "File: \"%s\", block %u, expected block size %d, but read %zu",
					   from_fullpath, blknum, BLCKSZ, page_size);

		/*
		 * We need to copy the page that was successfully
		 * retrieved from ptrack into our output "page" parameter.
		 */
		memcpy(page, ptrack_page, BLCKSZ);
		pg_free(ptrack_page);

		/*
		 * UPD: It apprears that is possible to get zeroed page or page with invalid header
		 * from shared buffer.
		 * Note, that getting page with wrong checksumm from shared buffer is
		 * acceptable.
		 */
		rc = validate_one_page(page, absolute_blknum,
							   InvalidXLogRecPtr, page_st,
							   checksum_version);

		/* It is ok to get zeroed page */
		if (rc == PAGE_IS_ZEROED)
			return PageIsOk;

		/* Getting page with invalid header from shared buffers is unacceptable */
		if (rc == PAGE_HEADER_IS_INVALID)
		{
			char *errormsg = NULL;
			get_header_errormsg(page, &errormsg);
			elog(ERROR, "Corruption detected in file \"%s\", block %u: %s",
								from_fullpath, blknum, errormsg);
		}

		/*
		 * We must set checksum here, because it is outdated
		 * in the block recieved from shared buffers.
		 */
		if (checksum_version)
			page_st->checksum = ((PageHeader) page)->pd_checksum = pg_checksum_page(page, absolute_blknum);
	}

	/*
	 * Skip page if page lsn is less than START_LSN of parent backup.
	 * Nullified pages must be copied by DELTA backup, just to be safe.
	 */
	if (backup_mode == BACKUP_MODE_DIFF_DELTA &&
		file->exists_in_prev &&
		page_st->lsn > 0 &&
		page_st->lsn < prev_backup_start_lsn)
	{
		elog(VERBOSE, "Skipping blknum %u in file: \"%s\"", blknum, from_fullpath);
		return SkipCurrentPage;
	}

	return PageIsOk;
}

/* split this function in two: compress() and backup() */
static int
compress_and_backup_page(pgFile *file, BlockNumber blknum,
						FILE *in, FILE *out, pg_crc32 *crc,
						int page_state, Page page,
						CompressAlg calg, int clevel,
						const char *from_fullpath, const char *to_fullpath)
{
	int         compressed_size = 0;
	size_t		write_buffer_size = 0;
	char        write_buffer[BLCKSZ*2];  /* compressed page may require more space than uncompressed */
	BackupPageHeader* bph = (BackupPageHeader*)write_buffer;
	const char *errormsg = NULL;

	/* Compress the page */
	compressed_size = do_compress(write_buffer + sizeof(BackupPageHeader),
								  sizeof(write_buffer) - sizeof(BackupPageHeader),
								  page, BLCKSZ, calg, clevel,
								  &errormsg);
	/* Something went wrong and errormsg was assigned, throw a warning */
	if (compressed_size < 0 && errormsg != NULL)
		elog(WARNING, "An error occured during compressing block %u of file \"%s\": %s",
			 blknum, from_fullpath, errormsg);

	file->compress_alg = calg; /* TODO: wtf? why here? */

	/* compression didn`t worked */
	if (compressed_size <= 0 || compressed_size >= BLCKSZ)
	{
		/* Do not compress page */
		memcpy(write_buffer + sizeof(BackupPageHeader), page, BLCKSZ);
		compressed_size = BLCKSZ;
	}
	bph->block = blknum;
	bph->compressed_size = compressed_size;
	write_buffer_size = compressed_size + sizeof(BackupPageHeader);

	/* Update CRC */
	COMP_FILE_CRC32(true, *crc, write_buffer, write_buffer_size);

	/* write data page */
	if (fio_fwrite(out, write_buffer, write_buffer_size) != write_buffer_size)
		elog(ERROR, "File: \"%s\", cannot write at block %u: %s",
			 to_fullpath, blknum, strerror(errno));

	file->write_size += write_buffer_size;
	file->uncompressed_size += BLCKSZ;

	return compressed_size;
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
				 int ptrack_version_num, const char *ptrack_schema,
				 HeaderMap *hdr_map, bool is_merge)
{
	int         rc;
	bool        use_pagemap;
	char       *errmsg = NULL;
	BlockNumber	err_blknum = 0;
	/* page headers */
	BackupPageHeader2 *headers = NULL;

	/* sanity */
	if (file->size % BLCKSZ != 0)
		elog(WARNING, "File: \"%s\", invalid file size %zu", from_fullpath, file->size);

	/*
	 * Compute expected number of blocks in the file.
	 * NOTE This is a normal situation, if the file size has changed
	 * since the moment we computed it.
	 */
	file->n_blocks = file->size/BLCKSZ;

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

	/*
	 * Read each page, verify checksum and write it to backup.
	 * If page map is empty or file is not present in previous backup
	 * backup all pages of the relation.
	 *
	 * In PTRACK 1.x there was a problem
	 * of data files with missing _ptrack map.
	 * Such files should be fully copied.
	 */

	if 	(file->pagemap.bitmapsize == PageBitmapIsEmpty ||
		 file->pagemap_isabsent || !file->exists_in_prev ||
		 !file->pagemap.bitmap)
		use_pagemap = false;
	else
		use_pagemap = true;

	/* Remote mode */
	if (fio_is_remote(FIO_DB_HOST))
	{

		rc = fio_send_pages(to_fullpath, from_fullpath, file,
							/* send prev backup START_LSN */
							backup_mode == BACKUP_MODE_DIFF_DELTA &&
							file->exists_in_prev ? prev_backup_start_lsn : InvalidXLogRecPtr,
							calg, clevel, checksum_version,
							/* send pagemap if any */
							use_pagemap,
							/* variables for error reporting */
							&err_blknum, &errmsg, &headers);
	}
	else
	{
		/* TODO: stop handling errors internally */
		rc = send_pages(conn_arg, to_fullpath, from_fullpath, file,
						/* send prev backup START_LSN */
						backup_mode == BACKUP_MODE_DIFF_DELTA &&
						file->exists_in_prev ? prev_backup_start_lsn : InvalidXLogRecPtr,
						calg, clevel, checksum_version, use_pagemap,
						&headers, backup_mode, ptrack_version_num, ptrack_schema);
	}

	/* check for errors */
	if (rc == FILE_MISSING)
	{
		elog(is_merge ? ERROR : LOG, "File not found: \"%s\"", from_fullpath);
		file->write_size = FILE_NOT_FOUND;
		goto cleanup;
	}

	else if (rc == WRITE_FAILED)
		elog(ERROR, "Cannot write block %u of \"%s\": %s",
				err_blknum, to_fullpath, strerror(errno));

	else if (rc == PAGE_CORRUPTION)
	{
		if (errmsg)
			elog(ERROR, "Corruption detected in file \"%s\", block %u: %s",
					from_fullpath, err_blknum, errmsg);
		else
			elog(ERROR, "Corruption detected in file \"%s\", block %u",
					from_fullpath, err_blknum);
	}
	/* OPEN_FAILED and READ_FAILED */
	else if (rc == OPEN_FAILED)
	{
		if (errmsg)
			elog(ERROR, "%s", errmsg);
		else
			elog(ERROR, "Cannot open file \"%s\"", from_fullpath);
	}
	else if (rc == READ_FAILED)
	{
		if (errmsg)
			elog(ERROR, "%s", errmsg);
		else
			elog(ERROR, "Cannot read file \"%s\"", from_fullpath);
	}

	file->read_size = rc * BLCKSZ;

	/* refresh n_blocks for FULL and DELTA */
	if (backup_mode == BACKUP_MODE_FULL ||
	    backup_mode == BACKUP_MODE_DIFF_DELTA)
		file->n_blocks = file->read_size / BLCKSZ;

	/* Determine that file didn`t changed in case of incremental backup */
	if (backup_mode != BACKUP_MODE_FULL &&
		file->exists_in_prev &&
		file->write_size == 0 &&
		file->n_blocks > 0)
	{
		file->write_size = BYTES_INVALID;
	}

cleanup:

	/* finish CRC calculation */
	FIN_FILE_CRC32(true, file->crc);

	/* dump page headers */
	write_page_headers(headers, file, hdr_map, is_merge);

	pg_free(errmsg);
	pg_free(file->pagemap.bitmap);
	pg_free(headers);
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
	{
		copy_pgcontrol_file(from_fullpath, FIO_DB_HOST,
							to_fullpath, FIO_BACKUP_HOST, file);
		return;
	}

	/*
	 * If nonedata file exists in previous backup
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
								  to_fullpath, file, missing_ok);
}

/*
 * Iterate over parent backup chain and lookup given destination file in
 * filelist of every chain member starting with FULL backup.
 * Apply changed blocks to destination file from every backup in parent chain.
 */
size_t
restore_data_file(parray *parent_chain, pgFile *dest_file, FILE *out,
				  const char *to_fullpath, bool use_bitmap, PageState *checksum_map,
				  XLogRecPtr shift_lsn, datapagemap_t *lsn_map, bool use_headers)
{
	size_t total_write_len = 0;
	char  *in_buf = pgut_malloc(STDIO_BUFSIZE);
	int    backup_seq = 0;

	/*
	 * FULL -> INCR -> DEST
	 *  2       1       0
	 * Restore of backups of older versions cannot be optimized with bitmap
	 * because of n_blocks
	 */
	if (use_bitmap)
		/* start with dest backup  */
		backup_seq = 0;
	else
		/* start with full backup */
		backup_seq = parray_num(parent_chain) - 1;

//	for (i = parray_num(parent_chain) - 1; i >= 0; i--)
//	for (i = 0; i < parray_num(parent_chain); i++)
	while (backup_seq >= 0 && backup_seq < parray_num(parent_chain))
	{
		char     from_root[MAXPGPATH];
		char     from_fullpath[MAXPGPATH];
		FILE    *in = NULL;

		pgFile **res_file = NULL;
		pgFile  *tmp_file = NULL;

		/* page headers */
		BackupPageHeader2 *headers = NULL;

		pgBackup   *backup = (pgBackup *) parray_get(parent_chain, backup_seq);

		if (use_bitmap)
			backup_seq++;
		else
			backup_seq--;

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

		/* If file was truncated in intermediate backup,
		 * it is ok not to truncate it now, because old blocks will be
		 * overwritten by new blocks from next backup.
		 */
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

		/* set stdio buffering for input data file */
		setvbuf(in, in_buf, _IOFBF, STDIO_BUFSIZE);

		/* get headers for this file */
		if (use_headers && tmp_file->n_headers > 0)
			headers = get_data_file_headers(&(backup->hdr_map), tmp_file,
											parse_program_version(backup->program_version),
											true);

		if (use_headers && !headers && tmp_file->n_headers > 0)
			elog(ERROR, "Failed to get page headers for file \"%s\"", from_fullpath);

		/*
		 * Restore the file.
		 * Datafiles are backed up block by block and every block
		 * have BackupPageHeader with meta information, so we cannot just
		 * copy the file from backup.
		 */
		total_write_len += restore_data_file_internal(in, out, tmp_file,
					  parse_program_version(backup->program_version),
					  from_fullpath, to_fullpath, dest_file->n_blocks,
					  use_bitmap ? &(dest_file)->pagemap : NULL,
					  checksum_map, backup->checksum_version,
					  /* shiftmap can be used only if backup state precedes the shift */
					  backup->stop_lsn <= shift_lsn ? lsn_map : NULL,
					  headers);

		if (fclose(in) != 0)
			elog(ERROR, "Cannot close file \"%s\": %s", from_fullpath,
				strerror(errno));

		pg_free(headers);

//		datapagemap_print_debug(&(dest_file)->pagemap);
	}
	pg_free(in_buf);

	return total_write_len;
}

/* Restore block from "in" file to "out" file.
 * If "nblocks" is greater than zero, then skip restoring blocks,
 * whose position if greater than "nblocks".
 * If map is NULL, then page bitmap cannot be used for restore optimization
 * Page bitmap optimize restore of incremental chains, consisting of more than one
 * backup. We restoring from newest to oldest and page, once restored, marked in map.
 * When the same page, but in older backup, encountered, we check the map, if it is
 * marked as already restored, then page is skipped.
 */
size_t
restore_data_file_internal(FILE *in, FILE *out, pgFile *file, uint32 backup_version,
					  const char *from_fullpath, const char *to_fullpath, int nblocks,
					  datapagemap_t *map, PageState *checksum_map, int checksum_version,
					  datapagemap_t *lsn_map, BackupPageHeader2 *headers)
{
	BlockNumber	blknum = 0;
	int  	n_hdr = -1;
	size_t	write_len = 0;
	off_t   cur_pos_out = 0;
	off_t   cur_pos_in = 0;

	/* should not be possible */
	Assert(!(backup_version >= 20400 && file->n_headers <= 0));

	/*
	 * We rely on stdio buffering of input and output.
	 * For buffering to be efficient, we try to minimize the
	 * number of lseek syscalls, because it forces buffer flush.
	 * For that, we track current write position in
	 * output file and issue fseek only when offset of block to be
	 * written not equal to current write position, which happens
	 * a lot when blocks from incremental backup are restored,
	 * but should never happen in case of blocks from FULL backup.
	 */
	if (fio_fseek(out, cur_pos_out) < 0)
			elog(ERROR, "Cannot seek block %u of \"%s\": %s",
				blknum, to_fullpath, strerror(errno));

	for (;;)
	{
		off_t		write_pos;
		size_t		len;
		size_t		read_len;
		DataPage	page;
		int32		compressed_size = 0;
		bool		is_compressed = false;

		/* incremental restore vars */
		uint16     page_crc = 0;
		XLogRecPtr page_lsn = InvalidXLogRecPtr;

		/* check for interrupt */
		if (interrupted || thread_interrupted)
			elog(ERROR, "Interrupted during data file restore");

		/* newer backups have headers in separate storage */
		if (headers)
		{
			n_hdr++;
			if (n_hdr >= file->n_headers)
				break;

			blknum = headers[n_hdr].block;
			page_lsn = headers[n_hdr].lsn;
			page_crc = headers[n_hdr].checksum;
			/* calculate payload size by comparing current and next page positions,
			 * page header is not included */
			compressed_size = headers[n_hdr+1].pos - headers[n_hdr].pos - sizeof(BackupPageHeader);

			Assert(compressed_size > 0);
			Assert(compressed_size <= BLCKSZ);

			read_len = compressed_size + sizeof(BackupPageHeader);
		}
		else
		{
			/* We get into this function either when restoring old backup
			 * or when merging something. Align read_len only when restoring
			 * or merging old backups.
			 */
			if (get_page_header(in, from_fullpath, &(page).bph, NULL, false))
			{
				cur_pos_in += sizeof(BackupPageHeader);

				/* backward compatibility kludge TODO: remove in 3.0 */
				blknum = page.bph.block;
				compressed_size = page.bph.compressed_size;

				/* this has a potential to backfire when retrying merge of old backups,
				 * so we just forbid the retrying of failed merges between versions >= 2.4.0 and
				 * version < 2.4.0
				 */
				if (backup_version >= 20400)
					read_len = compressed_size;
				else
					/* For some unknown and possibly dump reason I/O operations
					 * in versions < 2.4.0 were always aligned to 8 bytes.
					 * Now we have to deal with backward compatibility.
					 */
					read_len = MAXALIGN(compressed_size);

			}
			else
				break;
		}

		/*
		 * Backward compatibility kludge: in the good old days
		 * n_blocks attribute was available only in DELTA backups.
		 * File truncate in PAGE and PTRACK happened on the fly when
		 * special value PageIsTruncated is encountered.
		 * It was inefficient.
		 *
		 * Nowadays every backup type has n_blocks, so instead of
		 * writing and then truncating redundant data, writing
		 * is not happening in the first place.
		 * TODO: remove in 3.0.0
		 */
		if (compressed_size == PageIsTruncated)
		{
			/*
			 * Block header contains information that this block was truncated.
			 * We need to truncate file to this length.
			 */

			elog(VERBOSE, "Truncate file \"%s\" to block %u", to_fullpath, blknum);

			/* To correctly truncate file, we must first flush STDIO buffers */
			if (fio_fflush(out) != 0)
				elog(ERROR, "Cannot flush file \"%s\": %s", to_fullpath, strerror(errno));

			/* Set position to the start of file */
			if (fio_fseek(out, 0) < 0)
				elog(ERROR, "Cannot seek to the start of file \"%s\": %s", to_fullpath, strerror(errno));

			if (fio_ftruncate(out, blknum * BLCKSZ) != 0)
				elog(ERROR, "Cannot truncate file \"%s\": %s", to_fullpath, strerror(errno));

			break;
		}

		Assert(compressed_size > 0);
		Assert(compressed_size <= BLCKSZ);

		/* no point in writing redundant data */
		if (nblocks > 0 && blknum >= nblocks)
			break;

		if (compressed_size > BLCKSZ)
			elog(ERROR, "Size of a blknum %i exceed BLCKSZ: %i", blknum, compressed_size);

		/* Incremental restore in LSN mode */
		if (map && lsn_map && datapagemap_is_set(lsn_map, blknum))
			datapagemap_add(map, blknum);

		if (map && checksum_map && checksum_map[blknum].checksum != 0)
		{
			//elog(INFO, "HDR CRC: %u, MAP CRC: %u", page_crc, checksum_map[blknum].checksum);
			/*
			 * The heart of incremental restore in CHECKSUM mode
			 * If page in backup has the same checksum and lsn as
			 * page in backup, then page can be skipped.
			 */
			if (page_crc == checksum_map[blknum].checksum &&
				page_lsn == checksum_map[blknum].lsn)
			{
				datapagemap_add(map, blknum);
			}
		}

		/* if this page is marked as already restored, then skip it */
		if (map && datapagemap_is_set(map, blknum))
		{
			/* Backward compatibility kludge TODO: remove in 3.0
			 * go to the next page.
			 */
			if (!headers && fseek(in, read_len, SEEK_CUR) != 0)
				elog(ERROR, "Cannot seek block %u of \"%s\": %s",
					blknum, from_fullpath, strerror(errno));
			continue;
		}

		if (headers &&
			cur_pos_in != headers[n_hdr].pos)
		{
			if (fseek(in, headers[n_hdr].pos, SEEK_SET) != 0)
				elog(ERROR, "Cannot seek to offset %u of \"%s\": %s",
					headers[n_hdr].pos, from_fullpath, strerror(errno));

			cur_pos_in = headers[n_hdr].pos;
		}

		/* read a page from file */
		if (headers)
			len = fread(&page, 1, read_len, in);
		else
			len = fread(page.data, 1, read_len, in);

		if (len != read_len)
			elog(ERROR, "Cannot read block %u file \"%s\": %s",
						blknum, from_fullpath, strerror(errno));

		cur_pos_in += read_len;

		/*
		 * if page size is smaller than BLCKSZ, decompress the page.
		 * BUGFIX for versions < 2.0.23: if page size is equal to BLCKSZ.
		 * we have to check, whether it is compressed or not using
		 * page_may_be_compressed() function.
		 */
		if (compressed_size != BLCKSZ
			|| page_may_be_compressed(page.data, file->compress_alg,
									  backup_version))
		{
			is_compressed = true;
		}

		/*
		 * Seek and write the restored page.
		 * When restoring file from FULL backup, pages are written sequentially,
		 * so there is no need to issue fseek for every page.
		 */
		write_pos = blknum * BLCKSZ;

		if (cur_pos_out != write_pos)
		{
			if (fio_fseek(out, write_pos) < 0)
				elog(ERROR, "Cannot seek block %u of \"%s\": %s",
					blknum, to_fullpath, strerror(errno));

			cur_pos_out = write_pos;
		}

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
		cur_pos_out += BLCKSZ; /* update current write position */

		/* Mark page as restored to avoid reading this page when restoring parent backups */
		if (map)
			datapagemap_add(map, blknum);
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
	size_t     read_len = 0;
	char      *buf = pgut_malloc(STDIO_BUFSIZE); /* 64kB buffer */

	/* copy content */
	for (;;)
	{
		read_len = 0;

		/* check for interrupt */
		if (interrupted || thread_interrupted)
			elog(ERROR, "Interrupted during nonedata file restore");

		read_len = fread(buf, 1, STDIO_BUFSIZE, in);

		if (ferror(in))
			elog(ERROR, "Cannot read backup file \"%s\": %s",
				from_fullpath, strerror(errno));

		if (read_len > 0)
		{
			if (fio_fwrite(out, buf, read_len) != read_len)
				elog(ERROR, "Cannot write to \"%s\": %s", to_fullpath,
					 strerror(errno));
		}

		if (feof(in))
			break;
	}

	pg_free(buf);

	elog(VERBOSE, "Copied file \"%s\": %lu bytes", from_fullpath, file->write_size);
}

size_t
restore_non_data_file(parray *parent_chain, pgBackup *dest_backup,
					  pgFile *dest_file, FILE *out, const char *to_fullpath,
					  bool already_exists)
{
	char		from_root[MAXPGPATH];
	char		from_fullpath[MAXPGPATH];
	FILE		*in = NULL;

	pgFile		*tmp_file = NULL;
	pgBackup	*tmp_backup = NULL;

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
		tmp_backup = dest_backup->parent_backup_link;
		while (tmp_backup)
		{
			pgFile	   **res_file = NULL;

			/* lookup file in intermediate backup */
			res_file =  parray_bsearch(tmp_backup->files, dest_file, pgFileCompareRelPathWithExternal);
			tmp_file = (res_file) ? *res_file : NULL;

			/*
			 * It should not be possible not to find destination file in intermediate
			 * backup, without encountering full copy first.
			 */
			if (!tmp_file)
			{
				elog(ERROR, "Failed to locate nonedata file \"%s\" in backup %s",
					dest_file->rel_path, base36enc(tmp_backup->start_time));
				continue;
			}

			/* Full copy is found and it is null sized, nothing to do here */
			if (tmp_file->write_size == 0)
			{
				/* In case of incremental restore truncate file just to be safe */
				if (already_exists && fio_ftruncate(out, 0))
					elog(ERROR, "Cannot truncate file \"%s\": %s",
							to_fullpath, strerror(errno));
				return 0;
			}

			/* Full copy is found */
			if (tmp_file->write_size > 0)
				break;

			tmp_backup = tmp_backup->parent_backup_link;
		}
	}

	/* sanity */
	if (!tmp_backup)
		elog(ERROR, "Failed to locate a backup containing full copy of nonedata file \"%s\"",
			to_fullpath);

	if (!tmp_file)
		elog(ERROR, "Failed to locate a full copy of nonedata file \"%s\"", to_fullpath);

	if (tmp_file->write_size <= 0)
		elog(ERROR, "Full copy of nonedata file has invalid size: %li. "
				"Metadata corruption in backup %s in file: \"%s\"",
				tmp_file->write_size, base36enc(tmp_backup->start_time),
				to_fullpath);

	/* incremental restore */
	if (already_exists)
	{
		/* compare checksums of already existing file and backup file */
		pg_crc32 file_crc = fio_get_crc32(to_fullpath, FIO_DB_HOST, false);

		if (file_crc == tmp_file->crc)
		{
			elog(VERBOSE, "Already existing nonedata file \"%s\" has the same checksum, skip restore",
				to_fullpath);
			return 0;
		}

		/* Checksum mismatch, truncate file and overwrite it */
		if (fio_ftruncate(out, 0))
			elog(ERROR, "Cannot truncate file \"%s\": %s",
					to_fullpath, strerror(errno));
	}

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

	/* disable stdio buffering for nonedata files */
	setvbuf(in, NULL, _IONBF, BUFSIZ);

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
 * TODO: optimize remote copying
 */
void
backup_non_data_file_internal(const char *from_fullpath,
							fio_location from_location,
							const char *to_fullpath, pgFile *file,
							bool missing_ok)
{
	FILE       *in = NULL;
	FILE       *out = NULL;
	ssize_t     read_len = 0;
	char	   *buf = NULL;

	INIT_FILE_CRC32(true, file->crc);

	/* reset size summary */
	file->read_size = 0;
	file->write_size = 0;
	file->uncompressed_size = 0;

	/* open backup file for write  */
	out = fopen(to_fullpath, PG_BINARY_W);
	if (out == NULL)
		elog(ERROR, "Cannot open destination file \"%s\": %s",
			 to_fullpath, strerror(errno));

	/* update file permission */
	if (chmod(to_fullpath, file->mode) == -1)
		elog(ERROR, "Cannot change mode of \"%s\": %s", to_fullpath,
			 strerror(errno));

	/* backup remote file  */
	if (fio_is_remote(FIO_DB_HOST))
	{
		char *errmsg = NULL;
		int rc = fio_send_file(from_fullpath, to_fullpath, out, file, &errmsg);

		/* handle errors */
		if (rc == FILE_MISSING)
		{
			/* maybe deleted, it's not error in case of backup */
			if (missing_ok)
			{
				elog(LOG, "File \"%s\" is not found", from_fullpath);
				file->write_size = FILE_NOT_FOUND;
				goto cleanup;
			}
			else
				elog(ERROR, "File \"%s\" is not found", from_fullpath);
		}
		else if (rc == WRITE_FAILED)
			elog(ERROR, "Cannot write to \"%s\": %s", to_fullpath, strerror(errno));
		else if (rc != SEND_OK)
		{
			if (errmsg)
				elog(ERROR, "%s", errmsg);
			else
				elog(ERROR, "Cannot access remote file \"%s\"", from_fullpath);
		}

		pg_free(errmsg);
	}
	/* backup local file */
	else
	{
		/* open source file for read */
		in = fopen(from_fullpath, PG_BINARY_R);
		if (in == NULL)
		{
			/* maybe deleted, it's not error in case of backup */
			if (errno == ENOENT)
			{
				if (missing_ok)
				{
					elog(LOG, "File \"%s\" is not found", from_fullpath);
					file->write_size = FILE_NOT_FOUND;
					goto cleanup;
				}
				else
					elog(ERROR, "File \"%s\" is not found", from_fullpath);
			}

			elog(ERROR, "Cannot open file \"%s\": %s", from_fullpath,
				 strerror(errno));
		}

		/* disable stdio buffering for local input/output files to avoid triple buffering */
		setvbuf(in, NULL, _IONBF, BUFSIZ);
		setvbuf(out, NULL, _IONBF, BUFSIZ);

		/* allocate 64kB buffer */
		buf = pgut_malloc(CHUNK_SIZE);

		/* copy content and calc CRC */
		for (;;)
		{
			read_len = fread(buf, 1, CHUNK_SIZE, in);

			if (ferror(in))
				elog(ERROR, "Cannot read from file \"%s\": %s",
					 from_fullpath, strerror(errno));

			if (read_len > 0)
			{
				if (fwrite(buf, 1, read_len, out) != read_len)
					elog(ERROR, "Cannot write to file \"%s\": %s", to_fullpath,
						 strerror(errno));

				/* update CRC */
				COMP_FILE_CRC32(true, file->crc, buf, read_len);
				file->read_size += read_len;
			}

			if (feof(in))
				break;
		}
	}

	file->write_size = (int64) file->read_size;

	if (file->write_size > 0)
		file->uncompressed_size = file->write_size;

cleanup:
	/* finish CRC calculation and store into pgFile */
	FIN_FILE_CRC32(true, file->crc);

	if (in && fclose(in))
		elog(ERROR, "Cannot close the file \"%s\": %s", from_fullpath, strerror(errno));

	if (out && fclose(out))
		elog(ERROR, "Cannot close the file \"%s\": %s", to_fullpath, strerror(errno));

	pg_free(buf);
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
 * This function is expected to be executed multiple times,
 * so avoid using elog within it.
 * lsn from page is assigned to page_lsn pointer.
 * TODO: switch to enum for return codes.
 */
int
validate_one_page(Page page, BlockNumber absolute_blkno,
					XLogRecPtr stop_lsn, PageState *page_st,
					uint32 checksum_version)
{
	page_st->lsn = InvalidXLogRecPtr;
	page_st->checksum = 0;

	/* new level of paranoia */
	if (page == NULL)
		return PAGE_IS_NOT_FOUND;

	/* check that page header is ok */
	if (!parse_page(page, &(page_st)->lsn))
	{
		int		i;
		/* Check if the page is zeroed. */
		for (i = 0; i < BLCKSZ && page[i] == 0; i++);

		/* Page is zeroed. No need to verify checksums */
		if (i == BLCKSZ)
			return PAGE_IS_ZEROED;

		/* Page does not looking good */
		return PAGE_HEADER_IS_INVALID;
	}

	/* Verify checksum */
	page_st->checksum = pg_checksum_page(page, absolute_blkno);

	if (checksum_version)
	{
		/* Checksums are enabled, so check them. */
		if (page_st->checksum != ((PageHeader) page)->pd_checksum)
			return PAGE_CHECKSUM_MISMATCH;
	}

	/* At this point page header is sane, if checksums are enabled - the`re ok.
	 * Check that page is not from future.
	 * Note, this check should be used only by validate command.
	 */
	if (stop_lsn > 0)
	{
		/* Get lsn from page header. Ensure that page is from our time. */
		if (page_st->lsn > stop_lsn)
			return PAGE_LSN_FROM_FUTURE;
	}

	return PAGE_IS_VALID;
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
			elog(LOG, "File \"%s\" is not found", from_fullpath);
			return true;
		}

		elog(WARNING, "Cannot open file \"%s\": %s",
					from_fullpath, strerror(errno));
		return false;
	}

	if (file->size % BLCKSZ != 0)
		elog(WARNING, "File: \"%s\", invalid file size %zu", from_fullpath, file->size);

	/*
	 * Compute expected number of blocks in the file.
	 * NOTE This is a normal situation, if the file size has changed
	 * since the moment we computed it.
	 */
	nblocks = file->size/BLCKSZ;

	for (blknum = 0; blknum < nblocks; blknum++)
	{
		PageState page_st;
		page_state = prepare_page(NULL, file, InvalidXLogRecPtr,
									blknum, in, BACKUP_MODE_FULL,
									curr_page, false, checksum_version,
									0, NULL, from_fullpath, &page_st);

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
	}

	fclose(in);
	return is_valid;
}

/* Valiate pages of datafile in backup one by one */
bool
validate_file_pages(pgFile *file, const char *fullpath, XLogRecPtr stop_lsn,
					uint32 checksum_version, uint32 backup_version, HeaderMap *hdr_map)
{
	size_t		read_len = 0;
	bool		is_valid = true;
	FILE		*in;
	pg_crc32	crc;
	bool		use_crc32c = backup_version <= 20021 || backup_version >= 20025;
	BackupPageHeader2 *headers = NULL;
	int         n_hdr = -1;
	off_t       cur_pos_in = 0;

	elog(VERBOSE, "Validate relation blocks for file \"%s\"", fullpath);

	/* should not be possible */
	Assert(!(backup_version >= 20400 && file->n_headers <= 0));

	in = fopen(fullpath, PG_BINARY_R);
	if (in == NULL)
		elog(ERROR, "Cannot open file \"%s\": %s",
			 fullpath, strerror(errno));

	headers = get_data_file_headers(hdr_map, file, backup_version, false);

	if (!headers && file->n_headers > 0)
	{
		elog(WARNING, "Cannot get page headers for file \"%s\"", fullpath);
		return false;
	}

	/* calc CRC of backup file */
	INIT_FILE_CRC32(use_crc32c, crc);

	/* read and validate pages one by one */
	while (true)
	{
		int		rc = 0;
		size_t      len = 0;
		DataPage	compressed_page; /* used as read buffer */
		int	        compressed_size = 0;
		DataPage	page;
		BlockNumber blknum = 0;
		PageState	page_st;

		if (interrupted || thread_interrupted)
			elog(ERROR, "Interrupted during data file validation");

		/* newer backups have page headers in separate storage */
		if (headers)
		{
			n_hdr++;
			if (n_hdr >= file->n_headers)
				break;

			blknum = headers[n_hdr].block;
			/* calculate payload size by comparing current and next page positions,
			 * page header is not included.
			 */
			compressed_size = headers[n_hdr+1].pos - headers[n_hdr].pos - sizeof(BackupPageHeader);

			Assert(compressed_size > 0);
			Assert(compressed_size <= BLCKSZ);

			read_len = sizeof(BackupPageHeader) + compressed_size;

			if (cur_pos_in != headers[n_hdr].pos)
			{
				if (fio_fseek(in, headers[n_hdr].pos) < 0)
					elog(ERROR, "Cannot seek block %u of \"%s\": %s",
						blknum, fullpath, strerror(errno));
				else
					elog(INFO, "Seek to %u", headers[n_hdr].pos);

				cur_pos_in = headers[n_hdr].pos;
			}
		}
		/* old backups rely on header located directly in data file */
		else
		{
			if (get_page_header(in, fullpath, &(compressed_page).bph, &crc, use_crc32c))
			{
				/* Backward compatibility kludge, TODO: remove in 3.0
				 * for some reason we padded compressed pages in old versions
				 */
				blknum = compressed_page.bph.block;
				compressed_size = compressed_page.bph.compressed_size;
				read_len = MAXALIGN(compressed_size);
			}
			else
				break;
		}

		/* backward compatibility kludge TODO: remove in 3.0 */
		if (compressed_size == PageIsTruncated)
		{
			elog(INFO, "Block %u of \"%s\" is truncated",
				 blknum, fullpath);
			continue;
		}

		Assert(compressed_size <= BLCKSZ);
		Assert(compressed_size > 0);

		if (headers)
			len = fread(&compressed_page, 1, read_len, in);
		else
			len = fread(compressed_page.data, 1, read_len, in);

		if (len != read_len)
		{
			elog(WARNING, "Cannot read block %u file \"%s\": %s",
				blknum, fullpath, strerror(errno));
			return false;
		}

		/* update current position */
		cur_pos_in += read_len;

		if (headers)
			COMP_FILE_CRC32(use_crc32c, crc, &compressed_page, read_len);
		else
			COMP_FILE_CRC32(use_crc32c, crc, compressed_page.data, read_len);

		if (compressed_size != BLCKSZ
			|| page_may_be_compressed(compressed_page.data, file->compress_alg,
									  backup_version))
		{
			int32		uncompressed_size = 0;
			const char *errormsg = NULL;

			uncompressed_size = do_decompress(page.data, BLCKSZ,
											  compressed_page.data,
											  compressed_size,
											  file->compress_alg,
											  &errormsg);
			if (uncompressed_size < 0 && errormsg != NULL)
			{
				elog(WARNING, "An error occured during decompressing block %u of file \"%s\": %s",
					 blknum, fullpath, errormsg);
				return false;
			}

			if (uncompressed_size != BLCKSZ)
			{
				if (compressed_size == BLCKSZ)
				{
					is_valid = false;
					continue;
				}
				elog(WARNING, "Page %u of file \"%s\" uncompressed to %d bytes. != BLCKSZ",
						blknum, fullpath, uncompressed_size);
				return false;
			}

			rc = validate_one_page(page.data,
				                       file->segno * RELSEG_SIZE + blknum,
									   stop_lsn, &page_st, checksum_version);
		}
		else
			rc = validate_one_page(compressed_page.data,
									   file->segno * RELSEG_SIZE + blknum,
									   stop_lsn, &page_st, checksum_version);

		switch (rc)
		{
			case PAGE_IS_NOT_FOUND:
				elog(LOG, "File \"%s\", block %u, page is NULL", file->rel_path, blknum);
				break;
			case PAGE_IS_ZEROED:
				elog(LOG, "File: %s blknum %u, empty zeroed page", file->rel_path, blknum);
				break;
			case PAGE_HEADER_IS_INVALID:
				elog(WARNING, "Page header is looking insane: %s, block %i", file->rel_path, blknum);
				is_valid = false;
				break;
			case PAGE_CHECKSUM_MISMATCH:
				elog(WARNING, "File: %s blknum %u have wrong checksum: %u", file->rel_path, blknum, page_st.checksum);
				is_valid = false;
				break;
			case PAGE_LSN_FROM_FUTURE:
				elog(WARNING, "File: %s, block %u, checksum is %s. "
								"Page is from future: pageLSN %X/%X stopLSN %X/%X",
							file->rel_path, blknum,
							checksum_version ? "correct" : "not enabled",
							(uint32) (page_st.lsn >> 32), (uint32) page_st.lsn,
							(uint32) (stop_lsn >> 32), (uint32) stop_lsn);
				break;
		}
	}

	FIN_FILE_CRC32(use_crc32c, crc);
	fclose(in);

	if (crc != file->crc)
	{
		elog(WARNING, "Invalid CRC of backup file \"%s\": %X. Expected %X",
				fullpath, crc, file->crc);
		is_valid = false;
	}

	pg_free(headers);

	return is_valid;
}

/* read local data file and construct map with block checksums */
PageState*
get_checksum_map(const char *fullpath, uint32 checksum_version,
							int n_blocks, XLogRecPtr dest_stop_lsn, BlockNumber segmentno)
{
	PageState  *checksum_map = NULL;
	FILE       *in = NULL;
	BlockNumber blknum = 0;
	char        read_buffer[BLCKSZ];
	char        in_buf[STDIO_BUFSIZE];

	/* open file */
	in = fopen(fullpath, "r+b");
	if (!in)
		elog(ERROR, "Cannot open source file \"%s\": %s", fullpath, strerror(errno));

	/* truncate up to blocks */
	if (ftruncate(fileno(in), n_blocks * BLCKSZ) != 0)
		elog(ERROR, "Cannot truncate file to blknum %u \"%s\": %s",
				n_blocks, fullpath, strerror(errno));

	setvbuf(in, in_buf, _IOFBF, STDIO_BUFSIZE);

	/* initialize array of checksums */
	checksum_map = pgut_malloc(n_blocks * sizeof(PageState));
	memset(checksum_map, 0, n_blocks * sizeof(PageState));

	for (blknum = 0; blknum < n_blocks;  blknum++)
	{
		size_t read_len = fread(read_buffer, 1, BLCKSZ, in);
		PageState page_st;

		/* report error */
		if (ferror(in))
			elog(ERROR, "Cannot read block %u of \"%s\": %s",
					blknum, fullpath, strerror(errno));

		if (read_len == BLCKSZ)
		{
			int rc = validate_one_page(read_buffer, segmentno + blknum,
									   dest_stop_lsn, &page_st,
									   checksum_version);

			if (rc == PAGE_IS_VALID)
			{
//				if (checksum_version)
//					checksum_map[blknum].checksum = ((PageHeader) read_buffer)->pd_checksum;
//				else
//					checksum_map[blknum].checksum = page_st.checksum;
				checksum_map[blknum].checksum = page_st.checksum;
				checksum_map[blknum].lsn = page_st.lsn;
			}
		}
		else
			elog(ERROR, "Failed to read blknum %u from file \"%s\"", blknum, fullpath);

		if (feof(in))
			break;

		if (interrupted)
			elog(ERROR, "Interrupted during page reading");
	}

	if (in)
		fclose(in);

	return checksum_map;
}

/* return bitmap of valid blocks, bitmap is empty, then NULL is returned */
datapagemap_t *
get_lsn_map(const char *fullpath, uint32 checksum_version,
			int n_blocks, XLogRecPtr shift_lsn, BlockNumber segmentno)
{
	FILE           *in = NULL;
	BlockNumber     blknum = 0;
	char            read_buffer[BLCKSZ];
	char            in_buf[STDIO_BUFSIZE];
	datapagemap_t  *lsn_map = NULL;

	Assert(shift_lsn > 0);

	/* open file */
	in = fopen(fullpath, "r+b");
	if (!in)
		elog(ERROR, "Cannot open source file \"%s\": %s", fullpath, strerror(errno));

	/* truncate up to blocks */
	if (ftruncate(fileno(in), n_blocks * BLCKSZ) != 0)
		elog(ERROR, "Cannot truncate file to blknum %u \"%s\": %s",
				n_blocks, fullpath, strerror(errno));

	setvbuf(in, in_buf, _IOFBF, STDIO_BUFSIZE);

	lsn_map = pgut_malloc(sizeof(datapagemap_t));
	memset(lsn_map, 0, sizeof(datapagemap_t));

	for (blknum = 0; blknum < n_blocks;  blknum++)
	{
		size_t read_len = fread(read_buffer, 1, BLCKSZ, in);
		PageState page_st;

		/* report error */
		if (ferror(in))
			elog(ERROR, "Cannot read block %u of \"%s\": %s",
					blknum, fullpath, strerror(errno));

		if (read_len == BLCKSZ)
		{
			int rc = validate_one_page(read_buffer, segmentno + blknum,
									   shift_lsn, &page_st, checksum_version);

			if (rc == PAGE_IS_VALID)
				datapagemap_add(lsn_map, blknum);
		}
		else
			elog(ERROR, "Cannot read block %u from file \"%s\": %s",
					blknum, fullpath, strerror(errno));

		if (feof(in))
			break;

		if (interrupted)
			elog(ERROR, "Interrupted during page reading");
	}

	if (in)
		fclose(in);

	if (lsn_map->bitmapsize == 0)
	{
		pg_free(lsn_map);
		lsn_map = NULL;
	}

	return lsn_map;
}

/* Every page in data file contains BackupPageHeader, extract it */
bool
get_page_header(FILE *in, const char *fullpath, BackupPageHeader* bph,
				pg_crc32 *crc, bool use_crc32c)
{
	/* read BackupPageHeader */
	size_t read_len = fread(bph, 1, sizeof(BackupPageHeader), in);

	if (ferror(in))
		elog(ERROR, "Cannot read file \"%s\": %s",
				fullpath, strerror(errno));

	if (read_len != sizeof(BackupPageHeader))
	{
		if (read_len == 0 && feof(in))
			return false;		/* EOF found */
		else if (read_len != 0 && feof(in))
			elog(ERROR,
				 "Odd size page found at offset %lu of \"%s\"",
				  ftell(in), fullpath);
		else
			elog(ERROR, "Cannot read header at offset %lu of \"%s\": %s",
				 ftell(in), fullpath, strerror(errno));
	}

	/* In older versions < 2.4.0, when crc for file was calculated, header was
	 * not included in crc calculations. Now it is. And now we have
	 * the problem of backward compatibility for backups of old versions
	 */
	if (crc)
		COMP_FILE_CRC32(use_crc32c, *crc, bph, read_len);

	if (bph->block == 0 && bph->compressed_size == 0)
		elog(ERROR, "Empty block in file \"%s\"", fullpath);

	Assert(bph->compressed_size != 0);
	return true;
}

/* Open local backup file for writing, set permissions and buffering */
FILE*
open_local_file_rw(const char *to_fullpath, char **out_buf, uint32 buf_size)
{
	FILE *out = NULL;
	/* open backup file for write  */
	out = fopen(to_fullpath, PG_BINARY_W);
	if (out == NULL)
		elog(ERROR, "Cannot open backup file \"%s\": %s",
			 to_fullpath, strerror(errno));

	/* update file permission */
	if (chmod(to_fullpath, FILE_PERMISSION) == -1)
		elog(ERROR, "Cannot change mode of \"%s\": %s", to_fullpath,
			 strerror(errno));

	/* enable stdio buffering for output file */
	*out_buf = pgut_malloc(buf_size);
	setvbuf(out, *out_buf, _IOFBF, buf_size);

	return out;
}

/* backup local file */
int
send_pages(ConnectionArgs* conn_arg, const char *to_fullpath, const char *from_fullpath,
		   pgFile *file, XLogRecPtr prev_backup_start_lsn, CompressAlg calg, int clevel,
		   uint32 checksum_version, bool use_pagemap, BackupPageHeader2 **headers,
		   BackupMode backup_mode, int ptrack_version_num, const char *ptrack_schema)
{
	FILE *in = NULL;
	FILE *out = NULL;
	int   hdr_num = -1;
	off_t  cur_pos_out = 0;
	char  curr_page[BLCKSZ];
	int   n_blocks_read = 0;
	BlockNumber blknum = 0;
	datapagemap_iterator_t *iter = NULL;
	int   compressed_size = 0;

	/* stdio buffers */
	char *in_buf = NULL;
	char *out_buf = NULL;

	/* open source file for read */
	in = fopen(from_fullpath, PG_BINARY_R);
	if (in == NULL)
	{
		/*
		 * If file is not found, this is not en error.
		 * It could have been deleted by concurrent postgres transaction.
		 */
		if (errno == ENOENT)
			return FILE_MISSING;

		elog(ERROR, "Cannot open file \"%s\": %s", from_fullpath, strerror(errno));
	}

	/*
	 * Enable stdio buffering for local input file,
	 * unless the pagemap is involved, which
	 * imply a lot of random access.
	 */

	if (use_pagemap)
	{
		iter = datapagemap_iterate(&file->pagemap);
		datapagemap_next(iter, &blknum); /* set first block */

		setvbuf(in, NULL, _IONBF, BUFSIZ);
	}
	else
	{
		in_buf = pgut_malloc(STDIO_BUFSIZE);
		setvbuf(in, in_buf, _IOFBF, STDIO_BUFSIZE);
	}

	while (blknum < file->n_blocks)
	{
		PageState page_st;
		int rc = prepare_page(conn_arg, file, prev_backup_start_lsn,
									  blknum, in, backup_mode, curr_page,
									  true, checksum_version,
									  ptrack_version_num, ptrack_schema,
									  from_fullpath, &page_st);
		if (rc == PageIsTruncated)
			break;

		else if (rc == PageIsOk)
		{
			/* lazily open backup file (useful for s3) */
			if (!out)
				out = open_local_file_rw(to_fullpath, &out_buf, STDIO_BUFSIZE);

			hdr_num++;

			if (!*headers)
				*headers = (BackupPageHeader2 *) pgut_malloc(sizeof(BackupPageHeader2));
			else
				*headers = (BackupPageHeader2 *) pgut_realloc(*headers, (hdr_num+1) * sizeof(BackupPageHeader2));

			(*headers)[hdr_num].block = blknum;
			(*headers)[hdr_num].pos = cur_pos_out;
			(*headers)[hdr_num].lsn = page_st.lsn;
			(*headers)[hdr_num].checksum = page_st.checksum;

			compressed_size = compress_and_backup_page(file, blknum, in, out, &(file->crc),
														rc, curr_page, calg, clevel,
														from_fullpath, to_fullpath);
			cur_pos_out += compressed_size + sizeof(BackupPageHeader);
		}

		n_blocks_read++;

		/* next block */
		if (use_pagemap)
		{
			/* exit if pagemap is exhausted */
			if (!datapagemap_next(iter, &blknum))
				break;
		}
		else
			blknum++;
	}

	/*
	 * Add dummy header, so we can later extract the length of last header
	 * as difference between their offsets.
	 */
	if (*headers)
	{
		file->n_headers = hdr_num +1;
		*headers = (BackupPageHeader2 *) pgut_realloc(*headers, (hdr_num+2) * sizeof(BackupPageHeader2));
		(*headers)[hdr_num+1].pos = cur_pos_out;
	}

	/* cleanup */
	if (in && fclose(in))
		elog(ERROR, "Cannot close the source file \"%s\": %s",
			 to_fullpath, strerror(errno));

	/* close local output file */
	if (out && fclose(out))
		elog(ERROR, "Cannot close the backup file \"%s\": %s",
			 to_fullpath, strerror(errno));

	pg_free(iter);
	pg_free(in_buf);
	pg_free(out_buf);

	return n_blocks_read;
}

/*
 * Attempt to open header file, read content and return as
 * array of headers.
 * TODO: some access optimizations would be great here:
 * less fseeks, buffering, descriptor sharing, etc.
 */
BackupPageHeader2*
get_data_file_headers(HeaderMap *hdr_map, pgFile *file, uint32 backup_version, bool strict)
{
	bool     success = false;
	FILE    *in = NULL;
	size_t   read_len = 0;
	pg_crc32 hdr_crc;
	BackupPageHeader2 *headers = NULL;
	/* header decompression */
	int     z_len = 0;
	char   *zheaders = NULL;
	const char *errormsg = NULL;

	if (backup_version < 20400)
		return NULL;

	if (file->n_headers <= 0)
		return NULL;

	/* TODO: consider to make this descriptor thread-specific */
	in = fopen(hdr_map->path, PG_BINARY_R);

	if (!in)
	{
		elog(strict ? ERROR : WARNING, "Cannot open header file \"%s\": %s", hdr_map->path, strerror(errno));
		return NULL;
	}
	/* disable buffering for header file */
	setvbuf(in, NULL, _IONBF, BUFSIZ);

	if (fseek(in, file->hdr_off, SEEK_SET))
	{
		elog(strict ? ERROR : WARNING, "Cannot seek to position %lu in page header map \"%s\": %s",
			file->hdr_off, hdr_map->path, strerror(errno));
		goto cleanup;
	}

	/*
	 * The actual number of headers in header file is n+1, last one is a dummy header,
	 * used for calculation of read_len for actual last header.
	 */
	read_len = (file->n_headers+1) * sizeof(BackupPageHeader2);

	/* allocate memory for compressed headers */
	zheaders = pgut_malloc(file->hdr_size);
	memset(zheaders, 0, file->hdr_size);

	if (fread(zheaders, 1, file->hdr_size, in) != file->hdr_size)
	{
		elog(strict ? ERROR : WARNING, "Cannot read header file at offset: %li len: %i \"%s\": %s",
			file->hdr_off, file->hdr_size, hdr_map->path, strerror(errno));
		goto cleanup;
	}

	/* allocate memory for uncompressed headers */
	headers = pgut_malloc(read_len);
	memset(headers, 0, read_len);

	z_len = do_decompress(headers, read_len, zheaders, file->hdr_size,
						  ZLIB_COMPRESS, &errormsg);
	if (z_len <= 0)
	{
		if (errormsg)
			elog(strict ? ERROR : WARNING, "An error occured during metadata decompression for file \"%s\": %s",
				 file->rel_path, errormsg);
		else
			elog(strict ? ERROR : WARNING, "An error occured during metadata decompression for file \"%s\": %i",
				 file->rel_path, z_len);

		goto cleanup;
	}

	/* validate checksum */
	INIT_FILE_CRC32(true, hdr_crc);
	COMP_FILE_CRC32(true, hdr_crc, headers, read_len);
	FIN_FILE_CRC32(true, hdr_crc);

	if (hdr_crc != file->hdr_crc)
	{
		elog(strict ? ERROR : WARNING, "Header map for file \"%s\" crc mismatch \"%s\" "
				"offset: %lu, len: %lu, current: %u, expected: %u",
			file->rel_path, hdr_map->path, file->hdr_off, read_len, hdr_crc, file->hdr_crc);
		goto cleanup;
	}

	success = true;

cleanup:

	pg_free(zheaders);
	if (in && fclose(in))
		elog(ERROR, "Cannot close file \"%s\"", hdr_map->path);

	if (!success)
	{
		pg_free(headers);
		headers = NULL;
	}

	return headers;
}

/* write headers of all blocks belonging to file to header map and
 * save its offset and size */
void
write_page_headers(BackupPageHeader2 *headers, pgFile *file, HeaderMap *hdr_map, bool is_merge)
{
	size_t  read_len = 0;
	char   *map_path = NULL;
	/* header compression */
	int     z_len = 0;
	char   *zheaders = NULL;
	const char *errormsg = NULL;

	if (file->n_headers <= 0)
		return;

	/* when running merge we must write headers into temp map */
	map_path = (is_merge) ? hdr_map->path_tmp : hdr_map->path;
	read_len = (file->n_headers+1) * sizeof(BackupPageHeader2);

	/* calculate checksums */
	INIT_FILE_CRC32(true, file->hdr_crc);
	COMP_FILE_CRC32(true, file->hdr_crc, headers, read_len);
	FIN_FILE_CRC32(true, file->hdr_crc);

	zheaders = pgut_malloc(read_len*2);
	memset(zheaders, 0, read_len*2);

	/* compress headers */
	z_len = do_compress(zheaders, read_len*2, headers,
					   read_len, ZLIB_COMPRESS, 1, &errormsg);

	/* writing to header map must be serialized */
	pthread_lock(&(hdr_map->mutex)); /* what if we crash while trying to obtain mutex? */

	if (!hdr_map->fp)
	{
		elog(LOG, "Creating page header map \"%s\"", map_path);

		hdr_map->fp = fopen(map_path, PG_BINARY_W);
		if (hdr_map->fp == NULL)
			elog(ERROR, "Cannot open header file \"%s\": %s",
				 map_path, strerror(errno));

		/* enable buffering for header file */
		hdr_map->buf = pgut_malloc(LARGE_CHUNK_SIZE);
		setvbuf(hdr_map->fp, hdr_map->buf, _IOFBF, LARGE_CHUNK_SIZE);

		/* update file permission */
		if (chmod(map_path, FILE_PERMISSION) == -1)
			elog(ERROR, "Cannot change mode of \"%s\": %s", map_path,
				 strerror(errno));

		file->hdr_off = 0;
	}
	else
		file->hdr_off = hdr_map->offset;

	if (z_len <= 0)
	{
		if (errormsg)
			elog(ERROR, "An error occured during compressing metadata for file \"%s\": %s",
				 file->rel_path, errormsg);
		else
			elog(ERROR, "An error occured during compressing metadata for file \"%s\": %i",
				 file->rel_path, z_len);
	}

	elog(VERBOSE, "Writing headers for file \"%s\" offset: %li, len: %i, crc: %u",
			file->rel_path, file->hdr_off, z_len, file->hdr_crc);

	if (fwrite(zheaders, 1, z_len, hdr_map->fp) != z_len)
		elog(ERROR, "Cannot write to file \"%s\": %s", map_path, strerror(errno));

	file->hdr_size = z_len;   /* save the length of compressed headers */
	hdr_map->offset += z_len; /* update current offset in map */

	/* End critical section */
	pthread_mutex_unlock(&(hdr_map->mutex));

	pg_free(zheaders);
}

void
init_header_map(pgBackup *backup)
{
	backup->hdr_map.fp = NULL;
	backup->hdr_map.buf = NULL;
	join_path_components(backup->hdr_map.path, backup->root_dir, HEADER_MAP);
	join_path_components(backup->hdr_map.path_tmp, backup->root_dir, HEADER_MAP_TMP);
	backup->hdr_map.mutex = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
}

void
cleanup_header_map(HeaderMap *hdr_map)
{
	/* cleanup descriptor */
	if (hdr_map->fp && fclose(hdr_map->fp))
		elog(ERROR, "Cannot close file \"%s\"", hdr_map->path);
	hdr_map->fp = NULL;
	hdr_map->offset = 0;
	pg_free(hdr_map->buf);
	hdr_map->buf = NULL;
}
