/*-------------------------------------------------------------------------
 *
 * data.c: utils to parse and backup data pages
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2022, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include "storage/checksum.h"
#include "storage/checksum_impl.h"
#include <common/pg_lzcompress.h>
#include "utils/file.h"

#include <unistd.h>

#ifdef HAVE_LIBZ
#include <zlib.h>
#endif

#include "utils/thread.h"

/* for crc32_compat macros */
#include "compatibility/pg-11.h"

/* Union to ease operations on relation pages */
typedef struct DataPage
{
	BackupPageHeader bph;
	char            data[BLCKSZ];
} DataPage;

typedef struct backup_page_iterator {
	/* arguments */
	const char*        fullpath;
	pioReader_i        in;
	BackupPageHeader2 *headers;
	int                n_headers;
	uint32_t           backup_version;
	CompressAlg        compress_alg;

	/* iterator value */
	int64_t            cur_pos;
	int64_t            read_pos;
	BlockNumber	       blknum;
	XLogRecPtr         page_lsn;
	uint16_t           page_crc;
	uint32_t           n_hdr;
	bool               truncated;
	bool               is_compressed;
	ft_bytes_t         whole_read;
	ft_bytes_t         read_to;
	ft_bytes_t         compressed;
	DataPage           page;
} backup_page_iterator;

static err_i send_pages(const char *to_fullpath, const char *from_fullpath, pgFile *file,
					  XLogRecPtr prev_backup_start_lsn, CompressAlg calg, int clevel,
					  uint32 checksum_version,
					  BackupPageHeader2 **headers, BackupMode backup_mode);

static err_i copy_pages(const char *to_fullpath, const char *from_fullpath, pgFile *file,
					  XLogRecPtr sync_lsn, uint32 checksum_version,
					  BackupMode backup_mode);

static size_t restore_data_file_internal(pioReader_i in, pioDBWriter_i out, pgFile *file, uint32 backup_version,
										 const char *from_fullpath, const char *to_fullpath, int nblocks,
										 datapagemap_t *map, PageState *checksum_map, int checksum_version,
										 datapagemap_t *lsn_map, BackupPageHeader2 *headers);

static err_i send_file(const char *to_fullpath, const char *from_path, bool cut_zero_tail, pgFile *file);

#ifdef HAVE_LIBZ
/* Implementation of zlib compression method */
static int32
zlib_compress(void *dst, size_t dst_size, void const *src, size_t src_size,
			  int level)
{
	uLongf 	compressed_size = dst_size;
	int 	rc = compress2(dst, &compressed_size, src, src_size,
					   level);

	return rc == Z_OK ? compressed_size : rc;
}

/* Implementation of zlib compression method */
static int32
zlib_decompress(void *dst, size_t dst_size, void const *src, size_t src_size)
{
	uLongf dest_len = dst_size;
	int 	rc = uncompress(dst, &dest_len, src, src_size);

	return rc == Z_OK ? dest_len : rc;
}
#endif

/*
 * Compresses source into dest using algorithm. Returns the number of bytes
 * written in the destination buffer, or -1 if compression fails.
 */
int32
do_compress(void *dst, size_t dst_size, void const *src, size_t src_size,
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
			int32 ret;
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
do_decompress(void *dst, size_t dst_size, void const *src, size_t src_size,
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
			int32 ret;
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
page_may_be_compressed(Page page, CompressAlg alg)
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
#ifdef HAVE_LIBZ
		/* For zlib we can check page magic:
		 * https://stackoverflow.com/questions/9050260/what-does-a-zlib-header-look-like
		 */
		if (alg == ZLIB_COMPRESS && *(char *)page != ZLIB_MAGIC)
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
				"page size %zu is not equal to block size %u",
				PageGetPageSize(phdr), BLCKSZ);

	else if (phdr->pd_lower < SizeOfPageHeaderData)
		snprintf(*errormsg, ERRMSG_MAX_LEN, "page header invalid, "
				"pd_lower %i is less than page header size %zu",
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
				"pd_special %i is misaligned, expected %zu",
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

int
compress_page(char *write_buffer, size_t buffer_size, BlockNumber blknum, void *page,
			  CompressAlg calg, int clevel, const char *from_fullpath)
{
	const char *errormsg = NULL;
	int compressed_size;

	/* Compress the page */
	compressed_size = do_compress(write_buffer, buffer_size, page, BLCKSZ, calg, clevel,
								  &errormsg);
	/* Something went wrong and errormsg was assigned, throw a warning */
	if (compressed_size < 0 && errormsg != NULL)
		elog(WARNING, "An error occured during compressing block %u of file \"%s\": %s",
			 blknum, from_fullpath, errormsg);

	/* Compression skip magic part 1: compression didn`t work
	 * compresssed_size == BLCKSZ is a flag which shows non-compressed state
	 */
	if (compressed_size <= 0 || compressed_size >= BLCKSZ)
	{
		/* Do not compress page */
		memcpy(write_buffer, page, BLCKSZ);
		compressed_size = BLCKSZ;
	}

	return compressed_size;
}

static size_t
backup_page(pioWrite_i out, BlockNumber blknum, ft_bytes_t page,
			const char *to_fullpath, err_i *err)
{
	BackupPageHeader bph;
	size_t n;
	fobj_reset_err(err);

	bph.block = blknum;
	bph.compressed_size = page.len;

	*err = $i(pioWrite, out, .buf = ft_bytes(&bph, sizeof(bph)));
	if ($haserr(*err))
		return 0;
	n = sizeof(bph);

	/* write data page */
	*err = $i(pioWrite, out, .buf = page);
	if ($noerr(*err))
		n += page.len;
	return n;
}

/* Write page as-is. TODO: make it fastpath option in compress_and_backup_page() */
static int
write_page(pgFile *file, pioDBWriter_i out, int blknum, Page page)
{
	err_i err = $noerr();
	off_t target = blknum * BLCKSZ;

	err = $i(pioSeek, out, target);
	if ($haserr(err))
		ft_logerr(FT_ERROR, $errmsg(err), "write_page");

	/* write data page */
	err = $i(pioWrite, out, .buf = ft_bytes(page, BLCKSZ));
	if ($haserr(err))
		ft_log(FT_INFO, $errmsg(err), "write_page");

	file->write_size += BLCKSZ;
	file->uncompressed_size += BLCKSZ;

	return BLCKSZ;
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
backup_data_file(pgFile *file, const char *from_fullpath, const char *to_fullpath,
				 XLogRecPtr prev_backup_start_lsn, BackupMode backup_mode,
				 CompressAlg calg, int clevel, uint32 checksum_version,
				 HeaderMap *hdr_map, bool is_merge)
{
	/* page headers */
	BackupPageHeader2 *headers = NULL;
	err_i 		err = $noerr();

	/* sanity */
	if (file->size % BLCKSZ != 0)
		elog(WARNING, "File: \"%s\", invalid file size %zu", from_fullpath, file->size);

	/*
	 * Compute expected number of blocks in the file.
	 * NOTE This is a normal situation, if the file size has changed
	 * since the moment we computed it.
	 */
	file->n_blocks = ft_div_i64u32_to_i32(file->size, BLCKSZ);

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
	file->crc = 0; /* crc of empty file is 0 */

	/*
	 * Read each page, verify checksum and write it to backup.
	 * If page map is empty or file is not present in previous backup
	 * backup all pages of the relation.
	 *
	 * In PTRACK 1.x there was a problem
	 * of data files with missing _ptrack map.
	 * Such files should be fully copied.
	 */

	/* send prev backup START_LSN */
	XLogRecPtr start_lsn = (backup_mode == BACKUP_MODE_DIFF_DELTA || backup_mode == BACKUP_MODE_DIFF_PTRACK) &&
		file->exists_in_prev ? prev_backup_start_lsn : InvalidXLogRecPtr;
	/* TODO: stop handling errors internally */
	err = send_pages(to_fullpath, from_fullpath, file, start_lsn,
					   calg, clevel, checksum_version,
					   &headers, backup_mode);

	if ($haserr(err))
	{
		if (getErrno(err) == ENOENT)
		{
			elog(is_merge ? ERROR : LOG, "File not found: \"%s\"",
				 from_fullpath);
			file->write_size = FILE_NOT_FOUND;
			goto cleanup;
		}
		ft_logerr(FT_FATAL, $errmsg(err), "Copying data file \"%s\"", file->rel_path);
	}

	/* Determine that file didn`t changed in case of incremental backup */
	if (backup_mode != BACKUP_MODE_FULL &&
		file->exists_in_prev &&
		file->write_size == 0 &&
		file->n_blocks > 0)
	{
		file->write_size = BYTES_INVALID;
	}

cleanup:

	/* dump page headers */
	write_page_headers(headers, file, hdr_map, is_merge);

	pg_free(file->pagemap.bitmap);
	pg_free(headers);
}

/*
 * Catchup data file in the from_root directory to the to_root directory with
 * same relative path. If sync_lsn is not NULL, only pages with equal or
 * higher lsn will be copied.
 * Not just copy file, but read it block by block (use bitmap in case of
 * incremental catchup), validate page checksum.
 */
void
catchup_data_file(pgFile *file, const char *from_fullpath, const char *to_fullpath,
				  XLogRecPtr sync_lsn, BackupMode backup_mode,
				  uint32 checksum_version, int64_t prev_size)
{
	err_i       err = $noerr();

	/*
	 * Compute expected number of blocks in the file.
	 * NOTE This is a normal situation, if the file size has changed
	 * since the moment we computed it.
	 */
	file->n_blocks = ft_div_i64u32_to_i32(file->size, BLCKSZ);

	/*
	 * Skip unchanged file only if it exists in destination directory.
	 * This way we can correctly handle null-sized files which are
	 * not tracked by pagemap and thus always marked as unchanged.
	 */
	if (backup_mode == BACKUP_MODE_DIFF_PTRACK &&
		file->pagemap.bitmapsize == PageBitmapIsEmpty &&
		file->exists_in_prev && file->size == prev_size && !file->pagemap_isabsent)
	{
		/*
		 * There are none changed pages.
		 */
		file->write_size = BYTES_INVALID;
		return;
	}

	/* reset size summary */
	file->read_size = 0;
	file->write_size = 0;
	file->uncompressed_size = 0;

	/* send prev backup START_LSN */
	XLogRecPtr start_lsn = ((backup_mode == BACKUP_MODE_DIFF_DELTA || backup_mode == BACKUP_MODE_DIFF_PTRACK) &&
						   file->exists_in_prev) ? sync_lsn : InvalidXLogRecPtr;
	/* TODO: stop handling errors internally */
	err = copy_pages(to_fullpath, from_fullpath, file, start_lsn,
					checksum_version, backup_mode);
	if ($haserr(err))
	{
		if (getErrno(err) == ENOENT)
		{
			elog(LOG, "File not found: \"%s\"", from_fullpath);
			file->write_size = FILE_NOT_FOUND;
			goto cleanup;
		}
		ft_logerr(FT_FATAL, $errmsg(err), "Copying file \"%s\"", file->rel_path);
	}

	/* Determine that file didn`t changed in case of incremental catchup */
	if (backup_mode != BACKUP_MODE_FULL &&
		file->exists_in_prev &&
		file->write_size == 0 &&
		file->n_blocks > 0)
	{
		file->write_size = BYTES_INVALID;
	}

cleanup:
	pg_free(file->pagemap.bitmap);
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
		copy_pgcontrol_file(FIO_DB_HOST, from_fullpath,
							FIO_BACKUP_HOST, to_fullpath, file);
		return;
	}

	/*
	 * If non-data file exists in previous backup
	 * and its mtime is less than parent backup start time ... */
	if ((pg_strcasecmp(file->name, RELMAPPER_FILENAME) != 0) &&
		(prev_file && file->exists_in_prev &&
		 file->size == prev_file->size &&
		 file->mtime <= parent_backup_time))
	{
		/*
		 * file could be deleted under our feets.
		 * But then backup_non_data_file_internal will handle it safely
		 */
		if (file->forkName != cfm)
			file->crc = fio_get_crc32(FIO_DB_HOST, from_fullpath, false, true);
		else
			file->crc = fio_get_crc32_truncated(FIO_DB_HOST, from_fullpath, true);

		/* ...and checksum is the same... */
		if (EQ_CRC32C(file->crc, prev_file->crc))
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
restore_data_file(parray *parent_chain, pgFile *dest_file, pioDBWriter_i out,
				  const char *to_fullpath, bool use_bitmap, PageState *checksum_map,
				  XLogRecPtr shift_lsn, datapagemap_t *lsn_map, bool use_headers)
{
	size_t total_write_len = 0;
	char  *in_buf = pgut_malloc(STDIO_BUFSIZE);
	int    backup_seq = 0;
	err_i  err;
	pioDrive_i backup_drive = pioDriveForLocation(FIO_BACKUP_HOST);

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
		FOBJ_LOOP_ARP();
		char     from_root[MAXPGPATH];
		char     from_fullpath[MAXPGPATH];
		pioReader_i in;

		pgFile **res_file = NULL;
		pgFile  *tmp_file = NULL;

		/* page headers */
		BackupPageHeader2 *headers = NULL;

		pgBackup *backup = (pgBackup *) parray_get(parent_chain, backup_seq);

		if (use_bitmap)
			backup_seq++;
		else
			backup_seq--;

		/* lookup file in intermediate backup */
		res_file = parray_bsearch(backup->files, dest_file, pgFileCompareRelPathWithExternal);
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

		in = $i(pioOpenRead, backup_drive, from_fullpath, .err = &err);
		if ($haserr(err))
			ft_logerr(FT_FATAL, $errmsg(err), "Open backup file");

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

		$i(pioClose, in);

		pg_free(headers);

//		datapagemap_print_debug(&(dest_file)->pagemap);
	}
	pg_free(in_buf);

	if (dest_file->n_blocks > 0) /* old binary's backups didn't have n_blocks field */
	{
		err = $i(pioTruncate, out, .size = (int64_t)dest_file->n_blocks * BLCKSZ);
		if ($haserr(err))
			ft_logerr(FT_FATAL, $errmsg(err), "Could not truncate datafile");
	}

	return total_write_len;
}

static bool
backup_page_next(backup_page_iterator *it)
{
	size_t	read_len;
	int32_t compressed_size;
	err_i   err;


	it->truncated = false;
	/* newer backups have headers in separate storage */
	if (it->headers)
	{
		BackupPageHeader2* hd;
		uint32_t n_hdr = it->n_hdr;
		if (n_hdr >= it->n_headers)
			return false;
		it->n_hdr++;

		hd = &it->headers[n_hdr];
		it->blknum = hd->block;
		it->page_lsn = hd->lsn;
		it->page_crc = hd->checksum;

		ft_assert(hd->pos >= 0);
		ft_assert((hd+1)->pos > hd->pos + sizeof(BackupPageHeader));
		it->read_pos = hd->pos;

		/* calculate payload size by comparing current and next page positions */
		read_len = (hd+1)->pos - hd->pos;
		it->read_to = ft_bytes(&it->page, read_len);
		compressed_size = read_len - sizeof(BackupPageHeader);
		ft_assert(compressed_size <= BLCKSZ);
		it->whole_read = ft_bytes(&it->page, read_len);
	}
	else
	{
		/* We get into this branch either when restoring old backup
		 * or when merging something. Align read_len only when restoring
		 * or merging old backups.
		 */
		read_len = $i(pioRead, it->in, ft_bytes(&it->page.bph, sizeof(it->page.bph)),
					  .err = &err);
		if ($haserr(err))
			ft_logerr(FT_FATAL, $errmsg(err), "Reading block header");
		if (read_len == 0) /* end of file */
			return false;
		if (read_len != sizeof(it->page.bph))
			ft_log(FT_FATAL, "Cannot read header at offset %lld of \"%s\"",
				   (long long)it->cur_pos, it->fullpath);
		if (it->page.bph.block == 0 && it->page.bph.compressed_size == 0)
			ft_log(FT_FATAL, "Empty block in file \"%s\"", it->fullpath);

		it->cur_pos += sizeof(BackupPageHeader);
		it->read_pos = it->cur_pos;
		it->blknum = it->page.bph.block;
		compressed_size = it->page.bph.compressed_size;
		if (compressed_size == PageIsTruncated)
		{
			it->truncated = true;
			compressed_size = 0;
		}
		ft_assert(compressed_size >= 0 && compressed_size <= BLCKSZ);
		it->page_lsn = 0;
		it->page_crc = 0;

		/* this has a potential to backfire when retrying merge of old backups,
		 * so we just forbid the retrying of failed merges between versions >= 2.4.0 and
		 * version < 2.4.0
		 */
		if (it->backup_version >= 20400)
			read_len = compressed_size;
		else
			/* For some unknown and possibly dump reason I/O operations
			 * in versions < 2.4.0 were always aligned to 8 bytes.
			 * Now we have to deal with backward compatibility.
			 */
			read_len = MAXALIGN(compressed_size);
		it->read_to = ft_bytes(&it->page.data, read_len);
		it->whole_read = ft_bytes(&it->page,
								  sizeof(BackupPageHeader) + read_len);
	}

	it->compressed = ft_bytes(&it->page.data, compressed_size);
	return true;
}

static err_i
backup_page_read(backup_page_iterator *it)
{
	err_i	err;
	size_t  read_len;

	if (it->read_pos != it->cur_pos)
	{
		err = $i(pioSeek, it->in, it->read_pos);
		if ($haserr(err))
			ft_logerr(FT_FATAL, $errmsg(err), "Cannot seek block %u",
					  it->blknum);
		it->cur_pos = it->read_pos;
	}

	read_len = $i(pioRead, it->in, it->read_to, &err);
	if ($haserr(err))
		return $err(RT, "Cannot read block {blknum} of file {path}: {cause}",
					blknum(it->blknum), cause(err.self), path(it->fullpath));
	if (read_len != it->read_to.len)
		return $err(RT, "Short read of block {blknum} of file {path}",
					blknum(it->blknum), path(it->fullpath));
	it->cur_pos += read_len;

	it->is_compressed = it->compressed.len != BLCKSZ;
	/*
	 * Compression skip magic part 2:
	 * if page size is smaller than BLCKSZ, decompress the page.
	 * BUGFIX for versions < 2.0.23: if page size is equal to BLCKSZ.
	 * we have to check, whether it is compressed or not using
	 * page_may_be_compressed() function.
	 */
	if (!it->is_compressed && it->backup_version < 20023 &&
		page_may_be_compressed(it->compressed.ptr, it->compress_alg))
	{
		it->is_compressed = true;
	}
	return $noerr();
}

static err_i
backup_page_skip(backup_page_iterator *it)
{
	if (it->headers != NULL)
		return $noerr();

	/* Backward compatibility kludge TODO: remove in 3.0
	 * go to the next page.
	 */
	it->cur_pos += it->read_to.len;
	it->read_pos = it->cur_pos;
	return $i(pioSeek, it->in, it->cur_pos);
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
restore_data_file_internal(pioReader_i in, pioDBWriter_i out, pgFile *file, uint32 backup_version,
						   const char *from_fullpath, const char *to_fullpath, int nblocks,
						   datapagemap_t *map, PageState *checksum_map, int checksum_version,
						   datapagemap_t *lsn_map, BackupPageHeader2 *headers)
{
	size_t write_len = 0;
	off_t cur_pos_out = 0;
	err_i err = $noerr();

	backup_page_iterator iter = {
			.fullpath = from_fullpath,
			.in = in,
			.headers = headers,
			.n_headers = file->n_headers,
			.backup_version = backup_version,
			.compress_alg = file->compress_alg,
	};

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
	err = $i(pioSeek, out, cur_pos_out);
	if ($haserr(err))
		ft_logerr(FT_FATAL, $errmsg(err), "Cannot seek block %u");

	while (backup_page_next(&iter))
	{
		off_t		write_pos;

		/* check for interrupt */
		if (interrupted || thread_interrupted)
			elog(ERROR, "Interrupted during data file restore");

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
		if (iter.truncated)
		{
			/*
			 * Block header contains information that this block was truncated.
			 * We need to truncate file to this length.
			 */

			elog(VERBOSE, "Truncate file \"%s\" to block %u", to_fullpath, iter.blknum);

			err = $i(pioTruncate, out, (uint64_t)iter.blknum * BLCKSZ);
			if ($haserr(err))
				ft_logerr(FT_FATAL, $errmsg(err), "");

			break;
		}

		/* no point in writing redundant data */
		if (nblocks > 0 && iter.blknum >= nblocks)
			break;

		/* Incremental restore in LSN mode */
		if (map && lsn_map && datapagemap_is_set(lsn_map, iter.blknum))
			datapagemap_add(map, iter.blknum);

		if (map && checksum_map && checksum_map[iter.blknum].checksum != 0)
		{
			//elog(INFO, "HDR CRC: %u, MAP CRC: %u", page_crc, checksum_map[blknum].checksum);
			/*
			 * The heart of incremental restore in CHECKSUM mode
			 * If page in backup has the same checksum and lsn as
			 * page in backup, then page can be skipped.
			 */
			if (iter.page_crc == checksum_map[iter.blknum].checksum &&
				iter.page_lsn == checksum_map[iter.blknum].lsn)
			{
				datapagemap_add(map, iter.blknum);
			}
		}

		/* if this page is marked as already restored, then skip it */
		if (map && datapagemap_is_set(map, iter.blknum))
		{
			backup_page_skip(&iter);
			continue;
		}

		err = backup_page_read(&iter);
		if ($haserr(err))
			ft_logerr(FT_FATAL, $errmsg(err), "");

		/*
		 * Seek and write the restored page.
		 * When restoring file from FULL backup, pages are written sequentially,
		 * so there is no need to issue fseek for every page.
		 */
		write_pos = iter.blknum * BLCKSZ;

		if (cur_pos_out != write_pos)
		{
			err = $i(pioSeek, out, write_pos);
			if ($haserr(err))
				ft_logerr(FT_FATAL, $errmsg(err), "Cannot seek block %u",
						  iter.blknum);

			cur_pos_out = write_pos;
		}

		/*
		 * If page is compressed and restore is in remote mode,
		 * send compressed page to the remote side.
		 */
		if (iter.is_compressed)
			err = $i(pioWriteCompressed, out, iter.compressed,
					 .compress_alg = file->compress_alg);
		else
			err = $i(pioWrite, out, iter.compressed);
		if ($haserr(err))
			ft_logerr(FT_FATAL, $errmsg(err), "Cannot write block %u",
					  iter.blknum);

		write_len += BLCKSZ;
		cur_pos_out += BLCKSZ; /* update current write position */

		/* Mark page as restored to avoid reading this page when restoring parent backups */
		if (map)
			datapagemap_add(map, iter.blknum);
	}

	elog(LOG, "Copied file \"%s\": %zu bytes", from_fullpath, write_len);
	return write_len;
}

size_t
restore_non_data_file(parray *parent_chain, pgBackup *dest_backup,
					  pgFile *dest_file, pioDBWriter_i out, const char *to_fullpath,
					  bool already_exists)
{
	char		from_root[MAXPGPATH];
	char		from_fullpath[MAXPGPATH];
	pioReadStream_i in;
	err_i		err;

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
			pgFile	**res_file = NULL;

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
					dest_file->rel_path, backup_id_of(tmp_backup));
				continue;
			}

			/* Full copy is found and it is null sized, nothing to do here */
			if (tmp_file->write_size == 0)
			{
				/* In case of incremental restore truncate file just to be safe */
				if (already_exists)
				{
					err = $i(pioTruncate, out, 0);
					if ($haserr(err))
						ft_logerr(FT_FATAL, $errmsg(err), "");
				}
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
		elog(ERROR, "Failed to locate a backup containing full copy of non-data file \"%s\"",
			to_fullpath);

	if (!tmp_file)
		elog(ERROR, "Failed to locate a full copy of non-data file \"%s\"", to_fullpath);

	if (tmp_file->write_size <= 0)
		elog(ERROR, "Full copy of non-data file has invalid size: %lli. "
				"Metadata corruption in backup %s in file: \"%s\"",
				(long long)tmp_file->write_size, backup_id_of(tmp_backup),
				to_fullpath);

	/* incremental restore */
	if (already_exists)
	{
		/* compare checksums of already existing file and backup file */
		pg_crc32 file_crc;
		if (tmp_file->forkName == cfm &&
			    tmp_file->uncompressed_size > tmp_file->write_size)
			file_crc = fio_get_crc32_truncated(FIO_DB_HOST, to_fullpath, false);
		else
			file_crc = fio_get_crc32(FIO_DB_HOST, to_fullpath, false, false);

		if (file_crc == tmp_file->crc)
		{
			elog(LOG, "Already existing non-data file \"%s\" has the same checksum, skip restore",
				to_fullpath);
			return 0;
		}

		/* Checksum mismatch, truncate file and overwrite it */
		err = $i(pioTruncate, out, 0);
		if ($haserr(err))
			ft_logerr(FT_FATAL, $errmsg(err), "");
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

	in = $i(pioOpenReadStream, dest_backup->backup_location, from_fullpath,
			.err = &err);
	if ($haserr(err))
		ft_logerr(FT_FATAL, $errmsg(err), "Open backup file");

	err = pioCopy($reduce(pioWriteFlush, out), $reduce(pioRead, in));
	if ($haserr(err))
		ft_logerr(FT_FATAL, $errmsg(err), "Copying backup file");

	$i(pioClose, in);
	elog(LOG, "Copied file \"%s\": %llu bytes", from_fullpath,
		 (long long)dest_file->write_size);

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
	bool	cut_zero_tail;
	err_i	err;

	FOBJ_FUNC_ARP();
	cut_zero_tail = file->forkName == cfm;

	/* reset size summary */
	file->crc = 0;
	file->read_size = 0;
	file->write_size = 0;
	file->uncompressed_size = 0;

	/* backup non-data file  */
	err = send_file(to_fullpath, from_fullpath, cut_zero_tail, file);

	/* handle errors */
	if($haserr(err)) {
		if(getErrno(err) == ENOENT) {
			if(missing_ok) {
				elog(LOG, "File \"%s\" is not found", from_fullpath);
				file->write_size = FILE_NOT_FOUND;
				return;
			} else
				elog(ERROR, "File \"%s\" is not found", from_fullpath);
		} else
			elog(ERROR, "An error occured while copying %s: %s",
						from_fullpath, $errmsg(err));
	}

	file->uncompressed_size = file->read_size;
}

static err_i
send_file(const char *to_fullpath, const char *from_fullpath, bool cut_zero_tail, pgFile *file) {
	FOBJ_FUNC_ARP();
	err_i err = $noerr();
	pioReadStream_i in;
	pioWriteCloser_i out;
	pioDrive_i backup_drive = pioDriveForLocation(FIO_BACKUP_HOST);
	pioDrive_i db_drive = pioDriveForLocation(FIO_DB_HOST);

	/* open to_fullpath */
	out = $i(pioOpenRewrite, backup_drive, .path = to_fullpath,
				.permissions = file->mode, .err = &err);

	if($haserr(err))
		elog(ERROR, "Cannot open destination file \"%s\": %s",
					to_fullpath, $errmsg(err));

	/* open from_fullpath */
	in = $i(pioOpenReadStream, db_drive, .path = from_fullpath, .err = &err);

	if($haserr(err))
		goto cleanup;

	/*
	 * Copy content and calc CRC as it gets copied. Optionally pioZeroTail
	 * will be used.
	 */
	pioCRC32Counter *c  = pioCRC32Counter_alloc();
	pioCutZeroTail *zt  = pioCutZeroTail_alloc();
	pioFilter_i ztFlt   = bind_pioFilter(zt);
	pioFilter_i crcFlt  = bind_pioFilter(c);
	pioFilter_i fltrs[] = { ztFlt, crcFlt };

	err = pioCopyWithFilters($reduce(pioWriteFlush, out), $reduce(pioRead, in),
							  cut_zero_tail ? fltrs : &fltrs[1],
							  cut_zero_tail ? 2 : 1,
							  NULL);

	if($haserr(err))
		goto cleanup;

	if (file) {
		file->crc = pioCRC32Counter_getCRC32(c);
		file->read_size = pioCRC32Counter_getSize(c);
		file->write_size = pioCRC32Counter_getSize(c);
	}

cleanup:
	$i(pioClose, in);
	$i(pioClose, out);

	// has $noerr() by default
	return $iresult(err);
}

/*
 * Create empty file, used for partial restore
 */
bool
create_empty_file(const char *to_root, fio_location to_location, pgFile *file)
{
	FOBJ_FUNC_ARP();
	char		to_path[MAXPGPATH];
	pioDrive_i  drive = pioDriveForLocation(to_location);
	pioWriteCloser_i fl;
	err_i		err;

	/* open file for write  */
	join_path_components(to_path, to_root, file->rel_path);
	/*
	 * TODO: possibly it is better to use pioWriteFile, but it doesn't have
	 * permissions parameter, and I don't want to introduce is just for one
	 * use case
	 */
	fl = $i(pioOpenRewrite, drive,
			.path = to_path,
			.permissions = file->mode,
			.use_temp = false,
			.err = &err);
	if ($haserr(err))
		ft_logerr(FT_ERROR, $errmsg(err), "Creating empty file");

	err = $i(pioWriteFinish, fl);
	err = fobj_err_combine(err, $i(pioClose, fl));

	if ($haserr(err))
		ft_logerr(FT_ERROR, $errmsg(err), "Closing empty file");

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
 * Validate pages of datafile in PGDATA one by one.
 *
 * returns true if the file is valid
 * also returns true if the file was not found
 */
bool
check_data_file(pgFile *file, const char *from_fullpath, uint32 checksum_version)
{
	FOBJ_FUNC_ARP();
	pioDBDrive_i		local_location = pioDBDriveForLocation(FIO_LOCAL_HOST);
	pioPagesIterator_i	pages;
	bool				is_valid = true;
	err_i				err;

	pages = doIteratePages(local_location,
						   .from_fullpath = from_fullpath,
						   .file = file,
						   .checksum_version = checksum_version,
						   .backup_mode = BACKUP_MODE_FULL,
						   .just_validate = true,
						   .err = &err);
	if ($haserr(err))
	{
		if (getErrno(err) == ENOENT) {
			elog(LOG, "File \"%s\" is not found", from_fullpath);
			return true;
		}
		ft_logerr(FT_WARNING, $errmsg(err), "Cannot open file \"%s\"", from_fullpath);
		return false;
	}

	while(true)
	{
		PageIteratorValue value;
		err_i err = $i(pioNextPage, pages, &value);
		if ($haserr(err)) {
			ft_logerr(FT_FATAL, $errmsg(err), "Checking data file");
			return false;
		}
		if (value.page_result == PageIsTruncated)
			break;

		if (value.page_result == PageIsCorrupted)
		{
			/* Page is corrupted, no need to elog about it,
			 * prepare_page() already done that
			 *
			 * Still check the rest of the pages too
			 */
			is_valid = false;
			continue;
		}
	}

	return is_valid;
}

/* Valiate pages of datafile in backup one by one */
bool
validate_file_pages(pgFile *file, const char *fullpath, XLogRecPtr stop_lsn,
					uint32 checksum_version, uint32 backup_version, HeaderMap *hdr_map)
{
	FOBJ_FUNC_ARP();
	bool		is_valid = true;
	pg_crc32	crc;
	BackupPageHeader2 *headers = NULL;
	pioDrive_i  drive;
	err_i 		err;

	backup_page_iterator iter = {
			.fullpath = fullpath,
			.n_headers = file->n_headers,
			.backup_version = backup_version,
			.compress_alg = file->compress_alg,
	};

	elog(LOG, "Validate relation blocks for file \"%s\"", fullpath);

	/* should not be possible */
	Assert(!(backup_version >= 20400 && file->n_headers <= 0));

	drive = pioDriveForLocation(FIO_BACKUP_HOST);

	iter.in = $i(pioOpenRead, drive, fullpath, .err = &err);
	if ($haserr(err))
		ft_logerr(FT_FATAL, $errmsg(err), "");

	iter.headers = get_data_file_headers(hdr_map, file, backup_version, false);

	if (!iter.headers && file->n_headers > 0)
	{
		elog(WARNING, "Cannot get page headers for file \"%s\"", fullpath);
		return false;
	}

	/* calc CRC of backup file */
	INIT_CRC32_COMPAT(backup_version, crc);

	/* read and validate pages one by one */
	while (backup_page_next(&iter))
	{
		int		rc = 0;
		DataPage	page;
		ft_bytes_t  uncompressed;
		PageState	page_st;

		if (interrupted || thread_interrupted)
			elog(ERROR, "Interrupted during data file validation");

		/* backward compatibility kludge TODO: remove in 3.0 */
		if (iter.truncated)
		{
			elog(VERBOSE, "Block %u of \"%s\" is truncated",
				 iter.blknum, fullpath);
			continue;
		}

		ft_assert(iter.read_pos == iter.cur_pos);

		err = backup_page_read(&iter);
		if ($haserr(err))
		{
			ft_logerr(FT_WARNING, $errmsg(err), "");
			return false;
		}

		COMP_CRC32_COMPAT(backup_version, crc, iter.whole_read.ptr, iter.whole_read.len);

		if (iter.is_compressed)
		{
			int32       uncompressed_size = 0;
			const char *errormsg = NULL;

			uncompressed_size = do_decompress(page.data, BLCKSZ,
											  iter.compressed.ptr,
											  iter.compressed.len,
											  file->compress_alg,
											  &errormsg);
			if (uncompressed_size < 0 && errormsg != NULL)
			{
				elog(WARNING, "An error occured during decompressing block %u of file \"%s\": %s",
					 iter.blknum, fullpath, errormsg);
				return false;
			}

			if (uncompressed_size != BLCKSZ)
			{
				elog(WARNING, "Page %u of file \"%s\" uncompressed to %d bytes. != BLCKSZ",
					 iter.blknum, fullpath, uncompressed_size);
				if (iter.compressed.len == BLCKSZ)
				{
					is_valid = false;
					continue;
				}
				return false;
			}

			uncompressed = ft_bytes(page.data, BLCKSZ);
		}
		else
			uncompressed = iter.compressed;

		rc = validate_one_page(uncompressed.ptr,
							   file->segno * RELSEG_SIZE + iter.blknum,
							   stop_lsn, &page_st, checksum_version);

		switch (rc)
		{
			case PAGE_IS_NOT_FOUND:
				elog(VERBOSE, "File \"%s\", block %u, page is NULL", file->rel_path, iter.blknum);
				break;
			case PAGE_IS_ZEROED:
				elog(VERBOSE, "File: %s blknum %u, empty zeroed page", file->rel_path, iter.blknum);
				break;
			case PAGE_HEADER_IS_INVALID:
				elog(WARNING, "Page header is looking insane: %s, block %i", file->rel_path, iter.blknum);
				is_valid = false;
				break;
			case PAGE_CHECKSUM_MISMATCH:
				elog(WARNING, "File: %s blknum %u have wrong checksum: %u", file->rel_path, iter.blknum, page_st.checksum);
				is_valid = false;
				break;
			case PAGE_LSN_FROM_FUTURE:
				elog(WARNING, "File: %s, block %u, checksum is %s. "
								"Page is from future: pageLSN %X/%X stopLSN %X/%X",
							file->rel_path, iter.blknum,
							checksum_version ? "correct" : "not enabled",
							(uint32) (page_st.lsn >> 32), (uint32) page_st.lsn,
							(uint32) (stop_lsn >> 32), (uint32) stop_lsn);
				break;
		}
	}

	FIN_CRC32_COMPAT(backup_version, crc);
	$i(pioClose, iter.in);

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

		if (interrupted || thread_interrupted)
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
	FILE          *in = NULL;
	BlockNumber	   blknum = 0;
	char		   read_buffer[BLCKSZ];
	char		   in_buf[STDIO_BUFSIZE];
	datapagemap_t *lsn_map = NULL;

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

		if (interrupted || thread_interrupted)
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

#define FT_SLICE bpph2
#define FT_SLICE_TYPE BackupPageHeader2
#include <ft_array.inc.h>

/* backup local file */
static err_i
send_pages(const char *to_fullpath, const char *from_fullpath, pgFile *file,
		   XLogRecPtr prev_backup_start_lsn, CompressAlg calg, int clevel,
		   uint32 checksum_version, BackupPageHeader2 **headers,
		   BackupMode backup_mode)
{
	FOBJ_FUNC_ARP();
	pioDrive_i backup_location = pioDriveForLocation(FIO_BACKUP_HOST);
	pioDBDrive_i db_location = pioDBDriveForLocation(FIO_DB_HOST);
	pioPagesIterator_i pages;
	pioWriteCloser_i out = $null(pioWriteCloser);
	pioWriteFlush_i wrapped = $null(pioWriteFlush);
	pioCRC32Counter *crc32 = NULL;
	ft_arr_bpph2_t	harray = ft_arr_init();
	err_i err = $noerr();

	pages = doIteratePages(db_location, .from_fullpath = from_fullpath, .file = file,
			   .start_lsn = prev_backup_start_lsn, .calg = calg, .clevel = clevel,
			   .checksum_version = checksum_version, .backup_mode = backup_mode,
			   .err = &err);
	if ($haserr(err))
		return $iresult(err);

	while (true)
	{
		PageIteratorValue value;
		err_i err = $i(pioNextPage, pages, &value);
		if ($haserr(err))
			return $iresult(err);
		if (value.page_result == PageIsTruncated)
			break;

		if (value.page_result == PageIsOk) {
			if($isNULL(out))
			{
				out = $i(pioOpenRewrite, backup_location, to_fullpath,
						 .use_temp = false, .sync = true, .err = &err);
				if ($haserr(err))
					return $iresult(err);
				crc32 = pioCRC32Counter_alloc();
				wrapped = pioWrapWriteFilter($reduce(pioWriteFlush, out),
											 $bind(pioFilter, crc32),
											 BLCKSZ + sizeof(BackupPageHeader));
				file->compress_alg = calg;
			}

			ft_arr_bpph2_push(&harray, (BackupPageHeader2){
					.block = value.blknum,
					.pos = file->write_size,
					.lsn = value.state.lsn,
					.checksum = value.state.checksum,
			});

			file->uncompressed_size += BLCKSZ;
			file->write_size += backup_page($reduce(pioWrite, wrapped), value.blknum,
											ft_bytes(value.compressed_page, value.compressed_size),
											to_fullpath, &err);
			if ($haserr(err))
				return $iresult(err);
		}

		if (value.page_result == PageIsCorrupted)
		{
			err = $err(RT, "Page %d is corrupted",
					   blknum(value.blknum));
			return $iresult(err);
		}
		file->read_size += BLCKSZ;
	}
	file->n_blocks = $i(pioFinalPageN, pages);

	/*
	 * Add dummy header, so we can later extract the length of last header
	 * as difference between their offsets.
	 */
	if (harray.len > 0)
	{
		file->n_headers = harray.len;
		ft_arr_bpph2_push(&harray, (BackupPageHeader2){.pos=file->write_size});
		*headers = harray.ptr;
	}

	/* close local output file */
	if ($notNULL(out))
	{
		err = $i(pioWriteFinish, wrapped);
		if ($haserr(err))
			return $iresult(err);
		file->crc = pioCRC32Counter_getCRC32(crc32);
		ft_dbg_assert(file->write_size == pioCRC32Counter_getSize(crc32));

		err = $i(pioClose, out);
		if ($haserr(err))
			return $iresult(err);
	}

	return $noerr();
}

/*
 * Copy data file just as send_pages but without attaching additional header and compression
 */
static err_i
copy_pages(const char *to_fullpath, const char *from_fullpath, pgFile *file,
			  XLogRecPtr sync_lsn, uint32 checksum_version,
			  BackupMode backup_mode)
{
	FOBJ_FUNC_ARP();
	pioDBDrive_i	backup_location = pioDBDriveForLocation(FIO_BACKUP_HOST);
	err_i		err = $noerr();
	pioPagesIterator_i pages;
	pioDBWriter_i out;

	pages = doIteratePages(backup_location, .from_fullpath = from_fullpath,
			   .file = file, .start_lsn = sync_lsn,
			   .checksum_version = checksum_version,
			   .backup_mode = backup_mode, .err = &err);
	if ($haserr(err))
		return $iresult(err);

	out = $i(pioOpenWrite, backup_location, to_fullpath,
			 .permissions = file->mode, .err = &err);
	if ($haserr(err))
		return $iresult(err);

	while (true)
	{
		PageIteratorValue value;
		err = $i(pioNextPage, pages, &value);
		if ($haserr(err))
			return $iresult(err);

		if (value.page_result == PageIsTruncated)
			break;

		if(value.page_result == PageIsOk) {
			Assert(value.compressed_size == BLCKSZ); /* Assuming NONE_COMPRESS above */
			write_page(file, out, value.blknum, value.compressed_page);
		}

		if (value.page_result == PageIsCorrupted) {
			elog(WARNING, "Page %d of \"%s\" is corrupted",
				 value.blknum, file->rel_path);
		}

		file->read_size += BLCKSZ;
	}

	file->n_blocks = $i(pioFinalPageN, pages);
	file->size = (int64_t)file->n_blocks * BLCKSZ;
	err = $i(pioTruncate, out, file->size);
	if ($haserr(err))
		return $iresult(err);

	err = $i(pioWriteFinish, out);
	if ($haserr(err))
		return $iresult(err);

	err = $i(pioClose, out);
	if ($haserr(err))
		return $iresult(err);

	return $noerr();
}

/*
 * Attempt to open header file, read content and return as
 * array of headers.
 * TODO: some access optimizations would be great here:
 * less fseeks, buffering, descriptor sharing, etc.
 *
 * Used for post 2.4.0 backups
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
	setvbuf(in, NULL, _IONBF, 0);

	if (fseeko(in, file->hdr_off, SEEK_SET))
	{
		elog(strict ? ERROR : WARNING, "Cannot seek to position %llu in page header map \"%s\": %s",
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
		elog(strict ? ERROR : WARNING, "Cannot read header file at offset: %llu len: %i \"%s\": %s",
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
	INIT_CRC32C(hdr_crc);
	COMP_CRC32C(hdr_crc, headers, read_len);
	FIN_CRC32C(hdr_crc);

	if (hdr_crc != file->hdr_crc)
	{
		elog(strict ? ERROR : WARNING, "Header map for file \"%s\" crc mismatch \"%s\" "
				"offset: %llu, len: %zu, current: %u, expected: %u",
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
	FOBJ_FUNC_ARP();
	pioDBDrive_i	drive = pioDBDriveForLocation(FIO_BACKUP_HOST);
	err_i	err = $noerr();
	size_t  read_len = 0;
	/* header compression */
	int     z_len = 0;
	char   *zheaders = NULL;
	const char *errormsg = NULL;

	if (file->n_headers <= 0)
		return;

	/* when running merge we must write headers into temp map */
	read_len = (file->n_headers + 1) * sizeof(BackupPageHeader2);

	/* calculate checksums */
	INIT_CRC32C(file->hdr_crc);
	COMP_CRC32C(file->hdr_crc, headers, read_len);
	FIN_CRC32C(file->hdr_crc);

	zheaders = pgut_malloc(read_len * 2);
	memset(zheaders, 0, read_len * 2);

	/* compress headers */
	z_len = do_compress(zheaders, read_len * 2, headers,
						read_len, ZLIB_COMPRESS, 1, &errormsg);

	/* writing to header map must be serialized */
	pthread_lock(&(hdr_map->mutex)); /* what if we crash while trying to obtain mutex? */

	if ($isNULL(hdr_map->fp))
	{
		elog(LOG, "Creating page header map \"%s\"", hdr_map->path);

		hdr_map->fp = $iref( $i(pioOpenRewrite, drive, .path = hdr_map->path,
								.permissions = FILE_PERMISSION, .binary = true,
								.use_temp = is_merge, .sync = true,
								.err = &err) );
		if ($haserr(err))
		{
			ft_logerr(FT_FATAL, $errmsg(err), "opening header map for write");
		}

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

	elog(VERBOSE, "Writing headers for file \"%s\" offset: %llu, len: %i, crc: %u",
			file->rel_path, file->hdr_off, z_len, file->hdr_crc);

	err = $i(pioWrite, hdr_map->fp, .buf = ft_bytes(zheaders, z_len));
	if ($haserr(err))
		ft_logerr(FT_FATAL, $errmsg(err), "writing header map");

	file->hdr_size = z_len;	  /* save the length of compressed headers */
	hdr_map->offset += z_len; /* update current offset in map */

	/* End critical section */
	pthread_mutex_unlock(&(hdr_map->mutex));

	pg_free(zheaders);
}

void
init_header_map(pgBackup *backup)
{
	$setNULL(&backup->hdr_map.fp);

	join_path_components(backup->hdr_map.path, backup->root_dir, HEADER_MAP);
	backup->hdr_map.mutex = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
}

void
cleanup_header_map(HeaderMap *hdr_map)
{
	FOBJ_FUNC_ARP();
	err_i err = $noerr();

	/* cleanup descriptor */
	if ($notNULL(hdr_map->fp))
	{
		err = $i(pioClose, hdr_map->fp);
		if ($haserr(err))
			ft_logerr(FT_FATAL, $errmsg(err), "closing header map");
		$idel(&hdr_map->fp);
		$setNULL(&hdr_map->fp);
	}

	hdr_map->offset = 0;
}
