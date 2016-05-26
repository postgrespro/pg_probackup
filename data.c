/*-------------------------------------------------------------------------
 *
 * data.c: data parsing pages
 *
 * Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_arman.h"

#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "libpq/pqsignal.h"
#include "storage/block.h"
#include "storage/bufpage.h"

typedef struct BackupPageHeader
{
	BlockNumber	block;			/* block number */
	uint16		hole_offset;	/* number of bytes before "hole" */
	uint16		hole_length;	/* number of bytes in "hole" */
} BackupPageHeader;

static bool
parse_page(const DataPage *page,
		   XLogRecPtr *lsn, uint16 *offset, uint16 *length)
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
		page_data->pd_special == MAXALIGN(page_data->pd_special) &&
		!XLogRecPtrIsInvalid(*lsn))
	{
		*offset = page_data->pd_lower;
		*length = page_data->pd_upper - page_data->pd_lower;
		return true;
	}

	*offset = *length = 0;
	return false;
}

/*
 * Backup data file in the from_root directory to the to_root directory with
 * same relative path.
 * If lsn is not NULL, pages only which are modified after the lsn will be
 * copied.
 */
bool
backup_data_file(const char *from_root, const char *to_root,
				 pgFile *file, const XLogRecPtr *lsn)
{
	char				to_path[MAXPGPATH];
	FILE			   *in;
	FILE			   *out;
	BackupPageHeader	header;
	DataPage			page;		/* used as read buffer */
	BlockNumber			blknum = 0;
	size_t				read_len = 0;
	pg_crc32			crc;
	off_t				offset;

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

		/* maybe vanished, it's not error */
		if (errno == ENOENT)
			return false;

		elog(ERROR, "cannot open backup mode file \"%s\": %s",
			 file->path, strerror(errno));
	}

	/* open backup file for write  */
	if (check)
		snprintf(to_path, lengthof(to_path), "%s/tmp", backup_path);
	else
		join_path_components(to_path, to_root, file->path + strlen(from_root) + 1);
	out = fopen(to_path, "w");
	if (out == NULL)
	{
		int errno_tmp = errno;
		fclose(in);
		elog(ERROR, "cannot open backup file \"%s\": %s",
			 to_path, strerror(errno_tmp));
	}

	/* confirm server version */
	check_server_version();

	/*
	 * Read each page and write the page excluding hole. If it has been
	 * determined that the page can be copied safely, but no page map
	 * has been built, it means that we are in presence of a relation
	 * file that needs to be completely scanned. If a page map is present
	 * only scan the blocks needed. In each case, pages are copied without
	 * their hole to ensure some basic level of compression.
	 */
	if (file->pagemap.bitmapsize == 0)
	{
		for (blknum = 0;
			 (read_len = fread(&page, 1, sizeof(page), in)) == sizeof(page);
			 ++blknum)
		{
			XLogRecPtr	page_lsn;
			int		upper_offset;
			int		upper_length;

			header.block = blknum;

			/*
			 * If an invalid data page was found, fallback to simple copy to ensure
			 * all pages in the file don't have BackupPageHeader.
			 */
			if (!parse_page(&page, &page_lsn,
							&header.hole_offset, &header.hole_length))
			{
				elog(LOG, "%s fall back to simple copy", file->path);
				fclose(in);
				fclose(out);
				file->is_datafile = false;
				return copy_file(from_root, to_root, file);
			}

			file->read_size += read_len;

			/* if the page has not been modified since last backup, skip it */
			if (lsn && !XLogRecPtrIsInvalid(page_lsn) && page_lsn < *lsn)
				continue;

			upper_offset = header.hole_offset + header.hole_length;
			upper_length = BLCKSZ - upper_offset;

			/* write data page excluding hole */
			if (fwrite(&header, 1, sizeof(header), out) != sizeof(header) ||
				fwrite(page.data, 1, header.hole_offset, out) != header.hole_offset ||
				fwrite(page.data + upper_offset, 1, upper_length, out) != upper_length)
			{
				int errno_tmp = errno;
				/* oops */
				fclose(in);
				fclose(out);
				elog(ERROR, "cannot write at block %u of \"%s\": %s",
					 blknum, to_path, strerror(errno_tmp));
			}

			/* update CRC */
			COMP_CRC32C(crc, &header, sizeof(header));
			COMP_CRC32C(crc, page.data, header.hole_offset);
			COMP_CRC32C(crc, page.data + upper_offset, upper_length);

			file->write_size += sizeof(header) + read_len - header.hole_length;
		}
	}
	else
	{
		datapagemap_iterator_t *iter;
		iter = datapagemap_iterate(&file->pagemap);
		while (datapagemap_next(iter, &blknum))
		{
			XLogRecPtr	page_lsn;
			int		upper_offset;
			int		upper_length;
			int 	ret;

			offset = blknum * BLCKSZ;
			if (offset > 0)
			{
				ret = fseek(in, offset, SEEK_SET);
				if (ret != 0)
					elog(ERROR,
						 "Can't seek in file offset: %llu ret:%i\n",
						 (long long unsigned int) offset, ret);
			}
			read_len = fread(&page, 1, sizeof(page), in);

			header.block = blknum;

			/*
			 * If an invalid data page was found, fallback to simple copy to ensure
			 * all pages in the file don't have BackupPageHeader.
			 */
			if (!parse_page(&page, &page_lsn,
							&header.hole_offset, &header.hole_length))
			{
				elog(LOG, "%s fall back to simple copy", file->path);
				fclose(in);
				fclose(out);
				file->is_datafile = false;
				return copy_file(from_root, to_root, file);
			}

			/* if the page has not been modified since last backup, skip it */
			if (lsn && !XLogRecPtrIsInvalid(page_lsn) && page_lsn < *lsn)
				continue;

			file->read_size += read_len;
			
			upper_offset = header.hole_offset + header.hole_length;
			upper_length = BLCKSZ - upper_offset;

			/* write data page excluding hole */
			if (fwrite(&header, 1, sizeof(header), out) != sizeof(header) ||
				fwrite(page.data, 1, header.hole_offset, out) != header.hole_offset ||
				fwrite(page.data + upper_offset, 1, upper_length, out) != upper_length)
			{
				int errno_tmp = errno;
				/* oops */
				fclose(in);
				fclose(out);
				elog(ERROR, "cannot write at block %u of \"%s\": %s",
					 blknum, to_path, strerror(errno_tmp));
			}

			/* update CRC */
			COMP_CRC32C(crc, &header, sizeof(header));
			COMP_CRC32C(crc, page.data, header.hole_offset);
			COMP_CRC32C(crc, page.data + upper_offset, upper_length);

			file->write_size += sizeof(header) + read_len - header.hole_length;
		}
		pg_free(iter);
		/*
		 * If we have pagemap then file can't be a zero size.
		 * Otherwise, we will clear the last file.
		 * Increase read_size to delete after.
		 */
		if (file->read_size == 0)
			file->read_size++;
	}

	/*
	 * update file permission
	 * FIXME: Should set permission on open?
	 */
	if (!check && chmod(to_path, FILE_PERMISSION) == -1)
	{
		int errno_tmp = errno;
		fclose(in);
		fclose(out);
		elog(ERROR, "cannot change mode of \"%s\": %s", file->path,
			 strerror(errno_tmp));
	}

	fclose(in);
	fclose(out);

	/* finish CRC calculation and store into pgFile */
	FIN_CRC32C(crc);
	file->crc = crc;

	/* Treat empty file as not-datafile */
	if (file->read_size == 0)
		file->is_datafile = false;

	/* We do not backup if all pages skipped. */
	if (file->write_size == 0 && file->read_size > 0)
	{
		if (remove(to_path) == -1)
			elog(ERROR, "cannot remove file \"%s\": %s", to_path,
				 strerror(errno));
		return false;
	}

	/* remove $BACKUP_PATH/tmp created during check */
	if (check)
		remove(to_path);

	return true;
}

/*
 * Restore files in the from_root directory to the to_root directory with
 * same relative path.
 */
void
restore_data_file(const char *from_root,
				  const char *to_root,
				  pgFile *file)
{
	char				to_path[MAXPGPATH];
	FILE			   *in;
	FILE			   *out;
	BackupPageHeader	header;
	BlockNumber			blknum;

	/* If the file is not a datafile, just copy it. */
	if (!file->is_datafile)
	{
		copy_file(from_root, to_root, file);
		return;
	}

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
		DataPage	page;		/* used as read buffer */
		int			upper_offset;
		int			upper_length;

		/* read BackupPageHeader */
		read_len = fread(&header, 1, sizeof(header), in);
		if (read_len != sizeof(header))
		{
			int errno_tmp = errno;
			if (read_len == 0 && feof(in))
				break;		/* EOF found */
			else if (read_len != 0 && feof(in))
			{
				elog(ERROR,
					 "odd size page found at block %u of \"%s\"",
					 blknum, file->path);
			}
			else
			{
				elog(ERROR, "cannot read block %u of \"%s\": %s",
					 blknum, file->path, strerror(errno_tmp));
			}
		}

		if (header.block < blknum || header.hole_offset > BLCKSZ ||
			(int) header.hole_offset + (int) header.hole_length > BLCKSZ)
		{
			elog(ERROR, "backup is broken at block %u",
				 blknum);
		}

		upper_offset = header.hole_offset + header.hole_length;
		upper_length = BLCKSZ - upper_offset;

		/* read lower/upper into page.data and restore hole */
		memset(page.data + header.hole_offset, 0, header.hole_length);

		if (fread(page.data, 1, header.hole_offset, in) != header.hole_offset ||
			fread(page.data + upper_offset, 1, upper_length, in) != upper_length)
		{
			elog(ERROR, "cannot read block %u of \"%s\": %s",
				 blknum, file->path, strerror(errno));
		}

		/*
		 * Seek and write the restored page. Backup might have holes in
		 * differential backups.
		 */
		blknum = header.block;
		if (fseek(out, blknum * BLCKSZ, SEEK_SET) < 0)
			elog(ERROR, "cannot seek block %u of \"%s\": %s",
				 blknum, to_path, strerror(errno));
		if (fwrite(page.data, 1, sizeof(page), out) != sizeof(page))
			elog(ERROR, "cannot write block %u of \"%s\": %s",
				 blknum, file->path, strerror(errno));
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

bool
copy_file(const char *from_root, const char *to_root, pgFile *file)
{
	char		to_path[MAXPGPATH];
	FILE	   *in;
	FILE	   *out;
	size_t		read_len = 0;
	int			errno_tmp;
	char		buf[8192];
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
	if (check)
		snprintf(to_path, lengthof(to_path), "%s/tmp", backup_path);
	else
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
		/* update CRC */
		COMP_CRC32C(crc, buf, read_len);

		file->write_size += read_len;
		file->read_size += read_len;
	}

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

	if (check)
		remove(to_path);

	return true;
}

bool
calc_file(pgFile *file)
{
	FILE	   *in;
	size_t		read_len = 0;
	int			errno_tmp;
	char		buf[8192];
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
		if ((read_len = fread(buf, 1, sizeof(buf), in)) != sizeof(buf))
			break;

		/* update CRC */
		COMP_CRC32C(crc, buf, read_len);

		file->write_size += sizeof(buf);
		file->read_size += sizeof(buf);
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
