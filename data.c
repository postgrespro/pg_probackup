/*-------------------------------------------------------------------------
 *
 * data.c: compress / uncompress data pages
 *
 * Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_rman.h"

#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "libpq/pqsignal.h"
#include "storage/block.h"
#include "storage/bufpage.h"

#if PG_VERSION_NUM < 80300
#define XLogRecPtrIsInvalid(r)	((r).xrecoff == 0)
#endif

#ifdef HAVE_LIBZ
#include <zlib.h>

#define zlibOutSize 4096
#define zlibInSize  4096

static int doDeflate(z_stream *zp, size_t in_size, size_t out_size, void *inbuf,
	void *outbuf, FILE *in, FILE *out, pg_crc32 *crc, size_t *write_size,
	int flash);
static int doInflate(z_stream *zp, size_t in_size, size_t out_size,void *inbuf,
	void *outbuf, FILE *in, FILE *out, pg_crc32 *crc, size_t *read_size);

static int
doDeflate(z_stream *zp, size_t in_size, size_t out_size, void *inbuf,
	void *outbuf, FILE *in, FILE *out, pg_crc32 *crc, size_t *write_size,
	int flash)
{
	int	status;

	zp->next_in = inbuf;
	zp->avail_in = in_size;

	/* compresses until an input buffer becomes empty. */
	do
	{
		if (interrupted)
		{
			fclose(in);
			fclose(out);
			elog(ERROR_INTERRUPTED, _("interrupted during deflate"));
		}

		status = deflate(zp, flash);

		if (status == Z_STREAM_ERROR)
		{
			fclose(in);
			fclose(out);
			elog(ERROR_SYSTEM, _("can't compress data: %s"), zp->msg);
		}

		if (fwrite(outbuf, 1, out_size - zp->avail_out, out) !=
				out_size - zp->avail_out)
		{
			fclose(in);
			fclose(out);
			elog(ERROR_SYSTEM, _("can't write file: %s"), strerror(errno));
		}

		/* update CRC */
		COMP_CRC32(*crc, outbuf, out_size - zp->avail_out);

		*write_size += out_size - zp->avail_out;

		zp->next_out = outbuf;
		zp->avail_out = out_size;
	} while (zp->avail_in != 0);

	return status;
}

static int
doInflate(z_stream *zp, size_t in_size, size_t out_size,void *inbuf,
	void *outbuf, FILE *in, FILE *out, pg_crc32 *crc, size_t *read_size)
{
	int	status = Z_OK;

	zp->next_out = outbuf;
	zp->avail_out = out_size;

	/* decompresses until an output buffer becomes full. */
	for (;;)
	{
		if (interrupted)
			elog(ERROR_INTERRUPTED, _("interrupted during inflate"));

		/* input buffer becomes empty, read it from a file. */
		if (zp->avail_in == 0)
		{
			size_t	read_len;

			read_len = fread(inbuf, 1, in_size, in);

			if (read_len != in_size)
			{
				int errno_tmp = errno;

				if (!feof(in))
				{
					fclose(in);
					fclose(out);
					elog(ERROR_CORRUPTED,
						_("can't read compress file: %s"), strerror(errno_tmp));
				}

				if (read_len == 0 && *read_size == 0)
					return Z_STREAM_END;
			}

			zp->next_in = inbuf;
			zp->avail_in = read_len;
			*read_size += read_len;
		}

		/* decompresses input file data */
		status = inflate(zp, Z_NO_FLUSH);

		if (status == Z_STREAM_END)
		{
			if (feof(in))
				break;
			/* not reached to EOF, read again */
		}
		else if (status == Z_OK)
		{
			if (zp->avail_out == 0)
				break;
			/* more input needed to fill out_buf */
		}
		else if (status != Z_OK)
		{
			fclose(in);
			fclose(out);
			elog(ERROR_SYSTEM, _("can't uncompress data: %s"), strerror(errno));
		}
	}

	/* update CRC */
	COMP_CRC32(*crc, outbuf, out_size - zp->avail_out);

	return status;
}
#endif

#define PG_PAGE_LAYOUT_VERSION_v80		2	/* 8.0 */
#define PG_PAGE_LAYOUT_VERSION_v81		3	/* 8.1 - 8.2 */
#define PG_PAGE_LAYOUT_VERSION_v83		4	/* 8.3 - */

/* 80000 <= PG_VERSION_NUM < 80300 */
typedef struct PageHeaderData_v80
{
	XLogRecPtr		pd_lsn;
	TimeLineID		pd_tli;
	LocationIndex	pd_lower;
	LocationIndex	pd_upper;
	LocationIndex	pd_special;
	uint16			pd_pagesize_version;
	ItemIdData		pd_linp[1];
} PageHeaderData_v80;

#define PageGetPageSize_v80(page) \
	((Size) ((page)->pd_pagesize_version & (uint16) 0xFF00))
#define PageGetPageLayoutVersion_v80(page) \
	((page)->pd_pagesize_version & 0x00FF)
#define SizeOfPageHeaderData_v80	(offsetof(PageHeaderData_v80, pd_linp))

/* 80300 <= PG_VERSION_NUM */
typedef struct PageHeaderData_v83
{
	XLogRecPtr		pd_lsn;
	uint16			pd_tli;
	uint16			pd_flags;
	LocationIndex	pd_lower;
	LocationIndex	pd_upper;
	LocationIndex	pd_special;
	uint16			pd_pagesize_version;
	TransactionId	pd_prune_xid;
	ItemIdData		pd_linp[1];
} PageHeaderData_v83;

#define PageGetPageSize_v83(page) \
	((Size) ((page)->pd_pagesize_version & (uint16) 0xFF00))
#define PageGetPageLayoutVersion_v83(page) \
	((page)->pd_pagesize_version & 0x00FF)
#define SizeOfPageHeaderData_v83	(offsetof(PageHeaderData_v83, pd_linp))
#define PD_VALID_FLAG_BITS_v83		0x0007

typedef union DataPage
{
	PageHeaderData_v80	v80;	/* 8.0 - 8.2 */
	PageHeaderData_v83	v83;	/* 8.3 - */
	char				data[BLCKSZ];
} DataPage;

typedef struct BackupPageHeader
{
	BlockNumber	block;			/* block number */
	uint16		hole_offset;	/* number of bytes before "hole" */
	uint16		hole_length;	/* number of bytes in "hole" */
} BackupPageHeader;

static bool
parse_page(const DataPage *page, int server_version,
		   XLogRecPtr *lsn, uint16 *offset, uint16 *length)
{
	uint16		page_layout_version;

	/* Determine page layout version */
	if (server_version < 80100)
		page_layout_version = PG_PAGE_LAYOUT_VERSION_v80;
	else if (server_version < 80300)
		page_layout_version = PG_PAGE_LAYOUT_VERSION_v81;
	else
		page_layout_version = PG_PAGE_LAYOUT_VERSION_v83;

	/* Check normal case */
	if (server_version < 80300)
	{
		const PageHeaderData_v80 *v80 = &page->v80;

		if (PageGetPageSize_v80(v80) == BLCKSZ &&
			PageGetPageLayoutVersion_v80(v80) == page_layout_version &&
			v80->pd_lower >= SizeOfPageHeaderData_v80 &&
			v80->pd_lower <= v80->pd_upper &&
			v80->pd_upper <= v80->pd_special &&
			v80->pd_special <= BLCKSZ &&
			v80->pd_special == MAXALIGN(v80->pd_special) &&
			!XLogRecPtrIsInvalid(*lsn = v80->pd_lsn))
		{
			*offset = v80->pd_lower;
			*length = v80->pd_upper - v80->pd_lower;
			return true;
		}
	}
	else
	{
		const PageHeaderData_v83 *v83 = &page->v83;

		if (PageGetPageSize_v83(v83) == BLCKSZ &&
			PageGetPageLayoutVersion_v83(v83) == page_layout_version &&
			(v83->pd_flags & ~PD_VALID_FLAG_BITS_v83) == 0 &&
			v83->pd_lower >= SizeOfPageHeaderData_v83 &&
			v83->pd_lower <= v83->pd_upper &&
			v83->pd_upper <= v83->pd_special &&
			v83->pd_special <= BLCKSZ &&
			v83->pd_special == MAXALIGN(v83->pd_special) &&
			!XLogRecPtrIsInvalid(*lsn = v83->pd_lsn))
		{
			*offset = v83->pd_lower;
			*length = v83->pd_upper - v83->pd_lower;
			return true;
		}
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
				 pgFile *file, const XLogRecPtr *lsn, bool compress)
{
	char				to_path[MAXPGPATH];
	FILE			   *in;
	FILE			   *out;
	BackupPageHeader	header;
	DataPage			page;		/* used as read buffer */
	BlockNumber			blknum;
	size_t				read_len;
	int					errno_tmp;
	pg_crc32			crc;
	int					server_version;
#ifdef HAVE_LIBZ
	z_stream			z;
	char				outbuf[zlibOutSize];
#endif

	INIT_CRC32(crc);

	/* reset size summary */
	file->read_size = 0;
	file->write_size = 0;

	/* open backup mode file for read */
	in = fopen(file->path, "r");
	if (in == NULL)
	{
		FIN_CRC32(crc);
		file->crc = crc;

		/* maybe vanished, it's not error */
		if (errno == ENOENT)
			return false;

		elog(ERROR_SYSTEM, _("can't open backup mode file \"%s\": %s"),
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
		elog(ERROR_SYSTEM, _("can't open backup file \"%s\": %s"),
			to_path, strerror(errno_tmp));
	}

#ifdef HAVE_LIBZ
	if (compress)
	{
		z.zalloc = Z_NULL;
		z.zfree = Z_NULL;
		z.opaque = Z_NULL;

		if (deflateInit(&z, Z_DEFAULT_COMPRESSION) != Z_OK)
		{
			fclose(in);
			fclose(out);
			elog(ERROR_SYSTEM, _("can't initialize compression library: %s"),
				z.msg);
		}

		z.avail_in = 0;
		z.next_out = (void *) outbuf;
		z.avail_out = zlibOutSize;
	}
#endif

	/* confirm server version */
	server_version = get_server_version();

	/* read each page and write the page excluding hole */
	for (blknum = 0;
		 (read_len = fread(&page, 1, sizeof(page), in)) == sizeof(page);
		 ++blknum)
	{
		XLogRecPtr	page_lsn;
		int		upper_offset;
		int		upper_length;

		header.block = blknum;

		/*
		 * If a invalid data page was found, fallback to simple copy to ensure
		 * all pages in the file don't have BackupPageHeader.
		 */
		if (!parse_page(&page, server_version, &page_lsn,
						&header.hole_offset, &header.hole_length))
		{
			elog(LOG, "%s fall back to simple copy", file->path);
			fclose(in);
			fclose(out);
			file->is_datafile = false;
			return copy_file(from_root, to_root, file,
							 compress ? COMPRESSION : NO_COMPRESSION);
		}

		file->read_size += read_len;

		/* if the page has not been modified since last backup, skip it */
		if (lsn && !XLogRecPtrIsInvalid(page_lsn) && XLByteLT(page_lsn, *lsn))
			continue;

		upper_offset = header.hole_offset + header.hole_length;
		upper_length = BLCKSZ - upper_offset;

#ifdef HAVE_LIBZ
		if (compress)
		{
			doDeflate(&z, sizeof(header), sizeof(outbuf), &header, outbuf, in,
					  out, &crc, &file->write_size, Z_NO_FLUSH);
			doDeflate(&z, header.hole_offset, sizeof(outbuf), page.data, outbuf,
					  in, out, &crc, &file->write_size, Z_NO_FLUSH);
			doDeflate(&z, upper_length, sizeof(outbuf),
					  page.data + upper_offset, outbuf, in, out, &crc,
					  &file->write_size, Z_NO_FLUSH);
		}
		else
#endif
		{
			/* write data page excluding hole */
			if (fwrite(&header, 1, sizeof(header), out) != sizeof(header) ||
				fwrite(page.data, 1, header.hole_offset, out) != header.hole_offset ||
				fwrite(page.data + upper_offset, 1, upper_length, out) != upper_length)
			{
				int errno_tmp = errno;
				/* oops */
				fclose(in);
				fclose(out);
				elog(ERROR_SYSTEM, _("can't write at block %u of \"%s\": %s"),
					blknum, to_path, strerror(errno_tmp));
			}

			/* update CRC */
			COMP_CRC32(crc, &header, sizeof(header));
			COMP_CRC32(crc, page.data, header.hole_offset);
			COMP_CRC32(crc, page.data + upper_offset, upper_length);

			file->write_size += sizeof(header) + read_len - header.hole_length;
		}

	}
	errno_tmp = errno;
	if (!feof(in))
	{
		fclose(in);
		fclose(out);
		elog(ERROR_SYSTEM, _("can't read backup mode file \"%s\": %s"),
			file->path, strerror(errno_tmp));
	}

	/*
	 * The odd size page at the tail is probably a page exactly written now, so
	 * write whole of it.
	 */
	if (read_len > 0)
	{
		/*
		 * If the odd size page is the 1st page, fallback to simple copy because
		 * the file is not a datafile.
		 * Otherwise treat the page as a datapage with no hole.
		 */
		if (blknum == 0)
			file->is_datafile = false;
		else
		{
			header.block = blknum;
			header.hole_offset = 0;
			header.hole_length = 0;

#ifdef HAVE_LIBZ
			if (compress)
			{
				doDeflate(&z, sizeof(header), sizeof(outbuf), &header, outbuf,
					in, out, &crc, &file->write_size, Z_NO_FLUSH);
			}
			else
#endif
			{
				if (fwrite(&header, 1, sizeof(header), out) != sizeof(header))
				{
					int errno_tmp = errno;
					/* oops */
					fclose(in);
					fclose(out);
					elog(ERROR_SYSTEM,
						 _("can't write at block %u of \"%s\": %s"),
						 blknum, to_path, strerror(errno_tmp));
				}
				COMP_CRC32(crc, &header, sizeof(header));
				file->write_size += sizeof(header);
			}
		}

		/* write odd size page image */
#ifdef HAVE_LIBZ
		if (compress)
		{
			doDeflate(&z, read_len, sizeof(outbuf), page.data, outbuf, in, out,
				&crc, &file->write_size, Z_NO_FLUSH);
		}
		else
#endif
		{
			if (fwrite(page.data, 1, read_len, out) != read_len)
			{
				int errno_tmp = errno;
				/* oops */
				fclose(in);
				fclose(out);
				elog(ERROR_SYSTEM, _("can't write at block %u of \"%s\": %s"),
					blknum, to_path, strerror(errno_tmp));
			}

			COMP_CRC32(crc, page.data, read_len);
			file->write_size += read_len;
		}

		file->read_size += read_len;
	}

#ifdef HAVE_LIBZ
	if (compress)
	{
		if (file->read_size > 0)
		{
			while (doDeflate(&z, 0, sizeof(outbuf), NULL, outbuf, in, out, &crc,
							 &file->write_size, Z_FINISH) != Z_STREAM_END)
			{
			}
		}

		if (deflateEnd(&z) != Z_OK)
		{
			fclose(in);
			fclose(out);
			elog(ERROR_SYSTEM, _("can't close compression stream: %s"), z.msg);
		}
	}
#endif

	/*
	 * update file permission
	 * FIXME: Should set permission on open?
	 */
	if (!check && chmod(to_path, FILE_PERMISSION) == -1)
	{
		int errno_tmp = errno;
		fclose(in);
		fclose(out);
		elog(ERROR_SYSTEM, _("can't change mode of \"%s\": %s"), file->path,
			strerror(errno_tmp));
	}

	fclose(in);
	fclose(out);

	/* finish CRC calculation and store into pgFile */
	FIN_CRC32(crc);
	file->crc = crc;

	/* Treat empty file as not-datafile */
	if (file->read_size == 0)
		file->is_datafile = false;

	/* We do not backup if all pages skipped. */
	if (file->write_size == 0 && file->read_size > 0)
	{
		if (remove(to_path) == -1)
			elog(ERROR_SYSTEM, _("can't remove file \"%s\": %s"), to_path,
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
				  pgFile *file,
				  bool compress)
{
	char				to_path[MAXPGPATH];
	FILE			   *in;
	FILE			   *out;
	BackupPageHeader	header;
	BlockNumber			blknum;
#ifdef HAVE_LIBZ
	z_stream			z;
	int					status;
	char				inbuf[zlibInSize];
	pg_crc32			crc;
	size_t				read_size;
#endif

	/* If the file is not a datafile, just copy it. */
	if (!file->is_datafile)
	{
		copy_file(from_root, to_root, file,
			compress ? DECOMPRESSION : NO_COMPRESSION);
		return;
	}

	/* open backup mode file for read */
	in = fopen(file->path, "r");
	if (in == NULL)
	{
		elog(ERROR_SYSTEM, _("can't open backup file \"%s\": %s"), file->path,
			strerror(errno));
	}

	/*
	 * Open backup file for write. 	We use "r+" at first to overwrite only
	 * modified pages for incremental restore. If the file is not exists,
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
		elog(ERROR_SYSTEM, _("can't open restore target file \"%s\": %s"),
			to_path, strerror(errno_tmp));
	}

#ifdef HAVE_LIBZ
	if (compress)
	{
		z.zalloc = Z_NULL;
		z.zfree = Z_NULL;
		z.opaque = Z_NULL;
		z.next_in = Z_NULL;
		z.avail_in = 0;

		if (inflateInit(&z) != Z_OK)
			elog(ERROR_SYSTEM, _("can't initialize compression library: %s"),
				z.msg);
		INIT_CRC32(crc);
		read_size = 0;
	}
#endif

	for (blknum = 0; ; blknum++)
	{
		size_t		read_len;
		DataPage	page;		/* used as read buffer */
		int			upper_offset;
		int			upper_length;

		/* read BackupPageHeader */
#ifdef HAVE_LIBZ
		if (compress)
		{
			status = doInflate(&z, sizeof(inbuf), sizeof(header), inbuf,
						&header, in, out, &crc, &read_size);
			if (status == Z_STREAM_END)
			{
				if (z.avail_out != sizeof(header))
					elog(ERROR_CORRUPTED, _("backup is broken header"));
				break;
			}
			if (z.avail_out != 0)
				elog(ERROR_SYSTEM, _("can't read block %u of \"%s\""),
					blknum, file->path);
		}
		else
#endif
		{
			read_len = fread(&header, 1, sizeof(header), in);
			if (read_len != sizeof(header))
			{
				int errno_tmp = errno;
				if (read_len == 0 && feof(in))
					break;		/* EOF found */
				else if (read_len != 0 && feof(in))
				{
					elog(ERROR_CORRUPTED,
						_("odd size page found at block %u of \"%s\""),
						blknum, file->path);
				}
				else
				{
					elog(ERROR_SYSTEM, _("can't read block %u of \"%s\": %s"),
						blknum, file->path, strerror(errno_tmp));
				}
			}
		}

		if (header.block < blknum || header.hole_offset > BLCKSZ ||
			(int) header.hole_offset + (int) header.hole_length > BLCKSZ)
		{
			elog(ERROR_CORRUPTED, _("backup is broken at block %u"),
				blknum);
		}

		upper_offset = header.hole_offset + header.hole_length;
		upper_length = BLCKSZ - upper_offset;

		/* read lower/upper into page.data and restore hole */
		memset(page.data + header.hole_offset, 0, header.hole_length);

#ifdef HAVE_LIBZ
		if (compress)
		{
			elog(LOG, "\n%s() %s %d %d", __FUNCTION__, file->path, header.hole_offset, upper_length);
			if (header.hole_offset > 0)
			{
				doInflate(&z, sizeof(inbuf), header.hole_offset, inbuf,
					page.data, in, out, &crc, &read_size);
				if (z.avail_out != 0)
					elog(ERROR_SYSTEM, _("can't read block %u of \"%s\""),
						blknum, file->path);
			}

			if (upper_length > 0)
			{
				doInflate(&z, sizeof(inbuf), upper_length, inbuf,
					page.data + upper_offset, in, out, &crc, &read_size);
				if (z.avail_out != 0)
					elog(ERROR_SYSTEM, _("can't read block %u of \"%s\""),
						blknum, file->path);
			}
		}
		else
#endif
		{
			if (fread(page.data, 1, header.hole_offset, in) != header.hole_offset ||
				fread(page.data + upper_offset, 1, upper_length, in) != upper_length)
			{
				elog(ERROR_SYSTEM, _("can't read block %u of \"%s\": %s"),
					blknum, file->path, strerror(errno));
			}
		}

		/*
		 * Seek and write the restored page. Backup might have holes in
		 * incremental backups.
		 */
		blknum = header.block;
		if (fseek(out, blknum * BLCKSZ, SEEK_SET) < 0)
			elog(ERROR_SYSTEM, _("can't seek block %u of \"%s\": %s"),
				blknum, to_path, strerror(errno));
		if (fwrite(page.data, 1, sizeof(page), out) != sizeof(page))
			elog(ERROR_SYSTEM, _("can't write block %u of \"%s\": %s"),
				blknum, file->path, strerror(errno));
	}

#ifdef HAVE_LIBZ
	if (compress && inflateEnd(&z) != Z_OK)
		elog(ERROR_SYSTEM, _("can't close compression stream: %s"), z.msg);
#endif

	/* update file permission */
	if (chmod(to_path, file->mode) == -1)
	{
		int errno_tmp = errno;
		fclose(in);
		fclose(out);
		elog(ERROR_SYSTEM, _("can't change mode of \"%s\": %s"), to_path,
			strerror(errno_tmp));
	}
//aaa	if (chown(to_path, file->uid, file->gid) == -1)
//aaa	{
//aaa		int errno_tmp = errno;
//aaa		fclose(in);
//aaa		fclose(out);
//aaa		elog(ERROR_SYSTEM, _("can't change owner of \"%s\": %s"), to_path,
//aaa			strerror(errno_tmp));
//aaa	}

	fclose(in);
	fclose(out);
}

bool
copy_file(const char *from_root, const char *to_root, pgFile *file,
	CompressionMode mode)
{
	char		to_path[MAXPGPATH];
	FILE	   *in;
	FILE	   *out;
	size_t		read_len = 0;
	int			errno_tmp;
	char		buf[8192];
	struct stat	st;
	pg_crc32	crc;
#ifdef HAVE_LIBZ
	z_stream	z;
	int			status;
	char		outbuf[zlibOutSize];
	char		inbuf[zlibInSize];
#endif

	INIT_CRC32(crc);

	/* reset size summary */
	file->read_size = 0;
	file->write_size = 0;

	/* open backup mode file for read */
	in = fopen(file->path, "r");
	if (in == NULL)
	{
		FIN_CRC32(crc);
		file->crc = crc;

		/* maybe deleted, it's not error */
		if (errno == ENOENT)
			return false;

		elog(ERROR_SYSTEM, _("can't open source file \"%s\": %s"), file->path,
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
		elog(ERROR_SYSTEM, _("can't open destination file \"%s\": %s"),
			to_path, strerror(errno_tmp));
	}

	/* stat source file to change mode of destination file */
	if (fstat(fileno(in), &st) == -1)
	{
		fclose(in);
		fclose(out);
		elog(ERROR_SYSTEM, _("can't stat \"%s\": %s"), file->path,
			strerror(errno));
	}

#ifdef HAVE_LIBZ
	z.zalloc = Z_NULL;
	z.zfree = Z_NULL;
	z.opaque = Z_NULL;

	if (mode == COMPRESSION)
	{
		if (deflateInit(&z, Z_DEFAULT_COMPRESSION) != Z_OK)
		{
			fclose(in);
			fclose(out);
			elog(ERROR_SYSTEM, _("can't initialize compression library: %s"),
				z.msg);
		}

		z.avail_in = 0;
		z.next_out = (void *) outbuf;
		z.avail_out = zlibOutSize;
	}
	else if (mode == DECOMPRESSION)
	{
		z.next_in = Z_NULL;
		z.avail_in = 0;
		if (inflateInit(&z) != Z_OK)
		{
			fclose(in);
			fclose(out);
			elog(ERROR_SYSTEM, _("can't initialize compression library: %s"),
				z.msg);
		}
	}
#endif

	/* copy content and calc CRC */
	for (;;)
	{
#ifdef HAVE_LIBZ
		if (mode == COMPRESSION)
		{
			if ((read_len = fread(buf, 1, sizeof(buf), in)) != sizeof(buf))
				break;

			doDeflate(&z, read_len, sizeof(outbuf), buf, outbuf, in, out, &crc,
					  &file->write_size, Z_NO_FLUSH);
			file->read_size += sizeof(buf);
		}
		else if (mode == DECOMPRESSION)
		{
			status = doInflate(&z, sizeof(inbuf), sizeof(outbuf), inbuf, outbuf,
						in, out, &crc, &file->read_size);
			if (fwrite(outbuf, 1, sizeof(outbuf) - z.avail_out, out) !=
					sizeof(outbuf) - z.avail_out)
			{
				errno_tmp = errno;
				/* oops */
				fclose(in);
				fclose(out);
				elog(ERROR_SYSTEM, _("can't write to \"%s\": %s"), to_path,
					strerror(errno_tmp));
			}

			file->write_size += sizeof(outbuf) - z.avail_out;
			if (status == Z_STREAM_END)
				break;
		}
		else
#endif
		{
			if ((read_len = fread(buf, 1, sizeof(buf), in)) != sizeof(buf))
				break;

			if (fwrite(buf, 1, read_len, out) != read_len)
			{
				errno_tmp = errno;
				/* oops */
				fclose(in);
				fclose(out);
				elog(ERROR_SYSTEM, _("can't write to \"%s\": %s"), to_path,
					strerror(errno_tmp));
			}
			/* update CRC */
			COMP_CRC32(crc, buf, read_len);

			file->write_size += sizeof(buf);
			file->read_size += sizeof(buf);
		}
	}
	errno_tmp = errno;
	if (!feof(in))
	{
		fclose(in);
		fclose(out);
		elog(ERROR_SYSTEM, _("can't read backup mode file \"%s\": %s"),
			file->path, strerror(errno_tmp));
	}

	/* copy odd part. */
	if (read_len > 0)
	{
#ifdef HAVE_LIBZ
		if (mode == COMPRESSION)
		{
			doDeflate(&z, read_len, sizeof(outbuf), buf, outbuf, in, out, &crc,
					  &file->write_size, Z_NO_FLUSH);
		}
		else
#endif
		{
			if (fwrite(buf, 1, read_len, out) != read_len)
			{
				errno_tmp = errno;
				/* oops */
				fclose(in);
				fclose(out);
				elog(ERROR_SYSTEM, _("can't write to \"%s\": %s"), to_path,
					strerror(errno_tmp));
			}
			/* update CRC */
			COMP_CRC32(crc, buf, read_len);

			file->write_size += read_len;
		}

		file->read_size += read_len;
	}

#ifdef HAVE_LIBZ
	if (mode == COMPRESSION)
	{
		if (file->read_size > 0)
		{
			while (doDeflate(&z, 0, sizeof(outbuf), NULL, outbuf, in, out, &crc,
							 &file->write_size, Z_FINISH) != Z_STREAM_END)
			{
			}
		}

		if (deflateEnd(&z) != Z_OK)
		{
			fclose(in);
			fclose(out);
			elog(ERROR_SYSTEM, _("can't close compression stream: %s"), z.msg);
		}
	}
	else if (mode == DECOMPRESSION)
	{
		if (inflateEnd(&z) != Z_OK)
		{
			fclose(in);
			fclose(out);
			elog(ERROR_SYSTEM, _("can't close compression stream: %s"), z.msg);
		}
	}

#endif
	/* finish CRC calculation and store into pgFile */
	FIN_CRC32(crc);
	file->crc = crc;

	/* update file permission */
	if (chmod(to_path, st.st_mode) == -1)
	{
		errno_tmp = errno;
		fclose(in);
		fclose(out);
		elog(ERROR_SYSTEM, _("can't change mode of \"%s\": %s"), to_path,
			strerror(errno_tmp));
	}
//aaa	if (chown(to_path, file->uid, file->gid) == -1)
//aaa	{
//aaa		errno_tmp = errno;
//aaa		fclose(in);
//aaa		fclose(out);
//aaa		elog(ERROR_SYSTEM, _("can't change owner of \"%s\": %s"), to_path,
//aaa			strerror(errno_tmp));
//aaa	}

	fclose(in);
	fclose(out);

	if (check)
		remove(to_path);

	return true;
}
