/*-------------------------------------------------------------------------
 *
 * xlog.c: Parse WAL files.
 *
 * Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_arman.h"

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog_internal.h"

/*
 * XLogLongPageHeaderData is modified in 8.3, but the layout is compatible
 * except xlp_xlog_blcksz.
 */
typedef union XLogPage
{
	XLogPageHeaderData		header;
	XLogLongPageHeaderData	lheader;
	char					data[XLOG_BLCKSZ];
} XLogPage;

/*
 * Return whether the file is a WAL segment or not.
 * based on ValidXLOGHeader() in src/backend/access/transam/xlog.c.
 */
bool
xlog_is_complete_wal(const pgFile *file)
{
	FILE		   *fp;
	XLogPage		page;

	fp = fopen(file->path, "r");
	if (!fp)
		return false;
	if (fread(&page, 1, sizeof(page), fp) != XLOG_BLCKSZ)
	{
		fclose(fp);
		return false;
	}
	fclose(fp);

	/* check header */
	if (page.header.xlp_magic != XLOG_PAGE_MAGIC)
		return false;
	if ((page.header.xlp_info & ~XLP_ALL_FLAGS) != 0)
		return false;
	if ((page.header.xlp_info & XLP_LONG_HEADER) == 0)
		return false;
	if (page.lheader.xlp_seg_size != XLogSegSize)
		return false;
	if (page.lheader.xlp_xlog_blcksz != XLOG_BLCKSZ)
		return false;

	/*
	 * check size (actual file size, not backup file size)
	 * TODO: Support pre-compressed xlog. They might have different file sizes.
	 */
	if (file->size != XLogSegSize)
		return false;

	return true;
}

/*
 * based on XLogFileName() in xlog_internal.h
 */
void
xlog_fname(char *fname, TimeLineID tli, XLogRecPtr lsn)
{
	snprintf(fname, MAXFNAMELEN, "%08X%08X%08X", tli,
			 (uint32) (lsn >> 32),
			 (uint32) (lsn / XLogSegSize));
}
