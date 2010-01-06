/*-------------------------------------------------------------------------
 *
 * xlog.c: Parse WAL files.
 *
 * Copyright (c) 2009-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_rman.h"

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#if PG_VERSION_NUM >= 80400
typedef unsigned long Datum;
typedef struct MemoryContextData *MemoryContext;
#endif

#include "access/xlog_internal.h"

#define XLOG_PAGE_MAGIC_v80		0xD05C	/* 8.0 */
#define XLOG_PAGE_MAGIC_v81		0xD05D	/* 8.1 */
#define XLOG_PAGE_MAGIC_v82		0xD05E	/* 8.2 */
#define XLOG_PAGE_MAGIC_v83		0xD062	/* 8.3 */
#define XLOG_PAGE_MAGIC_v84		0xD063	/* 8.4 */
#define XLOG_PAGE_MAGIC_v85		0xD166	/* 8.5 */

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
xlog_is_complete_wal(const pgFile *file, int server_version)
{
	FILE		   *fp;
	XLogPage		page;
	uint16			xlog_page_magic;

	fp = fopen(file->path, "r");
	if (!fp)
		return false;
	if (fread(&page, 1, sizeof(page), fp) != XLOG_BLCKSZ)
	{
		fclose(fp);
		return false;
	}
	fclose(fp);

	/* xlog_page_magic from server version */
	if (server_version < 80000)
		return false;	/* never happen */
	else if (server_version < 80100)
		xlog_page_magic = XLOG_PAGE_MAGIC_v80;
	else if (server_version < 80200)
		xlog_page_magic = XLOG_PAGE_MAGIC_v81;
	else if (server_version < 80300)
		xlog_page_magic = XLOG_PAGE_MAGIC_v82;
	else if (server_version < 80400)
		xlog_page_magic = XLOG_PAGE_MAGIC_v83;
	else if (server_version < 80500)
		xlog_page_magic = XLOG_PAGE_MAGIC_v84;
	else if (server_version < 80600)
		xlog_page_magic = XLOG_PAGE_MAGIC_v85;
	else
		return false;	/* not supported */

	/* check header */
	if (page.header.xlp_magic != xlog_page_magic)
		return false;
	if ((page.header.xlp_info & ~XLP_ALL_FLAGS) != 0)
		return false;
	if ((page.header.xlp_info & XLP_LONG_HEADER) == 0)
		return false;
	if (page.lheader.xlp_seg_size != XLogSegSize)
		return false;
	if (server_version >= 80300 && page.lheader.xlp_xlog_blcksz != XLOG_BLCKSZ)
		return false;

	/*
	 * check size (actual file size, not backup file size)
	 * TODO: Support pre-compressed xlog. They might have different file sizes.
	 */
	if (file->size != XLogSegSize)
		return false;

	return true;
}

bool
xlog_logfname2lsn(const char *logfname, XLogRecPtr *lsn)
{
	uint32 tli;

	if (sscanf(logfname, "%08X%08X%08X",
			&tli, &lsn->xlogid, &lsn->xrecoff) != 3)
		return false;

	lsn->xrecoff *= XLogSegSize;
	return true;
}

/*
 * based on XLogFileName() in xlog_internal.h
 */
void
xlog_fname(char *fname, size_t len, TimeLineID tli, XLogRecPtr *lsn)
{
	snprintf(fname, len, "%08X%08X%08X", tli,
		lsn->xlogid, lsn->xrecoff / XLogSegSize);
}
