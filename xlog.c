/*-------------------------------------------------------------------------
 *
 * xlog.c: Parse WAL files.
 *
 * Copyright (c) 2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
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
#define XLOG_PAGE_MAGIC_v85		0xD063	/* 8.5 */

typedef struct XLogLongPageHeaderData_v81
{
	XLogPageHeaderData std;
	uint64		xlp_sysid;
	uint32		xlp_seg_size;
} XLogLongPageHeaderData_v81, *XLogLongPageHeader_v81;

typedef struct XLogLongPageHeaderData_v82
{
	XLogPageHeaderData std;		/* standard header fields */
	uint64		xlp_sysid;		/* system identifier from pg_control */
	uint32		xlp_seg_size;	/* just as a cross-check */
	uint32		xlp_xlog_blcksz;	/* just as a cross-check */
} XLogLongPageHeaderData_v82, *XLogLongPageHeader_v82;

typedef union XLogPage
{
	XLogPageHeaderData			header;
	XLogLongPageHeaderData_v81	long_v81;	/* 8.1 - 8.2 */
	XLogLongPageHeaderData_v82	long_v82;	/* 8.3 - */
	char						data[XLOG_BLCKSZ];
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
	if (server_version < 80100)
		xlog_page_magic = XLOG_PAGE_MAGIC_v80;
	else if (server_version < 80200)
		xlog_page_magic = XLOG_PAGE_MAGIC_v81;
	else if (server_version < 80300)
		xlog_page_magic = XLOG_PAGE_MAGIC_v82;
	else if (server_version < 80400)
		xlog_page_magic = XLOG_PAGE_MAGIC_v83;
	else if (server_version < 80500)
		xlog_page_magic = XLOG_PAGE_MAGIC_v84;
	else
		xlog_page_magic = XLOG_PAGE_MAGIC_v85;

	/* check header */
	if (page.header.xlp_magic != xlog_page_magic)
		return false;
	if ((page.header.xlp_info & ~XLP_ALL_FLAGS) != 0)
		return false;
	if (page.header.xlp_info & XLP_LONG_HEADER)
	{
		if (page.long_v81.xlp_seg_size != XLogSegSize)
			return false;

		/* compressed WAL (with lesslog) has 0 in lheader->xlp_xlog_blcksz. */
		if (server_version >= 80300)
		{
			if (page.long_v82.xlp_xlog_blcksz == XLOG_BLCKSZ)
			{
				/* check size (actual file size, not backup file size) */
				if (file->size != XLogSegSize)
					return false;
			}
			else
			{
				if (page.long_v82.xlp_xlog_blcksz != 0)
					return false;
			}
		}
		else if (file->size != XLogSegSize)
			return false;
	}

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
