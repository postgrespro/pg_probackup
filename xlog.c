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

/*
 * Return whether the file is a WAL segment or not.
 * based on ValidXLOGHeader() in src/backend/access/transam/xlog.c.
 */
bool
xlog_is_complete_wal(const pgFile *file)
{
	FILE *fp;
	char page[XLOG_BLCKSZ];
	XLogPageHeader header = (XLogPageHeader) page;
	XLogLongPageHeader lheader = (XLogLongPageHeader) page;

	fp = fopen(file->path, "r");
	if (!fp)
		return false;
	if (fread(page, 1, sizeof(page), fp) != XLOG_BLCKSZ)
	{
		fclose(fp);
		return false;
	}
	fclose(fp);

	/* check header */
	if (header->xlp_magic != XLOG_PAGE_MAGIC)
		return false;
	if ((header->xlp_info & ~XLP_ALL_FLAGS) != 0)
		return false;
		
	if (header->xlp_info & XLP_LONG_HEADER)
	{
		if (lheader->xlp_seg_size != XLogSegSize)
			return false;

		/* compressed WAL (with lesslog) has 0 in lheader->xlp_xlog_blcksz. */
		if (lheader->xlp_xlog_blcksz != XLOG_BLCKSZ &&
			lheader->xlp_xlog_blcksz != 0)
			return false;
	}

	/* check size (actual file size, not backup file size) */
	if (lheader->xlp_xlog_blcksz == XLOG_BLCKSZ && file->size != XLogSegSize)
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
