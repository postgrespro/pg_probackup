/*-------------------------------------------------------------------------
 *
 * parsexlog.c
 *	  Functions for reading Write-Ahead-Log
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "pg_probackup.h"

#include <unistd.h>

#include "commands/dbcommands_xlog.h"
#include "catalog/storage_xlog.h"
#include "access/transam.h"

/*
 * RmgrNames is an array of resource manager names, to make error messages
 * a bit nicer.
 */
#define PG_RMGR(symname,name,redo,desc,identify,startup,cleanup) \
  name,

static const char *RmgrNames[RM_MAX_ID + 1] = {
#include "access/rmgrlist.h"
};

static void extractPageInfo(XLogReaderState *record);
static bool getRecordTimestamp(XLogReaderState *record, TimestampTz *recordXtime);

static int	xlogreadfd = -1;
static XLogSegNo xlogreadsegno = -1;
static char xlogfpath[MAXPGPATH];
static bool xlogexists = false;

typedef struct XLogPageReadPrivate
{
	const char *archivedir;
	TimeLineID	tli;
} XLogPageReadPrivate;

static int SimpleXLogPageRead(XLogReaderState *xlogreader,
				   XLogRecPtr targetPagePtr,
				   int reqLen, XLogRecPtr targetRecPtr, char *readBuf,
				   TimeLineID *pageTLI);

/*
 * Read WAL from the archive directory, starting from 'startpoint' on the
 * given timeline, until 'endpoint'. Make note of the data blocks touched
 * by the WAL records, and return them in a page map.
 */
void
extractPageMap(const char *archivedir, XLogRecPtr startpoint, TimeLineID tli,
			   XLogRecPtr endpoint)
{
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char	   *errormsg;
	XLogPageReadPrivate private;

	private.archivedir = archivedir;
	private.tli = tli;
	xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &private);
	if (xlogreader == NULL)
		elog(ERROR, "out of memory");

	do
	{
		record = XLogReadRecord(xlogreader, startpoint, &errormsg);
		if (record == NULL)
		{
			XLogRecPtr	errptr;

			errptr = startpoint ? startpoint : xlogreader->EndRecPtr;

			if (errormsg)
				elog(ERROR, "could not read WAL record at %X/%X: %s",
						 (uint32) (errptr >> 32), (uint32) (errptr),
						 errormsg);
			else
				elog(ERROR, "could not read WAL record at %X/%X",
						 (uint32) (errptr >> 32),
						 (uint32) (errptr));
		}

		extractPageInfo(xlogreader);

		startpoint = InvalidXLogRecPtr; /* continue reading at next record */

	} while (xlogreader->ReadRecPtr != endpoint);

	XLogReaderFree(xlogreader);
	if (xlogreadfd != -1)
	{
		close(xlogreadfd);
		xlogreadfd = -1;
		xlogexists = false;
	}
}

void
validate_wal(pgBackup *backup,
			 const char *archivedir,
			 time_t target_time,
			 TransactionId target_xid,
			 TimeLineID tli)
{
	XLogRecPtr	startpoint = backup->start_lsn;
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char	   *errormsg;
	XLogPageReadPrivate private;
	TransactionId last_xid = InvalidTransactionId;
	TimestampTz last_time = 0;
	char		last_timestamp[100],
				target_timestamp[100];
	bool		all_wal = false,
				got_endpoint = false;

	private.archivedir = archivedir;
	private.tli = tli;
	xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &private);
	if (xlogreader == NULL)
		elog(ERROR, "out of memory");

	/* We will check it in the end */
	xlogfpath[0] = '\0';

	while (true)
	{
		bool timestamp_record;

		record = XLogReadRecord(xlogreader, startpoint, &errormsg);
		if (record == NULL)
		{
			if (errormsg)
				elog(WARNING, "%s", errormsg);

			break;
		}

		/* Got WAL record at stop_lsn */
		if (xlogreader->ReadRecPtr == backup->stop_lsn)
			got_endpoint = true;

		timestamp_record = getRecordTimestamp(xlogreader, &last_time);
		if (XLogRecGetXid(xlogreader) != InvalidTransactionId)
			last_xid = XLogRecGetXid(xlogreader);

		/* Check target xid */
		if (TransactionIdIsValid(target_xid) && target_xid == last_xid)
		{
			all_wal = true;
			break;
		}
		/* Check target time */
		else if (target_time != 0 && timestamp_record && timestamptz_to_time_t(last_time) >= target_time)
		{
			all_wal = true;
			break;
		}
		/* If there are no target xid and target time */
		else if (!TransactionIdIsValid(target_xid) && target_time == 0 &&
			xlogreader->ReadRecPtr == backup->stop_lsn)
		{
			all_wal = true;
			/* We don't stop here. We want to get last_xid and last_time */
		}

		startpoint = InvalidXLogRecPtr; /* continue reading at next record */
	}

	if (last_time > 0)
		time2iso(last_timestamp, lengthof(last_timestamp),
				 timestamptz_to_time_t(last_time));
	else
		time2iso(last_timestamp, lengthof(last_timestamp),
				 backup->recovery_time);
	if (last_xid == InvalidTransactionId)
		last_xid = backup->recovery_xid;

	/* There are all need WAL records */
	if (all_wal)
		elog(INFO, "backup validation completed successfully on time %s and xid " XID_FMT,
			 last_timestamp, last_xid);
	/* There are not need WAL records */
	else
	{
		if (xlogfpath[0] != 0)
		{
			/* XLOG reader couldnt read WAL segment */
			if (!xlogexists)
				elog(WARNING, "WAL segment \"%s\" is absent", xlogfpath);
			else if (xlogreadfd != -1)
				elog(WARNING, "error was occured during reading WAL segment \"%s\"",
					 xlogfpath);
		}

		if (!got_endpoint)
			elog(ERROR, "there are not enough WAL records to restore from %X/%X to %X/%X",
				 (uint32) (backup->start_lsn >> 32),
				 (uint32) (backup->start_lsn),
				 (uint32) (backup->stop_lsn >> 32),
				 (uint32) (backup->stop_lsn));
		else
		{
			if (target_time > 0)
				time2iso(target_timestamp, lengthof(target_timestamp),
						 target_time);

			elog(WARNING, "recovery can be done up to time %s and xid " XID_FMT,
				 last_timestamp, last_xid);

			if (TransactionIdIsValid(target_xid) && target_time != 0)
				elog(ERROR, "not enough WAL records to time %s and xid " XID_FMT,
					 target_timestamp, target_xid);
			else if (TransactionIdIsValid(target_xid))
				elog(ERROR, "not enough WAL records to xid " XID_FMT,
					 target_xid);
			else if (target_time != 0)
				elog(ERROR, "not enough WAL records to time %s",
					 target_timestamp);
		}
	}

	/* clean */
	XLogReaderFree(xlogreader);
	if (xlogreadfd != -1)
	{
		close(xlogreadfd);
		xlogreadfd = -1;
		xlogexists = false;
	}
}

/* XLogreader callback function, to read a WAL page */
static int
SimpleXLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr,
				   int reqLen, XLogRecPtr targetRecPtr, char *readBuf,
				   TimeLineID *pageTLI)
{
	XLogPageReadPrivate *private = (XLogPageReadPrivate *) xlogreader->private_data;
	uint32		targetPageOff;
	XLogSegNo	targetSegNo;

	XLByteToSeg(targetPagePtr, targetSegNo);
	targetPageOff = targetPagePtr % XLogSegSize;

	/*
	 * See if we need to switch to a new segment because the requested record
	 * is not in the currently open one.
	 */
	if (xlogreadfd >= 0 && !XLByteInSeg(targetPagePtr, xlogreadsegno))
	{
		close(xlogreadfd);
		xlogreadfd = -1;
		xlogexists = false;
	}

	XLByteToSeg(targetPagePtr, xlogreadsegno);

	if (xlogreadfd < 0)
	{
		char		xlogfname[MAXFNAMELEN];

		XLogFileName(xlogfname, private->tli, xlogreadsegno);
		snprintf(xlogfpath, MAXPGPATH, "%s/%s", private->archivedir,
				 xlogfname);

		if (fileExists(xlogfpath))
		{
			elog(LOG, "opening WAL segment \"%s\"", xlogfpath);

			xlogexists = true;
			xlogreadfd = open(xlogfpath, O_RDONLY | PG_BINARY, 0);

			if (xlogreadfd < 0)
			{
				elog(WARNING, "could not open WAL segment \"%s\": %s",
					 xlogfpath, strerror(errno));
				return -1;
			}
		}
		/* Exit without error if WAL segment doesn't exist */
		else
			return -1;
	}

	/*
	 * At this point, we have the right segment open.
	 */
	Assert(xlogreadfd != -1);

	/* Read the requested page */
	if (lseek(xlogreadfd, (off_t) targetPageOff, SEEK_SET) < 0)
	{
		elog(WARNING, "could not seek in file \"%s\": %s", xlogfpath,
			 strerror(errno));
		return -1;
	}

	if (read(xlogreadfd, readBuf, XLOG_BLCKSZ) != XLOG_BLCKSZ)
	{
		elog(WARNING, "could not read from file \"%s\": %s",
			 xlogfpath, strerror(errno));
		return -1;
	}

	Assert(targetSegNo == xlogreadsegno);

	*pageTLI = private->tli;
	return XLOG_BLCKSZ;
}

/*
 * Extract information on which blocks the current record modifies.
 */
static void
extractPageInfo(XLogReaderState *record)
{
	uint8		block_id;
	RmgrId		rmid = XLogRecGetRmid(record);
	uint8		info = XLogRecGetInfo(record);
	uint8		rminfo = info & ~XLR_INFO_MASK;

	/* Is this a special record type that I recognize? */

	if (rmid == RM_DBASE_ID && rminfo == XLOG_DBASE_CREATE)
	{
		/*
		 * New databases can be safely ignored. They would be completely
		 * copied if found.
		 */
	}
	else if (rmid == RM_DBASE_ID && rminfo == XLOG_DBASE_DROP)
	{
		/*
		 * An existing database was dropped. It is fine to ignore that
		 * they will be removed appropriately.
		 */
	}
	else if (rmid == RM_SMGR_ID && rminfo == XLOG_SMGR_CREATE)
	{
		/*
		 * We can safely ignore these. The file will be removed when
		 * combining the backups in the case of differential on.
		 */
	}
	else if (rmid == RM_SMGR_ID && rminfo == XLOG_SMGR_TRUNCATE)
	{
		/*
		 * We can safely ignore these. When we compare the sizes later on,
		 * we'll notice that they differ, and copy the missing tail from
		 * source system.
		 */
	}
	else if (info & XLR_SPECIAL_REL_UPDATE)
	{
		/*
		 * This record type modifies a relation file in some special way, but
		 * we don't recognize the type. That's bad - we don't know how to
		 * track that change.
		 */
		elog(ERROR, "WAL record modifies a relation, but record type is not recognized\n"
			 "lsn: %X/%X, rmgr: %s, info: %02X",
		  (uint32) (record->ReadRecPtr >> 32), (uint32) (record->ReadRecPtr),
				 RmgrNames[rmid], info);
	}

	for (block_id = 0; block_id <= record->max_block_id; block_id++)
	{
		RelFileNode rnode;
		ForkNumber	forknum;
		BlockNumber blkno;

		if (!XLogRecGetBlockTag(record, block_id, &rnode, &forknum, &blkno))
			continue;

		/* We only care about the main fork; others are copied in toto */
		if (forknum != MAIN_FORKNUM)
			continue;

		process_block_change(forknum, rnode, blkno);
	}
}

/*
 * Extract timestamp from WAL record.
 *
 * If the record contains a timestamp, returns true, and saves the timestamp
 * in *recordXtime. If the record type has no timestamp, returns false.
 * Currently, only transaction commit/abort records and restore points contain
 * timestamps.
 */
static bool
getRecordTimestamp(XLogReaderState *record, TimestampTz *recordXtime)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	uint8		xact_info = info & XLOG_XACT_OPMASK;
	uint8		rmid = XLogRecGetRmid(record);

	if (rmid == RM_XLOG_ID && info == XLOG_RESTORE_POINT)
	{
		*recordXtime = ((xl_restore_point *) XLogRecGetData(record))->rp_time;
		return true;
	}
	if (rmid == RM_XACT_ID && (xact_info == XLOG_XACT_COMMIT ||
							   xact_info == XLOG_XACT_COMMIT_PREPARED))
	{
		*recordXtime = ((xl_xact_commit *) XLogRecGetData(record))->xact_time;
		return true;
	}
	if (rmid == RM_XACT_ID && (xact_info == XLOG_XACT_ABORT ||
							   xact_info == XLOG_XACT_ABORT_PREPARED))
	{
		*recordXtime = ((xl_xact_abort *) XLogRecGetData(record))->xact_time;
		return true;
	}
	return false;
}

