/*-------------------------------------------------------------------------
 *
 * parsexlog.c
 *	  Functions for reading Write-Ahead-Log
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2015-2017, Postgres Professional
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

/* some from access/xact.h */
/*
 * XLOG allows to store some information in high 4 bits of log record xl_info
 * field. We use 3 for the opcode, and one about an optional flag variable.
 */
#define XLOG_XACT_COMMIT			0x00
#define XLOG_XACT_PREPARE			0x10
#define XLOG_XACT_ABORT				0x20
#define XLOG_XACT_COMMIT_PREPARED	0x30
#define XLOG_XACT_ABORT_PREPARED	0x40
#define XLOG_XACT_ASSIGNMENT		0x50
/* free opcode 0x60 */
/* free opcode 0x70 */

/* mask for filtering opcodes out of xl_info */
#define XLOG_XACT_OPMASK			0x70

typedef struct xl_xact_commit
{
	TimestampTz xact_time;		/* time of commit */

	/* xl_xact_xinfo follows if XLOG_XACT_HAS_INFO */
	/* xl_xact_dbinfo follows if XINFO_HAS_DBINFO */
	/* xl_xact_subxacts follows if XINFO_HAS_SUBXACT */
	/* xl_xact_relfilenodes follows if XINFO_HAS_RELFILENODES */
	/* xl_xact_invals follows if XINFO_HAS_INVALS */
	/* xl_xact_twophase follows if XINFO_HAS_TWOPHASE */
	/* xl_xact_origin follows if XINFO_HAS_ORIGIN, stored unaligned! */
} xl_xact_commit;

typedef struct xl_xact_abort
{
	TimestampTz xact_time;		/* time of abort */

	/* xl_xact_xinfo follows if XLOG_XACT_HAS_INFO */
	/* No db_info required */
	/* xl_xact_subxacts follows if HAS_SUBXACT */
	/* xl_xact_relfilenodes follows if HAS_RELFILENODES */
	/* No invalidation messages needed. */
	/* xl_xact_twophase follows if XINFO_HAS_TWOPHASE */
} xl_xact_abort;

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
 * Read WAL from the archive directory, from 'startpoint' to 'endpoint' on the
 * given timeline. Collect data blocks touched by the WAL records into a page map.
 *
 * If **prev_segno** is true then read all segments up to **endpoint** segment
 * minus one. Else read all segments up to **endpoint** segment.
 */
void
extractPageMap(const char *archivedir, XLogRecPtr startpoint, TimeLineID tli,
			   XLogRecPtr endpoint, bool prev_segno)
{
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char	   *errormsg;
	XLogPageReadPrivate private;
	XLogSegNo	endSegNo,
				nextSegNo = 0;

	if (!XRecOffIsValid(startpoint))
		elog(ERROR, "Invalid startpoint value %X/%X",
			 (uint32) (startpoint >> 32), (uint32) (startpoint));

	if (!XRecOffIsValid(endpoint))
		elog(ERROR, "Invalid endpoint value %X/%X",
			 (uint32) (endpoint >> 32), (uint32) (endpoint));

	private.archivedir = archivedir;
	private.tli = tli;
	xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &private);
	if (xlogreader == NULL)
		elog(ERROR, "out of memory");

	XLByteToSeg(endpoint, endSegNo);
	if (prev_segno)
		endSegNo--;

	do
	{
		record = XLogReadRecord(xlogreader, startpoint, &errormsg);

		if (record == NULL)
		{
			XLogRecPtr	errptr;

			errptr = startpoint ? startpoint : xlogreader->EndRecPtr;

			if (errormsg)
				elog(WARNING, "could not read WAL record at %X/%X: %s",
					 (uint32) (errptr >> 32), (uint32) (errptr),
					 errormsg);
			else
				elog(WARNING, "could not read WAL record at %X/%X",
					 (uint32) (errptr >> 32), (uint32) (errptr));

			/*
			 * If we don't have all WAL files from prev backup start_lsn to current
			 * start_lsn, we won't be able to build page map and PAGE backup will
			 * be incorrect. Stop it and throw an error.
			 */
			if (!xlogexists)
				elog(ERROR, "WAL segment \"%s\" is absent", xlogfpath);
			else if (xlogreadfd != -1)
				elog(ERROR, "Possible WAL CORRUPTION."
							"Error has occured during reading WAL segment \"%s\"", xlogfpath);
		}

		extractPageInfo(xlogreader);

		startpoint = InvalidXLogRecPtr; /* continue reading at next record */

		XLByteToSeg(xlogreader->EndRecPtr, nextSegNo);
	} while (nextSegNo <= endSegNo && xlogreader->EndRecPtr != endpoint);

	XLogReaderFree(xlogreader);
	if (xlogreadfd != -1)
	{
		close(xlogreadfd);
		xlogreadfd = -1;
		xlogexists = false;
	}
}

/*
 * Ensure that the backup has all wal files needed for recovery to consistent state.
 */
static void
validate_backup_wal_from_start_to_stop(pgBackup *backup,
									   char *backup_xlog_path,
									   TimeLineID tli)
{
	XLogRecPtr	startpoint = backup->start_lsn;
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char	   *errormsg;
	XLogPageReadPrivate private;
	bool		got_endpoint = false;

	private.archivedir = backup_xlog_path;
	private.tli = tli;

	/* We will check it in the end */
	xlogfpath[0] = '\0';

	xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &private);
	if (xlogreader == NULL)
		elog(ERROR, "out of memory");

	while (true)
	{
		record = XLogReadRecord(xlogreader, startpoint, &errormsg);

		if (record == NULL)
		{
			if (errormsg)
				elog(WARNING, "%s", errormsg);

			break;
		}

		/* Got WAL record at stop_lsn */
		if (xlogreader->ReadRecPtr == backup->stop_lsn)
		{
			got_endpoint = true;
			break;
		}
		startpoint = InvalidXLogRecPtr; /* continue reading at next record */
	}

	if (!got_endpoint)
	{
		if (xlogfpath[0] != 0)
		{
			/* XLOG reader couldn't read WAL segment.
			 * We throw a WARNING here to be able to update backup status below.
			 */
			if (!xlogexists)
			{
				elog(WARNING, "WAL segment \"%s\" is absent", xlogfpath);
			}
			else if (xlogreadfd != -1)
			{
				elog(WARNING, "Possible WAL CORRUPTION."
					"Error has occured during reading WAL segment \"%s\"", xlogfpath);
			}
		}

		/*
		 * If we don't have WAL between start_lsn and stop_lsn,
		 * the backup is definitely corrupted. Update its status.
		 */
			backup->status = BACKUP_STATUS_CORRUPT;
			pgBackupWriteBackupControlFile(backup);
			elog(ERROR, "there are not enough WAL records to restore from %X/%X to %X/%X",
				 (uint32) (backup->start_lsn >> 32),
				 (uint32) (backup->start_lsn),
				 (uint32) (backup->stop_lsn >> 32),
				 (uint32) (backup->stop_lsn));
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

/*
 * Ensure that the backup has all wal files needed for recovery to consistent
 * state. And check if we have in archive all files needed to restore the backup
 * up to the given recovery target.
 */
void
validate_wal(pgBackup *backup,
			 const char *archivedir,
			 time_t target_time,
			 TransactionId target_xid,
			 TimeLineID tli)
{
	XLogRecPtr	startpoint = backup->start_lsn;
	char	   *backup_id;
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char	   *errormsg;
	XLogPageReadPrivate private;
	TransactionId last_xid = InvalidTransactionId;
	TimestampTz last_time = 0;
	char		last_timestamp[100],
				target_timestamp[100];
	bool		all_wal = false;
	char		backup_xlog_path[MAXPGPATH];

	/* We need free() this later */
	backup_id = base36enc(backup->start_time);

	if (!XRecOffIsValid(backup->start_lsn))
		elog(ERROR, "Invalid start_lsn value %X/%X of backup %s",
			 (uint32) (backup->start_lsn >> 32), (uint32) (backup->start_lsn),
			 backup_id);

	if (!XRecOffIsValid(backup->stop_lsn))
		elog(ERROR, "Invalid stop_lsn value %X/%X of backup %s",
			 (uint32) (backup->stop_lsn >> 32), (uint32) (backup->stop_lsn),
			 backup_id);

	/*
	 * Check that the backup has all wal files needed
	 * for recovery to consistent state.
	 */
	if (backup->stream)
	{
		sprintf(backup_xlog_path, "/%s/%s/%s/%s",
				backup_instance_path, backup_id, DATABASE_DIR, PG_XLOG_DIR);

		validate_backup_wal_from_start_to_stop(backup, backup_xlog_path, tli);
	}
	else
		validate_backup_wal_from_start_to_stop(backup, (char *) archivedir, tli);

	free(backup_id);

	/*
	 * If recovery target is provided check that we can restore backup to a
	 * recoverty target time or xid.
	 */
	if (!TransactionIdIsValid(target_xid) || target_time == 0)
	{
		/* Recoverty target is not given so exit */
		elog(INFO, "backup validation completed successfully");
		return;
	}

	/*
	 * If recovery target is provided, ensure that archive files exist in
	 * archive directory.
	 */
	if (dir_is_empty(archivedir))
		elog(ERROR, "WAL archive is empty. You cannot restore backup to a recovery target without WAL archive.");

	/*
	 * Check if we have in archive all files needed to restore backup
	 * up to the given recovery target.
	 * In any case we cannot restore to the point before stop_lsn.
	 */
	private.archivedir = archivedir;

	private.tli = tli;
	xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &private);
	if (xlogreader == NULL)
		elog(ERROR, "out of memory");

	/* We will check it in the end */
	xlogfpath[0] = '\0';

	/* We can restore at least up to the backup end */
	time2iso(last_timestamp, lengthof(last_timestamp), backup->recovery_time);
	last_xid = backup->recovery_xid;

	if ((TransactionIdIsValid(target_xid) && target_xid == last_xid)
		|| (target_time != 0 && backup->recovery_time >= target_time))
		all_wal = true;

	startpoint = backup->stop_lsn;
	while (true)
	{
		bool		timestamp_record;

		record = XLogReadRecord(xlogreader, startpoint, &errormsg);
		if (record == NULL)
		{
			if (errormsg)
				elog(WARNING, "%s", errormsg);

			break;
		}

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

	/* There are all needed WAL records */
	if (all_wal)
		elog(INFO, "backup validation completed successfully on time %s and xid " XID_FMT,
			 last_timestamp, last_xid);
	/* Some needed WAL records are absent */
	else
	{
		if (xlogfpath[0] != 0)
		{
			/* XLOG reader couldn't read WAL segment.
			 * We throw a WARNING here to be able to update backup status below.
			 */
			if (!xlogexists)
			{
				elog(WARNING, "WAL segment \"%s\" is absent", xlogfpath);
			}
			else if (xlogreadfd != -1)
			{
				elog(WARNING, "Possible WAL CORRUPTION."
					"Error has occured during reading WAL segment \"%s\"", xlogfpath);
			}
		}

		elog(WARNING, "recovery can be done up to time %s and xid " XID_FMT,
				last_timestamp, last_xid);

		if (target_time > 0)
			time2iso(target_timestamp, lengthof(target_timestamp),
						target_time);
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

	/* clean */
	XLogReaderFree(xlogreader);
	if (xlogreadfd != -1)
	{
		close(xlogreadfd);
		xlogreadfd = -1;
		xlogexists = false;
	}
}

/*
 * Read from archived WAL segments latest recovery time and xid. All necessary
 * segments present at archive folder. We waited **stop_lsn** in
 * pg_stop_backup().
 */
bool
read_recovery_info(const char *archivedir, TimeLineID tli,
				   XLogRecPtr start_lsn, XLogRecPtr stop_lsn,
				   time_t *recovery_time, TransactionId *recovery_xid)
{
	XLogRecPtr	startpoint = stop_lsn;
	XLogReaderState *xlogreader;
	XLogPageReadPrivate private;
	bool		res;

	if (!XRecOffIsValid(start_lsn))
		elog(ERROR, "Invalid start_lsn value %X/%X",
			 (uint32) (start_lsn >> 32), (uint32) (start_lsn));

	if (!XRecOffIsValid(stop_lsn))
		elog(ERROR, "Invalid stop_lsn value %X/%X",
			 (uint32) (stop_lsn >> 32), (uint32) (stop_lsn));

	private.archivedir = archivedir;
	private.tli = tli;

	xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &private);
	if (xlogreader == NULL)
		elog(ERROR, "out of memory");

	/* Read records from stop_lsn down to start_lsn */
	do
	{
		XLogRecord *record;
		TimestampTz last_time = 0;
		char	   *errormsg;

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
					 (uint32) (errptr >> 32), (uint32) (errptr));
		}

		/* Read previous record */
		startpoint = record->xl_prev;

		if (getRecordTimestamp(xlogreader, &last_time))
		{
			*recovery_time = timestamptz_to_time_t(last_time);
			*recovery_xid = XLogRecGetXid(xlogreader);

			/* Found timestamp in WAL record 'record' */
			res = true;
			goto cleanup;
		}
	} while (startpoint >= start_lsn);

	/* Didn't find timestamp from WAL records between start_lsn and stop_lsn */
	res = false;

cleanup:
	XLogReaderFree(xlogreader);
	if (xlogreadfd != -1)
	{
		close(xlogreadfd);
		xlogreadfd = -1;
		xlogexists = false;
	}

	return res;
}

/*
 * Check if there is a WAL segment file in 'archivedir' which contains
 * 'target_lsn'.
 */
bool
wal_contains_lsn(const char *archivedir, XLogRecPtr target_lsn,
				 TimeLineID target_tli)
{
	XLogReaderState *xlogreader;
	XLogPageReadPrivate private;
	char	   *errormsg;
	bool		res;

	if (!XRecOffIsValid(target_lsn))
		elog(ERROR, "Invalid target_lsn value %X/%X",
			 (uint32) (target_lsn >> 32), (uint32) (target_lsn));

	private.archivedir = archivedir;
	private.tli = target_tli;

	xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &private);
	if (xlogreader == NULL)
		elog(ERROR, "out of memory");

	res = XLogReadRecord(xlogreader, target_lsn, &errormsg) != NULL;
	/* Didn't find 'target_lsn' and there is no error, return false */

	XLogReaderFree(xlogreader);
	if (xlogreadfd != -1)
	{
		close(xlogreadfd);
		xlogreadfd = -1;
		xlogexists = false;
	}

	return res;
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
 * Extract information about blocks modified in this record.
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
	else if (rmid == RM_XACT_ID && (xact_info == XLOG_XACT_COMMIT ||
							   xact_info == XLOG_XACT_COMMIT_PREPARED))
	{
		*recordXtime = ((xl_xact_commit *) XLogRecGetData(record))->xact_time;
		return true;
	}
	else if (rmid == RM_XACT_ID && (xact_info == XLOG_XACT_ABORT ||
							   xact_info == XLOG_XACT_ABORT_PREPARED))
	{
		*recordXtime = ((xl_xact_abort *) XLogRecGetData(record))->xact_time;
		return true;
	}

	return false;
}

