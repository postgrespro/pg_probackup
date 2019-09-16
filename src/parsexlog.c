/*-------------------------------------------------------------------------
 *
 * parsexlog.c
 *	  Functions for reading Write-Ahead-Log
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2015-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include "access/transam.h"
#include "catalog/pg_control.h"
#include "commands/dbcommands_xlog.h"
#include "catalog/storage_xlog.h"

#ifdef HAVE_LIBZ
#include <zlib.h>
#endif

#include "utils/thread.h"
#include <unistd.h>
#include <time.h>

/*
 * RmgrNames is an array of resource manager names, to make error messages
 * a bit nicer.
 */
#if PG_VERSION_NUM >= 100000
#define PG_RMGR(symname,name,redo,desc,identify,startup,cleanup,mask) \
  name,
#else
#define PG_RMGR(symname,name,redo,desc,identify,startup,cleanup) \
  name,
#endif

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

/*
 * XLogRecTarget allows to track the last recovery targets. Currently used only
 * within validate_wal().
 */
typedef struct XLogRecTarget
{
	TimestampTz	rec_time;
	TransactionId rec_xid;
	XLogRecPtr	rec_lsn;
} XLogRecTarget;

typedef struct XLogReaderData
{
	int			thread_num;
	TimeLineID	tli;

	XLogRecTarget cur_rec;
	XLogSegNo	xlogsegno;
	bool		xlogexists;

	char		 page_buf[XLOG_BLCKSZ];
	uint32		 prev_page_off;

	bool		need_switch;

	int			xlogfile;
	char		xlogpath[MAXPGPATH];

#ifdef HAVE_LIBZ
	gzFile		 gz_xlogfile;
	char		 gz_xlogpath[MAXPGPATH];
#endif
} XLogReaderData;

/* Function to process a WAL record */
typedef void (*xlog_record_function) (XLogReaderState *record,
									  XLogReaderData *reader_data,
									  bool *stop_reading);

/* An argument for a thread function */
typedef struct
{
	XLogReaderData reader_data;

	xlog_record_function process_record;

	XLogRecPtr	startpoint;
	XLogRecPtr	endpoint;
	XLogSegNo	endSegNo;

	/*
	 * The thread got the recovery target.
	 */
	bool		got_target;

	/*
	 * Return value from the thread.
	 * 0 means there is no error, 1 - there is an error.
	 */
	int			ret;
} xlog_thread_arg;

static int SimpleXLogPageRead(XLogReaderState *xlogreader,
				   XLogRecPtr targetPagePtr,
				   int reqLen, XLogRecPtr targetRecPtr, char *readBuf,
				   TimeLineID *pageTLI);
static XLogReaderState *InitXLogPageRead(XLogReaderData *reader_data,
										 const char *archivedir,
										 TimeLineID tli, uint32 segment_size,
										 bool manual_switch,
										 bool consistent_read,
										 bool allocate_reader);
static bool RunXLogThreads(const char *archivedir,
						   time_t target_time, TransactionId target_xid,
						   XLogRecPtr target_lsn,
						   TimeLineID tli, uint32 segment_size,
						   XLogRecPtr startpoint, XLogRecPtr endpoint,
						   bool consistent_read,
						   xlog_record_function process_record,
						   XLogRecTarget *last_rec);
//static XLogReaderState *InitXLogThreadRead(xlog_thread_arg *arg);
static bool SwitchThreadToNextWal(XLogReaderState *xlogreader,
								  xlog_thread_arg *arg);
static bool XLogWaitForConsistency(XLogReaderState *xlogreader);
static void *XLogThreadWorker(void *arg);
static void CleanupXLogPageRead(XLogReaderState *xlogreader);
static void PrintXLogCorruptionMsg(XLogReaderData *reader_data, int elevel);

static void extractPageInfo(XLogReaderState *record,
							XLogReaderData *reader_data, bool *stop_reading);
static void validateXLogRecord(XLogReaderState *record,
							   XLogReaderData *reader_data, bool *stop_reading);
static bool getRecordTimestamp(XLogReaderState *record, TimestampTz *recordXtime);

static XLogSegNo segno_start = 0;
/* Segment number where target record is located */
static XLogSegNo segno_target = 0;
/* Next segment number to read by a thread */
static XLogSegNo segno_next = 0;
/* Number of segments already read by threads */
static uint32 segnum_read = 0;
/* Number of detected corrupted or absent segments */
static uint32 segnum_corrupted = 0;
static pthread_mutex_t wal_segment_mutex = PTHREAD_MUTEX_INITIALIZER;

/* copied from timestamp.c */
static pg_time_t
timestamptz_to_time_t(TimestampTz t)
{
	pg_time_t	result;

#ifdef HAVE_INT64_TIMESTAMP
	result = (pg_time_t) (t / USECS_PER_SEC +
				 ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
#else
	result = (pg_time_t) (t +
				 ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
#endif
	return result;
}

static const char	   *wal_archivedir = NULL;
static uint32			wal_seg_size = 0;
/*
 * If true a wal reader thread switches to the next segment using
 * segno_next.
 */
static bool				wal_manual_switch = false;
/*
 * If true a wal reader thread waits for other threads if the thread met absent
 * wal segment.
 */
static bool				wal_consistent_read = false;

/*
 * Variables used within validate_wal() and validateXLogRecord() to stop workers
 */
static time_t			wal_target_time = 0;
static TransactionId	wal_target_xid = InvalidTransactionId;
static XLogRecPtr		wal_target_lsn = InvalidXLogRecPtr;

/*
 * Read WAL from the archive directory, from 'startpoint' to 'endpoint' on the
 * given timeline. Collect data blocks touched by the WAL records into a page map.
 *
 * Pagemap extracting is processed using threads. Each thread reads single WAL
 * file.
 */
void
extractPageMap(const char *archivedir, TimeLineID tli, uint32 wal_seg_size,
			   XLogRecPtr startpoint, XLogRecPtr endpoint)
{
	bool		extract_isok = true;
	time_t		start_time,
				end_time;

	elog(LOG, "Compiling pagemap");
	time(&start_time);

	extract_isok = RunXLogThreads(archivedir, 0, InvalidTransactionId,
								  InvalidXLogRecPtr, tli, wal_seg_size,
								  startpoint, endpoint, false, extractPageInfo,
								  NULL);

	time(&end_time);
	if (extract_isok)
		elog(LOG, "Pagemap compiled, time elapsed %.0f sec",
			 difftime(end_time, start_time));
	else
		elog(ERROR, "Pagemap compiling failed");
}

/*
 * Ensure that the backup has all wal files needed for recovery to consistent
 * state.
 *
 * WAL records reading is processed using threads. Each thread reads single WAL
 * file.
 */
static void
validate_backup_wal_from_start_to_stop(pgBackup *backup,
									   const char *archivedir, TimeLineID tli,
									   uint32 xlog_seg_size)
{
	bool		got_endpoint;

	got_endpoint = RunXLogThreads(archivedir, 0, InvalidTransactionId,
								  InvalidXLogRecPtr, tli, xlog_seg_size,
								  backup->start_lsn, backup->stop_lsn,
								  false, NULL, NULL);

	if (!got_endpoint)
	{
		/*
		 * If we don't have WAL between start_lsn and stop_lsn,
		 * the backup is definitely corrupted. Update its status.
		 */
		write_backup_status(backup, BACKUP_STATUS_CORRUPT);

		elog(WARNING, "There are not enough WAL records to consistenly restore "
			"backup %s from START LSN: %X/%X to STOP LSN: %X/%X",
			 base36enc(backup->start_time),
			 (uint32) (backup->start_lsn >> 32),
			 (uint32) (backup->start_lsn),
			 (uint32) (backup->stop_lsn >> 32),
			 (uint32) (backup->stop_lsn));
	}
}

/*
 * Ensure that the backup has all wal files needed for recovery to consistent
 * state. And check if we have in archive all files needed to restore the backup
 * up to the given recovery target.
 */
void
validate_wal(pgBackup *backup, const char *archivedir,
			 time_t target_time, TransactionId target_xid,
			 XLogRecPtr target_lsn, TimeLineID tli, uint32 wal_seg_size)
{
	const char *backup_id;
	XLogRecTarget last_rec;
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
		pgBackupGetPath2(backup, backup_xlog_path, lengthof(backup_xlog_path),
						 DATABASE_DIR, PG_XLOG_DIR);

		validate_backup_wal_from_start_to_stop(backup, backup_xlog_path, tli,
											   wal_seg_size);
	}
	else
		validate_backup_wal_from_start_to_stop(backup, (char *) archivedir, tli,
											   wal_seg_size);

	if (backup->status == BACKUP_STATUS_CORRUPT)
	{
		elog(WARNING, "Backup %s WAL segments are corrupted", backup_id);
		return;
	}
	/*
	 * If recovery target is provided check that we can restore backup to a
	 * recovery target time or xid.
	 */
	if (!TransactionIdIsValid(target_xid) && target_time == 0 &&
		!XRecOffIsValid(target_lsn))
	{
		/* Recovery target is not given so exit */
		elog(INFO, "Backup %s WAL segments are valid", backup_id);
		return;
	}

	/*
	 * If recovery target is provided, ensure that archive files exist in
	 * archive directory.
	 */
	if (dir_is_empty(archivedir, FIO_BACKUP_HOST))
		elog(ERROR, "WAL archive is empty. You cannot restore backup to a recovery target without WAL archive.");

	/*
	 * Check if we have in archive all files needed to restore backup
	 * up to the given recovery target.
	 * In any case we cannot restore to the point before stop_lsn.
	 */

	/* We can restore at least up to the backup end */
	last_rec.rec_time = 0;
	last_rec.rec_xid = backup->recovery_xid;
	last_rec.rec_lsn = backup->stop_lsn;

	time2iso(last_timestamp, lengthof(last_timestamp), backup->recovery_time);

	if ((TransactionIdIsValid(target_xid) && target_xid == last_rec.rec_xid)
		|| (target_time != 0 && backup->recovery_time >= target_time)
		|| (XRecOffIsValid(target_lsn) && last_rec.rec_lsn >= target_lsn))
		all_wal = true;

	all_wal = all_wal ||
		RunXLogThreads(archivedir, target_time, target_xid, target_lsn,
					   tli, wal_seg_size, backup->stop_lsn,
					   InvalidXLogRecPtr, true, validateXLogRecord, &last_rec);
	if (last_rec.rec_time > 0)
		time2iso(last_timestamp, lengthof(last_timestamp),
				 timestamptz_to_time_t(last_rec.rec_time));

	/* There are all needed WAL records */
	if (all_wal)
		elog(INFO, "Backup validation completed successfully on time %s, xid " XID_FMT " and LSN %X/%X",
			 last_timestamp, last_rec.rec_xid,
			 (uint32) (last_rec.rec_lsn >> 32), (uint32) last_rec.rec_lsn);
	/* Some needed WAL records are absent */
	else
	{
		elog(WARNING, "Recovery can be done up to time %s, xid " XID_FMT " and LSN %X/%X",
				last_timestamp, last_rec.rec_xid,
			 (uint32) (last_rec.rec_lsn >> 32), (uint32) last_rec.rec_lsn);

		if (target_time > 0)
			time2iso(target_timestamp, lengthof(target_timestamp), target_time);
		if (TransactionIdIsValid(target_xid) && target_time != 0)
			elog(ERROR, "Not enough WAL records to time %s and xid " XID_FMT,
					target_timestamp, target_xid);
		else if (TransactionIdIsValid(target_xid))
			elog(ERROR, "Not enough WAL records to xid " XID_FMT,
					target_xid);
		else if (target_time != 0)
			elog(ERROR, "Not enough WAL records to time %s",
					target_timestamp);
		else if (XRecOffIsValid(target_lsn))
			elog(ERROR, "Not enough WAL records to lsn %X/%X",
					(uint32) (target_lsn >> 32), (uint32) (target_lsn));
	}
}

/*
 * Read from archived WAL segments latest recovery time and xid. All necessary
 * segments present at archive folder. We waited **stop_lsn** in
 * pg_stop_backup().
 */
bool
read_recovery_info(const char *archivedir, TimeLineID tli, uint32 wal_seg_size,
				   XLogRecPtr start_lsn, XLogRecPtr stop_lsn,
				   time_t *recovery_time)
{
	XLogRecPtr	startpoint = stop_lsn;
	XLogReaderState *xlogreader;
	XLogReaderData reader_data;
	bool		res;

	if (!XRecOffIsValid(start_lsn))
		elog(ERROR, "Invalid start_lsn value %X/%X",
			 (uint32) (start_lsn >> 32), (uint32) (start_lsn));

	if (!XRecOffIsValid(stop_lsn))
		elog(ERROR, "Invalid stop_lsn value %X/%X",
			 (uint32) (stop_lsn >> 32), (uint32) (stop_lsn));

	xlogreader = InitXLogPageRead(&reader_data, archivedir, tli, wal_seg_size,
								  false, true, true);

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
				elog(ERROR, "Could not read WAL record at %X/%X: %s",
					 (uint32) (errptr >> 32), (uint32) (errptr),
					 errormsg);
			else
				elog(ERROR, "Could not read WAL record at %X/%X",
					 (uint32) (errptr >> 32), (uint32) (errptr));
		}

		/* Read previous record */
		startpoint = record->xl_prev;

		if (getRecordTimestamp(xlogreader, &last_time))
		{
			*recovery_time = timestamptz_to_time_t(last_time);

			/* Found timestamp in WAL record 'record' */
			res = true;
			goto cleanup;
		}
	} while (startpoint >= start_lsn);

	/* Didn't find timestamp from WAL records between start_lsn and stop_lsn */
	res = false;

cleanup:
	CleanupXLogPageRead(xlogreader);
	XLogReaderFree(xlogreader);

	return res;
}

/*
 * Check if there is a WAL segment file in 'archivedir' which contains
 * 'target_lsn'.
 */
bool
wal_contains_lsn(const char *archivedir, XLogRecPtr target_lsn,
				 TimeLineID target_tli, uint32 wal_seg_size)
{
	XLogReaderState *xlogreader;
	XLogReaderData reader_data;
	char	   *errormsg;
	bool		res;

	if (!XRecOffIsValid(target_lsn))
		elog(ERROR, "Invalid target_lsn value %X/%X",
			 (uint32) (target_lsn >> 32), (uint32) (target_lsn));

	xlogreader = InitXLogPageRead(&reader_data, archivedir, target_tli,
								  wal_seg_size, false, false, true);

	if (xlogreader == NULL)
			elog(ERROR, "Out of memory");

	xlogreader->system_identifier = instance_config.system_identifier;

	res = XLogReadRecord(xlogreader, target_lsn, &errormsg) != NULL;
	/* Didn't find 'target_lsn' and there is no error, return false */

	if (errormsg)
		elog(WARNING, "Could not read WAL record at %X/%X: %s",
				(uint32) (target_lsn >> 32), (uint32) (target_lsn), errormsg);

	CleanupXLogPageRead(xlogreader);
	XLogReaderFree(xlogreader);

	return res;
}

/*
 * Get LSN of a first record within the WAL segment with number 'segno'.
 */
XLogRecPtr
get_first_record_lsn(const char *archivedir, XLogSegNo	segno,
				 TimeLineID tli, uint32 wal_seg_size)
{
	XLogReaderState *xlogreader;
	XLogReaderData reader_data;
	XLogRecPtr	record = InvalidXLogRecPtr;
	XLogRecPtr	startpoint;
	char	wal_segment[MAXFNAMELEN];

	if (segno <= 1)
		elog(ERROR, "Invalid WAL segment number " UINT64_FORMAT, segno);

	GetXLogFileName(wal_segment, tli, segno, instance_config.xlog_seg_size);

	xlogreader = InitXLogPageRead(&reader_data, archivedir, tli, wal_seg_size,
								  false, false, true);
	if (xlogreader == NULL)
			elog(ERROR, "Out of memory");
	xlogreader->system_identifier = instance_config.system_identifier;

	/* Set startpoint to 0 in segno */
	GetXLogRecPtr(segno, 0, wal_seg_size, startpoint);

	record = XLogFindNextRecord(xlogreader, startpoint);

	if (XLogRecPtrIsInvalid(record))
		record = InvalidXLogRecPtr;
	else
		elog(LOG, "First record in WAL segment \"%s\": %X/%X", wal_segment,
				(uint32) (record >> 32), (uint32) (record));

	/* cleanup */
	CleanupXLogPageRead(xlogreader);
	XLogReaderFree(xlogreader);

	return record;
}

/*
 * Get LSN of a record prior to target_lsn.
 * If 'start_lsn' is in the segment with number 'segno' then start from 'start_lsn',
 * otherwise start from offset 0 within the segment.
 *
 * Returns LSN of a record which EndRecPtr is greater or equal to target_lsn.
 * If 'seek_prev_segment' is true, then look for prior record in prior WAL segment.
 *
 * it's unclear that "last" in "last_wal_lsn" refers to the
 * "closest to stop_lsn backward or forward, depending on seek_prev_segment setting".
 */
XLogRecPtr
get_prior_record_lsn(const char *archivedir, XLogRecPtr start_lsn,
				 XLogRecPtr stop_lsn, TimeLineID tli, bool seek_prev_segment,
				 uint32 wal_seg_size)
{
	XLogReaderState *xlogreader;
	XLogReaderData reader_data;
	XLogRecPtr	startpoint;
	XLogSegNo	start_segno;
	XLogSegNo	segno;
	XLogRecPtr	res = InvalidXLogRecPtr;

	GetXLogSegNo(stop_lsn, segno, wal_seg_size);

	if (segno <= 1)
		elog(ERROR, "Invalid WAL segment number " UINT64_FORMAT, segno);

	if (seek_prev_segment)
		segno = segno - 1;

	xlogreader = InitXLogPageRead(&reader_data, archivedir, tli, wal_seg_size,
								  false, false, true);

	if (xlogreader == NULL)
			elog(ERROR, "Out of memory");

	xlogreader->system_identifier = instance_config.system_identifier;

	/*
	 * Calculate startpoint. Decide: we should use 'start_lsn' or offset 0.
	 */
	GetXLogSegNo(start_lsn, start_segno, wal_seg_size);
	if (start_segno == segno)
		startpoint = start_lsn;
	else
	{
		XLogRecPtr	found;

		GetXLogRecPtr(segno, 0, wal_seg_size, startpoint);
		found = XLogFindNextRecord(xlogreader, startpoint);

		if (XLogRecPtrIsInvalid(found))
		{
			if (xlogreader->errormsg_buf[0] != '\0')
				elog(WARNING, "Could not read WAL record at %X/%X: %s",
					 (uint32) (startpoint >> 32), (uint32) (startpoint),
					 xlogreader->errormsg_buf);
			else
				elog(WARNING, "Could not read WAL record at %X/%X",
					 (uint32) (startpoint >> 32), (uint32) (startpoint));
			PrintXLogCorruptionMsg(&reader_data, ERROR);
		}
		startpoint = found;
	}

	while (true)
	{
		XLogRecord *record;
		char	   *errormsg;

		if (interrupted)
			elog(ERROR, "Interrupted during WAL reading");

		record = XLogReadRecord(xlogreader, startpoint, &errormsg);
		if (record == NULL)
		{
			XLogRecPtr	errptr;

			errptr = XLogRecPtrIsInvalid(startpoint) ? xlogreader->EndRecPtr :
				startpoint;

			if (errormsg)
				elog(WARNING, "Could not read WAL record at %X/%X: %s",
					 (uint32) (errptr >> 32), (uint32) (errptr),
					 errormsg);
			else
				elog(WARNING, "Could not read WAL record at %X/%X",
					 (uint32) (errptr >> 32), (uint32) (errptr));
			PrintXLogCorruptionMsg(&reader_data, ERROR);
		}

		if (xlogreader->EndRecPtr >= stop_lsn)
		{
			elog(LOG, "Record %X/%X has endpoint %X/%X which is equal or greater than requested LSN %X/%X",
				(uint32) (xlogreader->ReadRecPtr >> 32), (uint32) (xlogreader->ReadRecPtr),
				(uint32) (xlogreader->EndRecPtr >> 32), (uint32) (xlogreader->EndRecPtr),
				(uint32) (stop_lsn >> 32), (uint32) (stop_lsn));
			res = xlogreader->ReadRecPtr;
			break;
		}

		/* continue reading at next record */
		startpoint = InvalidXLogRecPtr;
	}

	CleanupXLogPageRead(xlogreader);
	XLogReaderFree(xlogreader);

	return res;
}

#ifdef HAVE_LIBZ
/*
 * Show error during work with compressed file
 */
static const char *
get_gz_error(gzFile gzf)
{
	int			errnum;
	const char *errmsg;

	errmsg = fio_gzerror(gzf, &errnum);
	if (errnum == Z_ERRNO)
		return strerror(errno);
	else
		return errmsg;
}
#endif

/* XLogreader callback function, to read a WAL page */
static int
SimpleXLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr,
				   int reqLen, XLogRecPtr targetRecPtr, char *readBuf,
				   TimeLineID *pageTLI)
{
	XLogReaderData *reader_data;
	uint32		targetPageOff;

	reader_data = (XLogReaderData *) xlogreader->private_data;
	targetPageOff = targetPagePtr % wal_seg_size;

	if (interrupted || thread_interrupted)
		elog(ERROR, "Thread [%d]: Interrupted during WAL reading",
			 reader_data->thread_num);

	/*
	 * See if we need to switch to a new segment because the requested record
	 * is not in the currently open one.
	 */
	if (!IsInXLogSeg(targetPagePtr, reader_data->xlogsegno, wal_seg_size))
	{
		elog(VERBOSE, "Thread [%d]: Need to switch to the next WAL segment, page LSN %X/%X, record being read LSN %X/%X",
			 reader_data->thread_num,
			 (uint32) (targetPagePtr >> 32), (uint32) (targetPagePtr),
			 (uint32) (xlogreader->currRecPtr >> 32),
			 (uint32) (xlogreader->currRecPtr ));

		/*
		 * If the last record on the page is not complete,
		 * we must continue reading pages in the same thread
		 */
		if (!XLogRecPtrIsInvalid(xlogreader->currRecPtr) &&
			xlogreader->currRecPtr < targetPagePtr)
		{
			CleanupXLogPageRead(xlogreader);

			/*
			 * Switch to the next WAL segment after reading contrecord.
			 */
			if (wal_manual_switch)
				reader_data->need_switch = true;
		}
		else
		{
			CleanupXLogPageRead(xlogreader);
			/*
			 * Do not switch to next WAL segment in this function. It is
			 * manually switched by a thread routine.
			 */
			if (wal_manual_switch)
			{
				reader_data->need_switch = true;
				return -1;
			}
		}
	}

	GetXLogSegNo(targetPagePtr, reader_data->xlogsegno, wal_seg_size);

	/* Try to switch to the next WAL segment */
	if (!reader_data->xlogexists)
	{
		char		xlogfname[MAXFNAMELEN];

		GetXLogFileName(xlogfname, reader_data->tli, reader_data->xlogsegno,
						wal_seg_size);
		snprintf(reader_data->xlogpath, MAXPGPATH, "%s/%s", wal_archivedir,
				 xlogfname);

		if (fileExists(reader_data->xlogpath, FIO_BACKUP_HOST))
		{
			elog(LOG, "Thread [%d]: Opening WAL segment \"%s\"",
				 reader_data->thread_num, reader_data->xlogpath);

			reader_data->xlogexists = true;
			reader_data->xlogfile = fio_open(reader_data->xlogpath,
											 O_RDONLY | PG_BINARY, FIO_BACKUP_HOST);

			if (reader_data->xlogfile < 0)
			{
				elog(WARNING, "Thread [%d]: Could not open WAL segment \"%s\": %s",
					 reader_data->thread_num, reader_data->xlogpath,
					 strerror(errno));
				return -1;
			}
		}
#ifdef HAVE_LIBZ
		/* Try to open compressed WAL segment */
		else
		{
			snprintf(reader_data->gz_xlogpath, sizeof(reader_data->gz_xlogpath),
					 "%s.gz", reader_data->xlogpath);
			if (fileExists(reader_data->gz_xlogpath, FIO_BACKUP_HOST))
			{
				elog(LOG, "Thread [%d]: Opening compressed WAL segment \"%s\"",
					 reader_data->thread_num, reader_data->gz_xlogpath);

				reader_data->xlogexists = true;
				reader_data->gz_xlogfile = fio_gzopen(reader_data->gz_xlogpath,
													  "rb", -1, FIO_BACKUP_HOST);
				if (reader_data->gz_xlogfile == NULL)
				{
					elog(WARNING, "Thread [%d]: Could not open compressed WAL segment \"%s\": %s",
						 reader_data->thread_num, reader_data->gz_xlogpath,
						 strerror(errno));
					return -1;
				}
			}
		}
#endif

		/* Exit without error if WAL segment doesn't exist */
		if (!reader_data->xlogexists)
			return -1;
	}

	/*
	 * At this point, we have the right segment open.
	 */
	Assert(reader_data->xlogexists);

	/*
	 * Do not read same page read earlier from the file, read it from the buffer
	 */
	if (reader_data->prev_page_off != 0 &&
		reader_data->prev_page_off == targetPageOff)
	{
		memcpy(readBuf, reader_data->page_buf, XLOG_BLCKSZ);
		*pageTLI = reader_data->tli;
		return XLOG_BLCKSZ;
	}

	/* Read the requested page */
	if (reader_data->xlogfile != -1)
	{
		if (fio_seek(reader_data->xlogfile, (off_t) targetPageOff) < 0)
		{
			elog(WARNING, "Thread [%d]: Could not seek in WAL segment \"%s\": %s",
				 reader_data->thread_num, reader_data->xlogpath, strerror(errno));
			return -1;
		}

		if (fio_read(reader_data->xlogfile, readBuf, XLOG_BLCKSZ) != XLOG_BLCKSZ)
		{
			elog(WARNING, "Thread [%d]: Could not read from WAL segment \"%s\": %s",
				 reader_data->thread_num, reader_data->xlogpath, strerror(errno));
			return -1;
		}
	}
#ifdef HAVE_LIBZ
	else
	{
		if (fio_gzseek(reader_data->gz_xlogfile, (z_off_t) targetPageOff, SEEK_SET) == -1)
		{
			elog(WARNING, "Thread [%d]: Could not seek in compressed WAL segment \"%s\": %s",
				reader_data->thread_num, reader_data->gz_xlogpath,
				get_gz_error(reader_data->gz_xlogfile));
			return -1;
		}

		if (fio_gzread(reader_data->gz_xlogfile, readBuf, XLOG_BLCKSZ) != XLOG_BLCKSZ)
		{
			elog(WARNING, "Thread [%d]: Could not read from compressed WAL segment \"%s\": %s",
				reader_data->thread_num, reader_data->gz_xlogpath,
				get_gz_error(reader_data->gz_xlogfile));
			return -1;
		}
	}
#endif

	memcpy(reader_data->page_buf, readBuf, XLOG_BLCKSZ);
	reader_data->prev_page_off = targetPageOff;
	*pageTLI = reader_data->tli;
	return XLOG_BLCKSZ;
}

/*
 * Initialize WAL segments reading.
 */
static XLogReaderState *
InitXLogPageRead(XLogReaderData *reader_data, const char *archivedir,
				 TimeLineID tli, uint32 segment_size, bool manual_switch,
				 bool consistent_read, bool allocate_reader)
{
	XLogReaderState *xlogreader = NULL;

	wal_archivedir = archivedir;
	wal_seg_size = segment_size;
	wal_manual_switch = manual_switch;
	wal_consistent_read = consistent_read;

	MemSet(reader_data, 0, sizeof(XLogReaderData));
	reader_data->tli = tli;
	reader_data->xlogfile = -1;

	if (allocate_reader)
	{
#if PG_VERSION_NUM >= 110000
		xlogreader = XLogReaderAllocate(wal_seg_size, &SimpleXLogPageRead,
										reader_data);
#else
		xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, reader_data);
#endif
		if (xlogreader == NULL)
			elog(ERROR, "Out of memory");
		xlogreader->system_identifier = instance_config.system_identifier;
	}

	return xlogreader;
}

/*
 * Comparison function to sort xlog_thread_arg array.
 */
static int
xlog_thread_arg_comp(const void *a1, const void *a2)
{
	const xlog_thread_arg *arg1 = a1;
	const xlog_thread_arg *arg2 = a2;

	return arg1->reader_data.xlogsegno - arg2->reader_data.xlogsegno;
}

/*
 * Run WAL processing routines using threads. Start from startpoint up to
 * endpoint. It is possible to send zero endpoint, threads will read WAL
 * infinitely in this case.
 */
static bool
RunXLogThreads(const char *archivedir, time_t target_time,
			   TransactionId target_xid, XLogRecPtr target_lsn, TimeLineID tli,
			   uint32 segment_size, XLogRecPtr startpoint, XLogRecPtr endpoint,
			   bool consistent_read, xlog_record_function process_record,
			   XLogRecTarget *last_rec)
{
	pthread_t  *threads;
	xlog_thread_arg *thread_args;
	int			i;
	int			threads_need = 0;
	XLogSegNo	endSegNo = 0;
	bool		result = true;

	if (!XRecOffIsValid(startpoint))
		elog(ERROR, "Invalid startpoint value %X/%X",
			 (uint32) (startpoint >> 32), (uint32) (startpoint));

	if (!XLogRecPtrIsInvalid(endpoint))
	{
		if (!XRecOffIsValid(endpoint))
			elog(ERROR, "Invalid endpoint value %X/%X",
				(uint32) (endpoint >> 32), (uint32) (endpoint));

		GetXLogSegNo(endpoint, endSegNo, segment_size);
	}

	/* Initialize static variables for workers */
	wal_target_time = target_time;
	wal_target_xid = target_xid;
	wal_target_lsn = target_lsn;

	GetXLogSegNo(startpoint, segno_start, segment_size);
	segno_target = 0;
	GetXLogSegNo(startpoint, segno_next, segment_size);
	segnum_read = 0;
	segnum_corrupted = 0;

	threads = (pthread_t *) pgut_malloc(sizeof(pthread_t) * num_threads);
	thread_args = (xlog_thread_arg *) pgut_malloc(sizeof(xlog_thread_arg) * num_threads);

	/*
	 * Initialize thread args.
	 *
	 * Each thread works with its own WAL segment and we need to adjust
	 * startpoint value for each thread.
	 */
	for (i = 0; i < num_threads; i++)
	{
		xlog_thread_arg *arg = &thread_args[i];

		InitXLogPageRead(&arg->reader_data, archivedir, tli, segment_size, true,
						 consistent_read, false);
		arg->reader_data.xlogsegno = segno_next;
		arg->reader_data.thread_num = i + 1;
		arg->process_record = process_record;
		arg->startpoint = startpoint;
		arg->endpoint = endpoint;
		arg->endSegNo = endSegNo;
		arg->got_target = false;
		/* By default there is some error */
		arg->ret = 1;

		threads_need++;
		segno_next++;
		/*
		 * If we need to read less WAL segments than num_threads, create less
		 * threads.
		 */
		if (endSegNo != 0 && segno_next > endSegNo)
			break;
		GetXLogRecPtr(segno_next, 0, segment_size, startpoint);
	}

	/* Run threads */
	thread_interrupted = false;
	for (i = 0; i < threads_need; i++)
	{
		elog(VERBOSE, "Start WAL reader thread: %d", i + 1);
		pthread_create(&threads[i], NULL, XLogThreadWorker, &thread_args[i]);
	}

	/* Wait for threads */
	for (i = 0; i < threads_need; i++)
	{
		pthread_join(threads[i], NULL);
		if (thread_args[i].ret == 1)
			result = false;
	}

	/* Release threads here, use thread_args only below */
	pfree(threads);
	threads = NULL;

	if (last_rec)
	{
		/*
		 * We need to sort xlog_thread_arg array by xlogsegno to return latest
		 * possible record up to which restore is possible. We need to sort to
		 * detect failed thread between start segment and target segment.
		 *
		 * Loop stops on first failed thread.
		 */
		if (threads_need > 1)
			qsort((void *) thread_args, threads_need, sizeof(xlog_thread_arg),
				  xlog_thread_arg_comp);

		for (i = 0; i < threads_need; i++)
		{
			XLogRecTarget *cur_rec;

			cur_rec = &thread_args[i].reader_data.cur_rec;
			/*
			 * If we got the target return minimum possible record.
			 */
			if (segno_target > 0)
			{
				if (thread_args[i].got_target &&
					thread_args[i].reader_data.xlogsegno == segno_target)
				{
					*last_rec = *cur_rec;
					break;
				}
			}
			/*
			 * Else return maximum possible record up to which restore is
			 * possible.
			 */
			else if (last_rec->rec_lsn < cur_rec->rec_lsn)
				*last_rec = *cur_rec;

			/*
			 * We reached failed thread, so stop here. We cannot use following
			 * WAL records after failed segment.
			 */
			if (thread_args[i].ret != 0)
				break;
		}
	}

	pfree(thread_args);

	return result;
}

/*
 * WAL reader worker.
 */
void *
XLogThreadWorker(void *arg)
{
	xlog_thread_arg *thread_arg = (xlog_thread_arg *) arg;
	XLogReaderData *reader_data = &thread_arg->reader_data;
	XLogReaderState *xlogreader;
	XLogSegNo	nextSegNo = 0;
	XLogRecPtr	found;
	uint32		prev_page_off = 0;
	bool		need_read = true;

#if PG_VERSION_NUM >= 110000
	xlogreader = XLogReaderAllocate(wal_seg_size, &SimpleXLogPageRead,
									reader_data);
#else
	xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, reader_data);
#endif
	if (xlogreader == NULL)
		elog(ERROR, "Thread [%d]: out of memory", reader_data->thread_num);
	xlogreader->system_identifier = instance_config.system_identifier;

	found = XLogFindNextRecord(xlogreader, thread_arg->startpoint);

	/*
	 * We get invalid WAL record pointer usually when WAL segment is absent or
	 * is corrupted.
	 */
	if (XLogRecPtrIsInvalid(found))
	{
		if (wal_consistent_read && XLogWaitForConsistency(xlogreader))
			need_read = false;
		else
		{
			if (xlogreader->errormsg_buf[0] != '\0')
				elog(WARNING, "Thread [%d]: Could not read WAL record at %X/%X: %s",
					reader_data->thread_num,
					(uint32) (thread_arg->startpoint >> 32),
					(uint32) (thread_arg->startpoint),
					xlogreader->errormsg_buf);
			else
				elog(WARNING, "Thread [%d]: Could not read WAL record at %X/%X",
					reader_data->thread_num,
					(uint32) (thread_arg->startpoint >> 32),
					(uint32) (thread_arg->startpoint));
			PrintXLogCorruptionMsg(reader_data, ERROR);
		}
	}

	thread_arg->startpoint = found;

	elog(VERBOSE, "Thread [%d]: Starting LSN: %X/%X",
		 reader_data->thread_num,
		 (uint32) (thread_arg->startpoint >> 32),
		 (uint32) (thread_arg->startpoint));

	while (need_read)
	{
		XLogRecord *record;
		char	   *errormsg;
		bool		stop_reading = false;

		if (interrupted || thread_interrupted)
			elog(ERROR, "Thread [%d]: Interrupted during WAL reading",
				 reader_data->thread_num);

		/*
		 * We need to switch to the next WAL segment after reading previous
		 * record. It may happen if we read contrecord.
		 */
		if (reader_data->need_switch &&
			!SwitchThreadToNextWal(xlogreader, thread_arg))
			break;

		record = XLogReadRecord(xlogreader, thread_arg->startpoint, &errormsg);

		if (record == NULL)
		{
			XLogRecPtr	errptr;

			/*
			 * There is no record, try to switch to the next WAL segment.
			 * Usually SimpleXLogPageRead() does it by itself. But here we need
			 * to do it manually to support threads.
			 */
			if (reader_data->need_switch && errormsg == NULL)
			{
				if (SwitchThreadToNextWal(xlogreader, thread_arg))
					continue;
				else
					break;
			}

			/*
			 * XLogWaitForConsistency() is normally used only with threads.
			 * Call it here for just in case.
			 */
			if (wal_consistent_read && XLogWaitForConsistency(xlogreader))
				break;
			else if (wal_consistent_read)
			{
				XLogSegNo	segno_report;

				pthread_lock(&wal_segment_mutex);
				segno_report = segno_start + segnum_read;
				pthread_mutex_unlock(&wal_segment_mutex);

				/*
				 * Report error message if this is the first corrupted WAL.
				*/
				if (reader_data->xlogsegno > segno_report)
					return NULL;	/* otherwise just stop the thread */
			}

			errptr = thread_arg->startpoint ?
				thread_arg->startpoint : xlogreader->EndRecPtr;

			if (errormsg)
				elog(WARNING, "Thread [%d]: Could not read WAL record at %X/%X: %s",
					 reader_data->thread_num,
					 (uint32) (errptr >> 32), (uint32) (errptr),
					 errormsg);
			else
				elog(WARNING, "Thread [%d]: Could not read WAL record at %X/%X",
					 reader_data->thread_num,
					 (uint32) (errptr >> 32), (uint32) (errptr));

			/*
			 * If we don't have all WAL files from prev backup start_lsn to current
			 * start_lsn, we won't be able to build page map and PAGE backup will
			 * be incorrect. Stop it and throw an error.
			 */
			PrintXLogCorruptionMsg(reader_data, ERROR);
		}

		getRecordTimestamp(xlogreader, &reader_data->cur_rec.rec_time);
		if (TransactionIdIsValid(XLogRecGetXid(xlogreader)))
			reader_data->cur_rec.rec_xid = XLogRecGetXid(xlogreader);
		reader_data->cur_rec.rec_lsn = xlogreader->ReadRecPtr;

		if (thread_arg->process_record)
			thread_arg->process_record(xlogreader, reader_data, &stop_reading);
		if (stop_reading)
		{
			thread_arg->got_target = true;

			pthread_lock(&wal_segment_mutex);
			/* We should store least target segment number */
			if (segno_target == 0 || segno_target > reader_data->xlogsegno)
				segno_target = reader_data->xlogsegno;
			pthread_mutex_unlock(&wal_segment_mutex);

			break;
		}

		/*
		 * Check if other thread got the target segment. Check it not very
		 * often, only every WAL page.
		 */
		if (wal_consistent_read && prev_page_off != 0 &&
			prev_page_off != reader_data->prev_page_off)
		{
			XLogSegNo	segno;

			pthread_lock(&wal_segment_mutex);
			segno = segno_target;
			pthread_mutex_unlock(&wal_segment_mutex);

			if (segno != 0 && segno < reader_data->xlogsegno)
				break;
		}
		prev_page_off = reader_data->prev_page_off;

		/* continue reading at next record */
		thread_arg->startpoint = InvalidXLogRecPtr;

		GetXLogSegNo(xlogreader->EndRecPtr, nextSegNo, wal_seg_size);

		if (thread_arg->endSegNo != 0 &&
			!XLogRecPtrIsInvalid(thread_arg->endpoint) &&
			/*
			 * Consider thread_arg->endSegNo and thread_arg->endpoint only if
			 * they are valid.
			 */
			xlogreader->ReadRecPtr >= thread_arg->endpoint &&
			nextSegNo >= thread_arg->endSegNo)
			break;
	}

	CleanupXLogPageRead(xlogreader);
	XLogReaderFree(xlogreader);

	/* Extracting is successful */
	thread_arg->ret = 0;
	return NULL;
}

/*
 * Do manual switch to the next WAL segment.
 *
 * Returns false if the reader reaches the end of a WAL segment list.
 */
static bool
SwitchThreadToNextWal(XLogReaderState *xlogreader, xlog_thread_arg *arg)
{
	XLogReaderData *reader_data;
	XLogRecPtr	found;

	reader_data = (XLogReaderData *) xlogreader->private_data;
	reader_data->need_switch = false;

	/* Critical section */
	pthread_lock(&wal_segment_mutex);
	Assert(segno_next);
	reader_data->xlogsegno = segno_next;
	segnum_read++;
	segno_next++;
	pthread_mutex_unlock(&wal_segment_mutex);

	/* We've reached the end */
	if (arg->endSegNo != 0 && reader_data->xlogsegno > arg->endSegNo)
		return false;

	/* Adjust next record position */
	GetXLogRecPtr(reader_data->xlogsegno, 0, wal_seg_size, arg->startpoint);
	/* We need to close previously opened file if it wasn't closed earlier */
	CleanupXLogPageRead(xlogreader);
	/* Skip over the page header and contrecord if any */
	found = XLogFindNextRecord(xlogreader, arg->startpoint);

	/*
	 * We get invalid WAL record pointer usually when WAL segment is
	 * absent or is corrupted.
	 */
	if (XLogRecPtrIsInvalid(found))
	{
		/*
		 * Check if we need to stop reading. We stop if other thread found a
		 * target segment.
		 */
		if (wal_consistent_read && XLogWaitForConsistency(xlogreader))
			return false;
		else if (wal_consistent_read)
		{
			XLogSegNo	segno_report;

			pthread_lock(&wal_segment_mutex);
			segno_report = segno_start + segnum_read;
			pthread_mutex_unlock(&wal_segment_mutex);

			/*
			 * Report error message if this is the first corrupted WAL.
			 */
			if (reader_data->xlogsegno > segno_report)
				return false;	/* otherwise just stop the thread */
		}

		elog(WARNING, "Thread [%d]: Could not read WAL record at %X/%X",
			 reader_data->thread_num,
			 (uint32) (arg->startpoint >> 32), (uint32) (arg->startpoint));
		PrintXLogCorruptionMsg(reader_data, ERROR);
	}
	arg->startpoint = found;

	elog(VERBOSE, "Thread [%d]: Switched to LSN %X/%X",
		 reader_data->thread_num,
		 (uint32) (arg->startpoint >> 32), (uint32) (arg->startpoint));

	return true;
}

/*
 * Wait for other threads since the current thread couldn't read its segment.
 * We need to decide is it fail or not.
 *
 * Returns true if there is no failure and previous target segment was found.
 * Otherwise return false.
 */
static bool
XLogWaitForConsistency(XLogReaderState *xlogreader)
{
	uint32		segnum_need;
	XLogReaderData *reader_data =(XLogReaderData *) xlogreader->private_data;
	bool		log_message = true;

	segnum_need = reader_data->xlogsegno - segno_start;
	while (true)
	{
		uint32		segnum_current_read;
		XLogSegNo	segno;

		if (log_message)
		{
			char		xlogfname[MAXFNAMELEN];

			GetXLogFileName(xlogfname, reader_data->tli, reader_data->xlogsegno,
							wal_seg_size);

			elog(VERBOSE, "Thread [%d]: Possible WAL corruption in %s. Wait for other threads to decide is this a failure",
				 reader_data->thread_num, xlogfname);
			log_message = false;
		}

		if (interrupted || thread_interrupted)
			elog(ERROR, "Thread [%d]: Interrupted during WAL reading",
				 reader_data->thread_num);

		pthread_lock(&wal_segment_mutex);
		segnum_current_read = segnum_read + segnum_corrupted;
		segno = segno_target;
		pthread_mutex_unlock(&wal_segment_mutex);

		/* Other threads read all previous segments and didn't find target */
		if (segnum_need <= segnum_current_read)
		{
			/* Mark current segment as corrupted */
			pthread_lock(&wal_segment_mutex);
			segnum_corrupted++;
			pthread_mutex_unlock(&wal_segment_mutex);
			return false;
		}

		if (segno != 0 && segno < reader_data->xlogsegno)
			return true;

		pg_usleep(500000L);	/* 500 ms */
	}

	/* We shouldn't reach it */
	return false;
}

/*
 * Cleanup after WAL segment reading.
 */
static void
CleanupXLogPageRead(XLogReaderState *xlogreader)
{
	XLogReaderData *reader_data;

	reader_data = (XLogReaderData *) xlogreader->private_data;
	if (reader_data->xlogfile >= 0)
	{
		fio_close(reader_data->xlogfile);
		reader_data->xlogfile = -1;
	}
#ifdef HAVE_LIBZ
	else if (reader_data->gz_xlogfile != NULL)
	{
		fio_gzclose(reader_data->gz_xlogfile);
		reader_data->gz_xlogfile = NULL;
	}
#endif
	reader_data->prev_page_off = 0;
	reader_data->xlogexists = false;
}

static void
PrintXLogCorruptionMsg(XLogReaderData *reader_data, int elevel)
{
	if (reader_data->xlogpath[0] != 0)
	{
		/*
		 * XLOG reader couldn't read WAL segment.
		 * We throw a WARNING here to be able to update backup status.
		 */
		if (!reader_data->xlogexists)
			elog(elevel, "Thread [%d]: WAL segment \"%s\" is absent",
				 reader_data->thread_num, reader_data->xlogpath);
		else if (reader_data->xlogfile != -1)
			elog(elevel, "Thread [%d]: Possible WAL corruption. "
						 "Error has occured during reading WAL segment \"%s\"",
				 reader_data->thread_num, reader_data->xlogpath);
#ifdef HAVE_LIBZ
		else if (reader_data->gz_xlogfile != NULL)
			elog(elevel, "Thread [%d]: Possible WAL corruption. "
						 "Error has occured during reading WAL segment \"%s\"",
				 reader_data->thread_num, reader_data->gz_xlogpath);
#endif
	}
	else
	{
		/* Cannot tell what happened specifically */
		elog(elevel, "Thread [%d]: An error occured during WAL reading",
			 reader_data->thread_num);
	}
}

/*
 * Extract information about blocks modified in this record.
 */
static void
extractPageInfo(XLogReaderState *record, XLogReaderData *reader_data,
				bool *stop_reading)
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

		/* We only care about the main fork; others are copied as is */
		if (forknum != MAIN_FORKNUM)
			continue;

		process_block_change(forknum, rnode, blkno);
	}
}

/*
 * Check the current read WAL record during validation.
 */
static void
validateXLogRecord(XLogReaderState *record, XLogReaderData *reader_data,
				   bool *stop_reading)
{
	/* Check target xid */
	if (TransactionIdIsValid(wal_target_xid) &&
		wal_target_xid == reader_data->cur_rec.rec_xid)
		*stop_reading = true;
	/* Check target time */
	else if (wal_target_time != 0 &&
			 timestamptz_to_time_t(reader_data->cur_rec.rec_time) >= wal_target_time)
		*stop_reading = true;
	/* Check target lsn */
	else if (XRecOffIsValid(wal_target_lsn) &&
			 reader_data->cur_rec.rec_lsn >= wal_target_lsn)
		*stop_reading = true;
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

