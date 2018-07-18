/*-------------------------------------------------------------------------
 *
 * parsexlog.c
 *	  Functions for reading Write-Ahead-Log
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <unistd.h>
#ifdef HAVE_LIBZ
#include <zlib.h>
#endif

#include "commands/dbcommands_xlog.h"
#include "catalog/storage_xlog.h"
#include "access/transam.h"
#include "utils/thread.h"

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

static void extractPageInfo(XLogReaderState *record);
static bool getRecordTimestamp(XLogReaderState *record, TimestampTz *recordXtime);

typedef struct XLogPageReadPrivate
{
	const char *archivedir;
	TimeLineID	tli;

	bool		manual_switch;
	bool		need_switch;

	int			xlogfile;
	XLogSegNo	xlogsegno;
	char		xlogpath[MAXPGPATH];
	bool		xlogexists;

#ifdef HAVE_LIBZ
	gzFile		gz_xlogfile;
	char		gz_xlogpath[MAXPGPATH];
#endif
} XLogPageReadPrivate;

/* An argument for a thread function */
typedef struct
{
	int			thread_num;
	XLogPageReadPrivate private_data;

	XLogRecPtr	startpoint;
	XLogRecPtr	endpoint;
	XLogSegNo	endSegNo;

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
static XLogReaderState *InitXLogPageRead(XLogPageReadPrivate *private_data,
										 const char *archivedir,
										 TimeLineID tli, bool allocate_reader);
static void CleanupXLogPageRead(XLogReaderState *xlogreader);
static void PrintXLogCorruptionMsg(XLogPageReadPrivate *private_data,
								   int elevel);

static XLogSegNo nextSegNoToRead = 0;
static pthread_mutex_t wal_segment_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * extractPageMap() worker.
 */
static void *
doExtractPageMap(void *arg)
{
	xlog_thread_arg *extract_arg = (xlog_thread_arg *) arg;
	XLogReaderState *xlogreader;
	XLogSegNo	nextSegNo = 0;
	char	   *errormsg;

	xlogreader = XLogReaderAllocate(&SimpleXLogPageRead,
									&extract_arg->private_data);
	if (xlogreader == NULL)
		elog(ERROR, "out of memory");

	extract_arg->startpoint = XLogFindNextRecord(xlogreader,
												 extract_arg->startpoint);

	elog(VERBOSE, "Start LSN of thread %d: %X/%X",
		 extract_arg->thread_num,
		 (uint32) (extract_arg->startpoint >> 32),
		 (uint32) (extract_arg->startpoint));

	do
	{
		XLogRecord *record;

		if (interrupted)
			elog(ERROR, "Interrupted during WAL reading");

		record = XLogReadRecord(xlogreader, extract_arg->startpoint, &errormsg);

		if (record == NULL)
		{
			XLogRecPtr	errptr;

			/* Try to switch to the next WAL segment */
			if (extract_arg->private_data.need_switch)
			{
				extract_arg->private_data.need_switch = false;

				pthread_lock(&wal_segment_mutex);
				Assert(nextSegNoToRead);
				extract_arg->private_data.xlogsegno = nextSegNoToRead;
				nextSegNoToRead++;
				pthread_mutex_unlock(&wal_segment_mutex);

				/* We reach the end */
				if (extract_arg->private_data.xlogsegno > extract_arg->endSegNo)
					break;

				/* Adjust next record position */
				XLogSegNoOffsetToRecPtr(extract_arg->private_data.xlogsegno, 0,
										extract_arg->startpoint);
				/* Skip over the page header */
				extract_arg->startpoint = XLogFindNextRecord(xlogreader,
															 extract_arg->startpoint);

				elog(VERBOSE, "Thread %d switched to LSN %X/%X",
					 extract_arg->thread_num,
					 (uint32) (extract_arg->startpoint >> 32),
					 (uint32) (extract_arg->startpoint));

				continue;
			}

			errptr = extract_arg->startpoint ?
				extract_arg->startpoint : xlogreader->EndRecPtr;

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
			PrintXLogCorruptionMsg(&extract_arg->private_data, ERROR);
		}

		extractPageInfo(xlogreader);

		/* continue reading at next record */
		extract_arg->startpoint = InvalidXLogRecPtr;

		XLByteToSeg(xlogreader->EndRecPtr, nextSegNo);
	} while (nextSegNo <= extract_arg->endSegNo &&
			 xlogreader->EndRecPtr < extract_arg->endpoint);

	CleanupXLogPageRead(xlogreader);
	XLogReaderFree(xlogreader);

	/* Extracting is successful */
	extract_arg->ret = 0;
	return NULL;
}

/*
 * Read WAL from the archive directory, from 'startpoint' to 'endpoint' on the
 * given timeline. Collect data blocks touched by the WAL records into a page map.
 *
 * If **prev_segno** is true then read all segments up to **endpoint** segment
 * minus one. Else read all segments up to **endpoint** segment.
 *
 * Pagemap extracting is processed using threads. Eeach thread reads single WAL
 * file.
 */
void
extractPageMap(const char *archivedir, XLogRecPtr startpoint, TimeLineID tli,
			   XLogRecPtr endpoint, bool prev_seg, parray *files)
{
	int			i;
	int			threads_need = 0;
	XLogSegNo	endSegNo;
	bool		extract_isok = true;
	pthread_t	threads[num_threads];
	xlog_thread_arg thread_args[num_threads];

	elog(LOG, "Compiling pagemap");
	if (!XRecOffIsValid(startpoint))
		elog(ERROR, "Invalid startpoint value %X/%X",
			 (uint32) (startpoint >> 32), (uint32) (startpoint));

	if (!XRecOffIsValid(endpoint))
		elog(ERROR, "Invalid endpoint value %X/%X",
			 (uint32) (endpoint >> 32), (uint32) (endpoint));

	XLByteToSeg(endpoint, endSegNo);
	if (prev_seg)
		endSegNo--;

	nextSegNoToRead = 0;

	/*
	 * Initialize thread args.
	 *
	 * Each thread works with its own WAL segment and we need to adjust
	 * startpoint value for each thread.
	 */
	for (i = 0; i < num_threads; i++)
	{
		InitXLogPageRead(&thread_args[i].private_data, archivedir, tli, false);
		thread_args[i].thread_num = i;
		thread_args[i].private_data.manual_switch = true;

		thread_args[i].startpoint = startpoint;
		thread_args[i].endpoint = endpoint;
		thread_args[i].endSegNo = endSegNo;
		/* By default there is some error */
		thread_args[i].ret = 1;

		/* Adjust startpoint to the next thread */
		if (nextSegNoToRead == 0)
			XLByteToSeg(startpoint, nextSegNoToRead);

		nextSegNoToRead++;
		/*
		 * If we need to read less WAL segments than num_threads, create less
		 * threads.
		 */
		if (nextSegNoToRead > endSegNo)
			break;
		XLogSegNoOffsetToRecPtr(nextSegNoToRead, 0, startpoint);
		/* Skip over the page header */
		startpoint += SizeOfXLogLongPHD;

		threads_need++;
	}

	/* Run threads */
	for (i = 0; i < threads_need; i++)
	{
		elog(VERBOSE, "Start WAL reader thread: %d", i);
		pthread_create(&threads[i], NULL, doExtractPageMap, &thread_args[i]);
	}

	/* Wait for threads */
	for (i = 0; i < threads_need; i++)
	{
		pthread_join(threads[i], NULL);
		if (thread_args[i].ret == 1)
			extract_isok = false;
	}
	if (extract_isok)
		elog(LOG, "Pagemap compiled");
	else
		elog(ERROR, "Pagemap compiling failed");
}

/*
 * Ensure that the backup has all wal files needed for recovery to consistent state.
 */
static void
validate_backup_wal_from_start_to_stop(pgBackup *backup,
									   char *backup_xlog_path, TimeLineID tli)
{
	XLogRecPtr	startpoint = backup->start_lsn;
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char	   *errormsg;
	XLogPageReadPrivate private;
	bool		got_endpoint = false;

	xlogreader = InitXLogPageRead(&private, backup_xlog_path, tli, true);

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
		PrintXLogCorruptionMsg(&private, WARNING);

		/*
		 * If we don't have WAL between start_lsn and stop_lsn,
		 * the backup is definitely corrupted. Update its status.
		 */
		backup->status = BACKUP_STATUS_CORRUPT;
		pgBackupWriteBackupControlFile(backup);

		elog(WARNING, "There are not enough WAL records to consistenly restore "
			"backup %s from START LSN: %X/%X to STOP LSN: %X/%X",
			 base36enc(backup->start_time),
			 (uint32) (backup->start_lsn >> 32),
			 (uint32) (backup->start_lsn),
			 (uint32) (backup->stop_lsn >> 32),
			 (uint32) (backup->stop_lsn));
	}

	/* clean */
	CleanupXLogPageRead(xlogreader);
	XLogReaderFree(xlogreader);
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
	const char *backup_id;
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
		snprintf(backup_xlog_path, sizeof(backup_xlog_path), "/%s/%s/%s/%s",
				backup_instance_path, backup_id, DATABASE_DIR, PG_XLOG_DIR);

		validate_backup_wal_from_start_to_stop(backup, backup_xlog_path, tli);
	}
	else
		validate_backup_wal_from_start_to_stop(backup, (char *) archivedir, tli);

	if (backup->status == BACKUP_STATUS_CORRUPT)
	{
		elog(WARNING, "Backup %s WAL segments are corrupted", backup_id);
		return;
	}
	/*
	 * If recovery target is provided check that we can restore backup to a
	 * recovery target time or xid.
	 */
	if (!TransactionIdIsValid(target_xid) && target_time == 0)
	{
		/* Recovery target is not given so exit */
		elog(INFO, "Backup %s WAL segments are valid", backup_id);
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
	xlogreader = InitXLogPageRead(&private, archivedir, tli, true);

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
		PrintXLogCorruptionMsg(&private, WARNING);

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
	CleanupXLogPageRead(xlogreader);
	XLogReaderFree(xlogreader);
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

	xlogreader = InitXLogPageRead(&private, archivedir, tli, true);

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
				 TimeLineID target_tli)
{
	XLogReaderState *xlogreader;
	XLogPageReadPrivate private;
	char	   *errormsg;
	bool		res;

	if (!XRecOffIsValid(target_lsn))
		elog(ERROR, "Invalid target_lsn value %X/%X",
			 (uint32) (target_lsn >> 32), (uint32) (target_lsn));

	xlogreader = InitXLogPageRead(&private, archivedir, target_tli, true);

	res = XLogReadRecord(xlogreader, target_lsn, &errormsg) != NULL;
	/* Didn't find 'target_lsn' and there is no error, return false */

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

	errmsg = gzerror(gzf, &errnum);
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
	XLogPageReadPrivate *private_data;
	uint32		targetPageOff;

	private_data = (XLogPageReadPrivate *) xlogreader->private_data;
	targetPageOff = targetPagePtr % XLogSegSize;

	/*
	 * See if we need to switch to a new segment because the requested record
	 * is not in the currently open one.
	 */
	if (!XLByteInSeg(targetPagePtr, private_data->xlogsegno))
	{
		CleanupXLogPageRead(xlogreader);
		if (private_data->manual_switch)
		{
			private_data->need_switch = true;
			return -1;
		}
	}

	XLByteToSeg(targetPagePtr, private_data->xlogsegno);

	if (!private_data->xlogexists)
	{
		char		xlogfname[MAXFNAMELEN];

		XLogFileName(xlogfname, private_data->tli, private_data->xlogsegno);
		snprintf(private_data->xlogpath, MAXPGPATH, "%s/%s",
				 private_data->archivedir, xlogfname);

		if (fileExists(private_data->xlogpath))
		{
			elog(LOG, "Opening WAL segment \"%s\"", private_data->xlogpath);

			private_data->xlogexists = true;
			private_data->xlogfile = open(private_data->xlogpath,
										  O_RDONLY | PG_BINARY, 0);

			if (private_data->xlogfile < 0)
			{
				elog(WARNING, "Could not open WAL segment \"%s\": %s",
					 private_data->xlogpath, strerror(errno));
				return -1;
			}
		}
#ifdef HAVE_LIBZ
		/* Try to open compressed WAL segment */
		else
		{
			snprintf(private_data->gz_xlogpath,
					 sizeof(private_data->gz_xlogpath), "%s.gz",
					 private_data->xlogpath);
			if (fileExists(private_data->gz_xlogpath))
			{
				elog(LOG, "Opening compressed WAL segment \"%s\"",
					 private_data->gz_xlogpath);

				private_data->xlogexists = true;
				private_data->gz_xlogfile = gzopen(private_data->gz_xlogpath,
												   "rb");
				if (private_data->gz_xlogfile == NULL)
				{
					elog(WARNING, "Could not open compressed WAL segment \"%s\": %s",
						 private_data->gz_xlogpath, strerror(errno));
					return -1;
				}
			}
		}
#endif

		/* Exit without error if WAL segment doesn't exist */
		if (!private_data->xlogexists)
			return -1;
	}

	/*
	 * At this point, we have the right segment open.
	 */
	Assert(private_data->xlogexists);

	/* Read the requested page */
	if (private_data->xlogfile != -1)
	{
		if (lseek(private_data->xlogfile, (off_t) targetPageOff, SEEK_SET) < 0)
		{
			elog(WARNING, "Could not seek in WAL segment \"%s\": %s",
				 private_data->xlogpath, strerror(errno));
			return -1;
		}

		if (read(private_data->xlogfile, readBuf, XLOG_BLCKSZ) != XLOG_BLCKSZ)
		{
			elog(WARNING, "Could not read from WAL segment \"%s\": %s",
				 private_data->xlogpath, strerror(errno));
			return -1;
		}
	}
#ifdef HAVE_LIBZ
	else
	{
		if (gzseek(private_data->gz_xlogfile, (z_off_t) targetPageOff, SEEK_SET) == -1)
		{
			elog(WARNING, "Could not seek in compressed WAL segment \"%s\": %s",
				 private_data->gz_xlogpath,
				 get_gz_error(private_data->gz_xlogfile));
			return -1;
		}

		if (gzread(private_data->gz_xlogfile, readBuf, XLOG_BLCKSZ) != XLOG_BLCKSZ)
		{
			elog(WARNING, "Could not read from compressed WAL segment \"%s\": %s",
				 private_data->gz_xlogpath,
				 get_gz_error(private_data->gz_xlogfile));
			return -1;
		}
	}
#endif

	*pageTLI = private_data->tli;
	return XLOG_BLCKSZ;
}

/*
 * Initialize WAL segments reading.
 */
static XLogReaderState *
InitXLogPageRead(XLogPageReadPrivate *private_data, const char *archivedir,
				 TimeLineID tli, bool allocate_reader)
{
	XLogReaderState *xlogreader = NULL;

	MemSet(private_data, 0, sizeof(XLogPageReadPrivate));
	private_data->archivedir = archivedir;
	private_data->tli = tli;
	private_data->xlogfile = -1;

	if (allocate_reader)
	{
		xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, private_data);
		if (xlogreader == NULL)
			elog(ERROR, "out of memory");
	}

	return xlogreader;
}

/*
 * Cleanup after WAL segment reading.
 */
static void
CleanupXLogPageRead(XLogReaderState *xlogreader)
{
	XLogPageReadPrivate *private_data;

	private_data = (XLogPageReadPrivate *) xlogreader->private_data;
	if (private_data->xlogfile >= 0)
	{
		close(private_data->xlogfile);
		private_data->xlogfile = -1;
	}
#ifdef HAVE_LIBZ
	else if (private_data->gz_xlogfile != NULL)
	{
		gzclose(private_data->gz_xlogfile);
		private_data->gz_xlogfile = NULL;
	}
#endif
	private_data->xlogexists = false;
}

static void
PrintXLogCorruptionMsg(XLogPageReadPrivate *private_data, int elevel)
{
	if (private_data->xlogpath[0] != 0)
	{
		/*
		 * XLOG reader couldn't read WAL segment.
		 * We throw a WARNING here to be able to update backup status.
		 */
		if (!private_data->xlogexists)
			elog(elevel, "WAL segment \"%s\" is absent", private_data->xlogpath);
		else if (private_data->xlogfile != -1)
			elog(elevel, "Possible WAL corruption. "
						 "Error has occured during reading WAL segment \"%s\"",
				 private_data->xlogpath);
#ifdef HAVE_LIBZ
		else if (private_data->gz_xlogpath != NULL)
			elog(elevel, "Possible WAL corruption. "
						 "Error has occured during reading WAL segment \"%s\"",
				 private_data->gz_xlogpath);
#endif
	}
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

