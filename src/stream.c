/*-------------------------------------------------------------------------
 *
 * stream.c: pg_probackup specific code for WAL streaming
 *
 * Portions Copyright (c) 2015-2020, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"
#include "receivelog.h"
#include "streamutil.h"

#include <time.h>
#include <unistd.h>

/*
 * global variable needed by ReceiveXlogStream()
 *
 * standby_message_timeout controls how often we send a message
 * back to the primary letting it know our progress, in milliseconds.
 *
 * in pg_probackup we use a default setting = 10 sec
 */
static int	standby_message_timeout = 10 * 1000;

/* stop_backup_lsn is set by pg_stop_backup() to stop streaming */
XLogRecPtr stop_backup_lsn = InvalidXLogRecPtr;
static XLogRecPtr stop_stream_lsn = InvalidXLogRecPtr;

/*
 * How long we should wait for streaming end in seconds.
 * Retrieved as checkpoint_timeout + checkpoint_timeout * 0.1
 */
static uint32 stream_stop_timeout = 0;
/* Time in which we started to wait for streaming end */
static time_t stream_stop_begin = 0;

/*
 * We need to wait end of WAL streaming before execute pg_stop_backup().
 */
typedef struct
{
	const char *basedir;
	PGconn	   *conn;

	/*
	 * Return value from the thread.
	 * 0 means there is no error, 1 - there is an error.
	 */
	int			ret;

	XLogRecPtr	startpos;
	TimeLineID	starttli;
} StreamThreadArg;

static pthread_t stream_thread;
static StreamThreadArg stream_thread_arg = {"", NULL, 1};

static parray *xlog_files_list = NULL;

static void IdentifySystem(StreamThreadArg *stream_thread_arg);
static int checkpoint_timeout(PGconn *backup_conn);
static void *StreamLog(void *arg);
static bool stop_streaming(XLogRecPtr xlogpos, uint32 timeline,
                           bool segment_finished);
static void add_walsegment_to_filelist(parray *filelist, uint32 timeline,
                                       XLogRecPtr xlogpos, char *basedir,
                                       uint32 xlog_seg_size);
static void add_history_file_to_filelist(parray *filelist, uint32 timeline,
										 char *basedir);

/*
 * Run IDENTIFY_SYSTEM through a given connection and
 * check system identifier and timeline are matching
 */
static void
IdentifySystem(StreamThreadArg *stream_thread_arg)
{
	PGresult	*res;

	uint64 stream_conn_sysidentifier = 0;
	char *stream_conn_sysidentifier_str;
	TimeLineID stream_conn_tli = 0;

	if (!CheckServerVersionForStreaming(stream_thread_arg->conn))
	{
		PQfinish(stream_thread_arg->conn);
		/*
		 * Error message already written in CheckServerVersionForStreaming().
		 * There's no hope of recovering from a version mismatch, so don't
		 * retry.
		 */
		elog(ERROR, "Cannot continue backup because stream connect has failed.");
	}

	/*
	 * Identify server, obtain server system identifier and timeline
	 */
	res = pgut_execute(stream_thread_arg->conn, "IDENTIFY_SYSTEM", 0, NULL);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		elog(WARNING,"Could not send replication command \"%s\": %s",
						"IDENTIFY_SYSTEM", PQerrorMessage(stream_thread_arg->conn));
		PQfinish(stream_thread_arg->conn);
		elog(ERROR, "Cannot continue backup because stream connect has failed.");
	}

	stream_conn_sysidentifier_str = PQgetvalue(res, 0, 0);
	stream_conn_tli = atoll(PQgetvalue(res, 0, 1));

	/* Additional sanity, primary for PG 9.5,
	 * where system id can be obtained only via "IDENTIFY SYSTEM"
	 */
	if (!parse_uint64(stream_conn_sysidentifier_str, &stream_conn_sysidentifier, 0))
		elog(ERROR, "%s is not system_identifier", stream_conn_sysidentifier_str);

	if (stream_conn_sysidentifier != instance_config.system_identifier)
		elog(ERROR, "System identifier mismatch. Connected PostgreSQL instance has system id: "
			"" UINT64_FORMAT ". Expected: " UINT64_FORMAT ".",
					stream_conn_sysidentifier, instance_config.system_identifier);

	if (stream_conn_tli != current.tli)
		elog(ERROR, "Timeline identifier mismatch. "
			"Connected PostgreSQL instance has timeline id: %X. Expected: %X.",
			stream_conn_tli, current.tli);

	PQclear(res);
}

/*
 * Retrieve checkpoint_timeout GUC value in seconds.
 */
static int
checkpoint_timeout(PGconn *backup_conn)
{
	PGresult   *res;
	const char *val;
	const char *hintmsg;
	int			val_int;

	res = pgut_execute(backup_conn, "show checkpoint_timeout", 0, NULL);
	val = PQgetvalue(res, 0, 0);

	if (!parse_int(val, &val_int, OPTION_UNIT_S, &hintmsg))
	{
		PQclear(res);
		if (hintmsg)
			elog(ERROR, "Invalid value of checkout_timeout %s: %s", val,
				 hintmsg);
		else
			elog(ERROR, "Invalid value of checkout_timeout %s", val);
	}

	PQclear(res);

	return val_int;
}

/*
 * Start the log streaming
 */
static void *
StreamLog(void *arg)
{
	StreamThreadArg *stream_arg = (StreamThreadArg *) arg;

	/*
	 * Always start streaming at the beginning of a segment
	 */
	stream_arg->startpos -= stream_arg->startpos % instance_config.xlog_seg_size;

    xlog_files_list = parray_new();

	/* Initialize timeout */
	stream_stop_begin = 0;

#if PG_VERSION_NUM >= 100000
	/* if slot name was not provided for temp slot, use default slot name */
	if (!replication_slot && temp_slot)
		replication_slot = "pg_probackup_slot";
#endif


#if PG_VERSION_NUM >= 110000
	/* Create temp repslot */
	if (temp_slot)
		CreateReplicationSlot(stream_arg->conn, replication_slot,
			NULL, temp_slot, true, true, false);
#endif

	/*
	 * Start the replication
	 */
	elog(LOG, "started streaming WAL at %X/%X (timeline %u)",
		 (uint32) (stream_arg->startpos >> 32), (uint32) stream_arg->startpos,
		  stream_arg->starttli);

#if PG_VERSION_NUM >= 90600
	{
		StreamCtl	ctl;

		MemSet(&ctl, 0, sizeof(ctl));

		ctl.startpos = stream_arg->startpos;
		ctl.timeline = stream_arg->starttli;
		ctl.sysidentifier = NULL;

#if PG_VERSION_NUM >= 100000
		ctl.walmethod = CreateWalDirectoryMethod(
			stream_arg->basedir,
//			(instance_config.compress_alg == NONE_COMPRESS) ? 0 : instance_config.compress_level,
			0,
			false);
		ctl.replication_slot = replication_slot;
		ctl.stop_socket = PGINVALID_SOCKET;
		ctl.do_sync = false; /* We sync all files at the end of backup */
//		ctl.mark_done        /* for future use in s3 */
#if PG_VERSION_NUM >= 100000 && PG_VERSION_NUM < 110000
		ctl.temp_slot = temp_slot;
#endif
#else
		ctl.basedir = (char *) stream_arg->basedir;
#endif

		ctl.stream_stop = stop_streaming;
		ctl.standby_message_timeout = standby_message_timeout;
		ctl.partial_suffix = NULL;
		ctl.synchronous = false;
		ctl.mark_done = false;

		if(ReceiveXlogStream(stream_arg->conn, &ctl) == false)
			elog(ERROR, "Problem in receivexlog");

#if PG_VERSION_NUM >= 100000
		if (!ctl.walmethod->finish())
			elog(ERROR, "Could not finish writing WAL files: %s",
				 strerror(errno));
#endif
	}
#else
	if(ReceiveXlogStream(stream_arg->conn, stream_arg->startpos, stream_arg->starttli,
						NULL, (char *) stream_arg->basedir, stop_streaming,
						standby_message_timeout, NULL, false, false) == false)
		elog(ERROR, "Problem in receivexlog");
#endif

    /* be paranoid and sort xlog_files_list,
     * so if stop_lsn segno is already in the list,
     * then list must be sorted to detect duplicates.
     */
    parray_qsort(xlog_files_list, pgFileCompareRelPathWithExternal);

    /* Add the last segment to the list */
    add_walsegment_to_filelist(xlog_files_list, stream_arg->starttli,
                               stop_stream_lsn, (char *) stream_arg->basedir,
                               instance_config.xlog_seg_size);

    /* append history file to walsegment filelist */
    add_history_file_to_filelist(xlog_files_list, stream_arg->starttli, (char *) stream_arg->basedir);

    /*
     * TODO: remove redundant WAL segments
     * walk pg_wal and remove files with segno greater that of stop_lsn`s segno +1
     */

	elog(LOG, "finished streaming WAL at %X/%X (timeline %u)",
		 (uint32) (stop_stream_lsn >> 32), (uint32) stop_stream_lsn, stream_arg->starttli);
	stream_arg->ret = 0;

	PQfinish(stream_arg->conn);
	stream_arg->conn = NULL;

	return NULL;
}

/*
 * for ReceiveXlogStream
 *
 * The stream_stop callback will be called every time data
 * is received, and whenever a segment is completed. If it returns
 * true, the streaming will stop and the function
 * return. As long as it returns false, streaming will continue
 * indefinitely.
 *
 * Stop WAL streaming if current 'xlogpos' exceeds 'stop_backup_lsn', which is
 * set by pg_stop_backup().
 *
 */
static bool
stop_streaming(XLogRecPtr xlogpos, uint32 timeline, bool segment_finished)
{
	static uint32 prevtimeline = 0;
	static XLogRecPtr prevpos = InvalidXLogRecPtr;

	/* check for interrupt */
	if (interrupted || thread_interrupted)
		elog(ERROR, "Interrupted during WAL streaming");

	/* we assume that we get called once at the end of each segment */
	if (segment_finished)
    {
        elog(VERBOSE, _("finished segment at %X/%X (timeline %u)"),
             (uint32) (xlogpos >> 32), (uint32) xlogpos, timeline);

        add_walsegment_to_filelist(xlog_files_list, timeline, xlogpos,
                                   (char*) stream_thread_arg.basedir,
                                   instance_config.xlog_seg_size);
    }

	/*
	 * Note that we report the previous, not current, position here. After a
	 * timeline switch, xlogpos points to the beginning of the segment because
	 * that's where we always begin streaming. Reporting the end of previous
	 * timeline isn't totally accurate, because the next timeline can begin
	 * slightly before the end of the WAL that we received on the previous
	 * timeline, but it's close enough for reporting purposes.
	 */
	if (prevtimeline != 0 && prevtimeline != timeline)
		elog(LOG, _("switched to timeline %u at %X/%X\n"),
			 timeline, (uint32) (prevpos >> 32), (uint32) prevpos);

	if (!XLogRecPtrIsInvalid(stop_backup_lsn))
	{
		if (xlogpos >= stop_backup_lsn)
		{
			stop_stream_lsn = xlogpos;
			return true;
		}

		/* pg_stop_backup() was executed, wait for the completion of stream */
		if (stream_stop_begin == 0)
		{
			elog(INFO, "Wait for LSN %X/%X to be streamed",
				 (uint32) (stop_backup_lsn >> 32), (uint32) stop_backup_lsn);

			stream_stop_begin = time(NULL);
		}

		if (time(NULL) - stream_stop_begin > stream_stop_timeout)
			elog(ERROR, "Target LSN %X/%X could not be streamed in %d seconds",
				 (uint32) (stop_backup_lsn >> 32), (uint32) stop_backup_lsn,
				 stream_stop_timeout);
	}

	prevtimeline = timeline;
	prevpos = xlogpos;

	return false;
}


/* --- External API --- */

/*
 * Maybe add a StreamOptions struct ?
 * Backup conn only needed to calculate stream_stop_timeout. Think about refactoring it.
 */
void
start_WAL_streaming(PGconn *backup_conn, char *stream_dst_path, ConnectionOptions *conn_opt,
					XLogRecPtr startpos, TimeLineID starttli)
{
	/* How long we should wait for streaming end after pg_stop_backup */
	stream_stop_timeout = checkpoint_timeout(backup_conn);
	//TODO Add a comment about this calculation
	stream_stop_timeout = stream_stop_timeout + stream_stop_timeout * 0.1;

	stream_thread_arg.basedir = stream_dst_path;

	/*
	 * Connect in replication mode to the server.
	 */
	stream_thread_arg.conn = pgut_connect_replication(conn_opt->pghost,
													  conn_opt->pgport,
													  conn_opt->pgdatabase,
													  conn_opt->pguser);
	/* sanity check*/
	IdentifySystem(&stream_thread_arg);

	/* Set error exit code as default */
	stream_thread_arg.ret = 1;
	/* we must use startpos as start_lsn from start_backup */
	stream_thread_arg.startpos = current.start_lsn;
	stream_thread_arg.starttli = current.tli;

	thread_interrupted = false;
	pthread_create(&stream_thread, NULL, StreamLog, &stream_thread_arg);
}

/* Wait for the completion of stream */
int
wait_WAL_streaming_end(parray *backup_files_list)
{
    pthread_join(stream_thread, NULL);

    parray_concat(backup_files_list, xlog_files_list);
    parray_free(xlog_files_list);
    return stream_thread_arg.ret;
}

/* Append streamed WAL segment to filelist  */
void
add_walsegment_to_filelist(parray *filelist, uint32 timeline, XLogRecPtr xlogpos, char *basedir, uint32 xlog_seg_size)
{
    XLogSegNo xlog_segno;
    char wal_segment_name[MAXFNAMELEN];
    char wal_segment_relpath[MAXPGPATH];
    char wal_segment_fullpath[MAXPGPATH];
    pgFile *file = NULL;
    pgFile **existing_file = NULL;

    GetXLogSegNo(xlogpos, xlog_segno, xlog_seg_size);

    /*
     * When xlogpos points to the zero offset (0/3000000),
     * it means that previous segment was just successfully streamed.
     * When xlogpos points to the positive offset,
     * then current segment is successfully streamed.
     */
    if (WalSegmentOffset(xlogpos, xlog_seg_size) == 0)
        xlog_segno--;

    GetXLogFileName(wal_segment_name, timeline, xlog_segno, xlog_seg_size);

    join_path_components(wal_segment_fullpath, basedir, wal_segment_name);
    join_path_components(wal_segment_relpath, PG_XLOG_DIR, wal_segment_name);

    file = pgFileNew(wal_segment_fullpath, wal_segment_relpath, false, 0, FIO_BACKUP_HOST);

    /*
     * Check if file is already in the list
     * stop_lsn segment can be added to this list twice, so
     * try not to add duplicates
     */

    existing_file = (pgFile **) parray_bsearch(filelist, file, pgFileCompareRelPathWithExternal);

    if (existing_file)
    {
        (*existing_file)->crc = pgFileGetCRC(wal_segment_fullpath, true, false);
        (*existing_file)->write_size = xlog_seg_size;
        (*existing_file)->uncompressed_size = xlog_seg_size;

        return;
    }

    /* calculate crc */
    file->crc = pgFileGetCRC(wal_segment_fullpath, true, false);

    /* Should we recheck it using stat? */
    file->write_size = xlog_seg_size;
    file->uncompressed_size = xlog_seg_size;

    /* append file to filelist */
    parray_append(filelist, file);
}

/* Append history file to filelist  */
void
add_history_file_to_filelist(parray *filelist, uint32 timeline, char *basedir)
{
    char filename[MAXFNAMELEN];
    char fullpath[MAXPGPATH];
    char relpath[MAXPGPATH];
    pgFile *file = NULL;

    /* Timeline 1 does not have a history file */
    if (timeline == 1)
        return;

    snprintf(filename, lengthof(filename), "%08X.history", timeline);
    join_path_components(fullpath, basedir, filename);
    join_path_components(relpath, PG_XLOG_DIR, filename);

    file = pgFileNew(fullpath, relpath, false, 0, FIO_BACKUP_HOST);

    /* calculate crc */
    file->crc = pgFileGetCRC(fullpath, true, false);
    file->write_size = file->size;
    file->uncompressed_size = file->size;

    /* append file to filelist */
    parray_append(filelist, file);
}
