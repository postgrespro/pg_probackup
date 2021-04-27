#include "pg_probackup.h"

#include <time.h>
#include <unistd.h>
//#include "postgres_fe.h"
//#include "libpq-fe.h"
//#include "access/xlog_internal.h"
//#include "common/file_utils.h"
#include "streamutil.h"
//#include "receivelog.h"
#include "streamer.h"

typedef struct
{
	int location;
	bool compression;
	int fd;
} pgWalFile;

/* fd and filename for currently open WAL file */
static pgWalFile walfile;
//static char current_walfile_name[MAXPGPATH] = "";
static bool reportFlushPosition = false;
static XLogRecPtr lastFlushPosition = InvalidXLogRecPtr;

static bool still_sending = true;	/* feedback still needs to be sent? */
static bool HistoryFileDone = false;

/*
 * Send a Standby Status Update message to server.
 */
static bool
sendFeedback(PGconn *conn, XLogRecPtr blockpos, TimestampTz now, bool replyRequested)
{
	char		replybuf[1 + 8 + 8 + 8 + 8 + 1];
	int			len = 0;

	replybuf[len] = 'r';
	len += 1;
	fe_sendint64(blockpos, &replybuf[len]); /* write */
	len += 8;
	if (reportFlushPosition)
		fe_sendint64(lastFlushPosition, &replybuf[len]);	/* flush */
	else
		fe_sendint64(InvalidXLogRecPtr, &replybuf[len]);	/* flush */
	len += 8;
	fe_sendint64(InvalidXLogRecPtr, &replybuf[len]);	/* apply */
	len += 8;
	fe_sendint64(now, &replybuf[len]);	/* sendTime */
	len += 8;
	replybuf[len] = replyRequested ? 1 : 0; /* replyRequested */
	len += 1;

	if (PQputCopyData(conn, replybuf, len) <= 0 || PQflush(conn))
	{
		elog(WARNING, "Could not send feedback packet: %s", PQerrorMessage(conn));
		return false;
	}

	return true;
}

static bool
writeTimeLineHistoryFile(XlogStreamCtl *ctl, char *filename, char *content)
{
	int     fd;
	int     size = strlen(content);
	char    histfname[MAXFNAMELEN];
	char    histfpath[MAXPGPATH];

	/*
	 * Check that the server's idea of how timeline history files should be
	 * named matches ours.
	 */
	TLHistoryFileName(histfname, ctl->timeline);
	if (strcmp(histfname, filename) != 0)
	{
		elog(WARNING, "Server reported unexpected history file name for timeline %u: %s",
				ctl->timeline, filename);
		return false;
	}

	join_components(histfpath, ctl->basedir, histfname);

	fd = ctl->open_for_write(histfname, ".tmp", 0, ctl);
	if (f == NULL)
	{
		fprintf(stderr, _("%s: could not create timeline history file \"%s\": %s\n"),
				progname, histfname, stream->walmethod->getlasterror());
		return false;
	}

	if ((int) stream->walmethod->write(f, content, size) != size)
	{
		fprintf(stderr, _("%s: could not write timeline history file \"%s\": %s\n"),
				progname, histfname, stream->walmethod->getlasterror());

		/*
		 * If we fail to make the file, delete it to release disk space
		 */
		stream->walmethod->close(f, CLOSE_UNLINK);

		return false;
	}

	if (stream->walmethod->close(f, CLOSE_NORMAL) != 0)
	{
		fprintf(stderr, _("%s: could not close file \"%s\": %s\n"),
				progname, histfname, stream->walmethod->getlasterror());
		return false;
	}

	/* Maintain archive_status, check close_walfile() for details. */
	if (stream->mark_done)
	{
		/* writes error message if failed */
		if (!mark_file_as_archived(stream, histfname))
			return false;
	}

	return true;
}

/*
 * The main loop. Handles the COPY stream after
 * initiating streaming with the START_REPLICATION command.
 *
 * If the COPY ends (not necessarily successfully) due a message from the
 * server, returns a PGresult and sets *stoppos to the last byte written.
 * On any other sort of error, returns NULL.
 */
static PGresult *
CopyStream(PGconn *conn, StreamCtl *stream, XLogRecPtr *stoppos)
{
	char	   *copybuf = NULL;
	TimestampTz last_status = -1;
	XLogRecPtr	blockpos = stream->startpos;

	still_sending = true;

	while (1)
	{
		int			r;
		TimestampTz now;
		long		sleeptime;

		/*
		 * Check if we should continue streaming, or abort at this point.
		 */
//		if (!CheckCopyStreamStop(conn, stream, blockpos, stoppos))
//			goto error;

		now = feGetCurrentTimestamp();

		/*
		 * Send feedback so that the server sees the latest WAL locations
		 * immediately.
		 */
		if (lastFlushPosition < blockpos && walfile.fd != 0)
		{
            lastFlushPosition = blockpos;
			if (!sendFeedback(conn, blockpos, now, false))
				goto error;
			last_status = now;
		}

		/*
		 * Potentially send a status message to the master
		 */
		if (still_sending && stream->standby_message_timeout > 0 &&
			feTimestampDifferenceExceeds(last_status, now,
										 stream->standby_message_timeout))
		{
			/* Time to send feedback! */
			if (!sendFeedback(conn, blockpos, now, false))
				goto error;
			last_status = now;
		}

//		/*
//		 * Calculate how long send/receive loops should sleep
//		 */
//		sleeptime = CalculateCopyStreamSleeptime(now, stream->standby_message_timeout,
//												 last_status);

//		r = CopyStreamReceiveNew(conn, sleeptime, stream->stop_socket, &copybuf);
		while (r != 0)
		{
			if (r == -1)
				goto error;
//			if (r == -2)
//			{
//				PGresult   *res = EndOfCopyStream(conn, stream, copybuf, blockpos, stoppos);
//
//				if (res == NULL)
//					goto error;
//				else
//					return res;
//			}

			/* Check the message type. */
//			if (copybuf[0] == 'k')
//			{
//				if (!ProcessKeepaliveMsg(conn, stream, copybuf, r, blockpos,
//										 &last_status))
//					goto error;
//			}
			if (copybuf[0] == 'w')
			{
                /* the most important part */
//				if (!ProcessXLogDataMsg(conn, stream, copybuf, r, &blockpos))
//					goto error;

				/*
				 * Check if we should continue streaming, or abort at this
				 * point.
				 */
//				if (!CheckCopyStreamStop(conn, stream, blockpos, stoppos))
//					goto error;
			}
			else
			{
				elog(WARNING, "Unrecognized streaming header: \"%c\"", copybuf[0]);
				goto error;
			}

			/*
			 * Process the received data, and any subsequent data we can read
			 * without blocking.
			 */
//			r = CopyStreamReceiveNew(conn, 0, stream->stop_socket, &copybuf);
		}
	}

error:
	if (copybuf != NULL)
		PQfreemem(copybuf);
	return NULL;
}


static bool
StartStream(PGconn *conn, StreamCtl *stream)
{
    char		query[128];
	char		slotcmd[128];
	PGresult   *res;
	XLogRecPtr	stoppos;

	/*
	 * Decide whether we want to report the flush position. If we report the
	 * flush position, the primary will know what WAL we'll possibly
	 * re-request, and it can then remove older WAL safely. We must always do
	 * that when we are using slots.
	 *
	 * Reporting the flush position makes one eligible as a synchronous
	 * replica. People shouldn't include generic names in
	 * synchronous_standby_names, but we've protected them against it so far,
	 * so let's continue to do so unless specifically requested.
	 */
	if (stream->replication_slot != NULL)
	{
		reportFlushPosition = true;
		sprintf(slotcmd, "SLOT \"%s\" ", stream->replication_slot);
	}
	else
	{
		if (stream->synchronous)
			reportFlushPosition = true;
		else
			reportFlushPosition = false;
		slotcmd[0] = 0;
	}

	if (stream->sysidentifier != NULL)
	{
		/* Validate system identifier hasn't changed */
		res = PQexec(conn, "IDENTIFY_SYSTEM");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			elog(WARNING, "Could not send replication command \"%s\": %s",
					"IDENTIFY_SYSTEM", PQerrorMessage(conn));
			PQclear(res);
			return false;
		}
		if (PQntuples(res) != 1 || PQnfields(res) < 3)
		{
			elog(WARNING, "Could not identify system: got %d rows and %d fields, "
                    "expected %d rows and %d or more fields",
					PQntuples(res), PQnfields(res), 1, 3);
			PQclear(res);
			return false;
		}
		if (strcmp(stream->sysidentifier, PQgetvalue(res, 0, 0)) != 0)
		{
			elog(WARNING, "System identifier does not match between base backup and streaming connection");
			PQclear(res);
			return false;
		}
		if (stream->timeline > atoi(PQgetvalue(res, 0, 1)))
		{
			elog(WARNING, "Starting timeline %u is not present in the server", stream->timeline);
			PQclear(res);
			return false;
		}
		PQclear(res);
	}

	/*
	 * initialize flush position to starting point, it's the caller's
	 * responsibility that that's sane.
	 */
	lastFlushPosition = stream->startpos;

	while (1)
	{
		/* Fetch the timeline history file for this timeline */
		if (!HistoryFileDone && stream->timeline != 1)
		{
			snprintf(query, sizeof(query), "TIMELINE_HISTORY %u", stream->timeline);
			res = PQexec(conn, query);
			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				/* FIXME: we might send it ok, but get an error */
				elog(WARNING, "Could not send replication command \"%s\": %s",
						"TIMELINE_HISTORY", PQresultErrorMessage(res));
				PQclear(res);
				return false;
			}

			/*
			 * The response to TIMELINE_HISTORY is a single row result set
			 * with two fields: filename and content
			 */
			if (PQnfields(res) != 2 || PQntuples(res) != 1)
			{
				elog(WARNING, "Unexpected response to TIMELINE_HISTORY command: "
							"got %d rows and %d fields, expected %d rows and %d fields",
						PQntuples(res), PQnfields(res), 1, 2);
			}

			/* Write the history file to disk */
//			writeTimeLineHistoryFile(stream,
//									 PQgetvalue(res, 0, 0),
//									 PQgetvalue(res, 0, 1));
//
			PQclear(res);
		}

		/* Initiate the replication stream at specified location */
		snprintf(query, sizeof(query), "START_REPLICATION %s%X/%X TIMELINE %u",
				 slotcmd,
				 (uint32) (stream->startpos >> 32), (uint32) stream->startpos,
				 stream->timeline);
		res = PQexec(conn, query);
		if (PQresultStatus(res) != PGRES_COPY_BOTH)
		{
			elog(WARNING, "Could not send replication command \"%s\": %s",
					"START_REPLICATION", PQresultErrorMessage(res));
			PQclear(res);
			return false;
		}
		PQclear(res);

		/* Stream the WAL */
		res = CopyStream(conn, stream, &stoppos);
		if (res == NULL)
			goto error;

		/*
		 * Streaming finished.
		 *
		 * There are two possible reasons for that: a controlled shutdown, or
		 * we reached the end of the current timeline. In case of
		 * end-of-timeline, the server sends a result set after Copy has
		 * finished, containing information about the next timeline. Read
		 * that, and restart streaming from the next timeline. In case of
		 * controlled shutdown, stop here.
		 */
		if (PQresultStatus(res) == PGRES_COMMAND_OK)
		{
			PQclear(res);

			/*
			 * End of replication (ie. controlled shut down of the server).
			 *
			 * Check if the callback thinks it's OK to stop here. If not,
			 * complain.
			 */
			elog(WARNING, "Replication stream was terminated before stop point");
			goto error;
		}
		else
		{
			/* Server returned an error. */
			elog(WARNING, "Unexpected termination of replication stream: %s", PQresultErrorMessage(res));
			PQclear(res);
			goto error;
		}
	}

error:
//	if (walfile != NULL && stream->walmethod->close(walfile, CLOSE_NO_RENAME) != 0)
//		fprintf(stderr, _("%s: could not close file \"%s\": %s\n"),
//				progname, current_walfile_name, stream->walmethod->getlasterror());
	walfile.fd = 0;
	return false;
}

/*
 * Handle end of the copy stream.
 */
//static PGresult *
//EndOfCopyStream(PGconn *conn, StreamCtl *stream, char *copybuf,
//					  XLogRecPtr blockpos, XLogRecPtr *stoppos)
//{
//	PGresult   *res = PQgetResult(conn);
//
//	/*
//	 * The server closed its end of the copy stream.  If we haven't closed
//	 * ours already, we need to do so now, unless the server threw an error,
//	 * in which case we don't.
//	 */
//	if (still_sending)
//	{
//		if (!close_walfile(stream, blockpos))
//		{
//			/* Error message written in close_walfile() */
//			PQclear(res);
//			return NULL;
//		}
//		if (PQresultStatus(res) == PGRES_COPY_IN)
//		{
//			if (PQputCopyEnd(conn, NULL) <= 0 || PQflush(conn))
//			{
//				fprintf(stderr,
//						_("%s: could not send copy-end packet: %s"),
//						progname, PQerrorMessage(conn));
//				PQclear(res);
//				return NULL;
//			}
//			res = PQgetResult(conn);
//		}
//		still_sending = false;
//	}
//	if (copybuf != NULL)
//		PQfreemem(copybuf);
//	*stoppos = blockpos;
//	return res;
//}

/*
 * Receive CopyData message available from XLOG stream, blocking for
 * maximum of 'timeout' ms.
 *
 * If data was received, returns the length of the data. *buffer is set to
 * point to a buffer holding the received message. The buffer is only valid
 * until the next CopyStreamReceive call.
 *
 * Returns 0 if no data was available within timeout, or if wait was
 * interrupted by signal or stop_socket input.
 * -1 on error. -2 if the server ended the COPY.
 */
//CopyStreamReceiveNew(PGconn *conn, long timeout, pgsocket stop_socket,
//static int
//				  char **buffer)
//{
//	char	   *copybuf = NULL;
//	int			rawlen;
//	if (*buffer != NULL)
//		PQfreemem(*buffer);
//
//	*buffer = NULL;
//	/* Try to receive a CopyData message */
//	rawlen = PQgetCopyData(conn, &copybuf, 1);
//
//	if (rawlen == 0)
//	{
//		int			ret;
//		/*
//		 * No data available.  Wait for some to appear, but not longer than
//
//		 * the specified timeout, so that we can ping the server.  Also stop
//		 * waiting if input appears on stop_socket.
//		 */
//		ret = CopyStreamPoll(conn, timeout, stop_socket);
//		if (ret <= 0)
//			return ret;
//		/* Now there is actually data on the socket */
//		if (PQconsumeInput(conn) == 0)
//
//		{
//			elog(WARNING, "Could not receive data from WAL stream: %s", PQerrorMessage(conn));
//			return -1;
//		}
//
//		/* Now that we've consumed some input, try again */
//		rawlen = PQgetCopyData(conn, &copybuf, 1);
//		if (rawlen == 0)
//			return 0;
//	}
//	if (rawlen == -1)			/* end-of-streaming or error */
//		return -2;
//	if (rawlen == -2)
//	{
//		elog(WARNING, "Could not read COPY data: %s", PQerrorMessage(conn));
//		return -1;
//	}
//
//	/* Return received messages to caller */
//	*buffer = copybuf;
//	return rawlen;
//}


/* PUBLIC API */ 
void
RunStream(PGconn *conn, StreamCtl *stream)
{
    StartStream(conn, stream);
}

