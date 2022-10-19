/*-------------------------------------------------------------------------
 *
 * streamutil.c - utility functions for pg_basebackup, pg_receivewal and
 *					pg_recvlogical
 *
 * Author: Magnus Hagander <magnus@hagander.net>
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_basebackup/streamutil.c
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <sys/time.h>
#include <unistd.h>

#include "common/connect.h"
#include "common/fe_memutils.h"
#include "logging.h"
#include "datatype/timestamp.h"
#include "port/pg_bswap.h"
#include "pqexpbuffer.h"
#include "receivelog.h"
#include "streamutil.h"


#define ERRCODE_DUPLICATE_OBJECT  "42710"

uint32		WalSegSz;

#include "simple_prompt.h"
#include "file_compat.h"


/*
 * From version 10, explicitly set wal segment size using SHOW wal_segment_size
 * since ControlFile is not accessible here.
 */
bool
RetrieveWalSegSize(PGconn *conn)
{
	PGresult   *res;
	char		xlog_unit[3];
	int			xlog_val,
				multiplier = 1;

	/* check connection existence */
	Assert(conn != NULL);

	res = PQexec(conn, "SHOW wal_segment_size");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		pg_log_error("could not send replication command \"%s\": %s",
					 "SHOW wal_segment_size", PQerrorMessage(conn));

		PQclear(res);
		return false;
	}
	if (PQntuples(res) != 1 || PQnfields(res) < 1)
	{
		pg_log_error("could not fetch WAL segment size: got %d rows and %d fields, expected %d rows and %d or more fields",
					 PQntuples(res), PQnfields(res), 1, 1);

		PQclear(res);
		return false;
	}

	/* fetch xlog value and unit from the result */
	if (sscanf(PQgetvalue(res, 0, 0), "%d%2s", &xlog_val, xlog_unit) != 2)
	{
		pg_log_error("WAL segment size could not be parsed");
		PQclear(res);
		return false;
	}

	PQclear(res);
	xlog_unit[2] = 0;
	/* set the multiplier based on unit to convert xlog_val to bytes */
	if (strcmp(xlog_unit, "MB") == 0)
		multiplier = 1024 * 1024;
	else if (strcmp(xlog_unit, "GB") == 0)
		multiplier = 1024 * 1024 * 1024;

	/* convert and set WalSegSz */
	WalSegSz = xlog_val * multiplier;

	if (!IsValidWalSegSize(WalSegSz))
	{
		pg_log_error(ngettext("WAL segment size must be a power of two between 1 MB and 1 GB, but the remote server reported a value of %d byte",
							  "WAL segment size must be a power of two between 1 MB and 1 GB, but the remote server reported a value of %d bytes",
							  WalSegSz),
					 WalSegSz);
		return false;
	}

	return true;
}


/*
 * Create a replication slot for the given connection. This function
 * returns true in case of success.
 */
bool
CreateReplicationSlot(PGconn *conn, const char *slot_name, const char *plugin,
					  bool is_temporary, bool is_physical, bool reserve_wal,
					  bool slot_exists_ok)
{
	PQExpBuffer query;
	PGresult   *res;

	query = createPQExpBuffer();

	Assert((is_physical && plugin == NULL) ||
		   (!is_physical && plugin != NULL));
	Assert(slot_name != NULL);

	/* Build query */
	appendPQExpBuffer(query, "CREATE_REPLICATION_SLOT \"%s\"", slot_name);
	if (is_temporary)
		appendPQExpBufferStr(query, " TEMPORARY");
	if (is_physical)
	{
		appendPQExpBufferStr(query, " PHYSICAL");
		if (reserve_wal)
			appendPQExpBufferStr(query, " RESERVE_WAL");
	}
	else
	{
		appendPQExpBuffer(query, " LOGICAL \"%s\"", plugin);
		if (PQserverVersion(conn) >= 100000)
			/* pg_recvlogical doesn't use an exported snapshot, so suppress */
			appendPQExpBufferStr(query, " NOEXPORT_SNAPSHOT");
	}

	res = PQexec(conn, query->data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);

		if (slot_exists_ok &&
			sqlstate &&
			strcmp(sqlstate, ERRCODE_DUPLICATE_OBJECT) == 0)
		{
			destroyPQExpBuffer(query);
			PQclear(res);
			return true;
		}
		else
		{
			pg_log_error("could not send replication command \"%s\": %s",
						 query->data, PQerrorMessage(conn));

			destroyPQExpBuffer(query);
			PQclear(res);
			return false;
		}
	}

	if (PQntuples(res) != 1 || PQnfields(res) != 4)
	{
		pg_log_error("could not create replication slot \"%s\": got %d rows and %d fields, expected %d rows and %d fields",
					 slot_name,
					 PQntuples(res), PQnfields(res), 1, 4);

		destroyPQExpBuffer(query);
		PQclear(res);
		return false;
	}

	destroyPQExpBuffer(query);
	PQclear(res);
	return true;
}

/*
 * Frontend version of GetCurrentTimestamp(), since we are not linked with
 * backend code.
 */
TimestampTz
feGetCurrentTimestamp(void)
{
	TimestampTz result;
	struct timeval tp;

	gettimeofday(&tp, NULL);

	result = (TimestampTz) tp.tv_sec -
		((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);
	result = (result * USECS_PER_SEC) + tp.tv_usec;

	return result;
}

/*
 * Frontend version of TimestampDifference(), since we are not linked with
 * backend code.
 */
void
feTimestampDifference(TimestampTz start_time, TimestampTz stop_time,
					  long *secs, int *microsecs)
{
	TimestampTz diff = stop_time - start_time;

	if (diff <= 0)
	{
		*secs = 0;
		*microsecs = 0;
	}
	else
	{
		*secs = (long) (diff / USECS_PER_SEC);
		*microsecs = (int) (diff % USECS_PER_SEC);
	}
}

/*
 * Frontend version of TimestampDifferenceExceeds(), since we are not
 * linked with backend code.
 */
bool
feTimestampDifferenceExceeds(TimestampTz start_time,
							 TimestampTz stop_time,
							 int msec)
{
	TimestampTz diff = stop_time - start_time;

	return (diff >= msec * INT64CONST(1000));
}

/*
 * Converts an int64 to network byte order.
 */

void
fe_sendint64(int64 i, char *buf)
{

	uint32		n32;

	/* High order half first, since we're doing MSB-first */
	n32 = (uint32) (i >> 32);
	n32 = htonl(n32);
	memcpy(&buf[0], &n32, 4);

	/* Now the low order half */
	n32 = (uint32) i;
	n32 = htonl(n32);
	memcpy(&buf[4], &n32, 4);
}

/*
 * Converts an int64 from network byte order to native format.
 */

int64
fe_recvint64(char *buf)
{
	int64		result;
	uint32		h32;
	uint32		l32;

	memcpy(&h32, buf, 4);
	memcpy(&l32, buf + 4, 4);
	h32 = ntohl(h32);
	l32 = ntohl(l32);

	result = h32;
	result <<= 32;
	result |= l32;

	return result;

}



