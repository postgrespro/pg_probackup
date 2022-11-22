/*-------------------------------------------------------------------------
 *
 * streamutil.h
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_basebackup/streamutil.h
 *-------------------------------------------------------------------------
 */

#ifndef STREAMUTIL_H
#define STREAMUTIL_H

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "libpq-fe.h"

extern uint32 WalSegSz;

/* Replication commands */
extern bool CreateReplicationSlot(PGconn *conn, const char *slot_name,
								  const char *plugin, bool is_temporary,
								  bool is_physical, bool reserve_wal,
								  bool slot_exists_ok);
extern bool RetrieveWalSegSize(PGconn *conn);
extern TimestampTz feGetCurrentTimestamp(void);
extern void feTimestampDifference(TimestampTz start_time, TimestampTz stop_time,
								  long *secs, int *microsecs);

extern bool feTimestampDifferenceExceeds(TimestampTz start_time, TimestampTz stop_time,
										 int msec);
extern void fe_sendint64(int64 i, char *buf);
extern int64 fe_recvint64(char *buf);

#endif							/* STREAMUTIL_H */
