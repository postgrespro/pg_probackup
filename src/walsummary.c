/*-------------------------------------------------------------------------
 *
 * walsummary.c: support functions for WAL summarize backups
 *
 * Uses PostgreSQL 17+ native WAL summarize feature for incremental backup.
 *
 * Portions Copyright (c) 2025, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#if PG_VERSION_NUM < 110000
#include "catalog/catalog.h"
#endif
#include "catalog/pg_tablespace.h"

/*
 * Check if the server has WAL summarize enabled.
 * Returns true if summarize_wal is enabled, false otherwise.
 */
bool
pg_is_walsummary_enabled(PGconn *backup_conn)
{
	PGresult   *res;
	char	   *setting;
	bool		enabled = false;

	/* Check PostgreSQL version - WAL summarize is only available in PG 17+ */
	res = pgut_execute(backup_conn,
					  "SELECT setting::text FROM pg_settings WHERE name = 'server_version_num'",
					  0, NULL);

	if (PQntuples(res) == 0)
	{
		PQclear(res);
		return false;
	}

	{
		int version_num = atoi(PQgetvalue(res, 0, 0));
		PQclear(res);

		if (version_num < 170000)
		{
			elog(WARNING, "WAL summarize backup mode requires PostgreSQL 17 or higher");
			return false;
		}
	}

	/* Check if summarize_wal is enabled */
	res = pgut_execute(backup_conn,
					  "SELECT setting FROM pg_settings WHERE name = 'summarize_wal'",
					  0, NULL);

	if (PQntuples(res) == 0)
	{
		PQclear(res);
		return false;
	}

	setting = PQgetvalue(res, 0, 0);

	if (strcmp(setting, "on") == 0)
		enabled = true;

	PQclear(res);
	return enabled;
}

/*
 * Get the current summarized LSN from the WAL summarizer.
 * Returns the LSN that has been summarized to disk, or InvalidXLogRecPtr if not available.
 */
XLogRecPtr
get_walsummary_summarized_lsn(PGconn *backup_conn)
{
	PGresult   *res;
	XLogRecPtr	summarized_lsn = InvalidXLogRecPtr;
	uint32		hi,
				lo;

	/* Check if we can get the summarizer state */
	res = pgut_execute(backup_conn,
					  "SELECT summarized_lsn FROM pg_get_wal_summarizer_state() "
					  "WHERE summarized_lsn IS NOT NULL",
					  0, NULL);

	if (PQntuples(res) == 0)
	{
		PQclear(res);
		return InvalidXLogRecPtr;
	}

	if (sscanf(PQgetvalue(res, 0, 0), "%X/%X", &hi, &lo) == 2)
		summarized_lsn = ((uint64) hi) << 32 | lo;

	PQclear(res);
	return summarized_lsn;
}

/*
 * Wait for the WAL summarizer to catch up to the specified LSN.
 *
 * This is important after backup completes to ensure that all WAL up to
 * the backup stop_lsn has been summarized, so the next incremental backup
 * will have complete information.
 *
 * Parameters:
 *   backup_conn - connection to the PostgreSQL server
 *   target_lsn  - the LSN we need the summarizer to reach
 *
 * Returns true if summarizer caught up, false if it was disabled or failed
 */
bool
wait_wal_summarization(PGconn *backup_conn, XLogRecPtr target_lsn)
{
	int			wait_seconds = 0;
	int			max_wait_seconds = 60;	/* Maximum wait time: 60 seconds */
	int			check_interval = 1;		/* Check every second */
	XLogRecPtr	last_summarized_lsn = InvalidXLogRecPtr;
	time_t		start_time;
	char		target_lsn_str[17 + 1];

	if (XLogRecPtrIsInvalid(target_lsn))
		return true;	/* Nothing to wait for */

	snprintf(target_lsn_str, sizeof(target_lsn_str), "%X/%X",
			 (uint32) (target_lsn >> 32), (uint32) target_lsn);

	time(&start_time);
	elog(INFO, "Waiting for WAL summarizer to catch up to %s", target_lsn_str);

	while (wait_seconds < max_wait_seconds)
	{
		PGresult   *res;
		XLogRecPtr	summarized_lsn = InvalidXLogRecPtr;
		uint32		hi, lo;
		bool		summarizer_running = false;

		/* Check summarizer state */
		res = pgut_execute(backup_conn,
						  "SELECT summarized_lsn, pending_lsn, summarizer_pid "
						  "FROM pg_get_wal_summarizer_state()",
						  0, NULL);

		if (PQntuples(res) > 0)
		{
			/* Check if summarizer is running */
			if (!PQgetisnull(res, 0, 2))
			{
				int pid = atoi(PQgetvalue(res, 0, 2));
				summarizer_running = (pid > 0);
			}

			/* Get summarized LSN */
			if (!PQgetisnull(res, 0, 0))
			{
				if (sscanf(PQgetvalue(res, 0, 0), "%X/%X", &hi, &lo) == 2)
					summarized_lsn = ((uint64) hi) << 32 | lo;
			}
		}

		PQclear(res);

		/* Check if summarizer is disabled */
		if (!summarizer_running)
		{
			elog(WARNING, "WAL summarizer is not running");
			return false;
		}

		/* Check if we've caught up */
		if (!XLogRecPtrIsInvalid(summarized_lsn) && summarized_lsn >= target_lsn)
		{
			elog(INFO, "WAL summarizer has caught up to %s after %d seconds",
				 target_lsn_str, wait_seconds);
			return true;
		}

		/* Show progress if LSN has advanced */
		if (!XLogRecPtrIsInvalid(summarized_lsn) &&
			XLogRecPtrIsInvalid(last_summarized_lsn))
		{
			char current_lsn_str[17 + 1];
			snprintf(current_lsn_str, sizeof(current_lsn_str), "%X/%X",
					 (uint32) (summarized_lsn >> 32), (uint32) summarized_lsn);
			elog(INFO, "WAL summarizer at %s, waiting to reach %s...",
				 current_lsn_str, target_lsn_str);
		}

		last_summarized_lsn = summarized_lsn;

		/* Wait before checking again */
		usleep(check_interval * 1000000);	/* microseconds */
		wait_seconds++;
	}

	/* Timeout */
	elog(WARNING, "Timed out waiting for WAL summarizer to catch up to %s after %d seconds",
		 target_lsn_str, max_wait_seconds);
	return false;
}

/*
 * Compare data files by (dbOid, tblspcOid, relOid, segno).
 *
 * WAL summaries report relation-global block numbers; the segment number is
 * needed to find the right 1GB segment file (relfilenode, relfilenode.1, ...)
 * and to compute the segment-local block offset. Used to sort/bsearch the list
 * of data files when applying summary contents to pagemaps.
 */
static int
pgFileCompareRelOidWithSegno(const void *a, const void *b)
{
	pgFile	   *fa = *(pgFile **) a;
	pgFile	   *fb = *(pgFile **) b;

	if (fa->dbOid != fb->dbOid)
		return fa->dbOid < fb->dbOid ? -1 : 1;
	if (fa->tblspcOid != fb->tblspcOid)
		return fa->tblspcOid < fb->tblspcOid ? -1 : 1;
	if (fa->relOid != fb->relOid)
		return fa->relOid < fb->relOid ? -1 : 1;
	if (fa->segno != fb->segno)
		return fa->segno < fb->segno ? -1 : 1;
	return 0;
}

/*
 * Build page maps from WAL summary information.
 *
 * This function queries pg_available_wal_summaries() to get available summary files,
 * then queries pg_wal_summary_contents() for each file to get the list of
 * changed blocks between start_lsn and end_lsn, and builds pagemap bitmaps for each data file.
 *
 * IMPORTANT: pg_wal_summary_contents() must run on the server where the data directory
 * exists. This function works for local backups. For remote backups, pg_probackup's
 * remote connection handling ensures the query runs on the server side.
 *
 * Parameters:
 *   files       - array of pgFile structures to update
 *   backup_conn - connection to the PostgreSQL server
 *   start_lsn   - start LSN of the range to query
 *   end_lsn     - end LSN of the range to query
 *   tli         - timeline ID
 */
void
make_pagemap_from_walsummary(parray *files,
							  PGconn *backup_conn,
							  XLogRecPtr start_lsn,
							  XLogRecPtr end_lsn,
							  TimeLineID tli)
{
	PGresult   *res;
	int			i;
	int			file_i;
	char		start_lsn_str[17 + 1];
	char		end_lsn_str[17 + 1];
	char		query[1024];
	parray	   *datafiles;
	int			total_blocks = 0;
	int			num_summaries;

	/* Build LSN strings for the query */
	snprintf(start_lsn_str, sizeof(start_lsn_str), "%X/%X",
			 (uint32) (start_lsn >> 32), (uint32) start_lsn);
	snprintf(end_lsn_str, sizeof(end_lsn_str), "%X/%X",
			 (uint32) (end_lsn >> 32), (uint32) end_lsn);

	elog(INFO, "Querying WAL summaries from %s to %s on timeline %u",
		 start_lsn_str, end_lsn_str, tli);

	/*
	 * First, get all available WAL summary files that overlap with our range
	 * We query summaries that intersect with [start_lsn, end_lsn]
	 *
	 * Note: WAL summary files use half-open interval semantics [start_lsn, end_lsn),
	 * meaning they include changes from start_lsn up to but NOT including end_lsn.
	 * Therefore we use > instead of >= for the end_lsn comparison to exclude
	 * summary files that end exactly at our start LSN.
	 */
	snprintf(query, sizeof(query),
			 "SELECT tli, start_lsn::text, end_lsn::text "
			 "FROM pg_available_wal_summaries() "
			 "WHERE tli = %u "
			 "AND end_lsn > '%s'::pg_lsn "
			 "AND start_lsn < '%s'::pg_lsn "
			 "ORDER BY start_lsn",
			 tli, start_lsn_str, end_lsn_str);

	res = pgut_execute(backup_conn, query, 0, NULL);

	if (PQntuples(res) == 0)
	{
		PQclear(res);
		elog(WARNING, "No WAL summaries found for the given range");
		return;
	}

	num_summaries = PQntuples(res);
	elog(INFO, "Found %d WAL summary files covering the requested range", num_summaries);

	/*
	 * Build a lookup of main-fork data files keyed by
	 * (dbOid, tblspcOid, relOid, segno) so a relation-global block number can be
	 * mapped to the right 1GB segment file. Only main-fork data files are
	 * page-tracked; other forks are copied whole.
	 */
	datafiles = parray_new();
	for (file_i = 0; file_i < (int) parray_num(files); file_i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, file_i);

		if (file->is_datafile && !file->is_cfs && file->external_dir_num == 0)
			parray_append(datafiles, file);
	}
	parray_qsort(datafiles, pgFileCompareRelOidWithSegno);

	for (i = 0; i < num_summaries; i++)
	{
		char	   *summary_start_lsn = PQgetvalue(res, i, 1);
		char	   *summary_end_lsn = PQgetvalue(res, i, 2);
		PGresult   *block_res;
		int			j;

		elog(VERBOSE, "Processing summary file: %s to %s",
			 summary_start_lsn, summary_end_lsn);

		/*
		 * Pass the summary file's own boundaries: pg_wal_summary_contents()
		 * identifies the file by exactly these LSNs, so clipping them to the
		 * requested range would reference a non-existent file name and fail
		 * with "could not open file". A block reported slightly before
		 * start_lsn is harmless -- it is filtered later by the per-page LSN
		 * check in prepare_page(). Only the main fork is page-tracked.
		 */
		snprintf(query, sizeof(query),
				 "SELECT reldatabase, reltablespace, relfilenode, relblocknumber "
				 "FROM pg_wal_summary_contents(%u, '%s'::pg_lsn, '%s'::pg_lsn) "
				 "WHERE NOT is_limit_block AND relforknumber = %d",
				 tli, summary_start_lsn, summary_end_lsn, MAIN_FORKNUM);

		block_res = pgut_execute(backup_conn, query, 0, NULL);

		for (j = 0; j < PQntuples(block_res); j++)
		{
			pgFile		key;
			pgFile	  **found;
			BlockNumber	blknum;

			key.dbOid = (Oid) strtoul(PQgetvalue(block_res, j, 0), NULL, 10);
			key.tblspcOid = (Oid) strtoul(PQgetvalue(block_res, j, 1), NULL, 10);
			key.relOid = (Oid) strtoul(PQgetvalue(block_res, j, 2), NULL, 10);
			blknum = (BlockNumber) strtoul(PQgetvalue(block_res, j, 3), NULL, 10);
			key.segno = (int) (blknum / RELSEG_SIZE);

			found = (pgFile **) parray_bsearch(datafiles, &key,
											   pgFileCompareRelOidWithSegno);
			if (found != NULL)
			{
				datapagemap_add(&(*found)->pagemap, blknum % RELSEG_SIZE);
				total_blocks++;
			}
		}

		PQclear(block_res);
	}

	PQclear(res);
	parray_free(datafiles);

	elog(INFO, "Mapped %d changed blocks from WAL summaries", total_blocks);
}
