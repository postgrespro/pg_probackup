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
 * Returns true if PostgreSQL is 17+ AND summarize_wal is enabled.
 * Returns false otherwise (caller is responsible for emitting an ERROR
 * with appropriate context — this function stays silent so the caller
 * can produce a single, specific error message).
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
			return false;
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
 *   backup_conn       - connection to the PostgreSQL server
 *   target_lsn        - the LSN we need the summarizer to reach
 *   max_wait_seconds  - maximum time to wait before giving up
 *
 * Returns true if summarizer caught up, false if it was disabled or failed
 */
bool
wait_wal_summarization(PGconn *backup_conn, XLogRecPtr target_lsn,
					   int max_wait_seconds)
{
	int			wait_seconds = 0;
	int			check_interval = 1;		/* Check every second */
	XLogRecPtr	last_summarized_lsn = InvalidXLogRecPtr;
	time_t		start_time;
	char		target_lsn_str[17 + 1];

	if (XLogRecPtrIsInvalid(target_lsn))
		return true;	/* Nothing to wait for */

	/* Fall back to a sane default if caller passed a non-positive value */
	if (max_wait_seconds <= 0)
		max_wait_seconds = ARCHIVE_TIMEOUT_DEFAULT;

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
 * Structure to hold changed block information for a file
 */
typedef struct BlockMapEntry
{
	Oid			dbOid;
	Oid			tblspcOid;
	Oid			relOid;
	ForkName	forkName;
	parray	   *blocknums;	/* array of BlockNumber */
} BlockMapEntry;

/*
 * Compare function for sorting BlockMapEntry by (dbOid, tblspcOid, relOid, forkName)
 */
static int
blockmap_compare(const void *a, const void *b)
{
	const BlockMapEntry *ea = *(const BlockMapEntry **) a;
	const BlockMapEntry *eb = *(const BlockMapEntry **) b;

	if (ea->dbOid != eb->dbOid)
		return ea->dbOid < eb->dbOid ? -1 : 1;
	if (ea->tblspcOid != eb->tblspcOid)
		return ea->tblspcOid < eb->tblspcOid ? -1 : 1;
	if (ea->relOid != eb->relOid)
		return ea->relOid < eb->relOid ? -1 : 1;
	return ea->forkName - eb->forkName;
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
	parray	   *blockmap_list;
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

	/* For each summary file, get the changed blocks */
	blockmap_list = parray_new();

	for (i = 0; i < num_summaries; i++)
	{
		char	   *summary_start_lsn = PQgetvalue(res, i, 1);
		char	   *summary_end_lsn = PQgetvalue(res, i, 2);
		PGresult   *block_res;
		int			j;

		elog(VERBOSE, "Processing summary file: %s to %s",
			 summary_start_lsn, summary_end_lsn);

		/*
		 * Query the summary file contents using the original file borders.
		 * pg_wal_summary_contents() uses start_lsn and end_lsn to construct
		 * the filename, so we must pass the exact values from
		 * pg_available_wal_summaries(). Using clipped/intersected LSN values
		 * would cause a "file not found" error.
		 *
		 * We already filtered summary files to only those overlapping our
		 * requested range, so all returned blocks are relevant.
		 */
		snprintf(query, sizeof(query),
				 "SELECT "
					"reldatabase, "
					"reltablespace, "
					"relfilenode, "
					"relforknumber, "
					"relblocknumber "
					"FROM pg_wal_summary_contents("
					"%u, '%s'::pg_lsn, '%s'::pg_lsn) "
					"WHERE NOT is_limit_block "
					"ORDER BY reldatabase, reltablespace, relfilenode, relforknumber, relblocknumber",
					tli,
					summary_start_lsn, summary_end_lsn);

		block_res = pgut_execute(backup_conn, query, 0, NULL);

		if (PQntuples(block_res) == 0)
			continue;

		elog(VERBOSE, "Found %d changed blocks in this summary", PQntuples(block_res));

		/* Process all blocks from this summary file */
		for (j = 0; j < PQntuples(block_res);)
		{
			Oid			curr_db_oid;
			Oid			curr_tblspc_oid;
			Oid			curr_relfilenode;
			int			curr_fork_number;
			ForkName	curr_fork_name;
			BlockMapEntry *map;

			curr_db_oid = atooid(PQgetvalue(block_res, j, 0));
			curr_tblspc_oid = atooid(PQgetvalue(block_res, j, 1));
			curr_relfilenode = atooid(PQgetvalue(block_res, j, 2));
			curr_fork_number = atoi(PQgetvalue(block_res, j, 3));

			/* Map fork number to ForkName */
			switch (curr_fork_number)
			{
				case 0:
					curr_fork_name = none;	/* main fork */
					break;
				case 1:
					curr_fork_name = fsm;
					break;
				case 2:
					curr_fork_name = vm;
					break;
				case 3:
					curr_fork_name = init;
					break;
				default:
					elog(WARNING, "Unknown fork number %d for relfilenode %u",
						 curr_fork_number, curr_relfilenode);
					/* Skip to next file */
					while (j < PQntuples(block_res) &&
						   atooid(PQgetvalue(block_res, j, 0)) == curr_db_oid &&
						   atooid(PQgetvalue(block_res, j, 1)) == curr_tblspc_oid &&
						   atooid(PQgetvalue(block_res, j, 2)) == curr_relfilenode &&
						   atoi(PQgetvalue(block_res, j, 3)) == curr_fork_number)
						j++;
					continue;
			}

			map = pgut_malloc(sizeof(BlockMapEntry));
			map->dbOid = curr_db_oid;
			map->tblspcOid = curr_tblspc_oid;
			map->relOid = curr_relfilenode;
			map->forkName = curr_fork_name;
			map->blocknums = parray_new();
			parray_append(blockmap_list, map);

			/* Collect all blocks for this file */
			while (j < PQntuples(block_res))
			{
				Oid			db_oid = atooid(PQgetvalue(block_res, j, 0));
				Oid			tblspc_oid = atooid(PQgetvalue(block_res, j, 1));
				Oid			relfilenode = atooid(PQgetvalue(block_res, j, 2));
				int			fork_number = atoi(PQgetvalue(block_res, j, 3));
				BlockNumber	block_number = (BlockNumber) strtoul(PQgetvalue(block_res, j, 4), NULL, 10);
				BlockNumber *blk;

				if (db_oid != curr_db_oid ||
					tblspc_oid != curr_tblspc_oid ||
					relfilenode != curr_relfilenode ||
					fork_number != curr_fork_number)
					break;

				blk = palloc(sizeof(BlockNumber));
				*blk = block_number;
				parray_append(map->blocknums, blk);
				total_blocks++;
				j++;
			}
		}

		PQclear(block_res);
	}

	PQclear(res);

	/* Sort the blockmap list and merge duplicate entries */
	if (parray_num(blockmap_list) > 0)
	{
		parray	   *merged_list;
		BlockMapEntry *prev = NULL;
		int			k;

		parray_qsort(blockmap_list, blockmap_compare);

		/* Merge consecutive entries with the same key */
		merged_list = parray_new();
		for (k = 0; k < parray_num(blockmap_list); k++)
		{
			BlockMapEntry *curr = (BlockMapEntry *) parray_get(blockmap_list, k);

			if (prev && blockmap_compare(&prev, &curr) == 0)
			{
				/* Same file: merge blocks from curr into prev */
				int		b;
				for (b = 0; b < parray_num(curr->blocknums); b++)
					parray_append(prev->blocknums, parray_get(curr->blocknums, b));
				parray_free(curr->blocknums);
				pfree(curr);
			}
			else
			{
				parray_append(merged_list, curr);
				prev = curr;
			}
		}
		parray_free(blockmap_list);
		blockmap_list = merged_list;
	}

	elog(INFO, "Mapped %d changed blocks to %zu files",
		 total_blocks, parray_num(blockmap_list));

	/* Iterate over files and match with WAL summary data */
	for (file_i = 0; file_i < parray_num(files); file_i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, file_i);
		BlockMapEntry key;
		BlockMapEntry **found_entry;
		BlockMapEntry *map = NULL;
		int			j;

		if (!file->is_datafile || file->is_cfs)
			continue;
		if (file->external_dir_num != 0)
			continue;

		/* Binary search for matching blockmap entry */
		key.dbOid = file->dbOid;
		key.tblspcOid = file->tblspcOid;
		key.relOid = file->relOid;
		key.forkName = file->forkName;
		key.blocknums = NULL;

		found_entry = (BlockMapEntry **) parray_bsearch(blockmap_list, &key, blockmap_compare);
		if (found_entry)
			map = *found_entry;

		/* Found matching entry */
		if (map && parray_num(map->blocknums) > 0)
		{
			int			nblocks;

			elog(VERBOSE, "Building pagemap for file \"%s\" with %zu changed blocks",
				 file->rel_path, parray_num(map->blocknums));

			/* Determine file size in blocks */
			if (file->size == 0)
				nblocks = 0;
			else
				nblocks = (file->size + BLCKSZ - 1) / BLCKSZ;

			/* Initialize pagemap */
			file->pagemap.bitmapsize = 0;
			file->pagemap.bitmap = NULL;

			/*
			 * WAL summary contains absolute block numbers. For relations
			 * larger than 1GB, blocks are split into segment files.
			 * We must convert absolute block numbers to segment-relative
			 * block numbers and only add blocks belonging to this segment.
			 */
			for (j = 0; j < parray_num(map->blocknums); j++)
			{
				BlockNumber blknum = *(BlockNumber *) parray_get(map->blocknums, j);
				BlockNumber blkno_inseg = blknum % RELSEG_SIZE;
				int			segno = blknum / RELSEG_SIZE;

				/* Skip blocks not in this segment */
				if (segno != file->segno)
					continue;

				if (blkno_inseg < (BlockNumber) nblocks)
					datapagemap_add(&file->pagemap, blkno_inseg);
				else
					elog(WARNING, "Block number %u (segment %d, offset %u) exceeds file size (%d blocks) for \"%s\"",
						 blknum, segno, blkno_inseg, nblocks, file->rel_path);
			}
		}
	}

	/* Free the blockmap list */
	for (i = 0; i < parray_num(blockmap_list); i++)
	{
		BlockMapEntry *entry = (BlockMapEntry *) parray_get(blockmap_list, i);
		int			j;

		for (j = 0; j < parray_num(entry->blocknums); j++)
			pfree(parray_get(entry->blocknums, j));

		parray_free(entry->blocknums);
		pfree(entry);
	}
	parray_free(blockmap_list);
}
