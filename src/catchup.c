/*-------------------------------------------------------------------------
 *
 * catchup.c: sync DB cluster
 *
 * Copyright (c) 2021, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#if PG_VERSION_NUM < 110000
#include "catalog/catalog.h"
#endif
#include "catalog/pg_tablespace.h"
#include "pgtar.h"
#include "streamutil.h"

#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "utils/thread.h"
#include "utils/file.h"

/*
 * Catchup routines
 */
static PGconn *catchup_collect_info(PGNodeInfo *source_node_info, const char *source_pgdata, const char *dest_pgdata);
static void catchup_preflight_checks(PGNodeInfo *source_node_info, PGconn *source_conn, const char *source_pgdata, 
					const char *dest_pgdata);
static void catchup_check_tablespaces_existance_in_tbsmapping(PGconn *conn);
static parray* catchup_get_tli_history(ConnectionOptions *conn_opt, TimeLineID tli);
static void do_catchup_instance(const char *source_pgdata, const char *dest_pgdata, PGconn *source_conn,
					PGNodeInfo *nodeInfo, bool no_sync, bool backup_logs);
static void *catchup_thread_runner(void *arg);

/*
 * Entry point of pg_probackup CATCHUP subcommand.
 */
int
do_catchup(const char *source_pgdata, const char *dest_pgdata, int num_threads)
{
	PGconn		*source_conn = NULL;
	PGNodeInfo	source_node_info;
	bool		no_sync = false;
	bool		backup_logs = false;

	source_conn = catchup_collect_info(&source_node_info, source_pgdata, dest_pgdata);
	catchup_preflight_checks(&source_node_info, source_conn, source_pgdata, dest_pgdata);

	do_catchup_instance(source_pgdata, dest_pgdata, source_conn, &source_node_info,
						no_sync, backup_logs);

	//REVIEW: Are we going to do that before release?
	/* TODO: show the amount of transfered data in bytes and calculate incremental ratio */

	return 0;
}

//REVIEW Please add a comment to this function.
//Besides, the name of this function looks strange to me.
//Maybe catchup_init_state() or catchup_setup() will do better?
//I'd also suggest to wrap all these fields into some CatchupState, but it isn't urgent.
/*
 * Prepare for work: fill some globals, open connection to source database
 */
static PGconn *
catchup_collect_info(PGNodeInfo	*source_node_info, const char *source_pgdata, const char *dest_pgdata)
{
	PGconn		*source_conn;

	/* Initialize PGInfonode */
	pgNodeInit(source_node_info);

	/* Get WAL segments size and system ID of source PG instance */
	instance_config.xlog_seg_size = get_xlog_seg_size(source_pgdata);
	instance_config.system_identifier = get_system_identifier(source_pgdata, FIO_DB_HOST);
	current.start_time = time(NULL);

	StrNCpy(current.program_version, PROGRAM_VERSION, sizeof(current.program_version));
	//REVIEW I guess these are some copy-paste leftovers. Let's clean them.
	//current.compress_alg = instance_config.compress_alg;
	//current.compress_level = instance_config.compress_level;

	/* Do some compatibility checks and fill basic info about PG instance */
	source_conn = pgdata_basic_setup(instance_config.conn_opt, source_node_info);

#if PG_VERSION_NUM >= 110000
	if (!RetrieveWalSegSize(source_conn))
		elog(ERROR, "Failed to retrieve wal_segment_size");
#endif

	get_ptrack_version(source_conn, source_node_info);
	if (source_node_info->ptrack_version_num > 0)
		source_node_info->is_ptrack_enabled = pg_is_ptrack_enabled(source_conn, source_node_info->ptrack_version_num);

	/* Obtain current timeline */
#if PG_VERSION_NUM >= 90600
	current.tli = get_current_timeline(source_conn);
#else
	instance_config.pgdata = source_pgdata;
	current.tli = get_current_timeline_from_control(source_pgdata, FIO_DB_HOST, false);
#endif

	elog(INFO, "Catchup start, pg_probackup version: %s, "
			"PostgreSQL version: %s, "
			"remote: %s, catchup-source-pgdata: %s, catchup-destination-pgdata: %s",
			PROGRAM_VERSION, source_node_info->server_version_str,
			IsSshProtocol()  ? "true" : "false",
			source_pgdata, dest_pgdata);

	if (current.from_replica)
		elog(INFO, "Running catchup from standby");

	return source_conn;
}

/*
 * Check that catchup can be performed on source and dest
 * this function is for checks, that can be performed without modification of data on disk
 */
static void
catchup_preflight_checks(PGNodeInfo *source_node_info, PGconn *source_conn,
		const char *source_pgdata, const char *dest_pgdata)
{
	/*  TODO
	 *  gsmol - fallback to FULL mode if dest PGDATA is empty
	 *  kulaginm -- I think this is a harmful feature. If user requested an incremental catchup, then
	 * he expects that this will be done quickly and efficiently. If, for example, he made a mistake
	 * with dest_dir, then he will receive a second full copy instead of an error message, and I think
	 * that in some cases he would prefer the error.
	 * I propose in future versions to offer a backup_mode auto, in which we will look to the dest_dir
	 * and decide which of the modes will be the most effective.
	 * I.e.:
	 *   if(requested_backup_mode == BACKUP_MODE_DIFF_AUTO)
	 *   {
	 *     if(dest_pgdata_is_empty)
	 *       backup_mode = BACKUP_MODE_FULL;
	 *     else
	 *       if(ptrack supported and applicable)
	 *         backup_mode = BACKUP_MODE_DIFF_PTRACK;
	 *       else
	 *         backup_mode = BACKUP_MODE_DIFF_DELTA;
	 *   }
	 */

	if (dir_is_empty(dest_pgdata, FIO_LOCAL_HOST))
	{
		if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK ||
			 current.backup_mode == BACKUP_MODE_DIFF_DELTA)
			elog(ERROR, "\"%s\" is empty but incremental catchup mode requested.",
				dest_pgdata);
	}
	else /* dest dir not empty */
	{
		if (current.backup_mode == BACKUP_MODE_FULL)
			elog(ERROR, "Can't perform full catchup into not empty directory \"%s\".",
				dest_pgdata);
	}

	/* check that postmaster is not running in destination */
	if (current.backup_mode != BACKUP_MODE_FULL)
	{
		pid_t   pid;
		pid = fio_check_postmaster(dest_pgdata, FIO_LOCAL_HOST);
		if (pid == 1) /* postmaster.pid is mangled */
		{
			char	pid_filename[MAXPGPATH];
			join_path_components(pid_filename, dest_pgdata, "postmaster.pid");
			elog(ERROR, "Pid file \"%s\" is mangled, cannot determine whether postmaster is running or not",
				pid_filename);
		}
		else if (pid > 1) /* postmaster is up */
		{
			elog(ERROR, "Postmaster with pid %u is running in destination directory \"%s\"",
				pid, dest_pgdata);
		}
	}

	/* Check that connected PG instance, source and destination PGDATA are the same */
	{
		uint64	source_conn_id, source_id, dest_id;

		source_conn_id = get_remote_system_identifier(source_conn);
		source_id = get_system_identifier(source_pgdata, FIO_DB_HOST); /* same as instance_config.system_identifier */

		if (source_conn_id != source_id)
			elog(ERROR, "Database identifiers mismatch: we connected to DB id %lu, but in \"%s\" we found id %lu",
				source_conn_id, source_pgdata, source_id);

		if (current.backup_mode != BACKUP_MODE_FULL)
		{
			dest_id = get_system_identifier(dest_pgdata, FIO_LOCAL_HOST);
			if (source_conn_id != dest_id)
			elog(ERROR, "Database identifiers mismatch: we connected to DB id %lu, but in \"%s\" we found id %lu",
				source_conn_id, dest_pgdata, dest_id);
		}
	}

	/* check PTRACK version */
	if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		if (source_node_info->ptrack_version_num == 0)
			elog(ERROR, "This PostgreSQL instance does not support ptrack");
		else if (source_node_info->ptrack_version_num < 20)
			elog(ERROR, "ptrack extension is too old.\n"
					"Upgrade ptrack to version >= 2");
		else if (!source_node_info->is_ptrack_enabled)
			elog(ERROR, "Ptrack is disabled");
	}

	/* check backup_label absence in dest */
	if (current.backup_mode != BACKUP_MODE_FULL)
	{
		char	backup_label_filename[MAXPGPATH];

		join_path_components(backup_label_filename, dest_pgdata, "backup_label");
		if (fio_access(backup_label_filename, F_OK, FIO_LOCAL_HOST) == 0)
			elog(ERROR, "Destination directory contains \"backup_control\" file");
	}

	if (current.from_replica && exclusive_backup)
		elog(ERROR, "Catchup from standby is available only for PG >= 9.6");

	/* if local catchup, check that we don't overwrite tablespace in source pgdata */
	if (!fio_is_remote(FIO_DB_HOST))
		catchup_check_tablespaces_existance_in_tbsmapping(source_conn);

	/* check timelines */
	if (current.backup_mode != BACKUP_MODE_FULL)
	{
		TimeLineID	dest_tli;
		parray		*source_timelines;

		dest_tli = get_current_timeline_from_control(dest_pgdata, FIO_LOCAL_HOST, false);

		source_timelines = catchup_get_tli_history(&instance_config.conn_opt, current.tli);

		if (source_timelines != NULL && !tliIsPartOfHistory(source_timelines, dest_tli))
			elog(ERROR, "Destination is not in source history");

		if (source_timelines != NULL)
		{
			parray_walk(source_timelines, pfree);
			parray_free(source_timelines);
		}
	}
}

/*
 * Check that all tablespaces exists in tablespace mapping (--tablespace-mapping option)
 * Emit fatal error if that (not existent in map) tablespace found
 */
static void
catchup_check_tablespaces_existance_in_tbsmapping(PGconn *conn)
{
	PGresult	*res;
	int		i;
	char		*tablespace_path = NULL;
	const char	*linked_path = NULL;
	char		*query = "SELECT pg_catalog.pg_tablespace_location(oid) "
						"FROM pg_catalog.pg_tablespace "
						"WHERE pg_catalog.pg_tablespace_location(oid) <> '';";

	res = pgut_execute(conn, query, 0, NULL);

	if (!res)
		elog(ERROR, "Failed to get list of tablespaces");

	for (i = 0; i < res->ntups; i++)
	{
		tablespace_path = PQgetvalue(res, i, 0);
		Assert (strlen(tablespace_path) > 0);

		canonicalize_path(tablespace_path);
		linked_path = leaked_abstraction_get_tablespace_mapping(tablespace_path);

		if (strcmp(tablespace_path, linked_path) == 0)
			/* same result -> not found in mapping */
			elog(ERROR, "Local catchup executed, but source database contains "
				"tablespace (\"%s\"), that are not listed in the map", tablespace_path);

		if (!is_absolute_path(linked_path))
			elog(ERROR, "Tablespace directory path must be an absolute path: %s\n",
				linked_path);
	}
	PQclear(res);
}

/*
 * Get timeline history via replication connection
 * returns parray* of TimeLineHistoryEntry*
 */
static parray*
catchup_get_tli_history(ConnectionOptions *conn_opt, TimeLineID tli)
{
	PGresult     *res;
	PGconn	     *conn;
	char         *history;
	char          query[128];
	parray	     *result = NULL;

	snprintf(query, sizeof(query), "TIMELINE_HISTORY %u", tli);

	/*
	 * Connect in replication mode to the server.
	 */
	conn = pgut_connect_replication(conn_opt->pghost,
									conn_opt->pgport,
									conn_opt->pgdatabase,
									conn_opt->pguser,
									false);

	if (!conn)
		return NULL;

	res = PQexec(conn, query);
	PQfinish(conn);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		elog(WARNING, "Could not send replication command \"%s\": %s",
					query, PQresultErrorMessage(res));
		PQclear(res);
		return NULL;
	}

	/*
	 * The response to TIMELINE_HISTORY is a single row result set
	 * with two fields: filename and content
	 */
	if (PQnfields(res) != 2 || PQntuples(res) != 1)
	{
		elog(ERROR, "Unexpected response to TIMELINE_HISTORY command: "
				"got %d rows and %d fields, expected %d rows and %d fields",
				PQntuples(res), PQnfields(res), 1, 2);
		PQclear(res);
		return NULL;
	}

	history = pgut_strdup(PQgetvalue(res, 0, 1));
	result = parse_tli_history_buffer(history, tli);

	/* some cleanup */
	pg_free(history);
	PQclear(res);

	return result;
}


/*
 * TODO:
 *  - add description
 * main worker function, to be moved into do_catchup() and then to be split into meaningful pieces
 */
static void
do_catchup_instance(const char *source_pgdata, const char *dest_pgdata, PGconn *source_conn,
					PGNodeInfo *source_node_info, bool no_sync, bool backup_logs)
{
	int			i;
	char		dest_xlog_path[MAXPGPATH];
	char		label[1024];
	RedoParams	dest_redo = { 0, InvalidXLogRecPtr, 0 };
	pgFile		*source_pg_control_file = NULL;

	/* arrays with meta info for multi threaded catchup */
	pthread_t	*threads;
	catchup_thread_runner_arg *threads_args;
	bool		catchup_isok = true;

	parray     *source_filelist = NULL;
	parray	   *dest_filelist = NULL;

	/* for fancy reporting */
	time_t		start_time, end_time;
	char		pretty_time[20];
	char		pretty_bytes[20];

	PGStopBackupResult	stop_backup_result;
	//REVIEW Is it relevant to catchup? I suppose it isn't, since catchup is a new code.
	//If we do need it, please write a comment explaining that.
	/* kludge against some old bug in archive_timeout. TODO: remove in 3.0.0 */
	int	     timeout = (instance_config.archive_timeout > 0) ?
				instance_config.archive_timeout : ARCHIVE_TIMEOUT_DEFAULT;
	char    *query_text = NULL;

	elog(LOG, "Database catchup start");

	/* notify start of backup to PostgreSQL server */
	time2iso(label, lengthof(label), current.start_time, false);
	strncat(label, " with pg_probackup", lengthof(label) -
			strlen(" with pg_probackup"));

	/* Call pg_start_backup function in PostgreSQL connect */
	pg_start_backup(label, smooth_checkpoint, &current, source_node_info, source_conn);
	elog(LOG, "pg_start_backup START LSN %X/%X", (uint32) (current.start_lsn >> 32), (uint32) (current.start_lsn));

	//REVIEW I wonder, if we can move this piece above and call before pg_start backup()?
	//It seems to be a part of setup phase.
	if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK ||
		 current.backup_mode == BACKUP_MODE_DIFF_DELTA)
	{
		dest_filelist = parray_new();
		dir_list_file(dest_filelist, dest_pgdata,
			true, true, false, backup_logs, true, 0, FIO_LOCAL_HOST);

		// fill dest_redo.lsn and dest_redo.tli
		get_redo(dest_pgdata, &dest_redo);
		elog(INFO, "syncLSN = %X/%X", (uint32) (dest_redo.lsn >> 32), (uint32) dest_redo.lsn);
	}

	//REVIEW I wonder, if we can move this piece above and call before pg_start backup()?
	//It seems to be a part of setup phase.
	/*
	 * TODO: move to separate function to use in both backup.c and catchup.c
	 */
	if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		XLogRecPtr	ptrack_lsn = get_last_ptrack_lsn(source_conn, source_node_info);

		// new ptrack is more robust and checks Start LSN
		if (ptrack_lsn > dest_redo.lsn || ptrack_lsn == InvalidXLogRecPtr)
			elog(ERROR, "LSN from ptrack_control in source %X/%X is greater than checkpoint LSN in destination %X/%X.\n"
						"You can perform only FULL catchup.",
						(uint32) (ptrack_lsn >> 32), (uint32) (ptrack_lsn),
						(uint32) (dest_redo.lsn >> 32),
						(uint32) (dest_redo.lsn));
	}

	/* Check that dest_redo.lsn is less than current.start_lsn */
	if (current.backup_mode != BACKUP_MODE_FULL &&
		dest_redo.lsn > current.start_lsn)
			elog(ERROR, "Current START LSN %X/%X is lower than SYNC LSN %X/%X, "
				"it may indicate that we are trying to catchup with PostgreSQL instance from the past",
				(uint32) (current.start_lsn >> 32), (uint32) (current.start_lsn),
				(uint32) (dest_redo.lsn >> 32), (uint32) (dest_redo.lsn));

	/* Start stream replication */
	join_path_components(dest_xlog_path, dest_pgdata, PG_XLOG_DIR);
	fio_mkdir(dest_xlog_path, DIR_PERMISSION, FIO_BACKUP_HOST);
	start_WAL_streaming(source_conn, dest_xlog_path, &instance_config.conn_opt,
						current.start_lsn, current.tli);

	source_filelist = parray_new();

	/* list files with the logical path. omit $PGDATA */
	if (fio_is_remote(FIO_DB_HOST))
		fio_list_dir(source_filelist, source_pgdata,
					 true, true, false, backup_logs, true, 0);
	else
		dir_list_file(source_filelist, source_pgdata,
					  true, true, false, backup_logs, true, 0, FIO_LOCAL_HOST);

	//REVIEW FIXME. Let's fix that before release.
	// TODO filter pg_xlog/wal?
	// TODO what if wal is not a dir (symlink to a dir)?

	/* close ssh session in main thread */
	fio_disconnect();

	//REVIEW Do we want to do similar calculation for dest?
	current.pgdata_bytes += calculate_datasize_of_filelist(source_filelist);
	pretty_size(current.pgdata_bytes, pretty_bytes, lengthof(pretty_bytes));
	elog(INFO, "Source PGDATA size: %s", pretty_bytes);

	/*
	 * Sort pathname ascending. It is necessary to create intermediate
	 * directories sequentially.
	 *
	 * For example:
	 * 1 - create 'base'
	 * 2 - create 'base/1'
	 *
	 * Sorted array is used at least in parse_filelist_filenames(),
	 * extractPageMap(), make_pagemap_from_ptrack().
	 */
	parray_qsort(source_filelist, pgFileCompareRelPathWithExternal);

	/* Extract information about files in source_filelist parsing their names:*/
	parse_filelist_filenames(source_filelist, source_pgdata);

	elog(LOG, "Start LSN (source): %X/%X, TLI: %X",
			(uint32) (current.start_lsn >> 32), (uint32) (current.start_lsn),
			current.tli);
	if (current.backup_mode != BACKUP_MODE_FULL)
		elog(LOG, "LSN in destination: %X/%X, TLI: %X",
			 (uint32) (dest_redo.lsn >> 32), (uint32) (dest_redo.lsn),
			 dest_redo.tli);

	/* Build page mapping in PTRACK mode */
	if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		time(&start_time);
		elog(INFO, "Extracting pagemap of changed blocks");

		/* Build the page map from ptrack information */
		make_pagemap_from_ptrack_2(source_filelist, source_conn,
								   source_node_info->ptrack_schema,
								   source_node_info->ptrack_version_num,
								   dest_redo.lsn);
		time(&end_time);
		elog(INFO, "Pagemap successfully extracted, time elapsed: %.0f sec",
			 difftime(end_time, start_time));
	}

	/*
	 * Make directories before catchup
	 */
	/*
	 * We iterate over source_filelist and for every directory with parent 'pg_tblspc'
	 * we must lookup this directory name in tablespace map.
	 * If we got a match, we treat this directory as tablespace.
	 * It means that we create directory specified in tablespace_map and
	 * original directory created as symlink to it.
	 */
	for (i = 0; i < parray_num(source_filelist); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(source_filelist, i);
		char parent_dir[MAXPGPATH];

		if (!S_ISDIR(file->mode))
			continue;

		/*
		 * check if it is fake "directory" and is a tablespace link
		 * this is because we passed the follow_symlink when building the list
		 */
		/* get parent dir of rel_path */
		strncpy(parent_dir, file->rel_path, MAXPGPATH);
		get_parent_directory(parent_dir);

		/* check if directory is actually link to tablespace */
		if (strcmp(parent_dir, PG_TBLSPC_DIR) != 0)
		{
			/* if the entry is a regular directory, create it in the destination */
			char		dirpath[MAXPGPATH];

			join_path_components(dirpath, dest_pgdata, file->rel_path);

			elog(VERBOSE, "Create directory '%s'", dirpath);
			fio_mkdir(dirpath, DIR_PERMISSION, FIO_BACKUP_HOST);
		}
		else
		{
			/* this directory located in pg_tblspc */
			const char *linked_path = NULL;
			char	to_path[MAXPGPATH];

			// perform additional check that this is actually symlink?
			//REVIEW Why is this code block separated?
			//REVIEW_ANSWER because i want to localize usage of source_full_path and symlink_content
			{ /* get full symlink path and map this path to new location */
				char	source_full_path[MAXPGPATH];
				char	symlink_content[MAXPGPATH];
				join_path_components(source_full_path, source_pgdata, file->rel_path);
				fio_readlink(source_full_path, symlink_content, sizeof(symlink_content), FIO_DB_HOST);
				//REVIEW What if we won't find mapping for this tablespace?
				//I'd expect a failure. Otherwise, we may spoil source database data.
				// REVIEW_ANSWER we checked that in preflight_checks for local catchup
				// and for remote catchup this may be correct behavior
				linked_path = leaked_abstraction_get_tablespace_mapping(symlink_content);
				elog(INFO, "Map tablespace full_path: \"%s\" old_symlink_content: \"%s\" new_symlink_content: \"%s\"\n",
					source_full_path,
					symlink_content,
					linked_path);
			}

			if (!is_absolute_path(linked_path))
				elog(ERROR, "Tablespace directory path must be an absolute path: %s\n",
						 linked_path);

			join_path_components(to_path, dest_pgdata, file->rel_path);

			elog(VERBOSE, "Create directory \"%s\" and symbolic link \"%s\"",
					 linked_path, to_path);

			/* create tablespace directory */
			if (fio_mkdir(linked_path, file->mode, FIO_BACKUP_HOST) != 0)
				elog(ERROR, "Could not create tablespace directory \"%s\": %s",
					 linked_path, strerror(errno));

			/* create link to linked_path */
			if (fio_symlink(linked_path, to_path, true, FIO_BACKUP_HOST) < 0)
				elog(ERROR, "Could not create symbolic link \"%s\" -> \"%s\": %s",
					 linked_path, to_path, strerror(errno));
		}
	}

	/*
	 * find pg_control file (in already sorted source_filelist)
	 * and exclude it from list for future special processing
	 */
	{
		int control_file_elem_index;
		pgFile search_key;
		MemSet(&search_key, 0, sizeof(pgFile));
		/* pgFileCompareRelPathWithExternal uses only .rel_path and .external_dir_num for comparision */
		search_key.rel_path = XLOG_CONTROL_FILE;
		search_key.external_dir_num = 0;
		control_file_elem_index = parray_bsearch_index(source_filelist, &search_key, pgFileCompareRelPathWithExternal);
		if(control_file_elem_index < 0)
			elog(ERROR, "\"%s\" not found in \"%s\"\n", XLOG_CONTROL_FILE, source_pgdata);
		source_pg_control_file = parray_remove(source_filelist, control_file_elem_index);
	}

	/*
	 * remove absent source files in dest (dropped tables, etc...)
	 * note: global/pg_control will also be deleted here
	 */
	if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK ||
		 current.backup_mode == BACKUP_MODE_DIFF_DELTA)
	{
		elog(INFO, "Removing redundant files in destination directory");
		parray_qsort(dest_filelist, pgFileCompareRelPathWithExternalDesc);
		for (i = 0; i < parray_num(dest_filelist); i++)
		{
			bool     redundant = true;
			pgFile	*file = (pgFile *) parray_get(dest_filelist, i);

			//REVIEW Can we maybe optimize it and use some merge-like algorithm
			//instead of bsearch for each file? Of course it isn't an urgent fix.
			//REVIEW_ANSWER yes, merge will be better
			if (parray_bsearch(source_filelist, file, pgFileCompareRelPathWithExternal))
				redundant = false;

			/* pg_filenode.map are always restored, because it's crc cannot be trusted */
			Assert(file->external_dir_num == 0);
			if (pg_strcasecmp(file->name, RELMAPPER_FILENAME) == 0)
				redundant = true;

			//REVIEW This check seems unneded. Anyway we delete only redundant stuff below.
			/* do not delete the useful internal directories */
			if (S_ISDIR(file->mode) && !redundant)
				continue;

			/* if file does not exists in destination list, then we can safely unlink it */
			if (redundant)
			{
				char		fullpath[MAXPGPATH];

				join_path_components(fullpath, dest_pgdata, file->rel_path);

				fio_delete(file->mode, fullpath, FIO_DB_HOST);
				elog(VERBOSE, "Deleted file \"%s\"", fullpath);

				/* shrink pgdata list */
				pgFileFree(file);
				parray_remove(dest_filelist, i);
				i--;
			}
		}
	}

	//REVIEW Hmm. Why do we need this at all?
	//I'd expect that we init pgfile with unset lock...
	//Not related to this patch, though.
	//REVIEW_ANSWER initialization in the pgFileInit function was proposed but was not accepted (see 2c8b7e9)
	/* clear file locks */
	pfilearray_clear_locks(source_filelist);

	/* Sort by size for load balancing */
	parray_qsort(source_filelist, pgFileCompareSizeDesc);

	/* Sort the array for binary search */
	if (dest_filelist)
		parray_qsort(dest_filelist, pgFileCompareRelPathWithExternal);

	/* init thread args */
	threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
	threads_args = (catchup_thread_runner_arg *) palloc(sizeof(catchup_thread_runner_arg) * num_threads);

	for (i = 0; i < num_threads; i++)
	{
		catchup_thread_runner_arg *arg = &(threads_args[i]);

		arg->nodeInfo = source_node_info;
		arg->from_root = source_pgdata;
		arg->to_root = dest_pgdata;
		arg->source_filelist = source_filelist;
		arg->dest_filelist = dest_filelist;
		arg->sync_lsn = dest_redo.lsn;
		arg->backup_mode = current.backup_mode;
		arg->thread_num = i + 1;
		/* By default there are some error */
		arg->ret = 1;
	}

	/* Run threads */
	thread_interrupted = false;
	elog(INFO, "Start transferring data files");
	time(&start_time);
	for (i = 0; i < num_threads; i++)
	{
		catchup_thread_runner_arg *arg = &(threads_args[i]);

		elog(VERBOSE, "Start thread num: %i", i);
		pthread_create(&threads[i], NULL, catchup_thread_runner, arg);
	}

	/* Wait threads */
	for (i = 0; i < num_threads; i++)
	{
		pthread_join(threads[i], NULL);
		if (threads_args[i].ret == 1)
			catchup_isok = false;
	}

	/* at last copy control file */
	if (catchup_isok)
	{
		char	from_fullpath[MAXPGPATH];
		char	to_fullpath[MAXPGPATH];
		join_path_components(from_fullpath, source_pgdata, source_pg_control_file->rel_path);
		join_path_components(to_fullpath, dest_pgdata, source_pg_control_file->rel_path);
		copy_pgcontrol_file(from_fullpath, FIO_DB_HOST,
				to_fullpath, FIO_BACKUP_HOST, source_pg_control_file);
	}

	time(&end_time);
	pretty_time_interval(difftime(end_time, start_time),
						 pretty_time, lengthof(pretty_time));
	if (catchup_isok)
		elog(INFO, "Data files are transferred, time elapsed: %s",
			pretty_time);
	else
		elog(ERROR, "Data files transferring failed, time elapsed: %s",
			pretty_time);

	//REVIEW The comment looks unrelated to the function. Do I miss something?
	//REVIEW_ANSWER because it is a part of pg_stop_backup() calling
	/* Notify end of backup */
	pg_silent_client_messages(source_conn);

	//REVIEW. Do we want to support pg 9.5? I suppose we never test it...
	//Maybe check it and error out early?
	/* Create restore point
	 * Only if backup is from master.
	 * For PG 9.5 create restore point only if pguser is superuser.
	 */
	if (!current.from_replica &&
		!(source_node_info->server_version < 90600 &&
		  !source_node_info->is_superuser)) //TODO: check correctness
		pg_create_restore_point(source_conn, current.start_time);

	/* Execute pg_stop_backup using PostgreSQL connection */
	pg_stop_backup_send(source_conn, source_node_info->server_version, current.from_replica, exclusive_backup, &query_text);

	/*
	 * Wait for the result of pg_stop_backup(), but no longer than
	 * archive_timeout seconds
	 */
	pg_stop_backup_consume(source_conn, source_node_info->server_version, exclusive_backup, timeout, query_text, &stop_backup_result);

	wait_wal_and_calculate_stop_lsn(dest_xlog_path, stop_backup_result.lsn, &current);

	/* Write backup_label and tablespace_map */
	Assert(stop_backup_result.backup_label_content != NULL);

	/* Write backup_label */
	pg_stop_backup_write_file_helper(dest_pgdata, PG_BACKUP_LABEL_FILE, "backup label",
		stop_backup_result.backup_label_content, stop_backup_result.backup_label_content_len,
		NULL);
	free(stop_backup_result.backup_label_content);
	stop_backup_result.backup_label_content = NULL;
	stop_backup_result.backup_label_content_len = 0;

	/* Write tablespace_map */
	if (stop_backup_result.tablespace_map_content != NULL)
	{
		// TODO what if tablespace is created during catchup?
		pg_stop_backup_write_file_helper(dest_pgdata, PG_TABLESPACE_MAP_FILE, "tablespace map",
			stop_backup_result.tablespace_map_content, stop_backup_result.tablespace_map_content_len,
			NULL);
		free(stop_backup_result.tablespace_map_content);
		stop_backup_result.tablespace_map_content = NULL;
		stop_backup_result.tablespace_map_content_len = 0;
	}

	if(wait_WAL_streaming_end(NULL))
		elog(ERROR, "WAL streaming failed");

	//REVIEW Please add a comment about these lsns. It is a crutial part of the algorithm.
	current.recovery_xid = stop_backup_result.snapshot_xid;

	elog(LOG, "Getting the Recovery Time from WAL");

	/* iterate over WAL from stop_backup lsn to start_backup lsn */
	if (!read_recovery_info(dest_xlog_path, current.tli,
						instance_config.xlog_seg_size,
						current.start_lsn, current.stop_lsn,
						&current.recovery_time))
	{
		elog(LOG, "Failed to find Recovery Time in WAL, forced to trust current_timestamp");
		current.recovery_time = stop_backup_result.invocation_time;
	}

	/* Cleanup */
	pg_free(query_text);

	/*
	 * In case of backup from replica >= 9.6 we must fix minRecPoint
	 */
	if (current.from_replica && !exclusive_backup)
	{
		set_min_recovery_point(source_pg_control_file, dest_pgdata, current.stop_lsn);
	}

	/* close ssh session in main thread */
	fio_disconnect();

	/* Sync all copied files unless '--no-sync' flag is used */
	if (no_sync)
		elog(WARNING, "Files are not synced to disk");
	else
	{
		char    to_fullpath[MAXPGPATH];

		elog(INFO, "Syncing copied files to disk");
		time(&start_time);

		for (i = 0; i < parray_num(source_filelist); i++)
		{
			pgFile *file = (pgFile *) parray_get(source_filelist, i);

			/* TODO: sync directory ? */
			if (S_ISDIR(file->mode))
				continue;

			if (file->write_size <= 0)
				continue;

			/* construct fullpath */
			Assert(file->external_dir_num == 0);
			join_path_components(to_fullpath, dest_pgdata, file->rel_path);

			if (fio_sync(to_fullpath, FIO_BACKUP_HOST) != 0)
				elog(ERROR, "Cannot sync file \"%s\": %s", to_fullpath, strerror(errno));
		}

		/*
		 * sync pg_control file
		 */
		join_path_components(to_fullpath, dest_pgdata, source_pg_control_file->rel_path);
		if (fio_sync(to_fullpath, FIO_BACKUP_HOST) != 0)
			elog(ERROR, "Cannot sync file \"%s\": %s", to_fullpath, strerror(errno));

		time(&end_time);
		pretty_time_interval(difftime(end_time, start_time),
							 pretty_time, lengthof(pretty_time));
		elog(INFO, "Files are synced, time elapsed: %s", pretty_time);
	}

	/* Cleanup */
	if (dest_filelist)
	{
		parray_walk(dest_filelist, pgFileFree);
		parray_free(dest_filelist);
	}
	parray_walk(source_filelist, pgFileFree);
	parray_free(source_filelist);
	pgFileFree(source_pg_control_file);
}

/*
 * Catchup file copier executed in separate threads
 */
static void *
catchup_thread_runner(void *arg)
{
	int			i;
	char		from_fullpath[MAXPGPATH];
	char		to_fullpath[MAXPGPATH];

	catchup_thread_runner_arg *arguments = (catchup_thread_runner_arg *) arg;
	int 		n_files = parray_num(arguments->source_filelist);

	/* catchup a file */
	for (i = 0; i < n_files; i++)
	{
		pgFile	*file = (pgFile *) parray_get(arguments->source_filelist, i);
		pgFile	*dest_file = NULL;

		/* We have already copied all directories */
		if (S_ISDIR(file->mode))
			continue;

		if (!pg_atomic_test_set_flag(&file->lock))
			continue;

		/* check for interrupt */
		if (interrupted || thread_interrupted)
			elog(ERROR, "interrupted during catchup");

		if (progress)
			elog(INFO, "Progress: (%d/%d). Process file \"%s\"",
				 i + 1, n_files, file->rel_path);

		/* construct destination filepath */
		Assert(file->external_dir_num == 0);
		join_path_components(from_fullpath, arguments->from_root, file->rel_path);
		join_path_components(to_fullpath, arguments->to_root, file->rel_path);

		/* Encountered some strange beast */
		if (!S_ISREG(file->mode))
			elog(WARNING, "Unexpected type %d of file \"%s\", skipping",
							file->mode, from_fullpath);

		/* Check that file exist in dest pgdata */
		if (arguments->backup_mode != BACKUP_MODE_FULL)
		{
			pgFile	**dest_file_tmp = NULL;
			dest_file_tmp = (pgFile **) parray_bsearch(arguments->dest_filelist,
											file, pgFileCompareRelPathWithExternal);
			if (dest_file_tmp)
			{
				/* File exists in destination PGDATA */
				file->exists_in_prev = true;
				dest_file = *dest_file_tmp;
			}
		}

		/* Do actual work */
		if (file->is_datafile && !file->is_cfs)
		{
			catchup_data_file(file, from_fullpath, to_fullpath,
								 arguments->sync_lsn,
								 arguments->backup_mode,
								 NONE_COMPRESS,
								 0,
								 arguments->nodeInfo->checksum_version,
								 arguments->nodeInfo->ptrack_version_num,
								 arguments->nodeInfo->ptrack_schema,
								 false);
		}
		else
		{
			backup_non_data_file(file, dest_file, from_fullpath, to_fullpath,
								 arguments->backup_mode, current.parent_backup, true);
		}

		if (file->write_size == FILE_NOT_FOUND)
			continue;

		if (file->write_size == BYTES_INVALID)
		{
			elog(VERBOSE, "Skipping the unchanged file: \"%s\", read %li bytes", from_fullpath, file->read_size);
			continue;
		}

		elog(VERBOSE, "File \"%s\". Copied "INT64_FORMAT " bytes",
						from_fullpath, file->write_size);
	}

	/* ssh connection to longer needed */
	fio_disconnect();

	/* Data files transferring is successful */
	arguments->ret = 0;

	return NULL;
}
