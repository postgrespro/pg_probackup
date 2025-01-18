/*-------------------------------------------------------------------------
 *
 * catchup.c: sync DB cluster
 *
 * Copyright (c) 2021-2025, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#if PG_VERSION_NUM < 110000
#include "catalog/catalog.h"
#endif
#include "catalog/pg_tablespace.h"
#include "access/timeline.h"
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
static PGconn *catchup_init_state(PGNodeInfo *source_node_info, const char *source_pgdata, const char *dest_pgdata);
static void catchup_preflight_checks(PGNodeInfo *source_node_info, PGconn *source_conn, const char *source_pgdata, 
					const char *dest_pgdata);
static void catchup_check_tablespaces_existance_in_tbsmapping(PGconn *conn);
static parray* catchup_get_tli_history(ConnectionOptions *conn_opt, TimeLineID tli);

//REVIEW I'd also suggest to wrap all these fields into some CatchupState, but it isn't urgent.
//REVIEW_ANSWER what for?
/*
 * Prepare for work: fill some globals, open connection to source database
 */
static PGconn *
catchup_init_state(PGNodeInfo	*source_node_info, const char *source_pgdata, const char *dest_pgdata)
{
	PGconn		*source_conn;

	/* Initialize PGInfonode */
	pgNodeInit(source_node_info);

	/* Get WAL segments size and system ID of source PG instance */
	instance_config.xlog_seg_size = get_xlog_seg_size(source_pgdata);
	instance_config.system_identifier = get_system_identifier(source_pgdata, FIO_DB_HOST, false);
	current.start_time = time(NULL);

	strlcpy(current.program_version, PROGRAM_VERSION, sizeof(current.program_version));

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
			"remote: %s, source-pgdata: %s, destination-pgdata: %s",
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
			elog(ERROR, "\"%s\" is empty, but incremental catchup mode requested.",
				dest_pgdata);
	}
	else /* dest dir not empty */
	{
		if (current.backup_mode == BACKUP_MODE_FULL)
			elog(ERROR, "Can't perform full catchup into non-empty directory \"%s\".",
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

	/* check backup_label absence in dest */
	if (current.backup_mode != BACKUP_MODE_FULL)
	{
		char	backup_label_filename[MAXPGPATH];

		join_path_components(backup_label_filename, dest_pgdata, PG_BACKUP_LABEL_FILE);
		if (fio_access(backup_label_filename, F_OK, FIO_LOCAL_HOST) == 0)
			elog(ERROR, "Destination directory contains \"" PG_BACKUP_LABEL_FILE "\" file");
	}

	/* Check that connected PG instance, source and destination PGDATA are the same */
	{
		uint64	source_conn_id, source_id, dest_id;

		source_conn_id = get_remote_system_identifier(source_conn);
		source_id = get_system_identifier(source_pgdata, FIO_DB_HOST, false); /* same as instance_config.system_identifier */

		if (source_conn_id != source_id)
			elog(ERROR, "Database identifiers mismatch: we connected to DB id %lu, but in \"%s\" we found id %lu",
				source_conn_id, source_pgdata, source_id);

		if (current.backup_mode != BACKUP_MODE_FULL)
		{
			ControlFileData dst_control;
			get_control_file_or_back_file(dest_pgdata, FIO_LOCAL_HOST, &dst_control);
			dest_id = dst_control.system_identifier;

			if (source_conn_id != dest_id)
			elog(ERROR, "Database identifiers mismatch: we connected to DB id %llu, but in \"%s\" we found id %llu",
				(long long)source_conn_id, dest_pgdata, (long long)dest_id);
		}
	}

	/* check PTRACK version */
	if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		if (source_node_info->ptrack_version_num == 0)
			elog(ERROR, "This PostgreSQL instance does not support ptrack");
		else if (source_node_info->ptrack_version_num < 200)
			elog(ERROR, "Ptrack extension is too old.\n"
					"Upgrade ptrack to version >= 2");
		else if (!source_node_info->is_ptrack_enabled)
			elog(ERROR, "Ptrack is disabled");
	}

	if (current.from_replica && exclusive_backup)
		elog(ERROR, "Catchup from standby is only available for PostgreSQL >= 9.6");

	/* check that we don't overwrite tablespace in source pgdata */
	catchup_check_tablespaces_existance_in_tbsmapping(source_conn);

	/* check timelines */
	if (current.backup_mode != BACKUP_MODE_FULL)
	{
		RedoParams	dest_redo = { 0, InvalidXLogRecPtr, 0 };

		/* fill dest_redo.lsn and dest_redo.tli */
		get_redo(dest_pgdata, FIO_LOCAL_HOST, &dest_redo);
		elog(LOG, "source.tli = %X, dest_redo.lsn = %X/%X, dest_redo.tli = %X",
			current.tli, (uint32) (dest_redo.lsn >> 32), (uint32) dest_redo.lsn, dest_redo.tli);

		if (current.tli != 1)
		{
			parray	*source_timelines; /* parray* of TimeLineHistoryEntry* */
			source_timelines = catchup_get_tli_history(&instance_config.conn_opt, current.tli);

			if (source_timelines == NULL)
				elog(ERROR, "Cannot get source timeline history");

			if (!satisfy_timeline(source_timelines, dest_redo.tli, dest_redo.lsn))
				elog(ERROR, "Destination is not in source timeline history");

			parray_walk(source_timelines, pfree);
			parray_free(source_timelines);
		}
		else /* special case -- no history files in source */
		{
			if (dest_redo.tli != 1)
				elog(ERROR, "Source is behind destination in timeline history");
		}
	}
}

/*
 * Check that all tablespaces exists in tablespace mapping (--tablespace-mapping option)
 * Check that all local mapped directories is empty if it is local FULL catchup
 * Emit fatal error if that (not existent in map or not empty) tablespace found
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
		linked_path = get_tablespace_mapping(tablespace_path);

		if (strcmp(tablespace_path, linked_path) == 0)
		/* same result -> not found in mapping */
		{
			if (!fio_is_remote(FIO_DB_HOST))
				elog(ERROR, "Local catchup executed, but source database contains "
					"tablespace (\"%s\"), that is not listed in the map", tablespace_path);
			else
				elog(WARNING, "Remote catchup executed and source database contains "
					"tablespace (\"%s\"), that is not listed in the map", tablespace_path);
		}

		if (!is_absolute_path(linked_path))
			elog(ERROR, "Tablespace directory path must be an absolute path: \"%s\"",
				linked_path);

		if (current.backup_mode == BACKUP_MODE_FULL
				&& !dir_is_empty(linked_path, FIO_LOCAL_HOST))
			elog(ERROR, "Target mapped tablespace directory (\"%s\") is not empty in FULL catchup",
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
	PGresult             *res;
	PGconn	             *conn;
	char                 *history;
	char                  query[128];
	parray	             *result = NULL;
	TimeLineHistoryEntry *entry = NULL;

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

	/* append last timeline entry (as read_timeline_history() do) */
	entry = pgut_new(TimeLineHistoryEntry);
	entry->tli = tli;
	entry->end = InvalidXLogRecPtr;
	parray_insert(result, 0, entry);

	return result;
}

/*
 * catchup multithreaded copy rountine and helper structure and function
 */

/* parameters for catchup_thread_runner() passed from catchup_multithreaded_copy() */
typedef struct
{
	PGNodeInfo *nodeInfo;
	const char *from_root;
	const char *to_root;
	parray	   *source_filelist;
	parray	   *dest_filelist;
	XLogRecPtr	sync_lsn;
	BackupMode	backup_mode;
	int	thread_num;
	size_t	transfered_bytes;
	bool	completed;
} catchup_thread_runner_arg;

/* Catchup file copier executed in separate thread */
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

		if (file->excluded)
			continue;

		if (!pg_atomic_test_set_flag(&file->lock))
			continue;

		/* check for interrupt */
		if (interrupted || thread_interrupted)
			elog(ERROR, "Interrupted during catchup");

		elog(progress ? INFO : LOG, "Progress: (%d/%d). Process file \"%s\"",
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
								 arguments->nodeInfo->checksum_version,
								 dest_file != NULL ? dest_file->size : 0);
		}
		else
		{
			backup_non_data_file(file, dest_file, from_fullpath, to_fullpath,
								 arguments->backup_mode, current.parent_backup, true);
		}

		/* file went missing during catchup */
		if (file->write_size == FILE_NOT_FOUND)
			continue;

		if (file->write_size == BYTES_INVALID)
		{
			elog(LOG, "Skipping the unchanged file: \"%s\", read %li bytes", from_fullpath, file->read_size);
			continue;
		}

		arguments->transfered_bytes += file->write_size;
		elog(LOG, "File \"%s\". Copied "INT64_FORMAT " bytes",
						from_fullpath, file->write_size);
	}

	/* ssh connection to longer needed */
	fio_disconnect();

	/* Data files transferring is successful */
	arguments->completed = true;

	return NULL;
}

/*
 * main multithreaded copier
 * returns size of transfered data file
 * or -1 in case of error
 */
static ssize_t
catchup_multithreaded_copy(int num_threads,
	PGNodeInfo *source_node_info,
	const char *source_pgdata_path,
	const char *dest_pgdata_path,
	parray	   *source_filelist,
	parray	   *dest_filelist,
	XLogRecPtr	sync_lsn,
	BackupMode	backup_mode)
{
	/* arrays with meta info for multi threaded catchup */
	catchup_thread_runner_arg *threads_args;
	pthread_t	*threads;

	bool all_threads_successful = true;
	ssize_t transfered_bytes_result = 0;
	int	i;

	/* init thread args */
	threads_args = (catchup_thread_runner_arg *) palloc(sizeof(catchup_thread_runner_arg) * num_threads);
	for (i = 0; i < num_threads; i++)
		threads_args[i] = (catchup_thread_runner_arg){
			.nodeInfo = source_node_info,
			.from_root = source_pgdata_path,
			.to_root = dest_pgdata_path,
			.source_filelist = source_filelist,
			.dest_filelist = dest_filelist,
			.sync_lsn = sync_lsn,
			.backup_mode = backup_mode,
			.thread_num = i + 1,
			.transfered_bytes = 0,
			.completed = false,
		};

	/* Run threads */
	thread_interrupted = false;
	threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
	if (!dry_run)
	{
		for (i = 0; i < num_threads; i++)
		{
			elog(VERBOSE, "Start thread num: %i", i);
			pthread_create(&threads[i], NULL, &catchup_thread_runner, &(threads_args[i]));
		}
	}

	/* Wait threads */
	for (i = 0; i < num_threads; i++)
	{
		if (!dry_run)
			pthread_join(threads[i], NULL);
		all_threads_successful &= threads_args[i].completed;
		transfered_bytes_result += threads_args[i].transfered_bytes;
	}

	free(threads);
	free(threads_args);
	return all_threads_successful ? transfered_bytes_result : -1;
}

/*
 * Sync every file in destination directory to disk
 */
static void
catchup_sync_destination_files(const char* pgdata_path, fio_location location, parray *filelist, pgFile *pg_control_file)
{
	char    fullpath[MAXPGPATH];
	time_t	start_time, end_time;
	char	pretty_time[20];
	int	i;

	elog(INFO, "Syncing copied files to disk");
	time(&start_time);

	for (i = 0; i < parray_num(filelist); i++)
	{
		pgFile *file = (pgFile *) parray_get(filelist, i);

		/* TODO: sync directory ?
		 * - at first glance we can rely on fs journaling,
		 *   which is enabled by default on most platforms
		 * - but PG itself is not relying on fs, its durable_sync
		 *   includes directory sync
		 */
		if (S_ISDIR(file->mode) || file->excluded)
			continue;

		Assert(file->external_dir_num == 0);
		join_path_components(fullpath, pgdata_path, file->rel_path);
		if (fio_sync(fullpath, location) != 0)
			elog(ERROR, "Cannot sync file \"%s\": %s", fullpath, strerror(errno));
	}

	/*
	 * sync pg_control file
	 */
	join_path_components(fullpath, pgdata_path, pg_control_file->rel_path);
	if (fio_sync(fullpath, location) != 0)
		elog(ERROR, "Cannot sync file \"%s\": %s", fullpath, strerror(errno));

	time(&end_time);
	pretty_time_interval(difftime(end_time, start_time),
						 pretty_time, lengthof(pretty_time));
	elog(INFO, "Files are synced, time elapsed: %s", pretty_time);
}

/*
 * Filter filelist helper function (used to process --exclude-path's)
 * filelist -- parray of pgFile *, can't be NULL
 * exclude_absolute_paths_list -- sorted parray of char * (absolute paths, starting with '/'), can be NULL
 * exclude_relative_paths_list -- sorted parray of char * (relative paths), can be NULL
 * logging_string -- helper parameter, used for generating verbose log messages ("Source" or "Destination")
 */
static void
filter_filelist(parray *filelist, const char *pgdata,
	parray *exclude_absolute_paths_list, parray *exclude_relative_paths_list,
	const char *logging_string)
{
	int i;

	if (exclude_absolute_paths_list == NULL && exclude_relative_paths_list == NULL)
		return;

	for (i = 0; i < parray_num(filelist); ++i)
	{
		char	full_path[MAXPGPATH];
		pgFile *file = (pgFile *) parray_get(filelist, i);
		join_path_components(full_path, pgdata, file->rel_path);

		if (
			(exclude_absolute_paths_list != NULL
			&& parray_bsearch(exclude_absolute_paths_list, full_path, pgPrefixCompareString)!= NULL
			) || (
			exclude_relative_paths_list != NULL
			&& parray_bsearch(exclude_relative_paths_list, file->rel_path, pgPrefixCompareString)!= NULL)
			)
		{
			elog(INFO, "%s file \"%s\" excluded with --exclude-path option", logging_string, full_path);
			file->excluded = true;
		}
	}
}

/*
 * Entry point of pg_probackup CATCHUP subcommand.
 * exclude_*_paths_list are parray's of char *
 */
int
do_catchup(const char *source_pgdata, const char *dest_pgdata, int num_threads, bool sync_dest_files,
	parray *exclude_absolute_paths_list, parray *exclude_relative_paths_list)
{
	PGconn		*source_conn = NULL;
	PGNodeInfo	source_node_info;
	bool		backup_logs = false;
	parray	*source_filelist = NULL;
	pgFile	*source_pg_control_file = NULL;
	parray	*dest_filelist = NULL;
	char	dest_xlog_path[MAXPGPATH];

	RedoParams	dest_redo = { 0, InvalidXLogRecPtr, 0 };
	PGStopBackupResult	stop_backup_result;
	bool		catchup_isok = true;

	int			i;

	/* for fancy reporting */
	time_t		start_time, end_time;
	ssize_t		transfered_datafiles_bytes = 0;
	ssize_t		transfered_walfiles_bytes = 0;
	char		pretty_source_bytes[20];

	char	dest_pg_control_fullpath[MAXPGPATH];
	char	dest_pg_control_bak_fullpath[MAXPGPATH];

	source_conn = catchup_init_state(&source_node_info, source_pgdata, dest_pgdata);
	catchup_preflight_checks(&source_node_info, source_conn, source_pgdata, dest_pgdata);

	/* we need to sort --exclude_path's for future searching */
	if (exclude_absolute_paths_list != NULL)
		parray_qsort(exclude_absolute_paths_list, pgCompareString);
	if (exclude_relative_paths_list != NULL)
		parray_qsort(exclude_relative_paths_list, pgCompareString);

	elog(INFO, "Database catchup start");

	if (current.backup_mode != BACKUP_MODE_FULL)
	{
		dest_filelist = parray_new();
		dir_list_file(dest_filelist, dest_pgdata,
			true, true, false, backup_logs, true, 0, FIO_LOCAL_HOST);
		filter_filelist(dest_filelist, dest_pgdata, exclude_absolute_paths_list, exclude_relative_paths_list, "Destination");

		// fill dest_redo.lsn and dest_redo.tli
		get_redo(dest_pgdata, FIO_LOCAL_HOST, &dest_redo);
		elog(INFO, "syncLSN = %X/%X", (uint32) (dest_redo.lsn >> 32), (uint32) dest_redo.lsn);

		/*
		 * Future improvement to catch partial catchup:
		 *  1. rename dest pg_control into something like pg_control.pbk
		 *   (so user can't start partial catchup'ed instance from this point)
		 *  2. try to read by get_redo() pg_control and pg_control.pbk (to detect partial catchup)
		 *  3. at the end (after copy of correct pg_control), remove pg_control.pbk
		 */
	}

	/*
	 * Make sure that sync point is withing ptrack tracking range
	 * TODO: move to separate function to use in both backup.c and catchup.c
	 */
	if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		XLogRecPtr	ptrack_lsn = get_last_ptrack_lsn(source_conn, &source_node_info);

		if (ptrack_lsn > dest_redo.lsn || ptrack_lsn == InvalidXLogRecPtr)
			elog(ERROR, "LSN from ptrack_control in source %X/%X is greater than checkpoint LSN in destination %X/%X.\n"
						"You can perform only FULL catchup.",
						(uint32) (ptrack_lsn >> 32), (uint32) (ptrack_lsn),
						(uint32) (dest_redo.lsn >> 32),
						(uint32) (dest_redo.lsn));
	}

	{
		char		label[1024];
		/* notify start of backup to PostgreSQL server */
		time2iso(label, lengthof(label), current.start_time, false);
		strncat(label, " with pg_probackup", lengthof(label) -
				strlen(" with pg_probackup"));

		/* Call pg_start_backup function in PostgreSQL connect */
		pg_start_backup(label, smooth_checkpoint, &current, &source_node_info, source_conn);
		elog(INFO, "pg_start_backup START LSN %X/%X", (uint32) (current.start_lsn >> 32), (uint32) (current.start_lsn));
	}

	/* Sanity: source cluster must be "in future" relatively to dest cluster */
	if (current.backup_mode != BACKUP_MODE_FULL &&
		dest_redo.lsn > current.start_lsn)
			elog(ERROR, "Current START LSN %X/%X is lower than SYNC LSN %X/%X, "
				"it may indicate that we are trying to catchup with PostgreSQL instance from the past",
				(uint32) (current.start_lsn >> 32), (uint32) (current.start_lsn),
				(uint32) (dest_redo.lsn >> 32), (uint32) (dest_redo.lsn));

	/* Start stream replication */
	join_path_components(dest_xlog_path, dest_pgdata, PG_XLOG_DIR);
	if (!dry_run)
	{
		fio_mkdir(dest_xlog_path, DIR_PERMISSION, FIO_LOCAL_HOST);
		start_WAL_streaming(source_conn, dest_xlog_path, &instance_config.conn_opt,
										current.start_lsn, current.tli, false);
	}
	else
		elog(INFO, "WAL streaming skipping with --dry-run option");

	source_filelist = parray_new();

	/* list files with the logical path. omit $PGDATA */
	if (fio_is_remote(FIO_DB_HOST))
		fio_list_dir(source_filelist, source_pgdata,
					 true, true, false, backup_logs, true, 0);
	else
		dir_list_file(source_filelist, source_pgdata,
					  true, true, false, backup_logs, true, 0, FIO_LOCAL_HOST);

	//REVIEW FIXME. Let's fix that before release.
	// TODO what if wal is not a dir (symlink to a dir)?
	// - Currently backup/restore transform pg_wal symlink to directory
	//   so the problem is not only with catchup.
	//   if we want to make it right - we must provide the way
	//   for symlink remapping during restore and catchup.
	//   By default everything must be left as it is.

	/* close ssh session in main thread */
	fio_disconnect();

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

	//REVIEW Do we want to do similar calculation for dest?
	//REVIEW_ANSWER what for?
	{
		ssize_t	source_bytes = 0;
		char	pretty_bytes[20];

		source_bytes += calculate_datasize_of_filelist(source_filelist);

		/* Extract information about files in source_filelist parsing their names:*/
		parse_filelist_filenames(source_filelist, source_pgdata);
		filter_filelist(source_filelist, source_pgdata, exclude_absolute_paths_list, exclude_relative_paths_list, "Source");

		current.pgdata_bytes += calculate_datasize_of_filelist(source_filelist);

		pretty_size(current.pgdata_bytes, pretty_source_bytes, lengthof(pretty_source_bytes));
		pretty_size(source_bytes - current.pgdata_bytes, pretty_bytes, lengthof(pretty_bytes));
		elog(INFO, "Source PGDATA size: %s (excluded %s)", pretty_source_bytes, pretty_bytes);
	}

	elog(INFO, "Start LSN (source): %X/%X, TLI: %X",
			(uint32) (current.start_lsn >> 32), (uint32) (current.start_lsn),
			current.tli);
	if (current.backup_mode != BACKUP_MODE_FULL)
		elog(INFO, "LSN in destination: %X/%X, TLI: %X",
			 (uint32) (dest_redo.lsn >> 32), (uint32) (dest_redo.lsn),
			 dest_redo.tli);

	/* Build page mapping in PTRACK mode */
	if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		time(&start_time);
		elog(INFO, "Extracting pagemap of changed blocks");

		/* Build the page map from ptrack information */
		make_pagemap_from_ptrack_2(source_filelist, source_conn,
									source_node_info.ptrack_schema,
									source_node_info.ptrack_version_num,
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
	 * It means that we create directory specified in tablespace map and
	 * original directory created as symlink to it.
	 */
	for (i = 0; i < parray_num(source_filelist); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(source_filelist, i);
		char parent_dir[MAXPGPATH];

		if (!S_ISDIR(file->mode) || file->excluded)
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

			elog(LOG, "Create directory '%s'", dirpath);
			if (!dry_run)
				fio_mkdir(dirpath, DIR_PERMISSION, FIO_LOCAL_HOST);
		}
		else
		{
			/* this directory located in pg_tblspc */
			const char *linked_path = NULL;
			char	to_path[MAXPGPATH];

			// TODO perform additional check that this is actually symlink?
			{ /* get full symlink path and map this path to new location */
				char	source_full_path[MAXPGPATH];
				char	symlink_content[MAXPGPATH];
				join_path_components(source_full_path, source_pgdata, file->rel_path);
				fio_readlink(source_full_path, symlink_content, sizeof(symlink_content), FIO_DB_HOST);
				/* we checked that mapping exists in preflight_checks for local catchup */
				linked_path = get_tablespace_mapping(symlink_content);
				elog(INFO, "Map tablespace full_path: \"%s\" old_symlink_content: \"%s\" new_symlink_content: \"%s\"\n",
					source_full_path,
					symlink_content,
					linked_path);
			}

			if (!is_absolute_path(linked_path))
				elog(ERROR, "Tablespace directory path must be an absolute path: %s\n",
						 linked_path);

			join_path_components(to_path, dest_pgdata, file->rel_path);

			elog(INFO, "Create directory \"%s\" and symbolic link \"%s\"",
					 linked_path, to_path);

			if (!dry_run)
			{
				/* create tablespace directory */
				if (fio_mkdir(linked_path, file->mode, FIO_LOCAL_HOST) != 0)
					elog(ERROR, "Could not create tablespace directory \"%s\": %s",
						 linked_path, strerror(errno));

				/* create link to linked_path */
				if (fio_symlink(linked_path, to_path, true, FIO_LOCAL_HOST) < 0)
					elog(ERROR, "Could not create symbolic link \"%s\" -> \"%s\": %s",
						 linked_path, to_path, strerror(errno));
			}
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

	/* TODO before public release: must be more careful with pg_control.
	 *       when running catchup or incremental restore
	 *       cluster is actually in two states
	 *       simultaneously - old and new, so
	 *       it must contain both pg_control files
	 *       describing those states: global/pg_control_old, global/pg_control_new
	 *       1. This approach will provide us with means of
	 *          robust detection of previos failures and thus correct operation retrying (or forbidding).
	 *       2. We will have the ability of preventing instance from starting
	 *          in the middle of our operations.
	 */

	/*
	 * remove absent source files in dest (dropped tables, etc...)
	 * note: global/pg_control will also be deleted here
	 * mark dest files (that excluded with source --exclude-path) also for exclusion
	 */
	if (current.backup_mode != BACKUP_MODE_FULL)
	{
		elog(INFO, "Removing redundant files in destination directory");
		parray_qsort(dest_filelist, pgFileCompareRelPathWithExternalDesc);
		for (i = 0; i < parray_num(dest_filelist); i++)
		{
			bool     redundant = true;
			pgFile	*file = (pgFile *) parray_get(dest_filelist, i);
			pgFile	**src_file = NULL;

			//TODO optimize it and use some merge-like algorithm
			//instead of bsearch for each file.
			src_file = (pgFile **) parray_bsearch(source_filelist, file, pgFileCompareRelPathWithExternal);

			if (src_file!= NULL && !(*src_file)->excluded && file->excluded)
				(*src_file)->excluded = true;

			if (src_file!= NULL || file->excluded)
				redundant = false;

			/* pg_filenode.map are always copied, because it's crc cannot be trusted */
			Assert(file->external_dir_num == 0);
			if (pg_strcasecmp(file->name, RELMAPPER_FILENAME) == 0)
				redundant = true;
			/* global/pg_control.pbk.bak is always keeped, because it's needed for restart failed incremental restore */
			if (pg_strcasecmp(file->rel_path, XLOG_CONTROL_BAK_FILE) == 0)
				redundant = false;

			/* if file does not exists in destination list, then we can safely unlink it */
			if (redundant)
			{
				char		fullpath[MAXPGPATH];

				join_path_components(fullpath, dest_pgdata, file->rel_path);
				if (!dry_run)
				{
					fio_delete(file->mode, fullpath, FIO_LOCAL_HOST);
				}
				elog(LOG, "Deleted file \"%s\"", fullpath);

				/* shrink dest pgdata list */
				pgFileFree(file);
				parray_remove(dest_filelist, i);
				i--;
			}
		}
	}

	/* clear file locks */
	pfilearray_clear_locks(source_filelist);

	/* Sort by size for load balancing */
	parray_qsort(source_filelist, pgFileCompareSizeDesc);

	/* Sort the array for binary search */
	if (dest_filelist)
		parray_qsort(dest_filelist, pgFileCompareRelPathWithExternal);

	join_path_components(dest_pg_control_fullpath, dest_pgdata, XLOG_CONTROL_FILE);
	join_path_components(dest_pg_control_bak_fullpath, dest_pgdata, XLOG_CONTROL_BAK_FILE);
	/*
	 * rename (if it exist) dest control file before restoring
	 * if it doesn't exist, that mean, that we already restoring in a previously failed
	 * pgdata, where XLOG_CONTROL_BAK_FILE exist
	 */
	if (current.backup_mode != BACKUP_MODE_FULL && !dry_run)
	{
		if (!fio_access(dest_pg_control_fullpath, F_OK, FIO_LOCAL_HOST))
		{
			pgFile *dst_control;
			dst_control = pgFileNew(dest_pg_control_bak_fullpath, XLOG_CONTROL_BAK_FILE,
			true,0, FIO_BACKUP_HOST);

			if(!fio_access(dest_pg_control_bak_fullpath, F_OK, FIO_LOCAL_HOST))
				fio_delete(dst_control->mode, dest_pg_control_bak_fullpath, FIO_LOCAL_HOST);
			fio_rename(dest_pg_control_fullpath, dest_pg_control_bak_fullpath, FIO_LOCAL_HOST);
			pgFileFree(dst_control);
		}
	}

	/* run copy threads */
	elog(INFO, "Start transferring data files");
	time(&start_time);
	transfered_datafiles_bytes = catchup_multithreaded_copy(num_threads, &source_node_info,
		source_pgdata, dest_pgdata,
		source_filelist, dest_filelist,
		dest_redo.lsn, current.backup_mode);
	catchup_isok = transfered_datafiles_bytes != -1;

	/* at last copy control file */
	if (catchup_isok && !dry_run)
	{
		char	from_fullpath[MAXPGPATH];
		char	to_fullpath[MAXPGPATH];
		join_path_components(from_fullpath, source_pgdata, source_pg_control_file->rel_path);
		join_path_components(to_fullpath, dest_pgdata, source_pg_control_file->rel_path);
		copy_pgcontrol_file(from_fullpath, FIO_DB_HOST,
				to_fullpath, FIO_LOCAL_HOST, source_pg_control_file);
		transfered_datafiles_bytes += source_pg_control_file->size;

		/* Now backup control file can be deled */
		if (current.backup_mode != BACKUP_MODE_FULL && !fio_access(dest_pg_control_bak_fullpath, F_OK, FIO_LOCAL_HOST)){
			pgFile *dst_control;
			dst_control = pgFileNew(dest_pg_control_bak_fullpath, XLOG_CONTROL_BAK_FILE,
			true,0, FIO_BACKUP_HOST);
			fio_delete(dst_control->mode, dest_pg_control_bak_fullpath, FIO_LOCAL_HOST);
			pgFileFree(dst_control);
		}
	}

	if (!catchup_isok && !dry_run)
	{
		char	pretty_time[20];
		char	pretty_transfered_data_bytes[20];

		time(&end_time);
		pretty_time_interval(difftime(end_time, start_time),
						 pretty_time, lengthof(pretty_time));
		pretty_size(transfered_datafiles_bytes, pretty_transfered_data_bytes, lengthof(pretty_transfered_data_bytes));

		elog(ERROR, "Catchup failed. Transfered: %s, time elapsed: %s",
			pretty_transfered_data_bytes, pretty_time);
	}

	/* Notify end of backup */
	{
		//REVIEW Is it relevant to catchup? I suppose it isn't, since catchup is a new code.
		//If we do need it, please write a comment explaining that.
		/* kludge against some old bug in archive_timeout. TODO: remove in 3.0.0 */
		int	     timeout = (instance_config.archive_timeout > 0) ?
					instance_config.archive_timeout : ARCHIVE_TIMEOUT_DEFAULT;
		char    *stop_backup_query_text = NULL;

		pg_silent_client_messages(source_conn);

		/* Execute pg_stop_backup using PostgreSQL connection */
		pg_stop_backup_send(source_conn, source_node_info.server_version, current.from_replica, exclusive_backup, &stop_backup_query_text);

		/*
		 * Wait for the result of pg_stop_backup(), but no longer than
		 * archive_timeout seconds
		 */
		pg_stop_backup_consume(source_conn, source_node_info.server_version, exclusive_backup, timeout, stop_backup_query_text, &stop_backup_result);

		/* Cleanup */
		pg_free(stop_backup_query_text);
	}

	if (!dry_run)
		wait_wal_and_calculate_stop_lsn(dest_xlog_path, stop_backup_result.lsn, &current);

#if PG_VERSION_NUM >= 90600
	/* Write backup_label */
	Assert(stop_backup_result.backup_label_content != NULL);
	if (!dry_run)
	{
		pg_stop_backup_write_file_helper(dest_pgdata, PG_BACKUP_LABEL_FILE, "backup label",
			stop_backup_result.backup_label_content, stop_backup_result.backup_label_content_len,
			NULL);
	}
	free(stop_backup_result.backup_label_content);
	stop_backup_result.backup_label_content = NULL;
	stop_backup_result.backup_label_content_len = 0;

	/* tablespace_map */
	if (stop_backup_result.tablespace_map_content != NULL)
	{
		// TODO what if tablespace is created during catchup?
		/* Because we have already created symlinks in pg_tblspc earlier,
		 * we do not need to write the tablespace_map file.
		 * So this call is unnecessary:
		 * pg_stop_backup_write_file_helper(dest_pgdata, PG_TABLESPACE_MAP_FILE, "tablespace map",
		 *	stop_backup_result.tablespace_map_content, stop_backup_result.tablespace_map_content_len,
		 *	NULL);
		 */
		free(stop_backup_result.tablespace_map_content);
		stop_backup_result.tablespace_map_content = NULL;
		stop_backup_result.tablespace_map_content_len = 0;
	}
#endif

	/* wait for end of wal streaming and calculate wal size transfered */
	if (!dry_run)
	{
		parray *wal_files_list = NULL;
		wal_files_list = parray_new();

		if (wait_WAL_streaming_end(wal_files_list))
			elog(ERROR, "WAL streaming failed");

		for (i = 0; i < parray_num(wal_files_list); i++)
		{
			pgFile *file = (pgFile *) parray_get(wal_files_list, i);
			transfered_walfiles_bytes += file->size;
		}

		parray_walk(wal_files_list, pgFileFree);
		parray_free(wal_files_list);
		wal_files_list = NULL;
	}

	/*
	 * In case of backup from replica >= 9.6 we must fix minRecPoint
	 */
	if (current.from_replica && !exclusive_backup)
	{
		set_min_recovery_point(source_pg_control_file, dest_pgdata, current.stop_lsn);
	}

	/* close ssh session in main thread */
	fio_disconnect();

	/* fancy reporting */
	{
		char	pretty_transfered_data_bytes[20];
		char	pretty_transfered_wal_bytes[20];
		char	pretty_time[20];

		time(&end_time);
		pretty_time_interval(difftime(end_time, start_time),
							 pretty_time, lengthof(pretty_time));
		pretty_size(transfered_datafiles_bytes, pretty_transfered_data_bytes, lengthof(pretty_transfered_data_bytes));
		pretty_size(transfered_walfiles_bytes, pretty_transfered_wal_bytes, lengthof(pretty_transfered_wal_bytes));

		elog(INFO, "Databases synchronized. Transfered datafiles size: %s, transfered wal size: %s, time elapsed: %s",
			pretty_transfered_data_bytes, pretty_transfered_wal_bytes, pretty_time);

		if (current.backup_mode != BACKUP_MODE_FULL)
			elog(INFO, "Catchup incremental ratio (less is better): %.f%% (%s/%s)",
				((float) transfered_datafiles_bytes / current.pgdata_bytes) * 100,
				pretty_transfered_data_bytes, pretty_source_bytes);
	}

	/* Sync all copied files unless '--no-sync' flag is used */
	if (sync_dest_files && !dry_run)
		catchup_sync_destination_files(dest_pgdata, FIO_LOCAL_HOST, source_filelist, source_pg_control_file);
	else
		elog(WARNING, "Files are not synced to disk");

	/* Cleanup */
	if (dest_filelist && !dry_run)
	{
		parray_walk(dest_filelist, pgFileFree);
	}
	parray_free(dest_filelist);
	parray_walk(source_filelist, pgFileFree);
	parray_free(source_filelist);
	pgFileFree(source_pg_control_file);

	return 0;
}
