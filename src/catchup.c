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
static PGconn *catchup_collect_info(PGNodeInfo *source_node_info, const char *source_pgdata, const char *dest_pgdata,
		BackupMode backup_mode, ConnectionOptions conn_opt);
static void catchup_preflight_checks(PGNodeInfo *source_node_info, PGconn *source_conn,
		const char *source_pgdata, BackupMode backup_mode);
static void do_catchup_instance(const char *source_pgdata, const char *dest_pgdata, PGconn *source_conn,
					PGNodeInfo *nodeInfo, BackupMode backup_mode, bool no_sync, bool backup_logs,
					bool dest_pgdata_is_empty);
static void *catchup_thread_runner(void *arg);

/*
 * Entry point of pg_probackup CATCHUP subcommand.
 */
int
do_catchup(const char *source_pgdata, const char *dest_pgdata, BackupMode backup_mode,
		   ConnectionOptions conn_opt, int num_threads)
{
	PGconn		*source_conn = NULL;
	PGNodeInfo	source_node_info;
	bool		no_sync = false;
	bool		backup_logs = false;
	bool        dest_pgdata_is_empty = dir_is_empty(dest_pgdata, FIO_LOCAL_HOST);

	source_conn = catchup_collect_info(&source_node_info, source_pgdata, dest_pgdata, backup_mode, conn_opt);
	catchup_preflight_checks(&source_node_info, source_conn, source_pgdata, backup_mode);

	if (!dest_pgdata_is_empty &&
		 check_incremental_compatibility(dest_pgdata,
										  instance_config.system_identifier,
										  INCR_CHECKSUM) != DEST_OK)
		elog(ERROR, "Incremental restore is not allowed");

	if (current.from_replica && exclusive_backup)
		elog(ERROR, "Catchup from standby is available only for PG >= 9.6");

	do_catchup_instance(source_pgdata, dest_pgdata, source_conn, &source_node_info,
						backup_mode, no_sync, backup_logs, dest_pgdata_is_empty);

	/* TODO: show the amount of transfered data in bytes and calculate incremental ratio */

	return 0;
}

static PGconn *
catchup_collect_info(PGNodeInfo	*source_node_info, const char *source_pgdata, const char *dest_pgdata,
		BackupMode backup_mode, ConnectionOptions conn_opt)
{
	PGconn		*source_conn;
	/* Initialize PGInfonode */
	pgNodeInit(source_node_info);

	/* Get WAL segments size and system ID of source PG instance */
	instance_config.xlog_seg_size = get_xlog_seg_size(source_pgdata);
	instance_config.system_identifier = get_system_identifier(source_pgdata);
	current.start_time = time(NULL);

	StrNCpy(current.program_version, PROGRAM_VERSION, sizeof(current.program_version));
	//current.compress_alg = instance_config.compress_alg;
	//current.compress_level = instance_config.compress_level;

	/* Do some compatibility checks and fill basic info about PG instance */
	source_conn = pgdata_basic_setup(conn_opt, source_node_info);

	/* below perform checks specific for backup command */
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
	current.tli = get_current_timeline_from_control(false);
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

static void
catchup_preflight_checks(PGNodeInfo *source_node_info, PGconn *source_conn,
		const char *source_pgdata, BackupMode backup_mode)
{
	// TODO: add sanity check that source PGDATA is not empty

	/* Check that connected PG instance and source PGDATA are the same */
	check_system_identifiers(source_conn, source_pgdata);

	if (backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		if (source_node_info->ptrack_version_num == 0)
			elog(ERROR, "This PostgreSQL instance does not support ptrack");
		else if (source_node_info->ptrack_version_num < 20)
			elog(ERROR, "ptrack extension is too old.\n"
					"Upgrade ptrack to version >= 2");
		else if (!source_node_info->is_ptrack_enabled)
			elog(ERROR, "Ptrack is disabled");
	}
}

/*
 * TODO:
 *  - add description
 *  - fallback to FULL mode if dest PGDATA is empty
 */
static void
do_catchup_instance(const char *source_pgdata, const char *dest_pgdata, PGconn *source_conn,
					PGNodeInfo *source_node_info, BackupMode backup_mode, bool no_sync, bool backup_logs,
					bool dest_pgdata_is_empty)
{
	int			i;
	char		dest_xlog_path[MAXPGPATH];
	char		label[1024];
	XLogRecPtr	sync_lsn = InvalidXLogRecPtr;

	/* arrays with meta info for multi threaded backup */
	pthread_t	*threads;
	catchup_thread_runner_arg *threads_args;
	bool		catchup_isok = true;

	parray     *source_filelist = NULL;
	parray	   *dest_filelist = NULL;
	parray	   *external_dirs = NULL;

	/* TODO: in case of timeline mistmatch, check that source PG timeline descending from dest PG timeline */
	parray       *tli_list = NULL;

	/* for fancy reporting */
	time_t		start_time, end_time;
	char		pretty_time[20];
	char		pretty_bytes[20];

	elog(LOG, "Database catchup start");

	/* notify start of backup to PostgreSQL server */
	time2iso(label, lengthof(label), current.start_time, false);
	strncat(label, " with pg_probackup", lengthof(label) -
			strlen(" with pg_probackup"));

	/* Call pg_start_backup function in PostgreSQL connect */
	pg_start_backup(label, smooth_checkpoint, &current, source_node_info, source_conn);
	elog(LOG, "pg_start_backup START LSN %X/%X", (uint32) (current.start_lsn >> 32), (uint32) (current.start_lsn));

	if (!dest_pgdata_is_empty &&
		(backup_mode == BACKUP_MODE_DIFF_PTRACK ||
		 backup_mode == BACKUP_MODE_DIFF_DELTA))
	{
		RedoParams	dest_redo;

		dest_filelist = parray_new();
		dir_list_file(dest_filelist, dest_pgdata,
			true, true, false, backup_logs, true, 0, FIO_LOCAL_HOST);

		// fill dest_redo.lsn and dest_redo.tli
		get_redo(dest_pgdata, &dest_redo);

		sync_lsn = dest_redo.lsn;
		elog(INFO, "syncLSN = %X/%X", (uint32) (sync_lsn >> 32), (uint32) sync_lsn);
	}

	/*
	 * TODO: move to separate function to use in both backup.c and catchup.c
	 */
	if (backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		XLogRecPtr	ptrack_lsn = get_last_ptrack_lsn(source_conn, source_node_info);

		// new ptrack is more robust and checks Start LSN
		if (ptrack_lsn > sync_lsn || ptrack_lsn == InvalidXLogRecPtr)
			elog(ERROR, "LSN from ptrack_control %X/%X is greater than checkpoint LSN  %X/%X.\n"
						"Create new full backup before an incremental one.",
						(uint32) (ptrack_lsn >> 32), (uint32) (ptrack_lsn),
						(uint32) (sync_lsn >> 32),
						(uint32) (sync_lsn));
	}

	/* Check that sync_lsn is less than current.start_lsn */
	/* TODO это нужно? */
	if (backup_mode != BACKUP_MODE_FULL &&
		sync_lsn > current.start_lsn)
			elog(ERROR, "Current START LSN %X/%X is lower than SYNC LSN %X/%X, "
				"it may indicate that we are trying to catchup with PostgreSQL instance from the past",
				(uint32) (current.start_lsn >> 32), (uint32) (current.start_lsn),
				(uint32) (sync_lsn >> 32), (uint32) (sync_lsn));

	/* Start stream replication */
	if (stream_wal)
	{
		join_path_components(dest_xlog_path, dest_pgdata, PG_XLOG_DIR);
		fio_mkdir(dest_xlog_path, DIR_PERMISSION, FIO_BACKUP_HOST);
		start_WAL_streaming(source_conn, dest_xlog_path, &instance_config.conn_opt,
							current.start_lsn, current.tli);
	}

	/* initialize backup list */
	source_filelist = parray_new();

	/* list files with the logical path. omit $PGDATA */
	if (fio_is_remote(FIO_DB_HOST))
		fio_list_dir(source_filelist, source_pgdata,
					 true, true, false, backup_logs, true, 0);
	else
		dir_list_file(source_filelist, source_pgdata,
					  true, true, false, backup_logs, true, 0, FIO_LOCAL_HOST);

	/* close ssh session in main thread */
	fio_disconnect();

	/* Calculate pgdata_bytes
	 * TODO: move to separate function to use in both backup.c and catchup.c
	 */
	for (i = 0; i < parray_num(source_filelist); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(source_filelist, i);

		if (file->external_dir_num != 0)
			continue;

		if (S_ISDIR(file->mode))
		{
			current.pgdata_bytes += 4096;
			continue;
		}

		current.pgdata_bytes += file->size;
	}

	pretty_size(current.pgdata_bytes, pretty_bytes, lengthof(pretty_bytes));
	elog(INFO, "PGDATA size: %s", pretty_bytes);

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

	/* Extract information about files in backup_list parsing their names:*/
	parse_filelist_filenames(source_filelist, source_pgdata);

	elog(LOG, "Current Start LSN: %X/%X, TLI: %X",
			(uint32) (current.start_lsn >> 32), (uint32) (current.start_lsn),
			current.tli);
	/* TODO проверить, нужна ли проверка TLI */
	/*if (backup_mode != BACKUP_MODE_FULL)
		elog(LOG, "Parent Start LSN: %X/%X, TLI: %X",
			 (uint32) (sync_lsn >> 32), (uint32) (sync_lsn),
			 prev_backup->tli);
	*/

	/* Build page mapping in PTRACK mode */

	if (backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		time(&start_time);
		elog(INFO, "Extracting pagemap of changed blocks");

		/* Build the page map from ptrack information */
		make_pagemap_from_ptrack_2(source_filelist, source_conn,
								   source_node_info->ptrack_schema,
								   source_node_info->ptrack_version_num,
								   sync_lsn);
		time(&end_time);
		elog(INFO, "Pagemap successfully extracted, time elapsed: %.0f sec",
			 difftime(end_time, start_time));
	}

	/*
	 * Make directories before catchup and setup threads at the same time
	 */
	for (i = 0; i < parray_num(source_filelist); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(source_filelist, i);

		/* if the entry was a directory, create it in the backup */
		if (S_ISDIR(file->mode))
		{
			char		dirpath[MAXPGPATH];

			if (file->external_dir_num)
			{
				char		temp[MAXPGPATH];
				/* TODO пока непонятно, разобраться! */
				/* snprintf(temp, MAXPGPATH, "%s%d", external_prefix,
						 file->external_dir_num); */
				join_path_components(dirpath, temp, file->rel_path);
			}
			else
				join_path_components(dirpath, dest_pgdata, file->rel_path);

			elog(VERBOSE, "Create directory '%s'", dirpath);
			fio_mkdir(dirpath, DIR_PERMISSION, FIO_BACKUP_HOST);
		}

		/* setup threads */
		pg_atomic_clear_flag(&file->lock);
	}

	/* Sort by size for load balancing */
	parray_qsort(source_filelist, pgFileCompareSize);

	/* init thread args with own file lists */
	threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
	threads_args = (catchup_thread_runner_arg *) palloc(sizeof(catchup_thread_runner_arg)*num_threads);

	for (i = 0; i < num_threads; i++)
	{
		catchup_thread_runner_arg *arg = &(threads_args[i]);

		arg->nodeInfo = source_node_info;
		arg->from_root = source_pgdata;
		arg->to_root = dest_pgdata;
		arg->source_filelist = source_filelist;
		arg->dest_filelist = dest_filelist;
		arg->sync_lsn = sync_lsn;
		arg->backup_mode = backup_mode;
		arg->thread_num = i+1;
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

	time(&end_time);
	pretty_time_interval(difftime(end_time, start_time),
						 pretty_time, lengthof(pretty_time));
	if (catchup_isok)
		elog(INFO, "Data files are transferred, time elapsed: %s",
			pretty_time);
	else
		elog(ERROR, "Data files transferring failed, time elapsed: %s",
			pretty_time);

	/* Notify end of backup */
	//!!!!!
	//catchup_pg_stop_backup(&current, source_conn, source_node_info, dest_pgdata);

/*
 * Notify end of backup to PostgreSQL server.
 */
//static void
//catchup_pg_stop_backup(pgBackup *backup, PGconn *pg_startbackup_conn, PGNodeInfo *source_node_info, const char *destination_dir)
//{
	PGStopBackupResult	stop_backup_result;
	/* kludge against some old bug in archive_timeout. TODO: remove in 3.0.0 */
	int	     timeout = (instance_config.archive_timeout > 0) ?
				instance_config.archive_timeout : ARCHIVE_TIMEOUT_DEFAULT;
	char    *query_text = NULL;

	pg_silent_client_messages(source_conn);

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
		backup_files_list);
	free(stop_backup_result.backup_label_content);
	stop_backup_result.backup_label_content = NULL;
	stop_backup_result.backup_label_content_len = 0;

	/* Write tablespace_map */
	if (stop_backup_result.tablespace_map_content != NULL)
	{
		pg_stop_backup_write_file_helper(dest_pgdata, PG_TABLESPACE_MAP_FILE, "tablespace map",
			stop_backup_result.tablespace_map_content, stop_backup_result.tablespace_map_content_len,
			backup_files_list);
		free(stop_backup_result.tablespace_map_content);
		stop_backup_result.tablespace_map_content = NULL;
		stop_backup_result.tablespace_map_content_len = 0;
	}

	/* This function will also add list of xlog files
	 * to the passed filelist */
	if(wait_WAL_streaming_end(backup_files_list))
		elog(ERROR, "WAL streaming failed");

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

	/* In case of backup from replica >= 9.6 we must fix minRecPoint,
	 * First we must find pg_control in source_filelist.
	 */
	if (current.from_replica && !exclusive_backup)
	{
		pgFile	   *pg_control = NULL;

		for (i = 0; i < parray_num(source_filelist); i++)
		{
			pgFile	   *tmp_file = (pgFile *) parray_get(source_filelist, i);

			if (tmp_file->external_dir_num == 0 &&
				(strcmp(tmp_file->rel_path, XLOG_CONTROL_FILE) == 0))
			{
				pg_control = tmp_file;
				break;
			}
		}

		if (!pg_control)
			elog(ERROR, "Failed to find file \"%s\" in backup filelist.",
							XLOG_CONTROL_FILE);

		set_min_recovery_point(pg_control, dest_pgdata, current.stop_lsn);
	}

	/* close ssh session in main thread */
	fio_disconnect();

	/* Sync all copied files unless '--no-sync' flag is used */
	if (no_sync)
		elog(WARNING, "Files are not synced to disk");
	else
	{
		elog(INFO, "Syncing copied files to disk");
		time(&start_time);

		for (i = 0; i < parray_num(source_filelist); i++)
		{
			char    to_fullpath[MAXPGPATH];
			pgFile *file = (pgFile *) parray_get(source_filelist, i);

			/* TODO: sync directory ? */
			if (S_ISDIR(file->mode))
				continue;

			if (file->write_size <= 0)
				continue;

			/* construct fullpath */
			if (file->external_dir_num == 0)
				join_path_components(to_fullpath, dest_pgdata, file->rel_path);
			/* TODO разобраться с external */
			/*else
			{
				char 	external_dst[MAXPGPATH];

				makeExternalDirPathByNum(external_dst, external_prefix,
										 file->external_dir_num);
				join_path_components(to_fullpath, external_dst, file->rel_path);
			}
			*/
			if (fio_sync(to_fullpath, FIO_BACKUP_HOST) != 0)
				elog(ERROR, "Cannot sync file \"%s\": %s", to_fullpath, strerror(errno));
		}

		time(&end_time);
		pretty_time_interval(difftime(end_time, start_time),
							 pretty_time, lengthof(pretty_time));
		elog(INFO, "Files are synced, time elapsed: %s", pretty_time);
	}

	/* Cleanup */
	if (!dest_pgdata_is_empty && dest_filelist)
	{
		parray_walk(dest_filelist, pgFileFree);
		parray_free(dest_filelist);
	}

	parray_walk(source_filelist, pgFileFree);
	parray_free(source_filelist);
	// где закрывается conn?
}

/*
 * TODO: add description
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
		/* TODO разобраться нужен ли external */
		if (file->external_dir_num == 0)
		{
			join_path_components(from_fullpath, arguments->from_root, file->rel_path);
			join_path_components(to_fullpath, arguments->to_root, file->rel_path);
		}
		/*else
		{
			char 	external_dst[MAXPGPATH];
			char	*external_path = parray_get(arguments->external_dirs,
												file->external_dir_num - 1);

			makeExternalDirPathByNum(external_dst,
								 arguments->external_prefix,
								 file->external_dir_num);

			join_path_components(to_fullpath, external_dst, file->rel_path);
			join_path_components(from_fullpath, external_path, file->rel_path);
		}
		*/

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
			elog(VERBOSE, "Skipping the unchanged file: \"%s\"", from_fullpath);
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
