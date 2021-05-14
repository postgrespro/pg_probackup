/*-------------------------------------------------------------------------
 *
 * catchup.c: sync DB cluster
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2021, Postgres Professional
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
static void *catchup_files(void *arg);

static void do_catchup_instance(char *source_pgdata, char *dest_pgdata, PGconn *source_conn, PGNodeInfo *nodeInfo, BackupMode backup_mode, bool no_sync, bool backup_logs);

static void
do_catchup_instance(char *source_pgdata, char *dest_pgdata, PGconn *source_conn, PGNodeInfo *nodeInfo, BackupMode backup_mode, bool no_sync, bool backup_logs)
{
	int			i;
	//char		database_path[MAXPGPATH];
	//char		external_prefix[MAXPGPATH]; /* Temp value. Used as template */
	char		dst_xlog_path[MAXPGPATH];
	char		label[1024];
	/* XLogRecPtr	prev_backup_start_lsn = InvalidXLogRecPtr; */
	XLogRecPtr	sync_lsn = InvalidXLogRecPtr;
	XLogRecPtr	start_lsn;

	/* arrays with meta info for multi threaded backup */
	pthread_t	*threads;
	catchup_files_arg *threads_args;
	bool		backup_isok = true;

	/* pgBackup   *prev_backup = NULL; */
	parray	   *prev_backup_filelist = NULL;
	parray	   *backup_list = NULL;
	parray	   *external_dirs = NULL;

	/* used for multitimeline incremental backup */
	parray       *tli_list = NULL;

	/* for fancy reporting */
	time_t		start_time, end_time;
	char		pretty_time[20];
	char		pretty_bytes[20];

	elog(LOG, "Database catchup start");
	if(current.external_dir_str)
	{
		external_dirs = make_external_directory_list(current.external_dir_str,
													 false);
		check_external_for_tablespaces(external_dirs, source_conn);
	}

	/* Clear ptrack files for not PTRACK backups */
	if (backup_mode != BACKUP_MODE_DIFF_PTRACK && nodeInfo->is_ptrack_enable)
		pg_ptrack_clear(source_conn, nodeInfo->ptrack_version_num);

	/* notify start of backup to PostgreSQL server */
	time2iso(label, lengthof(label), current.start_time, false);
	strncat(label, " with pg_probackup", lengthof(label) -
			strlen(" with pg_probackup"));

	/* Call pg_start_backup function in PostgreSQL connect */
	pg_start_backup(label, smooth_checkpoint, backup_mode, current.from_replica, &start_lsn, nodeInfo, source_conn);
	elog(LOG, "pg_start_backup START LSN %X/%X", (uint32) (start_lsn >> 32), (uint32) (start_lsn));

	/* Obtain current timeline */
#if PG_VERSION_NUM >= 90600
	current.tli = get_current_timeline(source_conn);
#else
	current.tli = get_current_timeline_from_control(false);
#endif

	/* In PAGE mode or in ARCHIVE wal-mode wait for current segment */
	if (backup_mode == BACKUP_MODE_DIFF_PAGE ||!stream_wal)
		/*
		 * Do not wait start_lsn for stream backup.
		 * Because WAL streaming will start after pg_start_backup() in stream
		 * mode.
		 */
		wait_wal_lsn(start_lsn, true, current.tli, false, true, ERROR, false, arclog_path);

	if (backup_mode == BACKUP_MODE_DIFF_PAGE ||
		backup_mode == BACKUP_MODE_DIFF_PTRACK ||
		backup_mode == BACKUP_MODE_DIFF_DELTA)
	{
		prev_backup_filelist = parray_new();
                dir_list_file(prev_backup_filelist, dest_pgdata,
					true, true, false, backup_logs, true, 0, FIO_LOCAL_HOST);

		sync_lsn = get_min_recovery_point(dest_pgdata);
		elog(INFO, "syncLSN = %X/%X", (uint32) (sync_lsn >> 32), (uint32) sync_lsn);
	}

	/*
	 * It`s illegal to take PTRACK backup if LSN from ptrack_control() is not
	 * equal to start_lsn of previous backup.
	 */
	if (backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		XLogRecPtr	ptrack_lsn = get_last_ptrack_lsn(source_conn, nodeInfo);

		if (nodeInfo->ptrack_version_num < 20)
		{
			elog(ERROR, "ptrack extension is too old.\n"
					"Upgrade ptrack to version >= 2");
		}
		else
		{
			// new ptrack is more robust and checks Start LSN
			if (ptrack_lsn > sync_lsn || ptrack_lsn == InvalidXLogRecPtr)
			{
				elog(ERROR, "LSN from ptrack_control %X/%X is greater than checkpoint LSN  %X/%X.\n"
							"Create new full backup before an incremental one.",
							(uint32) (ptrack_lsn >> 32), (uint32) (ptrack_lsn),
							(uint32) (sync_lsn >> 32),
							(uint32) (sync_lsn));
			}
		}
	}

	/* For incremental backup check that start_lsn is not from the past
	 * Though it will not save us if PostgreSQL instance is actually
	 * restored STREAM backup.
	 */
	/* TODO это нужно? */
	if (backup_mode != BACKUP_MODE_FULL &&
		sync_lsn > start_lsn)
			elog(ERROR, "Current START LSN %X/%X is lower than START LSN %X/%X. "
				"It may indicate that we are trying to backup PostgreSQL instance from the past.",
				(uint32) (start_lsn >> 32), (uint32) (start_lsn),
				(uint32) (sync_lsn >> 32), (uint32) (sync_lsn));

	/* Update running backup meta with START LSN */
	//write_backup(&current, true);

	//pgBackupGetPath(&current, database_path, lengthof(database_path),
	//				DATABASE_DIR);
	//pgBackupGetPath(&current, external_prefix, lengthof(external_prefix),
	//				EXTERNAL_DIR);

	/* start stream replication */
	if (stream_wal)
	{
		instance_config.system_identifier = get_system_identifier(source_pgdata);
		join_path_components(dst_xlog_path, dest_pgdata, PG_XLOG_DIR);
		fio_mkdir(dst_xlog_path, DIR_PERMISSION, FIO_BACKUP_HOST);

		start_WAL_streaming(source_conn, dst_xlog_path, &instance_config.conn_opt,
							start_lsn, current.tli);
	}

	/* initialize backup list */
	backup_files_list = parray_new();

	/* list files with the logical path. omit $PGDATA */
	if (fio_is_remote(FIO_DB_HOST))
		fio_list_dir(backup_files_list, source_pgdata,
					 true, true, false, backup_logs, true, 0);
	else
		dir_list_file(backup_files_list, source_pgdata,
					  true, true, false, backup_logs, true, 0, FIO_LOCAL_HOST);

	/*
	 * Append to backup list all files and directories
	 * from external directory option
	 */
	if (external_dirs)
	{
		for (i = 0; i < parray_num(external_dirs); i++)
		{
			/* External dirs numeration starts with 1.
			 * 0 value is not external dir */
			if (fio_is_remote(FIO_DB_HOST))
				fio_list_dir(backup_files_list, parray_get(external_dirs, i),
							 false, true, false, false, true, i+1);
			else
				dir_list_file(backup_files_list, parray_get(external_dirs, i),
							  false, true, false, false, true, i+1, FIO_LOCAL_HOST);
		}
	}

	/* close ssh session in main thread */
	fio_disconnect();

	/* Sanity check for backup_files_list, thank you, Windows:
	 * https://github.com/postgrespro/pg_probackup/issues/48
	 */

	if (parray_num(backup_files_list) < 100)
		elog(ERROR, "PGDATA is almost empty. Either it was concurrently deleted or "
			"pg_probackup do not possess sufficient permissions to list PGDATA content");

	/* Calculate pgdata_bytes */
	for (i = 0; i < parray_num(backup_files_list); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(backup_files_list, i);

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
	parray_qsort(backup_files_list, pgFileCompareRelPathWithExternal);

	/* Extract information about files in backup_list parsing their names:*/
	parse_filelist_filenames(backup_files_list, source_pgdata);

	elog(LOG, "Current Start LSN: %X/%X, TLI: %X",
			(uint32) (start_lsn >> 32), (uint32) (start_lsn),
			current.tli);
	/* TODO проверить, нужна ли проверка TLI */
	/*if (backup_mode != BACKUP_MODE_FULL)
		elog(LOG, "Parent Start LSN: %X/%X, TLI: %X",
			 (uint32) (sync_lsn >> 32), (uint32) (sync_lsn),
			 prev_backup->tli);
	*/
	/*
	 * Build page mapping in incremental mode.
	 */

	if (backup_mode == BACKUP_MODE_DIFF_PAGE ||
		backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		bool pagemap_isok = true;

		time(&start_time);
		elog(INFO, "Extracting pagemap of changed blocks");

		if (backup_mode == BACKUP_MODE_DIFF_PAGE)
		{
			/*
			 * Build the page map. Obtain information about changed pages
			 * reading WAL segments present in archives up to the point
			 * where this backup has started.
			 */
			/* TODO page пока не поддерживается */
			/* pagemap_isok = extractPageMap(arclog_path, instance_config.xlog_seg_size,
						   sync_lsn, prev_backup->tli,
						   current.start_lsn, current.tli, tli_list);
			*/
		}
		else if (backup_mode == BACKUP_MODE_DIFF_PTRACK)
		{
			/*
			 * Build the page map from ptrack information.
			 */
			make_pagemap_from_ptrack_2(backup_files_list, source_conn,
									   nodeInfo->ptrack_schema,
									   nodeInfo->ptrack_version_num,
									   sync_lsn);
		}

		time(&end_time);

		/* TODO: add ms precision */
		if (pagemap_isok)
			elog(INFO, "Pagemap successfully extracted, time elapsed: %.0f sec",
				 difftime(end_time, start_time));
		else
			elog(ERROR, "Pagemap extraction failed, time elasped: %.0f sec",
				 difftime(end_time, start_time));
	}

	/*
	 * Make directories before backup and setup threads at the same time
	 */
	for (i = 0; i < parray_num(backup_files_list); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(backup_files_list, i);

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
	parray_qsort(backup_files_list, pgFileCompareSize);
	/* Sort the array for binary search */
	if (prev_backup_filelist)
		parray_qsort(prev_backup_filelist, pgFileCompareRelPathWithExternal);

	/* write initial backup_content.control file and update backup.control  */
	//write_backup_filelist(&current, backup_files_list,
	//					  instance_config.pgdata, external_dirs, true);
	//write_backup(&current, true);

	/* Init backup page header map */
	//init_header_map(&current);

	/* init thread args with own file lists */
	threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
	threads_args = (catchup_files_arg *) palloc(sizeof(catchup_files_arg)*num_threads);

	for (i = 0; i < num_threads; i++)
	{
		catchup_files_arg *arg = &(threads_args[i]);

		arg->nodeInfo = nodeInfo;
		arg->from_root = source_pgdata;
		arg->to_root = dest_pgdata;
		/* TODO разобраться */
		//arg->external_prefix = external_prefix;
		//arg->external_dirs = external_dirs;
		arg->files_list = backup_files_list;
                /* TODO !!!! change to target file_list */
		arg->prev_filelist = prev_backup_filelist;
		/* arg->prev_start_lsn = prev_backup_start_lsn; */
		arg->prev_start_lsn = sync_lsn;
		arg->backup_mode = backup_mode;
		arg->conn_arg.conn = NULL;
		arg->conn_arg.cancel_conn = NULL;
		/* TODO !!!! */
		arg->hdr_map = &(current.hdr_map);
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
		catchup_files_arg *arg = &(threads_args[i]);

		elog(VERBOSE, "Start thread num: %i", i);
		pthread_create(&threads[i], NULL, catchup_files, arg);
	}

	/* Wait threads */
	for (i = 0; i < num_threads; i++)
	{
		pthread_join(threads[i], NULL);
		if (threads_args[i].ret == 1)
			backup_isok = false;
	}

	time(&end_time);
	pretty_time_interval(difftime(end_time, start_time),
						 pretty_time, lengthof(pretty_time));
	if (backup_isok)
		elog(INFO, "Data files are transferred, time elapsed: %s",
			pretty_time);
	else
		elog(ERROR, "Data files transferring failed, time elapsed: %s",
			pretty_time);

	/* clean previous backup file list */
	if (prev_backup_filelist)
	{
		parray_walk(prev_backup_filelist, pgFileFree);
		parray_free(prev_backup_filelist);
	}

	/* Notify end of backup */
	current.start_lsn = start_lsn;
	pg_stop_backup(&current, source_conn, nodeInfo, dest_pgdata);

	/* In case of backup from replica >= 9.6 we must fix minRecPoint,
	 * First we must find pg_control in backup_files_list.
	 */
	if (current.from_replica && !exclusive_backup)
	{
		pgFile	   *pg_control = NULL;

		for (i = 0; i < parray_num(backup_files_list); i++)
		{
			pgFile	   *tmp_file = (pgFile *) parray_get(backup_files_list, i);

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

	/* close and sync page header map */
	//if (current.hdr_map.fp)
	//{
	//	cleanup_header_map(&(current.hdr_map));
	//
	//	if (fio_sync(current.hdr_map.path, FIO_BACKUP_HOST) != 0)
	//		elog(ERROR, "Cannot sync file \"%s\": %s", current.hdr_map.path, strerror(errno));
	//}

	/* close ssh session in main thread */
	fio_disconnect();

	/* Print the list of files to backup catalog */
	//write_backup_filelist(&current, backup_files_list, instance_config.pgdata,
	//					  external_dirs, true);
	/* update backup control file to update size info */
	//write_backup(&current, true);

	/* Sync all copied files unless '--no-sync' flag is used */
	if (no_sync)
		elog(WARNING, "Backup files are not synced to disk");
	else
	{
		elog(INFO, "Syncing backup files to disk");
		time(&start_time);

		for (i = 0; i < parray_num(backup_files_list); i++)
		{
			char    to_fullpath[MAXPGPATH];
			pgFile *file = (pgFile *) parray_get(backup_files_list, i);

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
		elog(INFO, "Backup files are synced, time elapsed: %s", pretty_time);
	}

	/* be paranoid about instance been from the past */
	// if (backup_mode != BACKUP_MODE_FULL &&
	//	current.stop_lsn < prev_backup->stop_lsn)
	//		elog(ERROR, "Current backup STOP LSN %X/%X is lower than STOP LSN %X/%X of previous backup %s. "
	//			"It may indicate that we are trying to backup PostgreSQL instance from the past.",
	//			(uint32) (current.stop_lsn >> 32), (uint32) (current.stop_lsn),
	//			(uint32) (prev_backup->stop_lsn >> 32), (uint32) (prev_backup->stop_lsn),
	//			base36enc(prev_backup->stop_lsn));

	/* clean external directories list */
	if (external_dirs)
		free_dir_list(external_dirs);

	/* Cleanup */
	if (backup_list)
	{
		parray_walk(backup_list, pgBackupFree);
		parray_free(backup_list);
	}

	if (tli_list)
	{
		parray_walk(tli_list, timelineInfoFree);
		parray_free(tli_list);
	}

	parray_walk(backup_files_list, pgFileFree);
	parray_free(backup_files_list);
	backup_files_list = NULL;
	// где закрывается backup_conn?
}

/*
 * Entry point of pg_probackup CATCHUP subcommand.
 *
 */
int
do_catchup(char *source_pgdata, char *dest_pgdata, BackupMode backup_mode, ConnectionOptions conn_opt, bool stream_wal, int num_threads)
{
	PGconn		*backup_conn = NULL;
	PGNodeInfo	nodeInfo;
	//char		pretty_bytes[20];
	bool		no_sync = false;
	bool		backup_logs = false;

	/* Initialize PGInfonode */
	pgNodeInit(&nodeInfo);

	/* ugly hack */
	instance_config.xlog_seg_size = DEFAULT_XLOG_SEG_SIZE;

	//if (!instance_config.pgdata)
	//	elog(ERROR, "required parameter not specified: PGDATA "
	//					 "(-D, --pgdata)");

	/* Update backup status and other metainfo. */
	//current.status = BACKUP_STATUS_RUNNING;
	//current.start_time = start_time;

	StrNCpy(current.program_version, PROGRAM_VERSION,
			sizeof(current.program_version));

	//current.compress_alg = instance_config.compress_alg;
	//current.compress_level = instance_config.compress_level;

	/* Save list of external directories */
	//if (instance_config.external_dir_str &&
	//	(pg_strcasecmp(instance_config.external_dir_str, "none") != 0))
	//	current.external_dir_str = instance_config.external_dir_str;

	elog(INFO, "Catchup start, pg_probackup version: %s, `"
			"wal mode: %s, remote: %s, catchup-source-pgdata: %s, catchup-destination-pgdata: %s",
			PROGRAM_VERSION,
			current.stream ? "STREAM" : "ARCHIVE", IsSshProtocol()  ? "true" : "false",
			source_pgdata, dest_pgdata);

	/* Create backup directory and BACKUP_CONTROL_FILE */
	//if (pgBackupCreateDir(&current))
	//	elog(ERROR, "Cannot create backup directory");
	//if (!lock_backup(&current, true))
	//	elog(ERROR, "Cannot lock backup %s directory",
	//		 base36enc(current.start_time));
	//write_backup(&current, true);

	//elog(LOG, "Backup destination is initialized");

	/*
	 * setup backup_conn, do some compatibility checks and
	 * fill basic info about instance
	 */
	backup_conn = pgdata_basic_setup(instance_config.conn_opt, &nodeInfo);

	//if (current.from_replica)
	//	elog(INFO, "Backup %s is going to be taken from standby", base36enc(start_time));

	/* TODO, print PostgreSQL full version */
	//elog(INFO, "PostgreSQL version: %s", nodeInfo.server_version_str);

	/*
	 * Ensure that backup directory was initialized for the same PostgreSQL
	 * instance we opened connection to. And that target backup database PGDATA
	 * belogns to the same instance.
	 */
	//check_system_identifiers(backup_conn, instance_config.pgdata);

	/* below perform checks specific for backup command */
#if PG_VERSION_NUM >= 110000
	if (!RetrieveWalSegSize(backup_conn))
		elog(ERROR, "Failed to retrieve wal_segment_size");
#endif

	get_ptrack_version(backup_conn, &nodeInfo);
	//	elog(WARNING, "ptrack_version_num %d", ptrack_version_num);

	if (nodeInfo.ptrack_version_num > 0)
		nodeInfo.is_ptrack_enable = pg_ptrack_enable(backup_conn, nodeInfo.ptrack_version_num);

	if (backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		if (nodeInfo.ptrack_version_num == 0)
			elog(ERROR, "This PostgreSQL instance does not support ptrack");
		else
		{
			if (!nodeInfo.is_ptrack_enable)
				elog(ERROR, "Ptrack is disabled");
		}
	}

	if (current.from_replica && exclusive_backup)
		/* Check master connection options */
		if (instance_config.master_conn_opt.pghost == NULL)
			elog(ERROR, "Options for connection to master must be provided to perform backup from replica");

	/* backup data */
	do_catchup_instance(source_pgdata, dest_pgdata, backup_conn, &nodeInfo, backup_mode, no_sync, backup_logs);

	//if (!no_validate)
	//	pgBackupValidate(&current, NULL);

	/* Notify user about backup size */
	//if (current.stream)
	//	pretty_size(current.data_bytes + current.wal_bytes, pretty_bytes, lengthof(pretty_bytes));
	//else
	//	pretty_size(current.data_bytes, pretty_bytes, lengthof(pretty_bytes));
	//elog(INFO, "Backup %s resident size: %s", base36enc(current.start_time), pretty_bytes);

	//if (current.status == BACKUP_STATUS_OK ||
	//	current.status == BACKUP_STATUS_DONE)
	//	elog(INFO, "Backup %s completed", base36enc(current.start_time));
	//else
	//	elog(ERROR, "Backup %s failed", base36enc(current.start_time));

	return 0;
}

/*
 * Take a backup of the PGDATA at a file level.
 * Copy all directories and files listed in backup_files_list.
 * If the file is 'datafile' (regular relation's main fork), read it page by page,
 * verify checksum and copy.
 * In incremental backup mode, copy only files or datafiles' pages changed after
 * previous backup.
 */
static void *
catchup_files(void *arg)
{
	int			i;
	char		from_fullpath[MAXPGPATH];
	char		to_fullpath[MAXPGPATH];
	static time_t prev_time;

	catchup_files_arg *arguments = (catchup_files_arg *) arg;
	int 		n_catchup_files_list = parray_num(arguments->files_list);

	/* TODO !!!! remove current */
	prev_time = current.start_time;

	/* backup a file */
	for (i = 0; i < n_catchup_files_list; i++)
	{
		pgFile	*file = (pgFile *) parray_get(arguments->files_list, i);
		pgFile	*prev_file = NULL;

		/* We have already copied all directories */
		if (S_ISDIR(file->mode))
			continue;

		if (arguments->thread_num == 1)
		{
			/* update backup_content.control every 60 seconds */
			if ((difftime(time(NULL), prev_time)) > 60)
			{
				// write_backup_filelist(&current, arguments->files_list, arguments->from_root,
				//					  arguments->external_dirs, false);
				/* update backup control file to update size info */
				//write_backup(&current, true);

				prev_time = time(NULL);
			}
		}

		if (!pg_atomic_test_set_flag(&file->lock))
			continue;

		/* check for interrupt */
		if (interrupted || thread_interrupted)
			elog(ERROR, "interrupted during backup");

		if (progress)
			elog(INFO, "Progress: (%d/%d). Process file \"%s\"",
				 i + 1, n_catchup_files_list, file->rel_path);

		/* Handle zero sized files */
		//if (file->size == 0)
		//{
		//	file->write_size = 0;
		//	continue;
		//}

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

		/* Check that file exist in previous backup */
		if (arguments->backup_mode != BACKUP_MODE_FULL)
		{
			pgFile	**prev_file_tmp = NULL;
			prev_file_tmp = (pgFile **) parray_bsearch(arguments->prev_filelist,
											file, pgFileCompareRelPathWithExternal);
			if (prev_file_tmp)
			{
				/* File exists in previous backup */
				file->exists_in_prev = true;
				prev_file = *prev_file_tmp;
			}
		}

		/* backup file */
		if (file->is_datafile && !file->is_cfs)
		{
			catchup_data_file(&(arguments->conn_arg), file, from_fullpath, to_fullpath,
								 arguments->prev_start_lsn,
								 arguments->backup_mode,
								 NONE_COMPRESS,
								 0,
								 arguments->nodeInfo->checksum_version,
								 arguments->nodeInfo->ptrack_version_num,
								 arguments->nodeInfo->ptrack_schema,
								 arguments->hdr_map, false);
		}
		else
		{
			backup_non_data_file(file, prev_file, from_fullpath, to_fullpath,
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

	/* Close connection */
	if (arguments->conn_arg.conn)
		pgut_disconnect(arguments->conn_arg.conn);

	/* Data files transferring is successful */
	arguments->ret = 0;

	return NULL;
}

