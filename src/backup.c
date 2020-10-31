/*-------------------------------------------------------------------------
 *
 * backup.c: backup DB cluster, archived WAL
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#if PG_VERSION_NUM < 110000
#include "catalog/catalog.h"
#endif
#include "catalog/pg_tablespace.h"
#include "pgtar.h"
#include "receivelog.h"
#include "streamutil.h"

#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "utils/thread.h"
#include "utils/file.h"

static int	standby_message_timeout = 10 * 1000;	/* 10 sec = default */
static XLogRecPtr stop_backup_lsn = InvalidXLogRecPtr;
static XLogRecPtr stop_stream_lsn = InvalidXLogRecPtr;

/*
 * How long we should wait for streaming end in seconds.
 * Retrieved as checkpoint_timeout + checkpoint_timeout * 0.1
 */
static uint32 stream_stop_timeout = 0;
/* Time in which we started to wait for streaming end */
static time_t stream_stop_begin = 0;

const char *progname = "pg_probackup";

/* list of files contained in backup */
static parray *backup_files_list = NULL;

/* We need critical section for datapagemap_add() in case of using threads */
static pthread_mutex_t backup_pagemap_mutex = PTHREAD_MUTEX_INITIALIZER;

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

bool exclusive_backup = false;

/* Is pg_start_backup() was executed */
static bool backup_in_progress = false;
/* Is pg_stop_backup() was sent */
static bool pg_stop_backup_is_sent = false;

/*
 * Backup routines
 */
static void backup_cleanup(bool fatal, void *userdata);

static void *backup_files(void *arg);

static void do_backup_instance(PGconn *backup_conn, PGNodeInfo *nodeInfo, bool no_sync, bool backup_logs);

static void pg_start_backup(const char *label, bool smooth, pgBackup *backup,
							PGNodeInfo *nodeInfo, PGconn *conn);
static void pg_switch_wal(PGconn *conn);
static void pg_stop_backup(pgBackup *backup, PGconn *pg_startbackup_conn, PGNodeInfo *nodeInfo);
static int checkpoint_timeout(PGconn *backup_conn);

static XLogRecPtr wait_wal_lsn(XLogRecPtr lsn, bool is_start_lsn, TimeLineID tli,
								bool in_prev_segment, bool segment_only,
								int timeout_elevel, bool in_stream_dir);

static void *StreamLog(void *arg);
static void IdentifySystem(StreamThreadArg *stream_thread_arg);

static void check_external_for_tablespaces(parray *external_list,
										   PGconn *backup_conn);
static parray *get_database_map(PGconn *pg_startbackup_conn);

/* pgpro specific functions */
static bool pgpro_support(PGconn *conn);

/* Check functions */
static bool pg_checksum_enable(PGconn *conn);
static bool pg_is_in_recovery(PGconn *conn);
static bool pg_is_superuser(PGconn *conn);
static void check_server_version(PGconn *conn, PGNodeInfo *nodeInfo);
static void confirm_block_size(PGconn *conn, const char *name, int blcksz);
static void set_cfs_datafiles(parray *files, const char *root, char *relative, size_t i);

static void
backup_stopbackup_callback(bool fatal, void *userdata)
{
	PGconn *pg_startbackup_conn = (PGconn *) userdata;
	/*
	 * If backup is in progress, notify stop of backup to PostgreSQL
	 */
	if (backup_in_progress)
	{
		elog(WARNING, "backup in progress, stop backup");
		pg_stop_backup(NULL, pg_startbackup_conn, NULL);	/* don't care about stop_lsn in case of error */
	}
}

/*
 * Take a backup of a single postgresql instance.
 * Move files from 'pgdata' to a subdirectory in 'backup_path'.
 */
static void
do_backup_instance(PGconn *backup_conn, PGNodeInfo *nodeInfo, bool no_sync, bool backup_logs)
{
	int			i;
	char		database_path[MAXPGPATH];
	char		external_prefix[MAXPGPATH]; /* Temp value. Used as template */
	char		dst_backup_path[MAXPGPATH];
	char		label[1024];
	XLogRecPtr	prev_backup_start_lsn = InvalidXLogRecPtr;

	/* arrays with meta info for multi threaded backup */
	pthread_t	*threads;
	backup_files_arg *threads_args;
	bool		backup_isok = true;

	pgBackup   *prev_backup = NULL;
	parray	   *prev_backup_filelist = NULL;
	parray	   *backup_list = NULL;
	parray	   *external_dirs = NULL;
	parray	   *database_map = NULL;

	/* used for multitimeline incremental backup */
	parray       *tli_list = NULL;

	/* for fancy reporting */
	time_t		start_time, end_time;
	char		pretty_time[20];
	char		pretty_bytes[20];

	elog(LOG, "Database backup start");
	if(current.external_dir_str)
	{
		external_dirs = make_external_directory_list(current.external_dir_str,
													 false);
		check_external_for_tablespaces(external_dirs, backup_conn);
	}

	/* Clear ptrack files for not PTRACK backups */
	if (current.backup_mode != BACKUP_MODE_DIFF_PTRACK && nodeInfo->is_ptrack_enable)
		pg_ptrack_clear(backup_conn, nodeInfo->ptrack_version_num);

	/* notify start of backup to PostgreSQL server */
	time2iso(label, lengthof(label), current.start_time);
	strncat(label, " with pg_probackup", lengthof(label) -
			strlen(" with pg_probackup"));

	/* Call pg_start_backup function in PostgreSQL connect */
	pg_start_backup(label, smooth_checkpoint, &current, nodeInfo, backup_conn);

	/* Obtain current timeline */
#if PG_VERSION_NUM >= 90600
	current.tli = get_current_timeline(backup_conn);
#else
	current.tli = get_current_timeline_from_control(false);
#endif

	/* In PAGE mode or in ARCHIVE wal-mode wait for current segment */
	if (current.backup_mode == BACKUP_MODE_DIFF_PAGE ||!stream_wal)
		/*
		 * Do not wait start_lsn for stream backup.
		 * Because WAL streaming will start after pg_start_backup() in stream
		 * mode.
		 */
		wait_wal_lsn(current.start_lsn, true, current.tli, false, true, ERROR, false);

	/*
	 * In incremental backup mode ensure that already-validated
	 * backup on current timeline exists and get its filelist.
	 */
	if (current.backup_mode == BACKUP_MODE_DIFF_PAGE ||
		current.backup_mode == BACKUP_MODE_DIFF_PTRACK ||
		current.backup_mode == BACKUP_MODE_DIFF_DELTA)
	{
		/* get list of backups already taken */
		backup_list = catalog_get_backup_list(instance_name, INVALID_BACKUP_ID);

		prev_backup = catalog_get_last_data_backup(backup_list, current.tli, current.start_time);
		if (prev_backup == NULL)
		{
			/* try to setup multi-timeline backup chain */
			elog(WARNING, "Valid backup on current timeline %u is not found, "
						"trying to look up on previous timelines",
						current.tli);

			/* TODO: use read_timeline_history */
			tli_list = catalog_get_timelines(&instance_config);

			if (parray_num(tli_list) == 0)
				elog(WARNING, "Cannot find valid backup on previous timelines, "
							"WAL archive is not available");
			else
			{
				prev_backup = get_multi_timeline_parent(backup_list, tli_list, current.tli,
														current.start_time, &instance_config);

				if (prev_backup == NULL)
					elog(WARNING, "Cannot find valid backup on previous timelines");
			}

			/* failed to find suitable parent, error out */
			if (!prev_backup)
				elog(ERROR, "Create new full backup before an incremental one");
		}
	}

	if (prev_backup)
	{
        if (parse_program_version(prev_backup->program_version) > parse_program_version(PROGRAM_VERSION))
            elog(ERROR, "pg_probackup binary version is %s, but backup %s version is %s. "
                        "pg_probackup do not guarantee to be forward compatible. "
                        "Please upgrade pg_probackup binary.",
                        PROGRAM_VERSION, base36enc(prev_backup->start_time), prev_backup->program_version);

		elog(INFO, "Parent backup: %s", base36enc(prev_backup->start_time));

		/* Files of previous backup needed by DELTA backup */
		prev_backup_filelist = get_backup_filelist(prev_backup, true);

		/* If lsn is not NULL, only pages with higher lsn will be copied. */
		prev_backup_start_lsn = prev_backup->start_lsn;
		current.parent_backup = prev_backup->start_time;

		write_backup(&current, true);
	}

	/*
	 * It`s illegal to take PTRACK backup if LSN from ptrack_control() is not
	 * equal to start_lsn of previous backup.
	 */
	if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		XLogRecPtr	ptrack_lsn = get_last_ptrack_lsn(backup_conn, nodeInfo);

		if (nodeInfo->ptrack_version_num < 20)
		{
			// backward compatibility kludge: use Stop LSN for ptrack 1.x,
			if (ptrack_lsn > prev_backup->stop_lsn || ptrack_lsn == InvalidXLogRecPtr)
			{
				elog(ERROR, "LSN from ptrack_control %X/%X differs from Stop LSN of previous backup %X/%X.\n"
							"Create new full backup before an incremental one.",
							(uint32) (ptrack_lsn >> 32), (uint32) (ptrack_lsn),
							(uint32) (prev_backup->stop_lsn >> 32),
							(uint32) (prev_backup->stop_lsn));
			}
		}
		else
		{
			// new ptrack is more robust and checks Start LSN
			if (ptrack_lsn > prev_backup->start_lsn || ptrack_lsn == InvalidXLogRecPtr)
			{
				elog(ERROR, "LSN from ptrack_control %X/%X is greater than Start LSN of previous backup %X/%X.\n"
							"Create new full backup before an incremental one.",
							(uint32) (ptrack_lsn >> 32), (uint32) (ptrack_lsn),
							(uint32) (prev_backup->start_lsn >> 32),
							(uint32) (prev_backup->start_lsn));
			}
		}
	}

	/* For incremental backup check that start_lsn is not from the past
	 * Though it will not save us if PostgreSQL instance is actually
	 * restored STREAM backup.
	 */
	if (current.backup_mode != BACKUP_MODE_FULL &&
		prev_backup->start_lsn > current.start_lsn)
			elog(ERROR, "Current START LSN %X/%X is lower than START LSN %X/%X of previous backup %s. "
				"It may indicate that we are trying to backup PostgreSQL instance from the past.",
				(uint32) (current.start_lsn >> 32), (uint32) (current.start_lsn),
				(uint32) (prev_backup->start_lsn >> 32), (uint32) (prev_backup->start_lsn),
				base36enc(prev_backup->start_time));

	/* Update running backup meta with START LSN */
	write_backup(&current, true);

	pgBackupGetPath(&current, database_path, lengthof(database_path),
					DATABASE_DIR);
	pgBackupGetPath(&current, external_prefix, lengthof(external_prefix),
					EXTERNAL_DIR);

	/* start stream replication */
	if (stream_wal)
	{
		/* How long we should wait for streaming end after pg_stop_backup */
		stream_stop_timeout = checkpoint_timeout(backup_conn);
		stream_stop_timeout = stream_stop_timeout + stream_stop_timeout * 0.1;

		join_path_components(dst_backup_path, database_path, PG_XLOG_DIR);
		fio_mkdir(dst_backup_path, DIR_PERMISSION, FIO_BACKUP_HOST);

		stream_thread_arg.basedir = dst_backup_path;

		/*
		 * Connect in replication mode to the server.
		 */
		stream_thread_arg.conn = pgut_connect_replication(instance_config.conn_opt.pghost,
														  instance_config.conn_opt.pgport,
														  instance_config.conn_opt.pgdatabase,
														  instance_config.conn_opt.pguser);
		/* sanity */
		IdentifySystem(&stream_thread_arg);

		/* By default there are some error */
		stream_thread_arg.ret = 1;
		/* we must use startpos as start_lsn from start_backup */
		stream_thread_arg.startpos = current.start_lsn;
		stream_thread_arg.starttli = current.tli;

		thread_interrupted = false;
		pthread_create(&stream_thread, NULL, StreamLog, &stream_thread_arg);
	}

	/* initialize backup list */
	backup_files_list = parray_new();

	/* list files with the logical path. omit $PGDATA */
	if (fio_is_remote(FIO_DB_HOST))
		fio_list_dir(backup_files_list, instance_config.pgdata,
					 true, true, false, backup_logs, true, 0);
	else
		dir_list_file(backup_files_list, instance_config.pgdata,
					  true, true, false, backup_logs, true, 0, FIO_LOCAL_HOST);

	/*
	 * Get database_map (name to oid) for use in partial restore feature.
	 * It's possible that we fail and database_map will be NULL.
	 */
	database_map = get_database_map(backup_conn);

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
	parse_filelist_filenames(backup_files_list, instance_config.pgdata);

	elog(LOG, "Current Start LSN: %X/%X, TLI: %X",
			(uint32) (current.start_lsn >> 32), (uint32) (current.start_lsn),
			current.tli);
	if (current.backup_mode != BACKUP_MODE_FULL)
		elog(LOG, "Parent Start LSN: %X/%X, TLI: %X",
			 (uint32) (prev_backup->start_lsn >> 32), (uint32) (prev_backup->start_lsn),
			 prev_backup->tli);

	/*
	 * Build page mapping in incremental mode.
	 */

	if (current.backup_mode == BACKUP_MODE_DIFF_PAGE ||
		current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		bool pagemap_isok = true;

		time(&start_time);
		elog(INFO, "Extracting pagemap of changed blocks");

		if (current.backup_mode == BACKUP_MODE_DIFF_PAGE)
		{
			/*
			 * Build the page map. Obtain information about changed pages
			 * reading WAL segments present in archives up to the point
			 * where this backup has started.
			 */
			pagemap_isok = extractPageMap(arclog_path, instance_config.xlog_seg_size,
						   prev_backup->start_lsn, prev_backup->tli,
						   current.start_lsn, current.tli, tli_list);
		}
		else if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
		{
			/*
			 * Build the page map from ptrack information.
			 */
			if (nodeInfo->ptrack_version_num >= 20)
				make_pagemap_from_ptrack_2(backup_files_list, backup_conn,
										   nodeInfo->ptrack_schema,
										   nodeInfo->ptrack_version_num,
										   prev_backup_start_lsn);
			else if (nodeInfo->ptrack_version_num == 15 ||
					 nodeInfo->ptrack_version_num == 16 ||
					 nodeInfo->ptrack_version_num == 17)
				make_pagemap_from_ptrack_1(backup_files_list, backup_conn);
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
				snprintf(temp, MAXPGPATH, "%s%d", external_prefix,
						 file->external_dir_num);
				join_path_components(dirpath, temp, file->rel_path);
			}
			else
				join_path_components(dirpath, database_path, file->rel_path);

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
	write_backup_filelist(&current, backup_files_list,
						  instance_config.pgdata, external_dirs, true);
	write_backup(&current, true);

	/* Init backup page header map */
	init_header_map(&current);

	/* init thread args with own file lists */
	threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
	threads_args = (backup_files_arg *) palloc(sizeof(backup_files_arg)*num_threads);

	for (i = 0; i < num_threads; i++)
	{
		backup_files_arg *arg = &(threads_args[i]);

		arg->nodeInfo = nodeInfo;
		arg->from_root = instance_config.pgdata;
		arg->to_root = database_path;
		arg->external_prefix = external_prefix;
		arg->external_dirs = external_dirs;
		arg->files_list = backup_files_list;
		arg->prev_filelist = prev_backup_filelist;
		arg->prev_start_lsn = prev_backup_start_lsn;
		arg->conn_arg.conn = NULL;
		arg->conn_arg.cancel_conn = NULL;
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
		backup_files_arg *arg = &(threads_args[i]);

		elog(VERBOSE, "Start thread num: %i", i);
		pthread_create(&threads[i], NULL, backup_files, arg);
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
	pg_stop_backup(&current, backup_conn, nodeInfo);

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

		set_min_recovery_point(pg_control, database_path, current.stop_lsn);
	}

	/* close and sync page header map */
	if (current.hdr_map.fp)
	{
		cleanup_header_map(&(current.hdr_map));

		if (fio_sync(current.hdr_map.path, FIO_BACKUP_HOST) != 0)
			elog(ERROR, "Cannot sync file \"%s\": %s", current.hdr_map.path, strerror(errno));
	}

	/* close ssh session in main thread */
	fio_disconnect();

	/* Add archived xlog files into the list of files of this backup */
	if (stream_wal)
	{
		parray     *xlog_files_list;
		char		pg_xlog_path[MAXPGPATH];
		char		wal_full_path[MAXPGPATH];

		/* Scan backup PG_XLOG_DIR */
		xlog_files_list = parray_new();
		join_path_components(pg_xlog_path, database_path, PG_XLOG_DIR);
		dir_list_file(xlog_files_list, pg_xlog_path, false, true, false, false, true, 0,
					  FIO_BACKUP_HOST);

		/* TODO: Drop streamed WAL segments greater than stop_lsn */
		for (i = 0; i < parray_num(xlog_files_list); i++)
		{
			pgFile	   *file = (pgFile *) parray_get(xlog_files_list, i);

			join_path_components(wal_full_path, pg_xlog_path, file->rel_path);

			if (!S_ISREG(file->mode))
				continue;

			file->crc = pgFileGetCRC(wal_full_path, true, false);
			file->write_size = file->size;

			/* overwrite rel_path, because now it is relative to
			 * /backup_dir/backups/instance_name/backup_id/database/pg_xlog/
			 */
			pg_free(file->rel_path);

			/* Now it is relative to /backup_dir/backups/instance_name/backup_id/database/ */
			file->rel_path = pgut_strdup(GetRelativePath(wal_full_path, database_path));

			file->name = last_dir_separator(file->rel_path);

			if (file->name == NULL) // TODO: do it in pgFileInit
				file->name = file->rel_path;
			else
				file->name++;
		}

		/* Add xlog files into the list of backed up files */
		parray_concat(backup_files_list, xlog_files_list);
		parray_free(xlog_files_list);
	}

	/* write database map to file and add it to control file */
	if (database_map)
	{
		write_database_map(&current, database_map, backup_files_list);
		/* cleanup */
		parray_walk(database_map, db_map_entry_free);
		parray_free(database_map);
	}

	/* Print the list of files to backup catalog */
	write_backup_filelist(&current, backup_files_list, instance_config.pgdata,
						  external_dirs, true);
	/* update backup control file to update size info */
	write_backup(&current, true);

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
				join_path_components(to_fullpath, database_path, file->rel_path);
			else
			{
				char 	external_dst[MAXPGPATH];

				makeExternalDirPathByNum(external_dst, external_prefix,
										 file->external_dir_num);
				join_path_components(to_fullpath, external_dst, file->rel_path);
			}

			if (fio_sync(to_fullpath, FIO_BACKUP_HOST) != 0)
				elog(ERROR, "Cannot sync file \"%s\": %s", to_fullpath, strerror(errno));
		}

		time(&end_time);
		pretty_time_interval(difftime(end_time, start_time),
							 pretty_time, lengthof(pretty_time));
		elog(INFO, "Backup files are synced, time elapsed: %s", pretty_time);
	}

	/* be paranoid about instance been from the past */
	if (current.backup_mode != BACKUP_MODE_FULL &&
		current.stop_lsn < prev_backup->stop_lsn)
			elog(ERROR, "Current backup STOP LSN %X/%X is lower than STOP LSN %X/%X of previous backup %s. "
				"It may indicate that we are trying to backup PostgreSQL instance from the past.",
				(uint32) (current.stop_lsn >> 32), (uint32) (current.stop_lsn),
				(uint32) (prev_backup->stop_lsn >> 32), (uint32) (prev_backup->stop_lsn),
				base36enc(prev_backup->stop_lsn));

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
}

/*
 * Common code for CHECKDB and BACKUP commands.
 * Ensure that we're able to connect to the instance
 * check compatibility and fill basic info.
 * For checkdb launched in amcheck mode with pgdata validation
 * do not check system ID, it gives user an opportunity to
 * check remote PostgreSQL instance.
 * Also checking system ID in this case serves no purpose, because
 * all work is done by server.
 *
 * Returns established connection
 */
PGconn *
pgdata_basic_setup(ConnectionOptions conn_opt, PGNodeInfo *nodeInfo)
{
	PGconn *cur_conn;

	/* Create connection for PostgreSQL */
	cur_conn = pgut_connect(conn_opt.pghost, conn_opt.pgport,
							   conn_opt.pgdatabase,
							   conn_opt.pguser);

	current.primary_conninfo = pgut_get_conninfo_string(cur_conn);

	/* Confirm data block size and xlog block size are compatible */
	confirm_block_size(cur_conn, "block_size", BLCKSZ);
	confirm_block_size(cur_conn, "wal_block_size", XLOG_BLCKSZ);
	nodeInfo->block_size = BLCKSZ;
	nodeInfo->wal_block_size = XLOG_BLCKSZ;
	nodeInfo->is_superuser = pg_is_superuser(cur_conn);
	nodeInfo->pgpro_support = pgpro_support(cur_conn);

	current.from_replica = pg_is_in_recovery(cur_conn);

	/* Confirm that this server version is supported */
	check_server_version(cur_conn, nodeInfo);

	if (pg_checksum_enable(cur_conn))
		current.checksum_version = 1;
	else
		current.checksum_version = 0;

	nodeInfo->checksum_version = current.checksum_version;

	if (current.checksum_version)
		elog(LOG, "This PostgreSQL instance was initialized with data block checksums. "
					"Data block corruption will be detected");
	else
		elog(WARNING, "This PostgreSQL instance was initialized without data block checksums. "
						"pg_probackup have no way to detect data block corruption without them. "
						"Reinitialize PGDATA with option '--data-checksums'.");

	if (nodeInfo->is_superuser)
		elog(WARNING, "Current PostgreSQL role is superuser. "
						"It is not recommended to run backup or checkdb as superuser.");

	StrNCpy(current.server_version, nodeInfo->server_version_str,
			sizeof(current.server_version));

	return cur_conn;
}

/*
 * Entry point of pg_probackup BACKUP subcommand.
 */
int
do_backup(time_t start_time, pgSetBackupParams *set_backup_params,
			bool no_validate, bool no_sync, bool backup_logs)
{
	PGconn		*backup_conn = NULL;
	PGNodeInfo	nodeInfo;
	char		pretty_bytes[20];

	/* Initialize PGInfonode */
	pgNodeInit(&nodeInfo);

	if (!instance_config.pgdata)
		elog(ERROR, "required parameter not specified: PGDATA "
						 "(-D, --pgdata)");

	/* Update backup status and other metainfo. */
	current.status = BACKUP_STATUS_RUNNING;
	current.start_time = start_time;

	StrNCpy(current.program_version, PROGRAM_VERSION,
			sizeof(current.program_version));

	current.compress_alg = instance_config.compress_alg;
	current.compress_level = instance_config.compress_level;

	/* Save list of external directories */
	if (instance_config.external_dir_str &&
		(pg_strcasecmp(instance_config.external_dir_str, "none") != 0))
		current.external_dir_str = instance_config.external_dir_str;

	elog(INFO, "Backup start, pg_probackup version: %s, instance: %s, backup ID: %s, backup mode: %s, "
			"wal mode: %s, remote: %s, compress-algorithm: %s, compress-level: %i",
			PROGRAM_VERSION, instance_name, base36enc(start_time), pgBackupGetBackupMode(&current),
			current.stream ? "STREAM" : "ARCHIVE", IsSshProtocol()  ? "true" : "false",
			deparse_compress_alg(current.compress_alg), current.compress_level);

	/* Create backup directory and BACKUP_CONTROL_FILE */
	if (pgBackupCreateDir(&current))
		elog(ERROR, "Cannot create backup directory");
	if (!lock_backup(&current, true))
		elog(ERROR, "Cannot lock backup %s directory",
			 base36enc(current.start_time));
	write_backup(&current, true);

	/* set the error processing function for the backup process */
	pgut_atexit_push(backup_cleanup, NULL);

	elog(LOG, "Backup destination is initialized");

	/*
	 * setup backup_conn, do some compatibility checks and
	 * fill basic info about instance
	 */
	backup_conn = pgdata_basic_setup(instance_config.conn_opt, &nodeInfo);

	if (current.from_replica)
		elog(INFO, "Backup %s is going to be taken from standby", base36enc(start_time));

	/* TODO, print PostgreSQL full version */
	//elog(INFO, "PostgreSQL version: %s", nodeInfo.server_version_str);

	/*
	 * Ensure that backup directory was initialized for the same PostgreSQL
	 * instance we opened connection to. And that target backup database PGDATA
	 * belogns to the same instance.
	 */
	check_system_identifiers(backup_conn, instance_config.pgdata);

	/* below perform checks specific for backup command */
#if PG_VERSION_NUM >= 110000
	if (!RetrieveWalSegSize(backup_conn))
		elog(ERROR, "Failed to retrieve wal_segment_size");
#endif

	get_ptrack_version(backup_conn, &nodeInfo);
	//	elog(WARNING, "ptrack_version_num %d", ptrack_version_num);

	if (nodeInfo.ptrack_version_num > 0)
		nodeInfo.is_ptrack_enable = pg_ptrack_enable(backup_conn, nodeInfo.ptrack_version_num);

	if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
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

	/* add note to backup if requested */
	if (set_backup_params && set_backup_params->note)
		add_note(&current, set_backup_params->note);

	/* backup data */
	do_backup_instance(backup_conn, &nodeInfo, no_sync, backup_logs);
	pgut_atexit_pop(backup_cleanup, NULL);

	/* compute size of wal files of this backup stored in the archive */
	if (!current.stream)
	{
		XLogSegNo start_segno;
		XLogSegNo stop_segno;

		GetXLogSegNo(current.start_lsn, start_segno, instance_config.xlog_seg_size);
		GetXLogSegNo(current.stop_lsn, stop_segno, instance_config.xlog_seg_size);
		current.wal_bytes = (stop_segno - start_segno) * instance_config.xlog_seg_size;

		/*
		 * If start_lsn and stop_lsn are located in the same segment, then
		 * set wal_bytes to the size of 1 segment.
		 */
		if (current.wal_bytes <= 0)
			current.wal_bytes = instance_config.xlog_seg_size;
	}

	/* Backup is done. Update backup status */
	current.end_time = time(NULL);
	current.status = BACKUP_STATUS_DONE;
	write_backup(&current, true);

	/* Pin backup if requested */
	if (set_backup_params &&
		(set_backup_params->ttl > 0 ||
		 set_backup_params->expire_time > 0))
	{
		pin_backup(&current, set_backup_params);
	}

	if (!no_validate)
		pgBackupValidate(&current, NULL);

	/* Notify user about backup size */
	if (current.stream)
		pretty_size(current.data_bytes + current.wal_bytes, pretty_bytes, lengthof(pretty_bytes));
	else
		pretty_size(current.data_bytes, pretty_bytes, lengthof(pretty_bytes));
	elog(INFO, "Backup %s resident size: %s", base36enc(current.start_time), pretty_bytes);

	if (current.status == BACKUP_STATUS_OK ||
		current.status == BACKUP_STATUS_DONE)
		elog(INFO, "Backup %s completed", base36enc(current.start_time));
	else
		elog(ERROR, "Backup %s failed", base36enc(current.start_time));

	/*
	 * After successful backup completion remove backups
	 * which are expired according to retention policies
	 */
	if (delete_expired || merge_expired || delete_wal)
		do_retention();

	return 0;
}

/*
 * Confirm that this server version is supported
 */
static void
check_server_version(PGconn *conn, PGNodeInfo *nodeInfo)
{
	PGresult   *res = NULL;

	/* confirm server version */
	nodeInfo->server_version = PQserverVersion(conn);

	if (nodeInfo->server_version == 0)
		elog(ERROR, "Unknown server version %d", nodeInfo->server_version);

	if (nodeInfo->server_version < 100000)
		sprintf(nodeInfo->server_version_str, "%d.%d",
				nodeInfo->server_version / 10000,
				(nodeInfo->server_version / 100) % 100);
	else
		sprintf(nodeInfo->server_version_str, "%d",
				nodeInfo->server_version / 10000);

	if (nodeInfo->server_version < 90500)
		elog(ERROR,
			 "server version is %s, must be %s or higher",
			 nodeInfo->server_version_str, "9.5");

	if (current.from_replica && nodeInfo->server_version < 90600)
		elog(ERROR,
			 "server version is %s, must be %s or higher for backup from replica",
			 nodeInfo->server_version_str, "9.6");

	if (nodeInfo->pgpro_support)
		res = pgut_execute(conn, "SELECT pgpro_edition()", 0, NULL);

	/*
	 * Check major version of connected PostgreSQL and major version of
	 * compiled PostgreSQL.
	 */
#ifdef PGPRO_VERSION
	if (!res)
		/* It seems we connected to PostgreSQL (not Postgres Pro) */
		elog(ERROR, "%s was built with Postgres Pro %s %s, "
					"but connection is made with PostgreSQL %s",
			 PROGRAM_NAME, PG_MAJORVERSION, PGPRO_EDITION, nodeInfo->server_version_str);
	else
	{
		if (strcmp(nodeInfo->server_version_str, PG_MAJORVERSION) != 0 &&
				 strcmp(PQgetvalue(res, 0, 0), PGPRO_EDITION) != 0)
			elog(ERROR, "%s was built with Postgres Pro %s %s, "
						"but connection is made with Postgres Pro %s %s",
				 PROGRAM_NAME, PG_MAJORVERSION, PGPRO_EDITION,
				 nodeInfo->server_version_str, PQgetvalue(res, 0, 0));
	}
#else
	if (res)
		/* It seems we connected to Postgres Pro (not PostgreSQL) */
		elog(ERROR, "%s was built with PostgreSQL %s, "
					"but connection is made with Postgres Pro %s %s",
			 PROGRAM_NAME, PG_MAJORVERSION,
			 nodeInfo->server_version_str, PQgetvalue(res, 0, 0));
	else
	{
		if (strcmp(nodeInfo->server_version_str, PG_MAJORVERSION) != 0)
			elog(ERROR, "%s was built with PostgreSQL %s, but connection is made with %s",
				 PROGRAM_NAME, PG_MAJORVERSION, nodeInfo->server_version_str);
	}
#endif

	if (res)
		PQclear(res);

	/* Do exclusive backup only for PostgreSQL 9.5 */
	exclusive_backup = nodeInfo->server_version < 90600;
}

/*
 * Ensure that backup directory was initialized for the same PostgreSQL
 * instance we opened connection to. And that target backup database PGDATA
 * belogns to the same instance.
 * All system identifiers must be equal.
 */
void
check_system_identifiers(PGconn *conn, char *pgdata)
{
	uint64		system_id_conn;
	uint64		system_id_pgdata;

	system_id_pgdata = get_system_identifier(pgdata);
	system_id_conn = get_remote_system_identifier(conn);

	/* for checkdb check only system_id_pgdata and system_id_conn */
	if (current.backup_mode == BACKUP_MODE_INVALID)
	{
		if (system_id_conn != system_id_pgdata)
		{
			elog(ERROR, "Data directory initialized with system id " UINT64_FORMAT ", "
						"but connected instance system id is " UINT64_FORMAT,
				 system_id_pgdata, system_id_conn);
		}
		return;
	}

	if (system_id_conn != instance_config.system_identifier)
		elog(ERROR, "Backup data directory was initialized for system id " UINT64_FORMAT ", "
					"but connected instance system id is " UINT64_FORMAT,
			 instance_config.system_identifier, system_id_conn);

	if (system_id_pgdata != instance_config.system_identifier)
		elog(ERROR, "Backup data directory was initialized for system id " UINT64_FORMAT ", "
					"but target backup directory system id is " UINT64_FORMAT,
			 instance_config.system_identifier, system_id_pgdata);
}

/*
 * Ensure that target backup database is initialized with
 * compatible settings. Currently check BLCKSZ and XLOG_BLCKSZ.
 */
static void
confirm_block_size(PGconn *conn, const char *name, int blcksz)
{
	PGresult   *res;
	char	   *endp;
	int			block_size;

	res = pgut_execute(conn, "SELECT pg_catalog.current_setting($1)", 1, &name);
	if (PQntuples(res) != 1 || PQnfields(res) != 1)
		elog(ERROR, "cannot get %s: %s", name, PQerrorMessage(conn));

	block_size = strtol(PQgetvalue(res, 0, 0), &endp, 10);
	if ((endp && *endp) || block_size != blcksz)
		elog(ERROR,
			 "%s(%d) is not compatible(%d expected)",
			 name, block_size, blcksz);

	PQclear(res);
}

/*
 * Notify start of backup to PostgreSQL server.
 */
static void
pg_start_backup(const char *label, bool smooth, pgBackup *backup,
				PGNodeInfo *nodeInfo, PGconn *conn)
{
	PGresult   *res;
	const char *params[2];
	uint32		lsn_hi;
	uint32		lsn_lo;

	params[0] = label;

	/* 2nd argument is 'fast'*/
	params[1] = smooth ? "false" : "true";
	if (!exclusive_backup)
		res = pgut_execute(conn,
						   "SELECT pg_catalog.pg_start_backup($1, $2, false)",
						   2,
						   params);
	else
		res = pgut_execute(conn,
						   "SELECT pg_catalog.pg_start_backup($1, $2)",
						   2,
						   params);

	/*
	 * Set flag that pg_start_backup() was called. If an error will happen it
	 * is necessary to call pg_stop_backup() in backup_cleanup().
	 */
	backup_in_progress = true;
	pgut_atexit_push(backup_stopbackup_callback, conn);

	/* Extract timeline and LSN from results of pg_start_backup() */
	XLogDataFromLSN(PQgetvalue(res, 0, 0), &lsn_hi, &lsn_lo);
	/* Calculate LSN */
	backup->start_lsn = ((uint64) lsn_hi )<< 32 | lsn_lo;

	PQclear(res);

	if ((!stream_wal || current.backup_mode == BACKUP_MODE_DIFF_PAGE) &&
		!backup->from_replica &&
		!(nodeInfo->server_version < 90600 &&
		  !nodeInfo->is_superuser))
		/*
		 * Switch to a new WAL segment. It is necessary to get archived WAL
		 * segment, which includes start LSN of current backup.
		 * Don`t do this for replica backups and for PG 9.5 if pguser is not superuser
		 * (because in 9.5 only superuser can switch WAL)
		 */
		pg_switch_wal(conn);
}

/*
 * Switch to a new WAL segment. It should be called only for master.
 * For PG 9.5 it should be called only if pguser is superuser.
 */
static void
pg_switch_wal(PGconn *conn)
{
	PGresult   *res;

	/* Remove annoying NOTICE messages generated by backend */
	res = pgut_execute(conn, "SET client_min_messages = warning;", 0, NULL);
	PQclear(res);

#if PG_VERSION_NUM >= 100000
	res = pgut_execute(conn, "SELECT pg_catalog.pg_switch_wal()", 0, NULL);
#else
	res = pgut_execute(conn, "SELECT pg_catalog.pg_switch_xlog()", 0, NULL);
#endif

	PQclear(res);
}

/*
 * Check if the instance is PostgresPro fork.
 */
static bool
pgpro_support(PGconn *conn)
{
	PGresult   *res;

	res = pgut_execute(conn,
						  "SELECT proname FROM pg_proc WHERE proname='pgpro_edition'",
						  0, NULL);

	if (PQresultStatus(res) == PGRES_TUPLES_OK &&
		(PQntuples(res) == 1) &&
		(strcmp(PQgetvalue(res, 0, 0), "pgpro_edition") == 0))
	{
		PQclear(res);
		return true;
	}

	PQclear(res);
	return false;
}

/*
 * Fill 'datname to Oid' map
 *
 * This function can fail to get the map for legal reasons, e.g. missing
 * permissions on pg_database during `backup`.
 * As long as user do not use partial restore feature it`s fine.
 *
 * To avoid breaking a backward compatibility don't throw an ERROR,
 * throw a warning instead of an error and return NULL.
 * Caller is responsible for checking the result.
 */
parray *
get_database_map(PGconn *conn)
{
	PGresult   *res;
	parray *database_map = NULL;
	int i;

	/*
	 * Do not include template0 and template1 to the map
	 * as default databases that must always be restored.
	 */
	res = pgut_execute_extended(conn,
						  "SELECT oid, datname FROM pg_catalog.pg_database "
						  "WHERE datname NOT IN ('template1', 'template0')",
						  0, NULL, true, true);

	/* Don't error out, simply return NULL. See comment above. */
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		elog(WARNING, "Failed to get database map: %s",
			PQerrorMessage(conn));

		return NULL;
	}

	/* Construct database map */
	for (i = 0; i < PQntuples(res); i++)
	{
		char *datname = NULL;
		db_map_entry *db_entry = (db_map_entry *) pgut_malloc(sizeof(db_map_entry));

		/* get Oid */
		db_entry->dbOid = atoll(PQgetvalue(res, i, 0));

		/* get datname */
		datname = PQgetvalue(res, i, 1);
		db_entry->datname = pgut_malloc(strlen(datname) + 1);
		strcpy(db_entry->datname, datname);

		if (database_map == NULL)
			database_map = parray_new();

		parray_append(database_map, db_entry);
	}

	return database_map;
}

/* Check if ptrack is enabled in target instance */
static bool
pg_checksum_enable(PGconn *conn)
{
	PGresult   *res_db;

	res_db = pgut_execute(conn, "SHOW data_checksums", 0, NULL);

	if (strcmp(PQgetvalue(res_db, 0, 0), "on") == 0)
	{
		PQclear(res_db);
		return true;
	}
	PQclear(res_db);
	return false;
}

/* Check if target instance is replica */
static bool
pg_is_in_recovery(PGconn *conn)
{
	PGresult   *res_db;

	res_db = pgut_execute(conn, "SELECT pg_catalog.pg_is_in_recovery()", 0, NULL);

	if (PQgetvalue(res_db, 0, 0)[0] == 't')
	{
		PQclear(res_db);
		return true;
	}
	PQclear(res_db);
	return false;
}


/* Check if current PostgreSQL role is superuser */
static bool
pg_is_superuser(PGconn *conn)
{
	PGresult   *res;

	res = pgut_execute(conn, "SELECT pg_catalog.current_setting('is_superuser')", 0, NULL);

	if (strcmp(PQgetvalue(res, 0, 0), "on") == 0)
	{
		PQclear(res);
		return true;
	}
	PQclear(res);
	return false;
}

/*
 * Wait for target LSN or WAL segment, containing target LSN.
 *
 * Depending on value of flag in_stream_dir wait for target LSN to archived or
 * streamed in 'archive_dir' or 'pg_wal' directory.
 *
 * If flag 'is_start_lsn' is set then issue warning for first-time users.
 * If flag 'in_prev_segment' is set, look for LSN in previous segment,
 *  with EndRecPtr >= Target LSN. It should be used only for solving
 *  invalid XRecOff problem.
 * If flag 'segment_only' is set, then, instead of waiting for LSN, wait for segment,
 *  containing that LSN.
 * If flags 'in_prev_segment' and 'segment_only' are both set, then wait for
 *  previous segment.
 *
 * Flag 'in_stream_dir' determine whether we looking for WAL in 'pg_wal' directory or
 * in archive. Do note, that we cannot rely sorely on global variable 'stream_wal' because,
 * for example, PAGE backup must(!) look for start_lsn in archive regardless of wal_mode.
 *
 * 'timeout_elevel' determine the elevel for timeout elog message. If elevel lighter than
 * ERROR is used, then return InvalidXLogRecPtr. TODO: return something more concrete, for example 1.
 *
 * Returns target LSN if such is found, failing that returns LSN of record prior to target LSN.
 * Returns InvalidXLogRecPtr if 'segment_only' flag is used.
 */
static XLogRecPtr
wait_wal_lsn(XLogRecPtr target_lsn, bool is_start_lsn, TimeLineID tli,
			 bool in_prev_segment, bool segment_only,
			 int timeout_elevel, bool in_stream_dir)
{
	XLogSegNo	targetSegNo;
	char		pg_wal_dir[MAXPGPATH];
	char		wal_segment_path[MAXPGPATH],
			   *wal_segment_dir,
				wal_segment[MAXFNAMELEN];
	bool		file_exists = false;
	uint32		try_count = 0,
				timeout;
	char		*wal_delivery_str = in_stream_dir ? "streamed":"archived";

#ifdef HAVE_LIBZ
	char		gz_wal_segment_path[MAXPGPATH];
#endif

	/* Compute the name of the WAL file containing requested LSN */
	GetXLogSegNo(target_lsn, targetSegNo, instance_config.xlog_seg_size);
	if (in_prev_segment)
		targetSegNo--;
	GetXLogFileName(wal_segment, tli, targetSegNo,
					instance_config.xlog_seg_size);

	/*
	 * In pg_start_backup we wait for 'target_lsn' in 'pg_wal' directory if it is
	 * stream and non-page backup. Page backup needs archived WAL files, so we
	 * wait for 'target_lsn' in archive 'wal' directory for page backups.
	 *
	 * In pg_stop_backup it depends only on stream_wal.
	 */
	if (in_stream_dir)
	{
		pgBackupGetPath2(&current, pg_wal_dir, lengthof(pg_wal_dir),
						 DATABASE_DIR, PG_XLOG_DIR);
		join_path_components(wal_segment_path, pg_wal_dir, wal_segment);
		wal_segment_dir = pg_wal_dir;
	}
	else
	{
		join_path_components(wal_segment_path, arclog_path, wal_segment);
		wal_segment_dir = arclog_path;
	}

	/* TODO: remove this in 3.0 (it is a cludge against some old bug with archive_timeout) */
	if (instance_config.archive_timeout > 0)
		timeout = instance_config.archive_timeout;
	else
		timeout = ARCHIVE_TIMEOUT_DEFAULT;

	if (segment_only)
		elog(LOG, "Looking for segment: %s", wal_segment);
	else
		elog(LOG, "Looking for LSN %X/%X in segment: %s",
			 (uint32) (target_lsn >> 32), (uint32) target_lsn, wal_segment);

#ifdef HAVE_LIBZ
	snprintf(gz_wal_segment_path, sizeof(gz_wal_segment_path), "%s.gz",
			 wal_segment_path);
#endif

	/* Wait until target LSN is archived or streamed */
	while (true)
	{
		if (!file_exists)
		{
			file_exists = fileExists(wal_segment_path, FIO_BACKUP_HOST);

			/* Try to find compressed WAL file */
			if (!file_exists)
			{
#ifdef HAVE_LIBZ
				file_exists = fileExists(gz_wal_segment_path, FIO_BACKUP_HOST);
				if (file_exists)
					elog(LOG, "Found compressed WAL segment: %s", wal_segment_path);
#endif
			}
			else
				elog(LOG, "Found WAL segment: %s", wal_segment_path);
		}

		if (file_exists)
		{
			/* Do not check for target LSN */
			if (segment_only)
				return InvalidXLogRecPtr;

			/*
			 * A WAL segment found. Look for target LSN in it.
			 */
			if (!XRecOffIsNull(target_lsn) &&
				  wal_contains_lsn(wal_segment_dir, target_lsn, tli,
									instance_config.xlog_seg_size))
				/* Target LSN was found */
			{
				elog(LOG, "Found LSN: %X/%X", (uint32) (target_lsn >> 32), (uint32) target_lsn);
				return target_lsn;
			}

			/*
			 * If we failed to get target LSN in a reasonable time, try
			 * to get LSN of last valid record prior to the target LSN. But only
			 * in case of a backup from a replica.
			 * Note, that with NullXRecOff target_lsn we do not wait
			 * for 'timeout / 2' seconds before going for previous record,
			 * because such LSN cannot be delivered at all.
			 *
			 * There are two cases for this:
			 * 1. Replica returned readpoint LSN which just do not exists. We want to look
			 *  for previous record in the same(!) WAL segment which endpoint points to this LSN.
			 * 2. Replica returened endpoint LSN with NullXRecOff. We want to look
			 *  for previous record which endpoint points greater or equal LSN in previous WAL segment.
			 */
			if (current.from_replica &&
				(XRecOffIsNull(target_lsn) || try_count > timeout / 2))
			{
				XLogRecPtr	res;

				res = get_prior_record_lsn(wal_segment_dir, current.start_lsn, target_lsn, tli,
									   in_prev_segment, instance_config.xlog_seg_size);

				if (!XLogRecPtrIsInvalid(res))
				{
					/* LSN of the prior record was found */
					elog(LOG, "Found prior LSN: %X/%X",
						 (uint32) (res >> 32), (uint32) res);
					return res;
				}
			}
		}

		sleep(1);
		if (interrupted)
			elog(ERROR, "Interrupted during waiting for WAL archiving");
		try_count++;

		/* Inform user if WAL segment is absent in first attempt */
		if (try_count == 1)
		{
			if (segment_only)
				elog(INFO, "Wait for WAL segment %s to be %s",
					 wal_segment_path, wal_delivery_str);
			else
				elog(INFO, "Wait for LSN %X/%X in %s WAL segment %s",
					 (uint32) (target_lsn >> 32), (uint32) target_lsn,
					 wal_delivery_str, wal_segment_path);
		}

		if (!stream_wal && is_start_lsn && try_count == 30)
			elog(WARNING, "By default pg_probackup assume WAL delivery method to be ARCHIVE. "
				 "If continuous archiving is not set up, use '--stream' option to make autonomous backup. "
				 "Otherwise check that continuous archiving works correctly.");

		if (timeout > 0 && try_count > timeout)
		{
			if (file_exists)
				elog(timeout_elevel, "WAL segment %s was %s, "
					 "but target LSN %X/%X could not be archived in %d seconds",
					 wal_segment, wal_delivery_str,
					 (uint32) (target_lsn >> 32), (uint32) target_lsn, timeout);
			/* If WAL segment doesn't exist or we wait for previous segment */
			else
				elog(timeout_elevel,
					 "WAL segment %s could not be %s in %d seconds",
					 wal_segment, wal_delivery_str, timeout);

			return InvalidXLogRecPtr;
		}
	}
}

/*
 * Notify end of backup to PostgreSQL server.
 */
static void
pg_stop_backup(pgBackup *backup, PGconn *pg_startbackup_conn,
				PGNodeInfo *nodeInfo)
{
	PGconn		*conn;
	PGresult	*res;
	PGresult	*tablespace_map_content = NULL;
	uint32		lsn_hi;
	uint32		lsn_lo;
	//XLogRecPtr	restore_lsn = InvalidXLogRecPtr;
	int			pg_stop_backup_timeout = 0;
	char		path[MAXPGPATH];
	char		backup_label[MAXPGPATH];
	FILE		*fp;
	pgFile		*file;
	size_t		len;
	char	   *val = NULL;
	char	   *stop_backup_query = NULL;
	bool		stop_lsn_exists = false;
	XLogRecPtr	stop_backup_lsn_tmp = InvalidXLogRecPtr;

	/*
	 * We will use this values if there are no transactions between start_lsn
	 * and stop_lsn.
	 */
	time_t		recovery_time;
	TransactionId recovery_xid;

	if (!backup_in_progress)
		elog(ERROR, "backup is not in progress");

	conn = pg_startbackup_conn;

	/* Remove annoying NOTICE messages generated by backend */
	res = pgut_execute(conn, "SET client_min_messages = warning;",
					   0, NULL);
	PQclear(res);

	/* Make proper timestamp format for parse_time() */
	res = pgut_execute(conn, "SET datestyle = 'ISO, DMY';", 0, NULL);
	PQclear(res);

	/* Create restore point
	 * Only if backup is from master.
	 * For PG 9.5 create restore point only if pguser is superuser.
	 */
	if (backup != NULL && !backup->from_replica &&
		!(nodeInfo->server_version < 90600 &&
		  !nodeInfo->is_superuser))
	{
		const char *params[1];
		char		name[1024];

		snprintf(name, lengthof(name), "pg_probackup, backup_id %s",
					base36enc(backup->start_time));
		params[0] = name;

		res = pgut_execute(conn, "SELECT pg_catalog.pg_create_restore_point($1)",
						   1, params);
		PQclear(res);
	}

	/*
	 * send pg_stop_backup asynchronously because we could came
	 * here from backup_cleanup() after some error caused by
	 * postgres archive_command problem and in this case we will
	 * wait for pg_stop_backup() forever.
	 */

	if (!pg_stop_backup_is_sent)
	{
		bool		sent = false;

		if (!exclusive_backup)
		{
			/*
			 * Stop the non-exclusive backup. Besides stop_lsn it returns from
			 * pg_stop_backup(false) copy of the backup label and tablespace map
			 * so they can be written to disk by the caller.
			 * In case of backup from replica >= 9.6 we do not trust minRecPoint
			 * and stop_backup LSN, so we use latest replayed LSN as STOP LSN.
			 */

			/* current is used here because of cleanup */
			if (current.from_replica)
				stop_backup_query = "SELECT"
									" pg_catalog.txid_snapshot_xmax(pg_catalog.txid_current_snapshot()),"
									" current_timestamp(0)::timestamptz,"
#if PG_VERSION_NUM >= 100000
									" pg_catalog.pg_last_wal_replay_lsn(),"
#else
									" pg_catalog.pg_last_xlog_replay_location(),"
#endif
									" labelfile,"
									" spcmapfile"
#if PG_VERSION_NUM >= 100000
									" FROM pg_catalog.pg_stop_backup(false, false)";
#else
									" FROM pg_catalog.pg_stop_backup(false)";
#endif
			else
				stop_backup_query = "SELECT"
									" pg_catalog.txid_snapshot_xmax(pg_catalog.txid_current_snapshot()),"
									" current_timestamp(0)::timestamptz,"
									" lsn,"
									" labelfile,"
									" spcmapfile"
#if PG_VERSION_NUM >= 100000
									" FROM pg_catalog.pg_stop_backup(false, false)";
#else
									" FROM pg_catalog.pg_stop_backup(false)";
#endif

		}
		else
		{
			stop_backup_query =	"SELECT"
								" pg_catalog.txid_snapshot_xmax(pg_catalog.txid_current_snapshot()),"
								" current_timestamp(0)::timestamptz,"
								" pg_catalog.pg_stop_backup() as lsn";
		}

		sent = pgut_send(conn, stop_backup_query, 0, NULL, WARNING);
		pg_stop_backup_is_sent = true;
		if (!sent)
			elog(ERROR, "Failed to send pg_stop_backup query");
	}

	/* After we have sent pg_stop_backup, we don't need this callback anymore */
	pgut_atexit_pop(backup_stopbackup_callback, pg_startbackup_conn);

	/*
	 * Wait for the result of pg_stop_backup(), but no longer than
	 * archive_timeout seconds
	 */
	if (pg_stop_backup_is_sent && !in_cleanup)
	{
		res = NULL;

		while (1)
		{
			if (!PQconsumeInput(conn))
				elog(ERROR, "pg_stop backup() failed: %s",
						PQerrorMessage(conn));

			if (PQisBusy(conn))
			{
				pg_stop_backup_timeout++;
				sleep(1);

				if (interrupted)
				{
					pgut_cancel(conn);
					elog(ERROR, "interrupted during waiting for pg_stop_backup");
				}

				if (pg_stop_backup_timeout == 1)
					elog(INFO, "wait for pg_stop_backup()");

				/*
				 * If postgres haven't answered in archive_timeout seconds,
				 * send an interrupt.
				 */
				if (pg_stop_backup_timeout > instance_config.archive_timeout)
				{
					pgut_cancel(conn);
					elog(ERROR, "pg_stop_backup doesn't answer in %d seconds, cancel it",
						 instance_config.archive_timeout);
				}
			}
			else
			{
				res = PQgetResult(conn);
				break;
			}
		}

		/* Check successfull execution of pg_stop_backup() */
		if (!res)
			elog(ERROR, "pg_stop backup() failed");
		else
		{
			switch (PQresultStatus(res))
			{
				/*
				 * We should expect only PGRES_TUPLES_OK since pg_stop_backup
				 * returns tuples.
				 */
				case PGRES_TUPLES_OK:
					break;
				default:
					elog(ERROR, "query failed: %s query was: %s",
						 PQerrorMessage(conn), stop_backup_query);
			}
			elog(INFO, "pg_stop backup() successfully executed");
		}

		backup_in_progress = false;

//		char *target_lsn = "2/F578A000";
//		XLogDataFromLSN(target_lsn, &lsn_hi, &lsn_lo);

		/* Extract timeline and LSN from results of pg_stop_backup() */
		XLogDataFromLSN(PQgetvalue(res, 0, 2), &lsn_hi, &lsn_lo);
		/* Calculate LSN */
		stop_backup_lsn_tmp = ((uint64) lsn_hi) << 32 | lsn_lo;

		/* It is ok for replica to return invalid STOP LSN
		 * UPD: Apparently it is ok even for a master.
		 */
		if (!XRecOffIsValid(stop_backup_lsn_tmp))
		{
			char	   *xlog_path,
						stream_xlog_path[MAXPGPATH];
			XLogSegNo	segno = 0;
			XLogRecPtr	lsn_tmp = InvalidXLogRecPtr;

			/*
			 * Even though the value is invalid, it's expected postgres behaviour
			 * and we're trying to fix it below.
			 */
			elog(LOG, "Invalid offset in stop_lsn value %X/%X, trying to fix",
				 (uint32) (stop_backup_lsn_tmp >> 32), (uint32) (stop_backup_lsn_tmp));

			/*
			 * Note: even with gdb it is very hard to produce automated tests for
			 * contrecord + invalid LSN, so emulate it for manual testing.
			 */
			//stop_backup_lsn_tmp = stop_backup_lsn_tmp - XLOG_SEG_SIZE;
			//elog(WARNING, "New Invalid stop_backup_lsn value %X/%X",
			//	 (uint32) (stop_backup_lsn_tmp >> 32), (uint32) (stop_backup_lsn_tmp));

			if (stream_wal)
			{
				pgBackupGetPath2(backup, stream_xlog_path,
								 lengthof(stream_xlog_path),
								 DATABASE_DIR, PG_XLOG_DIR);
				xlog_path = stream_xlog_path;
			}
			else
				xlog_path = arclog_path;

			GetXLogSegNo(stop_backup_lsn_tmp, segno, instance_config.xlog_seg_size);

			/*
			 * Note, that there is no guarantee that corresponding WAL file even exists.
			 * Replica may return LSN from future and keep staying in present.
			 * Or it can return invalid LSN.
			 *
			 * That's bad, since we want to get real LSN to save it in backup label file
			 * and to use it in WAL validation.
			 *
			 * So we try to do the following:
			 * 1. Wait 'archive_timeout' seconds for segment containing stop_lsn and
			 *	  look for the first valid record in it.
			 * 	  It solves the problem of occasional invalid LSN on write-busy system.
			 * 2. Failing that, look for record in previous segment with endpoint
			 *	  equal or greater than stop_lsn. It may(!) solve the problem of invalid LSN
			 *	  on write-idle system. If that fails too, error out.
			 */

			/* stop_lsn is pointing to a 0 byte of xlog segment */
			if (stop_backup_lsn_tmp % instance_config.xlog_seg_size == 0)
			{
				/* Wait for segment with current stop_lsn, it is ok for it to never arrive */
				wait_wal_lsn(stop_backup_lsn_tmp, false, backup->tli,
							 false, true, WARNING, stream_wal);

				/* Get the first record in segment with current stop_lsn */
				lsn_tmp = get_first_record_lsn(xlog_path, segno, backup->tli,
										       instance_config.xlog_seg_size,
										       instance_config.archive_timeout);

				/* Check that returned LSN is valid and greater than stop_lsn */
				if (XLogRecPtrIsInvalid(lsn_tmp) ||
					!XRecOffIsValid(lsn_tmp) ||
					lsn_tmp < stop_backup_lsn_tmp)
				{
					/* Backup from master should error out here */
					if (!backup->from_replica)
						elog(ERROR, "Failed to get next WAL record after %X/%X",
									(uint32) (stop_backup_lsn_tmp >> 32),
									(uint32) (stop_backup_lsn_tmp));

					/* No luck, falling back to looking up for previous record */
					elog(WARNING, "Failed to get next WAL record after %X/%X, "
								"looking for previous WAL record",
								(uint32) (stop_backup_lsn_tmp >> 32),
								(uint32) (stop_backup_lsn_tmp));

					/* Despite looking for previous record there is not guarantee of success
					 * because previous record can be the contrecord.
					 */
					lsn_tmp = wait_wal_lsn(stop_backup_lsn_tmp, false, backup->tli,
											true, false, ERROR, stream_wal);

					/* sanity */
					if (!XRecOffIsValid(lsn_tmp) || XLogRecPtrIsInvalid(lsn_tmp))
						elog(ERROR, "Failed to get WAL record prior to %X/%X",
									(uint32) (stop_backup_lsn_tmp >> 32),
									(uint32) (stop_backup_lsn_tmp));
				}
			}
			/* stop lsn is aligned to xlog block size, just find next lsn */
			else if (stop_backup_lsn_tmp % XLOG_BLCKSZ == 0)
			{
				/* Wait for segment with current stop_lsn */
				wait_wal_lsn(stop_backup_lsn_tmp, false, backup->tli,
							 false, true, ERROR, stream_wal);

				/* Get the next closest record in segment with current stop_lsn */
				lsn_tmp = get_next_record_lsn(xlog_path, segno, backup->tli,
										       instance_config.xlog_seg_size,
										       instance_config.archive_timeout,
										       stop_backup_lsn_tmp);

				/* sanity */
				if (!XRecOffIsValid(lsn_tmp) || XLogRecPtrIsInvalid(lsn_tmp))
					elog(ERROR, "Failed to get WAL record next to %X/%X",
								(uint32) (stop_backup_lsn_tmp >> 32),
								(uint32) (stop_backup_lsn_tmp));
			}
			/* PostgreSQL returned something very illegal as STOP_LSN, error out */
			else
				elog(ERROR, "Invalid stop_backup_lsn value %X/%X",
					 (uint32) (stop_backup_lsn_tmp >> 32), (uint32) (stop_backup_lsn_tmp));

			/* Setting stop_backup_lsn will set stop point for streaming */
			stop_backup_lsn = lsn_tmp;
			stop_lsn_exists = true;
		}

		elog(LOG, "stop_lsn: %X/%X",
			(uint32) (stop_backup_lsn_tmp >> 32), (uint32) (stop_backup_lsn_tmp));

		/* Write backup_label and tablespace_map */
		if (!exclusive_backup)
		{
			Assert(PQnfields(res) >= 4);
			pgBackupGetPath(backup, path, lengthof(path), DATABASE_DIR);

			/* Write backup_label */
			join_path_components(backup_label, path, PG_BACKUP_LABEL_FILE);
			fp = fio_fopen(backup_label, PG_BINARY_W, FIO_BACKUP_HOST);
			if (fp == NULL)
				elog(ERROR, "can't open backup label file \"%s\": %s",
					 backup_label, strerror(errno));

			len = strlen(PQgetvalue(res, 0, 3));
			if (fio_fwrite(fp, PQgetvalue(res, 0, 3), len) != len ||
				fio_fflush(fp) != 0 ||
				fio_fclose(fp))
				elog(ERROR, "can't write backup label file \"%s\": %s",
					 backup_label, strerror(errno));

			/*
			 * It's vital to check if backup_files_list is initialized,
			 * because we could get here because the backup was interrupted
			 */
			if (backup_files_list)
			{
				file = pgFileNew(backup_label, PG_BACKUP_LABEL_FILE, true, 0,
								 FIO_BACKUP_HOST);

				file->crc = pgFileGetCRC(backup_label, true, false);

				file->write_size = file->size;
				file->uncompressed_size = file->size;
				parray_append(backup_files_list, file);
			}
		}

		if (sscanf(PQgetvalue(res, 0, 0), XID_FMT, &recovery_xid) != 1)
			elog(ERROR,
				 "result of txid_snapshot_xmax() is invalid: %s",
				 PQgetvalue(res, 0, 0));
		if (!parse_time(PQgetvalue(res, 0, 1), &recovery_time, true))
			elog(ERROR,
				 "result of current_timestamp is invalid: %s",
				 PQgetvalue(res, 0, 1));

		/* Get content for tablespace_map from stop_backup results
		 * in case of non-exclusive backup
		 */
		if (!exclusive_backup)
			val = PQgetvalue(res, 0, 4);

		/* Write tablespace_map */
		if (!exclusive_backup && val && strlen(val) > 0)
		{
			char		tablespace_map[MAXPGPATH];

			join_path_components(tablespace_map, path, PG_TABLESPACE_MAP_FILE);
			fp = fio_fopen(tablespace_map, PG_BINARY_W, FIO_BACKUP_HOST);
			if (fp == NULL)
				elog(ERROR, "can't open tablespace map file \"%s\": %s",
					 tablespace_map, strerror(errno));

			len = strlen(val);
			if (fio_fwrite(fp, val, len) != len ||
				fio_fflush(fp) != 0 ||
				fio_fclose(fp))
				elog(ERROR, "can't write tablespace map file \"%s\": %s",
					 tablespace_map, strerror(errno));

			if (backup_files_list)
			{
				file = pgFileNew(tablespace_map, PG_TABLESPACE_MAP_FILE, true, 0,
								 FIO_BACKUP_HOST);
				if (S_ISREG(file->mode))
				{
					file->crc = pgFileGetCRC(tablespace_map, true, false);
					file->write_size = file->size;
				}

				parray_append(backup_files_list, file);
			}
		}

		if (tablespace_map_content)
			PQclear(tablespace_map_content);
		PQclear(res);
	}

	/* Fill in fields if that is the correct end of backup. */
	if (backup != NULL)
	{
		char	   *xlog_path,
					stream_xlog_path[MAXPGPATH];

		/*
		 * Wait for stop_lsn to be archived or streamed.
		 * If replica returned valid STOP_LSN of not actually existing record,
		 * look for previous record with endpoint >= STOP_LSN.
		 */
		if (!stop_lsn_exists)
			stop_backup_lsn = wait_wal_lsn(stop_backup_lsn_tmp, false, backup->tli,
											false, false, ERROR, stream_wal);

		if (stream_wal)
		{
			/* Wait for the completion of stream */
			pthread_join(stream_thread, NULL);
			if (stream_thread_arg.ret == 1)
				elog(ERROR, "WAL streaming failed");

			pgBackupGetPath2(backup, stream_xlog_path,
							 lengthof(stream_xlog_path),
							 DATABASE_DIR, PG_XLOG_DIR);
			xlog_path = stream_xlog_path;
		}
		else
			xlog_path = arclog_path;

		backup->stop_lsn = stop_backup_lsn;
		backup->recovery_xid = recovery_xid;

		elog(LOG, "Getting the Recovery Time from WAL");

		/* iterate over WAL from stop_backup lsn to start_backup lsn */
		if (!read_recovery_info(xlog_path, backup->tli,
								instance_config.xlog_seg_size,
								backup->start_lsn, backup->stop_lsn,
								&backup->recovery_time))
		{
			elog(LOG, "Failed to find Recovery Time in WAL, forced to trust current_timestamp");
			backup->recovery_time = recovery_time;
		}
	}
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
 * Notify end of backup to server when "backup_label" is in the root directory
 * of the DB cluster.
 * Also update backup status to ERROR when the backup is not finished.
 */
static void
backup_cleanup(bool fatal, void *userdata)
{
	/*
	 * Update status of backup in BACKUP_CONTROL_FILE to ERROR.
	 * end_time != 0 means backup finished
	 */
	if (current.status == BACKUP_STATUS_RUNNING && current.end_time == 0)
	{
		elog(WARNING, "Backup %s is running, setting its status to ERROR",
			 base36enc(current.start_time));
		current.end_time = time(NULL);
		current.status = BACKUP_STATUS_ERROR;
		write_backup(&current, true);
	}
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
backup_files(void *arg)
{
	int			i;
	char		from_fullpath[MAXPGPATH];
	char		to_fullpath[MAXPGPATH];
	static time_t prev_time;

	backup_files_arg *arguments = (backup_files_arg *) arg;
	int 		n_backup_files_list = parray_num(arguments->files_list);

	prev_time = current.start_time;

	/* backup a file */
	for (i = 0; i < n_backup_files_list; i++)
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
				write_backup_filelist(&current, arguments->files_list, arguments->from_root,
									  arguments->external_dirs, false);
				/* update backup control file to update size info */
				write_backup(&current, true);

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
				 i + 1, n_backup_files_list, file->rel_path);

		/* Handle zero sized files */
		if (file->size == 0)
		{
			file->write_size = 0;
			continue;
		}

		/* construct destination filepath */
		if (file->external_dir_num == 0)
		{
			join_path_components(from_fullpath, arguments->from_root, file->rel_path);
			join_path_components(to_fullpath, arguments->to_root, file->rel_path);
		}
		else
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

		/* Encountered some strange beast */
		if (!S_ISREG(file->mode))
			elog(WARNING, "Unexpected type %d of file \"%s\", skipping",
							file->mode, from_fullpath);

		/* Check that file exist in previous backup */
		if (current.backup_mode != BACKUP_MODE_FULL)
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
			backup_data_file(&(arguments->conn_arg), file, from_fullpath, to_fullpath,
								 arguments->prev_start_lsn,
								 current.backup_mode,
								 instance_config.compress_alg,
								 instance_config.compress_level,
								 arguments->nodeInfo->checksum_version,
								 arguments->nodeInfo->ptrack_version_num,
								 arguments->nodeInfo->ptrack_schema,
								 arguments->hdr_map, false);
		}
		else
		{
			backup_non_data_file(file, prev_file, from_fullpath, to_fullpath,
								 current.backup_mode, current.parent_backup, true);
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

/*
 * Extract information about files in backup_list parsing their names:
 * - remove temp tables from the list
 * - remove unlogged tables from the list (leave the _init fork)
 * - set flags for database directories
 * - set flags for datafiles
 */
void
parse_filelist_filenames(parray *files, const char *root)
{
	size_t		i = 0;
	Oid			unlogged_file_reloid = 0;

	while (i < parray_num(files))
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);
		int 		sscanf_result;

		if (S_ISREG(file->mode) &&
			path_is_prefix_of_path(PG_TBLSPC_DIR, file->rel_path))
		{
			/*
			 * Found file in pg_tblspc/tblsOid/TABLESPACE_VERSION_DIRECTORY
			 * Legal only in case of 'pg_compression'
			 */
			if (strcmp(file->name, "pg_compression") == 0)
			{
				Oid			tblspcOid;
				Oid			dbOid;
				char		tmp_rel_path[MAXPGPATH];
				/*
				 * Check that the file is located under
				 * TABLESPACE_VERSION_DIRECTORY
				 */
				sscanf_result = sscanf(file->rel_path, PG_TBLSPC_DIR "/%u/%s/%u",
									   &tblspcOid, tmp_rel_path, &dbOid);

				/* Yes, it is */
				if (sscanf_result == 2 &&
					strncmp(tmp_rel_path, TABLESPACE_VERSION_DIRECTORY,
							strlen(TABLESPACE_VERSION_DIRECTORY)) == 0)
					set_cfs_datafiles(files, root, file->rel_path, i);
			}
		}

		if (S_ISREG(file->mode) && file->tblspcOid != 0 &&
			file->name && file->name[0])
		{
			if (file->forkName == init)
			{
				/*
				 * Do not backup files of unlogged relations.
				 * scan filelist backward and exclude these files.
				 */
				int			unlogged_file_num = i - 1;
				pgFile	   *unlogged_file = (pgFile *) parray_get(files,
																  unlogged_file_num);

				unlogged_file_reloid = file->relOid;

				while (unlogged_file_num >= 0 &&
					   (unlogged_file_reloid != 0) &&
					   (unlogged_file->relOid == unlogged_file_reloid))
				{
					pgFileFree(unlogged_file);
					parray_remove(files, unlogged_file_num);

					unlogged_file_num--;
					i--;

					unlogged_file = (pgFile *) parray_get(files,
														  unlogged_file_num);
				}
			}
		}

		i++;
	}
}

/* If file is equal to pg_compression, then we consider this tablespace as
 * cfs-compressed and should mark every file in this tablespace as cfs-file
 * Setting is_cfs is done via going back through 'files' set every file
 * that contain cfs_tablespace in his path as 'is_cfs'
 * Goings back through array 'files' is valid option possible because of current
 * sort rules:
 * tblspcOid/TABLESPACE_VERSION_DIRECTORY
 * tblspcOid/TABLESPACE_VERSION_DIRECTORY/dboid
 * tblspcOid/TABLESPACE_VERSION_DIRECTORY/dboid/1
 * tblspcOid/TABLESPACE_VERSION_DIRECTORY/dboid/1.cfm
 * tblspcOid/TABLESPACE_VERSION_DIRECTORY/pg_compression
 */
static void
set_cfs_datafiles(parray *files, const char *root, char *relative, size_t i)
{
	int			len;
	int			p;
	pgFile	   *prev_file;
	char	   *cfs_tblspc_path;

	cfs_tblspc_path = strdup(relative);
	if(!cfs_tblspc_path)
		elog(ERROR, "Out of memory");
	len = strlen("/pg_compression");
	cfs_tblspc_path[strlen(cfs_tblspc_path) - len] = 0;
	elog(VERBOSE, "CFS DIRECTORY %s, pg_compression path: %s", cfs_tblspc_path, relative);

	for (p = (int) i; p >= 0; p--)
	{
		prev_file = (pgFile *) parray_get(files, (size_t) p);

		elog(VERBOSE, "Checking file in cfs tablespace %s", prev_file->rel_path);

		if (strstr(prev_file->rel_path, cfs_tblspc_path) != NULL)
		{
			if (S_ISREG(prev_file->mode) && prev_file->is_datafile)
			{
				elog(VERBOSE, "Setting 'is_cfs' on file %s, name %s",
					prev_file->rel_path, prev_file->name);
				prev_file->is_cfs = true;
			}
		}
		else
		{
			elog(VERBOSE, "Breaking on %s", prev_file->rel_path);
			break;
		}
	}
	free(cfs_tblspc_path);
}

/*
 * Find pgfile by given rnode in the backup_files_list
 * and add given blkno to its pagemap.
 */
void
process_block_change(ForkNumber forknum, RelFileNode rnode, BlockNumber blkno)
{
//	char	   *path;
	char	   *rel_path;
	BlockNumber blkno_inseg;
	int			segno;
	pgFile	  **file_item;
	pgFile		f;

	segno = blkno / RELSEG_SIZE;
	blkno_inseg = blkno % RELSEG_SIZE;

	rel_path = relpathperm(rnode, forknum);
	if (segno > 0)
		f.rel_path = psprintf("%s.%u", rel_path, segno);
	else
		f.rel_path = rel_path;

	f.external_dir_num = 0;

	/* backup_files_list should be sorted before */
	file_item = (pgFile **) parray_bsearch(backup_files_list, &f,
										   pgFileCompareRelPathWithExternal);

	/*
	 * If we don't have any record of this file in the file map, it means
	 * that it's a relation that did not have much activity since the last
	 * backup. We can safely ignore it. If it is a new relation file, the
	 * backup would simply copy it as-is.
	 */
	if (file_item)
	{
		/* We need critical section only we use more than one threads */
		if (num_threads > 1)
			pthread_lock(&backup_pagemap_mutex);

		datapagemap_add(&(*file_item)->pagemap, blkno_inseg);

		if (num_threads > 1)
			pthread_mutex_unlock(&backup_pagemap_mutex);
	}

	if (segno > 0)
		pg_free(f.rel_path);
	pg_free(rel_path);

}

/*
 * Stop WAL streaming if current 'xlogpos' exceeds 'stop_backup_lsn', which is
 * set by pg_stop_backup().
 *
 * TODO: Add streamed file to file list when segment is finished
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
		elog(VERBOSE, _("finished segment at %X/%X (timeline %u)"),
			 (uint32) (xlogpos >> 32), (uint32) xlogpos, timeline);

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
			true);
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

	elog(LOG, "finished streaming WAL at %X/%X (timeline %u)",
		 (uint32) (stop_stream_lsn >> 32), (uint32) stop_stream_lsn, stream_arg->starttli);
	stream_arg->ret = 0;

	PQfinish(stream_arg->conn);
	stream_arg->conn = NULL;

	return NULL;
}

static void
check_external_for_tablespaces(parray *external_list, PGconn *backup_conn)
{
	PGresult   *res;
	int			i = 0;
	int			j = 0;
	char	   *tablespace_path = NULL;
	char	   *query = "SELECT pg_catalog.pg_tablespace_location(oid) "
						"FROM pg_catalog.pg_tablespace "
						"WHERE pg_catalog.pg_tablespace_location(oid) <> '';";

	res = pgut_execute(backup_conn, query, 0, NULL);

	/* Check successfull execution of query */
	if (!res)
		elog(ERROR, "Failed to get list of tablespaces");

	for (i = 0; i < res->ntups; i++)
	{
		tablespace_path = PQgetvalue(res, i, 0);
		Assert (strlen(tablespace_path) > 0);

		canonicalize_path(tablespace_path);

		for (j = 0; j < parray_num(external_list); j++)
		{
			char *external_path = parray_get(external_list, j);

			if (path_is_prefix_of_path(external_path, tablespace_path))
				elog(ERROR, "External directory path (-E option) \"%s\" "
							"contains tablespace \"%s\"",
							external_path, tablespace_path);
			if (path_is_prefix_of_path(tablespace_path, external_path))
				elog(WARNING, "External directory path (-E option) \"%s\" "
							  "is in tablespace directory \"%s\"",
							  tablespace_path, external_path);
		}
	}
	PQclear(res);

	/* Check that external directories do not overlap */
	if (parray_num(external_list) < 2)
		return;

	for (i = 0; i < parray_num(external_list); i++)
	{
		char *external_path = parray_get(external_list, i);

		for (j = 0; j < parray_num(external_list); j++)
		{
			char *tmp_external_path = parray_get(external_list, j);

			/* skip yourself */
			if (j == i)
				continue;

			if (path_is_prefix_of_path(external_path, tmp_external_path))
				elog(ERROR, "External directory path (-E option) \"%s\" "
							"contain another external directory \"%s\"",
							external_path, tmp_external_path);

		}
	}
}

/*
 * Run IDENTIFY_SYSTEM through a given connection and
 * check system identifier and timeline are matching
 */
void
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
