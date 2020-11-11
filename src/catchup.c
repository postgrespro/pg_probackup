/*-------------------------------------------------------------------------
 *
 * catchup.c: catch-up a replica
 *
 * Portions Copyright (c) 2015-2020, Postgres Professional
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

static void
do_catchup_instance(PGconn *catchup_donor_conn, PGNodeInfo *donor_node_info,
					CatchupParams *catchupParams);

/*
 * regular connection options and instance_config refer the server,
 * we get data from (donor). We expect it is active and accept connections.
 * 
 * 'catchup_pgdata_path' contiains a location of a recepient node PGDATA
 * Recipient instance must be shut down and contain a valid XLOG_CONTROL_FILE.
 *
 */
int
do_catchup(time_t start_time, pgSetBackupParams *set_backup_params,
		   char *catchup_pgdata_path)
{
	PGconn		*catchup_donor_conn = NULL;
	PGNodeInfo	donor_node_info;
	char		pretty_bytes[20];
	CatchupParams catchupParams;

	/* Initialize PGInfonode */
	pgNodeInit(&donor_node_info);

	if (!instance_config.pgdata)
		elog(ERROR, "required parameter not specified: PGDATA "
						 "(-D, --pgdata)");

	if(!catchup_pgdata_path)
		elog(ERROR, "required parameter not specified: --catchup-path");

	/* Ensure that catchup_pgdata_path is an absolute path */
	if (catchup_pgdata_path)
	{
		canonicalize_path(catchup_pgdata_path);

		if (!is_absolute_path(catchup_pgdata_path))
			elog(ERROR, "--catchup-path must be an absolute path");
	}

	catchupParams.dest_pgdata_path = catchup_pgdata_path;

	/* TODO Ensure that no instance is currently run on catchup pgdata */

	/* Read catchup params from the .. directory */
	get_catchup_from_control(catchup_pgdata_path, &catchupParams);

	/*
	 * Now do a catchup.
	 *
	 * From a donor's side it looks almost like an incremental backup,
	 * with a different destination.
	 *
	 * From a recipient's side it is almost like incremental restore,
	 * except that we do not touch configuration files.
	 * TODO: list these files explicitly
	 */

	/*
	 * setup catchup_donor_conn, do some compatibility checks and
	 * fill basic info about donor instance
	 */
	catchup_donor_conn = pgdata_basic_setup(instance_config.conn_opt, &donor_node_info);

	// TODO add check system identifier

	do_catchup_instance();

}

/* */
static void
do_catchup_instance(PGconn *catchup_donor_conn, PGNodeInfo *donor_node_info,
					CatchupParams *catchupParams)
{
	int			i;
	char		database_path[MAXPGPATH];
	char		dst_backup_path[MAXPGPATH];
	char		label[1024];

	/* arrays with meta info for multi threaded backup */
	pthread_t	*threads;
	backup_files_arg *threads_args;
	bool		catchup_isok = true;

	/* for fancy reporting */
	time_t		start_time, end_time;
	char		pretty_time[20];
	char		pretty_bytes[20];

	parray 		*dest_files_list = NULL;

	//TODO improve the message
	elog(LOG, "Database catchup start");

	/* notify start of backup to PostgreSQL server */
	time2iso(label, lengthof(label), current.start_time);
	strncat(label, " with pg_probackup", lengthof(label) -
			strlen(" with pg_probackup"));

	/* Call pg_start_backup function in PostgreSQL connect */
	pg_start_backup(label, smooth_checkpoint, &current, donor_node_info, catchup_donor_conn);

	/* Obtain current timeline. Do not support versions < 9.6 */
	current.tli = get_current_timeline(catchup_donor_conn);

	{
	/* In PAGE mode or in ARCHIVE wal-mode wait for current segment */
	if (current.backup_mode == BACKUP_MODE_DIFF_PAGE || !stream_wal)
		/*
		 * Do not wait start_lsn for stream backup.
		 * Because WAL streaming will start after pg_start_backup() in stream
		 * mode.
		 */
		wait_wal_lsn(current.start_lsn, true, current.tli, false, true, ERROR, false);
	}


	/* For incremental backup check that start_lsn is not from the past
	 * Though it will not save us if PostgreSQL instance is actually
	 * restored STREAM backup.
	 */
	if (current.backup_mode != BACKUP_MODE_FULL &&
		prev_backup->start_lsn > current.start_lsn)
			elog(ERROR, "Current START LSN %X/%X is lower than REDO LSN %X/%X of the catchup instance "
				"It may indicate that we are trying to catchup PostgreSQL instance from the future.",
				(uint32) (current.start_lsn >> 32), (uint32) (current.start_lsn),
				(uint32) (prev_backup->start_lsn >> 32), (uint32) (prev_backup->start_lsn));

	/* initialize backup's file list */
	backup_files_list = parray_new();

	/* start stream replication */
	if (stream_wal)
	{
		join_path_components(dst_catchup_wal_path,
							 catchupParams->dest_pgdata_path, PG_XLOG_DIR);

		start_WAL_streaming(catchup_donor_conn, dst_catchup_wal_path, &instance_config.conn_opt,
							current.start_lsn, current.tli);
	}

	/* list files with the logical path. omit $PGDATA */
	if (fio_is_remote(FIO_DB_HOST))
		fio_list_dir(backup_files_list, instance_config.pgdata,
					 true, true, false, backup_logs, true, 0);

	dir_list_file(dest_files_list, catchupParams->dest_pgdata_path,
				  true, true, false, false, true, 0, FIO_LOCAL_HOST);

	/* close ssh session in main thread */
	fio_disconnect();

	/* Sanity check for backup_files_list, thank you, Windows:
	 * https://github.com/postgrespro/pg_probackup/issues/48
	 */
	// TODO: specify pgdata in the message
	if (parray_num(backup_files_list) < 100)
		elog(ERROR, "PGDATA is almost empty. Either it was concurrently deleted or "
			"pg_probackup do not possess sufficient permissions to list PGDATA content");

	if (parray_num(dest_files_list) < 100)
		elog(ERROR, "PGDATA is almost empty. Either it was concurrently deleted or "
			"pg_probackup do not possess sufficient permissions to list PGDATA content");

	/*
	 * Sort pathname ascending.
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

	/*
	 * Setup threads
	 */
	for (i = 0; i < parray_num(backup_files_list); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(backup_files_list, i);

		/* setup threads */
		pg_atomic_clear_flag(&file->lock);
	}

	/* Sort by size for load balancing */
	parray_qsort(backup_files_list, pgFileCompareSize);

	/* Init backup page header map */
	init_header_map(&current);

	/* init thread args with own file lists */
	threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
	threads_args = (backup_files_arg *) palloc(sizeof(backup_files_arg)*num_threads);

	for (i = 0; i < num_threads; i++)
	{
		backup_files_arg *arg = &(threads_args[i]);

		arg->donor_node_info = donor_node_info;
		arg->from_root = instance_config.pgdata;
		arg->to_root = catchupParams->dest_pgdata_path;
		arg->files_list = backup_files_list;
		arg->prev_start_lsn = catchupParams->lsn;
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
		pthread_create(&threads[i], NULL, catchup_files, arg);
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
	pg_stop_backup(&current, catchup_donor_conn, donor_node_info);

	/* close and sync page header map */
	if (current.hdr_map.fp)
	{
		cleanup_header_map(&(current.hdr_map));

		if (fio_sync(current.hdr_map.path, FIO_BACKUP_HOST) != 0)
			elog(ERROR, "Cannot sync file \"%s\": %s", current.hdr_map.path, strerror(errno));
	}

	/* close ssh session in main thread */
	fio_disconnect();

	/* Cleanup */
	if (dest_backup_filelist)
	{
		parray_walk(dest_backup_filelist, pgFileFree);
		parray_free(dest_backup_filelist);
	}

	parray_walk(backup_files_list, pgFileFree);
	parray_free(backup_files_list);
	backup_files_list = NULL;
}
