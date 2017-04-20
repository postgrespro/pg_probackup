/*-------------------------------------------------------------------------
 *
 * backup.c: backup DB cluster, archived WAL
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <dirent.h>
#include <time.h>
#include <pthread.h>

#include "libpq/pqsignal.h"
#include "storage/bufpage.h"
#include "datapagemap.h"
#include "streamutil.h"
#include "receivelog.h"

static const char *backupModes[] = {"", "PAGE", "PTRACK", "FULL"};
static int	standby_message_timeout = 10 * 1000;	/* 10 sec = default */
static XLogRecPtr stop_backup_lsn = InvalidXLogRecPtr;
const char *progname = "pg_probackup";

/* list of files contained in backup */
static parray *backup_files_list = NULL;

static pthread_mutex_t check_stream_mut = PTHREAD_MUTEX_INITIALIZER;

static int is_ptrack_enable = false;

/* Backup connection */
static PGconn *backup_conn = NULL;

typedef struct
{
	const char *from_root;
	const char *to_root;
	parray *backup_files_list;
	parray *prev_backup_filelist;
	const XLogRecPtr *prev_backup_start_lsn;
} backup_files_args;

/*
 * Backup routines
 */
static void backup_cleanup(bool fatal, void *userdata);
static void backup_disconnect(bool fatal, void *userdata);

static void backup_files(void *arg);
static void do_backup_database(parray *backup_list);

static void pg_start_backup(const char *label, bool smooth, pgBackup *backup);
static void pg_stop_backup(pgBackup *backup);

static void add_pgdata_files(parray *files, const char *root);
static void write_backup_file_list(parray *files, const char *root);
static void wait_archive_lsn(XLogRecPtr lsn, bool prev_segno);
static void make_pagemap_from_ptrack(parray *files);
static void StreamLog(void *arg);

/* Ptrack functions */
static void pg_ptrack_clear(void);
static bool pg_ptrack_support(void);
static bool pg_ptrack_enable(void);
static bool pg_is_in_recovery(void);
static char *pg_ptrack_get_and_clear(Oid tablespace_oid,
									 Oid db_oid,
									 Oid rel_oid,
									 size_t *result_size);

/* Check functions */
static void check_server_version(void);
static void check_system_identifiers(void);
static void confirm_block_size(const char *name, int blcksz);


#define disconnect_and_exit(code)				\
	{											\
	if (conn != NULL) PQfinish(conn);			\
	exit(code);									\
	}


/*
 * Take a backup of database.
 */
static void
do_backup_database(parray *backup_list)
{
	size_t		i;
	char		database_path[MAXPGPATH];
	char		dst_backup_path[MAXPGPATH];
	char		label[1024];
	XLogRecPtr *prev_backup_start_lsn = NULL;

	pthread_t	backup_threads[num_threads];
	pthread_t	stream_thread;
	backup_files_args *backup_threads_args[num_threads];

	pgBackup   *prev_backup = NULL;
	char		prev_backup_filelist_path[MAXPGPATH];
	parray	   *prev_backup_filelist = NULL;

	elog(LOG, "Database backup start");

	/* Initialize size summary */
	current.data_bytes = 0;

	/* Obtain current timeline from PGDATA control file */
	current.tli = get_current_timeline(false);

	/*
	 * In incremental backup mode ensure that already-validated
	 * backup on current timeline exists.
	 */
	if (current.backup_mode == BACKUP_MODE_DIFF_PAGE ||
		current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		prev_backup = catalog_get_last_data_backup(backup_list, current.tli);
		if (prev_backup == NULL)
			elog(ERROR, "Valid backup on current timeline is not found."
						"Create new FULL backup before an incremental one.");
	}

	/* Clear ptrack files for FULL and PAGE backup */
	if (current.backup_mode != BACKUP_MODE_DIFF_PTRACK && is_ptrack_enable)
		pg_ptrack_clear();

	/* notify start of backup to PostgreSQL server */
	time2iso(label, lengthof(label), current.start_time);
	strncat(label, " with pg_probackup", lengthof(label));
	pg_start_backup(label, smooth_checkpoint, &current);

	pgBackupGetPath(&current, database_path, lengthof(database_path),
					DATABASE_DIR);

	/* start stream replication */
	if (stream_wal)
	{
		join_path_components(dst_backup_path, database_path, PG_XLOG_DIR);
		dir_create_dir(dst_backup_path, DIR_PERMISSION);

		pthread_mutex_lock(&check_stream_mut);
		pthread_create(&stream_thread, NULL, (void *(*)(void *)) StreamLog, dst_backup_path);
		pthread_mutex_lock(&check_stream_mut);
		if (conn == NULL)
			elog(ERROR, "Cannot continue backup because stream connect has failed.");

		pthread_mutex_unlock(&check_stream_mut);
	}

	/*
	 * If backup_label does not exist in $PGDATA, stop taking backup.
	 * NOTE. We can check it only on master, though.
	 */
	if(!from_replica)
	{
		char		label_path[MAXPGPATH];
		join_path_components(label_path, pgdata, PG_BACKUP_LABEL_FILE);

		/* Leave if no backup file */
		if (!fileExists(label_path))
		{
			elog(LOG, "%s does not exist, stopping backup", PG_BACKUP_LABEL_FILE);
			pg_stop_backup(NULL);
			elog(ERROR, "%s does not exist in PGDATA", PG_BACKUP_LABEL_FILE);
		}
	}

	/*
	 * To take incremental backup get the filelist of the last completed database
	 */
	if (current.backup_mode == BACKUP_MODE_DIFF_PAGE ||
		current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		Assert(prev_backup);
		pgBackupGetPath(prev_backup, prev_backup_filelist_path, lengthof(prev_backup_filelist_path),
						DATABASE_FILE_LIST);
		prev_backup_filelist = dir_read_file_list(pgdata, prev_backup_filelist_path);

		/* If lsn is not NULL, only pages with higher lsn will be copied. */
		prev_backup_start_lsn = &prev_backup->start_lsn;

		current.parent_backup = prev_backup->start_time;
		pgBackupWriteBackupControlFile(&current);
	}

	/* initialize backup list */
	backup_files_list = parray_new();

	/* list files with the logical path. omit $PGDATA */
	add_pgdata_files(backup_files_list, pgdata);

	if (current.backup_mode != BACKUP_MODE_FULL)
	{
		elog(LOG, "current_tli:%X", current.tli);
		elog(LOG, "prev_backup->start_lsn: %X/%X",
			 (uint32) (prev_backup->start_lsn >> 32), (uint32) (prev_backup->start_lsn));
		elog(LOG, "current.start_lsn: %X/%X",
			 (uint32) (current.start_lsn >> 32), (uint32) (current.start_lsn));

		/* TODO for some reason we sort the list for both incremental modes.
		 * Is it necessary?
		 */
		parray_qsort(backup_files_list, pgFileComparePathDesc);
	}

	/*
	 * Build page mapping in incremental mode.
	 */
	if (current.backup_mode == BACKUP_MODE_DIFF_PAGE)
	{
		/*
		 * Build the page map. Obtain information about changed pages
		 * reading WAL segments present in archives up to the point
		 * where this backup has started.
		 */
		extractPageMap(arclog_path, prev_backup->start_lsn, current.tli,
					   current.start_lsn,
					   /*
						* For backup from master wait for previous segment.
						* For backup from replica wait for current segment.
						*/
					   !from_replica);
	}
	else if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		XLogRecPtr	ptrack_lsn = get_last_ptrack_lsn();

		if (ptrack_lsn > prev_backup->stop_lsn)
		{
			elog(ERROR, "LSN from ptrack_control %lx differs from LSN of previous ptrack backup %lx.\n"
						"Create new full backup before an incremental one.",
						ptrack_lsn, prev_backup->start_lsn);
		}
		make_pagemap_from_ptrack(backup_files_list);
	}

	/* sort pathname ascending TODO What for?*/
	parray_qsort(backup_files_list, pgFileComparePath);

	/* make dirs before backup
	 * and setup threads at the same time
	 */
	for (i = 0; i < parray_num(backup_files_list); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(backup_files_list, i);

		/* if the entry was a directory, create it in the backup */
		if (S_ISDIR(file->mode))
		{
			char		dirpath[MAXPGPATH];
			char	   *dir_name = GetRelativePath(file->path, pgdata);

			if (verbose)
				elog(LOG, "Create directory \"%s\"", dir_name);

			join_path_components(dirpath, database_path, dir_name);
			dir_create_dir(dirpath, DIR_PERMISSION);
		}

		__sync_lock_release(&file->lock);
	}

	/* sort by size for load balancing */
	parray_qsort(backup_files_list, pgFileCompareSize);

	/* init thread args with own file lists */
	for (i = 0; i < num_threads; i++)
	{
		backup_files_args *arg = pg_malloc(sizeof(backup_files_args));

		arg->from_root = pgdata;
		arg->to_root = database_path;
		arg->backup_files_list = backup_files_list;
		arg->prev_backup_filelist = prev_backup_filelist;
		arg->prev_backup_start_lsn = prev_backup_start_lsn;
		backup_threads_args[i] = arg;
	}

	/* Run threads */
	for (i = 0; i < num_threads; i++)
	{
		if (verbose)
			elog(WARNING, "Start thread num:%li", parray_num(backup_threads_args[i]->backup_files_list));
		pthread_create(&backup_threads[i], NULL, (void *(*)(void *)) backup_files, backup_threads_args[i]);
	}

	/* Wait theads */
	for (i = 0; i < num_threads; i++)
	{
		pthread_join(backup_threads[i], NULL);
		pg_free(backup_threads_args[i]);
	}

	/* clean previous backup file list */
	if (prev_backup_filelist)
	{
		parray_walk(prev_backup_filelist, pgFileFree);
		parray_free(prev_backup_filelist);
	}

	/* Notify end of backup */
	pg_stop_backup(&current);

	/* Add archived xlog files into the list of files of this backup */
	if (stream_wal)
	{
		parray	   *xlog_files_list;
		char		pg_xlog_path[MAXPGPATH];

		/* Wait for the completion of stream */
		pthread_join(stream_thread, NULL);

		/* Scan backup PG_XLOG_DIR */
		xlog_files_list = parray_new();
		join_path_components(pg_xlog_path, database_path, PG_XLOG_DIR);
		dir_list_file(xlog_files_list, pg_xlog_path, false, true, false);

		for (i = 0; i < parray_num(xlog_files_list); i++)
		{
			pgFile	   *file = (pgFile *) parray_get(xlog_files_list, i);
			calc_file_checksum(file);
			/* Remove file path root prefix*/
			if (strstr(file->path, database_path) == file->path)
			{
				char	   *ptr = file->path;
				file->path = pstrdup(GetRelativePath(ptr, database_path));
				free(ptr);
			}
		}

		/* Add xlog files into the list of backed up files */
		parray_concat(backup_files_list, xlog_files_list);
		parray_free(xlog_files_list);
	}

	/* Print the list of files to backup catalog */
	write_backup_file_list(backup_files_list, pgdata);

	/* Compute summary of size of regular files in the backup */
	for (i = 0; i < parray_num(backup_files_list); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(backup_files_list, i);

		if (!S_ISREG(file->mode))
			continue;

		/* Count the amount of the data actually copied */
		current.data_bytes += file->write_size;
	}

	if (backup_files_list)
		parray_walk(backup_files_list, pgFileFree);
	parray_free(backup_files_list);
}

/*
 * Entry point of pg_probackup BACKUP subcommand.
 */
int
do_backup(void)
{
	parray	   *backup_list;
	bool		is_ptrack_support;

	/* PGDATA and BACKUP_MODE are always required */
	if (pgdata == NULL)
		elog(ERROR, "required parameter not specified: PGDATA "
						 "(-D, --pgdata)");
	if (current.backup_mode == BACKUP_MODE_INVALID)
		elog(ERROR, "required parameter not specified: BACKUP_MODE "
						 "(-b, --backup-mode)");

	/* Create connection for PostgreSQL */
	backup_conn = pgut_connect(pgut_dbname);
	pgut_atexit_push(backup_disconnect, NULL);

	/* Confirm that this server version is supported */
	check_server_version();
	/* Confirm data block size and xlog block size are compatible */
	confirm_block_size("block_size", BLCKSZ);
	confirm_block_size("wal_block_size", XLOG_BLCKSZ);

	from_replica = pg_is_in_recovery();
	current.checksum_version = get_data_checksum_version(true);
	current.stream = stream_wal;

	/* ptrack backup checks */
	is_ptrack_support = pg_ptrack_support();
	if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK && !is_ptrack_support)
		elog(ERROR, "This PostgreSQL instance does not support ptrack");

	if (is_ptrack_support)
	{
		is_ptrack_enable = pg_ptrack_enable();
		if(current.backup_mode == BACKUP_MODE_DIFF_PTRACK && !is_ptrack_enable)
			elog(ERROR, "Ptrack is disabled");
	}

	/* Get exclusive lock of backup catalog */
	catalog_lock();

	/*
	 * Ensure that backup directory was initialized for the same PostgreSQL
	 * instance we opened connection to. And that target backup database PGDATA
	 * belogns to the same instance.
	 */
	check_system_identifiers();

	elog(LOG, "Backup start. backup-mode = %s+%s",
		backupModes[current.backup_mode], current.stream?"STREAM":"ARCHIVE");

	/* Start backup. Update backup status. */
	current.status = BACKUP_STATUS_RUNNING;
	current.start_time = time(NULL);

	/* Create backup directory and BACKUP_CONTROL_FILE */
	if (pgBackupCreateDir(&current))
		elog(ERROR, "cannot create backup directory");
	pgBackupWriteBackupControlFile(&current);

	elog(LOG, "Backup destination is initialized");

	/* set the error processing function for the backup process */
	pgut_atexit_push(backup_cleanup, NULL);

	/* get list of backups already taken */
	backup_list = catalog_get_backup_list(INVALID_BACKUP_ID);
	if (backup_list == NULL)
		elog(ERROR, "Failed to get backup list.");

	/* backup data */
	do_backup_database(backup_list);
	pgut_atexit_pop(backup_cleanup, NULL);

	/* Backup is done. Update backup status */
	current.end_time = time(NULL);
	current.status = BACKUP_STATUS_DONE;
	pgBackupWriteBackupControlFile(&current);

	elog(LOG, "Backup completed. Total bytes : " INT64_FORMAT "",
		 current.data_bytes);

	pgBackupValidate(&current);

	return 0;
}

/*
 * Confirm that this server version is supported
 */
static void
check_server_version(void)
{
	static int server_version = 0;
	/* confirm server version */
	server_version = PQserverVersion(backup_conn);

	if (server_version < 90500)
		elog(ERROR,
			 "server version is %d.%d.%d, must be %s or higher",
			 server_version / 10000,
			 (server_version / 100) % 100,
			 server_version % 100, "9.5");

	if (from_replica && server_version < 90600)
		elog(ERROR,
			 "server version is %d.%d.%d, must be %s or higher for backup from replica",
			 server_version / 10000,
			 (server_version / 100) % 100,
			 server_version % 100, "9.6");
}

/*
 * Ensure that backup directory was initialized for the same PostgreSQL
 * instance we opened connection to. And that target backup database PGDATA
 * belogns to the same instance.
 * All system identifiers must be equal.
 */
static void
check_system_identifiers(void)
{
	PGresult   *res;
	uint64		system_id_conn;
	uint64		system_id_pgdata;
	char	   *val;

	system_id_pgdata = get_system_identifier();

	res = pgut_execute(backup_conn,
					   "SELECT system_identifier FROM pg_control_system()",
					   0, NULL);
	val = PQgetvalue(res, 0, 0);
	PQclear(res);

	if (!parse_uint64(val, &system_id_conn))
		elog(ERROR, "%s is not system_identifier", val);

	if (system_id_conn != system_identifier)
		elog(ERROR, "Backup data directory was initialized for system id %ld, but connected instance system id is %ld",
			 system_identifier, system_id_conn);

	if (system_id_pgdata != system_identifier)
		elog(ERROR, "Backup data directory was initialized for system id %ld, but target backup directory system id is %ld",
			 system_identifier, system_id_pgdata);
}

/*
 * Ensure that target backup database is initialized with
 * compatible settings. Currently check BLCKSZ and XLOG_BLCKSZ.
 */
static void
confirm_block_size(const char *name, int blcksz)
{
	PGresult   *res;
	char	   *endp;
	int			block_size;

	res = pgut_execute(backup_conn, "SELECT current_setting($1)", 1, &name);
	if (PQntuples(res) != 1 || PQnfields(res) != 1)
		elog(ERROR, "cannot get %s: %s", name, PQerrorMessage(backup_conn));

	block_size = strtol(PQgetvalue(res, 0, 0), &endp, 10);
	PQclear(res);

	if ((endp && *endp) || block_size != blcksz)
		elog(ERROR,
			 "%s(%d) is not compatible(%d expected)",
			 name, block_size, blcksz);
}

/*
 * Notify start of backup to PostgreSQL server.
 */
static void
pg_start_backup(const char *label, bool smooth, pgBackup *backup)
{
	PGresult   *res;
	const char *params[2];
	uint32		xlogid;
	uint32		xrecoff;

	params[0] = label;

	/* 2nd argument is 'fast'*/
	params[1] = smooth ? "false" : "true";
	if (from_replica)
		res = pgut_execute(backup_conn,
						   "SELECT pg_start_backup($1, $2, false)",
						   2,
						   params);
	else
		res = pgut_execute(backup_conn,
						   "SELECT pg_start_backup($1, $2)",
						   2,
						   params);

	/* Extract timeline and LSN from results of pg_start_backup() */
	XLogDataFromLSN(PQgetvalue(res, 0, 0), &xlogid, &xrecoff);
	/* Calculate LSN */
	backup->start_lsn = (XLogRecPtr) ((uint64) xlogid << 32) | xrecoff;

	if (!stream_wal)
		wait_archive_lsn(backup->start_lsn,
						 /*
						  * For backup from master wait for previous segment.
						  * For backup from replica wait for current segment.
						  */
						 !from_replica);

	PQclear(res);
}

/*
 * Check if the instance supports ptrack
 * TODO Implement check of ptrack_version() instead of existing one
 */
static bool
pg_ptrack_support(void)
{
	PGresult   *res_db;

	res_db = pgut_execute(backup_conn,
						  "SELECT proname FROM pg_proc WHERE proname='pg_ptrack_clear'",
						  0, NULL);

	if (PQntuples(res_db) == 0)
	{
		PQclear(res_db);
		return false;
	}
	PQclear(res_db);

	return true;
}

/* Check if ptrack is enabled in target instance */
static bool
pg_ptrack_enable(void)
{
	PGresult   *res_db;

	res_db = pgut_execute(backup_conn, "show ptrack_enable", 0, NULL);

	if (strcmp(PQgetvalue(res_db, 0, 0), "on") != 0)
	{
		PQclear(res_db);
		return false;
	}
	PQclear(res_db);
	return true;
}

/* Check if target instance is replica */
static bool
pg_is_in_recovery(void)
{
	PGresult   *res_db;

	res_db = pgut_execute(backup_conn, "SELECT pg_is_in_recovery()", 0, NULL);

	if (PQgetvalue(res_db, 0, 0)[0] == 't')
	{
		PQclear(res_db);
		return true;
	}
	PQclear(res_db);
	return false;
}

/* Clear ptrack files in all databases of the instance we connected to */
static void
pg_ptrack_clear(void)
{
	PGresult   *res_db,
			   *res;
	const char *dbname;
	int			i;

	res_db = pgut_execute(backup_conn, "SELECT datname FROM pg_database",
						  0, NULL);

	for(i = 0; i < PQntuples(res_db); i++)
	{
		PGconn	   *tmp_conn;

		dbname = PQgetvalue(res_db, i, 0);
		if (!strcmp(dbname, "template0"))
			continue;

		tmp_conn = pgut_connect(dbname);
		res = pgut_execute(tmp_conn, "SELECT pg_ptrack_clear()", 0, NULL);
		PQclear(res);

		pgut_disconnect(tmp_conn);
	}

	PQclear(res_db);
}

/* Read and clear ptrack files of the target relation.
 * Result is a bytea ptrack map of all segments of the target relation.
 */
static char *
pg_ptrack_get_and_clear(Oid tablespace_oid, Oid db_oid, Oid rel_oid,
						size_t *result_size)
{
	PGconn	   *tmp_conn;
	PGresult   *res_db,
			   *res;
	char	   *dbname;
	char	   *params[2];
	char	   *result;

	params[0] = palloc(64);
	params[1] = palloc(64);
	sprintf(params[0], "%i", db_oid);

	res_db = pgut_execute(backup_conn,
						  "SELECT datname FROM pg_database WHERE oid=$1",
						  1, (const char **) params);

	dbname = pstrdup(PQgetvalue(res_db, 0, 0));
	PQclear(res_db);

	tmp_conn = pgut_connect(dbname);
	sprintf(params[0], "%i", tablespace_oid);
	sprintf(params[1], "%i", rel_oid);

	res = pgut_execute(tmp_conn, "SELECT pg_ptrack_get_and_clear($1, $2)",
					   2, (const char **)params);
	result = (char *) PQunescapeBytea((unsigned char *) PQgetvalue(res, 0, 0),
									  result_size);
	PQclear(res);

	pgut_disconnect(tmp_conn);

	pfree(params[0]);
	pfree(params[1]);
	pfree(dbname);

	return result;
}

/*
 * Wait for target 'lsn' to be archived in archive 'wal' directory with
 * WAL segment file.
 */
static void
wait_archive_lsn(XLogRecPtr lsn, bool prev_segno)
{
	TimeLineID	tli;
	XLogSegNo	targetSegNo;
	char		wal_path[MAXPGPATH];
	char		wal_file[MAXFNAMELEN];
	uint32		try_count = 0;

	Assert(!stream_wal);

	tli = get_current_timeline(false);

	/* Compute the name of the WAL file containig requested LSN */
	XLByteToSeg(lsn, targetSegNo);
	if (prev_segno)
		targetSegNo--;
	XLogFileName(wal_file, tli, targetSegNo);

	join_path_components(wal_path, arclog_path, wal_file);

	/* Wait until switched WAL is archived */
	while (!fileExists(wal_path))
	{
		sleep(1);
		if (interrupted)
			elog(ERROR, "interrupted during waiting for WAL archiving");
		try_count++;

		/* Inform user if WAL segment is absent in first attempt */
		if (try_count == 1)
			elog(INFO, "wait for LSN %X/%X in archived WAL segment %s",
				 (uint32) (lsn >> 32), (uint32) lsn, wal_path);

		if (archive_timeout > 0 && try_count > archive_timeout)
			elog(ERROR,
				 "switched WAL segment %s could not be archived in %d seconds",
				 wal_file, archive_timeout);
	}

	/*
	 * WAL segment was archived. Check LSN on it if we waited current WAL
	 * segment, not previous.
	 */
	if (!prev_segno && !wal_contains_lsn(arclog_path, lsn, tli))
		elog(ERROR, "WAL segment %s doesn't contain target LSN %X/%X",
			 wal_file, (uint32) (lsn >> 32), (uint32) lsn);
}

/*
 * Notify end of backup to PostgreSQL server.
 */
static void
pg_stop_backup(pgBackup *backup)
{
	PGresult   *res;
	uint32		xlogid;
	uint32		xrecoff;

	/*
	 * We will use this values if there are no transactions between start_lsn
	 * and stop_lsn.
	 */
	time_t		recovery_time;
	TransactionId recovery_xid;

	/* Remove annoying NOTICE messages generated by backend */
	res = pgut_execute(backup_conn, "SET client_min_messages = warning;",
					   0, NULL);
	PQclear(res);

	if (from_replica)
		/*
		 * Stop the non-exclusive backup. Besides stop_lsn it returns from
		 * pg_stop_backup(false) copy of the backup label and tablespace map
		 * so they can be written to disk by the caller.
		 */
		res = pgut_execute(backup_conn,
						   "SELECT *, txid_snapshot_xmax(txid_current_snapshot()),"
						   " current_timestamp(0)::timestamp"
						   " FROM pg_stop_backup(false)",
						   0, NULL);
	else
		res = pgut_execute(backup_conn,
						   "SELECT *, txid_snapshot_xmax(txid_current_snapshot()),"
						   " current_timestamp(0)::timestamp"
						   " FROM pg_stop_backup()",
						   0, NULL);

	/* Extract timeline and LSN from results of pg_stop_backup() */
	XLogDataFromLSN(PQgetvalue(res, 0, 0), &xlogid, &xrecoff);
	/* Calculate LSN */
	stop_backup_lsn = (XLogRecPtr) ((uint64) xlogid << 32) | xrecoff;

	/* Write backup_label and tablespace_map for backup from replica */
	if (from_replica)
	{
		char		path[MAXPGPATH];
		char		backup_label[MAXPGPATH];
		FILE	   *fp;
		pgFile	   *file;

		Assert(PQnfields(res) >= 5);

		pgBackupGetPath(&current, path, lengthof(path), DATABASE_DIR);

		/* Write backup_label */
		join_path_components(backup_label, path, PG_BACKUP_LABEL_FILE);
		fp = fopen(backup_label, "w");
		if (fp == NULL)
			elog(ERROR, "can't open backup label file \"%s\": %s",
				 backup_label, strerror(errno));

		fwrite(PQgetvalue(res, 0, 1), 1, strlen(PQgetvalue(res, 0, 1)), fp);
		fclose(fp);

		file = pgFileNew(backup_label, true);
		calc_file_checksum(file);
		free(file->path);
		file->path = strdup(PG_BACKUP_LABEL_FILE);
		parray_append(backup_files_list, file);

		/* Write tablespace_map */
		if (strlen(PQgetvalue(res, 0, 2)) > 0)
		{
			char		tablespace_map[MAXPGPATH];

			join_path_components(tablespace_map, path, PG_TABLESPACE_MAP_FILE);
			fp = fopen(tablespace_map, "w");
			if (fp == NULL)
				elog(ERROR, "can't open tablespace map file \"%s\": %s",
					 tablespace_map, strerror(errno));

			fwrite(PQgetvalue(res, 0, 2), 1, strlen(PQgetvalue(res, 0, 2)), fp);
			fclose(fp);

			file = pgFileNew(tablespace_map, true);
			calc_file_checksum(file);
			free(file->path);
			file->path = strdup(PG_TABLESPACE_MAP_FILE);
			parray_append(backup_files_list, file);
		}

		if (sscanf(PQgetvalue(res, 0, 3), XID_FMT, &recovery_xid) != 1)
			elog(ERROR,
				 "result of txid_snapshot_xmax() is invalid: %s",
				 PQerrorMessage(backup_conn));
		if (!parse_time(PQgetvalue(res, 0, 4), &recovery_time))
			elog(ERROR,
				 "result of current_timestamp is invalid: %s",
				 PQerrorMessage(backup_conn));
	}
	else
	{
		if (sscanf(PQgetvalue(res, 0, 1), XID_FMT, &recovery_xid) != 1)
			elog(ERROR,
				 "result of txid_snapshot_xmax() is invalid: %s",
				 PQerrorMessage(backup_conn));
		if (!parse_time(PQgetvalue(res, 0, 2), &recovery_time))
			elog(ERROR,
				 "result of current_timestamp is invalid: %s",
				 PQerrorMessage(backup_conn));
	}

	PQclear(res);

	if (!stream_wal)
		wait_archive_lsn(stop_backup_lsn, false);

	/* Fill in fields if that is the correct end of backup. */
	if (backup != NULL)
	{
		char	   *xlog_path,
					stream_xlog_path[MAXPGPATH];

		if (stream_wal)
		{
			join_path_components(stream_xlog_path, pgdata, PG_XLOG_DIR);
			xlog_path = stream_xlog_path;
		}
		else
			xlog_path = arclog_path;

		backup->tli = get_current_timeline(false);
		backup->stop_lsn = stop_backup_lsn;

		if (!read_recovery_info(xlog_path, backup->tli,
								backup->start_lsn, backup->stop_lsn,
								&backup->recovery_time, &backup->recovery_xid))
		{
			backup->recovery_time = recovery_time;
			backup->recovery_xid = recovery_xid;
		}
	}
}

/*
 * Return true if the path is a existing regular file.
 */
bool
fileExists(const char *path)
{
	struct stat buf;

	if (stat(path, &buf) == -1 && errno == ENOENT)
		return false;
	else if (!S_ISREG(buf.st_mode))
		return false;
	else
		return true;
}

/*
 * Notify end of backup to server when "backup_label" is in the root directory
 * of the DB cluster.
 * Also update backup status to ERROR when the backup is not finished.
 */
static void
backup_cleanup(bool fatal, void *userdata)
{
	char path[MAXPGPATH];

	/* If backup_label exists in $PGDATA, notify stop of backup to PostgreSQL */
	join_path_components(path, pgdata, PG_BACKUP_LABEL_FILE);
	if (fileExists(path))
	{
		elog(LOG, "%s exists, stop backup", PG_BACKUP_LABEL_FILE);
		pg_stop_backup(NULL);	/* don't care stop_lsn on error case */
	}

	/*
	 * Update status of backup in BACKUP_CONTROL_FILE to ERROR.
	 * end_time != 0 means backup finished
	 */
	if (current.status == BACKUP_STATUS_RUNNING && current.end_time == 0)
	{
		elog(LOG, "Backup is running, update its status to ERROR");
		current.end_time = time(NULL);
		current.status = BACKUP_STATUS_ERROR;
		pgBackupWriteBackupControlFile(&current);
	}
}

/*
 * Disconnect backup connection during quit pg_probackup.
 */
static void
backup_disconnect(bool fatal, void *userdata)
{
	pgut_disconnect(backup_conn);
}

/* Count bytes in file */
static long
file_size(const char *file_path)
{
	long		r;
	FILE	   *f = fopen(file_path, "r");

	if (!f)
	{
		elog(ERROR, "%s: cannot open file \"%s\" for reading: %s\n",
				PROGRAM_NAME ,file_path, strerror(errno));
		return -1;
	}
	fseek(f, 0, SEEK_END);
	r = ftell(f);
	fclose(f);
	return r;
}

/*
 * Find corresponding file in previous backup.
 * Compare generations and return true if we don't need full copy
 * of the file, but just part of it.
 *
 * skip_size - size of the file in previous backup. We can skip it
 *			   and copy just remaining part of the file.
 */
bool
backup_compressed_file_partially(pgFile *file, void *arg, size_t *skip_size)
{
	bool result = false;
	pgFile *prev_file = NULL;
	size_t current_file_size;
	backup_files_args *arguments = (backup_files_args *) arg;

	if (arguments->prev_backup_filelist)
	{
		pgFile **p = (pgFile **) parray_bsearch(arguments->prev_backup_filelist,
												file, pgFileComparePath);
		if (p)
			prev_file = *p;

		/* If file's gc generation has changed since last backup, just copy it*/
		if (prev_file && prev_file->generation == file->generation)
		{
			current_file_size = file_size(file->path);

			if (prev_file->write_size == BYTES_INVALID)
				return false;

			*skip_size = prev_file->write_size;

			if (current_file_size >= prev_file->write_size)
			{
				elog(LOG, "Backup file %s partially: prev_size %lu, current_size  %lu",
					 file->path, prev_file->write_size, current_file_size);
				result = true;
			}
			else
				elog(ERROR, "Something is wrong with %s. current_file_size %lu, prev %lu",
					file->path, current_file_size, prev_file->write_size);
		}
		else
			elog(LOG, "Copy full %s.", file->path);
	}

	return result;
}

/*
 * Take a backup of the PGDATA at a file level.
 * Copy all directories and files listed in backup_files_list.
 * If the file is 'datafile' (regular relation's main fork), read it page by page,
 * verify checksum and copy.
 * In incremental backup mode, copy only files or datafiles' pages changed after
 * previous backup.
 * TODO review
 */
static void
backup_files(void *arg)
{
	int				i;
	backup_files_args *arguments = (backup_files_args *) arg;
	int n_backup_files_list = parray_num(arguments->backup_files_list);

	/* backup a file or create a directory */
	for (i = 0; i < n_backup_files_list; i++)
	{
		int			ret;
		struct stat	buf;

		pgFile *file = (pgFile *) parray_get(arguments->backup_files_list, i);
		if (__sync_lock_test_and_set(&file->lock, 1) != 0)
			continue;

		/* check for interrupt */
		if (interrupted)
			elog(ERROR, "interrupted during backup");

		if (progress)
			elog(LOG, "Progress: (%d/%d). Process file \"%s\"",
				 i + 1, n_backup_files_list, file->path);

		/* stat file to check its current state */
		ret = stat(file->path, &buf);
		if (ret == -1)
		{
			if (errno == ENOENT)
			{
				/*
				 * If file is not found, this is not en error.
				 * It could have been deleted by concurrent postgres transaction.
				 */
				file->write_size = BYTES_INVALID;
				elog(LOG, "File \"%s\" is not found", file->path);
				continue;
			}
			else
			{
				elog(ERROR,
					"can't stat file to backup \"%s\": %s",
					file->path, strerror(errno));
			}
		}

		/* We have already copied all directories */
		if (S_ISDIR(buf.st_mode))
			continue;

		if (S_ISREG(buf.st_mode))
		{
			/* skip files which have not been modified since last backup */
			/* TODO Implement: compare oldfile and newfile checksum. Now it's just a stub */
			if (arguments->prev_backup_filelist)
			{
				pgFile *prev_file = NULL;
				pgFile **p = (pgFile **) parray_bsearch(arguments->prev_backup_filelist,
														file, pgFileComparePath);
				if (p)
					prev_file = *p;

				if (prev_file && false)
				{
					file->write_size = BYTES_INVALID;
					if (verbose)
						elog(LOG, "File \"%s\" has not changed since previous backup",
							file->path);
					continue;
				}
			}

			/* copy the file into backup */
			if (file->is_datafile)
			{
				if (is_compressed_data_file(file))
				{
					/* TODO review */
					size_t skip_size = 0;
					if (backup_compressed_file_partially(file, arguments, &skip_size))
					{
						/* backup cfs segment partly */
						if (!copy_file_partly(arguments->from_root,
								arguments->to_root,
								file, skip_size))
						{
							/* record as skipped file in file_xxx.txt */
							file->write_size = BYTES_INVALID;
							elog(LOG, "skip");
							continue;
						}
					}
					else if (!copy_file(arguments->from_root,
								arguments->to_root,
								file))
					{
						/* record as skipped file in file_xxx.txt */
						file->write_size = BYTES_INVALID;
						elog(LOG, "skip");
						continue;
					}
				}
				else if (!backup_data_file(arguments->from_root,
									  arguments->to_root, file,
									  arguments->prev_backup_start_lsn))
				{
					file->write_size = BYTES_INVALID;
					elog(LOG, "File \"%s\" was not copied to backup", file->path);
					continue;
				}
			}
			else if (!copy_file(arguments->from_root,
							   arguments->to_root,
							   file))
			{
				file->write_size = BYTES_INVALID;
				elog(LOG, "File \"%s\" was not copied to backup", file->path);
				continue;
			}

			elog(LOG, "File \"%s\". Copied %lu bytes",
				 file->path, (unsigned long) file->write_size);
		}
		else
			elog(LOG, "unexpected file type %d", buf.st_mode);
	}
}

/*
 * Append files to the backup list array.
 * TODO review
 */
static void
add_pgdata_files(parray *files, const char *root)
{
	size_t		i;

	/* list files with the logical path. omit $PGDATA */
	dir_list_file(files, root, true, true, false);

	/* mark files that are possible datafile as 'datafile' */
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);
		char	   *relative;
		char	   *fname;
		size_t		path_len;

		/* data file must be a regular file */
		if (!S_ISREG(file->mode))
			continue;

		/* data files are under "base", "global", or "pg_tblspc" */
		relative = GetRelativePath(file->path, root);
		if (!path_is_prefix_of_path("base", relative) &&
			/*!path_is_prefix_of_path("global", relative) &&*/ //TODO What's wrong with this line?
			!path_is_prefix_of_path(PG_TBLSPC_DIR, relative))
			continue;

		/* Get file name from path */
		fname = last_dir_separator(relative);
		if (fname == NULL)
			fname = relative;
		else
			fname++;

		/* Remove temp tables from the list */
		if (fname[0] == 't' && isdigit(fname[1]))
		{
			pgFileFree(file);
			parray_remove(files, i);
			i--;
			continue;
		}

		path_len = strlen(file->path);
		/* Get link ptrack file to relations files */
		if (path_len > 6 &&
			strncmp(file->path + (path_len - 6), "ptrack", 6) == 0)
		{
			pgFile	   *search_file;
			pgFile	  **pre_search_file;
			int			segno = 0;

			while (true)
			{
				pgFile		tmp_file;

				tmp_file.path = pg_strdup(file->path);

				/* Segno fits into 6 digits since it is not more than 4000 */
				if (segno > 0)
					sprintf(tmp_file.path + path_len - 7, ".%d", segno);
				else
					tmp_file.path[path_len - 7] = '\0';

				pre_search_file = (pgFile **) parray_bsearch(files,
															 &tmp_file,
															 pgFileComparePath);

				if (pre_search_file != NULL)
				{
					search_file = *pre_search_file;
					search_file->ptrack_path = pg_strdup(file->path);
					search_file->segno = segno;
				}
				else
				{
					pg_free(tmp_file.path);
					break;
				}

				pg_free(tmp_file.path);
				segno++;
			}

			/* Remove ptrack file itself from backup list */
			pgFileFree(file);
			parray_remove(files, i);
			i--;
		}
		/* compress map file it is not data file */
		else if (path_len > 4 &&
				 strncmp(file->path + (path_len - 4), ".cfm", 4) == 0)
		{
			pgFile	   **pre_search_file;
			pgFile		tmp_file;

			tmp_file.path = pg_strdup(file->path);
			tmp_file.path[path_len - 4] = '\0';
			pre_search_file = (pgFile **) parray_bsearch(files,
														 &tmp_file,
														 pgFileComparePath);
			if (pre_search_file != NULL)
			{
				FileMap	   *map;
				int			md = open(file->path, O_RDWR|PG_BINARY, 0);

				if (md < 0)
					elog(ERROR, "cannot open cfm file '%s'", file->path);

				map = cfs_mmap(md);
				if (map == MAP_FAILED)
				{
					elog(LOG, "cfs_compression_ration failed to map file %s: %m", file->path);
					close(md);
					break;
				}

				(*pre_search_file)->generation = map->generation;

				if (cfs_munmap(map) < 0)
					elog(LOG, "CFS failed to unmap file %s: %m",
						 file->path);
				if (close(md) < 0)
					elog(LOG, "CFS failed to close file %s: %m",
						 file->path);
			}
			else
				elog(ERROR, "corresponding segment '%s' is not found",
					 tmp_file.path);

			pg_free(tmp_file.path);
		}
		/* name of data file start with digit */
		else if (isdigit(fname[0]))
		{
			int			find_dot;
			int			check_digit;
			char	   *text_segno;

			file->is_datafile = true;

			/*
			 * Find segment number.
			 */

			for (find_dot = (int) path_len - 1;
				 file->path[find_dot] != '.' && find_dot >= 0;
				 find_dot--);
			/* There is not segment number */
			if (find_dot <= 0)
				continue;

			text_segno = file->path + find_dot + 1;
			for (check_digit = 0; text_segno[check_digit] != '\0'; check_digit++)
				if (!isdigit(text_segno[check_digit]))
				{
					check_digit = -1;
					break;
				}

			if (check_digit != -1)
				file->segno = (int) strtol(text_segno, NULL, 10);
		}
	}
}

/*
 * Output the list of files to backup catalog DATABASE_FILE_LIST
 */
static void
write_backup_file_list(parray *files, const char *root)
{
	FILE	   *fp;
	char		path[MAXPGPATH];

	pgBackupGetPath(&current, path, lengthof(path), DATABASE_FILE_LIST);

	fp = fopen(path, "wt");
	if (fp == NULL)
		elog(ERROR, "cannot open file list \"%s\": %s", path,
			strerror(errno));

	print_file_list(fp, files, root);

	fclose(fp);
}

/*
 * A helper function to create the path of a relation file and segment.
 * The returned path is palloc'd
 */
static char *
datasegpath(RelFileNode rnode, ForkNumber forknum, BlockNumber segno)
{
	char	   *path;
	char	   *segpath;

	path = relpathperm(rnode, forknum);
	if (segno > 0)
	{
		segpath = psprintf("%s.%u", path, segno);
		pfree(path);
		return segpath;
	}
	else
		return path;
}

/*
 * Find pgfile by given rnode in the backup_files_list
 * and add given blkno to its pagemap.
 */
void
process_block_change(ForkNumber forknum, RelFileNode rnode, BlockNumber blkno)
{
	char		*path;
	char		*rel_path;
	BlockNumber blkno_inseg;
	int			segno;
	pgFile		*file_item = NULL;
	int			j;

	segno = blkno / RELSEG_SIZE;
	blkno_inseg = blkno % RELSEG_SIZE;

	rel_path = datasegpath(rnode, forknum, segno);
	path = pg_malloc(strlen(rel_path) + strlen(pgdata) + 2);
	sprintf(path, "%s/%s", pgdata, rel_path);

	for (j = 0; j < parray_num(backup_files_list); j++)
	{
		pgFile *p = (pgFile *) parray_get(backup_files_list, j);

		if (strcmp(p->path, path) == 0)
		{
			file_item = p;
			break;
		}
	}

	/*
	 * If we don't have any record of this file in the file map, it means
	 * that it's a relation that did not have much activity since the last
	 * backup. We can safely ignore it. If it is a new relation file, the
	 * backup would simply copy it as-is.
	 */
	if (file_item)
		datapagemap_add(&file_item->pagemap, blkno_inseg);

	pg_free(path);
	pg_free(rel_path);
}

/* TODO review it */
static void
make_pagemap_from_ptrack(parray *files)
{
	int i;

	for (i = 0; i < parray_num(files); i++)
	{
		pgFile *p = (pgFile *) parray_get(files, i);

		if (p->ptrack_path != NULL)
		{
			char 	* tablespace;
			Oid 	db_oid,
					rel_oid,
					tablespace_oid = 0;
			int 	sep_iter,
					sep_count = 0;
			char *ptrack_nonparsed;
			size_t ptrack_nonparsed_size = 0;
			size_t start_addr;

			tablespace = strstr(p->ptrack_path, PG_TBLSPC_DIR);

			if (tablespace)
				sscanf(tablespace+strlen(PG_TBLSPC_DIR)+1, "%i/", &tablespace_oid);

			/*
			 * Path has format:
			 * pg_tblspc/tablespace_oid/tablespace_version_subdir/db_oid/rel_oid
			 * base/db_oid/rel_oid
			 */
			sep_iter = strlen(p->path);
			while (sep_count != 2 && sep_iter >= 0)
				sep_iter--;

			sscanf(p->path + sep_iter + 1, "%u/%u", &db_oid, &rel_oid);
			elog(ERROR, "make_pagemap_from_ptrack. %s, %s, ",
						p->path, p->path + sep_iter + 1);
			
			ptrack_nonparsed = pg_ptrack_get_and_clear(tablespace_oid, db_oid,
												  rel_oid, &ptrack_nonparsed_size);

			/* TODO What is 8? */
			start_addr = (RELSEG_SIZE/8)*p->segno;
			if (start_addr + RELSEG_SIZE/8 > ptrack_nonparsed_size)
				p->pagemap.bitmapsize = ptrack_nonparsed_size - start_addr;
			else
				p->pagemap.bitmapsize =	RELSEG_SIZE/8;

			p->pagemap.bitmap = pg_malloc(p->pagemap.bitmapsize);
			memcpy(p->pagemap.bitmap, ptrack_nonparsed+start_addr, p->pagemap.bitmapsize);
			pg_free(ptrack_nonparsed);
		}
	}
}


/* TODO Add comment */
static bool
stop_streaming(XLogRecPtr xlogpos, uint32 timeline, bool segment_finished)
{
	static uint32 prevtimeline = 0;
	static XLogRecPtr prevpos = InvalidXLogRecPtr;

	/* we assume that we get called once at the end of each segment */
	if (verbose && segment_finished)
		fprintf(stderr, _("%s: finished segment at %X/%X (timeline %u)\n"),
				PROGRAM_NAME, (uint32) (xlogpos >> 32), (uint32) xlogpos,
				timeline);

	/*
	 * Note that we report the previous, not current, position here. After a
	 * timeline switch, xlogpos points to the beginning of the segment because
	 * that's where we always begin streaming. Reporting the end of previous
	 * timeline isn't totally accurate, because the next timeline can begin
	 * slightly before the end of the WAL that we received on the previous
	 * timeline, but it's close enough for reporting purposes.
	 */
	if (prevtimeline != 0 && prevtimeline != timeline)
		fprintf(stderr, _("%s: switched to timeline %u at %X/%X\n"),
				PROGRAM_NAME, timeline,
				(uint32) (prevpos >> 32), (uint32) prevpos);

	if (stop_backup_lsn != InvalidXLogRecPtr && xlogpos > stop_backup_lsn)
		return true;

	prevtimeline = timeline;
	prevpos = xlogpos;

	return false;
}

/*
 * Start the log streaming
 */
static void
StreamLog(void *arg)
{
	XLogRecPtr	startpos;
	TimeLineID	starttli;
	char *basedir = (char *)arg;

	/*
	 * Connect in replication mode to the server
	 */
	if (conn == NULL)
		conn = GetConnection();
	if (!conn)
	{
		pthread_mutex_unlock(&check_stream_mut);
		/* Error message already written in GetConnection() */
		return;
	}

	if (!CheckServerVersionForStreaming(conn))
	{
		/*
		 * Error message already written in CheckServerVersionForStreaming().
		 * There's no hope of recovering from a version mismatch, so don't
		 * retry.
		 */
		disconnect_and_exit(1);
	}

	/*
	 * Identify server, obtaining start LSN position and current timeline ID
	 * at the same time, necessary if not valid data can be found in the
	 * existing output directory.
	 */
	if (!RunIdentifySystem(conn, NULL, &starttli, &startpos, NULL))
		disconnect_and_exit(1);

	/* Ok we have normal stream connect and main process can work again */
	pthread_mutex_unlock(&check_stream_mut);

	/*
	 * We must use startpos as start_lsn from start_backup
	 */
	startpos = current.start_lsn;

	/*
	 * Always start streaming at the beginning of a segment
	 */
	startpos -= startpos % XLOG_SEG_SIZE;

	/*
	 * Start the replication
	 */
	if (verbose)
		fprintf(stderr,
				_("%s: starting log streaming at %X/%X (timeline %u)\n"),
				PROGRAM_NAME, (uint32) (startpos >> 32), (uint32) startpos,
				starttli);

#if PG_VERSION_NUM >= 90600
	{
		StreamCtl ctl;
		ctl.startpos = startpos;
		ctl.timeline = starttli;
		ctl.sysidentifier = NULL;
		ctl.basedir = basedir;
		ctl.stream_stop = stop_streaming;
		ctl.standby_message_timeout = standby_message_timeout;
		ctl.partial_suffix = NULL;
		ctl.synchronous = false;
		ctl.mark_done = false;
		if(ReceiveXlogStream(conn, &ctl) == false)
			elog(ERROR, "Problem in recivexlog");
	}
#else
	if(ReceiveXlogStream(conn, startpos, starttli, NULL, basedir,
					  stop_streaming, standby_message_timeout, NULL,
					  false, false) == false)
		elog(ERROR, "Problem in recivexlog");
#endif

	PQfinish(conn);
	conn = NULL;
}

/*
 * cfs_mmap() and cfs_munmap() function definitions mirror ones
 * from cfs.h, but doesn't use atomic variables, since they are
 * not allowed in frontend code.
 * TODO Is it so?
 * Since we cannot take atomic lock on files compressed by CFS,
 * it should care about not changing files while backup is running.
 */
FileMap* cfs_mmap(int md)
{
	FileMap* map;
#ifdef WIN32
    HANDLE mh = CreateFileMapping(_get_osfhandle(md), NULL, PAGE_READWRITE,
								  0, (DWORD)sizeof(FileMap), NULL);
    if (mh == NULL)
        return (FileMap*)MAP_FAILED;

    map = (FileMap*)MapViewOfFile(mh, FILE_MAP_ALL_ACCESS, 0, 0, 0);
	CloseHandle(mh);
    if (map == NULL)
        return (FileMap*)MAP_FAILED;

#else
	map = (FileMap*)mmap(NULL, sizeof(FileMap),
						 PROT_WRITE | PROT_READ, MAP_SHARED, md, 0);
#endif
	return map;
}

int cfs_munmap(FileMap* map)
{
#ifdef WIN32
	return UnmapViewOfFile(map) ? 0 : -1;
#else
	return munmap(map, sizeof(FileMap));
#endif
}
