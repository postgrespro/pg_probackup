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
#include "pgut/pgut-port.h"
#include "storage/bufpage.h"
#include "datapagemap.h"
#include "streamutil.h"
#include "receivelog.h"

/* wait 10 sec until WAL archive complete */
#define TIMEOUT_ARCHIVE 10

/* Server version */
static int server_version = 0;

static bool	in_backup = false;						/* TODO: more robust logic */
static int	standby_message_timeout = 10 * 1000;	/* 10 sec = default */
static XLogRecPtr stop_backup_lsn = InvalidXLogRecPtr;
const char *progname = "pg_probackup";

/* list of files contained in backup */
static parray *backup_files_list = NULL;
static volatile uint32	total_copy_files_increment;
static uint32 total_files_num;
static pthread_mutex_t check_stream_mut = PTHREAD_MUTEX_INITIALIZER;

/* Backup connection */
static PGconn *backup_conn = NULL;

typedef struct
{
	const char *from_root;
	const char *to_root;
	parray *files;
	parray *prev_files;
	const XLogRecPtr *lsn;
} backup_files_args;

/*
 * Backup routines
 */
static void backup_cleanup(bool fatal, void *userdata);
static void backup_disconnect(bool fatal, void *userdata);

static void backup_files(void *arg);
static parray *do_backup_database(parray *backup_list, bool smooth_checkpoint);

static void pg_start_backup(const char *label, bool smooth, pgBackup *backup);
static void pg_stop_backup(pgBackup *backup);
static void pg_switch_xlog(void);

static bool pg_is_standby(void);
static void add_pgdata_files(parray *files, const char *root);
static void create_file_list(parray *files, const char *root, bool is_append);
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
static void confirm_block_size(const char *name, int blcksz);


#define disconnect_and_exit(code)				\
	{											\
	if (conn != NULL) PQfinish(conn);			\
	exit(code);									\
	}


/*
 * Take a backup of database and return the list of files backed up.
 */
static parray *
do_backup_database(parray *backup_list, bool smooth_checkpoint)
{
	size_t		i;
	parray	   *prev_files = NULL;	/* file list of previous database backup */
	char		database_path[MAXPGPATH];
	char		dst_backup_path[MAXPGPATH];
	char		label[1024];
	XLogRecPtr *lsn = NULL;
	char		prev_file_txt[MAXPGPATH];	/* path of the previous backup
											 * list file */
	pthread_t	backup_threads[num_threads];
	pthread_t	stream_thread;
	backup_files_args *backup_threads_args[num_threads];
	bool		is_ptrack_support;

	/* repack the options */
	pgBackup   *prev_backup = NULL;

	elog(LOG, "database backup start");

	/* Initialize size summary */
	current.data_bytes = 0;

	/*
	 * Obtain current timeline by scanning control file, theh LSN
	 * obtained at output of pg_start_backup or pg_stop_backup does
	 * not contain this information.
	 */
	current.tli = get_current_timeline(false);

	is_ptrack_support = pg_ptrack_support();
	if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK && !is_ptrack_support)
		elog(ERROR, "Current Postgres instance does not support ptrack");

	if(current.backup_mode == BACKUP_MODE_DIFF_PTRACK && !pg_ptrack_enable())
		elog(ERROR, "ptrack is disabled");

	if (is_ptrack_support)
		is_ptrack_support = pg_ptrack_enable();
	/*
	 * In differential backup mode, check if there is an already-validated
	 * full backup on current timeline.
	 */
	if (current.backup_mode == BACKUP_MODE_DIFF_PAGE ||
		current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		prev_backup = catalog_get_last_data_backup(backup_list, current.tli);
		if (prev_backup == NULL)
			elog(ERROR, "Timeline has changed since last full backup. "
						"Create new full backup before an incremental one.");
	}

	/* clear ptrack files for FULL and DIFF backup */
	if (current.backup_mode != BACKUP_MODE_DIFF_PTRACK && is_ptrack_support)
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
		join_path_components(dst_backup_path, database_path, "pg_xlog");
		dir_create_dir(dst_backup_path, DIR_PERMISSION);

		pthread_mutex_lock(&check_stream_mut);
		pthread_create(&stream_thread, NULL, (void *(*)(void *)) StreamLog, dst_backup_path);
		pthread_mutex_lock(&check_stream_mut);
		if (conn == NULL)
			elog(ERROR, "I can't continue work because stream connect has failed.");

		pthread_mutex_unlock(&check_stream_mut);
	}

	if(!from_replica)
	{
		char		label_path[MAXPGPATH];

		/* If backup_label does not exist in $PGDATA, stop taking backup */
		join_path_components(label_path, pgdata, "backup_label");

		/* Leave if no backup file */
		if (!fileExists(label_path))
		{
			elog(LOG, "backup_label does not exist, stopping backup");
			pg_stop_backup(NULL);
			elog(ERROR, "backup_label does not exist in PGDATA.");
		}
	}

	/*
	 * To take differential backup, the file list of the last completed database
	 * backup is needed.
	 */
	if (current.backup_mode == BACKUP_MODE_DIFF_PAGE ||
		current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		Assert(prev_backup);
		pgBackupGetPath(prev_backup, prev_file_txt, lengthof(prev_file_txt),
						DATABASE_FILE_LIST);
		prev_files = dir_read_file_list(pgdata, prev_file_txt);

		/*
		 * Do backup only pages having larger LSN than previous backup.
		 */
		lsn = &prev_backup->start_lsn;
		elog(LOG, "backup only the page that there was of the update from LSN(%X/%08X)",
			 (uint32) (*lsn >> 32), (uint32) *lsn);

		current.parent_backup = prev_backup->start_time;
		pgBackupWriteIni(&current);
	}

	/* initialize backup list */
	backup_files_list = parray_new();

	/* list files with the logical path. omit $PGDATA */
	add_pgdata_files(backup_files_list, pgdata);

	/*
	 * Build page mapping in differential mode. When using this mode, the
	 * list of blocks to be taken is known by scanning the WAL segments
	 * present in archives up to the point where start backup has begun.
	 */
	if (current.backup_mode == BACKUP_MODE_DIFF_PAGE)
	{
		/* Now build the page map */
		parray_qsort(backup_files_list, pgFileComparePathDesc);
		elog(LOG, "extractPageMap");
		elog(LOG, "current_tli:%X", current.tli);
		elog(LOG, "prev_backup->start_lsn: %X/%X",
			 (uint32) (prev_backup->start_lsn >> 32),
			 (uint32) (prev_backup->start_lsn));
		elog(LOG, "current.start_lsn: %X/%X",
			 (uint32) (current.start_lsn >> 32),
			 (uint32) (current.start_lsn));
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
			elog(ERROR, "Wrong ptrack lsn:%lx prev:%lx current:%lx",
				 ptrack_lsn,
				 prev_backup->start_lsn,
				 current.start_lsn);
			elog(ERROR, "Create new full backup before an incremental one.");
		}
		parray_qsort(backup_files_list, pgFileComparePathDesc);
		make_pagemap_from_ptrack(backup_files_list);
	}

	/* sort pathname ascending */
	parray_qsort(backup_files_list, pgFileComparePath);

	/* make dirs before backup */
	for (i = 0; i < parray_num(backup_files_list); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(backup_files_list, i);

		/* if the entry was a directory, create it in the backup */
		if (S_ISDIR(file->mode))
		{
			char		dirpath[MAXPGPATH];
			char	   *dir_name = JoinPathEnd(file->path, pgdata);

			if (verbose)
				elog(LOG, "make directory \"%s\"", dir_name);
			join_path_components(dirpath, database_path, dir_name);
			dir_create_dir(dirpath, DIR_PERMISSION);
		}
		else
		{
			total_files_num++;
		}

		__sync_lock_release(&file->lock);
	}

	if (num_threads < 1)
		num_threads = 1;

	/* sort by size for load balancing */
	parray_qsort(backup_files_list, pgFileCompareSize);

	/* init thread args with own file lists */
	for (i = 0; i < num_threads; i++)
	{
		backup_files_args *arg = pg_malloc(sizeof(backup_files_args));

		arg->from_root = pgdata;
		arg->to_root = database_path;
		arg->files = backup_files_list;
		arg->prev_files = prev_files;
		arg->lsn = lsn;
		backup_threads_args[i] = arg;
	}

	total_copy_files_increment = 0;

	/* Run threads */
	for (i = 0; i < num_threads; i++)
	{
		if (verbose)
			elog(WARNING, "Start thread num:%li", parray_num(backup_threads_args[i]->files));
		pthread_create(&backup_threads[i], NULL, (void *(*)(void *)) backup_files, backup_threads_args[i]);
	}

	/* Wait theads */
	for (i = 0; i < num_threads; i++)
	{
		pthread_join(backup_threads[i], NULL);
		pg_free(backup_threads_args[i]);
	}

	/* clean previous backup file list */
	if (prev_files)
	{
		parray_walk(prev_files, pgFileFree);
		parray_free(prev_files);
	}

	if (progress)
		fprintf(stderr, "\n");

	/* Notify end of backup */
	pg_stop_backup(&current);

	if (stream_wal)
	{
		parray	   *list_file;
		char		pg_xlog_path[MAXPGPATH];

		/* We expect the completion of stream */
		pthread_join(stream_thread, NULL);

		/* Scan backup pg_xlog dir */
		list_file = parray_new();
		join_path_components(pg_xlog_path, database_path, "pg_xlog");
		dir_list_file(list_file, pg_xlog_path, false, true, false);

		/* Remove file path root prefix and calc meta */
		for (i = 0; i < parray_num(list_file); i++)
		{
			pgFile	   *file = (pgFile *) parray_get(list_file, i);

			calc_file(file);
			if (strstr(file->path, database_path) == file->path)
			{
				char	   *ptr = file->path;
				file->path = pstrdup(JoinPathEnd(ptr, database_path));
				free(ptr);
			}
		}
		parray_concat(backup_files_list, list_file);
		parray_free(list_file);
	}

	/* Create file list */
	create_file_list(backup_files_list, pgdata, false);

	/* Print summary of size of backup mode files */
	for (i = 0; i < parray_num(backup_files_list); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(backup_files_list, i);

		if (!S_ISREG(file->mode))
			continue;
		/*
		 * Count only the amount of data. For a full backup, the total
		 * amount of data written counts while for an differential
		 * backup only the data read counts.
		 */
		if (current.backup_mode == BACKUP_MODE_DIFF_PAGE ||
			current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
			current.data_bytes += file->write_size;
		else if (current.backup_mode == BACKUP_MODE_FULL)
			current.data_bytes += file->size;
	}

	elog(LOG, "database backup completed(Backup: " INT64_FORMAT ")",
		 current.data_bytes);
	elog(LOG, "========================================");

	return backup_files_list;
}


int
do_backup(bool smooth_checkpoint)
{
	parray	   *backup_list;
	parray	   *files_database;

	/* PGDATA and BACKUP_MODE are always required */
	if (pgdata == NULL)
		elog(ERROR, "Required parameter not specified: PGDATA "
						 "(-D, --pgdata)");

	/* A backup mode is needed */
	if (current.backup_mode == BACKUP_MODE_INVALID)
		elog(ERROR, "Required parameter not specified: BACKUP_MODE "
						 "(-b, --backup-mode)");

	/* Create connection for PostgreSQL */
	backup_conn = pgut_connect(pgut_dbname);
	pgut_atexit_push(backup_disconnect, NULL);

	/* Confirm data block size and xlog block size are compatible */
	check_server_version();

	/* setup cleanup callback function */
	in_backup = true;

	/* Block backup operations on a standby */
	from_replica = pg_is_in_recovery();
	if (pg_is_standby() && !from_replica)
		elog(ERROR, "backup is not allowed for standby");

	/* show configuration actually used */
	elog(LOG, "========================================");
	elog(LOG, "backup start");
	elog(LOG, "----------------------------------------");
	if (verbose)
		pgBackupWriteConfigSection(stderr, &current);
	elog(LOG, "----------------------------------------");

	/* get exclusive lock of backup catalog */
	catalog_lock(true);

	/* initialize backup result */
	current.status = BACKUP_STATUS_RUNNING;
	current.tli = 0;		/* get from result of pg_start_backup() */
	current.start_lsn = 0;
	current.stop_lsn = 0;
	current.start_time = time(NULL);
	current.end_time = (time_t) 0;
	current.data_bytes = BYTES_INVALID;
	current.block_size = BLCKSZ;
	current.wal_block_size = XLOG_BLCKSZ;
	current.recovery_xid = 0;
	current.recovery_time = (time_t) 0;
	current.checksum_version = get_data_checksum_version(true);
	current.stream = stream_wal;

	/* create backup directory and backup.ini */
	if (!check)
	{
		if (pgBackupCreateDir(&current))
			elog(ERROR, "cannot create backup directory");
		pgBackupWriteIni(&current);
	}
	elog(LOG, "backup destination is initialized");

	/* get list of backups already taken */
	backup_list = catalog_get_backup_list(0);
	if (!backup_list)
		elog(ERROR, "cannot process any more");

	/* set the error processing function for the backup process */
	pgut_atexit_push(backup_cleanup, NULL);

	/* backup data */
	files_database = do_backup_database(backup_list, smooth_checkpoint);
	pgut_atexit_pop(backup_cleanup, NULL);

	/* update backup status to DONE */
	current.end_time = time(NULL);
	current.status = BACKUP_STATUS_DONE;
	if (!check)
		pgBackupWriteIni(&current);

	/* Calculate the total data read */
	if (verbose)
	{
		int64		total_read = 0;

		/* Database data */
		if (current.backup_mode == BACKUP_MODE_FULL ||
			current.backup_mode == BACKUP_MODE_DIFF_PAGE ||
			current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
			total_read += current.data_bytes;

		if (total_read == 0)
			elog(LOG, "nothing to backup");
		else
			elog(LOG, "all backup completed(read: " INT64_FORMAT " write: "
						INT64_FORMAT ")",
				 total_read, current.data_bytes);
		elog(LOG, "========================================");
	}

	/* Cleanup backup mode file list */
	if (files_database)
		parray_walk(files_database, pgFileFree);
	parray_free(files_database);

	pgBackupValidate(&current, false, false);

	return 0;
}

/*
 * Get server version and confirm block sizes.
 */
static void
check_server_version(void)
{
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

	/* confirm block_size (BLCKSZ) and wal_block_size (XLOG_BLCKSZ) */
	confirm_block_size("block_size", BLCKSZ);
	confirm_block_size("wal_block_size", XLOG_BLCKSZ);
}

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

	/*
	 * Extract timeline and LSN from results of pg_start_backup()
	 */
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

static void
pg_ptrack_clear(void)
{
	PGresult   *res_db,
			   *res;
	const char *dbname;
	int			i;

	res_db = pgut_execute(backup_conn, "SELECT datname FROM pg_database",
						  0, NULL);

	for(i=0; i < PQntuples(res_db); i++)
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
	sprintf(params[1], "%i", rel_oid);

	res_db = pgut_execute(backup_conn,
						  "SELECT datname FROM pg_database WHERE oid=$1",
						  1, (const char **) params);

	dbname = pstrdup(PQgetvalue(res_db, 0, 0));
	PQclear(res_db);

	tmp_conn = pgut_connect(dbname);
	sprintf(params[0], "%i", tablespace_oid);

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

static void
wait_archive_lsn(XLogRecPtr lsn, bool prev_segno)
{
	TimeLineID	tli;
	XLogSegNo	targetSegNo;
	char		wal_path[MAXPGPATH];
	char		wal_file[MAXFNAMELEN];
	int			try_count = 0;

	tli = get_current_timeline(false);

	/* As well as WAL file name */
	XLByteToSeg(lsn, targetSegNo);
	if (prev_segno)
		targetSegNo--;
	XLogFileName(wal_file, tli, targetSegNo);

	join_path_components(wal_path, arclog_path, wal_file);
	elog(LOG, "wait for lsn %li in archived WAL segment %s", lsn, wal_path);

	/* Wait until switched WAL is archived */
	while (!fileExists(wal_path))
	{
		sleep(1);
		if (interrupted)
			elog(ERROR, "interrupted during waiting for WAL archiving");
		try_count++;
		if (try_count > TIMEOUT_ARCHIVE)
			elog(ERROR,
				 "switched WAL could not be archived in %d seconds",
				 TIMEOUT_ARCHIVE);
	}
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

	/* Remove annoying NOTICE messages generated by backend */
	res = pgut_execute(backup_conn, "SET client_min_messages = warning;",
					   0, NULL);
	PQclear(res);

	if (from_replica)
		res = pgut_execute(backup_conn,
						   "SELECT * FROM pg_stop_backup(false)", 0, NULL);
	else
		res = pgut_execute(backup_conn,
						   "SELECT * FROM pg_stop_backup()", 0, NULL);

	/*
	 * Extract timeline and LSN from results of pg_stop_backup()
	 */
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

		Assert(PQnfields(res) >= 3);

		pgBackupGetPath(&current, path, lengthof(path), DATABASE_DIR);
		join_path_components(backup_label, path, "backup_label");

		/* Write backup_label */
		fp = fopen(backup_label, "w");
		if (fp == NULL)
			elog(ERROR, "can't open backup label file \"%s\": %s",
				 backup_label, strerror(errno));

		fwrite(PQgetvalue(res, 0, 1), 1, strlen(PQgetvalue(res, 0, 1)), fp);
		fclose(fp);

		file = pgFileNew(backup_label, true);
		calc_file(file);
		free(file->path);
		file->path = strdup("backup_label");
		parray_append(backup_files_list, file);

		/* Write tablespace_map */
		if (strlen(PQgetvalue(res, 0, 2)) > 0)
		{
			char		tablespace_map[MAXPGPATH];

			join_path_components(tablespace_map, path, "tablespace_map");

			fp = fopen(tablespace_map, "w");
			if (fp == NULL)
				elog(ERROR, "can't open tablespace map file \"%s\": %s",
					 tablespace_map, strerror(errno));

			fwrite(PQgetvalue(res, 0, 2), 1, strlen(PQgetvalue(res, 0, 2)), fp);
			fclose(fp);

			file = pgFileNew(tablespace_map, true);
			calc_file(file);
			free(file->path);
			file->path = strdup("tablespace_map");
			parray_append(backup_files_list, file);
		}
	}

	PQclear(res);

	if (!stream_wal)
		wait_archive_lsn(stop_backup_lsn, false);

	/* Fill in fields if backup exists */
	if (backup != NULL)
	{
		backup->tli = get_current_timeline(false);
		backup->stop_lsn = stop_backup_lsn;

		if (from_replica)
			res = pgut_execute(backup_conn, TXID_CURRENT_IF_SQL, 0, NULL);
		else
			res = pgut_execute(backup_conn, TXID_CURRENT_SQL, 0, NULL);

		if (sscanf(PQgetvalue(res, 0, 0), XID_FMT, &backup->recovery_xid) != 1)
			elog(ERROR,
				 "result of txid_current() is invalid: %s",
				 PQerrorMessage(backup_conn));
		backup->recovery_time = time(NULL);

		elog(LOG, "finish backup: tli=%X lsn=%X/%08X xid=%s",
			 backup->tli,
			 (uint32) (backup->stop_lsn >> 32), (uint32) backup->stop_lsn,
			 PQgetvalue(res, 0, 0));

		PQclear(res);
	}
}

/*
 * Switch to a new WAL segment for master.
 */
static void
pg_switch_xlog(void)
{
	PGresult   *res;
	XLogRecPtr	lsn;
	uint32		xlogid;
	uint32		xrecoff;

	/* Remove annoying NOTICE messages generated by backend */
	res = pgut_execute(backup_conn, "SET client_min_messages = warning;", 0,
					   NULL);
	PQclear(res);

	res = pgut_execute(backup_conn, "SELECT * FROM pg_switch_xlog()",
					   0, NULL);

	/*
	 * Extract timeline and LSN from results of pg_stop_backup()
	 * and friends.
	 */
	XLogDataFromLSN(PQgetvalue(res, 0, 0), &xlogid, &xrecoff);
	/* Calculate LSN */
	lsn = (XLogRecPtr) ((uint64) xlogid << 32) | xrecoff;

	PQclear(res);

	/* Wait for returned lsn - 1 in archive folder */
	wait_archive_lsn(lsn, false);
}


/*
 * Check if node is a standby by looking at the presence of
 * recovery.conf.
 */
static bool
pg_is_standby(void)
{
	char	path[MAXPGPATH];
	snprintf(path, lengthof(path), "%s/recovery.conf", pgdata);
	make_native_path(path);
	return fileExists(path);
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

	if (!in_backup)
		return;

	/* If backup_label exist in $PGDATA, notify stop of backup to PostgreSQL */
	snprintf(path, lengthof(path), "%s/backup_label", pgdata);
	make_native_path(path);
	if (fileExists(path))
	{
		elog(LOG, "backup_label exists, stop backup");
		pg_stop_backup(NULL);	/* don't care stop_lsn on error case */
	}

	/*
	 * Update status of backup.ini to ERROR.
	 * end_time != 0 means backup finished
	 */
	if (current.status == BACKUP_STATUS_RUNNING && current.end_time == 0)
	{
		elog(LOG, "backup is running, update its status to ERROR");
		current.end_time = time(NULL);
		current.status = BACKUP_STATUS_ERROR;
		pgBackupWriteIni(&current);
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
file_size(const char *file)
{
	long		r;
	FILE	   *f = fopen(file, "r");

	if (!f)
	{
		elog(ERROR, "pg_probackup: could not open file \"%s\" for reading: %s\n",
				file, strerror(errno));
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

	if (arguments->prev_files)
	{
		pgFile **p = (pgFile **) parray_bsearch(arguments->prev_files,
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
 * Take differential backup at page level.
 */
static void
backup_files(void *arg)
{
	int				i;
	struct timeval	tv;

	backup_files_args *arguments = (backup_files_args *) arg;

	gettimeofday(&tv, NULL);

	/* backup a file or create a directory */
	for (i = 0; i < parray_num(arguments->files); i++)
	{
		int			ret;
		struct stat	buf;

		pgFile *file = (pgFile *) parray_get(arguments->files, i);
		if (__sync_lock_test_and_set(&file->lock, 1) != 0)
			continue;

		/* If current time is rewinded, abort this backup. */
		if (tv.tv_sec < file->mtime)
			elog(ERROR,
				 "current time may be rewound. Please retry with full backup mode.");

		/* check for interrupt */
		if (interrupted)
			elog(ERROR, "interrupted during backup");

		/* print progress in verbose mode */
		if (verbose)
			elog(LOG, "(%d/%lu) %s", i + 1, (unsigned long) parray_num(arguments->files),
				file->path + strlen(arguments->from_root) + 1);

		/* stat file to get file type, size and modify timestamp */
		ret = stat(file->path, &buf);
		if (ret == -1)
		{
			if (errno == ENOENT)
			{
				/* record as skipped file in file_xxx.txt */
				file->write_size = BYTES_INVALID;
				elog(LOG, "skip");
				continue;
			}
			else
			{
				elog(ERROR,
					"can't stat backup mode. \"%s\": %s",
					file->path, strerror(errno));
			}
		}

		/* skip dir because make before */
		if (S_ISDIR(buf.st_mode))
		{
			continue;
		}
		else if (S_ISREG(buf.st_mode))
		{
			/* skip files which have not been modified since last backup */
			if (arguments->prev_files)
			{
				pgFile *prev_file = NULL;
				pgFile **p = (pgFile **) parray_bsearch(arguments->prev_files, file, pgFileComparePath);
				if (p)
					prev_file = *p;

				if (prev_file && prev_file->mtime == file->mtime)
				{
					/* record as skipped file in file_xxx.txt */
					file->write_size = BYTES_INVALID;
					elog(LOG, "skip");
					continue;
				}
			}

			/*
			 * We will wait until the next second of mtime so that backup
			 * file should contain all modifications at the clock of mtime.
			 * timer resolution of ext3 file system is one second.
			 */

			if (tv.tv_sec == file->mtime)
			{
				/* update time and recheck */
				gettimeofday(&tv, NULL);
				while (tv.tv_sec <= file->mtime)
				{
					usleep(1000000 - tv.tv_usec);
					gettimeofday(&tv, NULL);
				}
			}

			/* copy the file into backup */
			if (file->is_datafile)
			{
				if (!backup_data_file(arguments->from_root,
									  arguments->to_root, file,
									  arguments->lsn))
				{
					/* record as skipped file in file_xxx.txt */
					file->write_size = BYTES_INVALID;
					elog(LOG, "skip");
					continue;
				}
			}
			else if (is_compressed_data_file(file))
			{
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
			else if (!copy_file(arguments->from_root,
							   arguments->to_root,
							   file))
			{
				/* record as skipped file in file_xxx.txt */
				file->write_size = BYTES_INVALID;
				elog(LOG, "skip");
				continue;
			}

			elog(LOG, "copied %lu", (unsigned long) file->write_size);
		}
		else
			elog(LOG, "unexpected file type %d", buf.st_mode);
		if (progress)
			fprintf(stderr, "\rProgress %i/%u", total_copy_files_increment, total_files_num-1);
		 __sync_fetch_and_add(&total_copy_files_increment, 1);
	}
}


/*
 * Append files to the backup list array.
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
		relative = file->path + strlen(root) + 1;
		if (!path_is_prefix_of_path("base", relative) &&
			/*!path_is_prefix_of_path("global", relative) &&*/ //TODO What's wrong with this line?
			!path_is_prefix_of_path("pg_tblspc", relative))
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
				(*pre_search_file)->is_datafile = false;

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
 * Output the list of files to backup catalog
 */
static void
create_file_list(parray *files, const char *root, bool is_append)
{
	FILE	   *fp;
	char		path[MAXPGPATH];

	if (!check)
	{
		/* output path is '$BACKUP_PATH/file_database.txt' */
		pgBackupGetPath(&current, path, lengthof(path), DATABASE_FILE_LIST);

		fp = fopen(path, is_append ? "at" : "wt");
		if (fp == NULL)
			elog(ERROR, "cannot open file list \"%s\": %s", path,
				strerror(errno));
		print_file_list(fp, files, root);
		fclose(fp);
	}
}

/*
 * A helper function to create the path of a relation file and segment.
 *
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
 * This routine gets called while reading WAL segments from the WAL archive,
 * for every block that have changed in the target system. It makes note of
 * all the changed blocks in the pagemap of the file and adds them in the
 * things to track for the backup.
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

static void
make_pagemap_from_ptrack(parray *files)
{
	int i;
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile *p = (pgFile *) parray_get(files, i);
		if (p->ptrack_path != NULL)
		{
			char *flat_memory;
			char *tmp_path = p->ptrack_path;
			char *tablespace;
			size_t path_length = strlen(p->ptrack_path);
			size_t flat_size = 0;
			size_t start_addr;
			Oid db_oid, rel_oid, tablespace_oid = 0;
			int sep_iter, sep_count = 0;

			/* Find target path*/
			for(sep_iter = (int)path_length; sep_iter >= 0; sep_iter--)
			{
				if (IS_DIR_SEP(tmp_path[sep_iter]))
				{
					sep_count++;
				}
				if (sep_count == 2)
				{
					tmp_path += sep_iter + 1;
					break;
				}
			}
			/* For unix only now */
			sscanf(tmp_path, "%u/%u_ptrack", &db_oid, &rel_oid);
			tablespace = strstr(p->ptrack_path, "pg_tblspc");
			if (tablespace != NULL)
				sscanf(tablespace+10, "%i/", &tablespace_oid);

			flat_memory = pg_ptrack_get_and_clear(tablespace_oid,
												  db_oid,
												  rel_oid,
												  &flat_size);

			start_addr = (RELSEG_SIZE/8)*p->segno;
			p->pagemap.bitmapsize = start_addr+RELSEG_SIZE/8 > flat_size ? flat_size - start_addr : RELSEG_SIZE/8;
			p->pagemap.bitmap = pg_malloc(p->pagemap.bitmapsize);
			memcpy(p->pagemap.bitmap, flat_memory+start_addr, p->pagemap.bitmapsize);
			pg_free(flat_memory);
		}
	}
}


static bool
stop_streaming(XLogRecPtr xlogpos, uint32 timeline, bool segment_finished)
{
	static uint32 prevtimeline = 0;
	static XLogRecPtr prevpos = InvalidXLogRecPtr;

	/* we assume that we get called once at the end of each segment */
	if (verbose && segment_finished)
		fprintf(stderr, _("%s: finished segment at %X/%X (timeline %u)\n"),
				progname, (uint32) (xlogpos >> 32), (uint32) xlogpos,
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
				progname, timeline,
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
				progname, (uint32) (startpos >> 32), (uint32) startpos,
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
