/*-------------------------------------------------------------------------
 *
 * backup.c: backup DB cluster, archived WAL
 *
 * Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_arman.h"

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
const char *progname = "pg_arman";

/* list of files contained in backup */
parray			*backup_files_list;

typedef struct
{
	const char *from_root;
	const char *to_root;
	parray *files;
	parray *prev_files;
	const XLogRecPtr *lsn;
	unsigned int start_file_idx;
	unsigned int end_file_idx;
} backup_files_args;

/*
 * Backup routines
 */
static void backup_cleanup(bool fatal, void *userdata);
static void backup_files(void *arg);
static parray *do_backup_database(parray *backup_list, pgBackupOption bkupopt);
static void confirm_block_size(const char *name, int blcksz);
static void pg_start_backup(const char *label, bool smooth, pgBackup *backup);
static void pg_stop_backup(pgBackup *backup);
static bool pg_is_standby(void);
static void get_lsn(PGresult *res, XLogRecPtr *lsn);
static void get_xid(PGresult *res, uint32 *xid);
static void pg_ptrack_clear(void);
static char *pg_ptrack_get_and_clear(Oid tablespace_oid,
									 Oid db_oid,
									 Oid rel_oid,
									 size_t *result_size);
static void add_files(parray *files, const char *root, bool add_root, bool is_pgdata);
static void create_file_list(parray *files,
							 const char *root,
							 const char *subdir,
							 const char *prefix,
							 bool is_append);
static void wait_for_archive(pgBackup *backup, const char *sql);
static void make_pagemap_from_ptrack(parray *files);
static void StreamLog(void *arg);


#define disconnect_and_exit(code)				\
	{											\
	if (conn != NULL) PQfinish(conn);			\
	exit(code);									\
	}


/*
 * Take a backup of database and return the list of files backed up.
 */
static parray *
do_backup_database(parray *backup_list, pgBackupOption bkupopt)
{
	int			i;
	parray	   *prev_files = NULL;	/* file list of previous database backup */
	FILE	   *fp;
	char		path[MAXPGPATH];
	char		dst_backup_path[MAXPGPATH];
	char		label[1024];
	XLogRecPtr *lsn = NULL;
	char		prev_file_txt[MAXPGPATH];	/* path of the previous backup
											 * list file */
	bool		has_backup_label  = true;	/* flag if backup_label is there */
	pthread_t	backup_threads[num_threads];
	pthread_t	stream_thread;
	backup_files_args *backup_threads_args[num_threads];

	/* repack the options */
	bool	smooth_checkpoint = bkupopt.smooth_checkpoint;
	pgBackup   *prev_backup = NULL;

	/* Block backup operations on a standby */
	if (pg_is_standby())
		elog(ERROR, "Backup cannot run on a standby.");

	elog(LOG, "database backup start");

	/* Initialize size summary */
	current.data_bytes = 0;

	/* do some checks on the node */
	sanityChecks();

	/*
	 * Obtain current timeline by scanning control file, theh LSN
	 * obtained at output of pg_start_backup or pg_stop_backup does
	 * not contain this information.
	 */
	current.tli = get_current_timeline(false);

	/*
	 * In differential backup mode, check if there is an already-validated
	 * full backup on current timeline.
	 */
	if (current.backup_mode == BACKUP_MODE_DIFF_PAGE ||
		current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		pgBackup   *prev_backup;

		prev_backup = catalog_get_last_data_backup(backup_list, current.tli);
		if (prev_backup == NULL)
			elog(ERROR, "Valid full backup not found for "
					"differential backup. Either create a full backup "
					"or validate existing one.");
	}

	/* clear ptrack files for FULL and DIFF backup */
	if (current.backup_mode != BACKUP_MODE_DIFF_PTRACK)
		pg_ptrack_clear();

	/* start stream replication */
	if (stream_wal)
	{
		pgBackupGetPath(&current, path, lengthof(path), DATABASE_DIR);
		join_path_components(dst_backup_path, path, "pg_xlog");
		dir_create_dir(dst_backup_path, DIR_PERMISSION);
		pthread_create(&stream_thread, NULL, (void *(*)(void *)) StreamLog, dst_backup_path);
	}

	/* notify start of backup to PostgreSQL server */
	time2iso(label, lengthof(label), current.start_time);
	strncat(label, " with pg_arman", lengthof(label));
	pg_start_backup(label, smooth_checkpoint, &current);

	/* If backup_label does not exist in $PGDATA, stop taking backup */
	snprintf(path, lengthof(path), "%s/backup_label", pgdata);
	make_native_path(path);
	if (!fileExists(path))
		has_backup_label = false;

	/* Leave if no backup file */
	if (!has_backup_label)
	{
		elog(LOG, "backup_label does not exist, stopping backup");
		pg_stop_backup(NULL);
		elog(ERROR, "backup_label does not exist in PGDATA.");
	}

	/*
	 * List directories and symbolic links with the physical path to make
	 * mkdirs.sh, then sort them in order of path. Omit $PGDATA.
	 */
	backup_files_list = parray_new();
	dir_list_file(backup_files_list, pgdata, NULL, false, false);

	if (!check)
	{
		pgBackupGetPath(&current, path, lengthof(path), MKDIRS_SH_FILE);
		fp = fopen(path, "wt");
		if (fp == NULL)
			elog(ERROR, "can't open make directory script \"%s\": %s",
				path, strerror(errno));
		dir_print_mkdirs_sh(fp, backup_files_list, pgdata);
		fclose(fp);
		if (chmod(path, DIR_PERMISSION) == -1)
			elog(ERROR, "can't change mode of \"%s\": %s", path,
				strerror(errno));
	}

	/* clear directory list */
	parray_walk(backup_files_list, pgFileFree);
	parray_free(backup_files_list);
	backup_files_list = NULL;

	/*
	 * To take differential backup, the file list of the last completed database
	 * backup is needed.
	 */
	if (current.backup_mode == BACKUP_MODE_DIFF_PAGE ||
		current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		/* find last completed database backup */
		prev_backup = catalog_get_last_data_backup(backup_list, current.tli);
		pgBackupGetPath(prev_backup, prev_file_txt, lengthof(prev_file_txt),
			DATABASE_FILE_LIST);
		prev_files = dir_read_file_list(pgdata, prev_file_txt);

		/*
		 * Do backup only pages having larger LSN than previous backup.
		 */
		lsn = &prev_backup->start_lsn;
		elog(LOG, "backup only the page that there was of the update from LSN(%X/%08X)",
			 (uint32) (*lsn >> 32), (uint32) *lsn);
	}

	/* initialize backup list */
	backup_files_list = parray_new();

	/* list files with the logical path. omit $PGDATA */
	add_files(backup_files_list, pgdata, false, true);

	/* backup files */
	pgBackupGetPath(&current, path, lengthof(path), DATABASE_DIR);

	/*
	 * Build page mapping in differential mode. When using this mode, the
	 * list of blocks to be taken is known by scanning the WAL segments
	 * present in archives up to the point where start backup has begun.
	 * However, normally this segment is not yet available in the archives,
	 * leading to failures when building the page map. Hence before doing
	 * anything and in order to ensure that all the segments needed for the
	 * scan are here, for a switch of the last segment with pg_switch_xlog.
	 */
	if (current.backup_mode == BACKUP_MODE_DIFF_PAGE)
	{
		/* Enforce archiving of last segment and wait for it to be here */
		wait_for_archive(&current, "SELECT * FROM pg_switch_xlog()");

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
					   current.start_lsn);
	}

	if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		XLogRecPtr ptrack_lsn = get_last_ptrack_lsn();
		if (ptrack_lsn > prev_backup->stop_lsn)
			elog(ERROR, "Wrong ptrack lsn:%lx prev:%lx current:%lx",
				ptrack_lsn,
				prev_backup->start_lsn,
				current.start_lsn);
		parray_qsort(backup_files_list, pgFileComparePathDesc);
		make_pagemap_from_ptrack(backup_files_list);
	}

	/* sort pathname ascending */
	parray_qsort(backup_files_list, pgFileComparePath);

	/* make dirs before backup */
	for (i = 0; i < parray_num(backup_files_list); i++)
	{
		int ret;
		struct stat buf;
		pgFile *file = (pgFile *) parray_get(backup_files_list, i);

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
		/* if the entry was a directory, create it in the backup */
		if (S_ISDIR(buf.st_mode))
		{
			char dirpath[MAXPGPATH];
			if (verbose)
				elog(LOG, "Make dir %s",  file->path + strlen(pgdata) + 1);
			join_path_components(dirpath, path, JoinPathEnd(file->path, pgdata));
			if (!check)
				dir_create_dir(dirpath, DIR_PERMISSION);
		}
	}

	if (num_threads < 1)
		num_threads = 1;

	for (i = 0; i < num_threads; i++)
	{
		backup_files_args *arg = pg_malloc(sizeof(backup_files_args));
		arg->from_root = pgdata;
		arg->to_root = path;
		arg->files = backup_files_list;
		arg->prev_files = prev_files;
		arg->lsn = lsn;
		arg->start_file_idx = i * (parray_num(backup_files_list)/num_threads);
		if (i == num_threads - 1)
			arg->end_file_idx = parray_num(backup_files_list);
		else
			arg->end_file_idx =  (i + 1) * (parray_num(backup_files_list)/num_threads);

		if (verbose)
			elog(WARNING, "Start thread for start_file_idx:%i end_file_idx:%i num:%li",
				arg->start_file_idx,
				arg->end_file_idx,
				parray_num(backup_files_list));
		backup_threads_args[i] = arg;
		pthread_create(&backup_threads[i], NULL, (void *(*)(void *)) backup_files, arg);
	}

	/* Wait theads */
	for (i = 0; i < num_threads; i++)
	{
		pthread_join(backup_threads[i], NULL);
		pg_free(backup_threads_args[i]);
	}

	/* Notify end of backup */
	pg_stop_backup(&current);

	if (stream_wal)
	{
		parray	*list_file;
		char pg_xlog_path[MAXPGPATH];

		/* We expect the completion of stream */
		pthread_join(stream_thread, NULL);

		/* Scan backup pg_xlog dir */
		list_file = parray_new();
		join_path_components(pg_xlog_path, path, "pg_xlog");
		dir_list_file(list_file, pg_xlog_path, NULL, true, false);

		/* Remove file path root prefix and calc meta */
		for (i = 0; i < parray_num(list_file); i++)
		{
			pgFile *file = (pgFile *)parray_get(list_file, i);

			calc_file(file);
			if (strstr(file->path, path) == file->path)
			{
				char *ptr = file->path;
				file->path = pstrdup(JoinPathEnd(ptr, path));
				free(ptr);
			}
		}
		parray_concat(backup_files_list, list_file);
	}

	/* Create file list */
	create_file_list(backup_files_list, pgdata, DATABASE_FILE_LIST, NULL, false);

	/* Print summary of size of backup mode files */
	for (i = 0; i < parray_num(backup_files_list); i++)
	{
		pgFile *file = (pgFile *) parray_get(backup_files_list, i);
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
do_backup(pgBackupOption bkupopt)
{
	parray *backup_list;
	parray *files_database;
	int		ret;

	/* repack the necessary options */
	int keep_data_generations = bkupopt.keep_data_generations;
	int keep_data_days        = bkupopt.keep_data_days;

	/* PGDATA and BACKUP_MODE are always required */
	if (pgdata == NULL)
		elog(ERROR, "Required parameter not specified: PGDATA "
						 "(-D, --pgdata)");

	/* A backup mode is needed */
	if (current.backup_mode == BACKUP_MODE_INVALID)
		elog(ERROR, "Required parameter not specified: BACKUP_MODE "
						 "(-b, --backup-mode)");

	/* Confirm data block size and xlog block size are compatible */
	check_server_version();

	/* setup cleanup callback function */
	in_backup = true;

	/* show configuration actually used */
	elog(LOG, "========================================");
	elog(LOG, "backup start");
	elog(LOG, "----------------------------------------");
	if (verbose)
		pgBackupWriteConfigSection(stderr, &current);
	elog(LOG, "----------------------------------------");

	/* get exclusive lock of backup catalog */
	ret = catalog_lock();
	if (ret == -1)
		elog(ERROR, "cannot lock backup catalog");
	else if (ret == 1)
		elog(ERROR,
			"another pg_arman is running, skipping this backup");

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

	/* create backup directory and backup.ini */
	if (!check)
	{
		if (pgBackupCreateDir(&current))
			elog(ERROR, "cannot create backup directory");
		pgBackupWriteIni(&current);
	}
	elog(LOG, "backup destination is initialized");

	/* get list of backups already taken */
	backup_list = catalog_get_backup_list(NULL);
	if (!backup_list)
		elog(ERROR, "cannot process any more");

	/* set the error processing function for the backup process */
	pgut_atexit_push(backup_cleanup, NULL);

	/* backup data */
	files_database = do_backup_database(backup_list, bkupopt);
	pgut_atexit_pop(backup_cleanup, NULL);

	/* update backup status to DONE */
	current.end_time = time(NULL);
	current.status = BACKUP_STATUS_DONE;
	if (!check)
		pgBackupWriteIni(&current);

	/* Calculate the total data read */
	if (verbose)
	{
		int64 total_read = 0;

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


	/* Delete old backup files after all backup operation. */
	pgBackupDelete(keep_data_generations, keep_data_days);

	/* Cleanup backup mode file list */
	if (files_database)
		parray_walk(files_database, pgFileFree);
	parray_free(files_database);

	/* release catalog lock */
	catalog_unlock();

	return 0;
}

/*
 * get server version and confirm block sizes.
 */
void
check_server_version(void)
{
	bool		my_conn;

	/* Leave if server has already been checked */
	if (server_version > 0)
		return;

	my_conn = (connection == NULL);

	if (my_conn)
		reconnect();

	/* confirm server version */
	server_version = PQserverVersion(connection);
	if (server_version < 90500)
		elog(ERROR,
			"server version is %d.%d.%d, must be %s or higher.",
			 server_version / 10000,
			 (server_version / 100) % 100,
			 server_version % 100, "9.5");

	/* confirm block_size (BLCKSZ) and wal_block_size (XLOG_BLCKSZ) */
	confirm_block_size("block_size", BLCKSZ);
	confirm_block_size("wal_block_size", XLOG_BLCKSZ);

	if (my_conn)
		disconnect();
}

static void
confirm_block_size(const char *name, int blcksz)
{
	PGresult   *res;
	char	   *endp;
	int			block_size;

	res = execute("SELECT current_setting($1)", 1, &name);
	if (PQntuples(res) != 1 || PQnfields(res) != 1)
		elog(ERROR, "cannot get %s: %s",
			name, PQerrorMessage(connection));
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
	PGresult	   *res;
	const char	   *params[2];

	params[0] = label;

	reconnect();

	/* 2nd argument is 'fast'*/
	params[1] = smooth ? "false" : "true";
	res = execute("SELECT pg_start_backup($1, $2)", 2, params);

	if (backup != NULL)
		get_lsn(res, &backup->start_lsn);
	PQclear(res);
	disconnect();
}

static void
pg_ptrack_clear(void)
{
	PGresult	*res_db, *res;
	const char *old_dbname = pgut_dbname;
	int i;

	reconnect();
	res_db = execute("SELECT datname FROM pg_database", 0, NULL);
	disconnect();
	for(i=0; i < PQntuples(res_db); i++)
	{
		pgut_dbname = PQgetvalue(res_db, i, 0);
		if (!strcmp(pgut_dbname, "template0"))
			continue;
		reconnect();
		res = execute("SELECT pg_ptrack_clear()", 0, NULL);
		PQclear(res);
	}
	PQclear(res_db);
	disconnect();
	pgut_dbname = old_dbname;
}

static char *
pg_ptrack_get_and_clear(Oid tablespace_oid, Oid db_oid, Oid rel_oid, size_t *result_size)
{
	PGresult	*res_db, *res;
	const char	*old_dbname = pgut_dbname;
	char		*params[2];
	char		*result;

	reconnect();
	params[0] = palloc(64);
	params[1] = palloc(64);
	sprintf(params[0], "%i", db_oid);
	sprintf(params[1], "%i", rel_oid);
	res_db = execute("SELECT datname FROM pg_database WHERE oid=$1", 1, (const char **)params);
	disconnect();
	pgut_dbname = pstrdup(PQgetvalue(res_db, 0, 0));
	PQclear(res_db);

	reconnect();
	sprintf(params[0], "%i", tablespace_oid);
	res = execute("SELECT pg_ptrack_get_and_clear($1, $2)", 2,  (const char **)params);
	result = (char *)PQunescapeBytea((unsigned char *)PQgetvalue(res, 0, 0), result_size);
	PQclear(res);
	pfree(params[0]);
	pfree(params[1]);

	pfree((char *)pgut_dbname);
	pgut_dbname = old_dbname;

	return result;
}

static void
wait_for_archive(pgBackup *backup, const char *sql)
{
	PGresult	   *res;
	char			ready_path[MAXPGPATH];
	char			file_name[MAXFNAMELEN];
	int				try_count;
	XLogRecPtr		lsn;
	TimeLineID		tli;
	XLogSegNo	targetSegNo;

	reconnect();

	/* Remove annoying NOTICE messages generated by backend */
	res = execute("SET client_min_messages = warning;", 0, NULL);
	PQclear(res);

	/* And execute the query wanted */
	res = execute(sql, 0, NULL);

	/* Get LSN from execution result */
	get_lsn(res, &lsn);

	/*
	 * Enforce TLI obtention if backup is not present as this code
	 * path can be taken as a callback at exit.
	 */
	tli = get_current_timeline(false);

	/* Fill in fields if backup exists */
	if (backup != NULL)
	{
		backup->tli = tli;
		backup->stop_lsn = lsn;
		elog(LOG, "%s(): tli=%X lsn=%X/%08X",
			 __FUNCTION__, backup->tli,
			 (uint32) (backup->stop_lsn >> 32),
			 (uint32) backup->stop_lsn);
	}

	/* As well as WAL file name */
	XLByteToSeg(lsn, targetSegNo);
	XLogFileName(file_name, tli, targetSegNo);

	snprintf(ready_path, lengthof(ready_path),
		"%s/pg_xlog/archive_status/%s.ready", pgdata,
			 file_name);
	elog(LOG, "%s() wait for %s", __FUNCTION__, ready_path);

	PQclear(res);

	res = execute(TXID_CURRENT_SQL, 0, NULL);
	if (backup != NULL)
	{
		get_xid(res, &backup->recovery_xid);
		backup->recovery_time = time(NULL);
	}
	disconnect();

	/* wait until switched WAL is archived */
	try_count = 0;
	while (fileExists(ready_path))
	{
		sleep(1);
		if (interrupted)
			elog(ERROR,
				"interrupted during waiting for WAL archiving");
		try_count++;
		if (try_count > TIMEOUT_ARCHIVE)
			elog(ERROR,
				"switched WAL could not be archived in %d seconds",
				TIMEOUT_ARCHIVE);
	}
	elog(LOG, "%s() .ready deleted in %d try", __FUNCTION__, try_count);
}

/*
 * Notify end of backup to PostgreSQL server.
 */
static void
pg_stop_backup(pgBackup *backup)
{
	if (stream_wal)
	{
		PGresult	*res;
		TimeLineID	tli;

		reconnect();

		/* Remove annoying NOTICE messages generated by backend */
		res = execute("SET client_min_messages = warning;", 0, NULL);
		PQclear(res);

		/* And execute the query wanted */
		res = execute("SELECT * FROM pg_stop_backup()", 0, NULL);

		/* Get LSN from execution result */
		get_lsn(res, &stop_backup_lsn);
		PQclear(res);

		/*
		 * Enforce TLI obtention if backup is not present as this code
		 * path can be taken as a callback at exit.
		 */
		tli = get_current_timeline(false);

		/* Fill in fields if backup exists */
		if (backup != NULL)
		{
			backup->tli = tli;
			backup->stop_lsn = stop_backup_lsn;
			elog(LOG, "%s(): tli=%X lsn=%X/%08X",
				 __FUNCTION__, backup->tli,
				 (uint32) (backup->stop_lsn >> 32),
				 (uint32) backup->stop_lsn);
		}

		res = execute(TXID_CURRENT_SQL, 0, NULL);
		if (backup != NULL)
		{
			get_xid(res, &backup->recovery_xid);
			backup->recovery_time = time(NULL);
		}
		PQclear(res);
		disconnect();
	}
	else
		wait_for_archive(backup,
		"SELECT * FROM pg_stop_backup()");
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
 * Get LSN from result of pg_start_backup() or pg_stop_backup().
 */
static void
get_lsn(PGresult *res, XLogRecPtr *lsn)
{
	uint32	xlogid;
	uint32	xrecoff;

	if (res == NULL || PQntuples(res) != 1 || PQnfields(res) != 1)
		elog(ERROR,
			"result of backup command is invalid: %s",
			PQerrorMessage(connection));

	/*
	 * Extract timeline and LSN from results of pg_stop_backup()
	 * and friends.
	 */

	XLogDataFromLSN(PQgetvalue(res, 0, 0), &xlogid, &xrecoff);
	/* Calculate LSN */
	*lsn = (XLogRecPtr) ((uint64) xlogid << 32) | xrecoff;
}

/*
 * Get XID from result of txid_current() after pg_stop_backup().
 */
static void
get_xid(PGresult *res, uint32 *xid)
{
	if (res == NULL || PQntuples(res) != 1 || PQnfields(res) != 1)
		elog(ERROR,
			"result of txid_current() is invalid: %s",
			PQerrorMessage(connection));

	if (sscanf(PQgetvalue(res, 0, 0), "%u", xid) != 1)
	{
		elog(ERROR,
			"result of txid_current() is invalid: %s",
			PQerrorMessage(connection));
	}
	elog(LOG, "%s():%s", __FUNCTION__, PQgetvalue(res, 0, 0));
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
	for (i = arguments->start_file_idx; i < arguments->end_file_idx; i++)
	{
		int			ret;
		struct stat	buf;

		pgFile *file = (pgFile *) parray_get(arguments->files, i);

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
			if (!(file->is_datafile
					? backup_data_file(arguments->from_root, arguments->to_root, file, arguments->lsn)
					: copy_file(arguments->from_root, arguments->to_root, file)))
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
	}
}


/*
 * Append files to the backup list array.
 */
static void
add_files(parray *files, const char *root, bool add_root, bool is_pgdata)
{
	parray	*list_file;
	int		 i;

	list_file = parray_new();

	/* list files with the logical path. omit $PGDATA */
	dir_list_file(list_file, root, pgdata_exclude, true, add_root);

	/* mark files that are possible datafile as 'datafile' */
	for (i = 0; i < parray_num(list_file); i++)
	{
		pgFile *file = (pgFile *) parray_get(list_file, i);
		char *relative;
		char *fname;
		int path_len;

		/* data file must be a regular file */
		if (!S_ISREG(file->mode))
			continue;

		/* data files are under "base", "global", or "pg_tblspc" */
		relative = file->path + strlen(root) + 1;
		if (is_pgdata &&
			!path_is_prefix_of_path("base", relative) &&
			/*!path_is_prefix_of_path("global", relative) &&*/
			!path_is_prefix_of_path("pg_tblspc", relative))
			continue;

		path_len = strlen(file->path);
		if (path_len > 6 && strncmp(file->path+(path_len-6), "ptrack", 6) == 0)
		{
			pgFile *search_file;
			pgFile **pre_search_file;
			int segno = 0;
			while(true) {
				pgFile tmp_file;
				tmp_file.path = pg_strdup(file->path);
				/* I hope segno not more than 999999 */
				if (segno > 0)
					sprintf(tmp_file.path+path_len-7, ".%d", segno);
				else
					tmp_file.path[path_len-7] = '\0';
				pre_search_file = (pgFile **) parray_bsearch(list_file, &tmp_file, pgFileComparePath);
				if (pre_search_file != NULL)
				{
					search_file = *pre_search_file;
					search_file->ptrack_path = pg_strdup(file->path);
					search_file->segno = segno;
				} else {
					pg_free(tmp_file.path);
					break;
				}
				pg_free(tmp_file.path);
				segno++;
			}

			pgFileFree(file);
			parray_remove(list_file, i);
			i--;
			continue;
		}

		/* name of data file start with digit */
		fname = last_dir_separator(relative);
		if (fname == NULL)
			fname = relative;
		else
			fname++;
		if (!isdigit(fname[0]))
			continue;

		file->is_datafile = true;
	}
	parray_concat(files, list_file);
}

/*
 * Output the list of files to backup catalog
 */
static void
create_file_list(parray *files,
				 const char *root,
				 const char *subdir,
				 const char *prefix,
				 bool is_append)
{
	FILE	*fp;
	char	 path[MAXPGPATH];

	if (!check)
	{
		/* output path is '$BACKUP_PATH/file_database.txt' */
		pgBackupGetPath(&current, path, lengthof(path), subdir);
		fp = fopen(path, is_append ? "at" : "wt");
		if (fp == NULL)
			elog(ERROR, "can't open file list \"%s\": %s", path,
				strerror(errno));
		dir_print_file_list(fp, files, root, prefix);
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

void make_pagemap_from_ptrack(parray *files)
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
			tablespace = palloc0(64);

			/* Find target path*/
			for(sep_iter = (int)path_length; sep_iter >= 0; sep_iter--)
			{
				if (IS_DIR_SEP(tmp_path[sep_iter]))
				{
					sep_count++;
				}
				if (sep_count == 3)
				{
					tmp_path += sep_iter + 1;
					break;
				}
			}
			/* For unix only now */
			sscanf(tmp_path, "%[^/]/%u/%u_ptrack", tablespace, &db_oid, &rel_oid);
			if (strcmp(tablespace, "base") != 0 && strcmp(tablespace, "global") != 0)
				sscanf(tablespace, "%i", &tablespace_oid);

			flat_memory = pg_ptrack_get_and_clear(tablespace_oid,
												  db_oid,
												  rel_oid,
												  &flat_size);

			start_addr = (RELSEG_SIZE/8)*p->segno;
			p->pagemap.bitmapsize = start_addr+RELSEG_SIZE/8 > flat_size ? flat_size - start_addr : RELSEG_SIZE/8;
			p->pagemap.bitmap = pg_malloc(p->pagemap.bitmapsize);
			memcpy(p->pagemap.bitmap, flat_memory+start_addr, p->pagemap.bitmapsize);
			pg_free(flat_memory);
			pg_free(tablespace);
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
		/* Error message already written in GetConnection() */
		return;

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

	ReceiveXlogStream(conn, startpos, starttli, NULL, basedir,
					  stop_streaming, standby_message_timeout, ".partial",
					  false, false);

	PQfinish(conn);
	conn = NULL;
}
