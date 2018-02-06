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
#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "datapagemap.h"
#include "receivelog.h"
#include "streamutil.h"
#include "pgtar.h"

static int	standby_message_timeout = 10 * 1000;	/* 10 sec = default */
static XLogRecPtr stop_backup_lsn = InvalidXLogRecPtr;

/*
 * How long we should wait for streaming end in seconds.
 * Retreived as checkpoint_timeout + checkpoint_timeout * 0.1
 */
static uint32 stream_stop_timeout = 0;
/* Time in which we started to wait for streaming end */
static time_t stream_stop_begin = 0;

const char *progname = "pg_probackup";

/* list of files contained in backup */
static parray *backup_files_list = NULL;

static pthread_mutex_t start_stream_mut = PTHREAD_MUTEX_INITIALIZER;
/*
 * We need to wait end of WAL streaming before execute pg_stop_backup().
 */
static pthread_t stream_thread;

static int is_ptrack_enable = false;
bool is_ptrack_support = false;
bool is_checksum_enabled = false;
bool exclusive_backup = false;

/* Backup connections */
static PGconn *backup_conn = NULL;
static PGconn *master_conn = NULL;
static PGconn *backup_conn_replication = NULL;

/* PostgreSQL server version from "backup_conn" */
static int server_version = 0;
static char server_version_str[100] = "";

/* Is pg_start_backup() was executed */
static bool backup_in_progress = false;
/* Is pg_stop_backup() was sent */
static bool pg_stop_backup_is_sent = false;

/*
 * Backup routines
 */
static void backup_cleanup(bool fatal, void *userdata);
static void backup_disconnect(bool fatal, void *userdata);

static void backup_files(void *arg);
static void remote_backup_files(void *arg);

static void do_backup_instance(void);

static void pg_start_backup(const char *label, bool smooth, pgBackup *backup);
static void pg_switch_wal(PGconn *conn);
static void pg_stop_backup(pgBackup *backup);
static int checkpoint_timeout(void);

static void parse_backup_filelist_filenames(parray *files, const char *root);
static void write_backup_file_list(parray *files, const char *root);
static void wait_wal_lsn(XLogRecPtr lsn, bool wait_prev_segment);
static void wait_replica_wal_lsn(XLogRecPtr lsn, bool is_start_backup);
static void make_pagemap_from_ptrack(parray *files);
static void StreamLog(void *arg);

static void get_remote_pgdata_filelist(parray *files);
static void ReceiveFileList(parray* files, PGconn *conn, PGresult *res, int rownum);
static void	remote_copy_file(PGconn *conn, pgFile* file);

/* Ptrack functions */
static void pg_ptrack_clear(void);
static bool pg_ptrack_support(void);
static bool pg_ptrack_enable(void);
static bool pg_checksum_enable(void);
static bool pg_is_in_recovery(void);
static bool pg_ptrack_get_and_clear_db(Oid dbOid, Oid tblspcOid);
static char *pg_ptrack_get_and_clear(Oid tablespace_oid,
									 Oid db_oid,
									 Oid rel_oid,
									 size_t *result_size);
static XLogRecPtr get_last_ptrack_lsn(void);

/* Check functions */
static void check_server_version(void);
static void check_system_identifiers(void);
static void confirm_block_size(const char *name, int blcksz);
static void set_cfs_datafiles(parray *files, const char *root, char *relative, size_t i);


#define disconnect_and_exit(code)				\
	{											\
	if (conn != NULL) PQfinish(conn);			\
	exit(code);									\
	}

/* Fill "files" with data about all the files to backup */
static void
get_remote_pgdata_filelist(parray *files)
{
	PGresult   *res;
	int resultStatus;
	int i;

	backup_conn_replication = pgut_connect_replication(pgut_dbname);

	if (PQsendQuery(backup_conn_replication, "FILE_BACKUP FILELIST") == 0)
		elog(ERROR,"%s: could not send replication command \"%s\": %s",
				PROGRAM_NAME, "FILE_BACKUP", PQerrorMessage(backup_conn_replication));

	res = PQgetResult(backup_conn_replication);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		resultStatus = PQresultStatus(res);
		PQclear(res);
		elog(ERROR, "cannot start getting FILE_BACKUP filelist: %s, result_status %d",
			 PQerrorMessage(backup_conn_replication), resultStatus);
	}

	if (PQntuples(res) < 1)
		elog(ERROR, "%s: no data returned from server", PROGRAM_NAME);

	for (i = 0; i < PQntuples(res); i++)
	{
		ReceiveFileList(files, backup_conn_replication, res, i);
	}

	res = PQgetResult(backup_conn_replication);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		elog(ERROR, "%s: final receive failed: %s",
				PROGRAM_NAME, PQerrorMessage(backup_conn_replication));
	}

	PQfinish(backup_conn_replication);
}

/*
 * workhorse for get_remote_pgdata_filelist().
 * Parse received message into pgFile structure.
 */
static void
ReceiveFileList(parray* files, PGconn *conn, PGresult *res, int rownum)
{
	char		filename[MAXPGPATH];
	pgoff_t		current_len_left = 0;
	bool		basetablespace;
	char	   *copybuf = NULL;
	pgFile	   *pgfile;

	/* What for do we need this basetablespace field?? */
	basetablespace = PQgetisnull(res, rownum, 0);
	if (basetablespace)
		elog(LOG,"basetablespace");
	else
		elog(LOG, "basetablespace %s", PQgetvalue(res, rownum, 1));

	res = PQgetResult(conn);

	if (PQresultStatus(res) != PGRES_COPY_OUT)
		elog(ERROR, "Could not get COPY data stream: %s", PQerrorMessage(conn));

	while (1)
	{
		int			r;
		int			filemode;

		if (copybuf != NULL)
		{
			PQfreemem(copybuf);
			copybuf = NULL;
		}

		r = PQgetCopyData(conn, &copybuf, 0);

		if (r == -2)
			elog(ERROR, "Could not read COPY data: %s", PQerrorMessage(conn));

		/* end of copy */
		if (r == -1)
			break;

		/* This must be the header for a new file */
		if (r != 512)
			elog(ERROR, "Invalid tar block header size: %d\n", r);

		current_len_left = read_tar_number(&copybuf[124], 12);

		/* Set permissions on the file */
		filemode = read_tar_number(&copybuf[100], 8);

		/* First part of header is zero terminated filename */
		snprintf(filename, sizeof(filename), "%s", copybuf);

		pgfile = pgFileInit(filename);
		pgfile->size = current_len_left;
		pgfile->mode |= filemode;

		if (filename[strlen(filename) - 1] == '/')
		{
			/* Symbolic link or directory has size zero */
			Assert (pgfile->size == 0);
			/* Ends in a slash means directory or symlink to directory */
			if (copybuf[156] == '5')
			{
				/* Directory */
				pgfile->mode |= S_IFDIR;
			}
			else if (copybuf[156] == '2')
			{
				/* Symlink */
				pgfile->mode |= S_IFLNK;
			}
			else
				elog(ERROR, "Unrecognized link indicator \"%c\"\n",
							 copybuf[156]);
		}
		else
		{
			/* regular file */
			pgfile->mode |= S_IFREG;
		}

		parray_append(files, pgfile);
	}

	if (copybuf != NULL)
		PQfreemem(copybuf);
}

/* read one file via replication protocol
 * and write it to the destination subdir in 'backup_path' */
static void	remote_copy_file(PGconn *conn, pgFile* file)
{
	PGresult	*res;
	char		*copybuf = NULL;
	char		buf[32768];
	FILE		*out;
	char		database_path[MAXPGPATH];
	char		to_path[MAXPGPATH];
	bool skip_padding = false;

	pgBackupGetPath(&current, database_path, lengthof(database_path),
					DATABASE_DIR);
	join_path_components(to_path, database_path, file->path);

	out = fopen(to_path, "w");
	if (out == NULL)
	{
		int errno_tmp = errno;
		elog(ERROR, "cannot open destination file \"%s\": %s",
			to_path, strerror(errno_tmp));
	}

	INIT_CRC32C(file->crc);

	/* read from stream and write to backup file */
	while (1)
	{
		int			row_length;
		int			errno_tmp;
		int			write_buffer_size = 0;
		if (copybuf != NULL)
		{
			PQfreemem(copybuf);
			copybuf = NULL;
		}

		row_length = PQgetCopyData(conn, &copybuf, 0);

		if (row_length == -2)
			elog(ERROR, "Could not read COPY data: %s", PQerrorMessage(conn));

		if (row_length == -1)
			break;

		if (!skip_padding)
		{
			write_buffer_size = Min(row_length, sizeof(buf));
			memcpy(buf, copybuf, write_buffer_size);
			COMP_CRC32C(file->crc, &buf, write_buffer_size);

			/* TODO calc checksum*/
			if (fwrite(buf, 1, write_buffer_size, out) != write_buffer_size)
			{
				errno_tmp = errno;
				/* oops */
				FIN_CRC32C(file->crc);
				fclose(out);
				PQfinish(conn);
				elog(ERROR, "cannot write to \"%s\": %s", to_path,
					strerror(errno_tmp));
			}

			file->read_size += write_buffer_size;
		}
		if (file->read_size >= file->size)
		{
			skip_padding = true;
		}
	}

	res = PQgetResult(conn);

	/* File is not found. That's normal. */
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		elog(ERROR, "final receive failed: status %d ; %s",PQresultStatus(res), PQerrorMessage(conn));
	}

	file->write_size = file->read_size;
	FIN_CRC32C(file->crc);

	fclose(out);
}

/*
 * Take a remote backup of the PGDATA at a file level.
 * Copy all directories and files listed in backup_files_list.
 */
static void
remote_backup_files(void *arg)
{
	int i;
	backup_files_args *arguments = (backup_files_args *) arg;
	int n_backup_files_list = parray_num(arguments->backup_files_list);
	PGconn		*file_backup_conn = NULL;

	for (i = 0; i < n_backup_files_list; i++)
	{
		char		*query_str;
		PGresult	*res;
		char		*copybuf = NULL;
		pgFile		*file;
		int			row_length;

		file = (pgFile *) parray_get(arguments->backup_files_list, i);

		/* We have already copied all directories */
		if (S_ISDIR(file->mode))
			continue;

		if (__sync_lock_test_and_set(&file->lock, 1) != 0)
			continue;

		file_backup_conn = pgut_connect_replication(pgut_dbname);

		/* check for interrupt */
		if (interrupted)
			elog(ERROR, "interrupted during backup");

		query_str = psprintf("FILE_BACKUP FILEPATH '%s'",file->path);

		if (PQsendQuery(file_backup_conn, query_str) == 0)
			elog(ERROR,"%s: could not send replication command \"%s\": %s",
				PROGRAM_NAME, query_str, PQerrorMessage(file_backup_conn));

		res = PQgetResult(file_backup_conn);

		/* File is not found. That's normal. */
		if (PQresultStatus(res) == PGRES_COMMAND_OK)
		{
			PQclear(res);
			PQfinish(file_backup_conn);
			continue;
		}

		if (PQresultStatus(res) != PGRES_COPY_OUT)
		{
			PQclear(res);
			PQfinish(file_backup_conn);
			elog(ERROR, "Could not get COPY data stream: %s", PQerrorMessage(file_backup_conn));
		}

		/* read the header of the file */
		row_length = PQgetCopyData(file_backup_conn, &copybuf, 0);

		if (row_length == -2)
			elog(ERROR, "Could not read COPY data: %s", PQerrorMessage(file_backup_conn));

		/* end of copy TODO handle it */
		if (row_length == -1)
			elog(ERROR, "Unexpected end of COPY data");

		if(row_length != 512)
			elog(ERROR, "Invalid tar block header size: %d\n", row_length);
		file->size = read_tar_number(&copybuf[124], 12);

		/* receive the data from stream and write to backup file */
		remote_copy_file(file_backup_conn, file);

		elog(VERBOSE, "File \"%s\". Copied %lu bytes",
				 file->path, (unsigned long) file->write_size);
		PQfinish(file_backup_conn);
	}
}

/*
 * Take a backup of a single postgresql instance.
 * Move files from 'pgdata' to a subdirectory in 'backup_path'.
 */
static void
do_backup_instance(void)
{
	int			i;
	char		database_path[MAXPGPATH];
	char		dst_backup_path[MAXPGPATH];
	char		label[1024];
	XLogRecPtr	prev_backup_start_lsn = InvalidXLogRecPtr;

	pthread_t	backup_threads[num_threads];
	backup_files_args *backup_threads_args[num_threads];

	pgBackup   *prev_backup = NULL;
	char		prev_backup_filelist_path[MAXPGPATH];
	parray	   *prev_backup_filelist = NULL;

	elog(LOG, "Database backup start");

	/* Initialize size summary */
	current.data_bytes = 0;

	/* Obtain current timeline */
	if (is_remote_backup)
	{
		char	   *sysidentifier;
		TimeLineID	starttli;
		XLogRecPtr	startpos;

		backup_conn_replication = pgut_connect_replication(pgut_dbname);

		/* Check replication prorocol connection */
		if (!RunIdentifySystem(backup_conn_replication, &sysidentifier,  &starttli, &startpos, NULL))
			elog(ERROR, "Failed to send command for remote backup");

// TODO implement the check
// 		if (&sysidentifier != system_identifier)
// 			elog(ERROR, "Backup data directory was initialized for system id %ld, but target backup directory system id is %ld",
// 			system_identifier, sysidentifier);

		current.tli = starttli;

		PQfinish(backup_conn_replication);
	}
	else
		current.tli = get_current_timeline(false);
	/*
	 * In incremental backup mode ensure that already-validated
	 * backup on current timeline exists and get its filelist.
	 */
	if (current.backup_mode == BACKUP_MODE_DIFF_PAGE ||
		current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		parray	   *backup_list;
		/* get list of backups already taken */
		backup_list = catalog_get_backup_list(INVALID_BACKUP_ID);
		if (backup_list == NULL)
			elog(ERROR, "Failed to get backup list.");

		prev_backup = catalog_get_last_data_backup(backup_list, current.tli);
		if (prev_backup == NULL)
			elog(ERROR, "Valid backup on current timeline is not found. "
						"Create new FULL backup before an incremental one.");
		parray_free(backup_list);

		pgBackupGetPath(prev_backup, prev_backup_filelist_path, lengthof(prev_backup_filelist_path),
						DATABASE_FILE_LIST);
		prev_backup_filelist = dir_read_file_list(pgdata, prev_backup_filelist_path);

		/* If lsn is not NULL, only pages with higher lsn will be copied. */
		prev_backup_start_lsn = prev_backup->start_lsn;
		current.parent_backup = prev_backup->start_time;

		pgBackupWriteBackupControlFile(&current);
	}

	/*
	 * It`s illegal to take PTRACK backup if LSN from ptrack_control() is not equal to
	 * stort_backup LSN of previous backup
	 */
	if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		XLogRecPtr	ptrack_lsn = get_last_ptrack_lsn();

		if (ptrack_lsn > prev_backup->stop_lsn || ptrack_lsn == InvalidXLogRecPtr)
		{
			elog(ERROR, "LSN from ptrack_control %lx differs from STOP LSN of previous backup %lx.\n"
						"Create new full backup before an incremental one.",
						ptrack_lsn, prev_backup->stop_lsn);
		}
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

		pthread_mutex_lock(&start_stream_mut);
		pthread_create(&stream_thread, NULL, (void *(*)(void *)) StreamLog, dst_backup_path);
		pthread_mutex_lock(&start_stream_mut);
		if (conn == NULL)
			elog(ERROR, "Cannot continue backup because stream connect has failed.");

		pthread_mutex_unlock(&start_stream_mut);
	}

	/* initialize backup list */
	backup_files_list = parray_new();

	/* list files with the logical path. omit $PGDATA */
	if (is_remote_backup)
		get_remote_pgdata_filelist(backup_files_list);
	else
		dir_list_file(backup_files_list, pgdata, true, true, false);

	/* Extract information about files in backup_list parsing their names:*/
	parse_backup_filelist_filenames(backup_files_list, pgdata);

	if (current.backup_mode != BACKUP_MODE_FULL)
	{
		elog(LOG, "current_tli:%X", current.tli);
		elog(LOG, "prev_backup->start_lsn: %X/%X",
			 (uint32) (prev_backup->start_lsn >> 32), (uint32) (prev_backup->start_lsn));
		elog(LOG, "current.start_lsn: %X/%X",
			 (uint32) (current.start_lsn >> 32), (uint32) (current.start_lsn));
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

	if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		parray_qsort(backup_files_list, pgFileComparePath);
		make_pagemap_from_ptrack(backup_files_list);
	}

	/*
	 * Sort pathname ascending. It is necessary to create intermediate
	 * directories sequentially.
	 *
	 * For example:
	 * 1 - create 'base'
	 * 2 - create 'base/1'
	 */
	parray_qsort(backup_files_list, pgFileComparePath);

	/*
	 * Make directories before backup
	 * and setup threads at the same time
	 */
	for (i = 0; i < parray_num(backup_files_list); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(backup_files_list, i);

		/* if the entry was a directory, create it in the backup */
		if (S_ISDIR(file->mode))
		{
			char		dirpath[MAXPGPATH];
			char	   *dir_name;
			char		database_path[MAXPGPATH];

			if (!is_remote_backup)
				dir_name = GetRelativePath(file->path, pgdata);
			else
				dir_name = file->path;

			elog(VERBOSE, "Create directory \"%s\"", dir_name);
			pgBackupGetPath(&current, database_path, lengthof(database_path),
					DATABASE_DIR);

			join_path_components(dirpath, database_path, dir_name);
			dir_create_dir(dirpath, DIR_PERMISSION);
		}

		/* setup threads */
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
		arg->thread_backup_conn = NULL;
		arg->thread_cancel_conn = NULL;
		backup_threads_args[i] = arg;
	}

	/* Run threads */
	elog(LOG, "Start transfering data files");
	for (i = 0; i < num_threads; i++)
	{
		elog(VERBOSE, "Start thread num: %i", i);

		if (!is_remote_backup)
			pthread_create(&backup_threads[i], NULL,
						   (void *(*)(void *)) backup_files,
						   backup_threads_args[i]);
		else
			pthread_create(&backup_threads[i], NULL,
						   (void *(*)(void *)) remote_backup_files,
						   backup_threads_args[i]);
	}

	/* Wait threads */
	for (i = 0; i < num_threads; i++)
	{
		pthread_join(backup_threads[i], NULL);
		pg_free(backup_threads_args[i]);
	}
	elog(LOG, "Data files are transfered");

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

		/* Scan backup PG_XLOG_DIR */
		xlog_files_list = parray_new();
		join_path_components(pg_xlog_path, database_path, PG_XLOG_DIR);
		dir_list_file(xlog_files_list, pg_xlog_path, false, true, false);

		for (i = 0; i < parray_num(xlog_files_list); i++)
		{
			pgFile	   *file = (pgFile *) parray_get(xlog_files_list, i);
			if (S_ISREG(file->mode))
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

		if (S_ISDIR(file->mode))
			current.data_bytes += 4096;

		/* Count the amount of the data actually copied */
		if (S_ISREG(file->mode))
			current.data_bytes += file->write_size;
	}

	parray_walk(backup_files_list, pgFileFree);
	parray_free(backup_files_list);
	backup_files_list = NULL;
}

/*
 * Entry point of pg_probackup BACKUP subcommand.
 */
int
do_backup(time_t start_time)
{

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

	/* Confirm data block size and xlog block size are compatible */
	confirm_block_size("block_size", BLCKSZ);
	confirm_block_size("wal_block_size", XLOG_BLCKSZ);

	from_replica = pg_is_in_recovery();

	/* Confirm that this server version is supported */
	check_server_version();

	/* TODO fix it for remote backup*/
	if (!is_remote_backup)
		current.checksum_version = get_data_checksum_version(true);

	is_checksum_enabled = pg_checksum_enable();

	if (is_checksum_enabled)
		elog(LOG, "This PostgreSQL instance initialized with data block checksums. "
					"Data block corruption will be detected");
	else
		elog(WARNING, "This PostgreSQL instance initialized without data block checksums. "
						"pg_probackup have no way to detect data block corruption without them. "
						"Reinitialize PGDATA with option '--data-checksums'.");
	
	StrNCpy(current.server_version, server_version_str,
			sizeof(current.server_version));
	current.stream = stream_wal;

	is_ptrack_support = pg_ptrack_support();
	if (is_ptrack_support)
	{
		is_ptrack_enable = pg_ptrack_enable();
	}

	if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		if (!is_ptrack_support)
			elog(ERROR, "This PostgreSQL instance does not support ptrack");
		else
		{
			if(!is_ptrack_enable)
				elog(ERROR, "Ptrack is disabled");
		}
	}

	if (from_replica)
	{
		/* Check master connection options */
		if (master_host == NULL)
			elog(ERROR, "Options for connection to master must be provided to perform backup from replica");

		/* Create connection to master server */
		master_conn = pgut_connect_extended(master_host, master_port, master_db, master_user);
	}

	/* Get exclusive lock of backup catalog */
	catalog_lock();

	/*
	 * Ensure that backup directory was initialized for the same PostgreSQL
	 * instance we opened connection to. And that target backup database PGDATA
	 * belogns to the same instance.
	 */
	/* TODO fix it for remote backup */
	if (!is_remote_backup)
		check_system_identifiers();


	/* Start backup. Update backup status. */
	current.status = BACKUP_STATUS_RUNNING;
	current.start_time = start_time;

	/* Create backup directory and BACKUP_CONTROL_FILE */
	if (pgBackupCreateDir(&current))
		elog(ERROR, "cannot create backup directory");
	pgBackupWriteBackupControlFile(&current);

	elog(LOG, "Backup destination is initialized");

	/* set the error processing function for the backup process */
	pgut_atexit_push(backup_cleanup, NULL);

	/* backup data */
	do_backup_instance();
	pgut_atexit_pop(backup_cleanup, NULL);

	/* compute size of wal files of this backup stored in the archive */
	if (!current.stream)
	{
		current.wal_bytes = XLOG_SEG_SIZE *
							(current.stop_lsn/XLogSegSize - current.start_lsn/XLogSegSize + 1);
	}

	/* Backup is done. Update backup status */
	current.end_time = time(NULL);
	current.status = BACKUP_STATUS_DONE;
	pgBackupWriteBackupControlFile(&current);

	//elog(LOG, "Backup completed. Total bytes : " INT64_FORMAT "",
	//		current.data_bytes);

	pgBackupValidate(&current);

	elog(INFO, "Backup %s completed", base36enc(current.start_time));

	/*
	 * After successfil backup completion remove backups
	 * which are expired according to retention policies
	 */
	if (delete_expired || delete_wal)
		do_retention_purge();

	return 0;
}

/*
 * Confirm that this server version is supported
 */
static void
check_server_version(void)
{
	PGresult   *res;

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

	/* Do exclusive backup only for PostgreSQL 9.5 */
	exclusive_backup = server_version < 90600 ||
		current.backup_mode == BACKUP_MODE_DIFF_PTRACK;

	/* Save server_version to use it in future */
	res = pgut_execute(backup_conn, "show server_version", 0, NULL, true);
	StrNCpy(server_version_str, PQgetvalue(res, 0, 0), sizeof(server_version_str));
	PQclear(res);
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
	uint64		system_id_conn;
	uint64		system_id_pgdata;

	system_id_pgdata = get_system_identifier(pgdata);
	system_id_conn = get_remote_system_identifier(backup_conn);

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

	res = pgut_execute(backup_conn, "SELECT current_setting($1)", 1, &name, true);
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
	PGconn	   *conn;

	params[0] = label;

	/* For replica we call pg_start_backup() on master */
	conn = (from_replica) ? master_conn : backup_conn;

	/* 2nd argument is 'fast'*/
	params[1] = smooth ? "false" : "true";
	if (!exclusive_backup)
		res = pgut_execute(conn,
						   "SELECT pg_start_backup($1, $2, false)",
						   2,
						   params,
						   true);
	else
		res = pgut_execute(conn,
						   "SELECT pg_start_backup($1, $2)",
						   2,
						   params,
						   true);

	/* Extract timeline and LSN from results of pg_start_backup() */
	XLogDataFromLSN(PQgetvalue(res, 0, 0), &xlogid, &xrecoff);
	/* Calculate LSN */
	backup->start_lsn = (XLogRecPtr) ((uint64) xlogid << 32) | xrecoff;

	PQclear(res);

	if (current.backup_mode == BACKUP_MODE_DIFF_PAGE)
		/*
		 * Switch to a new WAL segment. It is necessary to get archived WAL
		 * segment, which includes start LSN of current backup.
		 */
		pg_switch_wal(conn);

	if (!stream_wal)
	{
		/*
		 * Do not wait start_lsn for stream backup.
		 * Because WAL streaming will start after pg_start_backup() in stream
		 * mode.
		 */
		/* In PAGE mode wait for current segment... */
		if (current.backup_mode == BACKUP_MODE_DIFF_PAGE)
			wait_wal_lsn(backup->start_lsn, false);
		/* ...for others wait for previous segment */
		else
			wait_wal_lsn(backup->start_lsn, true);
	}

	/* Wait for start_lsn to be replayed by replica */
	if (from_replica)
		wait_replica_wal_lsn(backup->start_lsn, true);

	/*
	 * Set flag that pg_start_backup() was called. If an error will happen it
	 * is necessary to call pg_stop_backup() in backup_cleanup().
	 */
	backup_in_progress = true;
}

/*
 * Switch to a new WAL segment. It should be called only for master.
 */
static void
pg_switch_wal(PGconn *conn)
{
	PGresult   *res;

	/* Remove annoying NOTICE messages generated by backend */
	res = pgut_execute(conn, "SET client_min_messages = warning;", 0, NULL, true);
	PQclear(res);

	if (server_version >= 100000)
		res = pgut_execute(conn, "SELECT * FROM pg_switch_wal()", 0, NULL, true);
	else
		res = pgut_execute(conn, "SELECT * FROM pg_switch_xlog()", 0, NULL, true);

	PQclear(res);
}

/*
 * Check if the instance supports ptrack
 * TODO Maybe we should rather check ptrack_version()?
 */
static bool
pg_ptrack_support(void)
{
	PGresult   *res_db;

	res_db = pgut_execute(backup_conn,
						  "SELECT proname FROM pg_proc WHERE proname='ptrack_version'",
						  0, NULL, true);
	if (PQntuples(res_db) == 0)
	{
		PQclear(res_db);
		return false;
	}
	PQclear(res_db);

	res_db = pgut_execute(backup_conn,
						  "SELECT ptrack_version()",
						  0, NULL, true);
	if (PQntuples(res_db) == 0)
	{
		PQclear(res_db);
		return false;
	}

	/* Now we support only ptrack version 1.5 */
	if (strcmp(PQgetvalue(res_db, 0, 0), "1.5") != 0)
	{
		elog(WARNING, "Update your ptrack to the version 1.5. Current version is %s", PQgetvalue(res_db, 0, 0));
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

	res_db = pgut_execute(backup_conn, "show ptrack_enable", 0, NULL, true);

	if (strcmp(PQgetvalue(res_db, 0, 0), "on") != 0)
	{
		PQclear(res_db);
		return false;
	}
	PQclear(res_db);
	return true;
}

/* Check if ptrack is enabled in target instance */
static bool
pg_checksum_enable(void)
{
	PGresult   *res_db;

	res_db = pgut_execute(backup_conn, "show data_checksums", 0, NULL, true);

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

	res_db = pgut_execute(backup_conn, "SELECT pg_is_in_recovery()", 0, NULL, true);

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
	Oid dbOid, tblspcOid;
	char *params[2];

	params[0] = palloc(64);
	params[1] = palloc(64);
	res_db = pgut_execute(backup_conn, "SELECT datname, oid, dattablespace FROM pg_database",
						  0, NULL, true);

	for(i = 0; i < PQntuples(res_db); i++)
	{
		PGconn	   *tmp_conn;

		dbname = PQgetvalue(res_db, i, 0);
		if (strcmp(dbname, "template0") == 0)
			continue;

		dbOid = atoi(PQgetvalue(res_db, i, 1));
		tblspcOid = atoi(PQgetvalue(res_db, i, 2));

		tmp_conn = pgut_connect(dbname);
		res = pgut_execute(tmp_conn, "SELECT pg_ptrack_clear()", 0, NULL, true);

		sprintf(params[0], "%i", dbOid);
		sprintf(params[1], "%i", tblspcOid);
		res = pgut_execute(tmp_conn, "SELECT pg_ptrack_get_and_clear_db($1, $2)",
						   2, (const char **)params, true);
		PQclear(res);

		pgut_disconnect(tmp_conn);
	}

	pfree(params[0]);
	pfree(params[1]);
	PQclear(res_db);
}

static bool
pg_ptrack_get_and_clear_db(Oid dbOid, Oid tblspcOid)
{
	char	   *params[2];
	char	   *dbname;
	PGresult   *res_db;
	PGresult   *res;
	char	   *result;

	params[0] = palloc(64);
	params[1] = palloc(64);

	sprintf(params[0], "%i", dbOid);
	res_db = pgut_execute(backup_conn,
							"SELECT datname FROM pg_database WHERE oid=$1",
							1, (const char **) params, true);
	/*
	 * If database is not found, it's not an error.
	 * It could have been deleted since previous backup.
	 */
	if (PQntuples(res_db) != 1 || PQnfields(res_db) != 1)
		return false;

	dbname = PQgetvalue(res_db, 0, 0);

	/* Always backup all files from template0 database */
	if (strcmp(dbname, "template0") == 0)
	{
		PQclear(res_db);
		return true;
	}
	PQclear(res_db);

	sprintf(params[0], "%i", dbOid);
	sprintf(params[1], "%i", tblspcOid);
	res = pgut_execute(backup_conn, "SELECT pg_ptrack_get_and_clear_db($1, $2)",
						2, (const char **)params, true);

	if (PQnfields(res) != 1)
		elog(ERROR, "cannot perform pg_ptrack_get_and_clear_db()");
	
	result = PQgetvalue(res, 0, 0);
	PQclear(res);
	pfree(params[0]);
	pfree(params[1]);

	return (strcmp(result, "t") == 0);
}

/* Read and clear ptrack files of the target relation.
 * Result is a bytea ptrack map of all segments of the target relation.
 * case 1: we know a tablespace_oid, db_oid, and rel_filenode
 * case 2: we know db_oid and rel_filenode (no tablespace_oid, because file in pg_default)
 * case 3: we know only rel_filenode (because file in pg_global)
 */
static char *
pg_ptrack_get_and_clear(Oid tablespace_oid, Oid db_oid, Oid rel_filenode,
						size_t *result_size)
{
	PGconn	   *tmp_conn;
	PGresult   *res_db,
			   *res;
	char	   *params[2];
	char	   *result;
	char	   *val;

	params[0] = palloc(64);
	params[1] = palloc(64);

	/* regular file (not in directory 'global') */
	if (db_oid != 0)
	{
		char	   *dbname;

		sprintf(params[0], "%i", db_oid);
		res_db = pgut_execute(backup_conn,
							  "SELECT datname FROM pg_database WHERE oid=$1",
							  1, (const char **) params, true);
		/*
		 * If database is not found, it's not an error.
		 * It could have been deleted since previous backup.
		 */
		if (PQntuples(res_db) != 1 || PQnfields(res_db) != 1)
			return NULL;

		dbname = PQgetvalue(res_db, 0, 0);

		if (strcmp(dbname, "template0") == 0)
		{
			PQclear(res_db);
			return NULL;
		}

		tmp_conn = pgut_connect(dbname);
		sprintf(params[0], "%i", tablespace_oid);
		sprintf(params[1], "%i", rel_filenode);
		res = pgut_execute(tmp_conn, "SELECT pg_ptrack_get_and_clear($1, $2)",
						   2, (const char **)params, true);

		if (PQnfields(res) != 1)
			elog(ERROR, "cannot get ptrack file from database \"%s\" by tablespace oid %u and relation oid %u",
				 dbname, tablespace_oid, rel_filenode);
		PQclear(res_db);
		pgut_disconnect(tmp_conn);
	}
	/* file in directory 'global' */
	else
	{
		/*
		 * execute ptrack_get_and_clear for relation in pg_global
		 * Use backup_conn, cause we can do it from any database.
		 */
		sprintf(params[0], "%i", tablespace_oid);
		sprintf(params[1], "%i", rel_filenode);
		res = pgut_execute(backup_conn, "SELECT pg_ptrack_get_and_clear($1, $2)",
						   2, (const char **)params, true);

		if (PQnfields(res) != 1)
			elog(ERROR, "cannot get ptrack file from pg_global tablespace and relation oid %u",
			 rel_filenode);
	}

	val = PQgetvalue(res, 0, 0);

	/* TODO Now pg_ptrack_get_and_clear() returns bytea ending with \x.
	 * It should be fixed in future ptrack releases, but till then we
	 * can parse it.
	 */
	if (strcmp("x", val+1) == 0)
	{
		/* Ptrack file is missing */
		return NULL;
	}

	result = (char *) PQunescapeBytea((unsigned char *) PQgetvalue(res, 0, 0),
									  result_size);
	PQclear(res);
	pfree(params[0]);
	pfree(params[1]);

	return result;
}

/*
 * Wait for target 'lsn'.
 *
 * If current backup started in archive mode wait for 'lsn' to be archived in
 * archive 'wal' directory with WAL segment file.
 * If current backup started in stream mode wait for 'lsn' to be streamed in
 * 'pg_wal' directory.
 *
 * If 'wait_prev_segment' wait for previous segment.
 */
static void
wait_wal_lsn(XLogRecPtr lsn, bool wait_prev_segment)
{
	TimeLineID	tli;
	XLogSegNo	targetSegNo;
	char		wal_dir[MAXPGPATH],
				wal_segment_path[MAXPGPATH];
	char		wal_segment[MAXFNAMELEN];
	bool		file_exists = false;
	uint32		try_count = 0,
				timeout;

#ifdef HAVE_LIBZ
	char		gz_wal_segment_path[MAXPGPATH];
#endif

	tli = get_current_timeline(false);

	/* Compute the name of the WAL file containig requested LSN */
	XLByteToSeg(lsn, targetSegNo);
	if (wait_prev_segment)
		targetSegNo--;
	XLogFileName(wal_segment, tli, targetSegNo);

	if (stream_wal)
	{
		pgBackupGetPath2(&current, wal_dir, lengthof(wal_dir),
						 DATABASE_DIR, PG_XLOG_DIR);
		join_path_components(wal_segment_path, wal_dir, wal_segment);

		timeout = (uint32) checkpoint_timeout();
		timeout = timeout + timeout * 0.1;
	}
	else
	{
		join_path_components(wal_segment_path, arclog_path, wal_segment);
		timeout = archive_timeout;
	}

	if (wait_prev_segment)
		elog(LOG, "Looking for segment: %s", wal_segment);
	else
		elog(LOG, "Looking for LSN: %X/%X in segment: %s", (uint32) (lsn >> 32), (uint32) lsn, wal_segment);

#ifdef HAVE_LIBZ
	snprintf(gz_wal_segment_path, sizeof(gz_wal_segment_path), "%s.gz",
			 wal_segment_path);
#endif

	/* Wait until target LSN is archived or streamed */
	while (true)
	{
		if (!file_exists)
		{
			file_exists = fileExists(wal_segment_path);

			/* Try to find compressed WAL file */
			if (!file_exists)
			{
#ifdef HAVE_LIBZ
				file_exists = fileExists(gz_wal_segment_path);
				if (file_exists)
					elog(LOG, "Found compressed WAL segment: %s", wal_segment_path);
#endif
			}
			else
				elog(LOG, "Found WAL segment: %s", wal_segment_path);
		}

		if (file_exists)
		{
			/* Do not check LSN for previous WAL segment */
			if (wait_prev_segment)
				return;

			/*
			 * A WAL segment found. Check LSN on it.
			 */
			if ((stream_wal && wal_contains_lsn(wal_dir, lsn, tli)) ||
				(!stream_wal && wal_contains_lsn(arclog_path, lsn, tli)))
				/* Target LSN was found */
			{
				elog(LOG, "Found LSN: %X/%X", (uint32) (lsn >> 32), (uint32) lsn);
				return;
			}
		}

		sleep(1);
		if (interrupted)
			elog(ERROR, "Interrupted during waiting for WAL archiving");
		try_count++;

		/* Inform user if WAL segment is absent in first attempt */
		if (try_count == 1)
		{
			if (wait_prev_segment)
				elog(INFO, "Wait for WAL segment %s to be archived",
					 wal_segment_path);
			else
				elog(INFO, "Wait for LSN %X/%X in archived WAL segment %s",
					 (uint32) (lsn >> 32), (uint32) lsn, wal_segment_path);
		}

		if (timeout > 0 && try_count > timeout)
		{
			if (file_exists)
				elog(ERROR, "WAL segment %s was archived, "
					 "but target LSN %X/%X could not be archived in %d seconds",
					 wal_segment, (uint32) (lsn >> 32), (uint32) lsn, timeout);
			/* If WAL segment doesn't exist or we wait for previous segment */
			else
				elog(ERROR,
					 "Switched WAL segment %s could not be archived in %d seconds",
					 wal_segment, timeout);
		}
	}
}

/*
 * Wait for target 'lsn' on replica instance from master.
 */
static void
wait_replica_wal_lsn(XLogRecPtr lsn, bool is_start_backup)
{
	uint32		try_count = 0;

	Assert(from_replica);

	while (true)
	{
		PGresult   *res;
		uint32		xlogid;
		uint32		xrecoff;
		XLogRecPtr	replica_lsn;

		/*
		 * For lsn from pg_start_backup() we need it to be replayed on replica's
		 * data.
		 */
		if (is_start_backup)
		{
			if (server_version >= 100000)
				res = pgut_execute(backup_conn, "SELECT pg_last_wal_replay_lsn()",
								   0, NULL, true);
			else
				res = pgut_execute(backup_conn, "SELECT pg_last_xlog_replay_location()",
								   0, NULL, true);
		}
		/*
		 * For lsn from pg_stop_backup() we need it only to be received by
		 * replica and fsync()'ed on WAL segment.
		 */
		else
		{
			if (server_version >= 100000)
				res = pgut_execute(backup_conn, "SELECT pg_last_wal_receive_lsn()",
								   0, NULL, true);
			else
				res = pgut_execute(backup_conn, "SELECT pg_last_xlog_receive_location()",
								   0, NULL, true);
		}

		/* Extract timeline and LSN from result */
		XLogDataFromLSN(PQgetvalue(res, 0, 0), &xlogid, &xrecoff);
		/* Calculate LSN */
		replica_lsn = (XLogRecPtr) ((uint64) xlogid << 32) | xrecoff;
		PQclear(res);

		/* target lsn was replicated */
		if (replica_lsn >= lsn)
			break;

		sleep(1);
		if (interrupted)
			elog(ERROR, "Interrupted during waiting for target LSN");
		try_count++;

		/* Inform user if target lsn is absent in first attempt */
		if (try_count == 1)
			elog(INFO, "Wait for target LSN %X/%X to be received by replica",
				 (uint32) (lsn >> 32), (uint32) lsn);

		if (replica_timeout > 0 && try_count > replica_timeout)
			elog(ERROR, "Target LSN %X/%X could not be recevied by replica "
				 "in %d seconds",
				 (uint32) (lsn >> 32), (uint32) lsn,
				 replica_timeout);
	}
}

/*
 * Notify end of backup to PostgreSQL server.
 */
static void
pg_stop_backup(pgBackup *backup)
{
	PGconn		*conn;
	PGresult	*res;
	PGresult	*tablespace_map_content = NULL;
	uint32		xlogid;
	uint32		xrecoff;
	XLogRecPtr	restore_lsn = InvalidXLogRecPtr;
	int 		pg_stop_backup_timeout = 0;
	char		path[MAXPGPATH];
	char		backup_label[MAXPGPATH];
	FILE		*fp;
	pgFile		*file;
	size_t		len;
	char		*val = NULL;

	/*
	 * We will use this values if there are no transactions between start_lsn
	 * and stop_lsn.
	 */
	time_t		recovery_time;
	TransactionId recovery_xid;

	if (!backup_in_progress)
		elog(FATAL, "backup is not in progress");

	/* For replica we call pg_stop_backup() on master */
	conn = (from_replica) ? master_conn : backup_conn;

	/* Remove annoying NOTICE messages generated by backend */
	res = pgut_execute(conn, "SET client_min_messages = warning;",
					   0, NULL, true);
	PQclear(res);

	/* Create restore point */
	if (backup != NULL)
	{
		const char *params[1];
		char		name[1024];

		if (!from_replica)
			snprintf(name, lengthof(name), "pg_probackup, backup_id %s",
					 base36enc(backup->start_time));
		else
			snprintf(name, lengthof(name), "pg_probackup, backup_id %s. Replica Backup",
					 base36enc(backup->start_time));
		params[0] = name;

		res = pgut_execute(conn, "SELECT pg_create_restore_point($1)",
						   1, params, true);
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
			 */
			sent = pgut_send(conn,
								"SELECT"
								" txid_snapshot_xmax(txid_current_snapshot()),"
								" current_timestamp(0)::timestamptz,"
								" lsn,"
								" labelfile,"
								" spcmapfile"
								" FROM pg_stop_backup(false)",
								0, NULL, WARNING);
		}
		else
		{

			sent = pgut_send(conn,
								"SELECT"
								" txid_snapshot_xmax(txid_current_snapshot()),"
								" current_timestamp(0)::timestamptz,"
								" pg_stop_backup() as lsn",
								0, NULL, WARNING);
		}
		pg_stop_backup_is_sent = true;
		if (!sent)
			elog(ERROR, "Failed to send pg_stop_backup query");
	}

	/*
	 * Wait for the result of pg_stop_backup(),
	 * but no longer than PG_STOP_BACKUP_TIMEOUT seconds
	 */
	if (pg_stop_backup_is_sent && !in_cleanup)
	{
		while (1)
		{
			if (!PQconsumeInput(conn) || PQisBusy(conn))
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
				 * If postgres haven't answered in PG_STOP_BACKUP_TIMEOUT seconds,
				 * send an interrupt.
				 */
				if (pg_stop_backup_timeout > PG_STOP_BACKUP_TIMEOUT)
				{
					pgut_cancel(conn);
					elog(ERROR, "pg_stop_backup doesn't answer in %d seconds, cancel it",
						 PG_STOP_BACKUP_TIMEOUT);
				}
			}
			else
			{
				res = PQgetResult(conn);
				break;
			}
		}
		if (!res)
			elog(ERROR, "pg_stop backup() failed");
		else
			elog(INFO, "pg_stop backup() successfully executed");

		backup_in_progress = false;

		/* Extract timeline and LSN from results of pg_stop_backup() */
		XLogDataFromLSN(PQgetvalue(res, 0, 2), &xlogid, &xrecoff);
		/* Calculate LSN */
		stop_backup_lsn = (XLogRecPtr) ((uint64) xlogid << 32) | xrecoff;

		if (!XRecOffIsValid(stop_backup_lsn))
		{
			stop_backup_lsn = restore_lsn;
		}

		if (!XRecOffIsValid(stop_backup_lsn))
			elog(ERROR, "Invalid stop_backup_lsn value %X/%X",
				 (uint32) (stop_backup_lsn >> 32), (uint32) (stop_backup_lsn));

		/* Write backup_label and tablespace_map */
		if (!exclusive_backup)
		{
			Assert(PQnfields(res) >= 4);
			pgBackupGetPath(&current, path, lengthof(path), DATABASE_DIR);

			/* Write backup_label */
			join_path_components(backup_label, path, PG_BACKUP_LABEL_FILE);
			fp = fopen(backup_label, "w");
			if (fp == NULL)
				elog(ERROR, "can't open backup label file \"%s\": %s",
					 backup_label, strerror(errno));

			len = strlen(PQgetvalue(res, 0, 3));
			if (fwrite(PQgetvalue(res, 0, 3), 1, len, fp) != len ||
				fflush(fp) != 0 ||
				fsync(fileno(fp)) != 0 ||
				fclose(fp))
				elog(ERROR, "can't write backup label file \"%s\": %s",
					 backup_label, strerror(errno));

			/*
			 * It's vital to check if backup_files_list is initialized,
			 * because we could get here because the backup was interrupted
			 */
			if (backup_files_list)
			{
				file = pgFileNew(backup_label, true);
				calc_file_checksum(file);
				free(file->path);
				file->path = strdup(PG_BACKUP_LABEL_FILE);
				parray_append(backup_files_list, file);
			}
		}

		if (sscanf(PQgetvalue(res, 0, 0), XID_FMT, &recovery_xid) != 1)
			elog(ERROR,
				 "result of txid_snapshot_xmax() is invalid: %s",
				 PQerrorMessage(conn));
		if (!parse_time(PQgetvalue(res, 0, 1), &recovery_time))
			elog(ERROR,
				 "result of current_timestamp is invalid: %s",
				 PQerrorMessage(conn));

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
			fp = fopen(tablespace_map, "w");
			if (fp == NULL)
				elog(ERROR, "can't open tablespace map file \"%s\": %s",
					 tablespace_map, strerror(errno));

			len = strlen(val);
			if (fwrite(val, 1, len, fp) != len ||
				fflush(fp) != 0 ||
				fsync(fileno(fp)) != 0 ||
				fclose(fp))
				elog(ERROR, "can't write tablespace map file \"%s\": %s",
					 tablespace_map, strerror(errno));

			if (backup_files_list)
			{
				file = pgFileNew(tablespace_map, true);
				if (S_ISREG(file->mode))
					calc_file_checksum(file);
				free(file->path);
				file->path = strdup(PG_TABLESPACE_MAP_FILE);
				parray_append(backup_files_list, file);
			}
		}

		if (tablespace_map_content)
			PQclear(tablespace_map_content);
		PQclear(res);

		if (stream_wal)
			/* Wait for the completion of stream */
			pthread_join(stream_thread, NULL);
	}

	/* Fill in fields if that is the correct end of backup. */
	if (backup != NULL)
	{
		char	   *xlog_path,
					stream_xlog_path[MAXPGPATH];

		/* Wait for stop_lsn to be received by replica */
		if (from_replica)
			wait_replica_wal_lsn(stop_backup_lsn, false);
		/*
		 * Wait for stop_lsn to be archived or streamed.
		 * We wait for stop_lsn in stream mode just in case.
		 */
		wait_wal_lsn(stop_backup_lsn, false);

		if (stream_wal)
		{
			pgBackupGetPath2(backup, stream_xlog_path,
							 lengthof(stream_xlog_path),
							 DATABASE_DIR, PG_XLOG_DIR);
			xlog_path = stream_xlog_path;
		}
		else
			xlog_path = arclog_path;

		backup->tli = get_current_timeline(false);
		backup->stop_lsn = stop_backup_lsn;

		elog(LOG, "Getting the Recovery Time from WAL");

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
 * Retreive checkpoint_timeout GUC value in seconds.
 */
static int
checkpoint_timeout(void)
{
	PGresult   *res;
	const char *val;
	const char *hintmsg;
	int			val_int;

	res = pgut_execute(backup_conn, "show checkpoint_timeout", 0, NULL, true);
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
		elog(INFO, "Backup %s is running, setting its status to ERROR",
			 base36enc(current.start_time));
		current.end_time = time(NULL);
		current.status = BACKUP_STATUS_ERROR;
		pgBackupWriteBackupControlFile(&current);
	}

	/*
	 * If backup is in progress, notify stop of backup to PostgreSQL
	 */
	if (backup_in_progress)
	{
		elog(LOG, "backup in progress, stop backup");
		pg_stop_backup(NULL);	/* don't care stop_lsn on error case */
	}
}

/*
 * Disconnect backup connection during quit pg_probackup.
 */
static void
backup_disconnect(bool fatal, void *userdata)
{
	pgut_disconnect(backup_conn);
	if (master_conn)
		pgut_disconnect(master_conn);
}

/*
 * Take a backup of the PGDATA at a file level.
 * Copy all directories and files listed in backup_files_list.
 * If the file is 'datafile' (regular relation's main fork), read it page by page,
 * verify checksum and copy.
 * In incremental backup mode, copy only files or datafiles' pages changed after
 * previous backup.
 */
static void
backup_files(void *arg)
{
	int				i;
	backup_files_args *arguments = (backup_files_args *) arg;
	int n_backup_files_list = parray_num(arguments->backup_files_list);

	/* backup a file */
	for (i = 0; i < n_backup_files_list; i++)
	{
		int			ret;
		struct stat	buf;

		pgFile *file = (pgFile *) parray_get(arguments->backup_files_list, i);
		elog(VERBOSE, "Copying file:  \"%s\" ", file->path);
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
			/* copy the file into backup */
			if (file->is_datafile && !file->is_cfs)
			{
				/* backup block by block if datafile AND not compressed by cfs*/
				if (!backup_data_file(arguments,
									  arguments->from_root,
									  arguments->to_root, file,
									  arguments->prev_backup_start_lsn,
									  current.backup_mode))
				{
					file->write_size = BYTES_INVALID;
					elog(VERBOSE, "File \"%s\" was not copied to backup", file->path);
					continue;
				}
			}
			else if (!copy_file(arguments->from_root,
							   arguments->to_root,
							   file))
			{
				file->write_size = BYTES_INVALID;
				elog(VERBOSE, "File \"%s\" was not copied to backup", file->path);
				continue;
			}

			elog(VERBOSE, "File \"%s\". Copied %lu bytes",
				 file->path, (unsigned long) file->write_size);
		}
		else
			elog(LOG, "unexpected file type %d", buf.st_mode);
	}

	/* Close connection */
	if (arguments->thread_backup_conn)
		pgut_disconnect(arguments->thread_backup_conn);

}

/*
 * Extract information about files in backup_list parsing their names:
 * - remove temp tables from the list
 * - remove unlogged tables from the list (leave the _init fork)
 * - set flags for database directories
 * - set flags for datafiles
 */
static void
parse_backup_filelist_filenames(parray *files, const char *root)
{
	size_t		i;
	Oid unlogged_file_reloid = 0;

	for (i = 0; i < parray_num(files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);
		char	   *relative;
		char		filename[MAXPGPATH];
		int 		sscanf_result;

		relative = GetRelativePath(file->path, root);
		filename[0] = '\0';

		elog(VERBOSE, "-----------------------------------------------------: %s", relative);
		if (path_is_prefix_of_path("global", relative))
		{
			file->tblspcOid = GLOBALTABLESPACE_OID;
			sscanf_result = sscanf(relative, "global/%s", filename);
			elog(VERBOSE, "global sscanf result: %i", sscanf_result);
			if (strcmp(relative, "global") == 0)
			{
				Assert(S_ISDIR(file->mode));
				elog(VERBOSE, "the global itself, filepath %s", relative);
				file->is_database = true; /* TODO add an explanation */
			}
			else if (sscanf_result == 1)
			{
				elog(VERBOSE, "filename %s, filepath %s", filename, relative);
			}
		}
		else if (path_is_prefix_of_path("base", relative))
		{
			file->tblspcOid = DEFAULTTABLESPACE_OID;
			sscanf_result = sscanf(relative, "base/%u/%s", &(file->dbOid), filename);
			elog(VERBOSE, "base sscanf result: %i", sscanf_result);
			if (strcmp(relative, "base") == 0)
			{
				Assert(S_ISDIR(file->mode));
				elog(VERBOSE, "the base itself, filepath %s", relative);
			}
			else if (sscanf_result == 1)
			{
				Assert(S_ISDIR(file->mode));
				elog(VERBOSE, "dboid %u, filepath %s", file->dbOid, relative);
				file->is_database = true;
			}
			else
			{
				elog(VERBOSE, "dboid %u, filename %s, filepath %s", file->dbOid, filename, relative);
			}
		}
		else if (path_is_prefix_of_path(PG_TBLSPC_DIR, relative))
		{
			char		temp_relative_path[MAXPGPATH];

			sscanf_result = sscanf(relative, "pg_tblspc/%u/%s", &(file->tblspcOid), temp_relative_path);
			elog(VERBOSE, "pg_tblspc sscanf result: %i", sscanf_result);

			if (strcmp(relative, "pg_tblspc") == 0)
			{
				Assert(S_ISDIR(file->mode));
				elog(VERBOSE, "the pg_tblspc itself, filepath %s", relative);
			}
			else if (sscanf_result == 1)
			{
				Assert(S_ISDIR(file->mode));
				elog(VERBOSE, "tblspcOid %u, filepath %s", file->tblspcOid, relative);
			}
			else
			{
				/*continue parsing */
				sscanf_result = sscanf(temp_relative_path+strlen(TABLESPACE_VERSION_DIRECTORY)+1, "%u/%s",
									   &(file->dbOid), filename);
				elog(VERBOSE, "TABLESPACE_VERSION_DIRECTORY sscanf result: %i", sscanf_result);

				if (sscanf_result == -1)
				{
					elog(VERBOSE, "The TABLESPACE_VERSION_DIRECTORY itself, filepath %s", relative);
				}
				else if (sscanf_result == 0)
				{
					/* Found file in pg_tblspc/tblsOid/TABLESPACE_VERSION_DIRECTORY
					   Legal only in case of 'pg_compression'
					 */
					if (strcmp(file->name, "pg_compression") == 0)
					{
						elog(VERBOSE, "Found pg_compression file in TABLESPACE_VERSION_DIRECTORY, filepath %s", relative);
						/*Set every datafile in tablespace as is_cfs */
						set_cfs_datafiles(files, root, relative, i);
					}
					else
					{
						elog(VERBOSE, "Found illegal file in TABLESPACE_VERSION_DIRECTORY, filepath %s", relative);
					}

				}
				else if (sscanf_result == 1)
				{
					Assert(S_ISDIR(file->mode));
					elog(VERBOSE, "dboid %u, filepath %s", file->dbOid, relative);
					file->is_database = true;
				}
				else if (sscanf_result == 2)
				{
					elog(VERBOSE, "dboid %u, filename %s, filepath %s", file->dbOid, filename, relative);
				}
				else
				{
					elog(VERBOSE, "Illegal file filepath %s", relative);
				}
			}
		}
		else
		{
			elog(VERBOSE,"other dir or file, filepath %s", relative);
		}

		if (strcmp(filename, "ptrack_init") == 0)
		{
			/* Do not backup ptrack_init files */
			pgFileFree(file);
			parray_remove(files, i);
			i--;
			continue;
		}

		/* Check files located inside database directories */
		if (filename[0] != '\0' && file->dbOid != 0)
		{
			if (strcmp(filename, "pg_internal.init") == 0)
			{
				/* Do not pg_internal.init files
				 * (they contain some cache entries, so it's fine) */
				pgFileFree(file);
				parray_remove(files, i);
				i--;
				continue;
			}
			else if (filename[0] == 't' && isdigit(filename[1]))
			{
				elog(VERBOSE, "temp file, filepath %s", relative);
				/* Do not backup temp files */
				pgFileFree(file);
				parray_remove(files, i);
				i--;
				continue;
			}
			else if (isdigit(filename[0]))
			{
				/*
				 * TODO TODO TODO Files of this type can be compressed by cfs.
				 * Check that and do not mark them with 'is_datafile' flag.
				 */
				char *forkNameptr;
				char suffix[MAXPGPATH];

				forkNameptr = strstr(filename, "_");
				if (forkNameptr != NULL)
				{
					/* auxiliary fork of the relfile */
					sscanf(filename, "%u_%s", &(file->relOid), file->forkName);
					elog(VERBOSE, "relOid %u, forkName %s, filepath %s", file->relOid, file->forkName, relative);

					/* handle unlogged relations */
					if (strcmp(file->forkName, "init") == 0)
					{
						/*
						 * Do not backup files of unlogged relations.
						 * scan filelist backward and exclude these files.
						 */
						int unlogged_file_num = i-1;
						pgFile	   *unlogged_file = (pgFile *) parray_get(files, unlogged_file_num);

						unlogged_file_reloid = file->relOid;

						while (unlogged_file_num >= 0 &&
							   (unlogged_file_reloid != 0) &&
							   (unlogged_file->relOid == unlogged_file_reloid))
						{
							unlogged_file->size = 0;
							pgFileFree(unlogged_file);
							parray_remove(files, unlogged_file_num);
							unlogged_file_num--;
							i--;
							unlogged_file = (pgFile *) parray_get(files, unlogged_file_num);
						}
					}
					else if (strcmp(file->forkName, "ptrack") == 0)
					{
						/* Do not backup ptrack files */
						pgFileFree(file);
						parray_remove(files, i);
						i--;
					}
					else if ((unlogged_file_reloid != 0) &&
							 (file->relOid == unlogged_file_reloid))
					{
						/* Do not backup forks of unlogged relations */
						pgFileFree(file);
						parray_remove(files, i);
						i--;
					}
					continue;
				}

				sscanf_result = sscanf(filename, "%u.%d.%s", &(file->relOid), &(file->segno), suffix);
				if (sscanf_result == 0)
				{
					elog(ERROR, "cannot parse filepath %s", relative);
				}
				else if (sscanf_result == 1)
				{
					/* first segment of the relfile */
					elog(VERBOSE, "relOid %u, segno %d, filepath %s", file->relOid, 0, relative);
					if (strcmp(relative + strlen(relative) - strlen("cfm"), "cfm") == 0)
					{
						/* reloid.cfm */
						elog(VERBOSE, "Found cfm file %s", relative);
					}
					else
					{
						elog(VERBOSE, "Found first segment of the relfile %s", relative);
						file->is_datafile = true;
					}
				}
				else if (sscanf_result == 2)
				{
					/* not first segment of the relfile */
					elog(VERBOSE, "relOid %u, segno %d, filepath %s", file->relOid, file->segno, relative);
					file->is_datafile = true;
				}
				else
				{
					/*
					 * some cfs specific file.
					 * It is not datafile, because we cannot read it block by block
					 */
					elog(VERBOSE, "relOid %u, segno %d, suffix %s, filepath %s", file->relOid, file->segno, suffix, relative);
				}

				if ((unlogged_file_reloid != 0) &&
					(file->relOid == unlogged_file_reloid))
				{
					/* Do not backup segments of unlogged files */
					pgFileFree(file);
					parray_remove(files, i);
					i--;
				}
			}
		}
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
	char	   *relative_prev_file;

	cfs_tblspc_path = strdup(relative);
	if(!cfs_tblspc_path)
		elog(ERROR, "Out of memory");
	len = strlen("/pg_compression");
	cfs_tblspc_path[strlen(cfs_tblspc_path) - len] = 0;
	elog(VERBOSE, "CFS DIRECTORY %s, pg_compression path: %s", cfs_tblspc_path, relative);

	for (p = (int) i; p >= 0; p--)
	{
		prev_file = (pgFile *) parray_get(files, (size_t) p);
		relative_prev_file = GetRelativePath(prev_file->path, root);

		elog(VERBOSE, "Checking file in cfs tablespace %s", relative_prev_file);

		if (strstr(relative_prev_file, cfs_tblspc_path) != NULL)
		{
			if (S_ISREG(prev_file->mode) && prev_file->is_datafile)
			{
				elog(VERBOSE, "Setting 'is_cfs' on file %s, name %s",
					relative_prev_file, prev_file->name);
				prev_file->is_cfs = true;
			}
		}
		else
		{
			elog(VERBOSE, "Breaking on %s", relative_prev_file);
			break;
		}
	}
	free(cfs_tblspc_path);
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

	if (fflush(fp) != 0 ||
		fsync(fileno(fp)) != 0 ||
		fclose(fp))
		elog(ERROR, "cannot write file list \"%s\": %s", path, strerror(errno));
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

/*
 * Given a list of files in the instance to backup, build a pagemap for each
 * data file that has ptrack. Result is saved in the pagemap field of pgFile.
 * NOTE we rely on the fact that provided parray is sorted by file->path.
 */
static void
make_pagemap_from_ptrack(parray *files)
{
	size_t		i;
	Oid dbOid_with_ptrack_init = 0;
	Oid tblspcOid_with_ptrack_init = 0;
	char	   *ptrack_nonparsed = NULL;
	size_t		ptrack_nonparsed_size = 0;

	elog(LOG, "Compiling pagemap");
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);
		size_t		start_addr;

		/*
		 * If there is a ptrack_init file in the database,
		 * we must backup all its files, ignoring ptrack files for relations.
		 */
		if (file->is_database)
		{
			char *filename = strrchr(file->path, '/');

			Assert(filename != NULL);
			filename++;

			/*
			 * The function pg_ptrack_get_and_clear_db returns true
			 * if there was a ptrack_init file.
			 * Also ignore ptrack files for global tablespace,
			 * to avoid any possible specific errors.
			 */
			if ((file->tblspcOid == GLOBALTABLESPACE_OID) ||
				pg_ptrack_get_and_clear_db(file->dbOid, file->tblspcOid))
			{
				dbOid_with_ptrack_init = file->dbOid;
				tblspcOid_with_ptrack_init = file->tblspcOid;
			}
		}

		if (file->is_datafile)
		{
			if (file->tblspcOid == tblspcOid_with_ptrack_init
					&& file->dbOid == dbOid_with_ptrack_init)
			{
				/* ignore ptrack if ptrack_init exists */
				elog(VERBOSE, "Ignoring ptrack because of ptrack_init for file: %s", file->path);
				file->pagemap.bitmapsize = PageBitmapIsAbsent;
				continue;
			}

			/* get ptrack bitmap once for all segments of the file */
			if (file->segno == 0)
			{
				/* release previous value */
				pg_free(ptrack_nonparsed);
				ptrack_nonparsed_size = 0;

				ptrack_nonparsed = pg_ptrack_get_and_clear(file->tblspcOid, file->dbOid,
											   file->relOid, &ptrack_nonparsed_size);
			}

			if (ptrack_nonparsed != NULL)
			{
				/*
				 * pg_ptrack_get_and_clear() returns ptrack with VARHDR cutted out.
				 * Compute the beginning of the ptrack map related to this segment
				 *
				 * HEAPBLOCKS_PER_BYTE. Number of heap pages one ptrack byte can track: 8
				 * RELSEG_SIZE. Number of Pages per segment: 131072
				 * RELSEG_SIZE/HEAPBLOCKS_PER_BYTE. number of bytes in ptrack file needed
				 * to keep track on one relsegment: 16384
				 */
				start_addr = (RELSEG_SIZE/HEAPBLOCKS_PER_BYTE)*file->segno;

				if (start_addr + RELSEG_SIZE/HEAPBLOCKS_PER_BYTE > ptrack_nonparsed_size)
				{
					file->pagemap.bitmapsize = ptrack_nonparsed_size - start_addr;
					elog(VERBOSE, "pagemap size: %i", file->pagemap.bitmapsize);
				}
				else
				{
					file->pagemap.bitmapsize =	RELSEG_SIZE/HEAPBLOCKS_PER_BYTE;
					elog(VERBOSE, "pagemap size: %i", file->pagemap.bitmapsize);
				}

				file->pagemap.bitmap = pg_malloc(file->pagemap.bitmapsize);
				memcpy(file->pagemap.bitmap, ptrack_nonparsed+start_addr, file->pagemap.bitmapsize);
			}
			else
			{
				/*
				 * If ptrack file is missing, try to copy the entire file.
				 * It can happen in two cases:
				 * - files were created by commands that bypass buffer manager
				 * and, correspondingly, ptrack mechanism.
				 * i.e. CREATE DATABASE
				 * - target relation was deleted.
				 */
				elog(VERBOSE, "Ptrack is missing for file: %s", file->path);
				file->pagemap.bitmapsize = PageBitmapIsAbsent;
			}
		}
	}
	elog(LOG, "Pagemap compiled");
//	res = pgut_execute(backup_conn, "SET client_min_messages = warning;", 0, NULL, true);
//	PQclear(pgut_execute(backup_conn, "CHECKPOINT;", 0, NULL, true));
}


/*
 * Stop WAL streaming if current 'xlogpos' exceeds 'stop_backup_lsn', which is
 * set by pg_stop_backup().
 */
static bool
stop_streaming(XLogRecPtr xlogpos, uint32 timeline, bool segment_finished)
{
	static uint32 prevtimeline = 0;
	static XLogRecPtr prevpos = InvalidXLogRecPtr;

	/* we assume that we get called once at the end of each segment */
	if (segment_finished)
		elog(LOG, _("finished segment at %X/%X (timeline %u)\n"),
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
		if (xlogpos > stop_backup_lsn)
			return true;

		/* pg_stop_backup() was executed, wait for the completion of stream */
		if (stream_stop_timeout == 0)
		{
			elog(INFO, "Wait for LSN %X/%X to be streamed",
				 (uint32) (stop_backup_lsn >> 32), (uint32) stop_backup_lsn);

			stream_stop_timeout = checkpoint_timeout();
			stream_stop_timeout = stream_stop_timeout + stream_stop_timeout * 0.1;

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
static void
StreamLog(void *arg)
{
	XLogRecPtr	startpos;
	TimeLineID	starttli;
	char	   *basedir = (char *)arg;

	/*
	 * Connect in replication mode to the server
	 */
	if (conn == NULL)
		conn = pgut_connect_replication(pgut_dbname);
	if (!conn)
	{
		pthread_mutex_unlock(&start_stream_mut);
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
	pthread_mutex_unlock(&start_stream_mut);

	/*
	 * We must use startpos as start_lsn from start_backup
	 */
	startpos = current.start_lsn;

	/*
	 * Always start streaming at the beginning of a segment
	 */
	startpos -= startpos % XLOG_SEG_SIZE;

	/* Initialize timeout */
	stream_stop_timeout = 0;
	stream_stop_begin = 0;

	/*
	 * Start the replication
	 */
	elog(LOG, _("starting log streaming at %X/%X (timeline %u)\n"),
		 (uint32) (startpos >> 32), (uint32) startpos, starttli);

#if PG_VERSION_NUM >= 90600
	{
		StreamCtl	ctl;

		MemSet(&ctl, 0, sizeof(ctl));

		ctl.startpos = startpos;
		ctl.timeline = starttli;
		ctl.sysidentifier = NULL;

#if PG_VERSION_NUM >= 100000
		ctl.walmethod = CreateWalDirectoryMethod(basedir, 0, true);
		ctl.replication_slot = replication_slot;
		ctl.stop_socket = PGINVALID_SOCKET;
#else
		ctl.basedir = basedir;
#endif

		ctl.stream_stop = stop_streaming;
		ctl.standby_message_timeout = standby_message_timeout;
		ctl.partial_suffix = NULL;
		ctl.synchronous = false;
		ctl.mark_done = false;

		if(ReceiveXlogStream(conn, &ctl) == false)
			elog(ERROR, "Problem in receivexlog");

#if PG_VERSION_NUM >= 100000
		if (!ctl.walmethod->finish())
			elog(ERROR, "Could not finish writing WAL files: %s",
				 strerror(errno));
#endif
	}
#else
	if(ReceiveXlogStream(conn, startpos, starttli, NULL, basedir,
					  stop_streaming, standby_message_timeout, NULL,
					  false, false) == false)
		elog(ERROR, "Problem in receivexlog");
#endif

	PQfinish(conn);
	conn = NULL;
}

/*
 * Get lsn of the moment when ptrack was enabled the last time.
 */
static XLogRecPtr
get_last_ptrack_lsn(void)

{
	PGresult   *res;
	uint32		xlogid;
	uint32		xrecoff;
	XLogRecPtr	lsn;

	res = pgut_execute(backup_conn, "select pg_ptrack_control_lsn()", 0, NULL, true);

	/* Extract timeline and LSN from results of pg_start_backup() */
	XLogDataFromLSN(PQgetvalue(res, 0, 0), &xlogid, &xrecoff);
	/* Calculate LSN */
	lsn = (XLogRecPtr) ((uint64) xlogid << 32) | xrecoff;

	PQclear(res);
	return lsn;
}

char *
pg_ptrack_get_block(backup_files_args *arguments,
					Oid dbOid,
					Oid tblsOid,
					Oid relOid,
					BlockNumber blknum,
					size_t *result_size)
{
	PGresult   *res;
	char	   *params[4];
	char	   *result;

	params[0] = palloc(64);
	params[1] = palloc(64);
	params[2] = palloc(64);
	params[3] = palloc(64);

	/*
	 * Use tmp_conn, since we may work in parallel threads.
	 * We can connect to any database.
	 */
	sprintf(params[0], "%i", tblsOid);
	sprintf(params[1], "%i", dbOid);
	sprintf(params[2], "%i", relOid);
	sprintf(params[3], "%u", blknum);

	if (arguments->thread_backup_conn == NULL)
	{
		arguments->thread_backup_conn = pgut_connect(pgut_dbname);
	}

	if (arguments->thread_cancel_conn == NULL)
		arguments->thread_cancel_conn = PQgetCancel(arguments->thread_backup_conn);

	//elog(LOG, "db %i pg_ptrack_get_block(%i, %i, %u)",dbOid, tblsOid, relOid, blknum);
	res = pgut_execute_parallel(arguments->thread_backup_conn,
								arguments->thread_cancel_conn,
					"SELECT pg_ptrack_get_block_2($1, $2, $3, $4)",
					4, (const char **)params, true);

	if (PQnfields(res) != 1)
	{
		elog(VERBOSE, "cannot get file block for relation oid %u",
					   relOid);
		return NULL;
	}

	if (PQgetisnull(res, 0, 0))
	{
		elog(VERBOSE, "cannot get file block for relation oid %u",
				   relOid);
		return NULL;
	}

	result = (char *) PQunescapeBytea((unsigned char *) PQgetvalue(res, 0, 0),
									  result_size);

	PQclear(res);

	pfree(params[0]);
	pfree(params[1]);
	pfree(params[2]);
	pfree(params[3]);

	return result;
}