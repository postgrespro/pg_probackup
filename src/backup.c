/*-------------------------------------------------------------------------
 *
 * backup.c: backup DB cluster, archived WAL
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
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


/*
 * Macro needed to parse ptrack.
 * NOTE Keep those values syncronised with definitions in ptrack.h
 */
#define PTRACK_BITS_PER_HEAPBLOCK 1
#define HEAPBLOCKS_PER_BYTE (BITS_PER_BYTE / PTRACK_BITS_PER_HEAPBLOCK)

static int	standby_message_timeout = 10 * 1000;	/* 10 sec = default */
static XLogRecPtr stop_backup_lsn = InvalidXLogRecPtr;
static XLogRecPtr stop_stream_lsn = InvalidXLogRecPtr;

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
	parray	   *files_list;
} StreamThreadArg;

static pthread_t stream_thread;
static StreamThreadArg stream_thread_arg = {"", NULL, 1, NULL};

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

static void *backup_files(void *arg);
static void *remote_backup_files(void *arg);

static void do_backup_instance(void);

static void pg_start_backup(const char *label, bool smooth, pgBackup *backup);
static void pg_switch_wal(PGconn *conn);
static void pg_stop_backup(pgBackup *backup);
static int checkpoint_timeout(void);

//static void backup_list_file(parray *files, const char *root, )
static void parse_backup_filelist_filenames(parray *files, const char *root);
static XLogRecPtr wait_wal_lsn(XLogRecPtr lsn, bool is_start_lsn,
							   bool wait_prev_segment);
static void wait_replica_wal_lsn(XLogRecPtr lsn, bool is_start_backup);
static void make_pagemap_from_ptrack(parray *files);
static void *StreamLog(void *arg);

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

	backup_conn_replication = pgut_connect_replication(instance_config.pghost,
													   instance_config.pgport,
													   instance_config.pgdatabase,
													   instance_config.pguser);

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
#ifndef WIN32
				pgfile->mode |= S_IFLNK;
#else
				pgfile->mode |= S_IFDIR;
#endif
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
static void
remote_copy_file(PGconn *conn, pgFile* file)
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

	out = fopen(to_path, PG_BINARY_W);
	if (out == NULL)
	{
		int errno_tmp = errno;
		elog(ERROR, "cannot open destination file \"%s\": %s",
			to_path, strerror(errno_tmp));
	}

	INIT_FILE_CRC32(true, file->crc);

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
			COMP_FILE_CRC32(true, file->crc, buf, write_buffer_size);

			/* TODO calc checksum*/
			if (fwrite(buf, 1, write_buffer_size, out) != write_buffer_size)
			{
				errno_tmp = errno;
				/* oops */
				FIN_FILE_CRC32(true, file->crc);
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

	file->write_size = (int64) file->read_size;
	FIN_FILE_CRC32(true, file->crc);

	fclose(out);
}

/*
 * Take a remote backup of the PGDATA at a file level.
 * Copy all directories and files listed in backup_files_list.
 */
static void *
remote_backup_files(void *arg)
{
	int			i;
	backup_files_arg *arguments = (backup_files_arg *) arg;
	int			n_backup_files_list = parray_num(arguments->files_list);
	PGconn	   *file_backup_conn = NULL;

	for (i = 0; i < n_backup_files_list; i++)
	{
		char		*query_str;
		PGresult	*res;
		char		*copybuf = NULL;
		pgFile		*file;
		int			row_length;

		file = (pgFile *) parray_get(arguments->files_list, i);

		/* We have already copied all directories */
		if (S_ISDIR(file->mode))
			continue;

		if (!pg_atomic_test_set_flag(&file->lock))
			continue;

		file_backup_conn = pgut_connect_replication(instance_config.pghost,
													instance_config.pgport,
													instance_config.pgdatabase,
													instance_config.pguser);

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

		elog(VERBOSE, "File \"%s\". Copied " INT64_FORMAT " bytes",
			 file->path, file->write_size);
		PQfinish(file_backup_conn);
	}

	/* Data files transferring is successful */
	arguments->ret = 0;

	return NULL;
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

	/* arrays with meta info for multi threaded backup */
	pthread_t	*threads;
	backup_files_arg *threads_args;
	bool		backup_isok = true;

	pgBackup   *prev_backup = NULL;
	parray	   *prev_backup_filelist = NULL;
	parray	   *xlog_files_list = NULL;
	parray	   *backup_list = NULL;
	pgFile	   *pg_control = NULL;

	elog(LOG, "Database backup start");

	/* Initialize size summary */
	current.data_bytes = 0;

	/* Obtain current timeline */
	if (is_remote_backup)
	{
		char	   *sysidentifier;
		TimeLineID	starttli;
		XLogRecPtr	startpos;

		backup_conn_replication = pgut_connect_replication(instance_config.pghost,
														   instance_config.pgport,
														   instance_config.pgdatabase,
														   instance_config.pguser);

		/* Check replication prorocol connection */
		if (!RunIdentifySystem(backup_conn_replication, &sysidentifier,  &starttli, &startpos, NULL))
			elog(ERROR, "Failed to send command for remote backup");

// TODO implement the check
// 		if (&sysidentifier != instance_config.system_identifier)
// 			elog(ERROR, "Backup data directory was initialized for system id %ld, but target backup directory system id is %ld",
// 			instance_config.system_identifier, sysidentifier);

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
		current.backup_mode == BACKUP_MODE_DIFF_PTRACK ||
		current.backup_mode == BACKUP_MODE_DIFF_DELTA)
	{
		char		prev_backup_filelist_path[MAXPGPATH];

		/* get list of backups already taken */
		backup_list = catalog_get_backup_list(INVALID_BACKUP_ID);

		prev_backup = catalog_get_last_data_backup(backup_list, current.tli);
		if (prev_backup == NULL)
			elog(ERROR, "Valid backup on current timeline is not found. "
						"Create new FULL backup before an incremental one.");

		pgBackupGetPath(prev_backup, prev_backup_filelist_path,
						lengthof(prev_backup_filelist_path), DATABASE_FILE_LIST);
		/* Files of previous backup needed by DELTA backup */
		prev_backup_filelist = dir_read_file_list(NULL, prev_backup_filelist_path);

		/* If lsn is not NULL, only pages with higher lsn will be copied. */
		prev_backup_start_lsn = prev_backup->start_lsn;
		current.parent_backup = prev_backup->start_time;

		write_backup(&current);
	}

	/*
	 * It`s illegal to take PTRACK backup if LSN from ptrack_control() is not
	 * equal to stop_lsn of previous backup.
	 */
	if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		XLogRecPtr	ptrack_lsn = get_last_ptrack_lsn();

		if (ptrack_lsn > prev_backup->stop_lsn || ptrack_lsn == InvalidXLogRecPtr)
		{
			elog(ERROR, "LSN from ptrack_control %X/%X differs from STOP LSN of previous backup %X/%X.\n"
						"Create new full backup before an incremental one.",
						(uint32) (ptrack_lsn >> 32), (uint32) (ptrack_lsn),
						(uint32) (prev_backup->stop_lsn >> 32),
						(uint32) (prev_backup->stop_lsn));
		}
	}

	/* Clear ptrack files for FULL and PAGE backup */
	if (current.backup_mode != BACKUP_MODE_DIFF_PTRACK && is_ptrack_enable)
		pg_ptrack_clear();

	/* notify start of backup to PostgreSQL server */
	time2iso(label, lengthof(label), current.start_time);
	strncat(label, " with pg_probackup", lengthof(label) -
			strlen(" with pg_probackup"));
	pg_start_backup(label, smooth_checkpoint, &current);

	pgBackupGetPath(&current, database_path, lengthof(database_path),
					DATABASE_DIR);

	/* start stream replication */
	if (stream_wal)
	{
		join_path_components(dst_backup_path, database_path, PG_XLOG_DIR);
		fio_mkdir(dst_backup_path, DIR_PERMISSION, FIO_BACKUP_HOST);

		stream_thread_arg.basedir = dst_backup_path;

		/*
		 * Connect in replication mode to the server.
		 */
		stream_thread_arg.conn = pgut_connect_replication(instance_config.pghost,
														  instance_config.pgport,
														  instance_config.pgdatabase,
														  instance_config.pguser);

		if (!CheckServerVersionForStreaming(stream_thread_arg.conn))
		{
			PQfinish(stream_thread_arg.conn);
			/*
			 * Error message already written in CheckServerVersionForStreaming().
			 * There's no hope of recovering from a version mismatch, so don't
			 * retry.
			 */
			elog(ERROR, "Cannot continue backup because stream connect has failed.");
		}

		/*
		 * Identify server, obtaining start LSN position and current timeline ID
		 * at the same time, necessary if not valid data can be found in the
		 * existing output directory.
		 */
		if (!RunIdentifySystem(stream_thread_arg.conn, NULL, NULL, NULL, NULL))
		{
			PQfinish(stream_thread_arg.conn);
			elog(ERROR, "Cannot continue backup because stream connect has failed.");
		}

		if (is_remote_agent)
			xlog_files_list = parray_new();

        /* By default there are some error */
		stream_thread_arg.ret = 1;
		stream_thread_arg.files_list = xlog_files_list;

		pthread_create(&stream_thread, NULL, StreamLog, &stream_thread_arg);
	}

	/* initialize backup list */
	backup_files_list = parray_new();

	/* list files with the logical path. omit $PGDATA */
	if (is_remote_backup)
		get_remote_pgdata_filelist(backup_files_list);
	else
		dir_list_file(backup_files_list, instance_config.pgdata,
					  true, true, false, FIO_DB_HOST);

	/*
	 * Sort pathname ascending. It is necessary to create intermediate
	 * directories sequentially.
	 *
	 * For example:
	 * 1 - create 'base'
	 * 2 - create 'base/1'
	 *
	 * Sorted array is used at least in parse_backup_filelist_filenames(),
	 * extractPageMap(), make_pagemap_from_ptrack().
	 */
	parray_qsort(backup_files_list, pgFileComparePath);

	/* Extract information about files in backup_list parsing their names:*/
	parse_backup_filelist_filenames(backup_files_list, instance_config.pgdata);

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
		extractPageMap(arclog_path, current.tli, instance_config.xlog_seg_size,
					   prev_backup->start_lsn, current.start_lsn,
					   backup_files_list);
	}
	else if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
	{
		/*
		 * Build the page map from ptrack information.
		 */
		make_pagemap_from_ptrack(backup_files_list);
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
			char	   *dir_name;
			char		database_path[MAXPGPATH];

			if (!is_remote_backup)
				dir_name = GetRelativePath(file->path, instance_config.pgdata);
			else
				dir_name = file->path;

			elog(VERBOSE, "Create directory \"%s\"", dir_name);
			pgBackupGetPath(&current, database_path, lengthof(database_path),
					DATABASE_DIR);

			join_path_components(dirpath, database_path, dir_name);
			fio_mkdir(dirpath, DIR_PERMISSION, FIO_BACKUP_HOST);
		}

		/* setup threads */
		pg_atomic_clear_flag(&file->lock);
	}

	/* Sort by size for load balancing */
	parray_qsort(backup_files_list, pgFileCompareSize);
	/* Sort the array for binary search */
	if (prev_backup_filelist)
		parray_qsort(prev_backup_filelist, pgFileComparePath);

	/* init thread args with own file lists */
	threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
	threads_args = (backup_files_arg *) palloc(sizeof(backup_files_arg)*num_threads);

	for (i = 0; i < num_threads; i++)
	{
		backup_files_arg *arg = &(threads_args[i]);

		arg->from_root = instance_config.pgdata;
		arg->to_root = database_path;
		arg->files_list = backup_files_list;
		arg->prev_filelist = prev_backup_filelist;
		arg->prev_start_lsn = prev_backup_start_lsn;
		arg->backup_conn = NULL;
		arg->cancel_conn = NULL;
		/* By default there are some error */
		arg->ret = 1;
	}

	/* Run threads */
	elog(INFO, "Start transfering data files");
	for (i = 0; i < num_threads; i++)
	{
		backup_files_arg *arg = &(threads_args[i]);

		elog(VERBOSE, "Start thread num: %i", i);

		if (!is_remote_backup)
			pthread_create(&threads[i], NULL, backup_files, arg);
		else
			pthread_create(&threads[i], NULL, remote_backup_files, arg);
	}

	/* Wait threads */
	for (i = 0; i < num_threads; i++)
	{
		pthread_join(threads[i], NULL);
		if (threads_args[i].ret == 1)
			backup_isok = false;
	}
	if (backup_isok)
		elog(INFO, "Data files are transfered");
	else
		elog(ERROR, "Data files transferring failed");

	/* clean previous backup file list */
	if (prev_backup_filelist)
	{
		parray_walk(prev_backup_filelist, pgFileFree);
		parray_free(prev_backup_filelist);
	}

	/* In case of backup from replica >= 9.6 we must fix minRecPoint,
	 * First we must find pg_control in backup_files_list.
	 */
	if (current.from_replica && !exclusive_backup)
	{
		char		pg_control_path[MAXPGPATH];

		snprintf(pg_control_path, sizeof(pg_control_path), "%s/%s",
				 instance_config.pgdata, "global/pg_control");

		for (i = 0; i < parray_num(backup_files_list); i++)
		{
			pgFile	   *tmp_file = (pgFile *) parray_get(backup_files_list, i);

			if (strcmp(tmp_file->path, pg_control_path) == 0)
			{
				pg_control = tmp_file;
				break;
			}
		}
	}


	/* Notify end of backup */
	pg_stop_backup(&current);

	if (current.from_replica && !exclusive_backup)
		set_min_recovery_point(pg_control, database_path, current.stop_lsn);

	/* Add archived xlog files into the list of files of this backup */
	if (stream_wal)
	{
		if (xlog_files_list == NULL)
		{
			char		pg_xlog_path[MAXPGPATH];

			/* Scan backup PG_XLOG_DIR */
			xlog_files_list = parray_new();
			join_path_components(pg_xlog_path, database_path, PG_XLOG_DIR);
			dir_list_file(xlog_files_list, pg_xlog_path, false, true, false, FIO_BACKUP_HOST);
		}
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
	write_backup_filelist(&current, backup_files_list, instance_config.pgdata);

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

	/* Cleanup */
	if (backup_list)
	{
		parray_walk(backup_list, pgBackupFree);
		parray_free(backup_list);
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
	if (instance_config.pgdata == NULL)
		elog(ERROR, "required parameter not specified: PGDATA "
						 "(-D, --pgdata)");
	if (current.backup_mode == BACKUP_MODE_INVALID)
		elog(ERROR, "required parameter not specified: BACKUP_MODE "
						 "(-b, --backup-mode)");

	/* Create connection for PostgreSQL */
	backup_conn = pgut_connect(instance_config.pghost, instance_config.pgport,
							   instance_config.pgdatabase,
							   instance_config.pguser);
	pgut_atexit_push(backup_disconnect, NULL);

	current.primary_conninfo = pgut_get_conninfo_string(backup_conn);

#if PG_VERSION_NUM >= 110000
	if (!RetrieveWalSegSize(backup_conn))
		elog(ERROR, "Failed to retreive wal_segment_size");
#endif

	current.compress_alg = instance_config.compress_alg;
	current.compress_level = instance_config.compress_level;

	/* Confirm data block size and xlog block size are compatible */
	confirm_block_size("block_size", BLCKSZ);
	confirm_block_size("wal_block_size", XLOG_BLCKSZ);

	current.from_replica = pg_is_in_recovery();

	/* Confirm that this server version is supported */
	check_server_version();

	/* TODO fix it for remote backup*/
	if (!is_remote_backup)
		current.checksum_version = get_data_checksum_version(true);

	is_checksum_enabled = pg_checksum_enable();

	if (is_checksum_enabled)
		elog(LOG, "This PostgreSQL instance was initialized with data block checksums. "
					"Data block corruption will be detected");
	else
		elog(WARNING, "This PostgreSQL instance was initialized without data block checksums. "
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

	if (current.from_replica && exclusive_backup)
	{
		/* Check master connection options */
		if (instance_config.master_host == NULL)
			elog(ERROR, "Options for connection to master must be provided to perform backup from replica");

		/* Create connection to master server */
		master_conn = pgut_connect(instance_config.master_host,
								   instance_config.master_port,
								   instance_config.master_db,
								   instance_config.master_user);
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
	StrNCpy(current.program_version, PROGRAM_VERSION,
			sizeof(current.program_version));

	/* Create backup directory and BACKUP_CONTROL_FILE */
	if (pgBackupCreateDir(&current))
		elog(ERROR, "cannot create backup directory");
	write_backup(&current);

	elog(LOG, "Backup destination is initialized");

	/* set the error processing function for the backup process */
	pgut_atexit_push(backup_cleanup, NULL);

	/* backup data */
	do_backup_instance();
	pgut_atexit_pop(backup_cleanup, NULL);

	/* compute size of wal files of this backup stored in the archive */
	if (!current.stream)
	{
		current.wal_bytes = instance_config.xlog_seg_size *
			(current.stop_lsn / instance_config.xlog_seg_size -
			 current.start_lsn / instance_config.xlog_seg_size + 1);
	}

	/* Backup is done. Update backup status */
	current.end_time = time(NULL);
	current.status = BACKUP_STATUS_DONE;
	write_backup(&current);

	//elog(LOG, "Backup completed. Total bytes : " INT64_FORMAT "",
	//		current.data_bytes);

	if (is_remote_agent)
	{
		fio_transfer(FIO_BACKUP_START_TIME);
		fio_transfer(FIO_BACKUP_STOP_LSN);
	}
	else
		complete_backup();

	return 0;
}

void complete_backup(void)
{
	pgBackupValidate(&current);

	elog(INFO, "Backup %s completed", base36enc(current.start_time));

	/*
	 * After successfil backup completion remove backups
	 * which are expired according to retention policies
	 */
	if (delete_expired || delete_wal)
		do_retention_purge();
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

	if (server_version == 0)
		elog(ERROR, "Unknown server version %d", server_version);

	if (server_version < 100000)
		sprintf(server_version_str, "%d.%d",
				server_version / 10000,
				(server_version / 100) % 100);
	else
		sprintf(server_version_str, "%d",
				server_version / 10000);

	if (server_version < 90500)
		elog(ERROR,
			 "server version is %s, must be %s or higher",
			 server_version_str, "9.5");

	if (current.from_replica && server_version < 90600)
		elog(ERROR,
			 "server version is %s, must be %s or higher for backup from replica",
			 server_version_str, "9.6");

	res = pgut_execute_extended(backup_conn, "SELECT pgpro_edition()",
								0, NULL, true, true);

	/*
	 * Check major version of connected PostgreSQL and major version of
	 * compiled PostgreSQL.
	 */
#ifdef PGPRO_VERSION
	if (PQresultStatus(res) == PGRES_FATAL_ERROR)
		/* It seems we connected to PostgreSQL (not Postgres Pro) */
		elog(ERROR, "%s was built with Postgres Pro %s %s, "
					"but connection is made with PostgreSQL %s",
			 PROGRAM_NAME, PG_MAJORVERSION, PGPRO_EDITION, server_version_str);
	else if (strcmp(server_version_str, PG_MAJORVERSION) != 0 &&
			 strcmp(PQgetvalue(res, 0, 0), PGPRO_EDITION) != 0)
		elog(ERROR, "%s was built with Postgres Pro %s %s, "
					"but connection is made with Postgres Pro %s %s",
			 PROGRAM_NAME, PG_MAJORVERSION, PGPRO_EDITION,
			 server_version_str, PQgetvalue(res, 0, 0));
#else
	if (PQresultStatus(res) != PGRES_FATAL_ERROR)
		/* It seems we connected to Postgres Pro (not PostgreSQL) */
		elog(ERROR, "%s was built with PostgreSQL %s, "
					"but connection is made with Postgres Pro %s %s",
			 PROGRAM_NAME, PG_MAJORVERSION,
			 server_version_str, PQgetvalue(res, 0, 0));
	else if (strcmp(server_version_str, PG_MAJORVERSION) != 0)
		elog(ERROR, "%s was built with PostgreSQL %s, but connection is made with %s",
			 PROGRAM_NAME, PG_MAJORVERSION, server_version_str);
#endif

	PQclear(res);

	/* Do exclusive backup only for PostgreSQL 9.5 */
	exclusive_backup = server_version < 90600 ||
		current.backup_mode == BACKUP_MODE_DIFF_PTRACK;
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

	system_id_pgdata = get_system_identifier(instance_config.pgdata);
	system_id_conn = get_remote_system_identifier(backup_conn);

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
confirm_block_size(const char *name, int blcksz)
{
	PGresult   *res;
	char	   *endp;
	int			block_size;

	res = pgut_execute(backup_conn, "SELECT pg_catalog.current_setting($1)", 1, &name);
	if (PQntuples(res) != 1 || PQnfields(res) != 1)
		elog(ERROR, "cannot get %s: %s", name, PQerrorMessage(backup_conn));

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
pg_start_backup(const char *label, bool smooth, pgBackup *backup)
{
	PGresult   *res;
	const char *params[2];
	uint32		lsn_hi;
	uint32		lsn_lo;
	PGconn	   *conn;

	params[0] = label;

	/* For 9.5 replica we call pg_start_backup() on master */
	if (backup->from_replica && exclusive_backup)
		conn = master_conn;
	else
		conn = backup_conn;

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

	/* Extract timeline and LSN from results of pg_start_backup() */
	XLogDataFromLSN(PQgetvalue(res, 0, 0), &lsn_hi, &lsn_lo);
	/* Calculate LSN */
	backup->start_lsn = ((uint64) lsn_hi )<< 32 | lsn_lo;

	PQclear(res);

	if (current.backup_mode == BACKUP_MODE_DIFF_PAGE &&
			(!(backup->from_replica && !exclusive_backup)))
		/*
		 * Switch to a new WAL segment. It is necessary to get archived WAL
		 * segment, which includes start LSN of current backup.
		 * Don`t do this for replica backups unless it`s PG 9.5
		 */
		pg_switch_wal(conn);

	if (current.backup_mode == BACKUP_MODE_DIFF_PAGE)
		/* In PAGE mode wait for current segment... */
		wait_wal_lsn(backup->start_lsn, true, false);
	/*
	 * Do not wait start_lsn for stream backup.
	 * Because WAL streaming will start after pg_start_backup() in stream
	 * mode.
	 */
	else if (!stream_wal)
		/* ...for others wait for previous segment */
		wait_wal_lsn(backup->start_lsn, true, true);

	/* In case of backup from replica for PostgreSQL 9.5
	 * wait for start_lsn to be replayed by replica
	 */
	if (backup->from_replica && exclusive_backup)
		wait_replica_wal_lsn(backup->start_lsn, true);
}

/*
 * Switch to a new WAL segment. It should be called only for master.
 */
static void
pg_switch_wal(PGconn *conn)
{
	PGresult   *res;

	/* Remove annoying NOTICE messages generated by backend */
	res = pgut_execute(conn, "SET client_min_messages = warning;", 0, NULL);
	PQclear(res);

#if PG_VERSION_NUM >= 100000
	res = pgut_execute(conn, "SELECT * FROM pg_catalog.pg_switch_wal()", 0, NULL);
#else
	res = pgut_execute(conn, "SELECT * FROM pg_catalog.pg_switch_xlog()", 0, NULL);
#endif

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
						  0, NULL);
	if (PQntuples(res_db) == 0)
	{
		PQclear(res_db);
		return false;
	}
	PQclear(res_db);

	res_db = pgut_execute(backup_conn,
						  "SELECT pg_catalog.ptrack_version()",
						  0, NULL);
	if (PQntuples(res_db) == 0)
	{
		PQclear(res_db);
		return false;
	}

	/* Now we support only ptrack versions upper than 1.5 */
	if (strcmp(PQgetvalue(res_db, 0, 0), "1.5") != 0 &&
		strcmp(PQgetvalue(res_db, 0, 0), "1.6") != 0 &&
		strcmp(PQgetvalue(res_db, 0, 0), "1.7") != 0)
	{
		elog(WARNING, "Update your ptrack to the version 1.5 or upper. Current version is %s", PQgetvalue(res_db, 0, 0));
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

/* Check if ptrack is enabled in target instance */
static bool
pg_checksum_enable(void)
{
	PGresult   *res_db;

	res_db = pgut_execute(backup_conn, "show data_checksums", 0, NULL);

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

	res_db = pgut_execute(backup_conn, "SELECT pg_catalog.pg_is_in_recovery()", 0, NULL);

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
						  0, NULL);

	for(i = 0; i < PQntuples(res_db); i++)
	{
		PGconn	   *tmp_conn;

		dbname = PQgetvalue(res_db, i, 0);
		if (strcmp(dbname, "template0") == 0)
			continue;

		dbOid = atoi(PQgetvalue(res_db, i, 1));
		tblspcOid = atoi(PQgetvalue(res_db, i, 2));

		tmp_conn = pgut_connect(instance_config.pghost, instance_config.pgport,
								dbname,
								instance_config.pguser);
		res = pgut_execute(tmp_conn, "SELECT pg_catalog.pg_ptrack_clear()",
						   0, NULL);
		PQclear(res);

		sprintf(params[0], "%i", dbOid);
		sprintf(params[1], "%i", tblspcOid);
		res = pgut_execute(tmp_conn, "SELECT pg_catalog.pg_ptrack_get_and_clear_db($1, $2)",
						   2, (const char **)params);
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
	bool		result;

	params[0] = palloc(64);
	params[1] = palloc(64);

	sprintf(params[0], "%i", dbOid);
	res_db = pgut_execute(backup_conn,
							"SELECT datname FROM pg_database WHERE oid=$1",
							1, (const char **) params);
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
	res = pgut_execute(backup_conn, "SELECT pg_catalog.pg_ptrack_get_and_clear_db($1, $2)",
						2, (const char **)params);

	if (PQnfields(res) != 1)
		elog(ERROR, "cannot perform pg_ptrack_get_and_clear_db()");

	if (!parse_bool(PQgetvalue(res, 0, 0), &result))
		elog(ERROR,
			 "result of pg_ptrack_get_and_clear_db() is invalid: %s",
			 PQgetvalue(res, 0, 0));

	PQclear(res);
	pfree(params[0]);
	pfree(params[1]);

	return result;
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
							  1, (const char **) params);
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

		tmp_conn = pgut_connect(instance_config.pghost, instance_config.pgport,
								dbname,
								instance_config.pguser);
		sprintf(params[0], "%i", tablespace_oid);
		sprintf(params[1], "%i", rel_filenode);
		res = pgut_execute(tmp_conn, "SELECT pg_catalog.pg_ptrack_get_and_clear($1, $2)",
						   2, (const char **)params);

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
		res = pgut_execute(backup_conn, "SELECT pg_catalog.pg_ptrack_get_and_clear($1, $2)",
						   2, (const char **)params);

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
 * If 'is_start_lsn' is true and backup mode is PAGE then we wait for 'lsn' to
 * be archived in archive 'wal' directory regardless stream mode.
 *
 * If 'wait_prev_segment' wait for previous segment.
 *
 * Returns LSN of last valid record if wait_prev_segment is not true, otherwise
 * returns InvalidXLogRecPtr.
 */
static XLogRecPtr
wait_wal_lsn(XLogRecPtr lsn, bool is_start_lsn, bool wait_prev_segment)
{
	TimeLineID	tli;
	XLogSegNo	targetSegNo;
	char		pg_wal_dir[MAXPGPATH];
	char		wal_segment_path[MAXPGPATH],
			   *wal_segment_dir,
				wal_segment[MAXFNAMELEN];
	bool		file_exists = false;
	uint32		try_count = 0,
				timeout;

#ifdef HAVE_LIBZ
	char		gz_wal_segment_path[MAXPGPATH];
#endif

	tli = get_current_timeline(false);

	/* Compute the name of the WAL file containig requested LSN */
	GetXLogSegNo(lsn, targetSegNo, instance_config.xlog_seg_size);
	if (wait_prev_segment)
		targetSegNo--;
	GetXLogFileName(wal_segment, tli, targetSegNo,
					instance_config.xlog_seg_size);

	/*
	 * In pg_start_backup we wait for 'lsn' in 'pg_wal' directory if it is
	 * stream and non-page backup. Page backup needs archived WAL files, so we
	 * wait for 'lsn' in archive 'wal' directory for page backups.
	 *
	 * In pg_stop_backup it depends only on stream_wal.
	 */
	if (stream_wal &&
		(current.backup_mode != BACKUP_MODE_DIFF_PAGE || !is_start_lsn))
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

	if (instance_config.archive_timeout > 0)
		timeout = instance_config.archive_timeout;
	else
		timeout = ARCHIVE_TIMEOUT_DEFAULT;

	if (wait_prev_segment)
		elog(LOG, "Looking for segment: %s", wal_segment);
	else
		elog(LOG, "Looking for LSN %X/%X in segment: %s",
			 (uint32) (lsn >> 32), (uint32) lsn, wal_segment);

#ifdef HAVE_LIBZ
	snprintf(gz_wal_segment_path, sizeof(gz_wal_segment_path), "%s.gz",
			 wal_segment_path);
#endif

	/* Wait until target LSN is archived or streamed */
	while (true)
	{
		if (!file_exists)
		{
			file_exists = fileExists(wal_segment_path, is_start_lsn ? FIO_DB_HOST : FIO_BACKUP_HOST);

			/* Try to find compressed WAL file */
			if (!file_exists)
			{
#ifdef HAVE_LIBZ
				file_exists = fileExists(gz_wal_segment_path, is_start_lsn ? FIO_DB_HOST : FIO_BACKUP_HOST);
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
				return InvalidXLogRecPtr;

			/*
			 * A WAL segment found. Check LSN on it.
			 */
			if (wal_contains_lsn(wal_segment_dir, lsn, tli,
								 instance_config.xlog_seg_size))
				/* Target LSN was found */
			{
				elog(LOG, "Found LSN: %X/%X", (uint32) (lsn >> 32), (uint32) lsn);
				return lsn;
			}

			/*
			 * If we failed to get LSN of valid record in a reasonable time, try
			 * to get LSN of last valid record prior to the target LSN. But only
			 * in case of a backup from a replica.
			 */
			if (!exclusive_backup && current.from_replica &&
				(try_count > timeout / 4))
			{
				XLogRecPtr	res;

				res = get_last_wal_lsn(wal_segment_dir, current.start_lsn,
									   lsn, tli, false,
									   instance_config.xlog_seg_size);
				if (!XLogRecPtrIsInvalid(res))
				{
					/* LSN of the prior record was found */
					elog(LOG, "Found prior LSN: %X/%X, it is used as stop LSN",
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

	while (true)
	{
		XLogRecPtr	replica_lsn;

		/*
		 * For lsn from pg_start_backup() we need it to be replayed on replica's
		 * data.
		 */
		if (is_start_backup)
		{
			replica_lsn = get_checkpoint_location(backup_conn);
		}
		/*
		 * For lsn from pg_stop_backup() we need it only to be received by
		 * replica and fsync()'ed on WAL segment.
		 */
		else
		{
			PGresult   *res;
			uint32		lsn_hi;
			uint32		lsn_lo;

#if PG_VERSION_NUM >= 100000
			res = pgut_execute(backup_conn, "SELECT pg_catalog.pg_last_wal_receive_lsn()",
							   0, NULL);
#else
			res = pgut_execute(backup_conn, "SELECT pg_catalog.pg_last_xlog_receive_location()",
							   0, NULL);
#endif

			/* Extract LSN from result */
			XLogDataFromLSN(PQgetvalue(res, 0, 0), &lsn_hi, &lsn_lo);
			/* Calculate LSN */
			replica_lsn = ((uint64) lsn_hi) << 32 | lsn_lo;
			PQclear(res);
		}

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

		if (instance_config.replica_timeout > 0 &&
			try_count > instance_config.replica_timeout)
			elog(ERROR, "Target LSN %X/%X could not be recevied by replica "
				 "in %d seconds",
				 (uint32) (lsn >> 32), (uint32) lsn,
				 instance_config.replica_timeout);
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

	/*
	 * We will use this values if there are no transactions between start_lsn
	 * and stop_lsn.
	 */
	time_t		recovery_time;
	TransactionId recovery_xid;

	if (!backup_in_progress)
		elog(ERROR, "backup is not in progress");

	/* For 9.5 replica we call pg_stop_backup() on master */
	if (current.from_replica && exclusive_backup)
		conn = master_conn;
	else
		conn = backup_conn;

	/* Remove annoying NOTICE messages generated by backend */
	res = pgut_execute(conn, "SET client_min_messages = warning;",
					   0, NULL);
	PQclear(res);

	/* Create restore point
	 * only if it`s backup from master, or exclusive replica(wich connects to master)
	 */
	if (backup != NULL && (!current.from_replica || (current.from_replica && exclusive_backup)))
	{
		const char *params[1];
		char		name[1024];

		if (!current.from_replica)
			snprintf(name, lengthof(name), "pg_probackup, backup_id %s",
					 base36enc(backup->start_time));
		else
			snprintf(name, lengthof(name), "pg_probackup, backup_id %s. Replica Backup",
					 base36enc(backup->start_time));
		params[0] = name;

		res = pgut_execute(conn, "SELECT pg_catalog.pg_create_restore_point($1)",
						   1, params);
		/* Extract timeline and LSN from the result */
		XLogDataFromLSN(PQgetvalue(res, 0, 0), &lsn_hi, &lsn_lo);
		/* Calculate LSN */
		//restore_lsn = ((uint64) lsn_hi) << 32 | lsn_lo;
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

	/*
	 * Wait for the result of pg_stop_backup(), but no longer than
	 * archive_timeout seconds
	 */
	if (pg_stop_backup_is_sent && !in_cleanup)
	{
		res = NULL;

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

		/* Extract timeline and LSN from results of pg_stop_backup() */
		XLogDataFromLSN(PQgetvalue(res, 0, 2), &lsn_hi, &lsn_lo);
		/* Calculate LSN */
		stop_backup_lsn = ((uint64) lsn_hi) << 32 | lsn_lo;

		if (!XRecOffIsValid(stop_backup_lsn))
		{
			if (XRecOffIsNull(stop_backup_lsn))
			{
				char	   *xlog_path,
							stream_xlog_path[MAXPGPATH];

				if (stream_wal)
				{
					pgBackupGetPath2(backup, stream_xlog_path,
									 lengthof(stream_xlog_path),
									 DATABASE_DIR, PG_XLOG_DIR);
					xlog_path = stream_xlog_path;
				}
				else
					xlog_path = arclog_path;

				stop_backup_lsn = get_last_wal_lsn(xlog_path, backup->start_lsn,
												   stop_backup_lsn, backup->tli,
												   true, instance_config.xlog_seg_size);
				/*
				 * Do not check existance of LSN again below using
				 * wait_wal_lsn().
				 */
				stop_lsn_exists = true;
			}
			else
				elog(ERROR, "Invalid stop_backup_lsn value %X/%X",
					 (uint32) (stop_backup_lsn >> 32), (uint32) (stop_backup_lsn));
		}

		/* Write backup_label and tablespace_map */
		if (!exclusive_backup)
		{
			Assert(PQnfields(res) >= 4);
			pgBackupGetPath(&current, path, lengthof(path), DATABASE_DIR);

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
				file = pgFileNew(backup_label, true, FIO_BACKUP_HOST);
				calc_file_checksum(file);
				free(file->path);
				file->path = strdup(PG_BACKUP_LABEL_FILE);
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
				file = pgFileNew(tablespace_map, true, FIO_BACKUP_HOST);
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
		{
			/* Wait for the completion of stream */
			pthread_join(stream_thread, NULL);
			if (stream_thread_arg.ret == 1)
				elog(ERROR, "WAL streaming failed");
		}
	}

	/* Fill in fields if that is the correct end of backup. */
	if (backup != NULL)
	{
		char	   *xlog_path,
					stream_xlog_path[MAXPGPATH];

		/* Wait for stop_lsn to be received by replica */
		/* XXX Do we need this? */
//		if (current.from_replica)
//			wait_replica_wal_lsn(stop_backup_lsn, false);
		/*
		 * Wait for stop_lsn to be archived or streamed.
		 * We wait for stop_lsn in stream mode just in case.
		 */
		if (!stop_lsn_exists)
			stop_backup_lsn = wait_wal_lsn(stop_backup_lsn, false, false);

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

		/* iterate over WAL from stop_backup lsn to start_backup lsn */
		if (!read_recovery_info(xlog_path, backup->tli,
								instance_config.xlog_seg_size,
								backup->start_lsn, backup->stop_lsn,
								&backup->recovery_time, &backup->recovery_xid))
		{
			elog(LOG, "Failed to find Recovery Time in WAL. Forced to trust current_timestamp");
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
		write_backup(&current);
	}

	/*
	 * If backup is in progress, notify stop of backup to PostgreSQL
	 */
	if (backup_in_progress)
	{
		elog(WARNING, "backup in progress, stop backup");
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
static void *
backup_files(void *arg)
{
	int			i;
	backup_files_arg *arguments = (backup_files_arg *) arg;
	int			n_backup_files_list = parray_num(arguments->files_list);

	/* backup a file */
	for (i = 0; i < n_backup_files_list; i++)
	{
		int			ret;
		struct stat	buf;
		pgFile	   *file = (pgFile *) parray_get(arguments->files_list, i);

		elog(VERBOSE, "Copying file:  \"%s\" ", file->path);
		if (!pg_atomic_test_set_flag(&file->lock))
			continue;

		/* check for interrupt */
		if (interrupted)
			elog(ERROR, "interrupted during backup");

		if (progress)
			elog(INFO, "Progress: (%d/%d). Process file \"%s\"",
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
			pgFile	  **prev_file = NULL;

			/* Check that file exist in previous backup */
			if (current.backup_mode != BACKUP_MODE_FULL)
			{
				char	   *relative;
				pgFile		key;

				relative = GetRelativePath(file->path, arguments->from_root);
				key.path = relative;

				prev_file = (pgFile **) parray_bsearch(arguments->prev_filelist,
													   &key, pgFileComparePath);
				if (prev_file)
					/* File exists in previous backup */
					file->exists_in_prev = true;
			}
			/* copy the file into backup */
			if (file->is_datafile && !file->is_cfs)
			{
				char		to_path[MAXPGPATH];

				join_path_components(to_path, arguments->to_root,
									 file->path + strlen(arguments->from_root) + 1);

				/* backup block by block if datafile AND not compressed by cfs*/
				if (!backup_data_file(arguments, to_path, file,
									  arguments->prev_start_lsn,
									  current.backup_mode,
									  instance_config.compress_alg,
									  instance_config.compress_level))
				{
					file->write_size = BYTES_INVALID;
					elog(VERBOSE, "File \"%s\" was not copied to backup", file->path);
					continue;
				}
			}
			else if (strcmp(file->name, "pg_control") == 0)
				copy_pgcontrol_file(arguments->from_root, arguments->to_root,
									file, FIO_BACKUP_HOST);
			else
			{
				bool		skip = false;

				/* If non-data file has not changed since last backup... */
				if (prev_file && file->exists_in_prev &&
					buf.st_mtime < current.parent_backup)
				{
					calc_file_checksum(file);
					/* ...and checksum is the same... */
					if (EQ_TRADITIONAL_CRC32(file->crc, (*prev_file)->crc))
						skip = true; /* ...skip copying file. */
				}
				if (skip ||
					!copy_file(arguments->from_root, arguments->to_root, file, FIO_BACKUP_HOST))
				{
					file->write_size = BYTES_INVALID;
					elog(VERBOSE, "File \"%s\" was not copied to backup",
						 file->path);
					continue;
				}
			}

			elog(VERBOSE, "File \"%s\". Copied "INT64_FORMAT " bytes",
				 file->path, file->write_size);
		}
		else
			elog(WARNING, "unexpected file type %d", buf.st_mode);
	}

	/* Close connection */
	if (arguments->backup_conn)
		pgut_disconnect(arguments->backup_conn);

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
static void
parse_backup_filelist_filenames(parray *files, const char *root)
{
	size_t		i = 0;
	Oid			unlogged_file_reloid = 0;

	while (i < parray_num(files))
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);
		char	   *relative;
		int 		sscanf_result;

		relative = GetRelativePath(file->path, root);

		if (S_ISREG(file->mode) &&
			path_is_prefix_of_path(PG_TBLSPC_DIR, relative))
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
				sscanf_result = sscanf(relative, PG_TBLSPC_DIR "/%u/%s/%u",
									   &tblspcOid, tmp_rel_path, &dbOid);

				/* Yes, it is */
				if (sscanf_result == 2 &&
					strncmp(tmp_rel_path, TABLESPACE_VERSION_DIRECTORY,
							strlen(TABLESPACE_VERSION_DIRECTORY)) == 0)
					set_cfs_datafiles(files, root, relative, i);
			}
		}

		if (S_ISREG(file->mode) && file->tblspcOid != 0 &&
			file->name && file->name[0])
		{
			if (strcmp(file->forkName, "init") == 0)
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
 * Find pgfile by given rnode in the backup_files_list
 * and add given blkno to its pagemap.
 */
void
process_block_change(ForkNumber forknum, RelFileNode rnode, BlockNumber blkno)
{
	char	   *path;
	char	   *rel_path;
	BlockNumber blkno_inseg;
	int			segno;
	pgFile	  **file_item;
	pgFile		f;

	segno = blkno / RELSEG_SIZE;
	blkno_inseg = blkno % RELSEG_SIZE;

	rel_path = relpathperm(rnode, forknum);
	if (segno > 0)
		path = psprintf("%s/%s.%u", instance_config.pgdata, rel_path, segno);
	else
		path = psprintf("%s/%s", instance_config.pgdata, rel_path);

	pg_free(rel_path);

	f.path = path;
	/* backup_files_list should be sorted before */
	file_item = (pgFile **) parray_bsearch(backup_files_list, &f,
										   pgFileComparePath);

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

	pg_free(path);
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
			if (file->tblspcOid == tblspcOid_with_ptrack_init &&
				file->dbOid == dbOid_with_ptrack_init)
			{
				/* ignore ptrack if ptrack_init exists */
				elog(VERBOSE, "Ignoring ptrack because of ptrack_init for file: %s", file->path);
				file->pagemap_isabsent = true;
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

				/*
				 * If file segment was created after we have read ptrack,
				 * we won't have a bitmap for this segment.
				 */
				if (start_addr > ptrack_nonparsed_size)
				{
					elog(VERBOSE, "Ptrack is missing for file: %s", file->path);
					file->pagemap_isabsent = true;
				}
				else
				{

					if (start_addr + RELSEG_SIZE/HEAPBLOCKS_PER_BYTE > ptrack_nonparsed_size)
					{
						file->pagemap.bitmapsize = ptrack_nonparsed_size - start_addr;
						elog(VERBOSE, "pagemap size: %i", file->pagemap.bitmapsize);
					}
					else
					{
						file->pagemap.bitmapsize = RELSEG_SIZE/HEAPBLOCKS_PER_BYTE;
						elog(VERBOSE, "pagemap size: %i", file->pagemap.bitmapsize);
					}

					file->pagemap.bitmap = pg_malloc(file->pagemap.bitmapsize);
					memcpy(file->pagemap.bitmap, ptrack_nonparsed+start_addr, file->pagemap.bitmapsize);
				}
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
				file->pagemap_isabsent = true;
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

	/* check for interrupt */
	if (interrupted)
		elog(ERROR, "Interrupted during backup");

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
static void *
StreamLog(void *arg)
{
	XLogRecPtr	startpos;
	TimeLineID	starttli;
	StreamThreadArg *stream_arg = (StreamThreadArg *) arg;

	/*
	 * We must use startpos as start_lsn from start_backup
	 */
	startpos = current.start_lsn;
	starttli = current.tli;

	/*
	 * Always start streaming at the beginning of a segment
	 */
	startpos -= startpos % instance_config.xlog_seg_size;

	/* Initialize timeout */
	stream_stop_timeout = 0;
	stream_stop_begin = 0;

	/*
	 * Start the replication
	 */
	elog(LOG, _("started streaming WAL at %X/%X (timeline %u)"),
		 (uint32) (startpos >> 32), (uint32) startpos, starttli);

#if PG_VERSION_NUM >= 90600
	{
		StreamCtl	ctl;

		MemSet(&ctl, 0, sizeof(ctl));

		ctl.startpos = startpos;
		ctl.timeline = starttli;
		ctl.sysidentifier = NULL;

#if PG_VERSION_NUM >= 100000
		ctl.walmethod = CreateWalDirectoryMethod(stream_arg->basedir, 0, true, stream_arg->files_list);
		ctl.replication_slot = replication_slot;
		ctl.stop_socket = PGINVALID_SOCKET;
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
	if(ReceiveXlogStream(stream_arg->conn, startpos, starttli, NULL,
						 (char *) stream_arg->basedir, stop_streaming,
						 standby_message_timeout, NULL, false, false) == false)
		elog(ERROR, "Problem in receivexlog");
#endif

	elog(LOG, _("finished streaming WAL at %X/%X (timeline %u)"),
		 (uint32) (stop_stream_lsn >> 32), (uint32) stop_stream_lsn, starttli);
	stream_arg->ret = 0;

	PQfinish(stream_arg->conn);
	stream_arg->conn = NULL;

	return NULL;
}

/*
 * Get lsn of the moment when ptrack was enabled the last time.
 */
static XLogRecPtr
get_last_ptrack_lsn(void)

{
	PGresult   *res;
	uint32		lsn_hi;
	uint32		lsn_lo;
	XLogRecPtr	lsn;

	res = pgut_execute(backup_conn, "select pg_catalog.pg_ptrack_control_lsn()",
					   0, NULL);

	/* Extract timeline and LSN from results of pg_start_backup() */
	XLogDataFromLSN(PQgetvalue(res, 0, 0), &lsn_hi, &lsn_lo);
	/* Calculate LSN */
	lsn = ((uint64) lsn_hi) << 32 | lsn_lo;

	PQclear(res);
	return lsn;
}

char *
pg_ptrack_get_block(backup_files_arg *arguments,
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

	if (arguments->backup_conn == NULL)
	{
		arguments->backup_conn = pgut_connect(instance_config.pghost,
											  instance_config.pgport,
											  instance_config.pgdatabase,
											  instance_config.pguser);
	}

	if (arguments->cancel_conn == NULL)
		arguments->cancel_conn = PQgetCancel(arguments->backup_conn);

	//elog(LOG, "db %i pg_ptrack_get_block(%i, %i, %u)",dbOid, tblsOid, relOid, blknum);
	res = pgut_execute_parallel(arguments->backup_conn,
								arguments->cancel_conn,
					"SELECT pg_catalog.pg_ptrack_get_block_2($1, $2, $3, $4)",
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
