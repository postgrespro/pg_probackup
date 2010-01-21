/*-------------------------------------------------------------------------
 *
 * backup.c: backup DB cluster, archived WAL, serverlog.
 *
 * Copyright (c) 2009-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_rman.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <time.h>

#include "libpq/pqsignal.h"
#include "pgut/pgut-port.h"

#define TIMEOUT_ARCHIVE		10	/* wait 10 sec until WAL archive complete */

static bool	in_backup = false;	/* TODO: more robust logic */

/*
 * Backup routines
 */
static void backup_cleanup(bool fatal, void *userdata);
static void delete_old_files(const char *root, parray *files, int keep_files,
							 int keep_days, int server_version, bool is_arclog);
static void backup_files(const char *from_root, const char *to_root,
	parray *files, parray *prev_files, const XLogRecPtr *lsn, bool compress);
static parray *do_backup_database(parray *backup_list, bool smooth_checkpoint);
static parray *do_backup_arclog(parray *backup_list);
static parray *do_backup_srvlog(parray *backup_list);
static void confirm_block_size(const char *name, int blcksz);
static void pg_start_backup(const char *label, bool smooth, pgBackup *backup);
static void pg_stop_backup(pgBackup *backup);
static void pg_switch_xlog(pgBackup *backup);
static void get_lsn(PGresult *res, TimeLineID *timeline, XLogRecPtr *lsn);

static void delete_arclog_link(void);
static void delete_online_wal_backup(void);

static bool fileExists(const char *path);

/*
 * Take a backup of database.
 */
static parray *
do_backup_database(parray *backup_list, bool smooth_checkpoint)
{
	int			i;
	parray	   *files;
	parray	   *prev_files = NULL;	/* file list of previous database backup */
	FILE	   *fp;
	char		path[MAXPGPATH];
	char		label[1024];
	XLogRecPtr *lsn = NULL;

	if (!HAVE_DATABASE(&current))
		return NULL;

	elog(INFO, _("database backup start"));

	/* initialize size summary */
	current.total_data_bytes = 0;
	current.read_data_bytes = 0;

	/* notify start of backup to PostgreSQL server */
	time2iso(label, lengthof(label), current.start_time);
	strncat(label, " with pg_rman", lengthof(label));
	pg_start_backup(label, smooth_checkpoint, &current);
	pgut_atexit_push(backup_cleanup, NULL);

	/*
	 * list directories and symbolic links  with the physical path to make
	 * mkdirs.sh
	 * Sort in order of path.
	 * omit $PGDATA.
	 */
	files = parray_new();
	dir_list_file(files, pgdata, NULL, false, false);

	if (!check)
	{
		pgBackupGetPath(&current, path, lengthof(path), MKDIRS_SH_FILE);
		fp = fopen(path, "wt");
		if (fp == NULL)
			elog(ERROR_SYSTEM, _("can't open make directory script \"%s\": %s"),
				path, strerror(errno));
		dir_print_mkdirs_sh(fp, files, pgdata);
		fclose(fp);
		if (chmod(path, DIR_PERMISSION) == -1)
			elog(ERROR_SYSTEM, _("can't change mode of \"%s\": %s"), path,
				strerror(errno));
	}

	/* clear directory list */
	parray_walk(files, pgFileFree);
	parray_free(files);
	files = NULL;

	/*
	 * To take incremental backup, the file list of the last completed database
	 * backup is needed.
	 */
	if (current.backup_mode < BACKUP_MODE_FULL)
	{
		char		prev_file_txt[MAXPGPATH];
		pgBackup   *prev_backup;

		/* find last completed database backup */
		prev_backup = catalog_get_last_data_backup(backup_list);
		if (prev_backup == NULL)
		{
			elog(INFO, _("no previous full backup, do a full backup instead"));
			current.backup_mode = BACKUP_MODE_FULL;
		}
		else
		{
			pgBackupGetPath(prev_backup, prev_file_txt, lengthof(prev_file_txt),
				DATABASE_FILE_LIST);
			prev_files = dir_read_file_list(pgdata, prev_file_txt);

			/*
			 * Do backup only pages having larger LSN than previous backup.
			 */
			lsn = &prev_backup->start_lsn;
			elog(LOG, _("backup only the page that there was of the update from LSN(%X/%08X).\n"),
				lsn->xlogid, lsn->xrecoff);
		}
	}

	/* list files with the logical path. omit $PGDATA */
	files = parray_new();
	dir_list_file(files, pgdata, pgdata_exclude, true, false);

	/* mark files that are possible datafile as 'datafile' */
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile *file = (pgFile *) parray_get(files, i);
		char *relative;
		char *fname;

		/* data file must be a regular file */
		if (!S_ISREG(file->mode))
			continue;

		/* data files are under "base", "global", or "pg_tblspc" */
		relative = file->path + strlen(pgdata) + 1;
		if (!path_is_prefix_of_path("base", relative) &&
			!path_is_prefix_of_path("global", relative) &&
			!path_is_prefix_of_path("pg_tblspc", relative))
			continue;

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

	pgBackupGetPath(&current, path, lengthof(path), DATABASE_DIR);
	backup_files(pgdata, path, files, prev_files, lsn, current.compress_data);

	/* notify end of backup */
	pg_stop_backup(&current);
	pgut_atexit_pop(backup_cleanup, NULL);

	/* create file list */
	if (!check)
	{
		pgBackupGetPath(&current, path, lengthof(path), DATABASE_FILE_LIST);
		fp = fopen(path, "wt");
		if (fp == NULL)
			elog(ERROR_SYSTEM, _("can't open file list \"%s\": %s"), path,
				strerror(errno));
		dir_print_file_list(fp, files, pgdata);
		fclose(fp);
	}

	/* print summary of size of backup mode files */
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile *file = (pgFile *) parray_get(files, i);
		if (!S_ISREG(file->mode))
			continue;
		current.total_data_bytes += file->size;
		current.read_data_bytes += file->read_size;
		if (file->write_size != BYTES_INVALID)
			current.write_bytes += file->write_size;
	}

	if (verbose)
	{
		printf(_("database backup completed(read: " INT64_FORMAT " write: " INT64_FORMAT ")\n"),
			current.read_data_bytes, current.write_bytes);
		printf(_("========================================\n"));
	}

	return files;
}

/*
 * backup archived WAL incrementally.
 */
static parray *
do_backup_arclog(parray *backup_list)
{
	int			i;
	parray	   *files;
	parray	   *prev_files = NULL;	/* file list of previous database backup */
	FILE	   *fp;
	char		path[MAXPGPATH];
	char		timeline_dir[MAXPGPATH];
	char		prev_file_txt[MAXPGPATH];
	pgBackup   *prev_backup;
	int64		arclog_write_bytes = 0;
	char		last_wal[MAXPGPATH];

	if (!HAVE_ARCLOG(&current))
		return NULL;

	if (verbose)
	{
		printf(_("========================================\n"));
		printf(_("archived WAL backup start\n"));
	}

	/* initialize size summary */
	current.read_arclog_bytes = 0;

	/* switch xlog if database is not backed up */
	if (current.stop_lsn.xrecoff == 0)
		pg_switch_xlog(&current);

	/*
	 * To take incremental backup, the file list of the last completed database
	 * backup is needed.
	 */
	prev_backup = catalog_get_last_arclog_backup(backup_list);
	if (verbose && prev_backup == NULL)
		printf(_("no previous full backup, do a full backup instead\n"));

	if (prev_backup)
	{
		pgBackupGetPath(prev_backup, prev_file_txt, lengthof(prev_file_txt),
			ARCLOG_FILE_LIST);
		prev_files = dir_read_file_list(arclog_path, prev_file_txt);
	}

	/* list files with the logical path. omit ARCLOG_PATH */
	files = parray_new();
	dir_list_file(files, arclog_path, NULL, true, false);

	/* remove WALs archived after pg_stop_backup()/pg_switch_xlog() */
	xlog_fname(last_wal, lengthof(last_wal), current.tli, &current.stop_lsn);
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile *file = (pgFile *) parray_get(files, i);
		char *fname;
		if ((fname = last_dir_separator(file->path)))
			fname++;
		else
			fname = file->path;

		/* to backup backup history files, compare tli/lsn portion only */
		if (strncmp(fname, last_wal, 24) > 0)
		{
			parray_remove(files, i);
			i--;
		}
	}

	pgBackupGetPath(&current, path, lengthof(path), ARCLOG_DIR);
	backup_files(arclog_path, path, files, prev_files, NULL,
				 current.compress_data);

	/* create file list */
	if (!check)
	{
		pgBackupGetPath(&current, path, lengthof(path), ARCLOG_FILE_LIST);
		fp = fopen(path, "wt");
		if (fp == NULL)
			elog(ERROR_SYSTEM, _("can't open file list \"%s\": %s"), path,
				strerror(errno));
		dir_print_file_list(fp, files, arclog_path);
		fclose(fp);
	}

	/* print summary of size of backup files */
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile *file = (pgFile *) parray_get(files, i);
		if (!S_ISREG(file->mode))
			continue;
		current.read_arclog_bytes += file->read_size;
		if (file->write_size != BYTES_INVALID)
		{
			current.write_bytes += file->write_size;
			arclog_write_bytes += file->write_size;
		}
	}

	/*
	 * Backup timeline history files to special directory.
	 * We do this after create file list, because copy_file() update
	 * pgFile->write_size to actual size.
	 */
	join_path_components(timeline_dir, backup_path, TIMELINE_HISTORY_DIR);
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile *file = (pgFile *) parray_get(files, i);
		if (!S_ISREG(file->mode))
			continue;
		if (strstr(file->path, ".history") ==
				file->path + strlen(file->path) - strlen(".history"))
		{
			elog(LOG, _("(timeline history) %s"), file->path);
			copy_file(arclog_path, timeline_dir, file, NO_COMPRESSION);
		}
	}

	if (verbose)
	{
		printf(_("archived WAL backup completed(read: " INT64_FORMAT " write: " INT64_FORMAT ")\n"),
			current.read_arclog_bytes, arclog_write_bytes);
		printf(_("========================================\n"));
	}

	return files;
}

/*
 * Take a backup of serverlog.
 */
static parray *
do_backup_srvlog(parray *backup_list)
{
	int			i;
	parray	   *files;
	parray	   *prev_files = NULL;	/* file list of previous database backup */
	FILE	   *fp;
	char		path[MAXPGPATH];
	char		prev_file_txt[MAXPGPATH];
	pgBackup   *prev_backup;
	int64		srvlog_write_bytes = 0;

	if (!current.with_serverlog)
		return NULL;

	if (verbose)
	{
		printf(_("========================================\n"));
		printf(_("serverlog backup start\n"));
	}

	/* initialize size summary */
	current.read_srvlog_bytes = 0;

	/*
	 * To take incremental backup, the file list of the last completed database
	 * backup is needed.
	 */
	prev_backup = catalog_get_last_srvlog_backup(backup_list);
	if (verbose && prev_backup == NULL)
		printf(_("no previous full backup, do a full backup instead\n"));

	if (prev_backup)
	{
		pgBackupGetPath(prev_backup, prev_file_txt, lengthof(prev_file_txt),
			SRVLOG_FILE_LIST);
		prev_files = dir_read_file_list(srvlog_path, prev_file_txt);
	}

	/* list files with the logical path. omit SRVLOG_PATH */
	files = parray_new();
	dir_list_file(files, srvlog_path, NULL, true, false);

	pgBackupGetPath(&current, path, lengthof(path), SRVLOG_DIR);
	backup_files(srvlog_path, path, files, prev_files, NULL, false);

	/* create file list */
	if (!check)
	{
		pgBackupGetPath(&current, path, lengthof(path), SRVLOG_FILE_LIST);
		fp = fopen(path, "wt");
		if (fp == NULL)
			elog(ERROR_SYSTEM, _("can't open file list \"%s\": %s"), path,
				strerror(errno));
		dir_print_file_list(fp, files, srvlog_path);
		fclose(fp);
	}

	/* print summary of size of backup mode files */
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile *file = (pgFile *) parray_get(files, i);
		if (!S_ISREG(file->mode))
			continue;
		current.read_srvlog_bytes += file->read_size;
		if (file->write_size != BYTES_INVALID)
		{
			current.write_bytes += file->write_size;
			srvlog_write_bytes += file->write_size;
		}
	}

	if (verbose)
	{
		printf(_("serverlog backup completed(read: " INT64_FORMAT " write: " INT64_FORMAT ")\n"),
			current.read_srvlog_bytes, srvlog_write_bytes);
		printf(_("========================================\n"));
	}

	return files;
}

int
do_backup(bool smooth_checkpoint,
		  int keep_arclog_files,
		  int keep_arclog_days,
		  int keep_srvlog_files,
		  int keep_srvlog_days,
		  int keep_data_generations,
		  int keep_data_days)
{
	parray *backup_list;
	parray *files_database;
	parray *files_arclog;
	parray *files_srvlog;
	int		server_version;
	int		ret;

	/* PGDATA and BACKUP_MODE are always required */
	if (pgdata == NULL)
		elog(ERROR_ARGS, _("required parameter not specified: PGDATA (-D, --pgdata)"));

	if (current.backup_mode == BACKUP_MODE_INVALID)
		elog(ERROR_ARGS, _("required parameter not specified: BACKUP_MODE (-b, --backup-mode)"));

	/* ARCLOG_PATH is requried only when backup archive WAL */
	if (HAVE_ARCLOG(&current) && arclog_path == NULL)
		elog(ERROR_ARGS, _("required parameter not specified: ARCLOG_PATH (-A, --arclog-path)"));

	/* SRVLOG_PATH is required only when backup serverlog */
	if (current.with_serverlog && srvlog_path == NULL)
		elog(ERROR_ARGS, _("required parameter not specified: SRVLOG_PATH (-S, --srvlog-path)"));

#ifndef HAVE_LIBZ
	if (current->compress_data)
	{
		elog(WARNING, _("requested compression not available in this: installation -- archive will be uncompressed"));
		current->compress_data = false;
	}
#endif

	/* confirm data block size and xlog block size are compatible */
	server_version = get_server_version();

	/* setup cleanup callback function */
	in_backup = true;

	/* show configuration actually used */
	if (verbose)
	{
		printf(_("========================================\n"));
		printf(_("backup start\n"));
		printf(_("----------------------------------------\n"));
		pgBackupWriteConfigSection(stderr, &current);
		printf(_("----------------------------------------\n"));
	}

	/* get exclusive lock of backup catalog */
	ret = catalog_lock();
	if (ret == -1)
		elog(ERROR_SYSTEM, _("can't lock backup catalog."));
	else if (ret == 1)
		elog(ERROR_ALREADY_RUNNING,
			_("another pg_rman is running, skip this backup."));

	/* initialize backup result */
	current.status = BACKUP_STATUS_RUNNING;
	current.tli = 0;		/* get from result of pg_start_backup() */
	current.start_lsn.xlogid = 0;
	current.start_lsn.xrecoff = 0;
	current.stop_lsn.xlogid = 0;
	current.stop_lsn.xrecoff = 0;
	current.start_time = time(NULL);
	current.end_time = (time_t) 0;
	current.total_data_bytes = BYTES_INVALID;
	current.read_data_bytes = BYTES_INVALID;
	current.read_arclog_bytes = BYTES_INVALID;
	current.read_srvlog_bytes = BYTES_INVALID;
	current.write_bytes = 0;		/* write_bytes is valid always */
	current.block_size = BLCKSZ;
	current.wal_block_size = XLOG_BLCKSZ;

	/* create backup directory and backup.ini */
	if (!check)
	{
		if (pgBackupCreateDir(&current))
			elog(ERROR_SYSTEM, _("can't create backup directory."));
		pgBackupWriteIni(&current);
	}
	if (verbose)
		printf(_("backup destination is initialized.\n"));

	/* get list of backups already taken */
	backup_list = catalog_get_backup_list(NULL);

	/* backup data */
	files_database = do_backup_database(backup_list, smooth_checkpoint);

	/* backup archived WAL */
	files_arclog = do_backup_arclog(backup_list);

	/* backup serverlog */
	files_srvlog = do_backup_srvlog(backup_list);

	/* update backup status to DONE */
	current.end_time = time(NULL);
	current.status = BACKUP_STATUS_DONE;
	if (!check)
		pgBackupWriteIni(&current);

	if (verbose)
	{
		if (TOTAL_READ_SIZE(&current) == 0)
			printf(_("nothing to backup\n"));
		else
			printf(_("all backup completed(read: " INT64_FORMAT " write: "
				INT64_FORMAT ")\n"),
				TOTAL_READ_SIZE(&current), current.write_bytes);
		printf(_("========================================\n"));
	}

	/*
	 * Delete old files (archived WAL and serverlog) after update of status.
	 */
	if (HAVE_ARCLOG(&current))
		delete_old_files(arclog_path, files_arclog, keep_arclog_files,
			keep_arclog_days, server_version, true);
	if (current.with_serverlog)
		delete_old_files(srvlog_path, files_srvlog, keep_srvlog_files,
			keep_srvlog_days, server_version, false);

	/* Delete old backup files after all backup operation. */
	pgBackupDelete(keep_data_generations, keep_data_days);

	/* Cleanup backup mode file list */
	if (files_database)
		parray_walk(files_database, pgFileFree);
	parray_free(files_database);
	if (files_arclog)
		parray_walk(files_arclog, pgFileFree);
	parray_free(files_arclog);
	if (files_srvlog)
		parray_walk(files_srvlog, pgFileFree);
	parray_free(files_srvlog);

	/*
	 * If this backup is full backup, delete backup of online WAL.
	 * Note that sereverlog files which were backed up during first restoration
	 * don't be delete.
	 * Also delete symbolic link in the archive directory.
	 */
	if (current.backup_mode == BACKUP_MODE_FULL)
	{
		delete_online_wal_backup();
		delete_arclog_link();
	}

	/* release catalog lock */
	catalog_unlock();

	return 0;
}

/*
 * get server version and confirm block sizes.
 */
int
get_server_version(void)
{
	static int	server_version = 0;
	bool		my_conn;

	/* return cached server version */
	if (server_version > 0)
		return server_version;

	my_conn = (connection == NULL);

	if (my_conn)
		reconnect();

	/* confirm server version */
	server_version = PQserverVersion(connection);
	if (server_version < 80200)
		elog(ERROR_PG_INCOMPATIBLE,
			_("server version is %d.%d.%d, but must be 8.2 or higher."),
			server_version / 10000,
			(server_version / 100) % 100,
			server_version % 100);

	/* confirm block_size (BLCKSZ) and wal_block_size (XLOG_BLCKSZ) */
	confirm_block_size("block_size", BLCKSZ);
	if (server_version >= 80400)
		confirm_block_size("wal_block_size", XLOG_BLCKSZ);

	if (my_conn)
		disconnect();

	return server_version;
}

static void
confirm_block_size(const char *name, int blcksz)
{
	PGresult   *res;
	char	   *endp;
	int			block_size;

	res = execute("SELECT current_setting($1)", 1, &name);
	if (PQntuples(res) != 1 || PQnfields(res) != 1)
		elog(ERROR_PG_COMMAND, _("can't get %s: %s"),
			name, PQerrorMessage(connection));
	block_size = strtol(PQgetvalue(res, 0, 0), &endp, 10);
	PQclear(res);
	if ((endp && *endp) || block_size != blcksz)
		elog(ERROR_PG_INCOMPATIBLE,
			_("%s(%d) is not compatible(%d expected)"),
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
	int				server_version;

	params[0] = label;

	reconnect();
	server_version = get_server_version();
	if (server_version >= 80400)
	{
		/* 2nd argument is 'fast'*/
		params[1] = smooth ? "false" : "true";
		res = execute("SELECT * from pg_xlogfile_name_offset(pg_start_backup($1, $2))", 2, params);
	}
	else
	{
		/* v8.3 always uses smooth checkpoint */
		if (!smooth && server_version >= 80300)
			command("CHECKPOINT", 0, NULL);
		res = execute("SELECT * from pg_xlogfile_name_offset(pg_start_backup($1))", 1, params);
	}
	if (backup != NULL)
		get_lsn(res, &backup->tli, &backup->start_lsn);
	PQclear(res);
	disconnect();
}

static void
wait_for_archive(pgBackup *backup, const char *sql)
{
	PGresult	   *res;
	char			ready_path[MAXPGPATH];
	int				try_count;

	reconnect();
	res = execute(sql, 0, NULL);
	if (backup != NULL)
	{
		get_lsn(res, &backup->tli, &backup->stop_lsn);
		elog(LOG, _("%s(): tli=%X lsn=%X/%08X"), __FUNCTION__, backup->tli,
			backup->stop_lsn.xlogid, backup->stop_lsn.xrecoff);
	}

	/* get filename from the result of pg_xlogfile_name_offset() */
	snprintf(ready_path, lengthof(ready_path),
		"%s/pg_xlog/archive_status/%s.ready", pgdata, PQgetvalue(res, 0, 0));
	elog(LOG, "%s() wait for %s", __FUNCTION__, ready_path);

	PQclear(res);
	disconnect();

	/* wait until switched WAL is archived */
	try_count = 0;
	while (fileExists(ready_path))
	{
		sleep(1);
		if (interrupted)
			elog(ERROR_INTERRUPTED,
				_("interrupted during waiting for WAL archiving"));
		try_count++;
		if (try_count > TIMEOUT_ARCHIVE)
			elog(ERROR_ARCHIVE_FAILED,
				_("switched WAL could not be archived in %d seconds"),
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
	wait_for_archive(backup,
		"SELECT * FROM pg_xlogfile_name_offset(pg_stop_backup())");
}

/*
 * Force switch to a new transaction log file and update backup->tli.
 */
static void
pg_switch_xlog(pgBackup *backup)
{
	wait_for_archive(backup,
		"SELECT * FROM pg_xlogfile_name_offset(pg_switch_xlog())");
}

/*
 * Get TimeLineID and LSN from result of pg_xlogfile_name_offset().
 */
static void
get_lsn(PGresult *res, TimeLineID *timeline, XLogRecPtr *lsn)
{
	uint32 off_upper;

	if (res == NULL || PQntuples(res) != 1 || PQnfields(res) != 2)
		elog(ERROR_PG_COMMAND,
			_("result of pg_xlogfile_name_offset() is invalid: %s"),
			PQerrorMessage(connection));

	/* get TimeLineID, LSN from result of pg_stop_backup() */
	if (sscanf(PQgetvalue(res, 0, 0), "%08X%08X%08X",
			timeline, &lsn->xlogid, &off_upper) != 3 ||
		sscanf(PQgetvalue(res, 0, 1), "%u", &lsn->xrecoff) != 1)
	{
		elog(ERROR_PG_COMMAND,
			_("result of pg_xlogfile_name_offset() is invalid: %s"),
			PQerrorMessage(connection));
	}

	elog(LOG, "%s():%s %s",
		__FUNCTION__, PQgetvalue(res, 0, 0), PQgetvalue(res, 0, 1));
	lsn->xrecoff += off_upper << 24;
}

/*
 * Return true if the path is a existing regular file.
 */
static bool
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
		if (verbose)
			printf(_("backup_label exists, stop backup\n"));
		pg_stop_backup(NULL);	/* don't care stop_lsn on error case */
	}

	/*
	 * Update status of backup.ini to ERROR.
	 * end_time != 0 means backup finished
	 */
	if (current.status == BACKUP_STATUS_RUNNING && current.end_time == 0)
	{
		if (verbose)
			printf(_("backup is running, update its status to ERROR\n"));
		current.end_time = time(NULL);
		current.status = BACKUP_STATUS_ERROR;
		pgBackupWriteIni(&current);
	}
}

/* take incremental backup. */
static void
backup_files(const char *from_root,
			 const char *to_root,
			 parray *files,
			 parray *prev_files,
			 const XLogRecPtr *lsn,
			 bool compress)
{
	int				i;
	struct timeval	tv;

	/* sort pathname ascending */
	parray_qsort(files, pgFileComparePath);

	gettimeofday(&tv, NULL);

	/* backup a file or create a directory */
	for (i = 0; i < parray_num(files); i++)
	{
		int			ret;
		struct stat	buf;

		pgFile *file = (pgFile *) parray_get(files, i);

		/* check for interrupt */
		if (interrupted)
			elog(ERROR_INTERRUPTED, _("interrupted during backup"));

		/* print progress in verbose mode */
		if (verbose)
			printf(_("(%d/%lu) %s "), i + 1, (unsigned long) parray_num(files),
				file->path + strlen(from_root) + 1);

		/* stat file to get file type, size and modify timestamp */
		ret = stat(file->path, &buf);
		if (ret == -1)
		{
			if (errno == ENOENT)
			{
				/* record as skipped file in file_xxx.txt */
				file->write_size = BYTES_INVALID;
				if (verbose)
					printf(_("skip\n"));
				continue;
			}
			else
			{
				if (verbose)
					printf("\n");
				elog(ERROR_SYSTEM,
					_("can't stat backup mode. \"%s\": %s"),
					file->path, strerror(errno));
			}
		}

		/* if the entry was a directory, create it in the backup */
		if (S_ISDIR(buf.st_mode))
		{
			char dirpath[MAXPGPATH];

			join_path_components(dirpath, to_root, file->path + strlen(from_root) + 1);
			if (!check)
				dir_create_dir(dirpath, DIR_PERMISSION);
			if (verbose)
				printf(_("directory\n"));
		}
		else if (S_ISREG(buf.st_mode))
		{
			/* skip files which have not been modified since last backup */
			if (prev_files)
			{
				pgFile **p;
				pgFile *prev_file = NULL;

				p = (pgFile **) parray_bsearch(prev_files, file,
						pgFileComparePath);
				if (p)
					prev_file = *p;

				if (prev_file && prev_file->mtime >= file->mtime)
				{
					/* record as skipped file in file_xxx.txt */
					file->write_size = BYTES_INVALID;
					if (verbose)
						printf(_("skip\n"));
					continue;
				}
			}

			/*
			 * We will wait until the next second of mtime so that backup
			 * file should contain all modifications at the clock of mtime.
			 * timer resolution of ext3 file system is one second.
			 */
			if (tv.tv_sec <= file->mtime)
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
					? backup_data_file(from_root, to_root, file, lsn, compress)
					: copy_file(from_root, to_root, file,
								compress ? COMPRESSION : NO_COMPRESSION)))
			{
				/* record as skipped file in file_xxx.txt */
				file->write_size = BYTES_INVALID;
				if (verbose)
					printf(_("skip\n"));
				continue;
			}

			if (verbose)
			{
				/* print compression rate */
				if (file->write_size != file->size)
					printf(_("compressed %lu (%.2f%% of %lu)\n"),
						(unsigned long) file->write_size,
						100.0 * file->write_size / file->size,
						(unsigned long) file->size);
				else
					printf(_("copied %lu\n"), (unsigned long) file->write_size);
			}

		}
		else
		{
			if (verbose)
				printf(_(" unexpected file type %d\n"), buf.st_mode);
		}
	}
}

/*
 * Delete files modified before than KEEP_xxx_DAYS or more than KEEP_xxx_FILES
 * of newer files exist.
 */
static void
delete_old_files(const char *root,
				 parray *files,
				 int keep_files,
				 int keep_days,
				 int server_version,
				 bool is_arclog)
{
	int		i;
	int		j;
	int		file_num = 0;
	time_t	days_threshold = current.start_time - (keep_days * 60 * 60 * 24);

	if (verbose)
	{
		char files_str[100];
		char days_str[100];

		if (keep_files == KEEP_INFINITE)
			strncpy(files_str, "INFINITE", lengthof(files_str));
		else
			snprintf(files_str, lengthof(files_str), "%d", keep_files);

		if (keep_days == KEEP_INFINITE)
			strncpy(days_str, "INFINITE", lengthof(days_str));
		else
			snprintf(days_str, lengthof(days_str), "%d", keep_days);

		printf(_("delete old files from \"%s\" (files=%s, days=%s)\n"),
			root, files_str, days_str);
	}

	/* delete files which satisfy both conditions */
	if (keep_files == KEEP_INFINITE || keep_days == KEEP_INFINITE)
	{
		elog(LOG, "%s() infinite", __FUNCTION__);
		return;
	}

	parray_qsort(files, pgFileCompareMtime);
	for (i = parray_num(files) - 1; i >= 0; i--)
	{
		pgFile *file = (pgFile *) parray_get(files, i);

		elog(LOG, "%s() %s", __FUNCTION__, file->path);
		/* Delete completed WALs only. */
		if (is_arclog && !xlog_is_complete_wal(file, server_version))
		{
			elog(LOG, "%s() not complete WAL", __FUNCTION__);
			continue;
		}

		file_num++;

		/*
		 * If the mtime of the file is older than the threshold and there are
		 * enough number of files newer than the files, delete the file.
		 */
		if (file->mtime >= days_threshold)
		{
			elog(LOG, "%s() %lu is not older than %lu", __FUNCTION__,
				file->mtime, days_threshold);
			continue;
		}
		elog(LOG, "%s() %lu is older than %lu", __FUNCTION__,
			file->mtime, days_threshold);

		if (file_num <= keep_files)
		{
			elog(LOG, "%s() newer files are only %d", __FUNCTION__, file_num);
			continue;
		}

		/* Now we found a file should be deleted. */
		if (verbose)
			printf(_("delete \"%s\"\n"), file->path + strlen(root) + 1);

		/* delete corresponding backup history file if exists */
		file = (pgFile *) parray_remove(files, i);
		for (j = parray_num(files) - 1; j >= 0; j--)
		{
			pgFile *file2 = (pgFile *)parray_get(files, j);
			if (strstr(file2->path, file->path) == file2->path)
			{
				file2 = (pgFile *)parray_remove(files, j);
				if (verbose)
					printf(_("delete \"%s\"\n"),
						file2->path + strlen(root) + 1);
				if (!check)
					pgFileDelete(file2);
				pgFileFree(file2);
			}
		}
		if (!check)
			pgFileDelete(file);
		pgFileFree(file);
	}
}

static void
delete_online_wal_backup(void)
{
	int i;
	parray *files = parray_new();
	char work_path[MAXPGPATH];

	if (verbose)
	{
		printf(_("========================================\n"));
		printf(_("delete online WAL backup\n"));
	}

	snprintf(work_path, lengthof(work_path), "%s/%s/%s", backup_path,
		RESTORE_WORK_DIR, PG_XLOG_DIR);
	/* don't delete root dir */
	dir_list_file(files, work_path, NULL, true, false);
	if (parray_num(files) == 0)
	{
		parray_free(files);
		return;
	}

	parray_qsort(files, pgFileComparePathDesc);	/* delete from leaf */
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile *file = (pgFile *) parray_get(files, i);
		if (verbose)
			printf(_("delete \"%s\"\n"), file->path);
		if (!check)
			pgFileDelete(file);
	}

	parray_walk(files, pgFileFree);
	parray_free(files);
}

/*
 * Remove symbolic links point archived WAL in backup catalog.
 */
static void
delete_arclog_link(void)
{
	int i;
	parray *files = parray_new();

	if (verbose)
	{
		printf(_("========================================\n"));
		printf(_("delete symbolic link in archive directory\n"));
	}

	dir_list_file(files, arclog_path, NULL, false, false);
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile *file = (pgFile *) parray_get(files, i);

		if (!S_ISLNK(file->mode))
			continue;

		if (verbose)
			printf(_("delete \"%s\"\n"), file->path);

		if (!check && remove(file->path) == -1)
			elog(ERROR_SYSTEM, _("can't remove link \"%s\": %s"), file->path,
				strerror(errno));
	}

	parray_walk(files, pgFileFree);
	parray_free(files);
}
