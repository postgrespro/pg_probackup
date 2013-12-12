/*-------------------------------------------------------------------------
 *
 * backup.c: backup DB cluster, archived WAL, serverlog.
 *
 * Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_rman.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <dirent.h>
#include <time.h>

#include "catalog/pg_control.h"
#include "libpq/pqsignal.h"
#include "pgut/pgut-port.h"

/* wait 10 sec until WAL archive complete */
#define TIMEOUT_ARCHIVE		10

/* Server version */
static int server_version = 0;

static bool		 in_backup = false;	/* TODO: more robust logic */
/* List of commands to execute at error processing for snapshot */
static parray	*cleanup_list;

/*
 * Backup routines
 */
static void backup_cleanup(bool fatal, void *userdata);
static void delete_old_files(const char *root,
							 parray *files, int keep_files,
							 int keep_days, bool is_arclog);
static void backup_files(const char *from_root, const char *to_root,
	parray *files, parray *prev_files, const XLogRecPtr *lsn, bool compress, const char *prefix);
static parray *do_backup_database(parray *backup_list, pgBackupOption bkupopt);
static parray *do_backup_arclog(parray *backup_list);
static parray *do_backup_srvlog(parray *backup_list);
static void remove_stopinfo_from_backup_label(char *history_file, char *bkup_label);
static void make_backup_label(parray *backup_list);
static void confirm_block_size(const char *name, int blcksz);
static void pg_start_backup(const char *label, bool smooth, pgBackup *backup);
static void pg_stop_backup(pgBackup *backup);
static void pg_switch_xlog(pgBackup *backup);
static void get_lsn(PGresult *res, XLogRecPtr *lsn);
static void get_xid(PGresult *res, uint32 *xid);
static bool execute_restartpoint(pgBackupOption bkupopt);

static void delete_arclog_link(void);
static void delete_online_wal_backup(void);

static bool dirExists(const char *path);

static void add_files(parray *files, const char *root, bool add_root, bool is_pgdata);
static int strCompare(const void *str1, const void *str2);
static void create_file_list(parray *files, const char *root, const char *prefix, bool is_append);
static TimeLineID get_current_timeline(void);

/*
 * Take a backup of database.
 */
static parray *
do_backup_database(parray *backup_list, pgBackupOption bkupopt)
{
	int			i;
	parray	   *files;				/* backup file list from non-snapshot */
	parray	   *prev_files = NULL;	/* file list of previous database backup */
	FILE	   *fp;
	char		path[MAXPGPATH];
	char		label[1024];
	XLogRecPtr *lsn = NULL;
	char		prev_file_txt[MAXPGPATH];	/* path of the previous backup list file */
	bool		has_backup_label  = true;	/* flag if backup_label is there  */
	bool		has_recovery_conf = false;	/* flag if recovery.conf is there */

	/* repack the options */
	bool	smooth_checkpoint = bkupopt.smooth_checkpoint;

	if (!HAVE_DATABASE(&current)) {
		/* check if arclog backup. if arclog backup and no suitable full backup, */
		/* take full backup instead. */
		if (HAVE_ARCLOG(&current)) {
			pgBackup   *prev_backup;

			/* find last completed database backup */
			prev_backup = catalog_get_last_data_backup(backup_list);
			if (prev_backup == NULL)
			{
				elog(ERROR_SYSTEM, _("There is indeed a full backup but it is not validated."
							"So I can't take any arclog backup."
							"Please validate it and retry."));
///				elog(INFO, _("no previous full backup, performing a full backup instead"));
///				current.backup_mode = BACKUP_MODE_FULL;
			}
		}
		else
			return NULL;
	}

	elog(INFO, _("database backup start"));

	/* initialize size summary */
	current.total_data_bytes = 0;
	current.read_data_bytes = 0;

	/*
	 * Obtain current timeline by scanning control file, theh LSN
	 * obtained at output of pg_start_backup or pg_stop_backup does
	 * not contain this information.
	 */
	current.tli = get_current_timeline();

	/* notify start of backup to PostgreSQL server */
	time2iso(label, lengthof(label), current.start_time);
	strncat(label, " with pg_rman", lengthof(label));
	pg_start_backup(label, smooth_checkpoint, &current);

	/* If backup_label does not exist in $PGDATA, stop taking backup */
	snprintf(path, lengthof(path), "%s/backup_label", pgdata);
	make_native_path(path);
	if (!fileExists(path)) {
		has_backup_label = false;
	}
	snprintf(path, lengthof(path), "%s/recovery.conf", pgdata);
	make_native_path(path);
	if (fileExists(path)) {
		has_recovery_conf = true;
	}
	if (!has_backup_label && !has_recovery_conf)
	{
		if (verbose)
			printf(_("backup_label does not exist, stop backup\n"));
		pg_stop_backup(NULL);
		elog(ERROR_SYSTEM, _("backup_label does not exist in PGDATA."));
	}
	else if (has_recovery_conf)
	{

		if (!bkupopt.standby_host || !bkupopt.standby_port)
		{
			pg_stop_backup(NULL);
			elog(ERROR_SYSTEM, _("could not specified standby host or port."));
		}
		if (!execute_restartpoint(bkupopt))
		{
			pg_stop_backup(NULL);
			elog(ERROR_SYSTEM, _("could not execute restartpoint."));
		}
		current.is_from_standby = true;
	}

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
		pgBackup   *prev_backup;

		/* find last completed database backup */
		prev_backup = catalog_get_last_data_backup(backup_list);
		if (prev_backup == NULL || prev_backup->tli != current.tli)
		{
			elog(ERROR_SYSTEM, _("There is indeed a full backup but it is not validated."
						"So I can't take any incremental backup."
						"Please validate it and retry."));
///			elog(INFO, _("no previous full backup, performing a full backup instead"));
///			current.backup_mode = BACKUP_MODE_FULL;
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
				 (uint32) (*lsn >> 32), (uint32) *lsn);
		}
	}

	/* initialize backup list from non-snapshot */
	files = parray_new();
	join_path_components(path, backup_path, SNAPSHOT_SCRIPT_FILE);

	/*
	 * Check the existence of the snapshot-script.
	 * backup use snapshot when snapshot-script exists.
	 */
	if (fileExists(path))
	{
		parray		*tblspc_list;	/* list of name of TABLESPACE backup from snapshot */
		parray		*tblspcmp_list;	/* list of mounted directory of TABLESPACE in snapshot volume */
		PGresult	*tblspc_res;	/* contain spcname and oid in TABLESPACE */

		tblspc_list = parray_new();
		tblspcmp_list = parray_new();
		cleanup_list = parray_new();

		/*
		 * append 'pg_tblspc' to list of directory excluded from copy.
		 * because DB cluster and TABLESPACE are copied separately.
		 */
		for (i = 0; pgdata_exclude[i]; i++);	/* find first empty slot */
		pgdata_exclude[i] = PG_TBLSPC_DIR;

		/*
		 * when DB cluster is not contained in the backup from the snapshot,
		 * DB cluster is added to the backup file list from non-snapshot.
		 */
		parray_qsort(tblspc_list, strCompare);
		if (parray_bsearch(tblspc_list, "PG-DATA", strCompare) == NULL)
			add_files(files, pgdata, false, true);
		else
			/* remove the detected tablespace("PG-DATA") from tblspc_list */
			parray_rm(tblspc_list, "PG-DATA", strCompare);

		/*
		 * select the TABLESPACE backup from non-snapshot,
		 * and append TABLESPACE to the list backup from non-snapshot.
		 * TABLESPACE name and oid is obtained by inquiring of the database.
		 */

		reconnect();
		tblspc_res = execute("SELECT spcname, oid FROM pg_tablespace WHERE "
			"spcname NOT IN ('pg_default', 'pg_global') ORDER BY spcname ASC", 0, NULL);
		disconnect();
		for (i = 0; i < PQntuples(tblspc_res); i++)
		{
			char *name = PQgetvalue(tblspc_res, i, 0);
			char *oid = PQgetvalue(tblspc_res, i, 1);

			/* when not found, append it to the backup list from non-snapshot */
			if (parray_bsearch(tblspc_list, name, strCompare) == NULL)
			{
				char dir[MAXPGPATH];
				join_path_components(dir, pgdata, PG_TBLSPC_DIR);
				join_path_components(dir, dir, oid);
				add_files(files, dir, true, false);
			}
			else
				/* remove the detected tablespace from tblspc_list */
				parray_rm(tblspc_list, name, strCompare);
		}

		/*
		 * tblspc_list is not empty,
		 * so snapshot-script output the tablespace name that not exist.
		 */
		if (parray_num(tblspc_list) > 0)
			elog(ERROR_SYSTEM, _("snapshot-script output the name of tablespace that not exist"));

		/* clear array */
		parray_walk(tblspc_list, free);
		parray_free(tblspc_list);

		/* backup files from non-snapshot */
		pgBackupGetPath(&current, path, lengthof(path), DATABASE_DIR);
		backup_files(pgdata, path, files, prev_files, lsn, current.compress_data, NULL);

		/* notify end of backup */
		pg_stop_backup(&current);

		/* create file list of non-snapshot objects */
		create_file_list(files, pgdata, NULL, false);

		/* backup files from snapshot volume */
		for (i = 0; i < parray_num(tblspcmp_list); i++)
		{
			char *spcname;
			char *mp = NULL;
			char *item = (char *) parray_get(tblspcmp_list, i);
			parray *snapshot_files = parray_new();

			/*
			 * obtain the TABLESPACE name and the directory where it is stored.
			 * Note: strtok() replace the delimiter to '\0'. but no problem because
			 *       it doesn't use former value
			 */
			if ((spcname = strtok(item, "=")) == NULL || (mp = strtok(NULL, "\0")) == NULL)
				elog(ERROR_SYSTEM, _("snapshot-script output illegal format: %s"), item);

			if (verbose)
			{
				printf(_("========================================\n"));
				printf(_("backup files from snapshot: \"%s\"\n"), spcname);
			}

			/* tablespace storage directory not exist */
			if (!dirExists(mp))
				elog(ERROR_SYSTEM, _("tablespace storage directory doesn't exist: %s"), mp);

			/*
			 * create the previous backup file list to take incremental backup
			 * from the snapshot volume.
			 */
			if (prev_files != NULL)
				prev_files = dir_read_file_list(mp, prev_file_txt);

			/* when DB cluster is backup from snapshot, it backup from the snapshot */
			if (strcmp(spcname, "PG-DATA") == 0)
			{
				/* append DB cluster to backup file list */
				add_files(snapshot_files, mp, false, true);
				/* backup files of DB cluster from snapshot volume */
				backup_files(mp, path, snapshot_files, prev_files, lsn, current.compress_data, NULL);
				/* create file list of snapshot objects (DB cluster) */
				create_file_list(snapshot_files, mp, NULL, true);
				/* remove the detected tablespace("PG-DATA") from tblspcmp_list */
				parray_rm(tblspcmp_list, "PG-DATA", strCompare);
				i--;
			}
			/* backup TABLESPACE from snapshot volume */
			else
			{
				int j;

				/*
				 * obtain the oid from TABLESPACE information acquired by inquiring of database.
				 * and do backup files of TABLESPACE from snapshot volume.
				 */
				for (j = 0; j < PQntuples(tblspc_res); j++)
				{
					char  dest[MAXPGPATH];
					char  prefix[MAXPGPATH];
					char *name = PQgetvalue(tblspc_res, j, 0);
					char *oid = PQgetvalue(tblspc_res, j, 1);

					if (strcmp(spcname, name) == 0)
					{
						/* append TABLESPACE to backup file list */
						add_files(snapshot_files, mp, true, false);

						/* backup files of TABLESPACE from snapshot volume */
						join_path_components(prefix, PG_TBLSPC_DIR, oid);
						join_path_components(dest, path, prefix);
						backup_files(mp, dest, snapshot_files, prev_files, lsn, current.compress_data, prefix);

						/* create file list of snapshot objects (TABLESPACE) */
						create_file_list(snapshot_files, mp, prefix, true);
						/* remove the detected tablespace("PG-DATA") from tblspcmp_list */
						parray_rm(tblspcmp_list, spcname, strCompare);
						i--;
						break;
					}
				}
			}
			parray_concat(files, snapshot_files);
		}

		/*
		 * tblspcmp_list is not empty,
		 * so snapshot-script output the tablespace name that not exist.
		 */
		if (parray_num(tblspcmp_list) > 0)
			elog(ERROR_SYSTEM, _("snapshot-script output the name of tablespace that not exist"));

		/* clear array */
		parray_walk(tblspcmp_list, free);
		parray_free(tblspcmp_list);


		/* don't use 'parray_walk'. element of parray not allocate memory by malloc */
		parray_free(cleanup_list);
		PQclear(tblspc_res);
	}
	/* when snapshot-script not exist, DB cluster and TABLESPACE are backup
	 * at same time.
	 */
	else
	{
		/* list files with the logical path. omit $PGDATA */
		add_files(files, pgdata, false, true);

		/* backup files */
		pgBackupGetPath(&current, path, lengthof(path), DATABASE_DIR);
		backup_files(pgdata, path, files, prev_files, lsn, current.compress_data, NULL);

		/* notify end of backup */
		pg_stop_backup(&current);

		/* if backup is from standby, making backup_label from	*/
		/* backup.history file.					*/
		if (current.is_from_standby)
			make_backup_label(files);

		/* create file list */
		create_file_list(files, pgdata, NULL, false);
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

static bool
execute_restartpoint(pgBackupOption bkupopt)
{
	PGconn *sby_conn = NULL;
	const char *tmp_host;
	const char *tmp_port;
	tmp_host = pgut_get_host();
	tmp_port = pgut_get_port();
	pgut_set_host(bkupopt.standby_host);
	pgut_set_port(bkupopt.standby_port);
	sby_conn = reconnect_elevel(ERROR_PG_CONNECT);
	if (!sby_conn)
		return false;
	command("CHECKPOINT", 0, NULL);
	pgut_set_host(tmp_host);
	pgut_set_port(tmp_port);
	return true;
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
	if ((uint32) current.stop_lsn == 0)
		pg_switch_xlog(&current);

	/*
	 * To take incremental backup, the file list of the last completed
	 * database backup is needed.
	 */
	prev_backup = catalog_get_last_arclog_backup(backup_list);
	if (verbose && prev_backup == NULL)
		printf(_("no previous full backup, performing a full backup instead\n"));

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
				 current.compress_data, NULL);

	/* create file list */
	if (!check)
	{
		pgBackupGetPath(&current, path, lengthof(path), ARCLOG_FILE_LIST);
		fp = fopen(path, "wt");
		if (fp == NULL)
			elog(ERROR_SYSTEM, _("can't open file list \"%s\": %s"), path,
				strerror(errno));
		dir_print_file_list(fp, files, arclog_path, NULL);
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
		printf(_("no previous full backup, performing a full backup instead\n"));

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
	backup_files(srvlog_path, path, files, prev_files, NULL, false, NULL);

	/* create file list */
	if (!check)
	{
		pgBackupGetPath(&current, path, lengthof(path), SRVLOG_FILE_LIST);
		fp = fopen(path, "wt");
		if (fp == NULL)
			elog(ERROR_SYSTEM, _("can't open file list \"%s\": %s"), path,
				strerror(errno));
		dir_print_file_list(fp, files, srvlog_path, NULL);
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
do_backup(pgBackupOption bkupopt)
{
	parray *backup_list;
	parray *files_database;
	parray *files_arclog;
	parray *files_srvlog;
	int		ret;

	/* repack the necesary options */
	int	keep_arclog_files = bkupopt.keep_arclog_files;
	int	keep_arclog_days  = bkupopt.keep_arclog_days;
	int	keep_srvlog_files = bkupopt.keep_srvlog_files;
	int	keep_srvlog_days  = bkupopt.keep_srvlog_days;
	int	keep_data_generations = bkupopt.keep_data_generations;
	int	keep_data_days        = bkupopt.keep_data_days;

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
	if (current.compress_data)
	{
		elog(WARNING, _("requested compression not available in this: installation -- archive will be uncompressed"));
		current.compress_data = false;
	}
#endif

	/* confirm data block size and xlog block size are compatible */
	check_server_version();

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
	current.start_lsn = 0;
	current.stop_lsn = 0;
	current.start_time = time(NULL);
	current.end_time = (time_t) 0;
	current.total_data_bytes = BYTES_INVALID;
	current.read_data_bytes = BYTES_INVALID;
	current.read_arclog_bytes = BYTES_INVALID;
	current.read_srvlog_bytes = BYTES_INVALID;
	current.write_bytes = 0;		/* write_bytes is valid always */
	current.block_size = BLCKSZ;
	current.wal_block_size = XLOG_BLCKSZ;
	current.recovery_xid = 0;
	current.recovery_time = (time_t) 0;
	current.is_from_standby = false;

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
	if(!backup_list){
		elog(ERROR_SYSTEM, _("can't process any more."));
	}

	/* set the error processing function for the backup process */
	pgut_atexit_push(backup_cleanup, NULL);

	/* backup data */
	files_database = do_backup_database(backup_list, bkupopt);

	/* backup archived WAL */
	files_arclog = do_backup_arclog(backup_list);

	/* backup serverlog */
	files_srvlog = do_backup_srvlog(backup_list);
	pgut_atexit_pop(backup_cleanup, NULL);

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
			keep_arclog_days, true);
	if (current.with_serverlog)
		delete_old_files(srvlog_path, files_srvlog, keep_srvlog_files,
			keep_srvlog_days, false);

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

void
remove_stopinfo_from_backup_label(char *history_file, char *bkup_label)
{
	FILE	*read;
	FILE	*write;
	char	buf[MAXPGPATH * 2];

	if ((read  = fopen(history_file, "r")) == NULL)
		elog(ERROR_SYSTEM,
			_("can't open backup history file for standby backup."));
	if ((write = fopen(bkup_label, "w")) == NULL)
		elog(ERROR_SYSTEM,
			_("can't open backup_label file for standby backup."));
	while (fgets(buf, lengthof(buf), read) != NULL)
	{
		if (strstr(buf, "STOP") - buf == 0)
			continue;
		fputs(buf, write);
	}
	fclose(write);
	fclose(read);
}

/*
 *  creating backup_label from backup.history for standby backup.
 */
void
make_backup_label(parray *backup_list)
{
	char dest_path[MAXPGPATH];
	char src_bkup_history_file[MAXPGPATH];
	char dst_bkup_label_file[MAXPGPATH];
	char original_bkup_label_file[MAXPGPATH];
	parray *bkuped_arc_files = NULL;
	int i;

	pgBackupGetPath(&current, dest_path, lengthof(dest_path), DATABASE_DIR);
	bkuped_arc_files = parray_new();
	dir_list_file(bkuped_arc_files, arclog_path, NULL, true, false);

	for (i = parray_num(bkuped_arc_files) - 1; i >= 0; i--)
	{
		char *current_arc_fname;
		pgFile *current_arc_file;

		current_arc_file = (pgFile *) parray_get(bkuped_arc_files, i);
		current_arc_fname = last_dir_separator(current_arc_file->path) + 1;

		if(strlen(current_arc_fname) <= 24) continue;

		copy_file(arclog_path, dest_path, current_arc_file, NO_COMPRESSION);
		join_path_components(src_bkup_history_file, dest_path, current_arc_fname);
		join_path_components(dst_bkup_label_file, dest_path, PG_BACKUP_LABEL_FILE);
		join_path_components(original_bkup_label_file, pgdata, PG_BACKUP_LABEL_FILE);
		remove_stopinfo_from_backup_label(src_bkup_history_file, dst_bkup_label_file);

		dir_list_file(backup_list, dst_bkup_label_file, NULL, false, true);
		for (i = 0; i < parray_num(backup_list); i++)
		{
			pgFile *file = (pgFile *)parray_get(backup_list, i);
			if (strcmp(file->path, dst_bkup_label_file) == 0)
			{
				struct stat st;
				stat(dst_bkup_label_file, &st);
				file->write_size = st.st_size;
				file->crc        = pgFileGetCRC(file);
				strcpy(file->path, original_bkup_label_file);
			}
		}
		parray_qsort(backup_list, pgFileComparePath);
		break;
	}
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
	if (server_version != PG_VERSION_NUM)
		elog(ERROR_PG_INCOMPATIBLE,
			_("server version is %d.%d.%d, must be %s or higher."),
			 server_version / 10000,
			 (server_version / 100) % 100,
			 server_version % 100, PG_MAJORVERSION);

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
wait_for_archive(pgBackup *backup, const char *sql)
{
	PGresult	   *res;
	char			ready_path[MAXPGPATH];
	char			file_name[MAXFNAMELEN];
	int				try_count;
	XLogRecPtr		lsn;
	TimeLineID		tli;

	reconnect();
	res = execute(sql, 0, NULL);

	/* Get LSN from execution result */
	get_lsn(res, &lsn);

	/*
	 * Enforce TLI obtention if backup is not present as this code
	 * path can be taken as a callback at exit.
	 */
	if (backup != NULL)
		tli = backup->tli;
	else
		tli = get_current_timeline();

	/* Fill in fields if backup exists */
	if (backup != NULL)
	{
		backup->stop_lsn = lsn;
		elog(LOG, _("%s(): tli=%X lsn=%X/%08X"),
			 __FUNCTION__, backup->tli,
			 (uint32) (backup->stop_lsn >> 32),
			 (uint32) backup->stop_lsn);
	}

	/* As well as WAL file name */
	XLogFileName(file_name, tli, lsn);

	snprintf(ready_path, lengthof(ready_path),
		"%s/pg_xlog/archive_status/%s.ready", pgdata,
			 file_name);
	elog(LOG, "%s() wait for %s", __FUNCTION__, ready_path);

	PQclear(res);

	res = execute(TXID_CURRENT_SQL, 0, NULL);
	if(backup != NULL){
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
		"SELECT * FROM pg_stop_backup()");
}

/*
 * Force switch to a new transaction log file
 */
static void
pg_switch_xlog(pgBackup *backup)
{
	wait_for_archive(backup,
		"SELECT * FROM pg_switch_xlog())");
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
		elog(ERROR_PG_COMMAND,
			_("result of backup command is invalid: %s"),
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
	if(res == NULL || PQntuples(res) != 1 || PQnfields(res) != 1)
		elog(ERROR_PG_COMMAND,
			_("result of txid_current() is invalid: %s"),
			PQerrorMessage(connection));

	if(sscanf(PQgetvalue(res, 0, 0), "%u", xid) != 1)
	{
		elog(ERROR_PG_COMMAND,
			_("result of txid_current() is invalid: %s"),
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
 * Return true if the path is a existing directory.
 */
static bool
dirExists(const char *path)
{
	struct stat buf;

	if (stat(path, &buf) == -1 && errno == ENOENT)
		return false;
	else if (S_ISREG(buf.st_mode))
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
			 bool compress,
			 const char *prefix)
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

		/* If current time is rewinded, abort this backup. */
		if(tv.tv_sec < file->mtime){
			elog(ERROR_SYSTEM, _("current time may be rewound. Please retry with full backup mode."));
		}

		/* check for interrupt */
		if (interrupted)
			elog(ERROR_INTERRUPTED, _("interrupted during backup"));

		/* print progress in verbose mode */
		if (verbose)
		{
			if (prefix)
			{
				char path[MAXPGPATH];
				join_path_components(path, prefix, file->path + strlen(from_root) + 1);
				printf(_("(%d/%lu) %s "), i + 1, (unsigned long) parray_num(files), path);
			}
			else
				printf(_("(%d/%lu) %s "), i + 1, (unsigned long) parray_num(files),
					file->path + strlen(from_root) + 1);
		}

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

			join_path_components(dirpath, to_root, JoinPathEnd(file->path, from_root));
			if (!check){
				dir_create_dir(dirpath, DIR_PERMISSION);
			}
			if (verbose)
				printf(_("directory\n"));
		}
		else if (S_ISREG(buf.st_mode))
		{
			/* skip files which have not been modified since last backup */
			if (prev_files)
			{
				pgFile *prev_file = NULL;

				/*
				 * If prefix is not NULL, the table space is backup from the snapshot.
				 * Therefore, adjust file name to correspond to the file list.
				 */
				if (prefix)
				{
					int j;

					for (j = 0; j < parray_num(prev_files); j++)
					{
						pgFile *p = (pgFile *) parray_get(prev_files, j);
						char *prev_path;
						char curr_path[MAXPGPATH];

						prev_path = p->path + strlen(from_root) + 1;
						join_path_components(curr_path, prefix, file->path + strlen(from_root) + 1);
						if (strcmp(curr_path, prev_path) == 0)
						{
							prev_file = p;
							break;
						}
					}
				}
				else
				{
					pgFile **p = (pgFile **) parray_bsearch(prev_files, file, pgFileComparePath);
					if (p)
						prev_file = *p;
				}

				if (prev_file && prev_file->mtime == file->mtime)
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
		if (is_arclog && !xlog_is_complete_wal(file))
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

		/* data file must be a regular file */
		if (!S_ISREG(file->mode))
			continue;

		/* data files are under "base", "global", or "pg_tblspc" */
		relative = file->path + strlen(root) + 1;
		if (is_pgdata &&
			!path_is_prefix_of_path("base", relative) &&
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
	parray_concat(files, list_file);
}

/*
 * Comparison function for parray_bsearch() compare the character string.
 */
static int
strCompare(const void *str1, const void *str2)
{
	return strcmp(*(char **) str1, *(char **) str2);
}

/*
 * Output the list of backup files to backup catalog
 */
static void
create_file_list(parray *files, const char *root, const char *prefix, bool is_append)
{
	FILE	*fp;
	char	 path[MAXPGPATH];

	if (!check)
	{
		/* output path is '$BACKUP_PATH/file_database.txt' */
		pgBackupGetPath(&current, path, lengthof(path), DATABASE_FILE_LIST);
		fp = fopen(path, is_append ? "at" : "wt");
		if (fp == NULL)
			elog(ERROR_SYSTEM, _("can't open file list \"%s\": %s"), path,
				strerror(errno));
		dir_print_file_list(fp, files, root, prefix);
		fclose(fp);
	}
}

/*
 * Scan control file of given cluster at obtain the current timeline
 * since last checkpoint that occurred on it.
 */
static TimeLineID
get_current_timeline(void)
{
	char	   *buffer;
	size_t		size;
	ControlFileData control_file;

	/* First fetch file... */
	buffer = slurpFile(pgdata, "global/pg_control", &size);

	/* .. Then interpret it */
    if (size != PG_CONTROL_SIZE)
		elog(ERROR_CORRUPTED, "unexpected control file size %d, expected %d\n",
			 (int) size, PG_CONTROL_SIZE);
	memcpy(&control_file, buffer, sizeof(ControlFileData));

	/* Finally return the timeline wanted */
	return control_file.checkPointCopy.ThisTimeLineID;
}
