/*-------------------------------------------------------------------------
 *
 * clean.c: cleanup backup files.
 *
 * Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_rman.h"

#include <dirent.h>
#include <unistd.h>

static void clean_backup(Database db, pgBackup *backup);

#define CLEAN_MASK		(BACKUP_MASK(BACKUP_ERROR) | BACKUP_MASK(BACKUP_BAD))

void
do_clean(int keep_data_generations,
		 int keep_data_days,
		 int keep_srvlog_files,
		 int keep_srvlog_days)
{
	Database	db;
	List	   *backups;
	ListCell   *cell;

	db = db_open();
	backups = db_list_backups(db, make_range(0, NULL), CLEAN_MASK);

	foreach (cell, backups)
		clean_backup(db, lfirst(cell));

	db_close(db);
	list_free_deep(backups);
}

/*
 * Delete files of the backup and update the status to DELETED.
 */
static void
clean_backup(Database db, pgBackup *backup)
{
	char		datetime[DATESTRLEN];
	char		path[MAXPGPATH];

	elog(INFO, "clean: %s", date2str(datetime, backup->start_time));

	/*
	 * update the status to BAD before the actual deletion because abort
	 * during deletion could leave corrupted backup files.
	 */
	if (backup->status != BACKUP_BAD)
	{
		backup->status = BACKUP_BAD;
		db_update_status(db, backup, NIL);
	}

	/* remove data files. */
	make_backup_path(path, backup->start_time);
	remove_file(path);

	/* update the status to DELETED */
	backup->status = BACKUP_DELETED;
	db_update_status(db, backup, NIL);
}

#if 0
/* that are older than KEEP_xxx_DAYS and have more generations
 * than KEEP_xxx_FILES.
 */
	int		i;
	parray *backup_list;
	int		backup_num;
	time_t	days_threshold = current.start_time - (keep_days * 60 * 60 * 24);

	/* cleanup files which satisfy both condition */
	if (keep_generations == KEEP_INFINITE || keep_days == KEEP_INFINITE)
	{
		elog(LOG, "%s() infinite", __FUNCTION__);
		return;
	}

	/* get list of backups. */
	backup_list = catalog_get_backup_list(NULL);

/*
 * Remove backup files 
 */
void
clean_backup(int keep_generations, int keep_days)
{
	backup_num = 0;
	/* find cleanup target backup. */
	for (i = 0; i < parray_num(backup_list); i++)
	{
		pgBackup *backup = (pgBackup *)parray_get(backup_list, i);

		elog(LOG, "%s() %lu", __FUNCTION__, backup->start_time);
		/*
		 * when verify full backup was found, we can cleanup the backup
		 * that is older than it
		 */
		if (backup->backup_mode >= BACKUP_MODE_FULL &&
			backup->status == BACKUP_OK)
			backup_num++;

		/* do not include the latest full backup in a count. */
		if (backup_num - 1 <= keep_generations)
		{
			elog(LOG, "%s() backup are only %d", __FUNCTION__, backup_num);
			continue;
		}

		/*
		 * If the start time of the backup is older than the threshold and
		 * there are enough generations of full backups, cleanup the backup.
		 */
		if (backup->start_time >= days_threshold)
		{
			elog(LOG, "%s() %lu is not older than %lu", __FUNCTION__,
				backup->start_time, days_threshold);
			continue;
		}
	}
}





static void delete_old_files(const char *root, parray *files, int keep_files,
							 int keep_days, int server_version, bool is_arclog);
static void delete_arclog_link(void);
static void delete_online_wal_backup(void);

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
	time_t	days_threshold = start_time - (keep_days * 60 * 60 * 24);

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

		printf("cleanup old files from \"%s\" (files=%s, days=%s)\n",
			root, files_str, days_str);
	}

	/* cleanup files which satisfy both conditions */
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
		if (is_arclog && !xlog_completed(file, server_version))
		{
			elog(LOG, "%s() not complete WAL", __FUNCTION__);
			continue;
		}

		file_num++;

		/*
		 * If the mtime of the file is older than the threshold and there are
		 * enough number of files newer than the files, cleanup the file.
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
		printf("========================================\n"));
		printf("cleanup online WAL backup\n"));
	}

	snprintf(work_path, lengthof(work_path), "%s/%s/%s", backup_path,
		RESTORE_WORK_DIR, PG_XLOG_DIR);
	/* don't cleanup root dir */
	files = pgFileEnum(work_path, NULL, true, false);
	if (parray_num(files) == 0)
	{
		parray_free(files);
		return;
	}

	parray_qsort(files, pgFileComparePathDesc);	/* cleanup from leaf */
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile *file = (pgFile *) parray_get(files, i);
		if (verbose)
			printf("cleanup \"%s\"\n", file->path);
		if (!check)
			pgFileDelete(file);
	}

	parray_walk(files, pgFile_free);
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
		printf("========================================\n"));
		printf("cleanup symbolic link in archive directory\n"));
	}

	files = pgFileEnum(arclog_path, NULL, false, false);
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile *file = (pgFile *) parray_get(files, i);

		if (!S_ISLNK(file->mode))
			continue;

		if (verbose)
			printf("cleanup \"%s\"\n", file->path);

		if (!check && remove(file->path) == -1)
			elog(ERROR_SYSTEM, "could not remove link \"%s\": %s", file->path,
				strerror(errno));
	}

	parray_walk(files, pgFile_free);
	parray_free(files);
}


	/*
	 * Delete old files (archived WAL and serverlog) after update of status.
	 */
	if (HAVE_ARCLOG(&current))
		delete_old_files(arclog_path, files_arclog, keep_arclog_files,
			keep_arclog_days, server_version, true);

	/* Delete old backup files after all backup operation. */
	clean_backup(keep_data_generations, keep_data_days);

	/*
	 * If this backup is full backup, cleanup backup of online WAL.
	 * Note that sereverlog files which were backed up during first restoration
	 * don't be cleanup.
	 * Also cleanup symbolic link in the archive directory.
	 */
	if (backup_mode == BACKUP_MODE_FULL)
	{
		delete_online_wal_backup();
		delete_arclog_link();
	}

#endif
