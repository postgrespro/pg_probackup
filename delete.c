/*-------------------------------------------------------------------------
 *
 * delete.c: delete backup files.
 *
 * Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_rman.h"

static int pgBackupDeleteFiles(pgBackup *backup);
static bool checkIfDeletable(pgBackup *backup);

int
do_delete(pgBackupRange *range, bool force)
{
	int		i;
	int		ret;
	parray *backup_list;
	bool	do_delete;
	bool	force_delete;

	/* DATE are always required */
	if (!pgBackupRangeIsValid(range))
		elog(ERROR_ARGS, _("required delete range option not specified: delete DATE"));

	/* Lock backup catalog */
	ret = catalog_lock();
	if (ret == -1)
		elog(ERROR_SYSTEM, _("can't lock backup catalog."));
	else if (ret == 1)
		elog(ERROR_ALREADY_RUNNING,
			_("another pg_rman is running, stop delete."));

	/* Get complete list of backup */
	backup_list = catalog_get_backup_list(NULL);
	if (!backup_list)
		elog(ERROR_SYSTEM, _("No backup list found, can't process any more."));

	/* Find backups to be deleted */
	do_delete = false;
	force_delete = false;
	for (i = 0; i < parray_num(backup_list); i++)
	{
		pgBackup *backup = (pgBackup *)parray_get(backup_list, i);

		if(force)
			force_delete = checkIfDeletable(backup);

		/* delete backup and update status to DELETED */
		if (do_delete || force_delete)
		{
			/* check for interrupt */
			if (interrupted)
				elog(ERROR_INTERRUPTED, _("interrupted during delete backup"));

			pgBackupDeleteFiles(backup);
			continue;
		}

		/* Found the latest full backup */
		if (backup->backup_mode >= BACKUP_MODE_FULL &&
			backup->status == BACKUP_STATUS_OK &&
			backup->start_time <= range->begin)
			do_delete = true;
	}

	/* release catalog lock */
	catalog_unlock();

	/* cleanup */
	parray_walk(backup_list, pgBackupFree);
	parray_free(backup_list);

	return 0;
}

/*
 * Delete backups that are older than KEEP_xxx_DAYS and have more generations
 * than KEEP_xxx_FILES.
 */
void
pgBackupDelete(int keep_generations, int keep_days)
{
	int		i;
	parray *backup_list;
	int		backup_num;
	time_t	days_threshold = current.start_time - (keep_days * 60 * 60 * 24);


	if (verbose)
	{
		char generations_str[100];
		char days_str[100];

		if (keep_generations == KEEP_INFINITE)
			strncpy(generations_str, "INFINITE",
					lengthof(generations_str));
		else
			snprintf(generations_str, lengthof(generations_str),
					"%d", keep_generations);

		if (keep_days == KEEP_INFINITE)
			strncpy(days_str, "INFINITE", lengthof(days_str));
		else
			snprintf(days_str, lengthof(days_str), "%d", keep_days);

		printf(_("delete old backups (generations=%s, days=%s)\n"),
			generations_str, days_str);
	}

	/* Leave if an infinite generation of backups is kept */
	if (keep_generations == KEEP_INFINITE && keep_days == KEEP_INFINITE)
	{
		elog(LOG, "%s() infinite", __FUNCTION__);
		return;
	}

	/* Get a complete list of backups. */
	backup_list = catalog_get_backup_list(NULL);

	/* Find target backups to be deleted */
	backup_num = 0;
	for (i = 0; i < parray_num(backup_list); i++)
	{
		pgBackup *backup = (pgBackup *)parray_get(backup_list, i);

		elog(LOG, "%s() %lu", __FUNCTION__, backup->start_time);

		/*
		 * When a validate full backup was found, we can delete the
		 * backup that is older than it.
		 */
		if (backup->backup_mode >= BACKUP_MODE_FULL &&
			backup->status == BACKUP_STATUS_OK)
			backup_num++;

		/* Evaluate if this backup is eligible for removal */
		if (backup_num <= keep_generations)
		{
			/* Do not include the latest full backup in this count */
			elog(LOG, "%s() backup are only %d", __FUNCTION__, backup_num);
			continue;
		}
		else if (backup->start_time >= days_threshold)
		{
			/*
			 * If the start time of the backup is older than the threshold and
			 * there are enough generations of full backups, delete the backup.
			 */
			elog(LOG, "%s() %lu is not older than %lu", __FUNCTION__,
				backup->start_time, days_threshold);
			continue;
		}

		elog(LOG, "%s() %lu is older than %lu", __FUNCTION__,
			backup->start_time, days_threshold);

		/* delete backup and update status to DELETED */
		pgBackupDeleteFiles(backup);
	}

	/* cleanup */
	parray_walk(backup_list, pgBackupFree);
	parray_free(backup_list);
}

/*
 * Delete backup files of the backup and update the status of the backup to
 * BACKUP_STATUS_DELETED.
 */
static int
pgBackupDeleteFiles(pgBackup *backup)
{
	int		i;
	char	path[MAXPGPATH];
	char	timestamp[20];
	parray *files;

	/*
	 * If the backup was deleted already, nothing to do and such situation
	 * is not error.
	 */
	if (backup->status == BACKUP_STATUS_DELETED)
		return 0;

	time2iso(timestamp, lengthof(timestamp), backup->start_time);

	elog(INFO, _("delete: %s"), timestamp);

	/*
	 * update STATUS to BACKUP_STATUS_DELETING in preparation for the case which
	 * the error occurs before deleting all backup files.
	 */
	if (!check)
	{
		backup->status = BACKUP_STATUS_DELETING;
		pgBackupWriteIni(backup);
	}

	/* list files to be deleted */
	files = parray_new();
	pgBackupGetPath(backup, path, lengthof(path), DATABASE_DIR);
	dir_list_file(files, path, NULL, true, true);
	pgBackupGetPath(backup, path, lengthof(path), ARCLOG_DIR);
	dir_list_file(files, path, NULL, true, true);
	pgBackupGetPath(backup, path, lengthof(path), SRVLOG_DIR);
	dir_list_file(files, path, NULL, true, true);

	/* delete leaf node first */
	parray_qsort(files, pgFileComparePathDesc);
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile *file = (pgFile *) parray_get(files, i);

		/* print progress */
		elog(LOG, _("delete file(%d/%lu) \"%s\"\n"), i + 1,
				(unsigned long) parray_num(files), file->path);

		/* skip actual deletion in check mode */
		if (!check)
		{
			if (remove(file->path))
			{
				elog(WARNING, _("can't remove \"%s\": %s"), file->path,
					strerror(errno));
				parray_walk(files, pgFileFree);
				parray_free(files);
				return 1;
			}
		}
	}

	/*
	 * After deleting all of the backup files, update STATUS to
	 * BACKUP_STATUS_DELETED.
	 */
	if (!check)
	{
		backup->status = BACKUP_STATUS_DELETED;
		pgBackupWriteIni(backup);
	}

	parray_walk(files, pgFileFree);
	parray_free(files);

	return 0;
}

bool
checkIfDeletable(pgBackup *backup)
{
	/* find latest full backup. */
	if (backup->status != BACKUP_STATUS_OK &&
		backup->status != BACKUP_STATUS_DELETED &&
		backup->status != BACKUP_STATUS_DONE)
		return true;

	return false;
}
