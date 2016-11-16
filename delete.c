/*-------------------------------------------------------------------------
 *
 * delete.c: delete backup files.
 *
 * Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <dirent.h>
#include <unistd.h>

static int pgBackupDeleteFiles(pgBackup *backup);
int do_deletewal(time_t backup_id, bool strict);

int
do_delete(time_t backup_id)
{
	int			i;
	int			b_index;
	int			ret;
	parray		*backup_list;
	pgBackup	*last_backup;

	/* DATE are always required */
	if (backup_id == 0)
		elog(ERROR, "required backup ID not specified");

	/* Lock backup catalog */
	ret = catalog_lock();
	if (ret == -1)
		elog(ERROR, "can't lock backup catalog.");
	else if (ret == 1)
		elog(ERROR,
			"another pg_probackup is running, stop delete.");

	/* Get complete list of backups */
	backup_list = catalog_get_backup_list(0);
	if (!backup_list)
		elog(ERROR, "No backup list found, can't process any more.");

	/* Find backup to be deleted */
	for (i = 0; i < parray_num(backup_list); i++)
	{
		last_backup = (pgBackup *) parray_get(backup_list, i);
		if (last_backup->status == BACKUP_STATUS_OK &&
			last_backup->start_time == backup_id
		)
			goto found_backup;
	}

	elog(ERROR, "no backup found, cannot delete.");

found_backup:
	b_index = i;
	/* check for interrupt */
	if (interrupted)
		elog(ERROR, "interrupted during delete backup");

	/* just do it */
	pgBackupDeleteFiles(last_backup);

	/* remove all increments after removed backup */
	for (i = b_index - 1; i >= 0; i--)
	{
		pgBackup *backup = (pgBackup *) parray_get(backup_list, i);
		if (backup->backup_mode >= BACKUP_MODE_FULL)
			break;
		if (backup->status == BACKUP_STATUS_OK ||
			backup->backup_mode == BACKUP_MODE_DIFF_PAGE ||
			backup->backup_mode == BACKUP_MODE_DIFF_PTRACK
		)
			pgBackupDeleteFiles(backup);
	}

	/* release catalog lock */
	catalog_unlock();

	/* cleanup */
	parray_walk(backup_list, pgBackupFree);
	parray_free(backup_list);

	if (delete_wal)
		do_deletewal(backup_id, false);

	return 0;
}

int do_deletewal(time_t backup_id, bool strict)
{
	int			i;
	int			ret;
	parray		*backup_list;
	XLogRecPtr	oldest_lsn = InvalidXLogRecPtr;
	TimeLineID	oldest_tli;
	pgBackup	*last_backup;
	bool		backup_found = false;

	/*
	 * Delete in archive WAL segments that are not needed anymore. The oldest
	 * segment to be kept is the first segment that the oldest full backup
	 * found around needs to keep.
	 */
	/* Lock backup catalog */
	ret = catalog_lock();
	if (ret == -1)
		elog(ERROR, "can't lock backup catalog.");
	else if (ret == 1)
		elog(ERROR,
			"another pg_probackup is running, stop delete.");

	backup_list = catalog_get_backup_list(0);
	for (i = 0; i < parray_num(backup_list); i++)
	{
		last_backup = (pgBackup *) parray_get(backup_list, i);
		if (last_backup->status == BACKUP_STATUS_OK)
		{
			oldest_lsn = last_backup->start_lsn;
			oldest_tli = last_backup->tli;
			if (strict && backup_id != 0 && backup_id >= last_backup->start_time)
			{
				backup_found = true;
				break;
			}
		}
	}
	if (strict && backup_id != 0 && backup_found == false)
		elog(ERROR, "not found backup for deletwal command");
	catalog_unlock();
	parray_walk(backup_list, pgBackupFree);
	parray_free(backup_list);

	if (!XLogRecPtrIsInvalid(oldest_lsn))
	{
		XLogSegNo   targetSegNo;
		char		oldestSegmentNeeded[MAXFNAMELEN];
		DIR		   *arcdir;
		struct dirent *arcde;
		char		wal_file[MAXPGPATH];
		int			rc;

		XLByteToSeg(oldest_lsn, targetSegNo);
		XLogFileName(oldestSegmentNeeded, oldest_tli, targetSegNo);
		elog(LOG, "Removing segments older than %s", oldestSegmentNeeded);

		/*
		 * Now is time to do the actual work and to remove all the segments
		 * not needed anymore.
		 */
		if ((arcdir = opendir(arclog_path)) != NULL)
		{
			while (errno = 0, (arcde = readdir(arcdir)) != NULL)
			{
				/*
				 * We ignore the timeline part of the XLOG segment identifiers in
				 * deciding whether a segment is still needed.  This ensures that
				 * we won't prematurely remove a segment from a parent timeline.
				 * We could probably be a little more proactive about removing
				 * segments of non-parent timelines, but that would be a whole lot
				 * more complicated.
				 *
				 * We use the alphanumeric sorting property of the filenames to
				 * decide which ones are earlier than the exclusiveCleanupFileName
				 * file. Note that this means files are not removed in the order
				 * they were originally written, in case this worries you.
				 */
				if ((IsXLogFileName(arcde->d_name) ||
					 IsPartialXLogFileName(arcde->d_name)) &&
					strcmp(arcde->d_name + 8, oldestSegmentNeeded + 8) < 0)
				{
					/*
					 * Use the original file name again now, including any
					 * extension that might have been chopped off before testing
					 * the sequence.
					 */
					snprintf(wal_file, MAXPGPATH, "%s/%s",
							 arclog_path, arcde->d_name);

					rc = unlink(wal_file);
					if (rc != 0)
					{
						elog(WARNING, "could not remove file \"%s\": %s",
							 wal_file, strerror(errno));
						break;
					}
					elog(LOG, "removed WAL segment \"%s\"", wal_file);
				}
			}
			if (errno)
				elog(WARNING, "could not read archive location \"%s\": %s",
					 arclog_path, strerror(errno));
			if (closedir(arcdir))
				elog(WARNING, "could not close archive location \"%s\": %s",
					 arclog_path, strerror(errno));
		}
		else
			elog(WARNING, "could not open archive location \"%s\": %s",
				 arclog_path, strerror(errno));
	}

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

		elog(LOG, "deleted old backups (generations=%s, days=%s)",
			 generations_str, days_str);
	}

	/* Leave if an infinite generation of backups is kept */
	if (keep_generations == KEEP_INFINITE && keep_days == KEEP_INFINITE)
	{
		elog(LOG, "%s() infinite", __FUNCTION__);
		return;
	}

	/* Get a complete list of backups. */
	backup_list = catalog_get_backup_list(0);

	/* Find target backups to be deleted */
	backup_num = 0;
	for (i = 0; i < parray_num(backup_list); i++)
	{
		pgBackup   *backup = (pgBackup *) parray_get(backup_list, i);
		int			backup_num_evaluate = backup_num;

		elog(LOG, "%s() %lu", __FUNCTION__, backup->start_time);

		/*
		 * When a validate full backup was found, we can delete the
		 * backup that is older than it using the number of generations.
		 */
		if (backup->backup_mode == BACKUP_MODE_FULL &&
			backup->status == BACKUP_STATUS_OK)
			backup_num++;

		/* Evaluate if this backup is eligible for removal */
		if (backup_num_evaluate + 1 <= keep_generations &&
			keep_generations != KEEP_INFINITE)
		{
			/* Do not include the latest full backup in this count */
			elog(LOG, "%s() backup are only %d", __FUNCTION__, backup_num);
			continue;
		}
		else if (backup->start_time >= days_threshold &&
				 keep_days != KEEP_INFINITE)
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
	 * If the backup was deleted already, there is nothing to do.
	 */
	if (backup->status == BACKUP_STATUS_DELETED)
		return 0;

	time2iso(timestamp, lengthof(timestamp), backup->start_time);

	elog(INFO, "delete: %s %s", base36enc(backup->start_time), timestamp);


	/*
	 * Update STATUS to BACKUP_STATUS_DELETING in preparation for the case which
	 * the error occurs before deleting all backup files.
	 */
	if (!check)
	{
		backup->status = BACKUP_STATUS_DELETING;
		pgBackupWriteIni(backup);
	}

	/* list files to be deleted */
	files = parray_new();
	pgBackupGetPath(backup, path, lengthof(path), NULL);
	dir_list_file(files, path, NULL, true, true);

	/* delete leaf node first */
	parray_qsort(files, pgFileComparePathDesc);
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile *file = (pgFile *) parray_get(files, i);

		/* print progress */
		elog(LOG, "delete file(%d/%lu) \"%s\"", i + 1,
				(unsigned long) parray_num(files), file->path);

		/* skip actual deletion in check mode */
		if (!check)
		{
			if (remove(file->path))
			{
				elog(WARNING, "can't remove \"%s\": %s", file->path,
					strerror(errno));
				parray_walk(files, pgFileFree);
				parray_free(files);
				return 1;
			}
		}
	}

	parray_walk(files, pgFileFree);
	parray_free(files);

	return 0;
}
