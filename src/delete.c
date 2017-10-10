/*-------------------------------------------------------------------------
 *
 * delete.c: delete backup files.
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <dirent.h>
#include <time.h>
#include <unistd.h>

static int pgBackupDeleteFiles(pgBackup *backup);
static void delete_walfiles(XLogRecPtr oldest_lsn, TimeLineID oldest_tli);

int
do_delete(time_t backup_id)
{
	int			i;
	parray	   *backup_list,
			   *delete_list;
	time_t		parent_id = 0;
	bool		backup_found = false;
	XLogRecPtr	oldest_lsn = InvalidXLogRecPtr;
	TimeLineID	oldest_tli = 0;

	/* Get exclusive lock of backup catalog */
	catalog_lock();

	/* Get complete list of backups */
	backup_list = catalog_get_backup_list(INVALID_BACKUP_ID);
	if (backup_list == NULL)
		elog(ERROR, "Failed to get backup list.");

	if (backup_id != 0)
	{
		delete_list = parray_new();

		/* Find backup to be deleted and make increment backups array to be deleted */
		for (i = (int) parray_num(backup_list) - 1; i >= 0; i--)
		{
			pgBackup   *backup = (pgBackup *) parray_get(backup_list, (size_t) i);

			if (backup->start_time == backup_id)
			{
				parray_append(delete_list, backup);

				/*
				* Do not remove next backups, if target backup was finished
				* incorrectly.
				*/
				if (backup->status == BACKUP_STATUS_ERROR)
					break;

				/* Save backup id to retreive increment backups */
				parent_id = backup->start_time;
				backup_found = true;
			}
			else if (backup_found)
			{
				if (backup->backup_mode != BACKUP_MODE_FULL &&
					backup->parent_backup == parent_id)
				{
					/* Append to delete list increment backup */
					parray_append(delete_list, backup);
					/* Save backup id to retreive increment backups */
					parent_id = backup->start_time;
				}
				else
					break;
			}
		}

		if (parray_num(delete_list) == 0)
			elog(ERROR, "no backup found, cannot delete");

		/* Delete backups from the end of list */
		for (i = (int) parray_num(delete_list) - 1; i >= 0; i--)
		{
			pgBackup   *backup = (pgBackup *) parray_get(delete_list, (size_t) i);

			if (interrupted)
				elog(ERROR, "interrupted during delete backup");

			pgBackupDeleteFiles(backup);
		}

		parray_free(delete_list);
	}

	/* Clean WAL segments */
	if (delete_wal)
	{
		/* Find oldest LSN, used by backups */
		for (i = (int) parray_num(backup_list) - 1; i >= 0; i--)
		{
			pgBackup   *backup = (pgBackup *) parray_get(backup_list, (size_t) i);

			if (backup->status == BACKUP_STATUS_OK)
			{
				oldest_lsn = backup->start_lsn;
				oldest_tli = backup->tli;
				break;
			}
		}

		delete_walfiles(oldest_lsn, oldest_tli);
	}

	/* cleanup */
	parray_walk(backup_list, pgBackupFree);
	parray_free(backup_list);

	return 0;
}

/*
 * Remove backups by retention policy. Retention policy is configured by
 * retention_redundancy and retention_window variables.
 */
int
do_retention_purge(void)
{
	parray	   *backup_list;
	uint32		backup_num;
	size_t		i;
	time_t		days_threshold = time(NULL) - (retention_window * 60 * 60 * 24);
	XLogRecPtr	oldest_lsn = InvalidXLogRecPtr;
	TimeLineID	oldest_tli = 0;
	bool		keep_next_backup = true;	/* Do not delete first full backup */
	bool		backup_deleted = false;		/* At least one backup was deleted */

	if (retention_redundancy > 0)
		elog(LOG, "REDUNDANCY=%u", retention_redundancy);
	if (retention_window > 0)
		elog(LOG, "WINDOW=%u", retention_window);

	if (retention_redundancy == 0 && retention_window == 0)
		elog(ERROR, "retention policy is not set");

	/* Get exclusive lock of backup catalog */
	catalog_lock();

	/* Get a complete list of backups. */
	backup_list = catalog_get_backup_list(INVALID_BACKUP_ID);
	if (parray_num(backup_list) == 0)
	{
		elog(INFO, "backup list is empty, purging won't be executed");
		return 0;
	}

	/* Find target backups to be deleted */
	backup_num = 0;
	for (i = 0; i < parray_num(backup_list); i++)
	{
		pgBackup   *backup = (pgBackup *) parray_get(backup_list, i);
		uint32		backup_num_evaluate = backup_num;

		/* Consider only validated and correct backups */
		if (backup->status != BACKUP_STATUS_OK)
			continue;

		/*
		 * When a validate full backup was found, we can delete the
		 * backup that is older than it using the number of generations.
		 */
		if (backup->backup_mode == BACKUP_MODE_FULL)
			backup_num++;

		/* Evaluate if this backup is eligible for removal */
		if (keep_next_backup ||
			backup_num_evaluate + 1 <= retention_redundancy ||
			(retention_window > 0 && backup->recovery_time >= days_threshold))
		{
			/* Save LSN and Timeline to remove unnecessary WAL segments */
			oldest_lsn = backup->start_lsn;
			oldest_tli = backup->tli;

			/* Save parent backup of this incremental backup */
			if (backup->backup_mode != BACKUP_MODE_FULL)
				keep_next_backup = true;
			/*
			 * Previous incremental backup was kept or this is first backup
			 * so do not delete this backup.
			 */
			else
				keep_next_backup = false;

			continue;
		}

		/* Delete backup and update status to DELETED */
		pgBackupDeleteFiles(backup);
		backup_deleted = true;
	}

	/* Purge WAL files */
	delete_walfiles(oldest_lsn, oldest_tli);

	/* Cleanup */
	parray_walk(backup_list, pgBackupFree);
	parray_free(backup_list);

	if (backup_deleted)
		elog(INFO, "Purging finished");
	else
		elog(INFO, "Nothing to delete by retention policy");

	return 0;
}

/*
 * Delete backup files of the backup and update the status of the backup to
 * BACKUP_STATUS_DELETED.
 */
static int
pgBackupDeleteFiles(pgBackup *backup)
{
	size_t		i;
	char	   *backup_id;
	char		path[MAXPGPATH];
	char		timestamp[100];
	parray	   *files;

	/*
	 * If the backup was deleted already, there is nothing to do.
	 */
	if (backup->status == BACKUP_STATUS_DELETED)
		return 0;

	backup_id = base36enc(backup->start_time);
	time2iso(timestamp, lengthof(timestamp), backup->recovery_time);

	elog(INFO, "delete: %s %s", backup_id, timestamp);
	free(backup_id);

	/*
	 * Update STATUS to BACKUP_STATUS_DELETING in preparation for the case which
	 * the error occurs before deleting all backup files.
	 */
	backup->status = BACKUP_STATUS_DELETING;
	pgBackupWriteBackupControlFile(backup);

	/* list files to be deleted */
	files = parray_new();
	pgBackupGetPath(backup, path, lengthof(path), NULL);
	dir_list_file(files, path, false, true, true);

	/* delete leaf node first */
	parray_qsort(files, pgFileComparePathDesc);
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);

		/* print progress */
		elog(LOG, "delete file(%zd/%lu) \"%s\"", i + 1,
				(unsigned long) parray_num(files), file->path);

		if (remove(file->path))
		{
			elog(WARNING, "can't remove \"%s\": %s", file->path,
				strerror(errno));
			parray_walk(files, pgFileFree);
			parray_free(files);

			return 1;
		}
	}

	parray_walk(files, pgFileFree);
	parray_free(files);
	backup->status = BACKUP_STATUS_DELETED;

	return 0;
}

/*
 * Deletes WAL segments up to oldest_lsn or all WAL segments (if all backups
 * was deleted and so oldest_lsn is invalid).
 *
 *  oldest_lsn - if valid, function deletes WAL segments, which contain lsn
 *    older than oldest_lsn. If it is invalid function deletes all WAL segments.
 *  oldest_tli - is used to construct oldest WAL segment in addition to
 *    oldest_lsn.
 */
static void
delete_walfiles(XLogRecPtr oldest_lsn, TimeLineID oldest_tli)
{
	XLogSegNo   targetSegNo;
	char		oldestSegmentNeeded[MAXFNAMELEN];
	DIR		   *arcdir;
	struct dirent *arcde;
	char		wal_file[MAXPGPATH];
	char		max_wal_file[MAXPGPATH];
	char		min_wal_file[MAXPGPATH];
	int			rc;

	max_wal_file[0] = '\0';
	min_wal_file[0] = '\0';

	if (!XLogRecPtrIsInvalid(oldest_lsn))
	{
		XLByteToSeg(oldest_lsn, targetSegNo);
		XLogFileName(oldestSegmentNeeded, oldest_tli, targetSegNo);

		elog(LOG, "removing WAL segments older than %s", oldestSegmentNeeded);
	}
	else
		elog(LOG, "removing all WAL segments");

	/*
	 * Now it is time to do the actual work and to remove all the segments
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
			if (IsXLogFileName(arcde->d_name) ||
				IsPartialXLogFileName(arcde->d_name) ||
				IsBackupHistoryFileName(arcde->d_name))
			{
				if (XLogRecPtrIsInvalid(oldest_lsn) ||
					strncmp(arcde->d_name + 8, oldestSegmentNeeded + 8, 16) < 0)
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

					if (max_wal_file[0] == '\0' ||
						strcmp(max_wal_file + 8, arcde->d_name + 8) < 0)
						strcpy(max_wal_file, arcde->d_name);

					if (min_wal_file[0] == '\0' ||
						strcmp(min_wal_file + 8, arcde->d_name + 8) > 0)
						strcpy(min_wal_file, arcde->d_name);
				}
			}
		}

		if (min_wal_file[0] != '\0')
			elog(INFO, "removed min WAL segment \"%s\"", min_wal_file);
		if (max_wal_file[0] != '\0')
			elog(INFO, "removed max WAL segment \"%s\"", max_wal_file);

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


/* Delete all backup files and wal files of given instance. */
int
do_delete_instance(void)
{
	parray	   *backup_list;
	int i;
	char		instance_config_path[MAXPGPATH];

	/* Delete all backups. */
	backup_list = catalog_get_backup_list(INVALID_BACKUP_ID);

	for (i = 0; i < parray_num(backup_list); i++)
	{
		pgBackup   *backup = (pgBackup *) parray_get(backup_list, i);
		pgBackupDeleteFiles(backup);
	}

	/* Cleanup */
	parray_walk(backup_list, pgBackupFree);
	parray_free(backup_list);

	/* Delete all wal files. */
	delete_walfiles(InvalidXLogRecPtr, 0);

	/* Delete backup instance config file */
	join_path_components(instance_config_path, backup_instance_path, BACKUP_CATALOG_CONF_FILE);
	if (remove(instance_config_path))
	{
		elog(ERROR, "can't remove \"%s\": %s", instance_config_path,
			strerror(errno));
	}

	/* Delete instance root directories */
	if (rmdir(backup_instance_path) != 0)
		elog(ERROR, "can't remove \"%s\": %s", backup_instance_path,
			strerror(errno));
	if (rmdir(arclog_path) != 0)
		elog(ERROR, "can't remove \"%s\": %s", backup_instance_path,
			strerror(errno));

	elog(INFO, "Instance '%s' successfully deleted", instance_name);
	return 0;
}
