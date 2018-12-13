/*-------------------------------------------------------------------------
 *
 * delete.c: delete backup files.
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <dirent.h>
#include <time.h>
#include <unistd.h>

static void delete_walfiles(XLogRecPtr oldest_lsn, TimeLineID oldest_tli,
							uint32 xlog_seg_size);

void
do_delete(time_t backup_id)
{
	int			i;
	parray	   *backup_list,
			   *delete_list;
	pgBackup   *target_backup = NULL;
	time_t		parent_id = 0;
	XLogRecPtr	oldest_lsn = InvalidXLogRecPtr;
	TimeLineID	oldest_tli = 0;

	/* Get exclusive lock of backup catalog */
	catalog_lock();

	/* Get complete list of backups */
	backup_list = catalog_get_backup_list(INVALID_BACKUP_ID);

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
				target_backup = backup;
			}
			else if (target_backup)
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

			delete_backup_files(backup);
		}

		parray_free(delete_list);
	}

	/* Clean WAL segments */
	if (delete_wal)
	{
		Assert(target_backup);

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

		delete_walfiles(oldest_lsn, oldest_tli, instance_config.xlog_seg_size);
	}

	/* cleanup */
	parray_walk(backup_list, pgBackupFree);
	parray_free(backup_list);
}

/*
 * Remove backups by retention policy. Retention policy is configured by
 * retention_redundancy and retention_window variables.
 */
int
do_retention_purge(void)
{
	parray	   *backup_list;
	size_t		i;
	XLogRecPtr	oldest_lsn = InvalidXLogRecPtr;
	TimeLineID	oldest_tli = 0;
	bool		keep_next_backup = true;	/* Do not delete first full backup */
	bool		backup_deleted = false;		/* At least one backup was deleted */

	if (delete_expired)
	{
		if (instance_config.retention_redundancy > 0)
			elog(LOG, "REDUNDANCY=%u", instance_config.retention_redundancy);
		if (instance_config.retention_window > 0)
			elog(LOG, "WINDOW=%u", instance_config.retention_window);

		if (instance_config.retention_redundancy == 0
			&& instance_config.retention_window == 0)
		{
			elog(WARNING, "Retention policy is not set");
			if (!delete_wal)
				return 0;
		}
	}

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
	if (delete_expired &&
		(instance_config.retention_redundancy > 0 ||
		 instance_config.retention_window > 0))
	{
		time_t		days_threshold;
		uint32		backup_num = 0;

		days_threshold = time(NULL) -
			(instance_config.retention_window * 60 * 60 * 24);

		for (i = 0; i < parray_num(backup_list); i++)
		{
			pgBackup   *backup = (pgBackup *) parray_get(backup_list, i);
			uint32		backup_num_evaluate = backup_num;

			/* Consider only validated and correct backups */
			if (backup->status != BACKUP_STATUS_OK)
				continue;
			/*
			 * When a valid full backup was found, we can delete the
			 * backup that is older than it using the number of generations.
			 */
			if (backup->backup_mode == BACKUP_MODE_FULL)
				backup_num++;

			/* Evaluate retention_redundancy if this backup is eligible for removal */
			if (keep_next_backup ||
				instance_config.retention_redundancy >= backup_num_evaluate + 1 ||
				(instance_config.retention_window > 0 &&
				 backup->recovery_time >= days_threshold))
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
			delete_backup_files(backup);
			backup_deleted = true;
		}
	}

	/*
	 * If oldest_lsn and oldest_tli weren`t set because previous step was skipped
	 * then set them now if we are going to purge WAL
	 */
	if (delete_wal && (XLogRecPtrIsInvalid(oldest_lsn)))
	{
		pgBackup   *backup = (pgBackup *) parray_get(backup_list, parray_num(backup_list) - 1);
		oldest_lsn = backup->start_lsn;
		oldest_tli = backup->tli;
	}

	/* Be paranoid */
	if (XLogRecPtrIsInvalid(oldest_lsn))
		elog(ERROR, "Not going to purge WAL because LSN is invalid");

	/* Purge WAL files */
	if (delete_wal)
	{
		delete_walfiles(oldest_lsn, oldest_tli, instance_config.xlog_seg_size);
	}

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
void
delete_backup_files(pgBackup *backup)
{
	size_t		i;
	char		path[MAXPGPATH];
	char		timestamp[100];
	parray	   *files;
	size_t		num_files;

	/*
	 * If the backup was deleted already, there is nothing to do.
	 */
	if (backup->status == BACKUP_STATUS_DELETED)
	{
		elog(WARNING, "Backup %s already deleted",
			 base36enc(backup->start_time));
		return;
	}

	time2iso(timestamp, lengthof(timestamp), backup->recovery_time);

	elog(INFO, "Delete: %s %s",
		 base36enc(backup->start_time), timestamp);

	/*
	 * Update STATUS to BACKUP_STATUS_DELETING in preparation for the case which
	 * the error occurs before deleting all backup files.
	 */
	backup->status = BACKUP_STATUS_DELETING;
	write_backup_status(backup);

	/* list files to be deleted */
	files = parray_new();
	pgBackupGetPath(backup, path, lengthof(path), NULL);
	dir_list_file(files, path, false, true, true, 0);

	/* delete leaf node first */
	parray_qsort(files, pgFileComparePathDesc);
	num_files = parray_num(files);
	for (i = 0; i < num_files; i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);

		if (progress)
			elog(INFO, "Progress: (%zd/%zd). Process file \"%s\"",
				 i + 1, num_files, file->path);

		if (remove(file->path))
		{
			if (errno == ENOENT)
				elog(VERBOSE, "File \"%s\" is absent", file->path);
			else
				elog(ERROR, "Cannot remove \"%s\": %s", file->path,
					 strerror(errno));
			return;
		}
	}

	parray_walk(files, pgFileFree);
	parray_free(files);
	backup->status = BACKUP_STATUS_DELETED;

	return;
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
delete_walfiles(XLogRecPtr oldest_lsn, TimeLineID oldest_tli,
				uint32 xlog_seg_size)
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
		GetXLogSegNo(oldest_lsn, targetSegNo, xlog_seg_size);
		GetXLogFileName(oldestSegmentNeeded, oldest_tli, targetSegNo,
						xlog_seg_size);

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
			 * We ignore the timeline part of the WAL segment identifiers in
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
			 *
			 * We also should not forget that WAL segment can be compressed.
			 */
			if (IsXLogFileName(arcde->d_name) ||
				IsPartialXLogFileName(arcde->d_name) ||
				IsBackupHistoryFileName(arcde->d_name) ||
				IsCompressedXLogFileName(arcde->d_name))
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
		delete_backup_files(backup);
	}

	/* Cleanup */
	parray_walk(backup_list, pgBackupFree);
	parray_free(backup_list);

	/* Delete all wal files. */
	delete_walfiles(InvalidXLogRecPtr, 0, instance_config.xlog_seg_size);

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
