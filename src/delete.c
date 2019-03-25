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
static void do_retention_internal(void);
static void do_retention_wal(void);

static bool backup_deleted = false;   /* At least one backup was deleted */
static bool backup_merged = false;    /* At least one merge was enacted */

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
				/* TODO: Current algorithm is imperfect, it assume that backup
				 * can sire only one incremental chain
				 */

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

		catalog_lock_backup_list(delete_list, parray_num(delete_list) - 1, 0);

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

int do_retention(void)
{
	bool	retention_is_set = false; /* At least one retention policy is set */

	if (delete_expired || merge_expired)
	{
		if (instance_config.retention_redundancy > 0)
			elog(LOG, "REDUNDANCY=%u", instance_config.retention_redundancy);
		if (instance_config.retention_window > 0)
			elog(LOG, "WINDOW=%u", instance_config.retention_window);

		if (instance_config.retention_redundancy == 0 &&
			instance_config.retention_window == 0)
		{
			/* Retention is disabled but we still can cleanup
			 * failed backups and wal
			 */
			elog(WARNING, "Retention policy is not set");
			if (!delete_wal)
				return 0;
		}
		else
		/* At least one retention policy is active */
			retention_is_set = true;
	}

	if (retention_is_set && ((delete_expired || merge_expired) || dry_run))
		do_retention_internal();

	if (delete_wal && !dry_run)
		do_retention_wal();

	if (!backup_merged)
		elog(INFO, "There are no backups to merge by retention policy");

	if (backup_deleted)
		elog(INFO, "Purging finished");
	else
		elog(INFO, "There are no backups to delete by retention policy");

	return 0;

}

/*
 * Merge and purge backups by retention policy. Retention policy is configured by
 * retention_redundancy and retention_window variables.
 *
 * Invalid backups handled in Oracle style, so invalid backups are ignored
 * for the purpose of retention fulfillment,
 * i.e. CORRUPT full backup do not taken in account when deteremine
 * which FULL backup should be keeped for redundancy obligation(only valid do),
 * but if invalid backup is not guarded by retention - it is removed
 */
static void
do_retention_internal(void)
{
	parray	   *backup_list = NULL;
	parray	   *to_purge_list = parray_new();
	parray	   *to_keep_list = parray_new();
	int			i;
	int			j;
	time_t 		current_time;
	bool 		backup_list_is_empty = false;

	/* For retention calculation */
	uint32		n_full_backups = 0;
	int			cur_full_backup_num = 0;
	time_t		days_threshold = 0;

	/* For fancy reporting */
	float		actual_window = 0;

	/* Get a complete list of backups. */
	backup_list = catalog_get_backup_list(INVALID_BACKUP_ID);
	if (parray_num(backup_list) == 0)
		backup_list_is_empty = true;

	if (backup_list_is_empty)
	{
		elog(WARNING, "Backup list is empty, purging won't be executed");
		return;
	}

	/* Get current time */
	current_time = time(NULL);

	/* Calculate n_full_backups and days_threshold */
	if (!backup_list_is_empty)
	{
		if (instance_config.retention_redundancy > 0)
		{
			for (i = 0; i < parray_num(backup_list); i++)
			{
				pgBackup   *backup = (pgBackup *) parray_get(backup_list, i);

				/* Consider only valid backups for Redundancy */
				if (instance_config.retention_redundancy > 0 &&
					backup->backup_mode == BACKUP_MODE_FULL &&
					(backup->status == BACKUP_STATUS_OK ||
						backup->status == BACKUP_STATUS_DONE))
				{
					n_full_backups++;
				}
			}
		}

		if (instance_config.retention_window > 0)
		{
			days_threshold = current_time -
			(instance_config.retention_window * 60 * 60 * 24);
		}
	}

	elog(INFO, "Evaluate backups by retention");
	for (i = (int) parray_num(backup_list) - 1; i >= 0; i--)
	{

		pgBackup   *backup = (pgBackup *) parray_get(backup_list, (size_t) i);

		/* Remember the serial number of latest valid FULL backup */
		if (backup->backup_mode == BACKUP_MODE_FULL &&
			(backup->status == BACKUP_STATUS_OK ||
			 backup->status == BACKUP_STATUS_DONE))
		{
			cur_full_backup_num++;
		}

		/* Check if backup in needed by retention policy */
		if ((days_threshold == 0 || (days_threshold > backup->recovery_time)) &&
			(instance_config.retention_redundancy  <= (n_full_backups - cur_full_backup_num)))
		{
			/* This backup is not guarded by retention
			 *
			 * Redundancy = 1
			 * FULL CORRUPT in retention (not count toward redundancy limit)
			 * FULL  in retention
			 * ------retention redundancy -------
			 * PAGE4 in retention
			 * ------retention window -----------
			 * PAGE3 in retention
			 * PAGE2 out of retention
			 * PAGE1 out of retention
			 * FULL  out of retention  	<- We are here
			 * FULL CORRUPT out of retention
			 */

			/* Add backup to purge_list */
			elog(VERBOSE, "Mark backup %s for purge.", base36enc(backup->start_time));
			parray_append(to_purge_list, backup);
			continue;
		}

		/* Do not keep invalid backups by retention */
		if (backup->status != BACKUP_STATUS_OK &&
				backup->status != BACKUP_STATUS_DONE)
			continue;

		elog(VERBOSE, "Mark backup %s for retention.", base36enc(backup->start_time));
		parray_append(to_keep_list, backup);
	}

	/* Message about retention state of backups
	 * TODO: Float is ugly, rewrite somehow.
	 */

	/* sort keep_list and purge list */
	parray_qsort(to_keep_list, pgBackupCompareIdDesc);
	parray_qsort(to_purge_list, pgBackupCompareIdDesc);

	cur_full_backup_num = 1;
	for (i = 0; i < parray_num(backup_list); i++)
	{
		char		*action = "Ignore";

		pgBackup	*backup = (pgBackup *) parray_get(backup_list, i);

		if (in_backup_list(to_keep_list, backup))
			action = "Keep";

		if (in_backup_list(to_purge_list, backup))
			action = "Purge";

		if (backup->recovery_time == 0)
			actual_window = 0;
		else
			actual_window = ((float)current_time - (float)backup->recovery_time)/(60 * 60 * 24);

		elog(INFO, "Backup %s, mode: %s, status: %s. Redundancy: %i/%i, Time Window: %.2fd/%ud. %s",
				base36enc(backup->start_time),
				pgBackupGetBackupMode(backup),
				status2str(backup->status),
				cur_full_backup_num,
				instance_config.retention_redundancy,
				actual_window, instance_config.retention_window,
				action);

		if (backup->backup_mode == BACKUP_MODE_FULL)
				cur_full_backup_num++;
	}

	if (dry_run)
		goto finish;

	/*  Extreme example of keep_list
	 *
	 *	FULLc  <- keep
	 *	PAGEb2 <- keep
	 *	PAGEb1 <- keep
	 *	PAGEa2 <- keep
	 *	PAGEa1 <- keep
	 *  FULLb  <- in purge_list
	 *  FULLa  <- in purge_list
	 */

	/* Go to purge */
	if (delete_expired && !merge_expired)
		goto purge;

	/* IMPORTANT: we can merge to only those FULL backups, that are NOT
	 * guarded by retention and only from those incremental backups that
	 * are guarded by retention !!!
	 */

	/* Merging happens here */
	for (i = 0; i < parray_num(to_keep_list); i++)
	{
		char		*keep_backup_id = NULL;
		pgBackup	*full_backup = NULL;
		parray	    *merge_list = NULL;

		pgBackup	*keep_backup = (pgBackup *) parray_get(to_keep_list, i);

		/* keep list may shrink during merge */
		if (!keep_backup)
			break;

		elog(INFO, "Consider backup %s for merge", base36enc(keep_backup->start_time));

		/* In keep list we are looking for incremental backups */
		if (keep_backup->backup_mode == BACKUP_MODE_FULL)
			continue;

		/* Retain orphan backups in keep_list */
		if (!keep_backup->parent_backup_link)
			continue;

		/* If parent of current backup is also in keep list, go to the next */
		if (in_backup_list(to_keep_list, keep_backup->parent_backup_link))
		{
			/* make keep list a bit sparse */
			elog(INFO, "Sparsing keep list, remove %s", base36enc(keep_backup->start_time));
			parray_remove(to_keep_list, i);
			i--;
			continue;
		}

		elog(INFO, "Lookup parents for backup %s",
				base36enc(keep_backup->start_time));

		/* Got valid incremental backup, find its FULL ancestor */
		full_backup = find_parent_full_backup(keep_backup);

		/* Failed to find parent */
		if (!full_backup)
		{
			elog(WARNING, "Failed to find FULL parent for %s", base36enc(keep_backup->start_time));
			continue;
		}

		/* Check that ancestor is in purge_list */
		if (!in_backup_list(to_purge_list, full_backup))
		{
			elog(WARNING, "Skip backup %s for merging, "
				"because his FULL parent is not marked for purge", base36enc(keep_backup->start_time));
			continue;
		}

		/* FULL backup in purge list, thanks to sparsing of keep_list current backup is 
		 * final target for merge, but there could be intermediate incremental
		 * backups from purge_list.
		 */

		keep_backup_id = base36enc_dup(keep_backup->start_time);
		elog(INFO, "Merge incremental chain between FULL backup %s and backup %s",
					base36enc(full_backup->start_time), keep_backup_id);

		merge_list = parray_new();

		/* Form up a merge list */
		while(keep_backup->parent_backup_link)
		{
			parray_append(merge_list, keep_backup);
			keep_backup = keep_backup->parent_backup_link;
		}

		/* sanity */
		if (!merge_list)
			continue;

		/* sanity */
		if (parray_num(merge_list) == 0)
		{
			parray_free(merge_list);
			continue;
		}

		/* In the end add FULL backup for easy locking */
		parray_append(merge_list, full_backup);

		/* Remove FULL backup from purge list */
		parray_rm(to_purge_list, full_backup, pgBackupCompareId);

		/* Lock merge chain */
		catalog_lock_backup_list(merge_list, parray_num(merge_list) - 1, 0);

		for (j = parray_num(merge_list) - 1; j > 0; j--)
		{
			pgBackup   *from_backup = (pgBackup *) parray_get(merge_list, j - 1 );


			/* Consider this extreme case */
			//  PAGEa1    PAGEb1   both valid
			//      \     /
			//        FULL

			/* Check that FULL backup do not has multiple descendants */
			if (is_prolific(backup_list, full_backup))
			{
				elog(WARNING, "Backup %s has multiple valid descendants. "
						"Automatic merge is not possible.", base36enc(full_backup->start_time));
				break;
			}

			merge_backups(full_backup, from_backup);
			backup_merged = true;

			/* Try to remove merged incremental backup from both keep and purge lists */
			parray_rm(to_purge_list, from_backup, pgBackupCompareId);

			if (parray_rm(to_keep_list, from_backup, pgBackupCompareId) && (i >= 0))
				i--;
		}

		/* Cleanup */
		parray_free(merge_list);
	}

	elog(INFO, "Retention merging finished");

	if (!delete_expired)
		goto finish;

/* Do purging here */
purge:

	/* Remove backups by retention policy. Retention policy is configured by
	 * retention_redundancy and retention_window
	 * Remove only backups, that do not have children guarded by retention
	 *
	 * TODO: We do not consider the situation if child is marked for purge
	 * but parent isn`t. Maybe something bad happened with time on server?
	 */

	for (j = 0; j < parray_num(to_purge_list); j++)
	{
		bool purge = true;

		pgBackup   *delete_backup = (pgBackup *) parray_get(to_purge_list, j);

		elog(LOG, "Consider backup %s for purge",
						base36enc(delete_backup->start_time));

		/* Evaluate marked for delete backup against every backup in keep list.
		 * If marked for delete backup is recognized as parent of one of those,
		 * then this backup should not be deleted.
		 */
		for (i = 0; i < parray_num(to_keep_list); i++)
		{
			char		*keeped_backup_id;

			pgBackup   *keep_backup = (pgBackup *) parray_get(to_keep_list, i);

			/* Full backup cannot be a descendant */
			if (keep_backup->backup_mode == BACKUP_MODE_FULL)
				continue;

			keeped_backup_id = base36enc_dup(keep_backup->start_time);

			elog(LOG, "Check if backup %s is parent of backup %s",
						base36enc(delete_backup->start_time), keeped_backup_id);

			if (is_parent(delete_backup->start_time, keep_backup, true))
			{

				/* We must not delete this backup, evict it from purge list */
				elog(LOG, "Retain backup %s from purge because his "
					"descendant %s is guarded by retention",
						base36enc(delete_backup->start_time), keeped_backup_id);

				purge = false;
				break;
			}
		}

		/* Retain backup */
		if (!purge)
			continue;

		/* Actual purge */
		if (!lock_backup(delete_backup))
		{
			/* If the backup still is used, do not interrupt and go to the next */
			elog(WARNING, "Cannot lock backup %s directory, skip purging",
				 base36enc(delete_backup->start_time));
			continue;
		}

		/* Delete backup and update status to DELETED */
		delete_backup_files(delete_backup);
		backup_deleted = true;

	}

finish:
	/* Cleanup */
	parray_walk(backup_list, pgBackupFree);
	parray_free(backup_list);
	parray_free(to_keep_list);
	parray_free(to_purge_list);

}

/* Purge WAL */
static void
do_retention_wal(void)
{
	parray	   *backup_list = NULL;

	XLogRecPtr oldest_lsn = InvalidXLogRecPtr;
	TimeLineID oldest_tli = 0;
	bool backup_list_is_empty = false;

	/* Get new backup_list. Should we */
	backup_list = catalog_get_backup_list(INVALID_BACKUP_ID);

	if (parray_num(backup_list) == 0)
		backup_list_is_empty = true;

	/* Save LSN and Timeline to remove unnecessary WAL segments */
	if (!backup_list_is_empty)
	{
		pgBackup   *backup = NULL;
		/* Get LSN and TLI of oldest alive backup */
		backup = (pgBackup *) parray_get(backup_list, parray_num(backup_list) -1);

		oldest_tli = backup->tli;
		oldest_lsn = backup->start_lsn;
	}

	/* Be paranoid */
	if (!backup_list_is_empty && XLogRecPtrIsInvalid(oldest_lsn))
		elog(ERROR, "Not going to purge WAL because LSN is invalid");

	/* If no backups left after retention, do not drop all WAL files,
	 * because of race conditions there is a risk of messing up
	 * concurrent backup wals.
	 * But it is sort of ok to drop all WAL files if backup_list is empty
	 * from the very beginning, because a risk of concurrent backup
	 * is much smaller.
	 */

	/* Purge WAL files */
	delete_walfiles(oldest_lsn, oldest_tli, instance_config.xlog_seg_size);

	/* Cleanup */
	parray_walk(backup_list, pgBackupFree);
	parray_free(backup_list);
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
	write_backup_status(backup, BACKUP_STATUS_DELETING);

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

		pgFileDelete(file);
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

	catalog_lock_backup_list(backup_list, 0, parray_num(backup_list) - 1);

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
