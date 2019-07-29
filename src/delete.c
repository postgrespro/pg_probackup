/*-------------------------------------------------------------------------
 *
 * delete.c: delete backup files.
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <dirent.h>
#include <time.h>
#include <unistd.h>

static void delete_walfiles(XLogRecPtr oldest_lsn, TimeLineID oldest_tli,
							uint32 xlog_seg_size);
static void do_retention_internal(parray *backup_list, parray *to_keep_list,
									parray *to_purge_list);
static void do_retention_merge(parray *backup_list, parray *to_keep_list,
									parray *to_purge_list);
static void do_retention_purge(parray *to_keep_list, parray *to_purge_list);
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
	XLogRecPtr	oldest_lsn = InvalidXLogRecPtr;
	TimeLineID	oldest_tli = 0;

	/* Get complete list of backups */
	backup_list = catalog_get_backup_list(INVALID_BACKUP_ID);

	delete_list = parray_new();

	/* Find backup to be deleted and make increment backups array to be deleted */
	for (i = 0; i < parray_num(backup_list); i++)
	{
		pgBackup   *backup = (pgBackup *) parray_get(backup_list, i);

		if (backup->start_time == backup_id)
		{
			target_backup = backup;
			break;
		}
	}

	/* sanity */
	if (!target_backup)
		elog(ERROR, "Failed to find backup %s, cannot delete", base36enc(backup_id));

	/* form delete list */
	for (i = 0; i < parray_num(backup_list); i++)
	{
		pgBackup   *backup = (pgBackup *) parray_get(backup_list, i);

		/* check if backup is descendant of delete target */
		if (is_parent(target_backup->start_time, backup, false))
			parray_append(delete_list, backup);
	}
	parray_append(delete_list, target_backup);

	/* Lock marked for delete backups */
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

	/* Clean WAL segments */
	if (delete_wal)
	{
		Assert(target_backup);

		/* Find oldest LSN, used by backups */
		for (i = (int) parray_num(backup_list) - 1; i >= 0; i--)
		{
			pgBackup   *backup = (pgBackup *) parray_get(backup_list, (size_t) i);

			if (backup->status == BACKUP_STATUS_OK || backup->status == BACKUP_STATUS_DONE)
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
 * Merge and purge backups by retention policy. Retention policy is configured by
 * retention_redundancy and retention_window variables.
 *
 * Invalid backups handled in Oracle style, so invalid backups are ignored
 * for the purpose of retention fulfillment,
 * i.e. CORRUPT full backup do not taken in account when determine
 * which FULL backup should be keeped for redundancy obligation(only valid do),
 * but if invalid backup is not guarded by retention - it is removed
 */
int do_retention(void)
{
	parray	   *backup_list = NULL;
	parray	   *to_keep_list = parray_new();
	parray	   *to_purge_list = parray_new();

	bool	retention_is_set = false; /* At least one retention policy is set */
	bool 	backup_list_is_empty = false;

	backup_deleted = false;
	backup_merged = false;

	/* Get a complete list of backups. */
	backup_list = catalog_get_backup_list(INVALID_BACKUP_ID);

	if (parray_num(backup_list) == 0)
		backup_list_is_empty = true;

	if (delete_expired || merge_expired)
	{
		if (instance_config.retention_redundancy > 0)
			elog(LOG, "REDUNDANCY=%u", instance_config.retention_redundancy);
		if (instance_config.retention_window > 0)
			elog(LOG, "WINDOW=%u", instance_config.retention_window);

		if (instance_config.retention_redundancy == 0 &&
			instance_config.retention_window == 0)
		{
			/* Retention is disabled but we still can cleanup wal */
			elog(WARNING, "Retention policy is not set");
			if (!delete_wal)
				return 0;
		}
		else
			/* At least one retention policy is active */
			retention_is_set = true;
	}

	if (retention_is_set && backup_list_is_empty)
		elog(WARNING, "Backup list is empty, retention purge and merge are problematic");

	/* Populate purge and keep lists, and show retention state messages */
	if (retention_is_set && !backup_list_is_empty)
		do_retention_internal(backup_list, to_keep_list, to_purge_list);

	if (merge_expired && !dry_run && !backup_list_is_empty)
		do_retention_merge(backup_list, to_keep_list, to_purge_list);

	if (delete_expired && !dry_run && !backup_list_is_empty)
		do_retention_purge(to_keep_list, to_purge_list);

	/* TODO: some sort of dry run for delete_wal */
	if (delete_wal && !dry_run)
		do_retention_wal();

	/* TODO: consider dry-run flag */

	if (!backup_merged)
		elog(INFO, "There are no backups to merge by retention policy");

	if (backup_deleted)
		elog(INFO, "Purging finished");
	else
		elog(INFO, "There are no backups to delete by retention policy");

	/* Cleanup */
	parray_walk(backup_list, pgBackupFree);
	parray_free(backup_list);
	parray_free(to_keep_list);
	parray_free(to_purge_list);

	return 0;

}

/* Evaluate every backup by retention policies and populate purge and keep lists.
 * Also for every backup print its status ('Active' or 'Expired') according
 * to active retention policies.
 */
static void
do_retention_internal(parray *backup_list, parray *to_keep_list, parray *to_purge_list)
{
	int			i;
	time_t 		current_time;

	parray *redundancy_full_backup_list = NULL;

	/* For retention calculation */
	uint32		n_full_backups = 0;
	int			cur_full_backup_num = 0;
	time_t		days_threshold = 0;

	/* For fancy reporting */
	uint32		actual_window = 0;

	/* Get current time */
	current_time = time(NULL);

	/* Calculate n_full_backups and days_threshold */
	if (instance_config.retention_redundancy > 0)
	{
		for (i = 0; i < parray_num(backup_list); i++)
		{
			pgBackup   *backup = (pgBackup *) parray_get(backup_list, i);

			/* Consider only valid FULL backups for Redundancy */
			if (instance_config.retention_redundancy > 0 &&
				backup->backup_mode == BACKUP_MODE_FULL &&
				(backup->status == BACKUP_STATUS_OK ||
					backup->status == BACKUP_STATUS_DONE))
			{
				n_full_backups++;

				/* Add every FULL backup that satisfy Redundancy policy to separate list */
				if (n_full_backups <= instance_config.retention_redundancy)
				{
					if (!redundancy_full_backup_list)
						redundancy_full_backup_list = parray_new();

					parray_append(redundancy_full_backup_list, backup);
				}
			}
		}
		/* Sort list of full backups to keep */
		if (redundancy_full_backup_list)
			parray_qsort(redundancy_full_backup_list, pgBackupCompareIdDesc);
	}

	if (instance_config.retention_window > 0)
	{
		days_threshold = current_time -
		(instance_config.retention_window * 60 * 60 * 24);
	}

	elog(INFO, "Evaluate backups by retention");
	for (i = (int) parray_num(backup_list) - 1; i >= 0; i--)
	{

		bool redundancy_keep = false;
		pgBackup   *backup = (pgBackup *) parray_get(backup_list, (size_t) i);

		/* check if backup`s FULL ancestor is in redundancy list */
		if (redundancy_full_backup_list)
		{
			pgBackup   *full_backup = find_parent_full_backup(backup);

			if (full_backup && parray_bsearch(redundancy_full_backup_list,
											  full_backup,
											  pgBackupCompareIdDesc))
				redundancy_keep = true;
		}

		/* Remember the serial number of latest valid FULL backup */
		if (backup->backup_mode == BACKUP_MODE_FULL &&
			(backup->status == BACKUP_STATUS_OK ||
			 backup->status == BACKUP_STATUS_DONE))
		{
			cur_full_backup_num++;
		}

		/* Check if backup in needed by retention policy
		 * TODO: consider that ERROR backup most likely to have recovery_time == 0
		 */
		if ((days_threshold == 0 || (days_threshold > backup->recovery_time)) &&
			(instance_config.retention_redundancy == 0 || !redundancy_keep))
		{
			/* This backup is not guarded by retention
			 *
			 * Redundancy = 1
			 * FULL CORRUPT in retention (not count toward redundancy limit)
			 * FULL  in retention
			 * ------retention redundancy -------
			 * PAGE3 in retention
			 * ------retention window -----------
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
	}

	/* sort keep_list and purge list */
	parray_qsort(to_keep_list, pgBackupCompareIdDesc);
	parray_qsort(to_purge_list, pgBackupCompareIdDesc);

	/* FULL
	 * PAGE
	 * PAGE <- Only such backups must go into keep list
	 ---------retention window ----
	 * PAGE
	 * FULL
	 * PAGE
	 * FULL
	 */

	for (i = 0; i < parray_num(backup_list); i++)
	{
		pgBackup   *backup = (pgBackup *) parray_get(backup_list, i);

		/* Do not keep invalid backups by retention */
		if (backup->status != BACKUP_STATUS_OK &&
				backup->status != BACKUP_STATUS_DONE)
			continue;

		/* only incremental backups should be in keep list */
		if (backup->backup_mode == BACKUP_MODE_FULL)
			continue;

		/* orphan backup cannot be in keep list */
		if (!backup->parent_backup_link)
			continue;

		/* skip if backup already in purge list  */
		if (parray_bsearch(to_purge_list, backup, pgBackupCompareIdDesc))
			continue;

		/* if parent in purge_list, add backup to keep list */
		if (parray_bsearch(to_purge_list,
							backup->parent_backup_link,
							pgBackupCompareIdDesc))
		{
			/* make keep list a bit more compact */
			parray_append(to_keep_list, backup);
			continue;
		}
	}

	/* Message about retention state of backups
	 * TODO: message is ugly, rewrite it to something like show table in stdout.
	 */

	cur_full_backup_num = 1;
	for (i = 0; i < parray_num(backup_list); i++)
	{
		char		*action = "Active";

		pgBackup	*backup = (pgBackup *) parray_get(backup_list, i);

		if (parray_bsearch(to_purge_list, backup, pgBackupCompareIdDesc))
			action = "Expired";

		if (backup->recovery_time == 0)
			actual_window = 0;
		else
			actual_window = (current_time - backup->recovery_time)/(60 * 60 * 24);

		/* TODO: add ancestor(chain full backup) ID */
		elog(INFO, "Backup %s, mode: %s, status: %s. Redundancy: %i/%i, Time Window: %ud/%ud. %s",
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
}

/* Merge partially expired incremental chains */
static void
do_retention_merge(parray *backup_list, parray *to_keep_list, parray *to_purge_list)
{
	int i;
	int j;

	/* IMPORTANT: we can merge to only those FULL backup, that is NOT
	 * guarded by retention and final target of such merge must be
	 * an incremental backup that is guarded by retention !!!
	 *
	 *  PAGE4 E
	 *  PAGE3 D
	 --------retention window ---
	 *  PAGE2 C
	 *  PAGE1 B
	 *  FULL  A
	 *
	 * after retention merge:
	 * PAGE4 E
	 * FULL  D
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
			continue;

		elog(INFO, "Consider backup %s for merge", base36enc(keep_backup->start_time));

		/* Got valid incremental backup, find its FULL ancestor */
		full_backup = find_parent_full_backup(keep_backup);

		/* Failed to find parent */
		if (!full_backup)
		{
			elog(WARNING, "Failed to find FULL parent for %s", base36enc(keep_backup->start_time));
			continue;
		}

		/* Check that ancestor is in purge_list */
		if (!parray_bsearch(to_purge_list,
							full_backup,
							pgBackupCompareIdDesc))
		{
			elog(WARNING, "Skip backup %s for merging, "
				"because his FULL parent is not marked for purge", base36enc(keep_backup->start_time));
			continue;
		}

		/* FULL backup in purge list, thanks to compacting of keep_list current backup is
		 * final target for merge, but there could be intermediate incremental
		 * backups from purge_list.
		 */

		keep_backup_id = base36enc_dup(keep_backup->start_time);
		elog(INFO, "Merge incremental chain between FULL backup %s and backup %s",
					base36enc(full_backup->start_time), keep_backup_id);
		pg_free(keep_backup_id);

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


		/* Merge list example:
		 * 0 PAGE3
		 * 1 PAGE2
		 * 2 PAGE1
		 * 3 FULL
		 *
		 * Consequentially merge incremental backups from PAGE1 to PAGE3
		 * into FULL.
		 */

		for (j = parray_num(merge_list) - 2; j >= 0; j--)
		{
			pgBackup   *from_backup = (pgBackup *) parray_get(merge_list, j);


			/* Consider this extreme case */
			//  PAGEa1    PAGEb1   both valid
			//      \     /
			//        FULL

			/* Check that FULL backup do not has multiple descendants
			 * full_backup always point to current full_backup after merge
			 */
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
			parray_set(to_keep_list, i, NULL);
		}

		/* Cleanup */
		parray_free(merge_list);
	}

	elog(INFO, "Retention merging finished");

}

/* Purge expired backups */
static void
do_retention_purge(parray *to_keep_list, parray *to_purge_list)
{
	int i;
	int j;

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

			/* item could have been nullified in merge */
			if (!keep_backup)
				continue;

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
				pg_free(keeped_backup_id);
				break;
			}
			pg_free(keeped_backup_id);
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
}

/* Purge WAL */
static void
do_retention_wal(void)
{
	parray	   *backup_list = NULL;

	XLogRecPtr oldest_lsn = InvalidXLogRecPtr;
	TimeLineID oldest_tli = 0;
	bool backup_list_is_empty = false;
	int i;

	/* Get list of backups. */
	backup_list = catalog_get_backup_list(INVALID_BACKUP_ID);

	if (parray_num(backup_list) == 0)
		backup_list_is_empty = true;

	/* Save LSN and Timeline to remove unnecessary WAL segments */
	for (i = (int) parray_num(backup_list) - 1; i >= 0; i--)
	{
		pgBackup   *backup = (pgBackup *) parray_get(backup_list, parray_num(backup_list) -1);

		/* Get LSN and TLI of the oldest backup with valid start_lsn and tli */
		if (backup->tli > 0 && !XLogRecPtrIsInvalid(backup->start_lsn))
		{
			oldest_tli = backup->tli;
			oldest_lsn = backup->start_lsn;
			break;
		}
	}

	/* Be paranoid */
	if (!backup_list_is_empty && XLogRecPtrIsInvalid(oldest_lsn))
		elog(ERROR, "Not going to purge WAL because LSN is invalid");

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
	dir_list_file(files, path, false, true, true, 0, FIO_BACKUP_HOST);

	/* delete leaf node first */
	parray_qsort(files, pgFileComparePathDesc);
	num_files = parray_num(files);
	for (i = 0; i < num_files; i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);

		if (progress)
			elog(INFO, "Progress: (%zd/%zd). Process file \"%s\"",
				 i + 1, num_files, file->path);

		if (interrupted)
			elog(ERROR, "interrupted during delete backup");

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
	parray		*backup_list;
	parray		*xlog_files_list;
	int 		i;
	int 		rc;
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
	xlog_files_list = parray_new();
	dir_list_file(xlog_files_list, arclog_path, false, false, false, 0, FIO_BACKUP_HOST);

	for (i = 0; i < parray_num(xlog_files_list); i++)
	{
		pgFile	   *wal_file = (pgFile *) parray_get(xlog_files_list, i);
		if (S_ISREG(wal_file->mode))
		{
			rc = unlink(wal_file->path);
			if (rc != 0)
				elog(WARNING, "Failed to remove file \"%s\": %s",
					 wal_file->path, strerror(errno));
		}
	}

	/* Cleanup */
	parray_walk(xlog_files_list, pgFileFree);
	parray_free(xlog_files_list);

	/* Delete backup instance config file */
	join_path_components(instance_config_path, backup_instance_path, BACKUP_CATALOG_CONF_FILE);
	if (remove(instance_config_path))
	{
		elog(ERROR, "Can't remove \"%s\": %s", instance_config_path,
			strerror(errno));
	}

	/* Delete instance root directories */
	if (rmdir(backup_instance_path) != 0)
		elog(ERROR, "Can't remove \"%s\": %s", backup_instance_path,
			strerror(errno));

	if (rmdir(arclog_path) != 0)
		elog(ERROR, "Can't remove \"%s\": %s", arclog_path,
			strerror(errno));

	elog(INFO, "Instance '%s' successfully deleted", instance_name);
	return 0;
}
