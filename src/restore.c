/*-------------------------------------------------------------------------
 *
 * restore.c: restore DB cluster and archived WAL.
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include "access/timeline.h"

#include <sys/stat.h>
#include <unistd.h>

#include "utils/thread.h"

typedef struct
{
	parray	   *files;
	pgBackup   *backup;

	/*
	 * Return value from the thread.
	 * 0 means there is no error, 1 - there is an error.
	 */
	int			ret;
} restore_files_arg;

static void restore_backup(pgBackup *backup);
static void create_recovery_conf(time_t backup_id,
								 pgRecoveryTarget *rt,
								 pgBackup *backup);
static void *restore_files(void *arg);
static void remove_deleted_files(pgBackup *backup);


/*
 * Entry point of pg_probackup RESTORE and VALIDATE subcommands.
 */
int
do_restore_or_validate(time_t target_backup_id, pgRecoveryTarget *rt,
					   bool is_restore)
{
	int			i = 0;
	int			j = 0;
	parray	   *backups;
	pgBackup   *tmp_backup = NULL;
	pgBackup   *current_backup = NULL;
	pgBackup   *dest_backup = NULL;
	pgBackup   *base_full_backup = NULL;
	pgBackup   *corrupted_backup = NULL;
	int			dest_backup_index = 0;
	int			base_full_backup_index = 0;
	int			corrupted_backup_index = 0;
	char	   *action = is_restore ? "Restore":"Validate";

	if (is_restore)
	{
		if (pgdata == NULL)
			elog(ERROR,
				"required parameter not specified: PGDATA (-D, --pgdata)");
		/* Check if restore destination empty */
		if (!dir_is_empty(pgdata))
			elog(ERROR, "restore destination is not empty: \"%s\"", pgdata);
	}

	if (instance_name == NULL)
		elog(ERROR, "required parameter not specified: --instance");

	elog(LOG, "%s begin.", action);

	/* Get exclusive lock of backup catalog */
	catalog_lock();
	/* Get list of all backups sorted in order of descending start time */
	backups = catalog_get_backup_list(INVALID_BACKUP_ID);

	/* Find backup range we should restore or validate. */
	while ((i < parray_num(backups)) && !dest_backup)
	{
		current_backup = (pgBackup *) parray_get(backups, i);
		i++;

		/* Skip all backups which started after target backup */
		if (target_backup_id && current_backup->start_time > target_backup_id)
			continue;

		/*
		 * [PGPRO-1164] If BACKUP_ID is not provided for restore command,
		 *  we must find the first valid(!) backup.
		 */

		if (is_restore &&
			target_backup_id == INVALID_BACKUP_ID &&
			current_backup->status != BACKUP_STATUS_OK)
		{
			elog(WARNING, "Skipping backup %s, because it has non-valid status: %s",
				base36enc(current_backup->start_time), status2str(current_backup->status));
			continue;
		}

		/*
		 * We found target backup. Check its status and
		 * ensure that it satisfies recovery target.
		 */
		if ((target_backup_id == current_backup->start_time
			|| target_backup_id == INVALID_BACKUP_ID))
		{

			/* backup is not ok,
			 * but in case of CORRUPT, ORPHAN or DONE revalidation is possible
			 * unless --no-validate is used,
			 * in other cases throw an error.
			 */
			 // 1. validate
			 // 2. validate -i INVALID_ID <- allowed revalidate
			 // 3. restore -i INVALID_ID <- allowed revalidate and restore
			 // 4. restore <- impossible
			 // 5. restore --no-validate <- forbidden
			if (current_backup->status != BACKUP_STATUS_OK)
			{
				if ((current_backup->status == BACKUP_STATUS_DONE ||
					current_backup->status == BACKUP_STATUS_ORPHAN ||
					current_backup->status == BACKUP_STATUS_CORRUPT)
					&& !rt->restore_no_validate)
					elog(WARNING, "Backup %s has status: %s",
						 base36enc(current_backup->start_time), status2str(current_backup->status));
				else
					elog(ERROR, "Backup %s has status: %s",
						 base36enc(current_backup->start_time), status2str(current_backup->status));
			}

			if (rt->recovery_target_tli)
			{
				parray	   *timelines;

				elog(LOG, "target timeline ID = %u", rt->recovery_target_tli);
				/* Read timeline history files from archives */
				timelines = readTimeLineHistory_probackup(rt->recovery_target_tli);

				if (!satisfy_timeline(timelines, current_backup))
				{
					if (target_backup_id != INVALID_BACKUP_ID)
						elog(ERROR, "target backup %s does not satisfy target timeline",
							 base36enc(target_backup_id));
					else
						/* Try to find another backup that satisfies target timeline */
						continue;
				}
			}

			if (!satisfy_recovery_target(current_backup, rt))
			{
				if (target_backup_id != INVALID_BACKUP_ID)
					elog(ERROR, "target backup %s does not satisfy restore options",
						 base36enc(target_backup_id));
				else
					/* Try to find another backup that satisfies target options */
					continue;
			}

			/*
			 * Backup is fine and satisfies all recovery options.
			 * Save it as dest_backup
			 */
			dest_backup = current_backup;
		}
	}

	if (dest_backup == NULL)
		elog(ERROR, "Backup satisfying target options is not found.");

	dest_backup_index = get_backup_index_number(backups, dest_backup);

	/* If we already found dest_backup, look for full backup. */
	if (dest_backup->backup_mode == BACKUP_MODE_FULL)
			base_full_backup = dest_backup;
	else
	{
		int result;

		result = scan_parent_chain(dest_backup, &tmp_backup);

		if (result == 0)
		{
			/* chain is broken, determine missing backup ID
			 * and orphinize all his descendants
			 */
			char	   *missing_backup_id;
			time_t 		missing_backup_start_time;

			missing_backup_start_time = tmp_backup->parent_backup;
			missing_backup_id = base36enc_dup(tmp_backup->parent_backup);

			for (j = get_backup_index_number(backups, tmp_backup); j >= 0; j--)
			{
				pgBackup *backup = (pgBackup *) parray_get(backups, j);

				/* use parent backup start_time because he is missing
				 * and we must orphinize his descendants
				 */
				if (is_parent(missing_backup_start_time, backup, false))
				{
					if (backup->status == BACKUP_STATUS_OK)
					{
						backup->status = BACKUP_STATUS_ORPHAN;
						write_backup_status(backup);

						elog(WARNING, "Backup %s is orphaned because his parent %s is missing",
								base36enc(backup->start_time), missing_backup_id);
					}
					else
					{
						elog(WARNING, "Backup %s has missing parent %s",
								base36enc(backup->start_time), missing_backup_id);
					}
				}
			}
			/* No point in doing futher */
			elog(ERROR, "%s of backup %s failed.", action, base36enc(dest_backup->start_time));
		}
		else if (result == 1)
		{
			/* chain is intact, but at least one parent is invalid */
			char	   *parent_backup_id;

			/* parent_backup_id contain human-readable backup ID of oldest invalid backup */
			parent_backup_id = base36enc_dup(tmp_backup->start_time);

			for (j = get_backup_index_number(backups, tmp_backup) - 1; j >= 0; j--)
			{

				pgBackup *backup = (pgBackup *) parray_get(backups, j);

				if (is_parent(tmp_backup->start_time, backup, false))
				{
					if (backup->status == BACKUP_STATUS_OK)
					{
						backup->status = BACKUP_STATUS_ORPHAN;
						write_backup_status(backup);

						elog(WARNING,
							 "Backup %s is orphaned because his parent %s has status: %s",
							 base36enc(backup->start_time),
							 parent_backup_id,
							 status2str(tmp_backup->status));
					}
					else
					{
						elog(WARNING, "Backup %s has parent %s with status: %s",
								base36enc(backup->start_time), parent_backup_id,
								status2str(tmp_backup->status));
					}
				}
			}
			tmp_backup = find_parent_full_backup(dest_backup);
		}

		/*
		 * We have found full backup by link,
		 * now we need to walk the list to find its index.
		 *
		 * TODO I think we should rewrite it someday to use double linked list
		 * and avoid relying on sort order anymore.
		 */
		base_full_backup = tmp_backup;
	}

	if (base_full_backup == NULL)
		elog(ERROR, "Full backup satisfying target options is not found.");

	base_full_backup_index = get_backup_index_number(backups, base_full_backup);

	/*
	 * Ensure that directories provided in tablespace mapping are valid
	 * i.e. empty or not exist.
	 */
	if (is_restore)
		check_tablespace_mapping(dest_backup);

	if (!is_restore || !rt->restore_no_validate)
	{
		if (dest_backup->backup_mode != BACKUP_MODE_FULL)
			elog(INFO, "Validating parents for backup %s", base36enc(dest_backup->start_time));

		/*
		 * Validate backups from base_full_backup to dest_backup.
		 * At this point we are sure that parent chain is intact.
		 */
		for (i = base_full_backup_index; i >= dest_backup_index; i--)
		{
			tmp_backup = (pgBackup *) parray_get(backups, i);

			if (is_parent(base_full_backup->start_time, tmp_backup, true))
			{

				pgBackupValidate(tmp_backup);
				/* Maybe we should be more paranoid and check for !BACKUP_STATUS_OK? */
				if (tmp_backup->status == BACKUP_STATUS_CORRUPT)
				{
					corrupted_backup = tmp_backup;
					corrupted_backup_index = i;
					break;
				}
				/* We do not validate WAL files of intermediate backups
				 * It`s done to speed up restore
				 */
			}
		}

		/* There is no point in wal validation of corrupted backups */
		if (!corrupted_backup)
		{
			/*
			 * Validate corresponding WAL files.
			 * We pass base_full_backup timeline as last argument to this function,
			 * because it's needed to form the name of xlog file.
			 */
			validate_wal(dest_backup, arclog_path, rt->recovery_target_time,
						 rt->recovery_target_xid, rt->recovery_target_lsn,
						 base_full_backup->tli, xlog_seg_size);
		}
		/* Orphinize every OK descendant of corrupted backup */
		else
		{
			char	   *corrupted_backup_id;
			corrupted_backup_id = base36enc_dup(corrupted_backup->start_time);

			for (j = corrupted_backup_index - 1; j >= 0; j--)
			{
				pgBackup   *backup = (pgBackup *) parray_get(backups, j);

				if (is_parent(corrupted_backup->start_time, backup, false))
				{
					if (backup->status == BACKUP_STATUS_OK)
					{
						backup->status = BACKUP_STATUS_ORPHAN;
						write_backup_status(backup);

						elog(WARNING, "Backup %s is orphaned because his parent %s has status: %s",
							 base36enc(backup->start_time),
							 corrupted_backup_id,
							 status2str(corrupted_backup->status));
					}
				}
			}
			free(corrupted_backup_id);
		}
	}

	// TODO: rewrite restore to use parent_chain
	/*
	 * If dest backup is corrupted or was orphaned in previous check
	 * produce corresponding error message
	 */
	if (dest_backup->status == BACKUP_STATUS_OK)
	{
		if (rt->restore_no_validate)
			elog(INFO, "Backup %s is used without validation.", base36enc(dest_backup->start_time));
		else
			elog(INFO, "Backup %s is valid.", base36enc(dest_backup->start_time));
	}
	else if (dest_backup->status == BACKUP_STATUS_CORRUPT)
		elog(ERROR, "Backup %s is corrupt.", base36enc(dest_backup->start_time));
	else if (dest_backup->status == BACKUP_STATUS_ORPHAN)
		elog(ERROR, "Backup %s is orphan.", base36enc(dest_backup->start_time));
	else
		elog(ERROR, "Backup %s has status: %s",
				base36enc(dest_backup->start_time), status2str(dest_backup->status));

	/* We ensured that all backups are valid, now restore if required
	 * TODO: use parent_link
	 */
	if (is_restore)
	{
		for (i = base_full_backup_index; i >= dest_backup_index; i--)
		{
			pgBackup   *backup = (pgBackup *) parray_get(backups, i);

			if (rt->lsn_specified && parse_server_version(backup->server_version) < 100000)
				elog(ERROR, "Backup %s was created for version %s which doesn't support recovery_target_lsn",
						base36enc(dest_backup->start_time), dest_backup->server_version);

			restore_backup(backup);
		}

		/*
		 * Delete files which are not in dest backup file list. Files which were
		 * deleted between previous and current backup are not in the list.
		 */
		if (dest_backup->backup_mode != BACKUP_MODE_FULL)
			remove_deleted_files(dest_backup);

		/* Create recovery.conf with given recovery target parameters */
		create_recovery_conf(target_backup_id, rt, dest_backup);
	}

	/* cleanup */
	parray_walk(backups, pgBackupFree);
	parray_free(backups);

	elog(INFO, "%s of backup %s completed.",
		 action, base36enc(dest_backup->start_time));
	return 0;
}

/*
 * Restore one backup.
 */
void
restore_backup(pgBackup *backup)
{
	char		timestamp[100];
	char		this_backup_path[MAXPGPATH];
	char		database_path[MAXPGPATH];
	char		list_path[MAXPGPATH];
	parray	   *files;
	int			i;
	/* arrays with meta info for multi threaded backup */
	pthread_t  *threads;
	restore_files_arg *threads_args;
	bool		restore_isok = true;

	if (backup->status != BACKUP_STATUS_OK)
		elog(ERROR, "Backup %s cannot be restored because it is not valid",
			 base36enc(backup->start_time));

	/* confirm block size compatibility */
	if (backup->block_size != BLCKSZ)
		elog(ERROR,
			"BLCKSZ(%d) is not compatible(%d expected)",
			backup->block_size, BLCKSZ);
	if (backup->wal_block_size != XLOG_BLCKSZ)
		elog(ERROR,
			"XLOG_BLCKSZ(%d) is not compatible(%d expected)",
			backup->wal_block_size, XLOG_BLCKSZ);

	time2iso(timestamp, lengthof(timestamp), backup->start_time);
	elog(LOG, "restoring database from backup %s", timestamp);

	/*
	 * Restore backup directories.
	 * this_backup_path = $BACKUP_PATH/backups/instance_name/backup_id
	 */
	pgBackupGetPath(backup, this_backup_path, lengthof(this_backup_path), NULL);
	create_data_directories(pgdata, this_backup_path, true);

	/*
	 * Get list of files which need to be restored.
	 */
	pgBackupGetPath(backup, database_path, lengthof(database_path), DATABASE_DIR);
	pgBackupGetPath(backup, list_path, lengthof(list_path), DATABASE_FILE_LIST);
	files = dir_read_file_list(database_path, list_path);

	threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
	threads_args = (restore_files_arg *) palloc(sizeof(restore_files_arg)*num_threads);

	/* setup threads */
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);

		pg_atomic_clear_flag(&file->lock);
	}

	/* Restore files into target directory */
	for (i = 0; i < num_threads; i++)
	{
		restore_files_arg *arg = &(threads_args[i]);

		arg->files = files;
		arg->backup = backup;
		/* By default there are some error */
		threads_args[i].ret = 1;

		elog(LOG, "Start thread for num:%li", parray_num(files));

		pthread_create(&threads[i], NULL, restore_files, arg);
	}

	/* Wait theads */
	for (i = 0; i < num_threads; i++)
	{
		pthread_join(threads[i], NULL);
		if (threads_args[i].ret == 1)
			restore_isok = false;
	}
	if (!restore_isok)
		elog(ERROR, "Data files restoring failed");

	pfree(threads);
	pfree(threads_args);

	/* cleanup */
	parray_walk(files, pgFileFree);
	parray_free(files);

	if (log_level_console <= LOG || log_level_file <= LOG)
		elog(LOG, "restore %s backup completed", base36enc(backup->start_time));
}

/*
 * Delete files which are not in backup's file list from target pgdata.
 * It is necessary to restore incremental backup correctly.
 * Files which were deleted between previous and current backup
 * are not in the backup's filelist.
 */
static void
remove_deleted_files(pgBackup *backup)
{
	parray	   *files;
	parray	   *files_restored;
	char		filelist_path[MAXPGPATH];
	int			i;

	pgBackupGetPath(backup, filelist_path, lengthof(filelist_path), DATABASE_FILE_LIST);
	/* Read backup's filelist using target database path as base path */
	files = dir_read_file_list(pgdata, filelist_path);
	parray_qsort(files, pgFileComparePathDesc);

	/* Get list of files actually existing in target database */
	files_restored = parray_new();
	dir_list_file(files_restored, pgdata, true, true, false);
	/* To delete from leaf, sort in reversed order */
	parray_qsort(files_restored, pgFileComparePathDesc);

	for (i = 0; i < parray_num(files_restored); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files_restored, i);

		/* If the file is not in the file list, delete it */
		if (parray_bsearch(files, file, pgFileComparePathDesc) == NULL)
		{
			pgFileDelete(file);
			if (log_level_console <= LOG || log_level_file <= LOG)
				elog(LOG, "deleted %s", GetRelativePath(file->path, pgdata));
		}
	}

	/* cleanup */
	parray_walk(files, pgFileFree);
	parray_free(files);
	parray_walk(files_restored, pgFileFree);
	parray_free(files_restored);
}

/*
 * Restore files into $PGDATA.
 */
static void *
restore_files(void *arg)
{
	int			i;
	restore_files_arg *arguments = (restore_files_arg *)arg;

	for (i = 0; i < parray_num(arguments->files); i++)
	{
		char		from_root[MAXPGPATH];
		char	   *rel_path;
		pgFile	   *file = (pgFile *) parray_get(arguments->files, i);

		if (!pg_atomic_test_set_flag(&file->lock))
			continue;

		pgBackupGetPath(arguments->backup, from_root,
						lengthof(from_root), DATABASE_DIR);

		/* check for interrupt */
		if (interrupted)
			elog(ERROR, "interrupted during restore database");

		rel_path = GetRelativePath(file->path,from_root);

		if (progress)
			elog(LOG, "Progress: (%d/%lu). Process file %s ",
				 i + 1, (unsigned long) parray_num(arguments->files), rel_path);

		/*
		 * For PAGE and PTRACK backups skip files which haven't changed
		 * since previous backup and thus were not backed up.
		 * We cannot do the same when restoring DELTA backup because we need information
		 * about every file to correctly truncate them.
		 */
		if (file->write_size == BYTES_INVALID &&
			(arguments->backup->backup_mode == BACKUP_MODE_DIFF_PAGE
			|| arguments->backup->backup_mode == BACKUP_MODE_DIFF_PTRACK))
		{
			elog(VERBOSE, "The file didn`t change. Skip restore: %s", file->path);
			continue;
		}

		/* Directories were created before */
		if (S_ISDIR(file->mode))
		{
			elog(VERBOSE, "directory, skip");
			continue;
		}

		/* Do not restore tablespace_map file */
		if (path_is_prefix_of_path(PG_TABLESPACE_MAP_FILE, rel_path))
		{
			elog(VERBOSE, "skip tablespace_map");
			continue;
		}

		/*
		 * restore the file.
		 * We treat datafiles separately, cause they were backed up block by
		 * block and have BackupPageHeader meta information, so we cannot just
		 * copy the file from backup.
		 */
		elog(VERBOSE, "Restoring file %s, is_datafile %i, is_cfs %i",
			 file->path, file->is_datafile?1:0, file->is_cfs?1:0);
		if (file->is_datafile && !file->is_cfs)
		{
			char		to_path[MAXPGPATH];

			join_path_components(to_path, pgdata,
								 file->path + strlen(from_root) + 1);
			restore_data_file(to_path, file,
							  arguments->backup->backup_mode == BACKUP_MODE_DIFF_DELTA,
							  false);
		}
		else
			copy_file(from_root, pgdata, file);

		/* print size of restored file */
		if (file->write_size != BYTES_INVALID)
			elog(VERBOSE, "Restored file %s : " INT64_FORMAT " bytes",
				 file->path, file->write_size);
	}

	/* Data files restoring is successful */
	arguments->ret = 0;

	return NULL;
}

/* Create recovery.conf with given recovery target parameters */
static void
create_recovery_conf(time_t backup_id,
					 pgRecoveryTarget *rt,
					 pgBackup *backup)
{
	char		path[MAXPGPATH];
	FILE	   *fp;
	bool		need_restore_conf = false;

	if (!backup->stream
		|| (rt->time_specified || rt->xid_specified))
			need_restore_conf = true;

	/* No need to generate recovery.conf at all. */
	if (!(need_restore_conf || restore_as_replica))
		return;

	elog(LOG, "----------------------------------------");
	elog(LOG, "creating recovery.conf");

	snprintf(path, lengthof(path), "%s/recovery.conf", pgdata);
	fp = fopen(path, "wt");
	if (fp == NULL)
		elog(ERROR, "cannot open recovery.conf \"%s\": %s", path,
			strerror(errno));

	fprintf(fp, "# recovery.conf generated by pg_probackup %s\n",
		PROGRAM_VERSION);

	if (need_restore_conf)
	{

		fprintf(fp, "restore_command = '%s archive-get -B %s --instance %s "
					"--wal-file-path %%p --wal-file-name %%f'\n",
					PROGRAM_NAME, backup_path, instance_name);

		/*
		 * We've already checked that only one of the four following mutually
		 * exclusive options is specified, so the order of calls is insignificant.
		 */
		if (rt->recovery_target_name)
			fprintf(fp, "recovery_target_name = '%s'\n", rt->recovery_target_name);

		if (rt->time_specified)
			fprintf(fp, "recovery_target_time = '%s'\n", rt->target_time_string);

		if (rt->xid_specified)
			fprintf(fp, "recovery_target_xid = '%s'\n", rt->target_xid_string);

		if (rt->recovery_target_lsn)
			fprintf(fp, "recovery_target_lsn = '%s'\n", rt->target_lsn_string);

		if (rt->recovery_target_immediate)
			fprintf(fp, "recovery_target = 'immediate'\n");

		if (rt->inclusive_specified)
			fprintf(fp, "recovery_target_inclusive = '%s'\n",
					rt->recovery_target_inclusive?"true":"false");

		if (rt->recovery_target_tli)
			fprintf(fp, "recovery_target_timeline = '%u'\n", rt->recovery_target_tli);

		if (rt->recovery_target_action)
			fprintf(fp, "recovery_target_action = '%s'\n", rt->recovery_target_action);
	}

	if (restore_as_replica)
	{
		fprintf(fp, "standby_mode = 'on'\n");

		if (backup->primary_conninfo)
			fprintf(fp, "primary_conninfo = '%s'\n", backup->primary_conninfo);
	}

	if (fflush(fp) != 0 ||
		fsync(fileno(fp)) != 0 ||
		fclose(fp))
		elog(ERROR, "cannot write recovery.conf \"%s\": %s", path,
			 strerror(errno));
}

/*
 * Try to read a timeline's history file.
 *
 * If successful, return the list of component TLIs (the ancestor
 * timelines followed by target timeline). If we cannot find the history file,
 * assume that the timeline has no parents, and return a list of just the
 * specified timeline ID.
 * based on readTimeLineHistory() in timeline.c
 */
parray *
readTimeLineHistory_probackup(TimeLineID targetTLI)
{
	parray	   *result;
	char		path[MAXPGPATH];
	char		fline[MAXPGPATH];
	FILE	   *fd = NULL;
	TimeLineHistoryEntry *entry;
	TimeLineHistoryEntry *last_timeline = NULL;

	/* Look for timeline history file in archlog_path */
	snprintf(path, lengthof(path), "%s/%08X.history", arclog_path,
		targetTLI);

	/* Timeline 1 does not have a history file */
	if (targetTLI != 1)
	{
		fd = fopen(path, "rt");
		if (fd == NULL)
		{
			if (errno != ENOENT)
				elog(ERROR, "could not open file \"%s\": %s", path,
					strerror(errno));

			/* There is no history file for target timeline */
			elog(ERROR, "recovery target timeline %u does not exist",
				 targetTLI);
		}
	}

	result = parray_new();

	/*
	 * Parse the file...
	 */
	while (fd && fgets(fline, sizeof(fline), fd) != NULL)
	{
		char	   *ptr;
		TimeLineID	tli;
		uint32		switchpoint_hi;
		uint32		switchpoint_lo;
		int			nfields;

		for (ptr = fline; *ptr; ptr++)
		{
			if (!isspace((unsigned char) *ptr))
				break;
		}
		if (*ptr == '\0' || *ptr == '#')
			continue;

		nfields = sscanf(fline, "%u\t%X/%X", &tli, &switchpoint_hi, &switchpoint_lo);

		if (nfields < 1)
		{
			/* expect a numeric timeline ID as first field of line */
			elog(ERROR,
				 "syntax error in history file: %s. Expected a numeric timeline ID.",
				   fline);
		}
		if (nfields != 3)
			elog(ERROR,
				 "syntax error in history file: %s. Expected a transaction log switchpoint location.",
				   fline);

		if (last_timeline && tli <= last_timeline->tli)
			elog(ERROR,
				   "Timeline IDs must be in increasing sequence.");

		entry = pgut_new(TimeLineHistoryEntry);
		entry->tli = tli;
		entry->end = ((uint64) switchpoint_hi << 32) | switchpoint_lo;

		last_timeline = entry;
		/* Build list with newest item first */
		parray_insert(result, 0, entry);

		/* we ignore the remainder of each line */
	}

	if (fd)
		fclose(fd);

	if (last_timeline && targetTLI <= last_timeline->tli)
		elog(ERROR, "Timeline IDs must be less than child timeline's ID.");

	/* append target timeline */
	entry = pgut_new(TimeLineHistoryEntry);
	entry->tli = targetTLI;
	/* LSN in target timeline is valid */
	/* TODO ensure that -1UL --> -1L fix is correct */
	entry->end = (uint32) (-1L << 32) | -1L;
	parray_insert(result, 0, entry);

	return result;
}

bool
satisfy_recovery_target(const pgBackup *backup, const pgRecoveryTarget *rt)
{
	if (rt->xid_specified)
		return backup->recovery_xid <= rt->recovery_target_xid;

	if (rt->time_specified)
		return backup->recovery_time <= rt->recovery_target_time;

	if (rt->lsn_specified)
		return backup->stop_lsn <= rt->recovery_target_lsn;

	return true;
}

bool
satisfy_timeline(const parray *timelines, const pgBackup *backup)
{
	int			i;

	for (i = 0; i < parray_num(timelines); i++)
	{
		TimeLineHistoryEntry *timeline;

		timeline = (TimeLineHistoryEntry *) parray_get(timelines, i);
		if (backup->tli == timeline->tli &&
			backup->stop_lsn < timeline->end)
			return true;
	}
	return false;
}
/*
 * Get recovery options in the string format, parse them
 * and fill up the pgRecoveryTarget structure.
 */
pgRecoveryTarget *
parseRecoveryTargetOptions(const char *target_time,
					const char *target_xid,
					const char *target_inclusive,
					TimeLineID	target_tli,
					const char *target_lsn,
					bool target_immediate,
					const char *target_name,
					const char *target_action,
					bool		restore_no_validate)
{
	time_t		dummy_time;
	TransactionId dummy_xid;
	bool		dummy_bool;
	XLogRecPtr	dummy_lsn;
	/*
	 * count the number of the mutually exclusive options which may specify
	 * recovery target. If final value > 1, throw an error.
	 */
	int			recovery_target_specified = 0;
	pgRecoveryTarget *rt = pgut_new(pgRecoveryTarget);

	/* fill all options with default values */
	rt->time_specified = false;
	rt->xid_specified = false;
	rt->inclusive_specified = false;
	rt->lsn_specified = false;
	rt->recovery_target_time = 0;
	rt->recovery_target_xid  = 0;
	rt->recovery_target_lsn = InvalidXLogRecPtr;
	rt->target_time_string = NULL;
	rt->target_xid_string = NULL;
	rt->target_lsn_string = NULL;
	rt->recovery_target_inclusive = false;
	rt->recovery_target_tli  = 0;
	rt->recovery_target_immediate = false;
	rt->recovery_target_name = NULL;
	rt->recovery_target_action = NULL;
	rt->restore_no_validate = false;

	/* parse given options */
	if (target_time)
	{
		recovery_target_specified++;
		rt->time_specified = true;
		rt->target_time_string = target_time;

		if (parse_time(target_time, &dummy_time, false))
			rt->recovery_target_time = dummy_time;
		else
			elog(ERROR, "Invalid value of --time option %s", target_time);
	}

	if (target_xid)
	{
		recovery_target_specified++;
		rt->xid_specified = true;
		rt->target_xid_string = target_xid;

#ifdef PGPRO_EE
		if (parse_uint64(target_xid, &dummy_xid, 0))
#else
		if (parse_uint32(target_xid, &dummy_xid, 0))
#endif
			rt->recovery_target_xid = dummy_xid;
		else
			elog(ERROR, "Invalid value of --xid option %s", target_xid);
	}

	if (target_lsn)
	{
		recovery_target_specified++;
		rt->lsn_specified = true;
		rt->target_lsn_string = target_lsn;
		if (parse_lsn(target_lsn, &dummy_lsn))
			rt->recovery_target_lsn = dummy_lsn;
		else
			elog(ERROR, "Invalid value of --lsn option %s", target_lsn);
	}

	if (target_inclusive)
	{
		rt->inclusive_specified = true;
		if (parse_bool(target_inclusive, &dummy_bool))
			rt->recovery_target_inclusive = dummy_bool;
		else
			elog(ERROR, "Invalid value of --inclusive option %s", target_inclusive);
	}

	rt->recovery_target_tli  = target_tli;
	if (target_immediate)
	{
		recovery_target_specified++;
		rt->recovery_target_immediate = target_immediate;
	}

	if (restore_no_validate)
	{
		rt->restore_no_validate = restore_no_validate;
	}

	if (target_name)
	{
		recovery_target_specified++;
		rt->recovery_target_name = target_name;
	}

	if (target_action)
	{
		rt->recovery_target_action = target_action;

		if ((strcmp(target_action, "pause") != 0)
			&& (strcmp(target_action, "promote") != 0)
			&& (strcmp(target_action, "shutdown") != 0))
				elog(ERROR, "Invalid value of --recovery-target-action option %s", target_action);
	}
	else
	{
		/* Default recovery target action is pause */
		rt->recovery_target_action = "pause";
	}

	/* More than one mutually exclusive option was defined. */
	if (recovery_target_specified > 1)
		elog(ERROR, "At most one of --immediate, --target-name, --time, --xid, or --lsn can be used");

	/* If none of the options is defined, '--inclusive' option is meaningless */
	if (!(rt->xid_specified || rt->time_specified || rt->lsn_specified) && rt->recovery_target_inclusive)
		elog(ERROR, "--inclusive option applies when either --time or --xid is specified");

	return rt;
}
