/*-------------------------------------------------------------------------
 *
 * merge.c: merge FULL and incremental backups
 *
 * Copyright (c) 2018-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <sys/stat.h>
#include <unistd.h>

#include "utils/thread.h"

typedef struct
{
	parray		*merge_filelist;
	parray		*parent_chain;

	pgBackup	*dest_backup;
	pgBackup	*full_backup;

	const char	*full_database_dir;
	const char	*full_external_prefix;

//	size_t		in_place_merge_bytes;
	bool		compression_match;
	bool		program_version_match;
	bool        use_bitmap;
	bool        is_retry;

	/*
	 * Return value from the thread.
	 * 0 means there is no error, 1 - there is an error.
	 */
	int			ret;
} merge_files_arg;


static void *merge_files(void *arg);
static void
reorder_external_dirs(pgBackup *to_backup, parray *to_external,
					  parray *from_external);
static int
get_external_index(const char *key, const parray *list);

static void
merge_data_file(parray *parent_chain, pgBackup *full_backup,
				pgBackup *dest_backup, pgFile *dest_file,
				pgFile *tmp_file, const char *to_root, bool use_bitmap,
				bool is_retry);

static void
merge_non_data_file(parray *parent_chain, pgBackup *full_backup,
				pgBackup *dest_backup, pgFile *dest_file,
				pgFile *tmp_file, const char *full_database_dir,
				const char *full_external_prefix);

static bool is_forward_compatible(parray *parent_chain);

/*
 * Implementation of MERGE command.
 *
 * - Find target and its parent full backup
 * - Merge data files of target, parent and and intermediate backups
 * - Remove unnecessary files, which doesn't exist in the target backup anymore
 */
void
do_merge(time_t backup_id)
{
	parray	   *backups;
	parray	   *merge_list = parray_new();
	pgBackup   *dest_backup = NULL;
	pgBackup   *dest_backup_tmp = NULL;
	pgBackup   *full_backup = NULL;
	int			i;

	if (backup_id == INVALID_BACKUP_ID)
		elog(ERROR, "required parameter is not specified: --backup-id");

	if (instance_name == NULL)
		elog(ERROR, "required parameter is not specified: --instance");

	elog(INFO, "Merge started");

	/* Get list of all backups sorted in order of descending start time */
	backups = catalog_get_backup_list(instance_name, INVALID_BACKUP_ID);

	/* Find destination backup first */
	for (i = 0; i < parray_num(backups); i++)
	{
		pgBackup   *backup = (pgBackup *) parray_get(backups, i);

		/* found target */
		if (backup->start_time == backup_id)
		{
			/* sanity */
			if (backup->status != BACKUP_STATUS_OK &&
				backup->status != BACKUP_STATUS_DONE &&
				/* It is possible that previous merging was interrupted */
				backup->status != BACKUP_STATUS_MERGING &&
				backup->status != BACKUP_STATUS_MERGED &&
				backup->status != BACKUP_STATUS_DELETING)
				elog(ERROR, "Backup %s has status: %s",
						base36enc(backup->start_time), status2str(backup->status));

			dest_backup = backup;
			break;
		}
	}

	/*
	 * Handle the case of crash right after deletion of the target
	 * incremental backup. We still can recover from this.
	 * Iterate over backups and look for the FULL backup with
	 * MERGED status, that has merge-target-id eqial to backup_id.
	 */
	if (dest_backup == NULL)
	{
		for (i = 0; i < parray_num(backups); i++)
		{
			pgBackup   *backup = (pgBackup *) parray_get(backups, i);

			if (backup->status == BACKUP_STATUS_MERGED &&
				backup->merge_dest_backup == backup_id)
			{
				dest_backup = backup;
				break;
			}
		}
	}

	if (dest_backup == NULL)
		elog(ERROR, "Target backup %s was not found", base36enc(backup_id));

	/* It is possible to use FULL backup as target backup for merge.
	 * There are two possible cases:
	 * 1. The user want to merge FULL backup with closest incremental backup.
	 *		In this case we must find suitable destination backup and merge them.
	 *
	 * 2. Previous merge has failed after destination backup was deleted,
	 *    but before FULL backup was renamed:
	 * Example A:
	 *	PAGE2_1 OK
	 *	FULL2   OK
	 *	PAGE1_1 MISSING/DELETING <-
	 *	FULL1   MERGED/MERGING
	 */
	if (dest_backup->backup_mode == BACKUP_MODE_FULL)
	{
		full_backup = dest_backup;
		dest_backup = NULL;
		elog(INFO, "Merge target backup %s is full backup",
						base36enc(full_backup->start_time));

		/* sanity */
		if (full_backup->status == BACKUP_STATUS_DELETING)
			elog(ERROR, "Backup %s has status: %s",
							base36enc(full_backup->start_time),
							status2str(full_backup->status));

		/* Case #1 */
		if (full_backup->status == BACKUP_STATUS_OK ||
			full_backup->status == BACKUP_STATUS_DONE)
		{
			/* Check the case of FULL backup having more than one direct children */
			if (is_prolific(backups, full_backup))
				elog(ERROR, "Merge target is full backup and has multiple direct children, "
					"you must specify child backup id you want to merge with");

			elog(LOG, "Looking for closest incremental backup to merge with");

			/* Look for closest child backup */
			for (i = 0; i < parray_num(backups); i++)
			{
				pgBackup   *backup = (pgBackup *) parray_get(backups, i);

				/* skip unsuitable candidates */
				if (backup->status != BACKUP_STATUS_OK &&
					backup->status != BACKUP_STATUS_DONE)
					continue;

				if (backup->parent_backup == full_backup->start_time)
				{
					dest_backup = backup;
					break;
				}
			}

			/* sanity */
			if (dest_backup == NULL)
				elog(ERROR, "Failed to find merge candidate, "
							"backup %s has no valid children",
					base36enc(full_backup->start_time));

		}
		/* Case #2 */
		else if (full_backup->status == BACKUP_STATUS_MERGING)
		{
			/*
			 * MERGING - merge was ongoing at the moment of crash.
			 * We must find destination backup and rerun merge.
			 * If destination backup is missing, then merge must be aborted,
			 * there is no recovery from this situation.
			 */

			if (full_backup->merge_dest_backup == INVALID_BACKUP_ID)
				elog(ERROR, "Failed to determine merge destination backup");

			/* look up destination backup */
			for (i = 0; i < parray_num(backups); i++)
			{
				pgBackup   *backup = (pgBackup *) parray_get(backups, i);

				if (backup->start_time == full_backup->merge_dest_backup)
				{
					dest_backup = backup;
					break;
				}
			}
			if (!dest_backup)
			{
				char *tmp_backup_id = base36enc_dup(full_backup->start_time);
				elog(ERROR, "Full backup %s has unfinished merge with missing backup %s",
								tmp_backup_id,
								base36enc(full_backup->merge_dest_backup));
				pg_free(tmp_backup_id);
			}
		}
		else if (full_backup->status == BACKUP_STATUS_MERGED)
		{
			/*
			 * MERGED - merge crashed after files were transfered, but
			 * before rename could take place.
			 * If destination backup is missing, this is ok.
			 * If destination backup is present, then it should be deleted.
			 * After that FULL backup must acquire destination backup ID.
			 */

			/* destination backup may or may not exists */
			for (i = 0; i < parray_num(backups); i++)
			{
				pgBackup   *backup = (pgBackup *) parray_get(backups, i);

				if (backup->start_time == full_backup->merge_dest_backup)
				{
					dest_backup = backup;
					break;
				}
			}
			if (!dest_backup)
			{
				char *tmp_backup_id = base36enc_dup(full_backup->start_time);
				elog(WARNING, "Full backup %s has unfinished merge with missing backup %s",
								tmp_backup_id,
								base36enc(full_backup->merge_dest_backup));
				pg_free(tmp_backup_id);
			}
		}
		else
			elog(ERROR, "Backup %s has status: %s",
					base36enc(full_backup->start_time),
					status2str(full_backup->status));
	}
	else
	{
		/*
		 * Legal Case #1:
		 *	PAGE2 OK <- target
		 *	PAGE1 OK
		 *	FULL OK
		 * Legal Case #2:
		 *	PAGE2 MERGING <- target
		 *	PAGE1 MERGING
		 *	FULL MERGING
		 * Legal Case #3:
		 *	PAGE2 MERGING <- target
		 *	PAGE1 DELETING
		 *	FULL MERGED
		 * Legal Case #4:
		 *	PAGE2 MERGING <- target
		 *	PAGE1 missing
		 *	FULL MERGED
		 * Legal Case #5:
		 *	PAGE2 DELETING <- target
		 *	FULL MERGED
		 * Legal Case #6:
		 *	PAGE2 MERGING <- target
		 *	PAGE1 missing
		 *	FULL MERGED
		 * Illegal Case #7:
		 *	PAGE2 MERGING <- target
		 *	PAGE1 missing
		 *	FULL MERGING
		 */

		if (dest_backup->status == BACKUP_STATUS_MERGING ||
			dest_backup->status == BACKUP_STATUS_DELETING)
			elog(WARNING, "Rerun unfinished merge for backup %s",
							base36enc(dest_backup->start_time));

		/* First we should try to find parent FULL backup */
		full_backup = find_parent_full_backup(dest_backup);

		/* Chain is broken, one or more member of parent chain is missing */
		if (full_backup == NULL)
		{
			/* It is the legal state of affairs in Case #4, but
			 * only for MERGING incremental target backup and only
			 * if FULL backup has MERGED status.
			 */
			if (dest_backup->status != BACKUP_STATUS_MERGING)
				elog(ERROR, "Failed to find parent full backup for %s",
					base36enc(dest_backup->start_time));

			/* Find FULL backup that has unfinished merge with dest backup */
			for (i = 0; i < parray_num(backups); i++)
			{
				pgBackup   *backup = (pgBackup *) parray_get(backups, i);

				if (backup->merge_dest_backup == dest_backup->start_time)
				{
					full_backup = backup;
					break;
				}
			}

			if (!full_backup)
				elog(ERROR, "Failed to find full backup that has unfinished merge"
							"with backup %s, cannot rerun merge",
										base36enc(dest_backup->start_time));

			if (full_backup->status == BACKUP_STATUS_MERGED)
				elog(WARNING, "Incremental chain is broken, try to recover unfinished merge");
			else
				elog(ERROR, "Incremental chain is broken, merge is impossible to finish");
		}
		else
		{
			if ((full_backup->status == BACKUP_STATUS_MERGED ||
				full_backup->status == BACKUP_STATUS_MERGED) &&
				dest_backup->start_time != full_backup->merge_dest_backup)
			{
				char *tmp_backup_id = base36enc_dup(full_backup->start_time);
				elog(ERROR, "Full backup %s has unfinished merge with backup %s",
					tmp_backup_id, base36enc(full_backup->merge_dest_backup));
				pg_free(tmp_backup_id);
			}

		}
	}

	/* sanity */
	if (full_backup == NULL)
		elog(ERROR, "Parent full backup for the given backup %s was not found",
			 base36enc(backup_id));

	/* At this point NULL as dest_backup is allowed only in case of full backup
	 * having status MERGED */
	if (dest_backup == NULL && full_backup->status != BACKUP_STATUS_MERGED)
		elog(ERROR, "Cannot run merge for full backup %s",
					base36enc(full_backup->start_time));

	/* sanity */
	if (full_backup->status != BACKUP_STATUS_OK &&
		full_backup->status != BACKUP_STATUS_DONE &&
		/* It is possible that previous merging was interrupted */
		full_backup->status != BACKUP_STATUS_MERGED &&
		full_backup->status != BACKUP_STATUS_MERGING)
		elog(ERROR, "Backup %s has status: %s",
				base36enc(full_backup->start_time), status2str(full_backup->status));

	/* Form merge list */
	dest_backup_tmp = dest_backup;

	/* While loop below may looks strange, it is done so on purpose
	 * to handle both whole and broken incremental chains.
	 */
	while (dest_backup_tmp)
	{
		/* sanity */
		if (dest_backup_tmp->status != BACKUP_STATUS_OK &&
			dest_backup_tmp->status != BACKUP_STATUS_DONE &&
			/* It is possible that previous merging was interrupted */
			dest_backup_tmp->status != BACKUP_STATUS_MERGING &&
			dest_backup_tmp->status != BACKUP_STATUS_MERGED &&
			dest_backup_tmp->status != BACKUP_STATUS_DELETING)
			elog(ERROR, "Backup %s has status: %s",
					base36enc(dest_backup_tmp->start_time),
					status2str(dest_backup_tmp->status));

		if (dest_backup_tmp->backup_mode == BACKUP_MODE_FULL)
			break;

		parray_append(merge_list, dest_backup_tmp);
		dest_backup_tmp = dest_backup_tmp->parent_backup_link;
	}

	/* Add FULL backup */
	parray_append(merge_list, full_backup);

	/* Lock merge chain */
	catalog_lock_backup_list(merge_list, parray_num(merge_list) - 1, 0, true, true);

	/* do actual merge */
	merge_chain(merge_list, full_backup, dest_backup);

	pgBackupValidate(full_backup, NULL);
	if (full_backup->status == BACKUP_STATUS_CORRUPT)
		elog(ERROR, "Merging of backup %s failed", base36enc(backup_id));

	/* cleanup */
	parray_walk(backups, pgBackupFree);
	parray_free(backups);
	parray_free(merge_list);

	elog(INFO, "Merge of backup %s completed", base36enc(backup_id));
}

/*
 * Merge backup chain.
 * dest_backup - incremental backup.
 * parent_chain - array of backups starting with dest_backup and
 *	ending with full_backup.
 *
 * Copy backup files from incremental backups from parent_chain into
 * full backup directory.
 * Remove unnecessary directories and files from full backup directory.
 * Update metadata of full backup to represent destination backup.
 *
 * TODO: stop relying on caller to provide valid parent_chain, make sure
 * that chain is ok.
 */
void
merge_chain(parray *parent_chain, pgBackup *full_backup, pgBackup *dest_backup)
{
	int			i;
	char 		*dest_backup_id;
	char		full_external_prefix[MAXPGPATH];
	char		full_database_dir[MAXPGPATH];
	parray		*full_externals = NULL,
				*dest_externals = NULL;

	parray		*result_filelist = NULL;
	bool        use_bitmap = true;
	bool        is_retry = false;
//	size_t 		total_in_place_merge_bytes = 0;

	pthread_t	*threads = NULL;
	merge_files_arg *threads_args = NULL;
	time_t		merge_time;
	bool		merge_isok = true;
	/* for fancy reporting */
	time_t		end_time;
	char		pretty_time[20];
	/* in-place merge flags */
	bool		compression_match = false;
	bool		program_version_match = false;
	/* It's redundant to check block checksumms during merge */
	skip_block_validation = true;

	/* Handle corner cases of missing destination backup */
	if (dest_backup == NULL &&
		full_backup->status == BACKUP_STATUS_MERGED)
		goto merge_rename;

	if (!dest_backup)
		elog(ERROR, "Destination backup is missing, cannot continue merge");

	if (dest_backup->status == BACKUP_STATUS_MERGING ||
		full_backup->status == BACKUP_STATUS_MERGING ||
		full_backup->status == BACKUP_STATUS_MERGED)
	{
		is_retry = true;
		elog(INFO, "Retry failed merge of backup %s with parent chain", base36enc(dest_backup->start_time));
	}
	else
		elog(INFO, "Merging backup %s with parent chain", base36enc(dest_backup->start_time));

	/* sanity */
	if (full_backup->merge_dest_backup != INVALID_BACKUP_ID &&
		full_backup->merge_dest_backup != dest_backup->start_time)
	{
		char *merge_dest_backup_current = base36enc_dup(dest_backup->start_time);
		char *merge_dest_backup = base36enc_dup(full_backup->merge_dest_backup);

		elog(ERROR, "Cannot run merge for %s, because full backup %s has "
					"unfinished merge with backup %s",
			merge_dest_backup_current,
			base36enc(full_backup->start_time),
			merge_dest_backup);

		pg_free(merge_dest_backup_current);
		pg_free(merge_dest_backup);
	}

	/*
	 * Previous merging was interrupted during deleting source backup. It is
	 * safe just to delete it again.
	 */
	if (full_backup->status == BACKUP_STATUS_MERGED)
		goto merge_delete;

	/* Forward compatibility is not supported */
	for (i = parray_num(parent_chain) - 1; i >= 0; i--)
	{
		pgBackup   *backup = (pgBackup *) parray_get(parent_chain, i);

		if (parse_program_version(backup->program_version) >
			parse_program_version(PROGRAM_VERSION))
		{
			elog(ERROR, "Backup %s has been produced by pg_probackup version %s, "
						"but current program version is %s. Forward compatibility "
						"is not supported.",
				base36enc(backup->start_time),
				backup->program_version,
				PROGRAM_VERSION);
		}
	}

	/* If destination backup compression algorithm differs from
	 * full backup compression algorithm, then in-place merge is
	 * not possible.
	 */
	if (full_backup->compress_alg == dest_backup->compress_alg)
		compression_match = true;
	else
		elog(WARNING, "In-place merge is disabled because of compression "
					"algorithms mismatch");

	/*
	 * If current program version differs from destination backup version,
	 * then in-place merge is not possible.
	 */
	program_version_match = is_forward_compatible(parent_chain);

	/* Forbid merge retry for failed merges between 2.4.0 and any
	 * older version. Several format changes makes it impossible
	 * to determine the exact format any speific file is got.
	 */
	if (is_retry &&
		parse_program_version(dest_backup->program_version) >= 20400 &&
		parse_program_version(full_backup->program_version) < 20400)
	{
		elog(ERROR, "Retry of failed merge for backups with different between minor "
			"versions is forbidden to avoid data corruption because of storage format "
			"changes introduced in 2.4.0 version, please take a new full backup");
	}

	/*
	 * Validate or revalidate all members of parent chain
	 * with sole exception of FULL backup. If it has MERGING status
	 * then it isn't valid backup until merging is finished.
	 */
	elog(INFO, "Validate parent chain for backup %s",
					base36enc(dest_backup->start_time));

	for (i = parray_num(parent_chain) - 1; i >= 0; i--)
	{
		pgBackup   *backup = (pgBackup *) parray_get(parent_chain, i);

		/* FULL backup is not to be validated if its status is MERGING */
		if (backup->backup_mode == BACKUP_MODE_FULL &&
			backup->status == BACKUP_STATUS_MERGING)
		{
			continue;
		}

		pgBackupValidate(backup, NULL);

		if (backup->status != BACKUP_STATUS_OK)
			elog(ERROR, "Backup %s has status %s, merge is aborted",
				base36enc(backup->start_time), status2str(backup->status));
	}

	/*
	 * Get backup files.
	 */
	for (i = parray_num(parent_chain) - 1; i >= 0; i--)
	{
		pgBackup   *backup = (pgBackup *) parray_get(parent_chain, i);

		backup->files = get_backup_filelist(backup, true);
		parray_qsort(backup->files, pgFileCompareRelPathWithExternal);

		/* Set MERGING status for every member of the chain */
		if (backup->backup_mode == BACKUP_MODE_FULL)
		{
			/* In case of FULL backup also remember backup_id of
			 * of destination backup we are merging with, so
			 * we can safely allow rerun merge in case of failure.
			 */
			backup->merge_dest_backup = dest_backup->start_time;
			backup->status = BACKUP_STATUS_MERGING;
			write_backup(backup, true);
		}
		else
			write_backup_status(backup, BACKUP_STATUS_MERGING, instance_name, true);
	}

	/* Construct path to database dir: /backup_dir/instance_name/FULL/database */
	join_path_components(full_database_dir, full_backup->root_dir, DATABASE_DIR);
	/* Construct path to external dir: /backup_dir/instance_name/FULL/external */
	join_path_components(full_external_prefix, full_backup->root_dir, EXTERNAL_DIR);

	/* Create directories */
	create_data_directories(dest_backup->files, full_database_dir,
							dest_backup->root_dir, false, false, FIO_BACKUP_HOST);

	/* External directories stuff */
	if (dest_backup->external_dir_str)
		dest_externals = make_external_directory_list(dest_backup->external_dir_str, false);
	if (full_backup->external_dir_str)
		full_externals = make_external_directory_list(full_backup->external_dir_str, false);
	/*
	 * Rename external directories in FULL backup (if exists)
	 * according to numeration of external dirs in destionation backup.
	 */
	if (full_externals && dest_externals)
		reorder_external_dirs(full_backup, full_externals, dest_externals);

	/* bitmap optimization rely on n_blocks, which is generally available since 2.3.0 */
	if (parse_program_version(dest_backup->program_version) < 20300)
		use_bitmap = false;

	/* Setup threads */
	for (i = 0; i < parray_num(dest_backup->files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(dest_backup->files, i);

		/* if the entry was an external directory, create it in the backup */
		if (file->external_dir_num && S_ISDIR(file->mode))
		{
			char		dirpath[MAXPGPATH];
			char		new_container[MAXPGPATH];

			makeExternalDirPathByNum(new_container, full_external_prefix,
									 file->external_dir_num);
			join_path_components(dirpath, new_container, file->rel_path);
			dir_create_dir(dirpath, DIR_PERMISSION, false);
		}

		pg_atomic_init_flag(&file->lock);
	}

	threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
	threads_args = (merge_files_arg *) palloc(sizeof(merge_files_arg) * num_threads);

	thread_interrupted = false;
	merge_time = time(NULL);
	elog(INFO, "Start merging backup files");
	for (i = 0; i < num_threads; i++)
	{
		merge_files_arg *arg = &(threads_args[i]);
		arg->merge_filelist = parray_new();
		arg->parent_chain = parent_chain;
		arg->dest_backup = dest_backup;
		arg->full_backup = full_backup;
		arg->full_database_dir = full_database_dir;
		arg->full_external_prefix = full_external_prefix;

		arg->compression_match = compression_match;
		arg->program_version_match = program_version_match;
		arg->use_bitmap = use_bitmap;
		arg->is_retry = is_retry;
		/* By default there are some error */
		arg->ret = 1;

		elog(VERBOSE, "Start thread: %d", i);

		pthread_create(&threads[i], NULL, merge_files, arg);
	}

	/* Wait threads */
	result_filelist = parray_new();
	for (i = 0; i < num_threads; i++)
	{
		pthread_join(threads[i], NULL);
		if (threads_args[i].ret == 1)
			merge_isok = false;

		/* Compile final filelist */
		parray_concat(result_filelist, threads_args[i].merge_filelist);

		/* cleanup */
		parray_free(threads_args[i].merge_filelist);
		//total_in_place_merge_bytes += threads_args[i].in_place_merge_bytes;
	}

	time(&end_time);
	pretty_time_interval(difftime(end_time, merge_time),
						 pretty_time, lengthof(pretty_time));

	if (merge_isok)
		elog(INFO, "Backup files are successfully merged, time elapsed: %s",
				pretty_time);
	else
		elog(ERROR, "Backup files merging failed, time elapsed: %s",
				pretty_time);

	/* If temp header map is open, then close it and make rename */
	if (full_backup->hdr_map.fp)
	{
		cleanup_header_map(&(full_backup->hdr_map));

		/* sync new header map to disk */
		if (fio_sync(full_backup->hdr_map.path_tmp, FIO_BACKUP_HOST) != 0)
			elog(ERROR, "Cannot sync temp header map \"%s\": %s",
				full_backup->hdr_map.path_tmp, strerror(errno));

		/* Replace old header map with new one */
		if (rename(full_backup->hdr_map.path_tmp, full_backup->hdr_map.path))
			elog(ERROR, "Could not rename file \"%s\" to \"%s\": %s",
				 full_backup->hdr_map.path_tmp, full_backup->hdr_map.path, strerror(errno));
	}

	/* Close page header maps */
	for (i = parray_num(parent_chain) - 1; i >= 0; i--)
	{
		pgBackup   *backup = (pgBackup *) parray_get(parent_chain, i);
		cleanup_header_map(&(backup->hdr_map));
	}

	/*
	 * Update FULL backup metadata.
	 * We cannot set backup status to OK just yet,
	 * because it still has old start_time.
	 */
	StrNCpy(full_backup->program_version, PROGRAM_VERSION,
			sizeof(full_backup->program_version));
	full_backup->parent_backup = INVALID_BACKUP_ID;
	full_backup->start_lsn = dest_backup->start_lsn;
	full_backup->stop_lsn = dest_backup->stop_lsn;
	full_backup->recovery_time = dest_backup->recovery_time;
	full_backup->recovery_xid = dest_backup->recovery_xid;
	full_backup->tli = dest_backup->tli;
	full_backup->from_replica = dest_backup->from_replica;

	pfree(full_backup->external_dir_str);
	full_backup->external_dir_str = pgut_strdup(dest_backup->external_dir_str);
	pfree(full_backup->primary_conninfo);
	full_backup->primary_conninfo = pgut_strdup(dest_backup->primary_conninfo);

	full_backup->merge_time = merge_time;
	full_backup->end_time = time(NULL);

	full_backup->compress_alg = dest_backup->compress_alg;
	full_backup->compress_level = dest_backup->compress_level;

	/* If incremental backup is pinned,
	 * then result FULL backup must also be pinned.
	 * And reverse, if FULL backup was pinned and dest was not,
	 * then pinning is no more.
	 */
	full_backup->expire_time = dest_backup->expire_time;

	pg_free(full_backup->note);
	full_backup->note = NULL;

	if (dest_backup->note)
		full_backup->note = pgut_strdup(dest_backup->note);

	/* FULL backup must inherit wal mode. */
	full_backup->stream = dest_backup->stream;

	/* ARCHIVE backup must inherit wal_bytes too.
	 * STREAM backup will have its wal_bytes calculated by
	 * write_backup_filelist().
	 */
	if (!dest_backup->stream)
		full_backup->wal_bytes = dest_backup->wal_bytes;

	parray_qsort(result_filelist, pgFileCompareRelPathWithExternal);

	write_backup_filelist(full_backup, result_filelist, full_database_dir, NULL, true);
	write_backup(full_backup, true);

	/* Delete FULL backup files, that do not exists in destination backup
	 * Both arrays must be sorted in in reversed order to delete from leaf
	 */
	parray_qsort(dest_backup->files, pgFileCompareRelPathWithExternalDesc);
	parray_qsort(full_backup->files, pgFileCompareRelPathWithExternalDesc);
	for (i = 0; i < parray_num(full_backup->files); i++)
	{
		pgFile	   *full_file = (pgFile *) parray_get(full_backup->files, i);

		if (full_file->external_dir_num && full_externals)
		{
			char *dir_name = parray_get(full_externals, full_file->external_dir_num - 1);
			if (backup_contains_external(dir_name, full_externals))
				/* Dir already removed*/
				continue;
		}

		if (parray_bsearch(dest_backup->files, full_file, pgFileCompareRelPathWithExternalDesc) == NULL)
		{
			char		full_file_path[MAXPGPATH];

			/* We need full path, file object has relative path */
			join_path_components(full_file_path, full_database_dir, full_file->rel_path);

			pgFileDelete(full_file->mode, full_file_path);
			elog(VERBOSE, "Deleted \"%s\"", full_file_path);
		}
	}

	/* Critical section starts.
	 * Change status of FULL backup.
	 * Files are merged into FULL backup. It is time to remove incremental chain.
	 */
	full_backup->status = BACKUP_STATUS_MERGED;
	write_backup(full_backup, true);

merge_delete:
	for (i = parray_num(parent_chain) - 2; i >= 0; i--)
	{
		pgBackup   *backup = (pgBackup *) parray_get(parent_chain, i);
		delete_backup_files(backup);
	}

	/*
	 * PAGE2 DELETED
	 * PAGE1 DELETED
	 * FULL  MERGED
	 * If we crash now, automatic rerun of failed merge is still possible:
	 * The user should start merge with full backup ID as an argument to option '-i'.
	 */

merge_rename:
	/*
	 * Rename FULL backup directory to destination backup directory.
	 */
	if (dest_backup)
	{
		elog(LOG, "Rename %s to %s", full_backup->root_dir, dest_backup->root_dir);
		if (rename(full_backup->root_dir, dest_backup->root_dir) == -1)
			elog(ERROR, "Could not rename directory \"%s\" to \"%s\": %s",
				 full_backup->root_dir, dest_backup->root_dir, strerror(errno));

		/* update root_dir after rename */
		pg_free(full_backup->root_dir);
		full_backup->root_dir = pgut_strdup(dest_backup->root_dir);
	}
	else
	{
		/* Ugly */
		char 	backups_dir[MAXPGPATH];
		char 	instance_dir[MAXPGPATH];
		char 	destination_path[MAXPGPATH];

		join_path_components(backups_dir, backup_path, BACKUPS_DIR);
		join_path_components(instance_dir, backups_dir, instance_name);
		join_path_components(destination_path, instance_dir,
							base36enc(full_backup->merge_dest_backup));

		elog(LOG, "Rename %s to %s", full_backup->root_dir, destination_path);
		if (rename(full_backup->root_dir, destination_path) == -1)
			elog(ERROR, "Could not rename directory \"%s\" to \"%s\": %s",
				 full_backup->root_dir, destination_path, strerror(errno));

		/* update root_dir after rename */
		pg_free(full_backup->root_dir);
		full_backup->root_dir = pgut_strdup(destination_path);
	}

	/* Reinit path to database_dir */
	join_path_components(full_backup->database_dir, full_backup->root_dir, DATABASE_DIR);

	/* If we crash here, it will produce full backup in MERGED
	 * status, located in directory with wrong backup id.
	 * It should not be a problem.
	 */

	/*
	 * Merging finished, now we can safely update ID of the FULL backup
	 */
	dest_backup_id = base36enc_dup(full_backup->merge_dest_backup);
	elog(INFO, "Rename merged full backup %s to %s",
				base36enc(full_backup->start_time), dest_backup_id);

	full_backup->status = BACKUP_STATUS_OK;
	full_backup->start_time = full_backup->merge_dest_backup;
	full_backup->merge_dest_backup = INVALID_BACKUP_ID;
	write_backup(full_backup, true);
	/* Critical section end */

	/* Cleanup */
	pg_free(dest_backup_id);
	if (threads)
	{
		pfree(threads_args);
		pfree(threads);
	}

	if (result_filelist && parray_num(result_filelist) > 0)
	{
		parray_walk(result_filelist, pgFileFree);
		parray_free(result_filelist);
	}

	if (dest_externals != NULL)
		free_dir_list(dest_externals);

	if (full_externals != NULL)
		free_dir_list(full_externals);

	for (i = parray_num(parent_chain) - 1; i >= 0; i--)
	{
		pgBackup   *backup = (pgBackup *) parray_get(parent_chain, i);

		if (backup->files)
		{
			parray_walk(backup->files, pgFileFree);
			parray_free(backup->files);
		}
	}
}

/*
 * Thread worker of merge_chain().
 */
static void *
merge_files(void *arg)
{
	int		i;
	merge_files_arg *arguments = (merge_files_arg *) arg;
	size_t n_files = parray_num(arguments->dest_backup->files);

	for (i = 0; i < n_files; i++)
	{
		pgFile	   *dest_file = (pgFile *) parray_get(arguments->dest_backup->files, i);
		pgFile	   *tmp_file;
		bool		in_place = false; /* keep file as it is */

		/* check for interrupt */
		if (interrupted || thread_interrupted)
			elog(ERROR, "Interrupted during merge");

		if (!pg_atomic_test_set_flag(&dest_file->lock))
			continue;

		tmp_file = pgFileInit(dest_file->rel_path);
		tmp_file->mode = dest_file->mode;
		tmp_file->is_datafile = dest_file->is_datafile;
		tmp_file->is_cfs = dest_file->is_cfs;
		tmp_file->external_dir_num = dest_file->external_dir_num;
		tmp_file->dbOid = dest_file->dbOid;

		/* Directories were created before */
		if (S_ISDIR(dest_file->mode))
			goto done;

		if (progress)
			elog(INFO, "Progress: (%d/%lu). Merging file \"%s\"",
				i + 1, n_files, dest_file->rel_path);

		if (dest_file->is_datafile && !dest_file->is_cfs)
			tmp_file->segno = dest_file->segno;

		// If destination file is 0 sized, then go for the next
		if (dest_file->write_size == 0)
		{
			if (!dest_file->is_datafile || dest_file->is_cfs)
				tmp_file->crc = dest_file->crc;

			tmp_file->write_size = 0;
			goto done;
		}

		/*
		 * If file didn`t changed over the course of all incremental chain,
		 * then do in-place merge, unless destination backup has
		 * different compression algorithm.
		 * In-place merge is also impossible, if program version of destination
		 * backup differs from PROGRAM_VERSION
		 */
		if (arguments->program_version_match && arguments->compression_match &&
			!arguments->is_retry)
		{
			/*
			 * Case 1:
			 * in this case in place merge is possible:
			 * 0 PAGE; file, size BYTES_INVALID
			 * 1 PAGE; file, size BYTES_INVALID
			 * 2 FULL; file, size 100500
			 *
			 * Case 2:
			 * in this case in place merge is possible:
			 * 0 PAGE; file, size 0
			 * 1 PAGE; file, size 0
			 * 2 FULL; file, size 100500
			 *
			 * Case 3:
			 * in this case in place merge is impossible:
			 * 0 PAGE; file, size BYTES_INVALID
			 * 1 PAGE; file, size 100501
			 * 2 FULL; file, size 100500
			 *
			 * Case 4 (good candidate for future optimization):
			 * in this case in place merge is impossible:
			 * 0 PAGE; file, size BYTES_INVALID
			 * 1 PAGE; file, size 100501
			 * 2 FULL; file, not exists yet
			 */

			in_place = true;

			for (i = parray_num(arguments->parent_chain) - 1; i >= 0; i--)
			{
				pgFile	   **res_file = NULL;
				pgFile	   *file = NULL;

				pgBackup   *backup = (pgBackup *) parray_get(arguments->parent_chain, i);

				/* lookup file in intermediate backup */
				res_file =  parray_bsearch(backup->files, dest_file, pgFileCompareRelPathWithExternal);
				file = (res_file) ? *res_file : NULL;

				/* Destination file is not exists yet,
				 * in-place merge is impossible
				 */
				if (file == NULL)
				{
					in_place = false;
					break;
				}

				/* Skip file from FULL backup */
				if (backup->backup_mode == BACKUP_MODE_FULL)
					continue;

				if (file->write_size != BYTES_INVALID)
				{
					in_place = false;
					break;
				}
			}
		}

		/*
		 * In-place merge means that file in FULL backup stays as it is,
		 * no additional actions are required.
		 * page header map cannot be trusted when retrying, so no
		 * in place merge for retry.
		 */
		if (in_place)
		{
			pgFile	   **res_file = NULL;
			pgFile	   *file = NULL;
			res_file = parray_bsearch(arguments->full_backup->files, dest_file,
										pgFileCompareRelPathWithExternal);
			file = (res_file) ? *res_file : NULL;

			/* If file didn`t changed in any way, then in-place merge is possible */
			if (file &&
				file->n_blocks == dest_file->n_blocks)
			{
				BackupPageHeader2 *headers = NULL;

				elog(VERBOSE, "The file didn`t changed since FULL backup, skip merge: \"%s\"",
								file->rel_path);

				tmp_file->crc = file->crc;
				tmp_file->write_size = file->write_size;

				if (dest_file->is_datafile && !dest_file->is_cfs)
				{
					tmp_file->n_blocks = file->n_blocks;
					tmp_file->compress_alg = file->compress_alg;
					tmp_file->uncompressed_size = file->n_blocks * BLCKSZ;

					tmp_file->n_headers = file->n_headers;
					tmp_file->hdr_crc = file->hdr_crc;
				}
				else
					tmp_file->uncompressed_size = tmp_file->write_size;

				/* Copy header metadata from old map into a new one */
				tmp_file->n_headers = file->n_headers;
				headers = get_data_file_headers(&(arguments->full_backup->hdr_map), file,
						parse_program_version(arguments->full_backup->program_version),
						true);

				/* sanity */
				if (!headers && file->n_headers > 0)
					elog(ERROR, "Failed to get headers for file \"%s\"", file->rel_path);

				write_page_headers(headers, tmp_file, &(arguments->full_backup->hdr_map), true);
				pg_free(headers);

				//TODO: report in_place merge bytes.
				goto done;
			}
		}

		if (dest_file->is_datafile && !dest_file->is_cfs)
			merge_data_file(arguments->parent_chain,
							arguments->full_backup,
							arguments->dest_backup,
							dest_file, tmp_file,
							arguments->full_database_dir,
							arguments->use_bitmap,
							arguments->is_retry);
		else
			merge_non_data_file(arguments->parent_chain,
								arguments->full_backup,
								arguments->dest_backup,
								dest_file, tmp_file,
								arguments->full_database_dir,
								arguments->full_external_prefix);

done:
		parray_append(arguments->merge_filelist, tmp_file);
	}

	/* Data files merging is successful */
	arguments->ret = 0;

	return NULL;
}

/* Recursively delete a directory and its contents */
static void
remove_dir_with_files(const char *path)
{
	parray	   *files = parray_new();
	int			i;
	char 		full_path[MAXPGPATH];

	dir_list_file(files, path, false, false, true, false, false, 0, FIO_LOCAL_HOST);
	parray_qsort(files, pgFileCompareRelPathWithExternalDesc);
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);

		join_path_components(full_path, path, file->rel_path);

		pgFileDelete(file->mode, full_path);
		elog(VERBOSE, "Deleted \"%s\"", full_path);
	}

	/* cleanup */
	parray_walk(files, pgFileFree);
	parray_free(files);
}

/* Get index of external directory */
static int
get_external_index(const char *key, const parray *list)
{
	int			i;

	if (!list) /* Nowhere to search */
		return -1;
	for (i = 0; i < parray_num(list); i++)
	{
		if (strcmp(key, parray_get(list, i)) == 0)
			return i + 1;
	}
	return -1;
}

/* Rename directories in to_backup according to order in from_external */
static void
reorder_external_dirs(pgBackup *to_backup, parray *to_external,
					  parray *from_external)
{
	char		externaldir_template[MAXPGPATH];
	int			i;

	join_path_components(externaldir_template, to_backup->root_dir, EXTERNAL_DIR);
	for (i = 0; i < parray_num(to_external); i++)
	{
		int from_num = get_external_index(parray_get(to_external, i),
										  from_external);
		if (from_num == -1)
		{
			char old_path[MAXPGPATH];
			makeExternalDirPathByNum(old_path, externaldir_template, i + 1);
			remove_dir_with_files(old_path);
		}
		else if (from_num != i + 1)
		{
			char old_path[MAXPGPATH];
			char new_path[MAXPGPATH];
			makeExternalDirPathByNum(old_path, externaldir_template, i + 1);
			makeExternalDirPathByNum(new_path, externaldir_template, from_num);
			elog(VERBOSE, "Rename %s to %s", old_path, new_path);
			if (rename (old_path, new_path) == -1)
				elog(ERROR, "Could not rename directory \"%s\" to \"%s\": %s",
					 old_path, new_path, strerror(errno));
		}
	}
}

/* Merge is usually happens as usual backup/restore via temp files, unless
 * file didn`t changed since FULL backup AND full a dest backup have the
 * same compression algorithm. In this case file can be left as it is.
 */
void
merge_data_file(parray *parent_chain, pgBackup *full_backup,
				pgBackup *dest_backup, pgFile *dest_file, pgFile *tmp_file,
				const char *full_database_dir, bool use_bitmap, bool is_retry)
{
	FILE   *out = NULL;
	char   *buffer = pgut_malloc(STDIO_BUFSIZE);
	char    to_fullpath[MAXPGPATH];
	char    to_fullpath_tmp1[MAXPGPATH]; /* used for restore */
	char    to_fullpath_tmp2[MAXPGPATH]; /* used for backup */

	/* The next possible optimization is copying "as is" the file
	 * from intermediate incremental backup, that didn`t changed in
	 * subsequent incremental backups. TODO.
	 */

	/* set fullpath of destination file and temp files */
	join_path_components(to_fullpath, full_database_dir, tmp_file->rel_path);
	snprintf(to_fullpath_tmp1, MAXPGPATH, "%s_tmp1", to_fullpath);
	snprintf(to_fullpath_tmp2, MAXPGPATH, "%s_tmp2", to_fullpath);

	/* open temp file */
	out = fopen(to_fullpath_tmp1, PG_BINARY_W);
	if (out == NULL)
		elog(ERROR, "Cannot open merge target file \"%s\": %s",
			 to_fullpath_tmp1, strerror(errno));
	setvbuf(out, buffer, _IOFBF, STDIO_BUFSIZE);

	/* restore file into temp file */
	tmp_file->size = restore_data_file(parent_chain, dest_file, out, to_fullpath_tmp1,
									   use_bitmap, NULL, InvalidXLogRecPtr, NULL,
									   /* when retrying merge header map cannot be trusted */
									   is_retry ? false : true);
	if (fclose(out) != 0)
		elog(ERROR, "Cannot close file \"%s\": %s",
			 to_fullpath_tmp1, strerror(errno));

	pg_free(buffer);

	/* tmp_file->size is greedy, even if there is single 8KB block in file,
	 * that was overwritten twice during restore_data_file, we would assume that its size is
	 * 16KB.
	 * TODO: maybe we should just trust dest_file->n_blocks?
	 * No, we can`t, because current binary can be used to merge
	 * 2 backups of old versions, where n_blocks is missing.
	 */

	backup_data_file(NULL, tmp_file, to_fullpath_tmp1, to_fullpath_tmp2,
				 InvalidXLogRecPtr, BACKUP_MODE_FULL,
				 dest_backup->compress_alg, dest_backup->compress_level,
				 dest_backup->checksum_version, 0, NULL,
				 &(full_backup->hdr_map), true);

	/* drop restored temp file */
	if (unlink(to_fullpath_tmp1) == -1)
		elog(ERROR, "Cannot remove file \"%s\": %s", to_fullpath_tmp1,
			 strerror(errno));

	/*
	 * In old (=<2.2.7) versions of pg_probackup n_blocks attribute of files
	 * in PAGE and PTRACK wasn`t filled.
	 */
	//Assert(tmp_file->n_blocks == dest_file->n_blocks);

	/* Backward compatibility kludge:
	 * When merging old backups, it is possible that
	 * to_fullpath_tmp2 size will be 0, and so it will be
	 * truncated in backup_data_file().
	 * TODO: remove in 3.0.0
	 */
	if (tmp_file->write_size == 0)
		return;

	/* sync second temp file to disk */
	if (fio_sync(to_fullpath_tmp2, FIO_BACKUP_HOST) != 0)
		elog(ERROR, "Cannot sync merge temp file \"%s\": %s",
			to_fullpath_tmp2, strerror(errno));

	/* Do atomic rename from second temp file to destination file */
	if (rename(to_fullpath_tmp2, to_fullpath) == -1)
			elog(ERROR, "Could not rename file \"%s\" to \"%s\": %s",
				 to_fullpath_tmp2, to_fullpath, strerror(errno));

	/* drop temp file */
	unlink(to_fullpath_tmp1);
}

/*
 * For every destionation file lookup the newest file in chain and
 * copy it.
 * Additional pain is external directories.
 */
void
merge_non_data_file(parray *parent_chain, pgBackup *full_backup,
				pgBackup *dest_backup, pgFile *dest_file, pgFile *tmp_file,
				const char *full_database_dir, const char *to_external_prefix)
{
	int		i;
	char	to_fullpath[MAXPGPATH];
	char	to_fullpath_tmp[MAXPGPATH]; /* used for backup */
	char	from_fullpath[MAXPGPATH];
	pgBackup *from_backup = NULL;
	pgFile *from_file = NULL;

	/* We need to make full path to destination file */
	if (dest_file->external_dir_num)
	{
		char temp[MAXPGPATH];
		makeExternalDirPathByNum(temp, to_external_prefix,
								 dest_file->external_dir_num);
		join_path_components(to_fullpath, temp, dest_file->rel_path);
	}
	else
		join_path_components(to_fullpath, full_database_dir, dest_file->rel_path);

	snprintf(to_fullpath_tmp, MAXPGPATH, "%s_tmp", to_fullpath);

	/*
	 * Iterate over parent chain starting from direct parent of destination
	 * backup to oldest backup in chain, and look for the first
	 * full copy of destination file.
	 * Full copy is latest possible destination file with size equal(!)
	 * or greater than zero.
	 */
	for (i = 0; i < parray_num(parent_chain); i++)
	{
		pgFile	   **res_file = NULL;
		from_backup = (pgBackup *) parray_get(parent_chain, i);

		/* lookup file in intermediate backup */
		res_file =  parray_bsearch(from_backup->files, dest_file, pgFileCompareRelPathWithExternal);
		from_file = (res_file) ? *res_file : NULL;

		/*
		 * It should not be possible not to find source file in intermediate
		 * backup, without encountering full copy first.
		 */
		if (!from_file)
		{
			elog(ERROR, "Failed to locate nonedata file \"%s\" in backup %s",
				dest_file->rel_path, base36enc(from_backup->start_time));
			continue;
		}

		if (from_file->write_size > 0)
			break;
	}

	/* sanity */
	if (!from_backup)
		elog(ERROR, "Failed to found a backup containing full copy of nonedata file \"%s\"",
			dest_file->rel_path);

	if (!from_file)
		elog(ERROR, "Failed to locate a full copy of nonedata file \"%s\"", dest_file->rel_path);

	/* set path to source file */
	if (from_file->external_dir_num)
	{
		char temp[MAXPGPATH];
		char external_prefix[MAXPGPATH];

		join_path_components(external_prefix, from_backup->root_dir, EXTERNAL_DIR);
		makeExternalDirPathByNum(temp, external_prefix, dest_file->external_dir_num);

		join_path_components(from_fullpath, temp, from_file->rel_path);
	}
	else
	{
		char backup_database_dir[MAXPGPATH];
		join_path_components(backup_database_dir, from_backup->root_dir, DATABASE_DIR);
		join_path_components(from_fullpath, backup_database_dir, from_file->rel_path);
	}

	/* Copy file to FULL backup directory into temp file */
	backup_non_data_file(tmp_file, NULL, from_fullpath,
						 to_fullpath_tmp, BACKUP_MODE_FULL, 0, false);

	/* sync temp file to disk */
	if (fio_sync(to_fullpath_tmp, FIO_BACKUP_HOST) != 0)
		elog(ERROR, "Cannot sync merge temp file \"%s\": %s",
			to_fullpath_tmp, strerror(errno));

	/* Do atomic rename from second temp file to destination file */
	if (rename(to_fullpath_tmp, to_fullpath) == -1)
			elog(ERROR, "Could not rename file \"%s\" to \"%s\": %s",
				to_fullpath_tmp, to_fullpath, strerror(errno));

}

/*
 * If file format in incremental chain is compatible
 * with current storage format.
 * If not, then in-place merge is not possible.
 *
 * Consider the following examples:
 * STORAGE_FORMAT_VERSION = 2.4.4
 * 2.3.3 \
 * 2.3.4  \ disable in-place merge, because
 * 2.4.1  / current STORAGE_FORMAT_VERSION > 2.3.3
 * 2.4.3 /
 *
 * 2.4.4 \ enable in_place merge, because
 * 2.4.5 / current STORAGE_FORMAT_VERSION == 2.4.4
 *
 * 2.4.5 \ enable in_place merge, because
 * 2.4.6 / current STORAGE_FORMAT_VERSION < 2.4.5
 *
 */
bool
is_forward_compatible(parray *parent_chain)
{
	int       i;
	pgBackup *oldest_ver_backup = NULL;
	uint32    oldest_ver_in_chain = parse_program_version(PROGRAM_VERSION);

	for (i = 0; i < parray_num(parent_chain); i++)
	{
		pgBackup *backup = (pgBackup *) parray_get(parent_chain, i);
		uint32 current_version = parse_program_version(backup->program_version);

		if (!oldest_ver_backup)
			oldest_ver_backup = backup;

		if (current_version < oldest_ver_in_chain)
		{
			oldest_ver_in_chain = current_version;
			oldest_ver_backup = backup;
		}
	}

	if (oldest_ver_in_chain < parse_program_version(STORAGE_FORMAT_VERSION))
	{
		elog(WARNING, "In-place merge is disabled because of storage format incompatibility. "
					"Backup %s storage format version: %s, "
					"current storage format version: %s",
					base36enc(oldest_ver_backup->start_time),
					oldest_ver_backup->program_version,
					STORAGE_FORMAT_VERSION);
		return false;
	}

	return true;
}