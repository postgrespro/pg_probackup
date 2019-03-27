/*-------------------------------------------------------------------------
 *
 * merge.c: merge FULL and incremental backups
 *
 * Copyright (c) 2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <sys/stat.h>
#include <unistd.h>

#include "utils/thread.h"

typedef struct
{
	parray	   *to_files;
	parray	   *files;
	parray	   *from_external;

	pgBackup   *to_backup;
	pgBackup   *from_backup;
	const char *to_root;
	const char *from_root;
	const char *to_external_prefix;
	const char *from_external_prefix;

	/*
	 * Return value from the thread.
	 * 0 means there is no error, 1 - there is an error.
	 */
	int			ret;
} merge_files_arg;

static void merge_backups(pgBackup *backup, pgBackup *next_backup);
static void *merge_files(void *arg);
static void
reorder_external_dirs(pgBackup *to_backup, parray *to_external,
					  parray *from_external);
static int
get_external_index(const char *key, const parray *list);

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
	pgBackup   *full_backup = NULL;
	int			i;

	if (backup_id == INVALID_BACKUP_ID)
		elog(ERROR, "required parameter is not specified: --backup-id");

	if (instance_name == NULL)
		elog(ERROR, "required parameter is not specified: --instance");

	elog(INFO, "Merge started");

	/* Get list of all backups sorted in order of descending start time */
	backups = catalog_get_backup_list(INVALID_BACKUP_ID);

	/* Find destination backup first */
	for (i = 0; i < parray_num(backups); i++)
	{
		pgBackup   *backup = (pgBackup *) parray_get(backups, i);

		/* found target */
		if (backup->start_time == backup_id)
		{
			/* sanity */
			if (backup->status != BACKUP_STATUS_OK &&
				/* It is possible that previous merging was interrupted */
				backup->status != BACKUP_STATUS_MERGING &&
				backup->status != BACKUP_STATUS_DELETING)
					elog(ERROR, "Backup %s has status: %s",
						 base36enc(backup->start_time), status2str(backup->status));

			if (backup->backup_mode == BACKUP_MODE_FULL)
				elog(ERROR, "Backup %s is full backup",
					 base36enc(backup->start_time));

			dest_backup = backup;
			break;
		}
	}

	/* sanity */
	if (dest_backup == NULL)
		elog(ERROR, "Target backup %s was not found", base36enc(backup_id));

	/* get full backup */
	full_backup = find_parent_full_backup(dest_backup);

	/* sanity */
	if (full_backup == NULL)
		elog(ERROR, "Parent full backup for the given backup %s was not found",
			 base36enc(backup_id));

	/* sanity */
	if (full_backup->status != BACKUP_STATUS_OK &&
		/* It is possible that previous merging was interrupted */
		full_backup->status != BACKUP_STATUS_MERGING)
			elog(ERROR, "Backup %s has status: %s",
					base36enc(full_backup->start_time), status2str(full_backup->status));

	//Assert(full_backup_idx != dest_backup_idx);

	/* form merge list */
	while(dest_backup->parent_backup_link)
	{
		/* sanity */
		if (dest_backup->status != BACKUP_STATUS_OK &&
			/* It is possible that previous merging was interrupted */
			dest_backup->status != BACKUP_STATUS_MERGING &&
			dest_backup->status != BACKUP_STATUS_DELETING)
				elog(ERROR, "Backup %s has status: %s",
						base36enc(dest_backup->start_time), status2str(dest_backup->status));

		parray_append(merge_list, dest_backup);
		dest_backup = dest_backup->parent_backup_link;
	}

	/* Add FULL backup for easy locking */
	parray_append(merge_list, full_backup);

	/* Lock merge chain */
	catalog_lock_backup_list(merge_list, parray_num(merge_list) - 1, 0);

	/*
	 * Found target and full backups, merge them and intermediate backups
	 */
	for (i = parray_num(merge_list) - 2; i >= 0; i--)
	{
		pgBackup   *from_backup = (pgBackup *) parray_get(merge_list, i);

		merge_backups(full_backup, from_backup);
	}

	pgBackupValidate(full_backup);
	if (full_backup->status == BACKUP_STATUS_CORRUPT)
		elog(ERROR, "Merging of backup %s failed", base36enc(backup_id));

	/* cleanup */
	parray_walk(backups, pgBackupFree);
	parray_free(backups);
	parray_free(merge_list);

	elog(INFO, "Merge of backup %s completed", base36enc(backup_id));
}

/*
 * Merge two backups data files using threads.
 * - move instance files from from_backup to to_backup
 * - remove unnecessary directories and files from to_backup
 * - update metadata of from_backup, it becames FULL backup
 */
static void
merge_backups(pgBackup *to_backup, pgBackup *from_backup)
{
	char	   *to_backup_id = base36enc_dup(to_backup->start_time),
			   *from_backup_id = base36enc_dup(from_backup->start_time);
	char		to_backup_path[MAXPGPATH],
				to_database_path[MAXPGPATH],
				to_external_prefix[MAXPGPATH],
				from_backup_path[MAXPGPATH],
				from_database_path[MAXPGPATH],
				from_external_prefix[MAXPGPATH],
				control_file[MAXPGPATH];
	parray	   *files,
			   *to_files;
	parray	   *to_external = NULL,
			   *from_external = NULL;
	pthread_t  *threads = NULL;
	merge_files_arg *threads_args = NULL;
	int			i;
	time_t		merge_time;
	bool		merge_isok = true;

	merge_time = time(NULL);
	elog(INFO, "Merging backup %s with backup %s", from_backup_id, to_backup_id);

	/*
	 * Validate to_backup only if it is BACKUP_STATUS_OK. If it has
	 * BACKUP_STATUS_MERGING status then it isn't valid backup until merging
	 * finished.
	 */
	if (to_backup->status == BACKUP_STATUS_OK)
	{
		pgBackupValidate(to_backup);
		if (to_backup->status == BACKUP_STATUS_CORRUPT)
			elog(ERROR, "Interrupt merging");
	}

	/*
	 * It is OK to validate from_backup if it has BACKUP_STATUS_OK or
	 * BACKUP_STATUS_MERGING status.
	 */
	Assert(from_backup->status == BACKUP_STATUS_OK ||
		   from_backup->status == BACKUP_STATUS_MERGING);
	pgBackupValidate(from_backup);
	if (from_backup->status == BACKUP_STATUS_CORRUPT)
		elog(ERROR, "Interrupt merging");

	/*
	 * Make backup paths.
	 */
	pgBackupGetPath(to_backup, to_backup_path, lengthof(to_backup_path), NULL);
	pgBackupGetPath(to_backup, to_database_path, lengthof(to_database_path),
					DATABASE_DIR);
	pgBackupGetPath(to_backup, to_external_prefix, lengthof(to_database_path),
					EXTERNAL_DIR);
	pgBackupGetPath(from_backup, from_backup_path, lengthof(from_backup_path), NULL);
	pgBackupGetPath(from_backup, from_database_path, lengthof(from_database_path),
					DATABASE_DIR);
	pgBackupGetPath(from_backup, from_external_prefix, lengthof(from_database_path),
					EXTERNAL_DIR);

	/*
	 * Get list of files which will be modified or removed.
	 */
	pgBackupGetPath(to_backup, control_file, lengthof(control_file),
					DATABASE_FILE_LIST);
	to_files = dir_read_file_list(NULL, NULL, control_file);
	/* To delete from leaf, sort in reversed order */
	parray_qsort(to_files, pgFileComparePathDesc);
	/*
	 * Get list of files which need to be moved.
	 */
	pgBackupGetPath(from_backup, control_file, lengthof(control_file),
					DATABASE_FILE_LIST);
	files = dir_read_file_list(NULL, NULL, control_file);
	/* sort by size for load balancing */
	parray_qsort(files, pgFileCompareSize);

	/*
	 * Previous merging was interrupted during deleting source backup. It is
	 * safe just to delete it again.
	 */
	if (from_backup->status == BACKUP_STATUS_DELETING)
		goto delete_source_backup;

	write_backup_status(to_backup, BACKUP_STATUS_MERGING);
	write_backup_status(from_backup, BACKUP_STATUS_MERGING);

	create_data_directories(to_database_path, from_backup_path, false);

	threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
	threads_args = (merge_files_arg *) palloc(sizeof(merge_files_arg) * num_threads);

	/* Create external directories lists */
	if (to_backup->external_dir_str)
		to_external = make_external_directory_list(to_backup->external_dir_str);
	if (from_backup->external_dir_str)
		from_external = make_external_directory_list(from_backup->external_dir_str);

	/*
	 * Rename external directoties in to_backup (if exists)
	 * according to numeration of external dirs in from_backup.
	 */
	if (to_external)
		reorder_external_dirs(to_backup, to_external, from_external);

	/* Setup threads */
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);

		/* if the entry was an external directory, create it in the backup */
		if (file->external_dir_num && S_ISDIR(file->mode))
		{
			char		dirpath[MAXPGPATH];
			char		new_container[MAXPGPATH];

			makeExternalDirPathByNum(new_container, to_external_prefix,
									 file->external_dir_num);
			join_path_components(dirpath, new_container, file->path);
			dir_create_dir(dirpath, DIR_PERMISSION);
		}
		pg_atomic_init_flag(&file->lock);
	}

	thread_interrupted = false;
	for (i = 0; i < num_threads; i++)
	{
		merge_files_arg *arg = &(threads_args[i]);

		arg->to_files = to_files;
		arg->files = files;
		arg->to_backup = to_backup;
		arg->from_backup = from_backup;
		arg->to_root = to_database_path;
		arg->from_root = from_database_path;
		arg->from_external = from_external;
		arg->to_external_prefix = to_external_prefix;
		arg->from_external_prefix = from_external_prefix;
		/* By default there are some error */
		arg->ret = 1;

		elog(VERBOSE, "Start thread: %d", i);

		pthread_create(&threads[i], NULL, merge_files, arg);
	}

	/* Wait threads */
	for (i = 0; i < num_threads; i++)
	{
		pthread_join(threads[i], NULL);
		if (threads_args[i].ret == 1)
			merge_isok = false;
	}
	if (!merge_isok)
		elog(ERROR, "Data files merging failed");

	/*
	 * Update to_backup metadata.
	 */
	to_backup->status = BACKUP_STATUS_OK;
	StrNCpy(to_backup->program_version, PROGRAM_VERSION,
			sizeof(to_backup->program_version));
	to_backup->parent_backup = INVALID_BACKUP_ID;
	to_backup->start_lsn = from_backup->start_lsn;
	to_backup->stop_lsn = from_backup->stop_lsn;
	to_backup->recovery_time = from_backup->recovery_time;
	to_backup->recovery_xid = from_backup->recovery_xid;
	pfree(to_backup->external_dir_str);
	to_backup->external_dir_str = from_backup->external_dir_str;
	from_backup->external_dir_str = NULL; /* For safe pgBackupFree() */
	to_backup->merge_time = merge_time;
	to_backup->end_time = time(NULL);

	/*
	 * Target backup must inherit wal mode too.
	 */
	to_backup->stream = from_backup->stream;
	/* Compute summary of size of regular files in the backup */
	to_backup->data_bytes = 0;
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);

		if (S_ISDIR(file->mode))
			to_backup->data_bytes += 4096;
		/* Count the amount of the data actually copied */
		else if (S_ISREG(file->mode))
			to_backup->data_bytes += file->write_size;
	}
	/* compute size of wal files of this backup stored in the archive */
	if (!to_backup->stream)
		to_backup->wal_bytes = instance_config.xlog_seg_size *
			(to_backup->stop_lsn / instance_config.xlog_seg_size -
			 to_backup->start_lsn / instance_config.xlog_seg_size + 1);
	else
		to_backup->wal_bytes = BYTES_INVALID;

	write_backup_filelist(to_backup, files, from_database_path,
						  from_external_prefix, NULL);
	write_backup(to_backup);

delete_source_backup:
	/*
	 * Files were copied into to_backup. It is time to remove source backup
	 * entirely.
	 */
	delete_backup_files(from_backup);

	/*
	 * Delete files which are not in from_backup file list.
	 */
	parray_qsort(files, pgFileComparePathDesc);
	for (i = 0; i < parray_num(to_files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(to_files, i);

		if (file->external_dir_num && to_external)
		{
			char *dir_name = parray_get(to_external, file->external_dir_num - 1);
			if (backup_contains_external(dir_name, from_external))
				/* Dir already removed*/
				continue;
		}

		if (parray_bsearch(files, file, pgFileComparePathDesc) == NULL)
		{
			char		to_file_path[MAXPGPATH];
			char	   *prev_path;

			/* We need full path, file object has relative path */
			join_path_components(to_file_path, to_database_path, file->path);
			prev_path = file->path;
			file->path = to_file_path;

			pgFileDelete(file);
			elog(VERBOSE, "Deleted \"%s\"", file->path);

			file->path = prev_path;
		}
	}

	/*
	 * Rename FULL backup directory.
	 */
	elog(INFO, "Rename %s to %s", to_backup_id, from_backup_id);
	if (rename(to_backup_path, from_backup_path) == -1)
		elog(ERROR, "Could not rename directory \"%s\" to \"%s\": %s",
			 to_backup_path, from_backup_path, strerror(errno));

	/*
	 * Merging finished, now we can safely update ID of the destination backup.
	 */
	to_backup->start_time = from_backup->start_time;
	write_backup(to_backup);

	/* Cleanup */
	if (threads)
	{
		pfree(threads_args);
		pfree(threads);
	}

	parray_walk(to_files, pgFileFree);
	parray_free(to_files);

	parray_walk(files, pgFileFree);
	parray_free(files);

	pfree(to_backup_id);
	pfree(from_backup_id);
}

/*
 * Thread worker of merge_backups().
 */
static void *
merge_files(void *arg)
{
	merge_files_arg *argument = (merge_files_arg *) arg;
	pgBackup   *to_backup = argument->to_backup;
	pgBackup   *from_backup = argument->from_backup;
	int			i,
				num_files = parray_num(argument->files);

	for (i = 0; i < num_files; i++)
	{
		pgFile	   *file = (pgFile *) parray_get(argument->files, i);
		pgFile	   *to_file;
		pgFile	  **res_file;
		char		to_file_path[MAXPGPATH];	/* Path of target file */
		char		from_file_path[MAXPGPATH];
		char	   *prev_file_path;

		if (!pg_atomic_test_set_flag(&file->lock))
			continue;

		/* check for interrupt */
		if (interrupted || thread_interrupted)
			elog(ERROR, "Interrupted during merging backups");

		/* Directories were created before */
		if (S_ISDIR(file->mode))
			continue;

		if (progress)
			elog(INFO, "Progress: (%d/%d). Process file \"%s\"",
				 i + 1, num_files, file->path);

		res_file = parray_bsearch(argument->to_files, file,
								  pgFileComparePathWithExternalDesc);
		to_file = (res_file) ? *res_file : NULL;

		join_path_components(to_file_path, argument->to_root, file->path);

		/*
		 * Skip files which haven't changed since previous backup. But in case
		 * of DELTA backup we should consider n_blocks to truncate the target
		 * backup.
		 */
		if (file->write_size == BYTES_INVALID && file->n_blocks == -1)
		{
			elog(VERBOSE, "Skip merging file \"%s\", the file didn't change",
				 file->path);

			/*
			 * If the file wasn't changed in PAGE backup, retreive its
			 * write_size from previous FULL backup.
			 */
			if (to_file)
			{
				file->compress_alg = to_file->compress_alg;
				file->write_size = to_file->write_size;

				/*
				 * Recalculate crc for backup prior to 2.0.25.
				 */
				if (parse_program_version(from_backup->program_version) < 20025)
					file->crc = pgFileGetCRC(to_file_path, true, true, NULL);
				/* Otherwise just get it from the previous file */
				else
					file->crc = to_file->crc;
			}

			continue;
		}

		/* We need to make full path, file object has relative path */
		if (file->external_dir_num)
		{
			char temp[MAXPGPATH];
			makeExternalDirPathByNum(temp, argument->from_external_prefix,
									 file->external_dir_num);

			join_path_components(from_file_path, temp, file->path);
		}
		else
			join_path_components(from_file_path, argument->from_root,
								 file->path);
		prev_file_path = file->path;
		file->path = from_file_path;

		/*
		 * Move the file. We need to decompress it and compress again if
		 * necessary.
		 */
		elog(VERBOSE, "Merging file \"%s\", is_datafile %d, is_cfs %d",
			 file->path, file->is_database, file->is_cfs);

		if (file->is_datafile && !file->is_cfs)
		{
			/*
			 * We need more complicate algorithm if target file should be
			 * compressed.
			 */
			if (to_backup->compress_alg == PGLZ_COMPRESS ||
				to_backup->compress_alg == ZLIB_COMPRESS)
			{
				char		tmp_file_path[MAXPGPATH];
				char	   *prev_path;

				snprintf(tmp_file_path, MAXPGPATH, "%s_tmp", to_file_path);

				/* Start the magic */

				/*
				 * Merge files:
				 * - if target file exists restore and decompress it to the temp
				 *   path
				 * - decompress source file if necessary and merge it with the
				 *   target decompressed file
				 * - compress result file
				 */

				/*
				 * We need to decompress target file if it exists.
				 */
				if (to_file)
				{
					elog(VERBOSE, "Merge target and source files into the temporary path \"%s\"",
						 tmp_file_path);

					/*
					 * file->path points to the file in from_root directory. But we
					 * need the file in directory to_root.
					 */
					prev_path = to_file->path;
					to_file->path = to_file_path;
					/* Decompress target file into temporary one */
					restore_data_file(tmp_file_path, to_file, false, false,
									  parse_program_version(to_backup->program_version));
					to_file->path = prev_path;
				}
				else
					elog(VERBOSE, "Restore source file into the temporary path \"%s\"",
						 tmp_file_path);
				/* Merge source file with target file */
				restore_data_file(tmp_file_path, file,
								  from_backup->backup_mode == BACKUP_MODE_DIFF_DELTA,
								  false,
								  parse_program_version(from_backup->program_version));

				elog(VERBOSE, "Compress file and save it into the directory \"%s\"",
					 argument->to_root);

				/* Again we need to change path */
				prev_path = file->path;
				file->path = tmp_file_path;
				/* backup_data_file() requires file size to calculate nblocks */
				file->size = pgFileSize(file->path);
				/* Now we can compress the file */
				backup_data_file(NULL, /* We shouldn't need 'arguments' here */
								 to_file_path, file,
								 to_backup->start_lsn,
								 to_backup->backup_mode,
								 to_backup->compress_alg,
								 to_backup->compress_level);

				file->path = prev_path;

				/* We can remove temporary file now */
				if (unlink(tmp_file_path))
					elog(ERROR, "Could not remove temporary file \"%s\": %s",
						 tmp_file_path, strerror(errno));
			}
			/*
			 * Otherwise merging algorithm is simpler.
			 */
			else
			{
				/* We can merge in-place here */
				restore_data_file(to_file_path, file,
								  from_backup->backup_mode == BACKUP_MODE_DIFF_DELTA,
								  true,
								  parse_program_version(from_backup->program_version));

				/*
				 * We need to calculate write_size, restore_data_file() doesn't
				 * do that.
				 */
				file->write_size = pgFileSize(to_file_path);
				file->crc = pgFileGetCRC(to_file_path, true, true, NULL);
			}
		}
		else if (file->external_dir_num)
		{
			char	from_root[MAXPGPATH];
			char	to_root[MAXPGPATH];
			int		new_dir_num;
			char   *file_external_path = parray_get(argument->from_external,
													file->external_dir_num - 1);

			Assert(argument->from_external);
			new_dir_num = get_external_index(file_external_path,
											 argument->from_external);
			makeExternalDirPathByNum(from_root, argument->from_external_prefix,
									 file->external_dir_num);
			makeExternalDirPathByNum(to_root, argument->to_external_prefix,
									 new_dir_num);
			copy_file(from_root, to_root, file);
		}
		else if (strcmp(file->name, "pg_control") == 0)
			copy_pgcontrol_file(argument->from_root, argument->to_root, file);
		else
			copy_file(argument->from_root, argument->to_root, file);

		/*
		 * We need to save compression algorithm type of the target backup to be
		 * able to restore in the future.
		 */
		file->compress_alg = to_backup->compress_alg;

		if (file->write_size != BYTES_INVALID)
			elog(LOG, "Merged file \"%s\": " INT64_FORMAT " bytes",
				 file->path, file->write_size);

		/* Restore relative path */
		file->path = prev_file_path;
	}

	/* Data files merging is successful */
	argument->ret = 0;

	return NULL;
}

/* Recursively delete a directory and its contents */
static void
remove_dir_with_files(const char *path)
{
	parray *files = parray_new();
	dir_list_file(files, path, true, true, true, 0);
	parray_qsort(files, pgFileComparePathDesc);
	for (int i = 0; i < parray_num(files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);

		pgFileDelete(file);
		elog(VERBOSE, "Deleted \"%s\"", file->path);
	}
}

/* Get index of external directory */
static int
get_external_index(const char *key, const parray *list)
{
	if (!list) /* Nowhere to search */
		return -1;
	for (int i = 0; i < parray_num(list); i++)
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
	char externaldir_template[MAXPGPATH];

	pgBackupGetPath(to_backup, externaldir_template,
					lengthof(externaldir_template), EXTERNAL_DIR);
	for (int i = 0; i < parray_num(to_external); i++)
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
