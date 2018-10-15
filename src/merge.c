/*-------------------------------------------------------------------------
 *
 * merge.c: merge FULL and incremental backups
 *
 * Copyright (c) 2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include <sys/stat.h>
#include <unistd.h>

#include "pg_probackup.h"

#include "utils/thread.h"

typedef struct
{
	parray	   *to_files;
	parray	   *files;

	pgBackup   *to_backup;
	pgBackup   *from_backup;

	const char *to_root;
	const char *from_root;

	/*
	 * Return value from the thread.
	 * 0 means there is no error, 1 - there is an error.
	 */
	int			ret;
} merge_files_arg;

static void merge_backups(pgBackup *backup, pgBackup *next_backup);
static void *merge_files(void *arg);

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
	pgBackup   *dest_backup = NULL;
	pgBackup   *full_backup = NULL;
	time_t		prev_parent = INVALID_BACKUP_ID;
	int			i;
	int			dest_backup_idx = 0;
	int			full_backup_idx = 0;

	if (backup_id == INVALID_BACKUP_ID)
		elog(ERROR, "required parameter is not specified: --backup-id");

	if (instance_name == NULL)
		elog(ERROR, "required parameter is not specified: --instance");

	elog(LOG, "Merge started");

	catalog_lock();

	/* Get list of all backups sorted in order of descending start time */
	backups = catalog_get_backup_list(INVALID_BACKUP_ID);

	/* Find destination and parent backups */
	for (i = 0; i < parray_num(backups); i++)
	{
		pgBackup   *backup = (pgBackup *) parray_get(backups, i);

		if (backup->start_time > backup_id)
			continue;
		else if (backup->start_time == backup_id && !dest_backup)
		{
			if (backup->status != BACKUP_STATUS_OK &&
				/* It is possible that previous merging was interrupted */
				backup->status != BACKUP_STATUS_MERGING)
				elog(ERROR, "Backup %s has status: %s",
					 base36enc(backup->start_time), status2str(backup->status));

			if (backup->backup_mode == BACKUP_MODE_FULL)
				elog(ERROR, "Backup %s is full backup",
					 base36enc(backup->start_time));

			dest_backup = backup;
			dest_backup_idx = i;
		}
		else
		{
			Assert(dest_backup);

			if (backup->start_time != prev_parent)
				continue;

			if (backup->status != BACKUP_STATUS_OK &&
				/* It is possible that previous merging was interrupted */
				backup->status != BACKUP_STATUS_MERGING)
				elog(ERROR, "Backup %s has status: %s",
					 base36enc(backup->start_time), status2str(backup->status));

			/* If we already found dest_backup, look for full backup */
			if (dest_backup && backup->backup_mode == BACKUP_MODE_FULL)
			{
				full_backup = backup;
				full_backup_idx = i;

				/* Found target and full backups, so break the loop */
				break;
			}
		}

		prev_parent = backup->parent_backup;
	}

	if (dest_backup == NULL)
		elog(ERROR, "Target backup %s was not found", base36enc(backup_id));
	if (full_backup == NULL)
		elog(ERROR, "Parent full backup for the given backup %s was not found",
			 base36enc(backup_id));

	Assert(full_backup_idx != dest_backup_idx);

	/*
	 * Found target and full backups, merge them and intermediate backups
	 */
	for (i = full_backup_idx; i > dest_backup_idx; i--)
	{
		pgBackup   *to_backup = (pgBackup *) parray_get(backups, i);
		pgBackup   *from_backup = (pgBackup *) parray_get(backups, i - 1);

		merge_backups(to_backup, from_backup);
	}

	/* cleanup */
	parray_walk(backups, pgBackupFree);
	parray_free(backups);

	elog(LOG, "Merge completed");
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
				from_backup_path[MAXPGPATH],
				from_database_path[MAXPGPATH],
				control_file[MAXPGPATH];
	parray	   *files,
			   *to_files;
	pthread_t  *threads;
	merge_files_arg *threads_args;
	int			i;
	bool		merge_isok = true;

	elog(LOG, "Merging backup %s with backup %s", from_backup_id, to_backup_id);

	to_backup->status = BACKUP_STATUS_MERGING;
	pgBackupWriteBackupControlFile(to_backup);

	from_backup->status = BACKUP_STATUS_MERGING;
	pgBackupWriteBackupControlFile(from_backup);

	/*
	 * Make backup paths.
	 */
	pgBackupGetPath(to_backup, to_backup_path, lengthof(to_backup_path), NULL);
	pgBackupGetPath(to_backup, to_database_path, lengthof(to_database_path),
					DATABASE_DIR);
	pgBackupGetPath(from_backup, from_backup_path, lengthof(from_backup_path), NULL);
	pgBackupGetPath(from_backup, from_database_path, lengthof(from_database_path),
					DATABASE_DIR);

	create_data_directories(to_database_path, from_backup_path, false);

	/*
	 * Get list of files which will be modified or removed.
	 */
	pgBackupGetPath(to_backup, control_file, lengthof(control_file),
					DATABASE_FILE_LIST);
	to_files = dir_read_file_list(from_database_path, /* Use from_database_path
													   * so root path will be
													   * equal with 'files' */
								  control_file);
	/* To delete from leaf, sort in reversed order */
	parray_qsort(to_files, pgFileComparePathDesc);
	/*
	 * Get list of files which need to be moved.
	 */
	pgBackupGetPath(from_backup, control_file, lengthof(control_file),
					DATABASE_FILE_LIST);
	files = dir_read_file_list(from_database_path, control_file);
	/* sort by size for load balancing */
	parray_qsort(files, pgFileCompareSize);

	threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
	threads_args = (merge_files_arg *) palloc(sizeof(merge_files_arg) * num_threads);

	/* Setup threads */
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);

		pg_atomic_init_flag(&file->lock);
	}

	for (i = 0; i < num_threads; i++)
	{
		merge_files_arg *arg = &(threads_args[i]);

		arg->to_files = to_files;
		arg->files = files;
		arg->to_backup = to_backup;
		arg->from_backup = from_backup;
		arg->to_root = to_database_path;
		arg->from_root = from_database_path;
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
	 * Files were copied into to_backup and deleted from from_backup. Remove
	 * remaining directories from from_backup.
	 */
	parray_qsort(files, pgFileComparePathDesc);
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);

		if (!S_ISDIR(file->mode))
			continue;

		if (rmdir(file->path))
			elog(ERROR, "Could not remove directory \"%s\": %s",
				 file->path, strerror(errno));
	}
	if (rmdir(from_database_path))
		elog(ERROR, "Could not remove directory \"%s\": %s",
			 from_database_path, strerror(errno));
	if (unlink(control_file))
		elog(ERROR, "Could not remove file \"%s\": %s",
			 control_file, strerror(errno));

	pgBackupGetPath(from_backup, control_file, lengthof(control_file),
					BACKUP_CONTROL_FILE);
	if (unlink(control_file))
		elog(ERROR, "Could not remove file \"%s\": %s",
			 control_file, strerror(errno));

	if (rmdir(from_backup_path))
		elog(ERROR, "Could not remove directory \"%s\": %s",
			 from_backup_path, strerror(errno));

	/*
	 * Delete files which are not in from_backup file list.
	 */
	for (i = 0; i < parray_num(to_files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(to_files, i);

		if (parray_bsearch(files, file, pgFileComparePathDesc) == NULL)
		{
			pgFileDelete(file);
			elog(LOG, "Deleted \"%s\"", file->path);
		}
	}

	/*
	 * Rename FULL backup directory.
	 */
	if (rename(to_backup_path, from_backup_path) == -1)
		elog(ERROR, "Could not rename directory \"%s\" to \"%s\": %s",
			 to_backup_path, from_backup_path, strerror(errno));

	/*
	 * Update to_backup metadata.
	 */
	pgBackupCopy(to_backup, from_backup);
	/* Correct metadata */
	to_backup->backup_mode = BACKUP_MODE_FULL;
	to_backup->status = BACKUP_STATUS_OK;
	to_backup->parent_backup = INVALID_BACKUP_ID;
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
		to_backup->wal_bytes = xlog_seg_size *
			(to_backup->stop_lsn / xlog_seg_size -
			 to_backup->start_lsn / xlog_seg_size + 1);
	else
		to_backup->wal_bytes = BYTES_INVALID;

	pgBackupWriteFileList(to_backup, files, from_database_path);
	pgBackupWriteBackupControlFile(to_backup);

	/* Cleanup */
	pfree(threads_args);
	pfree(threads);

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
	int			to_root_len = strlen(argument->to_root);

	for (i = 0; i < num_files; i++)
	{
		pgFile	   *file = (pgFile *) parray_get(argument->files, i);

		if (!pg_atomic_test_set_flag(&file->lock))
			continue;

		/* check for interrupt */
		if (interrupted)
			elog(ERROR, "Interrupted during merging backups");

		if (progress)
			elog(LOG, "Progress: (%d/%d). Process file \"%s\"",
				 i + 1, num_files, file->path);

		/*
		 * Skip files which haven't changed since previous backup. But in case
		 * of DELTA backup we should consider n_blocks to truncate the target
		 * backup.
		 */
		if (file->write_size == BYTES_INVALID &&
			file->n_blocks == -1)
		{
			elog(VERBOSE, "Skip merging file \"%s\", the file didn't change",
				 file->path);

			/*
			 * If the file wasn't changed in PAGE backup, retreive its
			 * write_size from previous FULL backup.
			 */
			if (S_ISREG(file->mode))
			{
				pgFile	  **res_file;

				res_file = parray_bsearch(argument->to_files, file,
										  pgFileComparePathDesc);
				if (res_file && *res_file)
				{
					file->compress_alg = (*res_file)->compress_alg;
					file->write_size = (*res_file)->write_size;
					file->crc = (*res_file)->crc;
				}
			}

			continue;
		}

		/* Directories were created before */
		if (S_ISDIR(file->mode))
			continue;

		/*
		 * Move the file. We need to decompress it and compress again if
		 * necessary.
		 */
		elog(VERBOSE, "Moving file \"%s\", is_datafile %d, is_cfs %d",
			 file->path, file->is_database, file->is_cfs);

		if (file->is_datafile && !file->is_cfs)
		{
			char		to_path_tmp[MAXPGPATH];	/* Path of target file */

			join_path_components(to_path_tmp, argument->to_root,
								 file->path + to_root_len + 1);

			/*
			 * We need more complicate algorithm if target file exists and it is
			 * compressed.
			 */
			if (to_backup->compress_alg == PGLZ_COMPRESS ||
				to_backup->compress_alg == ZLIB_COMPRESS)
			{
				char		tmp_file_path[MAXPGPATH];
				char	   *prev_path;

				snprintf(tmp_file_path, MAXPGPATH, "%s_tmp", to_path_tmp);

				/* Start the magic */

				/*
				 * Merge files:
				 * - decompress first file
				 * - decompress second file and merge with first decompressed file
				 * - compress result file
				 */

				elog(VERBOSE, "File is compressed, decompress to the temporary file \"%s\"",
					 tmp_file_path);

				prev_path = file->path;
				/*
				 * We need to decompress target file only if it exists.
				 */
				if (fileExists(to_path_tmp))
				{
					/*
					 * file->path points to the file in from_root directory. But we
					 * need the file in directory to_root.
					 */
					file->path = to_path_tmp;

					/* Decompress first/target file */
					restore_data_file(tmp_file_path, file, false, false);

					file->path = prev_path;
				}
				/* Merge second/source file with first/target file */
				restore_data_file(tmp_file_path, file,
								  from_backup->backup_mode == BACKUP_MODE_DIFF_DELTA,
								  false);

				elog(VERBOSE, "Compress file and save it to the directory \"%s\"",
					 argument->to_root);

				/* Again we need change path */
				file->path = tmp_file_path;
				/* backup_data_file() requires file size to calculate nblocks */
				file->size = pgFileSize(file->path);
				/* Now we can compress the file */
				backup_data_file(NULL, /* We shouldn't need 'arguments' here */
								 to_path_tmp, file,
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
				restore_data_file(to_path_tmp, file,
								  from_backup->backup_mode == BACKUP_MODE_DIFF_DELTA,
								  true);

				/*
				 * We need to calculate write_size, restore_data_file() doesn't
				 * do that.
				 */
				file->write_size = pgFileSize(to_path_tmp);
				file->crc = pgFileGetCRC(to_path_tmp);
			}
			pgFileDelete(file);
		}
		else
			move_file(argument->from_root, argument->to_root, file);

		if (file->write_size != BYTES_INVALID)
			elog(LOG, "Moved file \"%s\": " INT64_FORMAT " bytes",
				 file->path, file->write_size);
	}

	/* Data files merging is successful */
	argument->ret = 0;

	return NULL;
}
