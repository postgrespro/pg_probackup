/*-------------------------------------------------------------------------
 *
 * validate.c: validate backup files.
 *
 * Portions Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <sys/stat.h>
#include <dirent.h>

#include "utils/thread.h"

static void *pgBackupValidateFiles(void *arg);
static void do_validate_instance(void);

static bool corrupted_backup_found = false;

typedef struct
{
	parray	   *files;
	bool		corrupted;
	XLogRecPtr	stop_lsn;
	uint32		checksum_version;

	/*
	 * Return value from the thread.
	 * 0 means there is no error, 1 - there is an error.
	 */
	int			ret;
} validate_files_arg;

/*
 * Validate backup files.
 */
void
pgBackupValidate(pgBackup *backup)
{
	char		base_path[MAXPGPATH];
	char		path[MAXPGPATH];
	parray	   *files;
	bool		corrupted = false;
	bool		validation_isok = true;
	/* arrays with meta info for multi threaded validate */
	pthread_t  *threads;
	validate_files_arg *threads_args;
	int			i;

	/* Revalidation is attempted for DONE, ORPHAN and CORRUPT backups */
	if (backup->status != BACKUP_STATUS_OK &&
		backup->status != BACKUP_STATUS_DONE &&
		backup->status != BACKUP_STATUS_ORPHAN &&
		backup->status != BACKUP_STATUS_CORRUPT)
	{
		elog(WARNING, "Backup %s has status %s. Skip validation.",
					base36enc(backup->start_time), status2str(backup->status));
		corrupted_backup_found = true;
		return;
	}

	if (backup->status == BACKUP_STATUS_OK || backup->status == BACKUP_STATUS_DONE)
		elog(INFO, "Validating backup %s", base36enc(backup->start_time));
	/* backups in MERGING status must have an option of revalidation without losing MERGING status
	else if (backup->status == BACKUP_STATUS_MERGING)
	{
		some message here
	}
	*/
	else
		elog(INFO, "Revalidating backup %s", base36enc(backup->start_time));

	if (backup->backup_mode != BACKUP_MODE_FULL &&
		backup->backup_mode != BACKUP_MODE_DIFF_PAGE &&
		backup->backup_mode != BACKUP_MODE_DIFF_PTRACK &&
		backup->backup_mode != BACKUP_MODE_DIFF_DELTA)
		elog(WARNING, "Invalid backup_mode of backup %s", base36enc(backup->start_time));

	pgBackupGetPath(backup, base_path, lengthof(base_path), DATABASE_DIR);
	pgBackupGetPath(backup, path, lengthof(path), DATABASE_FILE_LIST);
	files = dir_read_file_list(base_path, path);

	/* setup threads */
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);
		pg_atomic_clear_flag(&file->lock);
	}

	/* init thread args with own file lists */
	threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
	threads_args = (validate_files_arg *)
		palloc(sizeof(validate_files_arg) * num_threads);

	/* Validate files */
	for (i = 0; i < num_threads; i++)
	{
		validate_files_arg *arg = &(threads_args[i]);

		arg->files = files;
		arg->corrupted = false;
		arg->stop_lsn = backup->stop_lsn;
		arg->checksum_version = backup->checksum_version;
		/* By default there are some error */
		threads_args[i].ret = 1;

		pthread_create(&threads[i], NULL, pgBackupValidateFiles, arg);
	}

	/* Wait theads */
	for (i = 0; i < num_threads; i++)
	{
		validate_files_arg *arg = &(threads_args[i]);

		pthread_join(threads[i], NULL);
		if (arg->corrupted)
			corrupted = true;
		if (arg->ret == 1)
			validation_isok = false;
	}
	if (!validation_isok)
		elog(ERROR, "Data files validation failed");

	pfree(threads);
	pfree(threads_args);

	/* cleanup */
	parray_walk(files, pgFileFree);
	parray_free(files);

	/* Update backup status */
	backup->status = corrupted ? BACKUP_STATUS_CORRUPT : BACKUP_STATUS_OK;
	write_backup_status(backup);

	if (corrupted)
		elog(WARNING, "Backup %s data files are corrupted", base36enc(backup->start_time));
	else
		elog(INFO, "Backup %s data files are valid", base36enc(backup->start_time));
}

/*
 * Validate files in the backup.
 * NOTE: If file is not valid, do not use ERROR log message,
 * rather throw a WARNING and set arguments->corrupted = true.
 * This is necessary to update backup status.
 */
static void *
pgBackupValidateFiles(void *arg)
{
	int			i;
	validate_files_arg *arguments = (validate_files_arg *)arg;
	pg_crc32	crc;

	for (i = 0; i < parray_num(arguments->files); i++)
	{
		struct stat st;
		pgFile	   *file = (pgFile *) parray_get(arguments->files, i);

		if (!pg_atomic_test_set_flag(&file->lock))
			continue;

		if (interrupted)
			elog(ERROR, "Interrupted during validate");

		/* Validate only regular files */
		if (!S_ISREG(file->mode))
			continue;
		/*
		 * Skip files which has no data, because they
		 * haven't changed between backups.
		 */
		if (file->write_size == BYTES_INVALID)
			continue;

		/*
		 * Currently we don't compute checksums for
		 * cfs_compressed data files, so skip them.
		 */
		if (file->is_cfs)
			continue;

		/* print progress */
		elog(VERBOSE, "Validate files: (%d/%lu) %s",
			 i + 1, (unsigned long) parray_num(arguments->files), file->path);

		if (stat(file->path, &st) == -1)
		{
			if (errno == ENOENT)
				elog(WARNING, "Backup file \"%s\" is not found", file->path);
			else
				elog(WARNING, "Cannot stat backup file \"%s\": %s",
					file->path, strerror(errno));
			arguments->corrupted = true;
			break;
		}

		if (file->write_size != st.st_size)
		{
			elog(WARNING, "Invalid size of backup file \"%s\" : " INT64_FORMAT ". Expected %lu",
				 file->path, file->write_size, (unsigned long) st.st_size);
			arguments->corrupted = true;
			break;
		}

		crc = pgFileGetCRC(file->path);
		if (crc != file->crc)
		{
			elog(WARNING, "Invalid CRC of backup file \"%s\" : %X. Expected %X",
					file->path, file->crc, crc);
			arguments->corrupted = true;

			/* validate relation blocks */
			if (file->is_datafile)
			{
				if (!check_file_pages(file, arguments->stop_lsn, arguments->checksum_version))
					arguments->corrupted = true;
			}
		}
	}

	/* Data files validation is successful */
	arguments->ret = 0;

	return NULL;
}

/*
 * Validate all backups in the backup catalog.
 * If --instance option was provided, validate only backups of this instance.
 */
int
do_validate_all(void)
{
	if (instance_name == NULL)
	{
		/* Show list of instances */
		char		path[MAXPGPATH];
		DIR		   *dir;
		struct dirent *dent;

		/* open directory and list contents */
		join_path_components(path, backup_path, BACKUPS_DIR);
		dir = opendir(path);
		if (dir == NULL)
			elog(ERROR, "cannot open directory \"%s\": %s", path, strerror(errno));

		errno = 0;
		while ((dent = readdir(dir)))
		{
			char		child[MAXPGPATH];
			struct stat	st;

			/* skip entries point current dir or parent dir */
			if (strcmp(dent->d_name, ".") == 0 ||
				strcmp(dent->d_name, "..") == 0)
				continue;

			join_path_components(child, path, dent->d_name);

			if (lstat(child, &st) == -1)
				elog(ERROR, "cannot stat file \"%s\": %s", child, strerror(errno));

			if (!S_ISDIR(st.st_mode))
				continue;

			instance_name = dent->d_name;
			sprintf(backup_instance_path, "%s/%s/%s", backup_path, BACKUPS_DIR, instance_name);
			sprintf(arclog_path, "%s/%s/%s", backup_path, "wal", instance_name);
			xlog_seg_size = get_config_xlog_seg_size();

			do_validate_instance();
		}
	}
	else
	{
		do_validate_instance();
	}

	if (corrupted_backup_found)
	{
		elog(WARNING, "Some backups are not valid");
		return 1;
	}
	else
		elog(INFO, "All backups are valid");

	return 0;
}

/*
 * Validate all backups in the given instance of the backup catalog.
 */
static void
do_validate_instance(void)
{
	char	   *current_backup_id;
	int			i;
	int			j;
	parray	   *backups;
	pgBackup   *current_backup = NULL;

	elog(INFO, "Validate backups of the instance '%s'", instance_name);

	/* Get exclusive lock of backup catalog */
	catalog_lock();

	/* Get list of all backups sorted in order of descending start time */
	backups = catalog_get_backup_list(INVALID_BACKUP_ID);

	/* Examine backups one by one and validate them */
	for (i = 0; i < parray_num(backups); i++)
	{
		pgBackup   *base_full_backup;
		char	   *parent_backup_id;

		current_backup = (pgBackup *) parray_get(backups, i);

		/* Find ancestor for incremental backup */
		if (current_backup->backup_mode != BACKUP_MODE_FULL)
		{
			pgBackup   *tmp_backup = NULL;
			int result;

			result = scan_parent_chain(current_backup, &tmp_backup);

			/* chain is broken */
			if (result == 0)
			{
				/* determine missing backup ID */

				parent_backup_id = base36enc_dup(tmp_backup->parent_backup);
				corrupted_backup_found = true;

				/* orphanize current_backup */
				if (current_backup->status == BACKUP_STATUS_OK)
				{
					current_backup->status = BACKUP_STATUS_ORPHAN;
					write_backup_status(current_backup);
					elog(WARNING, "Backup %s is orphaned because his parent %s is missing",
							base36enc(current_backup->start_time),
							parent_backup_id);
				}
				else
				{
					elog(WARNING, "Backup %s has missing parent %s",
						base36enc(current_backup->start_time), parent_backup_id);
				}
				continue;
			}
			/* chain is whole, but at least one parent is invalid */
			else if (result == 1)
			{
				/* determine corrupt backup ID */
				parent_backup_id = base36enc_dup(tmp_backup->start_time);

				/* Oldest corrupt backup has a chance for revalidation */
				if (current_backup->start_time != tmp_backup->start_time)
				{
					/* orphanize current_backup */
					if (current_backup->status == BACKUP_STATUS_OK)
					{
						current_backup->status = BACKUP_STATUS_ORPHAN;
						write_backup_status(current_backup);
						elog(WARNING, "Backup %s is orphaned because his parent %s has status: %s",
								base36enc(current_backup->start_time), parent_backup_id,
								status2str(tmp_backup->status));
					}
					else
					{
						elog(WARNING, "Backup %s has parent %s with status: %s",
								base36enc(current_backup->start_time),parent_backup_id,
								status2str(tmp_backup->status));
					}
					continue;
				}
				base_full_backup = find_parent_full_backup(current_backup);
			}
			/* chain is whole, all parents are valid at first glance,
			 * current backup validation can proceed
			 */
			else
				base_full_backup = tmp_backup;
		}
		else
			base_full_backup = current_backup;

		/* Valiate backup files*/
		pgBackupValidate(current_backup);

		/* Validate corresponding WAL files */
		if (current_backup->status == BACKUP_STATUS_OK)
			validate_wal(current_backup, arclog_path, 0,
						 0, 0, base_full_backup->tli, xlog_seg_size);

		/*
		 * Mark every descendant of corrupted backup as orphan
		 */
		if (current_backup->status == BACKUP_STATUS_CORRUPT)
		{
			/* This is ridiculous but legal.
			 * PAGE1_2b <- OK
			 * PAGE1_2a <- OK
			 * PAGE1_1b <- ORPHAN
			 * PAGE1_1a <- CORRUPT
			 * FULL1    <- OK
			 */

			corrupted_backup_found = true;
			current_backup_id = base36enc_dup(current_backup->start_time);

			for (j = i - 1; j >= 0; j--)
			{
				pgBackup   *backup = (pgBackup *) parray_get(backups, j);

				if (is_parent(current_backup->start_time, backup, false))
				{
					if (backup->status == BACKUP_STATUS_OK)
					{
						backup->status = BACKUP_STATUS_ORPHAN;
						write_backup_status(backup);

						elog(WARNING, "Backup %s is orphaned because his parent %s has status: %s",
							 base36enc(backup->start_time),
							 current_backup_id,
							 status2str(current_backup->status));
					}
				}
			}
			free(current_backup_id);
		}

		/* For every OK backup we try to revalidate all his ORPHAN descendants. */
		if (current_backup->status == BACKUP_STATUS_OK)
		{
			/* revalidate all ORPHAN descendats
			 * be very careful not to miss a missing backup
			 * for every backup we must check that he is descendant of current_backup
			 */
			for (j = i - 1; j >= 0; j--)
			{
				pgBackup   *backup = (pgBackup *) parray_get(backups, j);
				pgBackup   *tmp_backup = NULL;
				int result;

				//PAGE3b ORPHAN
				//PAGE2b ORPHAN          -----
				//PAGE6a ORPHAN 			 |
				//PAGE5a CORRUPT 			 |
				//PAGE4a missing			 |
				//PAGE3a missing			 |
				//PAGE2a ORPHAN 			 |
				//PAGE1a OK <- we are here <-|
				//FULL OK

				if (is_parent(current_backup->start_time, backup, false))
				{
					/* Revalidation make sense only if parent chain is whole.
					 * is_parent() do not guarantee that.
					 */
					result = scan_parent_chain(backup, &tmp_backup);

					if (result == 1)
					{
						/* revalidation make sense only if oldest invalid backup is current_backup
						 */

						if (tmp_backup->start_time != backup->start_time)
							continue;

						if (backup->status == BACKUP_STATUS_ORPHAN)
						{
							/* Revaliate backup files*/
							pgBackupValidate(backup);

							if (backup->status == BACKUP_STATUS_OK)
							{
								//tmp_backup = find_parent_full_backup(dest_backup);
								/* Revalidation successful, validate corresponding WAL files */
								validate_wal(backup, arclog_path, 0,
											 0, 0, current_backup->tli,
											 xlog_seg_size);
							}
						}

						if (backup->status != BACKUP_STATUS_OK)
						{
							corrupted_backup_found = true;
							continue;
						}
					}
				}
			}
		}
	}

	/* cleanup */
	parray_walk(backups, pgBackupFree);
	parray_free(backups);
}
