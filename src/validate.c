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
	pgBackupWriteBackupControlFile(backup);

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
			break;
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
		current_backup = (pgBackup *) parray_get(backups, i);

		/* Valiate each backup along with its xlog files. */
		pgBackupValidate(current_backup);

		/* Ensure that the backup has valid list of parent backups */
		if (current_backup->status == BACKUP_STATUS_OK)
		{
			pgBackup   *base_full_backup = current_backup;

			if (current_backup->backup_mode != BACKUP_MODE_FULL)
			{
				base_full_backup = find_parent_backup(current_backup);

				if (base_full_backup == NULL)
					elog(ERROR, "Valid full backup for backup %s is not found.",
						 base36enc(current_backup->start_time));
			}

			/* Validate corresponding WAL files */
			validate_wal(current_backup, arclog_path, 0,
						 0, 0, base_full_backup->tli, xlog_seg_size);
		}

		/* Mark every incremental backup between corrupted backup and nearest FULL backup as orphans */
		if (current_backup->status == BACKUP_STATUS_CORRUPT)
		{
			int			j;

			corrupted_backup_found = true;
			current_backup_id = base36enc_dup(current_backup->start_time);
			for (j = i - 1; j >= 0; j--)
			{
				pgBackup   *backup = (pgBackup *) parray_get(backups, j);

				if (backup->backup_mode == BACKUP_MODE_FULL)
					break;
				if (backup->status != BACKUP_STATUS_OK)
					continue;
				else
				{
					backup->status = BACKUP_STATUS_ORPHAN;
					pgBackupWriteBackupControlFile(backup);

					elog(WARNING, "Backup %s is orphaned because his parent %s is corrupted",
						 base36enc(backup->start_time), current_backup_id);
				}
			}
			free(current_backup_id);
		}
	}

	/* cleanup */
	parray_walk(backups, pgBackupFree);
	parray_free(backups);
}
