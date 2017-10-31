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
#include <pthread.h>
#include <dirent.h>

static void pgBackupValidateFiles(void *arg);
static void do_validate_instance(void);

static bool corrupted_backup_found = false;

typedef struct
{
	parray *files;
	bool corrupted;
} validate_files_args;

/*
 * Validate backup files.
 */
void
pgBackupValidate(pgBackup *backup)
{
	char	   *backup_id_string;
	char		base_path[MAXPGPATH];
	char		path[MAXPGPATH];
	parray	   *files;
	bool		corrupted = false;
	pthread_t	validate_threads[num_threads];
	validate_files_args *validate_threads_args[num_threads];
	int			i;

	/* We need free() this later */
	backup_id_string = base36enc(backup->start_time);

	if (backup->status != BACKUP_STATUS_OK &&
		backup->status != BACKUP_STATUS_DONE)
	{
		elog(INFO, "Backup %s has status %s. Skip validation.",
					backup_id_string, status2str(backup->status));
		return;
	}

	elog(LOG, "Validate backup %s", backup_id_string);

	if (backup->backup_mode != BACKUP_MODE_FULL &&
		backup->backup_mode != BACKUP_MODE_DIFF_PAGE &&
		backup->backup_mode != BACKUP_MODE_DIFF_PTRACK)
		elog(LOG, "Invalid backup_mode of backup %s", backup_id_string);

	pgBackupGetPath(backup, base_path, lengthof(base_path), DATABASE_DIR);
	pgBackupGetPath(backup, path, lengthof(path), DATABASE_FILE_LIST);
	files = dir_read_file_list(base_path, path);

	/* setup threads */
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);
		__sync_lock_release(&file->lock);
	}

	/* Validate files */
	for (i = 0; i < num_threads; i++)
	{
		validate_files_args *arg = pg_malloc(sizeof(validate_files_args));
		arg->files = files;
		arg->corrupted = false;
		validate_threads_args[i] = arg;
		pthread_create(&validate_threads[i], NULL, (void *(*)(void *)) pgBackupValidateFiles, arg);
	}

	/* Wait theads */
	for (i = 0; i < num_threads; i++)
	{
		pthread_join(validate_threads[i], NULL);
		if (validate_threads_args[i]->corrupted)
			corrupted = true;
		pg_free(validate_threads_args[i]);
	}

	/* cleanup */
	parray_walk(files, pgFileFree);
	parray_free(files);

	/* Update backup status */
	backup->status = corrupted ? BACKUP_STATUS_CORRUPT : BACKUP_STATUS_OK;
	pgBackupWriteBackupControlFile(backup);

	if (corrupted)
		elog(WARNING, "Backup %s is corrupted", backup_id_string);
	else
		elog(LOG, "Backup %s is valid", backup_id_string);
	free(backup_id_string);
}

/*
 * Validate files in the backup.
 * NOTE: If file is not valid, do not use ERROR log message,
 * rather throw a WARNING and set arguments->corrupted = true.
 * This is necessary to update backup status.
 */
static void
pgBackupValidateFiles(void *arg)
{
	int		i;
	validate_files_args *arguments = (validate_files_args *)arg;
	pg_crc32	crc;

	for (i = 0; i < parray_num(arguments->files); i++)
	{
		struct stat st;

		pgFile *file = (pgFile *) parray_get(arguments->files, i);
		if (__sync_lock_test_and_set(&file->lock, 1) != 0)
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
		elog(LOG, "Validate files: (%d/%lu) %s",
			 i + 1, (unsigned long) parray_num(arguments->files), file->path);

		if (stat(file->path, &st) == -1)
		{
			if (errno == ENOENT)
				elog(WARNING, "Backup file \"%s\" is not found", file->path);
			else
				elog(WARNING, "Cannot stat backup file \"%s\": %s",
					file->path, strerror(errno));
			arguments->corrupted = true;
			return;
		}

		if (file->write_size != st.st_size)
		{
			elog(WARNING, "Invalid size of backup file \"%s\" : %lu. Expected %lu",
				 file->path, (unsigned long) file->write_size,
				 (unsigned long) st.st_size);
			arguments->corrupted = true;
			return;
		}

		crc = pgFileGetCRC(file);
		if (crc != file->crc)
		{
			elog(WARNING, "Invalid CRC of backup file \"%s\" : %X. Expected %X",
					file->path, file->crc, crc);
			arguments->corrupted = true;
			return;
		}
	}
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
			do_validate_instance();
		}
	}
	else
	{
		do_validate_instance();
	}

	if (corrupted_backup_found)
	{
		elog(INFO, "Some backups are not valid");
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
	int			i;
	parray	   *backups;
	pgBackup   *current_backup = NULL;

	elog(INFO, "Validate backups of the instance '%s'", instance_name);

	/* Get exclusive lock of backup catalog */
	catalog_lock();

	/* Get list of all backups sorted in order of descending start time */
	backups = catalog_get_backup_list(INVALID_BACKUP_ID);
	if (backups == NULL)
		elog(ERROR, "Failed to get backup list.");

	/* Valiate each backup along with its xlog files. */
	for (i = 0; i < parray_num(backups); i++)
	{
		char	   *backup_id;
		pgBackup   *base_full_backup = NULL;

		current_backup = (pgBackup *) parray_get(backups, i);
		backup_id = base36enc(current_backup->start_time);

		elog(INFO, "Validate backup %s", backup_id);

		if (current_backup->backup_mode != BACKUP_MODE_FULL)
		{
			for (int j = i + 1; j < parray_num(backups); j++)
			{
				pgBackup	   *backup = (pgBackup *) parray_get(backups, j);

				if (backup->backup_mode == BACKUP_MODE_FULL)
				{
					base_full_backup = backup;
					break;
				}
			}
		}
		else
			base_full_backup = current_backup;

		pgBackupValidate(current_backup);

		/* There is no point in wal validation for corrupted backup */
		if (current_backup->status == BACKUP_STATUS_OK)
		{
			if (base_full_backup == NULL)
				elog(ERROR, "Valid full backup for backup %s is not found.",
					 backup_id);
			/* Validate corresponding WAL files */
			validate_wal(current_backup, arclog_path, 0,
						0, base_full_backup->tli);
		}

		if (current_backup->status != BACKUP_STATUS_OK)
			corrupted_backup_found = true;

		free(backup_id);
	}

	/* cleanup */
	parray_walk(backups, pgBackupFree);
	parray_free(backups);
}
