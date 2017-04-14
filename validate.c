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

static void pgBackupValidateFiles(void *arg);

typedef struct
{
	parray *files;
	bool validate_crc;
	bool corrupted;
} validate_files_args;

/*
 * Validate backup files.
 */
void
pgBackupValidate(pgBackup *backup)
{
	char	*backup_id_string;
	char	base_path[MAXPGPATH];
	char	path[MAXPGPATH];
	parray *files;
	bool	corrupted = false;
	pthread_t	validate_threads[num_threads];
	validate_files_args *validate_threads_args[num_threads];
	int i;

	backup_id_string = base36enc(backup->start_time);

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
		pgFile *file = (pgFile *) parray_get(files, i);
		__sync_lock_release(&file->lock);
	}

	/* Validate files */
	for (i = 0; i < num_threads; i++)
	{
		validate_files_args *arg = pg_malloc(sizeof(validate_files_args));
		arg->files = files;
		arg->validate_crc = true;

		/* TODO Why didn't we validate checksums on restore before? */
// 		if (backup_subcmd == RESTORE)
// 			arg->validate_crc = false;

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
	pgBackupWriteConf(backup);

	if (corrupted)
		elog(WARNING, "Backup %s is corrupted", backup_id_string);
	else
		elog(LOG, "Backup %s is valid", backup_id_string);
}

/*
 * Validate files in the backup with size or CRC.
 * NOTE: If file is not valid, do not use ERROR log message,
 * rather throw a WARNING and set arguments->corrupted = true.
 * This is necessary to update backup status.
 */
static void
pgBackupValidateFiles(void *arg)
{
	int		i;
	validate_files_args *arguments = (validate_files_args *)arg;

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
		/* We don't compute checksums for compressed data, so skip them
		 * TODO Add checksums to compressed files.
		 */
		if (file->generation != -1)
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

		if (arguments->validate_crc)
		{
			pg_crc32	crc;

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
}
