/*-------------------------------------------------------------------------
 *
 * validate.c: validate backup files.
 *
 * Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
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
	const char *root;
	bool size_only;
	bool corrupted;
} validate_files_args;

int
do_validate(time_t backup_id,
			const char *target_time,
			const char *target_xid,
			const char *target_inclusive,
			TimeLineID target_tli)
{
	int			i;
	int			base_index;				/* index of base (full) backup */
	int			last_restored_index;	/* index of last restored database backup */
	parray	   *timelines;
	parray	   *backups;
	pgRecoveryTarget *rt = NULL;
	pgBackup   *base_backup = NULL;
	bool		backup_id_found = false,
				success_validate;

	catalog_lock(false);

	rt = checkIfCreateRecoveryConf(target_time, target_xid, target_inclusive);
	if (rt == NULL)
		elog(ERROR, "cannot create recovery.conf. specified args are invalid.");

	/* get list of backups. (index == 0) is the last backup */
	backups = catalog_get_backup_list(0);
	if (!backups)
		elog(ERROR, "cannot process any more.");

	/* Read timeline history files from archives */
	if (target_tli)
		timelines = readTimeLineHistory(target_tli);

	/* find last full backup which can be used as base backup. */
	elog(LOG, "searching recent full backup");
	for (i = 0; i < parray_num(backups); i++)
	{
		base_backup = (pgBackup *) parray_get(backups, i);

		if (backup_id && base_backup->start_time > backup_id)
			continue;

		if (backup_id == base_backup->start_time &&
			(base_backup->status == BACKUP_STATUS_OK ||
			 base_backup->status == BACKUP_STATUS_CORRUPT))
			backup_id_found = true;

		if (backup_id == base_backup->start_time &&
			(base_backup->status != BACKUP_STATUS_OK &&
			 base_backup->status != BACKUP_STATUS_CORRUPT))
			elog(ERROR, "given backup %s is %s", base36enc(backup_id), status2str(base_backup->status));

		if (base_backup->backup_mode < BACKUP_MODE_FULL ||
			(base_backup->status != BACKUP_STATUS_OK &&
			 base_backup->status != BACKUP_STATUS_CORRUPT))
			continue;

		if (target_tli)
		{
			if (satisfy_timeline(timelines, base_backup) &&
				satisfy_recovery_target(base_backup, rt) &&
				(backup_id_found || backup_id == 0))
				goto base_backup_found;
		}
		else
			if (satisfy_recovery_target(base_backup, rt) &&
				(backup_id_found || backup_id == 0))
				goto base_backup_found;

		backup_id_found = false;
	}
	/* no full backup found, cannot restore */
	elog(ERROR, "no full backup found, cannot validate.");

base_backup_found:
	base_index = i;

	if (backup_id != 0)
		stream_wal = base_backup->stream;

	/* validate base backup */
	success_validate = pgBackupValidate(base_backup, false, false);

	last_restored_index = base_index;

	/* restore following differential backup */
	elog(LOG, "searching differential backup...");

	for (i = base_index - 1; i >= 0; i--)
	{
		pgBackup *backup = (pgBackup *) parray_get(backups, i);

		/* don't use incomplete nor different timeline backup */
		if ((backup->status != BACKUP_STATUS_OK &&
			 backup->status != BACKUP_STATUS_CORRUPT) ||
			backup->tli != base_backup->tli)
			continue;

		if (backup->backup_mode == BACKUP_MODE_FULL)
			break;

		if (backup_id && backup->start_time > backup_id)
			break;

		/* use database backup only */
		if (backup->backup_mode != BACKUP_MODE_DIFF_PAGE &&
			backup->backup_mode != BACKUP_MODE_DIFF_PTRACK)
			continue;

		/* is the backup is necessary for restore to target timeline ? */
		if (target_tli)
		{
			if (!satisfy_timeline(timelines, backup) ||
				!satisfy_recovery_target(backup, rt))
				continue;
		}
		else
			if (!satisfy_recovery_target(backup, rt))
				continue;

		if (backup_id != 0)
			stream_wal = backup->stream;

		success_validate = success_validate &&
			pgBackupValidate(backup, false, false);
		last_restored_index = i;
	}

	/* and now we must check WALs */
	if (!stream_wal || rt->time_specified || rt->xid_specified)
		validate_wal((pgBackup *) parray_get(backups, last_restored_index),
					 arclog_path,
					 rt->recovery_target_time,
					 rt->recovery_target_xid,
					 base_backup->tli);
	else if (success_validate)
		elog(INFO, "backup validation stopped successfully");

	/* release catalog lock */
	catalog_unlock();

	/* cleanup */
	parray_walk(backups, pgBackupFree);
	parray_free(backups);

	return 0;
}

/*
 * Validate each files in the backup with its size.
 */
bool
pgBackupValidate(pgBackup *backup,
				 bool size_only,
				 bool for_get_timeline)
{
	char	*backup_id_string;
	char	base_path[MAXPGPATH];
	char	path[MAXPGPATH];
	parray *files;
	bool	corrupted = false;
	pthread_t	validate_threads[num_threads];
	validate_files_args *validate_threads_args[num_threads];

	backup_id_string = base36enc(backup->start_time);
	if (!for_get_timeline)
	{
		if (backup->backup_mode == BACKUP_MODE_FULL ||
			backup->backup_mode == BACKUP_MODE_DIFF_PAGE ||
			backup->backup_mode == BACKUP_MODE_DIFF_PTRACK)
			elog(INFO, "validate: %s backup and archive log files by %s",
				 backup_id_string, (size_only ? "SIZE" : "CRC"));
	}

	if (!check)
	{
		if (backup->backup_mode == BACKUP_MODE_FULL ||
			backup->backup_mode == BACKUP_MODE_DIFF_PAGE ||
			backup->backup_mode == BACKUP_MODE_DIFF_PTRACK)
		{
			int i;
			elog(LOG, "database files...");
			pgBackupGetPath(backup, base_path, lengthof(base_path), DATABASE_DIR);
			pgBackupGetPath(backup, path, lengthof(path),
				DATABASE_FILE_LIST);
			files = dir_read_file_list(base_path, path);

			/* setup threads */
			for (i = 0; i < parray_num(files); i++)
			{
				pgFile *file = (pgFile *) parray_get(files, i);
				__sync_lock_release(&file->lock);
			}

			/* restore files into $PGDATA */
			for (i = 0; i < num_threads; i++)
			{
				validate_files_args *arg = pg_malloc(sizeof(validate_files_args));
				arg->files = files;
				arg->root = base_path;
				arg->size_only = size_only;
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
			parray_walk(files, pgFileFree);
			parray_free(files);
		}

		/* update status to OK */
		if (corrupted)
			backup->status = BACKUP_STATUS_CORRUPT;
		else
			backup->status = BACKUP_STATUS_OK;
		pgBackupWriteIni(backup);

		if (corrupted)
			elog(WARNING, "backup %s is corrupted", backup_id_string);
		else
			elog(LOG, "backup %s is valid", backup_id_string);
	}

	return !corrupted;
}

static const char *
get_relative_path(const char *path, const char *root)
{
	size_t	rootlen = strlen(root);
	if (strncmp(path, root, rootlen) == 0 && path[rootlen] == '/')
		return path + rootlen + 1;
	else
		return path;
}

/*
 * Validate files in the backup with size or CRC.
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
			elog(ERROR, "interrupted during validate");

		/* skipped backup while differential backup */
		/* NOTE We don't compute checksums for compressed data,
		 * so skip it too */
		if (file->write_size == BYTES_INVALID
			|| !S_ISREG(file->mode)
			|| file->generation != -1)
			continue;

		/* print progress */
		elog(LOG, "(%d/%lu) %s", i + 1, (unsigned long) parray_num(arguments->files),
			get_relative_path(file->path, arguments->root));

		/* always validate file size */
		if (stat(file->path, &st) == -1)
		{
			if (errno == ENOENT)
				elog(WARNING, "backup file \"%s\" vanished", file->path);
			else
				elog(ERROR, "cannot stat backup file \"%s\": %s",
					get_relative_path(file->path, arguments->root), strerror(errno));
			arguments->corrupted = true;
			return;
		}
		if (file->write_size != st.st_size)
		{
			elog(WARNING, "size of backup file \"%s\" must be %lu but %lu",
				get_relative_path(file->path, arguments->root),
				(unsigned long) file->write_size,
				(unsigned long) st.st_size);
			arguments->corrupted = true;
			return;
		}

		/* validate CRC too */
		if (!arguments->size_only)
		{
			pg_crc32	crc;

			crc = pgFileGetCRC(file);
			if (crc != file->crc)
			{
				elog(WARNING, "CRC of backup file \"%s\" must be %X but %X",
					get_relative_path(file->path, arguments->root), file->crc, crc);
				arguments->corrupted = true;
				return;
			}
		}
	}
}
