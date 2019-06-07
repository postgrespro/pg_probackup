/*-------------------------------------------------------------------------
 *
 * restore.c: restore DB cluster and archived WAL.
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2019, Postgres Professional
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
	parray	   *external_dirs;
	char	   *external_prefix;
	parray	   *dest_external_dirs;
	parray	   *dest_files;
	parray	   *dbOid_exclude_list;

	/*
	 * Return value from the thread.
	 * 0 means there is no error, 1 - there is an error.
	 */
	int			ret;
} restore_files_arg;

static void restore_backup(pgBackup *backup, parray *dest_external_dirs,
						   parray *dest_files, parray *dbOid_exclude_list);
static void create_recovery_conf(time_t backup_id,
								 pgRecoveryTarget *rt,
								 pgBackup *backup);
static parray *read_timeline_history(TimeLineID targetTLI);
static void *restore_files(void *arg);

static parray *get_dbOid_exclude_list(pgBackup *backup, parray *datname_list,
									  bool partial_restore_type);

static int pgCompareOid(const void *f1, const void *f2);

/*
 * Entry point of pg_probackup RESTORE and VALIDATE subcommands.
 */
int
do_restore_or_validate(time_t target_backup_id, pgRecoveryTarget *rt,
					   bool is_restore, parray *datname_list, bool partial_restore_type)
{
	int			i = 0;
	int			j = 0;
	parray	   *backups;
	pgBackup   *tmp_backup = NULL;
	pgBackup   *current_backup = NULL;
	pgBackup   *dest_backup = NULL;
	pgBackup   *base_full_backup = NULL;
	pgBackup   *corrupted_backup = NULL;
	char	   *action = is_restore ? "Restore":"Validate";
	parray	   *parent_chain = NULL;
	parray	   *dbOid_exclude_list = NULL;

	if (is_restore)
	{
		if (instance_config.pgdata == NULL)
			elog(ERROR,
				"required parameter not specified: PGDATA (-D, --pgdata)");
		/* Check if restore destination empty */
		if (!dir_is_empty(instance_config.pgdata, FIO_DB_HOST))
			elog(ERROR, "restore destination is not empty: \"%s\"",
				 instance_config.pgdata);
	}

	if (instance_name == NULL)
		elog(ERROR, "required parameter not specified: --instance");

	elog(LOG, "%s begin.", action);

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
			(current_backup->status != BACKUP_STATUS_OK &&
			 current_backup->status != BACKUP_STATUS_DONE))
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
			 * but in case of CORRUPT or ORPHAN revalidation is possible
			 * unless --no-validate is used,
			 * in other cases throw an error.
			 */
			 // 1. validate
			 // 2. validate -i INVALID_ID <- allowed revalidate
			 // 3. restore -i INVALID_ID <- allowed revalidate and restore
			 // 4. restore <- impossible
			 // 5. restore --no-validate <- forbidden
			if (current_backup->status != BACKUP_STATUS_OK &&
				current_backup->status != BACKUP_STATUS_DONE)
			{
				if ((current_backup->status == BACKUP_STATUS_ORPHAN ||
					current_backup->status == BACKUP_STATUS_CORRUPT ||
					current_backup->status == BACKUP_STATUS_RUNNING)
					&& !rt->no_validate)
					elog(WARNING, "Backup %s has status: %s",
						 base36enc(current_backup->start_time), status2str(current_backup->status));
				else
					elog(ERROR, "Backup %s has status: %s",
						 base36enc(current_backup->start_time), status2str(current_backup->status));
			}

			if (rt->target_tli)
			{
				parray	   *timelines;

				elog(LOG, "target timeline ID = %u", rt->target_tli);
				/* Read timeline history files from archives */
				timelines = read_timeline_history(rt->target_tli);

				if (!satisfy_timeline(timelines, current_backup))
				{
					if (target_backup_id != INVALID_BACKUP_ID)
						elog(ERROR, "target backup %s does not satisfy target timeline",
							 base36enc(target_backup_id));
					else
						/* Try to find another backup that satisfies target timeline */
						continue;
				}

				parray_walk(timelines, pfree);
				parray_free(timelines);
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

			for (j = 0; j < parray_num(backups); j++)
			{
				pgBackup *backup = (pgBackup *) parray_get(backups, j);

				/* use parent backup start_time because he is missing
				 * and we must orphinize his descendants
				 */
				if (is_parent(missing_backup_start_time, backup, false))
				{
					if (backup->status == BACKUP_STATUS_OK ||
						backup->status == BACKUP_STATUS_DONE)
					{
						write_backup_status(backup, BACKUP_STATUS_ORPHAN);

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
			pg_free(missing_backup_id);
			/* No point in doing futher */
			elog(ERROR, "%s of backup %s failed.", action, base36enc(dest_backup->start_time));
		}
		else if (result == 1)
		{
			/* chain is intact, but at least one parent is invalid */
			char	   *parent_backup_id;

			/* parent_backup_id contain human-readable backup ID of oldest invalid backup */
			parent_backup_id = base36enc_dup(tmp_backup->start_time);

			for (j = 0; j < parray_num(backups); j++)
			{

				pgBackup *backup = (pgBackup *) parray_get(backups, j);

				if (is_parent(tmp_backup->start_time, backup, false))
				{
					if (backup->status == BACKUP_STATUS_OK ||
						backup->status == BACKUP_STATUS_DONE)
					{
						write_backup_status(backup, BACKUP_STATUS_ORPHAN);

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
			pg_free(parent_backup_id);
			tmp_backup = find_parent_full_backup(dest_backup);

			/* sanity */
			if (!tmp_backup)
				elog(ERROR, "Parent full backup for the given backup %s was not found",
						base36enc(dest_backup->start_time));
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

	/*
	 * Ensure that directories provided in tablespace mapping are valid
	 * i.e. empty or not exist.
	 */
	if (is_restore)
	{
		check_tablespace_mapping(dest_backup);

		/* no point in checking external directories if their restore is not resquested */
		if (!skip_external_dirs)
			check_external_dir_mapping(dest_backup);
	}

	/* At this point we are sure that parent chain is whole
	 * so we can build separate array, containing all needed backups,
	 * to simplify validation and restore
	 */
	parent_chain = parray_new();

	/* Take every backup that is a child of base_backup AND parent of dest_backup
	 * including base_backup and dest_backup
	 */

	tmp_backup = dest_backup;
	while(tmp_backup->parent_backup_link)
	{
		parray_append(parent_chain, tmp_backup);
		tmp_backup = tmp_backup->parent_backup_link;
	}

	parray_append(parent_chain, base_full_backup);

	/* for validation or restore with enabled validation */
	if (!is_restore || !rt->no_validate)
	{
		if (dest_backup->backup_mode != BACKUP_MODE_FULL)
			elog(INFO, "Validating parents for backup %s", base36enc(dest_backup->start_time));

		/*
		 * Validate backups from base_full_backup to dest_backup.
		 */
		for (i = parray_num(parent_chain) - 1; i >= 0; i--)
		{
			tmp_backup = (pgBackup *) parray_get(parent_chain, i);

			/* Do not interrupt, validate the next backup */
			if (!lock_backup(tmp_backup))
			{
				if (is_restore)
					elog(ERROR, "Cannot lock backup %s directory",
						 base36enc(tmp_backup->start_time));
				else
				{
					elog(WARNING, "Cannot lock backup %s directory, skip validation",
						 base36enc(tmp_backup->start_time));
					continue;
				}
			}

			pgBackupValidate(tmp_backup);
			/* After pgBackupValidate() only following backup
			 * states are possible: ERROR, RUNNING, CORRUPT and OK.
			 * Validate WAL only for OK, because there is no point
			 * in WAL validation for corrupted, errored or running backups.
			 */
			if (tmp_backup->status != BACKUP_STATUS_OK)
			{
				corrupted_backup = tmp_backup;
				break;
			}
			/* We do not validate WAL files of intermediate backups
			 * It`s done to speed up restore
			 */
		}

		/* There is no point in wal validation of corrupted backups */
		if (!corrupted_backup)
		{
			/*
			 * Validate corresponding WAL files.
			 * We pass base_full_backup timeline as last argument to this function,
			 * because it's needed to form the name of xlog file.
			 */
			validate_wal(dest_backup, arclog_path, rt->target_time,
						 rt->target_xid, rt->target_lsn,
						 base_full_backup->tli, instance_config.xlog_seg_size);
		}
		/* Orphinize every OK descendant of corrupted backup */
		else
		{
			char	   *corrupted_backup_id;
			corrupted_backup_id = base36enc_dup(corrupted_backup->start_time);

			for (j = 0; j < parray_num(backups); j++)
			{
				pgBackup   *backup = (pgBackup *) parray_get(backups, j);

				if (is_parent(corrupted_backup->start_time, backup, false))
				{
					if (backup->status == BACKUP_STATUS_OK ||
						backup->status == BACKUP_STATUS_DONE)
					{
						write_backup_status(backup, BACKUP_STATUS_ORPHAN);

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

	/*
	 * If dest backup is corrupted or was orphaned in previous check
	 * produce corresponding error message
	 */
	if (dest_backup->status == BACKUP_STATUS_OK ||
		dest_backup->status == BACKUP_STATUS_DONE)
	{
		if (rt->no_validate)
			elog(WARNING, "Backup %s is used without validation.", base36enc(dest_backup->start_time));
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
	 * TODO: before restore - lock entire parent chain
	 */
	if (is_restore)
	{
		parray	   *dest_external_dirs = NULL;
		parray	   *dest_files;
		char		control_file[MAXPGPATH],
					dest_backup_path[MAXPGPATH];
		int			i;

		/*
		 * Get a list of dbOids to skip if user requested the partial restore.
		 * It is important that we do this after(!) validation so
		 * database_map can be trusted.
		 * NOTE: database_map could be missing for legal reasons, e.g. missing
		 * permissions on pg_database during `backup` and, as long as user do not request
		 * partial restore, it`s OK
		 */
		if (datname_list)
			dbOid_exclude_list = get_dbOid_exclude_list(dest_backup, datname_list,
														  partial_restore_type);

		/*
		 * Preparations for actual restoring.
		 */
		pgBackupGetPath(dest_backup, control_file, lengthof(control_file),
						DATABASE_FILE_LIST);
		dest_files = dir_read_file_list(NULL, NULL, control_file,
										FIO_BACKUP_HOST);
		parray_qsort(dest_files, pgFileCompareRelPathWithExternal);

		/*
		 * Restore dest_backup internal directories.
		 */
		pgBackupGetPath(dest_backup, dest_backup_path,
						lengthof(dest_backup_path), NULL);
		create_data_directories(dest_files, instance_config.pgdata, dest_backup_path, true,
								FIO_DB_HOST);

		/*
		 * Restore dest_backup external directories.
		 */
		if (dest_backup->external_dir_str && !skip_external_dirs)
		{
			dest_external_dirs = make_external_directory_list(
												dest_backup->external_dir_str,
												true);
			if (parray_num(dest_external_dirs) > 0)
				elog(LOG, "Restore external directories");

			for (i = 0; i < parray_num(dest_external_dirs); i++)
				fio_mkdir(parray_get(dest_external_dirs, i),
						  DIR_PERMISSION, FIO_DB_HOST);
		}

		/*
		 * At least restore backups files starting from the parent backup.
		 */
		for (i = parray_num(parent_chain) - 1; i >= 0; i--)
		{
			pgBackup   *backup = (pgBackup *) parray_get(parent_chain, i);

			if (rt->lsn_string &&
				parse_server_version(backup->server_version) < 100000)
				elog(ERROR, "Backup %s was created for version %s which doesn't support recovery_target_lsn",
					 base36enc(dest_backup->start_time),
					 dest_backup->server_version);

			/*
			 * Backup was locked during validation if no-validate wasn't
			 * specified.
			 */
			if (rt->no_validate && !lock_backup(backup))
				elog(ERROR, "Cannot lock backup directory");

			restore_backup(backup, dest_external_dirs, dest_files, dbOid_exclude_list);
		}

		if (dest_external_dirs != NULL)
			free_dir_list(dest_external_dirs);

		parray_walk(dest_files, pgFileFree);
		parray_free(dest_files);

		/* Create recovery.conf with given recovery target parameters */
		create_recovery_conf(target_backup_id, rt, dest_backup);
	}

	/* cleanup */
	parray_walk(backups, pgBackupFree);
	parray_free(backups);
	parray_free(parent_chain);

	elog(INFO, "%s of backup %s completed.",
		 action, base36enc(dest_backup->start_time));
	return 0;
}

/*
 * Restore one backup.
 */
void
restore_backup(pgBackup *backup, parray *dest_external_dirs,
				parray *dest_files, parray *dbOid_exclude_list)
{
	char		timestamp[100];
	char		database_path[MAXPGPATH];
	char		external_prefix[MAXPGPATH];
	char		list_path[MAXPGPATH];
	parray	   *files;
	parray	   *external_dirs = NULL;
	int			i;
	/* arrays with meta info for multi threaded backup */
	pthread_t  *threads;
	restore_files_arg *threads_args;
	bool		restore_isok = true;

	if (backup->status != BACKUP_STATUS_OK &&
		backup->status != BACKUP_STATUS_DONE)
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
	elog(LOG, "Restoring database from backup %s", timestamp);

	if (backup->external_dir_str)
		external_dirs = make_external_directory_list(backup->external_dir_str,
													 true);

	/*
	 * Get list of files which need to be restored.
	 */
	pgBackupGetPath(backup, database_path, lengthof(database_path), DATABASE_DIR);
	pgBackupGetPath(backup, external_prefix, lengthof(external_prefix),
					EXTERNAL_DIR);
	pgBackupGetPath(backup, list_path, lengthof(list_path), DATABASE_FILE_LIST);
	files = dir_read_file_list(database_path, external_prefix, list_path,
							   FIO_BACKUP_HOST);

	/* Restore directories in do_backup_instance way */
	parray_qsort(files, pgFileComparePath);

	/*
	 * Make external directories before restore
	 * and setup threads at the same time
	 */
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);

		/*
		 * If the entry was an external directory, create it in the backup.
		 */
		if (!skip_external_dirs &&
			file->external_dir_num && S_ISDIR(file->mode) &&
			/* Do not create unnecessary external directories */
			parray_bsearch(dest_files, file, pgFileCompareRelPathWithExternal))
		{
			char	   *external_path;

			if (!external_dirs ||
				parray_num(external_dirs) < file->external_dir_num - 1)
				elog(ERROR, "Inconsistent external directory backup metadata");

			external_path = parray_get(external_dirs,
									   file->external_dir_num - 1);
			if (backup_contains_external(external_path, dest_external_dirs))
			{
				char		container_dir[MAXPGPATH];
				char		dirpath[MAXPGPATH];
				char	   *dir_name;

				makeExternalDirPathByNum(container_dir, external_prefix,
										file->external_dir_num);
				dir_name = GetRelativePath(file->path, container_dir);
				elog(VERBOSE, "Create directory \"%s\"", dir_name);
				join_path_components(dirpath, external_path, dir_name);
				fio_mkdir(dirpath, DIR_PERMISSION, FIO_DB_HOST);
			}
		}

		/* setup threads */
		pg_atomic_clear_flag(&file->lock);
	}
	threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
	threads_args = (restore_files_arg *) palloc(sizeof(restore_files_arg) *
												num_threads);

	/* Restore files into target directory */
	thread_interrupted = false;
	for (i = 0; i < num_threads; i++)
	{
		restore_files_arg *arg = &(threads_args[i]);

		arg->files = files;
		arg->backup = backup;
		arg->external_dirs = external_dirs;
		arg->external_prefix = external_prefix;
		arg->dest_external_dirs = dest_external_dirs;
		arg->dest_files = dest_files;
		arg->dbOid_exclude_list = dbOid_exclude_list;
		/* By default there are some error */
		threads_args[i].ret = 1;

		/* Useless message TODO: rewrite */
		elog(LOG, "Start thread for num:%zu", parray_num(files));

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

	if (external_dirs != NULL)
		free_dir_list(external_dirs);

	elog(LOG, "Restore %s backup completed", base36enc(backup->start_time));
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
		pgFile	   *file = (pgFile *) parray_get(arguments->files, i);

		if (!pg_atomic_test_set_flag(&file->lock))
			continue;

		pgBackupGetPath(arguments->backup, from_root,
						lengthof(from_root), DATABASE_DIR);

		/* check for interrupt */
		if (interrupted || thread_interrupted)
			elog(ERROR, "Interrupted during restore database");

		/* Directories were created before */
		if (S_ISDIR(file->mode))
			continue;

		if (progress)
			elog(INFO, "Progress: (%d/%lu). Process file %s ",
				 i + 1, (unsigned long) parray_num(arguments->files),
				 file->rel_path);

		/* only files from pgdata can be skipped by partial restore */
		if (arguments->dbOid_exclude_list &&
			file->external_dir_num == 0)
		{
			/* exclude map is not empty */
			if (parray_bsearch(arguments->dbOid_exclude_list,
							   &file->dbOid, pgCompareOid))
			{
				/* got a match, destination file will truncated */
				create_empty_file(FIO_BACKUP_HOST,
					  instance_config.pgdata, FIO_DB_HOST, file);

				elog(VERBOSE, "Exclude file due to partial restore: \"%s\"", file->rel_path);
				continue;
			}
		}

		/*
		 * For PAGE and PTRACK backups skip datafiles which haven't changed
		 * since previous backup and thus were not backed up.
		 * We cannot do the same when restoring DELTA backup because we need information
		 * about every datafile to correctly truncate them.
		 */
		if (file->write_size == BYTES_INVALID)
		{
			/* data file, only PAGE and PTRACK can skip */
			if (((file->is_datafile && !file->is_cfs) &&
				(arguments->backup->backup_mode == BACKUP_MODE_DIFF_PAGE ||
				 arguments->backup->backup_mode == BACKUP_MODE_DIFF_PTRACK)) ||
				/* non-data file can be skipped regardless of backup type */
				!(file->is_datafile && !file->is_cfs))
			{
				elog(VERBOSE, "The file didn`t change. Skip restore: \"%s\"", file->path);
				continue;
			}
		}

		/* Do not restore tablespace_map file */
		if (path_is_prefix_of_path(PG_TABLESPACE_MAP_FILE, file->rel_path))
		{
			elog(VERBOSE, "Skip tablespace_map");
			continue;
		}

		/* Do not restore database_map file */
		if ((file->external_dir_num == 0) &&
			strcmp(DATABASE_MAP, file->rel_path) == 0)
		{
			elog(VERBOSE, "Skip database_map");
			continue;
		}

		/* Do no restore external directory file if a user doesn't want */
		if (skip_external_dirs && file->external_dir_num > 0)
			continue;

		/* Skip unnecessary file */
		if (parray_bsearch(arguments->dest_files, file,
						   pgFileCompareRelPathWithExternal) == NULL)
			continue;

		/*
		 * restore the file.
		 * We treat datafiles separately, cause they were backed up block by
		 * block and have BackupPageHeader meta information, so we cannot just
		 * copy the file from backup.
		 */
		elog(VERBOSE, "Restoring file \"%s\", is_datafile %i, is_cfs %i",
			 file->path, file->is_datafile?1:0, file->is_cfs?1:0);

		if (file->is_datafile && !file->is_cfs)
		{
			char		to_path[MAXPGPATH];

			join_path_components(to_path, instance_config.pgdata,
								 file->rel_path);
			restore_data_file(to_path, file,
							  arguments->backup->backup_mode == BACKUP_MODE_DIFF_DELTA,
							  false,
							  parse_program_version(arguments->backup->program_version));
		}
		else if (file->external_dir_num)
		{
			char	   *external_path = parray_get(arguments->external_dirs,
												   file->external_dir_num - 1);
			if (backup_contains_external(external_path,
										 arguments->dest_external_dirs))
				copy_file(FIO_BACKUP_HOST,
						  external_path, FIO_DB_HOST, file, false);
		}
		else if (strcmp(file->name, "pg_control") == 0)
			copy_pgcontrol_file(from_root, FIO_BACKUP_HOST,
								instance_config.pgdata, FIO_DB_HOST,
								file);
		else
			copy_file(FIO_BACKUP_HOST,
					  instance_config.pgdata, FIO_DB_HOST,
					  file, false);

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
	bool		need_restore_conf;
	bool		target_latest;

	target_latest = rt->target_stop != NULL &&
		strcmp(rt->target_stop, "latest") == 0;
	need_restore_conf = !backup->stream ||
		(rt->time_string || rt->xid_string || rt->lsn_string) || target_latest;

	/* No need to generate recovery.conf at all. */
	if (!(need_restore_conf || restore_as_replica))
		return;

	elog(LOG, "----------------------------------------");
	elog(LOG, "creating recovery.conf");

	snprintf(path, lengthof(path), "%s/recovery.conf", instance_config.pgdata);
	fp = fio_fopen(path, "wt", FIO_DB_HOST);
	if (fp == NULL)
		elog(ERROR, "cannot open recovery.conf \"%s\": %s", path,
			strerror(errno));

	fio_fprintf(fp, "# recovery.conf generated by pg_probackup %s\n",
				PROGRAM_VERSION);

	if (need_restore_conf)
	{

		fio_fprintf(fp, "restore_command = '%s archive-get -B %s --instance %s "
					"--wal-file-path %%p --wal-file-name %%f'\n",
					PROGRAM_FULL_PATH ? PROGRAM_FULL_PATH : PROGRAM_NAME,
					backup_path, instance_name);

		/*
		 * We've already checked that only one of the four following mutually
		 * exclusive options is specified, so the order of calls is insignificant.
		 */
		if (rt->target_name)
			fio_fprintf(fp, "recovery_target_name = '%s'\n", rt->target_name);

		if (rt->time_string)
			fio_fprintf(fp, "recovery_target_time = '%s'\n", rt->time_string);

		if (rt->xid_string)
			fio_fprintf(fp, "recovery_target_xid = '%s'\n", rt->xid_string);

		if (rt->lsn_string)
			fio_fprintf(fp, "recovery_target_lsn = '%s'\n", rt->lsn_string);

		if (rt->target_stop && !target_latest)
			fio_fprintf(fp, "recovery_target = '%s'\n", rt->target_stop);

		if (rt->inclusive_specified)
			fio_fprintf(fp, "recovery_target_inclusive = '%s'\n",
					rt->target_inclusive ? "true" : "false");

		if (rt->target_tli)
			fio_fprintf(fp, "recovery_target_timeline = '%u'\n", rt->target_tli);

		if (rt->target_action)
			fio_fprintf(fp, "recovery_target_action = '%s'\n", rt->target_action);
	}

	if (restore_as_replica)
	{
		fio_fprintf(fp, "standby_mode = 'on'\n");

		if (backup->primary_conninfo)
			fio_fprintf(fp, "primary_conninfo = '%s'\n", backup->primary_conninfo);
	}

	if (fio_fflush(fp) != 0 ||
		fio_fclose(fp))
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
read_timeline_history(TimeLineID targetTLI)
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
	entry->end = InvalidXLogRecPtr;
	parray_insert(result, 0, entry);

	return result;
}

bool
satisfy_recovery_target(const pgBackup *backup, const pgRecoveryTarget *rt)
{
	if (rt->xid_string)
		return backup->recovery_xid <= rt->target_xid;

	if (rt->time_string)
		return backup->recovery_time <= rt->target_time;

	if (rt->lsn_string)
		return backup->stop_lsn <= rt->target_lsn;

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
			(XLogRecPtrIsInvalid(timeline->end) ||
			 backup->stop_lsn < timeline->end))
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
					const char *target_stop,
					const char *target_name,
					const char *target_action,
					bool		no_validate)
{
	bool		dummy_bool;
	/*
	 * count the number of the mutually exclusive options which may specify
	 * recovery target. If final value > 1, throw an error.
	 */
	int			recovery_target_specified = 0;
	pgRecoveryTarget *rt = pgut_new(pgRecoveryTarget);

	/* fill all options with default values */
	MemSet(rt, 0, sizeof(pgRecoveryTarget));

	/* parse given options */
	if (target_time)
	{
		time_t		dummy_time;

		recovery_target_specified++;
		rt->time_string = target_time;

		if (parse_time(target_time, &dummy_time, false))
			rt->target_time = dummy_time;
		else
			elog(ERROR, "Invalid value for --recovery-target-time option %s",
				 target_time);
	}

	if (target_xid)
	{
		TransactionId dummy_xid;

		recovery_target_specified++;
		rt->xid_string = target_xid;

#ifdef PGPRO_EE
		if (parse_uint64(target_xid, &dummy_xid, 0))
#else
		if (parse_uint32(target_xid, &dummy_xid, 0))
#endif
			rt->target_xid = dummy_xid;
		else
			elog(ERROR, "Invalid value for --recovery-target-xid option %s",
				 target_xid);
	}

	if (target_lsn)
	{
		XLogRecPtr	dummy_lsn;

		recovery_target_specified++;
		rt->lsn_string = target_lsn;
		if (parse_lsn(target_lsn, &dummy_lsn))
			rt->target_lsn = dummy_lsn;
		else
			elog(ERROR, "Invalid value of --ecovery-target-lsn option %s",
				 target_lsn);
	}

	if (target_inclusive)
	{
		rt->inclusive_specified = true;
		if (parse_bool(target_inclusive, &dummy_bool))
			rt->target_inclusive = dummy_bool;
		else
			elog(ERROR, "Invalid value for --recovery-target-inclusive option %s",
				 target_inclusive);
	}

	rt->target_tli = target_tli;
	if (target_stop)
	{
		if ((strcmp(target_stop, "immediate") != 0)
			&& (strcmp(target_stop, "latest") != 0))
			elog(ERROR, "Invalid value for --recovery-target option %s",
				 target_stop);

		recovery_target_specified++;
		rt->target_stop = target_stop;
	}

	rt->no_validate = no_validate;

	if (target_name)
	{
		recovery_target_specified++;
		rt->target_name = target_name;
	}

	if (target_action)
	{
		if ((strcmp(target_action, "pause") != 0)
			&& (strcmp(target_action, "promote") != 0)
			&& (strcmp(target_action, "shutdown") != 0))
			elog(ERROR, "Invalid value for --recovery-target-action option %s",
				 target_action);

		rt->target_action = target_action;
	}
	else
	{
		/* Default recovery target action is pause */
		rt->target_action = "pause";
	}

	/* More than one mutually exclusive option was defined. */
	if (recovery_target_specified > 1)
		elog(ERROR, "At most one of --recovery-target, --recovery-target-name, --recovery-target-time, --recovery-target-xid, or --recovery-target-lsn can be specified");

	/*
	 * If none of the options is defined, '--recovery-target-inclusive' option
	 * is meaningless.
	 */
	if (!(rt->xid_string || rt->time_string || rt->lsn_string) &&
		rt->target_inclusive)
		elog(ERROR, "--recovery-target-inclusive option applies when either --recovery-target-time, --recovery-target-xid or --recovery-target-lsn is specified");

	return rt;
}

/* Return array of dbOids of databases that should not be restored
 * Regardless of what option user used, db-include or db-exclude,
 * we always convert it into exclude_list.
 */
parray *
get_dbOid_exclude_list(pgBackup *backup, parray *datname_list, bool partial_restore_type)
{
	int i;
	int j;
	parray *database_map = NULL;
	parray *dbOid_exclude_list = NULL;

	/* get database_map from file */
	database_map = read_database_map(backup);

	/* partial restore requested but database_map is missing */
	if (!database_map)
		elog(ERROR, "Backup %s has empty or mangled database_map", base36enc(backup->start_time));

	/* So we have list of datnames and database_map for it.
	 * We must construct a list of dbOids to exclude.
	 */
	if (partial_restore_type)
	{
		/* For 'include' find dbOid of every datname NOT specified by user */
		for (i = 0; i < parray_num(datname_list); i++)
		{
			bool found_match = false;
			char   *datname = (char *) parray_get(datname_list, i);

			for (j = 0; j < parray_num(database_map); j++)
			{
				db_map_entry *db_entry = (db_map_entry *) parray_get(database_map, j);

				/* got a match */
				if (strcmp(db_entry->datname, datname) == 0)
				{
					found_match = true;
					/* for db-include we must exclude db_entry from database_map */
					parray_remove(database_map, j);
					j--;
				}
			}
			/* If specified datname is not found in database_map, error out */
			if (!found_match)
				elog(ERROR, "Failed to find a database '%s' in database_map of backup %s",
					datname, base36enc(backup->start_time));
		}

		/* At this moment only databases to exclude are left in the map */
		for (j = 0; j < parray_num(database_map); j++)
		{
			db_map_entry *db_entry = (db_map_entry *) parray_get(database_map, j);

			if (!dbOid_exclude_list)
				dbOid_exclude_list = parray_new();
			parray_append(dbOid_exclude_list, &db_entry->dbOid);
		}
	}
	else
	{
		/* For exclude job is easier, find dbOid for every specified datname  */
		for (i = 0; i < parray_num(datname_list); i++)
		{
			bool found_match = false;
			char   *datname = (char *) parray_get(datname_list, i);

			for (j = 0; j < parray_num(database_map); j++)
			{
				db_map_entry *db_entry = (db_map_entry *) parray_get(database_map, j);

				/* got a match */
				if (strcmp(db_entry->datname, datname) == 0)
				{
					found_match = true;
					/* for db-exclude we must add dbOid to exclude list */
					if (!dbOid_exclude_list)
						dbOid_exclude_list = parray_new();
					parray_append(dbOid_exclude_list, &db_entry->dbOid);
				}
			}
			/* If specified datname is not found in database_map, error out */
			if (!found_match)
				elog(ERROR, "Failed to find a database '%s' in database_map of backup %s",
					datname, base36enc(backup->start_time));
		}
	}

	/* extra sanity, we must be totally sure that list is not empty */
	if (!dbOid_exclude_list || parray_num(dbOid_exclude_list) < 1)
		elog(ERROR, "Failed to find a match for partial restore in database_map of backup %s",
					base36enc(backup->start_time));

	/* sort dbOid array in ASC order */
	parray_qsort(dbOid_exclude_list, pgCompareOid);

	return dbOid_exclude_list;
}

/* Compare two Oids */
int
pgCompareOid(const void *f1, const void *f2)
{
	Oid *f1p = *(Oid **)f1;
	Oid *f2p = *(Oid **)f2;

	return (*(Oid*)f1p - *(Oid*)f2p);
}