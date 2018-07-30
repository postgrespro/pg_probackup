/*-------------------------------------------------------------------------
 *
 * restore.c: restore DB cluster and archived WAL.
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <pthread.h>

#include "catalog/pg_control.h"

typedef struct
{
	parray *files;
	pgBackup *backup;

	/*
	 * Return value from the thread.
	 * 0 means there is no error, 1 - there is an error.
	 */
	int			ret;
} restore_files_args;

/* Tablespace mapping structures */

typedef struct TablespaceListCell
{
	struct TablespaceListCell *next;
	char		old_dir[MAXPGPATH];
	char		new_dir[MAXPGPATH];
} TablespaceListCell;

typedef struct TablespaceList
{
	TablespaceListCell *head;
	TablespaceListCell *tail;
} TablespaceList;

typedef struct TablespaceCreatedListCell
{
	struct TablespaceCreatedListCell *next;
	char		link_name[MAXPGPATH];
	char		linked_dir[MAXPGPATH];
} TablespaceCreatedListCell;

typedef struct TablespaceCreatedList
{
	TablespaceCreatedListCell *head;
	TablespaceCreatedListCell *tail;
} TablespaceCreatedList;

static void restore_backup(pgBackup *backup);
static void restore_directories(const char *pg_data_dir,
								const char *backup_dir);
static void check_tablespace_mapping(pgBackup *backup);
static void create_recovery_conf(time_t backup_id,
								 pgRecoveryTarget *rt,
								 pgBackup *backup);
static void restore_files(void *arg);
static void remove_deleted_files(pgBackup *backup);
static const char *get_tablespace_mapping(const char *dir);
static void set_tablespace_created(const char *link, const char *dir);
static const char *get_tablespace_created(const char *link);

/* Tablespace mapping */
static TablespaceList tablespace_dirs = {NULL, NULL};
static TablespaceCreatedList tablespace_created_dirs = {NULL, NULL};


/*
 * Entry point of pg_probackup RESTORE and VALIDATE subcommands.
 */
int
do_restore_or_validate(time_t target_backup_id,
		   pgRecoveryTarget *rt,
		   bool is_restore)
{
	int			i;
	parray	   *backups;
	parray	   *timelines;
	pgBackup   *current_backup = NULL;
	pgBackup   *dest_backup = NULL;
	pgBackup   *base_full_backup = NULL;
	pgBackup   *corrupted_backup = NULL;
	int			dest_backup_index = 0;
	int			base_full_backup_index = 0;
	int			corrupted_backup_index = 0;
	char 	   *action = is_restore ? "Restore":"Validate";

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
	if (backups == NULL)
		elog(ERROR, "Failed to get backup list.");

	/* Find backup range we should restore or validate. */
	for (i = 0; i < parray_num(backups); i++)
	{
		current_backup = (pgBackup *) parray_get(backups, i);

		/* Skip all backups which started after target backup */
		if (target_backup_id && current_backup->start_time > target_backup_id)
			continue;

		/*
		 * [PGPRO-1164] If BACKUP_ID is not provided for restore command,
		 *  we must find the first valid(!) backup.
		 */

		if (is_restore &&
			!dest_backup &&
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
			|| target_backup_id == INVALID_BACKUP_ID)
			&& !dest_backup)
		{

			/* backup is not ok,
			 * but in case of CORRUPT, ORPHAN or DONE revalidation can be done,
			 * in other cases throw an error.
			 */
			if (current_backup->status != BACKUP_STATUS_OK)
			{
				if (current_backup->status == BACKUP_STATUS_DONE ||
					current_backup->status == BACKUP_STATUS_ORPHAN ||
					current_backup->status == BACKUP_STATUS_CORRUPT)
					elog(WARNING, "Backup %s has status: %s",
						 base36enc(current_backup->start_time), status2str(current_backup->status));
				else
					elog(ERROR, "Backup %s has status: %s",
						 base36enc(current_backup->start_time), status2str(current_backup->status));
			}

			if (rt->recovery_target_tli)
			{
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
			dest_backup_index = i;
		}

	}

	/* If we already found dest_backup, look for full backup. */
	if (dest_backup)
	{
		base_full_backup = current_backup;

		if (current_backup->backup_mode != BACKUP_MODE_FULL)
		{
			base_full_backup = find_parent_backup(current_backup);

			if (base_full_backup == NULL)
				elog(ERROR, "Valid full backup for backup %s is not found.",
					base36enc(current_backup->start_time));
		}

		/*
		 * We have found full backup by link,
		 * now we need to walk the list to find its index.
		 *
		 * TODO I think we should rewrite it someday to use double linked list
		 * and avoid relying on sort order anymore.
		 */
		for (i = dest_backup_index; i < parray_num(backups); i++)
		{
			pgBackup * temp_backup = (pgBackup *) parray_get(backups, i);
			if (temp_backup->start_time == base_full_backup->start_time)
			{
				base_full_backup_index = i;
				break;
			}
		}
	}

	/*
	 * Ensure that directories provided in tablespace mapping are valid
	 * i.e. empty or not exist.
	 */
	if (is_restore)
		check_tablespace_mapping(dest_backup);

	if (dest_backup->backup_mode != BACKUP_MODE_FULL)
		elog(INFO, "Validating parents for backup %s", base36enc(dest_backup->start_time));

	/*
	 * Validate backups from base_full_backup to dest_backup.
	 */
	for (i = base_full_backup_index; i >= dest_backup_index; i--)
	{
		pgBackup   *backup = (pgBackup *) parray_get(backups, i);
		pgBackupValidate(backup);
		/* Maybe we should be more paranoid and check for !BACKUP_STATUS_OK? */
		if (backup->status == BACKUP_STATUS_CORRUPT)
		{
			corrupted_backup = backup;
			corrupted_backup_index = i;
			break;
		}
		/* We do not validate WAL files of intermediate backups
		 * It`s done to speed up restore
		 */
	}
	/* There is no point in wal validation
	 * if there is corrupted backup between base_backup and dest_backup
	 */
	if (!corrupted_backup)
		/*
		 * Validate corresponding WAL files.
		 * We pass base_full_backup timeline as last argument to this function,
		 * because it's needed to form the name of xlog file.
		 */
		validate_wal(dest_backup, arclog_path, rt->recovery_target_time,
					rt->recovery_target_xid, base_full_backup->tli);

	/* Set every incremental backup between corrupted backup and nearest FULL backup as orphans */
	if (corrupted_backup)
	{
		for (i = corrupted_backup_index - 1; i >= 0; i--)
		{
			pgBackup   *backup = (pgBackup *) parray_get(backups, i);
			/* Mark incremental OK backup as orphan */
			if (backup->backup_mode == BACKUP_MODE_FULL)
				break;
			if (backup->status != BACKUP_STATUS_OK)
				continue;
			else
			{
				char	   *backup_id,
						   *corrupted_backup_id;

				backup->status = BACKUP_STATUS_ORPHAN;
				pgBackupWriteBackupControlFile(backup);

				backup_id = base36enc_dup(backup->start_time);
				corrupted_backup_id = base36enc_dup(corrupted_backup->start_time);

				elog(WARNING, "Backup %s is orphaned because his parent %s is corrupted",
					 backup_id, corrupted_backup_id);

				free(backup_id);
				free(corrupted_backup_id);
			}
		}
	}

	/*
	 * If dest backup is corrupted or was orphaned in previous check
	 * produce corresponding error message
	 */
	if (dest_backup->status == BACKUP_STATUS_OK)
		elog(INFO, "Backup %s is valid.", base36enc(dest_backup->start_time));
	else if (dest_backup->status == BACKUP_STATUS_CORRUPT)
		elog(ERROR, "Backup %s is corrupt.", base36enc(dest_backup->start_time));
	else if (dest_backup->status == BACKUP_STATUS_ORPHAN)
		elog(ERROR, "Backup %s is orphan.", base36enc(dest_backup->start_time));
	else
		elog(ERROR, "Backup %s has status: %s",
				base36enc(dest_backup->start_time), status2str(dest_backup->status));

	/* We ensured that all backups are valid, now restore if required */
	if (is_restore)
	{
		for (i = base_full_backup_index; i >= dest_backup_index; i--)
		{
			pgBackup   *backup = (pgBackup *) parray_get(backups, i);
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
	pthread_t	restore_threads[num_threads];
	restore_files_args *restore_threads_args[num_threads];
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
	restore_directories(pgdata, this_backup_path);

	/*
	 * Get list of files which need to be restored.
	 */
	pgBackupGetPath(backup, database_path, lengthof(database_path), DATABASE_DIR);
	pgBackupGetPath(backup, list_path, lengthof(list_path), DATABASE_FILE_LIST);
	files = dir_read_file_list(database_path, list_path);

	/* setup threads */
	for (i = 0; i < parray_num(files); i++)
	{
		pgFile *file = (pgFile *) parray_get(files, i);
		__sync_lock_release(&file->lock);
	}

	/* Restore files into target directory */
	for (i = 0; i < num_threads; i++)
	{
		restore_files_args *arg = pg_malloc(sizeof(restore_files_args));
		arg->files = files;
		arg->backup = backup;
		/* By default there are some error */
		arg->ret = 1;

		elog(LOG, "Start thread for num:%li", parray_num(files));

		restore_threads_args[i] = arg;
		pthread_create(&restore_threads[i], NULL,
					   (void *(*)(void *)) restore_files, arg);
	}

	/* Wait theads */
	for (i = 0; i < num_threads; i++)
	{
		pthread_join(restore_threads[i], NULL);
		if (restore_threads_args[i]->ret == 1)
			restore_isok = false;

		pg_free(restore_threads_args[i]);
	}
	if (!restore_isok)
		elog(ERROR, "Data files restoring failed");

	/* cleanup */
	parray_walk(files, pgFileFree);
	parray_free(files);

	if (LOG_LEVEL_CONSOLE <= LOG || LOG_LEVEL_FILE <= LOG)
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
	int 		i;

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
			if (LOG_LEVEL_CONSOLE <= LOG || LOG_LEVEL_FILE <= LOG)
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
 * Restore backup directories from **backup_database_dir** to **pg_data_dir**.
 *
 * TODO: Think about simplification and clarity of the function.
 */
static void
restore_directories(const char *pg_data_dir, const char *backup_dir)
{
	parray	   *dirs,
			   *links;
	size_t		i;
	char		backup_database_dir[MAXPGPATH],
				to_path[MAXPGPATH];

	dirs = parray_new();
	links = parray_new();

	join_path_components(backup_database_dir, backup_dir, DATABASE_DIR);

	list_data_directories(dirs, backup_database_dir, true, false);
	read_tablespace_map(links, backup_dir);

	elog(LOG, "restore directories and symlinks...");

	for (i = 0; i < parray_num(dirs); i++)
	{
		pgFile	   *dir = (pgFile *) parray_get(dirs, i);
		char	   *relative_ptr = GetRelativePath(dir->path, backup_database_dir);

		Assert(S_ISDIR(dir->mode));

		/* First try to create symlink and linked directory */
		if (path_is_prefix_of_path(PG_TBLSPC_DIR, relative_ptr))
		{
			char	   *link_ptr = GetRelativePath(relative_ptr, PG_TBLSPC_DIR),
					   *link_sep,
					   *tmp_ptr;
			char		link_name[MAXPGPATH];
			pgFile	  **link;

			/* Extract link name from relative path */
			link_sep = first_dir_separator(link_ptr);
			if (link_sep != NULL)
			{
				int			len = link_sep - link_ptr;
				strncpy(link_name, link_ptr, len);
				link_name[len] = '\0';
			}
			else
				goto create_directory;

			tmp_ptr = dir->path;
			dir->path = link_name;
			/* Search only by symlink name without path */
			link = (pgFile **) parray_bsearch(links, dir, pgFileComparePath);
			dir->path = tmp_ptr;

			if (link)
			{
				const char *linked_path = get_tablespace_mapping((*link)->linked);
				const char *dir_created;

				if (!is_absolute_path(linked_path))
					elog(ERROR, "tablespace directory is not an absolute path: %s\n",
						 linked_path);

				/* Check if linked directory was created earlier */
				dir_created = get_tablespace_created(link_name);
				if (dir_created)
				{
					/*
					 * If symlink and linked directory were created do not
					 * create it second time.
					 */
					if (strcmp(dir_created, linked_path) == 0)
					{
						/*
						 * Create rest of directories.
						 * First check is there any directory name after
						 * separator.
						 */
						if (link_sep != NULL && *(link_sep + 1) != '\0')
							goto create_directory;
						else
							continue;
					}
					else
						elog(ERROR, "tablespace directory \"%s\" of page backup does not "
							 "match with previous created tablespace directory \"%s\" of symlink \"%s\"",
							 linked_path, dir_created, link_name);
				}

				/*
				 * This check was done in check_tablespace_mapping(). But do
				 * it again.
				 */
				if (!dir_is_empty(linked_path))
					elog(ERROR, "restore tablespace destination is not empty: \"%s\"",
						 linked_path);

				if (link_sep)
					elog(LOG, "create directory \"%s\" and symbolic link \"%.*s\"",
						 linked_path,
						 (int) (link_sep -  relative_ptr), relative_ptr);
				else
					elog(LOG, "create directory \"%s\" and symbolic link \"%s\"",
						 linked_path, relative_ptr);

				/* Firstly, create linked directory */
				dir_create_dir(linked_path, DIR_PERMISSION);

				join_path_components(to_path, pg_data_dir, PG_TBLSPC_DIR);
				/* Create pg_tblspc directory just in case */
				dir_create_dir(to_path, DIR_PERMISSION);

				/* Secondly, create link */
				join_path_components(to_path, to_path, link_name);
				if (symlink(linked_path, to_path) < 0)
					elog(ERROR, "could not create symbolic link \"%s\": %s",
						 to_path, strerror(errno));

				/* Save linked directory */
				set_tablespace_created(link_name, linked_path);

				/*
				 * Create rest of directories.
				 * First check is there any directory name after separator.
				 */
				if (link_sep != NULL && *(link_sep + 1) != '\0')
					goto create_directory;

				continue;
			}
		}

create_directory:
		elog(LOG, "create directory \"%s\"", relative_ptr);

		/* This is not symlink, create directory */
		join_path_components(to_path, pg_data_dir, relative_ptr);
		dir_create_dir(to_path, DIR_PERMISSION);
	}

	parray_walk(links, pgFileFree);
	parray_free(links);

	parray_walk(dirs, pgFileFree);
	parray_free(dirs);
}

/*
 * Check that all tablespace mapping entries have correct linked directory
 * paths. Linked directories must be empty or do not exist.
 *
 * If tablespace-mapping option is supplied, all OLDDIR entries must have
 * entries in tablespace_map file.
 */
static void
check_tablespace_mapping(pgBackup *backup)
{
	char		this_backup_path[MAXPGPATH];
	parray	   *links;
	size_t		i;
	TablespaceListCell *cell;
	pgFile	   *tmp_file = pgut_new(pgFile);

	links = parray_new();

	pgBackupGetPath(backup, this_backup_path, lengthof(this_backup_path), NULL);
	read_tablespace_map(links, this_backup_path);

	if (LOG_LEVEL_CONSOLE <= LOG || LOG_LEVEL_FILE <= LOG)
		elog(LOG, "check tablespace directories of backup %s",
			 base36enc(backup->start_time));

	/* 1 - each OLDDIR must have an entry in tablespace_map file (links) */
	for (cell = tablespace_dirs.head; cell; cell = cell->next)
	{
		tmp_file->linked = cell->old_dir;

		if (parray_bsearch(links, tmp_file, pgFileCompareLinked) == NULL)
			elog(ERROR, "--tablespace-mapping option's old directory "
				 "doesn't have an entry in tablespace_map file: \"%s\"",
				 cell->old_dir);
	}

	/* 2 - all linked directories must be empty */
	for (i = 0; i < parray_num(links); i++)
	{
		pgFile	   *link = (pgFile *) parray_get(links, i);
		const char *linked_path = link->linked;
		TablespaceListCell *cell;

		for (cell = tablespace_dirs.head; cell; cell = cell->next)
			if (strcmp(link->linked, cell->old_dir) == 0)
			{
				linked_path = cell->new_dir;
				break;
			}

		if (!is_absolute_path(linked_path))
			elog(ERROR, "tablespace directory is not an absolute path: %s\n",
				 linked_path);

		if (!dir_is_empty(linked_path))
			elog(ERROR, "restore tablespace destination is not empty: \"%s\"",
				 linked_path);
	}

	free(tmp_file);
	parray_walk(links, pgFileFree);
	parray_free(links);
}

/*
 * Restore files into $PGDATA.
 */
static void
restore_files(void *arg)
{
	int			i;
	restore_files_args *arguments = (restore_files_args *)arg;

	for (i = 0; i < parray_num(arguments->files); i++)
	{
		char		from_root[MAXPGPATH];
		char	   *rel_path;
		pgFile	   *file = (pgFile *) parray_get(arguments->files, i);

		if (__sync_lock_test_and_set(&file->lock, 1) != 0)
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
			elog(VERBOSE, "The file didn`t changed. Skip restore: %s", file->path);
			continue;
		}

		/* Directories was created before */
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
		elog(VERBOSE, "Restoring file %s, is_datafile %i, is_cfs %i", file->path, file->is_datafile?1:0, file->is_cfs?1:0);
		if (file->is_datafile && !file->is_cfs)
			restore_data_file(from_root, pgdata, file, arguments->backup);
		else
			copy_file(from_root, pgdata, file);

		/* print size of restored file */
		if (file->write_size != BYTES_INVALID)
			elog(LOG, "Restored file %s : %lu bytes",
				file->path, (unsigned long) file->write_size);
	}

	/* Data files restoring is successful */
	arguments->ret = 0;
}

/* Create recovery.conf with given recovery target parameters */
static void
create_recovery_conf(time_t backup_id,
					 pgRecoveryTarget *rt,
					 pgBackup *backup)
{
	char path[MAXPGPATH];
	FILE *fp;
	bool need_restore_conf = false;

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
	entry->end = (uint32) (-1UL << 32) | -1UL;
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

	return true;
}

bool
satisfy_timeline(const parray *timelines, const pgBackup *backup)
{
	int i;
	for (i = 0; i < parray_num(timelines); i++)
	{
		TimeLineHistoryEntry *timeline = (TimeLineHistoryEntry *) parray_get(timelines, i);
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
					bool target_immediate,
					const char *target_name,
					const char *target_action)
{
	time_t			dummy_time;
	TransactionId	dummy_xid;
	bool			dummy_bool;
	/*
	 * count the number of the mutually exclusive options which may specify
	 * recovery target. If final value > 1, throw an error.
	 */
	int				recovery_target_specified = 0;
	pgRecoveryTarget *rt = pgut_new(pgRecoveryTarget);

	/* fill all options with default values */
	rt->time_specified = false;
	rt->xid_specified = false;
	rt->inclusive_specified = false;
	rt->recovery_target_time = 0;
	rt->recovery_target_xid  = 0;
	rt->target_time_string = NULL;
	rt->target_xid_string = NULL;
	rt->recovery_target_inclusive = false;
	rt->recovery_target_tli  = 0;
	rt->recovery_target_immediate = false;
	rt->recovery_target_name = NULL;
	rt->recovery_target_action = NULL;

	/* parse given options */
	if (target_time)
	{
		recovery_target_specified++;
		rt->time_specified = true;
		rt->target_time_string = target_time;

		if (parse_time(target_time, &dummy_time))
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
		elog(ERROR, "At most one of --immediate, --target-name, --time, or --xid can be used");

	/* If none of the options is defined, '--inclusive' option is meaningless */
	if (!(rt->xid_specified || rt->time_specified) && rt->recovery_target_inclusive)
		elog(ERROR, "--inclusive option applies when either --time or --xid is specified");

	return rt;
}

/*
 * Split argument into old_dir and new_dir and append to tablespace mapping
 * list.
 *
 * Copy of function tablespace_list_append() from pg_basebackup.c.
 */
void
opt_tablespace_map(pgut_option *opt, const char *arg)
{
	TablespaceListCell *cell = pgut_new(TablespaceListCell);
	char	   *dst;
	char	   *dst_ptr;
	const char *arg_ptr;

	dst_ptr = dst = cell->old_dir;
	for (arg_ptr = arg; *arg_ptr; arg_ptr++)
	{
		if (dst_ptr - dst >= MAXPGPATH)
			elog(ERROR, "directory name too long");

		if (*arg_ptr == '\\' && *(arg_ptr + 1) == '=')
			;					/* skip backslash escaping = */
		else if (*arg_ptr == '=' && (arg_ptr == arg || *(arg_ptr - 1) != '\\'))
		{
			if (*cell->new_dir)
				elog(ERROR, "multiple \"=\" signs in tablespace mapping\n");
			else
				dst = dst_ptr = cell->new_dir;
		}
		else
			*dst_ptr++ = *arg_ptr;
	}

	if (!*cell->old_dir || !*cell->new_dir)
		elog(ERROR, "invalid tablespace mapping format \"%s\", "
			 "must be \"OLDDIR=NEWDIR\"", arg);

	/*
	 * This check isn't absolutely necessary.  But all tablespaces are created
	 * with absolute directories, so specifying a non-absolute path here would
	 * just never match, possibly confusing users.  It's also good to be
	 * consistent with the new_dir check.
	 */
	if (!is_absolute_path(cell->old_dir))
		elog(ERROR, "old directory is not an absolute path in tablespace mapping: %s\n",
			 cell->old_dir);

	if (!is_absolute_path(cell->new_dir))
		elog(ERROR, "new directory is not an absolute path in tablespace mapping: %s\n",
			 cell->new_dir);

	if (tablespace_dirs.tail)
		tablespace_dirs.tail->next = cell;
	else
		tablespace_dirs.head = cell;
	tablespace_dirs.tail = cell;
}

/*
 * Retrieve tablespace path, either relocated or original depending on whether
 * -T was passed or not.
 *
 * Copy of function get_tablespace_mapping() from pg_basebackup.c.
 */
static const char *
get_tablespace_mapping(const char *dir)
{
	TablespaceListCell *cell;

	for (cell = tablespace_dirs.head; cell; cell = cell->next)
		if (strcmp(dir, cell->old_dir) == 0)
			return cell->new_dir;

	return dir;
}

/*
 * Save create directory path into memory. We can use it in next page restore to
 * not raise the error "restore tablespace destination is not empty" in
 * restore_directories().
 */
static void
set_tablespace_created(const char *link, const char *dir)
{
	TablespaceCreatedListCell *cell = pgut_new(TablespaceCreatedListCell);

	strcpy(cell->link_name, link);
	strcpy(cell->linked_dir, dir);
	cell->next = NULL;

	if (tablespace_created_dirs.tail)
		tablespace_created_dirs.tail->next = cell;
	else
		tablespace_created_dirs.head = cell;
	tablespace_created_dirs.tail = cell;
}

/*
 * Is directory was created when symlink was created in restore_directories().
 */
static const char *
get_tablespace_created(const char *link)
{
	TablespaceCreatedListCell *cell;

	for (cell = tablespace_created_dirs.head; cell; cell = cell->next)
		if (strcmp(link, cell->link_name) == 0)
			return cell->linked_dir;

	return NULL;
}
