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
								 const char *target_time,
								 const char *target_xid,
								 const char *target_inclusive,
								 TimeLineID target_tli);
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
		   const char *target_time,
		   const char *target_xid,
		   const char *target_inclusive,
		   TimeLineID target_tli,
		   bool is_restore)
{
	int			i;
	parray	   *backups;
	parray	   *timelines;
	pgRecoveryTarget *rt = NULL;
	pgBackup   *current_backup = NULL;
	pgBackup   *dest_backup = NULL;
	pgBackup   *base_full_backup = NULL;
	int			dest_backup_index;
	int			base_full_backup_index;
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

	rt = parseRecoveryTargetOptions(target_time, target_xid, target_inclusive);

	elog(LOG, "%s begin.", action);

	/* Get exclusive lock of backup catalog */
	catalog_lock();
	/* Get list of all backups sorted in order of descending start time */
	backups = catalog_get_backup_list(INVALID_BACKUP_ID);
	if (backups == NULL)
		elog(ERROR, "Failed to get backup list.");

	if (target_tli)
	{
		elog(LOG, "target timeline ID = %u", target_tli);
		/* Read timeline history files from archives */
		timelines = readTimeLineHistory_probackup(target_tli);
	}

	/* Find backup range we should restore. */
	for (i = 0; i < parray_num(backups); i++)
	{
		current_backup = (pgBackup *) parray_get(backups, i);

		/* Skip all backups which started after target backup */
		if (target_backup_id && current_backup->start_time > target_backup_id)
			continue;

		/*
		 * We found target backup. Check its status and
		 * ensure that it satisfies recovery target.
		 */
		if ((target_backup_id == current_backup->start_time
			|| target_backup_id == INVALID_BACKUP_ID)
			&& !dest_backup)
		{
			if (current_backup->status != BACKUP_STATUS_OK)
				elog(ERROR, "given backup %s is in %s status",
					 base36enc(target_backup_id), status2str(current_backup->status));

			if (target_tli)
			{
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

		/* If we already found dest_backup, look for full backup. */
		if (dest_backup)
		{
			if (current_backup->backup_mode == BACKUP_MODE_FULL)
			{
				if (current_backup->status != BACKUP_STATUS_OK)
					elog(ERROR, "base backup %s for given backup %s is in %s status",
						 base36enc(current_backup->start_time),
						 base36enc(dest_backup->start_time),
						 status2str(current_backup->status));
				else
				{
					/* We found both dest and base backups. */
					base_full_backup = current_backup;
					base_full_backup_index = i;
					break;
				}
			}
			else
				/* Skip differential backups are ok */
				continue;
		}
	}

	if (base_full_backup == NULL)
		elog(ERROR, "Full backup satisfying target options is not found.");

	/*
	 * Ensure that directories provided in tablespace mapping are valid
	 * i.e. empty or not exist.
	 */
	if (is_restore)
		check_tablespace_mapping(dest_backup);

	/*
	 * Validate backups from base_full_backup to dest_backup.
	 */
	for (i = base_full_backup_index; i >= dest_backup_index; i--)
	{
		pgBackup   *backup = (pgBackup *) parray_get(backups, i);
		pgBackupValidate(backup);
	}

	/* We ensured that all backups are valid, now restore if required */
	if (is_restore)
	{
		for (i = base_full_backup_index; i >= dest_backup_index; i--)
		{
			pgBackup   *backup = (pgBackup *) parray_get(backups, i);
			if (backup->status == BACKUP_STATUS_OK)
				restore_backup(backup);
			else
				elog(ERROR, "backup %s is not valid",
					 base36enc(backup->start_time));
		}
	}

	/*
	 * Delete files which are not in dest backup file list. Files which were
	 * deleted between previous and current backup are not in the list.
	 */
	if (is_restore)
	{
		pgBackup   *dest_backup = (pgBackup *) parray_get(backups, dest_backup_index);
		if (dest_backup->backup_mode != BACKUP_MODE_FULL)
			remove_deleted_files(dest_backup);
	}

	if (!dest_backup->stream
		|| (target_time != NULL || target_xid != NULL))
	{
		if (is_restore)
			create_recovery_conf(target_backup_id, target_time, target_xid,
								target_inclusive, target_tli);
		else
			validate_wal(dest_backup, arclog_path, rt->recovery_target_time,
						 rt->recovery_target_xid, base_full_backup->tli);
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
	char		backup_path[MAXPGPATH];
	char		database_path[MAXPGPATH];
	char		list_path[MAXPGPATH];
	parray	   *files;
	int			i;
	pthread_t	restore_threads[num_threads];
	restore_files_args *restore_threads_args[num_threads];

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
	 */
	pgBackupGetPath(backup, backup_path, lengthof(backup_path), NULL);
	restore_directories(pgdata, backup_path);

	/*
	 * Get list of files which need to be restored.
	 */
	pgBackupGetPath(backup, database_path, lengthof(database_path), DATABASE_DIR);
	pgBackupGetPath(backup, list_path, lengthof(list_path), DATABASE_FILE_LIST);
	files = dir_read_file_list(database_path, list_path);
	for (i = parray_num(files) - 1; i >= 0; i--)
	{
		pgFile *file = (pgFile *) parray_get(files, i);

		/*
		 * Remove files which haven't changed since previous backup
		 * and was not backed up
		 */
		if (file->write_size == BYTES_INVALID)
			pgFileFree(parray_remove(files, i));
	}

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

		if (verbose)
			elog(LOG, "Start thread for num:%li", parray_num(files));

		restore_threads_args[i] = arg;
		pthread_create(&restore_threads[i], NULL, (void *(*)(void *)) restore_files, arg);
	}

	/* Wait theads */
	for (i = 0; i < num_threads; i++)
	{
		pthread_join(restore_threads[i], NULL);
		pg_free(restore_threads_args[i]);
	}

	/* cleanup */
	parray_walk(files, pgFileFree);
	parray_free(files);

	if (verbose)
	{
		char	   *backup_id;

		backup_id = base36enc(backup->start_time);
		elog(LOG, "restore %s backup completed", backup_id);
		free(backup_id);
	}
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
	char		database_path[MAXPGPATH];
	char		filelist_path[MAXPGPATH];
	int 		i;

	pgBackupGetPath(backup, database_path, lengthof(database_path), DATABASE_DIR);
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
			if (verbose)
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
			if (link_sep)
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
						/* Create rest of directories */
						if (link_sep && (link_sep + 1))
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

				/* Create rest of directories */
				if (link_sep && (link_sep + 1))
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
	char		backup_path[MAXPGPATH];
	parray	   *links;
	size_t		i;
	TablespaceListCell *cell;
	pgFile	   *tmp_file = pgut_new(pgFile);

	links = parray_new();

	pgBackupGetPath(backup, backup_path, lengthof(backup_path), NULL);
	read_tablespace_map(links, backup_path);

	elog(LOG, "check tablespace directories of backup %s", base36enc(backup->start_time));

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


		/* Directories was created before */
		if (S_ISDIR(file->mode))
		{
			elog(LOG, "directory, skip");
			continue;
		}

		/* not backed up */
		if (file->write_size == BYTES_INVALID)
		{
			elog(LOG, "not backed up, skip");
			continue;
		}

		/* Do not restore tablespace_map file */
		if (path_is_prefix_of_path(PG_TABLESPACE_MAP_FILE, rel_path))
		{
			elog(LOG, "skip tablespace_map");
			continue;
		}

		/* restore file */
		if (file->is_datafile)
		{
			if (is_compressed_data_file(file))
				restore_compressed_file(from_root, pgdata, file);
			else
				restore_data_file(from_root, pgdata, file, arguments->backup);
		}
		else
			copy_file(from_root, pgdata, file);

		/* print size of restored file */
		elog(LOG, "restored %lu\n", (unsigned long) file->write_size);
	}
}

static void
create_recovery_conf(time_t backup_id,
					 const char *target_time,
					 const char *target_xid,
					 const char *target_inclusive,
					 TimeLineID target_tli)
{
	char path[MAXPGPATH];
	FILE *fp;

	elog(LOG, "----------------------------------------");
	elog(LOG, "creating recovery.conf");

	snprintf(path, lengthof(path), "%s/recovery.conf", pgdata);
	fp = fopen(path, "wt");
	if (fp == NULL)
		elog(ERROR, "cannot open recovery.conf \"%s\": %s", path,
			strerror(errno));

	fprintf(fp, "# recovery.conf generated by pg_probackup %s\n",
		PROGRAM_VERSION);
	fprintf(fp, "restore_command = 'cp %s/%%f %%p'\n", arclog_path);

	if (target_time)
		fprintf(fp, "recovery_target_time = '%s'\n", target_time);
	else if (target_xid)
		fprintf(fp, "recovery_target_xid = '%s'\n", target_xid);
	else if (backup_id != 0)
	{
		/*
		 * We need to set this parameters only if 'backup_id' is provided
		 * because the backup will be recovered as soon as possible as stop_lsn
		 * is reached.
		 * If 'backup_id' is not set we want to replay all available WAL records,
		 * if 'recovery_target' is set all available WAL records will not be
		 * replayed.
		 */
		fprintf(fp, "recovery_target = 'immediate'\n");
		fprintf(fp, "recovery_target_action = 'promote'\n");
	}

	if (target_inclusive)
		fprintf(fp, "recovery_target_inclusive = '%s'\n", target_inclusive);

	if (target_tli)
		fprintf(fp, "recovery_target_timeline = '%u'\n", target_tli);

	fclose(fp);
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
 * TODO move arguments parsing and validation to getopt.
 */
pgRecoveryTarget *
parseRecoveryTargetOptions(const char *target_time,
                   const char *target_xid,
                   const char *target_inclusive)
{
	time_t			dummy_time;
	TransactionId	dummy_xid;
	bool			dummy_bool;
	pgRecoveryTarget *rt;

	/* Initialize pgRecoveryTarget */
	rt = pgut_new(pgRecoveryTarget);
	rt->time_specified = false;
	rt->xid_specified = false;
	rt->recovery_target_time = 0;
	rt->recovery_target_xid  = 0;
	rt->recovery_target_inclusive = false;

	if (target_time)
	{
		rt->time_specified = true;
		if (parse_time(target_time, &dummy_time))
			rt->recovery_target_time = dummy_time;
		else
			elog(ERROR, "Invalid value of --time option %s", target_time);
	}
	if (target_xid)
	{
		rt->xid_specified = true;
#ifdef PGPRO_EE
		if (parse_uint64(target_xid, &dummy_xid))
#else
		if (parse_uint32(target_xid, &dummy_xid))
#endif
			rt->recovery_target_xid = dummy_xid;
		else
			elog(ERROR, "Invalid value of --xid option %s", target_xid);
	}
	if (target_inclusive)
	{
		if (parse_bool(target_inclusive, &dummy_bool))
			rt->recovery_target_inclusive = dummy_bool;
		else
			elog(ERROR, "Invalid value of --inclusive option %s", target_inclusive);
	}

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
