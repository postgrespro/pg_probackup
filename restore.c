/*-------------------------------------------------------------------------
 *
 * restore.c: restore DB cluster and archived WAL.
 *
 * Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
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

static void restore_database(pgBackup *backup);
static void create_recovery_conf(time_t backup_id,
								 const char *target_time,
								 const char *target_xid,
								 const char *target_inclusive,
								 TimeLineID target_tli);
static void print_backup_lsn(const pgBackup *backup);
static void restore_files(void *arg);


bool existsTimeLineHistory(TimeLineID probeTLI);


int
do_restore(time_t backup_id,
		   const char *target_time,
		   const char *target_xid,
		   const char *target_inclusive,
		   TimeLineID target_tli)
{
	int			i;
	int			base_index;				/* index of base (full) backup */
	int			ret;
	TimeLineID	cur_tli;
	parray	   *backups;

	parray	   *files;
	parray	   *timelines;
	pgBackup   *base_backup = NULL;
	pgBackup   *dest_backup = NULL;
	pgRecoveryTarget *rt = NULL;
	bool		backup_id_found = false;

	/* PGDATA and ARCLOG_PATH are always required */
	if (pgdata == NULL)
		elog(ERROR,
			 "required parameter not specified: PGDATA (-D, --pgdata)");

	elog(LOG, "========================================");
	elog(LOG, "restore start");

	/* get exclusive lock of backup catalog */
	ret = catalog_lock(false);
	if (ret == -1)
		elog(ERROR, "cannot lock backup catalog.");
	else if (ret == 1)
		elog(ERROR,
			 "another pg_probackup is running, stop restore.");

	/* confirm the PostgreSQL server is not running */
	if (is_pg_running())
		elog(ERROR, "PostgreSQL server is running");

	rt = checkIfCreateRecoveryConf(target_time, target_xid, target_inclusive);
	if (rt == NULL)
		elog(ERROR, "cannot create recovery.conf. specified args are invalid.");

	/* get list of backups. (index == 0) is the last backup */
	backups = catalog_get_backup_list(0);
	if (!backups)
		elog(ERROR, "cannot process any more.");

	cur_tli = get_current_timeline(true);
	elog(LOG, "current instance timeline ID = %u", cur_tli);

	if (target_tli)
	{
		elog(LOG, "target timeline ID = %u", target_tli);
		/* Read timeline history files from archives */
		timelines = readTimeLineHistory(target_tli);
	}

	/* find last full backup which can be used as base backup. */
	elog(LOG, "searching recent full backup");
	for (i = 0; i < parray_num(backups); i++)
	{
		base_backup = (pgBackup *) parray_get(backups, i);

		if (backup_id && base_backup->start_time > backup_id)
			continue;

		if (backup_id == base_backup->start_time &&
			base_backup->status == BACKUP_STATUS_OK)
		{
			backup_id_found = true;
			dest_backup = base_backup;
		}

		if (backup_id == base_backup->start_time &&
			base_backup->status != BACKUP_STATUS_OK)
			elog(ERROR, "given backup %s is %s", base36enc(backup_id),
				 status2str(base_backup->status));

		if (dest_backup != NULL &&
			base_backup->backup_mode == BACKUP_MODE_FULL &&
			base_backup->status != BACKUP_STATUS_OK)
			elog(ERROR, "base backup %s for given backup %s is %s",
				 base36enc(base_backup->start_time),
				 base36enc(dest_backup->start_time),
				 status2str(base_backup->status));

		if (base_backup->backup_mode < BACKUP_MODE_FULL ||
			base_backup->status != BACKUP_STATUS_OK)
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
	elog(ERROR, "no full backup found, cannot restore.");

base_backup_found:
	base_index = i;

	/*
	 * Clear restore destination, but don't remove $PGDATA.
	 * To remove symbolic link, get file list with "omit_symlink = false".
	 */
	if (!check)
	{
		elog(LOG, "----------------------------------------");
		elog(LOG, "clearing restore destination");

		files = parray_new();
		dir_list_file(files, pgdata, false, false, false);
		parray_qsort(files, pgFileComparePathDesc);	/* delete from leaf */

		for (i = 0; i < parray_num(files); i++)
		{
			pgFile *file = (pgFile *) parray_get(files, i);
			pgFileDelete(file);
		}
		parray_walk(files, pgFileFree);
		parray_free(files);
	}

	print_backup_lsn(base_backup);

	if (backup_id != 0)
		stream_wal = base_backup->stream;

	/* restore base backup */
	restore_database(base_backup);

	/* restore following differential backup */
	elog(LOG, "searching differential backup...");

	for (i = base_index - 1; i >= 0; i--)
	{
		pgBackup *backup = (pgBackup *) parray_get(backups, i);

		/* don't use incomplete nor different timeline backup */
		if (backup->status != BACKUP_STATUS_OK ||
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

		print_backup_lsn(backup);
		restore_database(backup);
	}

	/* create recovery.conf */
	if (!stream_wal || target_time != NULL || target_xid != NULL)
		create_recovery_conf(backup_id, target_time, target_xid,
							 target_inclusive, base_backup->tli);

	/* release catalog lock */
	catalog_unlock();

	/* cleanup */
	parray_walk(backups, pgBackupFree);
	parray_free(backups);

	/* print restore complete message */
	if (!check)
	{
		elog(LOG, "all restore completed");
		elog(LOG, "========================================");
	}
	if (!check)
		elog(INFO, "restore complete. Recovery starts automatically when the PostgreSQL server is started.");

	return 0;
}

/*
 * Validate and restore backup.
 */
void
restore_database(pgBackup *backup)
{
	char	timestamp[100];
	char	path[MAXPGPATH];
	char	list_path[MAXPGPATH];
	int		ret;
	parray *files;
	int		i;
	pthread_t	restore_threads[num_threads];
	restore_files_args *restore_threads_args[num_threads];

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
	if (!check)
	{
		elog(LOG, "----------------------------------------");
		elog(LOG, "restoring database from backup %s", timestamp);
	}

	/*
	 * Validate backup files with its size, because load of CRC calculation is
	 * not right.
	 */
	pgBackupValidate(backup, true, false);

	/* make direcotries and symbolic links */
	pgBackupGetPath(backup, path, lengthof(path), MKDIRS_SH_FILE);
	if (!check)
	{
		char pwd[MAXPGPATH];

		/* keep orginal directory */
		if (getcwd(pwd, sizeof(pwd)) == NULL)
			elog(ERROR, "cannot get current working directory: %s",
				strerror(errno));

		/* create pgdata directory */
		dir_create_dir(pgdata, DIR_PERMISSION);

		/* change directory to pgdata */
		if (chdir(pgdata))
			elog(ERROR, "cannot change directory: %s",
				strerror(errno));

		/* Execute mkdirs.sh */
		ret = system(path);
		if (ret != 0)
			elog(ERROR, "cannot execute mkdirs.sh: %s",
				strerror(errno));

		/* go back to original directory */
		if (chdir(pwd))
			elog(ERROR, "cannot change directory: %s",
				strerror(errno));
	}

	/*
	 * get list of files which need to be restored.
	 */
	pgBackupGetPath(backup, path, lengthof(path), DATABASE_DIR);
	pgBackupGetPath(backup, list_path, lengthof(list_path), DATABASE_FILE_LIST);
	files = dir_read_file_list(path, list_path);
	for (i = parray_num(files) - 1; i >= 0; i--)
	{
		pgFile *file = (pgFile *) parray_get(files, i);

		/* remove files which are not backed up */
		if (file->write_size == BYTES_INVALID)
			pgFileFree(parray_remove(files, i));
	}

	for (i = 0; i < parray_num(files); i++)
	{
		pgFile *file = (pgFile *) parray_get(files, i);

		__sync_lock_release(&file->lock);
	}

	/* restore files into $PGDATA */
	for (i = 0; i < num_threads; i++)
	{
		restore_files_args *arg = pg_malloc(sizeof(restore_files_args));
		arg->files = files;
		arg->backup = backup;

		if (verbose)
			elog(WARNING, "Start thread for num:%li", parray_num(files));

		restore_threads_args[i] = arg;
		pthread_create(&restore_threads[i], NULL, (void *(*)(void *)) restore_files, arg);
	}

	/* Wait theads */
	for (i = 0; i < num_threads; i++)
	{
		pthread_join(restore_threads[i], NULL);
		pg_free(restore_threads_args[i]);
	}

	/* Delete files which are not in file list. */
	if (!check)
	{
		parray *files_now;

		parray_walk(files, pgFileFree);
		parray_free(files);

		/* re-read file list to change base path to $PGDATA */
		files = dir_read_file_list(pgdata, list_path);
		parray_qsort(files, pgFileComparePathDesc);

		/* get list of files restored to pgdata */
		files_now = parray_new();
		dir_list_file(files_now, pgdata, true, true, false);
		/* to delete from leaf, sort in reversed order */
		parray_qsort(files_now, pgFileComparePathDesc);

		for (i = 0; i < parray_num(files_now); i++)
		{
			pgFile *file = (pgFile *) parray_get(files_now, i);

			/* If the file is not in the file list, delete it */
			if (parray_bsearch(files, file, pgFileComparePathDesc) == NULL)
			{
				elog(LOG, "deleted %s", file->path + strlen(pgdata) + 1);
				pgFileDelete(file);
			}
		}

		parray_walk(files_now, pgFileFree);
		parray_free(files_now);
	}

	/* remove postmaster.pid */
	snprintf(path, lengthof(path), "%s/postmaster.pid", pgdata);
	if (remove(path) == -1 && errno != ENOENT)
		elog(ERROR, "cannot remove postmaster.pid: %s",
			strerror(errno));

	/* cleanup */
	parray_walk(files, pgFileFree);
	parray_free(files);

	if (!check)
		elog(LOG, "restore backup completed");
}


static void
restore_files(void *arg)
{
	int i;

	restore_files_args *arguments = (restore_files_args *)arg;

	/* restore files into $PGDATA */
	for (i = 0; i < parray_num(arguments->files); i++)
	{
		char from_root[MAXPGPATH];
		pgFile *file = (pgFile *) parray_get(arguments->files, i);
		if (__sync_lock_test_and_set(&file->lock, 1) != 0)
			continue;

		pgBackupGetPath(arguments->backup, from_root, lengthof(from_root), DATABASE_DIR);

		/* check for interrupt */
		if (interrupted)
			elog(ERROR, "interrupted during restore database");

		/* print progress */
		if (!check)
			elog(LOG, "(%d/%lu) %s ", i + 1, (unsigned long) parray_num(arguments->files),
				file->path + strlen(from_root) + 1);

		/* directories are created with mkdirs.sh */
		if (S_ISDIR(file->mode))
		{
			if (!check)
				elog(LOG, "directory, skip");
			continue;
		}

		/* not backed up */
		if (file->write_size == BYTES_INVALID)
		{
			if (!check)
				elog(LOG, "not backed up, skip");
			continue;
		}

		/* restore file */
		if (!check)
			restore_data_file(from_root, pgdata, file, arguments->backup);

		/* print size of restored file */
		if (!check)
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

	if (!check)
	{
		elog(LOG, "----------------------------------------");
		elog(LOG, "creating recovery.conf");
	}

	if (!check)
	{
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
			fprintf(fp, "recovery_target = 'immediate'\n");
			fprintf(fp, "recovery_target_action = 'promote'\n");
		}

		if (target_inclusive)
			fprintf(fp, "recovery_target_inclusive = '%s'\n", target_inclusive);

		fprintf(fp, "recovery_target_timeline = '%u'\n", target_tli);

		fclose(fp);
	}
}

/*
 * Try to read a timeline's history file.
 *
 * If successful, return the list of component pgTimeLine (the ancestor
 * timelines followed by target timeline).	If we cannot find the history file,
 * assume that the timeline has no parents, and return a list of just the
 * specified timeline ID.
 * based on readTimeLineHistory() in xlog.c
 */
parray *
readTimeLineHistory(TimeLineID targetTLI)
{
	parray	   *result;
	char		path[MAXPGPATH];
	char		fline[MAXPGPATH];
	FILE	   *fd;
	pgTimeLine *timeline;
	pgTimeLine *last_timeline = NULL;

	result = parray_new();

	/* search from arclog_path first */
	snprintf(path, lengthof(path), "%s/%08X.history", arclog_path,
		targetTLI);

	fd = fopen(path, "rt");
	if (fd == NULL)
	{
		if (errno != ENOENT)
			elog(ERROR, "could not open file \"%s\": %s", path,
				strerror(errno));
	}

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

		/* Parse one entry... */
		nfields = sscanf(fline, "%u\t%X/%X", &tli, &switchpoint_hi, &switchpoint_lo);

		timeline = pgut_new(pgTimeLine);
		timeline->tli = 0;
		timeline->end = 0;

		/* expect a numeric timeline ID as first field of line */
		timeline->tli = tli;

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

		if (last_timeline && timeline->tli <= last_timeline->tli)
			elog(ERROR,
				   "Timeline IDs must be in increasing sequence.");

		/* Build list with newest item first */
		parray_insert(result, 0, timeline);
		last_timeline = timeline;

		/* Calculate the end lsn finally */
		timeline->end = (XLogRecPtr)
			((uint64) switchpoint_hi << 32) | switchpoint_lo;
	}

	if (fd)
		fclose(fd);

	if (last_timeline && targetTLI <= last_timeline->tli)
		elog(ERROR,
			"Timeline IDs must be less than child timeline's ID.");

	/* append target timeline */
	timeline = pgut_new(pgTimeLine);
	timeline->tli = targetTLI;
	/* lsn in target timeline is valid */
	timeline->end = (uint32) (-1UL << 32) | -1UL;
	parray_insert(result, 0, timeline);

	/* dump timeline branches in verbose mode */
	if (verbose)
	{
		int i;

		for (i = 0; i < parray_num(result); i++)
		{
			pgTimeLine *timeline = parray_get(result, i);
			elog(LOG, "%s() result[%d]: %08X/%08X/%08X", __FUNCTION__, i,
				timeline->tli,
				 (uint32) (timeline->end >> 32),
				 (uint32) timeline->end);
		}
	}

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
		pgTimeLine *timeline = (pgTimeLine *) parray_get(timelines, i);
		if (backup->tli == timeline->tli &&
			backup->stop_lsn < timeline->end)
			return true;
	}
	return false;
}

/* get TLI of the latest full backup */
TimeLineID
get_fullbackup_timeline(parray *backups, const pgRecoveryTarget *rt)
{
	int			i;
	pgBackup   *base_backup = NULL;
	TimeLineID	ret;

	for (i = 0; i < parray_num(backups); i++)
	{
		base_backup = (pgBackup *) parray_get(backups, i);

		if (base_backup->backup_mode >= BACKUP_MODE_FULL)
		{
			/*
			 * Validate backup files with its size, because load of CRC
			 * calculation is not right.
			 */
			if (base_backup->status == BACKUP_STATUS_DONE)
				pgBackupValidate(base_backup, true, true);

			if (!satisfy_recovery_target(base_backup, rt))
				continue;

			if (base_backup->status == BACKUP_STATUS_OK)
				break;
		}
	}
	/* no full backup found, cannot restore */
	if (i == parray_num(backups))
		elog(ERROR, "no full backup found, cannot restore.");

	ret = base_backup->tli;

	return ret;
}

static void
print_backup_lsn(const pgBackup *backup)
{
	char timestamp[100];

	if (!verbose)
		return;

	time2iso(timestamp, lengthof(timestamp), backup->start_time);
	elog(LOG, "  %s (%X/%08X)",
		 timestamp,
		 (uint32) (backup->stop_lsn >> 32),
		 (uint32) backup->stop_lsn);
}

pgRecoveryTarget *
checkIfCreateRecoveryConf(const char *target_time,
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
			elog(ERROR, "cannot create recovery.conf with %s", target_time);
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
			elog(ERROR, "cannot create recovery.conf with %s", target_xid);
	}
	if (target_inclusive)
	{
		if (parse_bool(target_inclusive, &dummy_bool))
			rt->recovery_target_inclusive = dummy_bool;
		else
			elog(ERROR, "cannot create recovery.conf with %s", target_inclusive);
	}

	return rt;

}


/*
 * Probe whether a timeline history file exists for the given timeline ID
 */
bool
existsTimeLineHistory(TimeLineID probeTLI)
{
	char		path[MAXPGPATH];
	FILE		*fd;

	/* Timeline 1 does not have a history file, so no need to check */
	if (probeTLI == 1)
		return false;

	snprintf(path, lengthof(path), "%s/%08X.history", arclog_path, probeTLI);
	fd = fopen(path, "r");
	if (fd != NULL)
	{
		fclose(fd);
		return true;
	}
	else
	{
		if (errno != ENOENT)
			elog(ERROR, "Failed directory for path: %s", path);
		return false;
	}
}

/*
 * Find the newest existing timeline, assuming that startTLI exists.
 *
 * Note: while this is somewhat heuristic, it does positively guarantee
 * that (result + 1) is not a known timeline, and therefore it should
 * be safe to assign that ID to a new timeline.
 */
TimeLineID
findNewestTimeLine(TimeLineID startTLI)
{
	TimeLineID	newestTLI;
	TimeLineID	probeTLI;

	/*
	 * The algorithm is just to probe for the existence of timeline history
	 * files.  XXX is it useful to allow gaps in the sequence?
	 */
	newestTLI = startTLI;

	for (probeTLI = startTLI + 1;; probeTLI++)
	{
		if (existsTimeLineHistory(probeTLI))
		{
			newestTLI = probeTLI;		/* probeTLI exists */
		}
		else
		{
			/* doesn't exist, assume we're done */
			break;
		}
	}

	return newestTLI;
}
