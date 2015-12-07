/*-------------------------------------------------------------------------
 *
 * restore.c: restore DB cluster and archived WAL.
 *
 * Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_arman.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "catalog/pg_control.h"

static void backup_online_files(bool re_recovery);
static void restore_database(pgBackup *backup);
static void create_recovery_conf(const char *target_time,
								 const char *target_xid,
								 const char *target_inclusive,
								 TimeLineID target_tli);
static pgRecoveryTarget *checkIfCreateRecoveryConf(const char *target_time,
								 const char *target_xid,
								 const char *target_inclusive);
static parray * readTimeLineHistory(TimeLineID targetTLI);
static bool satisfy_timeline(const parray *timelines, const pgBackup *backup);
static bool satisfy_recovery_target(const pgBackup *backup,
									const pgRecoveryTarget *rt);
static TimeLineID get_current_timeline(void);
static TimeLineID get_fullbackup_timeline(parray *backups,
										  const pgRecoveryTarget *rt);
static void print_backup_id(const pgBackup *backup);
static void search_next_wal(const char *path,
							XLogRecPtr *need_lsn,
							parray *timelines);

int
do_restore(const char *target_time,
		   const char *target_xid,
		   const char *target_inclusive,
		   TimeLineID target_tli)
{
	int i;
	int base_index;				/* index of base (full) backup */
	int last_restored_index;	/* index of last restored database backup */
	int ret;
	TimeLineID	cur_tli;
	TimeLineID	backup_tli;
	parray *backups;
	pgBackup *base_backup = NULL;
	parray *files;
	parray *timelines;
	pgRecoveryTarget *rt = NULL;
	XLogRecPtr need_lsn;

	/* PGDATA and ARCLOG_PATH are always required */
	if (pgdata == NULL)
		elog(ERROR_ARGS,
			_("required parameter not specified: PGDATA (-D, --pgdata)"));
	if (arclog_path == NULL)
		elog(ERROR_ARGS,
			_("required parameter not specified: ARCLOG_PATH (-A, --arclog-path)"));

	if (verbose)
	{
		printf(_("========================================\n"));
		printf(_("restore start\n"));
	}

	/* get exclusive lock of backup catalog */
	ret = catalog_lock();
	if (ret == -1)
		elog(ERROR_SYSTEM, _("can't lock backup catalog."));
	else if (ret == 1)
		elog(ERROR_ALREADY_RUNNING,
			_("another pg_arman is running, stop restore."));

	/* confirm the PostgreSQL server is not running */
	if (is_pg_running())
		elog(ERROR_PG_RUNNING, _("PostgreSQL server is running"));

	rt = checkIfCreateRecoveryConf(target_time, target_xid, target_inclusive);
	if(rt == NULL){
		elog(ERROR_ARGS, _("can't create recovery.conf. specified args are invalid."));
	}

	/* get list of backups. (index == 0) is the last backup */
	backups = catalog_get_backup_list(NULL);
	if(!backups){
		elog(ERROR_SYSTEM, _("can't process any more."));
	}

	cur_tli = get_current_timeline();
	backup_tli = get_fullbackup_timeline(backups, rt);

	/* determine target timeline */
	if (target_tli == 0)
		target_tli = cur_tli != 0 ? cur_tli : backup_tli;

	if (verbose)
	{
		printf(_("current timeline ID = %u\n"), cur_tli);
		printf(_("latest full backup timeline ID = %u\n"), backup_tli);
		printf(_("target timeline ID = %u\n"), target_tli);
	}

	/* backup online WAL */
	backup_online_files(cur_tli != 0 && cur_tli != backup_tli);

	/*
	 * Clear restore destination, but don't remove $PGDATA.
	 * To remove symbolic link, get file list with "omit_symlink = false".
	 */
	if (!check)
	{
		if (verbose)
		{
			printf(_("----------------------------------------\n"));
			printf(_("clearing restore destination\n"));
		}
		files = parray_new();
		dir_list_file(files, pgdata, NULL, false, false);
		parray_qsort(files, pgFileComparePathDesc);	/* delete from leaf */

		for (i = 0; i < parray_num(files); i++)
		{
			pgFile *file = (pgFile *) parray_get(files, i);
			pgFileDelete(file);
		}
		parray_walk(files, pgFileFree);
		parray_free(files);
	}

	/* Read timeline history files from archives */
	timelines = readTimeLineHistory(target_tli);

	/* find last full backup which can be used as base backup. */
	if (verbose)
		printf(_("searching recent full backup\n"));
	for (i = 0; i < parray_num(backups); i++)
	{
		base_backup = (pgBackup *) parray_get(backups, i);

		if (base_backup->backup_mode < BACKUP_MODE_FULL ||
			base_backup->status != BACKUP_STATUS_OK)
			continue;

#ifndef HAVE_LIBZ
		/* Make sure we won't need decompression we haven't got */
		if (base_backup->compress_data)
			elog(ERROR_SYSTEM,
				_("can't restore from compressed backup (compression "
				  "not supported in this installation)"));

#endif
		if (satisfy_timeline(timelines, base_backup) &&
			satisfy_recovery_target(base_backup, rt))
			goto base_backup_found;
	}
	/* no full backup found, can't restore */
	elog(ERROR_NO_BACKUP, _("no full backup found, can't restore."));

base_backup_found:
	base_index = i;

	if (verbose)
		print_backup_id(base_backup);

	/* restore base backup */
	restore_database(base_backup);

	last_restored_index = base_index;

	/* restore following differential backup */
	if (verbose)
		printf(_("searching differential backup...\n"));
	for (i = base_index - 1; i >= 0; i--)
	{
		pgBackup *backup = (pgBackup *) parray_get(backups, i);

		/* don't use incomplete nor different timeline backup */
		if (backup->status != BACKUP_STATUS_OK ||
					backup->tli != base_backup->tli)
			continue;

		/* use database backup only */
		if (backup->backup_mode != BACKUP_MODE_DIFF_PAGE)
			continue;

		/* is the backup is necessary for restore to target timeline ? */
		if (!satisfy_timeline(timelines, backup) ||
			!satisfy_recovery_target(backup, rt))
			continue;

		if (verbose)
			print_backup_id(backup);

		restore_database(backup);
		last_restored_index = i;
	}

	for (i = last_restored_index; i >= 0; i--)
	{
		char	xlogpath[MAXPGPATH];
		if (verbose)
			printf(_("searching archived WAL...\n"));

		search_next_wal(arclog_path, &need_lsn, timelines);

		if (verbose)
			printf(_("searching online WAL...\n"));

		join_path_components(xlogpath, pgdata, PG_XLOG_DIR);
		search_next_wal(xlogpath, &need_lsn, timelines);

		if (verbose)
			printf(_("all necessary files are found.\n"));
	}

	/* create recovery.conf */
	create_recovery_conf(target_time, target_xid, target_inclusive, target_tli);

	/* release catalog lock */
	catalog_unlock();

	/* cleanup */
	parray_walk(backups, pgBackupFree);
	parray_free(backups);

	/* print restore complete message */
	if (verbose && !check)
	{
		printf(_("all restore completed\n"));
		printf(_("========================================\n"));
	}
	if (!check)
		elog(INFO, _("restore complete. Recovery starts automatically when the PostgreSQL server is started."));

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

	/* confirm block size compatibility */
	if (backup->block_size != BLCKSZ)
		elog(ERROR_PG_INCOMPATIBLE,
			_("BLCKSZ(%d) is not compatible(%d expected)"),
			backup->block_size, BLCKSZ);
	if (backup->wal_block_size != XLOG_BLCKSZ)
		elog(ERROR_PG_INCOMPATIBLE,
			_("XLOG_BLCKSZ(%d) is not compatible(%d expected)"),
			backup->wal_block_size, XLOG_BLCKSZ);

	time2iso(timestamp, lengthof(timestamp), backup->start_time);
	if (verbose && !check)
	{
		printf(_("----------------------------------------\n"));
		printf(_("restoring database from backup %s.\n"), timestamp);
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
			elog(ERROR_SYSTEM, _("can't get current working directory: %s"),
				strerror(errno));

		/* create pgdata directory */
		dir_create_dir(pgdata, DIR_PERMISSION);

		/* change directory to pgdata */
		if (chdir(pgdata))
			elog(ERROR_SYSTEM, _("can't change directory: %s"),
				strerror(errno));

		/* Execute mkdirs.sh */
		ret = system(path);
		if (ret != 0)
			elog(ERROR_SYSTEM, _("can't execute mkdirs.sh: %s"),
				strerror(errno));

		/* go back to original directory */
		if (chdir(pwd))
			elog(ERROR_SYSTEM, _("can't change directory: %s"),
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

	/* restore files into $PGDATA */
	for (i = 0; i < parray_num(files); i++)
	{
		char from_root[MAXPGPATH];
		pgFile *file = (pgFile *) parray_get(files, i);

		pgBackupGetPath(backup, from_root, lengthof(from_root), DATABASE_DIR);

		/* check for interrupt */
		if (interrupted)
			elog(ERROR_INTERRUPTED, _("interrupted during restore database"));

		/* print progress */
		if (verbose && !check)
			printf(_("(%d/%lu) %s "), i + 1, (unsigned long) parray_num(files),
				file->path + strlen(from_root) + 1);

		/* directories are created with mkdirs.sh */
		if (S_ISDIR(file->mode))
		{
			if (verbose && !check)
				printf(_("directory, skip\n"));
			continue;
		}

		/* not backed up */
		if (file->write_size == BYTES_INVALID)
		{
			if (verbose && !check)
				printf(_("not backed up, skip\n"));
			continue;
		}

		/* restore file */
		if (!check)
			restore_data_file(from_root, pgdata, file, backup->compress_data);

		/* print size of restored file */
		if (verbose && !check)
			printf(_("restored %lu\n"), (unsigned long) file->write_size);
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
		dir_list_file(files_now, pgdata, pgdata_exclude, true, false);
		/* to delete from leaf, sort in reversed order */
		parray_qsort(files_now, pgFileComparePathDesc);

		for (i = 0; i < parray_num(files_now); i++)
		{
			pgFile *file = (pgFile *) parray_get(files_now, i);

			/* If the file is not in the file list, delete it */
			if (parray_bsearch(files, file, pgFileComparePathDesc) == NULL)
			{
				if (verbose)
					printf(_("  delete %s\n"), file->path + strlen(pgdata) + 1);
				pgFileDelete(file);
			}
		}

		parray_walk(files_now, pgFileFree);
		parray_free(files_now);
	}

	/* remove postmaster.pid */
	snprintf(path, lengthof(path), "%s/postmaster.pid", pgdata);
	if (remove(path) == -1 && errno != ENOENT)
		elog(ERROR_SYSTEM, _("can't remove postmaster.pid: %s"),
			strerror(errno));

	/* cleanup */
	parray_walk(files, pgFileFree);
	parray_free(files);

	if (verbose && !check)
		printf(_("restore backup completed\n"));
}


static void
create_recovery_conf(const char *target_time,
					 const char *target_xid,
					 const char *target_inclusive,
					 TimeLineID target_tli)
{
	char path[MAXPGPATH];
	FILE *fp;

	if (verbose && !check)
	{
		printf(_("----------------------------------------\n"));
		printf(_("creating recovery.conf\n"));
	}

	if (!check)
	{
		snprintf(path, lengthof(path), "%s/recovery.conf", pgdata);
		fp = fopen(path, "wt");
		if (fp == NULL)
			elog(ERROR_SYSTEM, _("can't open recovery.conf \"%s\": %s"), path,
				strerror(errno));

		fprintf(fp, "# recovery.conf generated by pg_arman %s\n",
			PROGRAM_VERSION);
		fprintf(fp, "restore_command = 'cp %s/%%f %%p'\n", arclog_path);
		if (target_time)
			fprintf(fp, "recovery_target_time = '%s'\n", target_time);
		if (target_xid)
			fprintf(fp, "recovery_target_xid = '%s'\n", target_xid);
		if (target_inclusive)
			fprintf(fp, "recovery_target_inclusive = '%s'\n", target_inclusive);
		fprintf(fp, "recovery_target_timeline = '%u'\n", target_tli);

		fclose(fp);
	}
}

static void
backup_online_files(bool re_recovery)
{
	char work_path[MAXPGPATH];
	char pg_xlog_path[MAXPGPATH];
	bool files_exist;
	parray *files;

	if (verbose && !check)
	{
		printf(_("----------------------------------------\n"));
		printf(_("backup online WAL start\n"));
	}

	/* get list of files in $BACKUP_PATH/backup/pg_xlog */
	files = parray_new();
	snprintf(work_path, lengthof(work_path), "%s/%s/%s", backup_path,
		RESTORE_WORK_DIR, PG_XLOG_DIR);
	dir_list_file(files, work_path, NULL, true, false);

	files_exist = parray_num(files) > 0;

	parray_walk(files, pgFileFree);
	parray_free(files);

	/* If files exist in RESTORE_WORK_DIR and not re-recovery, use them. */
	if (files_exist && !re_recovery)
	{
		if (verbose)
			printf(_("online WALs have been already backed up, use them.\n"));

		return;
	}

	/* backup online WAL */
	snprintf(pg_xlog_path, lengthof(pg_xlog_path), "%s/pg_xlog", pgdata);
	snprintf(work_path, lengthof(work_path), "%s/%s/%s", backup_path,
		RESTORE_WORK_DIR, PG_XLOG_DIR);
	dir_create_dir(work_path, DIR_PERMISSION);
	dir_copy_files(pg_xlog_path, work_path);
}


/*
 * Try to read a timeline's history file.
 *
 * If successful, return the list of component pgTimeLine (the ancestor
 * timelines followed by target timeline).	If we can't find the history file,
 * assume that the timeline has no parents, and return a list of just the
 * specified timeline ID.
 * based on readTimeLineHistory() in xlog.c
 */
static parray *
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
			elog(ERROR_SYSTEM, _("could not open file \"%s\": %s"), path,
				strerror(errno));

		/* search from restore work directory next */
		snprintf(path, lengthof(path), "%s/%s/%s/%08X.history", backup_path,
			RESTORE_WORK_DIR, PG_XLOG_DIR, targetTLI);
		fd = fopen(path, "rt");
		if (fd == NULL)
		{
			if (errno != ENOENT)
				elog(ERROR_SYSTEM, _("could not open file \"%s\": %s"), path,
						strerror(errno));
		}
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
			elog(ERROR_CORRUPTED,
				 _("syntax error in history file: %s. Expected a numeric timeline ID."),
				   fline);
		}
		if (nfields != 3)
			elog(ERROR_CORRUPTED,
				 _("syntax error in history file: %s. Expected a transaction log switchpoint location."),
				   fline);

		if (last_timeline && timeline->tli <= last_timeline->tli)
			elog(ERROR_CORRUPTED,
				   _("Timeline IDs must be in increasing sequence."));

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
		elog(ERROR_CORRUPTED,
			_("Timeline IDs must be less than child timeline's ID."));

	/* append target timeline */
	timeline = pgut_new(pgTimeLine);
	timeline->tli = targetTLI;
	/* lsn in target timeline is valid */
	timeline->end = (uint32) (-1UL << 32) | -1UL;
	parray_insert(result, 0, timeline);

	/* dump timeline branches for debug */
	if (debug)
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

static bool
satisfy_recovery_target(const pgBackup *backup, const pgRecoveryTarget *rt)
{
	if (rt->xid_specified)
		return backup->recovery_xid <= rt->recovery_target_xid;

	if (rt->time_specified)
		return backup->recovery_time <= rt->recovery_target_time;

	return true;
}

static bool
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

/* get TLI of the current database */
static TimeLineID
get_current_timeline(void)
{
	ControlFileData ControlFile;
	int			fd;
	char		ControlFilePath[MAXPGPATH];
	pg_crc32	crc;
	TimeLineID	ret;

	snprintf(ControlFilePath, MAXPGPATH, "%s/global/pg_control", pgdata);

	if ((fd = open(ControlFilePath, O_RDONLY | PG_BINARY, 0)) == -1)
	{
		elog(WARNING, _("can't open pg_controldata file \"%s\": %s"),
			ControlFilePath, strerror(errno));
		return 0;
	}

	if (read(fd, &ControlFile, sizeof(ControlFileData)) != sizeof(ControlFileData))
	{
		elog(WARNING, _("can't read pg_controldata file \"%s\": %s"),
			ControlFilePath, strerror(errno));
		return 0;
	}
	close(fd);

	/* Check the CRC. */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc,
		   	(char *) &ControlFile,
		   	offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);

	if (!EQ_CRC32C(crc, ControlFile.crc))
	{
		elog(WARNING, _("Calculated CRC checksum does not match value stored in file.\n"
			"Either the file is corrupt, or it has a different layout than this program\n"
			"is expecting.  The results below are untrustworthy.\n"));
		return 0;
	}

	if (ControlFile.pg_control_version % 65536 == 0 && ControlFile.pg_control_version / 65536 != 0)
	{
		elog(WARNING, _("possible byte ordering mismatch\n"
			"The byte ordering used to store the pg_control file might not match the one\n"
			"used by this program.  In that case the results below would be incorrect, and\n"
			"the PostgreSQL installation would be incompatible with this data directory.\n"));
		return 0;
	}

	ret = ControlFile.checkPointCopy.ThisTimeLineID;

	return ret;
}

/* get TLI of the latest full backup */
static TimeLineID
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

			if(!satisfy_recovery_target(base_backup, rt))
				continue;

			if (base_backup->status == BACKUP_STATUS_OK)
				break;
		}
	}
	/* no full backup found, can't restore */
	if (i == parray_num(backups))
		elog(ERROR_NO_BACKUP, _("no full backup found, can't restore."));

	ret = base_backup->tli;

	return ret;
}

static void
print_backup_id(const pgBackup *backup)
{
	char timestamp[100];
	time2iso(timestamp, lengthof(timestamp), backup->start_time);
	printf(_("  %s (%X/%08X)\n"),
		   timestamp,
		   (uint32) (backup->stop_lsn >> 32),
		   (uint32) backup->stop_lsn);
}

static void
search_next_wal(const char *path, XLogRecPtr *need_lsn, parray *timelines)
{
	int		i;
	int		j;
	int		count;
	char	xlogfname[MAXFNAMELEN];
	char	pre_xlogfname[MAXFNAMELEN];
	char	xlogpath[MAXPGPATH];
	struct stat	st;

	count = 0;
	for (;;)
	{
		for (i = 0; i < parray_num(timelines); i++)
		{
			pgTimeLine *timeline = (pgTimeLine *) parray_get(timelines, i);

			xlog_fname(xlogfname, timeline->tli, *need_lsn);
			join_path_components(xlogpath, path, xlogfname);

			if (stat(xlogpath, &st) == 0)
				break;
		}

		/* not found */
		if (i == parray_num(timelines))
		{
			if (count == 1)
				printf(_("\n"));
			else if (count > 1)
				printf(_(" - %s\n"), pre_xlogfname);

			return;
		}

		count++;
		if (count == 1)
			printf(_("%s"), xlogfname);

		strcpy(pre_xlogfname, xlogfname);

		/* delete old TLI */
		for (j = i + 1; j < parray_num(timelines); j++)
			parray_remove(timelines, i + 1);
		/* XXX: should we add a linebreak when we find a timeline? */

		/*
		 * Move to next xlog segment. Note that we need to increment
		 * by XLogSegSize to jump directly to the next WAL segment file
		 * and as this value is used to generate the WAL file name with
		 * the current timeline of backup.
		 */
		*need_lsn += XLogSegSize;
	}
}

static pgRecoveryTarget *
checkIfCreateRecoveryConf(const char *target_time,
                   const char *target_xid,
                   const char *target_inclusive)
{
	time_t		dummy_time;
	unsigned int	dummy_xid;
	bool		dummy_bool;
	pgRecoveryTarget *rt;

	/* Initialize pgRecoveryTarget */
	rt = pgut_new(pgRecoveryTarget);
	rt->time_specified = false;
	rt->xid_specified = false;
	rt->recovery_target_time = 0;
	rt->recovery_target_xid  = 0;
	rt->recovery_target_inclusive = false;

	if(target_time){
		rt->time_specified = true;
		if(parse_time(target_time, &dummy_time))
			rt->recovery_target_time = dummy_time;
		else
			elog(ERROR_ARGS, _("can't create recovery.conf with %s"), target_time);
	}
	if(target_xid){
		rt->xid_specified = true;
		if(parse_uint32(target_xid, &dummy_xid))
			rt->recovery_target_xid = dummy_xid;
		else
			elog(ERROR_ARGS, _("can't create recovery.conf with %s"), target_xid);
	}
	if(target_inclusive){
		if(parse_bool(target_inclusive, &dummy_bool))
			rt->recovery_target_inclusive = dummy_bool;
		else
			elog(ERROR_ARGS, _("can't create recovery.conf with %s"), target_inclusive);
	}

	return rt;

}
