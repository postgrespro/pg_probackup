/*-------------------------------------------------------------------------
 *
 * catalog.c: backup catalog operation
 *
 * Portions Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <dirent.h>
#include <fcntl.h>
#include <libgen.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

static const char *backupModes[] = {"", "PAGE", "PTRACK", "FULL"};
static pgBackup *readBackupControlFile(const char *path);

static bool exit_hook_registered = false;
static char lock_file[MAXPGPATH];

static void
unlink_lock_atexit(void)
{
	int			res;
	res = unlink(lock_file);
	if (res != 0 && res != ENOENT)
		elog(WARNING, "%s: %s", lock_file, strerror(errno));
}

/*
 * Create a lockfile.
 */
void
catalog_lock(void)
{
	int			fd;
	char		buffer[MAXPGPATH * 2 + 256];
	int			ntries;
	int			len;
	int			encoded_pid;
	pid_t		my_pid,
				my_p_pid;

	join_path_components(lock_file, backup_instance_path, BACKUP_CATALOG_PID);

	/*
	 * If the PID in the lockfile is our own PID or our parent's or
	 * grandparent's PID, then the file must be stale (probably left over from
	 * a previous system boot cycle).  We need to check this because of the
	 * likelihood that a reboot will assign exactly the same PID as we had in
	 * the previous reboot, or one that's only one or two counts larger and
	 * hence the lockfile's PID now refers to an ancestor shell process.  We
	 * allow pg_ctl to pass down its parent shell PID (our grandparent PID)
	 * via the environment variable PG_GRANDPARENT_PID; this is so that
	 * launching the postmaster via pg_ctl can be just as reliable as
	 * launching it directly.  There is no provision for detecting
	 * further-removed ancestor processes, but if the init script is written
	 * carefully then all but the immediate parent shell will be root-owned
	 * processes and so the kill test will fail with EPERM.  Note that we
	 * cannot get a false negative this way, because an existing postmaster
	 * would surely never launch a competing postmaster or pg_ctl process
	 * directly.
	 */
	my_pid = getpid();
#ifndef WIN32
	my_p_pid = getppid();
#else

	/*
	 * Windows hasn't got getppid(), but doesn't need it since it's not using
	 * real kill() either...
	 */
	my_p_pid = 0;
#endif

	/*
	 * We need a loop here because of race conditions.  But don't loop forever
	 * (for example, a non-writable $backup_instance_path directory might cause a failure
	 * that won't go away).  100 tries seems like plenty.
	 */
	for (ntries = 0;; ntries++)
	{
		/*
		 * Try to create the lock file --- O_EXCL makes this atomic.
		 *
		 * Think not to make the file protection weaker than 0600.  See
		 * comments below.
		 */
		fd = open(lock_file, O_RDWR | O_CREAT | O_EXCL, 0600);
		if (fd >= 0)
			break;				/* Success; exit the retry loop */

		/*
		 * Couldn't create the pid file. Probably it already exists.
		 */
		if ((errno != EEXIST && errno != EACCES) || ntries > 100)
			elog(ERROR, "could not create lock file \"%s\": %s",
				 lock_file, strerror(errno));

		/*
		 * Read the file to get the old owner's PID.  Note race condition
		 * here: file might have been deleted since we tried to create it.
		 */
		fd = open(lock_file, O_RDONLY, 0600);
		if (fd < 0)
		{
			if (errno == ENOENT)
				continue;		/* race condition; try again */
			elog(ERROR, "could not open lock file \"%s\": %s",
				 lock_file, strerror(errno));
		}
		if ((len = read(fd, buffer, sizeof(buffer) - 1)) < 0)
			elog(ERROR, "could not read lock file \"%s\": %s",
				 lock_file, strerror(errno));
		close(fd);

		if (len == 0)
			elog(ERROR, "lock file \"%s\" is empty", lock_file);

		buffer[len] = '\0';
		encoded_pid = atoi(buffer);

		if (encoded_pid <= 0)
			elog(ERROR, "bogus data in lock file \"%s\": \"%s\"",
				 lock_file, buffer);

		/*
		 * Check to see if the other process still exists
		 *
		 * Per discussion above, my_pid, my_p_pid can be
		 * ignored as false matches.
		 *
		 * Normally kill() will fail with ESRCH if the given PID doesn't
		 * exist.
		 */
		if (encoded_pid != my_pid && encoded_pid != my_p_pid)
		{
			if (kill(encoded_pid, 0) == 0 ||
				(errno != ESRCH && errno != EPERM))
				elog(ERROR, "lock file \"%s\" already exists", lock_file);
		}

		/*
		 * Looks like nobody's home.  Unlink the file and try again to create
		 * it.  Need a loop because of possible race condition against other
		 * would-be creators.
		 */
		if (unlink(lock_file) < 0)
			elog(ERROR, "could not remove old lock file \"%s\": %s",
				 lock_file, strerror(errno));
	}

	/*
	 * Successfully created the file, now fill it.
	 */
	snprintf(buffer, sizeof(buffer), "%d\n", my_pid);

	errno = 0;
	if (write(fd, buffer, strlen(buffer)) != strlen(buffer))
	{
		int			save_errno = errno;

		close(fd);
		unlink(lock_file);
		/* if write didn't set errno, assume problem is no disk space */
		errno = save_errno ? save_errno : ENOSPC;
		elog(ERROR, "could not write lock file \"%s\": %s",
			 lock_file, strerror(errno));
	}
	if (fsync(fd) != 0)
	{
		int			save_errno = errno;

		close(fd);
		unlink(lock_file);
		errno = save_errno;
		elog(ERROR, "could not write lock file \"%s\": %s",
			 lock_file, strerror(errno));
	}
	if (close(fd) != 0)
	{
		int			save_errno = errno;

		unlink(lock_file);
		errno = save_errno;
		elog(ERROR, "could not write lock file \"%s\": %s",
			 lock_file, strerror(errno));
	}

	/*
	 * Arrange to unlink the lock file(s) at proc_exit.
	 */
	if (!exit_hook_registered)
	{
		atexit(unlink_lock_atexit);
		exit_hook_registered = true;
	}
}

/*
 * Read backup meta information from BACKUP_CONTROL_FILE.
 * If no backup matches, return NULL.
 */
pgBackup *
read_backup(time_t timestamp)
{
	pgBackup	tmp;
	char		conf_path[MAXPGPATH];

	tmp.start_time = timestamp;
	pgBackupGetPath(&tmp, conf_path, lengthof(conf_path), BACKUP_CONTROL_FILE);

	return readBackupControlFile(conf_path);
}

/*
 * Get backup_mode in string representation.
 */
const char *
pgBackupGetBackupMode(pgBackup *backup)
{
	return backupModes[backup->backup_mode];
}

static bool
IsDir(const char *dirpath, const char *entry)
{
	char		path[MAXPGPATH];
	struct stat	st;

	snprintf(path, MAXPGPATH, "%s/%s", dirpath, entry);

	return stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

/*
 * Create list of backups.
 * If 'requested_backup_id' is INVALID_BACKUP_ID, return list of all backups.
 * The list is sorted in order of descending start time.
 * If valid backup id is passed only matching backup will be added to the list.
 */
parray *
catalog_get_backup_list(time_t requested_backup_id)
{
	DIR			   *date_dir = NULL;
	struct dirent  *date_ent = NULL;
	parray		   *backups = NULL;
	pgBackup	   *backup = NULL;

	/* open backup instance backups directory */
	date_dir = opendir(backup_instance_path);
	if (date_dir == NULL)
	{
		elog(WARNING, "cannot open directory \"%s\": %s", backup_instance_path,
			strerror(errno));
		goto err_proc;
	}

	/* scan the directory and list backups */
	backups = parray_new();
	for (; (date_ent = readdir(date_dir)) != NULL; errno = 0)
	{
		char backup_conf_path[MAXPGPATH];
		char date_path[MAXPGPATH];

		/* skip not-directory entries and hidden entries */
		if (!IsDir(backup_instance_path, date_ent->d_name)
			|| date_ent->d_name[0] == '.')
			continue;

		/* open subdirectory of specific backup */
		join_path_components(date_path, backup_instance_path, date_ent->d_name);

		/* read backup information from BACKUP_CONTROL_FILE */
		snprintf(backup_conf_path, MAXPGPATH, "%s/%s", date_path, BACKUP_CONTROL_FILE);
		backup = readBackupControlFile(backup_conf_path);

		/* ignore corrupted backups */
		if (backup)
		{
			if (requested_backup_id != INVALID_BACKUP_ID
				&& requested_backup_id != backup->start_time)
			{
				pgBackupFree(backup);
				continue;
			}
			parray_append(backups, backup);
			backup = NULL;
		}

		if (errno && errno != ENOENT)
		{
			elog(WARNING, "cannot read date directory \"%s\": %s",
				 date_ent->d_name, strerror(errno));
			goto err_proc;
		}
	}
	if (errno)
	{
		elog(WARNING, "cannot read backup root directory \"%s\": %s",
			backup_instance_path, strerror(errno));
		goto err_proc;
	}

	closedir(date_dir);
	date_dir = NULL;

	parray_qsort(backups, pgBackupCompareIdDesc);

	return backups;

err_proc:
	if (date_dir)
		closedir(date_dir);
	if (backup)
		pgBackupFree(backup);
	if (backups)
		parray_walk(backups, pgBackupFree);
	parray_free(backups);
	return NULL;
}

/*
 * Find the last completed backup on given timeline
 */
pgBackup *
catalog_get_last_data_backup(parray *backup_list, TimeLineID tli)
{
	int			i;
	pgBackup   *backup = NULL;

	/* backup_list is sorted in order of descending ID */
	for (i = 0; i < parray_num(backup_list); i++)
	{
		backup = (pgBackup *) parray_get(backup_list, (size_t) i);

		if (backup->status == BACKUP_STATUS_OK && backup->tli == tli)
			return backup;
	}

	return NULL;
}

/* create backup directory in $BACKUP_PATH */
int
pgBackupCreateDir(pgBackup *backup)
{
	int		i;
	char	path[MAXPGPATH];
	char   *subdirs[] = { DATABASE_DIR, NULL };

	pgBackupGetPath(backup, path, lengthof(path), NULL);

	if (!dir_is_empty(path))
		elog(ERROR, "backup destination is not empty \"%s\"", path);

	dir_create_dir(path, DIR_PERMISSION);

	/* create directories for actual backup files */
	for (i = 0; subdirs[i]; i++)
	{
		pgBackupGetPath(backup, path, lengthof(path), subdirs[i]);
		dir_create_dir(path, DIR_PERMISSION);
	}

	return 0;
}

/*
 * Write information about backup.in to stream "out".
 */
void
pgBackupWriteControl(FILE *out, pgBackup *backup)
{
	char		timestamp[20];

	fprintf(out, "#Configuration\n");
	fprintf(out, "backup-mode = %s\n", pgBackupGetBackupMode(backup));
	fprintf(out, "stream = %s\n", backup->stream?"true":"false");
	
	fprintf(out, "\n#Compatibility\n");
	fprintf(out, "block-size = %u\n", backup->block_size);
	fprintf(out, "xlog-block-size = %u\n", backup->wal_block_size);
	fprintf(out, "checksum-version = %u\n", backup->checksum_version);

	fprintf(out, "\n#Result backup info\n");
	fprintf(out, "timelineid = %d\n", backup->tli);
	/* LSN returned by pg_start_backup */
	fprintf(out, "start-lsn = %x/%08x\n",
			(uint32) (backup->start_lsn >> 32),
			(uint32) backup->start_lsn);
	/* LSN returned by pg_stop_backup */
	fprintf(out, "stop-lsn = %x/%08x\n",
			(uint32) (backup->stop_lsn >> 32),
			(uint32) backup->stop_lsn);

	time2iso(timestamp, lengthof(timestamp), backup->start_time);
	fprintf(out, "start-time = '%s'\n", timestamp);
	if (backup->end_time > 0)
	{
		time2iso(timestamp, lengthof(timestamp), backup->end_time);
		fprintf(out, "end-time = '%s'\n", timestamp);
	}
	fprintf(out, "recovery-xid = " XID_FMT "\n", backup->recovery_xid);
	if (backup->recovery_time > 0)
	{
		time2iso(timestamp, lengthof(timestamp), backup->recovery_time);
		fprintf(out, "recovery-time = '%s'\n", timestamp);
	}

	/*
	 * Size of PGDATA directory. The size does not include size of related
	 * WAL segments in archive 'wal' directory.
	 */
	if (backup->data_bytes != BYTES_INVALID)
		fprintf(out, "data-bytes = " INT64_FORMAT "\n", backup->data_bytes);

	fprintf(out, "status = %s\n", status2str(backup->status));

	/* 'parent_backup' is set if it is incremental backup */
	if (backup->parent_backup != 0)
	{
		char	   *parent_backup = base36enc(backup->parent_backup);

		fprintf(out, "parent-backup-id = '%s'\n", parent_backup);
		free(parent_backup);
	}
}

/* create BACKUP_CONTROL_FILE */
void
pgBackupWriteBackupControlFile(pgBackup *backup)
{
	FILE   *fp = NULL;
	char	ini_path[MAXPGPATH];

	pgBackupGetPath(backup, ini_path, lengthof(ini_path), BACKUP_CONTROL_FILE);
	fp = fopen(ini_path, "wt");
	if (fp == NULL)
		elog(ERROR, "cannot open configuration file \"%s\": %s", ini_path,
			strerror(errno));

	pgBackupWriteControl(fp, backup);

	fclose(fp);
}

/*
 * Read BACKUP_CONTROL_FILE and create pgBackup.
 *  - Comment starts with ';'.
 *  - Do not care section.
 */
static pgBackup *
readBackupControlFile(const char *path)
{
	pgBackup   *backup = pgut_new(pgBackup);
	char	   *backup_mode = NULL;
	char	   *start_lsn = NULL;
	char	   *stop_lsn = NULL;
	char	   *status = NULL;
	char	   *parent_backup = NULL;

	pgut_option options[] =
	{
		{'s', 0, "backup-mode",			&backup_mode, SOURCE_FILE_STRICT},
		{'u', 0, "timelineid",			&backup->tli, SOURCE_FILE_STRICT},
		{'s', 0, "start-lsn",			&start_lsn, SOURCE_FILE_STRICT},
		{'s', 0, "stop-lsn",			&stop_lsn, SOURCE_FILE_STRICT},
		{'t', 0, "start-time",			&backup->start_time, SOURCE_FILE_STRICT},
		{'t', 0, "end-time",			&backup->end_time, SOURCE_FILE_STRICT},
		{'U', 0, "recovery-xid",		&backup->recovery_xid, SOURCE_FILE_STRICT},
		{'t', 0, "recovery-time",		&backup->recovery_time, SOURCE_FILE_STRICT},
		{'I', 0, "data-bytes",			&backup->data_bytes, SOURCE_FILE_STRICT},
		{'u', 0, "block-size",			&backup->block_size, SOURCE_FILE_STRICT},
		{'u', 0, "xlog-block-size",		&backup->wal_block_size, SOURCE_FILE_STRICT},
		{'u', 0, "checksum_version",	&backup->checksum_version, SOURCE_FILE_STRICT},
		{'b', 0, "stream",				&backup->stream, SOURCE_FILE_STRICT},
		{'s', 0, "status",				&status, SOURCE_FILE_STRICT},
		{'s', 0, "parent-backup-id",	&parent_backup, SOURCE_FILE_STRICT},
		{0}
	};

	if (access(path, F_OK) != 0)
		return NULL;

	pgBackup_init(backup);
	pgut_readopt(path, options, ERROR);

	if (backup_mode)
	{
		backup->backup_mode = parse_backup_mode(backup_mode);
		free(backup_mode);
	}

	if (start_lsn)
	{
		uint32 xlogid;
		uint32 xrecoff;

		if (sscanf(start_lsn, "%X/%X", &xlogid, &xrecoff) == 2)
			backup->start_lsn = (XLogRecPtr) ((uint64) xlogid << 32) | xrecoff;
		else
			elog(WARNING, "invalid START_LSN \"%s\"", start_lsn);
		free(start_lsn);
	}

	if (stop_lsn)
	{
		uint32 xlogid;
		uint32 xrecoff;

		if (sscanf(stop_lsn, "%X/%X", &xlogid, &xrecoff) == 2)
			backup->stop_lsn = (XLogRecPtr) ((uint64) xlogid << 32) | xrecoff;
		else
			elog(WARNING, "invalid STOP_LSN \"%s\"", stop_lsn);
		free(stop_lsn);
	}

	if (status)
	{
		if (strcmp(status, "OK") == 0)
			backup->status = BACKUP_STATUS_OK;
		else if (strcmp(status, "RUNNING") == 0)
			backup->status = BACKUP_STATUS_RUNNING;
		else if (strcmp(status, "ERROR") == 0)
			backup->status = BACKUP_STATUS_ERROR;
		else if (strcmp(status, "DELETING") == 0)
			backup->status = BACKUP_STATUS_DELETING;
		else if (strcmp(status, "DELETED") == 0)
			backup->status = BACKUP_STATUS_DELETED;
		else if (strcmp(status, "DONE") == 0)
			backup->status = BACKUP_STATUS_DONE;
		else if (strcmp(status, "CORRUPT") == 0)
			backup->status = BACKUP_STATUS_CORRUPT;
		else
			elog(WARNING, "invalid STATUS \"%s\"", status);
		free(status);
	}

	if (parent_backup)
	{
		backup->parent_backup = base36dec(parent_backup);
		free(parent_backup);
	}

	return backup;
}

BackupMode
parse_backup_mode(const char *value)
{
	const char *v = value;
	size_t		len;

	/* Skip all spaces detected */
	while (IsSpace(*v))
		v++;
	len = strlen(v);

	if (len > 0 && pg_strncasecmp("full", v, len) == 0)
		return BACKUP_MODE_FULL;
	else if (len > 0 && pg_strncasecmp("page", v, len) == 0)
		return BACKUP_MODE_DIFF_PAGE;
	else if (len > 0 && pg_strncasecmp("ptrack", v, len) == 0)
		return BACKUP_MODE_DIFF_PTRACK;

	/* Backup mode is invalid, so leave with an error */
	elog(ERROR, "invalid backup-mode \"%s\"", value);
	return BACKUP_MODE_INVALID;
}

/* free pgBackup object */
void
pgBackupFree(void *backup)
{
	free(backup);
}

/* Compare two pgBackup with their IDs (start time) in ascending order */
int
pgBackupCompareId(const void *l, const void *r)
{
	pgBackup *lp = *(pgBackup **)l;
	pgBackup *rp = *(pgBackup **)r;

	if (lp->start_time > rp->start_time)
		return 1;
	else if (lp->start_time < rp->start_time)
		return -1;
	else
		return 0;
}

/* Compare two pgBackup with their IDs in descending order */
int
pgBackupCompareIdDesc(const void *l, const void *r)
{
	return -pgBackupCompareId(l, r);
}

/*
 * Construct absolute path of the backup directory.
 * If subdir is not NULL, it will be appended after the path.
 */
void
pgBackupGetPath(const pgBackup *backup, char *path, size_t len, const char *subdir)
{
	char	   *datetime;

	datetime = base36enc(backup->start_time);
	if (subdir)
		snprintf(path, len, "%s/%s/%s", backup_instance_path, datetime, subdir);
	else
		snprintf(path, len, "%s/%s", backup_instance_path, datetime);
	free(datetime);

	make_native_path(path);
}
