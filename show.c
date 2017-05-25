/*-------------------------------------------------------------------------
 *
 * show.c: show backup information.
 *
 * Portions Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"
#include <time.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>


static void show_backup_list(FILE *out, parray *backup_list);
static void show_backup_detail(FILE *out, pgBackup *backup);
static int do_show_instance(time_t requested_backup_id);

int
do_show(time_t requested_backup_id)
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
			fprintf(stdout, "\nBACKUP INSTANCE '%s'\n", instance_name);
			do_show_instance(0);
		}
		return 0;
	}
	else
		return do_show_instance(requested_backup_id);
}

/*
 * If 'requested_backup_id' is INVALID_BACKUP_ID, show brief meta information
 * about all backups in the backup instance.
 * If valid backup id is passed, show detailed meta information
 * about specified backup.
 */
static int
do_show_instance(time_t requested_backup_id)
{
	if (requested_backup_id != INVALID_BACKUP_ID)
	{
		pgBackup   *backup;

		backup = read_backup(requested_backup_id);
		if (backup == NULL)
		{
			elog(INFO, "Requested backup \"%s\" is not found.",
				 /* We do not need free base36enc's result, we exit anyway */
				 base36enc(requested_backup_id));
			/* This is not error */
			return 0;
		}

		show_backup_detail(stdout, backup);

		/* cleanup */
		pgBackupFree(backup);

	}
	else
	{
		parray *backup_list;

		backup_list = catalog_get_backup_list(INVALID_BACKUP_ID);
		if (backup_list == NULL)
			elog(ERROR, "Failed to get backup list.");

		show_backup_list(stdout, backup_list);

		/* cleanup */
		parray_walk(backup_list, pgBackupFree);
		parray_free(backup_list);
	}

	return 0;
}

static void
pretty_size(int64 size, char *buf, size_t len)
{
	int exp = 0;

	/* minus means the size is invalid */
	if (size < 0)
	{
		strncpy(buf, "----", len);
		return;
	}

	/* determine postfix */
	while (size > 9999)
	{
		++exp;
		size /= 1000;
	}

	switch (exp)
	{
		case 0:
			snprintf(buf, len, INT64_FORMAT "B", size);
			break;
		case 1:
			snprintf(buf, len, INT64_FORMAT "kB", size);
			break;
		case 2:
			snprintf(buf, len, INT64_FORMAT "MB", size);
			break;
		case 3:
			snprintf(buf, len, INT64_FORMAT "GB", size);
			break;
		case 4:
			snprintf(buf, len, INT64_FORMAT "TB", size);
			break;
		case 5:
			snprintf(buf, len, INT64_FORMAT "PB", size);
			break;
		default:
			strncpy(buf, "***", len);
			break;
	}
}

/* TODO Add comment */
static TimeLineID
get_parent_tli(TimeLineID child_tli)
{
	TimeLineID	result = 0;
	char		path[MAXPGPATH];
	char		fline[MAXPGPATH];
	FILE	   *fd;

	/* Timeline 1 does not have a history file and parent timeline */
	if (child_tli == 1)
		return 0;

	/* Search history file in archives */
	snprintf(path, lengthof(path), "%s/%08X.history", arclog_path,
		child_tli);
	fd = fopen(path, "rt");
	if (fd == NULL)
	{
		if (errno != ENOENT)
			elog(ERROR, "could not open file \"%s\": %s", path,
				strerror(errno));

		/* Did not find history file, do not raise the error */
		return 0;
	}

	/*
	 * Parse the file...
	 */
	while (fgets(fline, sizeof(fline), fd) != NULL)
	{
		/* skip leading whitespace and check for # comment */
		char	   *ptr;
		char	   *endptr;

		for (ptr = fline; *ptr; ptr++)
		{
			if (!IsSpace(*ptr))
				break;
		}
		if (*ptr == '\0' || *ptr == '#')
			continue;

		/* expect a numeric timeline ID as first field of line */
		result = (TimeLineID) strtoul(ptr, &endptr, 0);
		if (endptr == ptr)
			elog(ERROR,
					"syntax error(timeline ID) in history file: %s",
					fline);
	}

	fclose(fd);

	/* TLI of the last line is parent TLI */
	return result;
}

static void
show_backup_list(FILE *out, parray *backup_list)
{
	int			i;

	/* show header */
	fputs("====================================================================================================================\n", out);
	fputs("ID      Recovery time        Mode    WAL      Current/Parent TLI    Time    Data    Start LSN    Stop LSN   Status  \n", out);
	fputs("====================================================================================================================\n", out);

	for (i = 0; i < parray_num(backup_list); i++)
	{
		pgBackup   *backup = parray_get(backup_list, i);
		TimeLineID	parent_tli;
		char	   *backup_id;
		char		timestamp[20] = "----";
		char		duration[20] = "----";
		char		data_bytes_str[10] = "----";

		if (backup->recovery_time != (time_t) 0)
			time2iso(timestamp, lengthof(timestamp), backup->recovery_time);
		if (backup->end_time != (time_t) 0)
			snprintf(duration, lengthof(duration), "%.*lfs",  0,
					 difftime(backup->end_time, backup->start_time));

		/*
		 * Calculate Data field, in the case of full backup this shows the
		 * total amount of data. For an differential backup, this size is only
		 * the difference of data accumulated.
		 */
		pretty_size(backup->data_bytes, data_bytes_str,
				lengthof(data_bytes_str));

		/* Get parent timeline before printing */
		parent_tli = get_parent_tli(backup->tli);
		backup_id = base36enc(backup->start_time);

		fprintf(out, "%-6s  %-19s  %-6s  %-7s  %3d / %-3d            %5s  %6s  %2X/%-8X  %2X/%-8X  %-8s\n",
				backup_id,
				timestamp,
				pgBackupGetBackupMode(backup),
				backup->stream ? "STREAM": "ARCHIVE",
				backup->tli,
				parent_tli,
				duration,
				data_bytes_str,
				(uint32) (backup->start_lsn >> 32),
				(uint32) backup->start_lsn,
				(uint32) (backup->stop_lsn >> 32),
				(uint32) backup->stop_lsn,
				status2str(backup->status));

		free(backup_id);
	}
}

static void
show_backup_detail(FILE *out, pgBackup *backup)
{
	pgBackupWriteControl(out, backup);
}
