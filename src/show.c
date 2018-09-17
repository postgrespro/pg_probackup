/*-------------------------------------------------------------------------
 *
 * show.c: show backup information.
 *
 * Portions Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <time.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>

#include "pqexpbuffer.h"

#include "utils/json.h"


static void show_instance_start(void);
static void show_instance_end(void);
static void show_instance(time_t requested_backup_id, bool show_name);
static int show_backup(time_t requested_backup_id);

static void show_instance_plain(parray *backup_list, bool show_name);
static void show_instance_json(parray *backup_list);

static PQExpBufferData show_buf;
static bool first_instance = true;
static int32 json_level = 0;

int
do_show(time_t requested_backup_id)
{
	if (instance_name == NULL &&
		requested_backup_id != INVALID_BACKUP_ID)
		elog(ERROR, "You must specify --instance to use --backup_id option");

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
			elog(ERROR, "Cannot open directory \"%s\": %s",
				 path, strerror(errno));

		show_instance_start();

		while (errno = 0, (dent = readdir(dir)) != NULL)
		{
			char		child[MAXPGPATH];
			struct stat	st;

			/* skip entries point current dir or parent dir */
			if (strcmp(dent->d_name, ".") == 0 ||
				strcmp(dent->d_name, "..") == 0)
				continue;

			join_path_components(child, path, dent->d_name);

			if (lstat(child, &st) == -1)
				elog(ERROR, "Cannot stat file \"%s\": %s",
					 child, strerror(errno));

			if (!S_ISDIR(st.st_mode))
				continue;

			instance_name = dent->d_name;
			sprintf(backup_instance_path, "%s/%s/%s", backup_path, BACKUPS_DIR, instance_name);

			show_instance(INVALID_BACKUP_ID, true);
		}

		if (errno)
			elog(ERROR, "Cannot read directory \"%s\": %s",
				 path, strerror(errno));

		if (closedir(dir))
			elog(ERROR, "Cannot close directory \"%s\": %s",
				 path, strerror(errno));

		show_instance_end();

		return 0;
	}
	else if (requested_backup_id == INVALID_BACKUP_ID ||
			 show_format == SHOW_JSON)
	{
		show_instance_start();
		show_instance(requested_backup_id, false);
		show_instance_end();

		return 0;
	}
	else
		return show_backup(requested_backup_id);
}

static void
pretty_size(int64 size, char *buf, size_t len)
{
	int			exp = 0;

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
			snprintf(buf, len, "%dB", (int) size);
			break;
		case 1:
			snprintf(buf, len, "%dkB", (int) size);
			break;
		case 2:
			snprintf(buf, len, "%dMB", (int) size);
			break;
		case 3:
			snprintf(buf, len, "%dGB", (int) size);
			break;
		case 4:
			snprintf(buf, len, "%dTB", (int) size);
			break;
		case 5:
			snprintf(buf, len, "%dPB", (int) size);
			break;
		default:
			strncpy(buf, "***", len);
			break;
	}
}

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

/*
 * Initialize instance visualization.
 */
static void
show_instance_start(void)
{
	initPQExpBuffer(&show_buf);

	if (show_format == SHOW_PLAIN)
		return;

	first_instance = true;
	json_level = 0;

	appendPQExpBufferChar(&show_buf, '[');
	json_level++;
}

/*
 * Finalize instance visualization.
 */
static void
show_instance_end(void)
{
	if (show_format == SHOW_JSON)
		appendPQExpBufferStr(&show_buf, "\n]\n");

	fputs(show_buf.data, stdout);
	termPQExpBuffer(&show_buf);
}

/*
 * Show brief meta information about all backups in the backup instance.
 */
static void
show_instance(time_t requested_backup_id, bool show_name)
{
	parray	   *backup_list;

	backup_list = catalog_get_backup_list(requested_backup_id);

	if (show_format == SHOW_PLAIN)
		show_instance_plain(backup_list, show_name);
	else if (show_format == SHOW_JSON)
		show_instance_json(backup_list);
	else
		elog(ERROR, "Invalid show format %d", (int) show_format);

	/* cleanup */
	parray_walk(backup_list, pgBackupFree);
	parray_free(backup_list);
}

/*
 * Show detailed meta information about specified backup.
 */
static int
show_backup(time_t requested_backup_id)
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

	if (show_format == SHOW_PLAIN)
		pgBackupWriteControl(stdout, backup);
	else
		elog(ERROR, "Invalid show format %d", (int) show_format);

	/* cleanup */
	pgBackupFree(backup);

	return 0;
}

/*
 * Plain output.
 */

/*
 * Show instance backups in plain format.
 */
static void
show_instance_plain(parray *backup_list, bool show_name)
{
	int			i;

	if (show_name)
		appendPQExpBuffer(&show_buf, "\nBACKUP INSTANCE '%s'\n", instance_name);

	/* if you add new fields here, fix the header */
	/* show header */
	appendPQExpBufferStr(&show_buf,
						 "============================================================================================================================================\n");
	appendPQExpBufferStr(&show_buf,
						 " Instance    Version  ID      Recovery time           Mode    WAL      Current/Parent TLI    Time    Data   Start LSN    Stop LSN    Status \n");
	appendPQExpBufferStr(&show_buf,
						 "============================================================================================================================================\n");

	for (i = 0; i < parray_num(backup_list); i++)
	{
		pgBackup   *backup = parray_get(backup_list, i);
		TimeLineID	parent_tli;
		char		timestamp[100] = "----";
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

		appendPQExpBuffer(&show_buf,
						  " %-11s %-8s %-6s  %-22s  %-6s  %-7s  %3d / %-3d            %5s  %6s  %2X/%-8X  %2X/%-8X  %-8s\n",
						  instance_name,
						  (backup->server_version[0] ? backup->server_version : "----"),
						  base36enc(backup->start_time),
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
	}
}

/*
 * Json output.
 */

/*
 * Show instance backups in json format.
 */
static void
show_instance_json(parray *backup_list)
{
	int			i;
	PQExpBuffer	buf = &show_buf;

	if (!first_instance)
		appendPQExpBufferChar(buf, ',');

	/* Begin of instance object */
	json_add(buf, JT_BEGIN_OBJECT, &json_level);

	json_add_value(buf, "instance", instance_name, json_level, false);
	json_add_key(buf, "backups", json_level, true);

	/*
	 * List backups.
	 */
	json_add(buf, JT_BEGIN_ARRAY, &json_level);

	for (i = 0; i < parray_num(backup_list); i++)
	{
		pgBackup   *backup = parray_get(backup_list, i);
		TimeLineID	parent_tli;
		char		timestamp[100] = "----";
		char		lsn[20];

		if (i != 0)
			appendPQExpBufferChar(buf, ',');

		json_add(buf, JT_BEGIN_OBJECT, &json_level);

		json_add_value(buf, "id", base36enc(backup->start_time), json_level,
					   false);

		if (backup->parent_backup != 0)
			json_add_value(buf, "parent-backup-id",
						   base36enc(backup->parent_backup), json_level, true);

		json_add_value(buf, "backup-mode", pgBackupGetBackupMode(backup),
					   json_level, true);

		json_add_value(buf, "wal", backup->stream ? "STREAM": "ARCHIVE",
					   json_level, true);

		json_add_value(buf, "compress-alg",
					   deparse_compress_alg(backup->compress_alg), json_level,
					   true);

		json_add_key(buf, "compress-level", json_level, true);
		appendPQExpBuffer(buf, "%d", backup->compress_level);

		json_add_value(buf, "from-replica",
					   backup->from_replica ? "true" : "false", json_level,
					   true);

		json_add_key(buf, "block-size", json_level, true);
		appendPQExpBuffer(buf, "%u", backup->block_size);

		json_add_key(buf, "xlog-block-size", json_level, true);
		appendPQExpBuffer(buf, "%u", backup->wal_block_size);

		json_add_key(buf, "checksum-version", json_level, true);
		appendPQExpBuffer(buf, "%u", backup->checksum_version);

		json_add_value(buf, "program-version", backup->program_version,
					   json_level, true);
		json_add_value(buf, "server-version", backup->server_version,
					   json_level, true);

		json_add_key(buf, "current-tli", json_level, true);
		appendPQExpBuffer(buf, "%d", backup->tli);

		json_add_key(buf, "parent-tli", json_level, true);
		parent_tli = get_parent_tli(backup->tli);
		appendPQExpBuffer(buf, "%u", parent_tli);

		snprintf(lsn, lengthof(lsn), "%X/%X",
				 (uint32) (backup->start_lsn >> 32), (uint32) backup->start_lsn);
		json_add_value(buf, "start-lsn", lsn, json_level, true);

		snprintf(lsn, lengthof(lsn), "%X/%X",
				 (uint32) (backup->stop_lsn >> 32), (uint32) backup->stop_lsn);
		json_add_value(buf, "stop-lsn", lsn, json_level, true);

		time2iso(timestamp, lengthof(timestamp), backup->start_time);
		json_add_value(buf, "start-time", timestamp, json_level, true);

		if (backup->end_time)
		{
			time2iso(timestamp, lengthof(timestamp), backup->end_time);
			json_add_value(buf, "end-time", timestamp, json_level, true);
		}

		json_add_key(buf, "recovery-xid", json_level, true);
		appendPQExpBuffer(buf, XID_FMT, backup->recovery_xid);

		if (backup->recovery_time > 0)
		{
			time2iso(timestamp, lengthof(timestamp), backup->recovery_time);
			json_add_value(buf, "recovery-time", timestamp, json_level, true);
		}

		if (backup->data_bytes != BYTES_INVALID)
		{
			json_add_key(buf, "data-bytes", json_level, true);
			appendPQExpBuffer(buf, INT64_FORMAT, backup->data_bytes);
		}

		if (backup->wal_bytes != BYTES_INVALID)
		{
			json_add_key(buf, "wal-bytes", json_level, true);
			appendPQExpBuffer(buf, INT64_FORMAT, backup->wal_bytes);
		}

		if (backup->primary_conninfo)
			json_add_value(buf, "primary_conninfo", backup->primary_conninfo,
						   json_level, true);

		json_add_value(buf, "status", status2str(backup->status), json_level,
					   true);

		json_add(buf, JT_END_OBJECT, &json_level);
	}

	/* End of backups */
	json_add(buf, JT_END_ARRAY, &json_level);

	/* End of instance object */
	json_add(buf, JT_END_OBJECT, &json_level);

	first_instance = false;
}
