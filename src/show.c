/*-------------------------------------------------------------------------
 *
 * show.c: show backup information.
 *
 * Portions Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"
#include "access/timeline.h"

#include <time.h>
#include <dirent.h>
#include <sys/stat.h>

#include "utils/json.h"

typedef struct ShowBackendRow
{
	const char *instance;
	const char *version;
	char		backup_id[20];
	char		recovery_time[100];
	const char *mode;
	const char *wal_mode;
	char		tli[20];
	char		duration[20];
	char		data_bytes[20];
	char		start_lsn[20];
	char		stop_lsn[20];
	const char *status;
} ShowBackendRow;


typedef struct ShowArchiveRow
{
	const char *instance;
	char		tli[20];
	char		parent_tli[20];
	char		start_lsn[20];
	char		min_segno[20];
	char		max_segno[20];
	char		n_files[20];
	char		size[20];
	const char *status;
} ShowArchiveRow;

static void show_instance_start(void);
static void show_instance_end(void);
static void show_instance(time_t requested_backup_id, bool show_name);
static int show_backup(time_t requested_backup_id);

static void show_instance_plain(parray *backup_list, bool show_name);
static void show_instance_json(parray *backup_list);

static void show_instance_archive(void);
static void show_archive_plain(parray *timelines_list, bool show_name);
static void show_archive_json(parray *tli_list);

static PQExpBufferData show_buf;
static bool first_instance = true;
static int32 json_level = 0;

int
do_show(time_t requested_backup_id, bool show_archive)
{
	if (instance_name == NULL &&
		requested_backup_id != INVALID_BACKUP_ID)
		elog(ERROR, "You must specify --instance to use --backup_id option");

	if (instance_name == NULL &&
		show_archive)
		elog(ERROR, "You must specify --instance to use --archive option");

	if (show_archive)
	{
		show_instance_start();
		show_instance_archive();
		show_instance_end();
		return 0;
	}
	else if (instance_name == NULL)
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

void
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
		// TODO for 3.0: we should ERROR out here.
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
#define SHOW_FIELDS_COUNT 12
	int			i;
	const char *names[SHOW_FIELDS_COUNT] =
					{ "Instance", "Version", "ID", "Recovery Time",
					  "Mode", "WAL", "Current/Parent TLI", "Time", "Data",
					  "Start LSN", "Stop LSN", "Status" };
	const char *field_formats[SHOW_FIELDS_COUNT] =
					{ " %-*s ", " %-*s ", " %-*s ", " %-*s ",
					  " %-*s ", " %-*s ", " %-*s ", " %*s ", " %*s ",
					  " %*s ", " %*s ", " %-*s "};
	uint32		widths[SHOW_FIELDS_COUNT];
	uint32		widths_sum = 0;
	ShowBackendRow *rows;
	time_t current_time = time(NULL);

	for (i = 0; i < SHOW_FIELDS_COUNT; i++)
		widths[i] = strlen(names[i]);

	rows = (ShowBackendRow *) palloc(parray_num(backup_list) *
									 sizeof(ShowBackendRow));

	/*
	 * Fill row values and calculate maximum width of each field.
	 */
	for (i = 0; i < parray_num(backup_list); i++)
	{
		pgBackup   *backup = parray_get(backup_list, i);
		ShowBackendRow *row = &rows[i];
		int			cur = 0;

		/* Instance */
		row->instance = instance_name;
		widths[cur] = Max(widths[cur], strlen(row->instance));
		cur++;

		/* Version */
		row->version = backup->server_version[0] ?
			backup->server_version : "----";
		widths[cur] = Max(widths[cur], strlen(row->version));
		cur++;

		/* ID */
		snprintf(row->backup_id, lengthof(row->backup_id), "%s",
				 base36enc(backup->start_time));
		widths[cur] = Max(widths[cur], strlen(row->backup_id));
		cur++;

		/* Recovery Time */
		if (backup->recovery_time != (time_t) 0)
			time2iso(row->recovery_time, lengthof(row->recovery_time),
					 backup->recovery_time);
		else
			StrNCpy(row->recovery_time, "----", sizeof(row->recovery_time));
		widths[cur] = Max(widths[cur], strlen(row->recovery_time));
		cur++;

		/* Mode */
		row->mode = pgBackupGetBackupMode(backup);
		widths[cur] = Max(widths[cur], strlen(row->mode));
		cur++;

		/* WAL */
		row->wal_mode = backup->stream ? "STREAM": "ARCHIVE";
		widths[cur] = Max(widths[cur], strlen(row->wal_mode));
		cur++;

		/* Current/Parent TLI */
		snprintf(row->tli, lengthof(row->tli), "%u / %u",
				 backup->tli,
				 backup->backup_mode == BACKUP_MODE_FULL ? 0 : get_parent_tli(backup->tli));
		widths[cur] = Max(widths[cur], strlen(row->tli));
		cur++;

		/* Time */
		if (backup->status == BACKUP_STATUS_RUNNING)
			snprintf(row->duration, lengthof(row->duration), "%.*lfs", 0,
					 difftime(current_time, backup->start_time));
		else if (backup->merge_time != (time_t) 0)
			snprintf(row->duration, lengthof(row->duration), "%.*lfs", 0,
					 difftime(backup->end_time, backup->merge_time));
		else if (backup->end_time != (time_t) 0)
			snprintf(row->duration, lengthof(row->duration), "%.*lfs", 0,
					 difftime(backup->end_time, backup->start_time));
		else
			StrNCpy(row->duration, "----", sizeof(row->duration));
		widths[cur] = Max(widths[cur], strlen(row->duration));
		cur++;

		/* Data */
		pretty_size(backup->data_bytes, row->data_bytes,
					lengthof(row->data_bytes));
		widths[cur] = Max(widths[cur], strlen(row->data_bytes));
		cur++;

		/* Start LSN */
		snprintf(row->start_lsn, lengthof(row->start_lsn), "%X/%X",
				 (uint32) (backup->start_lsn >> 32),
				 (uint32) backup->start_lsn);
		widths[cur] = Max(widths[cur], strlen(row->start_lsn));
		cur++;

		/* Stop LSN */
		snprintf(row->stop_lsn, lengthof(row->stop_lsn), "%X/%X",
				 (uint32) (backup->stop_lsn >> 32),
				 (uint32) backup->stop_lsn);
		widths[cur] = Max(widths[cur], strlen(row->stop_lsn));
		cur++;

		/* Status */
		row->status = status2str(backup->status);
		widths[cur] = Max(widths[cur], strlen(row->status));
	}

	for (i = 0; i < SHOW_FIELDS_COUNT; i++)
		widths_sum += widths[i] + 2 /* two space */;

	if (show_name)
		appendPQExpBuffer(&show_buf, "\nBACKUP INSTANCE '%s'\n", instance_name);

	/*
	 * Print header.
	 */
	for (i = 0; i < widths_sum; i++)
		appendPQExpBufferChar(&show_buf, '=');
	appendPQExpBufferChar(&show_buf, '\n');

	for (i = 0; i < SHOW_FIELDS_COUNT; i++)
	{
		appendPQExpBuffer(&show_buf, field_formats[i], widths[i], names[i]);
	}
	appendPQExpBufferChar(&show_buf, '\n');

	for (i = 0; i < widths_sum; i++)
		appendPQExpBufferChar(&show_buf, '=');
	appendPQExpBufferChar(&show_buf, '\n');

	/*
	 * Print values.
	 */
	for (i = 0; i < parray_num(backup_list); i++)
	{
		ShowBackendRow *row = &rows[i];
		int			cur = 0;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->instance);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->version);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->backup_id);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->recovery_time);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->mode);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->wal_mode);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->tli);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->duration);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->data_bytes);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->start_lsn);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->stop_lsn);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->status);
		cur++;

		appendPQExpBufferChar(&show_buf, '\n');
	}

	pfree(rows);
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

	json_add_value(buf, "instance", instance_name, json_level, true);
	json_add_key(buf, "backups", json_level);

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
					   true);

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

		json_add_key(buf, "compress-level", json_level);
		appendPQExpBuffer(buf, "%d", backup->compress_level);

		json_add_value(buf, "from-replica",
					   backup->from_replica ? "true" : "false", json_level,
					   true);

		json_add_key(buf, "block-size", json_level);
		appendPQExpBuffer(buf, "%u", backup->block_size);

		json_add_key(buf, "xlog-block-size", json_level);
		appendPQExpBuffer(buf, "%u", backup->wal_block_size);

		json_add_key(buf, "checksum-version", json_level);
		appendPQExpBuffer(buf, "%u", backup->checksum_version);

		json_add_value(buf, "program-version", backup->program_version,
					   json_level, true);
		json_add_value(buf, "server-version", backup->server_version,
					   json_level, true);

		json_add_key(buf, "current-tli", json_level);
		appendPQExpBuffer(buf, "%d", backup->tli);

		json_add_key(buf, "parent-tli", json_level);

		/* Only incremental backup can have Parent TLI */
		if (backup->backup_mode == BACKUP_MODE_FULL)
			parent_tli = 0;
		else
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

		json_add_key(buf, "recovery-xid", json_level);
		appendPQExpBuffer(buf, XID_FMT, backup->recovery_xid);

		if (backup->recovery_time > 0)
		{
			time2iso(timestamp, lengthof(timestamp), backup->recovery_time);
			json_add_value(buf, "recovery-time", timestamp, json_level, true);
		}

		if (backup->data_bytes != BYTES_INVALID)
		{
			json_add_key(buf, "data-bytes", json_level);
			appendPQExpBuffer(buf, INT64_FORMAT, backup->data_bytes);
		}

		if (backup->wal_bytes != BYTES_INVALID)
		{
			json_add_key(buf, "wal-bytes", json_level);
			appendPQExpBuffer(buf, INT64_FORMAT, backup->wal_bytes);
		}

		if (backup->primary_conninfo)
			json_add_value(buf, "primary_conninfo", backup->primary_conninfo,
						   json_level, true);

		if (backup->external_dir_str)
			json_add_value(buf, "external-dirs", backup->external_dir_str,
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

typedef struct timelineInfo timelineInfo;

struct timelineInfo {

	TimeLineID tli;
	TimeLineID parent_tli;
	timelineInfo *parent_link;

	XLogRecPtr start_lsn;
	XLogSegNo begin_segno;
	XLogSegNo end_segno;
	int		n_xlog_files;
	size_t	size;
	parray *backups; /* backups belonging to this timeline */
	parray *found_files; /* array of intervals of found files */
	parray *lost_files; /* array of intervals of lost files */
};

typedef struct xlogInterval
{
	XLogSegNo begin_segno;
	XLogSegNo end_segno;
} xlogInterval;

static timelineInfo *
timelineInfoNew(TimeLineID tli)
{
	timelineInfo *tlinfo = (timelineInfo *) pgut_malloc(sizeof(timelineInfo));
	MemSet(tlinfo, 0, sizeof(timelineInfo));
	tlinfo->tli = tli;
	return tlinfo;
}

/*
 * TODO
 * - save info about backup history files
 * - verify that such backup truly exist in the instance
 * - save info about that backup
 *
 * MAYBE
 * - make it independend from probackup file structure
 *   (i.e. no implicit path assumptions)
 */
static void
show_instance_archive()
{
	parray *xlog_files_list = parray_new();
	int n_files = 0;
	parray *timelineinfos;
	timelineInfo *tlinfo;

	elog(INFO, "show_instance_archive");

	dir_list_file(xlog_files_list, arclog_path, false, false, false, 0, FIO_BACKUP_HOST);
	parray_qsort(xlog_files_list, pgFileComparePath);
	n_files = parray_num(xlog_files_list);

	if (n_files == 0)
	{
		elog(INFO, "instance archive is empty");
		return;
	}

	timelineinfos = parray_new();
	tlinfo = NULL;

	/* walk through files and collect info about timelines */
	for (int i = 0; i < n_files; i++)
	{
		pgFile *file = (pgFile *) parray_get(xlog_files_list, i);
		int result = 0;
		TimeLineID tli;
		parray *timelines;
		uint32 log, seg, backup_start_lsn;
		XLogSegNo segno;

		result = sscanf(file->name, "%08X%08X%08X.%08X.backup", &tli, &log, &seg, &backup_start_lsn);
		segno = log*instance_config.xlog_seg_size + seg;

		if (result == 3)
		{
			elog(LOG, "filename is parsed, regular xlog, tli %u segno %lu", tli, segno);

			/* regular file new file belongs to new timeline */
			if (!tlinfo || tlinfo->tli != tli)
			{
				elog(LOG, "filename belongs to new tli");
				tlinfo = timelineInfoNew(tli);
				parray_append(timelineinfos, tlinfo);
			}
			else
			{
				/* check, if segments are consequent */
				XLogSegNo expected_segno = 0;

				/* NOTE order of checks is essential */
				if (tlinfo->end_segno)
					expected_segno = tlinfo->end_segno + 1;
				else if (tlinfo->start_lsn)
					expected_segno = tlinfo->start_lsn / instance_config.xlog_seg_size;
				else
					elog(ERROR, "no expected segno found...");

				if (segno != expected_segno)
				{
					xlogInterval *interval = palloc(sizeof(xlogInterval));;
					interval->begin_segno = expected_segno;
					interval->end_segno = segno-1;
					
					elog(WARNING, "segno %lu found instead of expected segno %lu at tli %u",
						 segno, expected_segno, tli);

					if (tlinfo->lost_files == NULL)
						tlinfo->lost_files = parray_new();
					
					parray_append(tlinfo->lost_files, interval);
				}
			}

			if (tlinfo->begin_segno == 0)
					tlinfo->begin_segno = segno;
			/* this file is the last for this timeline so far */
			tlinfo->end_segno = segno; 
			/* update counters */
			tlinfo->n_xlog_files++;
			tlinfo->size += file->size;
		}
		else if (result == 4)
		{
			elog(LOG, "filename is parsed, backup history xlog, tli %u segno %lu", tli, segno);
			if (tlinfo->tli != tli)
			{
				/* TODO We still need to count this file's size  */
				elog(WARNING, "backup history xlog found, that doesn't have corresponding wal file");
			}
			else
			{
				if (tlinfo->backups == NULL)
					tlinfo->backups = parray_new();

				parray_append(tlinfo->backups, &backup_start_lsn);
			}
		}
		else if (IsTLHistoryFileName(file->name))
		{
			TimeLineHistoryEntry *tln;
			elog(LOG, "filename is parsed timeline history file %s", file->name);

			sscanf(file->name, "%08X.history", &tli);

			timelines = read_timeline_history(tli);

			if (!tlinfo || tlinfo->tli != tli)
			{
				elog(LOG, "filename belongs to new tli");
				tlinfo = timelineInfoNew(tli);
				parray_append(timelineinfos, tlinfo);
				/*
				 * 1 is the latest timeline in the timelines list.
				 * 0 - is our timeline, which is of no interest here
				 */
				tln = (TimeLineHistoryEntry *) parray_get(timelines, 1);
				tlinfo->start_lsn = tln->end;
				tlinfo->parent_tli = tln->tli;

				for (int i = 0; i < parray_num(timelineinfos); i++)
				{
					timelineInfo *cur = (timelineInfo *) parray_get(timelineinfos, i);
					if (cur->tli == tlinfo->parent_tli)
					{
						tlinfo->parent_link = cur;
						break;
					}
				}
			}

			for (int t = 0; t < parray_num(timelines); t++)
			{
				TimeLineHistoryEntry *tln = (TimeLineHistoryEntry *) parray_get(timelines,t);
				elog(LOG, "timeline tli %u, end %X/%X ",
					 tln->tli, (uint32) (tln->end >> 32), (uint32) tln->end);
			}
			parray_walk(timelines, pfree);
			parray_free(timelines);
		}
		else
			elog(LOG, "filename is parsed, NOT regular xlog");
	}

	if (show_format == SHOW_PLAIN)
		show_archive_plain(timelineinfos, true);
	else if (show_format == SHOW_JSON)
		show_archive_json(timelineinfos);
	else
		elog(ERROR, "Invalid show format %d", (int) show_format);
	

	parray_walk(xlog_files_list, pfree);
	parray_free(xlog_files_list);
}

static void
show_archive_plain(parray *tli_list, bool show_name)
{
#define SHOW_ARCHIVE_FIELDS_COUNT 9
	int			i;
	const char *names[SHOW_ARCHIVE_FIELDS_COUNT] =
					{ "Instance", "TLI", "Parent TLI", "Start LSN", "Min Segno", "Max Segno", "N files", "Size", "Status"};
	const char *field_formats[SHOW_ARCHIVE_FIELDS_COUNT] =
					{ " %-*s ", " %-*s ", " %-*s ", " %-*s ", " %-*s ", " %-*s ", " %-*s ", " %-*s ", " %-*s "};
	uint32		widths[SHOW_ARCHIVE_FIELDS_COUNT];
	uint32		widths_sum = 0;
	ShowArchiveRow *rows;

	for (i = 0; i < SHOW_ARCHIVE_FIELDS_COUNT; i++)
		widths[i] = strlen(names[i]);

	rows = (ShowArchiveRow *) palloc(parray_num(tli_list) *
									 sizeof(ShowArchiveRow));

	/*
	 * Fill row values and calculate maximum width of each field.
	 */
	for (i = 0; i < parray_num(tli_list); i++)
	{
		timelineInfo *tlinfo = (timelineInfo *) parray_get(tli_list, i);
		ShowArchiveRow *row = &rows[i];
		int			cur = 0;

		/* Instance */
		row->instance = instance_name;
		widths[cur] = Max(widths[cur], strlen(row->instance));
		cur++;

		/* TLI */
		snprintf(row->tli, lengthof(row->tli), "%u",
				 tlinfo->tli);
		widths[cur] = Max(widths[cur], strlen(row->tli));
		cur++;

		/* Parent TLI */
		snprintf(row->parent_tli, lengthof(row->parent_tli), "%u",
				 tlinfo->parent_tli);
		widths[cur] = Max(widths[cur], strlen(row->parent_tli));
		cur++;

		/* Start LSN */
		snprintf(row->start_lsn, lengthof(row->start_lsn), "%X/%X",
				 (uint32) (tlinfo->start_lsn >> 32),
				 (uint32) tlinfo->start_lsn);
		widths[cur] = Max(widths[cur], strlen(row->start_lsn));
		cur++;

		/* Min Segno */
		snprintf(row->min_segno, lengthof(row->min_segno), "%08X%08X",
				 (uint32) tlinfo->begin_segno / instance_config.xlog_seg_size,
				 (uint32) tlinfo->begin_segno % instance_config.xlog_seg_size);
		widths[cur] = Max(widths[cur], strlen(row->min_segno));
		cur++;

		/* Max Segno */
		snprintf(row->max_segno, lengthof(row->max_segno), "%08X%08X",
				 (uint32) tlinfo->end_segno / instance_config.xlog_seg_size,
				 (uint32) tlinfo->end_segno % instance_config.xlog_seg_size);
		widths[cur] = Max(widths[cur], strlen(row->max_segno));
		cur++;
		/* N files */
		snprintf(row->n_files, lengthof(row->n_files), "%u",
				 tlinfo->n_xlog_files);
		widths[cur] = Max(widths[cur], strlen(row->n_files));
		cur++;

		/* Size */
		pretty_size(tlinfo->size, row->size,
					lengthof(row->size));
		widths[cur] = Max(widths[cur], strlen(row->size));
		cur++;

		/* Status */
		if (tlinfo->lost_files == NULL)
			row->status = status2str(BACKUP_STATUS_OK);
		else
			row->status = status2str(BACKUP_STATUS_CORRUPT);

		widths[cur] = Max(widths[cur], strlen(row->status));

	}

	for (i = 0; i < SHOW_ARCHIVE_FIELDS_COUNT; i++)
		widths_sum += widths[i] + 2 /* two space */;

	if (show_name)
		appendPQExpBuffer(&show_buf, "\nARCHIVE INSTANCE '%s'\n", instance_name);

	/*
	 * Print header.
	 */
	for (i = 0; i < widths_sum; i++)
		appendPQExpBufferChar(&show_buf, '=');
	appendPQExpBufferChar(&show_buf, '\n');

	for (i = 0; i < SHOW_ARCHIVE_FIELDS_COUNT; i++)
	{
		appendPQExpBuffer(&show_buf, field_formats[i], widths[i], names[i]);
	}
	appendPQExpBufferChar(&show_buf, '\n');

	for (i = 0; i < widths_sum; i++)
		appendPQExpBufferChar(&show_buf, '=');
	appendPQExpBufferChar(&show_buf, '\n');

	/*
	 * Print values.
	 */
	for (i = 0; i < parray_num(tli_list); i++)
	{
		ShowArchiveRow *row = &rows[i];
		int			cur = 0;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->instance);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->tli);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->parent_tli);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->start_lsn);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->min_segno);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->max_segno);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->n_files);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->size);
		cur++;
		
		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->status);
		cur++;
		appendPQExpBufferChar(&show_buf, '\n');
	}

	pfree(rows);
}

static void
show_archive_json(parray *tli_list)
{
	int			i;
	PQExpBuffer	buf = &show_buf;

	if (!first_instance)
		appendPQExpBufferChar(buf, ',');

	/* Begin of instance object */
	json_add(buf, JT_BEGIN_OBJECT, &json_level);

	json_add_value(buf, "instance", instance_name, json_level, true);
	json_add_key(buf, "timelines", json_level);

	/*
	 * List timelines.
	 */
	json_add(buf, JT_BEGIN_ARRAY, &json_level);

	for (i = 0; i < parray_num(tli_list); i++)
	{
		timelineInfo  *tlinfo = (timelineInfo  *) parray_get(tli_list, i);
		char		tmp_buf[20];

		if (i != 0)
			appendPQExpBufferChar(buf, ',');

		json_add(buf, JT_BEGIN_OBJECT, &json_level);

		json_add_key(buf, "tli", json_level);
		appendPQExpBuffer(buf, "%u", tlinfo->tli);

		json_add_key(buf, "parent-tli", json_level);
		appendPQExpBuffer(buf, "%u", tlinfo->parent_tli);
		
		snprintf(tmp_buf, lengthof(tmp_buf), "%X/%X",
				 (uint32) (tlinfo->start_lsn >> 32), (uint32) tlinfo->start_lsn);
		json_add_value(buf, "start-lsn", tmp_buf, json_level, true);

		snprintf(tmp_buf, lengthof(tmp_buf), "%08X%08X", 
				 (uint32) tlinfo->begin_segno / instance_config.xlog_seg_size,
				 (uint32) tlinfo->begin_segno % instance_config.xlog_seg_size);		
		json_add_value(buf, "min-segno", tmp_buf, json_level, true);

		snprintf(tmp_buf, lengthof(tmp_buf), "%08X%08X", 
				 (uint32) tlinfo->end_segno / instance_config.xlog_seg_size,
				 (uint32) tlinfo->end_segno % instance_config.xlog_seg_size);
		json_add_value(buf, "max-segno", tmp_buf, json_level, true);

		if (tlinfo->lost_files != NULL)
		{
			json_add_key(buf, "lost_files", json_level);
			json_add(buf, JT_BEGIN_ARRAY, &json_level);

			for (int j = 0; j < parray_num(tlinfo->lost_files); j++)
			{
				xlogInterval *lost_files = (xlogInterval *) parray_get(tlinfo->lost_files, j);

				if (j != 0)
					appendPQExpBufferChar(buf, ',');
		
				json_add(buf, JT_BEGIN_OBJECT, &json_level);

				snprintf(tmp_buf, lengthof(tmp_buf), "%08X%08X", 
				 (uint32) lost_files->begin_segno / instance_config.xlog_seg_size,
				 (uint32) lost_files->begin_segno % instance_config.xlog_seg_size);
				json_add_value(buf, "begin-segno", tmp_buf, json_level, true);

				snprintf(tmp_buf, lengthof(tmp_buf), "%08X%08X", 
				 (uint32) lost_files->end_segno / instance_config.xlog_seg_size,
				 (uint32) lost_files->end_segno % instance_config.xlog_seg_size);
				json_add_value(buf, "end-segno", tmp_buf, json_level, true);
				json_add(buf, JT_END_OBJECT, &json_level);
			}

			json_add(buf, JT_END_ARRAY, &json_level);
		}
		
		if (tlinfo->backups != NULL)
		{
			json_add_key(buf, "backups", json_level);
			json_add(buf, JT_BEGIN_ARRAY, &json_level);
			for (int j = 0; j < parray_num(tlinfo->backups); j++)
			{
				uint32 *bckp = parray_get(tlinfo->backups, j);

				if (j != 0)
					appendPQExpBufferChar(buf, ',');
		
				snprintf(tmp_buf, lengthof(tmp_buf), "%08X",  *bckp);
				json_add_value(buf, "start_lsn", tmp_buf, json_level, true);
			}

			json_add(buf, JT_END_ARRAY, &json_level);

		}

		json_add_key(buf, "n-files", json_level);
		appendPQExpBuffer(buf, "%d", tlinfo->n_xlog_files);

		json_add_key(buf, "size", json_level);
		appendPQExpBuffer(buf, "%lu", tlinfo->size);

		if (tlinfo->lost_files == NULL)
			json_add_value(buf, "status", status2str(BACKUP_STATUS_OK), json_level,
					   true);
		else
			json_add_value(buf, "status", status2str(BACKUP_STATUS_CORRUPT), json_level,
					   true);

		json_add(buf, JT_END_OBJECT, &json_level);
	}

	/* End of backups */
	json_add(buf, JT_END_ARRAY, &json_level);

	/* End of instance object */
	json_add(buf, JT_END_OBJECT, &json_level);

	first_instance = false;
}
