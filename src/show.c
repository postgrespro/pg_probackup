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

/* struct to align fields printed in plain format */
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

/* struct to align fields printed in plain format */
typedef struct ShowArchiveRow
{
	char		tli[20];
	char		parent_tli[20];
	char		switchpoint[20];
	char		min_segno[20];
	char		max_segno[20];
	char		n_segments[20];
	char		size[20];
	char		zratio[20];
	const char *status;
	char		n_backups[20];
} ShowArchiveRow;

static void show_instance_start(void);
static void show_instance_end(void);
static void show_instance(const char *instance_name, time_t requested_backup_id, bool show_name);
static void print_backup_json_object(PQExpBuffer buf, pgBackup *backup);
static int show_backup(const char *instance_name, time_t requested_backup_id);

static void show_instance_plain(const char *instance_name, parray *backup_list, bool show_name);
static void show_instance_json(const char *instance_name, parray *backup_list);

static void show_instance_archive(InstanceConfig *instance);
static void show_archive_plain(const char *instance_name, uint32 xlog_seg_size,
							   parray *timelines_list, bool show_name);
static void show_archive_json(const char *instance_name, uint32 xlog_seg_size,
							  parray *tli_list);

static pgBackup* get_closest_backup(timelineInfo *tlinfo, parray *backup_list);

static PQExpBufferData show_buf;
static bool first_instance = true;
static int32 json_level = 0;

/*
 * Entry point of pg_probackup SHOW subcommand.
 */
int
do_show(const char *instance_name, time_t requested_backup_id, bool show_archive)
{
	if (instance_name == NULL &&
		requested_backup_id != INVALID_BACKUP_ID)
		elog(ERROR, "You must specify --instance to use --backup_id option");

	/*
	 * if instance_name is not specified,
	 * show information about all instances in this backup catalog
	 */
	if (instance_name == NULL)
	{
		parray *instances = catalog_get_instance_list();

		show_instance_start();
		for (int i = 0; i < parray_num(instances); i++)
		{
			InstanceConfig *instance = parray_get(instances, i);
			char backup_instance_path[MAXPGPATH];

			sprintf(backup_instance_path, "%s/%s/%s", backup_path, BACKUPS_DIR, instance->name);

			if (show_archive)
				show_instance_archive(instance);
			else
				show_instance(instance->name, INVALID_BACKUP_ID, true);
		}
		show_instance_end();

		return 0;
	}
	/* always use  */
	else if (show_format == SHOW_JSON ||
			 requested_backup_id == INVALID_BACKUP_ID)
	{
		show_instance_start();

		if (show_archive)
		{
			InstanceConfig *instance = readInstanceConfigFile(instance_name);
			show_instance_archive(instance);
		}
		else
			show_instance(instance_name, requested_backup_id, false);

		show_instance_end();

		return 0;
	}
	else
	{
		if (show_archive)
		{
			InstanceConfig *instance = readInstanceConfigFile(instance_name);
			show_instance_archive(instance);
		}
		else
			show_backup(instance_name, requested_backup_id);

		return 0;
	}
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
show_instance(const char *instance_name, time_t requested_backup_id, bool show_name)
{
	parray	   *backup_list;

	backup_list = catalog_get_backup_list(instance_name, requested_backup_id);

	if (show_format == SHOW_PLAIN)
		show_instance_plain(instance_name, backup_list, show_name);
	else if (show_format == SHOW_JSON)
		show_instance_json(instance_name, backup_list);
	else
		elog(ERROR, "Invalid show format %d", (int) show_format);

	/* cleanup */
	parray_walk(backup_list, pgBackupFree);
	parray_free(backup_list);
}

/* helper routine to print backup info as json object */
static void
print_backup_json_object(PQExpBuffer buf, pgBackup *backup)
{
	TimeLineID	parent_tli;
	char		timestamp[100] = "----";
	char		lsn[20];

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
	else if (backup->parent_backup_link)
		parent_tli = backup->parent_backup_link->tli;
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

/*
 * Show detailed meta information about specified backup.
 */
static int
show_backup(const char *instance_name, time_t requested_backup_id)
{
	pgBackup   *backup;

	backup = read_backup(instance_name, requested_backup_id);
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
 * Show instance backups in plain format.
 */
static void
show_instance_plain(const char *instance_name, parray *backup_list, bool show_name)
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
	TimeLineID parent_tli = 0;

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

		if (backup->parent_backup_link != NULL)
			parent_tli = backup->parent_backup_link->tli;

		snprintf(row->tli, lengthof(row->tli), "%u / %u",
				 backup->tli,
				 backup->backup_mode == BACKUP_MODE_FULL ? 0 : parent_tli);
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
 * Show instance backups in json format.
 */
static void
show_instance_json(const char *instance_name, parray *backup_list)
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

		if (i != 0)
			appendPQExpBufferChar(buf, ',');

		print_backup_json_object(buf, backup);
	}

	/* End of backups */
	json_add(buf, JT_END_ARRAY, &json_level);

	/* End of instance object */
	json_add(buf, JT_END_OBJECT, &json_level);

	first_instance = false;
}

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
	tlinfo->switchpoint = InvalidXLogRecPtr;
	tlinfo->parent_link = NULL;
	return tlinfo;
}

/*
 * show information about WAL archive of the instance
 */
static void
show_instance_archive(InstanceConfig *instance)
{
	parray *xlog_files_list = parray_new();
	parray *timelineinfos;
	parray *backups;
	timelineInfo *tlinfo;
	char		arclog_path[MAXPGPATH];

	/* read all xlog files that belong to this archive */
	sprintf(arclog_path, "%s/%s/%s", backup_path, "wal", instance->name);
	dir_list_file(xlog_files_list, arclog_path, false, false, false, 0, FIO_BACKUP_HOST);
	parray_qsort(xlog_files_list, pgFileComparePath);

	timelineinfos = parray_new();
	tlinfo = NULL;

	/* walk through files and collect info about timelines */
	for (int i = 0; i < parray_num(xlog_files_list); i++)
	{
		pgFile *file = (pgFile *) parray_get(xlog_files_list, i);
		TimeLineID tli;
		parray *timelines;

		/* regular WAL file */
		if (strspn(file->name, "0123456789ABCDEF") == XLOG_FNAME_LEN)
		{
			int result = 0;
			uint32 log, seg;
			XLogSegNo segno;
			char suffix[MAXPGPATH];

			result = sscanf(file->name, "%08X%08X%08X.%s",
						&tli, &log, &seg, (char *) &suffix);

			if (result < 3)
			{
				elog(WARNING, "unexpected WAL file name \"%s\"", file->name);
				continue;
			}

			segno = log * instance->xlog_seg_size + seg;

			/* regular WAL file with suffix */
			if (result == 4)
			{
				/* backup history file. Currently we don't use them */
				if (IsBackupHistoryFileName(file->name))
				{
					elog(VERBOSE, "backup history file \"%s\". do nothing", file->name);
					continue;
				}
				/* we only expect compressed wal files with .gz suffix */
				else if (strcmp(suffix, "gz") != 0)
				{
					elog(WARNING, "unexpected WAL file name \"%s\"", file->name);
					continue;
				}
			}

			/* new file belongs to new timeline */
			if (!tlinfo || tlinfo->tli != tli)
			{
				tlinfo = timelineInfoNew(tli);
				parray_append(timelineinfos, tlinfo);
			}
			/*
			 * As it is impossible to detect if segments before segno are lost,
			 * or just do not exist, do not report them as lost.
			 */
			else if (tlinfo->n_xlog_files != 0)
			{
				/* check, if segments are consequent */
				XLogSegNo expected_segno = tlinfo->end_segno + 1;

				/* some segments are missing. remember them in lost_segments to report */
				if (segno != expected_segno)
				{
					xlogInterval *interval = palloc(sizeof(xlogInterval));;
					interval->begin_segno = expected_segno;
					interval->end_segno = segno - 1;

					if (tlinfo->lost_segments == NULL)
						tlinfo->lost_segments = parray_new();

					parray_append(tlinfo->lost_segments, interval);
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
		/* timeline history file */
		else if (IsTLHistoryFileName(file->name))
		{
			TimeLineHistoryEntry *tln;

			sscanf(file->name, "%08X.history", &tli);
			timelines = read_timeline_history(arclog_path, tli);

			if (!tlinfo || tlinfo->tli != tli)
			{
				tlinfo = timelineInfoNew(tli);
				parray_append(timelineinfos, tlinfo);
				/*
				 * 1 is the latest timeline in the timelines list.
				 * 0 - is our timeline, which is of no interest here
				 */
				tln = (TimeLineHistoryEntry *) parray_get(timelines, 1);
				tlinfo->switchpoint = tln->end;
				tlinfo->parent_tli = tln->tli;

				/* find parent timeline to link it with this one */
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

			parray_walk(timelines, pfree);
			parray_free(timelines);
		}
		else
			elog(WARNING, "unexpected WAL file name \"%s\"", file->name);
	}

	/* save information about backups belonging to each timeline */
	backups = catalog_get_backup_list(instance->name, INVALID_BACKUP_ID);

	for (int i = 0; i < parray_num(timelineinfos); i++)
	{
		timelineInfo *tlinfo = parray_get(timelineinfos, i);
		for (int j = 0; j < parray_num(backups); j++)
		{
			pgBackup *backup = parray_get(backups, j);
			if (tlinfo->tli == backup->tli)
			{
				if (tlinfo->backups == NULL)
					tlinfo->backups = parray_new();

				parray_append(tlinfo->backups, backup);
			}
		}
	}

	/* determine closest backup for every timeline */
	for (int i = 0; i < parray_num(timelineinfos); i++)
	{
		timelineInfo *tlinfo = parray_get(timelineinfos, i);

		/* only timeline with switchpoint can possibly have closest backup */
		if XLogRecPtrIsInvalid(tlinfo->switchpoint)
			continue;

		/* only timeline with parent timeline can possibly have closest backup */
		if (!tlinfo->parent_link)
			continue;

		tlinfo->closest_backup = get_closest_backup(tlinfo, backups);
	}

	if (show_format == SHOW_PLAIN)
		show_archive_plain(instance->name, instance->xlog_seg_size, timelineinfos, true);
	else if (show_format == SHOW_JSON)
		show_archive_json(instance->name, instance->xlog_seg_size, timelineinfos);
	else
		elog(ERROR, "Invalid show format %d", (int) show_format);

	parray_walk(xlog_files_list, pfree);
	parray_free(xlog_files_list);
}

static void
show_archive_plain(const char *instance_name, uint32 xlog_seg_size,
				   parray *tli_list, bool show_name)
{
#define SHOW_ARCHIVE_FIELDS_COUNT 10
	int			i;
	const char *names[SHOW_ARCHIVE_FIELDS_COUNT] =
					{ "TLI", "Parent TLI", "Switchpoint",
					  "Min Segno", "Max Segno", "N segments", "Size", "Zratio", "N backups", "Status"};
	const char *field_formats[SHOW_ARCHIVE_FIELDS_COUNT] =
					{ " %-*s ", " %-*s ", " %-*s ", " %-*s ",
					  " %-*s ", " %-*s ", " %-*s ", " %-*s ", " %-*s ", " %-*s "};
	uint32		widths[SHOW_ARCHIVE_FIELDS_COUNT];
	uint32		widths_sum = 0;
	ShowArchiveRow *rows;

	for (i = 0; i < SHOW_ARCHIVE_FIELDS_COUNT; i++)
		widths[i] = strlen(names[i]);

	rows = (ShowArchiveRow *) palloc0(parray_num(tli_list) *
									 sizeof(ShowArchiveRow));

	/*
	 * Fill row values and calculate maximum width of each field.
	 */
	for (i = 0; i < parray_num(tli_list); i++)
	{
		timelineInfo *tlinfo = (timelineInfo *) parray_get(tli_list, i);
		ShowArchiveRow *row = &rows[i];
		int			cur = 0;
		float		zratio = 0;

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

		/* Switchpoint LSN */
		snprintf(row->switchpoint, lengthof(row->switchpoint), "%X/%X",
				 (uint32) (tlinfo->switchpoint >> 32),
				 (uint32) tlinfo->switchpoint);
		widths[cur] = Max(widths[cur], strlen(row->switchpoint));
		cur++;

		/* Min Segno */
		snprintf(row->min_segno, lengthof(row->min_segno), "%08X%08X",
				 (uint32) tlinfo->begin_segno / xlog_seg_size,
				 (uint32) tlinfo->begin_segno % xlog_seg_size);
		widths[cur] = Max(widths[cur], strlen(row->min_segno));
		cur++;

		/* Max Segno */
		snprintf(row->max_segno, lengthof(row->max_segno), "%08X%08X",
				 (uint32) tlinfo->end_segno / xlog_seg_size,
				 (uint32) tlinfo->end_segno % xlog_seg_size);
		widths[cur] = Max(widths[cur], strlen(row->max_segno));
		cur++;

		/* N files */
		snprintf(row->n_segments, lengthof(row->n_segments), "%u",
				 tlinfo->n_xlog_files);
		widths[cur] = Max(widths[cur], strlen(row->n_segments));
		cur++;

		/* Size */
		pretty_size(tlinfo->size, row->size,
					lengthof(row->size));
		widths[cur] = Max(widths[cur], strlen(row->size));
		cur++;

		/* Zratio (compression ratio) */
		if (tlinfo->size != 0)
			zratio = (float) ((xlog_seg_size*tlinfo->n_xlog_files)/tlinfo->size);

		snprintf(row->zratio, lengthof(row->n_segments), "%.2f", zratio);
		widths[cur] = Max(widths[cur], strlen(row->zratio));
		cur++;

		/* N backups */
		snprintf(row->n_backups, lengthof(row->n_backups), "%lu",
				 tlinfo->backups?parray_num(tlinfo->backups):0);
		widths[cur] = Max(widths[cur], strlen(row->n_backups));
		cur++;

		/* Status */
		if (tlinfo->lost_segments == NULL)
			row->status = "OK";
		else
			row->status = "DEGRADED";
		widths[cur] = Max(widths[cur], strlen(row->status));
		cur++;
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
	for (i = parray_num(tli_list) - 1; i >= 0; i--)
	{
		ShowArchiveRow *row = &rows[i];
		int			cur = 0;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->tli);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->parent_tli);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->switchpoint);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->min_segno);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->max_segno);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->n_segments);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->size);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->zratio);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->n_backups);
		cur++;

		appendPQExpBuffer(&show_buf, field_formats[cur], widths[cur],
						  row->status);
		cur++;
		appendPQExpBufferChar(&show_buf, '\n');
	}

	pfree(rows);
}

static void
show_archive_json(const char *instance_name, uint32 xlog_seg_size,
				  parray *tli_list)
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

	for (i = parray_num(tli_list) - 1; i >= 0; i--)
	{
		timelineInfo  *tlinfo = (timelineInfo  *) parray_get(tli_list, i);
		char		tmp_buf[20];
		float		zratio = 0;

		if (i != (parray_num(tli_list) - 1))
			appendPQExpBufferChar(buf, ',');

		json_add(buf, JT_BEGIN_OBJECT, &json_level);

		json_add_key(buf, "tli", json_level);
		appendPQExpBuffer(buf, "%u", tlinfo->tli);

		json_add_key(buf, "parent-tli", json_level);
		appendPQExpBuffer(buf, "%u", tlinfo->parent_tli);

		snprintf(tmp_buf, lengthof(tmp_buf), "%X/%X",
				 (uint32) (tlinfo->switchpoint >> 32), (uint32) tlinfo->switchpoint);
		json_add_value(buf, "switchpoint", tmp_buf, json_level, true);

		snprintf(tmp_buf, lengthof(tmp_buf), "%08X%08X",
				 (uint32) tlinfo->begin_segno / xlog_seg_size,
				 (uint32) tlinfo->begin_segno % xlog_seg_size);
		json_add_value(buf, "min-segno", tmp_buf, json_level, true);

		snprintf(tmp_buf, lengthof(tmp_buf), "%08X%08X",
				 (uint32) tlinfo->end_segno / xlog_seg_size,
				 (uint32) tlinfo->end_segno % xlog_seg_size);
		json_add_value(buf, "max-segno", tmp_buf, json_level, true);

		json_add_key(buf, "n-segments", json_level);
		appendPQExpBuffer(buf, "%d", tlinfo->n_xlog_files);

		json_add_key(buf, "size", json_level);
		appendPQExpBuffer(buf, "%lu", tlinfo->size);

		json_add_key(buf, "zratio", json_level);
		if (tlinfo->size != 0)
			zratio = (float) ((xlog_seg_size*tlinfo->n_xlog_files)/tlinfo->size);
		appendPQExpBuffer(buf, "%.2f", zratio);

		if (tlinfo->closest_backup != NULL)
			snprintf(tmp_buf, lengthof(tmp_buf), "%s",
						base36enc(tlinfo->closest_backup->start_time));
		else
			snprintf(tmp_buf, lengthof(tmp_buf), "%s", "");

		json_add_value(buf, "closest-backup-id", tmp_buf, json_level, true);

		if (tlinfo->lost_segments == NULL)
			json_add_value(buf, "status", "OK", json_level, true);
		else
			json_add_value(buf, "status", "DEGRADED", json_level, true);

		json_add_key(buf, "lost-segments", json_level);

		if (tlinfo->lost_segments != NULL)
		{
			json_add(buf, JT_BEGIN_ARRAY, &json_level);

			for (int j = 0; j < parray_num(tlinfo->lost_segments); j++)
			{
				xlogInterval *lost_segments = (xlogInterval *) parray_get(tlinfo->lost_segments, j);

				if (j != 0)
					appendPQExpBufferChar(buf, ',');

				json_add(buf, JT_BEGIN_OBJECT, &json_level);

				snprintf(tmp_buf, lengthof(tmp_buf), "%08X%08X",
				 (uint32) lost_segments->begin_segno / xlog_seg_size,
				 (uint32) lost_segments->begin_segno % xlog_seg_size);
				json_add_value(buf, "begin-segno", tmp_buf, json_level, true);

				snprintf(tmp_buf, lengthof(tmp_buf), "%08X%08X",
				 (uint32) lost_segments->end_segno / xlog_seg_size,
				 (uint32) lost_segments->end_segno % xlog_seg_size);
				json_add_value(buf, "end-segno", tmp_buf, json_level, true);
				json_add(buf, JT_END_OBJECT, &json_level);
			}
			json_add(buf, JT_END_ARRAY, &json_level);
		}
		else
			appendPQExpBuffer(buf, "[]");

		json_add_key(buf, "backups", json_level);

		if (tlinfo->backups != NULL)
		{
			json_add(buf, JT_BEGIN_ARRAY, &json_level);
			for (int j = 0; j < parray_num(tlinfo->backups); j++)
			{
				pgBackup *backup = parray_get(tlinfo->backups, j);

				if (j != 0)
					appendPQExpBufferChar(buf, ',');

				print_backup_json_object(buf, backup);
			}

			json_add(buf, JT_END_ARRAY, &json_level);
		}
		else
			appendPQExpBuffer(buf, "[]");

		/* End of timeline */
		json_add(buf, JT_END_OBJECT, &json_level);
	}

	/* End of timelines object */
	json_add(buf, JT_END_ARRAY, &json_level);

	/* End of instance object */
	json_add(buf, JT_END_OBJECT, &json_level);

	first_instance = false;
}

/*
 * Iterate over parent timelines of a given timeline and look
 * for valid backup closest to given timeline switchpoint.
 *
 * Returns NULL if such backup is not found.
 */
pgBackup*
get_closest_backup(timelineInfo *tlinfo, parray *backup_list)
{
	pgBackup *closest_backup = NULL;
	int i;

	while (tlinfo->parent_link)
	{
		/*
		 * Iterate over backups belonging to parent timeline and look
		 * for candidates.
		 */
		for (i = 0; i < parray_num(backup_list); i++)
		{
			pgBackup   *backup = parray_get(backup_list, i);

			/* Backups belonging to timelines other than parent timeline can be safely skipped */
			if (backup->tli != tlinfo->parent_tli)
				continue;

			/* Backups in future can be safely skipped */
			if (backup->stop_lsn > tlinfo->switchpoint)
				continue;

			/* Only valid backups closest to switchpoint should be considered */
			if (backup->stop_lsn <= tlinfo->switchpoint &&
				(backup->status == BACKUP_STATUS_OK ||
				 backup->status == BACKUP_STATUS_DONE))
			{
				/*
				 * We have found first candidate.
				 * Now we should determine whether it`s closest to switchpoint or nor.
				 */

				if (!closest_backup)
					closest_backup = backup;

				/* Check if backup is closer to switchpoint than current candidate */
				if (backup->stop_lsn > closest_backup->stop_lsn)
					closest_backup = backup;
			}
		}

		/* Closest backup is found */
		if (closest_backup)
			break;

		/* Switch to parent */
		tlinfo = tlinfo->parent_link;
	}

	return closest_backup;
}
