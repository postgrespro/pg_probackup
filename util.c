/*-------------------------------------------------------------------------
 *
 * util.c: log messages to log file or stderr, and misc code.
 *
 * Portions Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <time.h>

#include "storage/bufpage.h"

char *
base36enc(long unsigned int value)
{
	char		base36[36] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	/* log(2**64) / log(36) = 12.38 => max 13 char + '\0' */
	char		buffer[14];
	unsigned int offset = sizeof(buffer);

	buffer[--offset] = '\0';
	do {
		buffer[--offset] = base36[value % 36];
	} while (value /= 36);

	return strdup(&buffer[offset]); // warning: this must be free-d by the user
}

long unsigned int
base36dec(const char *text)
{
	return strtoul(text, NULL, 36);
}

static void
checkControlFile(ControlFileData *ControlFile)
{
	pg_crc32c   crc;

	/* Calculate CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *) ControlFile, offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);

	/* Then compare it */
	if (!EQ_CRC32C(crc, ControlFile->crc))
		elog(ERROR, "Calculated CRC checksum does not match value stored in file.\n"
			 "Either the file is corrupt, or it has a different layout than this program\n"
			 "is expecting. The results below are untrustworthy.");

	if (ControlFile->pg_control_version % 65536 == 0 && ControlFile->pg_control_version / 65536 != 0)
		elog(ERROR, "possible byte ordering mismatch\n"
			 "The byte ordering used to store the pg_control file might not match the one\n"
			 "used by this program. In that case the results below would be incorrect, and\n"
			 "the PostgreSQL installation would be incompatible with this data directory.");
}

/*
 * Verify control file contents in the buffer src, and copy it to *ControlFile.
 */
static void
digestControlFile(ControlFileData *ControlFile, char *src, size_t size)
{
	if (size != PG_CONTROL_SIZE)
		elog(ERROR, "unexpected control file size %d, expected %d",
			 (int) size, PG_CONTROL_SIZE);

	memcpy(ControlFile, src, sizeof(ControlFileData));

	/* Additional checks on control file */
	checkControlFile(ControlFile);
}

/* TODO Add comment */
XLogRecPtr
get_last_ptrack_lsn(void)
{
	char		*buffer;
	size_t		size;
	XLogRecPtr	lsn;

	buffer = slurpFile(pgdata, "global/ptrack_control", &size, false);
	if (buffer == NULL)
		return 0;

	lsn = *(XLogRecPtr *)buffer;
	return lsn;
}

/*
 * Utility shared by backup and restore to fetch the current timeline
 * used by a node.
 */
TimeLineID
get_current_timeline(bool safe)
{
	ControlFileData ControlFile;
	char       *buffer;
	size_t      size;

	/* First fetch file... */
	buffer = slurpFile(pgdata, "global/pg_control", &size, safe);
	if (safe && buffer == NULL)
		return 0;

	digestControlFile(&ControlFile, buffer, size);
	pg_free(buffer);

	return ControlFile.checkPointCopy.ThisTimeLineID;
}

uint64
get_system_identifier(char *pgdata_path)
{
	ControlFileData ControlFile;
	char	   *buffer;
	size_t		size;

	/* First fetch file... */
	buffer = slurpFile(pgdata_path, "global/pg_control", &size, false);
	if (buffer == NULL)
		return 0;
	digestControlFile(&ControlFile, buffer, size);
	pg_free(buffer);

	return ControlFile.system_identifier;
}

uint32
get_data_checksum_version(bool safe)
{
	ControlFileData ControlFile;
	char       *buffer;
	size_t      size;

	/* First fetch file... */
	buffer = slurpFile(pgdata, "global/pg_control", &size, safe);
	if (buffer == NULL)
		return 0;
	digestControlFile(&ControlFile, buffer, size);
	pg_free(buffer);

	return ControlFile.data_checksum_version;
}


/*
 * Convert time_t value to ISO-8601 format string
 */
void
time2iso(char *buf, size_t len, time_t time)
{
	struct tm *tm = localtime(&time);

	strftime(buf, len, "%Y-%m-%d %H:%M:%S", tm);
}

/* copied from timestamp.c */
pg_time_t
timestamptz_to_time_t(TimestampTz t)
{
	pg_time_t	result;

#ifdef HAVE_INT64_TIMESTAMP
	result = (pg_time_t) (t / USECS_PER_SEC +
				 ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
#else
	result = (pg_time_t) (t +
				 ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
#endif
	return result;
}

const char *
status2str(BackupStatus status)
{
	static const char *statusName[] =
	{
		"UNKNOWN",
		"OK",
		"RUNNING",
		"ERROR",
		"DELETING",
		"DELETED",
		"DONE",
		"CORRUPT"
	};

	if (status < BACKUP_STATUS_INVALID || BACKUP_STATUS_CORRUPT < status)
		return "UNKNOWN";

	return statusName[status];
}

void
remove_trailing_space(char *buf, int comment_mark)
{
	int		i;
	char   *last_char = NULL;

	for (i = 0; buf[i]; i++)
	{
		if (buf[i] == comment_mark || buf[i] == '\n' || buf[i] == '\r')
		{
			buf[i] = '\0';
			break;
		}
	}
	for (i = 0; buf[i]; i++)
	{
		if (!isspace(buf[i]))
			last_char = buf + i;
	}
	if (last_char != NULL)
		*(last_char + 1) = '\0';

}

void
remove_not_digit(char *buf, size_t len, const char *str)
{
	int i, j;

	for (i = 0, j = 0; str[i] && j < len; i++)
	{
		if (!isdigit(str[i]))
			continue;
		buf[j++] = str[i];
	}
	buf[j] = '\0';
}

/* Fill pgBackup struct with default values */
void
pgBackup_init(pgBackup *backup)
{
	backup->backup_id = INVALID_BACKUP_ID;
	backup->backup_mode = BACKUP_MODE_INVALID;
	backup->status = BACKUP_STATUS_INVALID;
	backup->tli = 0;
	backup->start_lsn = 0;
	backup->stop_lsn = 0;
	backup->start_time = (time_t) 0;
	backup->end_time = (time_t) 0;
	backup->recovery_xid = 0;
	backup->recovery_time = (time_t) 0;
	backup->data_bytes = BYTES_INVALID;
	backup->block_size = BLCKSZ;
	backup->wal_block_size = XLOG_BLCKSZ;
	backup->stream = false;
	backup->parent_backup = 0;
}
