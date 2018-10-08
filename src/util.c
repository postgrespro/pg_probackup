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

const char *
base36enc(long unsigned int value)
{
	const char	base36[36] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	/* log(2**64) / log(36) = 12.38 => max 13 char + '\0' */
	static char	buffer[14];
	unsigned int offset = sizeof(buffer);

	buffer[--offset] = '\0';
	do {
		buffer[--offset] = base36[value % 36];
	} while (value /= 36);

	return &buffer[offset];
}

/*
 * Same as base36enc(), but the result must be released by the user.
 */
char *
base36enc_dup(long unsigned int value)
{
	const char	base36[36] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	/* log(2**64) / log(36) = 12.38 => max 13 char + '\0' */
	char		buffer[14];
	unsigned int offset = sizeof(buffer);

	buffer[--offset] = '\0';
	do {
		buffer[--offset] = base36[value % 36];
	} while (value /= 36);

	return strdup(&buffer[offset]);
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
#if PG_VERSION_NUM >= 100000
	int			ControlFileSize = PG_CONTROL_FILE_SIZE;
#else
	int			ControlFileSize = PG_CONTROL_SIZE;
#endif

	if (size != ControlFileSize)
		elog(ERROR, "unexpected control file size %d, expected %d",
			 (int) size, ControlFileSize);

	memcpy(ControlFile, src, sizeof(ControlFileData));

	/* Additional checks on control file */
	checkControlFile(ControlFile);
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

/*
 * Get last check point record ptr from pg_tonrol.
 */
XLogRecPtr
get_checkpoint_location(PGconn *conn)
{
#if PG_VERSION_NUM >= 90600
	PGresult   *res;
	uint32		lsn_hi;
	uint32		lsn_lo;
	XLogRecPtr	lsn;

#if PG_VERSION_NUM >= 100000
	res = pgut_execute(conn,
					   "SELECT checkpoint_lsn FROM pg_catalog.pg_control_checkpoint()",
					   0, NULL);
#else
	res = pgut_execute(conn,
					   "SELECT checkpoint_location FROM pg_catalog.pg_control_checkpoint()",
					   0, NULL);
#endif
	XLogDataFromLSN(PQgetvalue(res, 0, 0), &lsn_hi, &lsn_lo);
	PQclear(res);
	/* Calculate LSN */
	lsn = ((uint64) lsn_hi) << 32 | lsn_lo;

	return lsn;
#else
	char	   *buffer;
	size_t		size;
	ControlFileData ControlFile;

	buffer = fetchFile(conn, "global/pg_control", &size);
	digestControlFile(&ControlFile, buffer, size);
	pg_free(buffer);

	return ControlFile.checkPoint;
#endif
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

uint64
get_remote_system_identifier(PGconn *conn)
{
#if PG_VERSION_NUM >= 90600
	PGresult   *res;
	uint64		system_id_conn;
	char	   *val;

	res = pgut_execute(conn,
					   "SELECT system_identifier FROM pg_catalog.pg_control_system()",
					   0, NULL);
	val = PQgetvalue(res, 0, 0);
	if (!parse_uint64(val, &system_id_conn, 0))
	{
		PQclear(res);
		elog(ERROR, "%s is not system_identifier", val);
	}
	PQclear(res);

	return system_id_conn;
#else
	char	   *buffer;
	size_t		size;
	ControlFileData ControlFile;

	buffer = fetchFile(conn, "global/pg_control", &size);
	digestControlFile(&ControlFile, buffer, size);
	pg_free(buffer);

	return ControlFile.system_identifier;
#endif
}

uint32
get_data_checksum_version(bool safe)
{
	ControlFileData ControlFile;
	char	   *buffer;
	size_t		size;

	/* First fetch file... */
	buffer = slurpFile(pgdata, "global/pg_control", &size, safe);
	if (buffer == NULL)
		return 0;
	digestControlFile(&ControlFile, buffer, size);
	pg_free(buffer);

	return ControlFile.data_checksum_version;
}


/*
 * Convert time_t value to ISO-8601 format string. Always set timezone offset.
 */
void
time2iso(char *buf, size_t len, time_t time)
{
	struct tm  *ptm = gmtime(&time);
	time_t		gmt = mktime(ptm);
	time_t		offset;
	char	   *ptr = buf;

	ptm = localtime(&time);
	offset = time - gmt + (ptm->tm_isdst ? 3600 : 0);

	strftime(ptr, len, "%Y-%m-%d %H:%M:%S", ptm);

	ptr += strlen(ptr);
	snprintf(ptr, len - (ptr - buf), "%c%02d",
			 (offset >= 0) ? '+' : '-',
			 abs((int) offset) / SECS_PER_HOUR);

	if (abs((int) offset) % SECS_PER_HOUR != 0)
	{
		ptr += strlen(ptr);
		snprintf(ptr, len - (ptr - buf), ":%02d",
				 abs((int) offset % SECS_PER_HOUR) / SECS_PER_MINUTE);
	}
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

/* Parse string representation of the server version */
int
parse_server_version(char *server_version_str)
{
	int			nfields;
	int			result = 0;
	int			major_version = 0;
	int			minor_version = 0;

	nfields = sscanf(server_version_str, "%d.%d", &major_version, &minor_version);
	if (nfields == 2)
	{
		/* Server version lower than 10 */
		if (major_version > 10)
			elog(ERROR, "Server version format doesn't match major version %d", major_version);
		result = major_version * 10000 + minor_version * 100;
	}
	else if (nfields == 1)
	{
		if (major_version < 10)
			elog(ERROR, "Server version format doesn't match major version %d", major_version);
		result = major_version * 10000;
	}
	else
		elog(ERROR, "Unknown server version format");

	return result;
}

const char *
status2str(BackupStatus status)
{
	static const char *statusName[] =
	{
		"UNKNOWN",
		"OK",
		"ERROR",
		"RUNNING",
		"MERGING",
		"DELETING",
		"DELETED",
		"DONE",
		"ORPHAN",
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
