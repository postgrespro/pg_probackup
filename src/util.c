/*-------------------------------------------------------------------------
 *
 * util.c: log messages to log file or stderr, and misc code.
 *
 * Portions Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include "catalog/pg_control.h"

#include <time.h>

#include <unistd.h>

#include <sys/stat.h>

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

	if ((ControlFile->pg_control_version % 65536 == 0 || ControlFile->pg_control_version % 65536 > 10000) &&
			ControlFile->pg_control_version / 65536 != 0)
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
 * Write ControlFile to pg_control
 */
static void
writeControlFile(ControlFileData *ControlFile, char *path)
{
	int			fd;
	char       *buffer = NULL;

#if PG_VERSION_NUM >= 100000
	int			ControlFileSize = PG_CONTROL_FILE_SIZE;
#else
	int			ControlFileSize = PG_CONTROL_SIZE;
#endif

	/* copy controlFileSize */
	buffer = pg_malloc(ControlFileSize);
	memcpy(buffer, ControlFile, sizeof(ControlFileData));

	/* Write pg_control */
	unlink(path);
	fd = open(path,
			  O_RDWR | O_CREAT | O_EXCL | PG_BINARY,
			  S_IRUSR | S_IWUSR);

	if (fd < 0)
		elog(ERROR, "Failed to open file: %s", path);

	if (write(fd, buffer, ControlFileSize) != ControlFileSize)
		elog(ERROR, "Failed to overwrite file: %s", path);

	if (fsync(fd) != 0)
		elog(ERROR, "Failed to fsync file: %s", path);

	close(fd);
	pg_free(buffer);
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
get_system_identifier(const char *pgdata_path)
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
get_xlog_seg_size(char *pgdata_path)
{
#if PG_VERSION_NUM >= 110000
	ControlFileData ControlFile;
	char	   *buffer;
	size_t		size;

	/* First fetch file... */
	buffer = slurpFile(pgdata_path, "global/pg_control", &size, false);
	if (buffer == NULL)
		return 0;
	digestControlFile(&ControlFile, buffer, size);
	pg_free(buffer);

	return ControlFile.xlog_seg_size;
#else
	return (uint32) XLOG_SEG_SIZE;
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

pg_crc32c
get_pgcontrol_checksum(const char *pgdata_path)
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

	return ControlFile.crc;
}

/*
 * Rewrite minRecoveryPoint of pg_control in backup directory. minRecoveryPoint
 * 'as-is' is not to be trusted.
 */
void
set_min_recovery_point(pgFile *file, const char *backup_path, XLogRecPtr stop_backup_lsn)
{
	ControlFileData ControlFile;
	char       *buffer;
	size_t      size;
	char		fullpath[MAXPGPATH];

	/* First fetch file content */
	buffer = slurpFile(pgdata, XLOG_CONTROL_FILE, &size, false);
	if (buffer == NULL)
		elog(ERROR, "ERROR");

	digestControlFile(&ControlFile, buffer, size);

	elog(LOG, "Current minRecPoint %X/%X",
		(uint32) (ControlFile.minRecoveryPoint  >> 32),
		(uint32) ControlFile.minRecoveryPoint);

	elog(LOG, "Setting minRecPoint to %X/%X",
		(uint32) (stop_backup_lsn  >> 32),
		(uint32) stop_backup_lsn);

	ControlFile.minRecoveryPoint = stop_backup_lsn;

	/* Update checksum in pg_control header */
	INIT_CRC32C(ControlFile.crc);
	COMP_CRC32C(ControlFile.crc, (char *) &ControlFile,
				offsetof(ControlFileData, crc));
	FIN_CRC32C(ControlFile.crc);

	/* overwrite pg_control */
	snprintf(fullpath, sizeof(fullpath), "%s/%s", backup_path, XLOG_CONTROL_FILE);
	writeControlFile(&ControlFile, fullpath);

	/* Update pg_control checksum in backup_list */
	file->crc = ControlFile.crc;

	pg_free(buffer);
}

/*
 * Copy pg_control file to backup. We do not apply compression to this file.
 */
void
copy_pgcontrol_file(const char *from_root, const char *to_root, pgFile *file)
{
	ControlFileData ControlFile;
	char	   *buffer;
	size_t		size;
	char		to_path[MAXPGPATH];

	buffer = slurpFile(from_root, XLOG_CONTROL_FILE, &size, false);

	digestControlFile(&ControlFile, buffer, size);

	file->crc = ControlFile.crc;
	file->read_size = size;
	file->write_size = size;

	join_path_components(to_path, to_root, file->path + strlen(from_root) + 1);
	writeControlFile(&ControlFile, to_path);

	pg_free(buffer);
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

/*
 * Parse string representation of the server version.
 */
uint32
parse_server_version(const char *server_version_str)
{
	int			nfields;
	uint32		result = 0;
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
		elog(ERROR, "Unknown server version format %s", server_version_str);

	return result;
}

/*
 * Parse string representation of the program version.
 */
uint32
parse_program_version(const char *program_version)
{
	int			nfields;
	int			major = 0,
				minor = 0,
				micro = 0;
	uint32		result = 0;

	if (program_version == NULL || program_version[0] == '\0')
		return 0;

	nfields = sscanf(program_version, "%d.%d.%d", &major, &minor, &micro);
	if (nfields == 3)
		result = major * 10000 + minor * 100 + micro;
	else
		elog(ERROR, "Unknown program version format %s", program_version);

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
