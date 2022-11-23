/*-------------------------------------------------------------------------
 *
 * util.c: log messages to log file or stderr, and misc code.
 *
 * Portions Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2022, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <time.h>

#include <unistd.h>

#include <sys/stat.h>

static const char *statusName[] =
{
	"UNKNOWN",
	"OK",
	"ERROR",
	"RUNNING",
	"MERGING",
	"MERGED",
	"DELETING",
	"DELETED",
	"DONE",
	"ORPHAN",
	"CORRUPT"
};

const char *
base36enc_to(long unsigned int value, char buf[static base36bufsize])
{
	const char	base36[36] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	char	buffer[base36bufsize];
	char   *p;

	p = &buffer[sizeof(buffer)-1];
	*p = '\0';
	do {
		*(--p) = base36[value % 36];
	} while (value /= 36);

	/* I know, it doesn't look safe */
	strncpy(buf, p, base36bufsize);

	return buf;
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
 * Write ControlFile to pg_control
 */
static void
writeControlFile(fio_location location, const char *path, ControlFileData *ControlFile)
{
	int			fd;
	char       *buffer = NULL;

	int			ControlFileSize = PG_CONTROL_FILE_SIZE;

	/* copy controlFileSize */
	buffer = pg_malloc0(ControlFileSize);
	memcpy(buffer, ControlFile, sizeof(ControlFileData));

	/* Write pg_control */
	fd = fio_open(location, path,
				  O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);

	if (fd < 0)
		elog(ERROR, "Failed to open file: %s", path);

	if (fio_write(fd, buffer, ControlFileSize) != ControlFileSize)
		elog(ERROR, "Failed to overwrite file: %s", path);

	if (fio_flush(fd) != 0)
		elog(ERROR, "Failed to sync file: %s", path);

	fio_close(fd);
	pg_free(buffer);
}

/*
 * Utility shared by backup and restore to fetch the current timeline
 * used by a node.
 */
TimeLineID
get_current_timeline(PGconn *conn)
{

	PGresult   *res;
	TimeLineID tli = 0;
	char	   *val;

	res = pgut_execute_extended(conn,
				   "SELECT timeline_id FROM pg_catalog.pg_control_checkpoint()", 0, NULL, true, true);

	if (PQresultStatus(res) == PGRES_TUPLES_OK)
		val = PQgetvalue(res, 0, 0);
	else
		return get_current_timeline_from_control(FIO_DB_HOST, instance_config.pgdata);

	if (!parse_uint32(val, &tli, 0))
	{
		PQclear(res);
		elog(WARNING, "Invalid value of timeline_id %s", val);

		/* TODO 3.0 remove it and just error out */
		return get_current_timeline_from_control(FIO_DB_HOST, instance_config.pgdata);
	}

	return tli;
}

static err_i
get_control_file(fio_location location, path_t pgdata_path, path_t file,
				 ControlFileData *control, bool safe)
{
	pioDrive_i	drive;
	char		fullpath[MAXPGPATH];
	ft_bytes_t	bytes;
	err_i		err;

	fobj_reset_err(&err);

	join_path_components(fullpath, pgdata_path, file);

	drive = pioDriveForLocation(location);
	bytes = $i(pioReadFile, drive, .path = fullpath, .err = &err);
	if ($haserr(err) && safe)
	{
		ft_logerr(FT_WARNING, $errmsg(err), "Could not get control file");
		memset(control, 0, sizeof(ControlFileData));
		return $noerr();
	}
	if ($haserr(err))
		return $err(RT, "Could not get control file: {cause}",
					cause(err.self));

	if (bytes.len != PG_CONTROL_FILE_SIZE)
		return $err(RT, "unexpected control file size: {size}, expected {wantedSz}",
					size(bytes.len), wantedSz(PG_CONTROL_FILE_SIZE));

	memcpy(control, bytes.ptr, sizeof(ControlFileData));
	ft_bytes_free(&bytes);

	/* Additional checks on control file */
	checkControlFile(control);

	return $noerr();
}

/* Get timeline from pg_control file */
TimeLineID
get_current_timeline_from_control(fio_location location, const char *pgdata_path)
{
	FOBJ_FUNC_ARP();
	ControlFileData ControlFile;
	err_i			err;

	err = get_control_file(location, pgdata_path, XLOG_CONTROL_FILE,
						   &ControlFile, false);
	if ($haserr(err))
		ft_logerr(FT_FATAL, $errmsg(err), "Getting current timeline");

	return ControlFile.checkPointCopy.ThisTimeLineID;
}

/*
 * Get last check point record ptr from pg_control.
 */
XLogRecPtr
get_checkpoint_location(PGconn *conn)
{
	PGresult   *res;
	uint32		lsn_hi;
	uint32		lsn_lo;
	XLogRecPtr	lsn;

	res = pgut_execute(conn,
					   "SELECT checkpoint_lsn FROM pg_catalog.pg_control_checkpoint()",
					   0, NULL);
	XLogDataFromLSN(PQgetvalue(res, 0, 0), &lsn_hi, &lsn_lo);
	PQclear(res);
	/* Calculate LSN */
	lsn = ((uint64) lsn_hi) << 32 | lsn_lo;

	return lsn;
}

uint64
get_system_identifier(fio_location location, const char *pgdata_path, bool safe)
{
	FOBJ_FUNC_ARP();
	ControlFileData ControlFile;
	err_i			err;

	err = get_control_file(location, pgdata_path, XLOG_CONTROL_FILE,
						   &ControlFile, safe);
	if ($haserr(err))
		ft_logerr(FT_FATAL, $errmsg(err), "Getting system identifier");

	return ControlFile.system_identifier;
}

uint64
get_remote_system_identifier(PGconn *conn)
{
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
}

uint32
get_xlog_seg_size(const char *pgdata_path)
{
#if PG_VERSION_NUM >= 110000
	FOBJ_FUNC_ARP();
	ControlFileData ControlFile;
	err_i			err;

	err = get_control_file(FIO_DB_HOST, pgdata_path, XLOG_CONTROL_FILE,
						   &ControlFile, false);
	if ($haserr(err))
		ft_logerr(FT_FATAL, $errmsg(err), "Trying to fetch segment size");

	return ControlFile.xlog_seg_size;
#else
	return (uint32) XLOG_SEG_SIZE;
#endif
}

pg_crc32c
get_pgcontrol_checksum(const char *pgdata_path)
{
	FOBJ_FUNC_ARP();
	ControlFileData ControlFile;
	err_i			err;

	err = get_control_file(FIO_BACKUP_HOST, pgdata_path, XLOG_CONTROL_FILE,
						   &ControlFile, false);
	if ($haserr(err))
		ft_logerr(FT_FATAL, $errmsg(err), "Getting pgcontrol checksum");

	return ControlFile.crc;
}

void
get_redo(fio_location location, const char *pgdata_path, RedoParams *redo)
{
	FOBJ_FUNC_ARP();
	ControlFileData ControlFile;
	err_i			err;

	err = get_control_file(location, pgdata_path, XLOG_CONTROL_FILE,
						   &ControlFile, false);
	if ($haserr(err))
		ft_logerr(FT_FATAL, $errmsg(err), "Fetching redo lsn");

	redo->lsn = ControlFile.checkPointCopy.redo;
	redo->tli = ControlFile.checkPointCopy.ThisTimeLineID;

	if (ControlFile.minRecoveryPoint > 0 &&
		ControlFile.minRecoveryPoint < redo->lsn)
	{
		redo->lsn = ControlFile.minRecoveryPoint;
		redo->tli = ControlFile.minRecoveryPointTLI;
	}

	if (ControlFile.backupStartPoint > 0 &&
		ControlFile.backupStartPoint < redo->lsn)
	{
		redo->lsn = ControlFile.backupStartPoint;
		redo->tli = ControlFile.checkPointCopy.ThisTimeLineID;
	}

	redo->checksum_version = ControlFile.data_checksum_version;
}

/*
 * Rewrite minRecoveryPoint of pg_control in backup directory. minRecoveryPoint
 * 'as-is' is not to be trusted.
 */
void
set_min_recovery_point(pgFile *file, const char *backup_path,
					   XLogRecPtr stop_backup_lsn)
{
	FOBJ_FUNC_ARP();
	ControlFileData ControlFile;
	char			fullpath[MAXPGPATH];
	err_i			err;

	err = get_control_file(FIO_DB_HOST, instance_config.pgdata, XLOG_CONTROL_FILE,
						   &ControlFile, false);
	if ($haserr(err))
		ft_logerr(FT_FATAL, $errmsg(err), "Set min recovery point");

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
	join_path_components(fullpath, backup_path, XLOG_CONTROL_FILE);
	writeControlFile(FIO_LOCAL_HOST, fullpath, &ControlFile);

	/* Update pg_control checksum in backup_list */
	file->crc = ControlFile.crc;
}

/*
 * Copy pg_control file to backup. We do not apply compression to this file.
 */
void
copy_pgcontrol_file(fio_location from_location, const char *from_fullpath,
					fio_location to_location, const char *to_fullpath, pgFile *file)
{
	FOBJ_FUNC_ARP();
	ControlFileData ControlFile;
	err_i			err;

	err = get_control_file(from_location, from_fullpath, "",
						   &ControlFile, false);
	if ($haserr(err))
		ft_logerr(FT_FATAL, $errmsg(err), "Fetching control file");

	file->crc = ControlFile.crc;
	file->read_size = PG_CONTROL_FILE_SIZE;
	file->write_size = PG_CONTROL_FILE_SIZE;
	file->uncompressed_size = PG_CONTROL_FILE_SIZE;

	writeControlFile(to_location, to_fullpath, &ControlFile);
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
	if (status < BACKUP_STATUS_INVALID || BACKUP_STATUS_CORRUPT < status)
		return "UNKNOWN";

	return statusName[status];
}

const char *
status2str_color(BackupStatus status)
{
	char *status_str = pgut_malloc(20);

	/* UNKNOWN */
	if (status == BACKUP_STATUS_INVALID)
		snprintf(status_str, 20, "%s%s%s", TC_YELLOW_BOLD, "UNKNOWN", TC_RESET);
	/* CORRUPT, ERROR and ORPHAN */
	else if (status == BACKUP_STATUS_CORRUPT || status == BACKUP_STATUS_ERROR ||
			 status == BACKUP_STATUS_ORPHAN)
		snprintf(status_str, 20, "%s%s%s", TC_RED_BOLD, statusName[status], TC_RESET);
	/* MERGING, MERGED, DELETING and DELETED */
	else if (status == BACKUP_STATUS_MERGING || status == BACKUP_STATUS_MERGED ||
			 status == BACKUP_STATUS_DELETING || status == BACKUP_STATUS_DELETED)
		snprintf(status_str, 20, "%s%s%s", TC_YELLOW_BOLD, statusName[status], TC_RESET);
	/* OK and DONE */
	else
		snprintf(status_str, 20, "%s%s%s", TC_GREEN_BOLD, statusName[status], TC_RESET);

	return status_str;
}

BackupStatus
str2status(const char *status)
{
	BackupStatus i;

	for (i = BACKUP_STATUS_INVALID; i <= BACKUP_STATUS_CORRUPT; i++)
	{
		if (pg_strcasecmp(status, statusName[i]) == 0) return i;
	}

	return BACKUP_STATUS_INVALID;
}

bool
datapagemap_is_set(datapagemap_t *map, BlockNumber blkno)
{
	int			offset;
	int			bitno;

	offset = blkno / 8;
	bitno = blkno % 8;

	return (map->bitmapsize <= offset) ? false : (map->bitmap[offset] & (1 << bitno)) != 0;
}

/*
 * A debugging aid. Prints out the contents of the page map.
 */
void
datapagemap_print_debug(datapagemap_t *map)
{
	datapagemap_iterator_t *iter;
	BlockNumber blocknum;

	iter = datapagemap_iterate(map);
	while (datapagemap_next(iter, &blocknum))
		elog(VERBOSE, "  block %u", blocknum);

	pg_free(iter);
}

const char*
backup_id_of(pgBackup *backup)
{
	/* Change this Assert when backup_id will not be bound to start_time */
	Assert(backup->backup_id == backup->start_time || backup->start_time == 0);

	if (backup->backup_id_encoded[0] == '\x00')
	{
		base36enc_to(backup->backup_id, backup->backup_id_encoded);
	}
	return backup->backup_id_encoded;
}

void
reset_backup_id(pgBackup *backup)
{
	backup->backup_id = INVALID_BACKUP_ID;
	memset(backup->backup_id_encoded, 0, sizeof(backup->backup_id_encoded));
}
