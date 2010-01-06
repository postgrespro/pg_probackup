/*-------------------------------------------------------------------------
 *
 * pg_rman.h: Backup/Recovery manager for PostgreSQL.
 *
 * Copyright (c) 2009-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_RMAN_H
#define PG_RMAN_H

#include "postgres_fe.h"

#include <limits.h>
#include "libpq-fe.h"

#include "pgut/pgut.h"
#include "access/xlogdefs.h"
#include "utils/pg_crc.h"
#include "parray.h"

#if PG_VERSION_NUM < 80200
#define XLOG_BLCKSZ		BLCKSZ
#endif

/* Directory/File names */
#define DATABASE_DIR			"database"
#define ARCLOG_DIR				"arclog"
#define SRVLOG_DIR				"srvlog"
#define RESTORE_WORK_DIR		"backup"
#define PG_XLOG_DIR				"pg_xlog"
#define TIMELINE_HISTORY_DIR	"timeline_history"
#define BACKUP_INI_FILE			"backup.ini"
#define PG_RMAN_INI_FILE		"pg_rman.ini"
#define MKDIRS_SH_FILE			"mkdirs.sh"
#define DATABASE_FILE_LIST		"file_database.txt"
#define ARCLOG_FILE_LIST		"file_arclog.txt"
#define SRVLOG_FILE_LIST		"file_srvlog.txt"

/* Direcotry/File permission */
#define DIR_PERMISSION		(0700)
#define FILE_PERMISSION		(0600)

/* Exit code */
#define ERROR_ARCHIVE_FAILED	20	/* cannot archive xlog file */
#define ERROR_NO_BACKUP			21	/* backup was not found in the catalog */
#define ERROR_CORRUPTED			22	/* backup catalog is corrupted */
#define ERROR_ALREADY_RUNNING	23	/* another pg_rman is running */
#define ERROR_PG_INCOMPATIBLE	24	/* block size is not compatible */
#define ERROR_PG_RUNNING		25	/* PostgreSQL server is running */
#define ERROR_PID_BROKEN		26	/* postmaster.pid file is broken */

/* backup mode file */
typedef struct pgFile
{
	time_t	mtime;			/* time of last modification */
	mode_t	mode;			/* protection (file type and permission) */
	size_t	size;			/* size of the file */
	size_t	read_size;		/* size of the portion read (if only some pages are
							   backed up partially, it's different from size) */
	size_t	write_size;		/* size of the backed-up file. BYTES_INVALID means
							   that the file existed but was not backed up
							   because not modified since last backup. */
	pg_crc32 crc;			/* CRC value of the file, regular file only */
	char   *linked;			/* path of the linked file */
	bool	is_datafile;	/* true if the file is PostgreSQL data file */
	char	path[1]; 		/* path of the file */
} pgFile;

typedef struct pgBackupRange
{
	time_t	begin;
	time_t	end;			/* begin +1 when one backup is target */
} pgBackupRange;

#define pgBackupRangeIsValid(range)	\
	(((range)->begin != (time_t) 0) || ((range)->end != (time_t) 0))
#define pgBackupRangeIsSingle(range) \
	(pgBackupRangeIsValid(range) && (range)->begin == ((range)->end))

/* Backup status */
/* XXX re-order ? */
typedef enum BackupStatus
{
	BACKUP_STATUS_INVALID,		/* the pgBackup is invalid */
	BACKUP_STATUS_OK,			/* completed backup */
	BACKUP_STATUS_RUNNING,		/* running backup */
	BACKUP_STATUS_ERROR,		/* aborted because of unexpected error */
	BACKUP_STATUS_DELETING,		/* data files are being deleted */
	BACKUP_STATUS_DELETED,		/* data files have been deleted */
	BACKUP_STATUS_DONE,			/* completed but not validated yet */
	BACKUP_STATUS_CORRUPT		/* files are corrupted, not available */
} BackupStatus;

typedef enum BackupMode
{
	BACKUP_MODE_INVALID,
	BACKUP_MODE_ARCHIVE,		/* archive only */
	BACKUP_MODE_INCREMENTAL,	/* incremental backup */
	BACKUP_MODE_FULL			/* full backup */
} BackupMode;

/*
 * pg_rman takes backup into the directroy $BACKUP_PATH/<date>/<time>.
 *
 * status == -1 indicates the pgBackup is invalid.
 */
typedef struct pgBackup
{
	/* Backup Level */
	BackupMode	backup_mode;
	bool		with_serverlog;
	bool		compress_data;

	/* Status - one of BACKUP_STATUS_xxx */
	BackupStatus	status;

	/* Timestamp, etc. */
	TimeLineID	tli;
	XLogRecPtr	start_lsn;
	XLogRecPtr	stop_lsn;
	time_t		start_time;
	time_t		end_time;

	/* Size (-1 means not-backup'ed) */
	int64		total_data_bytes;
	int64		read_data_bytes;
	int64		read_arclog_bytes;
	int64		read_srvlog_bytes;
	int64		write_bytes;

	/* data/wal block size for compatibility check */
	uint32		block_size;
	uint32		wal_block_size;
} pgBackup;

/* special values of pgBackup */
#define KEEP_INFINITE			(INT_MAX)
#define BYTES_INVALID			(-1)

#define HAVE_DATABASE(backup)	((backup)->backup_mode >= BACKUP_MODE_INCREMENTAL)
#define HAVE_ARCLOG(backup)		((backup)->backup_mode >= BACKUP_MODE_ARCHIVE)
#define TOTAL_READ_SIZE(backup)	\
	((HAVE_DATABASE((backup)) ? (backup)->read_data_bytes : 0) + \
	 (HAVE_ARCLOG((backup)) ? (backup)->read_arclog_bytes : 0) + \
	 ((backup)->with_serverlog ? (backup)->read_srvlog_bytes : 0))

typedef struct pgTimeLine
{
	TimeLineID	tli;
	XLogRecPtr	end;
} pgTimeLine;

typedef enum CompressionMode
{
	NO_COMPRESSION,
	COMPRESSION,
	DECOMPRESSION,
} CompressionMode;

/* path configuration */
extern char *backup_path;
extern char *pgdata;
extern char *arclog_path;
extern char *srvlog_path;

/* common configuration */
extern bool verbose;
extern bool check;

/* current settings */
extern pgBackup current;

/* exclude directory list for $PGDATA file listing */
extern const char *pgdata_exclude[];

/* in backup.c */
extern int do_backup(bool smooth_checkpoint,
					 int keep_arclog_files,
					 int keep_arclog_days,
					 int keep_srvlog_files,
					 int keep_srvlog_days,
					 int keep_data_generations,
					 int keep_data_days);
extern BackupMode parse_backup_mode(const char *value, int elevel);
extern int get_server_version(void);

/* in restore.c */
extern int do_restore(const char *target_time,
					  const char *target_xid,
					  const char *target_inclusive,
					  TimeLineID target_tli);

/* in init.c */
extern int do_init(void);

/* in show.c */
extern int do_show(pgBackupRange *range, bool show_timeline, bool show_all);

/* in delete.c */
extern int do_delete(pgBackupRange *range);
extern void pgBackupDelete(int keep_generations, int keep_days);

/* in validate.c */
extern int do_validate(pgBackupRange *range);
extern void pgBackupValidate(pgBackup *backup, bool size_only);

/* in catalog.c */
extern pgBackup *catalog_get_backup(time_t timestamp);
extern parray *catalog_get_backup_list(const pgBackupRange *range);
extern pgBackup *catalog_get_last_data_backup(parray *backup_list);
extern pgBackup *catalog_get_last_arclog_backup(parray *backup_list);
extern pgBackup *catalog_get_last_srvlog_backup(parray *backup_list);

extern int catalog_lock(void);
extern void catalog_unlock(void);

extern void catalog_init_config(pgBackup *backup);

extern void pgBackupWriteConfigSection(FILE *out, pgBackup *backup);
extern void pgBackupWriteResultSection(FILE *out, pgBackup *backup);
extern void pgBackupWriteIni(pgBackup *backup);
extern void pgBackupGetPath(const pgBackup *backup, char *path, size_t len, const char *subdir);
extern int pgBackupCreateDir(pgBackup *backup);
extern void pgBackupFree(void *backup);
extern int pgBackupCompareId(const void *f1, const void *f2);
extern int pgBackupCompareIdDesc(const void *f1, const void *f2);

/* in dir.c */
extern void dir_list_file(parray *files, const char *root, const char *exclude[], bool omit_symlink, bool add_root);
extern void dir_print_mkdirs_sh(FILE *out, const parray *files, const char *root);
extern void dir_print_file_list(FILE *out, const parray *files, const char *root);
extern parray *dir_read_file_list(const char *root, const char *file_txt);

extern int dir_create_dir(const char *path, mode_t mode);
extern void dir_copy_files(const char *from_root, const char *to_root);

extern void pgFileDelete(pgFile *file);
extern void pgFileFree(void *file);
extern pg_crc32 pgFileGetCRC(pgFile *file);
extern int pgFileComparePath(const void *f1, const void *f2);
extern int pgFileComparePathDesc(const void *f1, const void *f2);
extern int pgFileCompareMtime(const void *f1, const void *f2);
extern int pgFileCompareMtimeDesc(const void *f1, const void *f2);

/* in xlog.c */
extern bool xlog_is_complete_wal(const pgFile *file, int server_version);
extern bool xlog_logfname2lsn(const char *logfname, XLogRecPtr *lsn);
extern void xlog_fname(char *fname, size_t len, TimeLineID tli, XLogRecPtr *lsn);

/* in data.c */
extern void backup_data_file(const char *from_root, const char *to_root,
							 pgFile *file, const XLogRecPtr *lsn, bool compress);
extern void restore_data_file(const char *from_root, const char *to_root,
							  pgFile *file, bool compress);
extern void copy_file(const char *from_root, const char *to_root,
					  pgFile *file, CompressionMode compress);

/* in util.c */
extern void time2iso(char *buf, size_t len, time_t time);
extern const char *status2str(BackupStatus status);
extern void remove_trailing_space(char *buf, int comment_mark);
extern void remove_not_digit(char *buf, size_t len, const char *str);

/* in pgsql_src/pg_ctl.c */
extern bool is_pg_running(void);

/* access/xlog_internal.h */
#define XLogSegSize		((uint32) XLOG_SEG_SIZE)
#define XLogSegsPerFile (((uint32) 0xffffffff) / XLogSegSize)
#define XLogFileSize	(XLogSegsPerFile * XLogSegSize)

#define NextLogSeg(logId, logSeg)	\
	do { \
		if ((logSeg) >= XLogSegsPerFile-1) \
		{ \
			(logId)++; \
			(logSeg) = 0; \
		} \
		else \
			(logSeg)++; \
	} while (0)

#define MAXFNAMELEN		64
#define XLogFileName(fname, tli, log, seg)	\
	snprintf(fname, MAXFNAMELEN, "%08X%08X%08X", tli, log, seg)

#endif /* PG_RMAN_H */
