/*-------------------------------------------------------------------------
 *
 * pg_arman.h: Backup/Recovery manager for PostgreSQL.
 *
 * Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
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
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "utils/pg_crc.h"
#include "parray.h"
#include "datapagemap.h"
#include "storage/bufpage.h"
#include "storage/block.h"

/* Query to fetch current transaction ID */
#define TXID_CURRENT_SQL	"SELECT txid_current();"

/* Directory/File names */
#define DATABASE_DIR			"database"
#define RESTORE_WORK_DIR		"backup"
#define PG_XLOG_DIR			"pg_xlog"
#define PG_TBLSPC_DIR			"pg_tblspc"
#define BACKUP_INI_FILE			"backup.ini"
#define PG_RMAN_INI_FILE		"pg_arman.ini"
#define MKDIRS_SH_FILE			"mkdirs.sh"
#define DATABASE_FILE_LIST		"file_database.txt"
#define PG_BACKUP_LABEL_FILE		"backup_label"
#define PG_BLACK_LIST			"black_list"

/* Direcotry/File permission */
#define DIR_PERMISSION		(0700)
#define FILE_PERMISSION		(0600)

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
	char	*path;			/* path of the file */
	char	*ptrack_path;
	int		segno;			/* Segment number for ptrack */
	datapagemap_t pagemap;
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

#define IsValidTime(tm)	\
	((tm.tm_sec >= 0 && tm.tm_sec <= 60) && 	/* range check for tm_sec (0-60)  */ \
	 (tm.tm_min >= 0 && tm.tm_min <= 59) && 	/* range check for tm_min (0-59)  */ \
	 (tm.tm_hour >= 0 && tm.tm_hour <= 23) && 	/* range check for tm_hour(0-23)  */ \
	 (tm.tm_mday >= 1 && tm.tm_mday <= 31) && 	/* range check for tm_mday(1-31)  */ \
	 (tm.tm_mon >= 0 && tm.tm_mon <= 11) && 	/* range check for tm_mon (0-23)  */ \
	 (tm.tm_year + 1900 >= 1900)) 			/* range check for tm_year(70-)    */

/* Effective data size */
#define MAPSIZE (BLCKSZ - MAXALIGN(SizeOfPageHeaderData))

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
	BACKUP_MODE_DIFF_PAGE,		/* differential page backup */
	BACKUP_MODE_DIFF_PTRACK,	/* differential page backup with ptrack system*/
	BACKUP_MODE_FULL			/* full backup */
} BackupMode;

/*
 * pg_arman takes backup into the directroy $BACKUP_PATH/<date>/<time>.
 *
 * status == -1 indicates the pgBackup is invalid.
 */
typedef struct pgBackup
{
	/* Backup Level */
	BackupMode	backup_mode;

	/* Status - one of BACKUP_STATUS_xxx */
	BackupStatus	status;

	/* Timestamp, etc. */
	TimeLineID	tli;
	XLogRecPtr	start_lsn;
	XLogRecPtr	stop_lsn;
	time_t		start_time;
	time_t		end_time;
	time_t		recovery_time;
	uint32		recovery_xid;

	/* Different sizes (-1 means nothing was backed up) */
	/*
	 * Amount of raw data. For a full backup, this is the total amount of
	 * data while for a differential backup this is just the difference
	 * of data taken.
	 */
	int64		data_bytes;

	/* data/wal block size for compatibility check */
	uint32		block_size;
	uint32		wal_block_size;
} pgBackup;

typedef struct pgBackupOption
{
	bool smooth_checkpoint;
	int  keep_data_generations;
	int  keep_data_days;
} pgBackupOption;


/* special values of pgBackup */
#define KEEP_INFINITE			(INT_MAX)
#define BYTES_INVALID			(-1)

typedef struct pgTimeLine
{
	TimeLineID	tli;
	XLogRecPtr	end;
} pgTimeLine;

typedef struct pgRecoveryTarget
{
	bool		time_specified;
	time_t		recovery_target_time;
	bool		xid_specified;
	unsigned int	recovery_target_xid;
	bool		recovery_target_inclusive;
} pgRecoveryTarget;

typedef union DataPage
{
	PageHeaderData	page_data;
	char			data[BLCKSZ];
} DataPage;

/*
 * return pointer that exceeds the length of prefix from character string.
 * ex. str="/xxx/yyy/zzz", prefix="/xxx/yyy", return="zzz".
 */
#define JoinPathEnd(str, prefix) \
	((strlen(str) <= strlen(prefix)) ? "" : str + strlen(prefix) + 1)

/*
 * Return timeline, xlog ID and record offset from an LSN of the type
 * 0/B000188, usual result from pg_stop_backup() and friends.
 */
#define XLogDataFromLSN(data, xlogid, xrecoff)		\
	sscanf(data, "%X/%X", xlogid, xrecoff)

/* path configuration */
extern char *backup_path;
extern char *pgdata;
extern char *arclog_path;

/* common configuration */
extern bool check;

/* current settings */
extern pgBackup current;

/* exclude directory list for $PGDATA file listing */
extern const char *pgdata_exclude[];

/* backup file list from non-snapshot */
extern parray *backup_files_list;

extern int num_threads;
extern bool stream_wal;

/* in backup.c */
extern int do_backup(pgBackupOption bkupopt);
extern BackupMode parse_backup_mode(const char *value);
extern void check_server_version(void);
extern bool fileExists(const char *path);
extern void process_block_change(ForkNumber forknum, RelFileNode rnode,
								 BlockNumber blkno);

/* in restore.c */
extern int do_restore(const char *target_time,
					  const char *target_xid,
					  const char *target_inclusive,
					  TimeLineID target_tli);

/* in init.c */
extern int do_init(void);

/* in show.c */
extern int do_show(pgBackupRange *range, bool show_all);

/* in delete.c */
extern int do_delete(pgBackupRange *range);
extern void pgBackupDelete(int keep_generations, int keep_days);

/* in fetch.c */
extern char *slurpFile(const char *datadir,
					   const char *path,
					   size_t *filesize,
					   bool safe);

/* in validate.c */
extern int do_validate(pgBackupRange *range);
extern void pgBackupValidate(pgBackup *backup,
							 bool size_only,
							 bool for_get_timeline);

/* in catalog.c */
extern pgBackup *catalog_get_backup(time_t timestamp);
extern parray *catalog_get_backup_list(const pgBackupRange *range);
extern pgBackup *catalog_get_last_data_backup(parray *backup_list,
											  TimeLineID tli);

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
extern void dir_list_file_internal(parray *files, const char *root, const char *exclude[],
					bool omit_symlink, bool add_root, parray *black_list);
extern void dir_print_mkdirs_sh(FILE *out, const parray *files, const char *root);
extern void dir_print_file_list(FILE *out, const parray *files, const char *root, const char *prefix);
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

/* in data.c */
extern bool backup_data_file(const char *from_root, const char *to_root,
							 pgFile *file, const XLogRecPtr *lsn);
extern void restore_data_file(const char *from_root, const char *to_root,
							  pgFile *file);
extern bool copy_file(const char *from_root, const char *to_root,
					  pgFile *file);

extern bool calc_file(pgFile *file);

/* parsexlog.c */
extern void extractPageMap(const char *datadir, XLogRecPtr startpoint,
						   TimeLineID tli, XLogRecPtr endpoint);

/* in util.c */
extern TimeLineID get_current_timeline(bool safe);
extern void sanityChecks(void);
extern void time2iso(char *buf, size_t len, time_t time);
extern const char *status2str(BackupStatus status);
extern void remove_trailing_space(char *buf, int comment_mark);
extern void remove_not_digit(char *buf, size_t len, const char *str);
extern XLogRecPtr get_last_ptrack_lsn(void);

/* in status.c */
extern bool is_pg_running(void);

#endif /* PG_RMAN_H */
