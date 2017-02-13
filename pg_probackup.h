/*-------------------------------------------------------------------------
 *
 * pg_probackup.h: Backup/Recovery manager for PostgreSQL.
 *
 * Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROBACKUP_H
#define PG_PROBACKUP_H

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
#include "storage/checksum.h"

/* Query to fetch current transaction ID */
#define TXID_CURRENT_SQL	"SELECT txid_current();"
#define TXID_CURRENT_IF_SQL	"SELECT txid_snapshot_xmax(txid_current_snapshot());"

/* Directory/File names */
#define DATABASE_DIR			"database"
#define BACKUPS_DIR				"backups"
#define PG_XLOG_DIR				"pg_xlog"
#define PG_TBLSPC_DIR			"pg_tblspc"
#define BACKUP_CONF_FILE		"backup.conf"
#define BACKUP_CATALOG_CONF_FILE	"pg_probackup.conf"
#define MKDIRS_SH_FILE			"mkdirs.sh"
#define DATABASE_FILE_LIST		"file_database.txt"
#define PG_BACKUP_LABEL_FILE	"backup_label"
#define PG_BLACK_LIST			"black_list"

/* Direcotry/File permission */
#define DIR_PERMISSION		(0700)
#define FILE_PERMISSION		(0600)

#ifndef PGPRO_EE
#define XID_FMT "%u"
#endif

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
	char	*linked;			/* path of the linked file */
	bool	is_datafile;	/* true if the file is PostgreSQL data file */
	char	*path;			/* path of the file */
	char	*ptrack_path;
	int		segno;			/* Segment number for ptrack */
	volatile uint32 lock;
	datapagemap_t pagemap;
} pgFile;

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
	BACKUP_MODE_INVALID = 0,
	BACKUP_MODE_DIFF_PAGE,		/* differential page backup */
	BACKUP_MODE_DIFF_PTRACK,	/* differential page backup with ptrack system*/
	BACKUP_MODE_FULL			/* full backup */
} BackupMode;

/*
 * pg_probackup takes backup into the directroy $BACKUP_PATH/<date>/<time>.
 *
 * status == -1 indicates the pgBackup is invalid.
 */
typedef struct pgBackup
{
	/* Backup Level */
	BackupMode		backup_mode;

	/* Status - one of BACKUP_STATUS_xxx */
	BackupStatus	status;

	/* Timestamp, etc. */
	TimeLineID		tli;
	XLogRecPtr		start_lsn;
	XLogRecPtr		stop_lsn;
	time_t			start_time;
	time_t			end_time;
	time_t			recovery_time;
	TransactionId	recovery_xid;

	/* Different sizes (-1 means nothing was backed up) */
	/*
	 * Amount of raw data. For a full backup, this is the total amount of
	 * data while for a differential backup this is just the difference
	 * of data taken.
	 */
	int64			data_bytes;

	/* data/wal block size for compatibility check */
	uint32			block_size;
	uint32			wal_block_size;
	uint32			checksum_version;
	bool			stream;
	time_t			parent_backup;
} pgBackup;

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
	bool			time_specified;
	time_t			recovery_target_time;
	bool			xid_specified;
	TransactionId	recovery_target_xid;
	bool			recovery_target_inclusive;
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
extern char arclog_path[MAXPGPATH];

/* common configuration */
extern bool check;

/* current settings */
extern pgBackup current;

/* exclude directory list for $PGDATA file listing */
extern const char *pgdata_exclude_dir[];

/* backup file list from non-snapshot */
extern parray *backup_files_list;

extern int num_threads;
extern bool stream_wal;
extern bool from_replica;
extern bool progress;
extern bool delete_wal;

extern uint64 system_identifier;

/* retention configuration */
extern uint32 retention_redundancy;
extern uint32 retention_window;

/* in backup.c */
extern int do_backup(bool smooth_checkpoint);
extern BackupMode parse_backup_mode(const char *value);
extern void check_server_version(void);
extern bool fileExists(const char *path);
extern void process_block_change(ForkNumber forknum, RelFileNode rnode,
								 BlockNumber blkno);

/* in restore.c */
extern int do_restore(time_t backup_id,
					  const char *target_time,
					  const char *target_xid,
					  const char *target_inclusive,
					  TimeLineID target_tli);
extern bool satisfy_timeline(const parray *timelines, const pgBackup *backup);
extern bool satisfy_recovery_target(const pgBackup *backup,
									const pgRecoveryTarget *rt);
extern TimeLineID get_fullbackup_timeline(parray *backups,
										  const pgRecoveryTarget *rt);
extern TimeLineID findNewestTimeLine(TimeLineID startTLI);
extern parray * readTimeLineHistory(TimeLineID targetTLI);
extern pgRecoveryTarget *checkIfCreateRecoveryConf(
	const char *target_time,
	const char *target_xid,
	const char *target_inclusive);

/* in init.c */
extern int do_init(void);

/* in show.c */
extern int do_show(time_t backup_id);
extern int do_retention_show(void);

/* in delete.c */
extern int do_delete(time_t backup_id);
extern int do_deletewal(time_t backup_id, bool strict);
extern int do_retention_purge(void);

/* in fetch.c */
extern char *slurpFile(const char *datadir,
					   const char *path,
					   size_t *filesize,
					   bool safe);

/* in validate.c */
extern int do_validate(time_t backup_id,
					   const char *target_time,
					   const char *target_xid,
					   const char *target_inclusive,
					   TimeLineID target_tli);
extern void do_validate_last(void);
extern void pgBackupValidate(pgBackup *backup,
							 bool size_only,
							 bool for_get_timeline);

extern pgBackup *read_backup(time_t timestamp);
extern void init_backup(pgBackup *backup);

extern parray *catalog_get_backup_list(time_t backup_id);
extern pgBackup *catalog_get_last_data_backup(parray *backup_list,
											  TimeLineID tli);

extern int catalog_lock(bool check_catalog);
extern void catalog_unlock(void);

extern void pgBackupWriteConfigSection(FILE *out, pgBackup *backup);
extern void pgBackupWriteResultSection(FILE *out, pgBackup *backup);
extern void pgBackupWriteIni(pgBackup *backup);
extern void pgBackupGetPath(const pgBackup *backup, char *path, size_t len, const char *subdir);
extern int pgBackupCreateDir(pgBackup *backup);
extern void pgBackupFree(void *backup);
extern int pgBackupCompareId(const void *f1, const void *f2);
extern int pgBackupCompareIdDesc(const void *f1, const void *f2);

/* in dir.c */
extern void dir_list_file(parray *files, const char *root, bool exclude,
						  bool omit_symlink, bool add_root);
extern void dir_list_file_internal(parray *files, const char *root, bool exclude,
						  bool omit_symlink, bool add_root, parray *black_list);
extern void dir_print_mkdirs_sh(FILE *out, const parray *files, const char *root);
extern void dir_print_file_list(FILE *out, const parray *files, const char *root, const char *prefix);
extern parray *dir_read_file_list(const char *root, const char *file_txt);

extern int dir_create_dir(const char *path, mode_t mode);
extern void dir_copy_files(const char *from_root, const char *to_root);

extern pgFile *pgFileNew(const char *path, bool omit_symlink);
extern void pgFileDelete(pgFile *file);
extern void pgFileFree(void *file);
extern pg_crc32 pgFileGetCRC(pgFile *file);
extern int pgFileComparePath(const void *f1, const void *f2);
extern int pgFileComparePathDesc(const void *f1, const void *f2);
extern int pgFileCompareSize(const void *f1, const void *f2);
extern int pgFileCompareMtime(const void *f1, const void *f2);
extern int pgFileCompareMtimeDesc(const void *f1, const void *f2);

/* in data.c */
extern bool backup_data_file(const char *from_root, const char *to_root,
							 pgFile *file, const XLogRecPtr *lsn);
extern void restore_data_file(const char *from_root, const char *to_root,
							  pgFile *file, pgBackup *backup);
extern bool copy_file(const char *from_root, const char *to_root,
					  pgFile *file);

extern bool calc_file(pgFile *file);

/* parsexlog.c */
extern void extractPageMap(const char *datadir,
						   XLogRecPtr startpoint,
						   TimeLineID tli,
						   XLogRecPtr endpoint);
extern void validate_wal(pgBackup *backup,
						 const char *archivedir,
						 XLogRecPtr startpoint,
						 time_t target_time,
						 TransactionId recovery_target_xid,
						 TimeLineID tli);

/* in util.c */
extern TimeLineID get_current_timeline(bool safe);
extern void sanityChecks(void);
extern void time2iso(char *buf, size_t len, time_t time);
extern const char *status2str(BackupStatus status);
extern void remove_trailing_space(char *buf, int comment_mark);
extern void remove_not_digit(char *buf, size_t len, const char *str);
extern XLogRecPtr get_last_ptrack_lsn(void);
extern uint32 get_data_checksum_version(bool safe);
extern char *base36enc(long unsigned int value);
extern long unsigned int base36dec(const char *text);
extern uint64 get_system_identifier(bool safe);
extern pg_time_t timestamptz_to_time_t(TimestampTz t);

/* in status.c */
extern bool is_pg_running(void);

/* some from access/xact.h */
/*
 * XLOG allows to store some information in high 4 bits of log record xl_info
 * field. We use 3 for the opcode, and one about an optional flag variable.
 */
#define XLOG_XACT_COMMIT			0x00
#define XLOG_XACT_PREPARE			0x10
#define XLOG_XACT_ABORT				0x20
#define XLOG_XACT_COMMIT_PREPARED	0x30
#define XLOG_XACT_ABORT_PREPARED	0x40
#define XLOG_XACT_ASSIGNMENT		0x50
/* free opcode 0x60 */
/* free opcode 0x70 */

/* mask for filtering opcodes out of xl_info */
#define XLOG_XACT_OPMASK			0x70

typedef struct xl_xact_commit
{
	TimestampTz xact_time;		/* time of commit */

	/* xl_xact_xinfo follows if XLOG_XACT_HAS_INFO */
	/* xl_xact_dbinfo follows if XINFO_HAS_DBINFO */
	/* xl_xact_subxacts follows if XINFO_HAS_SUBXACT */
	/* xl_xact_relfilenodes follows if XINFO_HAS_RELFILENODES */
	/* xl_xact_invals follows if XINFO_HAS_INVALS */
	/* xl_xact_twophase follows if XINFO_HAS_TWOPHASE */
	/* xl_xact_origin follows if XINFO_HAS_ORIGIN, stored unaligned! */
} xl_xact_commit;

typedef struct xl_xact_abort
{
	TimestampTz xact_time;		/* time of abort */

	/* xl_xact_xinfo follows if XLOG_XACT_HAS_INFO */
	/* No db_info required */
	/* xl_xact_subxacts follows if HAS_SUBXACT */
	/* xl_xact_relfilenodes follows if HAS_RELFILENODES */
	/* No invalidation messages needed. */
	/* xl_xact_twophase follows if XINFO_HAS_TWOPHASE */
} xl_xact_abort;

#endif /* PG_PROBACKUP_H */
