/*-------------------------------------------------------------------------
 *
 * pg_probackup.h: Backup/Recovery manager for PostgreSQL.
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2017, Postgres Professional
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
#include "access/timeline.h"

#ifndef WIN32
#include <sys/mman.h>
#endif

/* Directory/File names */
#define DATABASE_DIR			"database"
#define BACKUPS_DIR				"backups"
#define PG_XLOG_DIR				"pg_xlog"
#define PG_TBLSPC_DIR			"pg_tblspc"
#define BACKUP_CONTROL_FILE		"backup.control"
#define BACKUP_CATALOG_CONF_FILE	"pg_probackup.conf"
#define BACKUP_CATALOG_PID		"pg_probackup.pid"
#define DATABASE_FILE_LIST		"backup_content.control"
#define PG_BACKUP_LABEL_FILE	"backup_label"
#define PG_BLACK_LIST			"black_list"
#define PG_TABLESPACE_MAP_FILE "tablespace_map"

/* Direcotry/File permission */
#define DIR_PERMISSION		(0700)
#define FILE_PERMISSION		(0600)

/* 64-bit xid support for PGPRO_EE */
#ifndef PGPRO_EE
#define XID_FMT "%u"
#endif

/* Information about single file (or dir) in backup */
typedef struct pgFile
{
	mode_t	mode;			/* protection (file type and permission) */
	size_t	size;			/* size of the file */
	size_t	read_size;		/* size of the portion read (if only some pages are
							   backed up, it's different from size) */
	size_t	write_size;		/* size of the backed-up file. BYTES_INVALID means
							   that the file existed but was not backed up
							   because not modified since last backup. */
	pg_crc32 crc;			/* CRC value of the file, regular file only */
	char	*linked;		/* path of the linked file */
	bool	is_datafile;	/* true if the file is PostgreSQL data file */
	char	*path;			/* path of the file */
	char	*ptrack_path;	/* path of the ptrack fork of the relation */
	int		segno;			/* Segment number for ptrack */
	uint64	generation;		/* Generation of the compressed file. Set to '-1'
							 * for non-compressed files. If generation has changed,
							 * we cannot backup compressed file partially. */
	int		is_partial_copy; /* for compressed files. Set to '1' if backed up
							  * via copy_file_partly() */
	volatile uint32 lock;	/* lock for synchronization of parallel threads  */
	datapagemap_t pagemap;	/* bitmap of pages updated since previous backup */
} pgFile;

/* Current state of backup */
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
	BACKUP_MODE_DIFF_PAGE,		/* incremental page backup */
	BACKUP_MODE_DIFF_PTRACK,	/* incremental page backup with ptrack system*/
	BACKUP_MODE_FULL			/* full backup */
} BackupMode;

typedef enum ProbackupSubcmd
{
	INIT = 0,
	BACKUP,
	RESTORE,
	VALIDATE,
	SHOW,
	DELETE,
	SET_CONFIG,
	SHOW_CONFIG
} ProbackupSubcmd;

/* special values of pgBackup fields */
#define INVALID_BACKUP_ID	 0
#define BYTES_INVALID		(-1)

typedef struct pgBackupConfig
{
	uint64		system_identifier;
	char		*pgdata;
	const char	*pgdatabase;
	const char	*pghost;
	const char	*pgport;
	const char	*pguser;

	uint32		retention_redundancy;
	uint32		retention_window;
} pgBackupConfig;

/* Information about single backup stored in backup.conf */
typedef struct pgBackup
{
	BackupMode		backup_mode; /* Mode - one of BACKUP_MODE_xxx above*/
	time_t			backup_id;	 /* Identifier of the backup.
								  * Currently it's the same as start_time */
	BackupStatus	status;		/* Status - one of BACKUP_STATUS_xxx above*/
	TimeLineID		tli; 		/* timeline of start and stop baskup lsns */
	XLogRecPtr		start_lsn;	/* backup's starting transaction log location */
	XLogRecPtr		stop_lsn;	/* backup's finishing transaction log location */
	time_t			start_time;	/* since this moment backup has status
								 * BACKUP_STATUS_RUNNING */
	time_t			end_time;	/* the moment when backup was finished, or the moment
								 * when we realized that backup is broken */
	time_t			recovery_time;	/* Earliest moment for which you can restore
									 * the state of the database cluster using
									 * this backup */
	TransactionId	recovery_xid;	/* Earliest xid for which you can restore
									 * the state of the database cluster using
									 * this backup */
	/*
	 * Amount of raw data. For a full backup, this is the total amount of
	 * data while for a differential backup this is just the difference
	 * of data taken.
	 * BYTES_INVALID means nothing was backed up.
	 */
	int64			data_bytes;

	/* Fields needed for compatibility check */
	uint32			block_size;
	uint32			wal_block_size;
	uint32			checksum_version;

	bool			stream; 		/* Was this backup taken in stream mode?
									 * i.e. does it include all needed WAL files? */
	time_t			parent_backup; 	/* Identifier of the previous backup.
									 * Which is basic backup for this
									 * incremental backup. */
} pgBackup;

/* Recovery target for restore and validate subcommands */
typedef struct pgRecoveryTarget
{
	bool			time_specified;
	time_t			recovery_target_time;
	bool			xid_specified;
	TransactionId	recovery_target_xid;
	bool			recovery_target_inclusive;
} pgRecoveryTarget;

/* Union to ease operations on relation pages */
typedef union DataPage
{
	PageHeaderData	page_data;
	char			data[BLCKSZ];
} DataPage;

/*
 * This struct and function definitions mirror ones from cfs.h, but doesn't use
 * atomic variables, since they are not allowed in frontend code.
 */
typedef struct
{
	uint32 physSize;
	uint32 virtSize;
	uint32 usedSize;
	uint32 lock;
	pid_t	postmasterPid;
	uint64	generation;
	uint64	inodes[RELSEG_SIZE];
} FileMap;

extern FileMap* cfs_mmap(int md);
extern int cfs_munmap(FileMap* map);

/*
 * return pointer that exceeds the length of prefix from character string.
 * ex. str="/xxx/yyy/zzz", prefix="/xxx/yyy", return="zzz".
 */
#define GetRelativePath(str, prefix) \
	((strlen(str) <= strlen(prefix)) ? "" : str + strlen(prefix) + 1)

/*
 * Return timeline, xlog ID and record offset from an LSN of the type
 * 0/B000188, usual result from pg_stop_backup() and friends.
 */
#define XLogDataFromLSN(data, xlogid, xrecoff)		\
	sscanf(data, "%X/%X", xlogid, xrecoff)

/* in probackup.c */

/* path configuration */
extern char *backup_path;
extern char *pgdata;
extern char arclog_path[MAXPGPATH];

/* current settings */
extern pgBackup current;
extern ProbackupSubcmd	backup_subcmd;

extern bool	smooth_checkpoint;
extern int num_threads;
extern bool stream_wal;
extern bool from_replica;
extern bool progress;
extern bool delete_wal;
extern bool	delete_expired;
extern bool	apply_to_all;
extern bool	force_delete;
extern uint32 archive_timeout;

extern uint64 system_identifier;

extern uint32 retention_redundancy;
extern uint32 retention_window;

/* in dir.c */
/* exclude directory list for $PGDATA file listing */
extern const char *pgdata_exclude_dir[];

/* in backup.c */
extern int do_backup(void);
extern BackupMode parse_backup_mode(const char *value);
extern bool fileExists(const char *path);
extern void process_block_change(ForkNumber forknum, RelFileNode rnode,
								 BlockNumber blkno);

/* in restore.c */
extern int do_restore_or_validate(time_t target_backup_id,
					  const char *target_time,
					  const char *target_xid,
					  const char *target_inclusive,
					  TimeLineID target_tli,
					  bool is_restore);
extern bool satisfy_timeline(const parray *timelines, const pgBackup *backup);
extern bool satisfy_recovery_target(const pgBackup *backup,
									const pgRecoveryTarget *rt);
extern parray * readTimeLineHistory_probackup(TimeLineID targetTLI);
extern pgRecoveryTarget *parseRecoveryTargetOptions(
	const char *target_time,
	const char *target_xid,
	const char *target_inclusive);

extern void opt_tablespace_map(pgut_option *opt, const char *arg);

/* in init.c */
extern int do_init(void);

/* in configure.c */
extern int do_configure(bool show_only);
extern void pgBackupConfigInit(pgBackupConfig *config);
extern void writeBackupCatalogConfig(FILE *out, pgBackupConfig *config);
extern void writeBackupCatalogConfigFile(pgBackupConfig *config);
extern pgBackupConfig* readBackupCatalogConfigFile(void);

/* in show.c */
extern int do_show(time_t requested_backup_id);

/* in delete.c */
extern int do_delete(time_t backup_id);
extern int do_retention_purge(void);

/* in fetch.c */
extern char *slurpFile(const char *datadir,
					   const char *path,
					   size_t *filesize,
					   bool safe);

/* in help.c */
extern void help_pg_probackup(void);
extern void help_command(char *command);

/* in validate.c */
extern void pgBackupValidate(pgBackup* backup);

/* in catalog.c */
extern pgBackup *read_backup(time_t timestamp);
extern const char *pgBackupGetBackupMode(pgBackup *backup);

extern parray *catalog_get_backup_list(time_t requested_backup_id);
extern pgBackup *catalog_get_last_data_backup(parray *backup_list,
											  TimeLineID tli);
extern void catalog_lock(void);
extern void pgBackupWriteControl(FILE *out, pgBackup *backup);
extern void pgBackupWriteBackupControlFile(pgBackup *backup);
extern void pgBackupGetPath(const pgBackup *backup, char *path, size_t len, const char *subdir);
extern int pgBackupCreateDir(pgBackup *backup);
extern void pgBackupFree(void *backup);
extern int pgBackupCompareId(const void *f1, const void *f2);
extern int pgBackupCompareIdDesc(const void *f1, const void *f2);

/* in dir.c */
extern void dir_list_file(parray *files, const char *root, bool exclude,
						  bool omit_symlink, bool add_root);
extern void list_data_directories(parray *files, const char *path,
								  bool is_root, bool exclude);

extern void read_tablespace_map(parray *files, const char *backup_dir);

extern void print_file_list(FILE *out, const parray *files, const char *root);
extern parray *dir_read_file_list(const char *root, const char *file_txt);

extern int dir_create_dir(const char *path, mode_t mode);
extern bool dir_is_empty(const char *path);

extern pgFile *pgFileNew(const char *path, bool omit_symlink);
extern pgFile *pgFileInit(const char *path);
extern void pgFileDelete(pgFile *file);
extern void pgFileFree(void *file);
extern pg_crc32 pgFileGetCRC(pgFile *file);
extern int pgFileComparePath(const void *f1, const void *f2);
extern int pgFileComparePathDesc(const void *f1, const void *f2);
extern int pgFileCompareLinked(const void *f1, const void *f2);
extern int pgFileCompareSize(const void *f1, const void *f2);

/* in data.c */
extern bool backup_data_file(const char *from_root, const char *to_root,
							 pgFile *file, XLogRecPtr prev_backup_start_lsn);
extern void restore_data_file(const char *from_root, const char *to_root,
							  pgFile *file, pgBackup *backup);
extern void restore_compressed_file(const char *from_root,
									const char *to_root, pgFile *file);
extern bool is_compressed_data_file(pgFile *file);
extern bool backup_compressed_file_partially(pgFile *file,
											 void *arg,
											 size_t *skip_size);
extern bool copy_file(const char *from_root, const char *to_root,
					  pgFile *file);
extern bool copy_file_partly(const char *from_root, const char *to_root,
				 pgFile *file, size_t skip_size);

extern bool calc_file_checksum(pgFile *file);

/* parsexlog.c */
extern void extractPageMap(const char *datadir,
						   XLogRecPtr startpoint,
						   TimeLineID tli,
						   XLogRecPtr endpoint, bool prev_segno);
extern void validate_wal(pgBackup *backup,
						 const char *archivedir,
						 time_t target_time,
						 TransactionId target_xid,
						 TimeLineID tli);
extern bool read_recovery_info(const char *archivedir, TimeLineID tli,
							   XLogRecPtr start_lsn, XLogRecPtr stop_lsn,
							   time_t *recovery_time,
							   TransactionId *recovery_xid);
extern bool wal_contains_lsn(const char *archivedir, XLogRecPtr target_lsn,
							 TimeLineID target_tli);

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
extern uint64 get_system_identifier(void);
extern pg_time_t timestamptz_to_time_t(TimestampTz t);
extern void pgBackup_init(pgBackup *backup);

/* in status.c */
extern bool is_pg_running(void);

#endif /* PG_PROBACKUP_H */
