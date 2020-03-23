/*-------------------------------------------------------------------------
 *
 * pg_probackup.h: Backup/Recovery manager for PostgreSQL.
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROBACKUP_H
#define PG_PROBACKUP_H

#include "postgres_fe.h"
#include "libpq-fe.h"
#include "libpq-int.h"

#include "access/xlog_internal.h"
#include "utils/pg_crc.h"

#if PG_VERSION_NUM >= 120000
#include "common/logging.h"
#endif

#ifdef FRONTEND
#undef FRONTEND
#include <port/atomics.h>
#define FRONTEND
#else
#include <port/atomics.h>
#endif

#include "utils/configuration.h"
#include "utils/logger.h"
#include "utils/remote.h"
#include "utils/parray.h"
#include "utils/pgut.h"
#include "utils/file.h"

#include "datapagemap.h"

/* pgut client variables and full path */
extern const char  *PROGRAM_NAME;
extern const char  *PROGRAM_NAME_FULL;
extern const char  *PROGRAM_FULL_PATH;
extern const char  *PROGRAM_URL;
extern const char  *PROGRAM_EMAIL;

/* Directory/File names */
#define DATABASE_DIR				"database"
#define BACKUPS_DIR				"backups"
#if PG_VERSION_NUM >= 100000
#define PG_XLOG_DIR				"pg_wal"
#define PG_LOG_DIR 				"log"
#else
#define PG_XLOG_DIR				"pg_xlog"
#define PG_LOG_DIR 				"pg_log"
#endif
#define PG_TBLSPC_DIR			"pg_tblspc"
#define PG_GLOBAL_DIR			"global"
#define BACKUP_CONTROL_FILE		"backup.control"
#define BACKUP_CATALOG_CONF_FILE	"pg_probackup.conf"
#define BACKUP_CATALOG_PID		"backup.pid"
#define DATABASE_FILE_LIST		"backup_content.control"
#define PG_BACKUP_LABEL_FILE	"backup_label"
#define PG_TABLESPACE_MAP_FILE "tablespace_map"
#define EXTERNAL_DIR			"external_directories/externaldir"
#define DATABASE_MAP			"database_map"

/* Timeout defaults */
#define PARTIAL_WAL_TIMER			60
#define ARCHIVE_TIMEOUT_DEFAULT		300
#define REPLICA_TIMEOUT_DEFAULT		300

/* Directory/File permission */
#define DIR_PERMISSION		(0700)
#define FILE_PERMISSION		(0600)

/* 64-bit xid support for PGPRO_EE */
#ifndef PGPRO_EE
#define XID_FMT "%u"
#endif

#ifndef STDIN_FILENO
#define STDIN_FILENO 0
#define STDOUT_FILENO 1
#endif

/* stdio buffer size */
#define STDIO_BUFSIZE 65536

/* retry attempts */
#define PAGE_READ_ATTEMPTS 100

/* Check if an XLogRecPtr value is pointed to 0 offset */
#define XRecOffIsNull(xlrp) \
		((xlrp) % XLOG_BLCKSZ == 0)

typedef struct db_map_entry
{
	Oid dbOid;
	char *datname;
} db_map_entry;

typedef enum PartialRestoreType
{
	NONE,
	INCLUDE,
	EXCLUDE,
} PartialRestoreType;

typedef enum CompressAlg
{
	NOT_DEFINED_COMPRESS = 0,
	NONE_COMPRESS,
	PGLZ_COMPRESS,
	ZLIB_COMPRESS,
} CompressAlg;

#define INIT_FILE_CRC32(use_crc32c, crc) \
do { \
	if (use_crc32c) \
		INIT_CRC32C(crc); \
	else \
		INIT_TRADITIONAL_CRC32(crc); \
} while (0)
#define COMP_FILE_CRC32(use_crc32c, crc, data, len) \
do { \
	if (use_crc32c) \
		COMP_CRC32C((crc), (data), (len)); \
	else \
		COMP_TRADITIONAL_CRC32(crc, data, len); \
} while (0)
#define FIN_FILE_CRC32(use_crc32c, crc) \
do { \
	if (use_crc32c) \
		FIN_CRC32C(crc); \
	else \
		FIN_TRADITIONAL_CRC32(crc); \
} while (0)


/* Information about single file (or dir) in backup */
typedef struct pgFile
{
	char   *name;			/* file or directory name */
	mode_t	mode;			/* protection (file type and permission) */
	size_t	size;			/* size of the file */
	time_t  mtime;			/* file st_mtime attribute, can be used only
								during backup */
	size_t	read_size;		/* size of the portion read (if only some pages are
							   backed up, it's different from size) */
	int64	write_size;		/* size of the backed-up file. BYTES_INVALID means
							   that the file existed but was not backed up
							   because not modified since last backup. */
	int64	uncompressed_size;	/* size of the backed-up file before compression
								 * and adding block headers.
								 */
							/* we need int64 here to store '-1' value */
	pg_crc32 crc;			/* CRC value of the file, regular file only */
	char   *linked;			/* path of the linked file */
	bool	is_datafile;	/* true if the file is PostgreSQL data file */
	char   *path;			/* absolute path of the file */
	char   *rel_path;		/* relative path of the file */
	Oid		tblspcOid;		/* tblspcOid extracted from path, if applicable */
	Oid		dbOid;			/* dbOid extracted from path, if applicable */
	Oid		relOid;			/* relOid extracted from path, if applicable */
	char   *forkName;		/* forkName extracted from path, if applicable */
	int		segno;			/* Segment number for ptrack */
	int		n_blocks;		/* size of the file in blocks, readed during DELTA backup */
	bool	is_cfs;			/* Flag to distinguish files compressed by CFS*/
	bool	is_database;
	int		external_dir_num;	/* Number of external directory. 0 if not external */
	bool	exists_in_prev;		/* Mark files, both data and regular, that exists in previous backup */
	CompressAlg		compress_alg;		/* compression algorithm applied to the file */
	volatile 		pg_atomic_flag lock;/* lock for synchronization of parallel threads  */
	datapagemap_t	pagemap;			/* bitmap of pages updated since previous backup
										   may take up to 16kB per file */
	bool			pagemap_isabsent;	/* Used to mark files with unknown state of pagemap,
										 * i.e. datafiles without _ptrack */
} pgFile;

typedef struct page_map_entry
{
	const char	*path;		/* file or directory name */
	char		*pagemap;
	size_t		 pagemapsize;
} page_map_entry;

/* Special values of datapagemap_t bitmapsize */
#define PageBitmapIsEmpty 0		/* Used to mark unchanged datafiles */

/* Current state of backup */
typedef enum BackupStatus
{
	BACKUP_STATUS_INVALID,		/* the pgBackup is invalid */
	BACKUP_STATUS_OK,			/* completed backup */
	BACKUP_STATUS_ERROR,		/* aborted because of unexpected error */
	BACKUP_STATUS_RUNNING,		/* running backup */
	BACKUP_STATUS_MERGING,		/* merging backups */
	BACKUP_STATUS_MERGED,		/* backup has been successfully merged and now awaits
								 * the assignment of new start_time */
	BACKUP_STATUS_DELETING,		/* data files are being deleted */
	BACKUP_STATUS_DELETED,		/* data files have been deleted */
	BACKUP_STATUS_DONE,			/* completed but not validated yet */
	BACKUP_STATUS_ORPHAN,		/* backup validity is unknown but at least one parent backup is corrupted */
	BACKUP_STATUS_CORRUPT		/* files are corrupted, not available */
} BackupStatus;

typedef enum BackupMode
{
	BACKUP_MODE_INVALID = 0,
	BACKUP_MODE_DIFF_PAGE,		/* incremental page backup */
	BACKUP_MODE_DIFF_PTRACK,	/* incremental page backup with ptrack system */
	BACKUP_MODE_DIFF_DELTA,		/* incremental page backup with lsn comparison */
	BACKUP_MODE_FULL			/* full backup */
} BackupMode;

typedef enum ShowFormat
{
	SHOW_PLAIN,
	SHOW_JSON
} ShowFormat;


/* special values of pgBackup fields */
#define INVALID_BACKUP_ID	0    /* backup ID is not provided by user */
#define BYTES_INVALID		(-1) /* file didn`t changed since previous backup, DELTA backup do not rely on it */
#define FILE_NOT_FOUND		(-2) /* file disappeared during backup */
#define BLOCKNUM_INVALID	(-1)
#define PROGRAM_VERSION	"2.2.8"
#define AGENT_PROTOCOL_VERSION 20208


typedef struct ConnectionOptions
{
	const char *pgdatabase;
	const char *pghost;
	const char *pgport;
	const char *pguser;
} ConnectionOptions;

typedef struct ConnectionArgs
{
	PGconn	   *conn;
	PGcancel   *cancel_conn;
} ConnectionArgs;

/* Store values for --remote-* option for 'restore_command' constructor */
typedef struct ArchiveOptions
{
	const char *host;
	const char *port;
	const char *user;
} ArchiveOptions;

/*
 * An instance configuration. It can be stored in a configuration file or passed
 * from command line.
 */
typedef struct InstanceConfig
{
	char		*name;
	char		arclog_path[MAXPGPATH];
	char		backup_instance_path[MAXPGPATH];

	uint64		system_identifier;
	uint32		xlog_seg_size;

	char	   *pgdata;
	char	   *external_dir_str;

	ConnectionOptions conn_opt;
	ConnectionOptions master_conn_opt;

	uint32		replica_timeout;

	/* Wait timeout for WAL segment archiving */
	uint32		archive_timeout;

	/* cmdline to be used as restore_command */
	char	   *restore_command;

	/* Logger parameters */
	LoggerConfig logger;

	/* Remote access parameters */
	RemoteConfig remote;

	/* Retention options. 0 disables the option. */
	uint32		retention_redundancy;
	uint32		retention_window;
	uint32		wal_depth;

	CompressAlg	compress_alg;
	int			compress_level;

	/* Archive description */
	ArchiveOptions archive;
} InstanceConfig;

extern ConfigOption instance_options[];
extern InstanceConfig instance_config;
extern time_t current_time;

typedef struct PGNodeInfo
{
	uint32			block_size;
	uint32			wal_block_size;
	uint32			checksum_version;
	bool			is_superuser;
	bool			pgpro_support;

	int				server_version;
	char			server_version_str[100];

	int				ptrack_version_num;
	bool			is_ptrack_enable;
	const char		*ptrack_schema; /* used only for ptrack 2.x */

} PGNodeInfo;

typedef struct pgBackup pgBackup;

/* Information about single backup stored in backup.conf */
struct pgBackup
{
	BackupMode		backup_mode; /* Mode - one of BACKUP_MODE_xxx above*/
	time_t			backup_id;	 /* Identifier of the backup.
								  * Currently it's the same as start_time */
	BackupStatus	status;		/* Status - one of BACKUP_STATUS_xxx above*/
	TimeLineID		tli; 		/* timeline of start and stop backup lsns */
	XLogRecPtr		start_lsn;	/* backup's starting transaction log location */
	XLogRecPtr		stop_lsn;	/* backup's finishing transaction log location */
	time_t			start_time;	/* since this moment backup has status
								 * BACKUP_STATUS_RUNNING */
	time_t			merge_dest_backup;	/* start_time of incremental backup,
									 * this backup is merging with.
									 * Only available for FULL backups
									 * with MERGING or MERGED statuses */
	time_t			merge_time; /* the moment when merge was started or 0 */
	time_t			end_time;	/* the moment when backup was finished, or the moment
								 * when we realized that backup is broken */
	time_t			recovery_time;	/* Earliest moment for which you can restore
									 * the state of the database cluster using
									 * this backup */
	time_t			expire_time;	/* Backup expiration date */
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
	/* Size of WAL files needed to replay on top of this
	 * backup to reach the consistency.
	 */
	int64			wal_bytes;
	/* Size of data files before applying compression and block header,
	 * WAL files are not included.
	 */
	int64			uncompressed_bytes;

	/* Size of data files in PGDATA at the moment of backup. */
	int64			pgdata_bytes;

	CompressAlg		compress_alg;
	int				compress_level;

	/* Fields needed for compatibility check */
	uint32			block_size;
	uint32			wal_block_size;
	uint32			checksum_version;
	char			program_version[100];
	char			server_version[100];

	bool			stream;			/* Was this backup taken in stream mode?
									 * i.e. does it include all needed WAL files? */
	bool			from_replica;	/* Was this backup taken from replica */
	time_t			parent_backup; 	/* Identifier of the previous backup.
									 * Which is basic backup for this
									 * incremental backup. */
	pgBackup		*parent_backup_link;
	char			*primary_conninfo; /* Connection parameters of the backup
										* in the format suitable for recovery.conf */
	char			*external_dir_str;	/* List of external directories,
										 * separated by ':' */
	char			*root_dir;		/* Full path for root backup directory:
									   backup_path/instance_name/backup_id */
	parray			*files;			/* list of files belonging to this backup
									 * must be populated explicitly */
};

/* Recovery target for restore and validate subcommands */
typedef struct pgRecoveryTarget
{
	time_t			target_time;
	/* add one more field in order to avoid deparsing target_time back */
	const char	   *time_string;
	TransactionId	target_xid;
	/* add one more field in order to avoid deparsing target_xid back */
	const char	   *xid_string;
	XLogRecPtr		target_lsn;
	/* add one more field in order to avoid deparsing target_lsn back */
	const char	   *lsn_string;
	TimeLineID		target_tli;
	bool			target_inclusive;
	bool			inclusive_specified;
	const char	   *target_stop;
	const char	   *target_name;
	const char	   *target_action;
} pgRecoveryTarget;

/* Options needed for restore and validate commands */
typedef struct pgRestoreParams
{
	bool	force;
	bool	is_restore;
	bool	no_validate;
	bool	restore_as_replica;
	bool	skip_external_dirs;
	bool	skip_block_validation; //Start using it
	const char *restore_command;
	const char *primary_slot_name;

	/* options for partial restore */
	PartialRestoreType partial_restore_type;
	parray *partial_db_list;
	const char *primary_conninfo;
} pgRestoreParams;

/* Options needed for set-backup command */
typedef struct pgSetBackupParams
{
	int64	ttl; /* amount of time backup must be pinned
				  * -1 - do nothing
				  * 0 - disable pinning
				  */
	time_t	expire_time; /* Point in time before which backup
						  * must be pinned.
						  */
} pgSetBackupParams;

typedef struct
{
	PGNodeInfo *nodeInfo;

	const char *from_root;
	const char *to_root;
	const char *external_prefix;

	parray	   *files_list;
	parray	   *prev_filelist;
	parray	   *external_dirs;
	XLogRecPtr	prev_start_lsn;

	ConnectionArgs conn_arg;
	int			thread_num;

	/*
	 * Return value from the thread.
	 * 0 means there is no error, 1 - there is an error.
	 */
	int			ret;
} backup_files_arg;


typedef struct timelineInfo timelineInfo;

/* struct to collect info about timelines in WAL archive */
struct timelineInfo {

	TimeLineID tli;			/* this timeline */
	TimeLineID parent_tli;  /* parent timeline. 0 if none */
	timelineInfo *parent_link; /* link to parent timeline */
	XLogRecPtr switchpoint;	   /* if this timeline has a parent, then
								* switchpoint contains switchpoint LSN,
								* otherwise 0 */
	XLogSegNo begin_segno;	/* first present segment in this timeline */
	XLogSegNo end_segno;	/* last present segment in this timeline */
	size_t	n_xlog_files;	/* number of segments (only really existing)
							 * does not include lost segments */
	size_t	size;			/* space on disk taken by regular WAL files */
	parray *backups;		/* array of pgBackup sturctures with info
							 * about backups belonging to this timeline */
	parray *xlog_filelist;	/* array of ordinary WAL segments, '.partial'
							 * and '.backup' files belonging to this timeline */
	parray *lost_segments;	/* array of intervals of lost segments */
	parray *keep_segments;	/* array of intervals of segments used by WAL retention */
	pgBackup *closest_backup; /* link to valid backup, closest to timeline */
	pgBackup *oldest_backup; /* link to oldest backup on timeline */
	XLogRecPtr anchor_lsn; /* LSN belonging to the oldest segno to keep for 'wal-depth' */
	TimeLineID anchor_tli;	/* timeline of anchor_lsn */
};

typedef struct xlogInterval
{
	XLogSegNo begin_segno;
	XLogSegNo end_segno;
} xlogInterval;

typedef struct lsnInterval
{
	TimeLineID tli;
	XLogRecPtr begin_lsn;
	XLogRecPtr end_lsn;
} lsnInterval;

typedef enum xlogFileType
{
	SEGMENT,
	PARTIAL_SEGMENT,
	BACKUP_HISTORY_FILE
} xlogFileType;

typedef struct xlogFile
{
	pgFile file;
	XLogSegNo segno;
	xlogFileType type;
	bool keep; /* Used to prevent removal of WAL segments
				* required by ARCHIVE backups.
				*/
} xlogFile;


/*
 * When copying datafiles to backup we validate and compress them block
 * by block. Thus special header is required for each data block.
 */
typedef struct BackupPageHeader
{
	BlockNumber	block;			/* block number */
	int32		compressed_size;
} BackupPageHeader;

/* Special value for compressed_size field */
#define PageIsOk		 0
#define SkipCurrentPage -1
#define PageIsTruncated -2
#define PageIsCorrupted -3 /* used by checkdb */


/*
 * return pointer that exceeds the length of prefix from character string.
 * ex. str="/xxx/yyy/zzz", prefix="/xxx/yyy", return="zzz".
 *
 * Deprecated. Do not use this in new code.
 */
#define GetRelativePath(str, prefix) \
	((strlen(str) <= strlen(prefix)) ? "" : str + strlen(prefix) + 1)

/*
 * Return timeline, xlog ID and record offset from an LSN of the type
 * 0/B000188, usual result from pg_stop_backup() and friends.
 */
#define XLogDataFromLSN(data, xlogid, xrecoff)		\
	sscanf(data, "%X/%X", xlogid, xrecoff)

#define IsCompressedXLogFileName(fname) \
	(strlen(fname) == XLOG_FNAME_LEN + strlen(".gz") &&			\
	 strspn(fname, "0123456789ABCDEF") == XLOG_FNAME_LEN &&		\
	 strcmp((fname) + XLOG_FNAME_LEN, ".gz") == 0)

#if PG_VERSION_NUM >= 110000
#define GetXLogSegNo(xlrp, logSegNo, wal_segsz_bytes) \
	XLByteToSeg(xlrp, logSegNo, wal_segsz_bytes)
#define GetXLogRecPtr(segno, offset, wal_segsz_bytes, dest) \
	XLogSegNoOffsetToRecPtr(segno, offset, wal_segsz_bytes, dest)
#define GetXLogFileName(fname, tli, logSegNo, wal_segsz_bytes) \
	XLogFileName(fname, tli, logSegNo, wal_segsz_bytes)
#define IsInXLogSeg(xlrp, logSegNo, wal_segsz_bytes) \
	XLByteInSeg(xlrp, logSegNo, wal_segsz_bytes)
#define GetXLogSegName(fname, logSegNo, wal_segsz_bytes)	\
	snprintf(fname, 20, "%08X%08X",		\
			 (uint32) ((logSegNo) / XLogSegmentsPerXLogId(wal_segsz_bytes)), \
			 (uint32) ((logSegNo) % XLogSegmentsPerXLogId(wal_segsz_bytes)))

#define GetXLogSegNoFromScrath(logSegNo, log, seg, wal_segsz_bytes)	\
		logSegNo = (uint64) log * XLogSegmentsPerXLogId(wal_segsz_bytes) + seg
#else
#define GetXLogSegNo(xlrp, logSegNo, wal_segsz_bytes) \
	XLByteToSeg(xlrp, logSegNo)
#define GetXLogRecPtr(segno, offset, wal_segsz_bytes, dest) \
	XLogSegNoOffsetToRecPtr(segno, offset, dest)
#define GetXLogFileName(fname, tli, logSegNo, wal_segsz_bytes) \
	XLogFileName(fname, tli, logSegNo)
#define IsInXLogSeg(xlrp, logSegNo, wal_segsz_bytes) \
	XLByteInSeg(xlrp, logSegNo)
#define GetXLogSegName(fname, logSegNo, wal_segsz_bytes) \
	snprintf(fname, 20, "%08X%08X",\
			 (uint32) ((logSegNo) / XLogSegmentsPerXLogId), \
			 (uint32) ((logSegNo) % XLogSegmentsPerXLogId))

#define GetXLogSegNoFromScrath(logSegNo, log, seg, wal_segsz_bytes)	\
		logSegNo = (uint64) log * XLogSegmentsPerXLogId + seg
#endif

#define IsSshProtocol() (instance_config.remote.host && strcmp(instance_config.remote.proto, "ssh") == 0)

/* directory options */
extern char	   *backup_path;
extern char		backup_instance_path[MAXPGPATH];
extern char		arclog_path[MAXPGPATH];

/* common options */
extern int		num_threads;
extern bool		stream_wal;
extern bool		progress;
#if PG_VERSION_NUM >= 100000
/* In pre-10 'replication_slot' is defined in receivelog.h */
extern char	   *replication_slot;
#endif
extern bool 	temp_slot;

/* backup options */
extern bool		smooth_checkpoint;

/* remote probackup options */
extern char* remote_agent;

extern bool exclusive_backup;

/* delete options */
extern bool		delete_wal;
extern bool		delete_expired;
extern bool		merge_expired;
extern bool		dry_run;

/* compression options */
extern bool		compress_shortcut;

/* other options */
extern char *instance_name;

/* show options */
extern ShowFormat show_format;

/* checkdb options */
extern bool heapallindexed;
extern bool skip_block_validation;

/* current settings */
extern pgBackup current;

/* argv of the process */
extern char** commands_args;

/* in dir.c */
/* exclude directory list for $PGDATA file listing */
extern const char *pgdata_exclude_dir[];

/* in backup.c */
extern int do_backup(time_t start_time, bool no_validate,
					 pgSetBackupParams *set_backup_params, bool no_sync);
extern void do_checkdb(bool need_amcheck, ConnectionOptions conn_opt,
				  char *pgdata);
extern BackupMode parse_backup_mode(const char *value);
extern const char *deparse_backup_mode(BackupMode mode);
extern void process_block_change(ForkNumber forknum, RelFileNode rnode,
								 BlockNumber blkno);

extern char *pg_ptrack_get_block(ConnectionArgs *arguments,
								 Oid dbOid, Oid tblsOid, Oid relOid,
								 BlockNumber blknum, size_t *result_size,
								 int ptrack_version_num, const char *ptrack_schema);
/* in restore.c */
extern int do_restore_or_validate(time_t target_backup_id,
					  pgRecoveryTarget *rt,
					  pgRestoreParams *params,
					  bool no_sync);
extern bool satisfy_timeline(const parray *timelines, const pgBackup *backup);
extern bool satisfy_recovery_target(const pgBackup *backup,
									const pgRecoveryTarget *rt);
extern pgRecoveryTarget *parseRecoveryTargetOptions(
	const char *target_time, const char *target_xid,
	const char *target_inclusive, TimeLineID target_tli, const char* target_lsn,
	const char *target_stop, const char *target_name,
	const char *target_action);

extern parray *get_dbOid_exclude_list(pgBackup *backup, parray *datname_list,
										PartialRestoreType partial_restore_type);

extern parray *get_backup_filelist(pgBackup *backup);
extern parray *read_timeline_history(const char *arclog_path, TimeLineID targetTLI);

/* in merge.c */
extern void do_merge(time_t backup_id);
extern void merge_backups(pgBackup *backup, pgBackup *next_backup);
extern void merge_chain(parray *parent_chain,
						pgBackup *full_backup, pgBackup *dest_backup);

extern parray *read_database_map(pgBackup *backup);

/* in init.c */
extern int do_init(void);
extern int do_add_instance(InstanceConfig *instance);

/* in archive.c */
extern int do_archive_push(InstanceConfig *instance, char *wal_file_path,
						   char *wal_file_name, bool overwrite);
extern int do_archive_get(InstanceConfig *instance, char *wal_file_path,
						  char *wal_file_name);

/* in configure.c */
extern void do_show_config(void);
extern void do_set_config(bool missing_ok);
extern void init_config(InstanceConfig *config, const char *instance_name);
extern InstanceConfig *readInstanceConfigFile(const char *instance_name);

/* in show.c */
extern int do_show(const char *instance_name, time_t requested_backup_id, bool show_archive);

/* in delete.c */
extern void do_delete(time_t backup_id);
extern void delete_backup_files(pgBackup *backup);
extern int do_retention(void);
extern int do_delete_instance(void);

/* in fetch.c */
extern char *slurpFile(const char *datadir,
					   const char *path,
					   size_t *filesize,
					   bool safe,
					   fio_location location);
extern char *fetchFile(PGconn *conn, const char *filename, size_t *filesize);

/* in help.c */
extern void help_pg_probackup(void);
extern void help_command(char *command);

/* in validate.c */
extern void pgBackupValidate(pgBackup* backup, pgRestoreParams *params);
extern int do_validate_all(void);
extern int validate_one_page(Page page, BlockNumber absolute_blkno,
							 XLogRecPtr stop_lsn, XLogRecPtr *page_lsn,
							 uint32 checksum_version);

/* return codes for validate_one_page */
/* TODO: use enum */
#define PAGE_IS_VALID (-1)
#define PAGE_IS_NOT_FOUND (-2)
#define PAGE_IS_ZEROED (-3)
#define PAGE_HEADER_IS_INVALID (-4)
#define PAGE_CHECKSUM_MISMATCH (-5)
#define PAGE_LSN_FROM_FUTURE (-6)

/* in catalog.c */
extern pgBackup *read_backup(const char *instance_name, time_t timestamp);
extern void write_backup(pgBackup *backup);
extern void write_backup_status(pgBackup *backup, BackupStatus status,
								const char *instance_name);
extern void write_backup_data_bytes(pgBackup *backup);
extern bool lock_backup(pgBackup *backup);

extern const char *pgBackupGetBackupMode(pgBackup *backup);

extern parray *catalog_get_instance_list(void);
extern parray *catalog_get_backup_list(const char *instance_name, time_t requested_backup_id);
extern void catalog_lock_backup_list(parray *backup_list, int from_idx,
									 int to_idx);
extern pgBackup *catalog_get_last_data_backup(parray *backup_list,
											  TimeLineID tli,
											  time_t current_start_time);
extern pgBackup *get_multi_timeline_parent(parray *backup_list, parray *tli_list,
	                      TimeLineID current_tli, time_t current_start_time,
						  InstanceConfig *instance);
extern void timelineInfoFree(void *tliInfo);
extern parray *catalog_get_timelines(InstanceConfig *instance);
extern void do_set_backup(const char *instance_name, time_t backup_id,
							pgSetBackupParams *set_backup_params);
extern bool pin_backup(pgBackup	*target_backup,
							pgSetBackupParams *set_backup_params);
extern void pgBackupWriteControl(FILE *out, pgBackup *backup);
extern void write_backup_filelist(pgBackup *backup, parray *files,
								  const char *root, parray *external_list);

extern void pgBackupGetPath(const pgBackup *backup, char *path, size_t len,
							const char *subdir);
extern void pgBackupGetPath2(const pgBackup *backup, char *path, size_t len,
							 const char *subdir1, const char *subdir2);
extern void pgBackupGetPathInInstance(const char *instance_name,
				 const pgBackup *backup, char *path, size_t len,
				 const char *subdir1, const char *subdir2);
extern int pgBackupCreateDir(pgBackup *backup);
extern void pgNodeInit(PGNodeInfo *node);
extern void pgBackupInit(pgBackup *backup);
extern void pgBackupFree(void *backup);
extern int pgBackupCompareId(const void *f1, const void *f2);
extern int pgBackupCompareIdDesc(const void *f1, const void *f2);
extern int pgBackupCompareIdEqual(const void *l, const void *r);

extern pgBackup* find_parent_full_backup(pgBackup *current_backup);
extern int scan_parent_chain(pgBackup *current_backup, pgBackup **result_backup);
extern bool is_parent(time_t parent_backup_time, pgBackup *child_backup, bool inclusive);
extern bool is_prolific(parray *backup_list, pgBackup *target_backup);
extern bool in_backup_list(parray *backup_list, pgBackup *target_backup);
extern int get_backup_index_number(parray *backup_list, pgBackup *backup);
extern bool launch_agent(void);
extern void launch_ssh(char* argv[]);
extern void wait_ssh(void);

#define COMPRESS_ALG_DEFAULT NOT_DEFINED_COMPRESS
#define COMPRESS_LEVEL_DEFAULT 1

extern CompressAlg parse_compress_alg(const char *arg);
extern const char* deparse_compress_alg(int alg);

/* in dir.c */
extern void dir_list_file(parray *files, const char *root, bool exclude,
						  bool follow_symlink, bool add_root,
						  int external_dir_num, fio_location location);

extern void create_data_directories(parray *dest_files,
										const char *data_dir,
										const char *backup_dir,
										bool extract_tablespaces,
										fio_location location);

extern void read_tablespace_map(parray *files, const char *backup_dir);
extern void opt_tablespace_map(ConfigOption *opt, const char *arg);
extern void opt_externaldir_map(ConfigOption *opt, const char *arg);
extern void check_tablespace_mapping(pgBackup *backup);
extern void check_external_dir_mapping(pgBackup *backup);
extern char *get_external_remap(char *current_dir);

extern void print_database_map(FILE *out, parray *database_list);
extern void write_database_map(pgBackup *backup, parray *database_list,
								   parray *backup_file_list);
extern void db_map_entry_free(void *map);

extern void print_file_list(FILE *out, const parray *files, const char *root,
							const char *external_prefix, parray *external_list);
extern parray *dir_read_file_list(const char *root, const char *external_prefix,
								  const char *file_txt, fio_location location);
extern parray *make_external_directory_list(const char *colon_separated_dirs,
											bool remap);
extern void free_dir_list(parray *list);
extern void makeExternalDirPathByNum(char *ret_path, const char *pattern_path,
									 const int dir_num);
extern bool backup_contains_external(const char *dir, parray *dirs_list);

extern int dir_create_dir(const char *path, mode_t mode);
extern bool dir_is_empty(const char *path, fio_location location);

extern bool fileExists(const char *path, fio_location location);
extern size_t pgFileSize(const char *path);

extern pgFile *pgFileNew(const char *path, const char *rel_path,
						 bool follow_symlink, int external_dir_num,
						 fio_location location);
extern pgFile *pgFileInit(const char *path, const char *rel_path);
extern void pgFileDelete(pgFile *file, const char *full_path);

extern void pgFileFree(void *file);

extern pg_crc32 pgFileGetCRC(const char *file_path, bool missing_ok, bool use_crc32c);

extern int pgFileCompareName(const void *f1, const void *f2);
extern int pgFileComparePath(const void *f1, const void *f2);
extern int pgFileMapComparePath(const void *f1, const void *f2);
extern int pgFileComparePathWithExternal(const void *f1, const void *f2);
extern int pgFileCompareRelPathWithExternal(const void *f1, const void *f2);
extern int pgFileCompareRelPathWithExternalDesc(const void *f1, const void *f2);
extern int pgFileComparePathDesc(const void *f1, const void *f2);
extern int pgFileComparePathWithExternalDesc(const void *f1, const void *f2);
extern int pgFileCompareLinked(const void *f1, const void *f2);
extern int pgFileCompareSize(const void *f1, const void *f2);
extern int pgCompareOid(const void *f1, const void *f2);

/* in data.c */
extern bool check_data_file(ConnectionArgs *arguments, pgFile *file,
							const char *from_fullpath, uint32 checksum_version);

extern void backup_data_file(ConnectionArgs* conn_arg, pgFile *file,
								 const char *from_fullpath, const char *to_fullpath,
								 XLogRecPtr prev_backup_start_lsn, BackupMode backup_mode,
								 CompressAlg calg, int clevel, uint32 checksum_version,
								 int ptrack_version_num, const char *ptrack_schema, bool missing_ok);
extern void backup_non_data_file(pgFile *file, pgFile *prev_file,
								 const char *from_fullpath, const char *to_fullpath,
								 BackupMode backup_mode, time_t parent_backup_time,
								 bool missing_ok);
extern void backup_non_data_file_internal(const char *from_fullpath,
										  fio_location from_location,
										  const char *to_fullpath, pgFile *file,
										  bool missing_ok);

extern size_t restore_data_file(parray *parent_chain, pgFile *dest_file,
								  FILE *out, const char *to_fullpath);
extern size_t restore_data_file_internal(FILE *in, FILE *out, pgFile *file, uint32 backup_version,
								  const char *from_fullpath, const char *to_fullpath, int nblocks);
extern size_t restore_non_data_file(parray *parent_chain, pgBackup *dest_backup,
								  pgFile *dest_file, FILE *out, const char *to_fullpath);
extern void restore_non_data_file_internal(FILE *in, FILE *out, pgFile *file,
										   const char *from_fullpath, const char *to_fullpath);
extern bool create_empty_file(fio_location from_location, const char *to_root,
							  fio_location to_location, pgFile *file);

extern bool check_file_pages(pgFile *file, XLogRecPtr stop_lsn,
							 uint32 checksum_version, uint32 backup_version);
/* parsexlog.c */
extern bool extractPageMap(const char *archivedir, uint32 wal_seg_size,
						   XLogRecPtr startpoint, TimeLineID start_tli,
						   XLogRecPtr endpoint, TimeLineID end_tli,
						   parray *tli_list);
extern void validate_wal(pgBackup *backup, const char *archivedir,
						 time_t target_time, TransactionId target_xid,
						 XLogRecPtr target_lsn, TimeLineID tli,
						 uint32 seg_size);
extern bool read_recovery_info(const char *archivedir, TimeLineID tli,
							   uint32 seg_size,
							   XLogRecPtr start_lsn, XLogRecPtr stop_lsn,
							   time_t *recovery_time);
extern bool wal_contains_lsn(const char *archivedir, XLogRecPtr target_lsn,
							 TimeLineID target_tli, uint32 seg_size);
extern XLogRecPtr get_prior_record_lsn(const char *archivedir, XLogRecPtr start_lsn,
								   XLogRecPtr stop_lsn, TimeLineID tli,
								   bool seek_prev_segment, uint32 seg_size);

extern XLogRecPtr get_first_record_lsn(const char *archivedir, XLogRecPtr start_lsn,
									TimeLineID tli, uint32 wal_seg_size);

/* in util.c */
extern TimeLineID get_current_timeline(PGconn *conn);
extern TimeLineID get_current_timeline_from_control(bool safe);
extern XLogRecPtr get_checkpoint_location(PGconn *conn);
extern uint64 get_system_identifier(const char *pgdata_path);
extern uint64 get_remote_system_identifier(PGconn *conn);
extern uint32 get_data_checksum_version(bool safe);
extern pg_crc32c get_pgcontrol_checksum(const char *pgdata_path);
extern uint32 get_xlog_seg_size(char *pgdata_path);
extern void set_min_recovery_point(pgFile *file, const char *backup_path,
								   XLogRecPtr stop_backup_lsn);
extern void copy_pgcontrol_file(const char *from_fullpath, fio_location from_location,
					const char *to_fullpath, fio_location to_location, pgFile *file);

extern void time2iso(char *buf, size_t len, time_t time);
extern const char *status2str(BackupStatus status);
extern const char *base36enc(long unsigned int value);
extern char *base36enc_dup(long unsigned int value);
extern long unsigned int base36dec(const char *text);
extern uint32 parse_server_version(const char *server_version_str);
extern uint32 parse_program_version(const char *program_version);
extern bool   parse_page(Page page, XLogRecPtr *lsn);
extern int32  do_compress(void* dst, size_t dst_size, void const* src, size_t src_size,
						  CompressAlg alg, int level, const char **errormsg);
extern int32  do_decompress(void* dst, size_t dst_size, void const* src, size_t src_size,
							CompressAlg alg, const char **errormsg);

extern void pretty_size(int64 size, char *buf, size_t len);
extern void pretty_time_interval(int64 num_seconds, char *buf, size_t len);

extern PGconn *pgdata_basic_setup(ConnectionOptions conn_opt, PGNodeInfo *nodeInfo);
extern void check_system_identifiers(PGconn *conn, char *pgdata);
extern void parse_filelist_filenames(parray *files, const char *root);

/* in ptrack.c */
extern void make_pagemap_from_ptrack_1(parray* files, PGconn* backup_conn);
extern void make_pagemap_from_ptrack_2(parray* files, PGconn* backup_conn,
										const char *ptrack_schema, XLogRecPtr lsn);
extern void pg_ptrack_clear(PGconn *backup_conn, int ptrack_version_num);
extern void get_ptrack_version(PGconn *backup_conn, PGNodeInfo *nodeInfo);
extern bool pg_ptrack_enable(PGconn *backup_conn);
extern bool pg_ptrack_enable2(PGconn *backup_conn);
extern bool pg_ptrack_get_and_clear_db(Oid dbOid, Oid tblspcOid, PGconn *backup_conn);
extern char *pg_ptrack_get_and_clear(Oid tablespace_oid,
									 Oid db_oid,
									 Oid rel_oid,
									 size_t *result_size,
									 PGconn *backup_conn);
extern XLogRecPtr get_last_ptrack_lsn(PGconn *backup_conn, PGNodeInfo *nodeInfo);
extern parray * pg_ptrack_get_pagemapset(PGconn *backup_conn, const char *ptrack_schema, XLogRecPtr lsn);

/* FIO */
extern int fio_send_pages(FILE* in, FILE* out, pgFile *file, XLogRecPtr horizonLsn,
						   int calg, int clevel, uint32 checksum_version,
						   datapagemap_t *pagemap, BlockNumber* err_blknum, char **errormsg);

/* return codes for fio_send_pages */
#define WRITE_FAILED (-1)
#define REMOTE_ERROR (-2)
#define PAGE_CORRUPTION (-3)
#define SEND_OK (-4)

extern void get_header_errormsg(Page page, char **errormsg);
extern void get_checksum_errormsg(Page page, char **errormsg,
								  BlockNumber absolute_blkno);

#endif /* PG_PROBACKUP_H */
