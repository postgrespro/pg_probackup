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
#define PG_BLACK_LIST			"black_list"
#define PG_TABLESPACE_MAP_FILE "tablespace_map"
#define EXTERNAL_DIR			"external_directories/externaldir"

/* Timeout defaults */
#define ARCHIVE_TIMEOUT_DEFAULT		300
#define REPLICA_TIMEOUT_DEFAULT		300

/* Direcotry/File permission */
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

/* Check if an XLogRecPtr value is pointed to 0 offset */
#define XRecOffIsNull(xlrp) \
		((xlrp) % XLOG_BLCKSZ == 0)

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
	char	*name;			/* file or directory name */
	mode_t	mode;			/* protection (file type and permission) */
	size_t	size;			/* size of the file */
	size_t	read_size;		/* size of the portion read (if only some pages are
							   backed up, it's different from size) */
	int64	write_size;		/* size of the backed-up file. BYTES_INVALID means
							   that the file existed but was not backed up
							   because not modified since last backup. */
							/* we need int64 here to store '-1' value */
	pg_crc32 crc;			/* CRC value of the file, regular file only */
	char	*linked;		/* path of the linked file */
	bool	is_datafile;	/* true if the file is PostgreSQL data file */
	char	*path;			/* absolute path of the file */
	Oid		tblspcOid;		/* tblspcOid extracted from path, if applicable */
	Oid		dbOid;			/* dbOid extracted from path, if applicable */
	Oid		relOid;			/* relOid extracted from path, if applicable */
	char	*forkName;		/* forkName extracted from path, if applicable */
	int		segno;			/* Segment number for ptrack */
	int		n_blocks;		/* size of the file in blocks, readed during DELTA backup */
	bool	is_cfs;			/* Flag to distinguish files compressed by CFS*/
	bool	is_database;
	int		external_dir_num; /* Number of external directory. 0 if not external */
	bool	exists_in_prev;	/* Mark files, both data and regular, that exists in previous backup */
	CompressAlg compress_alg; /* compression algorithm applied to the file */
	volatile pg_atomic_flag lock;	/* lock for synchronization of parallel threads  */
	datapagemap_t pagemap;	/* bitmap of pages updated since previous backup */
	bool	pagemap_isabsent; /* Used to mark files with unknown state of pagemap,
							   * i.e. datafiles without _ptrack */
} pgFile;

typedef struct pg_indexEntry
{
	Oid indexrelid;
	char *name;
	char *dbname;
	char *amcheck_nspname; /* schema where amcheck extention is located */
	volatile pg_atomic_flag lock;	/* lock for synchronization of parallel threads  */
} pg_indexEntry;

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
#define PROGRAM_VERSION	"2.1.0"
#define AGENT_PROTOCOL_VERSION 20100

/*
 * An instance configuration. It can be stored in a configuration file or passed
 * from command line.
 */
typedef struct InstanceConfig
{
	uint64		system_identifier;
	uint32		xlog_seg_size;

	char	   *pgdata;
	char	   *external_dir_str;
	const char *pgdatabase;
	const char *pghost;
	const char *pgport;
	const char *pguser;

	const char *master_host;
	const char *master_port;
	const char *master_db;
	const char *master_user;
	uint32		replica_timeout;

	/* Wait timeout for WAL segment archiving */
	uint32		archive_timeout;

	/* Logger parameters */
	LoggerConfig logger;

	/* Remote access parameters */
	RemoteConfig remote;

	/* Retention options. 0 disables the option. */
	uint32		retention_redundancy;
	uint32		retention_window;

	CompressAlg	compress_alg;
	int			compress_level;
} InstanceConfig;

extern ConfigOption instance_options[];
extern InstanceConfig instance_config;

typedef struct pgBackup pgBackup;

/* Information about single backup stored in backup.conf */
struct pgBackup
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
	time_t			merge_time; /* the moment when merge was started or 0 */
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
	/* Size of WAL files in archive needed to restore this backup */
	int64			wal_bytes;

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
	bool			no_validate;
} pgRecoveryTarget;

typedef struct
{
	const char *from_root;
	const char *to_root;
	const char *external_prefix;

	parray	   *files_list;
	parray	   *prev_filelist;
	parray	   *external_dirs;
	XLogRecPtr	prev_start_lsn;

	PGconn	   *backup_conn;
	PGcancel   *cancel_conn;
	parray	   *index_list;
	int			thread_num;

	/*
	 * Return value from the thread.
	 * 0 means there is no error, 1 - there is an error.
	 */
	int			ret;
} backup_files_arg;

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
#define PageIsTruncated -2
#define SkipCurrentPage -3
#define PageIsCorrupted -4 /* used by checkdb */


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
#else
#define GetXLogSegNo(xlrp, logSegNo, wal_segsz_bytes) \
	XLByteToSeg(xlrp, logSegNo)
#define GetXLogRecPtr(segno, offset, wal_segsz_bytes, dest) \
	XLogSegNoOffsetToRecPtr(segno, offset, dest)
#define GetXLogFileName(fname, tli, logSegNo, wal_segsz_bytes) \
	XLogFileName(fname, tli, logSegNo)
#define IsInXLogSeg(xlrp, logSegNo, wal_segsz_bytes) \
	XLByteInSeg(xlrp, logSegNo)
#endif

#define IsSshProtocol() (instance_config.remote.host && strcmp(instance_config.remote.proto, "ssh") == 0)
#define IsReplicationProtocol() (instance_config.remote.host && strcmp(instance_config.remote.proto, "replication") == 0)

/* directory options */
extern char    *pg_probackup;
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

extern bool is_ptrack_support;
extern bool exclusive_backup;

/* restore options */
extern bool restore_as_replica;
extern bool skip_block_validation;
extern bool skip_external_dirs;

/* delete options */
extern bool		delete_wal;
extern bool		delete_expired;
extern bool		merge_expired;
extern bool		force_delete;
extern bool		dry_run;

/* compression options */
extern bool		compress_shortcut;

/* other options */
extern char *instance_name;

/* show options */
extern ShowFormat show_format;

/* checkdb options */
extern bool heapallindexed;

/* current settings */
extern pgBackup current;

/* argv of the process */
extern char** commands_args;

/* in dir.c */
/* exclude directory list for $PGDATA file listing */
extern const char *pgdata_exclude_dir[];

/* in backup.c */
extern int do_backup(time_t start_time, bool no_validate);
extern void do_checkdb(bool need_amcheck);
extern BackupMode parse_backup_mode(const char *value);
extern const char *deparse_backup_mode(BackupMode mode);
extern void process_block_change(ForkNumber forknum, RelFileNode rnode,
								 BlockNumber blkno);

extern char *pg_ptrack_get_block(backup_files_arg *arguments,
								 Oid dbOid, Oid tblsOid, Oid relOid,
								 BlockNumber blknum,
								 size_t *result_size);
/* in restore.c */
extern int do_restore_or_validate(time_t target_backup_id,
					  pgRecoveryTarget *rt,
					  bool is_restore);
extern bool satisfy_timeline(const parray *timelines, const pgBackup *backup);
extern bool satisfy_recovery_target(const pgBackup *backup,
									const pgRecoveryTarget *rt);
extern pgRecoveryTarget *parseRecoveryTargetOptions(
	const char *target_time, const char *target_xid,
	const char *target_inclusive, TimeLineID target_tli, const char* target_lsn,
	const char *target_stop, const char *target_name,
	const char *target_action, bool no_validate);

/* in merge.c */
extern void do_merge(time_t backup_id);
extern void merge_backups(pgBackup *backup, pgBackup *next_backup);

/* in init.c */
extern int do_init(void);
extern int do_add_instance(void);

/* in archive.c */
extern int do_archive_push(char *wal_file_path, char *wal_file_name,
						   bool overwrite);
extern int do_archive_get(char *wal_file_path, char *wal_file_name);


/* in configure.c */
extern void do_show_config(void);
extern void do_set_config(bool missing_ok);
extern void init_config(InstanceConfig *config);

/* in show.c */
extern int do_show(time_t requested_backup_id);

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
extern void pgBackupValidate(pgBackup* backup);
extern int do_validate_all(void);

/* in catalog.c */
extern pgBackup *read_backup(time_t timestamp);
extern void write_backup(pgBackup *backup);
extern void write_backup_status(pgBackup *backup, BackupStatus status);
extern bool lock_backup(pgBackup *backup);

extern const char *pgBackupGetBackupMode(pgBackup *backup);

extern parray *catalog_get_backup_list(time_t requested_backup_id);
extern void catalog_lock_backup_list(parray *backup_list, int from_idx,
									 int to_idx);
extern pgBackup *catalog_get_last_data_backup(parray *backup_list,
											  TimeLineID tli);
extern void pgBackupWriteControl(FILE *out, pgBackup *backup);
extern void write_backup_filelist(pgBackup *backup, parray *files,
								  const char *root, const char *external_prefix,
								  parray *external_list);

extern void pgBackupGetPath(const pgBackup *backup, char *path, size_t len,
							const char *subdir);
extern void pgBackupGetPath2(const pgBackup *backup, char *path, size_t len,
							 const char *subdir1, const char *subdir2);
extern int pgBackupCreateDir(pgBackup *backup);
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

#define COMPRESS_ALG_DEFAULT NOT_DEFINED_COMPRESS
#define COMPRESS_LEVEL_DEFAULT 1

extern CompressAlg parse_compress_alg(const char *arg);
extern const char* deparse_compress_alg(int alg);

/* in dir.c */
extern void dir_list_file(parray *files, const char *root, bool exclude,
						  bool omit_symlink, bool add_root, int external_dir_num, fio_location location);

extern void create_data_directories(const char *data_dir,
									const char *backup_dir,
									bool extract_tablespaces,
									fio_location location);

extern void read_tablespace_map(parray *files, const char *backup_dir);
extern void opt_tablespace_map(ConfigOption *opt, const char *arg);
extern void opt_externaldir_map(ConfigOption *opt, const char *arg);
extern void check_tablespace_mapping(pgBackup *backup);
extern void check_external_dir_mapping(pgBackup *backup);
extern char *get_external_remap(char *current_dir);

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

extern pgFile *pgFileNew(const char *path, bool omit_symlink, int external_dir_num, fio_location location);
extern pgFile *pgFileInit(const char *path);
extern void pgFileDelete(pgFile *file);
extern void pgFileFree(void *file);
extern pg_crc32 pgFileGetCRC(const char *file_path, bool use_crc32c,
							 bool raise_on_deleted, size_t *bytes_read, fio_location location);
extern int pgFileComparePath(const void *f1, const void *f2);
extern int pgFileComparePathWithExternal(const void *f1, const void *f2);
extern int pgFileComparePathDesc(const void *f1, const void *f2);
extern int pgFileComparePathWithExternalDesc(const void *f1, const void *f2);
extern int pgFileCompareLinked(const void *f1, const void *f2);
extern int pgFileCompareSize(const void *f1, const void *f2);

/* in data.c */
extern bool check_data_file(backup_files_arg* arguments,
							pgFile *file);
extern bool backup_data_file(backup_files_arg* arguments,
							 const char *to_path, pgFile *file,
							 XLogRecPtr prev_backup_start_lsn,
							 BackupMode backup_mode,
							 CompressAlg calg, int clevel);
extern void restore_data_file(const char *to_path,
							  pgFile *file, bool allow_truncate,
							  bool write_header,
							  uint32 backup_version);
extern bool copy_file(const char *from_root, fio_location from_location, const char *to_root, fio_location to_location, pgFile *file);
extern void move_file(const char *from_root, const char *to_root, pgFile *file);
extern void push_wal_file(const char *from_path, const char *to_path,
						  bool is_compress, bool overwrite);
extern void get_wal_file(const char *from_path, const char *to_path);

extern void calc_file_checksum(pgFile *file, fio_location location);

extern bool check_file_pages(pgFile *file, XLogRecPtr stop_lsn,
							 uint32 checksum_version, uint32 backup_version);
/* parsexlog.c */
extern void extractPageMap(const char *archivedir,
						   TimeLineID tli, uint32 seg_size,
						   XLogRecPtr startpoint, XLogRecPtr endpoint);
extern void validate_wal(pgBackup *backup, const char *archivedir,
						 time_t target_time, TransactionId target_xid,
						 XLogRecPtr target_lsn, TimeLineID tli,
						 uint32 seg_size);
extern bool read_recovery_info(const char *archivedir, TimeLineID tli,
							   uint32 seg_size,
							   XLogRecPtr start_lsn, XLogRecPtr stop_lsn,
							   time_t *recovery_time,
							   TransactionId *recovery_xid);
extern bool wal_contains_lsn(const char *archivedir, XLogRecPtr target_lsn,
							 TimeLineID target_tli, uint32 seg_size);
extern XLogRecPtr get_last_wal_lsn(const char *archivedir, XLogRecPtr start_lsn,
								   XLogRecPtr stop_lsn, TimeLineID tli,
								   bool seek_prev_segment, uint32 seg_size);

/* in util.c */
extern TimeLineID get_current_timeline(bool safe);
extern XLogRecPtr get_checkpoint_location(PGconn *conn);
extern uint64 get_system_identifier(const char *pgdata_path);
extern uint64 get_remote_system_identifier(PGconn *conn);
extern uint32 get_data_checksum_version(bool safe);
extern pg_crc32c get_pgcontrol_checksum(const char *pgdata_path);
extern uint32 get_xlog_seg_size(char *pgdata_path);
extern void set_min_recovery_point(pgFile *file, const char *backup_path,
								   XLogRecPtr stop_backup_lsn);
extern void copy_pgcontrol_file(const char *from_root, fio_location location, const char *to_root, fio_location to_location,
								pgFile *file);

extern void sanityChecks(void);
extern void time2iso(char *buf, size_t len, time_t time);
extern const char *status2str(BackupStatus status);
extern void remove_trailing_space(char *buf, int comment_mark);
extern void remove_not_digit(char *buf, size_t len, const char *str);
extern const char *base36enc(long unsigned int value);
extern char *base36enc_dup(long unsigned int value);
extern long unsigned int base36dec(const char *text);
extern uint32 parse_server_version(const char *server_version_str);
extern uint32 parse_program_version(const char *program_version);
extern bool   parse_page(Page page, XLogRecPtr *lsn);
int32  do_compress(void* dst, size_t dst_size, void const* src, size_t src_size,
				   CompressAlg alg, int level, const char **errormsg);

#endif /* PG_PROBACKUP_H */
