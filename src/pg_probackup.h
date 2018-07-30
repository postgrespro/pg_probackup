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
#include <libpq-fe.h>

#include "access/timeline.h"
#include "access/xlogdefs.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "storage/block.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "utils/pg_crc.h"
#include "common/relpath.h"
#include "port.h"

#ifdef FRONTEND
#undef FRONTEND
	#include "port/atomics.h"
#define FRONTEND
#endif

#include "utils/parray.h"
#include "utils/pgut.h"

#include "datapagemap.h"

# define PG_STOP_BACKUP_TIMEOUT 300
/*
 * Macro needed to parse ptrack.
 * NOTE Keep those values syncronised with definitions in ptrack.h
 */
#define PTRACK_BITS_PER_HEAPBLOCK 1
#define HEAPBLOCKS_PER_BYTE (BITS_PER_BYTE / PTRACK_BITS_PER_HEAPBLOCK)

/* Directory/File names */
#define DATABASE_DIR			"database"
#define BACKUPS_DIR				"backups"
#if PG_VERSION_NUM >= 100000
#define PG_XLOG_DIR				"pg_wal"
#else
#define PG_XLOG_DIR				"pg_xlog"
#endif
#define PG_TBLSPC_DIR			"pg_tblspc"
#define PG_GLOBAL_DIR			"global"
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

typedef enum CompressAlg
{
	NOT_DEFINED_COMPRESS = 0,
	NONE_COMPRESS,
	PGLZ_COMPRESS,
	ZLIB_COMPRESS,
} CompressAlg;

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
	bool	exists_in_prev;	/* Mark files, both data and regular, that exists in previous backup */
	CompressAlg compress_alg; /* compression algorithm applied to the file */
	volatile pg_atomic_flag lock;	/* lock for synchronization of parallel threads  */
	datapagemap_t pagemap;	/* bitmap of pages updated since previous backup */
	bool	pagemap_isabsent; /* Used to mark files with unknown state of pagemap,
							   * i.e. datafiles without _ptrack */
} pgFile;

/* Special values of datapagemap_t bitmapsize */
#define PageBitmapIsEmpty 0		/* Used to mark unchanged datafiles */

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

typedef enum ProbackupSubcmd
{
	NO_CMD = 0,
	INIT_CMD,
	ARCHIVE_PUSH_CMD,
	ARCHIVE_GET_CMD,
	ADD_INSTANCE_CMD,
	DELETE_INSTANCE_CMD,
	BACKUP_CMD,
	RESTORE_CMD,
	VALIDATE_CMD,
	SHOW_CMD,
	DELETE_CMD,
	SET_CONFIG_CMD,
	SHOW_CONFIG_CMD
} ProbackupSubcmd;

typedef enum ShowFormat
{
	SHOW_PLAIN,
	SHOW_JSON
} ShowFormat;


/* special values of pgBackup fields */
#define INVALID_BACKUP_ID	0    /* backup ID is not provided by user */
#define BYTES_INVALID		(-1)
#define BLOCKNUM_INVALID	(-1)

typedef struct pgBackupConfig
{
	uint64		system_identifier;
	char		*pgdata;
	const char	*pgdatabase;
	const char	*pghost;
	const char	*pgport;
	const char	*pguser;

	const char *master_host;
	const char *master_port;
	const char *master_db;
	const char *master_user;
	int			replica_timeout;

	int			log_level_console;
	int			log_level_file;
	char	   *log_filename;
	char	   *error_log_filename;
	char	   *log_directory;
	int			log_rotation_size;
	int			log_rotation_age;

	uint32		retention_redundancy;
	uint32		retention_window;

	CompressAlg	compress_alg;
	int			compress_level;
} pgBackupConfig;

typedef struct pgBackup pgBackup;

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
} pgBackup;

/* Recovery target for restore and validate subcommands */
typedef struct pgRecoveryTarget
{
	bool			time_specified;
	time_t			recovery_target_time;
	/* add one more field in order to avoid deparsing recovery_target_time back */
	const char		*target_time_string;
	bool			xid_specified;
	TransactionId	recovery_target_xid;
	/* add one more field in order to avoid deparsing recovery_target_xid back */
	const char		*target_xid_string;
	bool			lsn_specified;
	XLogRecPtr		recovery_target_lsn;
	/* add one more field in order to avoid deparsing recovery_target_lsn back */
	const char		*target_lsn_string;
	TimeLineID		recovery_target_tli;
	bool			recovery_target_inclusive;
	bool			inclusive_specified;
	bool			recovery_target_immediate;
	const char		*recovery_target_name;
	const char		*recovery_target_action;
	bool			restore_no_validate;
} pgRecoveryTarget;

/* Union to ease operations on relation pages */
typedef union DataPage
{
	PageHeaderData page_data;
	char		data[BLCKSZ];
} DataPage;

typedef struct
{
	const char *from_root;
	const char *to_root;

	parray	   *files_list;
	parray	   *prev_filelist;
	XLogRecPtr	prev_start_lsn;

	PGconn	   *backup_conn;
	PGcancel   *cancel_conn;

	/*
	 * Return value from the thread.
	 * 0 means there is no error, 1 - there is an error.
	 */
	int			ret;
} backup_files_arg;

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

/* directory options */
extern char	   *backup_path;
extern char		backup_instance_path[MAXPGPATH];
extern char	   *pgdata;
extern char		arclog_path[MAXPGPATH];

/* common options */
extern int		num_threads;
extern bool		stream_wal;
extern bool		progress;
#if PG_VERSION_NUM >= 100000
/* In pre-10 'replication_slot' is defined in receivelog.h */
extern char	   *replication_slot;
#endif

/* backup options */
extern bool		smooth_checkpoint;
extern uint32	archive_timeout;
extern bool		is_remote_backup;
extern const char *master_db;
extern const char *master_host;
extern const char *master_port;
extern const char *master_user;
extern uint32	replica_timeout;

extern bool is_ptrack_support;
extern bool is_checksum_enabled;
extern bool exclusive_backup;

/* restore options */
extern bool restore_as_replica;

/* delete options */
extern bool		delete_wal;
extern bool		delete_expired;
extern bool		apply_to_all;
extern bool		force_delete;

/* retention options */
extern uint32	retention_redundancy;
extern uint32	retention_window;

/* compression options */
extern CompressAlg compress_alg;
extern int		compress_level;
extern bool		compress_shortcut;

#define DEFAULT_COMPRESS_LEVEL 1

extern CompressAlg parse_compress_alg(const char *arg);
extern const char* deparse_compress_alg(int alg);
/* other options */
extern char *instance_name;
extern uint64 system_identifier;

/* show options */
extern ShowFormat show_format;

/* current settings */
extern pgBackup current;
extern ProbackupSubcmd backup_subcmd;

/* in dir.c */
/* exclude directory list for $PGDATA file listing */
extern const char *pgdata_exclude_dir[];

/* in backup.c */
extern int do_backup(time_t start_time);
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
extern parray * readTimeLineHistory_probackup(TimeLineID targetTLI);
extern pgRecoveryTarget *parseRecoveryTargetOptions(
	const char *target_time, const char *target_xid,
	const char *target_inclusive, TimeLineID target_tli, const char* target_lsn,
	bool target_immediate, const char *target_name,
	const char *target_action, bool restore_no_validate);

extern void opt_tablespace_map(pgut_option *opt, const char *arg);

/* in init.c */
extern int do_init(void);
extern int do_add_instance(void);

/* in archive.c */
extern int do_archive_push(char *wal_file_path, char *wal_file_name,
						   bool overwrite);
extern int do_archive_get(char *wal_file_path, char *wal_file_name);


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
extern int do_delete_instance(void);

/* in fetch.c */
extern char *slurpFile(const char *datadir,
					   const char *path,
					   size_t *filesize,
					   bool safe);
extern char *fetchFile(PGconn *conn, const char *filename, size_t *filesize);

/* in help.c */
extern void help_pg_probackup(void);
extern void help_command(char *command);

/* in validate.c */
extern void pgBackupValidate(pgBackup* backup);
extern int do_validate_all(void);

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
extern void pgBackupGetPath2(const pgBackup *backup, char *path, size_t len,
							 const char *subdir1, const char *subdir2);
extern int pgBackupCreateDir(pgBackup *backup);
extern void pgBackupFree(void *backup);
extern int pgBackupCompareId(const void *f1, const void *f2);
extern int pgBackupCompareIdDesc(const void *f1, const void *f2);

extern pgBackup* find_parent_backup(pgBackup *current_backup);

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

extern bool fileExists(const char *path);

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
extern bool backup_data_file(backup_files_arg* arguments,
							 const char *from_root, const char *to_root,
							 pgFile *file, XLogRecPtr prev_backup_start_lsn,
							 BackupMode backup_mode);
extern void restore_data_file(const char *from_root, const char *to_root,
							  pgFile *file, pgBackup *backup);
extern bool copy_file(const char *from_root, const char *to_root,
					  pgFile *file);
extern void push_wal_file(const char *from_path, const char *to_path,
						  bool is_compress, bool overwrite);
extern void get_wal_file(const char *from_path, const char *to_path);

extern bool calc_file_checksum(pgFile *file);

/* parsexlog.c */
extern void extractPageMap(const char *datadir,
						   XLogRecPtr startpoint,
						   TimeLineID tli,
						   XLogRecPtr endpoint, bool prev_seg,
						   parray *backup_files_list);
extern void validate_wal(pgBackup *backup,
						 const char *archivedir,
						 time_t target_time,
						 TransactionId target_xid,
						 XLogRecPtr target_lsn,
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
extern uint32 get_data_checksum_version(bool safe);
extern const char *base36enc(long unsigned int value);
extern char *base36enc_dup(long unsigned int value);
extern long unsigned int base36dec(const char *text);
extern uint64 get_system_identifier(char *pgdata);
extern uint64 get_remote_system_identifier(PGconn *conn);
extern pg_time_t timestamptz_to_time_t(TimestampTz t);
extern int parse_server_version(char *server_version_str);
extern void pgBackup_init(pgBackup *backup);

/* in status.c */
extern bool is_pg_running(void);

#ifdef WIN32
#ifdef _DEBUG
#define lseek _lseek
#define open _open
#define fstat _fstat
#define read _read
#define close _close
#define write _write
#define mkdir(dir,mode) _mkdir(dir)
#endif
#endif

#endif /* PG_PROBACKUP_H */
