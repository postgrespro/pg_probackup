/*-------------------------------------------------------------------------
 *
 * pg_probackup.c: Backup/Recovery manager for PostgreSQL.
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include "pg_getopt.h"
#include "streamutil.h"
#include "utils/file.h"

#include <sys/stat.h>

#include "utils/thread.h"
#include <time.h>

const char *PROGRAM_VERSION	= "2.0.24";
const char *PROGRAM_URL		= "https://github.com/postgrespro/pg_probackup";
const char *PROGRAM_EMAIL	= "https://github.com/postgrespro/pg_probackup/issues";

typedef enum ProbackupSubcmd
{
	NO_CMD = 0,
	INIT_CMD,
	ADD_INSTANCE_CMD,
	DELETE_INSTANCE_CMD,
	ARCHIVE_PUSH_CMD,
	ARCHIVE_GET_CMD,
	BACKUP_CMD,
	RESTORE_CMD,
	VALIDATE_CMD,
	DELETE_CMD,
	MERGE_CMD,
	SHOW_CMD,
	SET_CONFIG_CMD,
	SHOW_CONFIG_CMD
} ProbackupSubcmd;

/* directory options */
char	   *backup_path = NULL;
char	   *pgdata = NULL;
/*
 * path or to the data files in the backup catalog
 * $BACKUP_PATH/backups/instance_name
 */
char		backup_instance_path[MAXPGPATH];
/*
 * path or to the wal files in the backup catalog
 * $BACKUP_PATH/wal/instance_name
 */
char		arclog_path[MAXPGPATH] = "";

/* common options */
static char *backup_id_string = NULL;
int			num_threads = 1;
bool		stream_wal = false;
bool		progress = false;
#if PG_VERSION_NUM >= 100000
char	   *replication_slot = NULL;
#endif

/* backup options */
bool		backup_logs = false;
bool		smooth_checkpoint;
/* Wait timeout for WAL segment archiving */
uint32		archive_timeout = ARCHIVE_TIMEOUT_DEFAULT;
const char *master_db = NULL;
const char *master_host = NULL;
const char *master_port= NULL;
const char *master_user = NULL;
uint32		replica_timeout = REPLICA_TIMEOUT_DEFAULT;
char       *remote_host;
char       *remote_port;
char       *remote_proto = (char*)"ssh";
char       *ssh_config;
bool        is_remote_agent;
bool		is_remote_backup;

/* restore options */
static char		   *target_time;
static char		   *target_xid;
static char		   *target_lsn;
static char		   *target_inclusive;
static TimeLineID	target_tli;
static bool			target_immediate;
static char		   *target_name = NULL;
static char		   *target_action = NULL;

static pgRecoveryTarget *recovery_target_options = NULL;

bool restore_as_replica = false;
bool restore_no_validate = false;

bool skip_block_validation = false;

/* delete options */
bool		delete_wal = false;
bool		delete_expired = false;
bool		apply_to_all = false;
bool		force_delete = false;

/* retention options */
uint32		retention_redundancy = 0;
uint32		retention_window = 0;

/* compression options */
CompressAlg compress_alg = COMPRESS_ALG_DEFAULT;
int			compress_level = COMPRESS_LEVEL_DEFAULT;
bool 		compress_shortcut = false;


/* other options */
char	   *instance_name;
uint64		system_identifier = 0;

/*
 * Starting from PostgreSQL 11 WAL segment size may vary. Prior to
 * PostgreSQL 10 xlog_seg_size is equal to XLOG_SEG_SIZE.
 */
#if PG_VERSION_NUM >= 110000
uint32		xlog_seg_size = 0;
#else
uint32		xlog_seg_size = XLOG_SEG_SIZE;
#endif

/* archive push options */
static char *wal_file_path;
static char *wal_file_name;
static bool	file_overwrite = false;

/* show options */
ShowFormat show_format = SHOW_PLAIN;

/* current settings */
pgBackup	current;
static ProbackupSubcmd backup_subcmd = NO_CMD;

static bool help_opt = false;

static void opt_backup_mode(pgut_option *opt, const char *arg);
static void opt_log_level_console(pgut_option *opt, const char *arg);
static void opt_log_level_file(pgut_option *opt, const char *arg);
static void opt_compress_alg(pgut_option *opt, const char *arg);
static void opt_show_format(pgut_option *opt, const char *arg);

static void compress_init(void);

static pgut_option options[] =
{
	/* directory options */
	{ 'b',  1,  "help",					&help_opt,			SOURCE_CMDLINE },
	{ 's', 'D', "pgdata",				&pgdata,			SOURCE_CMDLINE },
	{ 's', 'B', "backup-path",			&backup_path,		SOURCE_CMDLINE },
	/* common options */
	{ 'u', 'j', "threads",				&num_threads,		SOURCE_CMDLINE },
	{ 'b', 2, "stream",					&stream_wal,		SOURCE_CMDLINE },
	{ 'b', 3, "progress",				&progress,			SOURCE_CMDLINE },
	{ 's', 'i', "backup-id",			&backup_id_string, SOURCE_CMDLINE },
	/* backup options */
	{ 'b', 10, "backup-pg-log",			&backup_logs,		SOURCE_CMDLINE },
	{ 'f', 'b', "backup-mode",			opt_backup_mode,	SOURCE_CMDLINE },
	{ 'b', 'C', "smooth-checkpoint",	&smooth_checkpoint,	SOURCE_CMDLINE },
	{ 's', 'S', "slot",					&replication_slot,	SOURCE_CMDLINE },
	{ 'u', 11, "archive-timeout",		&archive_timeout,	SOURCE_CMDLINE, SOURCE_DEFAULT,	OPTION_UNIT_S },
	{ 'b', 12, "delete-wal",			&delete_wal,		SOURCE_CMDLINE },
	{ 'b', 13, "delete-expired",		&delete_expired,	SOURCE_CMDLINE },
	{ 's', 14, "master-db",				&master_db,			SOURCE_CMDLINE, },
	{ 's', 15, "master-host",			&master_host,		SOURCE_CMDLINE, },
	{ 's', 16, "master-port",			&master_port,		SOURCE_CMDLINE, },
	{ 's', 17, "master-user",			&master_user,		SOURCE_CMDLINE, },
	{ 'u', 18, "replica-timeout",		&replica_timeout,	SOURCE_CMDLINE,	SOURCE_DEFAULT,	OPTION_UNIT_S },
    { 's', 19, "remote-host",			&remote_host,       SOURCE_CMDLINE, },
    { 's', 20, "remote-port",	        &remote_port,       SOURCE_CMDLINE, },
    { 's', 21, "remote-proto",	        &remote_proto,      SOURCE_CMDLINE, },
    { 's', 22, "ssh-config",	        &ssh_config,        SOURCE_CMDLINE, },
    { 'b', 23, "agent",				    &is_remote_agent,   SOURCE_CMDLINE, },
    { 'b', 24, "remote",				&is_remote_backup,	SOURCE_CMDLINE, },
	/* restore options */
	{ 's', 30, "time",					&target_time,		SOURCE_CMDLINE },
	{ 's', 31, "xid",					&target_xid,		SOURCE_CMDLINE },
	{ 's', 32, "inclusive",				&target_inclusive,	SOURCE_CMDLINE },
	{ 'u', 33, "timeline",				&target_tli,		SOURCE_CMDLINE },
	{ 'f', 'T', "tablespace-mapping",	opt_tablespace_map,	SOURCE_CMDLINE },
	{ 'b', 34, "immediate",				&target_immediate,	SOURCE_CMDLINE },
	{ 's', 35, "recovery-target-name",	&target_name,		SOURCE_CMDLINE },
	{ 's', 36, "recovery-target-action", &target_action,	SOURCE_CMDLINE },
	{ 'b', 'R', "restore-as-replica",	&restore_as_replica,	SOURCE_CMDLINE },
	{ 'b', 27, "no-validate",			&restore_no_validate,	SOURCE_CMDLINE },
	{ 's', 28, "lsn",					&target_lsn,		SOURCE_CMDLINE },
	{ 'b', 29, "skip-block-validation", &skip_block_validation,	SOURCE_CMDLINE },
	/* delete options */
	{ 'b', 130, "wal",					&delete_wal,		SOURCE_CMDLINE },
	{ 'b', 131, "expired",				&delete_expired,	SOURCE_CMDLINE },
	{ 'b', 132, "all",					&apply_to_all,		SOURCE_CMDLINE },
	/* TODO not implemented yet */
	{ 'b', 133, "force",				&force_delete,		SOURCE_CMDLINE },
	/* retention options */
	{ 'u', 134, "retention-redundancy",	&retention_redundancy, SOURCE_CMDLINE },
	{ 'u', 135, "retention-window",		&retention_window,	SOURCE_CMDLINE },
	/* compression options */
	{ 'f', 136, "compress-algorithm",	opt_compress_alg,	SOURCE_CMDLINE },
	{ 'u', 137, "compress-level",		&compress_level,	SOURCE_CMDLINE },
	{ 'b', 138, "compress",				&compress_shortcut,	SOURCE_CMDLINE },
	/* logging options */
	{ 'f', 140, "log-level-console",	opt_log_level_console,	SOURCE_CMDLINE },
	{ 'f', 141, "log-level-file",		opt_log_level_file,	SOURCE_CMDLINE },
	{ 's', 142, "log-filename",			&log_filename,		SOURCE_CMDLINE },
	{ 's', 143, "error-log-filename",	&error_log_filename, SOURCE_CMDLINE },
	{ 's', 144, "log-directory",		&log_directory,		SOURCE_CMDLINE },
	{ 'U', 145, "log-rotation-size",	&log_rotation_size,	SOURCE_CMDLINE,	SOURCE_DEFAULT,	OPTION_UNIT_KB },
	{ 'U', 146, "log-rotation-age",		&log_rotation_age,	SOURCE_CMDLINE,	SOURCE_DEFAULT,	OPTION_UNIT_MS },
	/* connection options */
	{ 's', 'd', "pgdatabase",			&pgut_dbname,		SOURCE_CMDLINE },
	{ 's', 'h', "pghost",				&host,				SOURCE_CMDLINE },
	{ 's', 'p', "pgport",				&port,				SOURCE_CMDLINE },
	{ 's', 'U', "pguser",				&username,			SOURCE_CMDLINE },
	{ 'B', 'w', "no-password",			&prompt_password,	SOURCE_CMDLINE },
	{ 'b', 'W', "password",				&force_password,	SOURCE_CMDLINE },
	/* other options */
	{ 'U', 150, "system-identifier",	&system_identifier,	SOURCE_FILE_STRICT },
	{ 's', 151, "instance",				&instance_name,		SOURCE_CMDLINE },
#if PG_VERSION_NUM >= 110000
	{ 'u', 152, "xlog-seg-size",		&xlog_seg_size,		SOURCE_FILE_STRICT},
#endif
	/* archive-push options */
	{ 's', 160, "wal-file-path",		&wal_file_path,		SOURCE_CMDLINE },
	{ 's', 161, "wal-file-name",		&wal_file_name,		SOURCE_CMDLINE },
	{ 'b', 162, "overwrite",			&file_overwrite,	SOURCE_CMDLINE },
	/* show options */
	{ 'f', 170, "format",				opt_show_format,	SOURCE_CMDLINE },
	{ 0 }
};

/*
 * Entry point of pg_probackup command.
 */
int
main(int argc, char *argv[])
{
	char	   *command = NULL,
			   *command_name;
	/* Check if backup_path is directory. */
	struct stat stat_buf;
	int			rc;

	/* initialize configuration */
	pgBackupInit(&current);

	PROGRAM_NAME = get_progname(argv[0]);
	set_pglocale_pgservice(argv[0], "pgscripts");

#if PG_VERSION_NUM >= 110000
	/*
	 * Reset WAL segment size, we will retreive it using RetrieveWalSegSize()
	 * later.
	 */
	WalSegSz = 0;
#endif

	/*
	 * Save main thread's tid. It is used call exit() in case of errors.
	 */
	main_tid = pthread_self();

	/* Parse subcommands and non-subcommand options */
	if (argc > 1)
	{
		if (strcmp(argv[1], "archive-push") == 0)
			backup_subcmd = ARCHIVE_PUSH_CMD;
		else if (strcmp(argv[1], "archive-get") == 0)
			backup_subcmd = ARCHIVE_GET_CMD;
		else if (strcmp(argv[1], "add-instance") == 0)
			backup_subcmd = ADD_INSTANCE_CMD;
		else if (strcmp(argv[1], "del-instance") == 0)
			backup_subcmd = DELETE_INSTANCE_CMD;
		else if (strcmp(argv[1], "init") == 0)
			backup_subcmd = INIT_CMD;
		else if (strcmp(argv[1], "backup") == 0)
			backup_subcmd = BACKUP_CMD;
		else if (strcmp(argv[1], "restore") == 0)
			backup_subcmd = RESTORE_CMD;
		else if (strcmp(argv[1], "validate") == 0)
			backup_subcmd = VALIDATE_CMD;
		else if (strcmp(argv[1], "delete") == 0)
			backup_subcmd = DELETE_CMD;
		else if (strcmp(argv[1], "merge") == 0)
			backup_subcmd = MERGE_CMD;
		else if (strcmp(argv[1], "show") == 0)
			backup_subcmd = SHOW_CMD;
		else if (strcmp(argv[1], "set-config") == 0)
			backup_subcmd = SET_CONFIG_CMD;
		else if (strcmp(argv[1], "show-config") == 0)
			backup_subcmd = SHOW_CONFIG_CMD;
		else if (strcmp(argv[1], "--help") == 0 ||
				 strcmp(argv[1], "-?") == 0 ||
				 strcmp(argv[1], "help") == 0)
		{
			if (argc > 2)
				help_command(argv[2]);
			else
				help_pg_probackup();
		}
		else if (strcmp(argv[1], "--version") == 0
				 || strcmp(argv[1], "version") == 0
				 || strcmp(argv[1], "-V") == 0)
		{
#ifdef PGPRO_VERSION
			fprintf(stderr, "%s %s (Postgres Pro %s %s)\n",
					PROGRAM_NAME, PROGRAM_VERSION,
					PGPRO_VERSION, PGPRO_EDITION);
#else
			fprintf(stderr, "%s %s (PostgreSQL %s)\n",
					PROGRAM_NAME, PROGRAM_VERSION, PG_VERSION);
#endif
			exit(0);
		}
		else
			elog(ERROR, "Unknown subcommand \"%s\"", argv[1]);
	}

	if (backup_subcmd == NO_CMD)
		elog(ERROR, "No subcommand specified");

	/*
	 * Make command string before getopt_long() will call. It permutes the
	 * content of argv.
	 */
	command_name = pstrdup(argv[1]);
	if (backup_subcmd == BACKUP_CMD ||
		backup_subcmd == RESTORE_CMD ||
		backup_subcmd == VALIDATE_CMD ||
		backup_subcmd == DELETE_CMD ||
		backup_subcmd == MERGE_CMD)
	{
		int			i,
					len = 0,
					allocated = 0;

		allocated = sizeof(char) * MAXPGPATH;
		command = (char *) palloc(allocated);

		for (i = 0; i < argc; i++)
		{
			int			arglen = strlen(argv[i]);

			if (arglen + len > allocated)
			{
				allocated *= 2;
				command = repalloc(command, allocated);
			}

			strncpy(command + len, argv[i], arglen);
			len += arglen;
			command[len++] = ' ';
		}

		command[len] = '\0';
	}

	optind += 1;
	/* Parse command line arguments */
	pgut_getopt(argc, argv, options);

	if (help_opt)
		help_command(command_name);

	/* backup_path is required for all pg_probackup commands except help */
	if (backup_path == NULL)
	{
		/*
		 * If command line argument is not set, try to read BACKUP_PATH
		 * from environment variable
		 */
		backup_path = getenv("BACKUP_PATH");
		if (backup_path == NULL)
			elog(ERROR, "required parameter not specified: BACKUP_PATH (-B, --backup-path)");
	}
	canonicalize_path(backup_path);

	/* Ensure that backup_path is an absolute path */
	if (!is_absolute_path(backup_path))
		elog(ERROR, "-B, --backup-path must be an absolute path");

	if (IsSshConnection()
		&& (backup_subcmd == BACKUP_CMD || backup_subcmd == ADD_INSTANCE_CMD || backup_subcmd == RESTORE_CMD))
	{
		if (is_remote_agent) {
			if (backup_subcmd != BACKUP_CMD) {
				fio_communicate(STDIN_FILENO, STDOUT_FILENO);
				return 0;
			}
			fio_redirect(STDIN_FILENO, STDOUT_FILENO);
		} else {
			/* Execute remote probackup */
			int status = remote_execute(argc, argv, backup_subcmd == BACKUP_CMD);
			if (status != 0)
			{
				return status;
			}
		}
	}

	if (!is_remote_agent)
	{
		/* Ensure that backup_path is a path to a directory */
		rc = stat(backup_path, &stat_buf);
		if (rc != -1 && !S_ISDIR(stat_buf.st_mode))
			elog(ERROR, "-B, --backup-path must be a path to directory");
	}

	/* command was initialized for a few commands */
	if (command)
	{
		elog_file(INFO, "command: %s", command);

		pfree(command);
		command = NULL;
	}

	/* Option --instance is required for all commands except init and show */
	if (backup_subcmd != INIT_CMD && backup_subcmd != SHOW_CMD &&
		backup_subcmd != VALIDATE_CMD)
	{
		if (instance_name == NULL)
			elog(ERROR, "required parameter not specified: --instance");
	}

	/*
	 * If --instance option was passed, construct paths for backup data and
	 * xlog files of this backup instance.
	 */
	if (instance_name)
	{
		sprintf(backup_instance_path, "%s/%s/%s",
				backup_path, BACKUPS_DIR, instance_name);
		sprintf(arclog_path, "%s/%s/%s", backup_path, "wal", instance_name);

		/*
		 * Ensure that requested backup instance exists.
		 * for all commands except init, which doesn't take this parameter
		 * and add-instance which creates new instance.
		 */
		if (backup_subcmd != INIT_CMD && backup_subcmd != ADD_INSTANCE_CMD && !is_remote_agent)
		{
			if (access(backup_instance_path, F_OK) != 0)
				elog(ERROR, "Instance '%s' does not exist in this backup catalog",
							instance_name);
		}
	}

	/*
	 * Read options from env variables or from config file,
	 * unless we're going to set them via set-config.
	 */
	if (instance_name && backup_subcmd != SET_CONFIG_CMD)
	{
		char		path[MAXPGPATH];

		/* Read environment variables */
		pgut_getopt_env(options);

		/* Read options from configuration file */
		join_path_components(path, backup_instance_path, BACKUP_CATALOG_CONF_FILE);
		pgut_readopt(path, options, ERROR, true);
	}

	/* Initialize logger */
	init_logger(backup_path);

	/*
	 * We have read pgdata path from command line or from configuration file.
	 * Ensure that pgdata is an absolute path.
	 */
	if (pgdata != NULL && !is_absolute_path(pgdata))
		elog(ERROR, "-D, --pgdata must be an absolute path");

#if PG_VERSION_NUM >= 110000
	/* Check xlog-seg-size option */
	if (instance_name &&
		backup_subcmd != INIT_CMD && backup_subcmd != SHOW_CMD &&
		backup_subcmd != ADD_INSTANCE_CMD && !IsValidWalSegSize(xlog_seg_size))
		elog(ERROR, "Invalid WAL segment size %u", xlog_seg_size);
#endif

	/* Sanity check of --backup-id option */
	if (backup_id_string != NULL)
	{
		if (backup_subcmd != RESTORE_CMD &&
			backup_subcmd != VALIDATE_CMD &&
			backup_subcmd != DELETE_CMD &&
			backup_subcmd != MERGE_CMD &&
			backup_subcmd != SHOW_CMD)
			elog(ERROR, "Cannot use -i (--backup-id) option together with the \"%s\" command",
				 command_name);

		current.backup_id = base36dec(backup_id_string);
		if (current.backup_id == 0)
			elog(ERROR, "Invalid backup-id \"%s\"", backup_id_string);
	}

	/* Setup stream options. They are used in streamutil.c. */
	if (host != NULL)
		dbhost = pstrdup(host);
	if (port != NULL)
		dbport = pstrdup(port);
	if (username != NULL)
		dbuser = pstrdup(username);

	/* setup exclusion list for file search */
	if (!backup_logs)
	{
		int			i;

		for (i = 0; pgdata_exclude_dir[i]; i++);		/* find first empty slot */

		/* Set 'pg_log' in first empty slot */
		pgdata_exclude_dir[i] = "pg_log";
	}

	if (backup_subcmd == VALIDATE_CMD || backup_subcmd == RESTORE_CMD)
	{
		/* parse all recovery target options into recovery_target_options structure */
		recovery_target_options = parseRecoveryTargetOptions(target_time, target_xid,
								   target_inclusive, target_tli, target_lsn, target_immediate,
								   target_name, target_action, restore_no_validate);
	}

	if (num_threads < 1)
		num_threads = 1;

	compress_init();

	/* do actual operation */
	switch (backup_subcmd)
	{
		case ARCHIVE_PUSH_CMD:
			return do_archive_push(wal_file_path, wal_file_name, file_overwrite);
		case ARCHIVE_GET_CMD:
			return do_archive_get(wal_file_path, wal_file_name);
		case ADD_INSTANCE_CMD:
			return do_add_instance();
		case DELETE_INSTANCE_CMD:
			return do_delete_instance();
		case INIT_CMD:
			return do_init();
		case BACKUP_CMD:
		    current.stream = stream_wal;
		    if (IsSshConnection() && !is_remote_agent)
			{
				current.status = BACKUP_STATUS_DONE;
				StrNCpy(current.program_version, PROGRAM_VERSION,
						sizeof(current.program_version));
				complete_backup();
				return 0;
			}
			else
			{
				const char *backup_mode;
				time_t		start_time;

				start_time = time(NULL);
				backup_mode = deparse_backup_mode(current.backup_mode);

				elog(INFO, "Backup start, pg_probackup version: %s, backup ID: %s, backup mode: %s, instance: %s, stream: %s, remote: %s",
						  PROGRAM_VERSION, base36enc(start_time), backup_mode, instance_name,
						  stream_wal ? "true" : "false", remote_host ? "true" : "false");

				return do_backup(start_time);
			}
		case RESTORE_CMD:
			return do_restore_or_validate(current.backup_id,
						  recovery_target_options,
						  true);
		case VALIDATE_CMD:
			if (current.backup_id == 0 && target_time == 0 && target_xid == 0)
				return do_validate_all();
			else
				return do_restore_or_validate(current.backup_id,
						  recovery_target_options,
						  false);
		case SHOW_CMD:
			return do_show(current.backup_id);
		case DELETE_CMD:
			if (delete_expired && backup_id_string)
				elog(ERROR, "You cannot specify --delete-expired and --backup-id options together");
			if (!delete_expired && !delete_wal && !backup_id_string)
				elog(ERROR, "You must specify at least one of the delete options: --expired |--wal |--backup_id");
			if (delete_wal && !delete_expired && !backup_id_string)
				return do_retention_purge();
			if (delete_expired)
				return do_retention_purge();
			else
				do_delete(current.backup_id);
			break;
		case MERGE_CMD:
			do_merge(current.backup_id);
			break;
		case SHOW_CONFIG_CMD:
			return do_configure(true);
		case SET_CONFIG_CMD:
			return do_configure(false);
		case NO_CMD:
			/* Should not happen */
			elog(ERROR, "Unknown subcommand");
	}

	return 0;
}

static void
opt_backup_mode(pgut_option *opt, const char *arg)
{
	current.backup_mode = parse_backup_mode(arg);
}

static void
opt_log_level_console(pgut_option *opt, const char *arg)
{
	log_level_console = parse_log_level(arg);
}

static void
opt_log_level_file(pgut_option *opt, const char *arg)
{
	log_level_file = parse_log_level(arg);
}

static void
opt_show_format(pgut_option *opt, const char *arg)
{
	const char *v = arg;
	size_t		len;

	/* Skip all spaces detected */
	while (IsSpace(*v))
		v++;
	len = strlen(v);

	if (len > 0)
	{
		if (pg_strncasecmp("plain", v, len) == 0)
			show_format = SHOW_PLAIN;
		else if (pg_strncasecmp("json", v, len) == 0)
			show_format = SHOW_JSON;
		else
			elog(ERROR, "Invalid show format \"%s\"", arg);
	}
	else
		elog(ERROR, "Invalid show format \"%s\"", arg);
}

static void
opt_compress_alg(pgut_option *opt, const char *arg)
{
	compress_alg = parse_compress_alg(arg);
}

/*
 * Initialize compress and sanity checks for compress.
 */
static void
compress_init(void)
{
	/* Default algorithm is zlib */
	if (compress_shortcut)
		compress_alg = ZLIB_COMPRESS;

	if (backup_subcmd != SET_CONFIG_CMD)
	{
		if (compress_level != COMPRESS_LEVEL_DEFAULT
			&& compress_alg == NOT_DEFINED_COMPRESS)
			elog(ERROR, "Cannot specify compress-level option without compress-alg option");
	}

	if (compress_level < 0 || compress_level > 9)
		elog(ERROR, "--compress-level value must be in the range from 0 to 9");

	if (compress_level == 0)
		compress_alg = NOT_DEFINED_COMPRESS;

	if (backup_subcmd == BACKUP_CMD || backup_subcmd == ARCHIVE_PUSH_CMD)
	{
#ifndef HAVE_LIBZ
		if (compress_alg == ZLIB_COMPRESS)
			elog(ERROR, "This build does not support zlib compression");
		else
#endif
		if (compress_alg == PGLZ_COMPRESS && num_threads > 1)
			elog(ERROR, "Multithread backup does not support pglz compression");
	}
}
