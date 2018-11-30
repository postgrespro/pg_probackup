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

#include <sys/stat.h>

#include "utils/configuration.h"
#include "utils/thread.h"
#include <time.h>

const char *PROGRAM_VERSION	= "2.0.25";
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
	SHOW_CONFIG_CMD,
	CHECKDB_CMD
} ProbackupSubcmd;

/* directory options */
char	   *backup_path = NULL;
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
bool		is_remote_backup = false;

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
bool		force_delete = false;

/* compression options */
bool 		compress_shortcut = false;

/* other options */
char	   *instance_name;

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

static void opt_backup_mode(ConfigOption *opt, const char *arg);
static void opt_show_format(ConfigOption *opt, const char *arg);

static void compress_init(void);

/*
 * Short name should be non-printable ASCII character.
 */
static ConfigOption cmd_options[] =
{
	/* directory options */
	{ 'b',  130, "help",			&help_opt,			SOURCE_CMD_STRICT },
	{ 's', 'B', "backup-path",		&backup_path,		SOURCE_CMD_STRICT },
	/* common options */
	{ 'u', 'j', "threads",			&num_threads,		SOURCE_CMD_STRICT },
	{ 'b', 131, "stream",			&stream_wal,		SOURCE_CMD_STRICT },
	{ 'b', 132, "progress",			&progress,			SOURCE_CMD_STRICT },
	{ 's', 'i', "backup-id",		&backup_id_string,	SOURCE_CMD_STRICT },
	/* backup options */
	{ 'b', 133, "backup-pg-log",	&backup_logs,		SOURCE_CMD_STRICT },
	{ 'f', 'b', "backup-mode",		opt_backup_mode,	SOURCE_CMD_STRICT },
	{ 'b', 'C', "smooth-checkpoint", &smooth_checkpoint,	SOURCE_CMD_STRICT },
	{ 's', 'S', "slot",				&replication_slot,	SOURCE_CMD_STRICT },
	{ 'b', 134, "delete-wal",		&delete_wal,		SOURCE_CMD_STRICT },
	{ 'b', 135, "delete-expired",	&delete_expired,	SOURCE_CMD_STRICT },
	/* TODO not completed feature. Make it unavailiable from user level
	 { 'b', 18, "remote",				&is_remote_backup,	SOURCE_CMD_STRICT, }, */
	/* restore options */
	{ 's', 136, "time",				&target_time,		SOURCE_CMD_STRICT },
	{ 's', 137, "xid",				&target_xid,		SOURCE_CMD_STRICT },
	{ 's', 138, "inclusive",		&target_inclusive,	SOURCE_CMD_STRICT },
	{ 'u', 139, "timeline",			&target_tli,		SOURCE_CMD_STRICT },
	{ 'f', 'T', "tablespace-mapping", opt_tablespace_map,	SOURCE_CMD_STRICT },
	{ 'b', 140, "immediate",		&target_immediate,	SOURCE_CMD_STRICT },
	{ 's', 141, "recovery-target-name",	&target_name,		SOURCE_CMD_STRICT },
	{ 's', 142, "recovery-target-action", &target_action,	SOURCE_CMD_STRICT },
	{ 'b', 'R', "restore-as-replica", &restore_as_replica,	SOURCE_CMD_STRICT },
	{ 'b', 143, "no-validate",		&restore_no_validate,	SOURCE_CMD_STRICT },
	{ 's', 144, "lsn",				&target_lsn,		SOURCE_CMD_STRICT },
	{ 'b', 154, "skip-block-validation", &skip_block_validation,	SOURCE_CMD_STRICT },
	/* delete options */
	{ 'b', 145, "wal",				&delete_wal,		SOURCE_CMD_STRICT },
	{ 'b', 146, "expired",			&delete_expired,	SOURCE_CMD_STRICT },
	/* TODO not implemented yet */
	{ 'b', 147, "force",			&force_delete,		SOURCE_CMD_STRICT },
	/* compression options */
	{ 'b', 148, "compress",			&compress_shortcut,	SOURCE_CMD_STRICT },
	/* connection options */
	{ 'B', 'w', "no-password",		&prompt_password,	SOURCE_CMD_STRICT },
	{ 'b', 'W', "password",			&force_password,	SOURCE_CMD_STRICT },
	/* other options */
	{ 's', 149, "instance",			&instance_name,		SOURCE_CMD_STRICT },
	/* archive-push options */
	{ 's', 150, "wal-file-path",	&wal_file_path,		SOURCE_CMD_STRICT },
	{ 's', 151, "wal-file-name",	&wal_file_name,		SOURCE_CMD_STRICT },
	{ 'b', 152, "overwrite",		&file_overwrite,	SOURCE_CMD_STRICT },
	/* show options */
	{ 'f', 153, "format",			opt_show_format,	SOURCE_CMD_STRICT },
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

	/* Initialize current backup */
	pgBackupInit(&current);

	/* Initialize current instance configuration */
	init_config(&instance_config);

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
		else if (strcmp(argv[1], "checkdb") == 0)
			backup_subcmd = CHECKDB_CMD;
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
	/* TODO why do we do that only for some commands? */
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
	/* Parse command line only arguments */
	config_get_opt(argc, argv, cmd_options, instance_options);

	pgut_init();

	if (help_opt)
		help_command(command_name);

	/* backup_path is required for all pg_probackup commands except help and checkdb */
	if (backup_path == NULL && backup_subcmd != CHECKDB_CMD)
	{
		/*
		 * If command line argument is not set, try to read BACKUP_PATH
		 * from environment variable
		 */
		backup_path = getenv("BACKUP_PATH");
		if (backup_path == NULL)
			elog(ERROR, "required parameter not specified: BACKUP_PATH (-B, --backup-path)");
	}

	if (backup_path != NULL)
	{
		canonicalize_path(backup_path);

		/* Ensure that backup_path is an absolute path */
		if (!is_absolute_path(backup_path))
			elog(ERROR, "-B, --backup-path must be an absolute path");

		/* Ensure that backup_path is a path to a directory */
		rc = stat(backup_path, &stat_buf);
		if (rc != -1 && !S_ISDIR(stat_buf.st_mode))
			elog(ERROR, "-B, --backup-path must be a path to directory");
	}

	/* TODO What is this block about?*/
	/* command was initialized for a few commands */
	if (command)
	{
		elog_file(INFO, "command: %s", command);

		pfree(command);
		command = NULL;
	}

	/* TODO it would be better to list commands that require instance option */
	/* Option --instance is required for all commands except init and show */
	if (backup_subcmd != INIT_CMD && backup_subcmd != SHOW_CMD &&
		backup_subcmd != VALIDATE_CMD && backup_subcmd != CHECKDB_CMD)
	{
		if (instance_name == NULL)
			elog(ERROR, "required parameter not specified: --instance");
	}

	/*
	 * If --instance option was passed, construct paths for backup data and
	 * xlog files of this backup instance.
	 */
	if ((backup_path != NULL) && instance_name)
	{
		sprintf(backup_instance_path, "%s/%s/%s",
				backup_path, BACKUPS_DIR, instance_name);
		sprintf(arclog_path, "%s/%s/%s", backup_path, "wal", instance_name);

		/*
		 * Ensure that requested backup instance exists.
		 * for all commands except init, which doesn't take this parameter
		 * and add-instance which creates new instance.
		 */
		if (backup_subcmd != INIT_CMD && backup_subcmd != ADD_INSTANCE_CMD)
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
	if (((backup_path != NULL) && instance_name)
		&& backup_subcmd != SET_CONFIG_CMD)
	{
		char		config_path[MAXPGPATH];
		/* Read environment variables */
		config_get_opt_env(instance_options);

		/* Read options from configuration file */
		join_path_components(config_path, backup_instance_path, BACKUP_CATALOG_CONF_FILE);
		config_read_opt(config_path, instance_options, ERROR, true);
	}

	/* Just read environment variables */
	if (backup_path == NULL && backup_subcmd == CHECKDB_CMD)
		config_get_opt_env(instance_options);
	
	/* Initialize logger */
	if (backup_subcmd != CHECKDB_CMD)
		init_logger(backup_path, &instance_config.logger);

	/*
	 * We have read pgdata path from command line or from configuration file.
	 * Ensure that pgdata is an absolute path.
	 */
	if (instance_config.pgdata != NULL &&
		!is_absolute_path(instance_config.pgdata))
		elog(ERROR, "-D, --pgdata must be an absolute path");

#if PG_VERSION_NUM >= 110000
	/* Check xlog-seg-size option */
	if (instance_name &&
		backup_subcmd != INIT_CMD && backup_subcmd != SHOW_CMD &&
		backup_subcmd != ADD_INSTANCE_CMD && backup_subcmd != SET_CONFIG_CMD &&
		!IsValidWalSegSize(instance_config.xlog_seg_size))
		elog(ERROR, "Invalid WAL segment size %u", instance_config.xlog_seg_size);
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
	if (instance_config.pghost != NULL)
		dbhost = pstrdup(instance_config.pghost);
	if (instance_config.pgport != NULL)
		dbport = pstrdup(instance_config.pgport);
	if (instance_config.pguser != NULL)
		dbuser = pstrdup(instance_config.pguser);

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
			{
				const char *backup_mode;
				time_t		start_time;

				start_time = time(NULL);
				backup_mode = deparse_backup_mode(current.backup_mode);
				current.stream = stream_wal;

				elog(INFO, "Backup start, pg_probackup version: %s, backup ID: %s, backup mode: %s, instance: %s, stream: %s, remote: %s",
						  PROGRAM_VERSION, base36enc(start_time), backup_mode, instance_name,
						  stream_wal ? "true" : "false", is_remote_backup ? "true" : "false");

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
			do_show_config();
			break;
		case SET_CONFIG_CMD:
			do_set_config();
			break;
		case CHECKDB_CMD:
			do_checkdb();
			break;
		case NO_CMD:
			/* Should not happen */
			elog(ERROR, "Unknown subcommand");
	}

	return 0;
}

static void
opt_backup_mode(ConfigOption *opt, const char *arg)
{
	current.backup_mode = parse_backup_mode(arg);
}

static void
opt_show_format(ConfigOption *opt, const char *arg)
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

/*
 * Initialize compress and sanity checks for compress.
 */
static void
compress_init(void)
{
	/* Default algorithm is zlib */
	if (compress_shortcut)
		instance_config.compress_alg = ZLIB_COMPRESS;

	if (backup_subcmd != SET_CONFIG_CMD)
	{
		if (instance_config.compress_level != COMPRESS_LEVEL_DEFAULT
			&& instance_config.compress_alg == NOT_DEFINED_COMPRESS)
			elog(ERROR, "Cannot specify compress-level option without compress-alg option");
	}

	if (instance_config.compress_level < 0 || instance_config.compress_level > 9)
		elog(ERROR, "--compress-level value must be in the range from 0 to 9");

	if (instance_config.compress_level == 0)
		instance_config.compress_alg = NOT_DEFINED_COMPRESS;

	if (backup_subcmd == BACKUP_CMD || backup_subcmd == ARCHIVE_PUSH_CMD)
	{
#ifndef HAVE_LIBZ
		if (compress_alg == ZLIB_COMPRESS)
			elog(ERROR, "This build does not support zlib compression");
		else
#endif
		if (instance_config.compress_alg == PGLZ_COMPRESS && num_threads > 1)
			elog(ERROR, "Multithread backup does not support pglz compression");
	}
}
