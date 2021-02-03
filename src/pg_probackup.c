/*-------------------------------------------------------------------------
 *
 * pg_probackup.c: Backup/Recovery manager for PostgreSQL.
 *
 * This is an entry point for the program.
 * Parse command name and it's options, verify them and call a
 * do_***() function that implements the command.
 *
 * Avoid using global variables in the code.
 * Pass all needed information as funciton arguments:
 *

 *
 * TODO (see pg_probackup_state.h):
 *
 * Functions that work with a backup catalog accept catalogState,
 * which currently only contains pathes to backup catalog subdirectories
 * + function specific options.
 * 
 * Functions that work with an instance accept instanceState argument, which
 * includes catalogState, instance_name,
 * info about pgdata associated with the instance (see pgState),
 * various instance config options, and list of backups belonging to the instance.
 * + function specific options.
 * 
 * Functions that work with multiple backups in the catalog
 * accept instanceState and info needed to determine the range of backups to handle.
 * + function specific options.
 *
 * Functions that work with a single backup accept backupState argument,
 * which includes link to the instanceState, backup_id and backup-specific info.
 * + function specific options.
 *
 * Functions that work with a postgreSQL instance (i.e. checkdb) accept pgState,
 * which includes info about pgdata directory and connection.
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"
#include "pg_probackup_state.h"

#include "pg_getopt.h"
#include "streamutil.h"
#include "utils/file.h"

#include <sys/stat.h>

#include "utils/configuration.h"
#include "utils/thread.h"
#include <time.h>

const char  *PROGRAM_NAME = NULL;		/* PROGRAM_NAME_FULL without .exe suffix
										 * if any */
const char  *PROGRAM_NAME_FULL = NULL;
const char  *PROGRAM_FULL_PATH = NULL;
const char  *PROGRAM_URL = "https://github.com/postgrespro/pg_probackup";
const char  *PROGRAM_EMAIL = "https://github.com/postgrespro/pg_probackup/issues";

/* ================ catalogState =========== */
/* directory options */
/* TODO make it local variable, pass as an argument to all commands that need it.  */
static char	   *backup_path = NULL;

static CatalogState *catalogState = NULL;
/* ================ catalogState (END) =========== */

/* colon separated external directories list ("/path1:/path2") */
char	   *externaldir = NULL;
/* common options */
int			num_threads = 1;
bool		stream_wal = false;
bool		no_color = false;
bool 		show_color = true;
bool        is_archive_cmd = false;
pid_t       my_pid = 0;
__thread int  my_thread_num = 1;
bool		progress = false;
bool		no_sync = false;
#if PG_VERSION_NUM >= 100000
char	   *replication_slot = NULL;
#endif
bool		temp_slot = false;

/* backup options */
bool         backup_logs = false;
bool         smooth_checkpoint;
char        *remote_agent;
static char *backup_note = NULL;
/* restore options */
static char		   *target_time = NULL;
static char		   *target_xid = NULL;
static char		   *target_lsn = NULL;
static char		   *target_inclusive = NULL;
static TimeLineID	target_tli;
static char		   *target_stop;
static bool			target_immediate;
static char		   *target_name = NULL;
static char		   *target_action = NULL;

static char *primary_conninfo = NULL;

static pgRecoveryTarget *recovery_target_options = NULL;
static pgRestoreParams *restore_params = NULL;

time_t current_time = 0;
static bool restore_as_replica = false;
bool no_validate = false;
IncrRestoreMode incremental_mode = INCR_NONE;

bool skip_block_validation = false;
bool skip_external_dirs = false;

/* array for datnames, provided via db-include and db-exclude */
static parray *datname_exclude_list = NULL;
static parray *datname_include_list = NULL;

/* checkdb options */
bool need_amcheck = false;
bool heapallindexed = false;
bool amcheck_parent = false;

/* delete options */
bool		delete_wal = false;
bool		delete_expired = false;
bool		merge_expired = false;
bool		force = false;
bool		dry_run = false;
static char *delete_status = NULL;
/* compression options */
static bool 		compress_shortcut = false;

/* ================ instanceState =========== */
static char	   *instance_name;

static InstanceState *instanceState = NULL;

/* ================ instanceState (END) =========== */

/* archive push options */
int		batch_size = 1;
static char *wal_file_path;
static char *wal_file_name;
static bool file_overwrite = false;
static bool no_ready_rename = false;

/* archive get options */
static char *prefetch_dir;
bool no_validate_wal = false;

/* show options */
ShowFormat show_format = SHOW_PLAIN;
bool show_archive = false;

/* set-backup options */
int64 ttl = -1;
static char *expire_time_string = NULL;
static pgSetBackupParams *set_backup_params = NULL;

/* ================ backupState =========== */
static char *backup_id_string = NULL;
pgBackup	current;
/* ================ backupState (END) =========== */

static bool help_opt = false;

static void opt_incr_restore_mode(ConfigOption *opt, const char *arg);
static void opt_backup_mode(ConfigOption *opt, const char *arg);
static void opt_show_format(ConfigOption *opt, const char *arg);

static void compress_init(ProbackupSubcmd const subcmd);

static void opt_datname_exclude_list(ConfigOption *opt, const char *arg);
static void opt_datname_include_list(ConfigOption *opt, const char *arg);

/*
 * Short name should be non-printable ASCII character.
 * Use values between 128 and 255.
 */
static ConfigOption cmd_options[] =
{
	/* directory options */
	{ 'b', 130, "help",			&help_opt,			SOURCE_CMD_STRICT },
	{ 's', 'B', "backup-path",		&backup_path,		SOURCE_CMD_STRICT },
	/* common options */
	{ 'u', 'j', "threads",			&num_threads,		SOURCE_CMD_STRICT },
	{ 'b', 131, "stream",			&stream_wal,		SOURCE_CMD_STRICT },
	{ 'b', 132, "progress",			&progress,			SOURCE_CMD_STRICT },
	{ 's', 'i', "backup-id",		&backup_id_string,	SOURCE_CMD_STRICT },
	{ 'b', 133, "no-sync",			&no_sync,			SOURCE_CMD_STRICT },
	{ 'b', 134, "no-color",			&no_color,			SOURCE_CMD_STRICT },
	/* backup options */
	{ 'b', 180, "backup-pg-log",	&backup_logs,		SOURCE_CMD_STRICT },
	{ 'f', 'b', "backup-mode",		opt_backup_mode,	SOURCE_CMD_STRICT },
	{ 'b', 'C', "smooth-checkpoint", &smooth_checkpoint,	SOURCE_CMD_STRICT },
	{ 's', 'S', "slot",				&replication_slot,	SOURCE_CMD_STRICT },
	{ 'b', 181, "temp-slot",		&temp_slot,			SOURCE_CMD_STRICT },
	{ 'b', 182, "delete-wal",		&delete_wal,		SOURCE_CMD_STRICT },
	{ 'b', 183, "delete-expired",	&delete_expired,	SOURCE_CMD_STRICT },
	{ 'b', 184, "merge-expired",	&merge_expired,		SOURCE_CMD_STRICT },
	{ 'b', 185, "dry-run",			&dry_run,			SOURCE_CMD_STRICT },
	{ 's', 238, "note",				&backup_note,		SOURCE_CMD_STRICT },
	/* restore options */
	{ 's', 136, "recovery-target-time",	&target_time,	SOURCE_CMD_STRICT },
	{ 's', 137, "recovery-target-xid",	&target_xid,	SOURCE_CMD_STRICT },
	{ 's', 144, "recovery-target-lsn",	&target_lsn,	SOURCE_CMD_STRICT },
	{ 's', 138, "recovery-target-inclusive",	&target_inclusive,	SOURCE_CMD_STRICT },
	{ 'u', 139, "recovery-target-timeline",		&target_tli,		SOURCE_CMD_STRICT },
	{ 's', 157, "recovery-target",	&target_stop,		SOURCE_CMD_STRICT },
	{ 'f', 'T', "tablespace-mapping", opt_tablespace_map,	SOURCE_CMD_STRICT },
	{ 'f', 155, "external-mapping",	opt_externaldir_map,	SOURCE_CMD_STRICT },
	{ 's', 141, "recovery-target-name",	&target_name,		SOURCE_CMD_STRICT },
	{ 's', 142, "recovery-target-action", &target_action,	SOURCE_CMD_STRICT },
	{ 'b', 143, "no-validate",		&no_validate,		SOURCE_CMD_STRICT },
	{ 'b', 154, "skip-block-validation", &skip_block_validation,	SOURCE_CMD_STRICT },
	{ 'b', 156, "skip-external-dirs", &skip_external_dirs,	SOURCE_CMD_STRICT },
	{ 'f', 158, "db-include", 		opt_datname_include_list, SOURCE_CMD_STRICT },
	{ 'f', 159, "db-exclude", 		opt_datname_exclude_list, SOURCE_CMD_STRICT },
	{ 'b', 'R', "restore-as-replica", &restore_as_replica,	SOURCE_CMD_STRICT },
	{ 's', 160, "primary-conninfo",	&primary_conninfo,	SOURCE_CMD_STRICT },
	{ 's', 'S', "primary-slot-name",&replication_slot,	SOURCE_CMD_STRICT },
	{ 'f', 'I', "incremental-mode", opt_incr_restore_mode,	SOURCE_CMD_STRICT },
	/* checkdb options */
	{ 'b', 195, "amcheck",			&need_amcheck,		SOURCE_CMD_STRICT },
	{ 'b', 196, "heapallindexed",	&heapallindexed,	SOURCE_CMD_STRICT },
	{ 'b', 197, "parent",			&amcheck_parent,	SOURCE_CMD_STRICT },
	/* delete options */
	{ 'b', 145, "wal",				&delete_wal,		SOURCE_CMD_STRICT },
	{ 'b', 146, "expired",			&delete_expired,	SOURCE_CMD_STRICT },
	{ 's', 172, "status",			&delete_status,		SOURCE_CMD_STRICT },

	/* TODO not implemented yet */
	{ 'b', 147, "force",			&force,				SOURCE_CMD_STRICT },
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
	{ 'b', 153, "no-ready-rename",	&no_ready_rename,	SOURCE_CMD_STRICT },
	{ 'i', 162, "batch-size",		&batch_size,		SOURCE_CMD_STRICT },
	/* archive-get options */
	{ 's', 163, "prefetch-dir",		&prefetch_dir,		SOURCE_CMD_STRICT },
	{ 'b', 164, "no-validate-wal",	&no_validate_wal,	SOURCE_CMD_STRICT },
	/* show options */
	{ 'f', 165, "format",			opt_show_format,	SOURCE_CMD_STRICT },
	{ 'b', 166, "archive",			&show_archive,		SOURCE_CMD_STRICT },
	/* set-backup options */
	{ 'I', 170, "ttl", &ttl, SOURCE_CMD_STRICT, SOURCE_DEFAULT, 0, OPTION_UNIT_S, option_get_value},
	{ 's', 171, "expire-time",		&expire_time_string,	SOURCE_CMD_STRICT },

	/* options for backward compatibility
	 * TODO: remove in 3.0.0
	 */
	{ 's', 136, "time",				&target_time,		SOURCE_CMD_STRICT },
	{ 's', 137, "xid",				&target_xid,		SOURCE_CMD_STRICT },
	{ 's', 138, "inclusive",		&target_inclusive,	SOURCE_CMD_STRICT },
	{ 'u', 139, "timeline",			&target_tli,		SOURCE_CMD_STRICT },
	{ 's', 144, "lsn",				&target_lsn,		SOURCE_CMD_STRICT },
	{ 'b', 140, "immediate",		&target_immediate,	SOURCE_CMD_STRICT },

	{ 0 }
};

/*
 * Entry point of pg_probackup command.
 */
int
main(int argc, char *argv[])
{
	char	   *command = NULL;
	ProbackupSubcmd backup_subcmd = NO_CMD;

	PROGRAM_NAME_FULL = argv[0];

	/* Check terminal presense and initialize ANSI escape codes for Windows */
	init_console();

	/* Initialize current backup */
	pgBackupInit(&current);

	/* Initialize current instance configuration */
	//TODO get git of this global variable craziness
	init_config(&instance_config, instance_name);

	PROGRAM_NAME = get_progname(argv[0]);
	PROGRAM_FULL_PATH = palloc0(MAXPGPATH);

	/* Get current time */
	current_time = time(NULL);

	my_pid = getpid();
	//set_pglocale_pgservice(argv[0], "pgscripts");

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
		backup_subcmd = parse_subcmd(argv[1]);
		switch(backup_subcmd)
		{
			case SSH_CMD:
#ifdef WIN32
				launch_ssh(argv);
				break;
#else
				elog(ERROR, "\"ssh\" command implemented only for Windows");
				break;
#endif
			case AGENT_CMD:
				/* 'No forward compatibility' sanity:
				 *   /old/binary  -> ssh execute -> /newer/binary agent version_num
				 * If we are executed as an agent for older binary, then exit with error
				 */
				if (argc > 2)
					elog(ERROR, "Version mismatch, pg_probackup binary with version '%s' "
							"is launched as an agent for pg_probackup binary with version '%s'",
							PROGRAM_VERSION, argv[2]);
				fio_communicate(STDIN_FILENO, STDOUT_FILENO);
				return 0;
			case HELP_CMD:
				if (argc > 2)
				{
					/* 'pg_probackup help command' style */
					help_command(parse_subcmd(argv[2]));
					exit(0);
				}
				else
				{
					help_pg_probackup();
					exit(0);
				}
				break;
			case VERSION_CMD:
				help_print_version();
				exit(0);
			case NO_CMD:
				elog(ERROR, "Unknown subcommand \"%s\"", argv[1]);
			default:
				/* Silence compiler warnings */
				break;
		}
	}
	else
		elog(ERROR, "No subcommand specified. Please run with \"help\" argument to see possible subcommands.");

	/*
	 * Make command string before getopt_long() will call. It permutes the
	 * content of argv.
	 */
	/* TODO why do we do that only for some commands? */
	if (backup_subcmd == BACKUP_CMD ||
		backup_subcmd == RESTORE_CMD ||
		backup_subcmd == VALIDATE_CMD ||
		backup_subcmd == DELETE_CMD ||
		backup_subcmd == MERGE_CMD ||
		backup_subcmd == SET_CONFIG_CMD ||
		backup_subcmd == SET_BACKUP_CMD)
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

	if (no_color)
		show_color = false;

	if (help_opt)
	{
		/* 'pg_probackup command --help' style */
		help_command(backup_subcmd);
		exit(0);
	}

	/* set location based on cmdline options only */
	setMyLocation(backup_subcmd);

	/* ===== catalogState ======*/
	if (backup_path == NULL)
	{
		/*
		 * If command line argument is not set, try to read BACKUP_PATH
		 * from environment variable
		 */
		backup_path = getenv("BACKUP_PATH");
	}

	if (backup_path != NULL)
	{
		canonicalize_path(backup_path);

		/* Ensure that backup_path is an absolute path */
		if (!is_absolute_path(backup_path))
			elog(ERROR, "-B, --backup-path must be an absolute path");

		catalogState = pgut_new(CatalogState);
		strncpy(catalogState->catalog_path, backup_path, MAXPGPATH);
		join_path_components(catalogState->backup_subdir_path,
							catalogState->catalog_path, BACKUPS_DIR);
		join_path_components(catalogState->wal_subdir_path,
							catalogState->catalog_path, WAL_SUBDIR);
	}

	/* backup_path is required for all pg_probackup commands except help, version and checkdb */
	if (backup_path == NULL &&
		backup_subcmd != CHECKDB_CMD &&
		backup_subcmd != HELP_CMD &&
		backup_subcmd != VERSION_CMD)
		elog(ERROR, "required parameter not specified: BACKUP_PATH (-B, --backup-path)");

	/* ===== catalogState (END) ======*/

	/* ===== instanceState ======*/

	/*
	 * Option --instance is required for all commands except
	 * init, show, checkdb and validate
	 */
	if (instance_name == NULL)
	{
		if (backup_subcmd != INIT_CMD && backup_subcmd != SHOW_CMD &&
			backup_subcmd != VALIDATE_CMD && backup_subcmd != CHECKDB_CMD)
			elog(ERROR, "required parameter not specified: --instance");
	}
	else
	{
		instanceState = pgut_new(InstanceState);
		instanceState->catalog_state = catalogState;

		strncpy(instanceState->instance_name, instance_name, MAXPGPATH);
		join_path_components(instanceState->instance_backup_subdir_path,
							catalogState->backup_subdir_path, instanceState->instance_name);
		join_path_components(instanceState->instance_wal_subdir_path,
							catalogState->wal_subdir_path, instanceState->instance_name);
		join_path_components(instanceState->instance_config_path,
							 instanceState->instance_backup_subdir_path, BACKUP_CATALOG_CONF_FILE);

	}
	/* ===== instanceState (END) ======*/

	/*
	 * If --instance option was passed, construct paths for backup data and
	 * xlog files of this backup instance.
	 */
	if ((backup_path != NULL) && instance_name)
	{
		/*
		 * Ensure that requested backup instance exists.
		 * for all commands except init, which doesn't take this parameter,
		 * add-instance, which creates new instance,
		 * and archive-get, which just do not require it at this point
		 */
		if (backup_subcmd != INIT_CMD && backup_subcmd != ADD_INSTANCE_CMD &&
			backup_subcmd != ARCHIVE_GET_CMD)
		{
			struct stat st;

			if (fio_stat(instanceState->instance_backup_subdir_path,
						 &st, true, FIO_BACKUP_HOST) != 0)
			{
				elog(WARNING, "Failed to access directory \"%s\": %s",
					instanceState->instance_backup_subdir_path, strerror(errno));

				// TODO: redundant message, should we get rid of it?
				elog(ERROR, "Instance '%s' does not exist in this backup catalog",
							instance_name);
			}
			else
			{
				/* Ensure that backup_path is a path to a directory */
				if (!S_ISDIR(st.st_mode))
					elog(ERROR, "-B, --backup-path must be a path to directory");
			}
		}
	}

	/*
	 * We read options from command line, now we need to read them from
	 * configuration file since we got backup path and instance name.
	 * For some commands an instance option isn't required, see above.
	 */
	if (instance_name)
	{
		/* Read environment variables */
		config_get_opt_env(instance_options);

		/* Read options from configuration file */
		if (backup_subcmd != ADD_INSTANCE_CMD &&
			backup_subcmd != ARCHIVE_GET_CMD)
		{
			if (backup_subcmd == CHECKDB_CMD)
				config_read_opt(instanceState->instance_config_path, instance_options, ERROR, true, true);
			else
				config_read_opt(instanceState->instance_config_path, instance_options, ERROR, true, false);

			/*
			 * We can determine our location only after reading the configuration file,
			 * unless we are running arcive-push/archive-get - they are allowed to trust
			 * cmdline only.
			 */
			setMyLocation(backup_subcmd);
		}
	}

	/*
	 * Disable logging into file for archive-push and archive-get.
	 * Note, that we should NOT use fio_is_remote() here,
	 * because it will launch ssh connection and we do not
	 * want it, because it will kill archive-get prefetch
	 * performance.
	 *
	 * TODO: make logging into file possible via ssh
	 */
	if (fio_is_remote_simple(FIO_BACKUP_HOST) &&
		(backup_subcmd == ARCHIVE_GET_CMD ||
		backup_subcmd == ARCHIVE_PUSH_CMD))
	{
		instance_config.logger.log_level_file = LOG_OFF;
		is_archive_cmd = true;
	}


	/* Just read environment variables */
	if (backup_path == NULL && backup_subcmd == CHECKDB_CMD)
		config_get_opt_env(instance_options);

	/* Sanity for checkdb, if backup_dir is provided but pgdata and instance are not */
	if (backup_subcmd == CHECKDB_CMD &&
		backup_path != NULL &&
		instance_name == NULL &&
		instance_config.pgdata == NULL)
			elog(ERROR, "required parameter not specified: --instance");

	/* Usually checkdb for file logging requires log_directory
	 * to be specified explicitly, but if backup_dir and instance name are provided,
	 * checkdb can use the usual default values or values from config
	 */
	if (backup_subcmd == CHECKDB_CMD &&
		(instance_config.logger.log_level_file != LOG_OFF &&
		 instance_config.logger.log_directory == NULL) &&
		(!instance_config.pgdata || !instance_name))
		elog(ERROR, "Cannot save checkdb logs to a file. "
			"You must specify --log-directory option when running checkdb with "
			"--log-level-file option enabled.");

	/* Initialize logger */
	init_logger(backup_path, &instance_config.logger);

	/* command was initialized for a few commands */
	if (command)
	{
		elog_file(INFO, "command: %s", command);

		pfree(command);
		command = NULL;
	}

	/* For archive-push and archive-get skip full path lookup */
	if ((backup_subcmd != ARCHIVE_GET_CMD &&
		backup_subcmd != ARCHIVE_PUSH_CMD) &&
		(find_my_exec(argv[0],(char *) PROGRAM_FULL_PATH) < 0))
	{
			PROGRAM_FULL_PATH = NULL;
			elog(WARNING, "%s: could not find a full path to executable", PROGRAM_NAME);
	}

	/*
	 * We have read pgdata path from command line or from configuration file.
	 * Ensure that pgdata is an absolute path.
	 */
	if (instance_config.pgdata != NULL)
		canonicalize_path(instance_config.pgdata);
	if (instance_config.pgdata != NULL &&
		!is_absolute_path(instance_config.pgdata))
		elog(ERROR, "-D, --pgdata must be an absolute path");

#if PG_VERSION_NUM >= 110000
	/* Check xlog-seg-size option */
	if (instance_name &&
		backup_subcmd != INIT_CMD &&
		backup_subcmd != ADD_INSTANCE_CMD && backup_subcmd != SET_CONFIG_CMD &&
		!IsValidWalSegSize(instance_config.xlog_seg_size))
	{
		/* If we are working with instance of PG<11 using PG11 binary,
		 * then xlog_seg_size is equal to zero. Manually set it to 16MB.
		 */
		if (instance_config.xlog_seg_size == 0)
			instance_config.xlog_seg_size = DEFAULT_XLOG_SEG_SIZE;
		else
			elog(ERROR, "Invalid WAL segment size %u", instance_config.xlog_seg_size);
	}
#endif

	/* Sanity check of --backup-id option */
	if (backup_id_string != NULL)
	{
		if (backup_subcmd != RESTORE_CMD &&
			backup_subcmd != VALIDATE_CMD &&
			backup_subcmd != DELETE_CMD &&
			backup_subcmd != MERGE_CMD &&
			backup_subcmd != SET_BACKUP_CMD &&
			backup_subcmd != SHOW_CMD)
			elog(ERROR, "Cannot use -i (--backup-id) option together with the \"%s\" command",
				 get_subcmd_name(backup_subcmd));

		current.backup_id = base36dec(backup_id_string);
		if (current.backup_id == 0)
			elog(ERROR, "Invalid backup-id \"%s\"", backup_id_string);
	}

	if (!instance_config.conn_opt.pghost && instance_config.remote.host)
		instance_config.conn_opt.pghost = instance_config.remote.host;

		/* Setup stream options. They are used in streamutil.c. */
	if (instance_config.conn_opt.pghost != NULL)
		dbhost = pstrdup(instance_config.conn_opt.pghost);
	if (instance_config.conn_opt.pgport != NULL)
		dbport = pstrdup(instance_config.conn_opt.pgport);
	if (instance_config.conn_opt.pguser != NULL)
		dbuser = pstrdup(instance_config.conn_opt.pguser);

	if (backup_subcmd == VALIDATE_CMD || backup_subcmd == RESTORE_CMD)
	{
		/*
		 * Parse all recovery target options into recovery_target_options
		 * structure.
		 */
		recovery_target_options =
			parseRecoveryTargetOptions(target_time, target_xid,
				target_inclusive, target_tli, target_lsn,
				(target_stop != NULL) ? target_stop :
					(target_immediate) ? "immediate" : NULL,
				target_name, target_action);

		if (force && backup_subcmd != RESTORE_CMD)
			elog(ERROR, "You cannot specify \"--force\" flag with the \"%s\" command",
				get_subcmd_name(backup_subcmd));

		if (force)
			no_validate = true;

		/* keep all params in one structure */
		restore_params = pgut_new(pgRestoreParams);
		restore_params->is_restore = (backup_subcmd == RESTORE_CMD);
		restore_params->force = force;
		restore_params->no_validate = no_validate;
		restore_params->restore_as_replica = restore_as_replica;
		restore_params->recovery_settings_mode = DEFAULT;

		restore_params->primary_slot_name = replication_slot;
		restore_params->skip_block_validation = skip_block_validation;
		restore_params->skip_external_dirs = skip_external_dirs;
		restore_params->partial_db_list = NULL;
		restore_params->partial_restore_type = NONE;
		restore_params->primary_conninfo = primary_conninfo;
		restore_params->incremental_mode = incremental_mode;

		/* handle partial restore parameters */
		if (datname_exclude_list && datname_include_list)
			elog(ERROR, "You cannot specify '--db-include' and '--db-exclude' together");

		if (datname_exclude_list)
		{
			restore_params->partial_restore_type = EXCLUDE;
			restore_params->partial_db_list = datname_exclude_list;
		}
		else if (datname_include_list)
		{
			restore_params->partial_restore_type = INCLUDE;
			restore_params->partial_db_list = datname_include_list;
		}
	}

	/*
	 * Parse set-backup options into set_backup_params structure.
	 */
	if (backup_subcmd == SET_BACKUP_CMD || backup_subcmd == BACKUP_CMD)
	{
		time_t expire_time = 0;

		if (expire_time_string && ttl >= 0)
			elog(ERROR, "You cannot specify '--expire-time' and '--ttl' options together");

		/* Parse string to seconds */
		if (expire_time_string)
		{
			if (!parse_time(expire_time_string, &expire_time, false))
				elog(ERROR, "Invalid value for '--expire-time' option: '%s'",
					 expire_time_string);
		}

		if (expire_time > 0 || ttl >= 0 || backup_note)
		{
			set_backup_params = pgut_new(pgSetBackupParams);
			set_backup_params->ttl = ttl;
			set_backup_params->expire_time = expire_time;
			set_backup_params->note = backup_note;

			if (backup_note && strlen(backup_note) > MAX_NOTE_SIZE)
				elog(ERROR, "Backup note cannot exceed %u bytes", MAX_NOTE_SIZE);
		}
	}

	/* sanity */
	if (backup_subcmd == VALIDATE_CMD && restore_params->no_validate)
		elog(ERROR, "You cannot specify \"--no-validate\" option with the \"%s\" command",
			get_subcmd_name(backup_subcmd));

	if (num_threads < 1)
		num_threads = 1;

	if (batch_size < 1)
		batch_size = 1;

	compress_init(backup_subcmd);

	/* do actual operation */
	switch (backup_subcmd)
	{
		case ARCHIVE_PUSH_CMD:
			do_archive_push(instanceState, &instance_config, wal_file_path, wal_file_name,
							batch_size, file_overwrite, no_sync, no_ready_rename);
			break;
		case ARCHIVE_GET_CMD:
			do_archive_get(instanceState, &instance_config, prefetch_dir,
						   wal_file_path, wal_file_name, batch_size, !no_validate_wal);
			break;
		case ADD_INSTANCE_CMD:
			return do_add_instance(instanceState, &instance_config);
		case DELETE_INSTANCE_CMD:
			return do_delete_instance(instanceState);
		case INIT_CMD:
			return do_init(catalogState);
		case BACKUP_CMD:
			{
				current.stream = stream_wal;

				/* sanity */
				if (current.backup_mode == BACKUP_MODE_INVALID)
					elog(ERROR, "required parameter not specified: BACKUP_MODE "
						 "(-b, --backup-mode)");

				return do_backup(instanceState, set_backup_params,
								 no_validate, no_sync, backup_logs);
			}
		case RESTORE_CMD:
			return do_restore_or_validate(instanceState, current.backup_id,
							recovery_target_options,
							restore_params, no_sync);
		case VALIDATE_CMD:
			if (current.backup_id == 0 && target_time == 0 && target_xid == 0 && !target_lsn)
			{
				/* sanity */
				if (datname_exclude_list || datname_include_list)
					elog(ERROR, "You must specify parameter (-i, --backup-id) for partial validation");

				return do_validate_all(catalogState, instanceState);
			}
			else
				/* PITR validation and, optionally, partial validation */
				return do_restore_or_validate(instanceState, current.backup_id,
						  recovery_target_options,
						  restore_params,
						  no_sync);
		case SHOW_CMD:
			return do_show(catalogState, instanceState, current.backup_id, show_archive);
		case DELETE_CMD:
			if (delete_expired && backup_id_string)
				elog(ERROR, "You cannot specify --delete-expired and (-i, --backup-id) options together");
			if (merge_expired && backup_id_string)
				elog(ERROR, "You cannot specify --merge-expired and (-i, --backup-id) options together");
			if (delete_status && backup_id_string)
				elog(ERROR, "You cannot specify --status and (-i, --backup-id) options together");
			if (!delete_expired && !merge_expired && !delete_wal && delete_status == NULL && !backup_id_string)
				elog(ERROR, "You must specify at least one of the delete options: "
								"--delete-expired |--delete-wal |--merge-expired |--status |(-i, --backup-id)");
			if (!backup_id_string)
			{
				if (delete_status)
					do_delete_status(instanceState, &instance_config, delete_status);
				else
					do_retention(instanceState);
			}
			else
					do_delete(instanceState, current.backup_id);
			break;
		case MERGE_CMD:
			do_merge(instanceState, current.backup_id);
			break;
		case SHOW_CONFIG_CMD:
			do_show_config();
			break;
		case SET_CONFIG_CMD:
			do_set_config(instanceState, false);
			break;
		case SET_BACKUP_CMD:
			if (!backup_id_string)
				elog(ERROR, "You must specify parameter (-i, --backup-id) for 'set-backup' command");
			do_set_backup(instanceState, current.backup_id, set_backup_params);
			break;
		case CHECKDB_CMD:
			do_checkdb(need_amcheck,
					   instance_config.conn_opt, instance_config.pgdata);
			break;
		case NO_CMD:
			/* Should not happen */
			elog(ERROR, "Unknown subcommand");
		case SSH_CMD:
		case AGENT_CMD:
			/* Может перейти на использование какого-нибудь do_agent() для однобразия? */
		case HELP_CMD:
		case VERSION_CMD:
			/* Silence compiler warnings, these already handled earlier */
			break;
	}

	return 0;
}

static void
opt_incr_restore_mode(ConfigOption *opt, const char *arg)
{
	if (pg_strcasecmp(arg, "none") == 0)
	{
		incremental_mode = INCR_NONE;
		return;
	}
	else if (pg_strcasecmp(arg, "checksum") == 0)
	{
		incremental_mode = INCR_CHECKSUM;
		return;
	}
	else if (pg_strcasecmp(arg, "lsn") == 0)
	{
		incremental_mode = INCR_LSN;
		return;
	}

	/* Backup mode is invalid, so leave with an error */
	elog(ERROR, "Invalid value for '--incremental-mode' option: '%s'", arg);
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
compress_init(ProbackupSubcmd const subcmd)
{
	/* Default algorithm is zlib */
	if (compress_shortcut)
		instance_config.compress_alg = ZLIB_COMPRESS;

	if (subcmd != SET_CONFIG_CMD)
	{
		if (instance_config.compress_level != COMPRESS_LEVEL_DEFAULT
			&& instance_config.compress_alg == NOT_DEFINED_COMPRESS)
			elog(ERROR, "Cannot specify compress-level option alone without "
												"compress-algorithm option");
	}

	if (instance_config.compress_level < 0 || instance_config.compress_level > 9)
		elog(ERROR, "--compress-level value must be in the range from 0 to 9");

	if (instance_config.compress_alg == ZLIB_COMPRESS && instance_config.compress_level == 0)
		elog(WARNING, "Compression level 0 will lead to data bloat!");

	if (subcmd == BACKUP_CMD || subcmd == ARCHIVE_PUSH_CMD)
	{
#ifndef HAVE_LIBZ
		if (instance_config.compress_alg == ZLIB_COMPRESS)
			elog(ERROR, "This build does not support zlib compression");
		else
#endif
		if (instance_config.compress_alg == PGLZ_COMPRESS && num_threads > 1)
			elog(ERROR, "Multithread backup does not support pglz compression");
	}
}

/* Construct array of datnames, provided by user via db-exclude option */
void
opt_datname_exclude_list(ConfigOption *opt, const char *arg)
{
	char *dbname = NULL;

	if (!datname_exclude_list)
		datname_exclude_list =  parray_new();

	dbname = pgut_malloc(strlen(arg) + 1);

	/* TODO add sanity for database name */
	strcpy(dbname, arg);

	parray_append(datname_exclude_list, dbname);
}

/* Construct array of datnames, provided by user via db-include option */
void
opt_datname_include_list(ConfigOption *opt, const char *arg)
{
	char *dbname = NULL;

	if (!datname_include_list)
		datname_include_list =  parray_new();

	dbname = pgut_malloc(strlen(arg) + 1);

	if (strcmp(dbname, "tempate0") == 0 ||
		strcmp(dbname, "tempate1") == 0)
		elog(ERROR, "Databases 'template0' and 'template1' cannot be used for partial restore or validation");

	strcpy(dbname, arg);

	parray_append(datname_include_list, dbname);
}
