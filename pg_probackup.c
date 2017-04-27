/*-------------------------------------------------------------------------
 *
 * pg_probackup.c: Backup/Recovery manager for PostgreSQL.
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"
#include "streamutil.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/stat.h>

const char *PROGRAM_VERSION	= "1.1.11";
const char *PROGRAM_URL		= "https://github.com/postgrespro/pg_probackup";
const char *PROGRAM_EMAIL	= "https://github.com/postgrespro/pg_probackup/issues";

/* path configuration */
char	   *backup_path = NULL;
char	   *pgdata = NULL;
char		arclog_path[MAXPGPATH] = "";

/* directory configuration */
pgBackup	current;
ProbackupSubcmd backup_subcmd;

bool		help = false;

char	   *backup_id_string_param = NULL;
bool		backup_logs = false;

bool		smooth_checkpoint;
int			num_threads = 1;
bool		stream_wal = false;
bool		from_replica = false;
bool		progress = false;
bool		delete_wal = false;
bool		delete_expired = false;
bool		apply_to_all = false;
bool		force_delete = false;
/* Wait timeout for WAL segment archiving */
uint32		archive_timeout = 300;

uint64		system_identifier = 0;

uint32		retention_redundancy = 0;
uint32		retention_window = 0;

/* restore configuration */
static char		   *target_time;
static char		   *target_xid;
static char		   *target_inclusive;
static TimeLineID	target_tli;

static void opt_backup_mode(pgut_option *opt, const char *arg);
static void opt_log_level(pgut_option *opt, const char *arg);

static pgut_option options[] =
{
	/* directory options */
	{ 'b',  1,  "help",					&help,				SOURCE_CMDLINE },
	{ 's', 'D', "pgdata",				&pgdata,			SOURCE_CMDLINE },
	{ 's', 'B', "backup-path",			&backup_path,		SOURCE_CMDLINE },
	/* common options */
	{ 'u', 'j', "threads",				&num_threads,		SOURCE_CMDLINE },
	{ 'b', 1, "stream",					&stream_wal,		SOURCE_CMDLINE },
	{ 'b', 2, "progress",				&progress,			SOURCE_CMDLINE },
	{ 's', 'i', "backup-id",			&backup_id_string_param, SOURCE_CMDLINE },
	/* backup options */
	{ 'b', 10, "backup-pg-log",			&backup_logs,		SOURCE_CMDLINE },
	{ 'f', 'b', "backup-mode",			opt_backup_mode,	SOURCE_CMDLINE },
	{ 'b', 'C', "smooth-checkpoint",	&smooth_checkpoint,	SOURCE_CMDLINE },
	{ 's', 'S', "slot",					&replication_slot,	SOURCE_CMDLINE },
	{ 'u', 11, "archive-timeout",		&archive_timeout,	SOURCE_CMDLINE },
	{ 'b', 12, "delete-expired",		&delete_expired,	SOURCE_CMDLINE },
	/* restore options */
	{ 's', 20, "time",					&target_time,		SOURCE_CMDLINE },
	{ 's', 21, "xid",					&target_xid,		SOURCE_CMDLINE },
	{ 's', 22, "inclusive",				&target_inclusive,	SOURCE_CMDLINE },
	{ 'u', 23, "timeline",				&target_tli,		SOURCE_CMDLINE },
	{ 'f', 'T', "tablespace-mapping",	opt_tablespace_map,	SOURCE_CMDLINE },
	/* delete options */
	{ 'b', 30, "wal",					&delete_wal,		SOURCE_CMDLINE },
	{ 'b', 31, "expired",				&delete_expired,	SOURCE_CMDLINE },
	{ 'b', 32, "all",					&apply_to_all,		SOURCE_CMDLINE },
	/* TODO not implemented yet */
	{ 'b', 33, "force",					&force_delete,		SOURCE_CMDLINE },
	/* retention options */
	{ 'u', 34, "retention-redundancy",	&retention_redundancy, SOURCE_CMDLINE },
	{ 'u', 35, "retention-window",		&retention_window,	SOURCE_CMDLINE },
	/* logging options */
	{ 'f', 40, "log-level",				opt_log_level,		SOURCE_CMDLINE },
	{ 's', 41, "log-filename",			&log_filename,		SOURCE_CMDLINE },
	{ 's', 42, "error-log-filename",	&error_log_filename, SOURCE_CMDLINE },
	{ 's', 43, "log-directory",			&log_directory,		SOURCE_CMDLINE },
	{ 'u', 44, "log-rotation-size",		&log_rotation_size,	SOURCE_CMDLINE },
	{ 'u', 45, "log-rotation-age",		&log_rotation_age,	SOURCE_CMDLINE },
	/* connection options */
	{ 's', 'd', "pgdatabase",			&pgut_dbname,		SOURCE_CMDLINE },
	{ 's', 'h', "pghost",				&host,				SOURCE_CMDLINE },
	{ 's', 'p', "pgport",				&port,				SOURCE_CMDLINE },
	{ 's', 'U', "pguser",				&username,			SOURCE_CMDLINE },
	{ 'B', 'w', "no-password",			&prompt_password,	SOURCE_CMDLINE },
	/* other options */
	{ 'U', 50, "system-identifier",		&system_identifier,	SOURCE_FILE_STRICT },
	{ 0 }
};

/*
 * Entry point of pg_probackup command.
 */
int
main(int argc, char *argv[])
{
	char		path[MAXPGPATH];
	/* Check if backup_path is directory. */
	struct stat stat_buf;
	int			rc;

	/* initialize configuration */
	pgBackup_init(&current);

	PROGRAM_NAME = get_progname(argv[0]);
	set_pglocale_pgservice(argv[0], "pgscripts");

	/* Parse subcommands and non-subcommand options */
	if (argc > 1)
	{
		if (strcmp(argv[1], "init") == 0)
			backup_subcmd = INIT;
		else if (strcmp(argv[1], "backup") == 0)
			backup_subcmd = BACKUP;
		else if (strcmp(argv[1], "restore") == 0)
			backup_subcmd = RESTORE;
		else if (strcmp(argv[1], "validate") == 0)
			backup_subcmd = VALIDATE;
		else if (strcmp(argv[1], "show") == 0)
			backup_subcmd = SHOW;
		else if (strcmp(argv[1], "delete") == 0)
			backup_subcmd = DELETE;
		else if (strcmp(argv[1], "set-config") == 0)
			backup_subcmd = SET_CONFIG;
		else if (strcmp(argv[1], "show-config") == 0)
			backup_subcmd = SHOW_CONFIG;
		else if (strcmp(argv[1], "--help") == 0
				|| strcmp(argv[1], "help") == 0
				|| strcmp(argv[1], "-?") == 0)
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
			if (argc == 2)
			{
				fprintf(stderr, "%s %s\n", PROGRAM_NAME, PROGRAM_VERSION);
				exit(0);
			}
			else if (strcmp(argv[2], "--help") == 0)
				help_command(argv[1]);
			else
				elog(ERROR, "Invalid arguments for \"%s\" subcommand", argv[1]);
		}
		else
			elog(ERROR, "Unknown subcommand");
	}

	/* Parse command line arguments */
	pgut_getopt(argc, argv, options);

	if (help)
		help_command(argv[2]);

	if (backup_path == NULL)
	{
		/* Try to read BACKUP_PATH from environment variable */
		backup_path = getenv("BACKUP_PATH");
		if (backup_path == NULL)
			elog(ERROR, "required parameter not specified: BACKUP_PATH (-B, --backup-path)");
	}

	rc = stat(backup_path, &stat_buf);
	/* If rc == -1,  there is no file or directory. So it's OK. */
	if (rc != -1 && !S_ISDIR(stat_buf.st_mode))
		elog(ERROR, "-B, --backup-path must be a path to directory");

	/* Do not read options from file or env if we're going to set them */
	if (backup_subcmd != SET_CONFIG)
	{
		/* Read options from configuration file */
		join_path_components(path, backup_path, BACKUP_CATALOG_CONF_FILE);
		pgut_readopt(path, options, ERROR);

		/* Read environment variables */
		pgut_getopt_env(options);
	}

	/*
	 * We read backup path from command line or from configuration file.
	 * Do final check.
	 */
	if (!is_absolute_path(backup_path))
		elog(ERROR, "-B, --backup-path must be an absolute path");

	/* Set log path */
	if (log_filename || error_log_filename)
	{
		if (log_directory)
			join_path_components(log_path, backup_path, log_directory);
		else
			join_path_components(log_path, backup_path, "log");
	}

	if (backup_id_string_param != NULL)
	{
		current.backup_id = base36dec(backup_id_string_param);
		if (current.backup_id == 0)
			elog(ERROR, "Invalid backup-id");
	}

	/* Setup stream options. They are used in streamutil.c. */
	if (pgut_dbname != NULL)
		dbname = pstrdup(pgut_dbname);
	if (host != NULL)
		dbhost = pstrdup(host);
	if (port != NULL)
		dbport = pstrdup(port);
	if (username != NULL)
		dbuser = pstrdup(username);

	/*
	 * We read pgdata path from command line or from configuration file.
	 * Do final check.
	 */
	if (pgdata != NULL && !is_absolute_path(pgdata))
		elog(ERROR, "-D, --pgdata must be an absolute path");

	join_path_components(arclog_path, backup_path, "wal");

	/* setup exclusion list for file search */
	if (!backup_logs)
	{
		int			i;

		for (i = 0; pgdata_exclude_dir[i]; i++);		/* find first empty slot */

		/* Set 'pg_log' in first empty slot */
		pgdata_exclude_dir[i] = "pg_log";
	}

	if (target_time != NULL && target_xid != NULL)
		elog(ERROR, "You can't specify recovery-target-time and recovery-target-xid at the same time");

	if (num_threads < 1)
		num_threads = 1;

	/* do actual operation */
	switch (backup_subcmd)
	{
		case INIT:
			return do_init();
		case BACKUP:
			return do_backup();
		case RESTORE:
			return do_restore_or_validate(current.backup_id,
						  target_time, target_xid,
						  target_inclusive, target_tli,
						  true);
		case VALIDATE:
			return do_restore_or_validate(current.backup_id,
						  target_time, target_xid,
						  target_inclusive, target_tli,
						  false);
		case SHOW:
			return do_show(current.backup_id);
		case DELETE:
			if (delete_expired && backup_id_string_param)
				elog(ERROR, "You cannot specify --delete-expired and --backup-id options together");
			if (delete_expired)
				return do_retention_purge();
			else
				return do_delete(current.backup_id);
		case SHOW_CONFIG:
			if (argc > 4)
				elog(ERROR, "show-config command doesn't accept any options");
			return do_configure(true);
		case SET_CONFIG:
			if (argc == 4)
				elog(ERROR, "set-config command requires at least one option");
			return do_configure(false);
	}

	return 0;
}

static void
opt_backup_mode(pgut_option *opt, const char *arg)
{
	current.backup_mode = parse_backup_mode(arg);
}

static void
opt_log_level(pgut_option *opt, const char *arg)
{
	log_level = parse_log_level(arg);
}
