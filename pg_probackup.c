/*-------------------------------------------------------------------------
 *
 * pg_probackup.c: Backup/Recovery manager for PostgreSQL.
 *
 * Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"
#include "streamutil.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/stat.h>

const char *PROGRAM_VERSION	= "1.0";
const char *PROGRAM_URL		= "https://github.com/postgrespro/pg_probackup";
const char *PROGRAM_EMAIL	= "https://github.com/postgrespro/pg_probackup/issues";

/* path configuration */
char *backup_path;
char *pgdata;
char arclog_path[MAXPGPATH];

/* common configuration */
bool check = false;

/* directory configuration */
pgBackup	current;

/* backup configuration */
static bool		smooth_checkpoint;
int				num_threads = 1;
bool			stream_wal = false;
bool			from_replica = false;
static bool		backup_logs = false;
bool			progress = false;
bool			delete_wal = false;

/* restore configuration */
static char		   *target_time;
static char		   *target_xid;
static char		   *target_inclusive;
static TimeLineID	target_tli;

uint64			system_identifier = 0;

/* retention configuration */
uint32			retention_redundancy = 0;
uint32			retention_window = 0;

static void opt_backup_mode(pgut_option *opt, const char *arg);

static pgut_option options[] =
{
	/* directory options */
	{ 's', 'D', "pgdata",				&pgdata,		SOURCE_CMDLINE },
	{ 's', 'B', "backup-path",			&backup_path,	SOURCE_CMDLINE },
	/* common options */
/*	{ 'b', 'c', "check",				&check },*/
	{ 'u', 'j', "threads",				&num_threads,	SOURCE_CMDLINE },
	{ 'b', 8, "stream",					&stream_wal,	SOURCE_CMDLINE },
	{ 'b', 11, "progress",				&progress,		SOURCE_CMDLINE },
	/* backup options */
	{ 'b', 10, "backup-pg-log",			&backup_logs,	SOURCE_CMDLINE },
	{ 'f', 'b', "backup-mode",			opt_backup_mode,		SOURCE_CMDLINE },
	{ 'b', 'C', "smooth-checkpoint",	&smooth_checkpoint,		SOURCE_CMDLINE },
	{ 's', 'S', "slot",					&replication_slot,		SOURCE_CMDLINE },
	/* options with only long name (keep-xxx) */
	/* restore options */
	{ 's',  3, "time",					&target_time,		SOURCE_CMDLINE },
	{ 's',  4, "xid",					&target_xid,		SOURCE_CMDLINE },
	{ 's',  5, "inclusive",				&target_inclusive,	SOURCE_CMDLINE },
	{ 'u',  6, "timeline",				&target_tli,		SOURCE_CMDLINE },
	/* delete options */
	{ 'b', 12, "wal",					&delete_wal },
	/* retention options */
	{ 'u', 13, "redundancy",			&retention_redundancy,	SOURCE_CMDLINE },
	{ 'u', 14, "window",				&retention_window,		SOURCE_CMDLINE },
	/* other */
	{ 'U', 15, "system-identifier",		&system_identifier,		SOURCE_FILE_STRICT },
	{ 0 }
};

/*
 * Entry point of pg_probackup command.
 */
int
main(int argc, char *argv[])
{
	const char	   *cmd = NULL,
				   *subcmd = NULL;
	const char	   *backup_id_string = NULL;
	time_t			backup_id = 0;
	int				i;

	/* do not buffer progress messages */
	setvbuf(stdout, 0, _IONBF, 0);	/* TODO: remove this */

	/* initialize configuration */
	init_backup(&current);

	/* overwrite configuration with command line arguments */
	i = pgut_getopt(argc, argv, options);

	for (; i < argc; i++)
	{
		if (cmd == NULL)
			cmd = argv[i];
		else if (strcmp(cmd, "retention") == 0)
			subcmd = argv[i];
		else if (backup_id_string == NULL &&
				 (strcmp(cmd, "show") == 0 ||
				 strcmp(cmd, "validate") == 0 ||
				 strcmp(cmd, "delete") == 0 ||
				 strcmp(cmd, "restore") == 0 ||
				 strcmp(cmd, "delwal") == 0))
			backup_id_string = argv[i];
		else
			elog(ERROR, "too many arguments");
	}

	/* command argument (backup/restore/show/...) is required. */
	if (cmd == NULL)
	{
		help(false);
		return 1;
	}

	if (backup_id_string != NULL)
	{
		backup_id = base36dec(backup_id_string);
		if (backup_id == 0) {
			elog(ERROR, "wrong ID");
		}
	}

	/* BACKUP_PATH is always required */
	if (backup_path == NULL)
		elog(ERROR, "required parameter not specified: BACKUP_PATH (-B, --backup-path)");
	else
	{
		char		path[MAXPGPATH];
		/* Check if backup_path is directory. */
		struct stat stat_buf;
		int			rc = stat(backup_path, &stat_buf);

		/* If rc == -1,  there is no file or directory. So it's OK. */
		if (rc != -1 && !S_ISDIR(stat_buf.st_mode))
			elog(ERROR, "-B, --backup-path must be a path to directory");

		join_path_components(path, backup_path, BACKUP_CATALOG_CONF_FILE);
		pgut_readopt(path, options, ERROR);
	}

	/* setup stream options */
	if (pgut_dbname != NULL)
		dbname = pstrdup(pgut_dbname);
	if (host != NULL)
		dbhost = pstrdup(host);
	if (port != NULL)
		dbport = pstrdup(port);
	if (username != NULL)
		dbuser = pstrdup(username);

	/* path must be absolute */
	if (!is_absolute_path(backup_path))
		elog(ERROR, "-B, --backup-path must be an absolute path");
	if (pgdata != NULL && !is_absolute_path(pgdata))
		elog(ERROR, "-D, --pgdata must be an absolute path");

	join_path_components(arclog_path, backup_path, "wal");

	/* setup exclusion list for file search */
	for (i = 0; pgdata_exclude[i]; i++);		/* find first empty slot */

	pgdata_exclude[i++] = arclog_path;

	if(!backup_logs)
		pgdata_exclude[i++] = "pg_log";

	if (target_time != NULL && target_xid != NULL)
		elog(ERROR, "You can't specify recovery-target-time and recovery-target-xid at the same time");

	if (num_threads < 1)
		num_threads = 1;

	/* do actual operation */
	if (pg_strcasecmp(cmd, "init") == 0)
		return do_init();
	else if (pg_strcasecmp(cmd, "backup") == 0)
	{
		int			res;

		/* Do the backup */
		res = do_backup(smooth_checkpoint);
		if (res != 0)
			return res;

		do_validate_last();
	}
	else if (pg_strcasecmp(cmd, "restore") == 0)
		return do_restore(backup_id,
						  target_time,
						  target_xid,
						  target_inclusive,
						  target_tli);
	else if (pg_strcasecmp(cmd, "show") == 0)
		return do_show(backup_id);
	else if (pg_strcasecmp(cmd, "validate") == 0)
	{
		if (backup_id == 0)
			elog(ERROR, "you must specify backup-ID for this command");
		return do_validate(backup_id,
						   target_time,
						   target_xid,
						   target_inclusive,
						   target_tli);
	}
	else if (pg_strcasecmp(cmd, "delete") == 0)
		return do_delete(backup_id);
	else if (pg_strcasecmp(cmd, "delwal") == 0)
		return do_deletewal(backup_id, true);
	else if (pg_strcasecmp(cmd, "retention") == 0)
	{
		if (subcmd == NULL)
			elog(ERROR, "you must specify retention command");
		else if (pg_strcasecmp(subcmd, "show") == 0)
			return do_retention_show();
		else if (pg_strcasecmp(subcmd, "purge") == 0)
			return do_retention_purge();
	}
	else
		elog(ERROR, "invalid command \"%s\"", cmd);

	return 0;
}

void
pgut_help(bool details)
{
	printf(_("%s manage backup/recovery of PostgreSQL database.\n\n"), PROGRAM_NAME);
	printf(_("Usage:\n"));
	printf(_("  %s [option...] init\n"), PROGRAM_NAME);
	printf(_("  %s [option...] backup\n"), PROGRAM_NAME);
	printf(_("  %s [option...] restore [backup-ID]\n"), PROGRAM_NAME);
	printf(_("  %s [option...] show [backup-ID]\n"), PROGRAM_NAME);
	printf(_("  %s [option...] validate backup-ID\n"), PROGRAM_NAME);
	printf(_("  %s [option...] delete backup-ID\n"), PROGRAM_NAME);
	printf(_("  %s [option...] delwal [backup-ID]\n"), PROGRAM_NAME);
	printf(_("  %s [option...] retention show|purge\n"), PROGRAM_NAME);

	if (!details)
		return;

	printf(_("\nCommon Options:\n"));
	printf(_("  -B, --backup-path=PATH    location of the backup storage area\n"));
	printf(_("  -D, --pgdata=PATH         location of the database storage area\n"));
	/*printf(_("  -c, --check               show what would have been done\n"));*/
	printf(_("\nBackup options:\n"));
	printf(_("  -b, --backup-mode=MODE    backup mode (full, page, ptrack)\n"));
	printf(_("  -C, --smooth-checkpoint   do smooth checkpoint before backup\n"));
	printf(_("      --stream              stream the transaction log and include it in the backup\n"));
	printf(_("  -S, --slot=SLOTNAME       replication slot to use\n"));
	printf(_("      --backup-pg-log       backup of pg_log directory\n"));
	printf(_("  -j, --threads=NUM         number of parallel threads\n"));
	printf(_("      --progress            show progress\n"));
	printf(_("\nRestore options:\n"));
	printf(_("      --time                time stamp up to which recovery will proceed\n"));
	printf(_("      --xid                 transaction ID up to which recovery will proceed\n"));
	printf(_("      --inclusive           whether we stop just after the recovery target\n"));
	printf(_("      --timeline            recovering into a particular timeline\n"));
	printf(_("  -j, --threads=NUM         number of parallel threads\n"));
	printf(_("      --progress            show progress\n"));
	printf(_("\nDelete options:\n"));
	printf(_("      --wal                 remove unnecessary wal files\n"));
	printf(_("\nRetention options:\n"));
	printf(_("      --redundancy          specifies how many full backups purge command should keep\n"));
	printf(_("      --window              specifies the number of days of recoverability\n"));
}

static void
opt_backup_mode(pgut_option *opt, const char *arg)
{
	current.backup_mode = parse_backup_mode(arg);
}
