/*-------------------------------------------------------------------------
 *
 * pg_arman.c: Backup/Recovery manager for PostgreSQL.
 *
 * Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_arman.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/stat.h>

const char *PROGRAM_VERSION	= "0.1";
const char *PROGRAM_URL		= "https://github.com/michaelpq/pg_arman";
const char *PROGRAM_EMAIL	= "https://github.com/michaelpq/pg_arman/issues";

/* path configuration */
char *backup_path;
char *pgdata;
char *arclog_path = NULL;

/* common configuration */
bool check = false;

/* directory configuration */
pgBackup	current;

/* backup configuration */
static bool		smooth_checkpoint;
static int		keep_data_generations = KEEP_INFINITE;
static int		keep_data_days = KEEP_INFINITE;
static bool		backup_validate = false;

/* restore configuration */
static char		   *target_time;
static char		   *target_xid;
static char		   *target_inclusive;
static TimeLineID	target_tli;

/* show configuration */
static bool			show_all = false;

static void opt_backup_mode(pgut_option *opt, const char *arg);
static void parse_range(pgBackupRange *range, const char *arg1, const char *arg2);

static pgut_option options[] =
{
	/* directory options */
	{ 's', 'D', "pgdata",		&pgdata,		SOURCE_ENV },
	{ 's', 'A', "arclog-path",	&arclog_path,	SOURCE_ENV },
	{ 's', 'B', "backup-path",	&backup_path,	SOURCE_ENV },
	/* common options */
	{ 'b', 'c', "check",		&check },
	/* backup options */
	{ 'f', 'b', "backup-mode",			opt_backup_mode,		SOURCE_ENV },
	{ 'b', 'C', "smooth-checkpoint",	&smooth_checkpoint,		SOURCE_ENV },
	/* options with only long name (keep-xxx) */
	{ 'i',  1, "keep-data-generations", &keep_data_generations, SOURCE_ENV },
	{ 'i',  2, "keep-data-days",		&keep_data_days,		SOURCE_ENV },
	/* restore options */
	{ 's',  3, "recovery-target-time",		&target_time,		SOURCE_ENV },
	{ 's',  4, "recovery-target-xid",		&target_xid,		SOURCE_ENV },
	{ 's',  5, "recovery-target-inclusive", &target_inclusive,	SOURCE_ENV },
	{ 'u',  6, "recovery-target-timeline",	&target_tli,		SOURCE_ENV },
	{ 'b',  7, "validate",					&backup_validate,	SOURCE_ENV },
	/* catalog options */
	{ 'b', 'a', "show-all",					&show_all },
	{ 0 }
};

/*
 * Entry point of pg_arman command.
 */
int
main(int argc, char *argv[])
{
	const char	   *cmd = NULL;
	const char	   *range1 = NULL;
	const char	   *range2 = NULL;
	pgBackupRange	range;
	int				i;

	/* do not buffer progress messages */
	setvbuf(stdout, 0, _IONBF, 0);	/* TODO: remove this */

	/* initialize configuration */
	catalog_init_config(&current);

	/* overwrite configuration with command line arguments */
	i = pgut_getopt(argc, argv, options);

	for (; i < argc; i++)
	{
		if (cmd == NULL)
			cmd = argv[i];
		else if (range1 == NULL)
			range1 = argv[i];
		else if (range2 == NULL)
			range2 = argv[i];
		else
			elog(ERROR, "too many arguments");
	}

	/* command argument (backup/restore/show/...) is required. */
	if (cmd == NULL)
	{
		help(false);
		return 1;
	}

	/* get object range argument if any */
	if (range1 && range2)
		parse_range(&range, range1, range2);
	else if (range1)
		parse_range(&range, range1, "");
	else
		range.begin = range.end = 0;

	/* Read default configuration from file. */
	if (backup_path)
	{
		char	path[MAXPGPATH];
		/* Check if backup_path is directory. */
		struct stat stat_buf;
		int rc = stat(backup_path, &stat_buf);

		/* If rc == -1,  there is no file or directory. So it's OK. */
		if (rc != -1 && !S_ISDIR(stat_buf.st_mode))
			elog(ERROR, "-B, --backup-path must be a path to directory");

		join_path_components(path, backup_path, PG_RMAN_INI_FILE);
		pgut_readopt(path, options, ERROR);
	}

	/* BACKUP_PATH is always required */
	if (backup_path == NULL)
		elog(ERROR, "required parameter not specified: BACKUP_PATH (-B, --backup-path)");

	/* path must be absolute */
	if (backup_path != NULL && !is_absolute_path(backup_path))
		elog(ERROR, "-B, --backup-path must be an absolute path");
	if (pgdata != NULL && !is_absolute_path(pgdata))
		elog(ERROR, "-D, --pgdata must be an absolute path");
	if (arclog_path != NULL && !is_absolute_path(arclog_path))
		elog(ERROR, "-A, --arclog-path must be an absolute path");

	/* Sanity checks with commands */
	if (pg_strcasecmp(cmd, "delete") == 0 && arclog_path == NULL)
		elog(ERROR, "delete command needs ARCLOG_PATH (-A, --arclog-path) to be set");

	/* setup exclusion list for file search */
	for (i = 0; pgdata_exclude[i]; i++)		/* find first empty slot */
		;
	if (arclog_path)
		pgdata_exclude[i++] = arclog_path;

	/* do actual operation */
	if (pg_strcasecmp(cmd, "init") == 0)
		return do_init();
	else if (pg_strcasecmp(cmd, "backup") == 0)
	{
		pgBackupOption bkupopt;
		int res;
		bkupopt.smooth_checkpoint = smooth_checkpoint;
		bkupopt.keep_data_generations = keep_data_generations;
		bkupopt.keep_data_days = keep_data_days;

		/* Do the backup */
		res = do_backup(bkupopt);
		if (res != 0)
			return res;

		/* If validation has been requested, do it */
		range.begin = current.start_time;
		range.end = current.start_time + 1;
		if (backup_validate)
			do_validate(&range);
	}
	else if (pg_strcasecmp(cmd, "restore") == 0)
		return do_restore(target_time, target_xid,
					target_inclusive, target_tli);
	else if (pg_strcasecmp(cmd, "show") == 0)
		return do_show(&range, show_all);
	else if (pg_strcasecmp(cmd, "validate") == 0)
		return do_validate(&range);
	else if (pg_strcasecmp(cmd, "delete") == 0)
		return do_delete(&range);
	else
		elog(ERROR, "invalid command \"%s\"", cmd);

	return 0;
}

void
pgut_help(bool details)
{
	printf(_("%s manage backup/recovery of PostgreSQL database.\n\n"), PROGRAM_NAME);
	printf(_("Usage:\n"));
	printf(_("  %s OPTION init\n"), PROGRAM_NAME);
	printf(_("  %s OPTION backup\n"), PROGRAM_NAME);
	printf(_("  %s OPTION restore\n"), PROGRAM_NAME);
	printf(_("  %s OPTION show [DATE]\n"), PROGRAM_NAME);
	printf(_("  %s OPTION validate [DATE]\n"), PROGRAM_NAME);
	printf(_("  %s OPTION delete DATE\n"), PROGRAM_NAME);

	if (!details)
		return;

	printf(_("\nCommon Options:\n"));
	printf(_("  -D, --pgdata=PATH         location of the database storage area\n"));
	printf(_("  -A, --arclog-path=PATH    location of archive WAL storage area\n"));
	printf(_("  -B, --backup-path=PATH    location of the backup storage area\n"));
	printf(_("  -c, --check               show what would have been done\n"));
	printf(_("\nBackup options:\n"));
	printf(_("  -b, --backup-mode=MODE    full or page\n"));
	printf(_("  -C, --smooth-checkpoint   do smooth checkpoint before backup\n"));
	printf(_("  --validate                validate backup after taking it\n"));
	printf(_("  --keep-data-generations=N keep GENERATION of full data backup\n"));
	printf(_("  --keep-data-days=DAY      keep enough data backup to recover to DAY days age\n"));
	printf(_("\nRestore options:\n"));
	printf(_("  --recovery-target-time    time stamp up to which recovery will proceed\n"));
	printf(_("  --recovery-target-xid     transaction ID up to which recovery will proceed\n"));
	printf(_("  --recovery-target-inclusive whether we stop just after the recovery target\n"));
	printf(_("  --recovery-target-timeline  recovering into a particular timeline\n"));
	printf(_("\nCatalog options:\n"));
	printf(_("  -a, --show-all            show deleted backup too\n"));
}

/*
 * Create range object from one or two arguments.
 * All not-digit characters in the argument(s) are ignored.
 * Both arg1 and arg2 must be valid pointer.
 */
static void
parse_range(pgBackupRange *range, const char *arg1, const char *arg2)
{
	size_t		len = strlen(arg1) + strlen(arg2) + 1;
	char	   *tmp;
	int			num;
	struct tm	tm;

	tmp = pgut_malloc(len);
	tmp[0] = '\0';
	if (arg1 != NULL)
		remove_not_digit(tmp, len, arg1);
	if (arg2 != NULL)
		remove_not_digit(tmp + strlen(tmp), len - strlen(tmp), arg2);

	memset(&tm, 0, sizeof(tm));
	tm.tm_year = 0;		/* tm_year is year - 1900 */
	tm.tm_mon = 0;		/* tm_mon is 0 - 11 */
	tm.tm_mday = 1;		/* tm_mday is 1 - 31 */
	tm.tm_hour = 0;
	tm.tm_min = 0;
	tm.tm_sec = 0;
	num = sscanf(tmp, "%04d %02d %02d %02d %02d %02d",
		&tm.tm_year, &tm.tm_mon, &tm.tm_mday,
		&tm.tm_hour, &tm.tm_min, &tm.tm_sec);

	if (num < 1)
	{
		if (strcmp(tmp,"") != 0)
			elog(ERROR, "supplied id(%s) is invalid", tmp);
		else
			elog(ERROR, "arguments are invalid. near \"%s\"", arg1);
	}

	free(tmp);

	/* adjust year and month to convert to time_t */
	tm.tm_year -= 1900;
	if (num > 1)
		tm.tm_mon -= 1;
	tm.tm_isdst = -1;

	if (!IsValidTime(tm))
		elog(ERROR, "supplied time(%s) is invalid.", arg1);

	range->begin = mktime(&tm);

	switch (num)
	{
		case 1:
			tm.tm_year++;
			break;
		case 2:
			tm.tm_mon++;
			break;
		case 3:
			tm.tm_mday++;
			break;
		case 4:
			tm.tm_hour++;
			break;
		case 5:
			tm.tm_min++;
			break;
		case 6:
			tm.tm_sec++;
			break;
	}
	range->end = mktime(&tm);
	range->end--;
}

static void
opt_backup_mode(pgut_option *opt, const char *arg)
{
	current.backup_mode = parse_backup_mode(arg);
}
