/*-------------------------------------------------------------------------
 *
 * init.c: manage backup catalog.
 *
 * Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_arman.h"

#include <unistd.h>
#include <dirent.h>

static void parse_postgresql_conf(const char *path, char **log_directory,
								  char **archive_command);

/*
 * selects function for scandir.
 */
static int selects(const struct dirent *dir)
{
  return dir->d_name[0] != '.';
}

/*
 * Initialize backup catalog.
 */
int
do_init(void)
{
	char	path[MAXPGPATH];
	char   *log_directory = NULL;
	char   *archive_command = NULL;
	FILE   *fp;

	struct dirent **dp;
	int results;
	if (access(backup_path, F_OK) == 0)
	{
		results = scandir(backup_path, &dp, selects, NULL);
		if (results != 0)
			elog(ERROR, "backup catalog already exist. and it's not empty");
	}

	/* create backup catalog root directory */
	dir_create_dir(backup_path, DIR_PERMISSION);

	/* create directories for backup of online files */
	join_path_components(path, backup_path, RESTORE_WORK_DIR);
	dir_create_dir(path, DIR_PERMISSION);
	snprintf(path, lengthof(path), "%s/%s/%s", backup_path, RESTORE_WORK_DIR,
		PG_XLOG_DIR);
	dir_create_dir(path, DIR_PERMISSION);

	/* read postgresql.conf */
	if (pgdata)
	{
		join_path_components(path, pgdata, "postgresql.conf");
		parse_postgresql_conf(path, &log_directory, &archive_command);
	}

	/* create pg_arman.ini */
	join_path_components(path, backup_path, PG_RMAN_INI_FILE);
	fp = fopen(path, "wt");
	if (fp == NULL)
		elog(ERROR, "cannot create pg_arman.ini: %s", strerror(errno));

	/* set ARCLOG_PATH refered with log_directory */
	if (arclog_path == NULL && archive_command && archive_command[0])
	{
		char *command = pgut_strdup(archive_command);
		char *begin;
		char *end;
		char *fname;

		/* example: 'cp "%p" /path/to/arclog/"%f"' */
		for (begin = command; *begin;)
		{
			begin = begin + strspn(begin, " \n\r\t\v");
			end = begin + strcspn(begin, " \n\r\t\v");
			*end = '\0';

			if ((fname = strstr(begin, "%f")) != NULL)
			{
				while (strchr(" \n\r\t\v\"'", *begin))
					begin++;
				fname--;
				while (fname > begin && strchr(" \n\r\t\v\"'/", fname[-1]))
					fname--;
				*fname = '\0';

				if (is_absolute_path(begin))
					arclog_path = pgut_strdup(begin);
				break;
			}

			begin = end + 1;
		}

		free(command);
	}
	if (arclog_path)
	{
		fprintf(fp, "ARCLOG_PATH='%s'\n", arclog_path);
		elog(INFO, "ARCLOG_PATH is set to '%s'", arclog_path);
	}
	else if (archive_command && archive_command[0])
		elog(WARNING, "ARCLOG_PATH is not set because failed to parse archive_command '%s'."
				"Please set ARCLOG_PATH in pg_arman.ini or environmental variable", archive_command);
	else
		elog(WARNING, "ARCLOG_PATH is not set because archive_command is empty."
				"Please set ARCLOG_PATH in pg_arman.ini or environmental variable");

	fprintf(fp, "\n");
	fclose(fp);

	free(archive_command);
	free(log_directory);

	return 0;
}

static void
parse_postgresql_conf(const char *path,
					  char **log_directory,
					  char **archive_command)
{
	pgut_option options[] =
	{
		{ 's', 0, "log_directory"		, NULL, SOURCE_ENV },
		{ 's', 0, "archive_command"		, NULL, SOURCE_ENV },
		{ 0 }
	};

	options[0].var = log_directory;
	options[1].var = archive_command;

	pgut_readopt(path, options, LOG);	/* ignore unknown options */
}
