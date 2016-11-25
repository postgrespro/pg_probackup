/*-------------------------------------------------------------------------
 *
 * init.c: manage backup catalog.
 *
 * Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

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
	char	arclog_path_dir[MAXPGPATH];
	char   *log_directory = NULL;
	char   *archive_command = NULL;
	FILE   *fp;
	uint64 _system_identifier;

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
	join_path_components(path, backup_path, BACKUPS_DIR);
	dir_create_dir(path, DIR_PERMISSION);

	/* read postgresql.conf */
	if (pgdata)
	{
		join_path_components(path, pgdata, "postgresql.conf");
		parse_postgresql_conf(path, &log_directory, &archive_command);
	}

	_system_identifier = get_system_identifier(false);
	/* create pg_probackup.conf */
	join_path_components(path, backup_path, PG_RMAN_INI_FILE);
	fp = fopen(path, "wt");
	if (fp == NULL)
		elog(ERROR, "cannot create pg_probackup.conf: %s", strerror(errno));

	join_path_components(arclog_path_dir, backup_path, "wal");
	dir_create_dir(arclog_path_dir, DIR_PERMISSION);

	fprintf(fp, "system-identifier = %li\n", _system_identifier);
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
