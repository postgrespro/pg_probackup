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
	FILE   *fp;
	uint64 _system_identifier;

	struct dirent **dp;
	int results;

	/* PGDATA is always required */
	if (pgdata == NULL)
		elog(ERROR, "Required parameter not specified: PGDATA "
						 "(-D, --pgdata)");

	if (access(backup_path, F_OK) == 0)
	{
		results = scandir(backup_path, &dp, selects, NULL);
		if (results != 0)
			elog(ERROR, "backup catalog already exist and it's not empty");
	}

	/* create backup catalog root directory */
	dir_create_dir(backup_path, DIR_PERMISSION);

	/* create directories for backup of online files */
	join_path_components(path, backup_path, BACKUPS_DIR);
	dir_create_dir(path, DIR_PERMISSION);

	/* Create "wal" directory */
	join_path_components(arclog_path_dir, backup_path, "wal");
	dir_create_dir(arclog_path_dir, DIR_PERMISSION);

	_system_identifier = get_system_identifier(false);
	/* create pg_probackup.conf */
	join_path_components(path, backup_path, BACKUP_CATALOG_CONF_FILE);
	fp = fopen(path, "wt");
	if (fp == NULL)
		elog(ERROR, "cannot create pg_probackup.conf: %s", strerror(errno));

	fprintf(fp, "system-identifier = %li\n", _system_identifier);
	fprintf(fp, "\n");
	fclose(fp);

	return 0;
}
