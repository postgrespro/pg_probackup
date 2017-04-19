/*-------------------------------------------------------------------------
 *
 * init.c: - initialize backup catalog.
 *
 * Portions Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <unistd.h>
#include <dirent.h>

/*
 * selects function for scandir.
 * Select all files except hidden.
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
	char		path[MAXPGPATH];
	char		arclog_path_dir[MAXPGPATH];

	struct dirent **dp;
	int results;
	pgBackupConfig *config = pgut_new(pgBackupConfig);

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

	/* Read system_identifier from PGDATA */
	system_identifier = get_system_identifier();

	/* create backup catalog root directory */
	dir_create_dir(backup_path, DIR_PERMISSION);

	/* create directories for backup of online files */
	join_path_components(path, backup_path, BACKUPS_DIR);
	dir_create_dir(path, DIR_PERMISSION);

	/* Create "wal" directory */
	join_path_components(arclog_path_dir, backup_path, "wal");
	dir_create_dir(arclog_path_dir, DIR_PERMISSION);

	/*
	 * Wite initial config. system-identifier and pgdata are set in
	 * init subcommand and will never be updated.
	 */
	pgBackupConfigInit(config);
	config->system_identifier = system_identifier;
	config->pgdata = pgdata;
	writeBackupCatalogConfigFile(config);

	return 0;
}
