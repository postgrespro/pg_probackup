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
#include <sys/stat.h>

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

	if (access(backup_path, F_OK) == 0)
	{
		results = scandir(backup_path, &dp, selects, NULL);
		if (results != 0)
			elog(ERROR, "backup catalog already exist and it's not empty");
	}

	/* create backup catalog root directory */
	dir_create_dir(backup_path, DIR_PERMISSION);

	/* create backup catalog data directory */
	join_path_components(path, backup_path, BACKUPS_DIR);
	dir_create_dir(path, DIR_PERMISSION);

	/* create backup catalog wal directory */
	join_path_components(arclog_path_dir, backup_path, "wal");
	dir_create_dir(arclog_path_dir, DIR_PERMISSION);

	return 0;
}

int
do_add_instance(void)
{
	char		path[MAXPGPATH];
	char		arclog_path_dir[MAXPGPATH];
    struct stat st;
	pgBackupConfig *config = pgut_new(pgBackupConfig);

	/* PGDATA is always required */
	if (pgdata == NULL)
		elog(ERROR, "Required parameter not specified: PGDATA "
						 "(-D, --pgdata)");

	/* Read system_identifier from PGDATA */
	system_identifier = get_system_identifier(pgdata);

	/* Ensure that all root directories already exist */
	if (access(backup_path, F_OK) != 0)
		elog(ERROR, "%s directory does not exist.", backup_path);

	join_path_components(path, backup_path, BACKUPS_DIR);
	if (access(path, F_OK) != 0)
		elog(ERROR, "%s directory does not exist.", path);

	join_path_components(arclog_path_dir, backup_path, "wal");
	if (access(arclog_path_dir, F_OK) != 0)
		elog(ERROR, "%s directory does not exist.", arclog_path_dir);

	/* Create directory for data files of this specific instance */
	if (stat(backup_instance_path, &st) == 0 && S_ISDIR(st.st_mode))
		elog(ERROR, "instance '%s' already exists", backup_instance_path);
	dir_create_dir(backup_instance_path, DIR_PERMISSION);

	/*
	 * Create directory for wal files of this specific instance.
	 * Existence check is extra paranoid because if we don't have such a
	 * directory in data dir, we shouldn't have it in wal as well.
	 */
	if (stat(arclog_path, &st) == 0 && S_ISDIR(st.st_mode))
		elog(ERROR, "arclog_path '%s' already exists", arclog_path);
	dir_create_dir(arclog_path, DIR_PERMISSION);

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
