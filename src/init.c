/*-------------------------------------------------------------------------
 *
 * init.c: - initialize backup catalog.
 *
 * Portions Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <unistd.h>
#include <sys/stat.h>

/*
 * Initialize backup catalog.
 */
int
do_init(void)
{
	char		path[MAXPGPATH];
	char		arclog_path_dir[MAXPGPATH];
	int			results;

	results = pg_check_dir(backup_path);
	if (results == 4)	/* exists and not empty*/
		elog(ERROR, "backup catalog already exist and it's not empty");
	else if (results == -1) /*trouble accessing directory*/
	{
		int errno_tmp = errno;
		elog(ERROR, "cannot open backup catalog directory \"%s\": %s",
			backup_path, strerror(errno_tmp));
	}

	/* create backup catalog root directory */
	dir_create_dir(backup_path, DIR_PERMISSION);

	/* create backup catalog data directory */
	join_path_components(path, backup_path, BACKUPS_DIR);
	dir_create_dir(path, DIR_PERMISSION);

	/* create backup catalog wal directory */
	join_path_components(arclog_path_dir, backup_path, "wal");
	dir_create_dir(arclog_path_dir, DIR_PERMISSION);

	elog(INFO, "Backup catalog '%s' successfully inited", backup_path);
	return 0;
}

int
do_add_instance(void)
{
	char		path[MAXPGPATH];
	char		arclog_path_dir[MAXPGPATH];
	struct stat st;

	/* PGDATA is always required */
	if (instance_config.pgdata == NULL)
		elog(ERROR, "Required parameter not specified: PGDATA "
						 "(-D, --pgdata)");

	/* Read system_identifier from PGDATA */
	instance_config.system_identifier = get_system_identifier(instance_config.pgdata);
	/* Starting from PostgreSQL 11 read WAL segment size from PGDATA */
	instance_config.xlog_seg_size = get_xlog_seg_size(instance_config.pgdata);

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
	 * Write initial configuration file.
	 * system-identifier, xlog-seg-size and pgdata are set in init subcommand
	 * and will never be updated.
	 *
	 * We need to manually set options source to save them to the configuration
	 * file.
	 */
	config_set_opt(instance_options, &instance_config.system_identifier,
				   SOURCE_FILE);
	config_set_opt(instance_options, &instance_config.xlog_seg_size,
				   SOURCE_FILE);
	/* pgdata was set through command line */
	do_set_config();

	elog(INFO, "Instance '%s' successfully inited", instance_name);
	return 0;
}
