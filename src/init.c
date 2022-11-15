/*-------------------------------------------------------------------------
 *
 * init.c: - initialize backup catalog.
 *
 * Portions Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <unistd.h>

/*
 * Initialize backup catalog.
 */
int
do_init(CatalogState *catalogState)
{
	pioDrive_i	backup_location = pioDriveForLocation(FIO_BACKUP_HOST);
	int			results;
	err_i		err;

	results = pg_check_dir(catalogState->catalog_path);

	if (results == 4)	/* exists and not empty*/
		elog(ERROR, "backup catalog already exist and it's not empty");
	else if (results == -1) /*trouble accessing directory*/
	{
		int errno_tmp = errno;
		elog(ERROR, "cannot open backup catalog directory \"%s\": %s",
			catalogState->catalog_path, strerror(errno_tmp));
	}

	/* create backup catalog root directory */
	err = $i(pioMakeDir, backup_location, .path = catalogState->catalog_path,
			 .mode = DIR_PERMISSION, .strict = false);
	if ($haserr(err))
	{
		elog(WARNING, "%s", $errmsg(err));
	}

	/* create backup catalog data directory */
	err = $i(pioMakeDir, backup_location, .path = catalogState->backup_subdir_path,
			 .mode = DIR_PERMISSION, .strict = false);
	if ($haserr(err))
	{
		elog(WARNING, "%s", $errmsg(err));
	}

	/* create backup catalog wal directory */
	err = $i(pioMakeDir, backup_location, .path = catalogState->wal_subdir_path,
			 .mode = DIR_PERMISSION, .strict = false);
	if ($haserr(err))
	{
		elog(WARNING, "%s", $errmsg(err));
	}

	elog(INFO, "Backup catalog '%s' successfully inited", catalogState->catalog_path);
	return 0;
}

int
do_add_instance(InstanceState *instanceState, InstanceConfig *instance)
{
	pioDrive_i	backup_location = pioDriveForLocation(FIO_BACKUP_HOST);
	struct stat st;
	CatalogState *catalogState = instanceState->catalog_state;
	err_i		err;

	/* PGDATA is always required */
	if (instance->pgdata == NULL)
		elog(ERROR, "Required parameter not specified: PGDATA "
						 "(-D, --pgdata)");

	/* Read system_identifier from PGDATA */
	instance->system_identifier = get_system_identifier(FIO_DB_HOST, instance->pgdata, false);
	/* Starting from PostgreSQL 11 read WAL segment size from PGDATA */
	instance->xlog_seg_size = get_xlog_seg_size(instance->pgdata);

	/* Ensure that all root directories already exist */
	/* TODO maybe call do_init() here instead of error?*/
	if (access(catalogState->catalog_path, F_OK) != 0)
		elog(ERROR, "Directory does not exist: '%s'", catalogState->catalog_path);

	if (access(catalogState->backup_subdir_path, F_OK) != 0)
		elog(ERROR, "Directory does not exist: '%s'", catalogState->backup_subdir_path);

	if (access(catalogState->wal_subdir_path, F_OK) != 0)
		elog(ERROR, "Directory does not exist: '%s'", catalogState->wal_subdir_path);

	if (stat(instanceState->instance_backup_subdir_path, &st) == 0 && S_ISDIR(st.st_mode))
		elog(ERROR, "Instance '%s' backup directory already exists: '%s'",
			instanceState->instance_name, instanceState->instance_backup_subdir_path);

	/*
	 * Create directory for wal files of this specific instance.
	 * Existence check is extra paranoid because if we don't have such a
	 * directory in data dir, we shouldn't have it in wal as well.
	 */
	if (stat(instanceState->instance_wal_subdir_path, &st) == 0 && S_ISDIR(st.st_mode))
		elog(ERROR, "Instance '%s' WAL archive directory already exists: '%s'",
				instanceState->instance_name, instanceState->instance_wal_subdir_path);

	/* Create directory for data files of this specific instance */
	err = $i(pioMakeDir, backup_location, .path = instanceState->instance_backup_subdir_path,
			 .mode = DIR_PERMISSION, .strict = false);
	if ($haserr(err))
	{
		elog(WARNING, "%s", $errmsg(err));
	}
	err = $i(pioMakeDir, backup_location, .path = instanceState->instance_wal_subdir_path,
			 .mode = DIR_PERMISSION, .strict = false);
	if ($haserr(err))
	{
		elog(WARNING, "%s", $errmsg(err));
	}

	/*
	 * Write initial configuration file.
	 * system-identifier, xlog-seg-size and pgdata are set in init subcommand
	 * and will never be updated.
	 *
	 * We need to manually set options source to save them to the configuration
	 * file.
	 */
	config_set_opt(instance_options, &instance->system_identifier,
				   SOURCE_FILE);
	config_set_opt(instance_options, &instance->xlog_seg_size,
				   SOURCE_FILE);

	/* Kludge: do not save remote options into config */
	config_set_opt(instance_options, &instance_config.remote.host,
				   SOURCE_DEFAULT);
	config_set_opt(instance_options, &instance_config.remote.proto,
				   SOURCE_DEFAULT);
	config_set_opt(instance_options, &instance_config.remote.port,
				   SOURCE_DEFAULT);
	config_set_opt(instance_options, &instance_config.remote.path,
				   SOURCE_DEFAULT);
	config_set_opt(instance_options, &instance_config.remote.user,
				   SOURCE_DEFAULT);
	config_set_opt(instance_options, &instance_config.remote.ssh_options,
				   SOURCE_DEFAULT);
	config_set_opt(instance_options, &instance_config.remote.ssh_config,
				   SOURCE_DEFAULT);

	/* pgdata was set through command line */
	do_set_config(instanceState, true);

	elog(INFO, "Instance '%s' successfully inited", instanceState->instance_name);
	return 0;
}
