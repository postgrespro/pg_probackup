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
	bool		empty;
	err_i		err;

	empty = $i(pioIsDirEmpty, backup_location,.path = catalogState->catalog_path,
			   .err = &err);

	if ($haserr(err))
		ft_logerr(FT_FATAL, $errmsg(err), "cannot open backup catalog directory");
	if (!empty)
		elog(ERROR, "backup catalog already exist and it's not empty");

	/* create backup catalog root directory */
	err = $i(pioMakeDir, backup_location, .path = catalogState->catalog_path,
			 .mode = DIR_PERMISSION, .strict = false);
	if ($haserr(err))
	{
		elog(ERROR, "Can not create backup catalog root directory: %s",
			 $errmsg(err));
	}

	/* create backup catalog data directory */
	err = $i(pioMakeDir, backup_location, .path = catalogState->backup_subdir_path,
			 .mode = DIR_PERMISSION, .strict = false);
	if ($haserr(err))
	{
		elog(ERROR, "Can not create backup catalog data directory: %s",
			 $errmsg(err));
	}

	/* create backup catalog wal directory */
	err = $i(pioMakeDir, backup_location, .path = catalogState->wal_subdir_path,
			 .mode = DIR_PERMISSION, .strict = false);
	if ($haserr(err))
	{
		elog(ERROR, "Can not create backup catalog WAL directory: %s",
			 $errmsg(err));
	}

	elog(INFO, "Backup catalog '%s' successfully inited", catalogState->catalog_path);
	return 0;
}

int
do_add_instance(InstanceState *instanceState, InstanceConfig *instance)
{
	pioDrive_i	backup_location = instanceState->backup_location;
	pioDrive_i	db_location = instanceState->database_location;
	CatalogState *catalogState = instanceState->catalog_state;
	err_i		err;
	bool		exists;
	int i;

	/* PGDATA is always required */
	if (instance->pgdata == NULL)
		elog(ERROR, "Required parameter not specified: PGDATA "
						 "(-D, --pgdata)");

	/* Read system_identifier from PGDATA */
	instance->system_identifier = get_system_identifier(db_location, instance->pgdata, false);
	/* Starting from PostgreSQL 11 read WAL segment size from PGDATA */
	instance->xlog_seg_size = get_xlog_seg_size(db_location, instance->pgdata);

	/* Ensure that all root directories already exist */
	/* TODO maybe call do_init() here instead of error?*/
	{
		const char *paths[] = {
				catalogState->catalog_path,
				catalogState->backup_subdir_path,
				catalogState->wal_subdir_path};
		for (i = 0; i < ft_arrsz(paths); i++)
		{
			exists = $i(pioExists, backup_location, .path = paths[i],
							 .expected_kind = PIO_KIND_DIRECTORY, .err = &err);
			if ($haserr(err))
				ft_logerr(FT_FATAL, $errmsg(err), "Check instance");
			if (!exists)
				elog(ERROR, "Directory does not exist: '%s'", paths[i]);
		}
	}

	{
		const char *paths[][2] = {
				{"backup", instanceState->instance_backup_subdir_path},
				{"WAL", instanceState->instance_wal_subdir_path},
		};
		for (i = 0; i < ft_arrsz(paths); i++)
		{
			exists = !$i(pioIsDirEmpty, backup_location, .path = paths[i][1],
						.err = &err);
			if ($haserr(err))
				ft_logerr(FT_FATAL, $errmsg(err), "Check instance");
			if (exists)
				elog(ERROR, "Instance '%s' %s directory already exists: '%s'",
					 instanceState->instance_name, paths[i][0], paths[i][1]);
		}
	}

	/* Create directory for data files of this specific instance */
	err = $i(pioMakeDir, backup_location, .path = instanceState->instance_backup_subdir_path,
			 .mode = DIR_PERMISSION, .strict = false);
	if ($haserr(err))
	{
		elog(ERROR, "Can not create instance backup directory: %s",
			 $errmsg(err));
	}
	err = $i(pioMakeDir, backup_location, .path = instanceState->instance_wal_subdir_path,
			 .mode = DIR_PERMISSION, .strict = false);
	if ($haserr(err))
	{
		elog(ERROR, "Can not create instance WAL directory: %s", $errmsg(err));
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
	do_set_config(instanceState);

	elog(INFO, "Instance '%s' successfully inited", instanceState->instance_name);
	return 0;
}
