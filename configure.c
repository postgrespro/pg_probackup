/*-------------------------------------------------------------------------
 *
 * configure.c: - manage backup catalog.
 *
 * Copyright (c) 2017-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

static void opt_log_level(pgut_option *opt, const char *arg);

static pgBackupConfig *cur_config = NULL;

/* Set configure options */
int
do_configure(bool show_only)
{
	pgBackupConfig *config = readBackupCatalogConfigFile();
	if (pgdata)
		config->pgdata = pgdata;
	if (pgut_dbname)
		config->pgdatabase = pgut_dbname;
	if (host)
		config->pghost = host;
	if (port)
		config->pgport = port;
	if (username)
		config->pguser = username;

	if (log_level_defined)
		config->log_level = log_level;
	if (log_filename)
		config->log_filename = log_filename;
	if (error_log_filename)
		config->error_log_filename = error_log_filename;
	if (log_directory)
		config->log_directory = log_directory;
	if (log_rotation_size)
		config->log_rotation_size = log_rotation_size;
	if (log_rotation_age)
		config->log_rotation_age = log_rotation_age;

	if (retention_redundancy)
		config->retention_redundancy = retention_redundancy;
	if (retention_window)
		config->retention_window = retention_window;

	if (show_only)
		writeBackupCatalogConfig(stderr, config);
	else
		writeBackupCatalogConfigFile(config);

	return 0;
}

void
pgBackupConfigInit(pgBackupConfig *config)
{
	config->system_identifier = 0;
	config->pgdata = NULL;
	config->pgdatabase = NULL;
	config->pghost = NULL;
	config->pgport = NULL;
	config->pguser = NULL;

	config->log_level = INT_MIN;	// INT_MIN means "undefined"
	config->log_filename = NULL;
	config->error_log_filename = NULL;
	config->log_directory = NULL;
	config->log_rotation_size = 0;
	config->log_rotation_age = 0;

	config->retention_redundancy = 0;
	config->retention_window = 0;
}

void
writeBackupCatalogConfig(FILE *out, pgBackupConfig *config)
{
	fprintf(out, "#Backup instance info\n");
	fprintf(out, "PGDATA = %s\n", config->pgdata);
	fprintf(out, "system-identifier = %li\n", config->system_identifier);

	fprintf(out, "#Connection parameters:\n");
	if (config->pgdatabase)
		fprintf(out, "PGDATABASE = %s\n", config->pgdatabase);
	if (config->pghost)
		fprintf(out, "PGHOST = %s\n", config->pghost);
	if (config->pgport)
		fprintf(out, "PGPORT = %s\n", config->pgport);
	if (config->pguser)
		fprintf(out, "PGUSER = %s\n", config->pguser);

	fprintf(out, "#Logging parameters:\n");
	if (config->log_level != INT_MIN)
		fprintf(out, "log-level = %s\n", deparse_log_level(config->log_level));
	if (config->log_filename)
		fprintf(out, "log-filename = %s\n", config->log_filename);
	if (config->error_log_filename)
		fprintf(out, "error-log-filename = %s\n", config->error_log_filename);
	if (config->log_directory)
		fprintf(out, "log-directory = %s\n", config->log_directory);
	if (config->log_rotation_size)
		fprintf(out, "log-rotation-size = %d\n", config->log_rotation_size);
	if (config->log_rotation_age)
		fprintf(out, "log-rotation-age = %d\n", config->log_rotation_age);

	fprintf(out, "#Retention parameters:\n");
	if (config->retention_redundancy)
		fprintf(out, "retention-redundancy = %u\n", config->retention_redundancy);
	if (config->retention_window)
		fprintf(out, "retention-window = %u\n", config->retention_window);

}

void
writeBackupCatalogConfigFile(pgBackupConfig *config)
{
	char		path[MAXPGPATH];
	FILE	   *fp;

	join_path_components(path, backup_instance_path, BACKUP_CATALOG_CONF_FILE);
	fp = fopen(path, "wt");
	if (fp == NULL)
		elog(ERROR, "cannot create %s: %s",
			 BACKUP_CATALOG_CONF_FILE, strerror(errno));

	writeBackupCatalogConfig(fp, config);
	fclose(fp);
}


pgBackupConfig*
readBackupCatalogConfigFile(void)
{
	pgBackupConfig *config = pgut_new(pgBackupConfig);
	char		path[MAXPGPATH];

	pgut_option options[] =
	{
		/* retention options */
		{ 'u', 0, "retention-redundancy",	&(config->retention_redundancy),SOURCE_FILE_STRICT },
		{ 'u', 0, "retention-window",		&(config->retention_window),	SOURCE_FILE_STRICT },
		/* logging options */
		{ 'f', 40, "log-level",				opt_log_level,					SOURCE_CMDLINE },
		{ 's', 41, "log-filename",			&(config->log_filename),		SOURCE_CMDLINE },
		{ 's', 42, "error-log-filename",	&(config->error_log_filename),	SOURCE_CMDLINE },
		{ 's', 43, "log-directory",			&(config->log_directory),		SOURCE_CMDLINE },
		{ 'u', 44, "log-rotation-size",		&(config->log_rotation_size),	SOURCE_CMDLINE },
		{ 'u', 45, "log-rotation-age",		&(config->log_rotation_age),	SOURCE_CMDLINE },
		/* connection options */
		{ 's', 0, "pgdata",					&(config->pgdata),				SOURCE_FILE_STRICT },
		{ 's', 0, "pgdatabase",				&(config->pgdatabase),			SOURCE_FILE_STRICT },
		{ 's', 0, "pghost",					&(config->pghost),				SOURCE_FILE_STRICT },
		{ 's', 0, "pgport",					&(config->pgport),				SOURCE_FILE_STRICT },
		{ 's', 0, "pguser",					&(config->pguser),				SOURCE_FILE_STRICT },
		/* other options */
		{ 'U', 0, "system-identifier",		&(config->system_identifier),	SOURCE_FILE_STRICT },
		{0}
	};

	cur_config = config;

	join_path_components(path, backup_instance_path, BACKUP_CATALOG_CONF_FILE);

	pgBackupConfigInit(config);
	pgut_readopt(path, options, ERROR);

	return config;

}

static void
opt_log_level(pgut_option *opt, const char *arg)
{
	cur_config->log_level = parse_log_level(arg);
}
