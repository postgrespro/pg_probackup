/*-------------------------------------------------------------------------
 *
 * configure.c: - manage backup catalog.
 *
 * Portions Copyright (c) 2017-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

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

	join_path_components(path, backup_path, BACKUPS_DIR);
	join_path_components(path, backup_path, BACKUP_CATALOG_CONF_FILE);
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
			/* configure options */
		{ 'U', 0, "system-identifier",		&(config->system_identifier),	SOURCE_FILE_STRICT },
		{ 's', 0, "pgdata",					&(config->pgdata),				SOURCE_FILE_STRICT },
		{ 's', 0, "pgdatabase",				&(config->pgdatabase),			SOURCE_FILE_STRICT },
		{ 's', 0, "pghost",					&(config->pghost),				SOURCE_FILE_STRICT },
		{ 's', 0, "pgport",					&(config->pgport),				SOURCE_FILE_STRICT },
		{ 's', 0, "pguser",					&(config->pguser),				SOURCE_FILE_STRICT },
		{ 'u', 0, "retention-redundancy",	&(config->retention_redundancy),SOURCE_FILE_STRICT },
		{ 'u', 0, "retention-window",		&(config->retention_window),	SOURCE_FILE_STRICT },
		{0}
	};

	join_path_components(path, backup_path, BACKUPS_DIR);
	join_path_components(path, backup_path, BACKUP_CATALOG_CONF_FILE);

	pgBackupConfigInit(config);
	pgut_readopt(path, options, ERROR);

	return config;

}
