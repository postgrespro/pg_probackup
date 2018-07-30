/*-------------------------------------------------------------------------
 *
 * configure.c: - manage backup catalog.
 *
 * Copyright (c) 2017-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include "pqexpbuffer.h"

#include "utils/json.h"


static void opt_log_level_console(pgut_option *opt, const char *arg);
static void opt_log_level_file(pgut_option *opt, const char *arg);
static void opt_compress_alg(pgut_option *opt, const char *arg);

static void show_configure_start(void);
static void show_configure_end(void);
static void show_configure(pgBackupConfig *config);

static void show_configure_json(pgBackupConfig *config);

static pgBackupConfig *cur_config = NULL;

static PQExpBufferData show_buf;
static int32 json_level = 0;

/*
 * All this code needs refactoring.
 */

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

	if (master_host)
		config->master_host = master_host;
	if (master_port)
		config->master_port = master_port;
	if (master_db)
		config->master_db = master_db;
	if (master_user)
		config->master_user = master_user;
	if (replica_timeout != 300)		/* 300 is default value */
		config->replica_timeout = replica_timeout;

	if (log_level_console != LOG_NONE)
		config->log_level_console = LOG_LEVEL_CONSOLE;
	if (log_level_file != LOG_NONE)
		config->log_level_file = LOG_LEVEL_FILE;
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

	if (compress_alg != NOT_DEFINED_COMPRESS)
		config->compress_alg = compress_alg;
	if (compress_level != DEFAULT_COMPRESS_LEVEL)
		config->compress_level = compress_level;

	if (show_only)
		show_configure(config);
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

	config->master_host = NULL;
	config->master_port = NULL;
	config->master_db = NULL;
	config->master_user = NULL;
	config->replica_timeout = INT_MIN;	/* INT_MIN means "undefined" */

	config->log_level_console = INT_MIN;	/* INT_MIN means "undefined" */
	config->log_level_file = INT_MIN;		/* INT_MIN means "undefined" */
	config->log_filename = NULL;
	config->error_log_filename = NULL;
	config->log_directory = NULL;
	config->log_rotation_size = 0;
	config->log_rotation_age = 0;

	config->retention_redundancy = 0;
	config->retention_window = 0;

	config->compress_alg = NOT_DEFINED_COMPRESS;
	config->compress_level = DEFAULT_COMPRESS_LEVEL;
}

void
writeBackupCatalogConfig(FILE *out, pgBackupConfig *config)
{
	uint64		res;
	const char *unit;

	fprintf(out, "#Backup instance info\n");
	fprintf(out, "PGDATA = %s\n", config->pgdata);
	fprintf(out, "system-identifier = " UINT64_FORMAT "\n", config->system_identifier);

	fprintf(out, "#Connection parameters:\n");
	if (config->pgdatabase)
		fprintf(out, "PGDATABASE = %s\n", config->pgdatabase);
	if (config->pghost)
		fprintf(out, "PGHOST = %s\n", config->pghost);
	if (config->pgport)
		fprintf(out, "PGPORT = %s\n", config->pgport);
	if (config->pguser)
		fprintf(out, "PGUSER = %s\n", config->pguser);

	fprintf(out, "#Replica parameters:\n");
	if (config->master_host)
		fprintf(out, "master-host = %s\n", config->master_host);
	if (config->master_port)
		fprintf(out, "master-port = %s\n", config->master_port);
	if (config->master_db)
		fprintf(out, "master-db = %s\n", config->master_db);
	if (config->master_user)
		fprintf(out, "master-user = %s\n", config->master_user);

	if (config->replica_timeout != INT_MIN)
	{
		convert_from_base_unit_u(config->replica_timeout, OPTION_UNIT_S,
								 &res, &unit);
		fprintf(out, "replica-timeout = " UINT64_FORMAT "%s\n", res, unit);
	}

	fprintf(out, "#Logging parameters:\n");
	if (config->log_level_console != INT_MIN)
		fprintf(out, "log-level-console = %s\n", deparse_log_level(config->log_level_console));
	if (config->log_level_file != INT_MIN)
		fprintf(out, "log-level-file = %s\n", deparse_log_level(config->log_level_file));
	if (config->log_filename)
		fprintf(out, "log-filename = %s\n", config->log_filename);
	if (config->error_log_filename)
		fprintf(out, "error-log-filename = %s\n", config->error_log_filename);
	if (config->log_directory)
		fprintf(out, "log-directory = %s\n", config->log_directory);

	/*
	 * Convert values from base unit
	 */
	if (config->log_rotation_size)
	{
		convert_from_base_unit_u(config->log_rotation_size, OPTION_UNIT_KB,
								 &res, &unit);
		fprintf(out, "log-rotation-size = " UINT64_FORMAT "%s\n", res, unit);
	}
	if (config->log_rotation_age)
	{
		convert_from_base_unit_u(config->log_rotation_age, OPTION_UNIT_S,
								 &res, &unit);
		fprintf(out, "log-rotation-age = " UINT64_FORMAT "%s\n", res, unit);
	}

	fprintf(out, "#Retention parameters:\n");
	if (config->retention_redundancy)
		fprintf(out, "retention-redundancy = %u\n", config->retention_redundancy);
	if (config->retention_window)
		fprintf(out, "retention-window = %u\n", config->retention_window);

	fprintf(out, "#Compression parameters:\n");

	fprintf(out, "compress-algorithm = %s\n", deparse_compress_alg(config->compress_alg));

	if (compress_level != config->compress_level)
		fprintf(out, "compress-level = %d\n", compress_level);
	else
		fprintf(out, "compress-level = %d\n", config->compress_level);
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
		/* compression options */
		{ 'f', 0, "compress-algorithm",		opt_compress_alg,				SOURCE_CMDLINE },
		{ 'u', 0, "compress-level",			&(config->compress_level),		SOURCE_CMDLINE },
		/* logging options */
		{ 'f', 0, "log-level-console",		opt_log_level_console,			SOURCE_CMDLINE },
		{ 'f', 0, "log-level-file",			opt_log_level_file,				SOURCE_CMDLINE },
		{ 's', 0, "log-filename",			&(config->log_filename),		SOURCE_CMDLINE },
		{ 's', 0, "error-log-filename",		&(config->error_log_filename),	SOURCE_CMDLINE },
		{ 's', 0, "log-directory",			&(config->log_directory),		SOURCE_CMDLINE },
		{ 'u', 0, "log-rotation-size",		&(config->log_rotation_size),	SOURCE_CMDLINE,	SOURCE_DEFAULT,	OPTION_UNIT_KB },
		{ 'u', 0, "log-rotation-age",		&(config->log_rotation_age),	SOURCE_CMDLINE,	SOURCE_DEFAULT,	OPTION_UNIT_S },
		/* connection options */
		{ 's', 0, "pgdata",					&(config->pgdata),				SOURCE_FILE_STRICT },
		{ 's', 0, "pgdatabase",				&(config->pgdatabase),			SOURCE_FILE_STRICT },
		{ 's', 0, "pghost",					&(config->pghost),				SOURCE_FILE_STRICT },
		{ 's', 0, "pgport",					&(config->pgport),				SOURCE_FILE_STRICT },
		{ 's', 0, "pguser",					&(config->pguser),				SOURCE_FILE_STRICT },
		/* replica options */
		{ 's', 0, "master-host",			&(config->master_host),			SOURCE_FILE_STRICT },
		{ 's', 0, "master-port",			&(config->master_port),			SOURCE_FILE_STRICT },
		{ 's', 0, "master-db",				&(config->master_db),			SOURCE_FILE_STRICT },
		{ 's', 0, "master-user",			&(config->master_user),			SOURCE_FILE_STRICT },
		{ 'u', 0, "replica-timeout",		&(config->replica_timeout),		SOURCE_CMDLINE,	SOURCE_DEFAULT,	OPTION_UNIT_S },
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
opt_log_level_console(pgut_option *opt, const char *arg)
{
	cur_config->log_level_console = parse_log_level(arg);
}

static void
opt_log_level_file(pgut_option *opt, const char *arg)
{
	cur_config->log_level_file = parse_log_level(arg);
}

static void
opt_compress_alg(pgut_option *opt, const char *arg)
{
	cur_config->compress_alg = parse_compress_alg(arg);
}

/*
 * Initialize configure visualization.
 */
static void
show_configure_start(void)
{
	if (show_format == SHOW_PLAIN)
		return;

	/* For now we need buffer only for JSON format */
	json_level = 0;
	initPQExpBuffer(&show_buf);
}

/*
 * Finalize configure visualization.
 */
static void
show_configure_end(void)
{
	if (show_format == SHOW_PLAIN)
		return;
	else
		appendPQExpBufferChar(&show_buf, '\n');

	fputs(show_buf.data, stdout);
	termPQExpBuffer(&show_buf);
}

/*
 * Show configure information of pg_probackup.
 */
static void
show_configure(pgBackupConfig *config)
{
	show_configure_start();

	if (show_format == SHOW_PLAIN)
		writeBackupCatalogConfig(stdout, config);
	else
		show_configure_json(config);

	show_configure_end();
}

/*
 * Json output.
 */

static void
show_configure_json(pgBackupConfig *config)
{
	PQExpBuffer	buf = &show_buf;

	json_add(buf, JT_BEGIN_OBJECT, &json_level);

	json_add_value(buf, "pgdata", config->pgdata, json_level, false);

	json_add_key(buf, "system-identifier", json_level, true);
	appendPQExpBuffer(buf, UINT64_FORMAT, config->system_identifier);

	/* Connection parameters */
	if (config->pgdatabase)
		json_add_value(buf, "pgdatabase", config->pgdatabase, json_level, true);
	if (config->pghost)
		json_add_value(buf, "pghost", config->pghost, json_level, true);
	if (config->pgport)
		json_add_value(buf, "pgport", config->pgport, json_level, true);
	if (config->pguser)
		json_add_value(buf, "pguser", config->pguser, json_level, true);

	/* Replica parameters */
	if (config->master_host)
		json_add_value(buf, "master-host", config->master_host, json_level,
					   true);
	if (config->master_port)
		json_add_value(buf, "master-port", config->master_port, json_level,
					   true);
	if (config->master_db)
		json_add_value(buf, "master-db", config->master_db, json_level, true);
	if (config->master_user)
		json_add_value(buf, "master-user", config->master_user, json_level,
					   true);

	if (config->replica_timeout != INT_MIN)
	{
		json_add_key(buf, "replica-timeout", json_level, true);
		appendPQExpBuffer(buf, "%d", config->replica_timeout);
	}

	/* Logging parameters */
	if (config->log_level_console != INT_MIN)
		json_add_value(buf, "log-level-console",
					   deparse_log_level(config->log_level_console), json_level,
					   true);
	if (config->log_level_file != INT_MIN)
		json_add_value(buf, "log-level-file",
					   deparse_log_level(config->log_level_file), json_level,
					   true);
	if (config->log_filename)
		json_add_value(buf, "log-filename", config->log_filename, json_level,
					   true);
	if (config->error_log_filename)
		json_add_value(buf, "error-log-filename", config->error_log_filename,
					   json_level, true);
	if (config->log_directory)
		json_add_value(buf, "log-directory", config->log_directory, json_level,
					   true);

	if (config->log_rotation_size)
	{
		json_add_key(buf, "log-rotation-size", json_level, true);
		appendPQExpBuffer(buf, "%d", config->log_rotation_size);
	}
	if (config->log_rotation_age)
	{
		json_add_key(buf, "log-rotation-age", json_level, true);
		appendPQExpBuffer(buf, "%d", config->log_rotation_age);
	}

	/* Retention parameters */
	if (config->retention_redundancy)
	{
		json_add_key(buf, "retention-redundancy", json_level, true);
		appendPQExpBuffer(buf, "%u", config->retention_redundancy);
	}
	if (config->retention_window)
	{
		json_add_key(buf, "retention-window", json_level, true);
		appendPQExpBuffer(buf, "%u", config->retention_window);
	}

	/* Compression parameters */
	json_add_value(buf, "compress-algorithm",
				   deparse_compress_alg(config->compress_alg), json_level,
				   true);

	json_add_key(buf, "compress-level", json_level, true);
	appendPQExpBuffer(buf, "%d", config->compress_level);

	json_add(buf, JT_END_OBJECT, &json_level);
}
