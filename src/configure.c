/*-------------------------------------------------------------------------
 *
 * configure.c: - manage backup catalog.
 *
 * Copyright (c) 2017-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <unistd.h>

#include "utils/configuration.h"
#include "utils/json.h"


static void assign_log_level_console(ConfigOption *opt, const char *arg);
static void assign_log_level_file(ConfigOption *opt, const char *arg);
static void assign_compress_alg(ConfigOption *opt, const char *arg);

static char *get_log_level_console(ConfigOption *opt);
static char *get_log_level_file(ConfigOption *opt);
static char *get_compress_alg(ConfigOption *opt);

static void show_configure_start(void);
static void show_configure_end(void);

static void show_configure_plain(ConfigOption *opt);
static void show_configure_json(ConfigOption *opt);

#define RETENTION_REDUNDANCY_DEFAULT	0
#define RETENTION_WINDOW_DEFAULT		0

#define OPTION_INSTANCE_GROUP	"Backup instance information"
#define OPTION_CONN_GROUP		"Connection parameters"
#define OPTION_REPLICA_GROUP	"Replica parameters"
#define OPTION_ARCHIVE_GROUP	"Archive parameters"
#define OPTION_LOG_GROUP		"Logging parameters"
#define OPTION_RETENTION_GROUP	"Retention parameters"
#define OPTION_COMPRESS_GROUP	"Compression parameters"
#define OPTION_REMOTE_GROUP		"Remote access parameters"

/*
 * Short name should be non-printable ASCII character.
 */
ConfigOption instance_options[] =
{
	/* Instance options */
	{
		's', 'D', "pgdata",
		&instance_config.pgdata, SOURCE_CMD, 0,
		OPTION_INSTANCE_GROUP, 0, option_get_value
	},
	{
		'U', 200, "system-identifier",
		&instance_config.system_identifier, SOURCE_FILE_STRICT, 0,
		OPTION_INSTANCE_GROUP, 0, option_get_value
	},
#if PG_VERSION_NUM >= 110000
	{
		'u', 201, "xlog-seg-size",
		&instance_config.xlog_seg_size, SOURCE_FILE_STRICT, 0,
		OPTION_INSTANCE_GROUP, 0, option_get_value
	},
#endif
	{
		's', 'E', "external-dirs",
		&instance_config.external_dir_str, SOURCE_CMD, 0,
		OPTION_INSTANCE_GROUP, 0, option_get_value
	},
	/* Connection options */
	{
		's', 'd', "pgdatabase",
		&instance_config.conn_opt.pgdatabase, SOURCE_CMD, 0,
		OPTION_CONN_GROUP, 0, option_get_value
	},
	{
		's', 'h', "pghost",
		&instance_config.conn_opt.pghost, SOURCE_CMD, 0,
		OPTION_CONN_GROUP, 0, option_get_value
	},
	{
		's', 'p', "pgport",
		&instance_config.conn_opt.pgport, SOURCE_CMD, 0,
		OPTION_CONN_GROUP, 0, option_get_value
	},
	{
		's', 'U', "pguser",
		&instance_config.conn_opt.pguser, SOURCE_CMD, 0,
		OPTION_CONN_GROUP, 0, option_get_value
	},
	/* Replica options */
	{
		's', 202, "master-db",
		&instance_config.master_conn_opt.pgdatabase, SOURCE_CMD, 0,
		OPTION_REPLICA_GROUP, 0, option_get_value
	},
	{
		's', 203, "master-host",
		&instance_config.master_conn_opt.pghost, SOURCE_CMD, 0,
		OPTION_REPLICA_GROUP, 0, option_get_value
	},
	{
		's', 204, "master-port",
		&instance_config.master_conn_opt.pgport, SOURCE_CMD, 0,
		OPTION_REPLICA_GROUP, 0, option_get_value
	},
	{
		's', 205, "master-user",
		&instance_config.master_conn_opt.pguser, SOURCE_CMD, 0,
		OPTION_REPLICA_GROUP, 0, option_get_value
	},
	{
		'u', 206, "replica-timeout",
		&instance_config.replica_timeout, SOURCE_CMD, SOURCE_DEFAULT,
		OPTION_REPLICA_GROUP, OPTION_UNIT_S, option_get_value
	},
	/* Archive options */
	{
		'u', 207, "archive-timeout",
		&instance_config.archive_timeout, SOURCE_CMD, SOURCE_DEFAULT,
		OPTION_ARCHIVE_GROUP, OPTION_UNIT_S, option_get_value
	},
	/* Logging options */
	{
		'f', 208, "log-level-console",
		assign_log_level_console, SOURCE_CMD, 0,
		OPTION_LOG_GROUP, 0, get_log_level_console
	},
	{
		'f', 209, "log-level-file",
		assign_log_level_file, SOURCE_CMD, 0,
		OPTION_LOG_GROUP, 0, get_log_level_file
	},
	{
		's', 210, "log-filename",
		&instance_config.logger.log_filename, SOURCE_CMD, 0,
		OPTION_LOG_GROUP, 0, option_get_value
	},
	{
		's', 211, "error-log-filename",
		&instance_config.logger.error_log_filename, SOURCE_CMD, 0,
		OPTION_LOG_GROUP, 0, option_get_value
	},
	{
		's', 212, "log-directory",
		&instance_config.logger.log_directory, SOURCE_CMD, 0,
		OPTION_LOG_GROUP, 0, option_get_value
	},
	{
		'U', 213, "log-rotation-size",
		&instance_config.logger.log_rotation_size, SOURCE_CMD, SOURCE_DEFAULT,
		OPTION_LOG_GROUP, OPTION_UNIT_KB, option_get_value
	},
	{
		'U', 214, "log-rotation-age",
		&instance_config.logger.log_rotation_age, SOURCE_CMD, SOURCE_DEFAULT,
		OPTION_LOG_GROUP, OPTION_UNIT_MS, option_get_value
	},
	/* Retention options */
	{
		'u', 215, "retention-redundancy",
		&instance_config.retention_redundancy, SOURCE_CMD, 0,
		OPTION_RETENTION_GROUP, 0, option_get_value
	},
	{
		'u', 216, "retention-window",
		&instance_config.retention_window, SOURCE_CMD, 0,
		OPTION_RETENTION_GROUP, 0, option_get_value
	},
	/* Compression options */
	{
		'f', 217, "compress-algorithm",
		assign_compress_alg, SOURCE_CMD, 0,
		OPTION_COMPRESS_GROUP, 0, get_compress_alg
	},
	{
		'u', 218, "compress-level",
		&instance_config.compress_level, SOURCE_CMD, 0,
		OPTION_COMPRESS_GROUP, 0, option_get_value
	},
	/* Remote backup options */
	{
		's', 219, "remote-proto",
		&instance_config.remote.proto, SOURCE_CMD, 0,
		OPTION_REMOTE_GROUP, 0, option_get_value
	},
	{
		's', 220, "remote-host",
		&instance_config.remote.host, SOURCE_CMD, 0,
		OPTION_REMOTE_GROUP, 0, option_get_value
	},
	{
		's', 221, "remote-port",
		&instance_config.remote.port, SOURCE_CMD, 0,
		OPTION_REMOTE_GROUP, 0, option_get_value
	},
	{
		's', 222, "remote-path",
		&instance_config.remote.path, SOURCE_CMD, 0,
		OPTION_REMOTE_GROUP, 0, option_get_value
	},
	{
		's', 223, "remote-user",
		&instance_config.remote.user, SOURCE_CMD, 0,
		OPTION_REMOTE_GROUP, 0, option_get_value
	},
	{
		's', 224, "ssh-options",
		&instance_config.remote.ssh_options, SOURCE_CMD, 0,
		OPTION_REMOTE_GROUP, 0, option_get_value
	},
	{
		's', 225, "ssh-config",
		&instance_config.remote.ssh_config, SOURCE_CMD, 0,
		OPTION_REMOTE_GROUP, 0, option_get_value
	},
	{ 0 }
};

/* An instance configuration with default options */
InstanceConfig instance_config;

static PQExpBufferData show_buf;
static int32 json_level = 0;
static const char *current_group = NULL;

/*
 * Show configure options including default values.
 */
void
do_show_config(void)
{
	int			i;

	show_configure_start();

	for (i = 0; instance_options[i].type; i++)
	{
		if (show_format == SHOW_PLAIN)
			show_configure_plain(&instance_options[i]);
		else
			show_configure_json(&instance_options[i]);
	}

	show_configure_end();
}

/*
 * Save configure options into BACKUP_CATALOG_CONF_FILE. Do not save default
 * values into the file.
 */
void
do_set_config(bool missing_ok)
{
	char		path[MAXPGPATH];
	char		path_temp[MAXPGPATH];
	FILE	   *fp;
	int			i;

	join_path_components(path, backup_instance_path, BACKUP_CATALOG_CONF_FILE);
	snprintf(path_temp, sizeof(path_temp), "%s.tmp", path);

	if (!missing_ok && !fileExists(path, FIO_LOCAL_HOST))
		elog(ERROR, "Configuration file \"%s\" doesn't exist", path);

	fp = fopen(path_temp, "wt");
	if (fp == NULL)
		elog(ERROR, "Cannot create configuration file \"%s\": %s",
			 BACKUP_CATALOG_CONF_FILE, strerror(errno));

	current_group = NULL;

	for (i = 0; instance_options[i].type; i++)
	{
		ConfigOption *opt = &instance_options[i];
		char	   *value;

		/* Save only options from command line */
		if (opt->source != SOURCE_CMD &&
			/* ...or options from the previous configure file */
			opt->source != SOURCE_FILE && opt->source != SOURCE_FILE_STRICT)
			continue;

		value = opt->get_value(opt);
		if (value == NULL)
			continue;

		if (current_group == NULL || strcmp(opt->group, current_group) != 0)
		{
			current_group = opt->group;
			fprintf(fp, "# %s\n", current_group);
		}

		if (strchr(value, ' '))
			fprintf(fp, "%s = '%s'\n", opt->lname, value);
		else
			fprintf(fp, "%s = %s\n", opt->lname, value);
		pfree(value);
	}

	fclose(fp);

	if (rename(path_temp, path) < 0)
	{
		int			errno_temp = errno;
		unlink(path_temp);
		elog(ERROR, "Cannot rename configuration file \"%s\" to \"%s\": %s",
			 path_temp, path, strerror(errno_temp));
	}
}

void
init_config(InstanceConfig *config)
{
	MemSet(config, 0, sizeof(InstanceConfig));

	/*
	 * Starting from PostgreSQL 11 WAL segment size may vary. Prior to
	 * PostgreSQL 10 xlog_seg_size is equal to XLOG_SEG_SIZE.
	 */
#if PG_VERSION_NUM >= 110000
	config->xlog_seg_size = 0;
#else
	config->xlog_seg_size = XLOG_SEG_SIZE;
#endif

	config->replica_timeout = REPLICA_TIMEOUT_DEFAULT;

	config->archive_timeout = ARCHIVE_TIMEOUT_DEFAULT;

	/* Copy logger defaults */
	config->logger = logger_config;

	config->retention_redundancy = RETENTION_REDUNDANCY_DEFAULT;
	config->retention_window = RETENTION_WINDOW_DEFAULT;

	config->compress_alg = COMPRESS_ALG_DEFAULT;
	config->compress_level = COMPRESS_LEVEL_DEFAULT;

	config->remote.proto = (char*)"ssh";
}

static void
assign_log_level_console(ConfigOption *opt, const char *arg)
{
	instance_config.logger.log_level_console = parse_log_level(arg);
}

static void
assign_log_level_file(ConfigOption *opt, const char *arg)
{
	instance_config.logger.log_level_file = parse_log_level(arg);
}

static void
assign_compress_alg(ConfigOption *opt, const char *arg)
{
	instance_config.compress_alg = parse_compress_alg(arg);
}

static char *
get_log_level_console(ConfigOption *opt)
{
	return pstrdup(deparse_log_level(instance_config.logger.log_level_console));
}

static char *
get_log_level_file(ConfigOption *opt)
{
	return pstrdup(deparse_log_level(instance_config.logger.log_level_file));
}

static char *
get_compress_alg(ConfigOption *opt)
{
	return pstrdup(deparse_compress_alg(instance_config.compress_alg));
}

/*
 * Initialize configure visualization.
 */
static void
show_configure_start(void)
{
	initPQExpBuffer(&show_buf);

	if (show_format == SHOW_PLAIN)
		current_group = NULL;
	else
	{
		json_level = 0;
		json_add(&show_buf, JT_BEGIN_OBJECT, &json_level);
	}
}

/*
 * Finalize configure visualization.
 */
static void
show_configure_end(void)
{
	if (show_format == SHOW_PLAIN)
		current_group = NULL;
	else
	{
		json_add(&show_buf, JT_END_OBJECT, &json_level);
		appendPQExpBufferChar(&show_buf, '\n');
	}

	fputs(show_buf.data, stdout);
	termPQExpBuffer(&show_buf);
}

/*
 * Plain output.
 */

static void
show_configure_plain(ConfigOption *opt)
{
	char	   *value;

	value = opt->get_value(opt);
	if (value == NULL)
		return;

	if (current_group == NULL || strcmp(opt->group, current_group) != 0)
	{
		current_group = opt->group;
		appendPQExpBuffer(&show_buf, "# %s\n", current_group);
	}

	appendPQExpBuffer(&show_buf, "%s = %s\n", opt->lname, value);
	pfree(value);
}

/*
 * Json output.
 */

static void
show_configure_json(ConfigOption *opt)
{
	char	   *value;

	value = opt->get_value(opt);
	if (value == NULL)
		return;

	json_add_value(&show_buf, opt->lname, value, json_level,
				   opt->type == 's' || opt->flags & OPTION_UNIT);
	pfree(value);
}
