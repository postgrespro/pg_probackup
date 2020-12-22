/*-------------------------------------------------------------------------
 *
 * logger.c: - log events into log file or stderr.
 *
 * Copyright (c) 2017-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <sys/stat.h>

#include "pg_probackup.h"
#include "logger.h"
#include "pgut.h"
#include "thread.h"
#include <time.h>

#include "utils/configuration.h"

/* Logger parameters */
LoggerConfig logger_config = {
	LOG_LEVEL_CONSOLE_DEFAULT,
	LOG_LEVEL_FILE_DEFAULT,
	LOG_FILENAME_DEFAULT,
	NULL,
	LOG_ROTATION_SIZE_DEFAULT,
	LOG_ROTATION_AGE_DEFAULT
};

/* Implementation for logging.h */

typedef enum
{
	PG_DEBUG,
	PG_PROGRESS,
	PG_WARNING,
	PG_FATAL
} eLogType;

void pg_log(eLogType type, const char *fmt,...) pg_attribute_printf(2, 3);

static void elog_internal(int elevel, bool file_only, const char *message);
static void elog_stderr(int elevel, const char *fmt, ...)
						pg_attribute_printf(2, 3);
static char *get_log_message(const char *fmt, va_list args) pg_attribute_printf(1, 0);

/* Functions to work with log files */
static void open_logfile(FILE **file, const char *filename_format);
static void release_logfile(bool fatal, void *userdata);
static char *logfile_getname(const char *format, time_t timestamp);
static FILE *logfile_open(const char *filename, const char *mode);

/* Static variables */

static FILE *log_file = NULL;
static FILE *error_log_file = NULL;

static bool exit_hook_registered = false;
/* Logging of the current thread is in progress */
static bool loggin_in_progress = false;

static pthread_mutex_t log_file_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * Initialize logger.
 *
 * If log_directory wasn't set by a user we use full path:
 * backup_directory/log
 */
void
init_logger(const char *root_path, LoggerConfig *config)
{
	/*
	 * If logging to file is enabled and log_directory wasn't set
	 * by user, init the path with default value: backup_directory/log/
	 * */
	if (config->log_level_file != LOG_OFF
		&& config->log_directory == NULL)
	{
		config->log_directory = pgut_malloc(MAXPGPATH);
		join_path_components(config->log_directory,
							 root_path, LOG_DIRECTORY_DEFAULT);
	}

	if (config->log_directory != NULL)
		canonicalize_path(config->log_directory);

	logger_config = *config;

#if PG_VERSION_NUM >= 120000
	/* Setup logging for functions from other modules called by pg_probackup */
	pg_logging_init(PROGRAM_NAME);
	errno = 0; /* sometimes pg_logging_init sets errno */

	switch (logger_config.log_level_console)
	{
		case VERBOSE:
			pg_logging_set_level(PG_LOG_DEBUG);
			break;
		case INFO:
		case NOTICE:
		case LOG:
			pg_logging_set_level(PG_LOG_INFO);
			break;
		case WARNING:
			pg_logging_set_level(PG_LOG_WARNING);
			break;
		case ERROR:
			pg_logging_set_level(PG_LOG_ERROR);
			break;
		default:
			break;
	};
#endif
}

static void
write_elevel(FILE *stream, int elevel)
{
	switch (elevel)
	{
		case VERBOSE:
			fputs("VERBOSE: ", stream);
			break;
		case LOG:
			fputs("LOG: ", stream);
			break;
		case INFO:
			fputs("INFO: ", stream);
			break;
		case NOTICE:
			fputs("NOTICE: ", stream);
			break;
		case WARNING:
			fputs("WARNING: ", stream);
			break;
		case ERROR:
			fputs("ERROR: ", stream);
			break;
		default:
			elog_stderr(ERROR, "invalid logging level: %d", elevel);
			break;
	}
}

/*
 * Exit with code if it is an error.
 * Check for in_cleanup flag to avoid deadlock in case of ERROR in cleanup
 * routines.
 */
static void
exit_if_necessary(int elevel)
{
	if (elevel > WARNING && !in_cleanup)
	{
		if (loggin_in_progress)
		{
			loggin_in_progress = false;
			pthread_mutex_unlock(&log_file_mutex);
		}

		if (remote_agent)
			sleep(1); /* Let parent receive sent messages */

		/* If this is not the main thread then don't call exit() */
		if (main_tid != pthread_self())
		{
			/* Interrupt other possible routines */
			thread_interrupted = true;
#ifdef WIN32
			ExitThread(elevel);
#else
			pthread_exit(NULL);
#endif
		}
		else
			exit(elevel);
	}
}

/*
 * Logs to stderr or to log file and exit if ERROR.
 *
 * Actual implementation for elog() and pg_log().
 */
static void
elog_internal(int elevel, bool file_only, const char *message)
{
	bool		write_to_file,
				write_to_error_log,
				write_to_stderr;
	time_t		log_time = (time_t) time(NULL);
	char		strfbuf[128];
	char		str_pid[128];

	write_to_file = elevel >= logger_config.log_level_file
		&& logger_config.log_directory
		&& logger_config.log_directory[0] != '\0';
	write_to_error_log = elevel >= ERROR && logger_config.error_log_filename &&
		logger_config.log_directory && logger_config.log_directory[0] != '\0';
	write_to_stderr = elevel >= logger_config.log_level_console && !file_only;

	if (remote_agent)
	{
		write_to_stderr |= write_to_error_log | write_to_file;
		write_to_error_log = write_to_file = false;
	}
	pthread_lock(&log_file_mutex);
	loggin_in_progress = true;

	if (write_to_file || write_to_error_log || is_archive_cmd)
		strftime(strfbuf, sizeof(strfbuf), "%Y-%m-%d %H:%M:%S %Z",
				 localtime(&log_time));

	snprintf(str_pid, sizeof(str_pid), "[%d]:", my_pid);

	/*
	 * Write message to log file.
	 * Do not write to file if this error was raised during write previous
	 * message.
	 */
	if (write_to_file)
	{
		if (log_file == NULL)
			open_logfile(&log_file, logger_config.log_filename ? logger_config.log_filename : LOG_FILENAME_DEFAULT);

		fprintf(log_file, "%s ", strfbuf);
		fprintf(log_file, "%s ", str_pid);
		write_elevel(log_file, elevel);

		fprintf(log_file, "%s\n", message);
		fflush(log_file);
	}

	/*
	 * Write error message to error log file.
	 * Do not write to file if this error was raised during write previous
	 * message.
	 */
	if (write_to_error_log)
	{
		if (error_log_file == NULL)
			open_logfile(&error_log_file, logger_config.error_log_filename);

		fprintf(error_log_file, "%s ", strfbuf);
		fprintf(error_log_file, "%s ", str_pid);
		write_elevel(error_log_file, elevel);

		fprintf(error_log_file, "%s\n", message);
		fflush(error_log_file);
	}

	/*
	 * Write to stderr if the message was not written to log file.
	 * Write to stderr if the message level is greater than WARNING anyway.
	 */
	if (write_to_stderr)
	{
		if (is_archive_cmd)
		{
			char		str_thread[64];
			/* [Issue #213] fix pgbadger parsing */
			snprintf(str_thread, sizeof(str_thread), "[%d-1]:", my_thread_num);

			fprintf(stderr, "%s ", strfbuf);
			fprintf(stderr, "%s ", str_pid);
			fprintf(stderr, "%s ", str_thread);
		}

		write_elevel(stderr, elevel);

		fprintf(stderr, "%s\n", message);
		fflush(stderr);
	}

	exit_if_necessary(elevel);

	loggin_in_progress = false;
	pthread_mutex_unlock(&log_file_mutex);
}

/*
 * Log only to stderr. It is called only within elog_internal() when another
 * logging already was started.
 */
static void
elog_stderr(int elevel, const char *fmt, ...)
{
	va_list		args;

	/*
	 * Do not log message if severity level is less than log_level.
	 * It is the little optimisation to put it here not in elog_internal().
	 */
	if (elevel < logger_config.log_level_console && elevel < ERROR)
		return;

	va_start(args, fmt);

	write_elevel(stderr, elevel);
	vfprintf(stderr, fmt, args);
	fputc('\n', stderr);
	fflush(stderr);

	va_end(args);

	exit_if_necessary(elevel);
}

/*
 * Formats text data under the control of fmt and returns it in an allocated
 * buffer.
 */
static char *
get_log_message(const char *fmt, va_list args)
{
	size_t		len = 256;		/* initial assumption about buffer size */

	for (;;)
	{
		char	   *result;
		size_t		newlen;
		va_list		copy_args;

		result = (char *) pgut_malloc(len);

		/* Try to format the data */
		va_copy(copy_args, args);
		newlen = pvsnprintf(result, len, fmt, copy_args);
		va_end(copy_args);

		if (newlen < len)
			return result;		/* success */

		/* Release buffer and loop around to try again with larger len. */
		pfree(result);
		len = newlen;
	}
}

/*
 * Logs to stderr or to log file and exit if ERROR.
 */
void
elog(int elevel, const char *fmt, ...)
{
	char	   *message;
	va_list		args;

	/*
	 * Do not log message if severity level is less than log_level.
	 * It is the little optimisation to put it here not in elog_internal().
	 */
	if (elevel < logger_config.log_level_console &&
		elevel < logger_config.log_level_file && elevel < ERROR)
		return;

	va_start(args, fmt);
	message = get_log_message(fmt, args);
	va_end(args);

	elog_internal(elevel, false, message);
	pfree(message);
}

/*
 * Logs only to log file and exit if ERROR.
 */
void
elog_file(int elevel, const char *fmt, ...)
{
	char	   *message;
	va_list		args;

	/*
	 * Do not log message if severity level is less than log_level.
	 * It is the little optimisation to put it here not in elog_internal().
	 */
	if (elevel < logger_config.log_level_file && elevel < ERROR)
		return;

	va_start(args, fmt);
	message = get_log_message(fmt, args);
	va_end(args);

	elog_internal(elevel, true, message);
	pfree(message);
}

/*
 * Implementation of pg_log() from logging.h.
 */
void
pg_log(eLogType type, const char *fmt, ...)
{
	char	   *message;
	va_list		args;
	int			elevel = INFO;

	/* Transform logging level from eLogType to utils/logger.h levels */
	switch (type)
	{
		case PG_DEBUG:
			elevel = LOG;
			break;
		case PG_PROGRESS:
			elevel = INFO;
			break;
		case PG_WARNING:
			elevel = WARNING;
			break;
		case PG_FATAL:
			elevel = ERROR;
			break;
		default:
			elog(ERROR, "invalid logging level: %d", type);
			break;
	}

	/*
	 * Do not log message if severity level is less than log_level.
	 * It is the little optimisation to put it here not in elog_internal().
	 */
	if (elevel < logger_config.log_level_console &&
		elevel < logger_config.log_level_file && elevel < ERROR)
		return;

	va_start(args, fmt);
	message = get_log_message(fmt, args);
	va_end(args);

	elog_internal(elevel, false, message);
	pfree(message);
}

/*
 * Parses string representation of log level.
 */
int
parse_log_level(const char *level)
{
	const char *v = level;
	size_t		len;

	/* Skip all spaces detected */
	while (isspace((unsigned char)*v))
		v++;
	len = strlen(v);

	if (len == 0)
		elog(ERROR, "log-level is empty");

	if (pg_strncasecmp("off", v, len) == 0)
		return LOG_OFF;
	else if (pg_strncasecmp("verbose", v, len) == 0)
		return VERBOSE;
	else if (pg_strncasecmp("log", v, len) == 0)
		return LOG;
	else if (pg_strncasecmp("info", v, len) == 0)
		return INFO;
	else if (pg_strncasecmp("notice", v, len) == 0)
		return NOTICE;
	else if (pg_strncasecmp("warning", v, len) == 0)
		return WARNING;
	else if (pg_strncasecmp("error", v, len) == 0)
		return ERROR;

	/* Log level is invalid */
	elog(ERROR, "invalid log-level \"%s\"", level);
	return 0;
}

/*
 * Converts integer representation of log level to string.
 */
const char *
deparse_log_level(int level)
{
	switch (level)
	{
		case LOG_OFF:
			return "OFF";
		case VERBOSE:
			return "VERBOSE";
		case LOG:
			return "LOG";
		case INFO:
			return "INFO";
		case NOTICE:
			return "NOTICE";
		case WARNING:
			return "WARNING";
		case ERROR:
			return "ERROR";
		default:
			elog(ERROR, "invalid log-level %d", level);
	}

	return NULL;
}

/*
 * Construct logfile name using timestamp information.
 *
 * Result is palloc'd.
 */
static char *
logfile_getname(const char *format, time_t timestamp)
{
	char	   *filename;
	size_t		len;
	struct tm  *tm = localtime(&timestamp);

	if (logger_config.log_directory == NULL ||
		logger_config.log_directory[0] == '\0')
		elog_stderr(ERROR, "logging path is not set");

	filename = (char *) pgut_malloc(MAXPGPATH);

	snprintf(filename, MAXPGPATH, "%s/", logger_config.log_directory);

	len = strlen(filename);

	/* Treat log_filename as a strftime pattern */
#ifdef WIN32
	if (pg_strftime(filename + len, MAXPGPATH - len, format, tm) <= 0)
#else
	if (strftime(filename + len, MAXPGPATH - len, format, tm) <= 0)
#endif
		elog_stderr(ERROR, "strftime(%s) failed: %s", format, strerror(errno));

	return filename;
}

/*
 * Open a new log file.
 */
static FILE *
logfile_open(const char *filename, const char *mode)
{
	FILE	   *fh;

	/*
	 * Create log directory if not present; ignore errors
	 */
	mkdir(logger_config.log_directory, S_IRWXU);

	fh = fopen(filename, mode);

	if (fh)
		setvbuf(fh, NULL, PG_IOLBF, 0);
	else
	{
		int			save_errno = errno;

		elog_stderr(ERROR, "could not open log file \"%s\": %s",
					filename, strerror(errno));
		errno = save_errno;
	}

	return fh;
}

/*
 * Open the log file.
 */
static void
open_logfile(FILE **file, const char *filename_format)
{
	char	   *filename;
	char		control[MAXPGPATH];
	struct stat	st;
	FILE	   *control_file;
	time_t		cur_time = time(NULL);
	bool		rotation_requested = false,
				logfile_exists = false,
				rotation_file_exists = false;

	filename = logfile_getname(filename_format, cur_time);

	/* "log_directory" was checked in logfile_getname() */
	snprintf(control, MAXPGPATH, "%s.rotation", filename);

	if (stat(filename, &st) == -1)
	{
		if (errno == ENOENT)
		{
			/* There is no file "filename" and rotation does not need */
			goto logfile_open;
		}
		else
			elog_stderr(ERROR, "cannot stat log file \"%s\": %s",
						filename, strerror(errno));
	}
	/* Found log file "filename" */
	logfile_exists = true;

	/* First check for rotation */
	if (logger_config.log_rotation_size > 0 ||
		logger_config.log_rotation_age > 0)
	{
		/* Check for rotation by age */
		if (logger_config.log_rotation_age > 0)
		{
			struct stat	control_st;

			if (stat(control, &control_st) < 0)
			{
				if (errno == ENOENT)
					/* '.rotation' file is not found, force  its recreation */
					elog_stderr(WARNING, "missing rotation file: \"%s\"",
								control);
				else
					elog_stderr(ERROR, "cannot stat rotation file \"%s\": %s",
								control, strerror(errno));
			}
			else
			{
				/* rotation file exists */
				char		buf[1024];

				control_file = fopen(control, "r");
				if (control_file == NULL)
					elog_stderr(ERROR, "cannot open rotation file \"%s\": %s",
								control, strerror(errno));

				rotation_file_exists = true;

				if (fgets(buf, lengthof(buf), control_file))
				{
					time_t		creation_time;

					if (!parse_int64(buf, (int64 *) &creation_time, 0))
					{
						/* Inability to parse value from .rotation file is
						 * concerning but not a critical error
						 */
						elog_stderr(WARNING, "rotation file \"%s\" has wrong "
									"creation timestamp \"%s\"",
									control, buf);
						rotation_file_exists = false;
					}
					else
						/* Parsed creation time */
						rotation_requested = (cur_time - creation_time) >
							/* convert to seconds from milliseconds */
							logger_config.log_rotation_age / 1000;
				}
				else
				{
					/* truncated .rotation file is not a critical error */
					elog_stderr(WARNING, "cannot read creation timestamp from "
								"rotation file \"%s\"", control);
					rotation_file_exists = false;
				}

				fclose(control_file);
			}
		}

		/* Check for rotation by size */
		if (!rotation_requested && logger_config.log_rotation_size > 0)
			rotation_requested = st.st_size >=
				/* convert to bytes */
				logger_config.log_rotation_size * 1024L;
	}

logfile_open:
	if (rotation_requested)
		*file = logfile_open(filename, "w");
	else
		*file = logfile_open(filename, "a");
	pfree(filename);

	/* Rewrite rotation control file */
	if (rotation_requested || !logfile_exists || !rotation_file_exists)
	{
		time_t		timestamp = time(NULL);

		control_file = fopen(control, "w");
		if (control_file == NULL)
			elog_stderr(ERROR, "cannot open rotation file \"%s\": %s",
						control, strerror(errno));

		fprintf(control_file, "%ld", timestamp);

		fclose(control_file);
	}

	/*
	 * Arrange to close opened file at proc_exit.
	 */
	if (!exit_hook_registered)
	{
		pgut_atexit_push(release_logfile, NULL);
		exit_hook_registered = true;
	}
}

/*
 * Closes opened file.
 */
static void
release_logfile(bool fatal, void *userdata)
{
	if (log_file)
	{
		fclose(log_file);
		log_file = NULL;
	}
	if (error_log_file)
	{
		fclose(error_log_file);
		error_log_file = NULL;
	}
}
