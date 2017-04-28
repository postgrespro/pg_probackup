/*-------------------------------------------------------------------------
 *
 * logger.c: - log events into log file or stderr.
 *
 * Portions Copyright (c) 2017-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#include "logger.h"

/* Logger parameters */

int			log_level = INFO;

char	   *log_filename = NULL;
char	   *error_log_filename = NULL;
char	   *log_directory = NULL;
char		log_path[MAXPGPATH] = "";

int			log_rotation_size = 0;
int			log_rotation_age = 0;

/* Implementation for logging.h */

typedef enum
{
	PG_DEBUG,
	PG_PROGRESS,
	PG_WARNING,
	PG_FATAL
} eLogType;

void pg_log(eLogType type, const char *fmt,...) pg_attribute_printf(2, 3);

static void elog_internal(int elevel, const char *fmt, va_list args)
						  pg_attribute_printf(2, 0);

/* Functions to work with log files */
static void open_logfile(FILE **file, const char *filename_format);
static void release_logfile(void);
static char *logfile_getname(const char *format, time_t timestamp);
static FILE *logfile_open(const char *filename, const char *mode);

/* Static variables */

static FILE *log_file = NULL;
static FILE *error_log_file = NULL;

static bool exit_hook_registered = false;
/* Logging to file is in progress */
static bool logging_to_file = false;

static pthread_mutex_t log_file_mutex = PTHREAD_MUTEX_INITIALIZER;

static void
write_elevel(FILE *stream, int elevel)
{
	switch (elevel)
	{
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
		case FATAL:
			fputs("FATAL: ", stream);
			break;
		case PANIC:
			fputs("PANIC: ", stream);
			break;
		default:
			elog(ERROR, "invalid logging level: %d", elevel);
			break;
	}
}

/*
 * Logs to stderr or to log file and exit if ERROR or FATAL.
 *
 * Actual implementation for elog() and pg_log().
 */
static void
elog_internal(int elevel, const char *fmt, va_list args)
{
	bool		wrote_to_file = false;

	pthread_mutex_lock(&log_file_mutex);

	/*
	 * Write message to log file.
	 * Do not write to file if this error was raised during write previous
	 * message.
	 */
	if (log_filename && !logging_to_file)
	{
		logging_to_file = true;

		if (log_file == NULL)
			open_logfile(&log_file, log_filename);

		write_elevel(log_file, elevel);

		vfprintf(log_file, fmt, args);
		fputc('\n', log_file);
		fflush(log_file);

		logging_to_file = false;
		wrote_to_file = true;
	}

	/*
	 * Write error message to error log file.
	 * Do not write to file if this error was raised during write previous
	 * message.
	 */
	if (elevel >= ERROR && error_log_filename && !logging_to_file)
	{
		logging_to_file = true;

		if (error_log_file == NULL)
			open_logfile(&error_log_file, error_log_filename);

		write_elevel(error_log_file, elevel);

		vfprintf(error_log_file, fmt, args);
		fputc('\n', error_log_file);
		fflush(error_log_file);

		logging_to_file = false;
	}

	/*
	 * Write to stderr if the message was not written to log file.
	 * Write to stderr if the message level is greater than WARNING anyway.
	 */
	if (!wrote_to_file ||
		elevel >= ERROR)
	{
		write_elevel(stderr, elevel);

		vfprintf(stderr, fmt, args);
		fputc('\n', stderr);
		fflush(stderr);
	}

	pthread_mutex_unlock(&log_file_mutex);

	/* Exit with code if it is an error */
	if (elevel > WARNING)
		exit(elevel);
}

/*
 * Logs to stderr or to log file and exit if ERROR or FATAL.
 */
void
elog(int elevel, const char *fmt, ...)
{
	va_list		args;

	/*
	 * Do not log message if severity level is less than log_level.
	 * It is the little optimisation to put it here not in elog_internal().
	 */
	if (elevel < log_level && elevel < ERROR)
		return;

	va_start(args, fmt);
	elog_internal(elevel, fmt, args);
	va_end(args);
}

/*
 * Implementation of pg_log() from logging.h.
 */
void
pg_log(eLogType type, const char *fmt, ...)
{
	va_list		args;
	int			elevel;

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
	}

	/*
	 * Do not log message if severity level is less than log_level.
	 * It is the little optimisation to put it here not in elog_internal().
	 */
	if (elevel < log_level && elevel < ERROR)
		return;

	va_start(args, fmt);
	elog_internal(elevel, fmt, args);
	va_end(args);
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

	if (pg_strncasecmp("verbose", v, len) == 0)
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
	else if (pg_strncasecmp("fatal", v, len) == 0)
		return FATAL;
	else if (pg_strncasecmp("panic", v, len) == 0)
		return PANIC;

	/* Log level is invalid */
	elog(ERROR, "invalid log-level \"%s\"", level);
	return 0;
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

	if (log_path[0] == '\0')
		elog(ERROR, "logging path is not set");

	filename = (char *) palloc(MAXPGPATH);

	snprintf(filename, MAXPGPATH, "%s/", log_path);

	len = strlen(filename);

	/* Treat log_filename as a strftime pattern */
	if (strftime(filename + len, MAXPGPATH - len, format, tm) <= 0)
		elog(ERROR, "strftime(%s) failed: %s", format, strerror(errno));

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
	mkdir(log_path, S_IRWXU);

	fh = fopen(filename, mode);

	if (fh)
		setvbuf(fh, NULL, PG_IOLBF, 0);
	else
	{
		int			save_errno = errno;

		elog(FATAL, "could not open log file \"%s\": %s",
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
	struct stat	st;
	bool		rotation_requested = false;

	filename = logfile_getname(filename_format, time(NULL));

	/* First check for rotation */
	if (log_rotation_size > 0 || log_rotation_age > 0)
	{
		if (stat(filename, &st) == -1)
		{
			if (errno == ENOENT)
			{
				/* There is no file "filename" and rotation does not need */
				goto logfile_open;
			}
			else
				elog(ERROR, "cannot stat log file \"%s\": %s",
					 filename, strerror(errno));
		}
		/* Found log file "filename" */

		/* Check for rotation by age */
		if (log_rotation_age > 0)
		{
			char		control[MAXPGPATH];
			struct stat	control_st;
			FILE	   *control_file;

			snprintf(control, MAXPGPATH, "%s.rotation", filename);
			if (stat(control, &control_st) == -1)
			{
				if (errno == ENOENT)
				{
					/* There is no control file for rotation */
					goto logfile_open;
				}
				else
					elog(ERROR, "cannot stat rotation file \"%s\": %s",
						 control, strerror(errno));
			}

			/* Found control file for rotation */

			control_file = fopen(control, "r");
			fclose(control_file);
		}

		/* Check for rotation by size */
		if (!rotation_requested && log_rotation_size > 0)
			rotation_requested = (st.st_size >= log_rotation_size * 1024L);
	}

logfile_open:
	if (rotation_requested)
		*file = logfile_open(filename, "w");
	else
		*file = logfile_open(filename, "a");
	pfree(filename);

	/*
	 * Arrange to close opened file at proc_exit.
	 */
	if (!exit_hook_registered)
	{
		atexit(release_logfile);
		exit_hook_registered = true;
	}
}

/*
 * Closes opened file.
 */
static void
release_logfile(void)
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
