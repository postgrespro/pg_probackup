/*-------------------------------------------------------------------------
 *
 * logger.c: - log events into log file or stderr.
 *
 * Portions Copyright (c) 2017-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include "pgtime.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "logger.h"

/* Logger parameters */

int			log_level = INFO;

char	   *log_filename = NULL;
char	   *error_log_filename = NULL;
char	   *log_directory = NULL;

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
static void open_logfile(void);
static void release_logfile(void);
static char *logfile_getname(pg_time_t timestamp);
static FILE *logfile_open(const char *filename, const char *mode);

/* Static variables */

static FILE *log_file = NULL;
static char *last_file_name = NULL;
static bool exit_hook_registered = false;
/* Logging to file is in progress */
static bool logging_to_file = false;

static pthread_mutex_t log_file_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * Logs to stderr or to log file and exit if ERROR or FATAL.
 *
 * Actual implementation for elog() and pg_log().
 */
static void
elog_internal(int elevel, const char *fmt, va_list args)
{
	bool		wrote_to_file = false;

	switch (elevel)
	{
		case LOG:
			fputs("LOG: ", stderr);
			break;
		case INFO:
			fputs("INFO: ", stderr);
			break;
		case NOTICE:
			fputs("NOTICE: ", stderr);
			break;
		case WARNING:
			fputs("WARNING: ", stderr);
			break;
		case ERROR:
			fputs("ERROR: ", stderr);
			break;
		case FATAL:
			fputs("FATAL: ", stderr);
			break;
		case PANIC:
			fputs("PANIC: ", stderr);
			break;
		default:
			elog(ERROR, "invalid logging level: %d", elevel);
			break;
	}

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
			open_logfile();

		vfprintf(log_file, fmt, args);
		fputc('\n', log_file);
		fflush(log_file);

		logging_to_file = false;
		wrote_to_file = true;
	}

	/*
	 * Write to stderr if the message was not written to log file.
	 * Write to stderr if the message level is greater than WARNING anyway.
	 */
	if (!wrote_to_file ||
		elevel >= ERROR)
	{
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
 * Construct logfile name using timestamp information.
 *
 * Result is palloc'd.
 */
static char *
logfile_getname(pg_time_t timestamp)
{
	char	   *filename;
	size_t		len;

	filename = (char *) palloc(MAXPGPATH);

	snprintf(filename, MAXPGPATH, "%s/", log_directory);

	len = strlen(filename);

	/* treat pgaudit.log_filename as a strftime pattern */
	pg_strftime(filename + len, MAXPGPATH - len, log_filename,
				pg_localtime(&timestamp, log_timezone));

	return filename;
}

/*
 * Open a new log file.
 */
static FILE *
logfile_open(const char *filename, const char *mode)
{
	FILE	   *fh;

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
open_logfile(void)
{
	char	   *filename;

	filename = logfile_getname(time(NULL));

	log_file = logfile_open(filename, "a");

	if (last_file_name != NULL)		/* probably shouldn't happen */
		pfree(last_file_name);

	last_file_name = filename;

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
	if (last_file_name != NULL)
		pfree(last_file_name);
}
