/*-------------------------------------------------------------------------
 *
 * logger.c: - log events into log file or stderr.
 *
 * Copyright (c) 2017-2017, Postgres Professional
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
#include "pgut.h"

/* Logger parameters */

int			log_level = INFO;
bool		log_level_defined = false;

char	   *log_filename = NULL;
char	   *error_log_filename = NULL;
char	   *log_directory = NULL;
/*
 * If log_path is empty logging is not initialized.
 * We will log only into stderr
 */
char		log_path[MAXPGPATH] = "";

/* Maximum size of an individual log file in kilobytes */
int			log_rotation_size = 0;
/* Maximum lifetime of an individual log file in minutes */
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
	bool		write_to_file,
				write_to_error_log,
				write_to_stderr;
	va_list		error_args,
				std_args;

	write_to_file = log_path[0] != '\0' && !logging_to_file &&
		(log_filename || error_log_filename);

	/*
	 * There is no need to lock if this is elog() from upper elog() and
	 * logging is not initialized.
	 */
	if (write_to_file)
		pthread_mutex_lock(&log_file_mutex);

	write_to_error_log =
		elevel >= ERROR && error_log_filename && write_to_file;
	write_to_stderr = elevel >= ERROR || !write_to_file;

	/* We need copy args only if we need write to error log file */
	if (write_to_error_log)
		va_copy(error_args, args);
	/*
	 * We need copy args only if we need write to stderr. But do not copy args
	 * if we need to log only to stderr.
	 */
	if (write_to_stderr && write_to_file)
		va_copy(std_args, args);

	/*
	 * Write message to log file.
	 * Do not write to file if this error was raised during write previous
	 * message.
	 */
	if (log_filename && write_to_file)
	{
		logging_to_file = true;

		if (log_file == NULL)
			open_logfile(&log_file, log_filename);

		write_elevel(log_file, elevel);

		vfprintf(log_file, fmt, args);
		fputc('\n', log_file);
		fflush(log_file);

		logging_to_file = false;
	}

	/*
	 * Write error message to error log file.
	 * Do not write to file if this error was raised during write previous
	 * message.
	 */
	if (write_to_error_log)
	{
		logging_to_file = true;

		if (error_log_file == NULL)
			open_logfile(&error_log_file, error_log_filename);

		write_elevel(error_log_file, elevel);

		vfprintf(error_log_file, fmt, error_args);
		fputc('\n', error_log_file);
		fflush(error_log_file);

		logging_to_file = false;
		va_end(error_args);
	}

	/*
	 * Write to stderr if the message was not written to log file.
	 * Write to stderr if the message level is greater than WARNING anyway.
	 */
	if (write_to_stderr)
	{
		write_elevel(stderr, elevel);

		if (write_to_file)
			vfprintf(stderr, fmt, std_args);
		else
			vfprintf(stderr, fmt, args);
		fputc('\n', stderr);
		fflush(stderr);

		if (write_to_file)
			va_end(std_args);
	}

	if (write_to_file)
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
 * Converts integer representation of log level to string.
 */
const char *
deparse_log_level(int level)
{
	switch (level)
	{
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
		case FATAL:
			return "FATAL";
		case PANIC:
			return "PANIC";
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
	char		control[MAXPGPATH];
	struct stat	st;
	FILE	   *control_file;
	time_t		cur_time = time(NULL);
	bool		rotation_requested = false,
				logfile_exists = false;

	filename = logfile_getname(filename_format, cur_time);

	/* "log_path" was checked in logfile_getname() */
	snprintf(control, MAXPGPATH, "%s.rotation", filename);

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
	logfile_exists = true;

	/* First check for rotation */
	if (log_rotation_size > 0 || log_rotation_age > 0)
	{
		/* Check for rotation by age */
		if (log_rotation_age > 0)
		{
			struct stat	control_st;

			if (stat(control, &control_st) == -1)
			{
				if (errno != ENOENT)
					elog(ERROR, "cannot stat rotation file \"%s\": %s",
						 control, strerror(errno));
			}
			else
			{
				char		buf[1024];

				control_file = fopen(control, "r");
				if (control_file == NULL)
					elog(ERROR, "cannot open rotation file \"%s\": %s",
						 control, strerror(errno));

				if (fgets(buf, lengthof(buf), control_file))
				{
					time_t		creation_time;

					if (!parse_int64(buf, (int64 *) &creation_time))
						elog(ERROR, "rotation file \"%s\" has wrong "
							 "creation timestamp \"%s\"",
							 control, buf);
					/* Parsed creation time */

					rotation_requested = (cur_time - creation_time) >
							/* convert to seconds */
							log_rotation_age * 60;
				}
				else
					elog(ERROR, "cannot read creation timestamp from "
						 "rotation file \"%s\"", control);

				fclose(control_file);
			}
		}

		/* Check for rotation by size */
		if (!rotation_requested && log_rotation_size > 0)
			rotation_requested = st.st_size >=
					/* convert to bytes */
					log_rotation_size * 1024L;
	}

logfile_open:
	if (rotation_requested)
		*file = logfile_open(filename, "w");
	else
		*file = logfile_open(filename, "a");
	pfree(filename);

	/* Rewrite rotation control file */
	if (rotation_requested || !logfile_exists)
	{
		time_t		timestamp = time(NULL);

		control_file = fopen(control, "w");
		if (control_file == NULL)
			elog(ERROR, "cannot open rotation file \"%s\": %s",
				 control, strerror(errno));

		fprintf(control_file, "%ld", timestamp);

		fclose(control_file);
	}

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
