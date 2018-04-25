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

int			log_level_console = LOG_NONE;
int			log_level_file = LOG_NONE;

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

static void elog_internal(int elevel, bool file_only, const char *fmt, va_list args)
						  pg_attribute_printf(3, 0);
static void elog_stderr(int elevel, const char *fmt, ...)
						pg_attribute_printf(2, 3);

/* Functions to work with log files */
static void open_logfile(FILE **file, const char *filename_format);
static void release_logfile(void);
static char *logfile_getname(const char *format, time_t timestamp);
static FILE *logfile_open(const char *filename, const char *mode);

/* Static variables */

static FILE *log_file = NULL;
static FILE *error_log_file = NULL;

static bool exit_hook_registered = false;
/* Logging of the current thread is in progress */
static bool loggin_in_progress = false;

static pthread_mutex_t log_file_mutex = PTHREAD_MUTEX_INITIALIZER;

void
init_logger(const char *root_path)
{
	/* Set log path */
	if (LOG_LEVEL_FILE != LOG_OFF || error_log_filename)
	{
		if (log_directory)
			strcpy(log_path, log_directory);
		else
			join_path_components(log_path, root_path, "log");
	}
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
		case FATAL:
			fputs("FATAL: ", stream);
			break;
		case PANIC:
			fputs("PANIC: ", stream);
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
		/* Interrupt other possible routines */
		interrupted = true;

		/* If this is not the main thread then don't call exit() */
#ifdef WIN32
		if (main_tid != GetCurrentThreadId())
			ExitThread(elevel);
#else
		if (!pthread_equal(main_tid, pthread_self()))
			pthread_exit(NULL);
#endif
		else
			exit(elevel);
	}
}

/*
 * Logs to stderr or to log file and exit if ERROR or FATAL.
 *
 * Actual implementation for elog() and pg_log().
 */
static void
elog_internal(int elevel, bool file_only, const char *fmt, va_list args)
{
	bool		write_to_file,
				write_to_error_log,
				write_to_stderr;
	va_list		error_args,
				std_args;
	time_t		log_time = (time_t) time(NULL);
	char		strfbuf[128];

	write_to_file = elevel >= LOG_LEVEL_FILE && log_path[0] != '\0';
	write_to_error_log = elevel >= ERROR && error_log_filename &&
		log_path[0] != '\0';
	write_to_stderr = elevel >= LOG_LEVEL_CONSOLE && !file_only;

	/*
	 * There is no need to lock if this is elog() from upper elog().
	 */
	if (!loggin_in_progress)
	{
		pthread_mutex_lock(&log_file_mutex);
		loggin_in_progress = true;
	}

	/* We need copy args only if we need write to error log file */
	if (write_to_error_log)
		va_copy(error_args, args);
	/*
	 * We need copy args only if we need write to stderr. But do not copy args
	 * if we need to log only to stderr.
	 */
	if (write_to_stderr && write_to_file)
		va_copy(std_args, args);

	if (write_to_file || write_to_error_log)
		strftime(strfbuf, sizeof(strfbuf), "%Y-%m-%d %H:%M:%S %Z",
				 localtime(&log_time));

	/*
	 * Write message to log file.
	 * Do not write to file if this error was raised during write previous
	 * message.
	 */
	if (write_to_file)
	{
		if (log_file == NULL)
		{
			if (log_filename == NULL)
				open_logfile(&log_file, "pg_probackup.log");
			else
				open_logfile(&log_file, log_filename);
		}

		fprintf(log_file, "%s: ", strfbuf);
		write_elevel(log_file, elevel);

		vfprintf(log_file, fmt, args);
		fputc('\n', log_file);
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
			open_logfile(&error_log_file, error_log_filename);

		fprintf(error_log_file, "%s: ", strfbuf);
		write_elevel(error_log_file, elevel);

		vfprintf(error_log_file, fmt, error_args);
		fputc('\n', error_log_file);
		fflush(error_log_file);

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

	if (loggin_in_progress)
	{
		pthread_mutex_unlock(&log_file_mutex);
		loggin_in_progress = false;
	}

	exit_if_necessary(elevel);
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
	if (elevel < LOG_LEVEL_CONSOLE && elevel < ERROR)
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
	if (elevel < LOG_LEVEL_CONSOLE && elevel < LOG_LEVEL_FILE && elevel < ERROR)
		return;

	va_start(args, fmt);
	elog_internal(elevel, false, fmt, args);
	va_end(args);
}

/*
 * Logs only to log file and exit if ERROR or FATAL.
 */
void
elog_file(int elevel, const char *fmt, ...)
{
	va_list		args;

	/*
	 * Do not log message if severity level is less than log_level.
	 * It is the little optimisation to put it here not in elog_internal().
	 */
	if (elevel < LOG_LEVEL_FILE && elevel < ERROR)
		return;

	va_start(args, fmt);
	elog_internal(elevel, true, fmt, args);
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
	if (elevel < LOG_LEVEL_CONSOLE && elevel < LOG_LEVEL_FILE && elevel < ERROR)
		return;

	va_start(args, fmt);
	elog_internal(elevel, false, fmt, args);
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
		elog_stderr(ERROR, "logging path is not set");

	filename = (char *) palloc(MAXPGPATH);

	snprintf(filename, MAXPGPATH, "%s/", log_path);

	len = strlen(filename);

	/* Treat log_filename as a strftime pattern */
	if (strftime(filename + len, MAXPGPATH - len, format, tm) <= 0)
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
	mkdir(log_path, S_IRWXU);

	fh = fopen(filename, mode);

	if (fh)
		setvbuf(fh, NULL, PG_IOLBF, 0);
	else
	{
		int			save_errno = errno;

		elog_stderr(FATAL, "could not open log file \"%s\": %s",
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
			elog_stderr(ERROR, "cannot stat log file \"%s\": %s",
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
					elog_stderr(ERROR, "cannot stat rotation file \"%s\": %s",
								control, strerror(errno));
			}
			else
			{
				char		buf[1024];

				control_file = fopen(control, "r");
				if (control_file == NULL)
					elog_stderr(ERROR, "cannot open rotation file \"%s\": %s",
								control, strerror(errno));

				if (fgets(buf, lengthof(buf), control_file))
				{
					time_t		creation_time;

					if (!parse_int64(buf, (int64 *) &creation_time, 0))
						elog_stderr(ERROR, "rotation file \"%s\" has wrong "
									"creation timestamp \"%s\"",
									control, buf);
					/* Parsed creation time */

					rotation_requested = (cur_time - creation_time) >
						/* convert to seconds */
						log_rotation_age * 60;
				}
				else
					elog_stderr(ERROR, "cannot read creation timestamp from "
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
