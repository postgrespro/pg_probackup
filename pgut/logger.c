/*-------------------------------------------------------------------------
 *
 * logger.c: - log events into csv-file or stderr.
 *
 * Portions Copyright (c) 2017-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "logger.h"

/* Logger parameters */

int			log_destination = LOG_DESTINATION_STDERR;
int			log_level = INFO;
bool		quiet = false;

char	   *log_filename = NULL;
char	   *log_error_filename = NULL;
char	   *log_directory = NULL;

int			log_rotation_size = 0;
int			log_rotation_age = 0;

/*
 * elog - log to stderr or to log file and exit if ERROR or FATAL
 */
void
elog(int elevel, const char *fmt, ...)
{
	va_list		args;

	/* Do not log message if severity level is less than log_level */
	if (elevel < log_level)
	{
		/* But exit with code it severity level is higher than WARNING */
		if (elevel > WARNING)
			exit(elevel);

		return;
	}

	if (quiet && elevel < WARNING)
		return;

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
	case FATAL:
		fputs("FATAL: ", stderr);
		break;
	case PANIC:
		fputs("PANIC: ", stderr);
		break;
	default:
		if (elevel >= ERROR)
			fputs("ERROR: ", stderr);
		break;
	}

	va_start(args, fmt);
	vfprintf(stderr, fmt, args);
	fputc('\n', stderr);
	fflush(stderr);
	va_end(args);

	if (elevel > WARNING)
		exit(elevel);
}
