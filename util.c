/*-------------------------------------------------------------------------
 *
 * util.c: log messages to log file or stderr, and misc code.
 *
 * Copyright (c) 2009-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_rman.h"

#include <time.h>

/*
 * Convert time_t value to ISO-8601 format string
 */
void
time2iso(char *buf, size_t len, time_t time)
{
	struct tm *tm = localtime(&time);

	strftime(buf, len, "%Y-%m-%d %H:%M:%S", tm);
}

const char *
status2str(BackupStatus status)
{
	static const char *statusName[] =
	{
		"UNKNOWN",
		"OK",
		"RUNNING",
		"ERROR",
		"DELETING",
		"DELETED",
		"DONE",
		"CORRUPT"
	};

	if (status < BACKUP_STATUS_INVALID || BACKUP_STATUS_CORRUPT < status)
		return "UNKNOWN";

	return statusName[status];
}

void
remove_trailing_space(char *buf, int comment_mark)
{
	int		i;
	char   *last_char = NULL;

	for (i = 0; buf[i]; i++)
	{
		if (buf[i] == comment_mark || buf[i] == '\n' || buf[i] == '\r')
		{
			buf[i] = '\0';
			break;
		}
	}
	for (i = 0; buf[i]; i++)
	{
		if (!isspace(buf[i]))
			last_char = buf + i;
	}
	if (last_char != NULL)
		*(last_char + 1) = '\0';

}

void
remove_not_digit(char *buf, size_t len, const char *str)
{
	int i, j;

	for (i = 0, j = 0; str[i] && j < len; i++)
	{
		if (!isdigit(str[i]))
			continue;
		buf[j++] = str[i];
	}
	buf[j] = '\0';
}
