/*-------------------------------------------------------------------------
 *
 * utils.c:
 *
 * Copyright (c) 2009-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_rman.h"

#include <dirent.h>
#include <time.h>
#include <limits.h>

#ifdef WIN32
#include <winioctl.h>
#endif

/*
 * Convert time_t value to ISO-8601 format string.
 * The size of buffer must be larger than DATESTRLEN.
 */
char *
date2str(char *buf, time_t date)
{
	struct tm *tm = localtime(&date);
	strftime(buf, DATESTRLEN, "%Y-%m-%d %H:%M:%S", tm);
	return buf;
}

/*
 * The size of buffer must be larger than TIMESTRLEN.
 */
char *
time2str(char *buf, time_t time)
{
	/* set empty if nagative duration */
	if (time < 0)
		buf[0] = '\0';
	else if (time >= 100 * 24 * 60 * 60)
		snprintf(buf, TIMESTRLEN, "%.1fd", time / 86400.0);
	else if (time >= 60 * 60)
		snprintf(buf, TIMESTRLEN, "%.1fh", time / 3600.0);
	else if (time >= 60)
		snprintf(buf, TIMESTRLEN, "%.1fm", time / 60.0);
	else
		snprintf(buf, TIMESTRLEN, "%lds", (long) time);
	return buf;
}

/*
 * The size of buffer must be larger than SIZESTRLEN.
 */
char *
size2str(char *buf, int64 size)
{
	int		exp;
	int64	base;
	double	n;
	static const char *units[] = { "B ", "KB", "MB", "GB", "TB", "PB" };

	/* set empty if nagative size */
	if (size < 0)
	{
		buf[0] = '\0';
		return buf;
	}

	/* determine the unit */
	for (exp = 0, base = 1;
		 exp < lengthof(units) && base * 1024 < size;
		 ++exp, base *= 1024)
		 ;

	n = size / (double) base;
	if (n >= 100.0)
		snprintf(buf, SIZESTRLEN, "%4.0f%s", n, units[exp]);
	else if (n >= 10.0)
		snprintf(buf, SIZESTRLEN, "%3.1f%s", n, units[exp]);
	else
		snprintf(buf, SIZESTRLEN, "%3.2f%s", n, units[exp]);

	return buf;
}

/*
 * Parse for backup mode. empty input is treated as full.
 */
BackupMode
parse_backup_mode(const char *value)
{
	const char *v = value;
	size_t		len;

	if (v == NULL)
		return MODE_FULL;	/* null input is full. */

	while (IsSpace(*v)) { v++; }
	if ((len = strlen(v)) == 0)
		return MODE_FULL;	/* empty input is full. */

	/* Do a prefix match. For example, "incr" means incremental.  */
	if (pg_strncasecmp("full", v, len) == 0)
		return MODE_FULL;
	else if (pg_strncasecmp("incremental", v, len) == 0)
		return MODE_INCREMENTAL;
	else if (pg_strncasecmp("archive", v, len) == 0)
		return MODE_ARCHIVE;

	ereport(ERROR,
		(errcode(EINVAL),
		 errmsg("invalid backup mode: '%s'", value)));
	return (BackupMode) -1;
}

XLogName
parse_xlogname(const char *value)
{
	XLogName	xlog;
	char		junk[2];

	if (sscanf(value, "%08X%08X%08X%1s",
		&xlog.tli, &xlog.log, &xlog.seg, junk) != 3)
		ereport(ERROR,
			(errcode(EINVAL),
			 errmsg("invalid xlog name: '%s'", value)));

	return xlog;
}

/* return max value of time_t */
time_t
time_max(void)
{
	static time_t	value = 0;

	if (value == 0)
	{
		if (sizeof(time_t) > sizeof(int32))
		{
			struct tm	tm = { 0 };

			/* '9999-12-31 23:59:59' for 64bit time_t */
			tm.tm_year = 9999 - 1900;
			tm.tm_mon = 12 - 1;
			tm.tm_mday = 31;
			tm.tm_hour = 23;
			tm.tm_min = 59;
			tm.tm_sec = 59;

			value = mktime(&tm);
		}
		else
		{
			/* '2038-01-19 03:14:07' for 32bit time_t */
			value = INT_MAX;
		}
	}

	return value;
}

/*
 * Create range object from one or two arguments.
 * All not-digit characters in the argument(s) are igonred.
 */
pgRange
make_range(int argc, char * const *argv)
{
	pgRange		range;
	const char *arg1;
	const char *arg2;
	size_t		len;
	char	   *tmp;
	int			i;
	struct tm	tm;
	char		junk[2];

	/* takes 0, 1, or 2 arguments */
	if (argc > 2)
		ereport(ERROR,
			(errcode(EINVAL),
			 errmsg("too many arguments")));

	/* no input means unlimited range */
	if (argc < 1)
	{
		range.begin = 0;
		range.end = time_max();
		return range;
	}

	arg1 = argv[0];
	arg2 = (argc > 1 ? argv[1] : "");

	/* tmp = replace( concat(arg1, arg2), !isalnum, ' ' ) */
	tmp = pgut_malloc(strlen(arg1) + strlen(arg2) + 1);
	len = 0;
	for (i = 0; arg1[i]; i++)
		tmp[len++] = (IsAlnum(arg1[i]) ? arg1[i] : ' ');
	for (i = 0; arg2[i]; i++)
		tmp[len++] = (IsAlnum(arg2[i]) ? arg2[i] : ' ');
	tmp[len] = '\0';

	/* parse for "YYYY-MM-DD HH:MI:SS" */
	tm.tm_year = 0;		/* tm_year is year - 1900 */
	tm.tm_mon = 0;		/* tm_mon is 0 - 11 */
	tm.tm_mday = 1;		/* tm_mday is 1 - 31 */
	tm.tm_hour = 0;
	tm.tm_min = 0;
	tm.tm_sec = 0;
	i = sscanf(tmp, "%04d %02d %02d %02d %02d %02d%1s",
		&tm.tm_year, &tm.tm_mon, &tm.tm_mday,
		&tm.tm_hour, &tm.tm_min, &tm.tm_sec, junk);
	if (i < 1 || 6 < i)
		ereport(ERROR,
			(errcode(EINVAL),
			 errmsg("invalid range syntax: '%s'", tmp)));

	free(tmp);

	/* adjust year */
	if (tm.tm_year < 100)
		tm.tm_year += 2000 - 1900;
	else if (tm.tm_year >= 1900)
		tm.tm_year -= 1900;

	/* adjust month */
	if (i > 1)
		tm.tm_mon -= 1;

	range.begin = mktime(&tm);
	switch (i)
	{
		case 1:
			tm.tm_year++;
			break;
		case 2:
			tm.tm_mon++;
			break;
		case 3:
			tm.tm_mday++;
			break;
		case 4:
			tm.tm_hour++;
			break;
		case 5:
			tm.tm_min++;
			break;
		case 6:
			tm.tm_sec++;
			break;
	}
	range.end = mktime(&tm);

	return range;
}

/*
 * check path is a directory and returns errno of opendir.
 * ENOENT is treated as succeeded if missing_ok.
 */
int
check_dir(const char *path, bool missing_ok)
{
	DIR	   *dir;

	if ((dir = opendir(path)) == NULL)
	{
		if (missing_ok && errno == ENOENT)
			return 0;
		else
			return errno;
	}

	closedir(dir);
	return 0;
}

/*
 * make sure the directory either doesn't exist or is empty
 *
 * Returns 0 if nonexistent, 1 if exists and empty, 2 if not empty,
 * or -1 if trouble accessing directory
 */
void
make_empty_dir(const char *path)
{
	DIR *dir;

	errno = 0;
	if ((dir = opendir(path)) == NULL)
	{
		/* Directory does not exist. */
		if (errno != ENOENT)
			ereport(ERROR,
				(errcode_errno(),
				 errmsg("could not access directory \"%s\": ", path)));

		pgut_mkdir(path);
	}
	else
	{
		/* Directory exists. */
		struct dirent *file;

		while ((file = readdir(dir)) != NULL)
		{
			if (strcmp(".", file->d_name) == 0 ||
				strcmp("..", file->d_name) == 0)
			{
				/* skip this and parent directory */
				continue;
			}
			else
			{
				/* Present and not empty */
				closedir(dir);

				ereport(ERROR,
					(errcode(EEXIST),
					 errmsg("directory \"%s\" exists but is not empty", path)));
			}
		}

#ifdef WIN32
		/*
		 * This fix is in mingw cvs (runtime/mingwex/dirent.c rev 1.4),
		 * but not in released version
		 */
		if (GetLastError() == ERROR_NO_MORE_FILES)
			errno = 0;
#endif
		closedir(dir);
	}
}

/*
 * Remove files recursively, but follow symbolic link to directories.
 * We remove the symbolic link files, but delete the linked directories.
 */
void
remove_file(const char *path)
{
	remove_children(path);

	if (remove(path) != 0 && errno != ENOENT)
		elog(ERROR, "could not remove file \"%s\": %s", path, strerror(errno));
}

void
remove_children(const char *path)
{
	DIR	   *dir;

	/* try to open as directory and remove children. */
	if ((dir = opendir(path)) != NULL)
	{
		struct dirent  *dent;

		while ((dent = readdir(dir)) != NULL)
		{
			char	child[MAXPGPATH];

			/* skip entries point current dir or parent dir */
			if (strcmp(dent->d_name, ".") == 0 ||
				strcmp(dent->d_name, "..") == 0)
				continue;

			join_path_components(child, path, dent->d_name);
			remove_file(child);
		}

		closedir(dir);
	}
}

#ifdef WIN32

#define REPARSE_DATA_SIZE		1024

/* same layout as REPARSE_DATA_BUFFER, which is defined only in old winnt.h */
typedef struct REPARSE_DATA
{
	ULONG	ReparseTag;
	WORD	ReparseDataLength;
	WORD	Reserved;
	union
	{
		struct
		{
			WORD	SubstituteNameOffset;
			WORD	SubstituteNameLength;
			WORD	PrintNameOffset;
			WORD	PrintNameLength;
			ULONG	Flags;
			WCHAR	PathBuffer[1];
		} Symlink;
		struct
		{
			WORD	SubstituteNameOffset;
			WORD	SubstituteNameLength;
			WORD	PrintNameOffset;
			WORD	PrintNameLength;
			WCHAR	PathBuffer[1];
		} Mount;
		struct
		{
			BYTE  DataBuffer[REPARSE_DATA_SIZE];
		} Generic;
	};
} REPARSE_DATA;

ssize_t
readlink(const char *path, char *target, size_t size)
{
    HANDLE			handle;
 	DWORD			attr;
	REPARSE_DATA	data;
 	DWORD			datasize;
	PCWSTR			wpath;
	int				wlen;
	int				r;

	attr = GetFileAttributes(path);
	if (attr == INVALID_FILE_ATTRIBUTES)
	{
		_dosmaperr(GetLastError());
        return -1;
    }
	if ((attr & FILE_ATTRIBUTE_REPARSE_POINT) == 0)
	{
		errno = EINVAL;	/* not a symlink */
        return -1;
	}

    handle = CreateFileA(path, 0,
		FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE, NULL,
		OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
	if (handle == INVALID_HANDLE_VALUE)
	{
		_dosmaperr(GetLastError());
        return -1;
    }

	wpath = NULL;
	if (DeviceIoControl(handle, FSCTL_GET_REPARSE_POINT, NULL, 0,
        &data, sizeof(data), &datasize, NULL))
	{
		switch (data.ReparseTag)
		{
			case IO_REPARSE_TAG_MOUNT_POINT:
			{
				wpath = data.Mount.PathBuffer + data.Mount.SubstituteNameOffset;
				wlen = data.Mount.SubstituteNameLength;
				break;
			}
			case IO_REPARSE_TAG_SYMLINK:
			{
				wpath = data.Symlink.PathBuffer + data.Symlink.SubstituteNameOffset;
				wlen = data.Symlink.SubstituteNameLength;
				break;
			}
		}
	}

	if (wpath == NULL)
		r = -1;
	else
	{
		if (wcsncmp(wpath, L"\\??\\", 4) == 0 ||
			wcsncmp(wpath, L"\\\\?\\", 4) == 0)
		{
			wpath += 4;
			wlen -= 4;
		}
		r = WideCharToMultiByte(CP_ACP, 0, wpath, wlen, target, size, NULL, NULL);
	}

	CloseHandle(handle);
	return r;
}

#endif
