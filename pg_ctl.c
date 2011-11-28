/*-------------------------------------------------------------------------
 *
 * pg_ctl.c: operations for control file
 *
 * Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <signal.h>
#include <sys/types.h>
#include <unistd.h>

#include "pg_rman.h"

/* PID can be negative for standalone backend */
typedef long pgpid_t;

static pgpid_t get_pgpid(void);
static bool postmaster_is_alive(pid_t pid);
static bool read_control_file(const char *path, ControlFileData *ctrl);

static pgpid_t
get_pgpid(void)
{
	FILE	   *fp;
	pgpid_t		pid;
	char		path[MAXPGPATH];

	snprintf(path, lengthof(path), "%s/postmaster.pid", pgdata);
	if ((fp = pgut_fopen(path, "r")) == NULL)
		return 0;	/* No pid file, not an error on startup */
	if (fscanf(fp, "%ld", &pid) != 1)
		elog(ERROR_INCOMPATIBLE, "invalid data in PID file \"%s\"", path);
	fclose(fp);

	return pid;
}

/*
 *	utility routines
 */

static bool
postmaster_is_alive(pid_t pid)
{
	/*
	 * Test to see if the process is still there.  Note that we do not
	 * consider an EPERM failure to mean that the process is still there;
	 * EPERM must mean that the given PID belongs to some other userid, and
	 * considering the permissions on $PGDATA, that means it's not the
	 * postmaster we are after.
	 *
	 * Don't believe that our own PID or parent shell's PID is the postmaster,
	 * either.	(Windows hasn't got getppid(), though.)
	 */
	if (pid == getpid())
		return false;
#ifndef WIN32
	if (pid == getppid())
		return false;
#endif
	if (kill(pid, 0) == 0)
		return true;
	return false;
}

/*
 * original is do_status() in src/bin/pg_ctl/pg_ctl.c
 * changes are:
 *   renamed from do_status() from do_status().
 *   return true if PG server is running.
 *   don't print any message.
 *   don't print postopts file.
 *   log with elog() in pgut library.
 */
bool
is_pg_running(void)
{
	pgpid_t		pid;

	pid = get_pgpid();
	if (pid == 0)				/* 0 means no pid file */
		return false;

	if (pid < 0)			/* standalone backend */
		pid = -pid;

	return postmaster_is_alive((pid_t) pid);
}

static void
compare_uint32(const char *name, uint32 server, uint32 backup)
{
	if (server != backup)
		elog(ERROR_INCOMPATIBLE,
			"incompatible %s: server=%u / backup=%u",
			name, server, backup);
}

static void
compare_uint64(const char *name, uint64 server, uint64 backup)
{
	if (server != backup)
		elog(ERROR_INCOMPATIBLE,
			"incompatible %s: server=" UINT64_FORMAT " / backup=" UINT64_FORMAT,
			name, server, backup);
}

static void
compare_double(const char *name, double server, double backup)
{
	if (server != backup)
		elog(ERROR_INCOMPATIBLE,
			"incompatible %s: server=%f / backup=%f",
			name, server, backup);
}

static void
compare_bool(const char *name, bool server, bool backup)
{
	if ((server && !backup) || (!server && backup))
		elog(ERROR_INCOMPATIBLE,
			"incompatible %s: server=%s / backup=%s",
			name, (server ? "true" : "false"), (backup ? "true" : "false"));
}

/* verify control file */
bool
verify_control_file(const char *pgdata, const char *catalog)
{
	char			path[MAXPGPATH];
	ControlFileData	ctrl;
	bool			in_pgdata;
	bool			in_backup;

	snprintf(path, MAXPGPATH, "%s/global/pg_control", pgdata);
	in_pgdata = read_control_file(path, &pgControlFile);

	snprintf(path, MAXPGPATH, "%s/pg_control", catalog);
	in_backup = read_control_file(path, &ctrl);

	if (in_pgdata)
	{
		if (in_backup)
		{
			/* compare control files */
			compare_uint32("pg_control version number",
				pgControlFile.pg_control_version, ctrl.pg_control_version);
			compare_uint32("catalog version number",
				pgControlFile.catalog_version_no, ctrl.catalog_version_no);
			compare_uint64("database system identifier",
				pgControlFile.system_identifier, ctrl.system_identifier);
			compare_uint32("maximum data alignment",
				pgControlFile.maxAlign, ctrl.maxAlign);
			compare_uint32("float format",
				pgControlFile.floatFormat, ctrl.floatFormat);
			compare_double("database block size",
				pgControlFile.blcksz, ctrl.blcksz);
			compare_uint32("blocks per segment of large relation",
				pgControlFile.relseg_size, ctrl.relseg_size);
			compare_uint32("wal block size",
				pgControlFile.xlog_blcksz, ctrl.xlog_blcksz);
			compare_uint32("bytes per wal segment",
				pgControlFile.xlog_seg_size, ctrl.xlog_seg_size);
			compare_uint32("maximum length of identifiers",
				pgControlFile.nameDataLen, ctrl.nameDataLen);
			compare_uint32("maximum columns in an index",
				pgControlFile.indexMaxKeys, ctrl.indexMaxKeys);
			compare_uint32("maximum size of a toast chunk",
				pgControlFile.toast_max_chunk_size, ctrl.toast_max_chunk_size);
			compare_bool("date/time type storage",
				pgControlFile.enableIntTimes, ctrl.enableIntTimes);
			compare_bool("float4 argument passing",
				pgControlFile.float4ByVal, ctrl.float4ByVal);
			compare_bool("float8 argument passing",
				pgControlFile.float8ByVal, ctrl.float8ByVal);
		}
		else
		{
			/* write in_backup pg_control */
			FILE *fp = pgut_fopen(path, "w");
			if (fwrite(&pgControlFile, 1, sizeof(ControlFileData), fp) != sizeof(ControlFileData))
			{
				fclose(fp);
				unlink(path);
				ereport(ERROR,
					(errcode_errno(),
					 errmsg("could not write control file \"%s\": ", path)));
			}
			fclose(fp);
		}
		return true;
	}
	else
	{
		if (in_backup)
		{
			/* volatile parts are unavialable */
			memset(((char *) &ctrl) + offsetof(ControlFileData, state), 0,
					offsetof(ControlFileData, maxAlign) - offsetof(ControlFileData, state));
			pgControlFile = ctrl;
		}
		else
			ereport(ERROR,
				(errcode(ENOENT),
				 errmsg("control files not found")));
		return false;
	}
}

/*
 * FIXME: ControlFileData might be changed in versions.
 */
static bool
read_control_file(const char *path, ControlFileData *ctrl)
{
	FILE	   *fp;
	pg_crc32	crc;

	if ((fp = pgut_fopen(path, "r")) == NULL)
	{
		memset(ctrl, 0, sizeof(ControlFileData));
		return false;
	}
	if (fread(ctrl, 1, sizeof(ControlFileData), fp) != sizeof(ControlFileData))
		elog(ERROR_INCOMPATIBLE,
			"could not read control file \"%s\": %s", path, strerror(errno));
	fclose(fp);

	/* Check the CRC. */
	INIT_CRC32(crc);
	COMP_CRC32(crc, ctrl, offsetof(ControlFileData, crc));
	FIN_CRC32(crc);
	if (!EQ_CRC32(crc, ctrl->crc))
		elog(ERROR_INCOMPATIBLE,
			"bad CRC checksum for control file \"%s\"", path);

	return true;
}
