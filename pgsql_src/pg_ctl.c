/*-------------------------------------------------------------------------
 *
 * pg_ctl --- start/stops/restarts the PostgreSQL server
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 *
 * $PostgreSQL: pgsql/src/bin/pg_ctl/pg_ctl.c,v 1.111 2009/06/11 14:49:07 momjian Exp $
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

static char pid_file[MAXPGPATH];


static pgpid_t
get_pgpid(void)
{
	FILE	   *pidf;
	long		pid;

	snprintf(pid_file, lengthof(pid_file), "%s/postmaster.pid", pgdata);
	pidf = fopen(pid_file, "r");
	if (pidf == NULL)
	{
		/* No pid file, not an error on startup */
		if (errno == ENOENT)
			return 0;
		else
			elog(ERROR_SYSTEM, _("could not open PID file \"%s\": %s\n"),
						 pid_file, strerror(errno));
	}
	if (fscanf(pidf, "%ld", &pid) != 1)
		elog(ERROR_PID_BROKEN, _("invalid data in PID file \"%s\"\n"), pid_file);
	fclose(pidf);
	return (pgpid_t) pid;
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

