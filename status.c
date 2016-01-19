/*-------------------------------------------------------------------------
 *
 * status.c
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 *
 * Monitor status of a PostgreSQL server.
 *
 *-------------------------------------------------------------------------
 */


#include "postgres_fe.h"

#include <signal.h>
#include <sys/types.h>
#include <unistd.h>

#include "pg_arman.h"

/* PID can be negative for standalone backend */
typedef long pgpid_t;

static pgpid_t get_pgpid(void);
static bool postmaster_is_alive(pid_t pid);

/*
 * get_pgpid
 *
 * Get PID of postmaster, by scanning postmaster.pid.
 */
static pgpid_t
get_pgpid(void)
{
	FILE	   *pidf;
	long		pid;
	char		pid_file[MAXPGPATH];

	snprintf(pid_file, lengthof(pid_file), "%s/postmaster.pid", pgdata);

	pidf = fopen(pid_file, "r");
	if (pidf == NULL)
	{
		/* No pid file, not an error on startup */
		if (errno == ENOENT)
			return 0;
		else
		{
			elog(ERROR, "could not open PID file \"%s\": %s",
				 pid_file, strerror(errno));
		}
	}
	if (fscanf(pidf, "%ld", &pid) != 1)
	{
		/* Is the file empty? */
		if (ftell(pidf) == 0 && feof(pidf))
			elog(ERROR, "the PID file \"%s\" is empty",
				 pid_file);
		else
			elog(ERROR, "invalid data in PID file \"%s\"\n",
				 pid_file);
	}
	fclose(pidf);
	return (pgpid_t) pid;
}

/*
 * postmaster_is_alive
 *
 * Check whether postmaster is alive or not.
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
 * is_pg_running
 *
 *
 */
bool
is_pg_running(void)
{
	pgpid_t		pid;

	pid = get_pgpid();

	/* 0 means no pid file */
	if (pid == 0)
		return false;

	/* Case of a standalone backend */
	if (pid < 0)
		pid = -pid;

	/* Check if postmaster is alive */
	return postmaster_is_alive((pid_t) pid);
}
