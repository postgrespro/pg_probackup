/*-------------------------------------------------------------------------
 *
 * pgut.c
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2017-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"
#include "postgres_fe.h"

#include "getopt_long.h"
#include "libpq-fe.h"
#include "libpq/pqsignal.h"
#include "pqexpbuffer.h"

#include <time.h>

#include "pgut.h"
#include "logger.h"
#include "file.h"


static char	   *password = NULL;
bool			prompt_password = true;
bool			force_password = false;

/* Database connections */
static PGcancel *volatile cancel_conn = NULL;

/* Interrupted by SIGINT (Ctrl+C) ? */
bool		interrupted = false;
bool		in_cleanup = false;
bool		in_password = false;

/* critical section when adding disconnect callbackups */
static pthread_mutex_t atexit_callback_disconnect_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Connection routines */
static void init_cancel_handler(void);
static void on_before_exec(PGconn *conn, PGcancel *thread_cancel_conn);
static void on_after_exec(PGcancel *thread_cancel_conn);
static void on_interrupt(void);
static void on_cleanup(void);
static pqsigfunc oldhandler = NULL;

static char ** pgut_pgfnames(const char *path, bool strict);
static void pgut_pgfnames_cleanup(char **filenames);

void discard_response(PGconn *conn);

/* Note that atexit handlers always called on the main thread */
void
pgut_init(void)
{
	init_cancel_handler();
	atexit(on_cleanup);
}

/*
 * Ask the user for a password; 'username' is the username the
 * password is for, if one has been explicitly specified.
 * Set malloc'd string to the global variable 'password'.
 */
static void
prompt_for_password(const char *username)
{
	in_password = true;

	if (password)
	{
		free(password);
		password = NULL;
	}

#if PG_VERSION_NUM >= 100000
	password = (char *) pgut_malloc(sizeof(char) * 100 + 1);
	if (username == NULL)
		simple_prompt("Password: ", password, 100, false);
	else
	{
		char	message[256];
		snprintf(message, lengthof(message), "Password for user %s: ", username);
		simple_prompt(message, password, 100, false);
	}
#else
	if (username == NULL)
		password = simple_prompt("Password: ", 100, false);
	else
	{
		char	message[256];
		snprintf(message, lengthof(message), "Password for user %s: ", username);
		password = simple_prompt(message, 100, false);
	}
#endif

	in_password = false;
}

/*
 * Copied from pg_basebackup.c
 * Escape a parameter value so that it can be used as part of a libpq
 * connection string, e.g. in:
 *
 * application_name=<value>
 *
 * The returned string is malloc'd. Return NULL on out-of-memory.
 */
static char *
escapeConnectionParameter(const char *src)
{
	bool		need_quotes = false;
	bool		need_escaping = false;
	const char *p;
	char	   *dstbuf;
	char	   *dst;

	/*
	 * First check if quoting is needed. Any quote (') or backslash (\)
	 * characters need to be escaped. Parameters are separated by whitespace,
	 * so any string containing whitespace characters need to be quoted. An
	 * empty string is represented by ''.
	 */
	if (strchr(src, '\'') != NULL || strchr(src, '\\') != NULL)
		need_escaping = true;

	for (p = src; *p; p++)
	{
		if (isspace((unsigned char) *p))
		{
			need_quotes = true;
			break;
		}
	}

	if (*src == '\0')
		return pg_strdup("''");

	if (!need_quotes && !need_escaping)
		return pg_strdup(src);	/* no quoting or escaping needed */

	/*
	 * Allocate a buffer large enough for the worst case that all the source
	 * characters need to be escaped, plus quotes.
	 */
	dstbuf = pg_malloc(strlen(src) * 2 + 2 + 1);

	dst = dstbuf;
	if (need_quotes)
		*(dst++) = '\'';
	for (; *src; src++)
	{
		if (*src == '\'' || *src == '\\')
			*(dst++) = '\\';
		*(dst++) = *src;
	}
	if (need_quotes)
		*(dst++) = '\'';
	*dst = '\0';

	return dstbuf;
}

/* Construct a connection string for possible future use in recovery.conf */
char *
pgut_get_conninfo_string(PGconn *conn)
{
	PQconninfoOption *connOptions;
	PQconninfoOption *option;
	PQExpBuffer buf = createPQExpBuffer();
	char	   *connstr;
	bool		firstkeyword = true;
	char	   *escaped;

	connOptions = PQconninfo(conn);
	if (connOptions == NULL)
		elog(ERROR, "out of memory");

	/* Construct a new connection string in key='value' format. */
	for (option = connOptions; option && option->keyword; option++)
	{
		/*
		 * Do not emit this setting if: - the setting is "replication",
		 * "dbname" or "fallback_application_name", since these would be
		 * overridden by the libpqwalreceiver module anyway. - not set or
		 * empty.
		 */
		if (strcmp(option->keyword, "replication") == 0 ||
			strcmp(option->keyword, "dbname") == 0 ||
			strcmp(option->keyword, "fallback_application_name") == 0 ||
			(option->val == NULL) ||
			(option->val != NULL && option->val[0] == '\0'))
			continue;

		/* do not print password, passfile and options into the file */
		if (strcmp(option->keyword, "password") == 0 ||
			strcmp(option->keyword, "passfile") == 0 ||
			strcmp(option->keyword, "options") == 0)
			continue;

		if (!firstkeyword)
			appendPQExpBufferChar(buf, ' ');

		firstkeyword = false;

		escaped = escapeConnectionParameter(option->val);
		appendPQExpBuffer(buf, "%s=%s", option->keyword, escaped);
		free(escaped);
	}

	connstr = pg_strdup(buf->data);
	destroyPQExpBuffer(buf);
	return connstr;
}

/* TODO: it is better to use PQconnectdbParams like in psql
 * It will allow to set application_name for pg_probackup
 */
PGconn *
pgut_connect(const char *host, const char *port,
			 const char *dbname, const char *username)
{
	PGconn	   *conn;

	if (interrupted && !in_cleanup)
		elog(ERROR, "interrupted");

	if (force_password && !prompt_password)
		elog(ERROR, "You cannot specify --password and --no-password options together");

	if (!password && force_password)
		prompt_for_password(username);

	/* Start the connection. Loop until we have a password if requested by backend. */
	for (;;)
	{
		conn = PQsetdbLogin(host, port, NULL, NULL,
							dbname, username, password);

		if (PQstatus(conn) == CONNECTION_OK)
		{
			pthread_lock(&atexit_callback_disconnect_mutex);
			pgut_atexit_push(pgut_disconnect_callback, conn);
			pthread_mutex_unlock(&atexit_callback_disconnect_mutex);
			return conn;
		}

		if (conn && PQconnectionNeedsPassword(conn) && prompt_password)
		{
			PQfinish(conn);
			prompt_for_password(username);

			if (interrupted)
				elog(ERROR, "interrupted");

			if (password == NULL || password[0] == '\0')
				elog(ERROR, "no password supplied");

			continue;
		}
		elog(ERROR, "could not connect to database %s: %s",
			 dbname, PQerrorMessage(conn));

		PQfinish(conn);
		return NULL;
	}
}

PGconn *
pgut_connect_replication(const char *host, const char *port,
						 const char *dbname, const char *username)
{
	PGconn	   *tmpconn;
	int			argcount = 7;	/* dbname, replication, fallback_app_name,
								 * host, user, port, password */
	int			i;
	const char **keywords;
	const char **values;

	if (interrupted && !in_cleanup)
		elog(ERROR, "interrupted");

	if (force_password && !prompt_password)
		elog(ERROR, "You cannot specify --password and --no-password options together");

	if (!password && force_password)
		prompt_for_password(username);

	i = 0;

	keywords = pg_malloc0((argcount + 1) * sizeof(*keywords));
	values = pg_malloc0((argcount + 1) * sizeof(*values));


	keywords[i] = "dbname";
	values[i] = "replication";
	i++;
	keywords[i] = "replication";
	values[i] = "true";
	i++;
	keywords[i] = "fallback_application_name";
	values[i] = PROGRAM_NAME;
	i++;

	if (host)
	{
		keywords[i] = "host";
		values[i] = host;
		i++;
	}
	if (username)
	{
		keywords[i] = "user";
		values[i] = username;
		i++;
	}
	if (port)
	{
		keywords[i] = "port";
		values[i] = port;
		i++;
	}

	/* Use (or reuse, on a subsequent connection) password if we have it */
	if (password)
	{
		keywords[i] = "password";
		values[i] = password;
	}
	else
	{
		keywords[i] = NULL;
		values[i] = NULL;
	}

	for (;;)
	{
		tmpconn = PQconnectdbParams(keywords, values, true);


		if (PQstatus(tmpconn) == CONNECTION_OK)
		{
			free(values);
			free(keywords);
			return tmpconn;
		}

		if (tmpconn && PQconnectionNeedsPassword(tmpconn) && prompt_password)
		{
			PQfinish(tmpconn);
			prompt_for_password(username);
			keywords[i] = "password";
			values[i] = password;
			continue;
		}

		elog(ERROR, "could not connect to database %s: %s",
			 dbname, PQerrorMessage(tmpconn));
		PQfinish(tmpconn);
		free(values);
		free(keywords);
		return NULL;
	}
}


void
pgut_disconnect(PGconn *conn)
{
	if (conn)
		PQfinish(conn);

	pthread_lock(&atexit_callback_disconnect_mutex);
	pgut_atexit_pop(pgut_disconnect_callback, conn);
	pthread_mutex_unlock(&atexit_callback_disconnect_mutex);
}


PGresult *
pgut_execute_parallel(PGconn* conn,
					  PGcancel* thread_cancel_conn, const char *query,
					  int nParams, const char **params,
					  bool text_result, bool ok_error, bool async)
{
	PGresult   *res;

	if (interrupted && !in_cleanup)
		elog(ERROR, "interrupted");

	/* write query to elog if verbose */
	if (logger_config.log_level_console <= VERBOSE ||
		logger_config.log_level_file <= VERBOSE)
	{
		int		i;

		if (strchr(query, '\n'))
			elog(VERBOSE, "(query)\n%s", query);
		else
			elog(VERBOSE, "(query) %s", query);
		for (i = 0; i < nParams; i++)
			elog(VERBOSE, "\t(param:%d) = %s", i, params[i] ? params[i] : "(null)");
	}

	if (conn == NULL)
	{
		elog(ERROR, "not connected");
		return NULL;
	}

	//on_before_exec(conn, thread_cancel_conn);
	if (async)
	{
		/* clean any old data */
		discard_response(conn);

		if (nParams == 0)
			PQsendQuery(conn, query);
		else
			PQsendQueryParams(conn, query, nParams, NULL, params, NULL, NULL,
								/*
								* Specify zero to obtain results in text format,
								* or one to obtain results in binary format.
								*/
								(text_result) ? 0 : 1);

		/* wait for processing, TODO: timeout */
		for (;;)
		{
			if (interrupted)
			{
				pgut_cancel(conn);
				pgut_disconnect(conn);
				elog(ERROR, "interrupted");
			}

			if (!PQconsumeInput(conn))
				elog(ERROR, "query failed: %s query was: %s",
						PQerrorMessage(conn), query);

			/* query is no done */
			if (!PQisBusy(conn))
				break;

			usleep(10000);
		}

		res = PQgetResult(conn);
	}
	else
	{
		if (nParams == 0)
			res = PQexec(conn, query);
		else
			res = PQexecParams(conn, query, nParams, NULL, params, NULL, NULL,
								/*
								* Specify zero to obtain results in text format,
								* or one to obtain results in binary format.
								*/
								(text_result) ? 0 : 1);
	}
	//on_after_exec(thread_cancel_conn);

	switch (PQresultStatus(res))
	{
		case PGRES_TUPLES_OK:
		case PGRES_COMMAND_OK:
		case PGRES_COPY_IN:
			break;
		default:
			if (ok_error && PQresultStatus(res) == PGRES_FATAL_ERROR)
				break;

			elog(ERROR, "query failed: %squery was: %s",
				 PQerrorMessage(conn), query);
			break;
	}

	return res;
}

PGresult *
pgut_execute(PGconn* conn, const char *query, int nParams, const char **params)
{
	return pgut_execute_extended(conn, query, nParams, params, true, false);
}

PGresult *
pgut_execute_extended(PGconn* conn, const char *query, int nParams,
					  const char **params, bool text_result, bool ok_error)
{
	PGresult   *res;
	ExecStatusType res_status;

	if (interrupted && !in_cleanup)
		elog(ERROR, "interrupted");

	/* write query to elog if verbose */
	if (logger_config.log_level_console <= VERBOSE ||
		logger_config.log_level_file <= VERBOSE)
	{
		int		i;

		if (strchr(query, '\n'))
			elog(VERBOSE, "(query)\n%s", query);
		else
			elog(VERBOSE, "(query) %s", query);
		for (i = 0; i < nParams; i++)
			elog(VERBOSE, "\t(param:%d) = %s", i, params[i] ? params[i] : "(null)");
	}

	if (conn == NULL)
	{
		elog(ERROR, "not connected");
		return NULL;
	}

	on_before_exec(conn, NULL);
	if (nParams == 0)
		res = PQexec(conn, query);
	else
		res = PQexecParams(conn, query, nParams, NULL, params, NULL, NULL,
						   /*
							* Specify zero to obtain results in text format,
							* or one to obtain results in binary format.
							*/
						   (text_result) ? 0 : 1);
	on_after_exec(NULL);

	res_status = PQresultStatus(res);
	switch (res_status)
	{
		case PGRES_TUPLES_OK:
		case PGRES_COMMAND_OK:
		case PGRES_COPY_IN:
			break;
		default:
			if (ok_error && res_status == PGRES_FATAL_ERROR)
				break;

			elog(ERROR, "query failed: %squery was: %s",
				 PQerrorMessage(conn), query);
			break;
	}

	return res;
}

bool
pgut_send(PGconn* conn, const char *query, int nParams, const char **params, int elevel)
{
	int			res;

	if (interrupted && !in_cleanup)
		elog(ERROR, "interrupted");

	/* write query to elog if verbose */
	if (logger_config.log_level_console <= VERBOSE ||
		logger_config.log_level_file <= VERBOSE)
	{
		int		i;

		if (strchr(query, '\n'))
			elog(VERBOSE, "(query)\n%s", query);
		else
			elog(VERBOSE, "(query) %s", query);
		for (i = 0; i < nParams; i++)
			elog(VERBOSE, "\t(param:%d) = %s", i, params[i] ? params[i] : "(null)");
	}

	if (conn == NULL)
	{
		elog(elevel, "not connected");
		return false;
	}

	if (nParams == 0)
		res = PQsendQuery(conn, query);
	else
		res = PQsendQueryParams(conn, query, nParams, NULL, params, NULL, NULL, 0);

	if (res != 1)
	{
		elog(elevel, "query failed: %squery was: %s",
			PQerrorMessage(conn), query);
		return false;
	}

	return true;
}

void
pgut_cancel(PGconn* conn)
{
	PGcancel *cancel_conn = PQgetCancel(conn);
	char		errbuf[256];

	if (cancel_conn != NULL)
	{
		if (PQcancel(cancel_conn, errbuf, sizeof(errbuf)))
			elog(WARNING, "Cancel request sent");
		else
			elog(WARNING, "Cancel request failed");
	}

	if (cancel_conn)
		PQfreeCancel(cancel_conn);
}

int
pgut_wait(int num, PGconn *connections[], struct timeval *timeout)
{
	/* all connections are busy. wait for finish */
	while (!interrupted)
	{
		int		i;
		fd_set	mask;
		int		maxsock;

		FD_ZERO(&mask);

		maxsock = -1;
		for (i = 0; i < num; i++)
		{
			int	sock;

			if (connections[i] == NULL)
				continue;
			sock = PQsocket(connections[i]);
			if (sock >= 0)
			{
				FD_SET(sock, &mask);
				if (maxsock < sock)
					maxsock = sock;
			}
		}

		if (maxsock == -1)
		{
			errno = ENOENT;
			return -1;
		}

		i = wait_for_sockets(maxsock + 1, &mask, timeout);
		if (i == 0)
			break;	/* timeout */

		for (i = 0; i < num; i++)
		{
			if (connections[i] && FD_ISSET(PQsocket(connections[i]), &mask))
			{
				PQconsumeInput(connections[i]);
				if (PQisBusy(connections[i]))
					continue;
				return i;
			}
		}
	}

	errno = EINTR;
	return -1;
}

#ifdef WIN32
static CRITICAL_SECTION cancelConnLock;
#endif

/*
 * on_before_exec
 *
 * Set cancel_conn to point to the current database connection.
 */
static void
on_before_exec(PGconn *conn, PGcancel *thread_cancel_conn)
{
	PGcancel   *old;

	if (in_cleanup)
		return;	/* forbid cancel during cleanup */

#ifdef WIN32
	EnterCriticalSection(&cancelConnLock);
#endif

	if (thread_cancel_conn)
	{
		//elog(WARNING, "Handle tread_cancel_conn. on_before_exec");
		old = thread_cancel_conn;

		/* be sure handle_interrupt doesn't use pointer while freeing */
		thread_cancel_conn = NULL;

		if (old != NULL)
			PQfreeCancel(old);

		thread_cancel_conn = PQgetCancel(conn);
	}
	else
	{
		/* Free the old one if we have one */
		old = cancel_conn;

		/* be sure handle_interrupt doesn't use pointer while freeing */
		cancel_conn = NULL;

		if (old != NULL)
			PQfreeCancel(old);

		cancel_conn = PQgetCancel(conn);
	}

#ifdef WIN32
	LeaveCriticalSection(&cancelConnLock);
#endif
}

/*
 * on_after_exec
 *
 * Free the current cancel connection, if any, and set to NULL.
 */
static void
on_after_exec(PGcancel *thread_cancel_conn)
{
	PGcancel   *old;

	if (in_cleanup)
		return;	/* forbid cancel during cleanup */

#ifdef WIN32
	EnterCriticalSection(&cancelConnLock);
#endif

	if (thread_cancel_conn)
	{
		//elog(WARNING, "Handle tread_cancel_conn. on_after_exec");
		old = thread_cancel_conn;

		/* be sure handle_interrupt doesn't use pointer while freeing */
		thread_cancel_conn = NULL;

		if (old != NULL)
			PQfreeCancel(old);
	}
	else
	{
		old = cancel_conn;

		/* be sure handle_interrupt doesn't use pointer while freeing */
		cancel_conn = NULL;

		if (old != NULL)
			PQfreeCancel(old);
	}
#ifdef WIN32
	LeaveCriticalSection(&cancelConnLock);
#endif
}

/*
 * Handle interrupt signals by cancelling the current command.
 */
static void
on_interrupt(void)
{
	int			save_errno = errno;
	char		errbuf[256];

	/* Set interrupted flag */
	interrupted = true;

	/*
	 * User prompts password, call on_cleanup() byhand. Unless we do that we will
	 * get stuck forever until a user enters a password.
	 */
	if (in_password)
	{
		on_cleanup();

		pqsignal(SIGINT, oldhandler);
		kill(0, SIGINT);
	}

	/* Send QueryCancel if we are processing a database query */
	if (!in_cleanup && cancel_conn != NULL &&
		PQcancel(cancel_conn, errbuf, sizeof(errbuf)))
	{
		elog(WARNING, "Cancel request sent");
	}

	errno = save_errno;			/* just in case the write changed it */
}

typedef struct pgut_atexit_item pgut_atexit_item;
struct pgut_atexit_item
{
	pgut_atexit_callback	callback;
	void				   *userdata;
	pgut_atexit_item	   *next;
};

static pgut_atexit_item *pgut_atexit_stack = NULL;

void
pgut_disconnect_callback(bool fatal, void *userdata)
{
	PGconn *conn = (PGconn *) userdata;
	if (conn)
		pgut_disconnect(conn);
}

void
pgut_atexit_push(pgut_atexit_callback callback, void *userdata)
{
	pgut_atexit_item *item;

	AssertArg(callback != NULL);

	item = pgut_new(pgut_atexit_item);
	item->callback = callback;
	item->userdata = userdata;
	item->next = pgut_atexit_stack;

	pgut_atexit_stack = item;
}

void
pgut_atexit_pop(pgut_atexit_callback callback, void *userdata)
{
	pgut_atexit_item  *item;
	pgut_atexit_item **prev;

	for (item = pgut_atexit_stack, prev = &pgut_atexit_stack;
		 item;
		 prev = &item->next, item = item->next)
	{
		if (item->callback == callback && item->userdata == userdata)
		{
			*prev = item->next;
			free(item);
			break;
		}
	}
}

static void
call_atexit_callbacks(bool fatal)
{
	pgut_atexit_item  *item;
	pgut_atexit_item  *next;

	for (item = pgut_atexit_stack; item; item = next)
	{
		next = item->next;
		item->callback(fatal, item->userdata);
	}
}

static void
on_cleanup(void)
{
	in_cleanup = true;
	interrupted = false;
	call_atexit_callbacks(false);
}

void *
pgut_malloc(size_t size)
{
	char *ret;

	if ((ret = malloc(size)) == NULL)
		elog(ERROR, "could not allocate memory (%lu bytes): %s",
			(unsigned long) size, strerror(errno));
	return ret;
}

void *
pgut_realloc(void *p, size_t size)
{
	char *ret;

	if ((ret = realloc(p, size)) == NULL)
		elog(ERROR, "could not re-allocate memory (%lu bytes): %s",
			(unsigned long) size, strerror(errno));
	return ret;
}

char *
pgut_strdup(const char *str)
{
	char *ret;

	if (str == NULL)
		return NULL;

	if ((ret = strdup(str)) == NULL)
		elog(ERROR, "could not duplicate string \"%s\": %s",
			str, strerror(errno));
	return ret;
}

FILE *
pgut_fopen(const char *path, const char *mode, bool missing_ok)
{
	FILE *fp;

	if ((fp = fio_open_stream(path, FIO_BACKUP_HOST)) == NULL)
	{
		if (missing_ok && errno == ENOENT)
			return NULL;

		elog(ERROR, "could not open file \"%s\": %s",
			path, strerror(errno));
	}

	return fp;
}

#ifdef WIN32
static int select_win32(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, const struct timeval * timeout);
#define select		select_win32
#endif

int
wait_for_socket(int sock, struct timeval *timeout)
{
	fd_set		fds;

	FD_ZERO(&fds);
	FD_SET(sock, &fds);
	return wait_for_sockets(sock + 1, &fds, timeout);
}

int
wait_for_sockets(int nfds, fd_set *fds, struct timeval *timeout)
{
	int		i;

	for (;;)
	{
		i = select(nfds, fds, NULL, NULL, timeout);
		if (i < 0)
		{
			if (interrupted)
				elog(ERROR, "interrupted");
			else if (errno != EINTR)
				elog(ERROR, "select failed: %s", strerror(errno));
		}
		else
			return i;
	}
}

#ifndef WIN32
static void
handle_interrupt(SIGNAL_ARGS)
{
	on_interrupt();
}

/* Handle various inrerruptions in the same way */
static void
init_cancel_handler(void)
{
	oldhandler = pqsignal(SIGINT, handle_interrupt);
	pqsignal(SIGQUIT, handle_interrupt);
	pqsignal(SIGTERM, handle_interrupt);
}
#else							/* WIN32 */

/*
 * Console control handler for Win32. Note that the control handler will
 * execute on a *different thread* than the main one, so we need to do
 * proper locking around those structures.
 */
static BOOL WINAPI
consoleHandler(DWORD dwCtrlType)
{
	if (dwCtrlType == CTRL_C_EVENT ||
		dwCtrlType == CTRL_BREAK_EVENT)
	{
		EnterCriticalSection(&cancelConnLock);
		on_interrupt();
		LeaveCriticalSection(&cancelConnLock);
		return TRUE;
	}
	else
		/* Return FALSE for any signals not being handled */
		return FALSE;
}

static void
init_cancel_handler(void)
{
	InitializeCriticalSection(&cancelConnLock);

	SetConsoleCtrlHandler(consoleHandler, TRUE);
}

int
sleep(unsigned int seconds)
{
	Sleep(seconds * 1000);
	return 0;
}

int
usleep(unsigned int usec)
{
	Sleep((usec + 999) / 1000);	/* rounded up */
	return 0;
}

#undef select
static int
select_win32(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, const struct timeval * timeout)
{
	struct timeval	remain;

	if (timeout != NULL)
		remain = *timeout;
	else
	{
		remain.tv_usec = 0;
		remain.tv_sec = LONG_MAX;	/* infinite */
	}

	/* sleep only one second because Ctrl+C doesn't interrupt select. */
	while (remain.tv_sec > 0 || remain.tv_usec > 0)
	{
		int				ret;
		struct timeval	onesec;

		if (remain.tv_sec > 0)
		{
			onesec.tv_sec = 1;
			onesec.tv_usec = 0;
			remain.tv_sec -= 1;
		}
		else
		{
			onesec.tv_sec = 0;
			onesec.tv_usec = remain.tv_usec;
			remain.tv_usec = 0;
		}

		ret = select(nfds, readfds, writefds, exceptfds, &onesec);
		if (ret != 0)
		{
			/* succeeded or error */
			return ret;
		}
		else if (interrupted)
		{
			errno = EINTR;
			return 0;
		}
	}

	return 0;	/* timeout */
}

#endif   /* WIN32 */

void
discard_response(PGconn *conn)
{
	PGresult   *res;

	do
	{
		res = PQgetResult(conn);
		if (res)
			PQclear(res);
	} while (res);
}

/*
 * pgfnames
 *
 * return a list of the names of objects in the argument directory.  Caller
 * must call pgfnames_cleanup later to free the memory allocated by this
 * function.
 */
char **
pgut_pgfnames(const char *path, bool strict)
{
	DIR		   *dir;
	struct dirent *file;
	char	  **filenames;
	int			numnames = 0;
	int			fnsize = 200;	/* enough for many small dbs */

	dir = opendir(path);
	if (dir == NULL)
	{
		elog(strict ? ERROR : WARNING, "could not open directory \"%s\": %m", path);
		return NULL;
	}

	filenames = (char **) palloc(fnsize * sizeof(char *));

	while (errno = 0, (file = readdir(dir)) != NULL)
	{
		if (strcmp(file->d_name, ".") != 0 && strcmp(file->d_name, "..") != 0)
		{
			if (numnames + 1 >= fnsize)
			{
				fnsize *= 2;
				filenames = (char **) repalloc(filenames,
											   fnsize * sizeof(char *));
			}
			filenames[numnames++] = pstrdup(file->d_name);
		}
	}

	if (errno)
	{
		elog(strict ? ERROR : WARNING, "could not read directory \"%s\": %m", path);
		return NULL;
	}

	filenames[numnames] = NULL;

	if (closedir(dir))
	{
		elog(strict ? ERROR : WARNING, "could not close directory \"%s\": %m", path);
		return NULL;
	}

	return filenames;
}

/*
 *	pgfnames_cleanup
 *
 *	deallocate memory used for filenames
 */
void
pgut_pgfnames_cleanup(char **filenames)
{
	char	  **fn;

	for (fn = filenames; *fn; fn++)
		pfree(*fn);

	pfree(filenames);
}

/* Shamelessly stolen from commom/rmtree.c */
bool
pgut_rmtree(const char *path, bool rmtopdir, bool strict)
{
	bool		result = true;
	char		pathbuf[MAXPGPATH];
	char	  **filenames;
	char	  **filename;
	struct stat statbuf;

	/*
	 * we copy all the names out of the directory before we start modifying
	 * it.
	 */
	filenames = pgut_pgfnames(path, strict);

	if (filenames == NULL)
		return false;

	/* now we have the names we can start removing things */
	for (filename = filenames; *filename; filename++)
	{
		snprintf(pathbuf, MAXPGPATH, "%s/%s", path, *filename);

		if (lstat(pathbuf, &statbuf) != 0)
		{
			elog(strict ? ERROR : WARNING, "could not stat file or directory \"%s\": %m", pathbuf);
			result = false;
			break;
		}

		if (S_ISDIR(statbuf.st_mode))
		{
			/* call ourselves recursively for a directory */
			if (!pgut_rmtree(pathbuf, true, strict))
			{
				result = false;
				break;
			}
		}
		else
		{
			if (unlink(pathbuf) != 0)
			{
				elog(strict ? ERROR : WARNING, "could not remove file or directory \"%s\": %m", pathbuf);
				result = false;
				break;
			}
		}
	}

	if (rmtopdir)
	{
		if (rmdir(path) != 0)
		{
			elog(strict ? ERROR : WARNING, "could not remove file or directory \"%s\": %m", path);
			result = false;
		}
	}

	pgut_pgfnames_cleanup(filenames);

	return result;
}
