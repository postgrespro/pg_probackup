/*-------------------------------------------------------------------------
 *
 * pgut.h
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2017-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGUT_H
#define PGUT_H

#include "libpq-fe.h"
#include "pqexpbuffer.h"

#include <assert.h>
#include <sys/time.h>

#include "access/xlogdefs.h"
#include "logger.h"

#if !defined(C_H) && !defined(__cplusplus)
#ifndef bool
typedef char bool;
#endif
#ifndef true
#define true	((bool) 1)
#endif
#ifndef false
#define false	((bool) 0)
#endif
#endif

#define INFINITE_STR		"INFINITE"

typedef enum pgut_optsrc
{
	SOURCE_DEFAULT,
	SOURCE_FILE_STRICT,
	SOURCE_ENV,
	SOURCE_FILE,
	SOURCE_CMDLINE,
	SOURCE_CONST
} pgut_optsrc;

/*
 * type:
 *	b: bool (true)
 *	B: bool (false)
 *  f: pgut_optfn
 *	i: 32bit signed integer
 *	u: 32bit unsigned integer
 *	I: 64bit signed integer
 *	U: 64bit unsigned integer
 *	s: string
 *  t: time_t
 */
typedef struct pgut_option
{
	char		type;
	uint8		sname;		/* short name */
	const char *lname;		/* long name */
	void	   *var;		/* pointer to variable */
	pgut_optsrc	allowed;	/* allowed source */
	pgut_optsrc	source;		/* actual source */
	int			flags;		/* option unit */
} pgut_option;

typedef void (*pgut_optfn) (pgut_option *opt, const char *arg);
typedef void (*pgut_atexit_callback)(bool fatal, void *userdata);

/*
 * bit values in "flags" of an option
 */
#define OPTION_UNIT_KB				0x1000	/* value is in kilobytes */
#define OPTION_UNIT_BLOCKS			0x2000	/* value is in blocks */
#define OPTION_UNIT_XBLOCKS			0x3000	/* value is in xlog blocks */
#define OPTION_UNIT_XSEGS			0x4000	/* value is in xlog segments */
#define OPTION_UNIT_MEMORY			0xF000	/* mask for size-related units */

#define OPTION_UNIT_MS				0x10000	/* value is in milliseconds */
#define OPTION_UNIT_S				0x20000	/* value is in seconds */
#define OPTION_UNIT_MIN				0x30000	/* value is in minutes */
#define OPTION_UNIT_TIME			0xF0000	/* mask for time-related units */

#define OPTION_UNIT					(OPTION_UNIT_MEMORY | OPTION_UNIT_TIME)

/*
 * pgut client variables and functions
 */
extern const char  *PROGRAM_NAME;
extern const char  *PROGRAM_VERSION;
extern const char  *PROGRAM_URL;
extern const char  *PROGRAM_EMAIL;

extern void	pgut_help(bool details);

/*
 * pgut framework variables and functions
 */
extern const char  *pgut_dbname;
extern const char  *host;
extern const char  *port;
extern const char  *username;
extern bool			prompt_password;
extern bool			force_password;

extern bool			interrupted;
extern bool			in_cleanup;
extern bool			in_password;	/* User prompts password */

extern int pgut_getopt(int argc, char **argv, pgut_option options[]);
extern void pgut_readopt(const char *path, pgut_option options[], int elevel);
extern void pgut_getopt_env(pgut_option options[]);
extern void pgut_atexit_push(pgut_atexit_callback callback, void *userdata);
extern void pgut_atexit_pop(pgut_atexit_callback callback, void *userdata);

/*
 * Database connections
 */
extern char *pgut_get_conninfo_string(PGconn *conn);
extern PGconn *pgut_connect(const char *dbname);
extern PGconn *pgut_connect_extended(const char *pghost, const char *pgport,
									 const char *dbname, const char *login);
extern PGconn *pgut_connect_replication(const char *dbname);
extern PGconn *pgut_connect_replication_extended(const char *pghost, const char *pgport,
									 const char *dbname, const char *pguser);
extern void pgut_disconnect(PGconn *conn);
extern PGresult *pgut_execute(PGconn* conn, const char *query, int nParams,
							  const char **params);
extern PGresult *pgut_execute_extended(PGconn* conn, const char *query, int nParams,
							  const char **params, bool text_result, bool ok_error);
extern PGresult *pgut_execute_parallel(PGconn* conn, PGcancel* thread_cancel_conn, 
							  const char *query, int nParams,
							  const char **params, bool text_result);
extern bool pgut_send(PGconn* conn, const char *query, int nParams, const char **params, int elevel);
extern void pgut_cancel(PGconn* conn);
extern int pgut_wait(int num, PGconn *connections[], struct timeval *timeout);

extern const char *pgut_get_host(void);
extern const char *pgut_get_port(void);
extern void pgut_set_host(const char *new_host);
extern void pgut_set_port(const char *new_port);

/*
 * memory allocators
 */
extern void *pgut_malloc(size_t size);
extern void *pgut_realloc(void *p, size_t size);
extern char *pgut_strdup(const char *str);
extern char *strdup_with_len(const char *str, size_t len);
extern char *strdup_trim(const char *str);

#define pgut_new(type)			((type *) pgut_malloc(sizeof(type)))
#define pgut_newarray(type, n)	((type *) pgut_malloc(sizeof(type) * (n)))

/*
 * file operations
 */
extern FILE *pgut_fopen(const char *path, const char *mode, bool missing_ok);

/*
 * Assert
 */
#undef Assert
#undef AssertArg
#undef AssertMacro

#ifdef USE_ASSERT_CHECKING
#define Assert(x)		assert(x)
#define AssertArg(x)	assert(x)
#define AssertMacro(x)	assert(x)
#else
#define Assert(x)		((void) 0)
#define AssertArg(x)	((void) 0)
#define AssertMacro(x)	((void) 0)
#endif

/*
 * StringInfo and string operations
 */
#define STRINGINFO_H

#define StringInfoData			PQExpBufferData
#define StringInfo				PQExpBuffer
#define makeStringInfo			createPQExpBuffer
#define initStringInfo			initPQExpBuffer
#define freeStringInfo			destroyPQExpBuffer
#define termStringInfo			termPQExpBuffer
#define resetStringInfo			resetPQExpBuffer
#define enlargeStringInfo		enlargePQExpBuffer
#define printfStringInfo		printfPQExpBuffer	/* reset + append */
#define appendStringInfo		appendPQExpBuffer
#define appendStringInfoString	appendPQExpBufferStr
#define appendStringInfoChar	appendPQExpBufferChar
#define appendBinaryStringInfo	appendBinaryPQExpBuffer

extern int appendStringInfoFile(StringInfo str, FILE *fp);
extern int appendStringInfoFd(StringInfo str, int fd);

extern bool parse_bool(const char *value, bool *result);
extern bool parse_bool_with_len(const char *value, size_t len, bool *result);
extern bool parse_int32(const char *value, int32 *result, int flags);
extern bool parse_uint32(const char *value, uint32 *result, int flags);
extern bool parse_int64(const char *value, int64 *result, int flags);
extern bool parse_uint64(const char *value, uint64 *result, int flags);
extern bool parse_time(const char *value, time_t *result, bool utc_default);
extern bool parse_int(const char *value, int *result, int flags,
					  const char **hintmsg);
extern bool parse_lsn(const char *value, XLogRecPtr *result);

extern void convert_from_base_unit(int64 base_value, int base_unit,
								   int64 *value, const char **unit);
extern void convert_from_base_unit_u(uint64 base_value, int base_unit,
									 uint64 *value, const char **unit);

#define IsSpace(c)		(isspace((unsigned char)(c)))
#define IsAlpha(c)		(isalpha((unsigned char)(c)))
#define IsAlnum(c)		(isalnum((unsigned char)(c)))
#define IsIdentHead(c)	(IsAlpha(c) || (c) == '_')
#define IsIdentBody(c)	(IsAlnum(c) || (c) == '_')
#define ToLower(c)		(tolower((unsigned char)(c)))
#define ToUpper(c)		(toupper((unsigned char)(c)))

/*
 * socket operations
 */
extern int wait_for_socket(int sock, struct timeval *timeout);
extern int wait_for_sockets(int nfds, fd_set *fds, struct timeval *timeout);

#ifdef WIN32
extern int sleep(unsigned int seconds);
extern int usleep(unsigned int usec);
#endif

#endif   /* PGUT_H */
