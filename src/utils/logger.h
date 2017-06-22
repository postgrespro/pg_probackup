/*-------------------------------------------------------------------------
 *
 * logger.h: - prototypes of logger functions.
 *
 * Portions Copyright (c) 2017-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOGGER_H
#define LOGGER_H

#include "postgres_fe.h"

/* Log level */
#define VERBOSE		(-5)
#define LOG			(-4)
#define INFO		(-3)
#define NOTICE		(-2)
#define WARNING		(-1)
#define ERROR		1
#define FATAL		2
#define PANIC		3

/* Logger parameters */

extern int			log_level;
extern bool			log_level_defined;

extern char		   *log_filename;
extern char		   *error_log_filename;
extern char		   *log_directory;
extern char			log_path[MAXPGPATH];

extern int			log_rotation_size;
extern int			log_rotation_age;

#undef elog
extern void elog(int elevel, const char *fmt, ...) pg_attribute_printf(2, 3);

extern int parse_log_level(const char *level);
extern const char *deparse_log_level(int level);

#endif   /* LOGGER_H */
