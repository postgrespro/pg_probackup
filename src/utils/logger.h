/*-------------------------------------------------------------------------
 *
 * logger.h: - prototypes of logger functions.
 *
 * Copyright (c) 2017-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOGGER_H
#define LOGGER_H

#include "postgres_fe.h"

#define LOG_NONE	(-10)

/* Log level */
#define VERBOSE		(-5)
#define LOG			(-4)
#define INFO		(-3)
#define NOTICE		(-2)
#define WARNING		(-1)
#define ERROR		1
#define FATAL		2
#define PANIC		3
#define LOG_OFF		10

/* Logger parameters */

extern int			log_to_file;
extern int			log_level_console;
extern int			log_level_file;

extern char		   *log_filename;
extern char		   *error_log_filename;
extern char		   *log_directory;
extern char			log_path[MAXPGPATH];

extern int			log_rotation_size;
extern int			log_rotation_age;

#define LOG_LEVEL_CONSOLE ((log_level_console == LOG_NONE) ? INFO : log_level_console)
#define LOG_LEVEL_FILE ((log_level_file == LOG_NONE) ? LOG_OFF : log_level_file)

#undef elog
extern void elog(int elevel, const char *fmt, ...) pg_attribute_printf(2, 3);

extern void init_logger(const char *root_path);

extern int parse_log_level(const char *level);
extern const char *deparse_log_level(int level);

#endif   /* LOGGER_H */
