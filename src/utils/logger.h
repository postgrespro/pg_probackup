/*-------------------------------------------------------------------------
 *
 * logger.h: - prototypes of logger functions.
 *
 * Copyright (c) 2017-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOGGER_H
#define LOGGER_H

#define LOG_NONE	(-10)

/* Log level */
#define VERBOSE		(-5)
#define LOG			(-4)
#define INFO		(-3)
#define NOTICE		(-2)
#define WARNING		(-1)
#define ERROR		1
#define LOG_OFF		10

typedef struct LoggerConfig
{
	int			log_level_console;
	int			log_level_file;
	char	   *log_filename;
	char	   *error_log_filename;
	char	   *log_directory;
	/* Maximum size of an individual log file in kilobytes */
	uint64		log_rotation_size;
	/* Maximum lifetime of an individual log file in minutes */
	uint64		log_rotation_age;
} LoggerConfig;

/* Logger parameters */
extern LoggerConfig logger_config;

#define LOG_ROTATION_SIZE_DEFAULT	0
#define LOG_ROTATION_AGE_DEFAULT	0

#define LOG_LEVEL_CONSOLE_DEFAULT	INFO
#define LOG_LEVEL_FILE_DEFAULT		LOG_OFF

#define LOG_FILENAME_DEFAULT		"pg_probackup.log"
#define LOG_DIRECTORY_DEFAULT		"log"

#undef elog
extern void elog(int elevel, const char *fmt, ...) pg_attribute_printf(2, 3);
extern void elog_file(int elevel, const char *fmt, ...) pg_attribute_printf(2, 3);
extern void elog_ft_log(enum FT_LOG_LEVEL, ft_source_position_t srcpos,
		const char* error, const char *fmt, va_list args)
		pg_attribute_printf(4, 0);

extern void init_logger(const char *root_path, LoggerConfig *config);
extern void init_console(void);

extern int parse_log_level(const char *level);
extern const char *deparse_log_level(int level);

#endif   /* LOGGER_H */
