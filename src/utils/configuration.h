/*-------------------------------------------------------------------------
 *
 * configuration.h: - prototypes of functions and structures for
 * configuration.
 *
 * Copyright (c) 2018-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#ifndef CONFIGURATION_H
#define CONFIGURATION_H

#include "postgres_fe.h"
#include "access/xlogdefs.h"

#define INFINITE_STR		"INFINITE"

typedef enum OptionSource
{
	SOURCE_DEFAULT,
	SOURCE_FILE_STRICT,
	SOURCE_CMD_STRICT,
	SOURCE_ENV,
	SOURCE_FILE,
	SOURCE_CMD,
	SOURCE_CONST
} OptionSource;

typedef struct ConfigOption ConfigOption;

typedef void (*option_assign_fn) (ConfigOption *opt, const char *arg);
/* Returns allocated string value */
typedef char *(*option_get_fn) (ConfigOption *opt);

/*
 * type:
 *	b: bool (true)
 *	B: bool (false)
 *  f: option_fn
 *	i: 32bit signed integer
 *	u: 32bit unsigned integer
 *	I: 64bit signed integer
 *	U: 64bit unsigned integer
 *	s: string
 *  t: time_t
 */
struct ConfigOption
{
	char		type;
	uint8		sname;			/* short name */
	const char *lname;			/* long name */
	void	   *var;			/* pointer to variable */
	OptionSource allowed;		/* allowed source */
	OptionSource source;		/* actual source */
	const char *group;			/* option group name */
	int			flags;			/* option unit */
	option_get_fn get_value;	/* function to get the value as a string,
								   should return allocated string*/
};

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

extern int config_get_opt(int argc, char **argv, ConfigOption cmd_options[],
						  ConfigOption options[]);
extern int config_read_opt(const char *path, ConfigOption options[], int elevel,
						   bool strict, bool missing_ok);
extern void config_get_opt_env(ConfigOption options[]);
extern void config_set_opt(ConfigOption options[], void *var,
						   OptionSource source);

extern char *option_get_value(ConfigOption *opt);

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

extern void time2iso(char *buf, size_t len, time_t time, bool utc);

extern void convert_from_base_unit(int64 base_value, int base_unit,
								   int64 *value, const char **unit);
extern void convert_from_base_unit_u(uint64 base_value, int base_unit,
									 uint64 *value, const char **unit);

#endif   /* CONFIGURATION_H */
