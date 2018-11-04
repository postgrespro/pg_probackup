/*-------------------------------------------------------------------------
 *
 * pgut.c
 *
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2017-2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "getopt_long.h"
#include "libpq-fe.h"
#include "libpq/pqsignal.h"
#include "pqexpbuffer.h"

#include <time.h>

#include "pgut.h"
#include "logger.h"
#include "file.h"

#define MAX_TZDISP_HOUR		15	/* maximum allowed hour part */
#define SECS_PER_MINUTE		60
#define MINS_PER_HOUR		60
#define MAXPG_LSNCOMPONENT	8

const char *PROGRAM_NAME = NULL;

const char	   *pgut_dbname = NULL;
const char	   *host = NULL;
const char	   *port = NULL;
const char	   *username = NULL;
static char	   *password = NULL;
bool			prompt_password = true;
bool			force_password = false;

/* Database connections */
static PGcancel *volatile cancel_conn = NULL;

/* Interrupted by SIGINT (Ctrl+C) ? */
bool		interrupted = false;
bool		in_cleanup = false;
bool		in_password = false;

static bool parse_pair(const char buffer[], char key[], char value[]);

/* Connection routines */
static void init_cancel_handler(void);
static void on_before_exec(PGconn *conn, PGcancel *thread_cancel_conn);
static void on_after_exec(PGcancel *thread_cancel_conn);
static void on_interrupt(void);
static void on_cleanup(void);
static void exit_or_abort(int exitcode);
static const char *get_username(void);
static pqsigfunc oldhandler = NULL;

/*
 * Unit conversion tables.
 *
 * Copied from guc.c.
 */
#define MAX_UNIT_LEN		3	/* length of longest recognized unit string */

typedef struct
{
	char		unit[MAX_UNIT_LEN + 1]; /* unit, as a string, like "kB" or
										 * "min" */
	int			base_unit;		/* OPTION_UNIT_XXX */
	int			multiplier;		/* If positive, multiply the value with this
								 * for unit -> base_unit conversion.  If
								 * negative, divide (with the absolute value) */
} unit_conversion;

static const char *memory_units_hint = "Valid units for this parameter are \"kB\", \"MB\", \"GB\", and \"TB\".";

static const unit_conversion memory_unit_conversion_table[] =
{
	{"TB", OPTION_UNIT_KB, 1024 * 1024 * 1024},
	{"GB", OPTION_UNIT_KB, 1024 * 1024},
	{"MB", OPTION_UNIT_KB, 1024},
	{"KB", OPTION_UNIT_KB, 1},
	{"kB", OPTION_UNIT_KB, 1},

	{"TB", OPTION_UNIT_BLOCKS, (1024 * 1024 * 1024) / (BLCKSZ / 1024)},
	{"GB", OPTION_UNIT_BLOCKS, (1024 * 1024) / (BLCKSZ / 1024)},
	{"MB", OPTION_UNIT_BLOCKS, 1024 / (BLCKSZ / 1024)},
	{"kB", OPTION_UNIT_BLOCKS, -(BLCKSZ / 1024)},

	{"TB", OPTION_UNIT_XBLOCKS, (1024 * 1024 * 1024) / (XLOG_BLCKSZ / 1024)},
	{"GB", OPTION_UNIT_XBLOCKS, (1024 * 1024) / (XLOG_BLCKSZ / 1024)},
	{"MB", OPTION_UNIT_XBLOCKS, 1024 / (XLOG_BLCKSZ / 1024)},
	{"kB", OPTION_UNIT_XBLOCKS, -(XLOG_BLCKSZ / 1024)},

	{""}						/* end of table marker */
};

static const char *time_units_hint = "Valid units for this parameter are \"ms\", \"s\", \"min\", \"h\", and \"d\".";

static const unit_conversion time_unit_conversion_table[] =
{
	{"d", OPTION_UNIT_MS, 1000 * 60 * 60 * 24},
	{"h", OPTION_UNIT_MS, 1000 * 60 * 60},
	{"min", OPTION_UNIT_MS, 1000 * 60},
	{"s", OPTION_UNIT_MS, 1000},
	{"ms", OPTION_UNIT_MS, 1},

	{"d", OPTION_UNIT_S, 60 * 60 * 24},
	{"h", OPTION_UNIT_S, 60 * 60},
	{"min", OPTION_UNIT_S, 60},
	{"s", OPTION_UNIT_S, 1},
	{"ms", OPTION_UNIT_S, -1000},

	{"d", OPTION_UNIT_MIN, 60 * 24},
	{"h", OPTION_UNIT_MIN, 60},
	{"min", OPTION_UNIT_MIN, 1},
	{"s", OPTION_UNIT_MIN, -60},
	{"ms", OPTION_UNIT_MIN, -1000 * 60},

	{""}						/* end of table marker */
};

static size_t
option_length(const pgut_option opts[])
{
	size_t		len;

	for (len = 0; opts && opts[len].type; len++) { }

	return len;
}

static int
option_has_arg(char type)
{
	switch (type)
	{
		case 'b':
		case 'B':
			return no_argument;
		default:
			return required_argument;
	}
}

static void
option_copy(struct option dst[], const pgut_option opts[], size_t len)
{
	size_t		i;

	for (i = 0; i < len; i++)
	{
		dst[i].name = opts[i].lname;
		dst[i].has_arg = option_has_arg(opts[i].type);
		dst[i].flag = NULL;
		dst[i].val = opts[i].sname;
	}
}

static pgut_option *
option_find(int c, pgut_option opts1[])
{
	size_t	i;

	for (i = 0; opts1 && opts1[i].type; i++)
		if (opts1[i].sname == c)
			return &opts1[i];

	return NULL;	/* not found */
}

static void
assign_option(pgut_option *opt, const char *optarg, pgut_optsrc src)
{
	const char *message;

	if (opt == NULL)
	{
		fprintf(stderr, "Try \"%s --help\" for more information.\n", PROGRAM_NAME);
		exit_or_abort(ERROR);
	}

	if (opt->source > src)
	{
		/* high prior value has been set already. */
		return;
	}
	/* Allow duplicate entries for function option */
	else if (src >= SOURCE_CMDLINE && opt->source >= src && opt->type != 'f')
	{
		message = "specified only once";
	}
	else
	{
		pgut_optsrc	orig_source = opt->source;

		/* can be overwritten if non-command line source */
		opt->source = src;

		switch (opt->type)
		{
			case 'b':
			case 'B':
				if (optarg == NULL)
				{
					*((bool *) opt->var) = (opt->type == 'b');
					return;
				}
				else if (parse_bool(optarg, (bool *) opt->var))
				{
					return;
				}
				message = "a boolean";
				break;
			case 'f':
				((pgut_optfn) opt->var)(opt, optarg);
				return;
			case 'i':
				if (parse_int32(optarg, opt->var, opt->flags))
					return;
				message = "a 32bit signed integer";
				break;
			case 'u':
				if (parse_uint32(optarg, opt->var, opt->flags))
					return;
				message = "a 32bit unsigned integer";
				break;
			case 'I':
				if (parse_int64(optarg, opt->var, opt->flags))
					return;
				message = "a 64bit signed integer";
				break;
			case 'U':
				if (parse_uint64(optarg, opt->var, opt->flags))
					return;
				message = "a 64bit unsigned integer";
				break;
			case 's':
				if (orig_source != SOURCE_DEFAULT)
					free(*(char **) opt->var);
				*(char **) opt->var = pgut_strdup(optarg);
				if (strcmp(optarg,"") != 0)
					return;
				message = "a valid string";
				break;
			case 't':
				if (parse_time(optarg, opt->var,
							   opt->source == SOURCE_FILE))
					return;
				message = "a time";
				break;
			default:
				elog(ERROR, "invalid option type: %c", opt->type);
				return;	/* keep compiler quiet */
		}
	}

	if (isprint(opt->sname))
		elog(ERROR, "option -%c, --%s should be %s: '%s'",
			opt->sname, opt->lname, message, optarg);
	else
		elog(ERROR, "option --%s should be %s: '%s'",
			opt->lname, message, optarg);
}

/*
 * Convert a value from one of the human-friendly units ("kB", "min" etc.)
 * to the given base unit.  'value' and 'unit' are the input value and unit
 * to convert from.  The converted value is stored in *base_value.
 *
 * Returns true on success, false if the input unit is not recognized.
 */
static bool
convert_to_base_unit(int64 value, const char *unit,
					 int base_unit, int64 *base_value)
{
	const unit_conversion *table;
	int			i;

	if (base_unit & OPTION_UNIT_MEMORY)
		table = memory_unit_conversion_table;
	else
		table = time_unit_conversion_table;

	for (i = 0; *table[i].unit; i++)
	{
		if (base_unit == table[i].base_unit &&
			strcmp(unit, table[i].unit) == 0)
		{
			if (table[i].multiplier < 0)
				*base_value = value / (-table[i].multiplier);
			else
			{
				/* Check for integer overflow first */
				if (value > PG_INT64_MAX / table[i].multiplier)
					return false;

				*base_value = value * table[i].multiplier;
			}
			return true;
		}
	}
	return false;
}

/*
 * Unsigned variant of convert_to_base_unit()
 */
static bool
convert_to_base_unit_u(uint64 value, const char *unit,
					   int base_unit, uint64 *base_value)
{
	const unit_conversion *table;
	int			i;

	if (base_unit & OPTION_UNIT_MEMORY)
		table = memory_unit_conversion_table;
	else
		table = time_unit_conversion_table;

	for (i = 0; *table[i].unit; i++)
	{
		if (base_unit == table[i].base_unit &&
			strcmp(unit, table[i].unit) == 0)
		{
			if (table[i].multiplier < 0)
				*base_value = value / (-table[i].multiplier);
			else
			{
				/* Check for integer overflow first */
				if (value > PG_UINT64_MAX / table[i].multiplier)
					return false;

				*base_value = value * table[i].multiplier;
			}
			return true;
		}
	}
	return false;
}

/*
 * Convert a value in some base unit to a human-friendly unit.  The output
 * unit is chosen so that it's the greatest unit that can represent the value
 * without loss.  For example, if the base unit is GUC_UNIT_KB, 1024 is
 * converted to 1 MB, but 1025 is represented as 1025 kB.
 */
void
convert_from_base_unit(int64 base_value, int base_unit,
					   int64 *value, const char **unit)
{
	const unit_conversion *table;
	int			i;

	*unit = NULL;

	if (base_unit & OPTION_UNIT_MEMORY)
		table = memory_unit_conversion_table;
	else
		table = time_unit_conversion_table;

	for (i = 0; *table[i].unit; i++)
	{
		if (base_unit == table[i].base_unit)
		{
			/*
			 * Accept the first conversion that divides the value evenly. We
			 * assume that the conversions for each base unit are ordered from
			 * greatest unit to the smallest!
			 */
			if (table[i].multiplier < 0)
			{
				/* Check for integer overflow first */
				if (base_value > PG_INT64_MAX / (-table[i].multiplier))
					continue;

				*value = base_value * (-table[i].multiplier);
				*unit = table[i].unit;
				break;
			}
			else if (base_value % table[i].multiplier == 0)
			{
				*value = base_value / table[i].multiplier;
				*unit = table[i].unit;
				break;
			}
		}
	}

	Assert(*unit != NULL);
}

/*
 * Unsigned variant of convert_from_base_unit()
 */
void
convert_from_base_unit_u(uint64 base_value, int base_unit,
						 uint64 *value, const char **unit)
{
	const unit_conversion *table;
	int			i;

	*unit = NULL;

	if (base_unit & OPTION_UNIT_MEMORY)
		table = memory_unit_conversion_table;
	else
		table = time_unit_conversion_table;

	for (i = 0; *table[i].unit; i++)
	{
		if (base_unit == table[i].base_unit)
		{
			/*
			 * Accept the first conversion that divides the value evenly. We
			 * assume that the conversions for each base unit are ordered from
			 * greatest unit to the smallest!
			 */
			if (table[i].multiplier < 0)
			{
				/* Check for integer overflow first */
				if (base_value > PG_UINT64_MAX / (-table[i].multiplier))
					continue;

				*value = base_value * (-table[i].multiplier);
				*unit = table[i].unit;
				break;
			}
			else if (base_value % table[i].multiplier == 0)
			{
				*value = base_value / table[i].multiplier;
				*unit = table[i].unit;
				break;
			}
		}
	}

	Assert(*unit != NULL);
}

static bool
parse_unit(char *unit_str, int flags, int64 value, int64 *base_value)
{
	/* allow whitespace between integer and unit */
	while (isspace((unsigned char) *unit_str))
		unit_str++;

	/* Handle possible unit */
	if (*unit_str != '\0')
	{
		char		unit[MAX_UNIT_LEN + 1];
		int			unitlen;
		bool		converted = false;

		if ((flags & OPTION_UNIT) == 0)
			return false;		/* this setting does not accept a unit */

		unitlen = 0;
		while (*unit_str != '\0' && !isspace((unsigned char) *unit_str) &&
			   unitlen < MAX_UNIT_LEN)
			unit[unitlen++] = *(unit_str++);
		unit[unitlen] = '\0';
		/* allow whitespace after unit */
		while (isspace((unsigned char) *unit_str))
			unit_str++;

		if (*unit_str == '\0')
			converted = convert_to_base_unit(value, unit, (flags & OPTION_UNIT),
											 base_value);
		if (!converted)
			return false;
	}

	return true;
}

/*
 * Unsigned variant of parse_unit()
 */
static bool
parse_unit_u(char *unit_str, int flags, uint64 value, uint64 *base_value)
{
	/* allow whitespace between integer and unit */
	while (isspace((unsigned char) *unit_str))
		unit_str++;

	/* Handle possible unit */
	if (*unit_str != '\0')
	{
		char		unit[MAX_UNIT_LEN + 1];
		int			unitlen;
		bool		converted = false;

		if ((flags & OPTION_UNIT) == 0)
			return false;		/* this setting does not accept a unit */

		unitlen = 0;
		while (*unit_str != '\0' && !isspace((unsigned char) *unit_str) &&
			   unitlen < MAX_UNIT_LEN)
			unit[unitlen++] = *(unit_str++);
		unit[unitlen] = '\0';
		/* allow whitespace after unit */
		while (isspace((unsigned char) *unit_str))
			unit_str++;

		if (*unit_str == '\0')
			converted = convert_to_base_unit_u(value, unit, (flags & OPTION_UNIT),
											   base_value);
		if (!converted)
			return false;
	}

	return true;
}

/*
 * Try to interpret value as boolean value.  Valid values are: true,
 * false, yes, no, on, off, 1, 0; as well as unique prefixes thereof.
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 */
bool
parse_bool(const char *value, bool *result)
{
	return parse_bool_with_len(value, strlen(value), result);
}

bool
parse_bool_with_len(const char *value, size_t len, bool *result)
{
	switch (*value)
	{
		case 't':
		case 'T':
			if (pg_strncasecmp(value, "true", len) == 0)
			{
				if (result)
					*result = true;
				return true;
			}
			break;
		case 'f':
		case 'F':
			if (pg_strncasecmp(value, "false", len) == 0)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		case 'y':
		case 'Y':
			if (pg_strncasecmp(value, "yes", len) == 0)
			{
				if (result)
					*result = true;
				return true;
			}
			break;
		case 'n':
		case 'N':
			if (pg_strncasecmp(value, "no", len) == 0)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		case 'o':
		case 'O':
			/* 'o' is not unique enough */
			if (pg_strncasecmp(value, "on", (len > 2 ? len : 2)) == 0)
			{
				if (result)
					*result = true;
				return true;
			}
			else if (pg_strncasecmp(value, "off", (len > 2 ? len : 2)) == 0)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		case '1':
			if (len == 1)
			{
				if (result)
					*result = true;
				return true;
			}
			break;
		case '0':
			if (len == 1)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		default:
			break;
	}

	if (result)
		*result = false;		/* suppress compiler warning */
	return false;
}

/*
 * Parse string as 32bit signed int.
 * valid range: -2147483648 ~ 2147483647
 */
bool
parse_int32(const char *value, int32 *result, int flags)
{
	int64	val;
	char   *endptr;

	if (strcmp(value, INFINITE_STR) == 0)
	{
		*result = PG_INT32_MAX;
		return true;
	}

	errno = 0;
	val = strtol(value, &endptr, 0);
	if (endptr == value || (*endptr && flags == 0))
		return false;

	/* Check for integer overflow */
	if (errno == ERANGE || val != (int64) ((int32) val))
		return false;

	if (!parse_unit(endptr, flags, val, &val))
		return false;

	/* Check for integer overflow again */
	if (val != (int64) ((int32) val))
		return false;

	*result = val;

	return true;
}

/*
 * Parse string as 32bit unsigned int.
 * valid range: 0 ~ 4294967295 (2^32-1)
 */
bool
parse_uint32(const char *value, uint32 *result, int flags)
{
	uint64	val;
	char   *endptr;

	if (strcmp(value, INFINITE_STR) == 0)
	{
		*result = PG_UINT32_MAX;
		return true;
	}

	errno = 0;
	val = strtoul(value, &endptr, 0);
	if (endptr == value || (*endptr && flags == 0))
		return false;

	/* Check for integer overflow */
	if (errno == ERANGE || val != (uint64) ((uint32) val))
		return false;

	if (!parse_unit_u(endptr, flags, val, &val))
		return false;

	/* Check for integer overflow again */
	if (val != (uint64) ((uint32) val))
		return false;

	*result = val;

	return true;
}

/*
 * Parse string as int64
 * valid range: -9223372036854775808 ~ 9223372036854775807
 */
bool
parse_int64(const char *value, int64 *result, int flags)
{
	int64	val;
	char   *endptr;

	if (strcmp(value, INFINITE_STR) == 0)
	{
		*result = PG_INT64_MAX;
		return true;
	}

	errno = 0;
#if defined(HAVE_LONG_INT_64)
	val = strtol(value, &endptr, 0);
#elif defined(HAVE_LONG_LONG_INT_64)
	val = strtoll(value, &endptr, 0);
#else
	val = strtol(value, &endptr, 0);
#endif
	if (endptr == value || (*endptr && flags == 0))
		return false;

	if (errno == ERANGE)
		return false;

	if (!parse_unit(endptr, flags, val, &val))
		return false;

	*result = val;

	return true;
}

/*
 * Parse string as uint64
 * valid range: 0 ~ (2^64-1)
 */
bool
parse_uint64(const char *value, uint64 *result, int flags)
{
	uint64	val;
	char   *endptr;

	if (strcmp(value, INFINITE_STR) == 0)
	{
		*result = PG_UINT64_MAX;
		return true;
	}

	errno = 0;
#if defined(HAVE_LONG_INT_64)
	val = strtoul(value, &endptr, 0);
#elif defined(HAVE_LONG_LONG_INT_64)
	val = strtoull(value, &endptr, 0);
#else
	val = strtoul(value, &endptr, 0);
#endif
	if (endptr == value || (*endptr && flags == 0))
		return false;

	if (errno == ERANGE)
		return false;

	if (!parse_unit_u(endptr, flags, val, &val))
		return false;

	*result = val;

	return true;
}

/*
 * Convert ISO-8601 format string to time_t value.
 *
 * If utc_default is true, then if timezone offset isn't specified tz will be
 * +00:00.
 */
bool
parse_time(const char *value, time_t *result, bool utc_default)
{
	size_t		len;
	int			fields_num,
				tz = 0,
				i;
	bool		tz_set = false;
	char	   *tmp;
	struct tm	tm;
	char		junk[2];

	/* tmp = replace( value, !isalnum, ' ' ) */
	tmp = pgut_malloc(strlen(value) + + 1);
	len = 0;
	fields_num = 1;

	while (*value)
	{
		if (IsAlnum(*value))
		{
			tmp[len++] = *value;
			value++;
		}
		else if (fields_num < 6)
		{
			fields_num++;
			tmp[len++] = ' ';
			value++;
		}
		/* timezone field is 7th */
		else if ((*value == '-' || *value == '+') && fields_num == 6)
		{
			int			hr,
						min,
						sec = 0;
			char	   *cp;

			errno = 0;
			hr = strtol(value + 1, &cp, 10);
			if ((value + 1) == cp || errno == ERANGE)
				return false;

			/* explicit delimiter? */
			if (*cp == ':')
			{
				errno = 0;
				min = strtol(cp + 1, &cp, 10);
				if (errno == ERANGE)
					return false;
				if (*cp == ':')
				{
					errno = 0;
					sec = strtol(cp + 1, &cp, 10);
					if (errno == ERANGE)
						return false;
				}
			}
			/* otherwise, might have run things together... */
			else if (*cp == '\0' && strlen(value) > 3)
			{
				min = hr % 100;
				hr = hr / 100;
				/* we could, but don't, support a run-together hhmmss format */
			}
			else
				min = 0;

			/* Range-check the values; see notes in datatype/timestamp.h */
			if (hr < 0 || hr > MAX_TZDISP_HOUR)
				return false;
			if (min < 0 || min >= MINS_PER_HOUR)
				return false;
			if (sec < 0 || sec >= SECS_PER_MINUTE)
				return false;

			tz = (hr * MINS_PER_HOUR + min) * SECS_PER_MINUTE + sec;
			if (*value == '-')
				tz = -tz;

			tz_set = true;

			fields_num++;
			value = cp;
		}
		/* wrong format */
		else if (!IsSpace(*value))
			return false;
	}
	tmp[len] = '\0';

	/* parse for "YYYY-MM-DD HH:MI:SS" */
	memset(&tm, 0, sizeof(tm));
	tm.tm_year = 0;		/* tm_year is year - 1900 */
	tm.tm_mon = 0;		/* tm_mon is 0 - 11 */
	tm.tm_mday = 1;		/* tm_mday is 1 - 31 */
	tm.tm_hour = 0;
	tm.tm_min = 0;
	tm.tm_sec = 0;
	i = sscanf(tmp, "%04d %02d %02d %02d %02d %02d%1s",
		&tm.tm_year, &tm.tm_mon, &tm.tm_mday,
		&tm.tm_hour, &tm.tm_min, &tm.tm_sec, junk);
	free(tmp);

	if (i < 1 || 6 < i)
		return false;

	/* adjust year */
	if (tm.tm_year < 100)
		tm.tm_year += 2000 - 1900;
	else if (tm.tm_year >= 1900)
		tm.tm_year -= 1900;

	/* adjust month */
	if (i > 1)
		tm.tm_mon -= 1;

	/* determine whether Daylight Saving Time is in effect */
	tm.tm_isdst = -1;

	*result = mktime(&tm);

	/* adjust time zone */
	if (tz_set || utc_default)
	{
		time_t		ltime = time(NULL);
		struct tm  *ptm = gmtime(&ltime);
		time_t		gmt = mktime(ptm);
		time_t		offset;

		/* UTC time */
		*result -= tz;

		/* Get local time */
		ptm = localtime(&ltime);
		offset = ltime - gmt + (ptm->tm_isdst ? 3600 : 0);

		*result += offset;
	}

	return true;
}

/*
 * Try to parse value as an integer.  The accepted formats are the
 * usual decimal, octal, or hexadecimal formats, optionally followed by
 * a unit name if "flags" indicates a unit is allowed.
 *
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 * If not okay and hintmsg is not NULL, *hintmsg is set to a suitable
 *	HINT message, or NULL if no hint provided.
 */
bool
parse_int(const char *value, int *result, int flags, const char **hintmsg)
{
	int64		val;
	char	   *endptr;

	/* To suppress compiler warnings, always set output params */
	if (result)
		*result = 0;
	if (hintmsg)
		*hintmsg = NULL;

	/* We assume here that int64 is at least as wide as long */
	errno = 0;
	val = strtol(value, &endptr, 0);

	if (endptr == value)
		return false;			/* no HINT for integer syntax error */

	if (errno == ERANGE || val != (int64) ((int32) val))
	{
		if (hintmsg)
			*hintmsg = "Value exceeds integer range.";
		return false;
	}

	/* allow whitespace between integer and unit */
	while (isspace((unsigned char) *endptr))
		endptr++;

	/* Handle possible unit */
	if (*endptr != '\0')
	{
		char		unit[MAX_UNIT_LEN + 1];
		int			unitlen;
		bool		converted = false;

		if ((flags & OPTION_UNIT) == 0)
			return false;		/* this setting does not accept a unit */

		unitlen = 0;
		while (*endptr != '\0' && !isspace((unsigned char) *endptr) &&
			   unitlen < MAX_UNIT_LEN)
			unit[unitlen++] = *(endptr++);
		unit[unitlen] = '\0';
		/* allow whitespace after unit */
		while (isspace((unsigned char) *endptr))
			endptr++;

		if (*endptr == '\0')
			converted = convert_to_base_unit(val, unit, (flags & OPTION_UNIT),
											 &val);
		if (!converted)
		{
			/* invalid unit, or garbage after the unit; set hint and fail. */
			if (hintmsg)
			{
				if (flags & OPTION_UNIT_MEMORY)
					*hintmsg = memory_units_hint;
				else
					*hintmsg = time_units_hint;
			}
			return false;
		}

		/* Check for overflow due to units conversion */
		if (val != (int64) ((int32) val))
		{
			if (hintmsg)
				*hintmsg = "Value exceeds integer range.";
			return false;
		}
	}

	if (result)
		*result = (int) val;
	return true;
}

bool
parse_lsn(const char *value, XLogRecPtr *result)
{
	uint32	xlogid;
	uint32	xrecoff;
	int		len1;
	int		len2;

	len1 = strspn(value, "0123456789abcdefABCDEF");
	if (len1 < 1 || len1 > MAXPG_LSNCOMPONENT || value[len1] != '/')
		elog(ERROR, "invalid LSN \"%s\"", value);
	len2 = strspn(value + len1 + 1, "0123456789abcdefABCDEF");
	if (len2 < 1 || len2 > MAXPG_LSNCOMPONENT || value[len1 + 1 + len2] != '\0')
		elog(ERROR, "invalid LSN \"%s\"", value);

	if (sscanf(value, "%X/%X", &xlogid, &xrecoff) == 2)
		*result = (XLogRecPtr) ((uint64) xlogid << 32) | xrecoff;
	else
	{
		elog(ERROR, "invalid LSN \"%s\"", value);
		return false;
	}

	return true;
}

static char *
longopts_to_optstring(const struct option opts[], const size_t len)
{
	size_t		i;
	char	   *result;
	char	   *s;

	result = pgut_malloc(len * 2 + 1);

	s = result;
	for (i = 0; i < len; i++)
	{
		if (!isprint(opts[i].val))
			continue;
		*s++ = opts[i].val;
		if (opts[i].has_arg != no_argument)
			*s++ = ':';
	}
	*s = '\0';

	return result;
}

void
pgut_getopt_env(pgut_option options[])
{
	size_t	i;

	for (i = 0; options && options[i].type; i++)
	{
		pgut_option	   *opt = &options[i];
		const char	   *value = NULL;

		/* If option was already set do not check env */
		if (opt->source > SOURCE_ENV || opt->allowed < SOURCE_ENV)
			continue;

		if (strcmp(opt->lname, "pgdata") == 0)
			value = getenv("PGDATA");
		if (strcmp(opt->lname, "port") == 0)
			value = getenv("PGPORT");
		if (strcmp(opt->lname, "host") == 0)
			value = getenv("PGHOST");
		if (strcmp(opt->lname, "username") == 0)
			value = getenv("PGUSER");
		if (strcmp(opt->lname, "pgdatabase") == 0)
		{
			value = getenv("PGDATABASE");
			if (value == NULL)
				value = getenv("PGUSER");
			if (value == NULL)
				value = get_username();
		}

		if (value)
			assign_option(opt, value, SOURCE_ENV);
	}
}

int
pgut_getopt(int argc, char **argv, pgut_option options[])
{
	int			c;
	int			optindex = 0;
	char	   *optstring;
	pgut_option *opt;
	struct option *longopts;
	size_t		len;

	len = option_length(options);
	longopts = pgut_newarray(struct option, len + 1 /* zero/end option */);
	option_copy(longopts, options, len);

	optstring = longopts_to_optstring(longopts, len);

	/* Assign named options */
	while ((c = getopt_long(argc, argv, optstring, longopts, &optindex)) != -1)
	{
		opt = option_find(c, options);
		if (opt && opt->allowed < SOURCE_CMDLINE)
			elog(ERROR, "option %s cannot be specified in command line",
				 opt->lname);
		/* Check 'opt == NULL' is performed in assign_option() */
		assign_option(opt, optarg, SOURCE_CMDLINE);
	}

	init_cancel_handler();
	atexit(on_cleanup);

	return optind;
}

/* compare two strings ignore cases and ignore -_ */
static bool
key_equals(const char *lhs, const char *rhs)
{
	for (; *lhs && *rhs; lhs++, rhs++)
	{
		if (strchr("-_ ", *lhs))
		{
			if (!strchr("-_ ", *rhs))
				return false;
		}
		else if (ToLower(*lhs) != ToLower(*rhs))
			return false;
	}

	return *lhs == '\0' && *rhs == '\0';
}

/*
 * Get configuration from configuration file.
 * Return number of parsed options
 */
int
pgut_readopt(const char *path, pgut_option options[], int elevel, bool strict)
{
	FILE   *fp;
	char	buf[1024];
	char	key[1024];
	char	value[1024];
	int		parsed_options = 0;

	if (!options)
		return parsed_options;

	if ((fp = fio_open_stream(path, FIO_BACKUP_HOST)) == NULL)
		return parsed_options;

	while (fgets(buf, lengthof(buf), fp))
	{
		size_t		i;

		for (i = strlen(buf); i > 0 && IsSpace(buf[i - 1]); i--)
			buf[i - 1] = '\0';

		if (parse_pair(buf, key, value))
		{
			for (i = 0; options[i].type; i++)
			{
				pgut_option *opt = &options[i];

				if (key_equals(key, opt->lname))
				{
					if (opt->allowed < SOURCE_FILE &&
						opt->allowed != SOURCE_FILE_STRICT)
						elog(elevel, "option %s cannot be specified in file", opt->lname);
					else if (opt->source <= SOURCE_FILE)
					{
						assign_option(opt, value, SOURCE_FILE);
						parsed_options++;
					}
					break;
				}
			}
			if (strict && !options[i].type)
				elog(elevel, "invalid option \"%s\" in file \"%s\"", key, path);
		}
	}

	fio_close_stream(fp);

	return parsed_options;
}

static const char *
skip_space(const char *str, const char *line)
{
	while (IsSpace(*str)) { str++; }
	return str;
}

static const char *
get_next_token(const char *src, char *dst, const char *line)
{
	const char   *s;
	int		i;
	int		j;

	if ((s = skip_space(src, line)) == NULL)
		return NULL;

	/* parse quoted string */
	if (*s == '\'')
	{
		s++;
		for (i = 0, j = 0; s[i] != '\0'; i++)
		{
			if (s[i] == '\\')
			{
				i++;
				switch (s[i])
				{
					case 'b':
						dst[j] = '\b';
						break;
					case 'f':
						dst[j] = '\f';
						break;
					case 'n':
						dst[j] = '\n';
						break;
					case 'r':
						dst[j] = '\r';
						break;
					case 't':
						dst[j] = '\t';
						break;
					case '0':
					case '1':
					case '2':
					case '3':
					case '4':
					case '5':
					case '6':
					case '7':
						{
							int			k;
							long		octVal = 0;

							for (k = 0;
								 s[i + k] >= '0' && s[i + k] <= '7' && k < 3;
									 k++)
								octVal = (octVal << 3) + (s[i + k] - '0');
							i += k - 1;
							dst[j] = ((char) octVal);
						}
						break;
					default:
						dst[j] = s[i];
						break;
				}
			}
			else if (s[i] == '\'')
			{
				i++;
				/* doubled quote becomes just one quote */
				if (s[i] == '\'')
					dst[j] = s[i];
				else
					break;
			}
			else
				dst[j] = s[i];
			j++;
		}
	}
	else
	{
		i = j = strcspn(s, "#\n\r\t\v");
		memcpy(dst, s, j);
	}

	dst[j] = '\0';
	return s + i;
}

static bool
parse_pair(const char buffer[], char key[], char value[])
{
	const char *start;
	const char *end;

	key[0] = value[0] = '\0';

	/*
	 * parse key
	 */
	start = buffer;
	if ((start = skip_space(start, buffer)) == NULL)
		return false;

	end = start + strcspn(start, "=# \n\r\t\v");

	/* skip blank buffer */
	if (end - start <= 0)
	{
		if (*start == '=')
			elog(ERROR, "syntax error in \"%s\"", buffer);
		return false;
	}

	/* key found */
	strncpy(key, start, end - start);
	key[end - start] = '\0';

	/* find key and value split char */
	if ((start = skip_space(end, buffer)) == NULL)
		return false;

	if (*start != '=')
	{
		elog(ERROR, "syntax error in \"%s\"", buffer);
		return false;
	}

	start++;

	/*
	 * parse value
	 */
	if ((end = get_next_token(start, value, buffer)) == NULL)
		return false;

	if ((start = skip_space(end, buffer)) == NULL)
		return false;

	if (*start != '\0' && *start != '#')
	{
		elog(ERROR, "syntax error in \"%s\"", buffer);
		return false;
	}

	return true;
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

		/* do not print password into the file */
		if (strcmp(option->keyword, "password") == 0)
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

PGconn *
pgut_connect(const char *dbname)
{
	return pgut_connect_extended(host, port, dbname, username);
}

PGconn *
pgut_connect_extended(const char *pghost, const char *pgport,
					  const char *dbname, const char *login)
{
	PGconn	   *conn;

	if (interrupted && !in_cleanup)
		elog(ERROR, "interrupted");

	if (force_password && !prompt_password)
		elog(ERROR, "You cannot specify --password and --no-password options together");

	if (!password && force_password)
		prompt_for_password(login);

	/* Start the connection. Loop until we have a password if requested by backend. */
	for (;;)
	{
		conn = PQsetdbLogin(pghost, pgport, NULL, NULL,
							dbname, login, password);

		if (PQstatus(conn) == CONNECTION_OK)
			return conn;

		if (conn && PQconnectionNeedsPassword(conn) && prompt_password)
		{
			PQfinish(conn);
			prompt_for_password(login);

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
pgut_connect_replication(const char *dbname)
{
	return pgut_connect_replication_extended(host, port, dbname, username);
}

PGconn *
pgut_connect_replication_extended(const char *pghost, const char *pgport,
								  const char *dbname, const char *pguser)
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
		prompt_for_password(pguser);

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

	if (pghost)
	{
		keywords[i] = "host";
		values[i] = pghost;
		i++;
	}
	if (pguser)
	{
		keywords[i] = "user";
		values[i] = pguser;
		i++;
	}
	if (pgport)
	{
		keywords[i] = "port";
		values[i] = pgport;
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
			prompt_for_password(pguser);
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
}


PGresult *
pgut_execute_parallel(PGconn* conn, 
					  PGcancel* thread_cancel_conn, const char *query, 
					  int nParams, const char **params,
					  bool text_result)
{
	PGresult   *res;

	if (interrupted && !in_cleanup)
		elog(ERROR, "interrupted");

	/* write query to elog if verbose */
	if (log_level_console <= VERBOSE || log_level_file <= VERBOSE)
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
	if (nParams == 0)
		res = PQexec(conn, query);
	else
		res = PQexecParams(conn, query, nParams, NULL, params, NULL, NULL,
						   /*
							* Specify zero to obtain results in text format,
							* or one to obtain results in binary format.
							*/
						   (text_result) ? 0 : 1);
	//on_after_exec(thread_cancel_conn);

	switch (PQresultStatus(res))
	{
		case PGRES_TUPLES_OK:
		case PGRES_COMMAND_OK:
		case PGRES_COPY_IN:
			break;
		default:
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
	if (log_level_console <= VERBOSE || log_level_file <= VERBOSE)
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
	if (log_level_console <= VERBOSE || log_level_file <= VERBOSE)
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

		/* be sure handle_sigint doesn't use pointer while freeing */
		thread_cancel_conn = NULL;

		if (old != NULL)
			PQfreeCancel(old);

		thread_cancel_conn = PQgetCancel(conn);
	}
	else
	{
		/* Free the old one if we have one */
		old = cancel_conn;

		/* be sure handle_sigint doesn't use pointer while freeing */
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

		/* be sure handle_sigint doesn't use pointer while freeing */
		thread_cancel_conn = NULL;

		if (old != NULL)
			PQfreeCancel(old);
	}
	else
	{
		old = cancel_conn;

		/* be sure handle_sigint doesn't use pointer while freeing */
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

	/* Set interruped flag */
	interrupted = true;

	/* User promts password, call on_cleanup() byhand */
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

	for (item = pgut_atexit_stack; item; item = item->next)
		item->callback(fatal, item->userdata);
}

static void
on_cleanup(void)
{
	in_cleanup = true;
	interrupted = false;
	call_atexit_callbacks(false);
}

static void
exit_or_abort(int exitcode)
{
	if (in_cleanup)
	{
		/* oops, error in cleanup*/
		call_atexit_callbacks(true);
		abort();
	}
	else
	{
		/* normal exit */
		exit(exitcode);
	}
}

/*
 * Returns the current user name.
 */
static const char *
get_username(void)
{
	const char *ret;

#ifndef WIN32
	struct passwd *pw;

	pw = getpwuid(geteuid());
	ret = (pw ? pw->pw_name : NULL);
#else
	static char username[128];	/* remains after function exit */
	DWORD		len = sizeof(username) - 1;

	if (GetUserName(username, &len))
		ret = username;
	else
	{
		_dosmaperr(GetLastError());
		ret = NULL;
	}
#endif

	if (ret == NULL)
		elog(ERROR, "%s: could not get current user name: %s",
				PROGRAM_NAME, strerror(errno));
	return ret;
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
handle_sigint(SIGNAL_ARGS)
{
	on_interrupt();
}

static void
init_cancel_handler(void)
{
	oldhandler = pqsignal(SIGINT, handle_sigint);
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
