/*-------------------------------------------------------------------------
 *
 * configuration.c: - function implementations to work with pg_probackup
 * configurations.
 *
 * Copyright (c) 2017-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"
#include "configuration.h"
#include "logger.h"
#include "pgut.h"
#include "file.h"

#include "datatype/timestamp.h"

#include "getopt_long.h"

#include <time.h>

#define MAXPG_LSNCOMPONENT	8

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

/*
 * Reading functions.
 */

static uint32
option_length(const ConfigOption opts[])
{
	uint32		len;

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
		  return no_argument;//optional_argument;
		default:
			return required_argument;
	}
}

static void
option_copy(struct option dst[], const ConfigOption opts[], size_t len)
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

static ConfigOption *
option_find(int c, ConfigOption opts1[])
{
	size_t	i;

	for (i = 0; opts1 && opts1[i].type; i++)
		if (opts1[i].sname == c)
			return &opts1[i];

	return NULL;	/* not found */
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

/*
 * Compare two strings ignore cases and ignore.
 */
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

static void
assign_option(ConfigOption *opt, const char *optarg, OptionSource src)
{
	const char *message;

	if (opt == NULL)
		elog(ERROR, "Option is not found. Try \"%s --help\" for more information.\n",
					PROGRAM_NAME);

	if (opt->source > src)
	{
		/* high prior value has been set already. */
		return;
	}
	/* Allow duplicate entries for function option */
	else if (src >= SOURCE_CMD && opt->source >= src && opt->type != 'f')
	{
		message = "specified only once";
	}
	else
	{
		OptionSource orig_source = opt->source;

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
				((option_assign_fn) opt->var)(opt, optarg);
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

				/* 'none' and 'off' are always disable the string parameter */
				//if (optarg && (pg_strcasecmp(optarg, "none") == 0))
				//{
				//	*(char **) opt->var = "none";
				//	return;
				//}

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
				elog(ERROR, "Invalid option type: %c", opt->type);
				return;	/* keep compiler quiet */
		}
	}

	if (optarg)
	{
		if (isprint(opt->sname))
			elog(ERROR, "Option -%c, --%s should be %s: '%s'",
				 opt->sname, opt->lname, message, optarg);
		else
			elog(ERROR, "Option --%s should be %s: '%s'",
				 opt->lname, message, optarg);
	}
	else
	{
		if (isprint(opt->sname))
			elog(ERROR, "Option -%c, --%s should be %s",
				 opt->sname, opt->lname, message);
		else
			elog(ERROR, "Option --%s should be %s",
				 opt->lname, message);
	}
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
			if (s[i] == '\'')
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
			elog(ERROR, "Syntax error in \"%s\"", buffer);
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
		elog(ERROR, "Syntax error in \"%s\"", buffer);
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
		elog(ERROR, "Syntax error in \"%s\"", buffer);
		return false;
	}

	return true;
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
		elog(ERROR, "Could not get current user name: %s", strerror(errno));
	return ret;
}

/*
 * Process options passed from command line.
 * TODO: currectly argument parsing treat missing argument for options
 * as invalid option
 */
int
config_get_opt(int argc, char **argv, ConfigOption cmd_options[],
			   ConfigOption options[])
{
	int			c;
	int			optindex = 0;
	char	   *optstring;
	struct option *longopts;
	uint32		cmd_len,
				len;

	cmd_len = option_length(cmd_options);
	len = option_length(options);

	longopts = pgut_newarray(struct option,
							 cmd_len + len + 1 /* zero/end option */);

	/* Concatenate two options */
	option_copy(longopts, cmd_options, cmd_len);
	option_copy(longopts + cmd_len, options, len + 1);

	optstring = longopts_to_optstring(longopts, cmd_len + len);

	/* Assign named options */
	while ((c = getopt_long(argc, argv, optstring, longopts, &optindex)) != -1)
	{
		ConfigOption *opt;

		opt = option_find(c, cmd_options);
		if (opt == NULL)
			opt = option_find(c, options);

		if (opt
			&& !remote_agent
			&& opt->allowed < SOURCE_CMD && opt->allowed != SOURCE_CMD_STRICT)
			elog(ERROR, "Option %s cannot be specified in command line",
				 opt->lname);
		/* Check 'opt == NULL' is performed in assign_option() */
		assign_option(opt, optarg, SOURCE_CMD);
	}

	return optind;
}

/*
 * Get configuration from configuration file.
 * Return number of parsed options.
 */
int
config_read_opt(const char *path, ConfigOption options[], int elevel,
				bool strict, bool missing_ok)
{
	FILE   *fp;
	char	buf[4096];
	char	key[1024];
	char	value[2048];
	int		parsed_options = 0;

	if (!options)
		return parsed_options;

	if ((fp = pgut_fopen(path, "rt", missing_ok)) == NULL)
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
				ConfigOption *opt = &options[i];

				if (key_equals(key, opt->lname))
				{
					if (opt->allowed < SOURCE_FILE &&
						opt->allowed != SOURCE_FILE_STRICT)
						elog(elevel, "Option %s cannot be specified in file",
							 opt->lname);
					else if (opt->source <= SOURCE_FILE)
					{
						assign_option(opt, value, SOURCE_FILE);
						parsed_options++;
					}
					break;
				}
			}
			if (strict && !options[i].type)
				elog(elevel, "Invalid option \"%s\" in file \"%s\"", key, path);
		}
	}

	if (ferror(fp))
		elog(ERROR, "Failed to read from file: \"%s\"", path);

	fio_close_stream(fp);

	return parsed_options;
}

/*
 * Process options passed as environment variables.
 */
void
config_get_opt_env(ConfigOption options[])
{
	size_t	i;

	for (i = 0; options && options[i].type; i++)
	{
		ConfigOption	   *opt = &options[i];
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

/*
 * Manually set source of the option. Find it by the pointer var.
 */
void
config_set_opt(ConfigOption options[], void *var, OptionSource source)
{
	int			i;

	for (i = 0; options[i].type; i++)
	{
		ConfigOption *opt = &options[i];

		if (opt->var == var)
		{
			if ((opt->allowed == SOURCE_FILE_STRICT && source != SOURCE_FILE) ||
				(opt->allowed == SOURCE_CMD_STRICT && source != SOURCE_CMD) ||
				(opt->allowed < source && opt->allowed >= SOURCE_ENV))
				elog(ERROR, "Invalid option source %d for %s",
					 source, opt->lname);

			opt->source = source;
			break;
		}
	}
}

/*
 * Return value of the function in the string representation. Result is
 * allocated string.
 */
char *
option_get_value(ConfigOption *opt)
{
	int64		value = 0;
	uint64		value_u = 0;
	const char *unit = NULL;

	/*
	 * If it is defined a unit for the option get readable value from base with
	 * unit name.
	 */
	if (opt->flags & OPTION_UNIT)
	{
		if (opt->type == 'i')
			convert_from_base_unit(*((int32 *) opt->var),
								   opt->flags & OPTION_UNIT, &value, &unit);
		else if (opt->type == 'i')
			convert_from_base_unit(*((int64 *) opt->var),
								   opt->flags & OPTION_UNIT, &value, &unit);
		else if (opt->type == 'u')
			convert_from_base_unit_u(*((uint32 *) opt->var),
									 opt->flags & OPTION_UNIT, &value_u, &unit);
		else if (opt->type == 'U')
			convert_from_base_unit_u(*((uint64 *) opt->var),
									 opt->flags & OPTION_UNIT, &value_u, &unit);
	}

	/* Get string representation itself */
	switch (opt->type)
	{
		case 'b':
		case 'B':
			return psprintf("%s", *((bool *) opt->var) ? "true" : "false");
		case 'i':
			if (opt->flags & OPTION_UNIT)
				return psprintf(INT64_FORMAT "%s", value, unit);
			else
				return psprintf("%d", *((int32 *) opt->var));
		case 'u':
			if (opt->flags & OPTION_UNIT)
				return psprintf(UINT64_FORMAT "%s", value_u, unit);
			else
				return psprintf("%u", *((uint32 *) opt->var));
		case 'I':
			if (opt->flags & OPTION_UNIT)
				return psprintf(INT64_FORMAT "%s", value, unit);
			else
				return psprintf(INT64_FORMAT, *((int64 *) opt->var));
		case 'U':
			if (opt->flags & OPTION_UNIT)
				return psprintf(UINT64_FORMAT "%s", value_u, unit);
			else
				return psprintf(UINT64_FORMAT, *((uint64 *) opt->var));
		case 's':
			if (*((char **) opt->var) == NULL)
				return NULL;
			/* 'none' and 'off' are always disable the string parameter */
			//if ((pg_strcasecmp(*((char **) opt->var), "none") == 0) ||
			//	(pg_strcasecmp(*((char **) opt->var), "off") == 0))
			//	return NULL;
			return pstrdup(*((char **) opt->var));
		case 't':
			{
				char	   *timestamp;
				time_t		t = *((time_t *) opt->var);

				if (t > 0)
				{
					timestamp = palloc(100);
					time2iso(timestamp, 100, t, false);
				}
				else
					timestamp = palloc0(1 /* just null termination */);
				return timestamp;
			}
		default:
			elog(ERROR, "Invalid option type: %c", opt->type);
			return NULL;	/* keep compiler quiet */
	}
}

/*
 * Parsing functions
 */

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
			converted = convert_to_base_unit_u(value, unit,
											   (flags & OPTION_UNIT),
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
 *
 * TODO: '0' converted into '2000-01-01 00:00:00'. Example: set-backup --expire-time=0
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

	char 	   *local_tz = getenv("TZ");

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
		else
			value++;
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

	if (i < 3 || i > 6)
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

	/*
	 * If tz is not set,
	 * treat it as UTC if requested, otherwise as local timezone
	 */
	if (tz_set || utc_default)
	{
		/* set timezone to UTC */
		pgut_setenv("TZ", "UTC");
#ifdef WIN32
		tzset();
#endif
	}

	/* convert time to utc unix time */
	*result = mktime(&tm);

	/* return old timezone back if any */
	if (local_tz)
		pgut_setenv("TZ", local_tz);
	else
		pgut_unsetenv("TZ");

#ifdef WIN32
	tzset();
#endif

	/* adjust time zone */
	if (tz_set || utc_default)
	{
		/* UTC time */
		*result -= tz;
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

/*
 * Convert time_t value to ISO-8601 format string. Always set timezone offset.
 */
void
time2iso(char *buf, size_t len, time_t time, bool utc)
{
	struct tm  *ptm = NULL;
	time_t		gmt;
	time_t		offset;
	char	   *ptr = buf;
	char 	   *local_tz = getenv("TZ");

	/* set timezone to UTC if requested */
	if (utc)
	{
		pgut_setenv("TZ", "UTC");
#ifdef WIN32
		tzset();
#endif
	}

	ptm = gmtime(&time);
	gmt = mktime(ptm);
	ptm = localtime(&time);

	if (utc)
	{
		/* return old timezone back if any */
		if (local_tz)
			pgut_setenv("TZ", local_tz);
		else
			pgut_unsetenv("TZ");
#ifdef WIN32
		tzset();
#endif
	}

	/* adjust timezone offset */
	offset = time - gmt + (ptm->tm_isdst ? 3600 : 0);

	strftime(ptr, len, "%Y-%m-%d %H:%M:%S", ptm);

	ptr += strlen(ptr);
	snprintf(ptr, len - (ptr - buf), "%c%02d",
			 (offset >= 0) ? '+' : '-',
			 abs((int) offset) / SECS_PER_HOUR);

	if (abs((int) offset) % SECS_PER_HOUR != 0)
	{
		ptr += strlen(ptr);
		snprintf(ptr, len - (ptr - buf), ":%02d",
				 abs((int) offset % SECS_PER_HOUR) / SECS_PER_MINUTE);
	}
}
