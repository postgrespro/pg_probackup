/*-------------------------------------------------------------------------
 *
 * json.c: - make json document.
 *
 * Copyright (c) 2018-2025, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "json.h"

static void json_add_indent(PQExpBuffer buf, int32 level);
static void json_add_escaped(PQExpBuffer buf, const char *str);

static bool add_comma = false;

/*
 * Start or end json token. Currently it is a json object or array.
 *
 * Function modifies level value and adds indent if it appropriate.
 */
void
json_add(PQExpBuffer buf, JsonToken type, int32 *level)
{
	switch (type)
	{
		case JT_BEGIN_ARRAY:
			appendPQExpBufferChar(buf, '[');
			*level += 1;
			add_comma = false;
			break;
		case JT_END_ARRAY:
			*level -= 1;
			if (*level == 0)
				appendPQExpBufferChar(buf, '\n');
			else
				json_add_indent(buf, *level);
			appendPQExpBufferChar(buf, ']');
			add_comma = true;
			break;
		case JT_BEGIN_OBJECT:
			json_add_indent(buf, *level);
			appendPQExpBufferChar(buf, '{');
			*level += 1;
			add_comma = false;
			break;
		case JT_END_OBJECT:
			*level -= 1;
			if (*level == 0)
				appendPQExpBufferChar(buf, '\n');
			else
				json_add_indent(buf, *level);
			appendPQExpBufferChar(buf, '}');
			add_comma = true;
			break;
		default:
			break;
	}
}

/*
 * Add json object's key. If it isn't first key we need to add a comma.
 */
void
json_add_key(PQExpBuffer buf, const char *name, int32 level)
{
	if (add_comma)
		appendPQExpBufferChar(buf, ',');
	json_add_indent(buf, level);

	json_add_escaped(buf, name);
	appendPQExpBufferStr(buf, ": ");

	add_comma = true;
}

/*
 * Add json object's key and value. If it isn't first key we need to add a
 * comma.
 */
void
json_add_value(PQExpBuffer buf, const char *name, const char *value,
			   int32 level, bool escaped)
{
	json_add_key(buf, name, level);

	if (escaped)
		json_add_escaped(buf, value);
	else
		appendPQExpBufferStr(buf, value);
}

static void
json_add_indent(PQExpBuffer buf, int32 level)
{
	uint16		i;

	if (level == 0)
		return;

	appendPQExpBufferChar(buf, '\n');
	for (i = 0; i < level; i++)
		appendPQExpBufferStr(buf, "    ");
}

static void
json_add_escaped(PQExpBuffer buf, const char *str)
{
	const char *p;

	appendPQExpBufferChar(buf, '"');
	for (p = str; *p; p++)
	{
		switch (*p)
		{
			case '\b':
				appendPQExpBufferStr(buf, "\\b");
				break;
			case '\f':
				appendPQExpBufferStr(buf, "\\f");
				break;
			case '\n':
				appendPQExpBufferStr(buf, "\\n");
				break;
			case '\r':
				appendPQExpBufferStr(buf, "\\r");
				break;
			case '\t':
				appendPQExpBufferStr(buf, "\\t");
				break;
			case '"':
				appendPQExpBufferStr(buf, "\\\"");
				break;
			case '\\':
				appendPQExpBufferStr(buf, "\\\\");
				break;
			default:
				if ((unsigned char) *p < ' ')
					appendPQExpBuffer(buf, "\\u%04x", (int) *p);
				else
					appendPQExpBufferChar(buf, *p);
				break;
		}
	}
	appendPQExpBufferChar(buf, '"');
}

void
json_add_min(PQExpBuffer buf, JsonToken type)
{
	switch (type)
	{
		case JT_BEGIN_OBJECT:
			appendPQExpBufferChar(buf, '{');
			add_comma = false;
			break;
		case JT_END_OBJECT:
			appendPQExpBufferStr(buf, "}\n");
			add_comma = true;
			break;
		default:
			break;
	}
}
