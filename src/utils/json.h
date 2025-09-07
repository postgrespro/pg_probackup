/*-------------------------------------------------------------------------
 *
 * json.h: - prototypes of json output functions.
 *
 * Copyright (c) 2018-2025, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#ifndef PROBACKUP_JSON_H
#define PROBACKUP_JSON_H

#include "postgres_fe.h"
#include "pqexpbuffer.h"

/*
 * Json document tokens.
 */
typedef enum
{
	JT_BEGIN_ARRAY,
	JT_END_ARRAY,
	JT_BEGIN_OBJECT,
	JT_END_OBJECT
} JsonToken;

extern void json_add(PQExpBuffer buf, JsonToken type, int32 *level);
extern void json_add_min(PQExpBuffer buf, JsonToken type);
extern void json_add_key(PQExpBuffer buf, const char *name, int32 level);
extern void json_add_value(PQExpBuffer buf, const char *name, const char *value,
						   int32 level, bool escaped);

#endif   /* PROBACKUP_JSON_H */
