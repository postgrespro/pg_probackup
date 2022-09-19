/*-------------------------------------------------------------------------
 * Logging framework for frontend programs
 *
 * Copyright (c) 2018-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 2021-2022, Postgres Professional
 *
 * src/include/common/logging.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COMMON_LOGGING_H
#define COMMON_LOGGING_H
#include "logger.h"

#define pg_log_fatal(...) elog(ERROR, __VA_ARGS__);

#define pg_log_error(...) elog(ERROR, __VA_ARGS__);

#define pg_log_warning(...) elog(WARNING, __VA_ARGS__);

#define pg_log_info(...) elog(INFO, __VA_ARGS__);




#endif							/* COMMON_LOGGING_H */



