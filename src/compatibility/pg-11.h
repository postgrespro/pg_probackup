/*-------------------------------------------------------------------------
 *
 * pg-11.h
 *        PostgreSQL <= 11 compatibility
 *
 * Copyright (c) 2022, Postgres Professional
 *
 * When PG-11 reaches the end of support, we will need to remove
 * *_CRC32_COMPAT macros and use *_CRC32C instead.
 * And this file will be removed.
 *-------------------------------------------------------------------------
 */

#ifndef PG11_COMPAT_H
#define PG11_COMPAT_H

#include "utils/pgut.h"

#if PG_VERSION_NUM >= 120000

#define INIT_CRC32_COMPAT(backup_version, crc) \
do { \
	Assert(backup_version >= 20025); \
	INIT_CRC32C(crc); \
} while (0)

#define COMP_CRC32_COMPAT(backup_version, crc, data, len) \
do { \
	Assert(backup_version >= 20025); \
	COMP_CRC32C((crc), (data), (len)); \
} while (0)

#define FIN_CRC32_COMPAT(backup_version, crc) \
do { \
	Assert(backup_version >= 20025); \
	FIN_CRC32C(crc); \
} while (0)

#else /* PG_VERSION_NUM < 120000 */

#define INIT_CRC32_COMPAT(backup_version, crc) \
do { \
	if (backup_version <= 20021 || backup_version >= 20025) \
		INIT_CRC32C(crc); \
	else \
		INIT_TRADITIONAL_CRC32(crc); \
} while (0)

#define COMP_CRC32_COMPAT(backup_version, crc, data, len) \
do { \
	if (backup_version <= 20021 || backup_version >= 20025) \
		COMP_CRC32C((crc), (data), (len)); \
	else \
		COMP_TRADITIONAL_CRC32(crc, data, len); \
} while (0)

#define FIN_CRC32_COMPAT(backup_version, crc) \
do { \
	if (backup_version <= 20021 || backup_version >= 20025) \
		FIN_CRC32C(crc); \
	else \
		FIN_TRADITIONAL_CRC32(crc); \
} while (0)

#endif /* PG_VERSION_NUM < 120000 */

#endif /* PG11_COMPAT_H */
