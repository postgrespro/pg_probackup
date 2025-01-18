/*-------------------------------------------------------------------------
 *
 * ptrack.c: support functions for ptrack backups
 *
 * Copyright (c) 2021-2025 Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#if PG_VERSION_NUM < 110000
#include "catalog/catalog.h"
#endif
#include "catalog/pg_tablespace.h"

/*
 * Macro needed to parse ptrack.
 * NOTE Keep those values synchronized with definitions in ptrack.h
 */
#define PTRACK_BITS_PER_HEAPBLOCK 1
#define HEAPBLOCKS_PER_BYTE (BITS_PER_BYTE / PTRACK_BITS_PER_HEAPBLOCK)

/*
 * Parse a string like "2.1" into int
 * result: int by formula major_number * 100 + minor_number
 * or -1 if string cannot be parsed
 */
static int
ptrack_parse_version_string(const char *version_str)
{
	int ma, mi;
	int sscanf_readed_count;
	if (sscanf(version_str, "%u.%2u%n", &ma, &mi, &sscanf_readed_count) != 2)
		return -1;
	if (sscanf_readed_count != strlen(version_str))
		return -1;
	return ma * 100 + mi;
}

/* Check if the instance supports compatible version of ptrack,
 * fill-in version number if it does.
 * Also for ptrack 2.x save schema namespace.
 */
void
get_ptrack_version(PGconn *backup_conn, PGNodeInfo *nodeInfo)
{
	PGresult	*res_db;
	char	*ptrack_version_str;
	int	ptrack_version_num;

	res_db = pgut_execute(backup_conn,
						  "SELECT extnamespace::regnamespace, extversion "
						  "FROM pg_catalog.pg_extension WHERE extname = 'ptrack'::name",
						  0, NULL);

	if (PQntuples(res_db) > 0)
	{
		/* ptrack 2.x is supported, save schema name and version */
		nodeInfo->ptrack_schema = pgut_strdup(PQgetvalue(res_db, 0, 0));

		if (nodeInfo->ptrack_schema == NULL)
			elog(ERROR, "Failed to obtain schema name of ptrack extension");

		ptrack_version_str = PQgetvalue(res_db, 0, 1);
	}
	else
	{
		/* ptrack 1.x is supported, save version */
		PQclear(res_db);
		res_db = pgut_execute(backup_conn,
							  "SELECT proname FROM pg_catalog.pg_proc WHERE proname='ptrack_version'::name",
							  0, NULL);

		if (PQntuples(res_db) == 0)
		{
			/* ptrack is not supported */
			PQclear(res_db);
			return;
		}

		/*
		 * it's ok not to have permission to call this old function in PGPRO-11 version (ok_error = true)
		 * see deprication notice https://postgrespro.com/docs/postgrespro/11/release-pro-11-9-1
		 */
		res_db = pgut_execute_extended(backup_conn,
							  "SELECT pg_catalog.ptrack_version()",
							  0, NULL, true, true);
		if (PQntuples(res_db) == 0)
		{
			PQclear(res_db);
			elog(WARNING, "Can't call pg_catalog.ptrack_version(), it is assumed that there is no ptrack extension installed.");
			return;
		}
		ptrack_version_str = PQgetvalue(res_db, 0, 0);
	}

	ptrack_version_num = ptrack_parse_version_string(ptrack_version_str);
	if (ptrack_version_num == -1)
		/* leave default nodeInfo->ptrack_version_num = 0 from pgNodeInit() */
		elog(WARNING, "Cannot parse ptrack version string \"%s\"",
			 ptrack_version_str);
	else
		nodeInfo->ptrack_version_num = ptrack_version_num;

	/* ptrack 1.X is buggy, so fall back to DELTA backup strategy for safety */
	if (nodeInfo->ptrack_version_num < 200)
	{
		if (current.backup_mode == BACKUP_MODE_DIFF_PTRACK)
		{
			elog(WARNING, "Update your ptrack to the version 2.1 or upper. Current version is %s. "
						"Fall back to DELTA backup.",
				ptrack_version_str);
			current.backup_mode = BACKUP_MODE_DIFF_DELTA;
		}
	}

	PQclear(res_db);
}

/*
 * Check if ptrack is enabled in target instance
 */
bool
pg_is_ptrack_enabled(PGconn *backup_conn, int ptrack_version_num)
{
	PGresult   *res_db;
	bool		result = false;

	if (ptrack_version_num > 200)
	{
		res_db = pgut_execute(backup_conn, "SHOW ptrack.map_size", 0, NULL);
		result = strcmp(PQgetvalue(res_db, 0, 0), "0") != 0 &&
				 strcmp(PQgetvalue(res_db, 0, 0), "-1") != 0;
		PQclear(res_db);
	}
	else if (ptrack_version_num == 200)
	{
		res_db = pgut_execute(backup_conn, "SHOW ptrack_map_size", 0, NULL);
		result = strcmp(PQgetvalue(res_db, 0, 0), "0") != 0;
		PQclear(res_db);
	}
	else
	{
		result = false;
	}

	return result;
}

/*
 * Get lsn of the moment when ptrack was enabled the last time.
 */
XLogRecPtr
get_last_ptrack_lsn(PGconn *backup_conn, PGNodeInfo *nodeInfo)

{
	PGresult   *res;
	uint32		lsn_hi;
	uint32		lsn_lo;
	XLogRecPtr	lsn;

	char query[128];

	if (nodeInfo->ptrack_version_num == 200)
		sprintf(query, "SELECT %s.pg_ptrack_control_lsn()", nodeInfo->ptrack_schema);
	else
		sprintf(query, "SELECT %s.ptrack_init_lsn()", nodeInfo->ptrack_schema);

	res = pgut_execute(backup_conn, query, 0, NULL);

	/* Extract timeline and LSN from results of pg_start_backup() */
	XLogDataFromLSN(PQgetvalue(res, 0, 0), &lsn_hi, &lsn_lo);
	/* Calculate LSN */
	lsn = ((uint64) lsn_hi) << 32 | lsn_lo;

	PQclear(res);
	return lsn;
}

/* ----------------------------
 * Ptrack 2.* support functions
 * ----------------------------
 */

/*
 * Fetch a list of changed files with their ptrack maps.
 */
parray *
pg_ptrack_get_pagemapset(PGconn *backup_conn, const char *ptrack_schema,
						 int ptrack_version_num, XLogRecPtr lsn)
{
	PGresult   *res;
	char		lsn_buf[17 + 1];
	char	   *params[1];
	parray	   *pagemapset = NULL;
	int			i;
	char		query[512];

	snprintf(lsn_buf, sizeof lsn_buf, "%X/%X", (uint32) (lsn >> 32), (uint32) lsn);
	params[0] = pstrdup(lsn_buf);

	if (!ptrack_schema)
		elog(ERROR, "Schema name of ptrack extension is missing");

	if (ptrack_version_num == 200)
		sprintf(query, "SELECT path, pagemap FROM %s.pg_ptrack_get_pagemapset($1) ORDER BY 1",
				ptrack_schema);
	else
		sprintf(query, "SELECT path, pagemap FROM %s.ptrack_get_pagemapset($1) ORDER BY 1",
				ptrack_schema);

	res = pgut_execute(backup_conn, query, 1, (const char **) params);
	pfree(params[0]);

	if (PQnfields(res) != 2)
		elog(ERROR, "Cannot get ptrack pagemapset");

	/* sanity ? */

	/* Construct database map */
	for (i = 0; i < PQntuples(res); i++)
	{
		page_map_entry *pm_entry = (page_map_entry *) pgut_malloc(sizeof(page_map_entry));

		/* get path */
		pm_entry->path = pgut_strdup(PQgetvalue(res, i, 0));

		/* get bytea */
		pm_entry->pagemap = (char *) PQunescapeBytea((unsigned char *) PQgetvalue(res, i, 1),
													&pm_entry->pagemapsize);

		if (pagemapset == NULL)
			pagemapset = parray_new();

		parray_append(pagemapset, pm_entry);
	}

	PQclear(res);

	return pagemapset;
}

/*
 * Given a list of files in the instance to backup, build a pagemap for each
 * data file that has ptrack. Result is saved in the pagemap field of pgFile.
 *
 * We fetch a list of changed files with their ptrack maps.  After that files
 * are merged with their bitmaps.  File without bitmap is treated as unchanged.
 */
void
make_pagemap_from_ptrack_2(parray *files,
						   PGconn *backup_conn,
						   const char *ptrack_schema,
						   int ptrack_version_num,
						   XLogRecPtr lsn)
{
	parray *filemaps;
	int		file_i = 0;
	page_map_entry *dummy_map = NULL;

	/* Receive all available ptrack bitmaps at once */
	filemaps = pg_ptrack_get_pagemapset(backup_conn, ptrack_schema,
										ptrack_version_num, lsn);

	if (filemaps != NULL)
		parray_qsort(filemaps, pgFileMapComparePath);
	else
		return;

	dummy_map = (page_map_entry *) pgut_malloc(sizeof(page_map_entry));

	/* Iterate over files and look for corresponding pagemap if any */
	for (file_i = 0; file_i < parray_num(files); file_i++)
	{
		pgFile *file = (pgFile *) parray_get(files, file_i);
		page_map_entry **res_map = NULL;
		page_map_entry *map = NULL;

		/*
		 * For now nondata files are not entitled to have pagemap
		 * TODO It's possible to use ptrack for incremental backup of
		 * relation forks. Not implemented yet.
		 */
		if (!file->is_datafile || file->is_cfs)
			continue;

		/* Consider only files from PGDATA (this check is probably redundant) */
		if (file->external_dir_num != 0)
			continue;

		if (filemaps)
		{
			dummy_map->path = file->rel_path;
			res_map = parray_bsearch(filemaps, dummy_map, pgFileMapComparePath);
			map = (res_map) ? *res_map : NULL;
		}

		/* Found map */
		if (map)
		{
			elog(VERBOSE, "Using ptrack pagemap for file \"%s\"", file->rel_path);
			file->pagemap.bitmapsize = map->pagemapsize;
			file->pagemap.bitmap = map->pagemap;
		}
	}

	free(dummy_map);
}
