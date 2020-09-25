/*-------------------------------------------------------------------------
 *
 * ptrack.c: support functions for ptrack backups
 *
 * Copyright (c) 2019 Postgres Professional
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
 * Given a list of files in the instance to backup, build a pagemap for each
 * data file that has ptrack. Result is saved in the pagemap field of pgFile.
 * NOTE we rely on the fact that provided parray is sorted by file->rel_path.
 */
void
make_pagemap_from_ptrack_1(parray *files, PGconn *backup_conn)
{
	size_t		i;
	Oid dbOid_with_ptrack_init = 0;
	Oid tblspcOid_with_ptrack_init = 0;
	char	   *ptrack_nonparsed = NULL;
	size_t		ptrack_nonparsed_size = 0;

	for (i = 0; i < parray_num(files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files, i);
		size_t		start_addr;

		/*
		 * If there is a ptrack_init file in the database,
		 * we must backup all its files, ignoring ptrack files for relations.
		 */
		if (file->is_database)
		{
			/*
			 * The function pg_ptrack_get_and_clear_db returns true
			 * if there was a ptrack_init file.
			 * Also ignore ptrack files for global tablespace,
			 * to avoid any possible specific errors.
			 */
			if ((file->tblspcOid == GLOBALTABLESPACE_OID) ||
				pg_ptrack_get_and_clear_db(file->dbOid, file->tblspcOid, backup_conn))
			{
				dbOid_with_ptrack_init = file->dbOid;
				tblspcOid_with_ptrack_init = file->tblspcOid;
			}
		}

		if (file->is_datafile)
		{
			if (file->tblspcOid == tblspcOid_with_ptrack_init &&
				file->dbOid == dbOid_with_ptrack_init)
			{
				/* ignore ptrack if ptrack_init exists */
				elog(VERBOSE, "Ignoring ptrack because of ptrack_init for file: %s", file->rel_path);
				file->pagemap_isabsent = true;
				continue;
			}

			/* get ptrack bitmap once for all segments of the file */
			if (file->segno == 0)
			{
				/* release previous value */
				pg_free(ptrack_nonparsed);
				ptrack_nonparsed_size = 0;

				ptrack_nonparsed = pg_ptrack_get_and_clear(file->tblspcOid, file->dbOid,
											   file->relOid, &ptrack_nonparsed_size, backup_conn);
			}

			if (ptrack_nonparsed != NULL)
			{
				/*
				 * pg_ptrack_get_and_clear() returns ptrack with VARHDR cut out.
				 * Compute the beginning of the ptrack map related to this segment
				 *
				 * HEAPBLOCKS_PER_BYTE. Number of heap pages one ptrack byte can track: 8
				 * RELSEG_SIZE. Number of Pages per segment: 131072
				 * RELSEG_SIZE/HEAPBLOCKS_PER_BYTE. number of bytes in ptrack file needed
				 * to keep track on one relsegment: 16384
				 */
				start_addr = (RELSEG_SIZE/HEAPBLOCKS_PER_BYTE)*file->segno;

				/*
				 * If file segment was created after we have read ptrack,
				 * we won't have a bitmap for this segment.
				 */
				if (start_addr > ptrack_nonparsed_size)
				{
					elog(VERBOSE, "Ptrack is missing for file: %s", file->rel_path);
					file->pagemap_isabsent = true;
				}
				else
				{

					if (start_addr + RELSEG_SIZE/HEAPBLOCKS_PER_BYTE > ptrack_nonparsed_size)
					{
						file->pagemap.bitmapsize = ptrack_nonparsed_size - start_addr;
						elog(VERBOSE, "pagemap size: %i", file->pagemap.bitmapsize);
					}
					else
					{
						file->pagemap.bitmapsize = RELSEG_SIZE/HEAPBLOCKS_PER_BYTE;
						elog(VERBOSE, "pagemap size: %i", file->pagemap.bitmapsize);
					}

					file->pagemap.bitmap = pg_malloc(file->pagemap.bitmapsize);
					memcpy(file->pagemap.bitmap, ptrack_nonparsed+start_addr, file->pagemap.bitmapsize);
				}
			}
			else
			{
				/*
				 * If ptrack file is missing, try to copy the entire file.
				 * It can happen in two cases:
				 * - files were created by commands that bypass buffer manager
				 * and, correspondingly, ptrack mechanism.
				 * i.e. CREATE DATABASE
				 * - target relation was deleted.
				 */
				elog(VERBOSE, "Ptrack is missing for file: %s", file->rel_path);
				file->pagemap_isabsent = true;
			}
		}
	}
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

	res_db = pgut_execute(backup_conn,
						  "SELECT extnamespace::regnamespace, extversion "
						  "FROM pg_catalog.pg_extension WHERE extname = 'ptrack'",
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
							  "SELECT proname FROM pg_proc WHERE proname='ptrack_version'",
							  0, NULL);

		if (PQntuples(res_db) == 0)
		{
			/* ptrack is not supported */
			PQclear(res_db);
			return;
		}

		res_db = pgut_execute(backup_conn,
							  "SELECT pg_catalog.ptrack_version()",
							  0, NULL);
		if (PQntuples(res_db) == 0)
		{
			/* TODO: Something went wrong, should we error out here? */
			PQclear(res_db);
			return;
		}
		ptrack_version_str = PQgetvalue(res_db, 0, 0);
	}

	if (strcmp(ptrack_version_str, "1.5") == 0)
		nodeInfo->ptrack_version_num = 15;
	else if (strcmp(ptrack_version_str, "1.6") == 0)
		nodeInfo->ptrack_version_num = 16;
	else if (strcmp(ptrack_version_str, "1.7") == 0)
		nodeInfo->ptrack_version_num = 17;
	else if (strcmp(ptrack_version_str, "2.0") == 0)
		nodeInfo->ptrack_version_num = 20;
	else if (strcmp(ptrack_version_str, "2.1") == 0)
		nodeInfo->ptrack_version_num = 21;
	else
		elog(WARNING, "Update your ptrack to the version 2.1 or upper. Current version is %s",
			 ptrack_version_str);

	/* ptrack 1.X is buggy, so fall back to DELTA backup strategy for safety */
	if (nodeInfo->ptrack_version_num >= 15 && nodeInfo->ptrack_version_num < 20)
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
pg_ptrack_enable(PGconn *backup_conn, int ptrack_version_num)
{
	PGresult   *res_db;
	bool		result = false;

	if (ptrack_version_num < 20)
	{
		res_db = pgut_execute(backup_conn, "SHOW ptrack_enable", 0, NULL);
		result = strcmp(PQgetvalue(res_db, 0, 0), "on") == 0;
	}
	else if (ptrack_version_num == 20)
	{
		res_db = pgut_execute(backup_conn, "SHOW ptrack_map_size", 0, NULL);
		result = strcmp(PQgetvalue(res_db, 0, 0), "0") != 0;
	}
	else
	{
		res_db = pgut_execute(backup_conn, "SHOW ptrack.map_size", 0, NULL);
		result = strcmp(PQgetvalue(res_db, 0, 0), "0") != 0 &&
				 strcmp(PQgetvalue(res_db, 0, 0), "-1") != 0;
	}

	PQclear(res_db);
	return result;
}


/* ----------------------------
 * Ptrack 1.* support functions
 * ----------------------------
 */

/* Clear ptrack files in all databases of the instance we connected to */
void
pg_ptrack_clear(PGconn *backup_conn, int ptrack_version_num)
{
	PGresult   *res_db,
			   *res;
	const char *dbname;
	int			i;
	Oid dbOid, tblspcOid;
	char *params[2];

	// FIXME Perform this check on caller's side
	if (ptrack_version_num >= 20)
		return;

	params[0] = palloc(64);
	params[1] = palloc(64);
	res_db = pgut_execute(backup_conn, "SELECT datname, oid, dattablespace FROM pg_database",
						  0, NULL);

	for(i = 0; i < PQntuples(res_db); i++)
	{
		PGconn	   *tmp_conn;

		dbname = PQgetvalue(res_db, i, 0);
		if (strcmp(dbname, "template0") == 0)
			continue;

		dbOid = atoll(PQgetvalue(res_db, i, 1));
		tblspcOid = atoll(PQgetvalue(res_db, i, 2));

		tmp_conn = pgut_connect(instance_config.conn_opt.pghost, instance_config.conn_opt.pgport,
								dbname,
								instance_config.conn_opt.pguser);

		res = pgut_execute(tmp_conn, "SELECT pg_catalog.pg_ptrack_clear()",
						   0, NULL);
		PQclear(res);

		sprintf(params[0], "%i", dbOid);
		sprintf(params[1], "%i", tblspcOid);
		res = pgut_execute(tmp_conn, "SELECT pg_catalog.pg_ptrack_get_and_clear_db($1, $2)",
						   2, (const char **)params);
		PQclear(res);

		pgut_disconnect(tmp_conn);
	}

	pfree(params[0]);
	pfree(params[1]);
	PQclear(res_db);
}

bool
pg_ptrack_get_and_clear_db(Oid dbOid, Oid tblspcOid, PGconn *backup_conn)
{
	char	   *params[2];
	char	   *dbname;
	PGresult   *res_db;
	PGresult   *res;
	bool		result;

	params[0] = palloc(64);
	params[1] = palloc(64);

	sprintf(params[0], "%i", dbOid);
	res_db = pgut_execute(backup_conn,
							"SELECT datname FROM pg_database WHERE oid=$1",
							1, (const char **) params);
	/*
	 * If database is not found, it's not an error.
	 * It could have been deleted since previous backup.
	 */
	if (PQntuples(res_db) != 1 || PQnfields(res_db) != 1)
		return false;

	dbname = PQgetvalue(res_db, 0, 0);

	/* Always backup all files from template0 database */
	if (strcmp(dbname, "template0") == 0)
	{
		PQclear(res_db);
		return true;
	}
	PQclear(res_db);

	sprintf(params[0], "%i", dbOid);
	sprintf(params[1], "%i", tblspcOid);
	res = pgut_execute(backup_conn, "SELECT pg_catalog.pg_ptrack_get_and_clear_db($1, $2)",
						2, (const char **)params);

	if (PQnfields(res) != 1)
		elog(ERROR, "cannot perform pg_ptrack_get_and_clear_db()");

	if (!parse_bool(PQgetvalue(res, 0, 0), &result))
		elog(ERROR,
			 "result of pg_ptrack_get_and_clear_db() is invalid: %s",
			 PQgetvalue(res, 0, 0));

	PQclear(res);
	pfree(params[0]);
	pfree(params[1]);

	return result;
}

/* Read and clear ptrack files of the target relation.
 * Result is a bytea ptrack map of all segments of the target relation.
 * case 1: we know a tablespace_oid, db_oid, and rel_filenode
 * case 2: we know db_oid and rel_filenode (no tablespace_oid, because file in pg_default)
 * case 3: we know only rel_filenode (because file in pg_global)
 */
char *
pg_ptrack_get_and_clear(Oid tablespace_oid, Oid db_oid, Oid rel_filenode,
						size_t *result_size, PGconn *backup_conn)
{
	PGconn	   *tmp_conn;
	PGresult   *res_db,
			   *res;
	char	   *params[2];
	char	   *result;
	char	   *val;

	params[0] = palloc(64);
	params[1] = palloc(64);

	/* regular file (not in directory 'global') */
	if (db_oid != 0)
	{
		char	   *dbname;

		sprintf(params[0], "%i", db_oid);
		res_db = pgut_execute(backup_conn,
							  "SELECT datname FROM pg_database WHERE oid=$1",
							  1, (const char **) params);
		/*
		 * If database is not found, it's not an error.
		 * It could have been deleted since previous backup.
		 */
		if (PQntuples(res_db) != 1 || PQnfields(res_db) != 1)
			return NULL;

		dbname = PQgetvalue(res_db, 0, 0);

		if (strcmp(dbname, "template0") == 0)
		{
			PQclear(res_db);
			return NULL;
		}

		tmp_conn = pgut_connect(instance_config.conn_opt.pghost, instance_config.conn_opt.pgport,
								dbname,
								instance_config.conn_opt.pguser);
		sprintf(params[0], "%i", tablespace_oid);
		sprintf(params[1], "%i", rel_filenode);
		res = pgut_execute(tmp_conn, "SELECT pg_catalog.pg_ptrack_get_and_clear($1, $2)",
						   2, (const char **)params);

		if (PQnfields(res) != 1)
			elog(ERROR, "cannot get ptrack file from database \"%s\" by tablespace oid %u and relation oid %u",
				 dbname, tablespace_oid, rel_filenode);
		PQclear(res_db);
		pgut_disconnect(tmp_conn);
	}
	/* file in directory 'global' */
	else
	{
		/*
		 * execute ptrack_get_and_clear for relation in pg_global
		 * Use backup_conn, cause we can do it from any database.
		 */
		sprintf(params[0], "%i", tablespace_oid);
		sprintf(params[1], "%i", rel_filenode);
		res = pgut_execute(backup_conn, "SELECT pg_catalog.pg_ptrack_get_and_clear($1, $2)",
						   2, (const char **)params);

		if (PQnfields(res) != 1)
			elog(ERROR, "cannot get ptrack file from pg_global tablespace and relation oid %u",
			 rel_filenode);
	}

	val = PQgetvalue(res, 0, 0);

	/* TODO Now pg_ptrack_get_and_clear() returns bytea ending with \x.
	 * It should be fixed in future ptrack releases, but till then we
	 * can parse it.
	 */
	if (strcmp("x", val+1) == 0)
	{
		/* Ptrack file is missing */
		return NULL;
	}

	result = (char *) PQunescapeBytea((unsigned char *) PQgetvalue(res, 0, 0),
									  result_size);
	PQclear(res);
	pfree(params[0]);
	pfree(params[1]);

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

	if (nodeInfo->ptrack_version_num < 20)
		res = pgut_execute(backup_conn, "SELECT pg_catalog.pg_ptrack_control_lsn()",
						   0, NULL);
	else
	{
		char query[128];

		if (nodeInfo->ptrack_version_num == 20)
			sprintf(query, "SELECT %s.pg_ptrack_control_lsn()", nodeInfo->ptrack_schema);
		else
			sprintf(query, "SELECT %s.ptrack_init_lsn()", nodeInfo->ptrack_schema);

		res = pgut_execute(backup_conn, query, 0, NULL);
	}

	/* Extract timeline and LSN from results of pg_start_backup() */
	XLogDataFromLSN(PQgetvalue(res, 0, 0), &lsn_hi, &lsn_lo);
	/* Calculate LSN */
	lsn = ((uint64) lsn_hi) << 32 | lsn_lo;

	PQclear(res);
	return lsn;
}

char *
pg_ptrack_get_block(ConnectionArgs *arguments,
					Oid dbOid,
					Oid tblsOid,
					Oid relOid,
					BlockNumber blknum,
					size_t *result_size,
					int ptrack_version_num,
					const char *ptrack_schema)
{
	PGresult   *res;
	char	   *params[4];
	char	   *result;

	params[0] = palloc(64);
	params[1] = palloc(64);
	params[2] = palloc(64);
	params[3] = palloc(64);

	/*
	 * Use tmp_conn, since we may work in parallel threads.
	 * We can connect to any database.
	 */
	sprintf(params[0], "%i", tblsOid);
	sprintf(params[1], "%i", dbOid);
	sprintf(params[2], "%i", relOid);
	sprintf(params[3], "%u", blknum);

	if (arguments->conn == NULL)
	{
		arguments->conn = pgut_connect(instance_config.conn_opt.pghost,
											  instance_config.conn_opt.pgport,
											  instance_config.conn_opt.pgdatabase,
											  instance_config.conn_opt.pguser);
	}

	if (arguments->cancel_conn == NULL)
		arguments->cancel_conn = PQgetCancel(arguments->conn);

	// elog(LOG, "db %i pg_ptrack_get_block(%i, %i, %u)",dbOid, tblsOid, relOid, blknum);

	if (ptrack_version_num < 20)
		res = pgut_execute_parallel(arguments->conn,
									arguments->cancel_conn,
						"SELECT pg_catalog.pg_ptrack_get_block_2($1, $2, $3, $4)",
						4, (const char **)params, true, false, false);
	else
	{
		char query[128];

		/* sanity */
		if (!ptrack_schema)
			elog(ERROR, "Schema name of ptrack extension is missing");

		if (ptrack_version_num == 20)
			sprintf(query, "SELECT %s.pg_ptrack_get_block($1, $2, $3, $4)", ptrack_schema);
		else
			elog(ERROR, "ptrack >= 2.1.0 does not support pg_ptrack_get_block()");
			// sprintf(query, "SELECT %s.ptrack_get_block($1, $2, $3, $4)", ptrack_schema);

		res = pgut_execute_parallel(arguments->conn,
									arguments->cancel_conn,
									query, 4, (const char **)params,
									true, false, false);
	}

	if (PQnfields(res) != 1)
	{
		elog(VERBOSE, "cannot get file block for relation oid %u",
					   relOid);
		return NULL;
	}

	if (PQgetisnull(res, 0, 0))
	{
		elog(VERBOSE, "cannot get file block for relation oid %u",
				   relOid);
		return NULL;
	}

	result = (char *) PQunescapeBytea((unsigned char *) PQgetvalue(res, 0, 0),
									  result_size);

	PQclear(res);

	pfree(params[0]);
	pfree(params[1]);
	pfree(params[2]);
	pfree(params[3]);

	return result;
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

	if (ptrack_version_num == 20)
		sprintf(query, "SELECT path, pagemap FROM %s.pg_ptrack_get_pagemapset($1) ORDER BY 1",
				ptrack_schema);
	else
		sprintf(query, "SELECT path, pagemap FROM %s.ptrack_get_pagemapset($1) ORDER BY 1",
				ptrack_schema);

	res = pgut_execute(backup_conn, query, 1, (const char **) params);
	pfree(params[0]);

	if (PQnfields(res) != 2)
		elog(ERROR, "cannot get ptrack pagemapset");

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
