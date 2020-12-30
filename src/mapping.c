/*-------------------------------------------------------------------------
 *
 *  mapping.c: various mapping utilities
 * 
 * This file contains functions to handle:
 *
 * - tablespace mapping
 * - external directories mapping
 * - database mapping (for partial restore)
 *
 * Portions Copyright (c) 2015-2020, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */
#include "pg_probackup.h"
#include "utils/file.h"


#if PG_VERSION_NUM < 110000
#include "catalog/catalog.h"
#endif
#include "catalog/pg_tablespace.h"

#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>

#include "utils/configuration.h"

/* ===== tablespace mapping ===== */
/*
 * Retrieve tablespace path, either relocated or original depending on whether
 * -T was passed or not.
 *
 * Based on function get_tablespace_mapping() from pg_basebackup.c.
 */
const char *
get_tablespace_mapping(const char *dir, TablespaceList tablespace_dirs)
{
	TablespaceListCell *cell;

	for (cell = tablespace_dirs.head; cell; cell = cell->next)
		if (strcmp(dir, cell->old_dir) == 0)
			return cell->new_dir;

	return dir;
}

/*
 * Read names of symbolic names of tablespaces with links to directories from
 * tablespace_map or tablespace_map.txt.
 */
void
read_tablespace_map(parray *files, const char *backup_dir)
{
	FILE	   *fp;
	char		database_dir[MAXPGPATH];
	char		map_path[MAXPGPATH];
	char		buf[MAXPGPATH * 2];

	join_path_components(database_dir, backup_dir, DATABASE_DIR);
	join_path_components(map_path, database_dir, PG_TABLESPACE_MAP_FILE);

	/* Exit if database/tablespace_map doesn't exist */
	if (!fileExists(map_path, FIO_BACKUP_HOST))
	{
		elog(LOG, "there is no file tablespace_map");
		return;
	}

	fp = fio_open_stream(map_path, FIO_BACKUP_HOST);
	if (fp == NULL)
		elog(ERROR, "cannot open \"%s\": %s", map_path, strerror(errno));

	while (fgets(buf, lengthof(buf), fp))
	{
		char		link_name[MAXPGPATH],
					path[MAXPGPATH];
		pgFile	   *file;

		if (sscanf(buf, "%1023s %1023s", link_name, path) != 2)
			elog(ERROR, "invalid format found in \"%s\"", map_path);

		file = pgut_new(pgFile);
		memset(file, 0, sizeof(pgFile));

		/* follow the convention for pgFileFree */
		file->name = pgut_strdup(link_name);
		file->linked = pgut_strdup(path);
		canonicalize_path(file->linked);

		parray_append(files, file);
	}

	if (ferror(fp))
			elog(ERROR, "Failed to read from file: \"%s\"", map_path);

	fio_close_stream(fp);
}

/*
 * Check that all tablespace mapping entries have correct linked directory
 * paths. Linked directories must be empty or do not exist, unless
 * we are running incremental restore, then linked directories can be nonempty.
 *
 * If tablespace-mapping option is supplied, all OLDDIR entries must have
 * entries in tablespace_map file.
 *
 *
 * TODO: maybe when running incremental restore with tablespace remapping, then
 * new tablespace directory MUST be empty? because there is no way
 * we can be sure, that files laying there belong to our instance.
 */
void
check_tablespace_mapping(pgBackup *backup, TablespaceList tablespace_dirs,
					 bool incremental, bool *tblspaces_are_empty)
{
//	char		this_backup_path[MAXPGPATH];
	parray	   *links;
	size_t		i;
	TablespaceListCell *cell;
	pgFile	   *tmp_file = pgut_new(pgFile);

	links = parray_new();

	read_tablespace_map(links, backup->root_dir);
	/* Sort links by the path of a linked file*/
	parray_qsort(links, pgFileCompareLinked);

	elog(LOG, "check tablespace directories of backup %s",
			base36enc(backup->start_time));

	/* 1 - each OLDDIR must have an entry in tablespace_map file (links) */
	for (cell = tablespace_dirs.head; cell; cell = cell->next)
	{
		tmp_file->linked = cell->old_dir;

		if (parray_bsearch(links, tmp_file, pgFileCompareLinked) == NULL)
			elog(ERROR, "--tablespace-mapping option's old directory "
				 "doesn't have an entry in tablespace_map file: \"%s\"",
				 cell->old_dir);

		/* For incremental restore, check that new directory is empty */
//		if (incremental)
//		{
//			if (!is_absolute_path(cell->new_dir))
//				elog(ERROR, "tablespace directory is not an absolute path: %s\n",
//					 cell->new_dir);
//
//			if (!dir_is_empty(cell->new_dir, FIO_DB_HOST))
//				elog(ERROR, "restore tablespace destination is not empty: \"%s\"",
//					 cell->new_dir);
//		}
	}

	/* 2 - all linked directories must be empty */
	for (i = 0; i < parray_num(links); i++)
	{
		pgFile	   *link = (pgFile *) parray_get(links, i);
		const char *linked_path = link->linked;
		TablespaceListCell *cell;

		for (cell = tablespace_dirs.head; cell; cell = cell->next)
			if (strcmp(link->linked, cell->old_dir) == 0)
			{
				linked_path = cell->new_dir;
				break;
			}

		if (!is_absolute_path(linked_path))
			elog(ERROR, "tablespace directory is not an absolute path: %s\n",
				 linked_path);

		if (!dir_is_empty(linked_path, FIO_DB_HOST))
		{
			if (!incremental)
				elog(ERROR, "restore tablespace destination is not empty: \"%s\"",
					 linked_path);
			*tblspaces_are_empty = false;
		}
	}

	free(tmp_file);
	parray_walk(links, pgFileFree);
	parray_free(links);
}

/*
 * Write a tablespace map returned by pg_stop_backup() for a given backup
 * into backup catalog. Add its meta to backup_files_list
 *
 * NULL check for tablespace_map must be done by the caller.
 */
void
write_tablespace_map(pgBackup *backup, char *tablespace_map,
                     parray *backup_files_list)
{
	FILE		*fp;
	pgFile		*file;
	char		tablespace_map_path[MAXPGPATH];
	size_t len = 0;
	int i;

	join_path_components(tablespace_map_path, backup->database_dir, PG_TABLESPACE_MAP_FILE);

	fp = fio_fopen(tablespace_map_path, PG_BINARY_W, FIO_BACKUP_HOST);
	if (fp == NULL)
		elog(ERROR, "Cannot open tablespace map \"%s\": %s", tablespace_map_path,
			 strerror(errno));

    /* print tablespace map to file */
    len = strlen(tablespace_map);
    if(fio_fwrite(fp, tablespace_map, len) != len)
		elog(ERROR, "can't write tablespace map file \"%s\": %s",
    			tablespace_map_path, strerror(errno));

	if (fio_fflush(fp) || fio_fclose(fp))
	{
		fio_unlink(tablespace_map_path, FIO_BACKUP_HOST);
		elog(ERROR, "Cannot write tablespace map \"%s\": %s",
			 tablespace_map_path, strerror(errno));
	}

	/* Add metadata to backup_content.control */
	file = pgFileNew(tablespace_map_path, PG_TABLESPACE_MAP_FILE, true, 0,
								 FIO_BACKUP_HOST);
	file->crc = pgFileGetCRC(tablespace_map_path, true, false);
	file->write_size = file->size;
	file->uncompressed_size = file->size;

	parray_append(backup_files_list, file);
}
/* ===== tablespace mapping (END) ===== */

/* ===== external directories mapping ===== */
char *
get_external_remap(char *current_dir, TablespaceList external_remap_list)
{
	TablespaceListCell *cell;

	for (cell = external_remap_list.head; cell; cell = cell->next)
	{
		char *old_dir = cell->old_dir;

		if (strcmp(old_dir, current_dir) == 0)
			return cell->new_dir;
	}
	return current_dir;
}

void
check_external_dir_mapping(pgBackup *backup, TablespaceList external_remap_list, bool incremental)
{
	TablespaceListCell *cell;
	parray *external_dirs_to_restore;
	int		i;

	elog(LOG, "check external directories of backup %s",
			base36enc(backup->start_time));

	if (!backup->external_dir_str)
	{
	 	if (external_remap_list.head)
			elog(ERROR, "--external-mapping option's old directory doesn't "
				 "have an entry in list of external directories of current "
				 "backup: \"%s\"", external_remap_list.head->old_dir);
		return;
	}

	external_dirs_to_restore = make_external_directory_list(
													backup->external_dir_str,
													NULL);
	/* 1 - each OLDDIR must have an entry in external_dirs_to_restore */
	for (cell = external_remap_list.head; cell; cell = cell->next)
	{
		bool		found = false;

		for (i = 0; i < parray_num(external_dirs_to_restore); i++)
		{
			char	    *external_dir = parray_get(external_dirs_to_restore, i);

			if (strcmp(cell->old_dir, external_dir) == 0)
			{
				/* Swap new dir name with old one, it is used by 2-nd step */
				parray_set(external_dirs_to_restore, i,
						   pgut_strdup(cell->new_dir));
				pfree(external_dir);

				found = true;
				break;
			}
		}
		if (!found)
			elog(ERROR, "--external-mapping option's old directory doesn't "
				 "have an entry in list of external directories of current "
				 "backup: \"%s\"", cell->old_dir);
	}

	/* 2 - all linked directories must be empty */
	for (i = 0; i < parray_num(external_dirs_to_restore); i++)
	{
		char	    *external_dir = (char *) parray_get(external_dirs_to_restore,
														i);

		if (!incremental && !dir_is_empty(external_dir, FIO_DB_HOST))
			elog(ERROR, "External directory is not empty: \"%s\"",
				 external_dir);
	}

	free_dir_list(external_dirs_to_restore);
}

/* ===== external directories mapping (END) ===== */



/* ===== database mapping ===== */

void
db_map_entry_free(void *entry)
{
	db_map_entry *m = (db_map_entry *) entry;

	free(m->datname);
	free(entry);
}

/*
 * Get a database map using given connection
 *
 * This function can fail to get the map for legal reasons, e.g. missing
 * permissions on pg_database during `backup`.
 * As long as user do not use partial restore feature it`s fine.
 *
 * To avoid breaking a backward compatibility don't throw an ERROR,
 * throw a warning instead of an error and return NULL.
 * Caller is responsible for checking the result.
 */
parray *
get_database_map(PGconn *conn)
{
	PGresult   *res;
	parray *database_map = NULL;
	int i;

	/*
	 * Do not include template0 and template1 to the map
	 * as default databases that must always be restored.
	 */
	res = pgut_execute_extended(conn,
						  "SELECT oid, datname FROM pg_catalog.pg_database "
						  "WHERE datname NOT IN ('template1', 'template0')",
						  0, NULL, true, true);

	/* Don't error out, simply return NULL. See comment above. */
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		elog(WARNING, "Failed to get database map: %s",
			PQerrorMessage(conn));

		return NULL;
	}

	/* Construct database map */
	for (i = 0; i < PQntuples(res); i++)
	{
		char *datname = NULL;
		db_map_entry *db_entry = (db_map_entry *) pgut_malloc(sizeof(db_map_entry));

		/* get Oid */
		db_entry->dbOid = atoll(PQgetvalue(res, i, 0));

		/* get datname */
		datname = PQgetvalue(res, i, 1);
		db_entry->datname = pgut_malloc(strlen(datname) + 1);
		strcpy(db_entry->datname, datname);

		if (database_map == NULL)
			database_map = parray_new();

		parray_append(database_map, db_entry);
	}

	return database_map;
}

/*
 * Read a database map for a given backup from backup catalog
 * return NULL if database_map in empty or missing
 */
parray *
read_database_map(pgBackup *backup)
{
	FILE		*fp;
	parray 		*database_map;
	char		buf[MAXPGPATH];
	char		database_map_path[MAXPGPATH];

	join_path_components(database_map_path, backup->database_dir, DATABASE_MAP);

	fp = fio_open_stream(database_map_path, FIO_BACKUP_HOST);
	if (fp == NULL)
	{
		/* It is NOT ok for database_map to be missing at this point, so
		 * we should error here.
		 * It`s a job of the caller to error if database_map is not empty.
		 */
		elog(ERROR, "Cannot open \"%s\": %s", database_map_path, strerror(errno));
	}

	database_map = parray_new();

	while (fgets(buf, lengthof(buf), fp))
	{
		char datname[MAXPGPATH];
		int64 dbOid;

		db_map_entry *db_entry = (db_map_entry *) pgut_malloc(sizeof(db_map_entry));

		get_control_value(buf, "dbOid", NULL, &dbOid, true);
		get_control_value(buf, "datname", datname, NULL, true);

		db_entry->dbOid = dbOid;
		db_entry->datname = pgut_strdup(datname);

		parray_append(database_map, db_entry);
	}

	if (ferror(fp))
			elog(ERROR, "Failed to read from file: \"%s\"", database_map_path);

	fio_close_stream(fp);

	/* Return NULL if file is empty */
	if (parray_num(database_map) == 0)
	{
		parray_free(database_map);
		return NULL;
	}

	return database_map;
}

/*
 * Write a database map for a given backup into backup catalog.
 * Add its meta to backup_files_list
 *
 * NULL check for database_map must be done by the caller.
 */
void
write_database_map(pgBackup *backup, parray *database_map, parray *backup_files_list)
{
	FILE		*fp;
	pgFile		*file;
	char		database_dir[MAXPGPATH];
	char		database_map_path[MAXPGPATH];
	int i;

	join_path_components(database_map_path, backup->database_dir, DATABASE_MAP);

	fp = fio_fopen(database_map_path, PG_BINARY_W, FIO_BACKUP_HOST);
	if (fp == NULL)
		elog(ERROR, "Cannot open database map \"%s\": %s", database_map_path,
			 strerror(errno));

    /* print database map to file */
	for (i = 0; i < parray_num(database_map); i++)
	{
		db_map_entry *db_entry = (db_map_entry *) parray_get(database_map, i);

		fio_fprintf(fp, "{\"dbOid\":\"%u\", \"datname\":\"%s\"}\n",
				db_entry->dbOid, db_entry->datname);
	}

	if (fio_fflush(fp) || fio_fclose(fp))
	{
		fio_unlink(database_map_path, FIO_BACKUP_HOST);
		elog(ERROR, "Cannot write database map \"%s\": %s",
			 database_map_path, strerror(errno));
	}

	/* Add metadata to backup_content.control */
	file = pgFileNew(database_map_path, DATABASE_MAP, true, 0,
								 FIO_BACKUP_HOST);
	file->crc = pgFileGetCRC(database_map_path, true, false);
	file->write_size = file->size;
	file->uncompressed_size = file->size;

	parray_append(backup_files_list, file);
}

/* ===== database mapping (END) ===== */

/*
 * Write a backup label returned by pg_stop_backup() for a given backup
 * into backup catalog. Add its meta to backup_files_list
 *
 * NULL check must be done by the caller.
 * TODO: find some better place for this function
 */
void
write_backup_label(pgBackup *backup, char *backup_label,
                     parray *backup_files_list)
{
	FILE		*fp;
	pgFile		*file;
	char		backup_label_path[MAXPGPATH];
	size_t len = 0;
	int i;

	join_path_components(backup_label_path, backup->database_dir, PG_BACKUP_LABEL_FILE);

	fp = fio_fopen(backup_label_path, PG_BINARY_W, FIO_BACKUP_HOST);
	if (fp == NULL)
		elog(ERROR, "Cannot open backup label \"%s\": %s", backup_label_path,
			 strerror(errno));

    /* print tablespace map to file */
    len = strlen(backup_label);
 	if (fio_fwrite(fp, backup_label, len) != len)
		elog(ERROR, "can't write backup label file \"%s\": %s",
                    backup_label_path, strerror(errno));

	if (fio_fflush(fp) || fio_fclose(fp))
	{
		fio_unlink(backup_label_path, FIO_BACKUP_HOST);
		elog(ERROR, "Cannot write backup label \"%s\": %s",
			 backup_label_path, strerror(errno));
	}

	/* Add metadata to backup_content.control */
	file = pgFileNew(backup_label_path, PG_BACKUP_LABEL_FILE, true, 0,
								 FIO_BACKUP_HOST);
	file->crc = pgFileGetCRC(backup_label_path, true, false);
	file->write_size = file->size;
	file->uncompressed_size = file->size;

	parray_append(backup_files_list, file);
}