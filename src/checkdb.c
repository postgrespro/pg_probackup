/*-------------------------------------------------------------------------
 *
 * src/checkdb.c
 * pg_probackup checkdb subcommand
 *
 * It allows to validate all data files located in PGDATA
 * via block checksums matching and page header sanity checks.
 * Optionally all indexes in all databases in PostgreSQL
 * instance can be logically verified using extensions
 * amcheck or amcheck_next.
 *
 * Portions Copyright (c) 2019-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "utils/thread.h"
#include "utils/file.h"


typedef struct
{
	/* list of files to validate */
	parray	   *files_list;
	/* if page checksums are enabled in this postgres instance? */
	uint32 checksum_version;
	/*
	 * conn and cancel_conn
	 * to use in check_data_file
	 * to connect to postgres if we've failed to validate page
	 * and want to read it via buffer cache to ensure
	 */
	ConnectionArgs conn_arg;
	/* number of thread for debugging */
	int			thread_num;
	/* pgdata path */
	const char	*from_root;
	/*
	 * Return value from the thread:
	 * 0 everything is ok
	 * 1 thread errored during execution, e.g. interruption (default value)
	 * 2 corruption is definitely(!) found
	 */
	int			ret;
} check_files_arg;


typedef struct
{
	/* list of indexes to amcheck */
	parray	   *index_list;
	/*
	 * credentials to connect to postgres instance
	 * used for compatibility checks of blocksize,
	 * server version and so on
	 */
	ConnectionOptions conn_opt;
	/*
	 * conn and cancel_conn
	 * to use in threads to connect to databases
	 */
	ConnectionArgs conn_arg;
	/* number of thread for debugging */
	int			thread_num;
	/*
	 * Return value from the thread:
	 * 0 everything is ok
	 * 1 thread errored during execution, e.g. interruption (default value)
	 * 2 corruption is definitely(!) found
	 */
	int			ret;
} check_indexes_arg;

typedef struct pg_indexEntry
{
	Oid indexrelid;
	char *name;
	char *namespace;
	bool heapallindexed_is_supported;
	/* schema where amcheck extension is located */
	char *amcheck_nspname;
	/* lock for synchronization of parallel threads  */
	volatile pg_atomic_flag lock;
} pg_indexEntry;

static void
pg_indexEntry_free(void *index)
{
	pg_indexEntry *index_ptr;

	if (index == NULL)
		return;

	index_ptr = (pg_indexEntry *) index;

	if (index_ptr->name)
		free(index_ptr->name);
	if (index_ptr->name)
		free(index_ptr->namespace);
	if (index_ptr->amcheck_nspname)
		free(index_ptr->amcheck_nspname);

	free(index_ptr);
}


static void *check_files(void *arg);
static void do_block_validation(char *pgdata, uint32 checksum_version);

static void *check_indexes(void *arg);
static parray* get_index_list(const char *dbname, bool first_db_with_amcheck,
							  PGconn *db_conn);
static bool amcheck_one_index(check_indexes_arg *arguments,
				 pg_indexEntry *ind);
static void do_amcheck(ConnectionOptions conn_opt, PGconn *conn);

/*
 * Check files in PGDATA.
 * Read all files listed in files_list.
 * If the file is 'datafile' (regular relation's main fork), read it page by page,
 * verify checksum and copy.
 */
static void *
check_files(void *arg)
{
	int			i;
	check_files_arg *arguments = (check_files_arg *) arg;
	int			n_files_list = 0;
	char		from_fullpath[MAXPGPATH];

	if (arguments->files_list)
		n_files_list = parray_num(arguments->files_list);

	/* check a file */
	for (i = 0; i < n_files_list; i++)
	{
		pgFile	   *file = (pgFile *) parray_get(arguments->files_list, i);

		/* check for interrupt */
		if (interrupted || thread_interrupted)
			elog(ERROR, "interrupted during checkdb");

		/* No need to check directories */
		if (S_ISDIR(file->mode))
			continue;

		if (!pg_atomic_test_set_flag(&file->lock))
			continue;

		join_path_components(from_fullpath, arguments->from_root, file->rel_path);

		elog(VERBOSE, "Checking file:  \"%s\" ", from_fullpath);

		if (progress)
			elog(INFO, "Progress: (%d/%d). Process file \"%s\"",
				 i + 1, n_files_list, from_fullpath);

		if (S_ISREG(file->mode))
		{
			/* check only uncompressed by cfs datafiles */
			if (file->is_datafile && !file->is_cfs)
			{
				/*
				 * TODO deep inside check_data_file
				 * uses global variables to set connections.
				 * Need refactoring.
				 */
				if (!check_data_file(&(arguments->conn_arg),
									 file, from_fullpath,
									 arguments->checksum_version))
					arguments->ret = 2; /* corruption found */
			}
		}
		else
			elog(WARNING, "unexpected file type %d", file->mode);
	}

	/* Ret values:
	 * 0 everything is ok
	 * 1 thread errored during execution, e.g. interruption (default value)
	 * 2 corruption is definitely(!) found
	 */
	if (arguments->ret == 1)
		arguments->ret = 0;

	return NULL;
}

/* collect list of files and run threads to check files in the instance */
static void
do_block_validation(char *pgdata, uint32 checksum_version)
{
	int			i;
	/* arrays with meta info for multi threaded check */
	pthread_t	*threads;
	check_files_arg *threads_args;
	bool		check_isok = true;
	parray *files_list = NULL;

	/* initialize file list */
	files_list = parray_new();

	/* list files with the logical path. omit $PGDATA */
	dir_list_file(files_list, pgdata, true, true,
				  false, false, true, 0, FIO_DB_HOST);

	/*
	 * Sort pathname ascending.
	 *
	 * For example:
	 * 1 - create 'base'
	 * 2 - create 'base/1'
	 */
	parray_qsort(files_list, pgFileCompareRelPathWithExternal);
	/* Extract information about files in pgdata parsing their names:*/
	parse_filelist_filenames(files_list, pgdata);

	/* setup threads */
	for (i = 0; i < parray_num(files_list); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(files_list, i);
		pg_atomic_init_flag(&file->lock);
	}

	/* Sort by size for load balancing */
	parray_qsort(files_list, pgFileCompareSize);

	/* init thread args with own file lists */
	threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
	threads_args = (check_files_arg *) palloc(sizeof(check_files_arg)*num_threads);

	for (i = 0; i < num_threads; i++)
	{
		check_files_arg *arg = &(threads_args[i]);

		arg->files_list = files_list;
		arg->checksum_version = checksum_version;
		arg->from_root = pgdata;

		arg->conn_arg.conn = NULL;
		arg->conn_arg.cancel_conn = NULL;

		arg->thread_num = i + 1;
		/* By default there is some error */
		arg->ret = 1;
	}

	elog(INFO, "Start checking data files");

	/* Run threads */
	for (i = 0; i < num_threads; i++)
	{
		check_files_arg *arg = &(threads_args[i]);

		elog(VERBOSE, "Start thread num: %i", i);

		pthread_create(&threads[i], NULL, check_files, arg);
	}

	/* Wait threads */
	for (i = 0; i < num_threads; i++)
	{
		pthread_join(threads[i], NULL);
		if (threads_args[i].ret > 0)
			check_isok = false;
	}

	/* cleanup */
	if (files_list)
	{
		parray_walk(files_list, pgFileFree);
		parray_free(files_list);
		files_list = NULL;
	}

	if (check_isok)
		elog(INFO, "Data files are valid");
	else
		elog(ERROR, "Checkdb failed");
}

/* Check indexes with amcheck */
static void *
check_indexes(void *arg)
{
	int			i;
	check_indexes_arg *arguments = (check_indexes_arg *) arg;
	int			n_indexes = 0;
	my_thread_num = arguments->thread_num;

	if (arguments->index_list)
		n_indexes = parray_num(arguments->index_list);

	for (i = 0; i < n_indexes; i++)
	{
		pg_indexEntry *ind = (pg_indexEntry *) parray_get(arguments->index_list, i);

		if (!pg_atomic_test_set_flag(&ind->lock))
			continue;

		/* check for interrupt */
		if (interrupted || thread_interrupted)
			elog(ERROR, "Thread [%d]: interrupted during checkdb --amcheck",
				arguments->thread_num);

		if (progress)
			elog(INFO, "Thread [%d]. Progress: (%d/%d). Amchecking index '%s.%s'",
				 arguments->thread_num, i + 1, n_indexes,
				 ind->namespace, ind->name);

		if (arguments->conn_arg.conn == NULL)
		{

			arguments->conn_arg.conn = pgut_connect(arguments->conn_opt.pghost,
												arguments->conn_opt.pgport,
												arguments->conn_opt.pgdatabase,
												arguments->conn_opt.pguser);
			arguments->conn_arg.cancel_conn = PQgetCancel(arguments->conn_arg.conn);
		}

		/* remember that we have a failed check */
		if (!amcheck_one_index(arguments, ind))
			arguments->ret = 2; /* corruption found */
	}

	/* Close connection. */
	if (arguments->conn_arg.conn)
		pgut_disconnect(arguments->conn_arg.conn);

	/* Ret values:
	 * 0 everything is ok
	 * 1 thread errored during execution, e.g. interruption (default value)
	 * 2 corruption is definitely(!) found
	 */
	if (arguments->ret == 1)
		arguments->ret = 0;

	return NULL;
}

/* Get index list for given database */
static parray*
get_index_list(const char *dbname, bool first_db_with_amcheck,
			   PGconn *db_conn)
{
	PGresult   *res;
	char *amcheck_nspname = NULL;
	int i;
	bool heapallindexed_is_supported = false;
	parray *index_list = NULL;

	res = pgut_execute(db_conn, "SELECT "
								"extname, nspname, extversion "
								"FROM pg_namespace n "
								"JOIN pg_extension e "
								"ON n.oid=e.extnamespace "
								"WHERE e.extname IN ('amcheck', 'amcheck_next') "
								"ORDER BY extversion DESC "
								"LIMIT 1",
								0, NULL);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		elog(ERROR, "Cannot check if amcheck is installed in database %s: %s",
			dbname, PQerrorMessage(db_conn));
	}

	if (PQntuples(res) < 1)
	{
		elog(WARNING, "Extension 'amcheck' or 'amcheck_next' are "
					  "not installed in database %s", dbname);
		return NULL;
	}

	amcheck_nspname = pgut_malloc(strlen(PQgetvalue(res, 0, 1)) + 1);
	strcpy(amcheck_nspname, PQgetvalue(res, 0, 1));

	/* heapallindexed_is_supported is database specific */
	if (strcmp(PQgetvalue(res, 0, 2), "1.0") != 0 &&
		strcmp(PQgetvalue(res, 0, 2), "1") != 0)
			heapallindexed_is_supported = true;

	elog(INFO, "Amchecking database '%s' using extension '%s' "
			   "version %s from schema '%s'",
				dbname, PQgetvalue(res, 0, 0), 
				PQgetvalue(res, 0, 2), PQgetvalue(res, 0, 1));

	if (!heapallindexed_is_supported && heapallindexed)
		elog(WARNING, "Extension '%s' version %s in schema '%s'"
					  "do not support 'heapallindexed' option",
					   PQgetvalue(res, 0, 0), PQgetvalue(res, 0, 2),
					   PQgetvalue(res, 0, 1));

	/*
	 * In order to avoid duplicates, select global indexes
	 * (tablespace pg_global with oid 1664) only once.
	 *
	 * select only persistent btree indexes.
	 */
	if (first_db_with_amcheck)
	{

		res = pgut_execute(db_conn, "SELECT cls.oid, cls.relname, nmspc.nspname "
									"FROM pg_catalog.pg_index idx "
									"LEFT JOIN pg_catalog.pg_class cls ON idx.indexrelid=cls.oid "
									"LEFT JOIN pg_catalog.pg_namespace nmspc ON cls.relnamespace=nmspc.oid "
									"LEFT JOIN pg_catalog.pg_am am ON cls.relam=am.oid "
									"WHERE am.amname='btree' AND cls.relpersistence != 't' "
									"ORDER BY nmspc.nspname DESC",
									0, NULL);
	}
	else
	{

		res = pgut_execute(db_conn, "SELECT cls.oid, cls.relname, nmspc.nspname "
									"FROM pg_catalog.pg_index idx "
									"LEFT JOIN pg_catalog.pg_class cls ON idx.indexrelid=cls.oid "
									"LEFT JOIN pg_catalog.pg_namespace nmspc ON cls.relnamespace=nmspc.oid "
									"LEFT JOIN pg_catalog.pg_am am ON cls.relam=am.oid "
									"WHERE am.amname='btree' AND cls.relpersistence != 't' AND "
									"(cls.reltablespace IN "
									"(SELECT oid from pg_catalog.pg_tablespace where spcname <> 'pg_global') "
									"OR cls.reltablespace = 0) "
									"ORDER BY nmspc.nspname DESC",
									0, NULL);
	}

	/* add info needed to check indexes into index_list */
	for (i = 0; i < PQntuples(res); i++)
	{
		pg_indexEntry *ind = (pg_indexEntry *) pgut_malloc(sizeof(pg_indexEntry));
		char *name = NULL;
		char *namespace = NULL;

		/* index oid */
		ind->indexrelid = atoll(PQgetvalue(res, i, 0));

		/* index relname */
		name = PQgetvalue(res, i, 1);
		ind->name = pgut_malloc(strlen(name) + 1);
		strcpy(ind->name, name);	/* enough buffer size guaranteed */

		/* index namespace */
		namespace = PQgetvalue(res, i, 2);
		ind->namespace = pgut_malloc(strlen(namespace) + 1);
		strcpy(ind->namespace, namespace);	/* enough buffer size guaranteed */

		ind->heapallindexed_is_supported = heapallindexed_is_supported;
		ind->amcheck_nspname = pgut_malloc(strlen(amcheck_nspname) + 1);
		strcpy(ind->amcheck_nspname, amcheck_nspname);
		pg_atomic_clear_flag(&ind->lock);

		if (index_list == NULL)
			index_list = parray_new();

		parray_append(index_list, ind);
	}

	PQclear(res);

	return index_list;
}

/* check one index. Return true if everything is ok, false otherwise. */
static bool
amcheck_one_index(check_indexes_arg *arguments,
				 pg_indexEntry *ind)
{
	PGresult   *res;
	char		*params[2];
	char		*query = NULL;

	params[0] = palloc(64);

	/* first argument is index oid */
	sprintf(params[0], "%u", ind->indexrelid);
	/* second argument is heapallindexed */
	params[1] = heapallindexed ? "true" : "false";

	if (interrupted)
		elog(ERROR, "Interrupted");

	if (ind->heapallindexed_is_supported)
	{
		query = palloc(strlen(ind->amcheck_nspname)+strlen("SELECT .bt_index_check($1, $2)")+1);
		sprintf(query, "SELECT %s.bt_index_check($1, $2)", ind->amcheck_nspname);

		res = pgut_execute_parallel(arguments->conn_arg.conn,
								arguments->conn_arg.cancel_conn,
								query, 2, (const char **)params, true, true, true);
	}
	else
	{
		query = palloc(strlen(ind->amcheck_nspname)+strlen("SELECT .bt_index_check($1)")+1);
		sprintf(query, "SELECT %s.bt_index_check($1)", ind->amcheck_nspname);

		res = pgut_execute_parallel(arguments->conn_arg.conn,
								arguments->conn_arg.cancel_conn,
								query, 1, (const char **)params, true, true, true);
	}

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		elog(WARNING, "Thread [%d]. Amcheck failed in database '%s' for index: '%s.%s': %s",
					   arguments->thread_num, arguments->conn_opt.pgdatabase,
					   ind->namespace, ind->name, PQresultErrorMessage(res));

		pfree(params[0]);
		pfree(query);
		PQclear(res);
		return false;
	}
	else
		elog(LOG, "Thread [%d]. Amcheck succeeded in database '%s' for index: '%s.%s'",
				arguments->thread_num,
				arguments->conn_opt.pgdatabase, ind->namespace, ind->name);

	pfree(params[0]);
	pfree(query);
	PQclear(res);
	return true;
}

/*
 * Entry point of checkdb --amcheck.
 *
 * Connect to all databases in the cluster
 * and get list of persistent indexes,
 * then run parallel threads to perform bt_index_check()
 * for all indexes from the list.
 *
 * If amcheck extension is not installed in the database,
 * skip this database and report it via warning message.
 */
static void
do_amcheck(ConnectionOptions conn_opt, PGconn *conn)
{
	int			i;
	/* arrays with meta info for multi threaded amcheck */
	pthread_t	*threads;
	check_indexes_arg *threads_args;
	bool		check_isok = true;
	PGresult   *res_db;
	int n_databases = 0;
	bool first_db_with_amcheck = true;
	bool db_skipped = false;

	elog(INFO, "Start amchecking PostgreSQL instance");

	res_db = pgut_execute(conn,
						"SELECT datname, oid, dattablespace "
						"FROM pg_database "
						"WHERE datname NOT IN ('template0', 'template1')",
						  0, NULL);

	/* we don't need this connection anymore */
	if (conn)
		pgut_disconnect(conn);

	n_databases =  PQntuples(res_db);

	/* For each database check indexes. In parallel. */
	for(i = 0; i < n_databases; i++)
	{
		int j;
		const char 	*dbname;
		PGconn 		*db_conn = NULL;
		parray 		*index_list = NULL;

		dbname = PQgetvalue(res_db, i, 0);
		db_conn = pgut_connect(conn_opt.pghost, conn_opt.pgport,
								dbname, conn_opt.pguser);

		index_list = get_index_list(dbname, first_db_with_amcheck,
									db_conn);

		/* we don't need this connection anymore */
		if (db_conn)
			pgut_disconnect(db_conn);

		if (index_list == NULL)
		{
			db_skipped = true;
			continue;
		}

		first_db_with_amcheck = false;

		/* init thread args with own index lists */
		threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
		threads_args = (check_indexes_arg *) palloc(sizeof(check_indexes_arg)*num_threads);

		for (j = 0; j < num_threads; j++)
		{
			check_indexes_arg *arg = &(threads_args[j]);

			arg->index_list = index_list;
			arg->conn_arg.conn = NULL;
			arg->conn_arg.cancel_conn = NULL;

			arg->conn_opt.pghost = conn_opt.pghost;
			arg->conn_opt.pgport = conn_opt.pgport;
			arg->conn_opt.pgdatabase = dbname;
			arg->conn_opt.pguser = conn_opt.pguser;

			arg->thread_num = j + 1;
			/* By default there are some error */
			arg->ret = 1;
		}

		/* Run threads */
		for (j = 0; j < num_threads; j++)
		{
			check_indexes_arg *arg = &(threads_args[j]);
			elog(VERBOSE, "Start thread num: %i", j);
			pthread_create(&threads[j], NULL, check_indexes, arg);
		}

		/* Wait threads */
		for (j = 0; j < num_threads; j++)
		{
			pthread_join(threads[j], NULL);
			if (threads_args[j].ret > 0)
				check_isok = false;
		}

		if (check_isok)
			elog(INFO, "Amcheck succeeded for database '%s'", dbname);
		else
			elog(WARNING, "Amcheck failed for database '%s'", dbname);

		parray_walk(index_list, pg_indexEntry_free);
		parray_free(index_list);

		if (interrupted)
			break;
	}

	/* cleanup */
	PQclear(res_db);

	/* Inform user about amcheck results */
	if (interrupted)
		elog(ERROR, "checkdb --amcheck is interrupted.");

	if (check_isok)
	{
		elog(INFO, "checkdb --amcheck finished successfully. "
					   "All checked indexes are valid.");

		if (db_skipped)
			elog(ERROR, "Some databases were not amchecked.");
		else
			elog(INFO, "All databases were amchecked.");
	}
	else
		elog(ERROR, "checkdb --amcheck finished with failure. "
					"Not all checked indexes are valid. %s",
					db_skipped?"Some databases were not amchecked.":
							   "All databases were amchecked.");
}

/* Entry point of pg_probackup CHECKDB subcommand */
void
do_checkdb(bool need_amcheck,
		   ConnectionOptions conn_opt, char *pgdata)
{
	PGNodeInfo nodeInfo;
	PGconn *cur_conn;

	/* Initialize PGInfonode */
	pgNodeInit(&nodeInfo);

	if (skip_block_validation && !need_amcheck)
		elog(ERROR, "Option '--skip-block-validation' must be used with '--amcheck' option");

	if (!skip_block_validation)
	{
		if (!pgdata)
			elog(ERROR, "required parameter not specified: PGDATA "
						 "(-D, --pgdata)");

		/* get node info */
		cur_conn = pgdata_basic_setup(conn_opt, &nodeInfo);

		/* ensure that conn credentials and pgdata are consistent */
		check_system_identifiers(cur_conn, pgdata);

		/*
		 * we don't need this connection anymore.
		 * block validation can last long time,
		 * so we don't hold the connection open,
		 * rather open new connection for amcheck
		 */
		if (cur_conn)
			pgut_disconnect(cur_conn);

		do_block_validation(pgdata, nodeInfo.checksum_version);
	}

	if (need_amcheck)
	{
		cur_conn = pgdata_basic_setup(conn_opt, &nodeInfo);
		do_amcheck(conn_opt, cur_conn);
	}
}
