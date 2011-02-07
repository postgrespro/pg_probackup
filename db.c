/*-------------------------------------------------------------------------
 *
 * db.c: SQLite3 access module
 *
 * Copyright (c) 2009-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_rman.h"

#include <sqlite3.h>
#include <time.h>

static const char *DB_CREATE_SQL[] =
{
	"CREATE TABLE backup (\n"
	"  id          integer,\n"
	"  status      integer,\n"
	"  mode        integer,\n"
	"  start_time  integer,\n"
	"  stop_time   integer,\n"
	"  timeline    integer,\n"
	"  start_xlog  integer,\n"
	"  stop_xlog   integer,\n"
	"  server_size integer,\n"
	"  dbfile_size integer,\n"
	"  arclog_size integer,\n"
	"  PRIMARY KEY (id)\n"
	")",

	"CREATE TABLE " DBFILE " (\n"
	"  id     integer,\n"
	"  name   text,\n"
	"  mtime  integer,\n"
	"  size   integer,\n"
	"  mode   integer,\n"
	"  flags  integer,\n"
	"  crc    integer,\n"
	"  PRIMARY KEY (id, name)\n"
	")",

	"CREATE TABLE " ARCLOG " (\n"
	"  name   text,\n"
	"  size   integer,\n"
	"  flags  integer,\n"
	"  crc    integer,\n"
	"  PRIMARY KEY (name)\n"
	")",

	NULL
};

static Database open_internal(int flags);
static void exec(Database db, const char *query);
static sqlite3_stmt *prepare(Database db, const char *query);
static void bind_int32(sqlite3_stmt *stmt, int n, int32 value);
static void bind_int64(sqlite3_stmt *stmt, int n, int64 value);
static void bind_size(sqlite3_stmt *stmt, int n, int64 value);
static void bind_text(sqlite3_stmt *stmt, int n, const char *value);
static void bind_xlog(sqlite3_stmt *stmt, int n, XLogName value);
static XLogName column_xlog(sqlite3_stmt *stmt, int n, TimeLineID tli);
static void step(sqlite3_stmt *stmt, int expected_code);
static void insert_dbfiles(Database db, int64 id, List *files);
static void insert_arclogs(Database db, List *files);

void
db_create(void)
{
	Database		db;
	int				i;

	db = open_internal(SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE);
	exec(db, "BEGIN EXCLUSIVE TRANSACTION");
	for (i = 0; DB_CREATE_SQL[i]; i++)
		exec(db, DB_CREATE_SQL[i]);
	exec(db, "COMMIT");
	sqlite3_close(db);
}

Database
db_open(void)
{
	return open_internal(SQLITE_OPEN_READWRITE);
}

void
db_close(Database db)
{
	sqlite3_close(db);
}

#define AVAIL_MASK		(BACKUP_MASK(BACKUP_DONE) | BACKUP_MASK(BACKUP_OK))

pgBackup *
db_start_backup(Database db, BackupMode mode)
{
	pgBackup	   *backup;
	sqlite3_stmt   *stmt;
	TimeLineID		tli = pgControlFile.checkPointCopy.ThisTimeLineID;
	int				i;

	Assert(tli != 0);

	/* start transaction with an exclusive lock */
	exec(db, "BEGIN EXCLUSIVE TRANSACTION");

	/* initialize backup status */
	backup = pgut_new(pgBackup);
	memset(backup, 0, sizeof(pgBackup));
	backup->status = BACKUP_ERROR;
	backup->mode = mode;
	backup->start_time = time(NULL);
	backup->server_size = -1;
	backup->dbfile_size = -1;
	backup->arclog_size = -1;

	/* retrieve a new id */
	stmt = prepare(db, "SELECT coalesce(max(id) + 1, 1) FROM backup");
	step(stmt, SQLITE_ROW);
	backup->id = sqlite3_column_int64(stmt, 0);
	sqlite3_finalize(stmt);

	/* retrieve a previous backup */
	stmt = prepare(db,
		"SELECT max(stop_xlog) FROM backup "
		"WHERE id < ? AND timeline = ? AND ((1 << status) & ?) <> 0 ");
	i = 0;
	bind_int64(stmt, ++i, backup->id);
	bind_int64(stmt, ++i, tli);
	bind_int32(stmt, ++i, AVAIL_MASK);
	step(stmt, SQLITE_ROW);

	backup->start_xlog = column_xlog(stmt, 0, tli);
	if (backup->start_xlog.tli == 0)
	{
		if (backup->mode < MODE_FULL)
			elog(INFO, "previous full backup not found. do a full backup instead");
		backup->mode = MODE_FULL;
	}
	else
	{
		/* goto the next segment */
		backup->start_xlog.seg++;
	}

	sqlite3_finalize(stmt);

	/* insert a backup row with 'ERROR' status */
	stmt = prepare(db,
		"INSERT INTO backup(id, status, mode, start_time) "
		"VALUES(?, ?, ?, ?)");
	i = 0;
	bind_int64(stmt, ++i, backup->id);
	bind_int32(stmt, ++i, backup->status);
	bind_int32(stmt, ++i, backup->mode);
	bind_int64(stmt, ++i, backup->start_time);
	step(stmt, SQLITE_DONE);
	sqlite3_finalize(stmt);

	/* ok, will start file copy */
	exec(db, "COMMIT");

	return backup;
}

void
db_stop_backup(Database db,
			   pgBackup *backup,
			   List *dbfiles,
			   List *arclogs)
{
	sqlite3_stmt   *stmt;
	int				i;

	if (backup->start_xlog.tli != backup->stop_xlog.tli)
		elog(ERROR, "invalid timeline");

	/* start transaction with an exclusive lock */
	exec(db, "BEGIN EXCLUSIVE TRANSACTION");

	insert_dbfiles(db, backup->id, dbfiles);
	insert_arclogs(db, arclogs);

	backup->stop_time = time(NULL);
	backup->status = BACKUP_DONE;

	/* update a backup status to 'DONE' */
	stmt = prepare(db,
		"UPDATE backup SET "
		"status = ?, mode = ?, stop_time = ?, "
		"timeline = ?, start_xlog = ?, stop_xlog = ?, "
		"server_size = ?, dbfile_size = ?, arclog_size = ? "
		"WHERE id = ?");
	i = 0;
	bind_int32(stmt, ++i, backup->status);
	bind_int32(stmt, ++i, backup->mode);
	bind_int64(stmt, ++i, backup->stop_time);
	bind_int64(stmt, ++i, backup->start_xlog.tli);
	bind_xlog(stmt, ++i, backup->start_xlog);
	bind_xlog(stmt, ++i, backup->stop_xlog);
	bind_size(stmt, ++i, backup->server_size);
	bind_size(stmt, ++i, backup->dbfile_size);
	bind_size(stmt, ++i, backup->arclog_size);
	bind_int64(stmt, ++i, backup->id);
	step(stmt, SQLITE_DONE);
	sqlite3_finalize(stmt);

	exec(db, "COMMIT");
}

/* insert dbfiles */
static void
insert_dbfiles(Database db, int64 id, List *files)
{
	ListCell	   *cell;
	sqlite3_stmt   *stmt;

	Assert(db);

	if (files == NIL)
		return;

	stmt = prepare(db, "INSERT INTO " DBFILE " VALUES(?, ?, ?, ?, ?, ?, ?)");

	foreach(cell, files)
	{
		const pgFile   *file = lfirst(cell);
		int				i;

		if (file->mode == MISSING_FILE)
			continue;

		i = 0;
		bind_int64(stmt, ++i, id);
		bind_text(stmt, ++i, file->name);
		bind_int64(stmt, ++i, file->mtime);
		bind_size(stmt, ++i, file->size);
		bind_int32(stmt, ++i, file->mode);
		bind_int32(stmt, ++i, (int32) file->flags);
		bind_int32(stmt, ++i, (int32) file->crc);
//		bind_text(stmt, ++i, file->linked);

		step(stmt, SQLITE_DONE);
		sqlite3_reset(stmt);
	}

	sqlite3_finalize(stmt);
}

/* insert dbfiles */
static void
insert_arclogs(Database db, List *files)
{
	ListCell	   *cell;
	sqlite3_stmt   *stmt;

	Assert(db);

	if (files == NIL)
		return;

	stmt = prepare(db, "INSERT INTO " ARCLOG " VALUES(?, ?, ?, ?)");

	foreach(cell, files)
	{
		const pgFile   *file = lfirst(cell);
		int				i;

		if (file->mode == MISSING_FILE)
			continue;

		i = 0;
		bind_text(stmt, ++i, file->name);
		bind_size(stmt, ++i, file->size);
		bind_int32(stmt, ++i, (int32) file->flags);
		bind_int32(stmt, ++i, (int32) file->crc);

		step(stmt, SQLITE_DONE);
		sqlite3_reset(stmt);
	}

	sqlite3_finalize(stmt);
}

void
db_check_modified(Database db, List *files)
{
	ListCell	   *cell;
	sqlite3_stmt   *stmt = NULL;

	foreach (cell, files)
	{
		pgFile		   *file = lfirst(cell);

		if (S_ISREG(file->mode) && (file->flags & PGFILE_PARTIAL) != 0)
		{
			time_t	mtime;

			if (stmt == NULL)
				stmt = prepare(db,
					"SELECT max(mtime) FROM " DBFILE " WHERE name = ?");

			bind_text(stmt, 1, file->name);
			step(stmt, SQLITE_ROW);

			/* NULL is converted to 0 */
			mtime = (time_t) sqlite3_column_int64(stmt, 0);

			if (file->mtime == mtime)
				file->flags |= PGFILE_UNMODIFIED;

			sqlite3_reset(stmt);
		}
	}

	if (stmt)
		sqlite3_finalize(stmt);
}

#define SELECT_FROM_BACKUP \
	"SELECT id, status, mode, start_time, stop_time, " \
	"timeline, start_xlog, stop_xlog, " \
	"coalesce(server_size, -1), " \
	"coalesce(dbfile_size, -1), " \
	"coalesce(arclog_size, -1) " \
	"FROM backup "

static List *
list_backups(sqlite3_stmt *stmt)
{
	List	   *backups = NIL;
	TimeLineID	timeline;

	while (sqlite3_step(stmt) == SQLITE_ROW)
	{
		pgBackup   *backup = pgut_new(pgBackup);
		int			i;

		memset(backup, 0, sizeof(pgBackup));
		i = 0;
		backup->id = sqlite3_column_int64(stmt, i++);
		backup->status = sqlite3_column_int64(stmt, i++);
		backup->mode = sqlite3_column_int(stmt, i++);
		backup->start_time = sqlite3_column_int64(stmt, i++);
		backup->stop_time = sqlite3_column_int64(stmt, i++);
		timeline = (TimeLineID) sqlite3_column_int64(stmt, i++);
		backup->start_xlog = column_xlog(stmt, i++, timeline);
		backup->stop_xlog = column_xlog(stmt, i++, timeline);
		backup->server_size = sqlite3_column_int64(stmt, i++);
		backup->dbfile_size = sqlite3_column_int64(stmt, i++);
		backup->arclog_size =  sqlite3_column_int64(stmt, i++);

		backups = lappend(backups, backup);
	}
	sqlite3_finalize(stmt);

	return backups;
}

List *
db_list_backups(Database db, pgRange range, bits32 mask)
{
	sqlite3_stmt   *stmt;
	int				i;
	
	stmt = prepare(db,
		SELECT_FROM_BACKUP
		"WHERE ? <= start_time AND start_time < ? "
		"AND ((1 << status) & ?) <> 0 "
		"ORDER BY id");
	i = 0;
	bind_int64(stmt, ++i, range.begin);
	bind_int64(stmt, ++i, range.end);
	bind_int32(stmt, ++i, mask);

	return list_backups(stmt);
}

/* return a full backup and successive incremental backups to recover to tli */
List *
db_list_backups_for_restore(Database db,
							time_t target_time,
							TimeLineID target_tli)
{
	sqlite3_stmt   *stmt;
	int				i;

#if 0
	/* is the backup is necessary for restore to target timeline ? */
	if (!satisfy_timeline(timelines, backup))
		continue;
	timelines = readTimeLineHistory(target_tli);

static bool
satisfy_timeline(const parray *timelines, const pgBackup *backup)
{
	int i;
	for (i = 0; i < parray_num(timelines); i++)
	{
		pgTimeLine *timeline = (pgTimeLine *) parray_get(timelines, i);
		if (backup->tli == timeline->tli &&
				XLByteLT(backup->stop_xlog, timeline->end))
			return true;
	}
	return false;
}
#endif



	stmt = prepare(db,
		SELECT_FROM_BACKUP
		"WHERE ((1 << status) & ?) <> 0 AND start_time < ? "
		"AND id >= (SELECT max(id) FROM backup "
					"WHERE ((1 << status) & ?) <> 0 AND start_time < ? " 
					"AND mode = ? AND ? IN (0, timeline) ) "
		"ORDER BY id");
	i = 0;
	bind_int32(stmt, ++i, AVAIL_MASK);
	bind_int64(stmt, ++i, target_time);
	bind_int32(stmt, ++i, AVAIL_MASK);
	bind_int64(stmt, ++i, target_time);
	bind_int32(stmt, ++i, MODE_FULL);
	bind_int64(stmt, ++i, target_tli);

	return list_backups(stmt);
}

List *
db_list_dbfiles(Database db, const pgBackup *backup)
{
	List		   *files = NIL;
	sqlite3_stmt   *stmt;
	int				i;
	char			root[MAXPGPATH];

	make_backup_path(root, backup->start_time);

	stmt = prepare(db,
		"SELECT name, mtime, size, mode, flags, crc FROM " DBFILE
		" WHERE id = ? ORDER BY name");
	i = 0;
	bind_int64(stmt, ++i, backup->id);

	while (sqlite3_step(stmt) == SQLITE_ROW)
	{
		pgFile	   *file;

		i = 0;
		file = pgFile_new((const char *) sqlite3_column_text(stmt, i++));
		file->mtime = (time_t) sqlite3_column_int64(stmt, i++);
		file->size = sqlite3_column_int64(stmt, i++);
		file->mode = (mode_t) sqlite3_column_int(stmt, i++);
		file->flags = sqlite3_column_int(stmt, i++);
		file->crc = (pg_crc32) sqlite3_column_int(stmt, i++);

		files = lappend(files, file);
	}
	sqlite3_finalize(stmt);

	return files;
}

static pgFile *
newMissingXLog(XLogName xlog)
{
	pgFile *file;
	char	name[XLOGNAMELEN];

	xlog_name(name, xlog);

	file = pgFile_new(name);
	file->size = 0;
	file->mode = S_IFREG;
	file->flags = 0;
	file->crc = 0;

	return file;
}

/*
 * list arclogs required by backup, or list all if backup is NULL.
 * The result includes all of required xlog files whenbackup specified even if
 * the xlog file is not in the catalog.
 */
List *
db_list_arclogs(Database db, const pgBackup *backup)
{
	List		   *files = NIL;
	sqlite3_stmt   *stmt;
	int				i;
	char			lo[XLOGNAMELEN];
	char			hi[XLOGNAMELEN];
	XLogName		next;
	XLogName		stop;

	if (backup)
	{
		next = backup->start_xlog;
		stop = backup->stop_xlog;
	}
	else
	{
		next.tli = next.log = next.seg = 0x00000000;
		stop.tli = stop.log = stop.seg = 0xFFFFFFFF;
	}
	xlog_name(lo, backup->start_xlog);
	xlog_name(hi, xlog_next(backup->stop_xlog));

	stmt = prepare(db,
		"SELECT name, size, flags, crc "
		"FROM " ARCLOG " WHERE ? <= name AND name < ? "
		"ORDER BY name");
	i = 0;
	bind_text(stmt, ++i, lo);
	bind_text(stmt, ++i, hi);

	while (sqlite3_step(stmt) == SQLITE_ROW)
	{
		pgFile	   *file;
		const char *name;

		i = 0;
		name = (const char *) sqlite3_column_text(stmt, i++);

		/* add missing arclogs */
		if (strlen(name) == 24 &&
			strspn(name, "0123456789ABCDEF") == 24)
		{
			XLogName xlog = parse_xlogname(name);
			while (xlog.tli == next.tli && (xlog.log <= next.log || xlog.seg < xlog.seg))
			{
				files = lappend(files, newMissingXLog(xlog));
				xlog = xlog_next(xlog);
			}
			next = xlog_next(xlog);
		}

		file = pgFile_new(name);
		file->size = sqlite3_column_int64(stmt, i++);
		file->mode = S_IFREG;
		file->flags = sqlite3_column_int(stmt, i++);
		file->crc = (pg_crc32) sqlite3_column_int(stmt, i++);
		files = lappend(files, file);
	}
	sqlite3_finalize(stmt);

	/* add missing arclogs */
	while (next.tli == stop.tli && (next.log <= stop.log || next.seg < stop.seg))
	{
		files = lappend(files, newMissingXLog(next));
		next = xlog_next(next);
	}

	return files;
}

void
db_update_status(Database db, const pgBackup *backup, List *arglogs)
{
	sqlite3_stmt   *stmt;
	ListCell	   *cell;
	int				i;

	/* start transaction with an exclusive lock */
	exec(db, "BEGIN EXCLUSIVE TRANSACTION");

	if (backup->status == BACKUP_DELETED)
	{
		/* delete files */
		stmt = prepare(db, "DELETE FROM " DBFILE " WHERE id = ?");
		i = 0;
		bind_int64(stmt, ++i, backup->id);
		step(stmt, SQLITE_DONE);
		sqlite3_finalize(stmt);

		/* delete the backup */
		stmt = prepare(db, "DELETE FROM backup WHERE id = ?");
		i = 0;
		bind_int64(stmt, ++i, backup->id);
		step(stmt, SQLITE_DONE);
		sqlite3_finalize(stmt);
	}
	else
	{
		/* update the backup status */
		stmt = prepare(db, "UPDATE backup SET status = ? WHERE id = ?");
		i = 0;
		bind_int32(stmt, ++i, backup->status);
		bind_int64(stmt, ++i, backup->id);
		step(stmt, SQLITE_DONE);
		sqlite3_finalize(stmt);
	}

	/* update status of archive logs */
	stmt = NULL;
	foreach (cell, arglogs)
	{
		const pgFile *file = lfirst(cell);

		if (file->flags & PGFILE_VERIFIED)
		{
			if (stmt == NULL)
				stmt = prepare(db,
					"UPDATE " ARCLOG " SET flags = ?, crc = ? WHERE name = ?");
			i = 0;
			bind_int32(stmt, ++i, (int32) file->flags);
			bind_int32(stmt, ++i, (int32) file->crc);
			bind_text(stmt, ++i, file->name);
			step(stmt, SQLITE_DONE);
			sqlite3_reset(stmt);
		}
	}
	if (stmt != NULL)
		sqlite3_finalize(stmt);

	exec(db, "COMMIT");
}

static Database
open_internal(int flags)
{
	Database	db;
	char		path[MAXPGPATH];

	join_path_components(path, backup_path, PG_RMAN_DATABASE);
	if (sqlite3_open_v2(path, &db, flags, NULL) != SQLITE_OK)
		elog(ERROR, "could not create database \"%s\": %s",
			 path, sqlite3_errmsg(db));

	return db;
}

static void
exec(Database db, const char *query)
{
	char *msg;

	if (sqlite3_exec(db, query, NULL, NULL, &msg) != SQLITE_OK)
		elog(ERROR, "could not execute query \"%s\": %s", query, msg);
}

static sqlite3_stmt *
prepare(Database db, const char *query)
{
	sqlite3_stmt   *stmt;
	int				code;
	
	if ((code = sqlite3_prepare_v2(db, query, -1, &stmt, NULL)) != SQLITE_OK)
		elog(ERROR, "could not prepare query \"%s\": %s",
			 query, sqlite3_errmsg(db));

	return stmt;
}

/* for int32, pg_crc32 and mode_t */
static void
bind_int32(sqlite3_stmt *stmt, int n, int32 value)
{
	int	code = sqlite3_bind_int(stmt, n, value);
	if (code != SQLITE_OK)
		elog(ERROR, "could not bind a parameter: code %d", code);
}

/* for int64 and time_t */
static void
bind_int64(sqlite3_stmt *stmt, int n, int64 value)
{
	int	code = sqlite3_bind_int64(stmt, n, value);
	if (code != SQLITE_OK)
		elog(ERROR, "could not bind a parameter: code %d", code);
}

/* for size_t */
static void
bind_size(sqlite3_stmt *stmt, int n, int64 value)
{
	if (value < 0)
		sqlite3_bind_null(stmt, n);
	else
		bind_int64(stmt, n, value);
}

/* for static null-terminated text */
static void
bind_text(sqlite3_stmt *stmt, int n, const char *value)
{
	int	code = sqlite3_bind_text(stmt, n, value, -1, SQLITE_STATIC);
	if (code != SQLITE_OK)
		elog(ERROR, "could not bind a parameter: code %d", code);
}

static void
bind_xlog(sqlite3_stmt *stmt, int n, XLogName value)
{
	if (value.tli == 0)
		sqlite3_bind_null(stmt, n);
	else
		bind_int64(stmt, n, (((int64) value.log) << 32 | value.seg));
}

/* return 0/0/0 if the input is null */
static XLogName
column_xlog(sqlite3_stmt *stmt, int n, TimeLineID tli)
{
	XLogName	xlog;

	if (tli == 0 || sqlite3_column_type(stmt, n) != SQLITE_INTEGER)
		memset(&xlog, 0, sizeof(xlog));
	else
	{
		int64	value = sqlite3_column_int64(stmt, n);

		xlog.tli = tli;
		xlog.log = (uint32) ((value >> 32) & 0xFFFFFFFF);
		xlog.seg = (uint32) (value & 0xFFFFFFFF);
	}

	return xlog;
}

static void
step(sqlite3_stmt *stmt, int expected_code)
{
	int	code = sqlite3_step(stmt);
	if (code != expected_code)
		elog(ERROR, "unexpected result in step: code %d", code);
}
