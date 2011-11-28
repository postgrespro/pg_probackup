/*-------------------------------------------------------------------------
 *
 * verify.c: verify backup files.
 *
 * Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_rman.h"

#include <unistd.h>

static bool verify_files(List *files, const char *root);
static bool verify_file(pgFile *file, const char *root);

typedef struct VerifyJob
{
	void		  (*routine)(struct VerifyJob *);
	pgFile		   *file;
	const char	   *root;
	volatile bool  *ok;
} VerifyJob;

/* copy the file into backup */
static void
verify_routine(VerifyJob *job)
{
	if (*job->ok && !verify_file(job->file, job->root))
		*job->ok = false;
}

#define VERIFY_MASK		(BACKUP_MASK(BACKUP_DONE))

void
do_verify(pgRange range)
{
	Database	db;
	List	   *backups;
	ListCell   *cell;

	db = db_open();
	backups = db_list_backups(db, range, VERIFY_MASK);

	foreach (cell, backups)
		verify_backup(db, lfirst(cell));

	db_close(db);
	list_free_deep(backups);
}

/*
 * Verify files in the backup and update the status to OK or BAD.
 */
void
verify_backup(Database db, pgBackup *backup)
{
	char		root[MAXPGPATH];
	char		datetime[DATESTRLEN];
	List	   *dbfiles;
	List	   *arclogs = NIL;
	bool		ok;

	if (backup->status == BACKUP_OK)
		return;	/* already verified */

	elog(INFO, "verify: %s", date2str(datetime, backup->start_time));
	make_backup_path(root, backup->start_time);

	/* Verify data files. */
	dbfiles = db_list_dbfiles(db, backup);
	ok = verify_files(dbfiles, root);
	list_destroy(dbfiles, pgFile_free);

	/* Verify archive log files. */
	arclogs = db_list_arclogs(db, backup);
	ok = ok && verify_files(arclogs, root);

	/* update the status to OK or BAD */
	backup->status = (ok ? BACKUP_OK : BACKUP_BAD);
	db_update_status(db, backup, arclogs);
	list_destroy(arclogs, pgFile_free);

	if (!ok)
		elog(WARNING, "corrupted backup: %s", datetime);
}

static bool
verify_files(List *files, const char *root)
{
	volatile bool	ok = true;
	ListCell   *cell;

	foreach (cell, files)
	{
		pgFile *file = lfirst(cell);
		VerifyJob *job;

		CHECK_FOR_INTERRUPTS();

		if (!ok)
			break;

		job = pgut_new(VerifyJob);
		job->routine = verify_routine;
		job->file = file;
		job->root = root;
		job->ok = &ok;
		job_push((Job *) job);
	}

	job_wait();

	return ok;
}

/*
 * Verify files in the backup with size or CRC.
 */
static bool
verify_file(pgFile *file, const char *root)
{
	FILE	   *fp;
	pg_crc32	crc;
	size_t		len;
	char		path[MAXPGPATH];
	char		buf[8291];

	/* skipped or already verified file */
	if (file->flags & (PGFILE_UNMODIFIED | PGFILE_VERIFIED))
		return true;

	/* not a file */
	if (!S_ISREG(file->mode))
	{
		/* XXX: check if exists? */
		return true;
	}

	elog(LOG, "verify file: %s", file->name);

	/*
	 * Exit on error and don't mark the backup in corrupted status to
	 * let users retry verification.
	 */
	join_path_components(path, root, file->name);
	if ((fp = pgut_fopen(path, "r+")) == NULL)
	{
		elog(WARNING, "missing file \"%s\"", path);
		return false;
	}

	/* Does file have correct crc? */
	INIT_CRC32(crc);
	while ((len = fread(buf, 1, sizeof(buf), fp)) > 0)
	{
		CHECK_FOR_INTERRUPTS();
		COMP_CRC32(crc, buf, len);
	}
	FIN_CRC32(crc);

	/* Update crc if not calclated yet. */
	if ((file->flags & PGFILE_CRC) == 0)
	{
		file->crc = crc;
		file->flags |= PGFILE_CRC;
	}
	else if (file->crc != crc)
	{
		fclose(fp);
		elog(WARNING, "corrupted file \"%s\"", path);
		return false;
	}

	/* Flush backup files into disk */
	if (fflush(fp) != 0 || fsync(fileno(fp)) != 0)
		ereport(ERROR,
			(errcode_errno(),
			 errmsg("could not flush file \"%s\": ", path)));

	fclose(fp);
	file->flags |= PGFILE_VERIFIED;
	return true;
}
