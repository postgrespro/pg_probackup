/*-------------------------------------------------------------------------
 *
 * archive.c: -  pg_probackup specific archive commands for archive backups.
 *
 *
 * Portions Copyright (c) 2018-2022, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include <unistd.h>
#include "pg_probackup.h"
#include "utils/thread.h"
#include "portability/instr_time.h"

static err_i push_file_internal(const char *wal_file_name,
                              const char *pg_xlog_dir,
                              const char *archive_dir,
                              bool overwrite, bool no_sync,
                              bool is_compress, int compress_level,
                              uint32 archive_timeout, bool *skipped);
static void *push_files(void *arg);
static void *get_files(void *arg);
static bool get_wal_file(const char *filename, const char *from_path, const char *to_path,
													bool prefetch_mode);
//static void copy_file_attributes(const char *from_path,
//								 fio_location from_location,
//								 const char *to_path, fio_location to_location,
//								 bool unlink_on_error);

static bool next_wal_segment_exists(TimeLineID tli, XLogSegNo segno, const char *prefetch_dir, uint32 wal_seg_size);
static uint32 run_wal_prefetch(const char *prefetch_dir, const char *archive_dir, TimeLineID tli,
							   XLogSegNo first_segno, int num_threads, bool inclusive, int batch_size,
							   uint32 wal_seg_size);
static bool wal_satisfy_from_prefetch(TimeLineID tli, XLogSegNo segno, const char *wal_file_name,
									  const char *prefetch_dir, const char *absolute_wal_file_path,
									  uint32 wal_seg_size, bool parse_wal);

static uint32 maintain_prefetch(const char *prefetch_dir, XLogSegNo first_segno, uint32 wal_seg_size);

static bool prefetch_stop = false;
static uint32 xlog_seg_size;

typedef struct
{
	const char *first_filename;
	const char *pg_xlog_dir;
	const char *archive_dir;
	const char *archive_status_dir;
	bool        overwrite;
	bool        compress;
	bool        no_sync;
	bool        no_ready_rename;
	uint32      archive_timeout;

	CompressAlg compress_alg;
	int         compress_level;
	int         thread_num;

	parray     *files;

	uint32      n_pushed;
	uint32      n_skipped;

	/*
	 * Return value from the thread.
	 * 0 means there is no error,
	 * 1 - there is an error.
	 * 2 - no error, but nothing to push
	 */
	int         ret;
} archive_push_arg;

typedef struct
{
	const char *prefetch_dir;
	const char *archive_dir;
	int         thread_num;
	parray     *files;
	uint32      n_fetched;
} archive_get_arg;

typedef struct WALSegno
{
	char        name[MAXFNAMELEN];
	volatile    pg_atomic_flag lock;
} WALSegno;

static int push_file(WALSegno *xlogfile, const char *archive_status_dir,
								   const char *pg_xlog_dir, const char *archive_dir,
								   bool overwrite, bool no_sync, uint32 archive_timeout,
								   bool no_ready_rename, bool is_compress,
								   int compress_level);

static parray *setup_push_filelist(const char *archive_status_dir,
								   const char *first_file, int batch_size);

/*
 * At this point, we already done one roundtrip to archive server
 * to get instance config.
 *
 * pg_probackup specific archive command for archive backups
 * set archive_command to
 * 'pg_probackup archive-push -B /home/anastasia/backup --wal-file-name %f',
 * to move backups into arclog_path.
 * Where archlog_path is $BACKUP_PATH/wal/instance_name
 */
void
do_archive_push(InstanceState *instanceState, InstanceConfig *instance, char *pg_xlog_dir,
				char *wal_file_name, int batch_size, bool overwrite,
				bool no_sync, bool no_ready_rename)
{
	uint64		i;
	/* usually instance pgdata/pg_wal/archive_status, empty if no_ready_rename or batch_size == 1 */
	char		archive_status_dir[MAXPGPATH] = "";
	bool		is_compress = false;

	/* arrays with meta info for multi threaded backup */
	pthread_t	*threads;
	archive_push_arg *threads_args;
	bool		push_isok = true;

	/* reporting */
	uint32      n_total_pushed = 0;
	uint32      n_total_skipped = 0;
	uint32      n_total_failed = 0;
	instr_time  start_time, end_time;
	double      push_time;
	char        pretty_time_str[20];

	/* files to push in multi-thread mode */
	parray     *batch_files = NULL;
	int         n_threads;

	if (!no_ready_rename || batch_size > 1)
		join_path_components(archive_status_dir, pg_xlog_dir, "archive_status");

#ifdef HAVE_LIBZ
	if (instance->compress_alg == ZLIB_COMPRESS)
		is_compress = true;
#endif

	/*  Setup filelist and locks */
	batch_files = setup_push_filelist(archive_status_dir, wal_file_name, batch_size);

	n_threads = num_threads;
	if (num_threads > parray_num(batch_files))
		n_threads = parray_num(batch_files);

	elog(INFO, "pg_probackup archive-push WAL file: %s, "
					"threads: %i/%i, batch: %zu/%i, compression: %s",
						wal_file_name, n_threads, num_threads,
						parray_num(batch_files), batch_size,
						is_compress ? "zlib" : "none");

	num_threads = n_threads;

	/* Single-thread push
	 * We don`t want to start multi-thread push, if number of threads in equal to 1,
	 * or the number of files ready to push is small.
	 * Multithreading in remote mode isn`t cheap,
	 * establishing ssh connection can take 100-200ms, so running and terminating
	 * one thread using generic multithread approach can take
	 * almost as much time as copying itself.
	 * TODO: maybe we should be more conservative and force single thread
	 * push if batch_files array is small.
	 */
	if (num_threads == 1 || (parray_num(batch_files) == 1))
	{
		INSTR_TIME_SET_CURRENT(start_time);
		for (i = 0; i < parray_num(batch_files); i++)
		{
			int rc;
			WALSegno *xlogfile = (WALSegno *) parray_get(batch_files, i);
			bool first_wal = strcmp(xlogfile->name, wal_file_name) == 0;

			rc = push_file(xlogfile, first_wal ? NULL : archive_status_dir,
						   pg_xlog_dir, instanceState->instance_wal_subdir_path,
						   overwrite, no_sync,
						   instance->archive_timeout,
						   no_ready_rename || first_wal,
						   is_compress && IsXLogFileName(xlogfile->name) ? true : false,
						   instance->compress_level);
			if (rc == 0)
				n_total_pushed++;
			else
				n_total_skipped++;
		}

		push_isok = true;
		goto push_done;
	}

	/* init thread args with its own segno */
	threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
	threads_args = (archive_push_arg *) palloc(sizeof(archive_push_arg) * num_threads);

	for (i = 0; i < num_threads; i++)
	{
		archive_push_arg *arg = &(threads_args[i]);

		arg->first_filename = wal_file_name;
		arg->archive_dir = instanceState->instance_wal_subdir_path;
		arg->pg_xlog_dir = pg_xlog_dir;
		arg->archive_status_dir = (!no_ready_rename || batch_size > 1) ? archive_status_dir : NULL;
		arg->overwrite = overwrite;
		arg->compress = is_compress;
		arg->no_sync = no_sync;
		arg->no_ready_rename = no_ready_rename;
		arg->archive_timeout = instance->archive_timeout;

		arg->compress_alg = instance->compress_alg;
		arg->compress_level = instance->compress_level;

		arg->files = batch_files;
		arg->n_pushed = 0;
		arg->n_skipped = 0;

		arg->thread_num = i+1;
		/* By default there are some error */
		arg->ret = 1;
	}

	/* Run threads */
	INSTR_TIME_SET_CURRENT(start_time);
	for (i = 0; i < num_threads; i++)
	{
		archive_push_arg *arg = &(threads_args[i]);
		pthread_create(&threads[i], NULL, push_files, arg);
	}

	/* Wait threads */
	for (i = 0; i < num_threads; i++)
	{
		pthread_join(threads[i], NULL);
		if (threads_args[i].ret == 1)
		{
			push_isok = false;
			n_total_failed++;
		}

		n_total_pushed += threads_args[i].n_pushed;
		n_total_skipped += threads_args[i].n_skipped;
	}

	/* Note, that we are leaking memory here,
	 * because pushing into archive is a very
	 * time-sensitive operation, so we skip freeing stuff.
	 */

push_done:
	fio_disconnect();
	/* calculate elapsed time */
	INSTR_TIME_SET_CURRENT(end_time);
	INSTR_TIME_SUBTRACT(end_time, start_time);
	push_time = INSTR_TIME_GET_DOUBLE(end_time);
	pretty_time_interval(push_time, pretty_time_str, 20);

	if (push_isok)
		/* report number of files pushed into archive */
		elog(INFO, "pg_probackup archive-push completed successfully, "
					"pushed: %u, skipped: %u, time elapsed: %s",
					n_total_pushed, n_total_skipped, pretty_time_str);
	else
		elog(ERROR, "pg_probackup archive-push failed, "
					"pushed: %i, skipped: %u, failed: %u, time elapsed: %s",
					n_total_pushed, n_total_skipped, n_total_failed,
					pretty_time_str);
}

/* ------------- INTERNAL FUNCTIONS ---------- */
/*
 * Copy files from pg_wal to archive catalog with possible compression.
 */
static void *
push_files(void *arg)
{
	int		i;
	int		rc;
	archive_push_arg *args = (archive_push_arg *) arg;

	set_my_thread_num(args->thread_num);

	for (i = 0; i < parray_num(args->files); i++)
	{
		bool      no_ready_rename = args->no_ready_rename;
		WALSegno *xlogfile = (WALSegno *) parray_get(args->files, i);

		if (!pg_atomic_test_set_flag(&xlogfile->lock))
			continue;

		/* Do not rename ready file of the first file,
		 * we do this to avoid flooding PostgreSQL log with
		 * warnings about ready file been missing.
		 */
		if (strcmp(args->first_filename, xlogfile->name) == 0)
			no_ready_rename = true;

		rc = push_file(xlogfile, args->archive_status_dir,
					   args->pg_xlog_dir, args->archive_dir,
					   args->overwrite, args->no_sync,
					   args->archive_timeout, no_ready_rename,
					   /* do not compress .backup, .partial and .history files */
					   args->compress && IsXLogFileName(xlogfile->name) ? true : false,
					   args->compress_level);

		if (rc == 0)
			args->n_pushed++;
		else
			args->n_skipped++;
	}

	/* close ssh connection */
	fio_disconnect();

	args->ret = 0;
	return NULL;
}

int
push_file(WALSegno *xlogfile, const char *archive_status_dir,
		  const char *pg_xlog_dir, const char *archive_dir,
		  bool overwrite, bool no_sync, uint32 archive_timeout,
		  bool no_ready_rename, bool is_compress,
		  int compress_level)
{
	bool  skipped = false;
	err_i err;

	elog(LOG, "pushing file \"%s\"", xlogfile->name);

	err = push_file_internal(xlogfile->name, pg_xlog_dir,
							archive_dir, overwrite, no_sync,
							is_compress, compress_level,
						    archive_timeout, &skipped);
	if ($haserr(err))
	{
		ft_logerr(FT_ERROR, $errmsg(err), "Archiving %s", xlogfile->name);
	}

	/* take '--no-ready-rename' flag into account */
	if (!no_ready_rename && archive_status_dir != NULL)
	{
		char	wal_file_dummy[MAXPGPATH];
		char	wal_file_ready[MAXPGPATH];
		char	wal_file_done[MAXPGPATH];

		join_path_components(wal_file_dummy, archive_status_dir, xlogfile->name);
		snprintf(wal_file_ready, MAXPGPATH, "%s.%s", wal_file_dummy, "ready");
		snprintf(wal_file_done, MAXPGPATH, "%s.%s", wal_file_dummy, "done");

		canonicalize_path(wal_file_ready);
		canonicalize_path(wal_file_done);
		/* It is ok to rename status file in archive_status directory */
		elog(LOG, "Rename \"%s\" to \"%s\"", wal_file_ready, wal_file_done);

		/* do not error out, if rename failed */
		if (fio_rename(FIO_DB_HOST, wal_file_ready, wal_file_done) < 0)
			elog(WARNING, "Cannot rename ready file \"%s\" to \"%s\": %s",
				wal_file_ready, wal_file_done, strerror(errno));
	}

	return skipped;
}

/*
 * Copy non WAL file, such as .backup or .history file, into WAL archive.
 * Optionally apply streaming compression to it.
 * Returns:
 *  0 - file was successfully pushed
 *  1 - push was skipped because file already exists in the archive and
 *      has the same checksum
 */
err_i
push_file_internal(const char *wal_file_name, const char *pg_xlog_dir,
                   const char *archive_dir, bool overwrite, bool no_sync,
                   bool is_compress, int compress_level,
                   uint32 archive_timeout, bool *skipped)
{
    FOBJ_FUNC_ARP();
    pioFile_i in;
    pioWriteCloser_i out;
    char from_fullpath[MAXPGPATH];
    char to_fullpath[MAXPGPATH];
/* partial handling */
    size_t len;
    err_i err = $noerr();

    pioDrive_i db_drive = pioDriveForLocation(FIO_DB_HOST);
    pioDrive_i backup_drive = pioDriveForLocation(FIO_BACKUP_HOST);

    /* from path */
    join_path_components(from_fullpath, pg_xlog_dir, wal_file_name);
    canonicalize_path(from_fullpath);
    /* to path */
    join_path_components(to_fullpath, archive_dir, wal_file_name);
    canonicalize_path(to_fullpath);
    if (is_compress)
    {
        /* destination file with .gz suffix */
        len = ft_strlcat(to_fullpath, ".gz", sizeof(to_fullpath));
        if (len >= sizeof(to_fullpath))
            return $iresult($err(RT, "File path too long: {path:q}",
								 path(to_fullpath)));
    }
    /* open destination partial file for write */

    if ($i(pioExists, backup_drive, .path = to_fullpath, .err = &err))
    {
        pg_crc32 crc32_src;
        pg_crc32 crc32_dst;

        crc32_src = $i(pioGetCRC32, db_drive, from_fullpath,
                       .compressed = false, .err = &err);
        if ($haserr(err))
			return $iresult(err);

        crc32_dst = $i(pioGetCRC32, backup_drive, to_fullpath,
                       .compressed = is_compress, .err = &err);
        if ($haserr(err))
			return $iresult(err);

        if (crc32_src == crc32_dst)
        {
            elog(LOG, "WAL file already exists in archive with the same "
                      "checksum, skip pushing: \"%s\"", from_fullpath);
			*skipped = true;
            return $noerr();
        }
        else if (overwrite)
        {
            elog(LOG, "WAL file already exists in archive with "
                      "different checksum, overwriting: \"%s\"",
                 to_fullpath);
        }
        else
        {
			return $iresult($err(RT, "WAL file already exists in archive with "
									 "different checksum: {path:q}",
									 path(to_fullpath)));
        }
    }
    else if ($haserr(err))
    {
		return $iresult(err);
    }

	/* Open source file for read */
	in = $i(pioOpen, db_drive, from_fullpath, O_RDONLY | PG_BINARY, .err = &err);
	if ($haserr(err))
		return $iresult(err);

	out = $i(pioOpenRewrite, backup_drive, .path = to_fullpath, .err = &err);
	if ($haserr(err))
		return $iresult(err);

    /* enable streaming compression */
    if (is_compress)
    {
#ifdef HAVE_LIBZ
        pioFilter_i flt = pioGZCompressFilter(compress_level);
        err = pioCopy($reduce(pioWriteFlush, out),
                      $reduce(pioRead, in),
                      flt);
#else
        elog(ERROR, "Compression is requested, but not compiled it");
#endif
    }
    else
    {
        err = pioCopy($reduce(pioWriteFlush, out),
                      $reduce(pioRead, in));
    }

    /* close source file */
    $i(pioClose, in); /* ignore error */

    if ($haserr(err))
		return $iresult(err);

    err = $i(pioClose, out, .sync = !no_sync);
    if ($haserr(err))
		return $iresult(err);

    return $noerr();
}

/* Copy file attributes */
//static void
//copy_file_attributes(const char *from_path, fio_location from_location,
//		  const char *to_path, fio_location to_location,
//		  bool unlink_on_error)
//{
//	struct stat st;
//
//	if (fio_stat(from_location, from_path, &st, true) == -1)
//	{
//		if (unlink_on_error)
//			fio_unlink(to_path, to_location);
//		elog(ERROR, "Cannot stat file \"%s\": %s",
//			 from_path, strerror(errno));
//	}
//
//	if (fio_chmod(to_location, to_path, st.st_mode) == -1)
//	{
//		if (unlink_on_error)
//			fio_unlink(to_path, to_location);
//		elog(ERROR, "Cannot change mode of file \"%s\": %s",
//			 to_path, strerror(errno));
//	}
//}

/* Look for files with '.ready' suffix in archive_status directory
 * and pack such files into batch sized array.
 */
parray *
setup_push_filelist(const char *archive_status_dir, const char *first_file,
					int batch_size)
{
	int i;
	WALSegno *xlogfile = NULL;
	parray  *status_files = NULL;
	parray  *batch_files = parray_new();

	/* guarantee that first filename is in batch list */
	xlogfile = palloc(sizeof(WALSegno));
	pg_atomic_init_flag(&xlogfile->lock);
	snprintf(xlogfile->name, MAXFNAMELEN, "%s", first_file);
	parray_append(batch_files, xlogfile);

	if (batch_size < 2)
		return batch_files;

	/* get list of files from archive_status */
	status_files = parray_new();
	db_list_dir(status_files, archive_status_dir, false, false, 0);
	parray_qsort(status_files, pgFileCompareName);

	for (i = 0; i < parray_num(status_files); i++)
	{
		int result = 0;
		char filename[MAXFNAMELEN];
		char suffix[MAXFNAMELEN];
		pgFile *file = (pgFile *) parray_get(status_files, i);

		result = sscanf(file->name, "%[^.]%s", (char *) &filename, (char *) &suffix);

		if (result != 2)
			continue;

		if (strcmp(suffix, ".ready") != 0)
			continue;

		/* first filename already in batch list */
		if (strcmp(filename, first_file) == 0)
			continue;

		xlogfile = palloc(sizeof(WALSegno));
		pg_atomic_init_flag(&xlogfile->lock);

		snprintf(xlogfile->name, MAXFNAMELEN, "%s", filename);
		parray_append(batch_files, xlogfile);

		if (parray_num(batch_files) >= batch_size)
			break;
	}

	/* cleanup */
	parray_walk(status_files, pgFileFree);
	parray_free(status_files);

	return batch_files;
}

/*
 * pg_probackup specific restore command.
 * Move files from arclog_path to pgdata/wal_file_path.
 *
 *  The problem with archive-get: we must be very careful about
 * erroring out, because postgres will interpretent our negative exit code
 * as the fact, that requested file is missing and may take irreversible actions.
 * So if file copying has failed we must retry several times before bailing out.
 *
 * TODO: add support of -D option.
 * TOTHINK: what can be done about ssh connection been broken?
 * TOTHINk: do we need our own rmtree function ?
 * TOTHINk: so sort of async prefetch ?

 */
void
do_archive_get(InstanceState *instanceState, InstanceConfig *instance, const char *prefetch_dir_arg,
			   char *wal_file_path, char *wal_file_name, int batch_size,
			   bool validate_wal)
{
	int         fail_count = 0;
	char        backup_wal_file_path[MAXPGPATH];
	char        absolute_wal_file_path[MAXPGPATH];
	char        current_dir[MAXPGPATH];
	char        prefetch_dir[MAXPGPATH];
	char        pg_xlog_dir[MAXPGPATH];
	char        prefetched_file[MAXPGPATH];

	/* reporting */
	uint32      n_fetched = 0;
	int         n_actual_threads = num_threads;
	uint32      n_files_in_prefetch = 0;

	/* time reporting */
	instr_time  start_time, end_time;
	double      get_time;
	char        pretty_time_str[20];

	if (wal_file_name == NULL)
		elog(ERROR, "Required parameter not specified: --wal-file-name %%f");

	if (wal_file_path == NULL)
		elog(ERROR, "Required parameter not specified: --wal_file_path %%p");

	if (!getcwd(current_dir, sizeof(current_dir)))
		elog(ERROR, "getcwd() error");

	/* path to PGDATA/pg_wal directory */
	join_path_components(pg_xlog_dir, current_dir, XLOGDIR);

	/* destination full filepath, usually it is PGDATA/pg_wal/RECOVERYXLOG */
	join_path_components(absolute_wal_file_path, current_dir, wal_file_path);

	/* full filepath to WAL file in archive directory.
	 * $BACKUP_PATH/wal/instance_name/000000010000000000000001 */
	join_path_components(backup_wal_file_path, instanceState->instance_wal_subdir_path, wal_file_name);

	INSTR_TIME_SET_CURRENT(start_time);
	if (num_threads > batch_size)
		n_actual_threads = batch_size;
	elog(INFO, "pg_probackup archive-get WAL file: %s, remote: %s, threads: %i/%i, batch: %i",
			wal_file_name, IsSshProtocol() ? "ssh" : "none", n_actual_threads, num_threads, batch_size);

	num_threads = n_actual_threads;

	elog(VERBOSE, "Obtaining XLOG_SEG_SIZE from pg_control file");
	instance->xlog_seg_size = get_xlog_seg_size(current_dir);

	/* Prefetch optimization kicks in only if simple XLOG segments is requested
	 * and batching is enabled.
	 *
	 * We check that file do exists in prefetch directory, then we validate it and
	 * rename to destination path.
	 * If file do not exists, then we run prefetch and rename it.
	 */
	if (IsXLogFileName(wal_file_name) && batch_size > 1)
	{
		XLogSegNo segno;
		TimeLineID tli;

		GetXLogFromFileName(wal_file_name, &tli, &segno, instance->xlog_seg_size);

		if (prefetch_dir_arg)
			/* use provided prefetch directory */
			snprintf(prefetch_dir, sizeof(prefetch_dir), "%s", prefetch_dir_arg);
		else
			/* use default path */
			join_path_components(prefetch_dir, pg_xlog_dir, "pbk_prefetch");

		/* Construct path to WAL file in prefetch directory.
		 * current_dir/pg_wal/pbk_prefech/000000010000000000000001
		 */
		join_path_components(prefetched_file, prefetch_dir, wal_file_name);

		/* check if file is available in prefetch directory */
		if (access(prefetched_file, F_OK) == 0)
		{
			/* Prefetched WAL segment is available, before using it, we must validate it.
			 * But for validation to work properly(because of contrecord), we must be sure
			 * that next WAL segment is also available in prefetch directory.
			 * If next segment do not exists in prefetch directory, we must provide it from
			 * archive. If it is NOT available in the archive, then file in prefetch directory
			 * cannot be trusted. In this case we discard all prefetched files and
			 * copy requested file directly from archive.
			 */
			if (!next_wal_segment_exists(tli, segno, prefetch_dir, instance->xlog_seg_size))
				n_fetched = run_wal_prefetch(prefetch_dir, instanceState->instance_wal_subdir_path,
											 tli, segno, num_threads, false, batch_size,
											 instance->xlog_seg_size);

			n_files_in_prefetch = maintain_prefetch(prefetch_dir, segno, instance->xlog_seg_size);

			if (wal_satisfy_from_prefetch(tli, segno, wal_file_name, prefetch_dir,
										  absolute_wal_file_path, instance->xlog_seg_size,
										  validate_wal))
			{
				n_files_in_prefetch--;
				elog(INFO, "pg_probackup archive-get used prefetched WAL segment %s, prefetch state: %u/%u",
						wal_file_name, n_files_in_prefetch, batch_size);
				goto get_done;
			}
			else
			{
				/* discard prefetch */
//				n_fetched = 0;
				pgut_rmtree(prefetch_dir, false, false);
			}
		}
		else
		{
			/* Do prefetch maintenance here */

			mkdir(prefetch_dir, DIR_PERMISSION); /* In case prefetch directory do not exists yet */

			/* We`ve failed to satisfy current request from prefetch directory,
			 * therefore we can discard its content, since it may be corrupted or
			 * contain stale files.
			 *
			 * UPDATE: we should not discard prefetch easily, because failing to satisfy
			 * request for WAL may come from this recovery behavior:
			 * https://www.postgresql.org/message-id/flat/16159-f5a34a3a04dc67e0%40postgresql.org
			 */
//			rmtree(prefetch_dir, false);

			/* prefetch files */
			n_fetched = run_wal_prefetch(prefetch_dir, instanceState->instance_wal_subdir_path,
										 tli, segno, num_threads, true, batch_size,
										 instance->xlog_seg_size);

			n_files_in_prefetch = maintain_prefetch(prefetch_dir, segno, instance->xlog_seg_size);

			if (wal_satisfy_from_prefetch(tli, segno, wal_file_name, prefetch_dir, absolute_wal_file_path,
										  instance->xlog_seg_size, validate_wal))
			{
				n_files_in_prefetch--;
				elog(INFO, "pg_probackup archive-get copied WAL file %s, prefetch state: %u/%u",
						wal_file_name, n_files_in_prefetch, batch_size);
				goto get_done;
			}
//			else
//			{
//				/* yet again failed to satisfy request from prefetch */
//				n_fetched = 0;
//				rmtree(prefetch_dir, false);
//			}
		}
	}

	/* we use it to extend partial file later  */
	xlog_seg_size = instance->xlog_seg_size;

	/* Either prefetch didn`t cut it, or batch mode is disabled or
	 * the requested file is not WAL segment.
	 * Copy file from the archive directly.
	 * Retry several times before bailing out.
	 *
	 * TODO:
	 * files copied from archive directly are not validated, which is not ok.
	 * TOTHINK:
	 * Current WAL validation cannot be applied to partial files.
	 */

	while (fail_count < 3)
	{
		if (get_wal_file(wal_file_name, backup_wal_file_path, absolute_wal_file_path, false))
		{
			fail_count = 0;
			elog(LOG, "pg_probackup archive-get copied WAL file %s", wal_file_name);
			n_fetched++;
			break;
		}
		else
			fail_count++;

		elog(LOG, "Failed to get WAL file %s, retry %i/3", wal_file_name, fail_count);
	}

	/* TODO/TOTHINK:
	 * If requested file is corrupted, we have no way to warn PostgreSQL about it.
	 * We either can:
	 * 1. feed to recovery and let PostgreSQL sort it out. Currently we do this.
	 * 2. error out.
	 *
	 * Also note, that we can detect corruption only if prefetch mode is used.
	 * TODO: if corruption or network problem encountered, kill yourself
	 * with SIGTERN to prevent recovery from starting up database.
	 */

get_done:
	INSTR_TIME_SET_CURRENT(end_time);
	INSTR_TIME_SUBTRACT(end_time, start_time);
	get_time = INSTR_TIME_GET_DOUBLE(end_time);
	pretty_time_interval(get_time, pretty_time_str, 20);

	if (fail_count == 0)
		elog(INFO, "pg_probackup archive-get completed successfully, fetched: %i/%i, time elapsed: %s",
				n_fetched, batch_size, pretty_time_str);
	else
		elog(ERROR, "pg_probackup archive-get failed to deliver WAL file: %s, time elapsed: %s",
				wal_file_name, pretty_time_str);
}

/*
 * Copy batch_size of regular WAL segments into prefetch directory,
 * starting with first_file.
 *
 * inclusive - should we copy first_file or not.
 */
uint32 run_wal_prefetch(const char *prefetch_dir, const char *archive_dir,
					 TimeLineID tli, XLogSegNo first_segno, int num_threads,
					 bool inclusive, int batch_size, uint32 wal_seg_size)
{
	int         i;
	XLogSegNo   segno;
	parray     *batch_files = parray_new();
	int 		n_total_fetched = 0;

	if (!inclusive)
		first_segno++;

	for (segno = first_segno; segno < (first_segno + batch_size); segno++)
	{
		WALSegno *xlogfile = palloc(sizeof(WALSegno));
		pg_atomic_init_flag(&xlogfile->lock);

		/* construct filename for WAL segment */
		GetXLogFileName(xlogfile->name, tli, segno, wal_seg_size);

		parray_append(batch_files, xlogfile);

	}

	/* copy segments */
	if (num_threads == 1)
	{
		for (i = 0; i < parray_num(batch_files); i++)
		{
			char    to_fullpath[MAXPGPATH];
			char    from_fullpath[MAXPGPATH];
			WALSegno *xlogfile = (WALSegno *) parray_get(batch_files, i);

			join_path_components(to_fullpath, prefetch_dir, xlogfile->name);
			join_path_components(from_fullpath, archive_dir, xlogfile->name);

			/* It is ok, maybe requested batch is greater than the number of available
			 * files in the archive
			 */
			if (!get_wal_file(xlogfile->name, from_fullpath, to_fullpath, true))
			{
				elog(LOG, "Thread [%d]: Failed to prefetch WAL segment %s", 0, xlogfile->name);
				break;
			}

			n_total_fetched++;
		}
	}
	else
	{
		/* arrays with meta info for multi threaded archive-get */
		pthread_t        *threads;
		archive_get_arg  *threads_args;

		/* init thread args */
		threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
		threads_args = (archive_get_arg *) palloc0(sizeof(archive_get_arg) * num_threads);

		for (i = 0; i < num_threads; i++)
		{
			archive_get_arg *arg = &(threads_args[i]);

			arg->prefetch_dir = prefetch_dir;
			arg->archive_dir = archive_dir;

			arg->thread_num = i+1;
			arg->files = batch_files;
			arg->n_fetched = 0;
		}

		/* Run threads */
		for (i = 0; i < num_threads; i++)
		{
			archive_get_arg *arg = &(threads_args[i]);
			pthread_create(&threads[i], NULL, get_files, arg);
		}

		/* Wait threads */
		for (i = 0; i < num_threads; i++)
		{
			pthread_join(threads[i], NULL);
			n_total_fetched += threads_args[i].n_fetched;
		}
	}
	/* TODO: free batch_files */
	return n_total_fetched;
}

/*
 * Copy files from archive catalog to pg_wal.
 */
static void *
get_files(void *arg)
{
	int		i;
	char    to_fullpath[MAXPGPATH];
	char    from_fullpath[MAXPGPATH];
	archive_get_arg *args = (archive_get_arg *) arg;

	set_my_thread_num(args->thread_num);

	for (i = 0; i < parray_num(args->files); i++)
	{
		WALSegno *xlogfile = (WALSegno *) parray_get(args->files, i);

		if (prefetch_stop)
			break;

		if (!pg_atomic_test_set_flag(&xlogfile->lock))
			continue;

		join_path_components(from_fullpath, args->archive_dir, xlogfile->name);
		join_path_components(to_fullpath, args->prefetch_dir, xlogfile->name);

		if (!get_wal_file(xlogfile->name, from_fullpath, to_fullpath, true))
		{
			/* It is ok, maybe requested batch is greater than the number of available
			 * files in the archive
			 */
			elog(LOG, "Failed to prefetch WAL segment %s", xlogfile->name);
			prefetch_stop = true;
			break;
		}

		args->n_fetched++;
	}

	/* close ssh connection */
	fio_disconnect();

	return NULL;
}

/*
 * Copy WAL segment from archive catalog to pgdata with possible decompression.
 * When running in prefetch mode, we should not error out.
 */
bool
get_wal_file(const char *filename, const char *from_fullpath,
			 const char *to_fullpath, bool prefetch_mode)
{
    FOBJ_FUNC_ARP();
    pioFile_i out = {NULL};
    pioFile_i in = {NULL};
    char *buf = pgut_malloc(OUT_BUF_SIZE); /* 1MB buffer */
    err_i err = $noerr();
    char from_fullpath_gz[MAXPGPATH];
    bool compressed = false;
    bool src_partial = false;

    pioDrive_i db_drive = pioDriveForLocation(FIO_DB_HOST);
    pioDrive_i backup_drive = pioDriveForLocation(FIO_BACKUP_HOST);

    snprintf(from_fullpath_gz, sizeof(from_fullpath_gz), "%s.gz",
             from_fullpath);

    /* open destination file */
    out = $i(pioOpen, db_drive, .path = to_fullpath, .err = &err,
               .flags = O_WRONLY | O_CREAT | O_EXCL | O_TRUNC | PG_BINARY);
    if ($haserr(err))
    {
        elog(WARNING, "%s", $errmsg(err));
        return false;
    }


#ifdef HAVE_LIBZ
    /* If requested file is regular WAL segment, then try to open it with '.gz' suffix... */
    if (IsXLogFileName(filename))
    {
        in = $i(pioOpen, backup_drive, from_fullpath_gz, O_RDONLY | PG_BINARY,
                  .err = &err);
        compressed = in.self != NULL;
        if ($haserr(err) && getErrno(err) != ENOENT)
            elog(ERROR, "Source file: %s", $errmsg(err));
    }
#endif
    if (in.self == NULL)
    {
        in = $i(pioOpen, backup_drive, from_fullpath, O_RDONLY | PG_BINARY,
                  .err = &err);
        if ($haserr(err) && getErrno(err) != ENOENT)
            elog(ERROR, "Source file: %s", $errmsg(err));
    }
    /* try partial file */
    if (in.self == NULL && !prefetch_mode && IsXLogFileName(filename))
    {
        char from_partial[MAXPGPATH];
#ifdef HAVE_LIBZ
        snprintf(from_partial, sizeof(from_partial), "%s.gz.partial",
                 from_fullpath);

        in = $i(pioOpen, backup_drive, from_partial, O_RDONLY | PG_BINARY,
                  .err = &err);
        compressed = in.self != NULL;
        if ($haserr(err) && getErrno(err) != ENOENT)
            elog(ERROR, "Source partial file: %s", $errmsg(err));
#endif

        if (in.self == NULL)
        {
            snprintf(from_partial, sizeof(from_partial), "%s.partial",
                     from_fullpath);
            in = $i(pioOpen, backup_drive,
                      .path = from_partial,
                      .flags = O_RDONLY | PG_BINARY,
                      .err = &err);
            if ($haserr(err) && getErrno(err) != ENOENT)
                elog(ERROR, "Source partial file: %s", $errmsg(err));
        }

        src_partial = true;
    }

    if (in.self == NULL)
    {
        $i(pioClose, out);
        $i(pioRemove, db_drive, to_fullpath, true);
        free(buf);
        if (!prefetch_mode)
            elog(LOG, "Target WAL file is missing: %s", filename);
        return false;
    }

#ifdef HAVE_LIBZ
    if (compressed)
    {
        pioFilter_i flt = pioGZDecompressFilter(src_partial);
        err = pioCopy($reduce(pioWriteFlush, out),
                      $reduce(pioRead, in),
                      flt);
    }
    else
#endif
    {
        err = pioCopy($reduce(pioWriteFlush, out),
                      $reduce(pioRead, in));
    }

    /* close source file */
    $i(pioClose, in); /* ignore error */

    if ($haserr(err))
    {
        $i(pioClose, out);
        $i(pioRemove, db_drive, to_fullpath, true);
        elog(ERROR, "%s", $errmsg(err));
    }

    /* If partial file was used as source, then it is very likely that destination
     * file is not equal to XLOG_SEG_SIZE - that is the way pg_receivexlog works.
     * We must manually extent it up to XLOG_SEG_SIZE.
     */
    if (src_partial)
    {
        err = $i(pioTruncate, out, xlog_seg_size);
        if ($haserr(err))
        {
            elog(WARNING, "Extend file: %s", $errmsg(err));
            $i(pioClose, out);
            $i(pioRemove, db_drive, to_fullpath, true);
            free(buf);
            return false;
        }
    }

    err = $i(pioClose, out);
    if ($haserr(err))
    {
        elog(WARNING, "%s", $errmsg(err));
        $i(pioRemove, db_drive, to_fullpath, true);
        free(buf);
        return false;
    }

    elog(LOG, "WAL file successfully %s: %s",
         prefetch_mode ? "prefetched" : "copied", filename);
    free(buf);
    return true;
}

bool next_wal_segment_exists(TimeLineID tli, XLogSegNo segno, const char *prefetch_dir, uint32 wal_seg_size)
{
	char        next_wal_filename[MAXFNAMELEN];
	char        next_wal_fullpath[MAXPGPATH];

	GetXLogFileName(next_wal_filename, tli, segno + 1, wal_seg_size);

	join_path_components(next_wal_fullpath, prefetch_dir, next_wal_filename);

	if (access(next_wal_fullpath, F_OK) == 0)
		return true;

	return false;
}

/* Try to use content of prefetch directory to satisfy request for WAL segment
 * If file is found, then validate it and rename.
 * If requested file do not exists or validation has failed, then
 * caller must copy WAL file directly from archive.
 */
bool wal_satisfy_from_prefetch(TimeLineID tli, XLogSegNo segno, const char *wal_file_name,
							   const char *prefetch_dir, const char *absolute_wal_file_path,
							   uint32 wal_seg_size, bool parse_wal)
{
	char prefetched_file[MAXPGPATH];

	join_path_components(prefetched_file, prefetch_dir, wal_file_name);

	/* If prefetched file do not exists, then nothing can be done */
	if (access(prefetched_file, F_OK) != 0)
		return false;

	/* If the next WAL segment do not exists in prefetch directory,
	 * then current segment cannot be validated, therefore cannot be used
	 * to satisfy recovery request.
	 */
	if (parse_wal && !next_wal_segment_exists(tli, segno, prefetch_dir, wal_seg_size))
		return false;

	if (parse_wal && !validate_wal_segment(tli, segno, prefetch_dir, wal_seg_size))
	{
		/* prefetched WAL segment is not looking good */
		elog(LOG, "Prefetched WAL segment %s is invalid, cannot use it", wal_file_name);
		unlink(prefetched_file);
		return false;
	}

	/* file is available in prefetch directory */
	if (rename(prefetched_file, absolute_wal_file_path) == 0)
		return true;
	else
	{
		elog(WARNING, "Cannot rename file '%s' to '%s': %s",
				prefetched_file, absolute_wal_file_path, strerror(errno));
		unlink(prefetched_file);
	}

	return false;
}

/*
 * Maintain prefetch directory: drop redundant files
 * Return number of files in prefetch directory.
 */
uint32 maintain_prefetch(const char *prefetch_dir, XLogSegNo first_segno, uint32 wal_seg_size)
{
	DIR		   *dir;
	struct dirent *dir_ent;
	uint32 n_files = 0;

	XLogSegNo segno;
	TimeLineID tli;

	char fullpath[MAXPGPATH];

	dir = opendir(prefetch_dir);
	if (dir == NULL)
	{
		if (errno != ENOENT)
			elog(WARNING, "Cannot open directory \"%s\": %s", prefetch_dir, strerror(errno));

		return n_files;
	}

	while ((dir_ent = readdir(dir)))
	{
		/* Skip entries point current dir or parent dir */
		if (strcmp(dir_ent->d_name, ".") == 0 ||
			strcmp(dir_ent->d_name, "..") == 0)
			continue;

		if (IsXLogFileName(dir_ent->d_name))
		{

			GetXLogFromFileName(dir_ent->d_name, &tli, &segno, wal_seg_size);

			/* potentially useful segment, keep it */
			if (segno >= first_segno)
			{
				n_files++;
				continue;
			}
		}

		join_path_components(fullpath, prefetch_dir, dir_ent->d_name);
		unlink(fullpath);
	}

	closedir(dir);

	return n_files;
}
