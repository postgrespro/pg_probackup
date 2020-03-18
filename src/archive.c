/*-------------------------------------------------------------------------
 *
 * archive.c: -  pg_probackup specific archive commands for archive backups.
 *
 *
 * Portions Copyright (c) 2018-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include <unistd.h>
#include "pg_probackup.h"
#include "utils/thread.h"
#include "instr_time.h"

static int push_file_internal_uncompressed(const char *wal_file_name, const char *pg_xlog_dir,
								  const char *archive_dir, bool overwrite, bool no_sync,
								  int thread_num, uint32 archive_timeout);
#ifdef HAVE_LIBZ
static int push_file_internal_gz(const char *wal_file_name, const char *pg_xlog_dir,
									 const char *archive_dir, bool overwrite, bool no_sync,
									 int compress_level, int thread_num, uint32 archive_timeout);
#endif
static void *push_files(void *arg);
static void get_wal_file(const char *from_path, const char *to_path);
#ifdef HAVE_LIBZ
static const char *get_gz_error(gzFile gzf, int errnum);
#endif
static void copy_file_attributes(const char *from_path,
								 fio_location from_location,
								 const char *to_path, fio_location to_location,
								 bool unlink_on_error);

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

typedef struct WALSegno
{
	char        name[MAXFNAMELEN];
	volatile    pg_atomic_flag lock;
} WALSegno;

static int push_file(WALSegno *xlogfile, const char *archive_status_dir,
								   const char *pg_xlog_dir, const char *archive_dir,
								   bool overwrite, bool no_sync, uint32 archive_timeout,
								   bool no_ready_rename, bool is_compress,
								   int compress_level, int thread_num);

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
do_archive_push(InstanceConfig *instance, char *wal_file_path,
				char *wal_file_name, int batch_size, bool overwrite,
				bool no_sync, bool no_ready_rename)
{
	uint64		i;
	char		current_dir[MAXPGPATH];
	char		pg_xlog_dir[MAXPGPATH];
	char		archive_status_dir[MAXPGPATH];
	uint64		system_id;
	bool		is_compress = false;

	/* arrays with meta info for multi threaded backup */
	pthread_t	*threads;
	archive_push_arg *threads_args;
	bool		push_isok = true;

	/* reporting */
	uint32      n_total_pushed = 0;
	uint32      n_total_skipped = 0;
	uint32      n_total_failed = 0;
	pid_t       my_pid;
	instr_time  start_time, end_time;
	double      push_time;
	char        pretty_time_str[20];

	/* files to push in multi-thread mode */
	parray     *batch_files = NULL;
	int         n_threads;

	my_pid = getpid();

	if (wal_file_name == NULL)
		elog(ERROR, "Required parameter is not specified: --wal-file-name %%f");

	if (!getcwd(current_dir, sizeof(current_dir)))
		elog(ERROR, "getcwd() error");

	/* verify that archive-push --instance parameter is valid */
	system_id = get_system_identifier(current_dir);

	if (instance->pgdata == NULL)
		elog(ERROR, "Cannot read pg_probackup.conf for this instance");

	if (system_id != instance->system_identifier)
		elog(ERROR, "Refuse to push WAL segment %s into archive. Instance parameters mismatch."
					"Instance '%s' should have SYSTEM_ID = " UINT64_FORMAT " instead of " UINT64_FORMAT,
				wal_file_name, instance->name, instance->system_identifier, system_id);

	if (instance->compress_alg == PGLZ_COMPRESS)
		elog(ERROR, "Cannot use pglz for WAL compression");

	join_path_components(pg_xlog_dir, current_dir, XLOGDIR);
	join_path_components(archive_status_dir, pg_xlog_dir, "archive_status");

	/* Create 'archlog_path' directory. Do nothing if it already exists. */
	//fio_mkdir(instance->arclog_path, DIR_PERMISSION, FIO_BACKUP_HOST);

#ifdef HAVE_LIBZ
	if (instance->compress_alg == ZLIB_COMPRESS)
		is_compress = true;
#endif

	/*  Setup filelist and locks */
	batch_files = setup_push_filelist(archive_status_dir, wal_file_name, batch_size);

	n_threads = num_threads;
	if (num_threads > parray_num(batch_files))
		n_threads = parray_num(batch_files);

	elog(INFO, "PID [%d]: pg_probackup push file %s into archive, "
					"threads: %i/%i, batch: %lu/%i, compression: %s",
						my_pid, wal_file_name, n_threads, num_threads,
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

			rc = push_file(xlogfile, archive_status_dir,
						   pg_xlog_dir, instance->arclog_path,
						   overwrite, no_sync,
						   instance->archive_timeout,
						   no_ready_rename || (strcmp(xlogfile->name, wal_file_name) == 0) ? true : false,
						   is_compress && IsXLogFileName(xlogfile->name) ? true : false,
						   instance->compress_level, 1);
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
		arg->archive_dir = instance->arclog_path;
		arg->pg_xlog_dir = pg_xlog_dir;
		arg->archive_status_dir = archive_status_dir;
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
	 * time-sensetive operation, so we skip freeing stuff.
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
		elog(INFO, "PID [%d]: pg_probackup archive-push completed successfully, "
					"pushed: %u, skipped: %u, time elapsed: %s",
					my_pid, n_total_pushed, n_total_skipped, pretty_time_str);
	else
		elog(ERROR, "PID [%d]: pg_probackup archive-push failed, "
					"pushed: %i, skipped: %u, failed: %u, time elapsed: %s",
					my_pid, n_total_pushed, n_total_skipped, n_total_failed,
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
					   args->compress_level, args->thread_num);

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
		  int compress_level, int thread_num)
{
	int     rc;
	char	wal_file_dummy[MAXPGPATH];

	join_path_components(wal_file_dummy, archive_status_dir, xlogfile->name);

	elog(LOG, "Thread [%d]: pushing file \"%s\"", thread_num, xlogfile->name);

	/* If compression is not required, then just copy it as is */
	if (!is_compress)
		rc = push_file_internal_uncompressed(xlogfile->name, pg_xlog_dir,
											 archive_dir, overwrite, no_sync,
											 thread_num, archive_timeout);
#ifdef HAVE_LIBZ
	else
		rc = push_file_internal_gz(xlogfile->name, pg_xlog_dir, archive_dir,
								   overwrite, no_sync, compress_level,
								   thread_num, archive_timeout);
#endif

	/* take '--no-ready-rename' flag into account */
	if (!no_ready_rename)
	{
		char	wal_file_ready[MAXPGPATH];
		char	wal_file_done[MAXPGPATH];

		snprintf(wal_file_ready, MAXPGPATH, "%s.%s", wal_file_dummy, "ready");
		snprintf(wal_file_done, MAXPGPATH, "%s.%s", wal_file_dummy, "done");

		canonicalize_path(wal_file_ready);
		canonicalize_path(wal_file_done);
		/* It is ok to rename status file in archive_status directory */
		elog(VERBOSE, "Thread [%d]: Rename \"%s\" to \"%s\"", thread_num,
										wal_file_ready, wal_file_done);

		/* do not error out, if rename failed */
		if (fio_rename(wal_file_ready, wal_file_done, FIO_DB_HOST) < 0)
			elog(WARNING, "Thread [%d]: Cannot rename ready file \"%s\" to \"%s\": %s",
				thread_num, wal_file_ready, wal_file_done, strerror(errno));
	}

	return rc;
}

/*
 * Copy non WAL file, such as .backup or .history file, into WAL archive.
 * Such files are not compressed.
 * Returns:
 *  0 - file was successfully pushed
 *  1 - push was skipped because file already exists in the archive and
 *      has the same checksum
 */
int
push_file_internal_uncompressed(const char *wal_file_name, const char *pg_xlog_dir,
								const char *archive_dir, bool overwrite, bool no_sync,
								int thread_num, uint32 archive_timeout)
{
	FILE	   *in = NULL;
	int			out = -1;
	char		buf[STDIO_BUFSIZE];
//	char		buf[XLOG_BLCKSZ];
	char		from_fullpath[MAXPGPATH];
	char		to_fullpath[MAXPGPATH];
	/* partial handling */
	struct stat		st;
	char		to_fullpath_part[MAXPGPATH];
	int			partial_try_count = 0;
	int			partial_file_size = 0;
	bool		partial_is_stale = true;

	/* from path */
	join_path_components(from_fullpath, pg_xlog_dir, wal_file_name);
	canonicalize_path(from_fullpath);
	/* to path */
	join_path_components(to_fullpath, archive_dir, wal_file_name);
	canonicalize_path(to_fullpath);

	/* Open source file for read */
	in = fio_fopen(from_fullpath, PG_BINARY_R, FIO_DB_HOST);
	if (in == NULL)
		elog(ERROR, "Thread [%d]: Cannot open source file \"%s\": %s",
					thread_num, from_fullpath, strerror(errno));

	/* open destination partial file for write */
	snprintf(to_fullpath_part, sizeof(to_fullpath_part), "%s.part", to_fullpath);

	/* Grab lock by creating temp file in exclusive mode */
	out = fio_open(to_fullpath_part, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, FIO_BACKUP_HOST);
	if (out < 0)
	{
		if (errno != EEXIST)
			elog(ERROR, "Thread [%d]: Failed to open temp WAL file \"%s\": %s",
							thread_num, to_fullpath_part, strerror(errno));
		/* Already existing destination temp file is not an error condition */
	}
	else
		goto part_opened;

	/*
	 * Partial file already exists, it could have happened due to:
	 * 1. failed archive-push
	 * 2. concurrent archiving
	 *
	 * For ARCHIVE_TIMEOUT period we will try to create partial file
	 * and look for the size of already existing partial file, to
	 * determine if it is changing or not.
	 * If after ARCHIVE_TIMEOUT we still failed to create partial
	 * file, we will make a decision about discarding
	 * already existing partial file.
	 */

	while (partial_try_count < archive_timeout)
	{
		if (fio_stat(to_fullpath_part, &st, false, FIO_BACKUP_HOST) < 0)
		{
			if (errno == ENOENT)
			{
				//part file is gone, lets try to grab it
				out = fio_open(to_fullpath_part, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, FIO_BACKUP_HOST);
				if (out < 0)
				{
					if (errno != EEXIST)
						elog(ERROR, "Thread [%d]: Failed to open temp WAL file \"%s\": %s",
										thread_num, to_fullpath_part, strerror(errno));
				}
				else
					/* Successfully created partial file */
					break;
			}
			else
				elog(ERROR, "Thread [%d]: Cannot stat temp WAL file \"%s\": %s",
							thread_num, to_fullpath_part, strerror(errno));
		}

		/* first round */
		if (!partial_try_count)
		{
			elog(LOG, "Thread [%d]: Temp WAL file already exists, "
							"waiting on it %u seconds: \"%s\"",
							thread_num, archive_timeout, to_fullpath_part);
			partial_file_size = st.st_size;
		}

		/* file size is changing */
		if (st.st_size > partial_file_size)
			partial_is_stale = false;

		sleep(1);
		partial_try_count++;
	}
	/* The possible exit conditions:
	 * 1. File is grabbed
	 * 2. File is not grabbed, and it is not stale
	 * 2. File is not grabbed, and it is stale.
	 */

	/*
	 * If temp file was not grabbed for ARCHIVE_TIMEOUT and temp file is not stale,
	 * then exit with error.
	 */
	if (out < 0)
	{
		if (!partial_is_stale)
			elog(ERROR, "Thread [%d]: Failed to open temp WAL file \"%s\" in %i seconds",
									thread_num, to_fullpath_part, archive_timeout);

		/* Partial segment is considered stale, so reuse it */
		elog(LOG, "Thread [%d]: Reusing stale temp WAL file \"%s\"",
											thread_num, to_fullpath_part);
		fio_unlink(to_fullpath_part, FIO_BACKUP_HOST);

		out = fio_open(to_fullpath_part, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, FIO_BACKUP_HOST);
		if (out < 0)
			elog(ERROR, "Thread [%d]: Cannot open temp WAL file \"%s\": %s",
							thread_num, to_fullpath_part, strerror(errno));
	}

part_opened:
	elog(VERBOSE, "Thread [%d]: Temp WAL file successfully created: \"%s\"",
													thread_num, to_fullpath_part);
	/* Check if possible to skip copying */
	if (fileExists(to_fullpath, FIO_BACKUP_HOST))
	{
		pg_crc32 crc32_src;
		pg_crc32 crc32_dst;

		crc32_src = fio_get_crc32(from_fullpath, FIO_DB_HOST, false);
		crc32_dst = fio_get_crc32(to_fullpath, FIO_DB_HOST, false);

		if (crc32_src == crc32_dst)
		{
			elog(LOG, "Thread [%d]: WAL file already exists in archive with the same "
					"checksum, skip pushing: \"%s\"", thread_num, from_fullpath);
			/* cleanup */
			fio_fclose(in);
			fio_close(out);
			fio_unlink(to_fullpath_part, FIO_BACKUP_HOST);
			return 1;
		}
		else
		{
			if (overwrite)
				elog(LOG, "Thread [%d]: WAL file already exists in archive with "
						"different checksum, overwriting: \"%s\"", thread_num, to_fullpath);
			else
			{
				/* Overwriting is forbidden,
				 * so we must unlink partial file and exit with error.
				 */
				fio_unlink(to_fullpath_part, FIO_BACKUP_HOST);
				elog(ERROR, "Thread [%d]: WAL file already exists in archive with "
						"different checksum: \"%s\"", thread_num, to_fullpath);
			}
		}
	}

	/* copy content */
	for (;;)
	{
		ssize_t		read_len = 0;

		read_len = fio_fread(in, buf, sizeof(buf));

		if (read_len < 0)
		{
			fio_unlink(to_fullpath_part, FIO_BACKUP_HOST);
			elog(ERROR, "Thread [%d]: Cannot read source file \"%s\": %s",
						thread_num, from_fullpath, strerror(errno));
		}

		if (read_len > 0)
		{
			if (fio_write(out, buf, read_len) != read_len)
			{
				fio_unlink(to_fullpath_part, FIO_BACKUP_HOST);
				elog(ERROR, "Thread [%d]: Cannot write to destination temp file \"%s\": %s",
							thread_num, to_fullpath_part, strerror(errno));
			}
		}

		if (read_len == 0)
			break;
	}

	/* close source file */
	if (fio_fclose(in))
	{
		fio_unlink(to_fullpath_part, FIO_BACKUP_HOST);
		elog(ERROR, "Thread [%d]: Cannot close source WAL file \"%s\": %s",
					thread_num, from_fullpath, strerror(errno));
	}

	/* close temp file */
	if (fio_close(out) != 0)
	{
		fio_unlink(to_fullpath_part, FIO_BACKUP_HOST);
		elog(ERROR, "Thread [%d]: Cannot close temp WAL file \"%s\": %s",
					thread_num, to_fullpath_part, strerror(errno));
	}

	/* sync temp file to disk */
	if (!no_sync)
	{
		if (fio_sync(to_fullpath_part, FIO_BACKUP_HOST) != 0)
			elog(ERROR, "Thread [%d]: Failed to sync file \"%s\": %s",
						thread_num, to_fullpath_part, strerror(errno));
	}

	elog(VERBOSE, "Thread [%d]: Rename \"%s\" to \"%s\"",
					thread_num, to_fullpath_part, to_fullpath);

	//copy_file_attributes(from_path, FIO_DB_HOST, to_path_temp, FIO_BACKUP_HOST, true);

	/* Rename temp file to destination file */
	if (fio_rename(to_fullpath_part, to_fullpath, FIO_BACKUP_HOST) < 0)
	{
		fio_unlink(to_fullpath_part, FIO_BACKUP_HOST);
		elog(ERROR, "Thread [%d]: Cannot rename file \"%s\" to \"%s\": %s",
					thread_num, to_fullpath_part, to_fullpath, strerror(errno));
	}

	return 0;
}

#ifdef HAVE_LIBZ
/*
 * Push WAL segment into archive and apply streaming compression to it.
 * Returns:
 *  0 - file was successfully pushed
 *  1 - push was skipped because file already exists in the archive and
 *      has the same checksum
 */
int
push_file_internal_gz(const char *wal_file_name, const char *pg_xlog_dir,
					  const char *archive_dir, bool overwrite, bool no_sync,
					  int compress_level, int thread_num, uint32 archive_timeout)
{
	FILE	   *in = NULL;
	gzFile		out = NULL;
	int			errno_temp;
	char		buf[STDIO_BUFSIZE];
	char		from_fullpath[MAXPGPATH];
	char		to_fullpath[MAXPGPATH];
	char		to_fullpath_gz[MAXPGPATH];

	/* partial handling */
	struct stat		st;

	char		to_fullpath_gz_part[MAXPGPATH];
	int			partial_try_count = 0;
	int			partial_file_size = 0;
	bool		partial_is_stale = true;

	/* from path */
	join_path_components(from_fullpath, pg_xlog_dir, wal_file_name);
	canonicalize_path(from_fullpath);
	/* to path */
	join_path_components(to_fullpath, archive_dir, wal_file_name);
	canonicalize_path(to_fullpath);

	/* destination file with .gz suffix */
	snprintf(to_fullpath_gz, sizeof(to_fullpath_gz), "%s.gz", to_fullpath);
	/* destination temp file */
	snprintf(to_fullpath_gz_part, sizeof(to_fullpath_gz_part), "%s.part", to_fullpath_gz);

	/* Open source file for read */
	in = fio_fopen(from_fullpath, PG_BINARY_R, FIO_DB_HOST);
	if (in == NULL)
		elog(ERROR, "Thread [%d]: Cannot open source WAL file \"%s\": %s",
					thread_num, from_fullpath, strerror(errno));

	/* Grab lock by creating temp file in exclusive mode */
	out = fio_gzopen(to_fullpath_gz_part, PG_BINARY_W, compress_level, FIO_BACKUP_HOST);
	if (out == NULL)
	{
		if (errno != EEXIST)
			elog(ERROR, "Thread [%d]: Cannot open temp WAL file \"%s\": %s",
							thread_num, to_fullpath_gz_part, strerror(errno));
		/* Already existing destination temp file is not an error condition */
	}
	else
		goto part_opened;

	/*
	 * Partial file already exists, it could have happened due to:
	 * 1. failed archive-push
	 * 2. concurrent archiving
	 *
	 * For ARCHIVE_TIMEOUT period we will try to create partial file
	 * and look for the size of already existing partial file, to
	 * determine if it is changing or not.
	 * If after ARCHIVE_TIMEOUT we still failed to create partial
	 * file, we will make a decision about discarding
	 * already existing partial file.
	 */

	while (partial_try_count < archive_timeout)
	{
		if (fio_stat(to_fullpath_gz_part, &st, false, FIO_BACKUP_HOST) < 0)
		{
			if (errno == ENOENT)
			{
				//part file is gone, lets try to grab it
				out = fio_gzopen(to_fullpath_gz_part, PG_BINARY_W, compress_level, FIO_BACKUP_HOST);
				if (out == NULL)
				{
					if (errno != EEXIST)
						elog(ERROR, "Thread [%d]: Failed to open temp WAL file \"%s\": %s",
									thread_num, to_fullpath_gz_part, strerror(errno));
				}
				else
					/* Successfully created partial file */
					break;
			}
			else
				elog(ERROR, "Thread [%d]: Cannot stat temp WAL file \"%s\": %s",
							thread_num, to_fullpath_gz_part, strerror(errno));
		}

		/* first round */
		if (!partial_try_count)
		{
			elog(LOG, "Thread [%d]: Temp WAL file already exists, "
							"waiting on it %u seconds: \"%s\"",
							thread_num, archive_timeout, to_fullpath_gz_part);
			partial_file_size = st.st_size;
		}

		/* file size is changing */
		if (st.st_size > partial_file_size)
			partial_is_stale = false;

		sleep(1);
		partial_try_count++;
	}
	/* The possible exit conditions:
	 * 1. File is grabbed
	 * 2. File is not grabbed, and it is not stale
	 * 2. File is not grabbed, and it is stale.
	 */

	/*
	 * If temp file was not grabbed for ARCHIVE_TIMEOUT and temp file is not stale,
	 * then exit with error.
	 */
	if (out == NULL)
	{
		if (!partial_is_stale)
			elog(ERROR, "Thread [%d]: Failed to open temp WAL file \"%s\" in %i seconds",
								thread_num, to_fullpath_gz_part, archive_timeout);

		/* Partial segment is considered stale, so reuse it */
		elog(LOG, "Thread [%d]: Reusing stale temp WAL file \"%s\"",
											thread_num, to_fullpath_gz_part);
		fio_unlink(to_fullpath_gz_part, FIO_BACKUP_HOST);

		out = fio_gzopen(to_fullpath_gz_part, PG_BINARY_W, compress_level, FIO_BACKUP_HOST);
		if (out == NULL)
			elog(ERROR, "Thread [%d]: Cannot open temp WAL file \"%s\": %s",
								thread_num, to_fullpath_gz_part, strerror(errno));
	}

part_opened:
	elog(VERBOSE, "Thread [%d]: Temp WAL file successfully created: \"%s\"",
					thread_num, to_fullpath_gz_part);
	/* Check if possible to skip copying,
	 */
	if (fileExists(to_fullpath_gz, FIO_BACKUP_HOST))
	{
		pg_crc32 crc32_src;
		pg_crc32 crc32_dst;

		/* what if one of them goes missing */
		crc32_src = fio_get_crc32(from_fullpath, FIO_DB_HOST, false);
		crc32_dst = fio_get_crc32(to_fullpath_gz, FIO_DB_HOST, true);

		if (crc32_src == crc32_dst)
		{
			elog(LOG, "Thread [%d]: WAL file already exists in archive with the same "
					"checksum, skip pushing: \"%s\"", thread_num, from_fullpath);
			/* cleanup */
			fio_fclose(in);
			fio_gzclose(out);
			fio_unlink(to_fullpath_gz_part, FIO_BACKUP_HOST);
			return 1;
		}
		else
		{
			if (overwrite)
				elog(LOG, "Thread [%d]: WAL file already exists in archive with "
						"different checksum, overwriting: \"%s\"", thread_num, to_fullpath_gz);
			else
			{
				/* Overwriting is forbidden,
				 * so we must unlink partial file and exit with error.
				 */
				fio_fclose(in);
				fio_gzclose(out);
				fio_unlink(to_fullpath_gz_part, FIO_BACKUP_HOST);
				elog(ERROR, "Thread [%d]: WAL file already exists in archive with "
						"different checksum: \"%s\"", thread_num, to_fullpath_gz);
			}
		}
	}

	/* copy content */
	for (;;)
	{
		ssize_t		read_len = 0;

		read_len = fio_fread(in, buf, sizeof(buf));

		if (read_len < 0)
		{
			fio_unlink(to_fullpath_gz_part, FIO_BACKUP_HOST);
			elog(ERROR, "Thread [%d]: Cannot read from source file \"%s\": %s",
								thread_num, from_fullpath, strerror(errno));
		}

		if (read_len > 0)
		{
			if (fio_gzwrite(out, buf, read_len) != read_len)
			{
				errno_temp = errno;
				fio_unlink(to_fullpath_gz_part, FIO_BACKUP_HOST);
				elog(ERROR, "Thread [%d]: Cannot write to compressed temp WAL file \"%s\": %s",
							 thread_num, to_fullpath_gz_part, get_gz_error(out, errno_temp));
			}
		}

		if (read_len == 0)
			break;
	}

	/* close source file */
	if (fio_fclose(in))
	{
		fio_unlink(to_fullpath_gz_part, FIO_BACKUP_HOST);
		elog(ERROR, "Thread [%d]: Cannot close source WAL file \"%s\": %s",
					thread_num, from_fullpath, strerror(errno));
	}

	/* close temp file */
	if (fio_gzclose(out) != 0)
	{
		errno_temp = errno;
		fio_unlink(to_fullpath_gz_part, FIO_BACKUP_HOST);
		elog(ERROR, "Thread [%d]: Cannot close compressed temp WAL file \"%s\": %s",
					thread_num, to_fullpath_gz_part, strerror(errno_temp));
	}

	/* sync temp file to disk */
	if (!no_sync)
	{
		if (fio_sync(to_fullpath_gz_part, FIO_BACKUP_HOST) != 0)
			elog(ERROR, "Thread [%d]: Failed to sync file \"%s\": %s",
						thread_num, to_fullpath_gz_part, strerror(errno));
	}

	elog(VERBOSE, "Thread [%d]: Rename \"%s\" to \"%s\"",
					thread_num, to_fullpath_gz_part, to_fullpath_gz);

	//copy_file_attributes(from_path, FIO_DB_HOST, to_path_temp, FIO_BACKUP_HOST, true);

	/* Rename temp file to destination file */
	if (fio_rename(to_fullpath_gz_part, to_fullpath_gz, FIO_BACKUP_HOST) < 0)
	{
		fio_unlink(to_fullpath_gz_part, FIO_BACKUP_HOST);
		elog(ERROR, "Thread [%d]: Cannot rename file \"%s\" to \"%s\": %s",
					thread_num, to_fullpath_gz_part, to_fullpath_gz, strerror(errno));
	}

	return 0;
}
#endif

/*
 * pg_probackup specific restore command.
 * Move files from arclog_path to pgdata/wal_file_path.
 */
int
do_archive_get(InstanceConfig *instance,
			   char *wal_file_path, char *wal_file_name)
{
	char		backup_wal_file_path[MAXPGPATH];
	char		absolute_wal_file_path[MAXPGPATH];
	char		current_dir[MAXPGPATH];

	if (wal_file_name == NULL && wal_file_path == NULL)
		elog(ERROR, "required parameters are not specified: --wal-file-name %%f --wal-file-path %%p");

	if (wal_file_name == NULL)
		elog(ERROR, "required parameter not specified: --wal-file-name %%f");

	if (wal_file_path == NULL)
		elog(ERROR, "required parameter not specified: --wal-file-path %%p");

	canonicalize_path(wal_file_path);

	if (!getcwd(current_dir, sizeof(current_dir)))
		elog(ERROR, "getcwd() error");

	join_path_components(absolute_wal_file_path, current_dir, wal_file_path);
	join_path_components(backup_wal_file_path, instance->arclog_path, wal_file_name);

	elog(INFO, "pg_probackup archive-get from %s to %s",
		 backup_wal_file_path, absolute_wal_file_path);
	get_wal_file(backup_wal_file_path, absolute_wal_file_path);
	elog(INFO, "pg_probackup archive-get completed successfully");

	return 0;
}

/*
 * Copy WAL segment from archive catalog to pgdata with possible decompression.
 */
void
get_wal_file(const char *from_path, const char *to_path)
{
	FILE	   *in = NULL;
	int			out;
	char		buf[XLOG_BLCKSZ];
	const char *from_path_p = from_path;
	char		to_path_temp[MAXPGPATH];
	int			errno_temp;
	bool		is_decompress = false;

#ifdef HAVE_LIBZ
	char		gz_from_path[MAXPGPATH];
	gzFile		gz_in = NULL;
#endif

	/* First check source file for existance */
	if (fio_access(from_path, F_OK, FIO_BACKUP_HOST) != 0)
	{
#ifdef HAVE_LIBZ
		/*
		 * Maybe we need to decompress the file. Check it with .gz
		 * extension.
		 */
		snprintf(gz_from_path, sizeof(gz_from_path), "%s.gz", from_path);
		if (fio_access(gz_from_path, F_OK, FIO_BACKUP_HOST) == 0)
		{
			/* Found compressed file */
			is_decompress = true;
			from_path_p = gz_from_path;
		}
#endif
		/* Didn't find compressed file */
		if (!is_decompress)
			elog(ERROR, "Source WAL file \"%s\" doesn't exist",
				 from_path);
	}

	/* open file for read */
	if (!is_decompress)
	{
		in = fio_fopen(from_path, PG_BINARY_R, FIO_BACKUP_HOST);
		if (in == NULL)
			elog(ERROR, "Cannot open source WAL file \"%s\": %s",
					from_path, strerror(errno));
	}
#ifdef HAVE_LIBZ
	else
	{
		gz_in = fio_gzopen(gz_from_path, PG_BINARY_R, Z_DEFAULT_COMPRESSION,
						   FIO_BACKUP_HOST);
		if (gz_in == NULL)
			elog(ERROR, "Cannot open compressed WAL file \"%s\": %s",
					 gz_from_path, strerror(errno));
	}
#endif

	/* open backup file for write  */
	snprintf(to_path_temp, sizeof(to_path_temp), "%s.part", to_path);

	out = fio_open(to_path_temp, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, FIO_DB_HOST);
	if (out < 0)
		elog(ERROR, "Cannot open destination temporary WAL file \"%s\": %s",
				to_path_temp, strerror(errno));

	/* copy content */
	for (;;)
	{
		int read_len = 0;

#ifdef HAVE_LIBZ
		if (is_decompress)
		{
			read_len = fio_gzread(gz_in, buf, sizeof(buf));
			if (read_len <= 0 && !fio_gzeof(gz_in))
			{
				errno_temp = errno;
				fio_unlink(to_path_temp, FIO_DB_HOST);
				elog(ERROR, "Cannot read compressed WAL file \"%s\": %s",
					 gz_from_path, get_gz_error(gz_in, errno_temp));
			}
		}
		else
#endif
		{
			read_len = fio_fread(in, buf, sizeof(buf));
			if (read_len < 0)
			{
				errno_temp = errno;
				fio_unlink(to_path_temp, FIO_DB_HOST);
				elog(ERROR, "Cannot read source WAL file \"%s\": %s",
					 from_path, strerror(errno_temp));
			}
		}

		if (read_len > 0)
		{
			if (fio_write(out, buf, read_len) != read_len)
			{
				errno_temp = errno;
				fio_unlink(to_path_temp, FIO_DB_HOST);
				elog(ERROR, "Cannot write to WAL file \"%s\": %s", to_path_temp,
					 strerror(errno_temp));
			}
		}

		/* Check for EOF */
#ifdef HAVE_LIBZ
		if (is_decompress)
		{
			if (fio_gzeof(gz_in) || read_len == 0)
				break;
		}
		else
#endif
		{
			if (/* feof(in) || */ read_len == 0)
				break;
		}
	}

	if (fio_close(out) != 0)
	{
		errno_temp = errno;
		fio_unlink(to_path_temp, FIO_DB_HOST);
		elog(ERROR, "Cannot write WAL file \"%s\": %s",
			 to_path_temp, strerror(errno_temp));
	}

#ifdef HAVE_LIBZ
	if (is_decompress)
	{
		if (fio_gzclose(gz_in) != 0)
		{
			errno_temp = errno;
			fio_unlink(to_path_temp, FIO_DB_HOST);
			elog(ERROR, "Cannot close compressed WAL file \"%s\": %s",
				 gz_from_path, get_gz_error(gz_in, errno_temp));
		}
	}
	else
#endif
	{
		if (fio_fclose(in))
		{
			errno_temp = errno;
			fio_unlink(to_path_temp, FIO_DB_HOST);
			elog(ERROR, "Cannot close source WAL file \"%s\": %s",
				 from_path, strerror(errno_temp));
		}
	}

	/* update file permission. */
	copy_file_attributes(from_path_p, FIO_BACKUP_HOST, to_path_temp, FIO_DB_HOST, true);

	if (fio_rename(to_path_temp, to_path, FIO_DB_HOST) < 0)
	{
		errno_temp = errno;
		fio_unlink(to_path_temp, FIO_DB_HOST);
		elog(ERROR, "Cannot rename WAL file \"%s\" to \"%s\": %s",
			 to_path_temp, to_path, strerror(errno_temp));
	}

#ifdef HAVE_LIBZ
	if (is_decompress)
		elog(INFO, "WAL file decompressed from \"%s\"", gz_from_path);
#endif
}

#ifdef HAVE_LIBZ
/*
 * Show error during work with compressed file
 */
static const char *
get_gz_error(gzFile gzf, int errnum)
{
	int			gz_errnum;
	const char *errmsg;

	errmsg = fio_gzerror(gzf, &gz_errnum);
	if (gz_errnum == Z_ERRNO)
		return strerror(errnum);
	else
		return errmsg;
}
#endif

/* Copy file attributes */
static void
copy_file_attributes(const char *from_path, fio_location from_location,
		  const char *to_path, fio_location to_location,
		  bool unlink_on_error)
{
	struct stat st;

	if (fio_stat(from_path, &st, true, from_location) == -1)
	{
		if (unlink_on_error)
			fio_unlink(to_path, to_location);
		elog(ERROR, "Cannot stat file \"%s\": %s",
			 from_path, strerror(errno));
	}

	if (fio_chmod(to_path, st.st_mode, to_location) == -1)
	{
		if (unlink_on_error)
			fio_unlink(to_path, to_location);
		elog(ERROR, "Cannot change mode of file \"%s\": %s",
			 to_path, strerror(errno));
	}
}

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
	strncpy(xlogfile->name, first_file, MAXFNAMELEN);
	parray_append(batch_files, xlogfile);

	if (batch_size < 2)
		return batch_files;

	/* get list of files from archive_status */
	status_files = parray_new();
	dir_list_file(status_files, archive_status_dir, false, false, false, 0, FIO_DB_HOST);
	parray_qsort(status_files, pgFileComparePath);

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

		strncpy(xlogfile->name, filename, MAXFNAMELEN);
		parray_append(batch_files, xlogfile);

		if (parray_num(batch_files) >= batch_size)
			break;
	}

	/* cleanup */
	parray_walk(status_files, pgFileFree);
	parray_free(status_files);

	return batch_files;
}
