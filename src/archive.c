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

static int push_wal_file_internal(const char *wal_file_name, const char *pg_xlog_dir,
									const char *archive_dir, bool overwrite, int thread_num);
#ifdef HAVE_LIBZ
static int gz_push_wal_file_internal(const char *wal_file_name, const char *pg_xlog_dir,
									const char *archive_dir, bool overwrite,
									int compress_level, int thread_num);
#endif
static void *push_wal_segno(void *arg);
static void get_wal_file(const char *from_path, const char *to_path);
#ifdef HAVE_LIBZ
static const char *get_gz_error(gzFile gzf, int errnum);
#endif
static bool fileEqualCRC(const char *path1, const char *path2,
						 bool path2_is_compressed);
static void copy_file_attributes(const char *from_path,
								 fio_location from_location,
								 const char *to_path, fio_location to_location,
								 bool unlink_on_error);

static void push_wal_file(const char *from_path, const char *to_path,
						bool is_compress, bool overwrite, int compress_level);

typedef struct
{
	TimeLineID	tli;
	XLogSegNo	segno;
	uint32		xlog_seg_size;

	const char *pg_xlog_dir;
	const char *archive_dir;
	bool		overwrite;
	bool		compress;

	CompressAlg compress_alg;
	int			compress_level;
	int			thread_num;

	/*
	 * Return value from the thread.
	 * 0 means there is no error, 1 - there is an error.
	 */
	int			ret;
} archive_push_arg;

/*
 * At this point, we already done one roundtrip to archive server
 * to get instance config.
 */
int
do_archive_push_new(InstanceConfig *instance, char *wal_file_path,
								char *wal_file_name, bool overwrite)
{
	int			i;
	char		current_dir[MAXPGPATH];
	char		pg_xlog_dir[MAXPGPATH];
	uint64		system_id;
	bool		is_compress = false;

	/* filename parsing */
	TimeLineID	tli;
	XLogSegNo	first_segno;

	/* arrays with meta info for multi threaded backup */
	pthread_t	*threads;
	archive_push_arg *threads_args;
	bool		push_isok = true;

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

#ifdef HAVE_LIBZ
	if (instance->compress_alg == ZLIB_COMPRESS)
		is_compress = true;
#endif

	/* Single-thread push
	 * There are two cases, when we don`t want to start multi-thread push:
	 *  - number of threads is equal to 1, multi-threading isn`t cheap to start,
	 *    so creating, running and terminating one thread using generic
	 *    multi-thread approach take almost as much time as copying itself.
	 *  - file to push is not WAL file, but .history or .backup file.
	 *    we do not apply compression to such files.
	 *
	 * TODO: .partial files should also be pushed in single thread,
	 * but compression can be applied to them.
	 */
	if (num_threads == 1 || !IsXLogFileName(wal_file_name))
	{
		if (!IsXLogFileName(wal_file_name))
			is_compress = false; /* disable compression */

		elog(INFO, "pg_probackup: push file %s into archive, threads: 1, compression: %s",
						wal_file_name, is_compress ? "zlib" : "none");

		/* TODO: print zratio */
		if (is_compress)
			gz_push_wal_file_internal(wal_file_name, pg_xlog_dir, instance->arclog_path,
										overwrite, instance->compress_level, 1);
		else
			push_wal_file_internal(wal_file_name, pg_xlog_dir, instance->arclog_path,
																		overwrite, 1);

		push_isok = true;
		goto push_done;
	}

	/* parse WAL filename */
	if (IsXLogFileName(wal_file_name))
		GetXLogFromFileName(wal_file_name, &tli, &first_segno, instance->xlog_seg_size);

	/* TODO: report PID */
	elog(INFO, "pg_probackup: push file %s into archive, threads: %i, compression: %s",
					wal_file_name, num_threads, is_compress ? "zlib" : "none");

	/* TODO: report actual executed command */

	/* init thread args with its own segno */
	threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
	threads_args = (archive_push_arg *) palloc(sizeof(archive_push_arg) * num_threads);

	for (i = 0; i < num_threads; i++)
	{
		archive_push_arg *arg = &(threads_args[i]);

		arg->tli = tli;
		arg->segno = first_segno + i;
		arg->xlog_seg_size = instance->xlog_seg_size;
		arg->archive_dir = instance->arclog_path;
		arg->pg_xlog_dir = pg_xlog_dir;
		arg->overwrite = overwrite;
		arg->compress = is_compress;

		/* TODO, support --no-sync flag */

		arg->compress_alg = instance->compress_alg;
		arg->compress_level = instance->compress_level;

		arg->thread_num = i+1;
		/* By default there are some error */
		arg->ret = 1;
	}

	/* Run threads */
	thread_interrupted = false;
//	time(&start_time);
	for (i = 0; i < num_threads; i++)
	{
		archive_push_arg *arg = &(threads_args[i]);
		pthread_create(&threads[i], NULL, push_wal_segno, arg);
	}

	/* Wait threads */
	for (i = 0; i < num_threads; i++)
	{
		pthread_join(threads[i], NULL);
		if (threads_args[i].ret == 1) /* TODO: use retval to count actually pushed segments */
			push_isok = false;
	}

	/* Cleanup: we don`t do garbage collection on purpose to save time,
	 * pushing into archive is a very time-sensetive operation.
	 */

push_done:
	if (push_isok)
	{
		/* report number of segments pushed into archive */
		elog(INFO, "pg_probackup: file %s successfully pushed into archive, "
					"number of pushed files: , time elapsed: ",
					wal_file_name);
		return 0;
	}

	return 1;
}

/* ------------- INTERNAL FUNCTIONS ---------- */
/*
 * Copy WAL segment from pgdata to archive catalog with possible compression.
 */
static void *
push_wal_segno(void *arg)
{
	int		rc;
	char 	wal_filename[MAXPGPATH];
	archive_push_arg *args = (archive_push_arg *) arg;

	char 	archive_status_dir[MAXPGPATH];
	char 	wal_file_dummy[MAXPGPATH];
	char 	wal_file_ready[MAXPGPATH];
	char 	wal_file_done[MAXPGPATH];

	/* At first we must construct WAL filename from segno, tli and xlog_seg_size */
	GetXLogFileName(wal_filename, args->tli, args->segno, args->xlog_seg_size);

	join_path_components(archive_status_dir, args->pg_xlog_dir, "archive_status");
	join_path_components(wal_file_dummy, archive_status_dir, wal_filename);
	snprintf(wal_file_ready, MAXPGPATH, "%s.%s", wal_file_dummy, "ready");
	snprintf(wal_file_done, MAXPGPATH, "%s.%s", wal_file_dummy, "done");

	/* For additional threads we must check the existence of .ready file */
	if (args->thread_num != 1)
	{
		if (!fileExists(wal_file_ready, FIO_DB_HOST))
		{
			/* no ready file, nothing to do here */
			args->ret = 0;
			return NULL;
		}
	}
	elog(LOG, "Thread [%d]: pushing file \"%s\"", args->thread_num, wal_filename);

	/* If compression is not required, then just copy it as is */
	if (!args->compress)
		rc = push_wal_file_internal(wal_filename, args->pg_xlog_dir,
							args->archive_dir, args->overwrite,
							args->thread_num);
#ifdef HAVE_LIBZ
	else
		rc = gz_push_wal_file_internal(wal_filename, args->pg_xlog_dir,
								args->archive_dir, args->overwrite,
								args->compress_level, args->thread_num);
#endif

	/* TODO: Disable this behaivouir */
	if (rc == 0 && args->thread_num != 1)
	{
		/* It is ok to rename status file in archive_status directory */
		elog(VERBOSE, "Thread [%d]: Rename \"%s\" to \"%s\"", args->thread_num,
										wal_file_ready, wal_file_done);
		if (fio_rename(wal_file_ready, wal_file_done, FIO_DB_HOST) < 0)
			elog(ERROR, "Thread [%d]: Cannot rename file \"%s\" to \"%s\": %s",
				args->thread_num, wal_file_ready, wal_file_done, strerror(errno));
	}

	args->ret = 0;
	return NULL;
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
push_wal_file_internal(const char *wal_file_name, const char *pg_xlog_dir,
					const char *archive_dir, bool overwrite, int thread_num)
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
	/* to path */
	join_path_components(to_fullpath, archive_dir, wal_file_name);

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

	/* Partial file is already exists, it could have happened due to failed archive-push,
	 * in this case partial file can be discarded, or due to concurrent archiving.
	 */
	while (partial_try_count < PARTIAL_WAL_TIMER)
	{
		/* TODO: handle interrupt */

		if (fio_stat(to_fullpath_part, &st, false, FIO_BACKUP_HOST) < 0)
		{
			if (errno == ENOENT)
			{
				//part file is gone, lets try to grab it
				out = fio_open(to_fullpath_part, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, FIO_BACKUP_HOST);
				if (out < 0)
				{
					if (errno != EEXIST)
						elog(ERROR, "Thread [%d]: Failed to open temp file \"%s\": %s",
										thread_num, to_fullpath_part, strerror(errno));
				}
				else
					/* Successfully created partial file */
					break;
			}
			else
				elog(ERROR, "Thread [%d]: Cannot stat destination temp file \"%s\": %s",
							thread_num, to_fullpath_part, strerror(errno));
		}

		/* first round */
		if (!partial_try_count)
		{
			elog(VERBOSE, "Thread [%d]: Destination temp file already exists, waiting on it: \"%s\"",
							thread_num, to_fullpath_part);
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
			elog(ERROR, "Thread [%d]: Failed to open destination temp file \"%s\" in %i seconds",
									thread_num, to_fullpath_part, PARTIAL_WAL_TIMER);

		/* Partial segment is considered stale, so reuse it */
		elog(LOG, "Thread [%d]: Reusing stale destination temp file \"%s\"",
											thread_num, to_fullpath_part);
		fio_unlink(to_fullpath_part, FIO_BACKUP_HOST);

		out = fio_open(to_fullpath_part, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, FIO_BACKUP_HOST);
		if (out < 0)
			elog(ERROR, "Thread [%d]: Cannot open destination temp file \"%s\": %s",
							thread_num, to_fullpath_part, strerror(errno));
	}

part_opened:
	elog(VERBOSE, "Thread [%d]: Destination temp file successfully created: \"%s\"",
													thread_num, to_fullpath_part);
	/* Check if possible to skip copying */
	if (fileExists(to_fullpath, FIO_BACKUP_HOST))
	{
		pg_crc32 crc32_src;
		pg_crc32 crc32_dst;

		/* What if one of them goes missing ? */
		crc32_src = fio_get_crc32(from_fullpath, FIO_DB_HOST, false);
		crc32_dst = fio_get_crc32(to_fullpath, FIO_DB_HOST, false);

		if (crc32_src == crc32_dst)
		{
			elog(LOG, "Thread [%d]: WAL file already exists in archive with the same "
					"checksum, skip pushing file: \"%s\"", thread_num, from_fullpath);
			/* cleanup */
			fio_unlink(to_fullpath_part, FIO_BACKUP_HOST);
			return 1;
		}
		else
		{
			elog(LOG, "Thread [%d]: WAL file already exists in archive with "
						"different checksum: \"%s\"", thread_num, to_fullpath);

			if (overwrite)
				elog(LOG, "Thread [%d]: File \"%s\" already exists, overwriting",
														thread_num, to_fullpath);
			else
			{
				/* Overwriting is forbidden,
				 * so we must unlink partial file and exit with error.
				 */
				fio_unlink(to_fullpath_part, FIO_BACKUP_HOST);
				elog(ERROR, "Thread [%d]: File \"%s\" already exists",
												thread_num, to_fullpath);
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
	if (fio_sync(to_fullpath_part, FIO_BACKUP_HOST) != 0)
		elog(ERROR, "Thread [%d]: Failed to sync file \"%s\": %s",
					thread_num, to_fullpath_part, strerror(errno));

	elog(VERBOSE, "Thread [%d]: Rename \"%s\" to \"%s\"",
					thread_num, to_fullpath_part, to_fullpath);

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
gz_push_wal_file_internal(const char *wal_file_name, const char *pg_xlog_dir,
						  const char *archive_dir, bool overwrite,
						  int compress_level, int thread_num)
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
	/* to path */
	join_path_components(to_fullpath, archive_dir, wal_file_name);

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
			elog(ERROR, "Thread [%d]: Failed to open temp WAL file \"%s\": %s",
							thread_num, to_fullpath_gz_part, strerror(errno));
		/* Already existing destination temp file is not an error condition */
	}
	else
		goto part_opened;

	/* Partial file is already exists, it could have happened due to failed archive-push,
	 * in this case partial file can be discarded, or due to concurrent archiving.
	 */
	while (partial_try_count < PARTIAL_WAL_TIMER)
	{
		/* handle interrupt */
		if (interrupted || thread_interrupted)
			elog(ERROR, "Interrupted");

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
				elog(ERROR, "Thread [%d]: Cannot stat destination temp file \"%s\": %s",
							thread_num, to_fullpath_gz_part, strerror(errno));
		}

		/* first round */
		if (!partial_try_count)
		{
			elog(VERBOSE, "Thread [%d]: Destination temp WAL file already exists, waiting on it: \"%s\"",
							thread_num, to_fullpath_gz_part);
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
			elog(ERROR, "Thread [%d]: Failed to open destination temp file \"%s\" in %i seconds",
								thread_num, to_fullpath_gz_part, PARTIAL_WAL_TIMER);

		/* Partial segment is considered stale, so reuse it */
		elog(LOG, "Thread [%d]: Reusing stale destination temp file \"%s\"",
											thread_num, to_fullpath_gz_part);
		fio_unlink(to_fullpath_gz_part, FIO_BACKUP_HOST);

		out = fio_gzopen(to_fullpath_gz_part, PG_BINARY_W, compress_level, FIO_BACKUP_HOST);
		if (out == NULL)
			elog(ERROR, "Thread [%d]: Cannot open destination temp file \"%s\": %s",
								thread_num, to_fullpath_gz_part, strerror(errno));
	}

part_opened:
	elog(VERBOSE, "Thread [%d]: Destination temp file successfully created: \"%s\"",
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
					"checksum, skip pushing file: \"%s\"", thread_num, from_fullpath);
			/* cleanup */
			fio_unlink(to_fullpath_gz_part, FIO_BACKUP_HOST);
			return 1;
		}
		else
		{
			elog(LOG, "Thread [%d]: WAL file already exists in archive with "
						"different checksum: \"%s\"", thread_num, to_fullpath_gz);

			if (overwrite)
				elog(LOG, "Thread [%d]: File \"%s\" already exists, overwriting",
									thread_num, to_fullpath_gz);
			else
			{
				/* Overwriting is forbidden,
				 * so we must unlink partial file and exit with error.
				 */
				fio_unlink(to_fullpath_gz_part, FIO_BACKUP_HOST);
				elog(ERROR, "Thread [%d]: File \"%s\" already exists",
										thread_num, to_fullpath_gz);
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
				elog(ERROR, "Thread [%d]: Cannot write to compressed temp file \"%s\": %s",
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
	if (fio_sync(to_fullpath_gz_part, FIO_BACKUP_HOST) != 0)
		elog(ERROR, "Thread [%d]: Failed to sync file \"%s\": %s",
					thread_num, to_fullpath_gz_part, strerror(errno));

	elog(VERBOSE, "Thread [%d]: Rename \"%s\" to \"%s\"",
					thread_num, to_fullpath_gz_part, to_fullpath_gz);

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
 * pg_probackup specific archive command for archive backups
 * set archive_command = 'pg_probackup archive-push -B /home/anastasia/backup
 * --wal-file-path %p --wal-file-name %f', to move backups into arclog_path.
 * Where archlog_path is $BACKUP_PATH/wal/system_id.
 * Currently it just copies wal files to the new location.
 */
int
do_archive_push(InstanceConfig *instance,
				char *wal_file_path, char *wal_file_name, bool overwrite)
{
	char		backup_wal_file_path[MAXPGPATH];
	char		absolute_wal_file_path[MAXPGPATH];
	char		current_dir[MAXPGPATH];
	uint64		system_id;
	bool		is_compress = false;

	if (wal_file_name == NULL && wal_file_path == NULL)
		elog(ERROR, "required parameters are not specified: --wal-file-name %%f --wal-file-path %%p");

	if (wal_file_name == NULL)
		elog(ERROR, "required parameter not specified: --wal-file-name %%f");

	if (wal_file_path == NULL)
		elog(ERROR, "required parameter not specified: --wal-file-path %%p");

	canonicalize_path(wal_file_path);

	if (!getcwd(current_dir, sizeof(current_dir)))
		elog(ERROR, "getcwd() error");

	/* verify that archive-push --instance parameter is valid */
	system_id = get_system_identifier(current_dir);

	if (instance->pgdata == NULL)
		elog(ERROR, "cannot read pg_probackup.conf for this instance");

	if(system_id != instance->system_identifier)
		elog(ERROR, "Refuse to push WAL segment %s into archive. Instance parameters mismatch."
					"Instance '%s' should have SYSTEM_ID = " UINT64_FORMAT " instead of " UINT64_FORMAT,
			 wal_file_name, instance->name, instance->system_identifier,
			 system_id);

	/* Create 'archlog_path' directory. Do nothing if it already exists. */
	fio_mkdir(instance->arclog_path, DIR_PERMISSION, FIO_BACKUP_HOST);

	join_path_components(absolute_wal_file_path, current_dir, wal_file_path);
	join_path_components(backup_wal_file_path, instance->arclog_path, wal_file_name);

	elog(INFO, "pg_probackup archive-push from %s to %s", absolute_wal_file_path, backup_wal_file_path);

	if (instance->compress_alg == PGLZ_COMPRESS)
		elog(ERROR, "pglz compression is not supported");

#ifdef HAVE_LIBZ
	if (instance->compress_alg == ZLIB_COMPRESS)
		is_compress = IsXLogFileName(wal_file_name);
#endif

	push_wal_file(absolute_wal_file_path, backup_wal_file_path, is_compress,
				  overwrite, instance->compress_level);
	elog(INFO, "pg_probackup archive-push completed successfully");

	return 0;
}

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

/* ------------- INTERNAL FUNCTIONS ---------- */
/*
 * Copy WAL segment from pgdata to archive catalog with possible compression.
 */
void
push_wal_file(const char *from_path, const char *to_path, bool is_compress,
			  bool overwrite, int compress_level)
{
	FILE	   *in = NULL;
	int			out = -1;
	char		buf[XLOG_BLCKSZ];
	const char *to_path_p;
	char		to_path_temp[MAXPGPATH];
	int			errno_temp;
	/* partial handling */
	struct stat		st;
	int			partial_try_count = 0;
	int			partial_file_size = 0;
	bool		partial_file_exists = false;

#ifdef HAVE_LIBZ
	char		gz_to_path[MAXPGPATH];
	gzFile		gz_out = NULL;

	if (is_compress)
	{
		snprintf(gz_to_path, sizeof(gz_to_path), "%s.gz", to_path);
		to_path_p = gz_to_path;
	}
	else
#endif
		to_path_p = to_path;

	/* open source file for read */
	in = fio_fopen(from_path, PG_BINARY_R, FIO_DB_HOST);
	if (in == NULL)
		elog(ERROR, "Cannot open source WAL file \"%s\": %s", from_path,
			 strerror(errno));

	/* Check if possible to skip copying */
	// check that file exists
	if (fileExists(to_path_p, FIO_BACKUP_HOST))
	{
		// get
		if (fileEqualCRC(from_path, to_path_p, is_compress))
			return;
			/* Do not copy and do not rise error. Just quit as normal. */
		else if (!overwrite)
			elog(ERROR, "WAL segment \"%s\" already exists.", to_path_p);
	}

	/* open destination partial file for write */
#ifdef HAVE_LIBZ
	if (is_compress)
	{
		snprintf(to_path_temp, sizeof(to_path_temp), "%s.part", gz_to_path);

		gz_out = fio_gzopen(to_path_temp, PG_BINARY_W, compress_level, FIO_BACKUP_HOST);
		if (gz_out == NULL)
		{
			partial_file_exists = true;
			elog(LOG, "Cannot open destination temporary WAL file \"%s\": %s",
									to_path_temp, strerror(errno));
		}
	}
	else
#endif
	{
		snprintf(to_path_temp, sizeof(to_path_temp), "%s.part", to_path);

		out = fio_open(to_path_temp, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, FIO_BACKUP_HOST);
		if (out < 0)
		{
			partial_file_exists = true;
			elog(WARNING, "Cannot open destination temporary WAL file \"%s\": %s",
				 to_path_temp, strerror(errno));
		}
	}

	/* Partial file is already exists, it could have happened due to failed archive-push,
	 * in this case partial file can be discarded, or due to concurrent archiving.
	 *
	 * Our main goal here is to try to handle partial file to prevent stalling of
	 * continious archiving.
	 * To ensure that ecncountered partial file is actually a stale "orphaned" file,
	 * check its size every second.
	 * If the size has not changed in PARTIAL_WAL_TIMER seconds, we can consider
	 * the file stale and reuse it.
	 * If file size is changing, it means that another archiver works at the same
	 * directory with the same files. Such partial files cannot be reused.
	 */
	if (partial_file_exists)
	{
		while (partial_try_count < PARTIAL_WAL_TIMER)
		{

			if (fio_stat(to_path_temp, &st, false, FIO_BACKUP_HOST) < 0)
				/* It is ok if partial is gone, we can safely error out */
				elog(ERROR, "Cannot stat destination temporary WAL file \"%s\": %s", to_path_temp,
					strerror(errno));

			/* first round */
			if (!partial_try_count)
				partial_file_size = st.st_size;

			/* file size is changing */
			if (st.st_size > partial_file_size)
				elog(ERROR, "Destination temporary WAL file \"%s\" is not stale", to_path_temp);

			sleep(1);
			partial_try_count++;
		}

		/* Partial segment is considered stale, so reuse it */
		elog(WARNING, "Reusing stale destination temporary WAL file \"%s\"", to_path_temp);
		fio_unlink(to_path_temp, FIO_BACKUP_HOST);

#ifdef HAVE_LIBZ
		if (is_compress)
		{
			gz_out = fio_gzopen(to_path_temp, PG_BINARY_W, compress_level, FIO_BACKUP_HOST);
			if (gz_out == NULL)
				elog(ERROR, "Cannot open destination temporary WAL file \"%s\": %s",
					to_path_temp, strerror(errno));
		}
		else
#endif
		{
			out = fio_open(to_path_temp, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, FIO_BACKUP_HOST);
			if (out < 0)
				elog(ERROR, "Cannot open destination temporary WAL file \"%s\": %s",
					to_path_temp, strerror(errno));
		}
	}

	/* copy content */
	for (;;)
	{
		ssize_t		read_len = 0;

		read_len = fio_fread(in, buf, sizeof(buf));

		if (read_len < 0)
		{
			errno_temp = errno;
			fio_unlink(to_path_temp, FIO_BACKUP_HOST);
			elog(ERROR,
				 "Cannot read source WAL file \"%s\": %s",
				 from_path, strerror(errno_temp));
		}

		if (read_len > 0)
		{
#ifdef HAVE_LIBZ
			if (is_compress)
			{
				if (fio_gzwrite(gz_out, buf, read_len) != read_len)
				{
					errno_temp = errno;
					fio_unlink(to_path_temp, FIO_BACKUP_HOST);
					elog(ERROR, "Cannot write to compressed WAL file \"%s\": %s",
						 to_path_temp, get_gz_error(gz_out, errno_temp));
				}
			}
			else
#endif
			{
				if (fio_write(out, buf, read_len) != read_len)
				{
					errno_temp = errno;
					fio_unlink(to_path_temp, FIO_BACKUP_HOST);
					elog(ERROR, "Cannot write to WAL file \"%s\": %s",
						 to_path_temp, strerror(errno_temp));
				}
			}
		}

		if (read_len == 0)
			break;
	}

#ifdef HAVE_LIBZ
	if (is_compress)
	{
		if (fio_gzclose(gz_out) != 0)
		{
			errno_temp = errno;
			fio_unlink(to_path_temp, FIO_BACKUP_HOST);
			elog(ERROR, "Cannot close compressed WAL file \"%s\": %s",
				 to_path_temp, get_gz_error(gz_out, errno_temp));
		}
	}
	else
#endif
	{
		if (fio_flush(out) != 0 || fio_close(out) != 0)
		{
			errno_temp = errno;
			fio_unlink(to_path_temp, FIO_BACKUP_HOST);
			elog(ERROR, "Cannot write WAL file \"%s\": %s",
				 to_path_temp, strerror(errno_temp));
		}
	}

	if (fio_fclose(in))
	{
		errno_temp = errno;
		fio_unlink(to_path_temp, FIO_BACKUP_HOST);
		elog(ERROR, "Cannot close source WAL file \"%s\": %s",
			 from_path, strerror(errno_temp));
	}

	/* update file permission. */
	copy_file_attributes(from_path, FIO_DB_HOST, to_path_temp, FIO_BACKUP_HOST, true);

	if (fio_rename(to_path_temp, to_path_p, FIO_BACKUP_HOST) < 0)
	{
		errno_temp = errno;
		fio_unlink(to_path_temp, FIO_BACKUP_HOST);
		elog(ERROR, "Cannot rename WAL file \"%s\" to \"%s\": %s",
			 to_path_temp, to_path_p, strerror(errno_temp));
	}

#ifdef HAVE_LIBZ
	if (is_compress)
		elog(INFO, "WAL file compressed to \"%s\"", gz_to_path);
#endif
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

	if (fio_flush(out) != 0 || fio_close(out) != 0)
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

/*
 * compare CRC of two WAL files.
 * If necessary, decompress WAL file from path2
 *
 * We must compare crc of two files.
 * Files can be remote.
 * Files can be in compressed state.
 *
 * If caller wants to compare uncompressed files, then
 * decompress flag must be set.
 */
static bool
pgCompareWalCRC(const char *path1, const char *path2, bool decompress)
{
	pg_crc32	crc1;
	pg_crc32	crc2;

	/* Get checksum of backup file */
//#ifdef HAVE_LIBZ
//	if (path2_is_compressed)
//	{
//		char 		buf [1024];
//		gzFile		gz_in = NULL;
//
//		INIT_FILE_CRC32(true, crc2);
//		gz_in = fio_gzopen(path2, PG_BINARY_R, Z_DEFAULT_COMPRESSION, FIO_BACKUP_HOST);
//		if (gz_in == NULL)
//			/* File cannot be read */
//			elog(ERROR,
//					 "Cannot compare WAL file \"%s\" with compressed \"%s\"",
//					 path1, path2);
//
//		for (;;)
//		{
//			int read_len = fio_gzread(gz_in, buf, sizeof(buf));
//			if (read_len <= 0 && !fio_gzeof(gz_in))
//			{
//				/* An error occurred while reading the file */
//				elog(WARNING,
//					 "Cannot compare WAL file \"%s\" with compressed \"%s\": %d",
//					 path1, path2, read_len);
//				return false;
//			}
//			COMP_FILE_CRC32(true, crc2, buf, read_len);
//			if (fio_gzeof(gz_in) || read_len == 0)
//				break;
//		}
//		FIN_FILE_CRC32(true, crc2);
//
//		if (fio_gzclose(gz_in) != 0)
//			elog(ERROR, "Cannot close compressed WAL file \"%s\": %s",
//				 path2, get_gz_error(gz_in, errno));
//	}
//	else
//#endif
//	crc2 = pgFileGetCRC(path2, true, true, NULL, FIO_BACKUP_HOST);
	crc1 = fio_get_crc32(path2, FIO_DB_HOST, false);

	/* Get checksum of original file */
	crc2 = pgFileGetCRC(path1, true, true, NULL, FIO_DB_HOST);

	return EQ_CRC32C(crc1, crc2);
}

/*
 * compare CRC of two WAL files.
 * If necessary, decompress WAL file from path2
 */
static bool
fileEqualCRC(const char *path1, const char *path2, bool path2_is_compressed)
{
	pg_crc32	crc1;
	pg_crc32	crc2;

	/* Get checksum of backup file */
#ifdef HAVE_LIBZ
	if (path2_is_compressed)
	{
		char 		buf [1024];
		gzFile		gz_in = NULL;

		INIT_FILE_CRC32(true, crc2);
		gz_in = fio_gzopen(path2, PG_BINARY_R, Z_DEFAULT_COMPRESSION, FIO_BACKUP_HOST);
		if (gz_in == NULL)
			/* File cannot be read */
			elog(ERROR,
					 "Cannot compare WAL file \"%s\" with compressed \"%s\"",
					 path1, path2);

		for (;;)
		{
			int read_len = fio_gzread(gz_in, buf, sizeof(buf));
			if (read_len <= 0 && !fio_gzeof(gz_in))
			{
				/* An error occurred while reading the file */
				elog(WARNING,
					 "Cannot compare WAL file \"%s\" with compressed \"%s\": %d",
					 path1, path2, read_len);
				return false;
			}
			COMP_FILE_CRC32(true, crc2, buf, read_len);
			if (fio_gzeof(gz_in) || read_len == 0)
				break;
		}
		FIN_FILE_CRC32(true, crc2);

		if (fio_gzclose(gz_in) != 0)
			elog(ERROR, "Cannot close compressed WAL file \"%s\": %s",
				 path2, get_gz_error(gz_in, errno));
	}
	else
#endif
	{
		crc2 = pgFileGetCRC(path2, true, true, NULL, FIO_BACKUP_HOST);
	}

	/* Get checksum of original file */
	crc1 = pgFileGetCRC(path1, true, true, NULL, FIO_DB_HOST);

	return EQ_CRC32C(crc1, crc2);
}

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
