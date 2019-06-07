/*-------------------------------------------------------------------------
 *
 * archive.c: -  pg_probackup specific archive commands for archive backups.
 *
 *
 * Portions Copyright (c) 2018-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <unistd.h>

static void push_wal_file(const char *from_path, const char *to_path,
						  bool is_compress, bool overwrite);
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

/*
 * pg_probackup specific archive command for archive backups
 * set archive_command = 'pg_probackup archive-push -B /home/anastasia/backup
 * --wal-file-path %p --wal-file-name %f', to move backups into arclog_path.
 * Where archlog_path is $BACKUP_PATH/wal/system_id.
 * Currently it just copies wal files to the new location.
 * TODO: Planned options: list the arclog content,
 * compute and validate checksums.
 */
int
do_archive_push(char *wal_file_path, char *wal_file_name, bool overwrite)
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

	if (instance_config.pgdata == NULL)
		elog(ERROR, "cannot read pg_probackup.conf for this instance");

	if(system_id != instance_config.system_identifier)
		elog(ERROR, "Refuse to push WAL segment %s into archive. Instance parameters mismatch."
					"Instance '%s' should have SYSTEM_ID = " UINT64_FORMAT " instead of " UINT64_FORMAT,
			 wal_file_name, instance_name, instance_config.system_identifier,
			 system_id);

	/* Create 'archlog_path' directory. Do nothing if it already exists. */
	fio_mkdir(arclog_path, DIR_PERMISSION, FIO_BACKUP_HOST);

	join_path_components(absolute_wal_file_path, current_dir, wal_file_path);
	join_path_components(backup_wal_file_path, arclog_path, wal_file_name);

	elog(INFO, "pg_probackup archive-push from %s to %s", absolute_wal_file_path, backup_wal_file_path);

	if (instance_config.compress_alg == PGLZ_COMPRESS)
		elog(ERROR, "pglz compression is not supported");

#ifdef HAVE_LIBZ
	if (instance_config.compress_alg == ZLIB_COMPRESS)
		is_compress = IsXLogFileName(wal_file_name);
#endif

	push_wal_file(absolute_wal_file_path, backup_wal_file_path, is_compress,
				  overwrite);
	elog(INFO, "pg_probackup archive-push completed successfully");

	return 0;
}

/*
 * pg_probackup specific restore command.
 * Move files from arclog_path to pgdata/wal_file_path.
 */
int
do_archive_get(char *wal_file_path, char *wal_file_name)
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
	join_path_components(backup_wal_file_path, arclog_path, wal_file_name);

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
			  bool overwrite)
{
	FILE	   *in = NULL;
	FILE       *out = NULL;
	char		buf[XLOG_BLCKSZ];
	const char *to_path_p;
	char		to_path_temp[MAXPGPATH];
	int			errno_temp;

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

	/* open file for read */
	in = fio_fopen(from_path, PG_BINARY_R, FIO_DB_HOST, false);
	if (in == NULL)
		elog(ERROR, "Cannot open source WAL file \"%s\": %s", from_path,
			 strerror(errno));

	/* Check if possible to skip copying */
	if (fileExists(to_path_p, FIO_BACKUP_HOST))
	{
		if (fileEqualCRC(from_path, to_path_p, is_compress))
			return;
			/* Do not copy and do not rise error. Just quit as normal. */
		else if (!overwrite)
			elog(ERROR, "WAL segment \"%s\" already exists.", to_path_p);
	}

	/* open backup file for write  */
#ifdef HAVE_LIBZ
	if (is_compress)
	{
		snprintf(to_path_temp, sizeof(to_path_temp), "%s.partial", gz_to_path);

		gz_out = fio_gzopen(to_path_temp, PG_BINARY_W, instance_config.compress_level, FIO_BACKUP_HOST,
							instance_config.encryption);
		if (gz_out == NULL)
			elog(ERROR, "Cannot open destination temporary WAL file \"%s\": %s",
				 to_path_temp, strerror(errno));
	}
	else
#endif
	{
		int	out_fd = -1;
		snprintf(to_path_temp, sizeof(to_path_temp), "%s.partial", to_path);

		out_fd = fio_open(to_path_temp, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, FIO_BACKUP_HOST);
		if (out_fd < 0)
			elog(ERROR, "Cannot open destination temporary WAL file \"%s\": %s",
				 to_path_temp, strerror(errno));
		out = fio_fdopen(to_path_temp, out_fd, PG_BINARY_W, instance_config.encryption);
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
				ssize_t write_len = fio_fwrite(out, buf, read_len);
 				if (write_len != read_len)
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
		if (fio_fflush(out) != 0 && fio_fclose(out) != 0)
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
		in = fio_fopen(from_path, PG_BINARY_R, FIO_BACKUP_HOST, instance_config.encryption);
		if (in == NULL)
			elog(ERROR, "Cannot open source WAL file \"%s\": %s",
					from_path, strerror(errno));
	}
#ifdef HAVE_LIBZ
	else
	{
		gz_in = fio_gzopen(gz_from_path, PG_BINARY_R, Z_DEFAULT_COMPRESSION,
						   FIO_BACKUP_HOST, instance_config.encryption);
		if (gz_in == NULL)
			elog(ERROR, "Cannot open compressed WAL file \"%s\": %s",
					 gz_from_path, strerror(errno));
	}
#endif

	/* open backup file for write  */
	snprintf(to_path_temp, sizeof(to_path_temp), "%s.partial", to_path);

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
		gz_in = fio_gzopen(path2, PG_BINARY_R, Z_DEFAULT_COMPRESSION, FIO_BACKUP_HOST, instance_config.encryption);
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
