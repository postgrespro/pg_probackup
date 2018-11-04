/*-------------------------------------------------------------------------
 *
 * walmethods.c - implementations of different ways to write received wal
 *
 * NOTE! The caller must ensure that only one method is instantiated in
 *		 any given program, and that it's only instantiated once!
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_basebackup/walmethods.c
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#ifdef HAVE_LIBZ
#include <zlib.h>
#endif

#include "pgtar.h"
#include "common/file_perm.h"
#include "common/file_utils.h"

#include "receivelog.h"
#include "streamutil.h"
#include "pg_probackup.h"

/* Size of zlib buffer for .tar.gz */
#define ZLIB_OUT_SIZE 4096

/*-------------------------------------------------------------------------
 * WalDirectoryMethod - write wal to a directory looking like pg_wal
 *-------------------------------------------------------------------------
 */

/*
 * Global static data for this method
 */
typedef struct DirectoryMethodData
{
	char	   *basedir;
	int			compression;
	bool		sync;
} DirectoryMethodData;
static DirectoryMethodData *dir_data = NULL;

/*
 * Local file handle
 */
typedef struct DirectoryMethodFile
{
	int			fd;
	off_t		currpos;
	char	   *pathname;
	char	   *fullpath;
	char	   *temp_suffix;
#ifdef HAVE_LIBZ
	gzFile		gzfp;
#endif
} DirectoryMethodFile;

static const char *
dir_getlasterror(void)
{
	/* Directory method always sets errno, so just use strerror */
	return strerror(errno);
}

static Walfile
dir_open_for_write(const char *pathname, const char *temp_suffix, size_t pad_to_size)
{
	static char tmppath[MAXPGPATH];
	int			fd;
	DirectoryMethodFile *f;
#ifdef HAVE_LIBZ
	gzFile		gzfp = NULL;
#endif

	snprintf(tmppath, sizeof(tmppath), "%s/%s%s%s",
			 dir_data->basedir, pathname,
			 dir_data->compression > 0 ? ".gz" : "",
			 temp_suffix ? temp_suffix : "");

	/*
	 * Open a file for non-compressed as well as compressed files. Tracking
	 * the file descriptor is important for dir_sync() method as gzflush()
	 * does not do any system calls to fsync() to make changes permanent on
	 * disk.
	 */
	fd = fio_open(tmppath, O_WRONLY | O_CREAT | PG_BINARY, FIO_BACKUP_HOST);
	if (fd < 0)
		return NULL;

#ifdef HAVE_LIBZ
	if (dir_data->compression > 0)
	{
		gzfp = gzdopen(fd, "wb");
		if (gzfp == NULL)
		{
			close(fd);
			return NULL;
		}

		if (gzsetparams(gzfp, dir_data->compression,
						Z_DEFAULT_STRATEGY) != Z_OK)
		{
			gzclose(gzfp);
			return NULL;
		}
	}
#endif

	/* Do pre-padding on non-compressed files */
	if (pad_to_size && dir_data->compression == 0)
	{
		PGAlignedXLogBlock zerobuf;
		int			bytes;

		memset(zerobuf.data, 0, XLOG_BLCKSZ);
		for (bytes = 0; bytes < pad_to_size; bytes += XLOG_BLCKSZ)
		{
			errno = 0;
			if (fio_write(fd, zerobuf.data, XLOG_BLCKSZ) != XLOG_BLCKSZ)
			{
				int			save_errno = errno;

				fio_close(fd);

				/*
				 * If write didn't set errno, assume problem is no disk space.
				 */
				errno = save_errno ? save_errno : ENOSPC;
				return NULL;
			}
		}

		if (fio_seek(fd, 0) != 0)
		{
			int			save_errno = errno;

			fio_close(fd);
			errno = save_errno;
			return NULL;
		}
	}

	/*
	 * fsync WAL file and containing directory, to ensure the file is
	 * persistently created and zeroed (if padded). That's particularly
	 * important when using synchronous mode, where the file is modified and
	 * fsynced in-place, without a directory fsync.
	 */
	if (!is_remote_agent && dir_data->sync)
	{
		if (fsync_fname(tmppath, false, progname) != 0 ||
			fsync_parent_path(tmppath, progname) != 0)
		{
#ifdef HAVE_LIBZ
			if (dir_data->compression > 0)
				gzclose(gzfp);
			else
#endif
				close(fd);
			return NULL;
		}
	}

	f = pg_malloc0(sizeof(DirectoryMethodFile));
#ifdef HAVE_LIBZ
	if (dir_data->compression > 0)
		f->gzfp = gzfp;
#endif
	f->fd = fd;
	f->currpos = 0;
	f->pathname = pg_strdup(pathname);
	f->fullpath = pg_strdup(tmppath);
	if (temp_suffix)
		f->temp_suffix = pg_strdup(temp_suffix);

	return f;
}

static ssize_t
dir_write(Walfile f, const void *buf, size_t count)
{
	ssize_t		r;
	DirectoryMethodFile *df = (DirectoryMethodFile *) f;

	Assert(f != NULL);

#ifdef HAVE_LIBZ
	if (dir_data->compression > 0)
		r = (ssize_t) gzwrite(df->gzfp, buf, count);
	else
#endif
		r = fio_write(df->fd, buf, count);
	if (r > 0)
		df->currpos += r;
	return r;
}

static off_t
dir_get_current_pos(Walfile f)
{
	Assert(f != NULL);

	/* Use a cached value to prevent lots of reseeks */
	return ((DirectoryMethodFile *) f)->currpos;
}

static int
dir_close(Walfile f, WalCloseMethod method)
{
	int			r;
	DirectoryMethodFile *df = (DirectoryMethodFile *) f;
	static char tmppath[MAXPGPATH];
	static char tmppath2[MAXPGPATH];

	Assert(f != NULL);

#ifdef HAVE_LIBZ
	if (dir_data->compression > 0)
		r = gzclose(df->gzfp);
	else
#endif
		r = fio_close(df->fd);

	if (r == 0)
	{
		/* Build path to the current version of the file */
		if (method == CLOSE_NORMAL && df->temp_suffix)
		{
			/*
			 * If we have a temp prefix, normal operation is to rename the
			 * file.
			 */
			snprintf(tmppath, sizeof(tmppath), "%s/%s%s%s",
					 dir_data->basedir, df->pathname,
					 dir_data->compression > 0 ? ".gz" : "",
					 df->temp_suffix);
			snprintf(tmppath2, sizeof(tmppath2), "%s/%s%s",
					 dir_data->basedir, df->pathname,
					 dir_data->compression > 0 ? ".gz" : "");
			r = durable_rename(tmppath, tmppath2, progname);
		}
		else if (method == CLOSE_UNLINK)
		{
			/* Unlink the file once it's closed */
			snprintf(tmppath, sizeof(tmppath), "%s/%s%s%s",
					 dir_data->basedir, df->pathname,
					 dir_data->compression > 0 ? ".gz" : "",
					 df->temp_suffix ? df->temp_suffix : "");
			r = unlink(tmppath);
		}
		else
		{
			/*
			 * Else either CLOSE_NORMAL and no temp suffix, or
			 * CLOSE_NO_RENAME. In this case, fsync the file and containing
			 * directory if sync mode is requested.
			 */
			if (dir_data->sync && !is_remote_agent)
			{
				r = fsync_fname(df->fullpath, false, progname);
				if (r == 0)
					r = fsync_parent_path(df->fullpath, progname);
			}
		}
	}

	pg_free(df->pathname);
	pg_free(df->fullpath);
	if (df->temp_suffix)
		pg_free(df->temp_suffix);
	pg_free(df);

	return r;
}

static int
dir_sync(Walfile f)
{
	Assert(f != NULL);

	if (!dir_data->sync)
		return 0;

#ifdef HAVE_LIBZ
	if (dir_data->compression > 0)
	{
		if (gzflush(((DirectoryMethodFile *) f)->gzfp, Z_SYNC_FLUSH) != Z_OK)
			return -1;
	}
#endif

	return fio_flush(((DirectoryMethodFile *) f)->fd);
}

static ssize_t
dir_get_file_size(const char *pathname)
{
	struct stat statbuf;
	static char tmppath[MAXPGPATH];
	int fd;

	snprintf(tmppath, sizeof(tmppath), "%s/%s",
			 dir_data->basedir, pathname);

	fd = fio_open(tmppath, O_RDONLY|PG_BINARY, FIO_BACKUP_HOST);
	if (fd >= 0)
	{
		if (fio_stat(fd, &statbuf) != 0)
		{
			fio_close(fd);
			return -1;
		}
		fio_close(fd);
		return statbuf.st_size;
	}
	return -1;
}

static bool
dir_existsfile(const char *pathname)
{
	static char tmppath[MAXPGPATH];

	snprintf(tmppath, sizeof(tmppath), "%s/%s",
			 dir_data->basedir, pathname);

	return fio_access(tmppath, F_OK, FIO_BACKUP_HOST) == 0;
}

static bool
dir_finish(void)
{
	if (dir_data->sync && !is_remote_agent)
	{
		/*
		 * Files are fsynced when they are closed, but we need to fsync the
		 * directory entry here as well.
		 */
		if (fsync_fname(dir_data->basedir, true, progname) != 0)
			return false;
	}
	return true;
}


WalWriteMethod *
CreateWalDirectoryMethod(const char *basedir, int compression, bool sync)
{
	WalWriteMethod *method;

	method = pg_malloc0(sizeof(WalWriteMethod));
	method->open_for_write = dir_open_for_write;
	method->write = dir_write;
	method->get_current_pos = dir_get_current_pos;
	method->get_file_size = dir_get_file_size;
	method->close = dir_close;
	method->sync = dir_sync;
	method->existsfile = dir_existsfile;
	method->finish = dir_finish;
	method->getlasterror = dir_getlasterror;

	dir_data = pg_malloc0(sizeof(DirectoryMethodData));
	dir_data->compression = compression;
	dir_data->basedir = pg_strdup(basedir);
	dir_data->sync = sync;

	return method;
}

void
FreeWalDirectoryMethod(void)
{
	pg_free(dir_data->basedir);
	pg_free(dir_data);
}

