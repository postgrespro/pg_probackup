/*-------------------------------------------------------------------------
 *
 * walmethods.c - implementations of different ways to write received wal
 *
 * NOTE! The caller must ensure that only one method is instantiated in
 *		 any given program, and that it's only instantiated once!
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_basebackup/walmethods.c
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"
#include "postgres_fe.h"

#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#ifdef HAVE_LIBZ
#include <zlib.h>
#endif

#include "common/file_utils.h"
#include "pgtar.h"
#include "receivelog.h"
#include "streamutil.h"

/* Size of zlib buffer for .tar.gz */
#define ZLIB_OUT_SIZE 4096

#include "file_compat.h"

#ifndef unconstify
#define unconstify(underlying_type, expr) \
	((underlying_type) (expr))
#endif

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
	const char *lasterrstring;	/* if set, takes precedence over lasterrno */
	int			lasterrno;
} DirectoryMethodData;
static DirectoryMethodData *dir_data = NULL;

/*
 * Local file handle
 */
typedef struct DirectoryMethodFile
{
	//int			fd;
	pioFile_i fd;
	off_t		currpos;
	char	   *pathname;
	char	   *fullpath;
	char	   *temp_suffix;
#ifdef HAVE_LIBZ
	gzFile		gzfp;
#endif
} DirectoryMethodFile;

#define dir_clear_error() \
	(dir_data->lasterrstring = NULL, dir_data->lasterrno = 0)
#define dir_set_error(msg) \
	(dir_data->lasterrstring = _(msg))

static const char *
dir_getlasterror(void)
{
	if (dir_data->lasterrstring)
		return dir_data->lasterrstring;
	return strerror(dir_data->lasterrno);
}

static char *
dir_get_file_name(const char *pathname, const char *temp_suffix)
{
	char	   *filename = pg_malloc0(MAXPGPATH * sizeof(char));

	snprintf(filename, MAXPGPATH, "%s%s%s",
			 pathname, dir_data->compression > 0 ? ".gz" : "",
			 temp_suffix ? temp_suffix : "");

	return filename;
}

static Walfile
dir_open_for_write(const char *pathname, const char *temp_suffix, size_t pad_to_size)
{
	FOBJ_FUNC_ARP();
	char		tmppath[MAXPGPATH];
	char	   *filename;
	pioFile_i			fd;
	DirectoryMethodFile *f;
	pioDrive_i backup_drive = pioDriveForLocation(FIO_BACKUP_HOST);
	err_i err = $noerr();

#ifdef HAVE_LIBZ
	gzFile		gzfp = NULL;
#endif

	dir_clear_error();

	filename = dir_get_file_name(pathname, temp_suffix);
	snprintf(tmppath, sizeof(tmppath), "%s/%s",
			 dir_data->basedir, filename);
	pg_free(filename);

	/*
	 * Open a file for non-compressed as well as compressed files. Tracking
	 * the file descriptor is important for dir_sync() method as gzflush()
	 * does not do any system calls to fsync() to make changes permanent on
	 * disk.
	 */
	//fd = open(tmppath, O_WRONLY | O_CREAT | PG_BINARY, pg_file_create_mode);
	fd = $i(pioOpen, backup_drive, tmppath, O_WRONLY | O_CREAT | PG_BINARY, .err = &err);
	if ($haserr(err))
	{
		dir_data->lasterrno = getErrno(err);
		return NULL;
	}

#ifdef HAVE_LIBZ
	if (dir_data->compression > 0)
	{
		/* vvs
		gzfp = gzdopen(fd, "wb");
		if (gzfp == NULL)
		{
			dir_data->lasterrno = errno;
			close(fd);
			return NULL;
		}

		if (gzsetparams(gzfp, dir_data->compression,
						Z_DEFAULT_STRATEGY) != Z_OK)
		{
			dir_data->lasterrno = errno;
			gzclose(gzfp);
			return NULL;
		}
		*/
	}
#endif

	/* Do pre-padding on non-compressed files */
	if (pad_to_size && dir_data->compression == 0)
	{

        err = $i(pioTruncate, fd, pad_to_size);
        if ($haserr(err))
        {
			dir_data->lasterrno =getErrno(err);
            $i(pioClose, fd);
            return NULL;
        }


/* vvs
		PGAlignedXLogBlock zerobuf;
		int			bytes;

		memset(zerobuf.data, 0, XLOG_BLCKSZ);
		for (bytes = 0; bytes < pad_to_size; bytes += XLOG_BLCKSZ)
		{
			errno = 0;
			if (write(fd, zerobuf.data, XLOG_BLCKSZ) != XLOG_BLCKSZ)
			{
				dir_data->lasterrno = errno ? errno : ENOSPC;
				close(fd);
				return NULL;
			}
		}

		if (lseek(fd, 0, SEEK_SET) != 0)
		{
			dir_data->lasterrno = errno;
			close(fd);
			return NULL;
		}
*/		
	}

	/*
	 * fsync WAL file and containing directory, to ensure the file is
	 * persistently created and zeroed (if padded). That's particularly
	 * important when using synchronous mode, where the file is modified and
	 * fsynced in-place, without a directory fsync.
	 */
	if (dir_data->sync)
	{
		err = $i(pioFlush, fd);

        if ($haserr(err))
        {
			dir_data->lasterrno =getErrno(err);
            $i(pioClose, fd);
            return NULL;
        }
		/*
		if (fsync_fname_compat(tmppath, false) != 0 ||
			fsync_parent_path_compat(tmppath) != 0)
		{
			dir_data->lasterrno = errno;
#ifdef HAVE_LIBZ
			if (dir_data->compression > 0)
				gzclose(gzfp);
			else
#endif
				close(fd);
			return NULL;
		}
		*/
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
	FOBJ_FUNC_ARP();
	ssize_t		r = 0;
	DirectoryMethodFile *df = (DirectoryMethodFile *) f;

	Assert(f != NULL);
	dir_clear_error();

	ft_bytes_t fBuf;
	err_i err = $noerr();
#ifdef HAVE_LIBZ
	if (dir_data->compression > 0)
	{
		/* vvs
		errno = 0;
		r = (ssize_t) gzwrite(df->gzfp, buf, count);
		if (r != count)
		{
			dir_data->lasterrno = errno ? errno : ENOSPC;
		}
		*/
	}
	else
#endif
	{
		errno = 0;
		//fBuf.ptr=buf;
		//fBuf.len=count;
		fBuf = ft_bytes((void *)buf, count);
		r = $i(pioWrite, df->fd, fBuf, &err);
		if (r != count || $haserr(err))
		{
			dir_data->lasterrno = getErrno(err) ? getErrno(err) : ENOSPC;
		}


/* vvs
		r = write(df->fd, buf, count);
		if (r != count)
		{
			dir_data->lasterrno = errno ? errno : ENOSPC;
		}
*/
	}
	if (r > 0)
		df->currpos += r;
	return r;
}

static off_t
dir_get_current_pos(Walfile f)
{
	Assert(f != NULL);
	dir_clear_error();

	/* Use a cached value to prevent lots of reseeks */
	return ((DirectoryMethodFile *) f)->currpos;
}

static int
dir_close(Walfile f, WalCloseMethod method)
{
	int			r = 0;
	DirectoryMethodFile *df = (DirectoryMethodFile *) f;
	char		tmppath[MAXPGPATH];
	char		tmppath2[MAXPGPATH];
	err_i err = $noerr();
	pioDrive_i backup_drive = pioDriveForLocation(FIO_BACKUP_HOST);

	Assert(f != NULL);
	dir_clear_error();

#ifdef HAVE_LIBZ
	if (dir_data->compression > 0)
	{
		errno = 0;				/* in case gzclose() doesn't set it */
		r = gzclose(df->gzfp);
	}
	else
#endif
		//r = close(df->fd);
		err = $i(pioClose, df->fd, dir_data->sync);
	if ($haserr(err))
	{
		dir_data->lasterrno = getErrno(err);
		r = -1;
	}
	else
	{
		/* Build path to the current version of the file */
		if (method == CLOSE_NORMAL && df->temp_suffix)
		{
			char	   *filename;
			char	   *filename2;

			/*
			 * If we have a temp prefix, normal operation is to rename the
			 * file.
			 */
			filename = dir_get_file_name(df->pathname, df->temp_suffix);
			snprintf(tmppath, sizeof(tmppath), "%s/%s",
					 dir_data->basedir, filename);
			pg_free(filename);

			/* permanent name, so no need for the prefix */
			filename2 = dir_get_file_name(df->pathname, NULL);
			snprintf(tmppath2, sizeof(tmppath2), "%s/%s",
					 dir_data->basedir, filename2);
			pg_free(filename2);


			//r = durable_rename_compat(tmppath, tmppath2);
			err = $i(pioRename, backup_drive, tmppath, tmppath2);


		}
		else if (method == CLOSE_UNLINK)
		{
			char	   *filename;

			/* Unlink the file once it's closed */
			filename = dir_get_file_name(df->pathname, df->temp_suffix);
			snprintf(tmppath, sizeof(tmppath), "%s/%s",
					 dir_data->basedir, filename);
			pg_free(filename);
			//r = unlink(tmppath);
			err = $i(pioRemove, backup_drive, tmppath);
		}
		else
		{
			/*
			 * Else either CLOSE_NORMAL and no temp suffix, or
			 * CLOSE_NO_RENAME. In this case, fsync the file and containing
			 * directory if sync mode is requested.
			 */
			/* vvs  - sync on close
			if (dir_data->sync)
			{
				r = fsync_fname_compat(df->fullpath, false);
				if (r == 0)
					r = fsync_parent_path_compat(df->fullpath);
			}
			*/
		}
	}

	if ($haserr(err)){
		dir_data->lasterrno = getErrno(err);
		r = -1;
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
	//int			r;
	err_i err = $noerr();
	Assert(f != NULL);
	dir_clear_error();

	if (!dir_data->sync)
		return 0;

#ifdef HAVE_LIBZ
	if (dir_data->compression > 0)
	{
		if (gzflush(((DirectoryMethodFile *) f)->gzfp, Z_SYNC_FLUSH) != Z_OK)
		{
			dir_data->lasterrno = errno;
			return -1;
		}
	}
#endif
/*
	r = fsync(((DirectoryMethodFile *) f)->fd);
	if (r < 0)
		dir_data->lasterrno = errno;
	return r;
*/

	err = $i(pioFlush, ((DirectoryMethodFile *) f)->fd);
	if ($haserr(err))
	{
		dir_data->lasterrno = getErrno(err);
		return -1;
	}	
	return 0;



}

static ssize_t
dir_get_file_size(const char *pathname)
{
	struct stat statbuf;
	char		tmppath[MAXPGPATH];
	err_i err = $noerr();
	pioDrive_i backup_drive = pioDriveForLocation(FIO_BACKUP_HOST);

	snprintf(tmppath, sizeof(tmppath), "%s/%s",
			 dir_data->basedir, pathname);

/*
	if (stat(tmppath, &statbuf) != 0)
	{
		dir_data->lasterrno = errno;
		return -1;
	}
*/
	
	statbuf = $i(pioStat, backup_drive, .err = &err);
	if ($haserr(err))
	{
		dir_data->lasterrno = getErrno(err);
		return -1;
	}	

	return statbuf.st_size;
}

static int
dir_compression(void)
{
	return dir_data->compression;
}

static bool
dir_existsfile(const char *pathname)
{
	char		tmppath[MAXPGPATH];
	//int			fd;
	bool ret;

	err_i err = $noerr();
	pioDrive_i backup_drive = pioDriveForLocation(FIO_BACKUP_HOST);

	dir_clear_error();

	snprintf(tmppath, sizeof(tmppath), "%s/%s",
			 dir_data->basedir, pathname);

/* vvs
	fd = open(tmppath, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
		return false;
	close(fd);
	return true;
*/

	ret = $i(pioExists, backup_drive, tmppath, &err);
	if ($haserr(err))
	{
		dir_data->lasterrno = getErrno(err);
	}	

	return ret;

}

static bool
dir_finish(void)
{
	dir_clear_error();

	if (dir_data->sync)
	{
		/*
		 * Files are fsynced when they are closed, but we need to fsync the
		 * directory entry here as well.
		 */
		/* vvs temp
		if (fsync_fname_compat(dir_data->basedir, true) != 0)
		{
			dir_data->lasterrno = errno;
			return false;
		}
		*/
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
	method->get_file_name = dir_get_file_name;
	method->compression = dir_compression;
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
	dir_data = NULL;
}
