#include <c.h>
#include <fcntl.h>
#include "logging.h"
#include "file_compat.h"
/*vvs*/
/*
 * fsync_fname -- Try to fsync a file or directory
 *
 * Ignores errors trying to open unreadable files, or trying to fsync
 * directories on systems where that isn't allowed/required.  All other errors
 * are fatal.
 */
int
fsync_fname_compat(const char* fname, bool isdir)
{
	int			fd;
	int			flags;
	int			returncode;

	/*
	 * Some OSs require directories to be opened read-only whereas other
	 * systems don't allow us to fsync files opened read-only; so we need both
	 * cases here.  Using O_RDWR will cause us to fail to fsync files that are
	 * not writable by our userid, but we assume that's OK.
	 */
	flags = PG_BINARY;
	if (!isdir)
		flags |= O_RDWR;
	else
		flags |= O_RDONLY;

	/*
	 * Open the file, silently ignoring errors about unreadable files (or
	 * unsupported operations, e.g. opening a directory under Windows), and
	 * logging others.
	 */
	fd = open(fname, flags, 0);
	if (fd < 0)
	{
		if (errno == EACCES || (isdir && errno == EISDIR))
			return 0;
		pg_log_error("could not open file \"%s\": %m", fname);
		return -1;
	}

	returncode = fsync(fd);

	/*
	 * Some OSes don't allow us to fsync directories at all, so we can ignore
	 * those errors. Anything else needs to be reported.
	 */
	if (returncode != 0 && !(isdir && (errno == EBADF || errno == EINVAL)))
	{
		pg_log_fatal("could not fsync file \"%s\": %m", fname);
		(void)close(fd);
		exit(EXIT_FAILURE);
	}

	(void)close(fd);
	return 0;
}

/*
 * fsync_parent_path -- fsync the parent path of a file or directory
 *
 * This is aimed at making file operations persistent on disk in case of
 * an OS crash or power failure.
 */
int
fsync_parent_path_compat(const char* fname)
{
	char		parentpath[MAXPGPATH];

	strlcpy(parentpath, fname, MAXPGPATH);
	get_parent_directory(parentpath);

	/*
	 * get_parent_directory() returns an empty string if the input argument is
	 * just a file name (see comments in path.c), so handle that as being the
	 * current directory.
	 */
	if (strlen(parentpath) == 0)
		strlcpy(parentpath, ".", MAXPGPATH);

	if (fsync_fname_compat(parentpath, true) != 0)
		return -1;

	return 0;
}

/*
 * durable_rename -- rename(2) wrapper, issuing fsyncs required for durability
 *
 * Wrapper around rename, similar to the backend version.
 */
int
durable_rename_compat(const char* oldfile, const char* newfile)
{
	int			fd;

	/*
	 * First fsync the old and target path (if it exists), to ensure that they
	 * are properly persistent on disk. Syncing the target file is not
	 * strictly necessary, but it makes it easier to reason about crashes;
	 * because it's then guaranteed that either source or target file exists
	 * after a crash.
	 */
	if (fsync_fname_compat(oldfile, false) != 0)
		return -1;

	fd = open(newfile, PG_BINARY | O_RDWR, 0);
	if (fd < 0)
	{
		if (errno != ENOENT)
		{
			pg_log_error("could not open file \"%s\": %m", newfile);
			return -1;
		}
	}
	else
	{
		if (fsync(fd) != 0)
		{
			pg_log_fatal("could not fsync file \"%s\": %m", newfile);
			close(fd);
			exit(EXIT_FAILURE);
		}
		close(fd);
	}

	/* Time to do the real deal... */
	if (rename(oldfile, newfile) != 0)
	{
		pg_log_error("could not rename file \"%s\" to \"%s\": %m",
			oldfile, newfile);
		return -1;
	}

	/*
	 * To guarantee renaming the file is persistent, fsync the file with its
	 * new name, and its containing directory.
	 */
	if (fsync_fname_compat(newfile, false) != 0)
		return -1;

	if (fsync_parent_path_compat(newfile) != 0)
		return -1;

	return 0;
}


/* Modes for creating directories and files in the data directory */
int			pg_dir_create_mode = PG_DIR_MODE_OWNER;
int			pg_file_create_mode = PG_FILE_MODE_OWNER;
/*
 * Mode mask to pass to umask().  This is more of a preventative measure since
 * all file/directory creates should be performed using the create modes above.
 */
int			pg_mode_mask = PG_MODE_MASK_OWNER;


/*
 * Set create modes and mask to use when writing to PGDATA based on the data
 * directory mode passed.  If group read/execute are present in the mode, then
 * create modes and mask will be relaxed to allow group read/execute on all
 * newly created files and directories.
 */
void
SetDataDirectoryCreatePerm(int dataDirMode)
{
	/* If the data directory mode has group access */
	if ((PG_DIR_MODE_GROUP & dataDirMode) == PG_DIR_MODE_GROUP)
	{
		pg_dir_create_mode = PG_DIR_MODE_GROUP;
		pg_file_create_mode = PG_FILE_MODE_GROUP;
		pg_mode_mask = PG_MODE_MASK_GROUP;
	}
	/* Else use default permissions */
	else
	{
		pg_dir_create_mode = PG_DIR_MODE_OWNER;
		pg_file_create_mode = PG_FILE_MODE_OWNER;
		pg_mode_mask = PG_MODE_MASK_OWNER;
	}
}



/*
 * Get the create modes and mask to use when writing to PGDATA by examining the
 * mode of the PGDATA directory and calling SetDataDirectoryCreatePerm().
 *
 * Errors are not handled here and should be reported by the application when
 * false is returned.
 *
 * Suppress when on Windows, because there may not be proper support for Unix-y
 * file permissions.
 */
bool
GetDataDirectoryCreatePerm(const char* dataDir)
{
#if !defined(WIN32) && !defined(__CYGWIN__)
	struct stat statBuf;

	/*
	 * If an error occurs getting the mode then return false.  The caller is
	 * responsible for generating an error, if appropriate, indicating that we
	 * were unable to access the data directory.
	 */
	if (stat(dataDir, &statBuf) == -1)
		return false;

	/* Set permissions */
	SetDataDirectoryCreatePerm(statBuf.st_mode);
	return true;
#else							/* !defined(WIN32) && !defined(__CYGWIN__) */
	/*
	 * On Windows, we don't have anything to do here since they don't have
	 * Unix-y permissions.
	 */
	return true;
#endif
}

