#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

#include "pg_probackup.h"
#include "file.h"
#include "storage/checksum.h"

#define PRINTF_BUF_SIZE  1024
#define FILE_PERMISSIONS 0600

static __thread unsigned long fio_fdset = 0;
static __thread void* fio_stdin_buffer;
static __thread int fio_stdout = 0;
static __thread int fio_stdin = 0;
static __thread int fio_stderr = 0;
static char *async_errormsg = NULL;

fio_location MyLocation;

typedef struct
{
	BlockNumber nblocks;
	BlockNumber segmentno;
	XLogRecPtr  horizonLsn;
	uint32      checksumVersion;
	int         calg;
	int         clevel;
	int         bitmapsize;
	int         path_len;
} fio_send_request;


typedef struct
{
	char path[MAXPGPATH];
	bool exclude;
	bool follow_symlink;
	bool add_root;
	bool backup_logs;
	bool exclusive_backup;
	bool skip_hidden;
	int  external_dir_num;
} fio_list_dir_request;

typedef struct
{
	mode_t  mode;
	size_t  size;
	time_t  mtime;
	bool    is_datafile;
	bool    is_database;
	Oid     tblspcOid;
	Oid     dbOid;
	Oid     relOid;
	ForkName   forkName;
	int     segno;
	int     external_dir_num;
	int     linked_len;
} fio_pgFile;

typedef struct
{
	BlockNumber n_blocks;
	BlockNumber segmentno;
	XLogRecPtr  stop_lsn;
	uint32      checksumVersion;
} fio_checksum_map_request;

typedef struct
{
	BlockNumber n_blocks;
	BlockNumber segmentno;
	XLogRecPtr  shift_lsn;
	uint32      checksumVersion;
} fio_lsn_map_request;


/* Convert FIO pseudo handle to index in file descriptor array */
#define fio_fileno(f) (((size_t)f - 1) | FIO_PIPE_MARKER)

#if defined(WIN32)
#undef open(a, b, c)
#undef fopen(a, b)
#endif

/* Use specified file descriptors as stdin/stdout for FIO functions */
void fio_redirect(int in, int out, int err)
{
	fio_stdin = in;
	fio_stdout = out;
	fio_stderr = err;
}

void fio_error(int rc, int size, char const* file, int line)
{
	if (remote_agent)
	{
		fprintf(stderr, "%s:%d: processed %d bytes instead of %d: %s\n", file, line, rc, size, rc >= 0 ? "end of data" :  strerror(errno));
		exit(EXIT_FAILURE);
	}
	else
	{
		char buf[PRINTF_BUF_SIZE+1];
//		Assert(false);
		int err_size = read(fio_stderr, buf, PRINTF_BUF_SIZE);
		if (err_size > 0)
		{
			buf[err_size] = '\0';
			elog(ERROR, "Agent error: %s", buf);
		}
		else
			elog(ERROR, "Communication error: %s", rc >= 0 ? "end of data" :  strerror(errno));
	}
}

/* Check if file descriptor is local or remote (created by FIO) */
static bool fio_is_remote_fd(int fd)
{
	return (fd & FIO_PIPE_MARKER) != 0;
}

#ifdef WIN32

#undef stat

/*
 * The stat() function in win32 is not guaranteed to update the st_size
 * field when run. So we define our own version that uses the Win32 API
 * to update this field.
 */
static int
fio_safestat(const char *path, struct stat *buf)
{
    int            r;
    WIN32_FILE_ATTRIBUTE_DATA attr;

    r = stat(path, buf);
    if (r < 0)
        return r;

    if (!GetFileAttributesEx(path, GetFileExInfoStandard, &attr))
    {
        errno = ENOENT;
        return -1;
    }

    /*
     * XXX no support for large files here, but we don't do that in general on
     * Win32 yet.
     */
    buf->st_size = attr.nFileSizeLow;

    return 0;
}

#define stat(x, y) fio_safestat(x, y)

/* TODO: use real pread on Linux */
static ssize_t pread(int fd, void* buf, size_t size, off_t off)
{
	off_t rc = lseek(fd, off, SEEK_SET);
	if (rc != off)
		return -1;
	return read(fd, buf, size);
}
static int remove_file_or_dir(char const* path)
{
	int rc = remove(path);
#ifdef WIN32
	if (rc < 0 && errno == EACCESS)
		rc = rmdir(path);
#endif
	return rc;
}
#else
#define remove_file_or_dir(path) remove(path)
#endif

/* Check if specified location is local for current node */
bool fio_is_remote(fio_location location)
{
	bool is_remote = MyLocation != FIO_LOCAL_HOST
		&& location != FIO_LOCAL_HOST
		&& location != MyLocation;
	if (is_remote && !fio_stdin && !launch_agent())
		elog(ERROR, "Failed to establish SSH connection: %s", strerror(errno));
	return is_remote;
}

/* Check if specified location is local for current node */
bool fio_is_remote_simple(fio_location location)
{
	bool is_remote = MyLocation != FIO_LOCAL_HOST
		&& location != FIO_LOCAL_HOST
		&& location != MyLocation;
	return is_remote;
}

/* Try to read specified amount of bytes unless error or EOF are encountered */
static ssize_t fio_read_all(int fd, void* buf, size_t size)
{
	size_t offs = 0;
	while (offs < size)
	{
		ssize_t rc = read(fd, (char*)buf + offs, size - offs);
		if (rc < 0)
		{
			if (errno == EINTR)
				continue;
			elog(ERROR, "fio_read_all error, fd %i: %s", fd, strerror(errno));
			return rc;
		}
		else if (rc == 0)
			break;

		offs += rc;
	}
	return offs;
}

/* Try to write specified amount of bytes unless error is encountered */
static ssize_t fio_write_all(int fd, void const* buf, size_t size)
{
	size_t offs = 0;
	while (offs < size)
	{
		ssize_t rc = write(fd, (char*)buf + offs, size - offs);
		if (rc <= 0)
		{
			if (errno == EINTR)
				continue;

			elog(ERROR, "fio_write_all error, fd %i: %s", fd, strerror(errno));

			return rc;
		}
		offs += rc;
	}
	return offs;
}

/* Get version of remote agent */
int fio_get_agent_version(void)
{
	fio_header hdr;
	hdr.cop = FIO_AGENT_VERSION;
	hdr.size = 0;

	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

	return hdr.arg;
}

/* Open input stream. Remote file is fetched to the in-memory buffer and then accessed through Linux fmemopen */
FILE* fio_open_stream(char const* path, fio_location location)
{
	FILE* f;
	if (fio_is_remote(location))
	{
		fio_header hdr;
		hdr.cop = FIO_LOAD;
		hdr.size = strlen(path) + 1;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_SEND);
		if (hdr.size > 0)
		{
			Assert(fio_stdin_buffer == NULL);
			fio_stdin_buffer = pgut_malloc(hdr.size);
			IO_CHECK(fio_read_all(fio_stdin, fio_stdin_buffer, hdr.size), hdr.size);
#ifdef WIN32
			f = tmpfile();
			IO_CHECK(fwrite(f, 1, hdr.size, fio_stdin_buffer), hdr.size);
			SYS_CHECK(fseek(f, 0, SEEK_SET));
#else
			f = fmemopen(fio_stdin_buffer, hdr.size, "r");
#endif
		}
		else
		{
			f = NULL;
		}
	}
	else
	{
		f = fopen(path, "rt");
	}
	return f;
}

/* Close input stream */
int fio_close_stream(FILE* f)
{
	if (fio_stdin_buffer)
	{
		free(fio_stdin_buffer);
		fio_stdin_buffer = NULL;
	}
	return fclose(f);
}

/* Open directory */
DIR* fio_opendir(char const* path, fio_location location)
{
	DIR* dir;
	if (fio_is_remote(location))
	{
		int i;
		fio_header hdr;
		unsigned long mask;

		mask = fio_fdset;
		for (i = 0; (mask & 1) != 0; i++, mask >>= 1);
		if (i == FIO_FDMAX) {
			elog(ERROR, "Descriptor pool for remote files is exhausted, "
					"probably too many remote directories are opened");
		}
		hdr.cop = FIO_OPENDIR;
		hdr.handle = i;
		hdr.size = strlen(path) + 1;
		fio_fdset |= 1 << i;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

		if (hdr.arg != 0)
		{
			errno = hdr.arg;
			fio_fdset &= ~(1 << hdr.handle);
			return NULL;
		}
		dir = (DIR*)(size_t)(i + 1);
	}
	else
	{
		dir = opendir(path);
	}
	return dir;
}

/* Get next directory entry */
struct dirent* fio_readdir(DIR *dir)
{
	if (fio_is_remote_file((FILE*)dir))
	{
		fio_header hdr;
		static __thread struct dirent entry;

		hdr.cop = FIO_READDIR;
		hdr.handle = (size_t)dir - 1;
		hdr.size = 0;
		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_SEND);
		if (hdr.size) {
			Assert(hdr.size == sizeof(entry));
			IO_CHECK(fio_read_all(fio_stdin, &entry, sizeof(entry)), sizeof(entry));
		}

		return hdr.size ? &entry : NULL;
	}
	else
	{
		return readdir(dir);
	}
}

/* Close directory */
int fio_closedir(DIR *dir)
{
	if (fio_is_remote_file((FILE*)dir))
	{
		fio_header hdr;
		hdr.cop = FIO_CLOSEDIR;
		hdr.handle = (size_t)dir - 1;
		hdr.size = 0;
		fio_fdset &= ~(1 << hdr.handle);

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		return 0;
	}
	else
	{
		return closedir(dir);
	}
}

/* Open file */
int fio_open(char const* path, int mode, fio_location location)
{
	int fd;
	if (fio_is_remote(location))
	{
		int i;
		fio_header hdr;
		unsigned long mask;

		mask = fio_fdset;
		for (i = 0; (mask & 1) != 0; i++, mask >>= 1);
		if (i == FIO_FDMAX)
			elog(ERROR, "Descriptor pool for remote files is exhausted, "
					"probably too many remote files are opened");

		hdr.cop = FIO_OPEN;
		hdr.handle = i;
		hdr.size = strlen(path) + 1;
		hdr.arg = mode;
//		hdr.arg = mode & ~O_EXCL;
//		elog(INFO, "PATH: %s MODE: %i, %i", path, mode, O_EXCL);
//		elog(INFO, "MODE: %i", hdr.arg);
		fio_fdset |= 1 << i;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

		/* check results */
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

		if (hdr.arg != 0)
		{
			errno = hdr.arg;
			fio_fdset &= ~(1 << hdr.handle);
			return -1;
		}
		fd = i | FIO_PIPE_MARKER;
	}
	else
	{
		fd = open(path, mode, FILE_PERMISSIONS);
	}
	return fd;
}


/* Close ssh session */
void
fio_disconnect(void)
{
	if (fio_stdin)
	{
		fio_header hdr;
		hdr.cop = FIO_DISCONNECT;
		hdr.size = 0;
		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_DISCONNECTED);
		SYS_CHECK(close(fio_stdin));
		SYS_CHECK(close(fio_stdout));
		fio_stdin = 0;
		fio_stdout = 0;
		wait_ssh();
	}
}

/* Open stdio file */
FILE* fio_fopen(char const* path, char const* mode, fio_location location)
{
	FILE	   *f = NULL;

	if (fio_is_remote(location))
	{
		int flags = 0;
		int fd;
		if (strcmp(mode, PG_BINARY_W) == 0) {
			flags = O_TRUNC|PG_BINARY|O_RDWR|O_CREAT;
		} else if (strcmp(mode, "w") == 0) {
			flags = O_TRUNC|O_RDWR|O_CREAT;
		} else if (strcmp(mode, PG_BINARY_R) == 0) {
			flags = O_RDONLY|PG_BINARY;
		} else if (strcmp(mode, "r") == 0) {
			flags = O_RDONLY;
		} else if (strcmp(mode, PG_BINARY_R "+") == 0) {
			/* stdio fopen("rb+") actually doesn't create unexisted file, but probackup frequently
			 * needs to open existed file or create new one if not exists.
			 * In stdio it can be done using two fopen calls: fopen("r+") and if failed then fopen("w").
			 * But to eliminate extra call which especially critical in case of remote connection
			 * we change r+ semantic to create file if not exists.
			 */
			flags = O_RDWR|O_CREAT|PG_BINARY;
		} else if (strcmp(mode, "r+") == 0) { /* see comment above */
			flags |= O_RDWR|O_CREAT;
		} else if (strcmp(mode, "a") == 0) {
			flags |= O_CREAT|O_RDWR|O_APPEND;
		} else {
			Assert(false);
		}
		fd = fio_open(path, flags, location);
		if (fd >= 0)
			f = (FILE*)(size_t)((fd + 1) & ~FIO_PIPE_MARKER);
	}
	else
	{
		f = fopen(path, mode);
		if (f == NULL && strcmp(mode, PG_BINARY_R "+") == 0)
			f = fopen(path, PG_BINARY_W);
	}
	return f;
}

/* Format output to file stream */
int fio_fprintf(FILE* f, char const* format, ...)
{
	int rc;
    va_list args;
    va_start (args, format);
	if (fio_is_remote_file(f))
	{
		char buf[PRINTF_BUF_SIZE];
#ifdef HAS_VSNPRINTF
		rc = vsnprintf(buf, sizeof(buf), format,  args);
#else
		rc = vsprintf(buf, format,  args);
#endif
		if (rc > 0) {
			fio_fwrite(f, buf, rc);
		}
	}
	else
	{
		rc = vfprintf(f, format, args);
	}
    va_end (args);
	return rc;
}

/* Flush stream data (does nothing for remote file) */
int fio_fflush(FILE* f)
{
	int rc = 0;
	if (!fio_is_remote_file(f))
		rc = fflush(f);
	return rc;
}

/* Sync file to the disk (does nothing for remote file) */
int fio_flush(int fd)
{
	return fio_is_remote_fd(fd) ? 0 : fsync(fd);
}

/* Close output stream */
int fio_fclose(FILE* f)
{
	return fio_is_remote_file(f)
		? fio_close(fio_fileno(f))
		: fclose(f);
}

/* Close file */
int fio_close(int fd)
{
	if (fio_is_remote_fd(fd))
	{
		fio_header hdr;

		hdr.cop = FIO_CLOSE;
		hdr.handle = fd & ~FIO_PIPE_MARKER;
		hdr.size = 0;
		fio_fdset &= ~(1 << hdr.handle);

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		/* Note, that file is closed without waiting for confirmation */

		return 0;
	}
	else
	{
		return close(fd);
	}
}

/* Truncate stdio file */
int fio_ftruncate(FILE* f, off_t size)
{
	return fio_is_remote_file(f)
		? fio_truncate(fio_fileno(f), size)
		: ftruncate(fileno(f), size);
}

/* Truncate file
 * TODO: make it synchronous
 */
int fio_truncate(int fd, off_t size)
{
	if (fio_is_remote_fd(fd))
	{
		fio_header hdr;

		hdr.cop = FIO_TRUNCATE;
		hdr.handle = fd & ~FIO_PIPE_MARKER;
		hdr.size = 0;
		hdr.arg = size;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		return 0;
	}
	else
	{
		return ftruncate(fd, size);
	}
}


/*
 * Read file from specified location.
 */
int fio_pread(FILE* f, void* buf, off_t offs)
{
	if (fio_is_remote_file(f))
	{
		int fd = fio_fileno(f);
		fio_header hdr;

		hdr.cop = FIO_PREAD;
		hdr.handle = fd & ~FIO_PIPE_MARKER;
		hdr.size = 0;
		hdr.arg = offs;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_SEND);
		if (hdr.size != 0)
			IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);

		/* TODO: error handling */

		return hdr.arg;
	}
	else
	{
		/* For local file, opened by fopen, we should use stdio functions */
		int rc = fseek(f, offs, SEEK_SET);

		if (rc < 0)
			return rc;

		return fread(buf, 1, BLCKSZ, f);
	}
}

/* Set position in stdio file */
int fio_fseek(FILE* f, off_t offs)
{
	return fio_is_remote_file(f)
		? fio_seek(fio_fileno(f), offs)
		: fseek(f, offs, SEEK_SET);
}

/* Set position in file */
/* TODO: make it synchronous or check async error */
int fio_seek(int fd, off_t offs)
{
	if (fio_is_remote_fd(fd))
	{
		fio_header hdr;

		hdr.cop = FIO_SEEK;
		hdr.handle = fd & ~FIO_PIPE_MARKER;
		hdr.size = 0;
		hdr.arg = offs;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		return 0;
	}
	else
	{
		return lseek(fd, offs, SEEK_SET);
	}
}

/* seek is asynchronous */
static void
fio_seek_impl(int fd, off_t offs)
{
	int rc;

	/* Quick exit for tainted agent */
	if (async_errormsg)
		return;

	rc = lseek(fd, offs, SEEK_SET);

	if (rc < 0)
	{
		async_errormsg = pgut_malloc(ERRMSG_MAX_LEN);
		snprintf(async_errormsg, ERRMSG_MAX_LEN, "%s", strerror(errno));
	}
}

/* Write data to stdio file */
size_t fio_fwrite(FILE* f, void const* buf, size_t size)
{
	if (fio_is_remote_file(f))
		return fio_write(fio_fileno(f), buf, size);
	else
		return fwrite(buf, 1, size, f);
}

/* Write data to the file synchronously */
ssize_t fio_write(int fd, void const* buf, size_t size)
{
	if (fio_is_remote_fd(fd))
	{
		fio_header hdr;

		hdr.cop = FIO_WRITE;
		hdr.handle = fd & ~FIO_PIPE_MARKER;
		hdr.size = size;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, buf, size), size);

		/* check results */
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

		/* set errno */
		if (hdr.arg > 0)
		{
			errno = hdr.arg;
			return -1;
		}

		return size;
	}
	else
	{
		return write(fd, buf, size);
	}
}

static void
fio_write_impl(int fd, void const* buf, size_t size, int out)
{
	int rc;
	fio_header hdr;

	rc = write(fd, buf, size);

	hdr.arg = 0;
	hdr.size = 0;

	if (rc < 0)
		hdr.arg = errno;

	/* send header */
	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));

	return;
}

size_t fio_fwrite_async(FILE* f, void const* buf, size_t size)
{
	return fio_is_remote_file(f)
		? fio_write_async(fio_fileno(f), buf, size)
		: fwrite(buf, 1, size, f);
}

/* Write data to the file */
/* TODO: support async report error */
ssize_t fio_write_async(int fd, void const* buf, size_t size)
{
	if (size == 0)
		return 0;

	if (fio_is_remote_fd(fd))
	{
		fio_header hdr;

		hdr.cop = FIO_WRITE_ASYNC;
		hdr.handle = fd & ~FIO_PIPE_MARKER;
		hdr.size = size;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, buf, size), size);

		return size;
	}
	else
	{
		return write(fd, buf, size);
	}
}

static void
fio_write_async_impl(int fd, void const* buf, size_t size, int out)
{
	int rc;

	/* Quick exit if agent is tainted */
	if (async_errormsg)
		return;

	rc = write(fd, buf, size);

	if (rc <= 0)
	{
		async_errormsg = pgut_malloc(ERRMSG_MAX_LEN);
		snprintf(async_errormsg, ERRMSG_MAX_LEN, "%s", strerror(errno));
	}
}

int32
fio_decompress(void* dst, void const* src, size_t size, int compress_alg, char **errormsg)
{
	const char *internal_errormsg = NULL;
	int32 uncompressed_size = do_decompress(dst, BLCKSZ,
										    src,
											size,
											compress_alg, &internal_errormsg);

	if (uncompressed_size < 0 && internal_errormsg != NULL)
	{
		*errormsg = pgut_malloc(ERRMSG_MAX_LEN);
		snprintf(*errormsg, ERRMSG_MAX_LEN, "An error occured during decompressing block: %s", internal_errormsg);
		return -1;
	}

	if (uncompressed_size != BLCKSZ)
	{
		*errormsg = pgut_malloc(ERRMSG_MAX_LEN);
		snprintf(*errormsg, ERRMSG_MAX_LEN, "Page uncompressed to %d bytes != BLCKSZ", uncompressed_size);
		return -1;
	}
	return uncompressed_size;
}

/* Write data to the file */
ssize_t fio_fwrite_async_compressed(FILE* f, void const* buf, size_t size, int compress_alg)
{
	if (fio_is_remote_file(f))
	{
		fio_header hdr;

		hdr.cop = FIO_WRITE_COMPRESSED_ASYNC;
		hdr.handle = fio_fileno(f) & ~FIO_PIPE_MARKER;
		hdr.size = size;
		hdr.arg = compress_alg;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, buf, size), size);

		return size;
	}
	else
	{
		char uncompressed_buf[BLCKSZ];
		char *errormsg = NULL;
		int32 uncompressed_size = fio_decompress(uncompressed_buf, buf, size, compress_alg, &errormsg);

		if (uncompressed_size < 0)
			elog(ERROR, "%s", errormsg);

		return fwrite(uncompressed_buf, 1, uncompressed_size, f);
	}
}

static void
fio_write_compressed_impl(int fd, void const* buf, size_t size, int compress_alg)
{
	int rc;
	int32 uncompressed_size;
	char uncompressed_buf[BLCKSZ];

	/* If the previous command already have failed,
	 * then there is no point in bashing a head against the wall
	 */
	if (async_errormsg)
		return;

	/* decompress chunk */
	uncompressed_size = fio_decompress(uncompressed_buf, buf, size, compress_alg, &async_errormsg);

	if (uncompressed_size < 0)
		return;

	rc = write(fd, uncompressed_buf, uncompressed_size);

	if (rc <= 0)
	{
		async_errormsg = pgut_malloc(ERRMSG_MAX_LEN);
		snprintf(async_errormsg, ERRMSG_MAX_LEN, "%s", strerror(errno));
	}
}

/* check if remote agent encountered any error during execution of async operations */
int
fio_check_error_file(FILE* f, char **errmsg)
{
	if (fio_is_remote_file(f))
	{
		fio_header hdr;

		hdr.cop = FIO_GET_ASYNC_ERROR;
		hdr.size = 0;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		/* check results */
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

		if (hdr.size > 0)
		{
			*errmsg = pgut_malloc(ERRMSG_MAX_LEN);
			IO_CHECK(fio_read_all(fio_stdin, *errmsg, hdr.size), hdr.size);
			return 1;
		}
	}

	return 0;
}

/* check if remote agent encountered any error during execution of async operations */
int
fio_check_error_fd(int fd, char **errmsg)
{
	if (fio_is_remote_fd(fd))
	{
		fio_header hdr;

		hdr.cop = FIO_GET_ASYNC_ERROR;
		hdr.size = 0;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		/* check results */
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

		if (hdr.size > 0)
		{
			*errmsg = pgut_malloc(ERRMSG_MAX_LEN);
			IO_CHECK(fio_read_all(fio_stdin, *errmsg, hdr.size), hdr.size);
			return 1;
		}
	}
	return 0;
}

static void
fio_get_async_error_impl(int out)
{
	fio_header hdr;
	hdr.cop = FIO_GET_ASYNC_ERROR;

	/* send error message */
	if (async_errormsg)
	{
		hdr.size = strlen(async_errormsg) + 1;

		/* send header */
		IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));

		/* send message itself */
		IO_CHECK(fio_write_all(out, async_errormsg, hdr.size), hdr.size);

		//TODO: should we reset the tainted state ?
//		pg_free(async_errormsg);
//		async_errormsg = NULL;
	}
	else
	{
		hdr.size = 0;
		/* send header */
		IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
	}
}

/* Read data from stdio file */
ssize_t fio_fread(FILE* f, void* buf, size_t size)
{
	size_t rc;
	if (fio_is_remote_file(f))
		return fio_read(fio_fileno(f), buf, size);
	rc = fread(buf, 1, size, f);
	return rc == 0 && !feof(f) ? -1 : rc;
}

/* Read data from file */
ssize_t fio_read(int fd, void* buf, size_t size)
{
	if (fio_is_remote_fd(fd))
	{
		fio_header hdr;

		hdr.cop = FIO_READ;
		hdr.handle = fd & ~FIO_PIPE_MARKER;
		hdr.size = 0;
		hdr.arg = size;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_SEND);
		IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);

		return hdr.size;
	}
	else
	{
		return read(fd, buf, size);
	}
}

/* Get information about file */
int fio_stat(char const* path, struct stat* st, bool follow_symlink, fio_location location)
{
	if (fio_is_remote(location))
	{
		fio_header hdr;
		size_t path_len = strlen(path) + 1;

		hdr.cop = FIO_STAT;
		hdr.handle = -1;
		hdr.arg = follow_symlink;
		hdr.size = path_len;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_STAT);
		IO_CHECK(fio_read_all(fio_stdin, st, sizeof(*st)), sizeof(*st));

		if (hdr.arg != 0)
		{
			errno = hdr.arg;
			return -1;
		}
		return 0;
	}
	else
	{
		return follow_symlink ? stat(path, st) : lstat(path,  st);
	}
}

/* Check presence of the file */
int fio_access(char const* path, int mode, fio_location location)
{
	if (fio_is_remote(location))
	{
		fio_header hdr;
		size_t path_len = strlen(path) + 1;
		hdr.cop = FIO_ACCESS;
		hdr.handle = -1;
		hdr.size = path_len;
		hdr.arg = mode;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_ACCESS);

		if (hdr.arg != 0)
		{
			errno = hdr.arg;
			return -1;
		}
		return 0;
	}
	else
	{
		return access(path, mode);
	}
}

/* Create symbolic link */
int fio_symlink(char const* target, char const* link_path, bool overwrite, fio_location location)
{
	if (fio_is_remote(location))
	{
		fio_header hdr;
		size_t target_len = strlen(target) + 1;
		size_t link_path_len = strlen(link_path) + 1;
		hdr.cop = FIO_SYMLINK;
		hdr.handle = -1;
		hdr.size = target_len + link_path_len;
		hdr.arg = overwrite ? 1 : 0;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, target, target_len), target_len);
		IO_CHECK(fio_write_all(fio_stdout, link_path, link_path_len), link_path_len);

		return 0;
	}
	else
	{
		if (overwrite)
			remove_file_or_dir(link_path);

		return symlink(target, link_path);
	}
}

static void fio_symlink_impl(int out, char *buf, bool overwrite)
{
	char *linked_path = buf;
	char *link_path = buf + strlen(buf) + 1;

	if (overwrite)
		remove_file_or_dir(link_path);

	if (symlink(linked_path, link_path))
		elog(ERROR, "Could not create symbolic link \"%s\": %s",
			link_path, strerror(errno));
}

/* Rename file */
int fio_rename(char const* old_path, char const* new_path, fio_location location)
{
	if (fio_is_remote(location))
	{
		fio_header hdr;
		size_t old_path_len = strlen(old_path) + 1;
		size_t new_path_len = strlen(new_path) + 1;
		hdr.cop = FIO_RENAME;
		hdr.handle = -1;
		hdr.size = old_path_len + new_path_len;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, old_path, old_path_len), old_path_len);
		IO_CHECK(fio_write_all(fio_stdout, new_path, new_path_len), new_path_len);

		//TODO: wait for confirmation.

		return 0;
	}
	else
	{
		return rename(old_path, new_path);
	}
}

/* Sync file to disk */
int fio_sync(char const* path, fio_location location)
{
	if (fio_is_remote(location))
	{
		fio_header hdr;
		size_t path_len = strlen(path) + 1;
		hdr.cop = FIO_SYNC;
		hdr.handle = -1;
		hdr.size = path_len;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

		if (hdr.arg != 0)
		{
			errno = hdr.arg;
			return -1;
		}

		return 0;
	}
	else
	{
		int fd;

		fd = open(path, O_WRONLY | PG_BINARY, FILE_PERMISSIONS);
		if (fd < 0)
			return -1;

		if (fsync(fd) < 0)
		{
			close(fd);
			return -1;
		}
		close(fd);

		return 0;
	}
}

/* Get crc32 of file */
pg_crc32 fio_get_crc32(const char *file_path, fio_location location, bool decompress)
{
	if (fio_is_remote(location))
	{
		fio_header hdr;
		size_t path_len = strlen(file_path) + 1;
		pg_crc32 crc = 0;
		hdr.cop = FIO_GET_CRC32;
		hdr.handle = -1;
		hdr.size = path_len;
		hdr.arg = 0;

		if (decompress)
			hdr.arg = 1;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, file_path, path_len), path_len);
		IO_CHECK(fio_read_all(fio_stdin, &crc, sizeof(crc)), sizeof(crc));

		return crc;
	}
	else
	{
		if (decompress)
			return pgFileGetCRCgz(file_path, true, true);
		else
			return pgFileGetCRC(file_path, true, true);
	}
}

/* Remove file */
int fio_unlink(char const* path, fio_location location)
{
	if (fio_is_remote(location))
	{
		fio_header hdr;
		size_t path_len = strlen(path) + 1;
		hdr.cop = FIO_UNLINK;
		hdr.handle = -1;
		hdr.size = path_len;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);

		// TODO: error is swallowed ?
		return 0;
	}
	else
	{
		return remove_file_or_dir(path);
	}
}

/* Create directory
 * TODO: add strict flag
 */
int fio_mkdir(char const* path, int mode, fio_location location)
{
	if (fio_is_remote(location))
	{
		fio_header hdr;
		size_t path_len = strlen(path) + 1;
		hdr.cop = FIO_MKDIR;
		hdr.handle = -1;
		hdr.size = path_len;
		hdr.arg = mode;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_MKDIR);

		return hdr.arg;
	}
	else
	{
		return dir_create_dir(path, mode, false);
	}
}

/* Change file mode */
int fio_chmod(char const* path, int mode, fio_location location)
{
	if (fio_is_remote(location))
	{
		fio_header hdr;
		size_t path_len = strlen(path) + 1;
		hdr.cop = FIO_CHMOD;
		hdr.handle = -1;
		hdr.size = path_len;
		hdr.arg = mode;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);

		return 0;
	}
	else
	{
		return chmod(path, mode);
	}
}

#ifdef HAVE_LIBZ

#define ZLIB_BUFFER_SIZE     (64*1024)
#define MAX_WBITS            15 /* 32K LZ77 window */
#define DEF_MEM_LEVEL        8
/* last bit used to differenciate remote gzFile from local gzFile
 * TODO: this is insane, we should create our own scructure for this,
 * not flip some bits in someone's else and hope that it will not break
 * between zlib versions.
 */
#define FIO_GZ_REMOTE_MARKER 1

typedef struct fioGZFile
{
	z_stream strm;
	int      fd;
	int      errnum;
	bool     compress;
	bool     eof;
	Bytef    buf[ZLIB_BUFFER_SIZE];
} fioGZFile;

/* check if remote agent encountered any error during execution of async operations */
int
fio_check_error_fd_gz(gzFile f, char **errmsg)
{
	if (f && ((size_t)f & FIO_GZ_REMOTE_MARKER))
	{
		fio_header hdr;

		hdr.cop = FIO_GET_ASYNC_ERROR;
		hdr.size = 0;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		/* check results */
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

		if (hdr.size > 0)
		{
			*errmsg = pgut_malloc(ERRMSG_MAX_LEN);
			IO_CHECK(fio_read_all(fio_stdin, *errmsg, hdr.size), hdr.size);
			return 1;
		}
	}
	return 0;
}

/* On error returns NULL and errno should be checked */
gzFile
fio_gzopen(char const* path, char const* mode, int level, fio_location location)
{
	int rc;
	if (fio_is_remote(location))
	{
		fioGZFile* gz = (fioGZFile*) pgut_malloc(sizeof(fioGZFile));
		memset(&gz->strm, 0, sizeof(gz->strm));
		gz->eof = 0;
		gz->errnum = Z_OK;
		/* check if file opened for writing */
		if (strcmp(mode, PG_BINARY_W) == 0) /* compress */
		{
			gz->strm.next_out = gz->buf;
			gz->strm.avail_out = ZLIB_BUFFER_SIZE;
			rc = deflateInit2(&gz->strm,
							  level,
							  Z_DEFLATED,
							  MAX_WBITS + 16, DEF_MEM_LEVEL,
							  Z_DEFAULT_STRATEGY);
			if (rc == Z_OK)
			{
				gz->compress = 1;
				gz->fd = fio_open(path, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY, location);
				if (gz->fd < 0)
				{
					free(gz);
					return NULL;
				}
			}
		}
		else
		{
			gz->strm.next_in = gz->buf;
			gz->strm.avail_in = ZLIB_BUFFER_SIZE;
			rc = inflateInit2(&gz->strm, 15 + 16);
			gz->strm.avail_in = 0;
			if (rc == Z_OK)
			{
				gz->compress = 0;
				gz->fd = fio_open(path, O_RDONLY | PG_BINARY, location);
				if (gz->fd < 0)
				{
					free(gz);
					return NULL;
				}
			}
		}
		if (rc != Z_OK)
		{
			elog(ERROR, "zlib internal error when opening file %s: %s",
				path, gz->strm.msg);
		}
		return (gzFile)((size_t)gz + FIO_GZ_REMOTE_MARKER);
	}
	else
	{
		gzFile file;
		/* check if file opened for writing */
		if (strcmp(mode, PG_BINARY_W) == 0)
		{
			int fd = open(path, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY, FILE_PERMISSIONS);
			if (fd < 0)
				return NULL;
			file = gzdopen(fd, mode);
		}
		else
			file = gzopen(path, mode);
		if (file != NULL && level != Z_DEFAULT_COMPRESSION)
		{
			if (gzsetparams(file, level, Z_DEFAULT_STRATEGY) != Z_OK)
				elog(ERROR, "Cannot set compression level %d: %s",
					 level, strerror(errno));
		}
		return file;
	}
}

int
fio_gzread(gzFile f, void *buf, unsigned size)
{
	if ((size_t)f & FIO_GZ_REMOTE_MARKER)
	{
		int rc;
		fioGZFile* gz = (fioGZFile*)((size_t)f - FIO_GZ_REMOTE_MARKER);

		if (gz->eof)
		{
			return 0;
		}

		gz->strm.next_out = (Bytef *)buf;
		gz->strm.avail_out = size;

		while (1)
		{
			if (gz->strm.avail_in != 0) /* If there is some data in receiver buffer, then decompress it */
			{
				rc = inflate(&gz->strm, Z_NO_FLUSH);
				if (rc == Z_STREAM_END)
				{
					gz->eof = 1;
				}
				else if (rc != Z_OK)
				{
					gz->errnum = rc;
					return -1;
				}
				if (gz->strm.avail_out != size)
				{
					return size - gz->strm.avail_out;
				}
				if (gz->strm.avail_in == 0)
				{
					gz->strm.next_in = gz->buf;
				}
			}
			else
			{
				gz->strm.next_in = gz->buf;
			}
			rc = fio_read(gz->fd, gz->strm.next_in + gz->strm.avail_in,
						  gz->buf + ZLIB_BUFFER_SIZE - gz->strm.next_in - gz->strm.avail_in);
			if (rc > 0)
			{
				gz->strm.avail_in += rc;
			}
			else
			{
				if (rc == 0)
				{
					gz->eof = 1;
				}
				return rc;
			}
		}
	}
	else
	{
		return gzread(f, buf, size);
	}
}

int
fio_gzwrite(gzFile f, void const* buf, unsigned size)
{
	if ((size_t)f & FIO_GZ_REMOTE_MARKER)
	{
		int rc;
		fioGZFile* gz = (fioGZFile*)((size_t)f - FIO_GZ_REMOTE_MARKER);

		gz->strm.next_in = (Bytef *)buf;
		gz->strm.avail_in = size;

		do
		{
			if (gz->strm.avail_out == ZLIB_BUFFER_SIZE) /* Compress buffer is empty */
			{
				gz->strm.next_out = gz->buf; /* Reset pointer to the  beginning of buffer */

				if (gz->strm.avail_in != 0) /* Has something in input buffer */
				{
					rc = deflate(&gz->strm, Z_NO_FLUSH);
					Assert(rc == Z_OK);
					gz->strm.next_out = gz->buf; /* Reset pointer to the  beginning of buffer */
				}
				else
				{
					break;
				}
			}
			rc = fio_write_async(gz->fd, gz->strm.next_out, ZLIB_BUFFER_SIZE - gz->strm.avail_out);
			if (rc >= 0)
			{
				gz->strm.next_out += rc;
				gz->strm.avail_out += rc;
			}
			else
			{
				return rc;
			}
		} while (gz->strm.avail_out != ZLIB_BUFFER_SIZE || gz->strm.avail_in != 0);

		return size;
	}
	else
	{
		return gzwrite(f, buf, size);
	}
}

int
fio_gzclose(gzFile f)
{
	if ((size_t)f & FIO_GZ_REMOTE_MARKER)
	{
		fioGZFile* gz = (fioGZFile*)((size_t)f - FIO_GZ_REMOTE_MARKER);
		int rc;
		if (gz->compress)
		{
			gz->strm.next_out = gz->buf;
			rc = deflate(&gz->strm, Z_FINISH);
			Assert(rc == Z_STREAM_END && gz->strm.avail_out != ZLIB_BUFFER_SIZE);
			deflateEnd(&gz->strm);
			rc = fio_write(gz->fd, gz->buf, ZLIB_BUFFER_SIZE - gz->strm.avail_out);
			if (rc != ZLIB_BUFFER_SIZE - gz->strm.avail_out)
			{
				return -1;
			}
		}
		else
		{
			inflateEnd(&gz->strm);
		}
		rc = fio_close(gz->fd);
		free(gz);
		return rc;
	}
	else
	{
		return gzclose(f);
	}
}

int fio_gzeof(gzFile f)
{
	if ((size_t)f & FIO_GZ_REMOTE_MARKER)
	{
		fioGZFile* gz = (fioGZFile*)((size_t)f - FIO_GZ_REMOTE_MARKER);
		return gz->eof;
	}
	else
	{
		return gzeof(f);
	}
}

const char* fio_gzerror(gzFile f, int *errnum)
{
	if ((size_t)f & FIO_GZ_REMOTE_MARKER)
	{
		fioGZFile* gz = (fioGZFile*)((size_t)f - FIO_GZ_REMOTE_MARKER);
		if (errnum)
			*errnum = gz->errnum;
		return gz->strm.msg;
	}
	else
	{
		return gzerror(f, errnum);
	}
}

z_off_t fio_gzseek(gzFile f, z_off_t offset, int whence)
{
	Assert(!((size_t)f & FIO_GZ_REMOTE_MARKER));
	return gzseek(f, offset, whence);
}


#endif

/* Send file content
 * Note: it should not be used for large files.
 */
static void fio_load_file(int out, char const* path)
{
	int fd = open(path, O_RDONLY);
	fio_header hdr;
	void* buf = NULL;

	hdr.cop = FIO_SEND;
	hdr.size = 0;

	if (fd >= 0)
	{
		off_t size = lseek(fd, 0, SEEK_END);
		buf = pgut_malloc(size);
		lseek(fd, 0, SEEK_SET);
		IO_CHECK(fio_read_all(fd, buf, size), size);
		hdr.size = size;
		SYS_CHECK(close(fd));
	}
	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
	if (buf)
	{
		IO_CHECK(fio_write_all(out, buf, hdr.size), hdr.size);
		free(buf);
	}
}

/*
 * Return number of actually(!) readed blocks, attempts or
 * half-readed block are not counted.
 * Return values in case of error:
 *  FILE_MISSING
 *  OPEN_FAILED
 *  READ_ERROR
 *  PAGE_CORRUPTION
 *  WRITE_FAILED
 *
 * If none of the above, this function return number of blocks
 * readed by remote agent.
 *
 * In case of DELTA mode horizonLsn must be a valid lsn,
 * otherwise it should be set to InvalidXLogRecPtr.
 */
int fio_send_pages(const char *to_fullpath, const char *from_fullpath, pgFile *file,
				   XLogRecPtr horizonLsn, int calg, int clevel, uint32 checksum_version,
				   bool use_pagemap, BlockNumber* err_blknum, char **errormsg,
				   BackupPageHeader2 **headers)
{
	FILE *out = NULL;
	char *out_buf = NULL;
	struct {
		fio_header hdr;
		fio_send_request arg;
	} req;
	BlockNumber	n_blocks_read = 0;
	BlockNumber blknum = 0;

	/* send message with header

	  8bytes       24bytes             var        var
	--------------------------------------------------------------
	| fio_header | fio_send_request | FILE PATH | BITMAP(if any) |
	--------------------------------------------------------------
	*/

	req.hdr.cop = FIO_SEND_PAGES;

	if (use_pagemap)
	{
		req.hdr.size = sizeof(fio_send_request) + (*file).pagemap.bitmapsize + strlen(from_fullpath) + 1;
		req.arg.bitmapsize = (*file).pagemap.bitmapsize;

		/* TODO: add optimization for the case of pagemap
		 * containing small number of blocks with big serial numbers:
		 * https://github.com/postgrespro/pg_probackup/blob/remote_page_backup/src/utils/file.c#L1211
		 */
	}
	else
	{
		req.hdr.size = sizeof(fio_send_request) + strlen(from_fullpath) + 1;
		req.arg.bitmapsize = 0;
	}

	req.arg.nblocks = file->size/BLCKSZ;
	req.arg.segmentno = file->segno * RELSEG_SIZE;
	req.arg.horizonLsn = horizonLsn;
	req.arg.checksumVersion = checksum_version;
	req.arg.calg = calg;
	req.arg.clevel = clevel;
	req.arg.path_len = strlen(from_fullpath) + 1;

	file->compress_alg = calg; /* TODO: wtf? why here? */

//<-----
//	datapagemap_iterator_t *iter;
//	BlockNumber blkno;
//	iter = datapagemap_iterate(pagemap);
//	while (datapagemap_next(iter, &blkno))
//		elog(INFO, "block %u", blkno);
//	pg_free(iter);
//<-----

	/* send header */
	IO_CHECK(fio_write_all(fio_stdout, &req, sizeof(req)), sizeof(req));

	/* send file path */
	IO_CHECK(fio_write_all(fio_stdout, from_fullpath, req.arg.path_len), req.arg.path_len);

	/* send pagemap if any */
	if (use_pagemap)
		IO_CHECK(fio_write_all(fio_stdout, (*file).pagemap.bitmap, (*file).pagemap.bitmapsize), (*file).pagemap.bitmapsize);

	while (true)
	{
		fio_header hdr;
		char buf[BLCKSZ + sizeof(BackupPageHeader)];
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

		if (interrupted)
			elog(ERROR, "Interrupted during page reading");

		if (hdr.cop == FIO_ERROR)
		{
			/* FILE_MISSING, OPEN_FAILED and READ_FAILED */
			if (hdr.size > 0)
			{
				IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);
				*errormsg = pgut_malloc(hdr.size);
				snprintf(*errormsg, hdr.size, "%s", buf);
			}

			return hdr.arg;
		}
		else if (hdr.cop == FIO_SEND_FILE_CORRUPTION)
		{
			*err_blknum = hdr.arg;

			if (hdr.size > 0)
			{
				IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);
				*errormsg = pgut_malloc(hdr.size);
				snprintf(*errormsg, hdr.size, "%s", buf);
			}
			return PAGE_CORRUPTION;
		}
		else if (hdr.cop == FIO_SEND_FILE_EOF)
		{
			/* n_blocks_read reported by EOF */
			n_blocks_read = hdr.arg;

			/* receive headers if any */
			if (hdr.size > 0)
			{
				*headers = pgut_malloc(hdr.size);
				IO_CHECK(fio_read_all(fio_stdin, *headers, hdr.size), hdr.size);
				file->n_headers = (hdr.size / sizeof(BackupPageHeader2)) -1;
			}

			break;
		}
		else if (hdr.cop == FIO_PAGE)
		{
			blknum = hdr.arg;

			Assert(hdr.size <= sizeof(buf));
			IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);

			COMP_FILE_CRC32(true, file->crc, buf, hdr.size);

			/* lazily open backup file */
			if (!out)
				out = open_local_file_rw(to_fullpath, &out_buf, STDIO_BUFSIZE);

			if (fio_fwrite(out, buf, hdr.size) != hdr.size)
			{
				fio_fclose(out);
				*err_blknum = blknum;
				return WRITE_FAILED;
			}
			file->write_size += hdr.size;
			file->uncompressed_size += BLCKSZ;
		}
		else
			elog(ERROR, "Remote agent returned message of unexpected type: %i", hdr.cop);
	}

	if (out)
		fclose(out);
	pg_free(out_buf);

	return n_blocks_read;
}

/* TODO: read file using large buffer
 * Return codes:
 *  FIO_ERROR:
 *  	FILE_MISSING (-1)
 *  	OPEN_FAILED  (-2)
 *  	READ_FAILED  (-3)

 *  FIO_SEND_FILE_CORRUPTION
 *  FIO_SEND_FILE_EOF
 */
static void fio_send_pages_impl(int out, char* buf)
{
	FILE        *in = NULL;
	BlockNumber  blknum = 0;
	int          current_pos = 0;
	BlockNumber  n_blocks_read = 0;
	PageState    page_st;
	char         read_buffer[BLCKSZ+1];
	char         in_buf[STDIO_BUFSIZE];
	fio_header   hdr;
	fio_send_request *req = (fio_send_request*) buf;
	char             *from_fullpath = (char*) buf + sizeof(fio_send_request);
	bool with_pagemap = req->bitmapsize > 0 ? true : false;
	/* error reporting */
	char *errormsg = NULL;
	/* parse buffer */
	datapagemap_t *map = NULL;
	datapagemap_iterator_t *iter = NULL;
	/* page headers */
	int32       hdr_num = -1;
	int32       cur_pos_out = 0;
	BackupPageHeader2 *headers = NULL;

	/* open source file */
	in = fopen(from_fullpath, PG_BINARY_R);
	if (!in)
	{
		hdr.cop = FIO_ERROR;

		/* do not send exact wording of ENOENT error message
		 * because it is a very common error in our case, so
		 * error code is enough.
		 */
		if (errno == ENOENT)
		{
			hdr.arg = FILE_MISSING;
			hdr.size = 0;
		}
		else
		{
			hdr.arg = OPEN_FAILED;
			errormsg = pgut_malloc(ERRMSG_MAX_LEN);
			/* Construct the error message */
			snprintf(errormsg, ERRMSG_MAX_LEN, "Cannot open file \"%s\": %s",
					 from_fullpath, strerror(errno));
			hdr.size = strlen(errormsg) + 1;
		}

		/* send header and message */
		IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
		if (errormsg)
			IO_CHECK(fio_write_all(out, errormsg, hdr.size), hdr.size);

		goto cleanup;
	}

	if (with_pagemap)
	{
		map = pgut_malloc(sizeof(datapagemap_t));
		map->bitmapsize = req->bitmapsize;
		map->bitmap = (char*) buf + sizeof(fio_send_request) + req->path_len;

		/* get first block */
		iter = datapagemap_iterate(map);
		datapagemap_next(iter, &blknum);

		setvbuf(in, NULL, _IONBF, BUFSIZ);
	}
	else
		setvbuf(in, in_buf, _IOFBF, STDIO_BUFSIZE);

	/* TODO: what is this barrier for? */
	read_buffer[BLCKSZ] = 1; /* barrier */

	while (blknum < req->nblocks)
	{
		int    rc = 0;
		size_t read_len = 0;
		int    retry_attempts = PAGE_READ_ATTEMPTS;

		/* TODO: handle signals on the agent */
		if (interrupted)
			elog(ERROR, "Interrupted during remote page reading");

		/* read page, check header and validate checksumms */
		for (;;)
		{
			/*
			 * Optimize stdio buffer usage, fseek only when current position
			 * does not match the position of requested block.
			 */
			if (current_pos != blknum*BLCKSZ)
			{
				current_pos = blknum*BLCKSZ;
				if (fseek(in, current_pos, SEEK_SET) != 0)
					elog(ERROR, "fseek to position %u is failed on remote file '%s': %s",
							current_pos, from_fullpath, strerror(errno));
			}

			read_len = fread(read_buffer, 1, BLCKSZ, in);

			current_pos += read_len;

			/* report error */
			if (ferror(in))
			{
				hdr.cop = FIO_ERROR;
				hdr.arg = READ_FAILED;

				errormsg = pgut_malloc(ERRMSG_MAX_LEN);
				/* Construct the error message */
				snprintf(errormsg, ERRMSG_MAX_LEN, "Cannot read block %u of '%s': %s",
						blknum, from_fullpath, strerror(errno));
				hdr.size = strlen(errormsg) + 1;

				/* send header and message */
				IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
				IO_CHECK(fio_write_all(out, errormsg, hdr.size), hdr.size);
				goto cleanup;
			}

			if (read_len == BLCKSZ)
			{
				rc = validate_one_page(read_buffer, req->segmentno + blknum,
										   InvalidXLogRecPtr, &page_st,
										   req->checksumVersion);

				/* TODO: optimize copy of zeroed page */
				if (rc == PAGE_IS_ZEROED)
					break;
				else if (rc == PAGE_IS_VALID)
					break;
			}

			if (feof(in))
				goto eof;
//		  	else /* readed less than BLKSZ bytes, retry */

			/* File is either has insane header or invalid checksum,
			 * retry. If retry attempts are exhausted, report corruption.
			 */
			if (--retry_attempts == 0)
			{
				hdr.cop = FIO_SEND_FILE_CORRUPTION;
				hdr.arg = blknum;

				/* Construct the error message */
				if (rc == PAGE_HEADER_IS_INVALID)
					get_header_errormsg(read_buffer, &errormsg);
				else if (rc == PAGE_CHECKSUM_MISMATCH)
					get_checksum_errormsg(read_buffer, &errormsg,
										  req->segmentno + blknum);

				/* if error message is not empty, set payload size to its length */
				hdr.size = errormsg ? strlen(errormsg) + 1 : 0;

				/* send header */
				IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));

				/* send error message if any */
				if (errormsg)
					IO_CHECK(fio_write_all(out, errormsg, hdr.size), hdr.size);

				goto cleanup;
			}
		}

		n_blocks_read++;

		/*
		 * horizonLsn is not 0 only in case of delta backup.
		 * As far as unsigned number are always greater or equal than zero,
		 * there is no sense to add more checks.
		 */
		if ((req->horizonLsn == InvalidXLogRecPtr) ||                 /* full, page, ptrack */
			(page_st.lsn == InvalidXLogRecPtr) ||                     /* zeroed page */
			(req->horizonLsn > 0 && page_st.lsn > req->horizonLsn))   /* delta */
		{
			int  compressed_size = 0;
			char write_buffer[BLCKSZ*2];
			BackupPageHeader* bph = (BackupPageHeader*)write_buffer;

			/* compress page */
			hdr.cop = FIO_PAGE;
			hdr.arg = blknum;

			compressed_size = do_compress(write_buffer + sizeof(BackupPageHeader),
										  sizeof(write_buffer) - sizeof(BackupPageHeader),
										  read_buffer, BLCKSZ, req->calg, req->clevel,
										  NULL);

			if (compressed_size <= 0 || compressed_size >= BLCKSZ)
			{
				/* Do not compress page */
				memcpy(write_buffer + sizeof(BackupPageHeader), read_buffer, BLCKSZ);
				compressed_size = BLCKSZ;
			}
			bph->block = blknum;
			bph->compressed_size = compressed_size;

			hdr.size = compressed_size + sizeof(BackupPageHeader);

			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			IO_CHECK(fio_write_all(out, write_buffer, hdr.size), hdr.size);

			/* set page header for this file */
			hdr_num++;
			if (!headers)
				headers = (BackupPageHeader2 *) pgut_malloc(sizeof(BackupPageHeader2));
			else
				headers = (BackupPageHeader2 *) pgut_realloc(headers, (hdr_num+1) * sizeof(BackupPageHeader2));

			headers[hdr_num].block = blknum;
			headers[hdr_num].lsn = page_st.lsn;
			headers[hdr_num].checksum = page_st.checksum;
			headers[hdr_num].pos = cur_pos_out;

			cur_pos_out += hdr.size;
		}

		/* next block */
		if (with_pagemap)
		{
			/* exit if pagemap is exhausted */
			if (!datapagemap_next(iter, &blknum))
				break;
		}
		else
			blknum++;
	}

eof:
	/* We are done, send eof */
	hdr.cop = FIO_SEND_FILE_EOF;
	hdr.arg = n_blocks_read;
	hdr.size = 0;

	if (headers)
	{
		hdr.size = (hdr_num+2) * sizeof(BackupPageHeader2);

		/* add dummy header */
		headers = (BackupPageHeader2 *) pgut_realloc(headers, (hdr_num+2) * sizeof(BackupPageHeader2));
		headers[hdr_num+1].pos = cur_pos_out;
	}
	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));

	if (headers)
		IO_CHECK(fio_write_all(out, headers, hdr.size), hdr.size);

cleanup:
	pg_free(map);
	pg_free(iter);
	pg_free(errormsg);
	pg_free(headers);
	if (in)
		fclose(in);
	return;
}

/* Receive chunks of compressed data, decompress them and write to
 * destination file.
 * Return codes:
 *   FILE_MISSING (-1)
 *   OPEN_FAILED  (-2)
 *   READ_FAILED  (-3)
 *   WRITE_FAILED (-4)
 *   ZLIB_ERROR   (-5)
 *   REMOTE_ERROR (-6)
 */
int fio_send_file_gz(const char *from_fullpath, const char *to_fullpath, FILE* out, char **errormsg)
{
	fio_header hdr;
	int exit_code = SEND_OK;
	char *in_buf = pgut_malloc(CHUNK_SIZE);    /* buffer for compressed data */
	char *out_buf = pgut_malloc(OUT_BUF_SIZE); /* 1MB buffer for decompressed data */
	size_t path_len = strlen(from_fullpath) + 1;
	/* decompressor */
	z_stream *strm = NULL;

	hdr.cop = FIO_SEND_FILE;
	hdr.size = path_len;

//	elog(VERBOSE, "Thread [%d]: Attempting to open remote compressed WAL file '%s'",
//			thread_num, from_fullpath);

	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	IO_CHECK(fio_write_all(fio_stdout, from_fullpath, path_len), path_len);

	for (;;)
	{
		fio_header hdr;
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

		if (hdr.cop == FIO_SEND_FILE_EOF)
		{
			break;
		}
		else if (hdr.cop == FIO_ERROR)
		{
			/* handle error, reported by the agent */
			if (hdr.size > 0)
			{
				IO_CHECK(fio_read_all(fio_stdin, in_buf, hdr.size), hdr.size);
				*errormsg = pgut_malloc(hdr.size);
				snprintf(*errormsg, hdr.size, "%s", in_buf);
			}
			exit_code = hdr.arg;
			goto cleanup;
		}
		else if (hdr.cop == FIO_PAGE)
		{
			int rc;
			Assert(hdr.size <= CHUNK_SIZE);
			IO_CHECK(fio_read_all(fio_stdin, in_buf, hdr.size), hdr.size);

			/* We have received a chunk of compressed data, lets decompress it */
			if (strm == NULL)
			{
				/* Initialize decompressor */
				strm = pgut_malloc(sizeof(z_stream));
				memset(strm, 0, sizeof(z_stream));

				/* The fields next_in, avail_in initialized before init */
				strm->next_in = (Bytef *)in_buf;
				strm->avail_in = hdr.size;

				rc = inflateInit2(strm, 15 + 16);

				if (rc != Z_OK)
				{
					*errormsg = pgut_malloc(ERRMSG_MAX_LEN);
					snprintf(*errormsg, ERRMSG_MAX_LEN,
							"Failed to initialize decompression stream for file '%s': %i: %s",
							from_fullpath, rc, strm->msg);
					exit_code = ZLIB_ERROR;
					goto cleanup;
				}
			}
			else
			{
				strm->next_in = (Bytef *)in_buf;
				strm->avail_in = hdr.size;
			}

			strm->next_out = (Bytef *)out_buf; /* output buffer */
			strm->avail_out = OUT_BUF_SIZE; /* free space in output buffer */

			/*
			 * From zlib documentation:
			 * The application must update next_in and avail_in when avail_in
			 * has dropped to zero. It must update next_out and avail_out when
			 * avail_out has dropped to zero.
			 */
			while (strm->avail_in != 0) /* while there is data in input buffer, decompress it */
			{
				/* decompress until there is no data to decompress,
				 * or buffer with uncompressed data is full
				 */
				rc = inflate(strm, Z_NO_FLUSH);
				if (rc == Z_STREAM_END)
					/* end of stream */
					break;
				else if (rc != Z_OK)
				{
					/* got an error */
					*errormsg = pgut_malloc(ERRMSG_MAX_LEN);
					snprintf(*errormsg, ERRMSG_MAX_LEN,
							"Decompression failed for file '%s': %i: %s",
							from_fullpath, rc, strm->msg);
					exit_code = ZLIB_ERROR;
					goto cleanup;
				}

				if (strm->avail_out == 0)
				{
					/* Output buffer is full, write it out */
					if (fwrite(out_buf, 1, OUT_BUF_SIZE, out) != OUT_BUF_SIZE)
					{
						exit_code = WRITE_FAILED;
						goto cleanup;
					}

					strm->next_out = (Bytef *)out_buf; /* output buffer */
					strm->avail_out = OUT_BUF_SIZE;
				}
			}

			/* write out leftovers if any */
			if (strm->avail_out != OUT_BUF_SIZE)
			{
				int len = OUT_BUF_SIZE - strm->avail_out;

				if (fwrite(out_buf, 1, len, out) != len)
				{
					exit_code = WRITE_FAILED;
					goto cleanup;
				}
			}
		}
		else
			elog(ERROR, "Remote agent returned message of unexpected type: %i", hdr.cop);
	}

cleanup:
	if (exit_code < OPEN_FAILED)
		fio_disconnect(); /* discard possible pending data in pipe */

	if (strm)
	{
		inflateEnd(strm);
		pg_free(strm);
	}

	pg_free(in_buf);
	pg_free(out_buf);
	return exit_code;
}

/* Receive chunks of data and write them to destination file.
 * Return codes:
 *   SEND_OK       (0)
 *   FILE_MISSING (-1)
 *   OPEN_FAILED  (-2)
 *   READ_FAILED  (-3)
 *   WRITE_FAILED (-4)
 *
 * OPEN_FAILED and READ_FAIL should also set errormsg.
 * If pgFile is not NULL then we must calculate crc and read_size for it.
 */
int fio_send_file(const char *from_fullpath, const char *to_fullpath, FILE* out,
												pgFile *file, char **errormsg)
{
	fio_header hdr;
	int exit_code = SEND_OK;
	size_t path_len = strlen(from_fullpath) + 1;
	char *buf = pgut_malloc(CHUNK_SIZE);    /* buffer */

	hdr.cop = FIO_SEND_FILE;
	hdr.size = path_len;

//	elog(VERBOSE, "Thread [%d]: Attempting to open remote WAL file '%s'",
//			thread_num, from_fullpath);

	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	IO_CHECK(fio_write_all(fio_stdout, from_fullpath, path_len), path_len);

	for (;;)
	{
		/* receive data */
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

		if (hdr.cop == FIO_SEND_FILE_EOF)
		{
			break;
		}
		else if (hdr.cop == FIO_ERROR)
		{
			/* handle error, reported by the agent */
			if (hdr.size > 0)
			{
				IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);
				*errormsg = pgut_malloc(hdr.size);
				snprintf(*errormsg, hdr.size, "%s", buf);
			}
			exit_code = hdr.arg;
			break;
		}
		else if (hdr.cop == FIO_PAGE)
		{
			Assert(hdr.size <= CHUNK_SIZE);
			IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);

			/* We have received a chunk of data data, lets write it out */
			if (fwrite(buf, 1, hdr.size, out) != hdr.size)
			{
				exit_code = WRITE_FAILED;
				break;
			}

			if (file)
			{
				file->read_size += hdr.size;
				COMP_FILE_CRC32(true, file->crc, buf, hdr.size);
			}
		}
		else
		{
			/* TODO: fio_disconnect may get assert fail when running after this */
			elog(ERROR, "Remote agent returned message of unexpected type: %i", hdr.cop);
		}
	}

	if (exit_code < OPEN_FAILED)
		fio_disconnect(); /* discard possible pending data in pipe */

	pg_free(buf);
	return exit_code;
}

/* Send file content
 * On error we return FIO_ERROR message with following codes
 *  FIO_ERROR:
 *      FILE_MISSING (-1)
 *      OPEN_FAILED  (-2)
 *      READ_FAILED  (-3)
 *
 *  FIO_PAGE
 *  FIO_SEND_FILE_EOF
 *
 */
static void fio_send_file_impl(int out, char const* path)
{
	FILE      *fp;
	fio_header hdr;
	char      *buf = pgut_malloc(CHUNK_SIZE);
	size_t	   read_len = 0;
	char      *errormsg = NULL;

	/* open source file for read */
	/* TODO: check that file is regular file */
	fp = fopen(path, PG_BINARY_R);
	if (!fp)
	{
		hdr.cop = FIO_ERROR;

		/* do not send exact wording of ENOENT error message
		 * because it is a very common error in our case, so
		 * error code is enough.
		 */
		if (errno == ENOENT)
		{
			hdr.arg = FILE_MISSING;
			hdr.size = 0;
		}
		else
		{
			hdr.arg = OPEN_FAILED;
			errormsg = pgut_malloc(ERRMSG_MAX_LEN);
			/* Construct the error message */
			snprintf(errormsg, ERRMSG_MAX_LEN, "Cannot open file '%s': %s", path, strerror(errno));
			hdr.size = strlen(errormsg) + 1;
		}

		/* send header and message */
		IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
		if (errormsg)
			IO_CHECK(fio_write_all(out, errormsg, hdr.size), hdr.size);

		goto cleanup;
	}

	/* disable stdio buffering */
	setvbuf(fp, NULL, _IONBF, BUFSIZ);

	/* copy content */
	for (;;)
	{
		read_len = fread(buf, 1, CHUNK_SIZE, fp);

		/* report error */
		if (ferror(fp))
		{
			hdr.cop = FIO_ERROR;
			errormsg = pgut_malloc(ERRMSG_MAX_LEN);
			hdr.arg = READ_FAILED;
			/* Construct the error message */
			snprintf(errormsg, ERRMSG_MAX_LEN, "Cannot read from file '%s': %s", path, strerror(errno));
			hdr.size = strlen(errormsg) + 1;
			/* send header and message */
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			IO_CHECK(fio_write_all(out, errormsg, hdr.size), hdr.size);

			goto cleanup;
		}

		if (read_len > 0)
		{
			/* send chunk */
			hdr.cop = FIO_PAGE;
			hdr.size = read_len;
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			IO_CHECK(fio_write_all(out, buf, read_len), read_len);
		}

		if (feof(fp))
			break;
	}

	/* we are done, send eof */
	hdr.cop = FIO_SEND_FILE_EOF;
	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));

cleanup:
	if (fp)
		fclose(fp);
	pg_free(buf);
	pg_free(errormsg);
	return;
}

/* Compile the array of files located on remote machine in directory root */
static void fio_list_dir_internal(parray *files, const char *root, bool exclude,
								  bool follow_symlink, bool add_root, bool backup_logs,
								  bool skip_hidden, int external_dir_num)
{
	fio_header hdr;
	fio_list_dir_request req;
	char *buf = pgut_malloc(CHUNK_SIZE);

	/* Send to the agent message with parameters for directory listing */
	snprintf(req.path, MAXPGPATH, "%s", root);
	req.exclude = exclude;
	req.follow_symlink = follow_symlink;
	req.add_root = add_root;
	req.backup_logs = backup_logs;
	req.exclusive_backup = exclusive_backup;
	req.skip_hidden = skip_hidden;
	req.external_dir_num = external_dir_num;

	hdr.cop = FIO_LIST_DIR;
	hdr.size = sizeof(req);

	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	IO_CHECK(fio_write_all(fio_stdout, &req, hdr.size), hdr.size);

	for (;;)
	{
		/* receive data */
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

		if (hdr.cop == FIO_SEND_FILE_EOF)
		{
			/* the work is done */
			break;
		}
		else if (hdr.cop == FIO_SEND_FILE)
		{
			pgFile *file = NULL;
			fio_pgFile  fio_file;

			/* receive rel_path */
			IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);
			file = pgFileInit(buf);

			/* receive metainformation */
			IO_CHECK(fio_read_all(fio_stdin, &fio_file, sizeof(fio_file)), sizeof(fio_file));

			file->mode = fio_file.mode;
			file->size = fio_file.size;
			file->mtime = fio_file.mtime;
			file->is_datafile = fio_file.is_datafile;
			file->is_database = fio_file.is_database;
			file->tblspcOid = fio_file.tblspcOid;
			file->dbOid = fio_file.dbOid;
			file->relOid = fio_file.relOid;
			file->forkName = fio_file.forkName;
			file->segno = fio_file.segno;
			file->external_dir_num = fio_file.external_dir_num;

			if (fio_file.linked_len > 0)
			{
				IO_CHECK(fio_read_all(fio_stdin, buf, fio_file.linked_len), fio_file.linked_len);

				file->linked = pgut_malloc(fio_file.linked_len);
				snprintf(file->linked, fio_file.linked_len, "%s", buf);
			}

//			elog(INFO, "Received file: %s, mode: %u, size: %lu, mtime: %lu",
//				file->rel_path, file->mode, file->size, file->mtime);

			parray_append(files, file);
		}
		else
		{
			/* TODO: fio_disconnect may get assert fail when running after this */
			elog(ERROR, "Remote agent returned message of unexpected type: %i", hdr.cop);
		}
	}

	pg_free(buf);
}


/*
 * To get the arrays of files we use the same function dir_list_file(),
 * that is used for local backup.
 * After that we iterate over arrays and for every file send at least
 * two messages to main process:
 * 1. rel_path
 * 2. metainformation (size, mtime, etc)
 * 3. link path (optional)
 *
 * TODO: replace FIO_SEND_FILE and FIO_SEND_FILE_EOF with dedicated messages
 */
static void fio_list_dir_impl(int out, char* buf)
{
	int i;
	fio_header hdr;
	fio_list_dir_request *req = (fio_list_dir_request*) buf;
	parray *file_files = parray_new();

	/*
	 * Disable logging into console any messages with exception of ERROR messages,
	 * because currently we have no mechanism to notify the main process
	 * about then message been sent.
	 * TODO: correctly send elog messages from agent to main process.
	 */
	instance_config.logger.log_level_console = ERROR;
	exclusive_backup = req->exclusive_backup;

	dir_list_file(file_files, req->path, req->exclude, req->follow_symlink,
				  req->add_root, req->backup_logs, req->skip_hidden,
				  req->external_dir_num, FIO_LOCAL_HOST);

	/* send information about files to the main process */
	for (i = 0; i < parray_num(file_files); i++)
	{
		fio_pgFile  fio_file;
		pgFile	   *file = (pgFile *) parray_get(file_files, i);

		fio_file.mode = file->mode;
		fio_file.size = file->size;
		fio_file.mtime = file->mtime;
		fio_file.is_datafile = file->is_datafile;
		fio_file.is_database = file->is_database;
		fio_file.tblspcOid = file->tblspcOid;
		fio_file.dbOid = file->dbOid;
		fio_file.relOid = file->relOid;
		fio_file.forkName = file->forkName;
		fio_file.segno = file->segno;
		fio_file.external_dir_num = file->external_dir_num;

		if (file->linked)
			fio_file.linked_len = strlen(file->linked) + 1;
		else
			fio_file.linked_len = 0;

		hdr.cop = FIO_SEND_FILE;
		hdr.size = strlen(file->rel_path) + 1;

		/* send rel_path first */
		IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(out, file->rel_path, hdr.size), hdr.size);

		/* now send file metainformation */
		IO_CHECK(fio_write_all(out, &fio_file, sizeof(fio_file)), sizeof(fio_file));

		/* If file is a symlink, then send link path */
		if (file->linked)
			IO_CHECK(fio_write_all(out, file->linked, fio_file.linked_len), fio_file.linked_len);

		pgFileFree(file);
	}

	parray_free(file_files);
	hdr.cop = FIO_SEND_FILE_EOF;
	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
}

/* Wrapper for directory listing */
void fio_list_dir(parray *files, const char *root, bool exclude,
				  bool follow_symlink, bool add_root, bool backup_logs,
				  bool skip_hidden, int external_dir_num)
{
	if (fio_is_remote(FIO_DB_HOST))
		fio_list_dir_internal(files, root, exclude, follow_symlink, add_root,
							  backup_logs, skip_hidden, external_dir_num);
	else
		dir_list_file(files, root, exclude, follow_symlink, add_root,
					  backup_logs, skip_hidden, external_dir_num, FIO_LOCAL_HOST);
}

PageState *
fio_get_checksum_map(const char *fullpath, uint32 checksum_version, int n_blocks,
					 XLogRecPtr dest_stop_lsn, BlockNumber segmentno, fio_location location)
{
	if (fio_is_remote(location))
	{
		fio_header hdr;
		fio_checksum_map_request req_hdr;
		PageState *checksum_map = NULL;
		size_t path_len = strlen(fullpath) + 1;

		req_hdr.n_blocks = n_blocks;
		req_hdr.segmentno = segmentno;
		req_hdr.stop_lsn = dest_stop_lsn;
		req_hdr.checksumVersion = checksum_version;

		hdr.cop = FIO_GET_CHECKSUM_MAP;
		hdr.size = sizeof(req_hdr) + path_len;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, &req_hdr, sizeof(req_hdr)), sizeof(req_hdr));
		IO_CHECK(fio_write_all(fio_stdout, fullpath, path_len), path_len);

		/* receive data */
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

		if (hdr.size > 0)
		{
			checksum_map = pgut_malloc(n_blocks * sizeof(PageState));
			memset(checksum_map, 0, n_blocks * sizeof(PageState));
			IO_CHECK(fio_read_all(fio_stdin, checksum_map, hdr.size * sizeof(PageState)), hdr.size * sizeof(PageState));
		}

		return checksum_map;
	}
	else
	{

		return get_checksum_map(fullpath, checksum_version,
								n_blocks, dest_stop_lsn, segmentno);
	}
}

static void fio_get_checksum_map_impl(int out, char *buf)
{
	fio_header  hdr;
	PageState  *checksum_map = NULL;
	char       *fullpath = (char*) buf + sizeof(fio_checksum_map_request);
	fio_checksum_map_request *req = (fio_checksum_map_request*) buf;

	checksum_map = get_checksum_map(fullpath, req->checksumVersion,
									req->n_blocks, req->stop_lsn, req->segmentno);
	hdr.size = req->n_blocks;

	/* send array of PageState`s to main process */
	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
	if (hdr.size > 0)
		IO_CHECK(fio_write_all(out, checksum_map, hdr.size * sizeof(PageState)), hdr.size * sizeof(PageState));

	pg_free(checksum_map);
}

datapagemap_t *
fio_get_lsn_map(const char *fullpath, uint32 checksum_version,
				int n_blocks, XLogRecPtr shift_lsn, BlockNumber segmentno,
				fio_location location)
{
	datapagemap_t* lsn_map = NULL;

	if (fio_is_remote(location))
	{
		fio_header hdr;
		fio_lsn_map_request req_hdr;
		size_t path_len = strlen(fullpath) + 1;

		req_hdr.n_blocks = n_blocks;
		req_hdr.segmentno = segmentno;
		req_hdr.shift_lsn = shift_lsn;
		req_hdr.checksumVersion = checksum_version;

		hdr.cop = FIO_GET_LSN_MAP;
		hdr.size = sizeof(req_hdr) + path_len;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, &req_hdr, sizeof(req_hdr)), sizeof(req_hdr));
		IO_CHECK(fio_write_all(fio_stdout, fullpath, path_len), path_len);

		/* receive data */
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

		if (hdr.size > 0)
		{
			lsn_map = pgut_malloc(sizeof(datapagemap_t));
			memset(lsn_map, 0, sizeof(datapagemap_t));

			lsn_map->bitmap = pgut_malloc(hdr.size);
			lsn_map->bitmapsize = hdr.size;

			IO_CHECK(fio_read_all(fio_stdin, lsn_map->bitmap, hdr.size), hdr.size);
		}
	}
	else
	{
		lsn_map = get_lsn_map(fullpath, checksum_version, n_blocks,
							  shift_lsn, segmentno);
	}

	return lsn_map;
}

static void fio_get_lsn_map_impl(int out, char *buf)
{
	fio_header     hdr;
	datapagemap_t *lsn_map = NULL;
	char          *fullpath = (char*) buf + sizeof(fio_lsn_map_request);
	fio_lsn_map_request *req = (fio_lsn_map_request*) buf;

	lsn_map = get_lsn_map(fullpath, req->checksumVersion, req->n_blocks,
						  req->shift_lsn, req->segmentno);
	if (lsn_map)
		hdr.size = lsn_map->bitmapsize;
	else
		hdr.size = 0;

	/* send bitmap to main process */
	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
	if (hdr.size > 0)
		IO_CHECK(fio_write_all(out, lsn_map->bitmap, hdr.size), hdr.size);

	if (lsn_map)
	{
		pg_free(lsn_map->bitmap);
		pg_free(lsn_map);
	}
}

/*
 * Go to the remote host and get postmaster pid from file postmaster.pid
 * and check that process is running, if process is running, return its pid number.
 */
pid_t fio_check_postmaster(const char *pgdata, fio_location location)
{
	if (fio_is_remote(location))
	{
		fio_header hdr;

		hdr.cop = FIO_CHECK_POSTMASTER;
		hdr.size = strlen(pgdata) + 1;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, pgdata, hdr.size), hdr.size);

		/* receive result */
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		return hdr.arg;
	}
	else
		return check_postmaster(pgdata);
}

static void fio_check_postmaster_impl(int out, char *buf)
{
	fio_header  hdr;
	pid_t       postmaster_pid;
	char       *pgdata = (char*) buf;

	postmaster_pid = check_postmaster(pgdata);

	/* send arrays of checksums to main process */
	hdr.arg = postmaster_pid;
	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
}

/*
 * Delete file pointed by the pgFile.
 * If the pgFile points directory, the directory must be empty.
 */
void
fio_delete(mode_t mode, const char *fullpath, fio_location location)
{
	if (fio_is_remote(location))
	{
		fio_header  hdr;

		hdr.cop = FIO_DELETE;
		hdr.size = strlen(fullpath) + 1;
		hdr.arg = mode;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, fullpath, hdr.size), hdr.size);

	}
	else
		pgFileDelete(mode, fullpath);
}

static void
fio_delete_impl(mode_t mode, char *buf)
{
	char  *fullpath = (char*) buf;

	pgFileDelete(mode, fullpath);
}

/* Execute commands at remote host */
void fio_communicate(int in, int out)
{
	/*
	 * Map of file and directory descriptors.
	 * The same mapping is used in agent and master process, so we
	 * can use the same index at both sides.
	 */
	int fd[FIO_FDMAX];
	DIR* dir[FIO_FDMAX];
	struct dirent* entry;
	size_t buf_size = 128*1024;
	char* buf = (char*)pgut_malloc(buf_size);
	fio_header hdr;
	struct stat st;
	int rc;
	int tmp_fd;
	pg_crc32 crc;

#ifdef WIN32
    SYS_CHECK(setmode(in, _O_BINARY));
    SYS_CHECK(setmode(out, _O_BINARY));
#endif

    /* Main loop until end of processing all master commands */
	while ((rc = fio_read_all(in, &hdr, sizeof hdr)) == sizeof(hdr)) {
		if (hdr.size != 0) {
			if (hdr.size > buf_size) {
				/* Extend buffer on demand */
				buf_size = hdr.size;
				buf = (char*)realloc(buf, buf_size);
			}
			IO_CHECK(fio_read_all(in, buf, hdr.size), hdr.size);
		}
		errno = 0; /* reset errno */
		switch (hdr.cop) {
		  case FIO_LOAD: /* Send file content */
			fio_load_file(out, buf);
			break;
		  case FIO_OPENDIR: /* Open directory for traversal */
			dir[hdr.handle] = opendir(buf);
			hdr.arg = dir[hdr.handle] == NULL ? errno : 0;
			hdr.size = 0;
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			break;
		  case FIO_READDIR: /* Get next directory entry */
			hdr.cop = FIO_SEND;
			entry = readdir(dir[hdr.handle]);
			if (entry != NULL)
			{
				hdr.size = sizeof(*entry);
				IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
				IO_CHECK(fio_write_all(out, entry, hdr.size), hdr.size);
			}
			else
			{
				hdr.size = 0;
				IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			}
			break;
		  case FIO_CLOSEDIR: /* Finish directory traversal */
			SYS_CHECK(closedir(dir[hdr.handle]));
			break;
		  case FIO_OPEN: /* Open file */
			fd[hdr.handle] = open(buf, hdr.arg, FILE_PERMISSIONS);
			hdr.arg = fd[hdr.handle] < 0 ? errno : 0;
			hdr.size = 0;
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			break;
		  case FIO_CLOSE: /* Close file */
			SYS_CHECK(close(fd[hdr.handle]));
			break;
		  case FIO_WRITE: /* Write to the current position in file */
//			IO_CHECK(fio_write_all(fd[hdr.handle], buf, hdr.size), hdr.size);
			fio_write_impl(fd[hdr.handle], buf, hdr.size, out);
			break;
		  case FIO_WRITE_ASYNC: /* Write to the current position in file */
			fio_write_async_impl(fd[hdr.handle], buf, hdr.size, out);
			break;
		  case FIO_WRITE_COMPRESSED_ASYNC: /* Write to the current position in file */
			fio_write_compressed_impl(fd[hdr.handle], buf, hdr.size, hdr.arg);
			break;
		  case FIO_READ: /* Read from the current position in file */
			if ((size_t)hdr.arg > buf_size) {
				buf_size = hdr.arg;
				buf = (char*)realloc(buf, buf_size);
			}
			rc = read(fd[hdr.handle], buf, hdr.arg);
			hdr.cop = FIO_SEND;
			hdr.size = rc > 0 ? rc : 0;
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			if (hdr.size != 0)
				IO_CHECK(fio_write_all(out, buf, hdr.size), hdr.size);
			break;
		  case FIO_PREAD: /* Read from specified position in file, ignoring pages beyond horizon of delta backup */
			rc = pread(fd[hdr.handle], buf, BLCKSZ, hdr.arg);
			hdr.cop = FIO_SEND;
			hdr.arg = rc;
			hdr.size = rc >= 0 ? rc : 0;
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			if (hdr.size != 0)
				IO_CHECK(fio_write_all(out, buf, hdr.size),  hdr.size);
			break;
		  case FIO_AGENT_VERSION:
			hdr.arg = AGENT_PROTOCOL_VERSION;
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			break;
		  case FIO_STAT: /* Get information about file with specified path */
			hdr.size = sizeof(st);
			rc = hdr.arg ? stat(buf, &st) : lstat(buf, &st);
			hdr.arg = rc < 0 ? errno : 0;
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			IO_CHECK(fio_write_all(out, &st, sizeof(st)), sizeof(st));
			break;
		  case FIO_ACCESS: /* Check presence of file with specified name */
			hdr.size = 0;
			hdr.arg = access(buf, hdr.arg) < 0 ? errno  : 0;
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			break;
		  case FIO_RENAME: /* Rename file */
			SYS_CHECK(rename(buf, buf + strlen(buf) + 1));
			break;
		  case FIO_SYMLINK: /* Create symbolic link */
			fio_symlink_impl(out, buf, hdr.arg > 0 ? true : false);
			break;
		  case FIO_UNLINK: /* Remove file or directory (TODO: Win32) */
			SYS_CHECK(remove_file_or_dir(buf));
			break;
		  case FIO_MKDIR:  /* Create directory */
			hdr.size = 0;
			hdr.arg = dir_create_dir(buf, hdr.arg, false);
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			break;
		  case FIO_CHMOD:  /* Change file mode */
			SYS_CHECK(chmod(buf, hdr.arg));
			break;
		  case FIO_SEEK:   /* Set current position in file */
			fio_seek_impl(fd[hdr.handle], hdr.arg);
			break;
		  case FIO_TRUNCATE: /* Truncate file */
			SYS_CHECK(ftruncate(fd[hdr.handle], hdr.arg));
			break;
		  case FIO_LIST_DIR:
			fio_list_dir_impl(out, buf);
			break;
		  case FIO_SEND_PAGES:
			// buf contain fio_send_request header and bitmap.
			fio_send_pages_impl(out, buf);
			break;
		  case FIO_SEND_FILE:
			fio_send_file_impl(out, buf);
			break;
		  case FIO_SYNC:
			/* open file and fsync it */
			tmp_fd = open(buf, O_WRONLY | PG_BINARY, FILE_PERMISSIONS);
			if (tmp_fd < 0)
				hdr.arg = errno;
			else
			{
				if (fsync(tmp_fd) == 0)
					hdr.arg = 0;
				else
					hdr.arg = errno;
			}
			close(tmp_fd);

			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			break;
		  case FIO_GET_CRC32:
			/* calculate crc32 for a file */
			if (hdr.arg == 1)
				crc = pgFileGetCRCgz(buf, true, true);
			else
				crc = pgFileGetCRC(buf, true, true);
			IO_CHECK(fio_write_all(out, &crc, sizeof(crc)), sizeof(crc));
			break;
		  case FIO_GET_CHECKSUM_MAP:
			/* calculate crc32 for a file */
			fio_get_checksum_map_impl(out, buf);
			break;
		  case FIO_GET_LSN_MAP:
			/* calculate crc32 for a file */
			fio_get_lsn_map_impl(out, buf);
			break;
		  case FIO_CHECK_POSTMASTER:
			/* calculate crc32 for a file */
			fio_check_postmaster_impl(out, buf);
			break;
		  case FIO_DELETE:
			/* delete file */
			fio_delete_impl(hdr.arg, buf);
			break;
		  case FIO_DISCONNECT:
			hdr.cop = FIO_DISCONNECTED;
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			break;
		  case FIO_GET_ASYNC_ERROR:
			fio_get_async_error_impl(out);
			break;
		  default:
			Assert(false);
		}
	}
	free(buf);
	if (rc != 0) { /* Not end of stream: normal pipe close */
		perror("read");
		exit(EXIT_FAILURE);
	}
}

