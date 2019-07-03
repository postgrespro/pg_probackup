#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

#ifdef WIN32
#define __thread __declspec(thread)
#else
#include <pthread.h>
#endif

#include "pg_probackup.h"
#include "file.h"
#include "storage/checksum.h"

#define PRINTF_BUF_SIZE  1024
#define FILE_PERMISSIONS 0600
#define PAGE_READ_ATTEMPTS 100

static __thread unsigned long fio_fdset = 0;
static __thread void* fio_stdin_buffer;
static __thread int fio_stdout = 0;
static __thread int fio_stdin = 0;

fio_location MyLocation;

typedef struct
{
	BlockNumber nblocks;
	BlockNumber segBlockNum;
	XLogRecPtr  horizonLsn;
	uint32      checksumVersion;
	int         calg;
	int         clevel;
} fio_send_request;


/* Convert FIO pseudo handle to index in file descriptor array */
#define fio_fileno(f) (((size_t)f - 1) | FIO_PIPE_MARKER)

/* Use specified file descriptors as stding/stdout for FIO functions */
void fio_redirect(int in, int out)
{
	fio_stdin = in;
	fio_stdout = out;
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
static bool fio_is_remote(fio_location location)
{
	bool is_remote = MyLocation != FIO_LOCAL_HOST
		&& location != FIO_LOCAL_HOST
		&& location != MyLocation;
	if (is_remote && !fio_stdin && !launch_agent())
		elog(ERROR, "Failed to establish SSH connection: %s", strerror(errno));
	return is_remote;
}

/* Try to read specified amount of bytes unless error or EOF are encountered */
static ssize_t fio_read_all(int fd, void* buf, size_t size)
{
	size_t offs = 0;
	while (offs < size)
	{
		ssize_t rc = read(fd, (char*)buf + offs, size - offs);
		if (rc < 0) {
			if (errno == EINTR) {
				continue;
			}
			return rc;
		} else if (rc == 0) {
			break;
		}
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
		if (rc <= 0) {
			if (errno == EINTR) {
				continue;
			}
			return rc;
		}
		offs += rc;
	}
	return offs;
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
			return NULL;
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
		if (i == FIO_FDMAX) {
			return -1;
		}
		hdr.cop = FIO_OPEN;
		hdr.handle = i;
		hdr.size = strlen(path) + 1;
		hdr.arg = mode & ~O_EXCL;
		fio_fdset |= 1 << i;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

		if (hdr.arg != 0)
		{
			errno = hdr.arg;
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

/* Flush stream data  (does nothing for remote file) */
int fio_fflush(FILE* f)
{
	int rc = 0;
	if (!fio_is_remote_file(f))
	{
		rc = fflush(f);
		if (rc == 0) {
			rc = fsync(fileno(f));
		}
	}
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

/* Truncate file */
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

		return hdr.arg;
	}
	else
		return pread(fileno(f), buf, BLCKSZ, offs);
}

/* Set position in stdio file */
int fio_fseek(FILE* f, off_t offs)
{
	return fio_is_remote_file(f)
		? fio_seek(fio_fileno(f),  offs)
		: fseek(f, offs, SEEK_SET);
}

/* Set position in file */
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

/* Write data to stdio file */
size_t fio_fwrite(FILE* f, void const* buf, size_t size)
{
	return fio_is_remote_file(f)
		? fio_write(fio_fileno(f), buf, size)
		: fwrite(buf, 1, size, f);
}

/* Write data to the file */
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

		return size;
	}
	else
	{
		return write(fd, buf, size);
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

/* Get information about stdio file */
int fio_ffstat(FILE* f, struct stat* st)
{
	return fio_is_remote_file(f)
		? fio_fstat(fio_fileno(f), st)
		: fio_fstat(fileno(f), st);
}

/* Get information about file descriptor */
int fio_fstat(int fd, struct stat* st)
{
	if (fio_is_remote_fd(fd))
	{
		fio_header hdr;

		hdr.cop = FIO_FSTAT;
		hdr.handle = fd & ~FIO_PIPE_MARKER;
		hdr.size = 0;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_FSTAT);
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
		return fstat(fd, st);
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

/* Create symbolink link */
int fio_symlink(char const* target, char const* link_path, fio_location location)
{
	if (fio_is_remote(location))
	{
		fio_header hdr;
		size_t target_len = strlen(target) + 1;
		size_t link_path_len = strlen(link_path) + 1;
		hdr.cop = FIO_SYMLINK;
		hdr.handle = -1;
		hdr.size = target_len + link_path_len;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, target, target_len), target_len);
		IO_CHECK(fio_write_all(fio_stdout, link_path, link_path_len), link_path_len);

		return 0;
	}
	else
	{
		return symlink(target, link_path);
	}
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

		return 0;
	}
	else
	{
		return rename(old_path, new_path);
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

		return 0;
	}
	else
	{
		return remove_file_or_dir(path);
	}
}

/* Create directory */
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
		return dir_create_dir(path, mode);
	}
}

/* Checnge file mode */
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
				if (fio_access(path, F_OK, location) == 0)
				{
					elog(LOG, "File %s exists", path);
					free(gz);
					errno = EEXIST;
					return NULL;
				}
				gz->fd = fio_open(path, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY, location);
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
			}
		}
		if (rc != Z_OK)
		{
			free(gz);
			return NULL;
		}
		return (gzFile)((size_t)gz + FIO_GZ_REMOTE_MARKER);
	}
	else
	{
		gzFile file;
		if (strcmp(mode, PG_BINARY_W) == 0)
		{
			int fd = open(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, FILE_PERMISSIONS);
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
			if (gz->strm.avail_in != 0) /* If there is some data in receiver buffer, then decmpress it */
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
			rc = fio_read(gz->fd, gz->strm.next_in + gz->strm.avail_in, gz->buf + ZLIB_BUFFER_SIZE - gz->strm.next_in - gz->strm.avail_in);
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
					gz->strm.next_out = gz->buf; /* Reset pointer to the  beginning of bufer */
				}
				else
				{
					break;
				}
			}
			rc = fio_write(gz->fd, gz->strm.next_out, ZLIB_BUFFER_SIZE - gz->strm.avail_out);
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

/* Send file content */
static void fio_send_file(int out, char const* path)
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

int fio_send_pages(FILE* in, FILE* out, pgFile *file,
				   XLogRecPtr horizonLsn, BlockNumber* nBlocksSkipped, int calg, int clevel)
{
	struct {
		fio_header hdr;
		fio_send_request arg;
	} req;
	BlockNumber	n_blocks_read = 0;
	BlockNumber blknum = 0;

	Assert(fio_is_remote_file(in));

	req.hdr.cop = FIO_SEND_PAGES;
	req.hdr.size = sizeof(fio_send_request);
	req.hdr.handle = fio_fileno(in) & ~FIO_PIPE_MARKER;

	req.arg.nblocks = file->size/BLCKSZ;
	req.arg.segBlockNum = file->segno * RELSEG_SIZE;
	req.arg.horizonLsn = horizonLsn;
	req.arg.checksumVersion = current.checksum_version;
	req.arg.calg = calg;
	req.arg.clevel = clevel;

	file->compress_alg = calg;

	IO_CHECK(fio_write_all(fio_stdout, &req, sizeof(req)), sizeof(req));

	while (true)
	{
		fio_header hdr;
		char buf[BLCKSZ + sizeof(BackupPageHeader)];
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_PAGE);

		if ((int)hdr.arg < 0) /* read error */
		{
			return (int)hdr.arg;
		}

		blknum = hdr.arg;
		if (hdr.size == 0) /* end of segment */
			break;

		Assert(hdr.size <= sizeof(buf));
		IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);

		COMP_FILE_CRC32(true, file->crc, buf, hdr.size);

		if (fio_fwrite(out, buf, hdr.size) != hdr.size)
		{
			int	errno_tmp = errno;
			fio_fclose(out);
			elog(ERROR, "File: %s, cannot write backup at block %u: %s",
				 file->path, blknum, strerror(errno_tmp));
		}
		file->write_size += hdr.size;
		n_blocks_read++;

		if (((BackupPageHeader*)buf)->compressed_size == PageIsTruncated)
		{
			blknum += 1;
			break;
		}
		file->read_size += BLCKSZ;
	}
	*nBlocksSkipped = blknum - n_blocks_read;
	return blknum;
}

static void fio_send_pages_impl(int fd, int out, fio_send_request* req)
{
	BlockNumber blknum;
	char read_buffer[BLCKSZ+1];
	fio_header hdr;

	hdr.cop = FIO_PAGE;
	read_buffer[BLCKSZ] = 1; /* barrier */

	for (blknum = 0; blknum < req->nblocks; blknum++)
	{
		int retry_attempts = PAGE_READ_ATTEMPTS;
		XLogRecPtr page_lsn = InvalidXLogRecPtr;

		while (true)
		{
			ssize_t rc = pread(fd, read_buffer, BLCKSZ, blknum*BLCKSZ);

			if (rc <= 0)
			{
				if (rc < 0)
				{
					hdr.arg = -errno;
					hdr.size = 0;
					Assert((int)hdr.arg < 0);
					IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
				}
				else
				{
					BackupPageHeader bph;
					bph.block = blknum;
					bph.compressed_size = PageIsTruncated;
					hdr.arg = blknum;
					hdr.size = sizeof(bph);
					IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
					IO_CHECK(fio_write_all(out, &bph, sizeof(bph)), sizeof(bph));
				}
				return;
			}
			else if (rc == BLCKSZ)
			{
				if (!parse_page((Page)read_buffer, &page_lsn))
				{
					int i;
					for (i = 0; read_buffer[i] == 0; i++);

					/* Page is zeroed. No need to check header and checksum. */
					if (i == BLCKSZ)
						break;
				}
				else if (!req->checksumVersion
						 || pg_checksum_page(read_buffer, req->segBlockNum + blknum) == ((PageHeader)read_buffer)->pd_checksum)
				{
					break;
				}
			}

			if (--retry_attempts == 0)
			{
				hdr.size = 0;
				hdr.arg = PAGE_CHECKSUM_MISMATCH;
				IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
				return;
			}
		}
		/* horizonLsn is not 0 for delta backup. As far as unsigned number are always greater or equal than zero, there is no sense to add more checks */
		if (page_lsn >= req->horizonLsn || page_lsn == InvalidXLogRecPtr)
		{
			char write_buffer[BLCKSZ*2];
			BackupPageHeader* bph = (BackupPageHeader*)write_buffer;
			const char *errormsg = NULL;

			hdr.arg = bph->block = blknum;
			hdr.size = sizeof(BackupPageHeader);

			bph->compressed_size = do_compress(write_buffer + sizeof(BackupPageHeader), sizeof(write_buffer) - sizeof(BackupPageHeader),
											   read_buffer, BLCKSZ, req->calg, req->clevel,
											   &errormsg);
			if (bph->compressed_size <= 0 || bph->compressed_size >= BLCKSZ)
			{
				/* Do not compress page */
				memcpy(write_buffer + sizeof(BackupPageHeader), read_buffer, BLCKSZ);
				bph->compressed_size = BLCKSZ;
			}
			hdr.size += MAXALIGN(bph->compressed_size);

			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			IO_CHECK(fio_write_all(out, write_buffer, hdr.size), hdr.size);
		}
	}
	hdr.size = 0;
	hdr.arg = blknum;
	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
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

#ifdef WIN32
    SYS_CHECK(setmode(in, _O_BINARY));
    SYS_CHECK(setmode(out, _O_BINARY));
#endif

    /* Main loop until command of processing master command */
	while ((rc = fio_read_all(in, &hdr, sizeof hdr)) == sizeof(hdr)) {
		if (hdr.size != 0) {
			if (hdr.size > buf_size) {
				/* Extend buffer on demand */
				buf_size = hdr.size;
				buf = (char*)realloc(buf, buf_size);
			}
			IO_CHECK(fio_read_all(in, buf, hdr.size), hdr.size);
		}
		switch (hdr.cop) {
		  case FIO_LOAD: /* Send file content */
			fio_send_file(out, buf);
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
			IO_CHECK(fio_write_all(fd[hdr.handle], buf, hdr.size), hdr.size);
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
		  case FIO_FSTAT: /* Get information about opened file */
			hdr.size = sizeof(st);
			hdr.arg = fstat(fd[hdr.handle], &st) < 0 ? errno : 0;
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			IO_CHECK(fio_write_all(out, &st, sizeof(st)), sizeof(st));
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
			SYS_CHECK(symlink(buf, buf + strlen(buf) + 1));
			break;
		  case FIO_UNLINK: /* Remove file or directory (TODO: Win32) */
			SYS_CHECK(remove_file_or_dir(buf));
			break;
		  case FIO_MKDIR:  /* Create direcory */
			hdr.size = 0;
			hdr.arg = dir_create_dir(buf, hdr.arg);
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			break;
		  case FIO_CHMOD:  /* Change file mode */
			SYS_CHECK(chmod(buf, hdr.arg));
			break;
		  case FIO_SEEK:   /* Set current position in file */
			SYS_CHECK(lseek(fd[hdr.handle], hdr.arg, SEEK_SET));
			break;
		  case FIO_TRUNCATE: /* Truncate file */
			SYS_CHECK(ftruncate(fd[hdr.handle], hdr.arg));
			break;
		  case FIO_SEND_PAGES:
			Assert(hdr.size == sizeof(fio_send_request));
			fio_send_pages_impl(fd[hdr.handle], out, (fio_send_request*)buf);
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

