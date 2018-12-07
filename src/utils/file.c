#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/stat.h>

#include "pg_probackup.h"
#include "file.h"

#define PRINTF_BUF_SIZE  1024
#define FILE_PERMISSIONS 0600

static pthread_mutex_t fio_read_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t fio_write_mutex = PTHREAD_MUTEX_INITIALIZER;
static unsigned long fio_fdset = 0;
static void* fio_stdin_buffer;
static int fio_stdout = 0;
static int fio_stdin = 0;

static fio_binding fio_bindings[] =
{
	{&current.start_time, sizeof(current.start_time)}
};

#define fio_fileno(f) (((size_t)f - 1) | FIO_PIPE_MARKER)

void fio_redirect(int in, int out)
{
	fio_stdin = in;
	fio_stdout = out;
}

static bool fio_is_remote_file(FILE* file)
{
	return (size_t)file <= FIO_FDMAX;
}

static bool fio_is_remote_fd(int fd)
{
	return (fd & FIO_PIPE_MARKER) != 0;
}

static bool fio_is_remote(fio_location location)
{
	return location == FIO_REMOTE_HOST
		|| (location == FIO_BACKUP_HOST && is_remote_agent)
		|| (location == FIO_DB_HOST && !is_remote_agent && IsSshConnection());
}

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
			fio_stdin_buffer = malloc(hdr.size);
			IO_CHECK(fio_read_all(fio_stdin, fio_stdin_buffer, hdr.size), hdr.size);
			f = fmemopen(fio_stdin_buffer, hdr.size, "r");
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

int fio_close_stream(FILE* f)
{
	if (fio_stdin_buffer)
	{
		free(fio_stdin_buffer);
		fio_stdin_buffer = NULL;
	}
	return fclose(f);
}

int fio_open(char const* path, int mode, fio_location location)
{
	int fd;
	if (fio_is_remote(location))
	{
		int i;
		fio_header hdr;
		unsigned long mask;

		SYS_CHECK(pthread_mutex_lock(&fio_write_mutex));

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

		SYS_CHECK(pthread_mutex_unlock(&fio_write_mutex));

		fd = i | FIO_PIPE_MARKER;
	}
	else
	{
		fd = open(path, mode, FILE_PERMISSIONS);
	}
	return fd;
}

FILE* fio_fopen(char const* path, char const* mode, fio_location location)
{
	FILE* f;
	if (fio_is_remote(location))
	{
		int flags = O_RDWR|O_CREAT|PG_BINARY|(strchr(mode, '+') ? 0 : O_TRUNC);
		int fd = fio_open(path, flags, location);
		f = (FILE*)(size_t)((fd + 1) & ~FIO_PIPE_MARKER);
	}
	else
	{
		f = fopen(path, mode);
	}
	return f;
}

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

int fio_flush(int fd)
{
	return fio_is_remote_fd(fd) ? 0 : fsync(fd);
}

int fio_fclose(FILE* f)
{
	return fio_is_remote_file(f)
		? fio_close(fio_fileno(f))
		: fclose(f);
}

int fio_close(int fd)
{
	if (fio_is_remote_fd(fd))
	{
		fio_header hdr;

		SYS_CHECK(pthread_mutex_lock(&fio_write_mutex));

		hdr.cop = FIO_CLOSE;
		hdr.handle = fd & ~FIO_PIPE_MARKER;
		hdr.size = 0;
		fio_fdset &= ~(1 << hdr.handle);

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		SYS_CHECK(pthread_mutex_unlock(&fio_write_mutex));
		return 0;
	}
	else
	{
		return close(fd);
	}
}

int fio_ftruncate(FILE* f, off_t size)
{
	return fio_is_remote_file(f)
		? fio_truncate(fio_fileno(f), size)
		: ftruncate(fileno(f), size);
}

int fio_truncate(int fd, off_t size)
{
	if (fio_is_remote_fd(fd))
	{
		fio_header hdr;

		SYS_CHECK(pthread_mutex_lock(&fio_write_mutex));

		hdr.cop = FIO_TRUNCATE;
		hdr.handle = fd & ~FIO_PIPE_MARKER;
		hdr.size = 0;
		hdr.arg = size;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		SYS_CHECK(pthread_mutex_unlock(&fio_write_mutex));
		return 0;
	}
	else
	{
		return ftruncate(fd, size);
	}
}

int fio_fseek(FILE* f, off_t offs)
{
	return fio_is_remote_file(f)
		? fio_seek(fio_fileno(f),  offs)
		: fseek(f, offs, SEEK_SET);
}

int fio_seek(int fd, off_t offs)
{
	if (fio_is_remote_fd(fd))
	{
		fio_header hdr;

		SYS_CHECK(pthread_mutex_lock(&fio_write_mutex));

		hdr.cop = FIO_SEEK;
		hdr.handle = fd & ~FIO_PIPE_MARKER;
		hdr.size = 0;
		hdr.arg = offs;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		SYS_CHECK(pthread_mutex_unlock(&fio_write_mutex));
		return 0;
	}
	else
	{
		return lseek(fd, offs, SEEK_SET);
	}
}

size_t fio_fwrite(FILE* f, void const* buf, size_t size)
{
	return fio_is_remote_file(f)
		? fio_write(fio_fileno(f), buf, size)
		: fwrite(buf, 1, size, f);
}

ssize_t fio_write(int fd, void const* buf, size_t size)
{
	if (fio_is_remote_fd(fd))
	{
		fio_header hdr;

		SYS_CHECK(pthread_mutex_lock(&fio_write_mutex));

		hdr.cop = FIO_WRITE;
		hdr.handle = fd & ~FIO_PIPE_MARKER;
		hdr.size = size;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, buf, size), size);

		SYS_CHECK(pthread_mutex_unlock(&fio_write_mutex));
		return size;
	}
	else
	{
		return write(fd, buf, size);
	}
}

size_t fio_fread(FILE* f, void* buf, size_t size)
{
	return fio_is_remote_file(f)
		? fio_read(fio_fileno(f), buf, size)
		: fread(buf, 1, size, f);
}

ssize_t fio_read(int fd, void* buf, size_t size)
{
	if (fio_is_remote_fd(fd))
	{
		fio_header hdr;

		hdr.cop = FIO_READ;
		hdr.handle = fd & ~FIO_PIPE_MARKER;
		hdr.size = 0;
		hdr.arg = size;

		SYS_CHECK(pthread_mutex_lock(&fio_read_mutex));
		SYS_CHECK(pthread_mutex_lock(&fio_write_mutex));

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		SYS_CHECK(pthread_mutex_unlock(&fio_write_mutex));

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_SEND);
		IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);

		SYS_CHECK(pthread_mutex_unlock(&fio_read_mutex));

		return hdr.size;
	}
	else
	{
		return read(fd, buf, size);
	}
}

int fio_ffstat(FILE* f, struct stat* st)
{
	return fio_is_remote_file(f)
		? fio_fstat(fio_fileno(f), st)
		: fio_fstat(fileno(f), st);
}

int fio_fstat(int fd, struct stat* st)
{
	if (fio_is_remote_fd(fd))
	{
		fio_header hdr;

		hdr.cop = FIO_FSTAT;
		hdr.handle = fd & ~FIO_PIPE_MARKER;
		hdr.size = 0;

		SYS_CHECK(pthread_mutex_lock(&fio_read_mutex));
		SYS_CHECK(pthread_mutex_lock(&fio_write_mutex));

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		SYS_CHECK(pthread_mutex_unlock(&fio_write_mutex));

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_FSTAT);
		IO_CHECK(fio_read_all(fio_stdin, st, sizeof(*st)), sizeof(*st));

		SYS_CHECK(pthread_mutex_unlock(&fio_read_mutex));

		return hdr.arg;
	}
	else
	{
		return fstat(fd, st);
	}
}

int fio_stat(char const* path, struct stat* st, bool follow_symlinks, fio_location location)
{
	if (fio_is_remote(location))
	{
		fio_header hdr;
		size_t path_len = strlen(path) + 1;

		hdr.cop = FIO_STAT;
		hdr.handle = -1;
		hdr.arg = follow_symlinks;
		hdr.size = path_len;

		SYS_CHECK(pthread_mutex_lock(&fio_read_mutex));
		SYS_CHECK(pthread_mutex_lock(&fio_write_mutex));

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);

		SYS_CHECK(pthread_mutex_unlock(&fio_write_mutex));

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_STAT);
		IO_CHECK(fio_read_all(fio_stdin, st, sizeof(*st)), sizeof(*st));

		SYS_CHECK(pthread_mutex_unlock(&fio_read_mutex));

		return hdr.arg;
	}
	else
	{
		return follow_symlinks ? stat(path, st) : lstat(path,  st);
	}
}

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

		SYS_CHECK(pthread_mutex_lock(&fio_read_mutex));
		SYS_CHECK(pthread_mutex_lock(&fio_write_mutex));

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);

		SYS_CHECK(pthread_mutex_unlock(&fio_write_mutex));

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_ACCESS);

		SYS_CHECK(pthread_mutex_unlock(&fio_read_mutex));

		return hdr.arg;
	}
	else
	{
		return access(path, mode);
	}
}

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

		SYS_CHECK(pthread_mutex_lock(&fio_write_mutex));

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, old_path, old_path_len), old_path_len);
		IO_CHECK(fio_write_all(fio_stdout, new_path, new_path_len), new_path_len);

		SYS_CHECK(pthread_mutex_unlock(&fio_write_mutex));
		return 0;
	}
	else
	{
		return rename(old_path, new_path);
	}
}

int fio_unlink(char const* path, fio_location location)
{
	if (fio_is_remote(location))
	{
		fio_header hdr;
		size_t path_len = strlen(path) + 1;
		hdr.cop = FIO_UNLINK;
		hdr.handle = -1;
		hdr.size = path_len;

		SYS_CHECK(pthread_mutex_lock(&fio_write_mutex));

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);

		SYS_CHECK(pthread_mutex_unlock(&fio_write_mutex));
		return 0;
	}
	else
	{
		return unlink(path);
	}
}

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

		SYS_CHECK(pthread_mutex_lock(&fio_write_mutex));

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);

		SYS_CHECK(pthread_mutex_unlock(&fio_write_mutex));
		return 0;
	}
	else
	{
		return dir_create_dir(path, mode);
	}
}

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

		SYS_CHECK(pthread_mutex_lock(&fio_write_mutex));

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);

		SYS_CHECK(pthread_mutex_unlock(&fio_write_mutex));
		return 0;
	}
	else
	{
		return chmod(path, mode);
	}
}

#ifdef HAVE_LIBZ
gzFile fio_gzopen(char const* path, char const* mode, int* tmp_fd, fio_location location)
{
	gzFile file;
	if (fio_is_remote(location))
	{
		int fd = mkstemp("gz.XXXXXX");
		if (fd < 0)
			return NULL;
		if (strcmp(mode, PG_BINARY_W) == 0)
		{
			*tmp_fd = fd;
		}
		else
		{
			int rd = fio_open(path, O_RDONLY|PG_BINARY, location);
			struct stat st;
			void* buf;
			if (rd < 0) {
				return NULL;
			}
			SYS_CHECK(fio_fstat(rd, &st));
			buf = malloc(st.st_size);
			IO_CHECK(fio_read(rd, buf, st.st_size), st.st_size);
			IO_CHECK(write(fd, buf, st.st_size), st.st_size);
			SYS_CHECK(fio_close(rd));
			free(buf);
			*tmp_fd = -1;
		}
		file = gzdopen(fd, mode);
	}
	else
	{
		*tmp_fd = -1;
		file = gzopen(path, mode);
	}
	return file;
}

int fio_gzclose(gzFile file, char const* path, int tmp_fd)
{
	if (tmp_fd >= 0)
	{
		off_t size;
		void* buf;
		int fd;

		SYS_CHECK(gzflush(file, Z_FINISH));

		size = lseek(tmp_fd, 0,  SEEK_END);
		buf = malloc(size);

		lseek(tmp_fd, 0, SEEK_SET);
		IO_CHECK(read(tmp_fd, buf, size), size);

		SYS_CHECK(gzclose(file)); /* should close tmp_fd */

		fd = fio_open(path, O_RDWR|O_CREAT|O_TRUNC, FILE_PERMISSIONS);
		if (fd < 0) {
			free(buf);
			return -1;
		}
		IO_CHECK(fio_write(fd, buf, size), size);
		free(buf);
		return fio_close(fd);
	}
	else
	{
		return gzclose(file);
	}
}
#endif


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
		buf = malloc(size);
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

void fio_transfer(fio_shared_variable var)
{
	size_t var_size = fio_bindings[var].size;
	fio_header* msg = (fio_header*)malloc(sizeof(fio_header) + var_size);

	msg->cop = FIO_TRANSFER;
	msg->arg = var;
	msg->size = var_size;
	memcpy(msg+1, fio_bindings[var].address, var_size);

	SYS_CHECK(pthread_mutex_lock(&fio_write_mutex));

	IO_CHECK(fio_write_all(fio_stdout, msg, sizeof(fio_header) + var_size), sizeof(fio_header) + var_size);

	SYS_CHECK(pthread_mutex_unlock(&fio_write_mutex));
	free(msg);
}

void fio_communicate(int in, int out)
{
	int fd[FIO_FDMAX];
	size_t buf_size = 128*1024;
	char* buf = (char*)malloc(buf_size);
	fio_header hdr;
	struct stat st;
	int rc;

	while ((rc = fio_read_all(in, &hdr, sizeof hdr)) == sizeof(hdr)) {
		if (hdr.size != 0) {
			if (hdr.size > buf_size) {
				buf_size = hdr.size;
				buf = (char*)realloc(buf, buf_size);
			}
			IO_CHECK(fio_read_all(in, buf, hdr.size), hdr.size);
		}
		switch (hdr.cop) {
		  case FIO_LOAD:
			fio_send_file(out, buf);
			break;
		  case FIO_OPEN:
			SYS_CHECK(fd[hdr.handle] = open(buf, hdr.arg, FILE_PERMISSIONS));
			break;
		  case FIO_CLOSE:
			SYS_CHECK(close(fd[hdr.handle]));
			break;
		  case FIO_WRITE:
			IO_CHECK(fio_write_all(fd[hdr.handle], buf, hdr.size), hdr.size);
			break;
		  case FIO_READ:
			if ((size_t)hdr.arg > buf_size) {
				buf_size = hdr.arg;
				buf = (char*)realloc(buf, buf_size);
			}
			rc = read(fd[hdr.handle], buf, hdr.arg);
			hdr.cop = FIO_SEND;
			hdr.size = rc > 0 ? rc : 0;
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)),  sizeof(hdr));
			IO_CHECK(fio_write_all(out, buf, rc),  rc);
			break;
		  case FIO_FSTAT:
			hdr.size = sizeof(st);
			hdr.arg = fstat(fd[hdr.handle], &st);
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			IO_CHECK(fio_write_all(out, &st, sizeof(st)), sizeof(st));
			break;
		  case FIO_STAT:
			hdr.size = sizeof(st);
			hdr.arg = hdr.arg ? stat(buf, &st) : lstat(buf, &st);
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			IO_CHECK(fio_write_all(out, &st, sizeof(st)), sizeof(st));
			break;
		  case FIO_ACCESS:
			hdr.size = 0;
			hdr.arg = access(buf, hdr.arg);
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			break;
		  case FIO_RENAME:
			SYS_CHECK(rename(buf, buf + strlen(buf)));
			break;
		  case FIO_UNLINK:
			SYS_CHECK(unlink(buf));
			break;
		  case FIO_MKDIR:
			SYS_CHECK(dir_create_dir(buf, hdr.arg));
			break;
		  case FIO_CHMOD:
			SYS_CHECK(chmod(buf, hdr.arg));
			break;
		  case FIO_SEEK:
			SYS_CHECK(lseek(fd[hdr.handle], hdr.arg, SEEK_SET));
			break;
		  case FIO_TRUNCATE:
			SYS_CHECK(ftruncate(fd[hdr.handle], hdr.arg));
			break;
		  case FIO_TRANSFER:
			Assert(hdr.size == fio_bindings[hdr.arg].size);
			memcpy(fio_bindings[hdr.arg].address, buf, hdr.size);
			break;
		  default:
			Assert(false);
		}
	}
	free(buf);
	if (rc != 0) {
		perror("read");
		exit(EXIT_FAILURE);
	}
}

