#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/stat.h>

#include "pg_probackup.h"
#include "file.h"

#define PRINTF_BUF_SIZE 1024

static pthread_mutex_t fio_mutex = PTHREAD_MUTEX_INITIALIZER;
static unsigned long fio_fdset = 0;
static void* fio_stdin_buffer;
static int fio_stdout = 0;
static int fio_stdin = 0;

void fio_redirect(int in, int out)
{
	fio_stdin = in;
	fio_stdout = out;
}

static bool fio_is_remote_file(FILE* fd)
{
	return (size_t)fd <= FIO_FDMAX;
}

static bool fio_is_remote(fio_location location)
{
	return (location == FIO_BACKUP_HOST && is_remote_agent)
		|| (location == FIO_DB_HOST && ssh_host != NULL);
}

static ssize_t fio_read(int fd, void* buf, size_t size)
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
		}
		offs += rc;
	}
	return size;
}

FILE* fio_open_stream(char const* path, fio_location location)
{
	FILE* f;
	if (fio_is_remote(location))
	{
		fio_header hdr;
		hdr.cop = FIO_READ;
		hdr.size = strlen(path) + 1;

		IO_CHECK(write(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(write(fio_stdout, path, hdr.size), hdr.size);

		IO_CHECK(fio_read(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		if (hdr.size > 0)
		{
			Assert(fio_stdin_buffer == NULL);
			fio_stdin_buffer = malloc(hdr.size);
			IO_CHECK(fio_read(fio_stdin, fio_stdin_buffer, hdr.size), hdr.size);
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

FILE* fio_open(char const* path, char const* mode, fio_location location)
{
	FILE* f;
	if (fio_is_remote(location))
	{
		int i;
		fio_header hdr;
		unsigned long mask;

		SYS_CHECK(pthread_mutex_lock(&fio_mutex));
		mask = fio_fdset;
		for (i = 0; (mask & 1) != 0; i++, mask >>= 1);
		if (i == FIO_FDMAX) {
			return NULL;
		}
		hdr.cop = strchr(mode,'+') ? FIO_OPEN_EXISTED : FIO_OPEN_NEW;
		hdr.handle = i;
		hdr.size = strlen(path) + 1;
		fio_fdset |= 1 << i;

		IO_CHECK(write(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(write(fio_stdout, path, hdr.size), hdr.size);

		SYS_CHECK(pthread_mutex_unlock(&fio_mutex));

		f = (FILE*)(size_t)(i + 1);
	}
	else
	{
		f = fopen(path, mode);
	}
	return f;
}

int fio_printf(FILE* f, char const* format, ...)
{
	int rc;
    va_list args;
    va_start (args, format);
	if (fio_stdout)
	{
		char buf[PRINTF_BUF_SIZE];
#ifdef HAS_VSNPRINTF
		rc = vsnprintf(buf, sizeof(buf), format,  args);
#else
		rc = vsprintf(buf, format,  args);
#endif
		if (rc > 0) {
			fio_write(f, buf, rc);
		}
	}
	else
	{
		rc = vfprintf(f, format, args);
	}
    va_end (args);
	return rc;
}

int fio_flush(FILE* f)
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

int fio_close(FILE* f)
{
	if (fio_is_remote_file(f))
	{
		fio_header hdr;

		SYS_CHECK(pthread_mutex_lock(&fio_mutex));

		hdr.cop = FIO_CLOSE;
		hdr.handle = (size_t)f - 1;
		hdr.size = 0;
		fio_fdset &= ~(1 << hdr.handle);

		IO_CHECK(write(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		SYS_CHECK(pthread_mutex_unlock(&fio_mutex));
		return 0;
	}
	else
	{
		return fclose(f);
	}
}

int fio_truncate(FILE* f, off_t size)
{
	if (fio_is_remote_file(f))
	{
		fio_header hdr;

		SYS_CHECK(pthread_mutex_lock(&fio_mutex));

		hdr.cop = FIO_TRUNCATE;
		hdr.handle = (size_t)f - 1;
		hdr.size = 0;
		hdr.arg = size;

		IO_CHECK(write(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		SYS_CHECK(pthread_mutex_unlock(&fio_mutex));
		return 0;
	}
	else
	{
		return ftruncate(fileno(f), size);
	}
}

int fio_seek(FILE* f, off_t offs)
{
	if (fio_is_remote_file(f))
	{
		fio_header hdr;

		SYS_CHECK(pthread_mutex_lock(&fio_mutex));

		hdr.cop = FIO_SEEK;
		hdr.handle = (size_t)f - 1;
		hdr.size = 0;
		hdr.arg = offs;

		IO_CHECK(write(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		SYS_CHECK(pthread_mutex_unlock(&fio_mutex));
		return 0;
	}
	else
	{
		return fseek(f, offs, SEEK_SET);
	}
}

size_t fio_write(FILE* f, void const* buf, size_t size)
{
	if (fio_is_remote_file(f))
	{
		fio_header hdr;

		SYS_CHECK(pthread_mutex_lock(&fio_mutex));

		hdr.cop = FIO_WRITE;
		hdr.handle = (size_t)f - 1;
		hdr.size = size;

		IO_CHECK(write(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(write(fio_stdout, buf, size), size);

		SYS_CHECK(pthread_mutex_unlock(&fio_mutex));
		return size;
	}
	else
	{
		return fwrite(buf, 1, size, f);
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

		IO_CHECK(write(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(write(fio_stdout, old_path, old_path_len), old_path_len);
		IO_CHECK(write(fio_stdout, new_path, new_path_len), new_path_len);

		SYS_CHECK(pthread_mutex_unlock(&fio_mutex));
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

		IO_CHECK(write(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(write(fio_stdout, path, path_len), path_len);

		SYS_CHECK(pthread_mutex_unlock(&fio_mutex));
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

		IO_CHECK(write(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(write(fio_stdout, path, path_len), path_len);

		SYS_CHECK(pthread_mutex_unlock(&fio_mutex));
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

		IO_CHECK(write(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(write(fio_stdout, path, path_len), path_len);

		SYS_CHECK(pthread_mutex_unlock(&fio_mutex));
		return 0;
	}
	else
	{
		return chmod(path, mode);
	}
}

static void fio_send_file(int out, char const* path)
{
	int fd = open(path, O_RDONLY);
	fio_header hdr;
	void* buf = NULL;

	hdr.size = 0;
	hdr.cop = FIO_READ;

	if (fd >= 0)
	{
		off_t size = lseek(fd, 0, SEEK_END);
		buf = malloc(size);
		lseek(fd, 0, SEEK_SET);
		IO_CHECK(fio_read(fd, buf, size), size);
		hdr.size = size;
		SYS_CHECK(close(fd));
	}
	IO_CHECK(write(out, &hdr, sizeof(hdr)), sizeof(hdr));
	if (buf)
	{
		IO_CHECK(write(out, buf, hdr.size), hdr.size);
		free(buf);
	}
}

void fio_communicate(int in, int out)
{
	int fd[FIO_FDMAX];
	char buf[BLCKSZ*2]; /* need more space for page header */
	fio_header hdr;
	int rc;

	while ((rc = read(in, &hdr, sizeof hdr)) == sizeof(hdr)) {
		if (hdr.size != 0) {
			Assert(hdr.size < sizeof(buf));
			IO_CHECK(fio_read(in, buf, hdr.size), hdr.size);
		}
		switch (hdr.cop) {
		  case FIO_READ:
			fio_send_file(out, buf);
			break;
		  case FIO_OPEN_NEW:
			SYS_CHECK(fd[hdr.handle] = open(buf, O_RDWR|O_CREAT|O_TRUNC, 0777));
			break;
		  case FIO_OPEN_EXISTED:
			SYS_CHECK(fd[hdr.handle] = open(buf, O_RDWR|O_CREAT, 0777));
			break;
		  case FIO_CLOSE:
			SYS_CHECK(close(fd[hdr.handle]));
			break;
		  case FIO_WRITE:
			IO_CHECK(write(fd[hdr.handle], buf, hdr.size), hdr.size);
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
		  default:
			Assert(false);
		}
	}

	if (rc != 0) {
		perror("read");
		exit(EXIT_FAILURE);
	}
}

