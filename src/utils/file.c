#include <stdio.h>
#include <unistd.h>

#include "pg_probackup.h"
#include <signal.h>

#include "file.h"
#include "catalog/pg_tablespace.h"
#if PG_VERSION_NUM < 110000
#include "catalog/catalog.h"
#endif
#include "storage/checksum.h"

#define PRINTF_BUF_SIZE  1024

static __thread uint64_t fio_fdset = 0;
static __thread int fio_stdout = 0;
static __thread int fio_stdin = 0;
static __thread int fio_stderr = 0;
static char *async_errormsg = NULL;

#define PAGE_ZEROSEARCH_COARSE_GRANULARITY 4096
#define PAGE_ZEROSEARCH_FINE_GRANULARITY 64
static const char zerobuf[PAGE_ZEROSEARCH_COARSE_GRANULARITY] = {0};

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
	bool handle_tablespaces;
	bool follow_symlink;
	bool backup_logs;
	bool skip_hidden;
	int  external_dir_num;
} fio_list_dir_request;

typedef struct {
    char path[MAXPGPATH];
	bool root_as_well;
} fio_remove_dir_request;

typedef struct
{
	pio_file_kind_e kind;
	mode_t  mode;
	int64_t size;
	time_t  mtime;
	bool    is_datafile;
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

typedef struct __attribute__((packed))
{
	int32_t segno;
	int32_t pagemaplen;
	XLogRecPtr start_lsn;
	CompressAlg calg;
	int clevel;
	uint32 checksum_version;
	int just_validate;
} fio_iterate_pages_request;

struct __attribute__((packed)) fio_req_open_rewrite {
	uint32_t  permissions;
	bool      binary;
	bool      use_temp;
};

struct __attribute__((packed)) fio_req_open_write {
	uint32_t  permissions;
	bool exclusive;
};

/* Convert FIO pseudo handle to index in file descriptor array */
#define fio_fileno(f) (((size_t)f - 1) | FIO_PIPE_MARKER)

#if defined(WIN32)
#undef open
#undef fopen
#endif

static void dir_list_file(parray *files, const char *root, bool handle_tablespaces,
						  bool follow_symlink, bool backup_logs, bool skip_hidden,
						  int external_dir_num, pioDBDrive_i drive);
static void dir_list_file_internal(parray *files, pgFile *parent, const char *parent_dir,
								   bool handle_tablespaces, bool follow_symlink, bool backup_logs,
								   bool skip_hidden, int external_dir_num, pioDBDrive_i drive);

void
setMyLocation(ProbackupSubcmd const subcmd)
{

#ifdef WIN32
	if (IsSshProtocol())
		elog(ERROR, "Currently remote operations on Windows are not supported");
#endif

	if (!IsSshProtocol())
	{
		MyLocation = FIO_LOCAL_HOST;
		return;
	}

	switch (subcmd)
	{
	case ARCHIVE_GET_CMD:
	case ARCHIVE_PUSH_CMD:
		MyLocation = FIO_DB_HOST;
		break;
	case BACKUP_CMD:
	case RESTORE_CMD:
	case ADD_INSTANCE_CMD:
	case CATCHUP_CMD:
		MyLocation = FIO_BACKUP_HOST;
		break;
	default:
		MyLocation = FIO_LOCAL_HOST;
		break;
	}
}

/* Use specified file descriptors as stdin/stdout for FIO functions */
void
fio_redirect(int in, int out, int err)
{
	fio_stdin = in;
	fio_stdout = out;
	fio_stderr = err;
}

void
fio_error(int rc, int size, const char* file, int line)
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
static bool
fio_is_remote_fd(int fd)
{
	return (fd & FIO_PIPE_MARKER) != 0;
}

#ifdef WIN32
/* TODO: use real pread on Linux */
static ssize_t
pread(int fd, void* buf, size_t size, off_t off)
{
	off_t rc = lseek(fd, off, SEEK_SET);
	if (rc != off)
		return -1;
	return read(fd, buf, size);
}
#endif /* WIN32 */

#ifdef WIN32
static int
remove_file_or_dir(const char* path)
{
	int rc = remove(path);

	if (rc < 0 && errno == EACCES)
		rc = rmdir(path);
	return rc;
}
#else
#define remove_file_or_dir(path) remove(path)
#endif

static void
fio_ensure_remote(void)
{
	if (!fio_stdin && !launch_agent())
		elog(ERROR, "Failed to establish SSH connection: %s", strerror(errno));
}

/* Check if specified location is local for current node */
bool
fio_is_remote(fio_location location)
{
	bool is_remote = MyLocation != FIO_LOCAL_HOST
		&& location != FIO_LOCAL_HOST
		&& location != MyLocation;
	if (is_remote && !fio_stdin && !launch_agent())
		elog(ERROR, "Failed to establish SSH connection: %s", strerror(errno));
	return is_remote;
}

/* Check if specified location is local for current node */
bool
fio_is_remote_simple(fio_location location)
{
	bool is_remote = MyLocation != FIO_LOCAL_HOST
		&& location != FIO_LOCAL_HOST
		&& location != MyLocation;
	return is_remote;
}

static int
find_free_handle(void)
{
	uint64_t m = fio_fdset;
	int i;
	for (i = 0; (m & 1); i++, m>>=1) {}
	if (i == FIO_FDMAX) {
		elog(ERROR, "Descriptor pool for remote files is exhausted, "
					"probably too many remote directories are opened");
	}
	return i;
}

static void
set_handle(int i)
{
	fio_fdset |= 1 << i;
}

static void
unset_handle(int i)
{
	fio_fdset &= ~(1 << i);
}

/* Try to read specified amount of bytes unless error or EOF are encountered */
static ssize_t
fio_read_all(int fd, void* buf, size_t size)
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
static ssize_t
fio_write_all(int fd, void const* buf, size_t size)
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
void
fio_get_agent_version(int* protocol, char* payload_buf, size_t payload_buf_size)
{
	fio_header hdr = (fio_header){.cop = FIO_AGENT_VERSION};

	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
	if (hdr.size > payload_buf_size)
	{
		elog(ERROR, "Corrupted remote compatibility protocol: insufficient payload_buf_size=%zu", payload_buf_size);
	}

	*protocol = hdr.arg;
	IO_CHECK(fio_read_all(fio_stdin, payload_buf, hdr.size), hdr.size);
}

pio_file_kind_e
pio_statmode2file_kind(mode_t mode, const char* path)
{
	pio_file_kind_e	kind = PIO_KIND_UNKNOWN;
	if (S_ISREG(mode))
		kind = PIO_KIND_REGULAR;
	else if (S_ISDIR(mode))
		kind = PIO_KIND_DIRECTORY;
#ifdef S_ISLNK
	else if (S_ISLNK(mode))
		kind = PIO_KIND_SYMLINK;
#endif
#ifdef S_ISFIFO
	else if (S_ISFIFO(mode))
		kind = PIO_KIND_FIFO;
#endif
#ifdef S_ISSOCK
	else if (S_ISFIFO(mode))
		kind = PIO_KIND_SOCK;
#endif
#ifdef S_ISCHR
	else if (S_ISCHR(mode))
		kind = PIO_KIND_CHARDEV;
#endif
#ifdef S_ISBLK
	else if (S_ISBLK(mode))
		kind = PIO_KIND_BLOCKDEV;
#endif
	else
		elog(ERROR, "Unsupported file mode kind \"%x\" for file '%s'",
			 mode, path);
	return kind;
}

pio_file_kind_e
pio_str2file_kind(const char* str, const char* path)
{
	pio_file_kind_e	kind = PIO_KIND_UNKNOWN;
	if (strncmp(str, "reg", 3) == 0)
		kind = PIO_KIND_REGULAR;
	else if (strncmp(str, "dir", 3) == 0)
		kind = PIO_KIND_DIRECTORY;
	else if (strncmp(str, "sym", 3) == 0)
		kind = PIO_KIND_SYMLINK;
	else if (strncmp(str, "fifo", 4) == 0)
		kind = PIO_KIND_FIFO;
	else if (strncmp(str, "sock", 4) == 0)
		kind = PIO_KIND_SOCK;
	else if (strncmp(str, "chdev", 5) == 0)
		kind = PIO_KIND_CHARDEV;
	else if (strncmp(str, "bldev", 5) == 0)
		kind = PIO_KIND_BLOCKDEV;
	else
		elog(ERROR, "Unknown file kind \"%s\" for file '%s'",
			 str, path);
	return kind;
}

const char*
pio_file_kind2str(pio_file_kind_e kind, const char *path)
{
	switch (kind)
	{
		case PIO_KIND_REGULAR:
			return "reg";
		case PIO_KIND_DIRECTORY:
			return "dir";
		case PIO_KIND_SYMLINK:
			return "sym";
		case PIO_KIND_FIFO:
			return "fifo";
		case PIO_KIND_SOCK:
			return "sock";
		case PIO_KIND_CHARDEV:
			return "chdev";
		case PIO_KIND_BLOCKDEV:
			return "bldev";
		default:
			elog(ERROR, "Unknown file kind \"%d\" for file '%s'",
				 kind, path);
	}
	return NULL;
}

#ifndef S_ISGID
#define S_ISGID 0
#endif
#ifndef S_ISUID
#define S_ISUID 0
#endif
#ifndef S_ISVTX
#define S_ISVTX 0
#endif

mode_t
pio_limit_mode(mode_t mode)
{
	if (S_ISDIR(mode))
		mode &= 0x1ff | S_ISGID | S_ISUID | S_ISVTX;
	else
		mode &= 0x1ff;
	return mode;
}

/* Open directory */
DIR*
fio_opendir(fio_location location, const char* path)
{
	DIR* dir;
	if (fio_is_remote(location))
	{
		int handle;
		fio_header hdr;

		handle = find_free_handle();
		hdr.cop = FIO_OPENDIR;
		hdr.handle = handle;
		hdr.size = strlen(path) + 1;
		set_handle(handle);

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

		if (hdr.arg != 0)
		{
			errno = hdr.arg;
			unset_handle(hdr.handle);
			return NULL;
		}
		dir = (DIR*)(size_t)(handle + 1);
	}
	else
	{
		dir = opendir(path);
	}
	return dir;
}

/* Get next directory entry */
struct dirent*
fio_readdir(DIR *dir)
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
int
fio_closedir(DIR *dir)
{
	if (fio_is_remote_file((FILE*)dir))
	{
		fio_header hdr;
		hdr.cop = FIO_CLOSEDIR;
		hdr.handle = (size_t)dir - 1;
		hdr.size = 0;
		unset_handle(hdr.handle);

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		return 0;
	}
	else
	{
		return closedir(dir);
	}
}

/* Open file */
int
fio_open(fio_location location, const char* path, int mode)
{
	int fd;
	if (fio_is_remote(location))
	{
		int handle;
		fio_header hdr;

		handle = find_free_handle();
		hdr.cop = FIO_OPEN;
		hdr.handle = handle;
		hdr.size = strlen(path) + 1;
		hdr.arg = mode;
//		hdr.arg = mode & ~O_EXCL;
//		elog(INFO, "PATH: %s MODE: %i, %i", path, mode, O_EXCL);
//		elog(INFO, "MODE: %i", hdr.arg);
		set_handle(handle);

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

		/* check results */
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

		if (hdr.arg != 0)
		{
			errno = hdr.arg;
			unset_handle(hdr.handle);
			return -1;
		}
		fd = handle | FIO_PIPE_MARKER;
	}
	else
	{
		fd = open(path, mode, FILE_PERMISSION);
	}
	return fd;
}


/* Close ssh session */
void
fio_disconnect(void)
{
	if (fio_stdin)
	{
		fio_header hdr = (fio_header){.cop = FIO_DISCONNECT};
		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_DISCONNECTED);
		SYS_CHECK(close(fio_stdin));
		SYS_CHECK(close(fio_stdout));
		SYS_CHECK(close(fio_stderr));
		fio_stdin = 0;
		fio_stdout = 0;
		fio_stderr = 0;
		wait_ssh();
	}
}

/* Open stdio file */
FILE*
fio_fopen(fio_location location, const char* path, const char* mode)
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
		fd = fio_open(location, path, flags);
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

/* Flush stream data (does nothing for remote file) */
int
fio_fflush(FILE* f)
{
	int rc = 0;
	if (!fio_is_remote_file(f))
		rc = fflush(f);
	return rc;
}

/* Close output stream */
int
fio_fclose(FILE* f)
{
	return fio_is_remote_file(f)
		? fio_close(fio_fileno(f))
		: fclose(f);
}

/* Close file */
int
fio_close(int fd)
{
	if (fio_is_remote_fd(fd))
	{
		fio_header hdr = {
			.cop = FIO_CLOSE,
			.handle = fd & ~FIO_PIPE_MARKER,
			.size = 0,
			.arg = 0,
		};

		unset_handle(hdr.handle);
		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		/* Wait for response */
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_CLOSE);

		if (hdr.arg != 0)
		{
			errno = hdr.arg;
			return -1;
		}

		return 0;
	}
	else
	{
		return close(fd);
	}
}

/* Close remote file implementation */
static void
fio_close_impl(int fd, int out)
{
	fio_header hdr = {
		.cop = FIO_CLOSE,
		.handle = -1,
		.size = 0,
		.arg = 0,
	};

	if (close(fd) != 0)
		hdr.arg = errno;

	/* send header */
	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
}

/* Truncate stdio file */
int
fio_ftruncate(FILE* f, off_t size)
{
	return fio_is_remote_file(f)
		? fio_truncate(fio_fileno(f), size)
		: ftruncate(fileno(f), size);
}

/* Truncate file
 * TODO: make it synchronous
 */
int
fio_truncate(int fd, off_t size)
{
	if (fio_is_remote_fd(fd))
	{
		fio_header hdr = {
			.cop = FIO_TRUNCATE,
			.handle = fd & ~FIO_PIPE_MARKER,
			.size = 0,
			.arg = size,
		};

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		return 0;
	}
	else
	{
		return ftruncate(fd, size);
	}
}

/* Set position in stdio file */
int
fio_fseek(FILE* f, off_t offs)
{
	return fio_is_remote_file(f)
		? fio_seek(fio_fileno(f), offs)
		: fseek(f, offs, SEEK_SET);
}

/* Set position in file */
/* TODO: make it synchronous or check async error */
int
fio_seek(int fd, off_t offs)
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

/*
 * Write buffer to descriptor by calling write(),
 * If size of written data is less than buffer size,
 * then try to write what is left.
 * We do this to get honest errno if there are some problems
 * with filesystem, since writing less than buffer size
 * is not considered an error.
 */
static ssize_t
durable_write(int fd, const char* buf, size_t size)
{
	off_t current_pos = 0;
	size_t bytes_left = size;

	while (bytes_left > 0)
	{
		int rc = write(fd, buf + current_pos, bytes_left);

		if (rc <= 0)
			return rc;

		bytes_left -= rc;
		current_pos += rc;
	}

	return size;
}

static void
fio_write_impl(int fd, void const* buf, size_t size, int out)
{
	fio_header hdr = {
		.cop = FIO_WRITE,
		.handle = -1,
		.size = 0,
		.arg = 0,
	};
	int rc;

	rc = durable_write(fd, buf, size);

	if (rc < 0)
		hdr.arg = errno;

	/* send header */
	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));

	return;
}

size_t
fio_fwrite_async(FILE* f, void const* buf, size_t size)
{
	return fio_is_remote_file(f)
		? fio_write_async(fio_fileno(f), buf, size)
		: fwrite(buf, 1, size, f);
}

/* Write data to the file */
/* TODO: support async report error */
ssize_t
fio_write_async(int fd, void const* buf, size_t size)
{
	if (size == 0)
		return 0;

	if (fio_is_remote_fd(fd))
	{
		fio_header hdr = {
			.cop = FIO_WRITE_ASYNC,
			.handle = fd & ~FIO_PIPE_MARKER,
			.size = size,
			.arg = 0,
		};

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, buf, size), size);
		return size;
	}
	else
		return durable_write(fd, buf, size);
}

static void
fio_write_async_impl(int fd, void const* buf, size_t size, int out)
{
	/* Quick exit if agent is tainted */
	if (async_errormsg)
		return;

	if (durable_write(fd, buf, size) <= 0)
	{
		async_errormsg = pgut_malloc(ERRMSG_MAX_LEN);
		snprintf(async_errormsg, ERRMSG_MAX_LEN, "%s", strerror(errno));
	}
}

static int32
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
ssize_t
fio_fwrite_async_compressed(FILE* f, void const* buf, size_t size, int compress_alg)
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
		char *errormsg = NULL;
		char decompressed_buf[BLCKSZ];
		int32 decompressed_size = fio_decompress(decompressed_buf, buf, size, compress_alg, &errormsg);

		if (decompressed_size < 0)
			elog(ERROR, "%s", errormsg);

		return fwrite(decompressed_buf, 1, decompressed_size, f);
	}
}

static void
fio_write_compressed_impl(int fd, void const* buf, size_t size, int compress_alg)
{
	int32 decompressed_size;
	char decompressed_buf[BLCKSZ];

	/* If the previous command already have failed,
	 * then there is no point in bashing a head against the wall
	 */
	if (async_errormsg)
		return;

	/* decompress chunk */
	decompressed_size = fio_decompress(decompressed_buf, buf, size, compress_alg, &async_errormsg);

	if (decompressed_size < 0)
		return;

	if (durable_write(fd, decompressed_buf, decompressed_size) <= 0)
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

/* Read data from file */
ssize_t
fio_read(int fd, void* buf, size_t size)
{
	if (fio_is_remote_fd(fd))
	{
		fio_header hdr = {
			.cop = FIO_READ,
			.handle = fd & ~FIO_PIPE_MARKER,
			.size = 0,
			.arg = size,
		};

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_SEND);
		IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);
		errno = hdr.arg;

		return hdr.size;
	}
	else
	{
		return read(fd, buf, size);
	}
}

/*
 * Read value of a symbolic link
 * this is a wrapper about readlink() syscall
 * side effects: string truncation occur (and it
 * can be checked by caller by comparing
 * returned value >= valsiz)
 */
ssize_t
fio_readlink(fio_location location, const char *path, char *value, size_t valsiz)
{
	if (!fio_is_remote(location))
	{
		/* readlink don't place trailing \0 */
		ssize_t len = readlink(path, value, valsiz);
		value[len < valsiz ? len : valsiz] = '\0';
		return len;
	}
	else
	{
		fio_header hdr;
		size_t path_len = strlen(path) + 1;

		hdr.cop = FIO_READLINK;
		hdr.handle = -1;
		Assert(valsiz <= UINT_MAX); /* max value of fio_header.arg */
		hdr.arg = valsiz;
		hdr.size = path_len;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_READLINK);
		Assert(hdr.size <= valsiz);
		IO_CHECK(fio_read_all(fio_stdin, value, hdr.size), hdr.size);
		value[hdr.size < valsiz ? hdr.size : valsiz] = '\0';
		return hdr.size;
	}
}

/* Create symbolic link */
int
fio_symlink(fio_location location, const char* target, const char* link_path, bool overwrite)
{
	if (fio_is_remote(location))
	{
		size_t target_len = strlen(target) + 1;
		size_t link_path_len = strlen(link_path) + 1;
		fio_header hdr = {
			.cop = FIO_SYMLINK,
			.handle = -1,
			.size = target_len + link_path_len,
			.arg = overwrite ? 1 : 0,
		};

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, target, target_len), target_len);
		IO_CHECK(fio_write_all(fio_stdout, link_path, link_path_len), link_path_len);

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_SYMLINK);

		if (hdr.arg != 0)
		{
			errno = hdr.arg;
			return -1;
		}
		return 0;
	}
	else
	{
		if (overwrite)
			remove_file_or_dir(link_path);

		return symlink(target, link_path);
	}
}

static void
fio_symlink_impl(const char* target, const char* link_path, bool overwrite, int out)
{
	fio_header hdr = {
		.cop = FIO_SYMLINK,
		.handle = -1,
		.size = 0,
		.arg = 0,
	};

	if (overwrite)
		remove_file_or_dir(link_path);

	if (symlink(target, link_path) != 0)
		hdr.arg = errno;

	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
}

/* Rename file */
int
fio_rename(fio_location location, const char* old_path, const char* new_path)
{
	if (fio_is_remote(location))
	{
		size_t old_path_len = strlen(old_path) + 1;
		size_t new_path_len = strlen(new_path) + 1;
		fio_header hdr = {
			.cop = FIO_RENAME,
			.handle = -1,
			.size = old_path_len + new_path_len,
			.arg = 0,
		};

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, old_path, old_path_len), old_path_len);
		IO_CHECK(fio_write_all(fio_stdout, new_path, new_path_len), new_path_len);

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_RENAME);

		if (hdr.arg != 0)
		{
			errno = hdr.arg;
			return -1;
		}
		return 0;
	}
	else
	{
		return rename(old_path, new_path);
	}
}

static void
fio_rename_impl(char const* old_path, const char* new_path, int out)
{
	fio_header hdr = {
		.cop = FIO_RENAME,
		.handle = -1,
		.size = 0,
		.arg = 0,
	};

	if (rename(old_path, new_path) != 0)
		hdr.arg = errno;

	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
}

/* Sync file to disk */
int
fio_sync(fio_location location, const char* path)
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

		fd = open(path, O_WRONLY | PG_BINARY, FILE_PERMISSION);
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

enum {
	GET_CRC32_DECOMPRESS = 1,
	GET_CRC32_MISSING_OK = 2,
	GET_CRC32_TRUNCATED  = 4
};

/* Get crc32 of file */
static pg_crc32
fio_get_crc32_ex(fio_location location, const char *file_path,
			  bool decompress, bool missing_ok, bool truncated)
{
	if (decompress && truncated)
		elog(ERROR, "Could not calculate CRC for compressed truncated file");

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
			hdr.arg = GET_CRC32_DECOMPRESS;
		if (missing_ok)
			hdr.arg |= GET_CRC32_MISSING_OK;
		if (truncated)
			hdr.arg |= GET_CRC32_TRUNCATED;

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, file_path, path_len), path_len);
		IO_CHECK(fio_read_all(fio_stdin, &crc, sizeof(crc)), sizeof(crc));

		return crc;
	}
	else
	{
		if (decompress)
			return pgFileGetCRC32Cgz(file_path, missing_ok);
		else if (truncated)
			return pgFileGetCRC32CTruncated(file_path, missing_ok);
		else
			return pgFileGetCRC32C(file_path, missing_ok);
	}
}

/*
 * Remove file or directory
 * if missing_ok, then ignore ENOENT error
 */
pg_crc32
fio_get_crc32(fio_location location, const char *file_path,
			  bool decompress, bool missing_ok)
{
	return fio_get_crc32_ex(location, file_path, decompress, missing_ok, false);
}

pg_crc32
fio_get_crc32_truncated(fio_location location, const char *file_path,
						bool missing_ok)
{
	return fio_get_crc32_ex(location, file_path, false, missing_ok, true);
}

/* Remove file */
int
fio_remove(fio_location location, const char* path, bool missing_ok)
{
	int result = 0;

	if (fio_is_remote(location))
	{
		fio_header hdr = {
			.cop = FIO_REMOVE,
			.handle = -1,
			.size = strlen(path) + 1,
			.arg = missing_ok ? 1 : 0,
		};

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_REMOVE);

		if (hdr.arg != 0)
		{
			errno = hdr.arg;
			result = -1;
		}
	}
	else
	{
		if (remove_file_or_dir(path) != 0)
		{
			if (!missing_ok || errno != ENOENT)
				result = -1;
		}
	}
	return result;
}

static void
fio_remove_impl(const char* path, bool missing_ok, int out)
{
	fio_header hdr = {
		.cop = FIO_REMOVE,
		.handle = -1,
		.size = 0,
		.arg = 0,
	};

	if (remove_file_or_dir(path) != 0)
	{
		if (!missing_ok || errno != ENOENT)
			hdr.arg = errno;
	}

	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
}

/*
 * Create directory, also create parent directories if necessary.
 * In strict mode treat already existing directory as error.
 * Return values:
 *  0 - ok
 * -1 - error (check errno)
 */
static int
dir_create_dir(const char *dir, mode_t mode, bool strict)
{
	char		parent[MAXPGPATH];

	strncpy(parent, dir, MAXPGPATH);
	get_parent_directory(parent);

	/* Create parent first */
	if (access(parent, F_OK) == -1)
		dir_create_dir(parent, mode, false);

	/* Create directory */
	if (mkdir(dir, mode) == -1)
	{
		if (errno == EEXIST && !strict)	/* already exist */
			return 0;
		return -1;
	}

	return 0;
}

/*
 * Executed by remote agent.
 */
static void
fio_mkdir_impl(const char* path, int mode, bool strict, int out)
{
	fio_header hdr = {
		.cop = FIO_MKDIR,
		.handle = -1,
		.size = 0,
		.arg = 0,
	};

	if (dir_create_dir(path, mode, strict) != 0)
		hdr.arg = errno;

	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
}

/* Change file mode */
int
fio_chmod(fio_location location, const char* path, int mode)
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
	bool     eof;
	Bytef    buf[ZLIB_BUFFER_SIZE];
} fioGZFile;

/* On error returns NULL and errno should be checked */
gzFile
fio_gzopen(fio_location location, const char* path, const char* mode, int level)
{
	int rc;

	if (strchr(mode, 'w') != NULL) /* compress */
	{
		Assert(false);
		elog(ERROR, "fio_gzopen(\"wb\") is not implemented");
	}

	if (fio_is_remote(location))
	{
		fioGZFile* gz = (fioGZFile*) pgut_malloc(sizeof(fioGZFile));
		memset(&gz->strm, 0, sizeof(gz->strm));
		gz->eof = 0;
		gz->errnum = Z_OK;
		gz->strm.next_in = gz->buf;
		gz->strm.avail_in = ZLIB_BUFFER_SIZE;
		rc = inflateInit2(&gz->strm, 15 + 16);
		gz->strm.avail_in = 0;
		if (rc == Z_OK)
		{
			gz->fd = fio_open(location, path, O_RDONLY | PG_BINARY);
			if (gz->fd < 0)
			{
				free(gz);
				return NULL;
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
fio_gzclose(gzFile f)
{
	if ((size_t)f & FIO_GZ_REMOTE_MARKER)
	{
		fioGZFile* gz = (fioGZFile*)((size_t)f - FIO_GZ_REMOTE_MARKER);
		int rc;
		inflateEnd(&gz->strm);
		rc = fio_close(gz->fd);
		free(gz);
		return rc;
	}
	else
	{
		return gzclose(f);
	}
}

const char*
fio_gzerror(gzFile f, int *errnum)
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

z_off_t
fio_gzseek(gzFile f, z_off_t offset, int whence)
{
	Assert(!((size_t)f & FIO_GZ_REMOTE_MARKER));
	return gzseek(f, offset, whence);
}


#endif

/* Send file content
 * Note: it should not be used for large files.
 */
static void
fio_load_file(int out, const char* path)
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

static void
fio_send_pio_err(int out, err_i err)
{
	const char *err_msg = $errmsg(err);
	fio_header hdr = {.cop = FIO_PIO_ERROR, .size = strlen(err_msg) + 1, .arg = getErrno(err)};

	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
	IO_CHECK(fio_write_all(out, err_msg, hdr.size), hdr.size);

	/* We also need to send source location and all the KVs */
}

static err_i
fio_receive_pio_err(fio_header *hdr)
{
	int pio_errno = hdr->arg;
	char *err_msg = pg_malloc(hdr->size);

	IO_CHECK(fio_read_all(fio_stdin, err_msg, hdr->size), hdr->size);

	return $syserr(pio_errno, err_msg);
}

static void
fio_iterate_pages_impl(pioDBDrive_i drive, int out, const char *path,
						datapagemap_t pagemap,
						fio_iterate_pages_request *params)
{
	pioPagesIterator_i	pages;
	err_i				err = $noerr();
	fio_header			hdr = {.cop=FIO_ITERATE_DATA};
	BlockNumber			finalN;

	pages = $i(pioIteratePages, drive,
			   .path      = path,
			   .segno     = params->segno,
			   .pagemap   = pagemap,
			   .start_lsn = params->start_lsn,
			   .calg      = params->calg,
			   .clevel    = params->clevel,
			   .checksum_version = params->checksum_version,
			   .just_validate    = params->just_validate,
			   .err		  = &err);

	if ($haserr(err))
	{
		fio_send_pio_err(out, err);
		return;
	}
	ft_strbuf_t req = ft_strbuf_zero();
	while (true)
	{
		PageIteratorValue value;

		err_i err = $i(pioNextPage, pages, &value);
		if ($haserr(err)) {
			fio_send_pio_err(out, err);
			return;
		}
		if (value.page_result == PageIsTruncated)
			break;

		//send page + state
		size_t value_size = sizeof(PageIteratorValue) - BLCKSZ + value.compressed_size;

		hdr.size = value_size;

		ft_strbuf_reset_for_reuse(&req);
		ft_strbuf_catbytes(&req, ft_bytes(&hdr, sizeof(hdr)));
		ft_strbuf_catbytes(&req, ft_bytes(&value, value_size));

		IO_CHECK(fio_write_all(out, req.ptr, req.len), req.len);
	}

	ft_strbuf_reset_for_reuse(&req);

	finalN = $i(pioFinalPageN, pages);
	hdr = (fio_header){.cop = FIO_ITERATE_EOF, .size = sizeof(finalN)};
	ft_strbuf_catbytes(&req, ft_bytes(&hdr, sizeof(hdr)));
	ft_strbuf_catbytes(&req, ft_bytes(&finalN, sizeof(finalN)));

	IO_CHECK(fio_write_all(out, req.ptr, req.len), req.len);

	ft_strbuf_free(&req);
}

typedef struct send_file_state {
	bool		calc_crc;
	uint32_t	crc;
	int64_t		read_size;
	int64_t		write_size;
} send_file_state;

/* find page border of all-zero tail */
static size_t
find_zero_tail(char *buf, size_t len)
{
	size_t i, l;
	size_t granul = sizeof(zerobuf);

	if (len == 0)
		return 0;

	/* fast check for last bytes */
	l = Min(len, PAGE_ZEROSEARCH_FINE_GRANULARITY);
	i = len - l;
	if (memcmp(buf + i, zerobuf, l) != 0)
		return len;

	/* coarse search for zero tail */
	i = (len-1) & ~(granul-1);
	l = len - i;
	for (;;)
	{
		if (memcmp(buf+i, zerobuf, l) != 0)
		{
			i += l;
			break;
		}
		if (i == 0)
			break;
		i -= granul;
		l = granul;
	}

	len = i;
	/* search zero tail with finer granularity */
	for (granul = sizeof(zerobuf)/2;
		 len > 0 && granul >= PAGE_ZEROSEARCH_FINE_GRANULARITY;
		 granul /= 2)
	{
		if (granul > l)
			continue;
		i = (len-1) & ~(granul-1);
		l = len - i;
		if (memcmp(buf+i, zerobuf, l) == 0)
			len = i;
	}

	return len;
}

static void
fio_send_file_crc(send_file_state* st, char *buf, size_t len)
{
	int64_t 	write_size;

	if (!st->calc_crc)
		return;

	write_size = st->write_size;
	while (st->read_size > write_size)
	{
		size_t	crc_len = Min(st->read_size - write_size, sizeof(zerobuf));
		COMP_CRC32C(st->crc, zerobuf, crc_len);
		write_size += crc_len;
	}

	if (len > 0)
		COMP_CRC32C(st->crc, buf, len);
}

static bool
fio_send_file_write(FILE* out, send_file_state* st, char *buf, size_t len)
{
	if (len == 0)
		return true;

	if (st->read_size > st->write_size &&
		fseeko(out, st->read_size, SEEK_SET) != 0)
	{
		return false;
	}

	if (fwrite(buf, 1, len, out) != len)
	{
		return false;
	}

	st->read_size += len;
	st->write_size = st->read_size;

	return true;
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
int
fio_send_file(const char *from_fullpath, FILE* out, bool cut_zero_tail,
												pgFile *file, char **errormsg)
{
	fio_header hdr;
	int exit_code = SEND_OK;
	size_t path_len = strlen(from_fullpath) + 1;
	char *buf = pgut_malloc(CHUNK_SIZE);    /* buffer */
	send_file_state st = {false, 0, 0, 0};

	memset(&hdr, 0, sizeof(hdr));

	if (file)
	{
		st.calc_crc = true;
		st.crc = file->crc;
	}

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
			if (st.write_size < st.read_size)
			{
				if (!cut_zero_tail)
				{
					/*
					 * We still need to calc crc for zero tail.
					 */
					fio_send_file_crc(&st, NULL, 0);

					/*
					 * Let's write single zero byte to the end of file to restore
					 * logical size.
					 * Well, it would be better to use ftruncate here actually,
					 * but then we need to change interface.
					 */
					st.read_size -= 1;
					buf[0] = 0;
					if (!fio_send_file_write(out, &st, buf, 1))
					{
						exit_code = WRITE_FAILED;
						break;
					}
				}
			}

			if (file)
			{
				file->crc = st.crc;
				file->read_size = st.read_size;
				file->write_size = st.write_size;
			}
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
			fio_send_file_crc(&st, buf, hdr.size);
			if (!fio_send_file_write(out, &st, buf, hdr.size))
			{
				exit_code = WRITE_FAILED;
				break;
			}
		}
		else if (hdr.cop == FIO_PAGE_ZERO)
		{
			Assert(hdr.size == 0);
			Assert(hdr.arg <= CHUNK_SIZE);

			/*
			 * We have received a chunk of zero data, lets just think we
			 * wrote it.
			 */
			st.read_size += hdr.arg;
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

int
fio_send_file_local(const char *from_fullpath, FILE* out, bool cut_zero_tail,
					pgFile *file, char **errormsg)
{
	FILE* in;
	char* buf;
	size_t read_len, non_zero_len;
	int exit_code = SEND_OK;
	send_file_state st = {false, 0, 0, 0};

	if (file)
	{
		st.calc_crc = true;
		st.crc = file->crc;
	}

	/* open source file for read */
	in = fopen(from_fullpath, PG_BINARY_R);
	if (in == NULL)
	{
		/* maybe deleted, it's not error in case of backup */
		if (errno == ENOENT)
			return FILE_MISSING;


		*errormsg = psprintf("Cannot open file \"%s\": %s", from_fullpath,
							 strerror(errno));
		return OPEN_FAILED;
	}

	/* disable stdio buffering for local input/output files to avoid triple buffering */
	setvbuf(in, NULL, _IONBF, BUFSIZ);
	setvbuf(out, NULL, _IONBF, BUFSIZ);

	/* allocate 64kB buffer */
	buf = pgut_malloc(CHUNK_SIZE);

	/* copy content and calc CRC */
	for (;;)
	{
		read_len = fread(buf, 1, CHUNK_SIZE, in);

		if (ferror(in))
		{
			*errormsg = psprintf("Cannot read from file \"%s\": %s",
								 from_fullpath, strerror(errno));
			exit_code = READ_FAILED;
			goto cleanup;
		}

		if (read_len > 0)
		{
			non_zero_len = find_zero_tail(buf, read_len);
			/*
			 * It is dirty trick to silence warnings in CFS GC process:
			 * backup at least cfs header size bytes.
			 */
			if (st.read_size + non_zero_len < PAGE_ZEROSEARCH_FINE_GRANULARITY &&
				st.read_size + read_len > 0)
			{
				non_zero_len = Min(PAGE_ZEROSEARCH_FINE_GRANULARITY,
								   st.read_size + read_len);
				non_zero_len -= st.read_size;
			}
			if (non_zero_len > 0)
			{
				fio_send_file_crc(&st, buf, non_zero_len);
				if (!fio_send_file_write(out, &st, buf, non_zero_len))
				{
					exit_code = WRITE_FAILED;
					goto cleanup;
				}
			}
			if (non_zero_len < read_len)
			{
				/* Just pretend we wrote it. */
				st.read_size += read_len - non_zero_len;
			}
		}

		if (feof(in))
			break;
	}

	if (st.write_size < st.read_size)
	{
		if (!cut_zero_tail)
		{
			/*
			 * We still need to calc crc for zero tail.
			 */
			fio_send_file_crc(&st, NULL, 0);

			/*
			 * Let's write single zero byte to the end of file to restore
			 * logical size.
			 * Well, it would be better to use ftruncate here actually,
			 * but then we need to change interface.
			 */
			st.read_size -= 1;
			buf[0] = 0;
			if (!fio_send_file_write(out, &st, buf, 1))
			{
				exit_code = WRITE_FAILED;
				goto cleanup;
			}
		}
	}

	if (file)
	{
		file->crc = st.crc;
		file->read_size = st.read_size;
		file->write_size = st.write_size;
	}

	cleanup:
	free(buf);
	fclose(in);
	return exit_code;
}

/* Send open file content
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
static bool
fio_send_file_content_impl(int fd, int out, const char* path)
{
	fio_header hdr;
	int save_errno;
	char *buf = pgut_malloc(CHUNK_SIZE);
	size_t read_len = 0;
	char *errormsg = NULL;
	int64_t read_size = 0;
	int64_t non_zero_len;

	/* copy content */
	for (;;)
	{
		read_len = fio_read_all(fd, buf, CHUNK_SIZE);

		/* report error */
		if (read_len < 0)
		{
			save_errno = errno;
			hdr.cop = FIO_ERROR;
			errormsg = pgut_malloc(ERRMSG_MAX_LEN);
			hdr.arg = READ_FAILED;
			/* Construct the error message */
			snprintf(errormsg, ERRMSG_MAX_LEN, "Cannot read from file '%s': %s",
					 path, strerror(save_errno));
			hdr.size = strlen(errormsg) + 1;
			/* send header and message */
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			IO_CHECK(fio_write_all(out, errormsg, hdr.size), hdr.size);

			free(errormsg);
			free(buf);

			return false;
		}

		if (read_len == 0)
			break;

		/* send chunk */
		non_zero_len = find_zero_tail(buf, read_len);
		/*
		 * It is dirty trick to silence warnings in CFS GC process:
		 * backup at least cfs header size bytes.
		 */
		if (read_size + non_zero_len < PAGE_ZEROSEARCH_FINE_GRANULARITY &&
			read_size + read_len > 0)
		{
			non_zero_len = Min(PAGE_ZEROSEARCH_FINE_GRANULARITY,
							   read_size + read_len);
			non_zero_len -= read_size;
		}

		if (non_zero_len > 0)
		{
			hdr.cop = FIO_PAGE;
			hdr.size = non_zero_len;
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			IO_CHECK(fio_write_all(out, buf, non_zero_len), non_zero_len);
		}

		if (read_len > 0)
		{
			/* send chunk */
			hdr.cop = FIO_PAGE_ZERO;
			hdr.size = 0;
			hdr.arg = read_len - non_zero_len;
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
		}

		read_size += read_len;
	}

	/* we are done, send eof */
	hdr.cop = FIO_SEND_FILE_EOF;
	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));

	free(buf);
	return true;
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
static void
fio_send_file_impl(int out, const char* path)
{
	int        fd;
    int        save_errno;
    fio_header hdr;
	char      *errormsg = NULL;

	/* open source file for read */
	/* TODO: check that file is regular file */
	fd = open(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
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
            save_errno = errno;
			hdr.arg = OPEN_FAILED;
			errormsg = pgut_malloc(ERRMSG_MAX_LEN);
			/* Construct the error message */
			snprintf(errormsg, ERRMSG_MAX_LEN, "Cannot open file '%s': %s",
                     path, strerror(save_errno));
			hdr.size = strlen(errormsg) + 1;
		}

		/* send header and message */
		IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
		if (errormsg)
        {
            IO_CHECK(fio_write_all(out, errormsg, hdr.size), hdr.size);
            free(errormsg);
        }

		return;
	}

    fio_send_file_content_impl(fd, out, path);

    close(fd);
}

/*
 * Read the local file to compute its CRC.
 * We cannot make decision about file decompression because
 * user may ask to backup already compressed files and we should be
 * obvious about it.
 */
pg_crc32
pgFileGetCRC32C(const char *file_path, bool missing_ok)
{
	FILE	   *fp;
	pg_crc32	crc = 0;
	char	   *buf;
	size_t		len = 0;

	INIT_CRC32C(crc);

	/* open file in binary read mode */
	fp = fopen(file_path, PG_BINARY_R);
	if (fp == NULL)
	{
		if (missing_ok && errno == ENOENT)
		{
			FIN_CRC32C(crc);
			return crc;
		}

		elog(ERROR, "Cannot open file \"%s\": %s",
			 file_path, strerror(errno));
	}

	/* disable stdio buffering */
	setvbuf(fp, NULL, _IONBF, BUFSIZ);
	buf = pgut_malloc(STDIO_BUFSIZE);

	/* calc CRC of file */
	do
	{
		if (interrupted)
			elog(ERROR, "interrupted during CRC calculation");

		len = fread(buf, 1, STDIO_BUFSIZE, fp);

		if (ferror(fp))
			elog(ERROR, "Cannot read \"%s\": %s", file_path, strerror(errno));

		COMP_CRC32C(crc, buf, len);
	}
	while (!feof(fp));

	FIN_CRC32C(crc);
	fclose(fp);
	pg_free(buf);

	return crc;
}

/*
 * Read the local file to compute CRC for it extened to real_size.
 */
pg_crc32
pgFileGetCRC32CTruncated(const char *file_path, bool missing_ok)
{
	FILE	   *fp;
	char	   *buf;
	size_t		len = 0;
	size_t		non_zero_len;
	send_file_state st = {true, 0, 0, 0};

	INIT_CRC32C(st.crc);

	/* open file in binary read mode */
	fp = fopen(file_path, PG_BINARY_R);
	if (fp == NULL)
	{
		if (missing_ok && errno == ENOENT)
		{
			FIN_CRC32C(st.crc);
			return st.crc;
		}

		elog(ERROR, "Cannot open file \"%s\": %s",
			 file_path, strerror(errno));
	}

	/* disable stdio buffering */
	setvbuf(fp, NULL, _IONBF, BUFSIZ);
	buf = pgut_malloc(CHUNK_SIZE);

	/* calc CRC of file */
	do
	{
		if (interrupted)
			elog(ERROR, "interrupted during CRC calculation");

		len = fread(buf, 1, STDIO_BUFSIZE, fp);

		if (ferror(fp))
			elog(ERROR, "Cannot read \"%s\": %s", file_path, strerror(errno));

		non_zero_len = find_zero_tail(buf, len);
		/* same trick as in fio_send_file */
		if (st.read_size + non_zero_len < PAGE_ZEROSEARCH_FINE_GRANULARITY &&
			st.read_size + len > 0)
		{
			non_zero_len = Min(PAGE_ZEROSEARCH_FINE_GRANULARITY,
							   st.read_size + len);
			non_zero_len -= st.read_size;
		}
		if (non_zero_len)
		{
			fio_send_file_crc(&st, buf, non_zero_len);
			st.write_size += st.read_size + non_zero_len;
		}
		st.read_size += len;

	} while (!feof(fp));

	FIN_CRC32C(st.crc);
	fclose(fp);
	pg_free(buf);

	return st.crc;
}

/*
 * Read the local file to compute its CRC.
 * We cannot make decision about file decompression because
 * user may ask to backup already compressed files and we should be
 * obvious about it.
 */
pg_crc32
pgFileGetCRC32Cgz(const char *file_path, bool missing_ok)
{
	gzFile fp;
	pg_crc32 crc = 0;
	int len = 0;
	int err;
	char *buf;

	INIT_CRC32C(crc);

	/* open file in binary read mode */
	fp = gzopen(file_path, PG_BINARY_R);
	if (fp == NULL)
	{
		if (missing_ok && errno == ENOENT)
		{
			FIN_CRC32C(crc);
			return crc;
		}

		elog(ERROR, "Cannot open file \"%s\": %s",
			 file_path, strerror(errno));
	}

	buf = pgut_malloc(STDIO_BUFSIZE);

	/* calc CRC of file */
	for (;;)
	{
		if (interrupted)
			elog(ERROR, "interrupted during CRC calculation");

		len = gzread(fp, buf, STDIO_BUFSIZE);

		if (len <= 0)
		{
			/* we either run into eof or error */
			if (gzeof(fp))
				break;
			else
			{
				const char *err_str = NULL;

				err_str = gzerror(fp, &err);
				elog(ERROR, "Cannot read from compressed file %s", err_str);
			}
		}

		/* update CRC */
		COMP_CRC32C(crc, buf, len);
	}

	FIN_CRC32C(crc);
	gzclose(fp);
	pg_free(buf);

	return crc;
}

#if PG_VERSION_NUM < 120000
/*
 * Read the local file to compute its CRC using traditional algorithm.
 * (*_TRADITIONAL_CRC32 macros)
 * This was used only in version 2.0.22--2.0.24
 * And never used for PG >= 12
 * To be removed with end of PG-11 support
 */
pg_crc32
pgFileGetCRC32(const char *file_path, bool missing_ok)
{
	FILE	   *fp;
	pg_crc32	crc = 0;
	char	   *buf;
	size_t		len = 0;

	INIT_TRADITIONAL_CRC32(crc);

	/* open file in binary read mode */
	fp = fopen(file_path, PG_BINARY_R);
	if (fp == NULL)
	{
		if (missing_ok && errno == ENOENT)
		{
			FIN_TRADITIONAL_CRC32(crc);
			return crc;
		}

		elog(ERROR, "Cannot open file \"%s\": %s",
			file_path, strerror(errno));
	}

	/* disable stdio buffering */
	setvbuf(fp, NULL, _IONBF, BUFSIZ);
	buf = pgut_malloc(STDIO_BUFSIZE);

	/* calc CRC of file */
	do
	{
		if (interrupted)
			elog(ERROR, "interrupted during CRC calculation");

		len = fread(buf, 1, STDIO_BUFSIZE, fp);

		if (ferror(fp))
			elog(ERROR, "Cannot read \"%s\": %s", file_path, strerror(errno));

		COMP_TRADITIONAL_CRC32(crc, buf, len);
	}
	while (!feof(fp));

	FIN_TRADITIONAL_CRC32(crc);
	fclose(fp);
	pg_free(buf);

	return crc;
}
#endif /* PG_VERSION_NUM < 120000 */

void db_list_dir(parray *files, const char *root, bool handle_tablespaces,
					bool backup_logs, int external_dir_num) {
	pioDrive_i drive = pioDriveForLocation(FIO_DB_HOST);
	$i(pioListDir, drive, .files = files, .root = root, .handle_tablespaces = handle_tablespaces,
			.symlink_and_hidden = true, .backup_logs = backup_logs, .skip_hidden = true,
			.external_dir_num = external_dir_num);
}

void backup_list_dir(parray *files, const char *root) {
	pioDrive_i drive = pioDriveForLocation(FIO_BACKUP_HOST);
	$i(pioListDir, drive, .files = files, .root = root, .handle_tablespaces = false,
			.symlink_and_hidden = false, .backup_logs = false, .skip_hidden = false,
			.external_dir_num = 0);
}

/*
 * WARNING! this function is not paired with fio_remove_dir
 * because there is no such function. Instead, it is paired
 * with pioRemoteDrive_pioRemoveDir, see PBCKP-234 for further details
 */
static void
fio_remove_dir_impl(int out, char* buf) {
    fio_remove_dir_request  *frdr = (fio_remove_dir_request *)buf;
    pioDrive_i drive = pioDriveForLocation(FIO_LOCAL_HOST);

    // In an essence this all is just a wrapper for a pioRemoveDir call on a local drive
    $i(pioRemoveDir, drive, .root = frdr->path, .root_as_well = frdr->root_as_well);

    fio_header hdr;
    hdr.cop = FIO_REMOVE_DIR;
    hdr.arg = 0;

    IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
}

/*
 * List files, symbolic links and directories in the directory "root" and add
 * pgFile objects to "files".  We add "root" to "files" if add_root is true.
 *
 * When follow_symlink is true, symbolic link is ignored and only file or
 * directory linked to will be listed.
 *
 * TODO: make it strictly local
 */
static void
dir_list_file(parray *files, const char *root, bool handle_tablespaces, bool follow_symlink,
			  bool backup_logs, bool skip_hidden, int external_dir_num, pioDBDrive_i drive)
{
	pgFile	   *file;

	Assert(!$i(pioIsRemote, drive));

	file = pgFileNew(root, "", follow_symlink, external_dir_num,
					 $reduce(pioDrive, drive));
	if (file == NULL)
	{
		/* For external directory this is not ok */
		if (external_dir_num > 0)
			elog(ERROR, "External directory is not found: \"%s\"", root);
		else
			return;
	}

	if (file->kind != PIO_KIND_DIRECTORY)
	{
		if (external_dir_num > 0)
			elog(ERROR, " --external-dirs option \"%s\": directory or symbolic link expected",
				 root);
		else
			elog(WARNING, "Skip \"%s\": unexpected file format", root);
		return;
	}

	dir_list_file_internal(files, file, root, handle_tablespaces, follow_symlink,
						   backup_logs, skip_hidden, external_dir_num, drive);

	pgFileFree(file);
}

/*
 * List files in parent->path directory.
 * If "handle_tablespaces" is true, handle recursive tablespaces
 * and the ones located inside pgdata.
 * If "follow_symlink" is true, follow symlinks so that the
 * fio_stat call fetches the info from the file pointed to by the
 * symlink, not from the symlink itself.
 *
 * TODO: should we check for interrupt here ?
 */
static void
dir_list_file_internal(parray *files, pgFile *parent, const char *parent_dir,
					   bool handle_tablespaces, bool follow_symlink, bool backup_logs,
					   bool skip_hidden, int external_dir_num, pioDBDrive_i drive)
{
	DIR			  *dir;
	struct dirent *dent;
	bool in_tablespace = false;

	Assert(!$i(pioIsRemote, drive));

	if (parent->kind != PIO_KIND_DIRECTORY)
		elog(ERROR, "\"%s\" is not a directory", parent_dir);

	in_tablespace = path_is_prefix_of_path(PG_TBLSPC_DIR, parent->rel_path);

	/* Open directory and list contents */
	dir = opendir(parent_dir);
	if (dir == NULL)
	{
		if (errno == ENOENT)
		{
			/* Maybe the directory was removed */
			return;
		}
		elog(ERROR, "Cannot open directory \"%s\": %s",
			 parent_dir, strerror(errno));
	}

	errno = 0;
	while ((dent = readdir(dir)))
	{
		pgFile	   *file;
		char		child[MAXPGPATH];
		char		rel_child[MAXPGPATH];

		join_path_components(child, parent_dir, dent->d_name);
		join_path_components(rel_child, parent->rel_path, dent->d_name);

		file = pgFileNew(child, rel_child, follow_symlink,
						 external_dir_num, $reduce(pioDrive, drive));
		if (file == NULL)
			continue;

		/* Skip entries point current dir or parent dir */
		if (file->kind == PIO_KIND_DIRECTORY &&
			(strcmp(dent->d_name, ".") == 0 || strcmp(dent->d_name, "..") == 0))
		{
			pgFileFree(file);
			continue;
		}

		/* skip hidden files and directories */
		if (skip_hidden && file->name[0] == '.') {
			elog(WARNING, "Skip hidden file: '%s'", child);
			pgFileFree(file);
			continue;
		}

		/*
		 * Add only files, directories and links. Skip sockets and other
		 * unexpected file formats.
		 */
		if (file->kind != PIO_KIND_DIRECTORY && file->kind != PIO_KIND_REGULAR)
		{
			elog(WARNING, "Skip '%s': unexpected file format", child);
			pgFileFree(file);
			continue;
		}

		if(handle_tablespaces) {
			/*
			 * Do not copy tablespaces twice. It may happen if the tablespace is located
			 * inside the PGDATA.
			 */
			if (file->kind == PIO_KIND_DIRECTORY &&
				strcmp(file->name, TABLESPACE_VERSION_DIRECTORY) == 0)
			{
				Oid			tblspcOid;
				char		tmp_rel_path[MAXPGPATH];
				int			sscanf_res;

				/*
				 * Valid path for the tablespace is
				 * pg_tblspc/tblsOid/TABLESPACE_VERSION_DIRECTORY
				 */
				if (!path_is_prefix_of_path(PG_TBLSPC_DIR, file->rel_path))
					continue;
				sscanf_res = sscanf(file->rel_path, PG_TBLSPC_DIR "/%u/%s",
									&tblspcOid, tmp_rel_path);
				if (sscanf_res == 0)
					continue;
			}

			if (in_tablespace) {
				char        tmp_rel_path[MAXPGPATH];
				ssize_t      sscanf_res;

				sscanf_res = sscanf(file->rel_path, PG_TBLSPC_DIR "/%u/%[^/]/%u/",
									&(file->tblspcOid), tmp_rel_path,
									&(file->dbOid));

				/*
				 * We should skip other files and directories rather than
				 * TABLESPACE_VERSION_DIRECTORY, if this is recursive tablespace.
				 */
				if (sscanf_res == 2 && strcmp(tmp_rel_path, TABLESPACE_VERSION_DIRECTORY) != 0)
					continue;
			} else if (path_is_prefix_of_path("global", file->rel_path)) {
				file->tblspcOid = GLOBALTABLESPACE_OID;
			} else if (path_is_prefix_of_path("base", file->rel_path)) {
				file->tblspcOid = DEFAULTTABLESPACE_OID;
				sscanf(file->rel_path, "base/%u/", &(file->dbOid));
			}
		}

		parray_append(files, file);

		/*
		 * If the entry is a directory call dir_list_file_internal()
		 * recursively.
		 */
		if (file->kind == PIO_KIND_DIRECTORY)
			dir_list_file_internal(files, file, child, handle_tablespaces, follow_symlink,
								   backup_logs, skip_hidden, external_dir_num, drive);
	}

	if (errno && errno != ENOENT)
	{
		int			errno_tmp = errno;
		closedir(dir);
		elog(ERROR, "Cannot read directory \"%s\": %s",
			 parent_dir, strerror(errno_tmp));
	}
	closedir(dir);
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
static void
fio_list_dir_impl(int out, char* buf, pioDBDrive_i drive)
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

	dir_list_file(file_files, req->path, req->handle_tablespaces,
				  req->follow_symlink, req->backup_logs, req->skip_hidden,
				  req->external_dir_num, drive);

	/* send information about files to the main process */
	for (i = 0; i < parray_num(file_files); i++)
	{
		fio_pgFile  fio_file;
		pgFile	   *file = (pgFile *) parray_get(file_files, i);

		fio_file.kind = file->kind;
		fio_file.mode = file->mode;
		fio_file.size = file->size;
		fio_file.mtime = file->mtime;
		fio_file.is_datafile = file->is_datafile;
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
	hdr = (fio_header){.cop = FIO_SEND_FILE_EOF};
	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
}

PageState *
fio_get_checksum_map(fio_location location, const char *fullpath, uint32 checksum_version,
					 int n_blocks, XLogRecPtr dest_stop_lsn, BlockNumber segmentno)
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

static void
fio_get_checksum_map_impl(char *buf, int out)
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
fio_get_lsn_map(fio_location location, const char *fullpath,
				uint32 checksum_version, int n_blocks,
				XLogRecPtr shift_lsn, BlockNumber segmentno)
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

static void
fio_get_lsn_map_impl(char *buf, int out)
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
 * Return pid of postmaster process running in given pgdata on local machine.
 * Return 0 if there is none.
 * Return 1 if postmaster.pid is mangled.
 */
static pid_t
local_check_postmaster(const char *pgdata)
{
	FILE  *fp;
	pid_t  pid;
	long long lpid;
	char   pid_file[MAXPGPATH];

	join_path_components(pid_file, pgdata, "postmaster.pid");

	fp = fopen(pid_file, "r");
	if (fp == NULL)
	{
		/* No pid file, acceptable*/
		if (errno == ENOENT)
			return 0;
		else
			elog(ERROR, "Cannot open file \"%s\": %s",
				pid_file, strerror(errno));
	}

	if (fscanf(fp, "%lli", &lpid) == 1)
	{
		pid = lpid;
	}
	else
	{
		/* something is wrong with the file content */
		pid = 1;
	}

	if (pid > 1)
	{
		if (kill(pid, 0) != 0)
		{
			/* process no longer exists */
			if (errno == ESRCH)
				pid = 0;
			else
				elog(ERROR, "Failed to send signal 0 to a process %lld: %s",
						(long long)pid, strerror(errno));
		}
	}

	fclose(fp);
	return pid;
}

/*
 * Go to the remote host and get postmaster pid from file postmaster.pid
 * and check that process is running, if process is running, return its pid number.
 */
pid_t
fio_check_postmaster(fio_location location, const char *pgdata)
{
	if (fio_is_remote(location))
	{
		fio_header hdr = {
			.cop = FIO_CHECK_POSTMASTER,
			.handle = -1,
			.size = strlen(pgdata) + 1,
			.arg = 0,
		};

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_write_all(fio_stdout, pgdata, hdr.size), hdr.size);

		/* receive result */
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
		Assert(hdr.cop == FIO_CHECK_POSTMASTER);

		return hdr.arg;
	}
	else
		return local_check_postmaster(pgdata);
}

static void
fio_check_postmaster_impl(const char *pgdata, int out)
{
	fio_header hdr = {
		.cop = FIO_CHECK_POSTMASTER,
		.handle = -1,
		.size = 0,
		.arg = 0,
	};

	hdr.arg = local_check_postmaster(pgdata);

	/* send arrays of checksums to main process */
	IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
}

/* Execute commands at remote host */
void
fio_communicate(int in, int out)
{
	/*
	 * Map of file and directory descriptors.
	 * The same mapping is used in agent and master process, so we
	 * can use the same index at both sides.
	 */
	int fd[FIO_FDMAX];
	DIR* dir[FIO_FDMAX];

	fobj_t objs[FIO_FDMAX] = {0};
	err_i  async_errs[FIO_FDMAX] = {0};

	struct dirent* entry;
	size_t buf_size = 128*1024;
	char* buf = (char*)pgut_malloc(buf_size);
	fio_header hdr;
	pioDBDrive_i drive;
	pio_stat_t st;
	ft_bytes_t bytes;
	int rc;
	int tmp_fd;
	pg_crc32 crc;
	err_i err = $noerr();

	FOBJ_FUNC_ARP();

	drive = pioDBDriveForLocation(FIO_LOCAL_HOST);

#ifdef WIN32
	SYS_CHECK(setmode(in, _O_BINARY));
	SYS_CHECK(setmode(out, _O_BINARY));
#endif

	/* Main loop until end of processing all master commands */
	while ((rc = fio_read_all(in, &hdr, sizeof hdr)) == sizeof(hdr)) {
		FOBJ_LOOP_ARP();
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
			fd[hdr.handle] = open(buf, hdr.arg, FILE_PERMISSION);
			hdr.arg = fd[hdr.handle] < 0 ? errno : 0;
			hdr.size = 0;
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			break;
		  case FIO_CLOSE: /* Close file */
			fio_close_impl(fd[hdr.handle], out);
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
			errno = 0;
			rc = read(fd[hdr.handle], buf, hdr.arg);
			hdr.cop = FIO_SEND;
			hdr.size = rc > 0 ? rc : 0;
			hdr.arg = rc >= 0 ? 0 : errno;
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
			{
				size_t payload_size = prepare_compatibility_str(buf, buf_size);

				hdr.arg = AGENT_PROTOCOL_VERSION;
				hdr.size = payload_size;

				IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
				IO_CHECK(fio_write_all(out, buf, payload_size), payload_size);
				break;
			}
		  case FIO_STAT: /* Get information about file with specified path */
			hdr.size = sizeof(st);
			st = $i(pioStat, drive, buf, .follow_symlink = hdr.arg != 0,
					 .err = &err);
			hdr.arg = $haserr(err) ? getErrno(err) : 0;
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			IO_CHECK(fio_write_all(out, &st, sizeof(st)), sizeof(st));
			break;
		  case FIO_FILES_ARE_SAME:
			hdr.arg = (int)$i(pioFilesAreSame, drive, buf, buf+strlen(buf)+1);
			hdr.size = 0;
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			break;
		  case FIO_READ_FILE_AT_ONCE:
			bytes = $i(pioReadFile, drive, .path = buf,
					  .binary = hdr.arg != 0, .err = &err);
			if ($haserr(err))
			{
				const char *msg = $errmsg(err);
				hdr.arg = getErrno(err);
				hdr.size = strlen(msg) + 1;
				IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
				IO_CHECK(fio_write_all(out, msg, hdr.size), hdr.size);
			}
			else
			{
				hdr.arg = 0;
				hdr.size = bytes.len;
				IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
				if (bytes.len > 0)
					IO_CHECK(fio_write_all(out, bytes.ptr, bytes.len), bytes.len);
			}
			ft_bytes_free(&bytes);
			break;
		  case FIO_WRITE_FILE_AT_ONCE:
			bytes = ft_bytes(buf, hdr.size);
			ft_bytes_consume(&bytes, strlen(buf)+1);
			err = $i(pioWriteFile, drive, .path = buf,
					 .content = bytes, .binary = hdr.arg);
			if ($haserr(err))
			{
				const char *msg = $errmsg(err);
				hdr.arg = getErrno(err);
				hdr.size = strlen(msg) + 1;
				IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
				IO_CHECK(fio_write_all(out, msg, hdr.size), hdr.size);
			}
			else
			{
				hdr.arg = 0;
				hdr.size = 0;
				IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			}
			break;
		  case FIO_ACCESS: /* Check presence of file with specified name */
			hdr.size = 0;
			hdr.arg = access(buf, hdr.arg) < 0 ? errno  : 0;
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			break;
		  case FIO_RENAME: /* Rename file */
			/* possible buffer overflow */
			fio_rename_impl(buf, buf + strlen(buf) + 1, out);
			break;
		  case FIO_SYMLINK: /* Create symbolic link */
			fio_symlink_impl(buf, buf + strlen(buf) + 1, hdr.arg == 1, out);
			break;
		  case FIO_REMOVE: /* Remove file or directory (TODO: Win32) */
			fio_remove_impl(buf, hdr.arg == 1, out);
			break;
		  case FIO_MKDIR:  /* Create directory */
			fio_mkdir_impl(buf, hdr.arg, hdr.handle == 1, out);
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
			fio_list_dir_impl(out, buf, drive);
			break;
          case FIO_REMOVE_DIR:
            fio_remove_dir_impl(out, buf);
            break;
		  case FIO_SEND_FILE:
			fio_send_file_impl(out, buf);
			break;
          case FIO_SEND_FILE_CONTENT:
            fio_send_file_content_impl(fd[hdr.handle], out, buf);
            break;
		  case FIO_SYNC:
			/* open file and fsync it */
			tmp_fd = open(buf, O_WRONLY | PG_BINARY, FILE_PERMISSION);
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
          case FIO_SYNC_FILE:
            if (fsync(fd[hdr.handle]) == 0)
                hdr.arg = 0;
            else
                hdr.arg = errno;
            IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
            break;
		  case FIO_GET_CRC32:
			Assert((hdr.arg & GET_CRC32_TRUNCATED) == 0 ||
				   (hdr.arg & (GET_CRC32_TRUNCATED|GET_CRC32_DECOMPRESS)) == GET_CRC32_TRUNCATED);
			/* calculate crc32 for a file */
			if ((hdr.arg & GET_CRC32_DECOMPRESS))
				crc = pgFileGetCRC32Cgz(buf, (hdr.arg & GET_CRC32_MISSING_OK) != 0);
			else if ((hdr.arg & GET_CRC32_TRUNCATED))
				crc = pgFileGetCRC32CTruncated(buf, (hdr.arg & GET_CRC32_MISSING_OK) != 0);
			else
				crc = pgFileGetCRC32C(buf, (hdr.arg & GET_CRC32_MISSING_OK) != 0);
			IO_CHECK(fio_write_all(out, &crc, sizeof(crc)), sizeof(crc));
			break;
		  case FIO_GET_CHECKSUM_MAP:
			fio_get_checksum_map_impl(buf, out);
			break;
		  case FIO_GET_LSN_MAP:
			fio_get_lsn_map_impl(buf, out);
			break;
		  case FIO_CHECK_POSTMASTER:
			fio_check_postmaster_impl(buf, out);
			break;
		  case FIO_DISCONNECT:
			hdr.cop = FIO_DISCONNECTED;
			IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			free(buf);
			return;
		  case FIO_GET_ASYNC_ERROR:
			fio_get_async_error_impl(out);
			break;
		  case FIO_READLINK: /* Read content of a symbolic link */
			{
				/*
				 * We need a buf for a arguments and for a result at the same time
				 * hdr.size = strlen(symlink_name) + 1
				 * hdr.arg = bufsize for a answer (symlink content)
				 */
				size_t filename_size = (size_t)hdr.size;
				if (filename_size + hdr.arg > buf_size) {
					buf_size = hdr.arg;
					buf = (char*)realloc(buf, buf_size);
				}
				rc = readlink(buf, buf + filename_size, hdr.arg);
				hdr.cop = FIO_READLINK;
				hdr.size = rc > 0 ? rc : 0;
				IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
				if (hdr.size != 0)
					IO_CHECK(fio_write_all(out, buf + filename_size, hdr.size), hdr.size);
			}
			break;
		case FIO_ITERATE_PAGES:
			{
				ft_bytes_t bytes = {.ptr = buf, .len = hdr.size};
				fio_iterate_pages_request *params;
				char         *from_fullpath;
				datapagemap_t pagemap;

				params = (fio_iterate_pages_request*)bytes.ptr;
				ft_bytes_consume(&bytes, sizeof(*params));

				pagemap.bitmapsize = params->pagemaplen;
				pagemap.bitmap = bytes.ptr;
				ft_bytes_consume(&bytes, pagemap.bitmapsize);

				from_fullpath = bytes.ptr;

				fio_iterate_pages_impl(drive, out, from_fullpath, pagemap, params);
			}
			break;
		case PIO_OPEN_REWRITE:
		{
			struct fio_req_open_rewrite *req = (void*)buf;
			const char *path = buf + sizeof(*req);
			pioWriteCloser_i fl;
			err_i err;

			ft_assert(hdr.handle >= 0);
			ft_assert(objs[hdr.handle] == NULL);

			fl = $i(pioOpenRewrite, drive, .path = path,
					.permissions = req->permissions,
					.binary = req->binary,
					.use_temp = req->use_temp,
					.err = &err);
			if ($haserr(err))
				fio_send_pio_err(out, err);
			else
			{
				hdr.size = 0;
				IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
				objs[hdr.handle] = $ref(fl.self);
			}
			break;
		}
		case PIO_OPEN_WRITE:
		{
			struct fio_req_open_write *req = (void*)buf;
			const char *path = buf + sizeof(*req);
			pioDBWriter_i fl;
			err_i err;

			ft_assert(hdr.handle >= 0);
			ft_assert(objs[hdr.handle] == NULL);

			fl = $i(pioOpenWrite, drive, .path = path,
					.permissions = req->permissions,
					.exclusive = req->exclusive,
					.err = &err);
			if ($haserr(err))
				fio_send_pio_err(out, err);
			else
			{
				hdr.size = 0;
				IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
				objs[hdr.handle] = $ref(fl.self);
			}
			break;
		}
		case PIO_WRITE_ASYNC:
		{
			err_i  err;

			ft_assert(hdr.handle >= 0);
			ft_assert(objs[hdr.handle] != NULL);

			$(pioWrite, objs[hdr.handle], ft_bytes(buf, hdr.size),
						.err = &err);
			if ($haserr(err))
				$iset(&async_errs[hdr.handle], err);
			break;
		}
		case PIO_SEEK:
		{
			err_i  err;
			uint64_t offs;

			ft_assert(hdr.handle >= 0);
			ft_assert(objs[hdr.handle] != NULL);
			ft_assert(hdr.size == sizeof(uint64_t));

			memcpy(&offs, buf, sizeof(uint64_t));

			err = $(pioSeek, objs[hdr.handle], offs);
			if ($haserr(err))
				$iset(&async_errs[hdr.handle], err);
			break;
		}
		case PIO_GET_ASYNC_ERROR:
		{
			ft_assert(hdr.handle >= 0);
			ft_assert(objs[hdr.handle] != NULL);
			ft_assert(hdr.size == 0);

			if ($haserr(async_errs[hdr.handle]))
			{
				fio_send_pio_err(out, async_errs[hdr.handle]);
				$idel(&async_errs[hdr.handle]);
			}
			else
				IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			break;
		}
		case PIO_CLOSE:
		{
			err_i err;

			ft_assert(hdr.handle >= 0);
			ft_assert(objs[hdr.handle] != NULL);
			ft_assert(hdr.size == 1);

			err = $(pioClose, objs[hdr.handle], .sync = buf[0]);
			err = fobj_err_combine(err, async_errs[hdr.handle]);
			if ($haserr(err))
			{
				fio_send_pio_err(out, err);
			}
			else
			{
				hdr.size = 0;
				IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			}
			$del(&objs[hdr.handle]);
			$idel(&async_errs[hdr.handle]);
			break;
		}
		case PIO_DISPOSE:
		{
			ft_assert(hdr.handle >= 0);
			ft_assert(objs[hdr.handle] != NULL);
			ft_assert(hdr.size == 0);

			$del(&objs[hdr.handle]);
			$idel(&async_errs[hdr.handle]);
			break;
		}
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

// CLASSES

typedef struct pioLocalDrive
{
} pioLocalDrive;
#define kls__pioLocalDrive	iface__pioDBDrive, iface(pioDBDrive)
fobj_klass(pioLocalDrive);

typedef struct pioRemoteDrive
{
} pioRemoteDrive;
#define kls__pioRemoteDrive	iface__pioDBDrive, iface(pioDBDrive)
fobj_klass(pioRemoteDrive);

typedef struct pioFile
{
    const char *path;
    int		flags;
    bool	closed;
} pioFile;
#define kls__pioFile	mth(fobjDispose)
fobj_klass(pioFile);

typedef struct pioLocalFile
{
    pioFile	p;
    int		fd;
} pioLocalFile;
#define kls__pioLocalFile	iface__pioFile, iface(pioFile)
fobj_klass(pioLocalFile);

typedef struct pioLocalWriteFile
{
	ft_str_t path;
	ft_str_t path_tmp;
	FILE*	 fl;
	ft_bytes_t buf;
	bool     use_temp;
	bool     renamed;
} pioLocalWriteFile;
#define kls__pioLocalWriteFile	iface__pioDBWriter, mth(fobjDispose), \
								iface(pioWriteCloser, pioDBWriter)
fobj_klass(pioLocalWriteFile);

typedef struct pioRemoteFile
{
    pioFile	p;
    int		handle;
    bool    asyncMode;
    bool    asyncEof;
    bool	didAsync;
    err_i asyncError;
    /* chunks size is CHUNK_SIZE */
    void*   asyncChunk;
    ft_bytes_t  chunkRest;
} pioRemoteFile;
#define kls__pioRemoteFile	iface__pioFile, iface(pioFile), \
                            mth(pioSetAsync, pioAsyncRead, pioAsyncWrite, pioAsyncError)
fobj_klass(pioRemoteFile);

typedef struct pioRemoteWriteFile {
	ft_str_t	path;
	int			handle;
} pioRemoteWriteFile;
#define kls__pioRemoteWriteFile	iface__pioDBWriter, mth(fobjDispose), \
								iface(pioWriteCloser, pioDBWriter)
fobj_klass(pioRemoteWriteFile);

typedef struct pioReadFilter {
    pioRead_i	wrapped;
    pioFilter_i	filter;
    pioFltInPlace_i inplace;
    char*		buffer;
    size_t		len;
    size_t		capa;
    bool        eof;
    bool        finished;
} pioReadFilter;
#define kls__pioReadFilter	mth(pioRead, pioClose)
fobj_klass(pioReadFilter);

typedef struct pioWriteFilter {
    pioWriteFlush_i	wrapped;
    pioFilter_i		filter;
    pioFltInPlace_i inplace;
    char*			buffer;
    size_t			capa;
    bool			finished;
} pioWriteFilter;
#define kls__pioWriteFilter iface__pioWriteFlush, iface(pioWriteFlush), \
                            mth(pioClose)
fobj_klass(pioWriteFilter);

#ifdef HAVE_LIBZ
typedef struct pioGZCompress {
    z_stream    strm;
    bool        finished;
} pioGZCompress;

typedef struct pioGZDecompress {
    z_stream    strm;
    bool        eof;
    bool        finished;
    bool        ignoreTruncate;
} pioGZDecompress;

#define kls__pioGZCompress	iface__pioFilter, mth(fobjDispose), iface(pioFilter)
fobj_klass(pioGZCompress);
#define kls__pioGZDecompress	iface__pioFilter, mth(fobjDispose), iface(pioFilter)
fobj_klass(pioGZDecompress);
#endif

/* CRC32 counter */
typedef struct pioDevNull
{
} pioDevNull;

#define kls__pioDevNull iface__pioWriteFlush, iface(pioWriteFlush)
fobj_klass(pioDevNull);

typedef struct pioCRC32Counter
{
	pg_crc32		crc;
	int64_t			size;
} pioCRC32Counter;

static pioDBDrive_i localDrive;
static pioDBDrive_i remoteDrive;

pioDrive_i
pioDriveForLocation(fio_location loc)
{
    if (fio_is_remote(loc))
        return $reduce(pioDrive, remoteDrive);
    else
        return $reduce(pioDrive, localDrive);
}

pioDBDrive_i
pioDBDriveForLocation(fio_location loc)
{
	if (fio_is_remote(loc))
		return remoteDrive;
	else
		return localDrive;
}

/* Base physical file type */

static void
pioFile_fobjDispose(VSelf)
{
    Self(pioFile);

    ft_assert(self->closed, "File \"%s\" is disposing unclosed", self->path);
    ft_free((void*)self->path);
    self->path = NULL;
}

static bool
common_pioExists(fobj_t self, path_t path, pio_file_kind_e expected_kind, err_i *err)
{
    pio_stat_t buf;
    fobj_reset_err(err);

    /* follow symlink ? */
    buf = $(pioStat, self, path, true, err);
    if (getErrno(*err) == ENOENT)
    {
        *err = $noerr();
        return false;
    }
    if ($noerr(*err) && buf.pst_kind != expected_kind)
        *err = $err(SysErr, "File {path:q} is not of an expected kind", path(path));
    if ($haserr(*err)) {
        *err = $syserr(getErrno(*err), "Could not check file existance: {cause:$M}",
					   cause((*err).self));
    }
    return $noerr(*err);
}

/* LOCAL DRIVE */

static pioFile_i
pioLocalDrive_pioOpen(VSelf, path_t path, int flags,
                      int permissions, err_i *err)
{
    int	fd;
    fobj_reset_err(err);
    fobj_t file;

    if (permissions == 0)
        fd = open(path, flags, FILE_PERMISSION);
    else
        fd = open(path, flags, permissions);
    if (fd < 0)
    {
        *err = $syserr(errno, "Cannot open file {path:q}", path(path));
        return (pioFile_i){NULL};
    }

    file = $alloc(pioLocalFile, .fd = fd,
                  .p = { .path = ft_cstrdup(path), .flags = flags } );
    return bind_pioFile(file);
}

static pioWriteCloser_i
pioLocalDrive_pioOpenRewrite(VSelf, path_t path, int permissions,
						     bool binary, bool use_temp, err_i *err)
{
	Self(pioLocalDrive);
	ft_str_t	temppath;
	int			fd = -1;
	FILE*		fl;
	ft_bytes_t  buf;
	fobj_t		res;

	fobj_reset_err(err);

	if (use_temp)
	{
		temppath = ft_asprintf("%s~tmpXXXXXX", path);
		fd = mkstemp(temppath.ptr);
	}
	else
	{
		temppath = ft_strdupc(path);
		fd = open(path, O_CREAT|O_TRUNC|O_WRONLY, permissions);
	}

	if (fd < 0)
	{
		*err = $syserr(errno, "Create file {path} failed", path(temppath.ptr));
		close(fd);
		ft_str_free(&temppath);
		return $null(pioWriteCloser);
	}

#ifdef WIN32
	if (binary && _setmode(fd, _O_BINARY) < 0)
	{
		*err = $syserr(errno, "Changing permissions for {path} failed",
					   path(temppath.ptr));
		close(fd);
		ft_str_free(&temppath);
		return $null(pioWriteCloser);
	}
#endif

	if (chmod(temppath.ptr, permissions))
	{
		*err = $syserr(errno, "Changing permissions for {path} failed",
					   path(temppath.ptr));
		close(fd);
		ft_str_free(&temppath);
		return $null(pioWriteCloser);
	}

	fl = fdopen(fd, binary ? "wb" : "w");
	ft_assert(fl != NULL);

	buf = ft_bytes_alloc(CHUNK_SIZE);
	setvbuf(fl, buf.ptr, _IOFBF, buf.len);

	res = $alloc(pioLocalWriteFile,
				 .path = ft_strdupc(path),
				 .path_tmp = temppath,
				 .use_temp = use_temp,
				 .fl = fl,
				 .buf = buf);
	return $bind(pioWriteCloser, res);
}

static pioDBWriter_i
pioLocalDrive_pioOpenWrite(VSelf, path_t path, int permissions,
						   bool exclusive, err_i *err)
{
	Self(pioLocalDrive);
	int			fd = -1;
	FILE*		fl;
	ft_bytes_t  buf;
	fobj_t		res;
	int			flags;

	fobj_reset_err(err);

	flags = O_CREAT|O_WRONLY|PG_BINARY;
	if (exclusive)
		flags |= O_EXCL;

	fd = open(path, flags, permissions);

	if (fd < 0)
	{
		*err = $syserr(errno, "Create file {path} failed", path(path));
		close(fd);
		return $null(pioDBWriter);
	}

	if (!exclusive && chmod(path, permissions))
	{
		*err = $syserr(errno, "Changing permissions for {path} failed",
					   path(path));
		close(fd);
		return $null(pioDBWriter);
	}

	fl = fdopen(fd, "wb");
	ft_assert(fl != NULL);

	buf = ft_bytes_alloc(CHUNK_SIZE);
	setvbuf(fl, buf.ptr, _IOFBF, buf.len);

	res = $alloc(pioLocalWriteFile,
				 .path = ft_strdupc(path),
				 .path_tmp = ft_strdupc(path),
				 .use_temp = false,
				 .fl = fl,
				 .buf = buf);
	return $bind(pioDBWriter, res);
}

static pio_stat_t
pioLocalDrive_pioStat(VSelf, path_t path, bool follow_symlink, err_i *err)
{
    struct stat	st = {0};
    pio_stat_t pst = {0};
    int	r;
    fobj_reset_err(err);

    r = follow_symlink ? stat(path, &st) : lstat(path, &st);
    if (r < 0)
        *err = $syserr(errno, "Cannot stat file {path:q}", path(path));
	else
	{
		pst.pst_kind = pio_statmode2file_kind(st.st_mode, path);
		pst.pst_mode = pio_limit_mode(st.st_mode);
		pst.pst_size = st.st_size;
		pst.pst_mtime = st.st_mtime;
	}
    return pst;
}

/*
 * Compare, that filename1 and filename2 is the same file
 * in windows compare only filenames
 */
static bool
pioLocalDrive_pioFilesAreSame(VSelf, path_t file1, path_t file2)
{
#ifndef WIN32
	struct stat	stat1, stat2;

	if (stat(file1, &stat1) < 0)
		elog(ERROR, "Can't stat file \"%s\": %s", file1, strerror(errno));

	if (stat(file2, &stat2) < 0)
		elog(ERROR, "Can't stat file \"%s\": %s", file1, strerror(errno));

	return stat1.st_ino == stat2.st_ino && stat1.st_dev == stat2.st_dev;
#else
	char	*abs_name1 = make_absolute_path(file1);
	char	*abs_name2 = make_absolute_path(file2);
	bool	result = strcmp(abs_name1, abs_name2) == 0;
	free(abs_name2);
	free(abs_name1);
	return result;
#endif
}

#define pioLocalDrive_pioExists common_pioExists

static err_i
pioLocalDrive_pioRemove(VSelf, path_t path, bool missing_ok)
{
    if (remove_file_or_dir(path) != 0)
    {
        if (!missing_ok || errno != ENOENT)
            return $syserr(errno, "Cannot remove {path:q}", path(path));
    }
    return $noerr();
}

static err_i
pioLocalDrive_pioRename(VSelf, path_t old_path, path_t new_path)
{
    if (rename(old_path, new_path) != 0)
        return $syserr(errno, "Cannot rename file {old_path:q} to {new_path:q}",
                       old_path(old_path), new_path(new_path));
    return $noerr();
}

static pg_crc32
pioLocalDrive_pioGetCRC32(VSelf, path_t path, bool compressed, err_i *err)
{
    fobj_reset_err(err);
    elog(VERBOSE, "Local Drive calculate crc32 for '%s', compressed=%d",
         path, compressed);
    if (compressed)
        return pgFileGetCRC32Cgz(path, false);
    else
        return pgFileGetCRC32C(path, false);
}

static bool
pioLocalDrive_pioIsRemote(VSelf)
{
    return false;
}

static err_i
pioLocalDrive_pioMakeDir(VSelf, path_t path, mode_t mode, bool strict)
{
	int rc = dir_create_dir(path, mode, strict);
	if (rc == 0) return $noerr();
	return $syserr(errno, "Cannot make dir {path:q}", path(path));
}

static void
pioLocalDrive_pioListDir(VSelf, parray *files, const char *root, bool handle_tablespaces,
                         bool follow_symlink, bool backup_logs, bool skip_hidden,
                         int external_dir_num) {
    FOBJ_FUNC_ARP();
	Self(pioLocalDrive);
    dir_list_file(files, root, handle_tablespaces, follow_symlink, backup_logs,
                        skip_hidden, external_dir_num, $bind(pioDBDrive, self));
}

static void
pioLocalDrive_pioRemoveDir(VSelf, const char *root, bool root_as_well) {
    FOBJ_FUNC_ARP();
    Self(pioLocalDrive);
	char full_path[MAXPGPATH];
    /* list files to be deleted */
    parray* files = parray_new();
	$(pioListDir, self, .files = files, .root = root, .handle_tablespaces = false,
			.symlink_and_hidden = false, .backup_logs = false, .skip_hidden = false, .external_dir_num = 0);


	// adding the root directory because it must be deleted too
	if(root_as_well)
		parray_append(files, pgFileNew(root, "", false, 0, $bind(pioDrive, self)));

    /* delete leaf node first */
    parray_qsort(files, pgFileCompareRelPathWithExternalDesc);
    size_t num_files = parray_num(files);
    for (int i = 0; i < num_files; i++)
    {
        pgFile	   *file = (pgFile *) parray_get(files, i);

        join_path_components(full_path, root, file->rel_path);

        if (interrupted)
            elog(ERROR, "interrupted during the directory deletion: %s", full_path);

        if (progress)
            elog(INFO, "Progress: (%d/%zd). Delete file \"%s\"",
                 i + 1, num_files, full_path);

        err_i err = $(pioRemove, self, full_path, false);
        if($haserr(err))
            elog(ERROR, "Cannot remove file or directory \"%s\": %s", full_path, $errmsg(err));
    }

    parray_walk(files, pgFileFree);
    parray_free(files);
}

static ft_bytes_t
pioLocalDrive_pioReadFile(VSelf, path_t path, bool binary, err_i* err)
{
	FOBJ_FUNC_ARP();
	Self(pioLocalDrive);
	pioFile_i	fl;
	pio_stat_t	st;
	ft_bytes_t	res = ft_bytes(NULL, 0);
	size_t		amount;

	fobj_reset_err(err);

	st = $(pioStat, self, .path = path, .follow_symlink = true, .err = err);
	if ($haserr(*err))
	{
		$iresult(*err);
		return res;
	}
	if (st.pst_kind != PIO_KIND_REGULAR)
	{
		*err = $err(RT, "File {path:q} is not regular: {kind}", path(path),
					kind(pio_file_kind2str(st.pst_kind, path)),
					errNo(EACCES));
		$iresult(*err);
		return res;
	}

	/* forbid too large file because of remote protocol */
	if (st.pst_size >= PIO_READ_WRITE_FILE_LIMIT)
	{
		*err = $err(RT, "File {path:q} is too large: {size}", path(path),
					size(st.pst_size), errNo(EFBIG));
		$iresult(*err);
		return res;
	}

	if (binary)
		res = ft_bytes_alloc(st.pst_size);
	else
	{
		res = ft_bytes_alloc(st.pst_size + 1);
		res.len -= 1;
	}

	/*
	 * rely on "local file is read whole at once always".
	 * Is it true?
	 */
	fl = $(pioOpen, self, .path = path, .flags = O_RDONLY | (binary ? PG_BINARY : 0),
		   .err = err);
	if ($haserr(*err))
	{
		$iresult(*err);
		return res;
	}

	amount = pioReadFull($reduce(pioRead, fl), res, err);
	if ($haserr(*err))
	{
		ft_bytes_free(&res);
		$iresult(*err);
		return res;
	}

	if (amount != st.pst_size)
	{
		ft_bytes_free(&res);
		*err = $err(RT, "File {path:q} is truncated while reading",
					path(path), errNo(EBUSY));
		$iresult(*err);
		return res;
	}

	res.len = amount;
	if (!binary)
		res.ptr[amount] = 0;

	$i(pioClose, fl);
	return res;
}

static err_i
pioLocalDrive_pioWriteFile(VSelf, path_t path, ft_bytes_t content, bool binary)
{
	FOBJ_FUNC_ARP();
	Self(pioLocalDrive);
	err_i 		err;
	pioWriteCloser_i fl;

	fobj_reset_err(&err);

	if (content.len > PIO_READ_WRITE_FILE_LIMIT)
	{
		err = $err(RT, "File content too large {path:q}: {size}",
				   path(path), size(content.len), errNo(EOVERFLOW));
		return $iresult(err);
	}

	fl = $(pioOpenRewrite, self, path,
		   .binary = binary, .err = &err);
	if ($haserr(err))
		return $iresult(err);

	$i(pioWrite, fl, content, .err = &err);
	if ($haserr(err))
		return $iresult(err);

	err = $i(pioWriteFinish, fl);
	if ($haserr(err))
		return $iresult(err);

	err = $i(pioClose, fl);
	return $iresult(err);
}

/* LOCAL FILE */
static void
pioLocalFile_fobjDispose(VSelf)
{
	Self(pioLocalFile);
	if (!self->p.closed)
	{
		close(self->fd);
		self->fd = -1;
		self->p.closed = true;
	}
}

static err_i
pioLocalFile_pioClose(VSelf, bool sync)
{
    Self(pioLocalFile);
    err_i	err = $noerr();
    int r;

    ft_assert(self->fd >= 0, "Closed file abused \"%s\"", self->p.path);

    if (sync && (self->p.flags & O_ACCMODE) != O_RDONLY)
    {
        r = fsync(self->fd);
        if (r < 0)
            err = $syserr(errno, "Cannot fsync file {path:q}",
						  path(self->p.path));
    }
    r = close(self->fd);
    if (r < 0 && $isNULL(err))
        err = $syserr(errno, "Cannot close file {path:q}",
					  path(self->p.path));
    self->fd = -1;
    self->p.closed = true;
    return err;
}

static size_t
pioLocalFile_pioRead(VSelf, ft_bytes_t buf, err_i *err)
{
    Self(pioLocalFile);
    ssize_t r;
    fobj_reset_err(err);

    ft_assert(self->fd >= 0, "Closed file abused \"%s\"", self->p.path);

    r = read(self->fd, buf.ptr, buf.len);
    if (r < 0)
    {
        *err = $syserr(errno, "Cannot read from {path:q}",
					   path(self->p.path));
        return 0;
    }
    return r;
}

static size_t
pioLocalFile_pioWrite(VSelf, ft_bytes_t buf, err_i *err)
{
    Self(pioLocalFile);
    ssize_t r;
    fobj_reset_err(err);

    ft_assert(self->fd >= 0, "Closed file abused \"%s\"", self->p.path);

    if (buf.len == 0)
        return 0;

    r = durable_write(self->fd, buf.ptr, buf.len);
    if (r < 0)
    {
        *err = $syserr(errno, "Cannot write to file {path:q}",
					   path(self->p.path));
        return 0;
    }
    if (r < buf.len)
    {
        *err = $err(SysErr, "Short write on {path:q}: {writtenSz} < {wantedSz}",
                    path(self->p.path), writtenSz(r), wantedSz(buf.len),
					errNo(EIO));
    }
    return r;
}

static err_i
pioLocalFile_pioSeek(VSelf, off_t offs)
{
	Self(pioLocalFile);

	ft_assert(self->fd >= 0, "Closed file abused \"%s\"", self->p.path);

	off_t pos = lseek(self->fd, offs, SEEK_SET);

	if (pos == (off_t)-1)
		return $syserr(errno, "Can not seek to {offs} in file {path:q}", offs(offs), path(self->p.path));
	ft_assert(pos == offs);
	return $noerr();
}

static err_i
pioLocalFile_pioWriteFinish(VSelf)
{
    Self(pioLocalFile);
    ft_assert(self->fd >= 0, "Closed file abused \"%s\"", self->p.path);
    /* do nothing for unbuffered file */
    return $noerr();
}

static err_i
pioLocalFile_pioTruncate(VSelf, size_t sz)
{
    Self(pioLocalFile);
    ft_assert(self->fd >= 0, "Closed file abused \"%s\"", self->p.path);

    if (ftruncate(self->fd, sz) < 0)
        return $syserr(errno, "Cannot truncate file {path:q}",
					   path(self->p.path));
    return $noerr();
}

static fobjStr*
pioLocalFile_fobjRepr(VSelf)
{
    Self(pioLocalFile);
    return $fmt("pioLocalFile({path:q}, fd:{fd}",
                (path, $S(self->p.path)), (fd, $I(self->fd)));
}

static size_t
pioLocalWriteFile_pioWrite(VSelf, ft_bytes_t buf, err_i* err)
{
	Self(pioLocalWriteFile);
	fobj_reset_err(err);
	size_t r;

	if (buf.len == 0)
		return 0;

	r = fwrite(buf.ptr, 1, buf.len, self->fl);
	if (r < buf.len)
		*err = $syserr(errno, "Writting file {path:q}",
					   path(self->path_tmp.ptr));
	return r;
}

static err_i
pioLocalWriteFile_pioSeek(VSelf, off_t offs)
{
	Self(pioLocalWriteFile);

	ft_assert(self->fl != NULL, "Closed file abused \"%s\"", self->path.ptr);

	if (fseeko(self->fl, offs, SEEK_SET))
		return $syserr(errno, "Can not seek to {offs} in file {path:q}", offs(offs), path(self->path.ptr));

	return $noerr();
}

static err_i
pioLocalWriteFile_pioWriteFinish(VSelf)
{
	Self(pioLocalWriteFile);
	err_i	err = $noerr();

	if (fflush(self->fl) != 0)
		err = $syserr(errno, "Flushing file {path:q}",
					  path(self->path_tmp.ptr));
	return err;
}

static err_i
pioLocalWriteFile_pioClose(VSelf, bool sync)
{
	Self(pioLocalWriteFile);
	int fd;
	int r;

	fd = fileno(self->fl);

	if (ferror(self->fl))
	{
		fclose(self->fl);
		self->fl = NULL;
		if (remove(self->path_tmp.ptr))
			return $syserr(errno, "Couldn't remove file {path:q}",
						   path(self->path_tmp.ptr));
		return $noerr();
	}

	if (fflush(self->fl) != 0)
		return $syserr(errno, "Flushing file {path:q}",
					  path(self->path_tmp.ptr));

	if (sync)
	{
		r = fsync(fd);
		if (r < 0)
			return $syserr(errno, "Cannot fsync file {path:q}",
				 		   path(self->path_tmp.ptr));
	}

	if (self->use_temp)
	{
		if (rename(self->path_tmp.ptr, self->path.ptr))
			return $syserr(errno, "Cannot rename file {old_path:q} to {new_path:q}",
						   old_path(self->path_tmp.ptr),
						   new_path(self->path.ptr));
		/* mark as renamed so fobjDispose will not delete it */
		self->renamed = true;

		if (sync)
		{
			/*
			 * To guarantee renaming the file is persistent, fsync the file with its
			 * new name, and its containing directory.
			 */
			r = fsync(fd);
			if (r < 0)
				return $syserr(errno, "Cannot fsync file {path:q}",
							   path(self->path.ptr));

			if (fsync_parent_path_compat(self->path.ptr) != 0)
				return $syserr(errno, "Cannot fsync file {path:q}",
							   path(self->path.ptr));
		}
	}

	if (fclose(self->fl))
		return $syserr(errno, "Cannot close file {path:q}",
					   path(self->path_tmp.ptr));
	self->fl = NULL;

	return $noerr();
}

static void
pioLocalWriteFile_fobjDispose(VSelf)
{
	Self(pioLocalWriteFile);
	if (self->fl != NULL)
	{
		fclose(self->fl);
		self->fl = NULL;
	}
	if (self->use_temp && !self->renamed)
	{
		remove(self->path_tmp.ptr);
	}
	ft_str_free(&self->path);
	ft_str_free(&self->path_tmp);
	ft_bytes_free(&self->buf);
}

/* REMOTE DRIVE */

static pioFile_i
pioRemoteDrive_pioOpen(VSelf, path_t path,
                       int flags, int permissions,
                       err_i *err)
{
    int handle;
    fio_header hdr;
    fobj_reset_err(err);
    fobj_t file;

	handle = find_free_handle();

    hdr.cop = FIO_OPEN;
    hdr.handle = handle;
    hdr.size = strlen(path) + 1;
    hdr.arg = flags;
	set_handle(handle);

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

    /* check results */
    IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

    if (hdr.arg != 0)
    {
        *err = $syserr((int)hdr.arg, "Cannot open remote file {path:q}",
					   path(path));
		unset_handle(hdr.handle);
        return (pioFile_i){NULL};
    }
    file = $alloc(pioRemoteFile, .handle = handle,
                  .p = { .path = ft_cstrdup(path), .flags = flags });
    return bind_pioFile(file);
}

static pio_stat_t
pioRemoteDrive_pioStat(VSelf, path_t path, bool follow_symlink, err_i *err)
{
    pio_stat_t	st = {0};
    fio_header hdr = {
            .cop = FIO_STAT,
            .handle = -1,
            .size = strlen(path) + 1,
            .arg = follow_symlink,
    };
    fobj_reset_err(err);

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

    IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
    ft_dbg_assert(hdr.cop == FIO_STAT);
    IO_CHECK(fio_read_all(fio_stdin, &st, sizeof(st)), sizeof(st));

    if (hdr.arg != 0)
    {
        *err = $syserr((int)hdr.arg, "Cannot stat remote file {path:q}",
					   path(path));
    }
    return st;
}

static bool
pioRemoteDrive_pioFilesAreSame(VSelf, path_t file1, path_t file2)
{
	fio_header hdr = {
			.cop = FIO_FILES_ARE_SAME,
			.handle = -1,
			.arg = 0,
	};
	char _buf[512];
	ft_strbuf_t buf = ft_strbuf_init_stack(_buf, sizeof(_buf));
	ft_strbuf_catc(&buf, file1);
	ft_strbuf_cat1(&buf, '\x00');
	ft_strbuf_catc(&buf, file2);
	hdr.size = buf.len + 1;

	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	IO_CHECK(fio_write_all(fio_stdout, buf.ptr, buf.len+1), buf.len+1);

	IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
	ft_dbg_assert(hdr.cop == FIO_FILES_ARE_SAME);

	ft_strbuf_free(&buf);

	return hdr.arg == 1;
}

#define pioRemoteDrive_pioExists common_pioExists

static err_i
pioRemoteDrive_pioRemove(VSelf, path_t path, bool missing_ok)
{
    fio_header hdr = {
            .cop = FIO_REMOVE,
            .handle = -1,
            .size = strlen(path) + 1,
            .arg = missing_ok ? 1 : 0,
    };

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

    IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
    ft_dbg_assert(hdr.cop == FIO_REMOVE);

    if (hdr.arg != 0)
    {
        return $syserr((int)hdr.arg, "Cannot remove remote file {path:q}",
					   path(path));
    }
    return $noerr();
}

static err_i
pioRemoteDrive_pioRename(VSelf, path_t old_path, path_t new_path)
{
    size_t old_path_len = strlen(old_path) + 1;
    size_t new_path_len = strlen(new_path) + 1;
    fio_header hdr = {
            .cop = FIO_RENAME,
            .handle = -1,
            .size = old_path_len + new_path_len,
            .arg = 0,
    };

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_write_all(fio_stdout, old_path, old_path_len), old_path_len);
    IO_CHECK(fio_write_all(fio_stdout, new_path, new_path_len), new_path_len);

    IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
    ft_dbg_assert(hdr.cop == FIO_RENAME);

    if (hdr.arg != 0)
    {
        return $syserr((int)hdr.arg, "Cannot rename remote file {old_path:q} to {new_path:q}",
                       old_path(old_path), new_path(new_path));
    }
    return $noerr();
}

static pg_crc32
pioRemoteDrive_pioGetCRC32(VSelf, path_t path, bool compressed, err_i *err)
{
    fio_header hdr;
    size_t path_len = strlen(path) + 1;
    pg_crc32 crc = 0;
    fobj_reset_err(err);

    hdr.cop = FIO_GET_CRC32;
    hdr.handle = -1;
    hdr.size = path_len;
    hdr.arg = 0;

    if (compressed)
        hdr.arg = GET_CRC32_DECOMPRESS;
    elog(VERBOSE, "Remote Drive calculate crc32 for '%s', hdr.arg=%d",
         path, compressed);

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);
    IO_CHECK(fio_read_all(fio_stdin, &crc, sizeof(crc)), sizeof(crc));

    return crc;
}

static bool
pioRemoteDrive_pioIsRemote(VSelf)
{
    return true;
}

static err_i
pioRemoteDrive_pioMakeDir(VSelf, path_t path, mode_t mode, bool strict)
{
	fio_header hdr = {
		.cop = FIO_MKDIR,
		.handle = strict ? 1 : 0, /* ugly "hack" to pass more params*/
		.size = strlen(path) + 1,
		.arg = mode,
	};

	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

	IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
	Assert(hdr.cop == FIO_MKDIR);

	if (hdr.arg == 0)
	{
		return $noerr();
	}
	return $syserr(hdr.arg, "Cannot make dir {path:q}", path(path));
}

static void
pioRemoteDrive_pioListDir(VSelf, parray *files, const char *root, bool handle_tablespaces,
                          bool follow_symlink, bool backup_logs, bool skip_hidden,
                          int external_dir_num) {
    FOBJ_FUNC_ARP();
    fio_header hdr;
    fio_list_dir_request req;
    char *buf = pgut_malloc(CHUNK_SIZE);

    /* Send to the agent message with parameters for directory listing */
	memset(&req, 0, sizeof(req));
    snprintf(req.path, MAXPGPATH, "%s", root);
    req.handle_tablespaces = handle_tablespaces;
    req.follow_symlink = follow_symlink;
    req.backup_logs = backup_logs;
	req.skip_hidden = skip_hidden;
    req.external_dir_num = external_dir_num;

	hdr = (fio_header){.cop = FIO_LIST_DIR, .size=sizeof(req)};

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_write_all(fio_stdout, &req, hdr.size), hdr.size);

    for (;;) {
        /* receive data */
        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

        if (hdr.cop == FIO_SEND_FILE_EOF) {
            /* the work is done */
            break;
        } else if (hdr.cop == FIO_SEND_FILE) {
            pgFile *file = NULL;
            fio_pgFile  fio_file;

            /* receive rel_path */
            IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);
            file = pgFileInit(buf);

            /* receive metainformation */
            IO_CHECK(fio_read_all(fio_stdin, &fio_file, sizeof(fio_file)), sizeof(fio_file));

            file->kind = fio_file.kind;
            file->mode = fio_file.mode;
            file->size = fio_file.size;
            file->mtime = fio_file.mtime;
            file->is_datafile = fio_file.is_datafile;
            file->tblspcOid = fio_file.tblspcOid;
            file->dbOid = fio_file.dbOid;
            file->relOid = fio_file.relOid;
            file->forkName = fio_file.forkName;
            file->segno = fio_file.segno;
            file->external_dir_num = fio_file.external_dir_num;

            if (fio_file.linked_len > 0) {
                IO_CHECK(fio_read_all(fio_stdin, buf, fio_file.linked_len), fio_file.linked_len);

                file->linked = pgut_malloc(fio_file.linked_len);
                snprintf(file->linked, fio_file.linked_len, "%s", buf);
            }

//			elog(INFO, "Received file: %s, mode: %u, size: %lu, mtime: %lu",
//				file->rel_path, file->mode, file->size, file->mtime);

            parray_append(files, file);
        } else {
            /* TODO: fio_disconnect may get assert fail when running after this */
            elog(ERROR, "Remote agent returned message of unexpected type: %i", hdr.cop);
        }
    }

    pg_free(buf);
}

static void
pioRemoteDrive_pioRemoveDir(VSelf, const char *root, bool root_as_well) {
    FOBJ_FUNC_ARP();
    fio_header hdr;
    fio_remove_dir_request req;

    /* Send to the agent message with parameters for directory listing */
    snprintf(req.path, MAXPGPATH, "%s", root);
	req.root_as_well = root_as_well;

    hdr.cop = FIO_REMOVE_DIR;
    hdr.size = sizeof(req);

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_write_all(fio_stdout, &req, hdr.size), hdr.size);

    /* get the response */
    IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
    Assert(hdr.cop == FIO_REMOVE_DIR);

    if (hdr.arg != 0)
        elog(ERROR, "couldn't remove remote dir");
}

static ft_bytes_t
pioRemoteDrive_pioReadFile(VSelf, path_t path, bool binary, err_i* err)
{
	FOBJ_FUNC_ARP();
	Self(pioRemoteDrive);
	ft_bytes_t res;

	fobj_reset_err(err);

	fio_ensure_remote();

	fio_header hdr = {
			.cop = FIO_READ_FILE_AT_ONCE,
			.handle = -1,
			.size = strlen(path)+1,
			.arg = binary,
	};

	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

	/* get the response */
	IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
	Assert(hdr.cop == FIO_READ_FILE_AT_ONCE);

	res = ft_bytes_alloc(hdr.size);
	IO_CHECK(fio_read_all(fio_stdin, res.ptr, hdr.size), hdr.size);

	if (hdr.arg != 0)
	{
		*err = $syserr((int)hdr.arg, "Could not read remote file {path:q}: {causeStr}",
					   path(path), causeStr(res.ptr));
		$iresult(*err);
		ft_bytes_free(&res);
	}

	return res;
}

static err_i
pioRemoteDrive_pioWriteFile(VSelf, path_t path, ft_bytes_t content, bool binary)
{
	FOBJ_FUNC_ARP();
	Self(pioLocalDrive);
	fio_header	hdr;
	ft_bytes_t	msg;
	err_i		err;
	ft_strbuf_t buf = ft_strbuf_zero();

	fobj_reset_err(&err);

	fio_ensure_remote();

	if (content.len > PIO_READ_WRITE_FILE_LIMIT)
	{
		err = $err(RT, "File content too large {path:q}: {size}",
				   path(path), size(content.len), errNo(EOVERFLOW));
		return $iresult(err);
	}

	ft_strbuf_catc(&buf, path);
	ft_strbuf_cat1(&buf, '\x00');
	ft_strbuf_catbytes(&buf, content);

	hdr = (fio_header){
			.cop = FIO_WRITE_FILE_AT_ONCE,
			.handle = -1,
			.size = buf.len,
			.arg = binary,
	};

	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	IO_CHECK(fio_write_all(fio_stdout, buf.ptr, buf.len), buf.len);

	ft_strbuf_free(&buf);

	IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
	ft_dbg_assert(hdr.cop == FIO_WRITE_FILE_AT_ONCE);

	if (hdr.arg != 0)
	{
		msg = ft_bytes_alloc(hdr.size);
		IO_CHECK(fio_read_all(fio_stdin, msg.ptr, hdr.size), hdr.size);
		err = $syserr((int)hdr.arg, "Could not write remote file {path:q}: {causeStr}",
					   path(path), causeStr(msg.ptr));
		ft_bytes_free(&msg);
		return $iresult(err);
	}

	return $noerr();
}

/* REMOTE FILE */

static err_i
pioRemoteFile_pioSync(VSelf)
{
    Self(pioRemoteFile);

    fio_header hdr;
    hdr.cop = FIO_SYNC_FILE;
    hdr.handle = self->handle;
    hdr.arg = 0;
    hdr.size = 0;

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

    if (hdr.arg != 0)
    {
        return $syserr((int)hdr.arg, "Cannot fsync remote file {path:q}",
					   path(self->p.path));
    }
    return $noerr();
}

static err_i
pioRemoteFile_doClose(VSelf)
{
	Self(pioRemoteFile);
	err_i err = $noerr();
	fio_header hdr;

	hdr = (fio_header){
			.cop = FIO_CLOSE,
			.handle = self->handle,
			.size = 0,
			.arg = 0,
	};

	unset_handle(hdr.handle);
	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

	/* Wait for response */
	IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
	ft_dbg_assert(hdr.cop == FIO_CLOSE);

	if (hdr.arg != 0 && $isNULL(err))
	{
		err = $syserr((int)hdr.arg, "Cannot close remote file {path:q}",
					  path(self->p.path));
	}

	self->p.closed = true;

	return err;
}

static err_i
pioRemoteFile_pioClose(VSelf, bool sync)
{
	Self(pioRemoteFile);
	err_i err = $noerr();

	ft_assert(self->handle >= 0, "Remote closed file abused \"%s\"", self->p.path);

	if (sync && (self->p.flags & O_ACCMODE) != O_RDONLY)
		err = pioRemoteFile_pioSync(self);

	return fobj_err_combine(err, pioRemoteFile_doClose(self));
}

static size_t
pioRemoteFile_pioAsyncRead(VSelf, ft_bytes_t buf, err_i *err)
{
    Self(pioRemoteFile);
    fio_header      hdr = {0};
    size_t          buflen = buf.len;
    ft_bytes_t      bytes;
    fobj_reset_err(err);

    ft_assert(self->handle >= 0, "Remote closed file abused \"%s\"", self->p.path);

    if (self->asyncEof)
    {
        return 0;
    }
    else if (!self->didAsync)
    {
        /* start reading */
        hdr.cop = FIO_SEND_FILE_CONTENT;
        hdr.handle = self->handle;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        if (self->asyncChunk == NULL)
            self->asyncChunk = ft_malloc(CHUNK_SIZE);
        self->didAsync = true;
    }

    while (buf.len > 0)
    {
        if (self->chunkRest.len > 0)
        {
            ft_bytes_move(&buf, &self->chunkRest);
            continue;
        }

        if (buf.len >= CHUNK_SIZE)
            bytes = ft_bytes(buf.ptr, CHUNK_SIZE);
        else
            bytes = ft_bytes(self->asyncChunk, CHUNK_SIZE);

        /* receive data */
        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

        if (hdr.cop == FIO_SEND_FILE_EOF)
        {
            self->asyncEof = true;
            break;
        }
        else if (hdr.cop == FIO_ERROR)
        {
            int erno = EIO;
            switch ((int)hdr.arg)
            {
                case FILE_MISSING:
                    erno = ENOENT;
                    break;
                case OPEN_FAILED:
                    /* We should be already opened. */
                    ft_assert((int)hdr.arg != OPEN_FAILED);
                    break;
                case READ_FAILED:
                    erno = EIO;
                    break;
            }
            /* handle error, reported by the agent */
            if (hdr.size > 0)
            {
                ft_assert(hdr.size < CHUNK_SIZE);
                IO_CHECK(fio_read_all(fio_stdin, self->asyncChunk, hdr.size), hdr.size);
                ft_assert(((char*)self->asyncChunk)[hdr.size] == 0);
                *err = $syserr(erno, "Cannot async read remote file {path:q}: {remotemsg}",
                               remotemsg(self->asyncChunk),
                               path(self->p.path));
                break;
            }
            else
            {
                *err = $syserr(erno, "Cannot async read remote file {path:q}",
							   path(self->p.path));
            }
            fio_disconnect(); /* discard possible pending data in pipe */
            break;
        }
        else if (hdr.cop == FIO_PAGE)
        {
            ft_assert(hdr.size <= CHUNK_SIZE);
            IO_CHECK(fio_read_all(fio_stdin, bytes.ptr, hdr.size), hdr.size);

            if (bytes.ptr != buf.ptr)
            {
                bytes.len = hdr.size;
                ft_bytes_move(&buf, &bytes);
                self->chunkRest = bytes;
            }
            else
            {
                ft_bytes_consume(&buf, hdr.size);
            }
        }
        else if (hdr.cop == FIO_PAGE_ZERO)
        {
            ft_assert(hdr.arg <= CHUNK_SIZE);
            ft_assert(hdr.size == 0);

            memset(bytes.ptr, 0, hdr.arg);
            if (bytes.ptr != buf.ptr)
            {
                bytes.len = hdr.arg;
                ft_bytes_move(&buf, &bytes);
                self->chunkRest = bytes;
            }
            else
            {
                ft_bytes_consume(&buf, hdr.arg);
            }
        }
        else
        {
            /* TODO: fio_disconnect may get assert fail when running after this */
            elog(ERROR, "Remote agent returned message of unexpected type: %i", hdr.cop);
        }
    }

    return (buflen - buf.len);
}

static size_t
pioRemoteFile_pioRead(VSelf, ft_bytes_t buf, err_i *err)
{
    Self(pioRemoteFile);

    ft_assert(self->handle >= 0, "Remote closed file abused \"%s\"", self->p.path);

    if (self->asyncMode)
        return $(pioAsyncRead, self, buf, err);

    fio_header hdr = {
            .cop = FIO_READ,
            .handle = self->handle,
            .size = 0,
            .arg = buf.len,
    };
    fobj_reset_err(err);

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

    IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
    ft_dbg_assert(hdr.cop == FIO_SEND);
    IO_CHECK(fio_read_all(fio_stdin, buf.ptr, hdr.size), hdr.size);
    if (hdr.arg != 0) {
        *err = $syserr((int)hdr.arg, "Cannot read remote file {path:q}",
					   path(self->p.path));
        return 0;
    }

    return hdr.size;
}

static size_t
pioRemoteFile_pioWrite(VSelf, ft_bytes_t buf, err_i *err)
{
    Self(pioRemoteFile);
    fio_header hdr;
    fobj_reset_err(err);

    ft_assert(self->handle >= 0, "Remote closed file abused \"%s\"", self->p.path);

    if (buf.len == 0)
        return 0;

    if (self->asyncMode)
        return pioAsyncWrite(self, buf, err);

    hdr = (fio_header){
            .cop = FIO_WRITE,
            .handle = self->handle,
            .size = buf.len,
            .arg = 0,
    };

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_write_all(fio_stdout, buf.ptr, buf.len), buf.len);

    /* check results */
    IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
    ft_dbg_assert(hdr.cop == FIO_WRITE);

    /* set errno */
    if (hdr.arg != 0) {
        *err = $syserr((int)hdr.arg, "Cannot write remote file {path:q}",
					   path(self->p.path));
        return 0;
    }

    return buf.len;
}

static err_i
pioRemoteFile_pioSeek(VSelf, off_t offs)
{
	Self(pioRemoteFile);
	fio_header hdr;

	ft_assert(self->handle >= 0, "Remote closed file abused \"%s\"", self->p.path);

	hdr.cop = FIO_SEEK;
	hdr.handle = self->handle;
	hdr.size = 0;
	hdr.arg = offs;

	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

	return $noerr();
}

static err_i
pioRemoteFile_pioWriteFinish(VSelf)
{
    Self(pioRemoteFile);

    ft_assert(self->handle >= 0, "Remote closed file abused \"%s\"", self->p.path);

    if (self->asyncMode)
        return pioAsyncError(self);
    return $noerr();
}

static err_i
pioRemoteFile_pioTruncate(VSelf, size_t sz)
{
    Self(pioRemoteFile);

    ft_assert(self->handle >= 0, "Remote closed file abused \"%s\"", self->p.path);

    fio_header hdr = {
            .cop = FIO_TRUNCATE,
            .handle = self->handle,
            .size = 0,
            .arg = sz,
    };

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

    return $noerr();
}

static err_i
pioRemoteFile_pioSetAsync(VSelf, bool async)
{
    Self(pioRemoteFile);

    ft_assert(self->handle >= 0, "Remote closed file abused \"%s\"", self->p.path);

    if (!self->asyncMode && async)
    {
        if ((self->p.flags & O_ACCMODE) == O_RDWR)
            return $err(RT, "Could not enable async mode on Read-Write file");
        self->asyncMode = true;
    }
    else if (self->asyncMode && !async)
    {
        err_i err = pioAsyncError(self);
        self->asyncMode = false;
        return err;
    }
    return $noerr();
}

static size_t
pioRemoteFile_pioAsyncWrite(VSelf, ft_bytes_t buf, err_i *err)
{
    Self(pioRemoteFile);
    fio_header hdr;

    ft_assert(self->handle >= 0, "Remote closed file abused \"%s\"", self->p.path);

    if ($haserr(self->asyncError)) {
        *err = self->asyncError;
        return 0;
    }

    if (buf.len == 0)
        return 0;

    hdr = (fio_header){
            .cop = FIO_WRITE_ASYNC,
            .handle = self->handle,
            .size = buf.len,
            .arg = 0,
    };

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_write_all(fio_stdout, buf.ptr, buf.len), buf.len);
    self->didAsync = true;
    return buf.len;
}

static err_i
pioRemoteFile_pioAsyncError(VSelf)
{
    Self(pioRemoteFile);
    char *errmsg;
    fio_header hdr;

    if ($haserr(self->asyncError) || !self->didAsync)
    {
        self->didAsync = false;
        return self->asyncError;
    }

    hdr.cop = FIO_GET_ASYNC_ERROR;
    hdr.size = 0;

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

    /* check results */
    IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

    if (hdr.size == 0)
        return $noerr();

    errmsg = pgut_malloc(ERRMSG_MAX_LEN);
    IO_CHECK(fio_read_all(fio_stdin, errmsg, hdr.size), hdr.size);
    self->asyncError = $err(SysErr, "{remotemsg}", remotemsg(errmsg));
    self->didAsync = false;
    free(errmsg);
    return self->asyncError;
}

static void
pioRemoteFile_fobjDispose(VSelf)
{
    Self(pioRemoteFile);
	if (!self->p.closed)
	{
		err_i	err;

		err = pioRemoteFile_doClose(self);
		if ($haserr(err))
			elog(WARNING, "%s", $errmsg(err));
	}
    $idel(&self->asyncError);
    ft_free(self->asyncChunk);
}

static fobjStr*
pioRemoteFile_fobjRepr(VSelf)
{
    Self(pioRemoteFile);
    return $fmt("pioRemoteFile({path:q}, hnd:{hnd}, async:{asyncMode}, err:{asyncError})",
                (path, $S(self->p.path)),
                (hnd, $I(self->handle)),
                (asyncMode, $B(self->asyncMode)),
                (err, self->asyncError.self));
}

static pioWriteCloser_i
pioRemoteDrive_pioOpenRewrite(VSelf, path_t path, int permissions,
							 bool binary, bool use_temp, err_i *err)
{
	Self(pioRemoteDrive);
	ft_strbuf_t buf = ft_strbuf_zero();
	fobj_t		fl;
	int         handle = find_free_handle();

	fio_header hdr = {
			.cop    = PIO_OPEN_REWRITE,
			.handle = handle,
	};

	struct fio_req_open_rewrite req = {
			.permissions = permissions,
			.binary = binary,
			.use_temp = use_temp
	};

	fio_ensure_remote();

	ft_strbuf_catbytes(&buf, ft_bytes(&hdr, sizeof(hdr)));
	ft_strbuf_catbytes(&buf, ft_bytes(&req, sizeof(req)));
	ft_strbuf_catc(&buf, path);
	ft_strbuf_cat1(&buf, '\0');

	((fio_header*)buf.ptr)->size = buf.len - sizeof(hdr);

	IO_CHECK(fio_write_all(fio_stdout, buf.ptr, buf.len), buf.len);

	IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

	if (hdr.cop == FIO_PIO_ERROR)
	{
		*err = fio_receive_pio_err(&hdr);
		return $null(pioWriteCloser);
	}
	ft_dbg_assert(hdr.cop == PIO_OPEN_REWRITE &&
				  hdr.handle == handle);

	set_handle(handle);

	fl = $alloc(pioRemoteWriteFile,
				.path = ft_strdupc(path),
				.handle = handle);
	return $bind(pioWriteCloser, fl);
}

static pioDBWriter_i
pioRemoteDrive_pioOpenWrite(VSelf, path_t path, int permissions,
							bool exclusive, err_i *err)
{
	Self(pioRemoteDrive);
	ft_strbuf_t buf = ft_strbuf_zero();
	fobj_t		fl;
	int         handle = find_free_handle();

	fio_header hdr = {
			.cop    = PIO_OPEN_WRITE,
			.handle = handle,
	};

	struct fio_req_open_write req = {
			.permissions = permissions,
			.exclusive   = exclusive,
	};

	fio_ensure_remote();

	ft_strbuf_catbytes(&buf, ft_bytes(&hdr, sizeof(hdr)));
	ft_strbuf_catbytes(&buf, ft_bytes(&req, sizeof(req)));
	ft_strbuf_catc(&buf, path);
	ft_strbuf_cat1(&buf, '\0');

	((fio_header*)buf.ptr)->size = buf.len - sizeof(hdr);

	IO_CHECK(fio_write_all(fio_stdout, buf.ptr, buf.len), buf.len);

	IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

	if (hdr.cop == FIO_PIO_ERROR)
	{
		*err = fio_receive_pio_err(&hdr);
		return $null(pioDBWriter);
	}
	ft_dbg_assert(hdr.cop == PIO_OPEN_WRITE &&
				  hdr.handle == handle);

	set_handle(handle);

	fl = $alloc(pioRemoteWriteFile,
				.path = ft_strdupc(path),
				.handle = handle);
	return $bind(pioDBWriter, fl);
}

static size_t
pioRemoteWriteFile_pioWrite(VSelf, ft_bytes_t buf, err_i* err)
{
	Self(pioRemoteWriteFile);
	fobj_reset_err(err);
	fio_header hdr;

	ft_assert(self->handle >= 0);

	if (buf.len == 0)
		return 0;

	hdr = (fio_header){
			.cop = PIO_WRITE_ASYNC,
			.handle = self->handle,
			.size = buf.len,
			.arg = 0,
	};

	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	IO_CHECK(fio_write_all(fio_stdout, buf.ptr, buf.len), buf.len);

	return buf.len;
}

static err_i
pioRemoteWriteFile_pioSeek(VSelf, off_t offs)
{
	Self(pioRemoteWriteFile);
	struct __attribute__((packed)) {
		fio_header hdr;
		uint64_t   off;
	} req = {
			.hdr = {
					.cop = PIO_SEEK,
					.handle = self->handle,
					.size = sizeof(uint64_t),
			},
			.off = offs,
	};

	ft_assert(self->handle >= 0, "Remote closed file abused \"%s\"", self->path.ptr);

	IO_CHECK(fio_write_all(fio_stdout, &req, sizeof(req)), sizeof(req));

	return $noerr();
}

static err_i
pioRemoteWriteFile_pioWriteFinish(VSelf)
{
	Self(pioRemoteWriteFile);
	fio_header hdr;

	ft_assert(self->handle >= 0);

	hdr = (fio_header){
			.cop = PIO_GET_ASYNC_ERROR,
			.handle = self->handle,
	};

	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

	IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

	if (hdr.cop == FIO_PIO_ERROR)
		return fio_receive_pio_err(&hdr);
	ft_dbg_assert(hdr.cop == PIO_GET_ASYNC_ERROR);

	return $noerr();
}

static err_i
pioRemoteWriteFile_pioClose(VSelf, bool sync)
{
	Self(pioRemoteWriteFile);
	fio_header hdr;
	struct __attribute__((packed)) {
		fio_header hdr;
		bool sync;
	} req = {
		.hdr = {
				.cop = PIO_CLOSE,
				.handle = self->handle,
				.size = 1,
		},
		.sync = sync,
	};

	ft_assert(self->handle >= 0);

	IO_CHECK(fio_write_all(fio_stdout, &req, sizeof(req)), sizeof(req));
	IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

	unset_handle(self->handle);
	self->handle = -1;

	if (hdr.cop == FIO_PIO_ERROR)
		return fio_receive_pio_err(&hdr);
	return $noerr();
}

static void
pioRemoteWriteFile_fobjDispose(VSelf)
{
	Self(pioRemoteWriteFile);

	if (self->handle >= 0)
	{
		fio_header hdr = {
				.cop = PIO_DISPOSE,
				.handle = self->handle,
		};
		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		unset_handle(self->handle);
	}
	ft_str_free(&self->path);
}

pioRead_i
pioWrapReadFilter(pioRead_i fl, pioFilter_i flt, size_t buf_size)
{
    void *buf;
    pioReadFilter* wrap;

    buf = ft_malloc(buf_size);
    wrap = $alloc(pioReadFilter,
                  .wrapped = $iref(fl),
                  .filter = $iref(flt),
                  .buffer = buf,
                  .capa = buf_size);
    $implements(pioFltInPlace, flt.self, &wrap->inplace);
    return bind_pioRead(wrap);
}

static size_t
pioReadFilter_pioRead(VSelf, ft_bytes_t wbuf, err_i *err)
{
    Self(pioReadFilter);
    fobj_reset_err(err);
    pioFltTransformResult tr;
    size_t      wlen = wbuf.len;
    ft_bytes_t	rbuf;
    size_t 		r;

    if (self->eof && self->finished)
        return 0;

    if ($notNULL(self->inplace) && !self->eof)
    {
        r = pioReadFull(self->wrapped, wbuf, err);
        if (r > 0)
        {
            err_i flterr;
            flterr = $i(pioFltInPlace, self->inplace, ft_bytes(wbuf.ptr, r));
            *err = fobj_err_combine(*err, flterr);
            ft_bytes_consume(&wbuf, r);
        }

        if ($haserr(*err))
            return wlen - wbuf.len;
        if (r == 0)
        {
            self->eof = true;
            goto eof;
        }
    }

    while (wbuf.len > 0)
    {
        /* feed filter */
        rbuf = ft_bytes(self->buffer, self->len);
        while (rbuf.len > 0)
        {
            tr = $i(pioFltTransform, self->filter, rbuf, wbuf, err);
            if ($haserr(*err))
                return wlen - wbuf.len;
            ft_bytes_consume(&rbuf, tr.consumed);
            ft_bytes_consume(&wbuf, tr.produced);

            if (tr.produced == 0) /* Probably need more input to produce */
                break;
        }

        if (self->eof)
            break;

        /* move rest if any */
        if (rbuf.len > 0)
            memmove(self->buffer, rbuf.ptr, rbuf.len);
        self->len = rbuf.len;

        /* feed buffer */
        rbuf = ft_bytes(self->buffer, self->capa);
        ft_bytes_consume(&rbuf, self->len);
        ft_assert(rbuf.len > 0);
        r = $i(pioRead, self->wrapped, rbuf, err);
        if ($haserr(*err))
            return wlen - wbuf.len;
        if (r == 0)
            self->eof = true;
        self->len += r;
    }

eof:
    while (wbuf.len > 0 && self->eof)
    {
        r = $i(pioFltFinish, self->filter, wbuf, err);
        if ($haserr(*err))
            return (ssize_t)(wlen - wbuf.len);
        ft_bytes_consume(&wbuf, r);
        if (r == 0)
        {
            self->finished = true;
            break;
        }
    }

    return wlen - wbuf.len;
}

static err_i
pioReadFilter_pioClose(VSelf, bool sync)
{
    Self(pioReadFilter);
    err_i err = $noerr();
    err_i errcl = $noerr();
    size_t r;

    if (!self->finished)
    {
        r = $i(pioFltFinish, self->filter, ft_bytes(NULL, 0), &err);
        ft_assert(r == 0);
    }
    if ($ifdef(errcl =, pioClose, self->wrapped.self, sync))
        err = fobj_err_combine(err, errcl);
    return err;
}

static void
pioReadFilter_fobjDispose(VSelf)
{
    Self(pioReadFilter);
    $idel(&self->wrapped);
    $idel(&self->filter);
    ft_free(self->buffer);
}

static fobjStr*
pioReadFilter_fobjRepr(VSelf)
{
    Self(pioReadFilter);
    return $fmt("pioReadFilter(wrapped: {wrapped}, filter: {filter})",
                (wrapped, self->wrapped.self),
                (filter,  self->filter.self));
}

pioWriteFlush_i
pioWrapWriteFilter(pioWriteFlush_i fl, pioFilter_i flt, size_t buf_size)
{
    void *buf;
    pioWriteFilter* wrap;

    buf = ft_malloc(buf_size);
    wrap = $alloc(pioWriteFilter,
                  .wrapped = $iref(fl),
                  .filter = $iref(flt),
                  .buffer = buf,
                  .capa = buf_size);
    $implements(pioFltInPlace, flt.self, &wrap->inplace);
    return bind_pioWriteFlush(wrap);
}

static size_t
pioWriteFilter_pioWrite(VSelf, ft_bytes_t rbuf, err_i *err)
{
    Self(pioWriteFilter);
    fobj_reset_err(err);
    pioFltTransformResult tr;
    size_t      rlen = rbuf.len;
    ft_bytes_t	wbuf;
    size_t 		r;

    if ($notNULL(self->inplace))
    {
        *err = $i(pioFltInPlace, self->inplace, rbuf);
        if ($haserr(*err))
            return 0;
        r = $i(pioWrite, self->wrapped, rbuf, err);
        ft_bytes_consume(&rbuf, r);
        return rlen - rbuf.len;
    }

    while (rbuf.len > 0)
    {
        wbuf = ft_bytes(self->buffer, self->capa);
        while (wbuf.len > 0)
        {
            tr = $i(pioFltTransform, self->filter, rbuf, wbuf, err);
            if ($haserr(*err))
                return rlen - rbuf.len;
            ft_bytes_consume(&rbuf, tr.consumed);
            ft_bytes_consume(&wbuf, tr.produced);

            if (tr.produced == 0) /* Probably need more input to produce */
                break;
        }

        /* feed writer */
        wbuf = ft_bytes(self->buffer, (char*)wbuf.ptr - (char*)self->buffer);
        if (wbuf.len == 0)
        {
            ft_dbg_assert(rbuf.len == 0);
            break;
        }
        r = $i(pioWrite, self->wrapped, wbuf, err);
        if ($haserr(*err))
            return rlen - rbuf.len;
        ft_assert(r == wbuf.len);
    }

    if (rbuf.len)
    {
        *err = $err(SysErr, "short write: {writtenSz} < {wantedSz}",
                    writtenSz(rlen - rbuf.len), wantedSz(rbuf.len));
    }
    return rlen - rbuf.len;
}

static err_i
pioWriteFilter_pioWriteFinish(VSelf)
{
    Self(pioWriteFilter);
    err_i err = $noerr();
    ft_bytes_t	wbuf;
    size_t 		r;

    while (!self->finished)
    {
        wbuf = ft_bytes(self->buffer, self->capa);
        while (wbuf.len > 0)
        {
            r = $i(pioFltFinish, self->filter, wbuf, &err);
            if ($haserr(err))
                return err;
            ft_bytes_consume(&wbuf, r);
            if (r == 0)
            {
                self->finished = true;
                break;
            }
        }

        /* feed writer */
        wbuf = ft_bytes(self->buffer, (char*)wbuf.ptr - (char*)self->buffer);
        if (wbuf.len == 0)
            break;

        ft_assert(wbuf.len > 0);
        r = $i(pioWrite, self->wrapped, wbuf, &err);
        if ($haserr(err))
            return err;
        ft_assert(r == wbuf.len);
    }

    return $i(pioWriteFinish, self->wrapped);
}

static err_i
pioWriteFilter_pioClose(VSelf, bool sync)
{
    Self(pioWriteFilter);
    err_i err = $noerr();
    err_i errcl = $noerr();
    size_t r;

    if (!self->finished)
    {
        r = $i(pioFltFinish, self->filter, ft_bytes(NULL, 0), &err);
        ft_assert(r == 0);
    }
    if ($ifdef(errcl =, pioClose, self->wrapped.self, sync))
        err = fobj_err_combine(err, errcl);
    return err;
}

static void
pioWriteFilter_fobjDispose(VSelf)
{
    Self(pioWriteFilter);
    $idel(&self->wrapped);
    $idel(&self->filter);
    ft_free(self->buffer);
}

static fobjStr*
pioWriteFilter_fobjRepr(VSelf)
{
    Self(pioWriteFilter);
    return $fmt("pioWriteFilter(wrapped: {wrapped}, filter: {filter})",
                (wrapped, self->wrapped.self),
                (filter,  self->filter.self));
}

#ifdef HAVE_LIBZ
static err_i
newGZError(const char *gzmsg, int gzerrno)
{
    if (gzerrno == Z_OK && errno == 0)
        return $noerr();
    if (gzerrno == Z_ERRNO) {
        return $syserr(errno, "System error during GZ");
    }

    return $err(GZ, "GZ error: {gzErrStr}", gzErrStr(gzmsg), gzErrNo(gzerrno));
}

pioFilter_i
pioGZCompressFilter(int level)
{
    pioGZCompress  *gz;
    int				rc;

    gz = $alloc(pioGZCompress);
    rc = deflateInit2(&gz->strm,
                      level,
                      Z_DEFLATED,
                      MAX_WBITS + 16, DEF_MEM_LEVEL,
                      Z_DEFAULT_STRATEGY);
    ft_assert(rc == Z_OK, "zlib internal error: %s", gz->strm.msg);
    return bind_pioFilter(gz);
}

pioFilter_i
pioGZDecompressFilter(bool ignoreTruncate)
{
    pioGZDecompress	*gz;
    int              rc;

    gz = $alloc(pioGZDecompress, .ignoreTruncate = ignoreTruncate);

    rc = inflateInit2(&gz->strm, 15 + 16);
    ft_assert(rc == Z_OK, "zlib internal error: %s", gz->strm.msg);
    return bind_pioFilter(gz);
}

static pioFltTransformResult
pioGZCompress_pioFltTransform(VSelf, ft_bytes_t rbuf, ft_bytes_t wbuf, err_i *err)
{
    Self(pioGZCompress);
    pioFltTransformResult  tr = {0, 0};
    size_t  rlen = rbuf.len;
    size_t	wlen = wbuf.len;
    ssize_t rc;
    fobj_reset_err(err);

    if (self->finished)
    {
        *err = $err(RT, "pioGZCompress already finished");
        return tr;
    }

    while (rbuf.len > 0 && wbuf.len > 0)
    {
        self->strm.next_in = (Bytef *)rbuf.ptr;
        self->strm.avail_in = rbuf.len;
        self->strm.next_out = (Bytef *)wbuf.ptr;
        self->strm.avail_out = wbuf.len;

        rc = deflate(&self->strm, Z_NO_FLUSH);
        ft_dbg_assert(rc == Z_OK);

        ft_bytes_consume(&wbuf, wbuf.len - self->strm.avail_out);
        ft_bytes_consume(&rbuf, rbuf.len - self->strm.avail_in);
    }

    tr.produced = wlen - wbuf.len;
    tr.consumed = rlen - rbuf.len;
    return tr;
}

static size_t
pioGZCompress_pioFltFinish(VSelf, ft_bytes_t wbuf, err_i *err)
{
    Self(pioGZCompress);
    size_t	wlen = wbuf.len;
    int     rc;
    fobj_reset_err(err);

    if (self->finished)
        return 0;

    while (wbuf.len > 0)
    {
        self->strm.avail_in = 0;
        self->strm.next_out = (Bytef *)wbuf.ptr;
        self->strm.avail_out = wbuf.len;

        rc = deflate(&self->strm, Z_FINISH);

        ft_bytes_consume(&wbuf, wbuf.len - self->strm.avail_out);

        if (rc == Z_STREAM_END)
        {
            rc = deflateEnd(&self->strm);
            ft_dbg_assert(rc == Z_OK);
            self->finished = true;
            break;
        }
        ft_dbg_assert(rc == Z_OK);
    }

    return wlen - wbuf.len;
}

static void
pioGZCompress_fobjDispose(VSelf)
{
    Self(pioGZCompress);
    int rc;

    if (!self->finished)
    {
        rc = deflateEnd(&self->strm);
        ft_dbg_assert(rc == Z_OK || rc == Z_DATA_ERROR);
    }
}

static fobjStr*
pioGZCompress_fobjRepr(VSelf)
{
    Self(pioGZCompress);
    return $S("pioGZCompress");
}

static pioFltTransformResult
pioGZDecompress_pioFltTransform(VSelf, ft_bytes_t rbuf, ft_bytes_t wbuf, err_i* err)
{
    Self(pioGZDecompress);
    pioFltTransformResult  tr = {0, 0};
    size_t  rlen = rbuf.len;
    size_t	wlen = wbuf.len;
    int rc;
    fobj_reset_err(err);

    if (self->finished)
    {
        *err = $err(RT, "pioGZDecompress already finished");
        return tr;
    }

    if (self->eof)
        return tr;

    while (rbuf.len > 0 && wbuf.len > 0)
    {
        self->strm.next_in = (Bytef *)rbuf.ptr;
        self->strm.avail_in = rbuf.len;
        self->strm.next_out = (Bytef *)wbuf.ptr;
        self->strm.avail_out = wbuf.len;

        rc = inflate(&self->strm, Z_NO_FLUSH);

        ft_bytes_consume(&wbuf, wbuf.len - self->strm.avail_out);
        ft_bytes_consume(&rbuf, rbuf.len - self->strm.avail_in);

        if (rc == Z_STREAM_END)
        {
            self->eof = true;
            break;
        }
        else if (rc != Z_OK)
        {
            *err = newGZError(self->strm.msg, rc);
            break;
        }
    }

    tr.produced += wlen - wbuf.len;
    tr.consumed += rlen - rbuf.len;
    return tr;
}

static size_t
pioGZDecompress_pioFltFinish(VSelf, ft_bytes_t wbuf, err_i *err)
{
    Self(pioGZDecompress);
    size_t	wlen = wbuf.len;
    int     rc;
    fobj_reset_err(err);

    if (self->finished)
        return 0;

    while (wbuf.len > 0 && !self->eof)
    {
        self->strm.avail_in = 0;
        self->strm.next_out = (Bytef *)wbuf.ptr;
        self->strm.avail_out = wbuf.len;

        rc = inflate(&self->strm, Z_SYNC_FLUSH);

        ft_bytes_consume(&wbuf, wbuf.len - self->strm.avail_out);

        if (rc == Z_STREAM_END)
        {
            self->eof = true;
        }
        else if (rc == Z_BUF_ERROR && self->ignoreTruncate)
        {
            self->eof = true;
        }
        else if (rc != Z_OK)
        {
            *err = newGZError(self->strm.msg, rc);
            break;
        }
    }

    if (self->eof && !self->finished)
    {
        rc = inflateEnd(&self->strm);
        ft_dbg_assert(rc == Z_OK);
        self->finished = true;
    }

    return wlen - wbuf.len;
}

static void
pioGZDecompress_fobjDispose(VSelf)
{
    Self(pioGZDecompress);
    int rc;

    if (!self->finished) {
        rc = inflateEnd(&self->strm);
        ft_dbg_assert(rc == Z_OK);
    }
}

static fobjStr*
pioGZDecompress_fobjRepr(VSelf)
{
    Self(pioGZCompress);
    return $S("pioGZDecompress");
}
#endif

/* Transform filter method */
/* Must count crc32 of new portion of data. No output needed */
static pioFltTransformResult
pioCRC32Counter_pioFltTransform(VSelf, ft_bytes_t rbuf, ft_bytes_t wbuf, err_i *err)
{
	Self(pioCRC32Counter);
	pioFltTransformResult  tr = {0, 0};
	fobj_reset_err(err);
	size_t copied = ft_min(wbuf.len, rbuf.len);

	if (interrupted)
		elog(ERROR, "interrupted during CRC calculation");

	COMP_CRC32C(self->crc, rbuf.ptr, copied);

	memmove(wbuf.ptr, rbuf.ptr, copied);

	tr.produced = copied;
	tr.consumed = copied;
	self->size += copied;

	return tr;
}

static err_i
pioCRC32Counter_pioFltInPlace(VSelf, ft_bytes_t rbuf)
{
	Self(pioCRC32Counter);

	COMP_CRC32C(self->crc, rbuf.ptr, rbuf.len);
	self->size += rbuf.len;

	return $noerr();
}

static size_t
pioCRC32Counter_pioFltFinish(VSelf, ft_bytes_t wbuf, err_i *err)
{
	Self(pioCRC32Counter);
	fobj_reset_err(err);

	FIN_CRC32C(self->crc);

	return 0;
}

pg_crc32
pioCRC32Counter_getCRC32(pioCRC32Counter* flt)
{
	return flt->crc;
}

int64_t
pioCRC32Counter_getSize(pioCRC32Counter* flt)
{
	return flt->size;
}

pioWriteFlush_i
pioDevNull_alloc(void)
{
	fobj_t				wrap;

	wrap = $alloc(pioDevNull);
	return bind_pioWriteFlush(wrap);
}

static size_t
pioDevNull_pioWrite(VSelf, ft_bytes_t buf, err_i *err)
{
	fobj_reset_err(err);
	return buf.len;
}

static err_i
pioDevNull_pioWriteFinish(VSelf)
{
	return $noerr();
}

pioCRC32Counter*
pioCRC32Counter_alloc(void)
{
	pioCRC32Counter		*res;
	pg_crc32			init_crc = 0;

	INIT_CRC32C(init_crc);

	res = $alloc(pioCRC32Counter, .crc = init_crc);

	return res;
}


err_i
pioCopyWithFilters(pioWriteFlush_i dest, pioRead_i src,
                   pioFilter_i *filters, int nfilters, size_t *copied)
{
    FOBJ_FUNC_ARP();
    size_t      _fallback_copied = 0;
    err_i       err = $noerr();
    err_i       rerr = $noerr();
    err_i       werr = $noerr();
    void*       buf;
    int         i;

    if (copied == NULL)
        copied = &_fallback_copied;

    if ($ifdef(err = , pioSetAsync, src.self) && $haserr(err))
        elog(ERROR, "Cannot enable async mode on source \"%s\": %s",
             $irepr(src), $errmsg(err));

    if ($ifdef(err = , pioSetAsync, dest.self) && $haserr(err))
        elog(ERROR, "Cannot enable async mode on destination \"%s\": %s",
             $irepr(dest), $errmsg(err));

    for (i = nfilters - 1; i >= 0; i--)
        dest = pioWrapWriteFilter(dest, filters[i], OUT_BUF_SIZE);

    buf = fobj_alloc_temp(OUT_BUF_SIZE);

    while (!$haserr(rerr) && !$haserr(werr))
    {
        size_t read_len = 0;
        size_t write_len = 0;

        read_len = $i(pioRead, src, ft_bytes(buf, OUT_BUF_SIZE), &rerr);

        if (read_len == 0)
            break;

        write_len = $i(pioWrite, dest, ft_bytes(buf, read_len), &werr);
        *copied += write_len;
        if (write_len != read_len)
        {
			werr = $err(SysErr, "Short write to destination file {path}: {writtenSz} < {wantedSz}",
					 path($irepr(dest)),
					 wantedSz(read_len), writtenSz(write_len));
        }
    }

    err = fobj_err_combine(rerr, werr);
    if ($haserr(err))
        return $iresult(err);

    /* pioWriteFinish will check for async error if destination was remote */
    err = $i(pioWriteFinish, dest);
    if ($haserr(err))
        err = $err(SysErr, "Cannot flush file {path}: {cause}",
                     path($irepr(dest)), cause(err.self));
    return $iresult(err);
}

size_t
pioReadFull(pioRead_i src, ft_bytes_t bytes, err_i* err)
{
	ft_bytes_t	b;
	size_t		r;
	fobj_reset_err(err);

	b = bytes;
	while (b.len)
	{
		r = $i(pioRead, src, b, err);
		Assert(r <= b.len);
		ft_bytes_consume(&b, r);
		if ($haserr(*err))
			break;
		if (r == 0)
			break;
	}
	return bytes.len - b.len;
}

void
init_pio_line_reader(pio_line_reader *r, pioRead_i source, size_t max_length) {
	r->source = $iref(source);
	r->buf = ft_bytes_alloc(max_length);
	r->rest = ft_bytes(NULL, 0);
}

void
deinit_pio_line_reader(pio_line_reader *r)
{
	$idel(&r->source);
	ft_bytes_free(&r->buf);
	r->rest = ft_bytes(NULL, 0);
}

ft_bytes_t
pio_line_reader_getline(pio_line_reader *r, err_i *err)
{
	ft_bytes_t res;
	ft_bytes_t tmp;
	size_t     sz;
	char       last;

	fobj_reset_err(err);

retry:
	res = ft_bytes_shift_line(&r->rest);
	/* if we got too long line */
	if (res.len == r->buf.len)
	{
		*err = $err(RT, "Line doesn't fit buffer of size {size}",
				   size(r->buf.len));
		/* restore rest to produce error again next time */
		r->rest = r->buf;
		return ft_bytes(NULL, 0);
	}

	last = res.len != 0 ? res.ptr[res.len-1] : 0;
	/* not first time and definitely reached end of line */
	if (res.len != 0 && (last == '\n' || last == '\r'))
		return res;

	if (res.ptr != NULL)
		memmove(r->buf.ptr, res.ptr, res.len);

	r->rest = ft_bytes(r->buf.ptr, res.len);
	tmp = r->buf;
	ft_bytes_consume(&tmp, res.len);
	sz = $i(pioRead, r->source, tmp, err);
	r->rest.len += sz;
	if ($haserr(*err))
		return ft_bytes(NULL, 0);
	/* reached end of file */
	if (sz == 0)
	{
		res = r->rest;
		r->rest = ft_bytes(NULL, 0);
		return res;
	}
	goto retry;
}

typedef struct pioRemotePagesIterator
{
	bool valid;
	BlockNumber n_blocks;
} pioRemotePagesIterator;

typedef struct pioLocalPagesIterator
{
	BlockNumber	blknum;
	BlockNumber	lastblkn;
	BlockNumber n_blocks;

	bool		just_validate;
	int			segno;
	datapagemap_t map;
	FILE		*in;
	void		*buf;
	const char	*from_fullpath;
	/* prev_backup_start_lsn */
	XLogRecPtr	start_lsn;

	CompressAlg	calg;
	int			clevel;
	uint32		checksum_version;
} pioLocalPagesIterator;

#define kls__pioLocalPagesIterator	iface__pioPagesIterator, iface(pioPagesIterator), \
		mth(fobjDispose)
fobj_klass(pioLocalPagesIterator);

#define kls__pioRemotePagesIterator	iface__pioPagesIterator, iface(pioPagesIterator)
fobj_klass(pioRemotePagesIterator);

static pioPagesIterator_i
pioRemoteDrive_pioIteratePages(VSelf, path_t from_fullpath,
								int segno, datapagemap_t pagemap,
								XLogRecPtr start_lsn,
								CompressAlg calg, int clevel,
								uint32 checksum_version, bool just_validate, err_i *err)
{
	Self(pioRemoteDrive);
	fobj_t iter = {0};
	fio_header hdr = {.cop = FIO_ITERATE_PAGES};
	ft_strbuf_t buf = ft_strbuf_zero();
	fio_iterate_pages_request req = {
			.segno = segno,
			.pagemaplen = pagemap.bitmapsize,
			.start_lsn = start_lsn,
			.calg = calg,
			.clevel = clevel,
			.checksum_version = checksum_version,
			.just_validate = just_validate,
	};

	ft_strbuf_catbytes(&buf, ft_bytes(&hdr, sizeof(hdr)));
	ft_strbuf_catbytes(&buf, ft_bytes(&req, sizeof(req)));
	ft_strbuf_catbytes(&buf, ft_bytes(pagemap.bitmap, pagemap.bitmapsize));
	ft_strbuf_catc(&buf, from_fullpath);
	ft_strbuf_cat1(&buf, '\0');

	((fio_header*)buf.ptr)->size = buf.len - sizeof(fio_header);

	IO_CHECK(fio_write_all(fio_stdout, buf.ptr, buf.len), buf.len);

	ft_strbuf_free(&buf);

	iter = $alloc(pioRemotePagesIterator, .valid = true);

	return bind_pioPagesIterator(iter);
}

static err_i
pioRemotePagesIterator_pioNextPage(VSelf, PageIteratorValue *value)
{
	Self(pioRemotePagesIterator);

	fio_header hdr;

	value->compressed_size = 0;

	if (!self->valid) {
		value->page_result = PageIsTruncated;
		return $noerr();
	}
	IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
	if (hdr.cop == FIO_PIO_ERROR)
	{
		self->valid = false;
		return fio_receive_pio_err(&hdr);
	}
	else if (hdr.cop == FIO_ITERATE_EOF)
	{
		ft_assert(hdr.size == sizeof(BlockNumber));
		self->valid = false;
		IO_CHECK(fio_read_all(fio_stdin, &self->n_blocks, sizeof(self->n_blocks)), sizeof(self->n_blocks));
		value->page_result = PageIsTruncated;
		return $noerr();
	}
	else if (hdr.cop == FIO_ITERATE_DATA)
	{
		Assert(hdr.size <= sizeof(PageIteratorValue));
		memset(value, 0, sizeof(PageIteratorValue));
		IO_CHECK(fio_read_all(fio_stdin, (void*)value, hdr.size), hdr.size);

		return $noerr();
	}
	self->valid = false;
	return $err(RT, "Unexpected operation {intCode} in remote pioNextPage",
				intCode(hdr.cop));
}

static BlockNumber
pioRemotePagesIterator_pioFinalPageN(VSelf)
{
	Self(pioRemotePagesIterator);
	return self->n_blocks;
}

pioPagesIterator_i
doIteratePages_impl(pioIteratePages_i drive, struct doIteratePages_params p)
{
	datapagemap_t pagemap = {0};
	fobj_reset_err(p.err);

	/*
	 * If page map is empty or file is not present in destination directory,
	 * then copy backup all pages of the relation.
	 */
	if (p.file->pagemap.bitmapsize != PageBitmapIsEmpty &&
		!p.file->pagemap_isabsent && p.file->exists_in_prev &&
		p.file->pagemap.bitmap)
		pagemap = p.file->pagemap;

	/* Skip page if page lsn is less than START_LSN of parent backup. */
	if (p.start_lsn != InvalidXLogRecPtr)
	{
		if (!p.file->exists_in_prev)
			p.start_lsn = InvalidXLogRecPtr;
		if (p.backup_mode != BACKUP_MODE_DIFF_DELTA &&
		    p.backup_mode != BACKUP_MODE_DIFF_PTRACK)
			p.start_lsn = InvalidXLogRecPtr;
	}

	return $i(pioIteratePages, drive,
			  .path = p.from_fullpath,
			  .segno = p.file->segno,
			  .pagemap = pagemap,
			  .start_lsn = p.start_lsn,
			  .calg = p.calg,
			  .clevel = p.clevel,
			  .checksum_version = p.checksum_version,
			  .just_validate = p.just_validate,
			  .err = p.err);
}

static pioPagesIterator_i
pioLocalDrive_pioIteratePages(VSelf, path_t path,
							  int segno, datapagemap_t pagemap,
							  XLogRecPtr start_lsn,
							  CompressAlg calg, int clevel,
							  uint32 checksum_version, bool just_validate, err_i *err)
{
	Self(pioLocalDrive);
	fobj_t	iter = {0};
	BlockNumber n_blocks;
	FILE   *in;
	void   *buf;
	size_t  bufsz;
	int		fd;
	struct stat st;

	fobj_reset_err(err);

	in = fopen(path, PG_BINARY_R);
	if (!in)
	{
		pioPagesIterator_i ret = {0};
		*err = $syserr(errno, "Cannot iterate pages");
		return ret;
	}

	fd = fileno(in);
	if (fstat(fd, &st) == -1)
	{
		fclose(in);
		*err = $syserr(errno, "Cannot stat datafile");
		return $null(pioPagesIterator);
	}

	bufsz = pagemap.bitmapsize > 0 ? SMALL_CHUNK_SIZE : MEDIUM_CHUNK_SIZE;
	buf = ft_malloc(bufsz);
	setvbuf(in, buf, _IOFBF, bufsz);

	/*
	 * Compute expected number of blocks in the file.
	 * NOTE This is a normal situation, if the file size has changed
	 * since the moment we computed it.
	 */
	n_blocks = ft_div_i64u32_to_i32(st.st_size, BLCKSZ);

	iter = $alloc(pioLocalPagesIterator,
				  .segno = segno,
				  .n_blocks = n_blocks,
				  .just_validate = just_validate,
				  .from_fullpath = path,
				  .map = pagemap,
				  .in = in,
				  .buf = buf,
				  .start_lsn = start_lsn,
				  .calg = calg,
				  .clevel = clevel,
				  .checksum_version = checksum_version);

	return bind_pioPagesIterator(iter);
}

static void
pioLocalPagesIterator_fobjDispose(VSelf)
{
	Self(pioLocalPagesIterator);

	if (self->buf) ft_free(self->buf);
	if (self->in) fclose(self->in);
}

static int32 prepare_page(pioLocalPagesIterator *iter,
						  BlockNumber blknum,
						  Page page,
						  PageState *page_st);

static err_i
pioLocalPagesIterator_pioNextPage(VSelf, PageIteratorValue *value)
{
	FOBJ_FUNC_ARP();
	Self(pioLocalPagesIterator);
	char page_buf[BLCKSZ];
	BlockNumber blknum;
	BlockNumber n_blocks;
	int rc = PageIsOk;

	blknum = self->blknum;
	value->compressed_size = 0;
	if (self->blknum >= self->n_blocks)
		goto truncated;

	/* next block */
	if (self->map.bitmapsize &&
		!datapagemap_first(self->map, &blknum))
	{
		self->blknum = self->n_blocks;
		goto truncated;
	}

	value->blknum = blknum;
	self->blknum = blknum+1;

	rc = prepare_page(self, blknum, page_buf, &value->state);
	value->page_result = rc;
	if (rc == PageIsTruncated)
		goto re_stat;
	self->lastblkn = blknum+1;
	if (rc == PageIsOk && !self->just_validate)
	{
		value->compressed_size = compress_page(value->compressed_page, BLCKSZ,
											   value->blknum, page_buf, self->calg,
											   self->clevel, self->from_fullpath);
	}
	return $noerr();

re_stat:
	{
		/*
		 * prepare_page found file is shorter than expected.
		 * Lets re-investigate its length.
		 */
		struct stat st;
		int fd = fileno(self->in);
		if (fstat(fd, &st) < 0)
			return $syserr(errno, "Re-stat-ting file {path}",
						   path(self->from_fullpath));
		n_blocks = ft_div_i64u32_to_i32(st.st_size, BLCKSZ);
		/* we should not "forget" already produced pages */
		if (n_blocks < self->lastblkn)
			n_blocks = self->lastblkn;
		if (n_blocks < self->n_blocks)
			self->n_blocks = blknum;
	}
truncated:
	value->page_result = PageIsTruncated;
	return $noerr();
}

static BlockNumber
pioLocalPagesIterator_pioFinalPageN(VSelf)
{
	Self(pioLocalPagesIterator);
	return self->n_blocks;
}

/*
 * Retrieves a page taking the backup mode into account
 * and writes it into argument "page". Argument "page"
 * should be a pointer to allocated BLCKSZ of bytes.
 *
 * Prints appropriate warnings/errors/etc into log.
 * Returns:
 *                 PageIsOk(0) if page was successfully retrieved
 *         PageIsTruncated(-1) if the page was truncated
 *         SkipCurrentPage(-2) if we need to skip this page,
 *                                only used for DELTA and PTRACK backup
 *         PageIsCorrupted(-3) if the page checksum mismatch
 *                                or header corruption,
 *                                only used for checkdb
 *                                TODO: probably we should always
 *                                      return it to the caller
 */
static int32
prepare_page(pioLocalPagesIterator *iter, BlockNumber blknum, Page page, PageState *page_st)
{
	int			try_again = PAGE_READ_ATTEMPTS;
	bool		page_is_valid = false;
	const char *from_fullpath = iter->from_fullpath;
	BlockNumber absolute_blknum = iter->segno * RELSEG_SIZE + blknum;
	int rc = 0;

	/* check for interrupt */
	if (interrupted || thread_interrupted)
		elog(ERROR, "Interrupted during page reading");

	/*
	 * Read the page and verify its header and checksum.
	 * Under high write load it's possible that we've read partly
	 * flushed page, so try several times before throwing an error.
	 */
	while (!page_is_valid && try_again--)
	{
		int read_len = fseeko(iter->in, (off_t)blknum * BLCKSZ, SEEK_SET);
		if (read_len == 0) /* seek is successful */
		{
			/* read the block */
			read_len = fread(page, 1, BLCKSZ, iter->in);
			if (read_len == 0 && ferror(iter->in))
				read_len = -1;
		}

		/* The block could have been truncated. It is fine. */
		if (read_len == 0)
		{
			elog(VERBOSE, "Cannot read block %u of \"%s\": "
						  "block truncated", blknum, from_fullpath);
			return PageIsTruncated;
		}
		else if (read_len < 0)
			elog(ERROR, "Cannot read block %u of \"%s\": %s",
				 blknum, from_fullpath, strerror(errno));
		else if (read_len != BLCKSZ)
			elog(WARNING, "Cannot read block %u of \"%s\": "
						  "read %i of %d, try again",
				 blknum, from_fullpath, read_len, BLCKSZ);
		else
		{
			/* We have BLCKSZ of raw data, validate it */
			rc = validate_one_page(page, absolute_blknum,
								   InvalidXLogRecPtr, page_st,
								   iter->checksum_version);
			switch (rc)
			{
				case PAGE_IS_ZEROED:
					elog(VERBOSE, "File: \"%s\" blknum %u, empty page", from_fullpath, blknum);
					return PageIsOk;

				case PAGE_IS_VALID:
					/* in DELTA or PTRACK modes we must compare lsn */
					if (iter->start_lsn != InvalidXLogRecPtr)
						page_is_valid = true;
					else
						return PageIsOk;
					break;

				case PAGE_HEADER_IS_INVALID:
					elog(VERBOSE, "File: \"%s\" blknum %u have wrong page header, try again",
						 from_fullpath, blknum);
					break;

				case PAGE_CHECKSUM_MISMATCH:
					elog(VERBOSE, "File: \"%s\" blknum %u have wrong checksum, try again",
						 from_fullpath, blknum);
					break;
				default:
					Assert(false);
			}
		}
		/* avoid re-reading once buffered data, flushing on further attempts, see PBCKP-150 */
		fflush(iter->in);
	}

	/*
	 * If page is not valid after PAGE_READ_ATTEMPTS attempts to read it
	 * throw an error.
	 */
	if (!page_is_valid)
	{
		int elevel = ERROR;
		char *errormsg = NULL;

		/* Get the details of corruption */
		if (rc == PAGE_HEADER_IS_INVALID)
			get_header_errormsg(page, &errormsg);
		else if (rc == PAGE_CHECKSUM_MISMATCH)
			get_checksum_errormsg(page, &errormsg,
								  absolute_blknum);

		/* Error out in case of merge or backup without ptrack support;
		 * issue warning in case of checkdb or backup with ptrack support
		 */
		if (iter->just_validate)
			elevel = WARNING;

		if (errormsg)
			elog(elevel, "Corruption detected in file \"%s\", block %u: %s",
				 from_fullpath, blknum, errormsg);
		else
			elog(elevel, "Corruption detected in file \"%s\", block %u",
				 from_fullpath, blknum);

		pg_free(errormsg);
		return PageIsCorrupted;
	}

	/* Checkdb not going futher */
	if (iter->just_validate)
		return PageIsOk;

	/*
	 * Skip page if page lsn is less than START_LSN of parent backup.
	 * Nullified pages must be copied by DELTA backup, just to be safe.
	 */
	if (page_st->lsn > 0 &&
		page_st->lsn < iter->start_lsn)
	{
		elog(VERBOSE, "Skipping blknum %u in file: \"%s\", page_st->lsn: %X/%X, prev_backup_start_lsn: %X/%X",
			 blknum, from_fullpath,
			 (uint32) (page_st->lsn >> 32), (uint32) page_st->lsn,
			 (uint32) (iter->start_lsn >> 32), (uint32) iter->start_lsn);
		return SkipCurrentPage;
	}

	return PageIsOk;
}


fobj_klass_handle(pioLocalPagesIterator);
fobj_klass_handle(pioRemotePagesIterator);

fobj_klass_handle(pioFile);
fobj_klass_handle(pioLocalDrive);
fobj_klass_handle(pioRemoteDrive);
fobj_klass_handle(pioLocalFile, inherits(pioFile), mth(fobjDispose, fobjRepr));
fobj_klass_handle(pioRemoteFile, inherits(pioFile), mth(fobjDispose, fobjRepr));
fobj_klass_handle(pioLocalWriteFile);
fobj_klass_handle(pioRemoteWriteFile);
fobj_klass_handle(pioWriteFilter, mth(fobjDispose, fobjRepr));
fobj_klass_handle(pioReadFilter, mth(fobjDispose, fobjRepr));
fobj_klass_handle(pioDevNull);
fobj_klass_handle(pioCRC32Counter);

#ifdef HAVE_LIBZ
fobj_klass_handle(pioGZCompress, mth(fobjRepr));
fobj_klass_handle(pioGZDecompress, mth(fobjRepr));
#endif

void
init_pio_objects(void)
{
    FOBJ_FUNC_ARP();

    localDrive = bindref_pioDBDrive($alloc(pioLocalDrive));
    remoteDrive = bindref_pioDBDrive($alloc(pioRemoteDrive));
}
