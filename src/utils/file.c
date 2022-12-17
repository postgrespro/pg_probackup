#include <stdio.h>
#include <unistd.h>

#include "pg_probackup.h"
#include <signal.h>

#include "file.h"
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

#define PIO_DIR_REMOTE_BATCH 100

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
	bool      sync;
};

struct __attribute__((packed)) fio_req_open_write {
	uint32_t  permissions;
	bool exclusive;
	bool sync;
};

#if defined(WIN32)
#undef open
#undef fopen
#endif

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

	if (pio_errno)
		return $err(SysErr, "(remote) {causeStr}",
					causeStr(err_msg), errNo(pio_errno));

	return $err(RT, "(remote) {causeStr}", causeStr(err_msg));
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

	fobj_t objs[FIO_FDMAX] = {0};
	err_i  async_errs[FIO_FDMAX] = {0};

	size_t buf_size = 128*1024;
	char* buf = (char*)pgut_malloc(buf_size);
	fio_header hdr;
	pioDBDrive_i drive;
	pio_stat_t st;
	ft_bytes_t bytes;
	ft_str_t   path;
	ft_str_t   path2;
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
			bytes = ft_bytes(buf, hdr.size);
			path = ft_bytes_shift_zt(&bytes);
			path2 = ft_bytes_shift_zt(&bytes);
			hdr.arg = (int)$i(pioFilesAreSame, drive, path.ptr, path2.ptr);
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
			path = ft_bytes_shift_zt(&bytes);
			err = $i(pioWriteFile, drive, .path = path.ptr,
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
		  case FIO_SEEK:   /* Set current position in file */
			fio_seek_impl(fd[hdr.handle], hdr.arg);
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
					.sync = req->sync,
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
					.sync = req->sync,
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

			err = $(pioWrite, objs[hdr.handle], ft_bytes(buf, hdr.size));
			if ($haserr(err))
				$iset(&async_errs[hdr.handle], err);
			break;
		}
		case PIO_WRITE_COMPRESSED_ASYNC:
		{
			err_i  err;

			ft_assert(hdr.handle >= 0);
			ft_assert(objs[hdr.handle] != NULL);

			err = $(pioWriteCompressed, objs[hdr.handle], ft_bytes(buf, hdr.size),
								  .compress_alg = hdr.arg);
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
		case PIO_TRUNCATE:
		{
			err_i  err;
			uint64_t offs;

			ft_assert(hdr.handle >= 0);
			ft_assert(objs[hdr.handle] != NULL);
			ft_assert(hdr.size == sizeof(uint64_t));

			memcpy(&offs, buf, sizeof(uint64_t));

			err = $(pioTruncate, objs[hdr.handle], offs);
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
		case PIO_DIR_OPEN:
		{
			ft_assert(hdr.handle >= 0 && hdr.handle < FIO_FDMAX);
			ft_assert(objs[hdr.handle] == NULL);
			pioDirIter_i iter;

			iter = $i(pioOpenDir, drive, buf, .err = &err);
			if ($haserr(err))
				fio_send_pio_err(out, err);
			else
			{
				objs[hdr.handle] = $ref(iter.self);
				hdr.size = 0;
				IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			}
			break;
		}
		case PIO_DIR_NEXT:
		{
			ft_assert(hdr.handle >= 0 && hdr.handle < FIO_FDMAX);
			ft_assert(objs[hdr.handle] != NULL);
			ft_strbuf_t  stats = ft_strbuf_zero();
			ft_strbuf_t  names = ft_strbuf_zero();
			pio_dirent_t dirent;
			int          n;

			for (n = 0; n < PIO_DIR_REMOTE_BATCH; n++)
			{
				dirent = $(pioDirNext, objs[hdr.handle], .err = &err);
				if ($haserr(err))
					break;
				if (dirent.stat.pst_kind == PIO_KIND_UNKNOWN)
					break;
				ft_strbuf_catbytes(&stats, FT_BYTES_FOR(dirent.stat));
				ft_strbuf_cat_zt(&names, dirent.name);
			}

			if ($haserr(err))
				fio_send_pio_err(out, err);
			else
			{
				hdr.arg = n;
				hdr.size = stats.len + names.len;
				IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
				if (n > 0)
				{
					IO_CHECK(fio_write_all(out, stats.ptr, stats.len), stats.len);
					IO_CHECK(fio_write_all(out, names.ptr, names.len), names.len);
				}
			}
			ft_strbuf_free(&stats);
			ft_strbuf_free(&names);
			break;
		}
		case PIO_IS_DIR_EMPTY:
		{
			bool is_empty;

			is_empty = $i(pioIsDirEmpty, drive, buf, .err = &err);
			if ($haserr(err))
				fio_send_pio_err(out, err);
			else
			{
				hdr.size = 0;
				hdr.arg = is_empty;

				IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
			}
			break;
		}
		case PIO_CLOSE:
		{
			ft_assert(hdr.handle >= 0);
			ft_assert(objs[hdr.handle] != NULL);

			err = $(pioClose, objs[hdr.handle]);
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


typedef struct pio_recursive_dir {
	pioDrive_i    drive;
	ft_arr_cstr_t recurse;
	ft_str_t      root;
	ft_str_t      parent;
	pioDirIter_i  iter;
	pio_dirent_t  dirent;
	bool          dont_recurse_current;
	ft_strbuf_t   namebuf;
} pioRecursiveDir;
#define kls__pioRecursiveDir mth(fobjDispose)
fobj_klass(pioRecursiveDir);

pio_recursive_dir_t*
pio_recursive_dir_alloc(pioDrive_i drive, path_t root, err_i *err)
{
	pioDirIter_i iter;
	fobj_reset_err(err);

	iter = $i(pioOpenDir, drive, root, err);
	if ($haserr(*err))
		return NULL;

	return $alloc(pioRecursiveDir, .drive = drive,
				  .root = ft_strdupc(root),
				  .parent = ft_strdupc(""),
				  .iter = $iref(iter),
				  .recurse = ft_arr_init(),
				  .namebuf = ft_strbuf_zero());
}

static pio_dirent_t
pio_recursive_dir_next_impl(pio_recursive_dir_t* self, err_i* err)
{
	if (self->dirent.stat.pst_kind == PIO_KIND_DIRECTORY &&
	    !self->dont_recurse_current)
	{
		ft_arr_cstr_push(&self->recurse, ft_strdup(self->dirent.name).ptr);
	}

	ft_strbuf_reset_for_reuse(&self->namebuf);
	self->dont_recurse_current = false;

	self->dirent = $i(pioDirNext, self->iter, .err = err);
	if ($haserr(*err))
		return self->dirent;

	if (self->dirent.stat.pst_kind != PIO_KIND_UNKNOWN)
	{
		ft_strbuf_cat(&self->namebuf, self->parent);
		ft_strbuf_cat_path(&self->namebuf, self->dirent.name);
		self->dirent.name = ft_strbuf_ref(&self->namebuf);
		return self->dirent;
	}

	*err = $i(pioClose, self->iter);
	$idel(&self->iter);
	if ($haserr(*err))
		return self->dirent;

next_dir:
	if (self->recurse.len == 0)
		return self->dirent;

	ft_str_free(&self->parent);
	self->parent = ft_cstr(ft_arr_cstr_pop(&self->recurse));

	ft_strbuf_cat(&self->namebuf, self->root);
	ft_strbuf_cat_path(&self->namebuf, self->parent);

	self->iter = $i(pioOpenDir, self->drive, .path = self->namebuf.ptr,
					.err = err);
	if ($haserr(*err))
	{
		/* someone deleted dir under our feet */
		if (getErrno(*err) == ENOENT)
		{
			*err = $noerr();
			goto next_dir;
		}

		return self->dirent;
	}

	$iref(self->iter);

	return pio_recursive_dir_next_impl(self, err);
}

pio_dirent_t
pio_recursive_dir_next(pio_recursive_dir_t* self, err_i* err)
{
	FOBJ_FUNC_ARP();
	pio_dirent_t ent;
	fobj_reset_err(err);

	ent = pio_recursive_dir_next_impl(self, err);
	$iresult(*err);
	return ent;
}

void
pio_recursive_dir_dont_recurse_current(pio_recursive_dir_t* self)
{
	ft_assert(self->dirent.stat.pst_kind == PIO_KIND_DIRECTORY);
	self->dont_recurse_current = true;
}

static void
pioRecursiveDir_fobjDispose(VSelf)
{
	Self(pioRecursiveDir);

	if ($notNULL(self->iter))
		$i(pioClose, self->iter);
	$idel(&self->iter);
	ft_str_free(&self->root);
	ft_str_free(&self->parent);
	ft_arr_cstr_free(&self->recurse);
	ft_strbuf_free(&self->namebuf);
}

void
pio_recursive_dir_free(pio_recursive_dir_t* self)
{
	/* we are releasing bound resources,
	 * but self will be dealloced in FOBJ's ARP */
	pioRecursiveDir_fobjDispose(self);
}
fobj_klass_handle(pioRecursiveDir);

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
    bool	closed;
} pioFile;
#define kls__pioFile	mth(fobjDispose)
fobj_klass(pioFile);

typedef struct pioLocalFile
{
    pioFile	p;
    int		fd;
} pioLocalFile;
#define kls__pioLocalFile	iface__pioReader, iface(pioReader, pioReadStream)
fobj_klass(pioLocalFile);

typedef struct pioLocalWriteFile
{
	ft_str_t path;
	ft_str_t path_tmp;
	FILE*	 fl;
	ft_bytes_t buf;
	bool     use_temp;
	bool     delete_in_dispose;
	bool     sync;
} pioLocalWriteFile;
#define kls__pioLocalWriteFile	iface__pioDBWriter, mth(fobjDispose), \
								iface(pioWriteCloser, pioDBWriter)
fobj_klass(pioLocalWriteFile);

typedef struct pioLocalDir
{
	ft_str_t path;
	DIR*     dir;
	ft_strbuf_t name_buf;
} pioLocalDir;
#define kls__pioLocalDir iface__pioDirIter, iface(pioDirIter), mth(fobjDispose)
fobj_klass(pioLocalDir);

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
#define kls__pioRemoteFile	iface__pioReader, \
                            iface(pioReader, pioReadStream), \
                            mth(pioSetAsync, pioAsyncRead)
fobj_klass(pioRemoteFile);

typedef struct pioRemoteWriteFile {
	ft_str_t	path;
	int			handle;
	bool		did_async;
} pioRemoteWriteFile;
#define kls__pioRemoteWriteFile	iface__pioDBWriter, mth(fobjDispose), \
								iface(pioWriteCloser, pioDBWriter)
fobj_klass(pioRemoteWriteFile);

typedef struct pioRemoteDir
{
	ft_str_t path;
	int      handle;
	int		 pos;
	ft_bytes_t      names_buf;
	ft_arr_dirent_t entries;
} pioRemoteDir;
#define kls__pioRemoteDir iface__pioDirIter, iface(pioDirIter), mth(fobjDispose)
fobj_klass(pioRemoteDir);

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

typedef struct pioGZDecompressWrapperObj {
	bool		ignoreTruncate;
} pioGZDecompressWrapperObj;

#define kls__pioGZCompress	iface__pioFilter, mth(fobjDispose), iface(pioFilter)
fobj_klass(pioGZCompress);
#define kls__pioGZDecompress	iface__pioFilter, mth(fobjDispose), iface(pioFilter)
fobj_klass(pioGZDecompress);
#define kls__pioGZDecompressWrapperObj mth(pioWrapRead)
fobj_klass(pioGZDecompressWrapperObj);
#endif

typedef struct pioReSeekableReader {
	pioReader_i           reader;
	pioRead_i             wrapped;
	pioWrapRead_i         wrapper;
	int64_t               pos;
	bool                  closed;
	bool                  had_err;
} pioReSeekableReader;
#define kls__pioReSeekableReader iface__pioReader, mth(fobjDispose)
fobj_klass(pioReSeekableReader);

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

static pioReader_i
pioLocalDrive_pioOpenRead(VSelf, path_t path, err_i *err)
{
    int	fd;
    fobj_reset_err(err);
    fobj_t file;

	fd = open(path, O_RDONLY);
    if (fd < 0)
    {
        *err = $syserr(errno, "Cannot open file {path:q}", path(path));
        return (pioReader_i){NULL};
    }

    file = $alloc(pioLocalFile, .fd = fd,
                  .p = { .path = ft_cstrdup(path) } );
    return $bind(pioReader, file);
}

static pioReadStream_i
pioLocalDrive_pioOpenReadStream(VSelf, path_t path, err_i *err)
{
	Self(pioLocalDrive);
	return $reduce(pioReadStream, $(pioOpenRead, self, path, .err = err));
}

static pioWriteCloser_i
pioLocalDrive_pioOpenRewrite(VSelf, path_t path, int permissions,
						     bool binary, bool use_temp, bool sync, err_i *err)
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
				 .delete_in_dispose = true,
				 .fl = fl,
				 .sync = sync,
				 .buf = buf);
	return $bind(pioWriteCloser, res);
}

static pioDBWriter_i
pioLocalDrive_pioOpenWrite(VSelf, path_t path, int permissions,
						   bool exclusive, bool sync, err_i *err)
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
				 .delete_in_dispose = exclusive,
				 .fl = fl,
				 .sync = sync,
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

static pioDirIter_i
pioLocalDrive_pioOpenDir(VSelf, path_t path, err_i* err)
{
	Self(pioLocalDrive);
	DIR* dir;
	fobj_reset_err(err);

	dir = opendir(path);
	if (dir == NULL)
	{
		*err = $syserr(errno, "Cannot open dir {path:q}", path(path));
		return $null(pioDirIter);
	}

	return $bind(pioDirIter,
				 $alloc(pioLocalDir,
						.path = ft_strdupc(path),
						.dir = dir));
}

static bool
pioLocalDrive_pioIsDirEmpty(VSelf, path_t path, err_i* err)
{
	Self(pioLocalDrive);
	DIR* dir;
	struct dirent *dent;
	bool is_empty = true;
	fobj_reset_err(err);

	dir = opendir(path);
	if (dir == NULL)
	{
		if (errno == ENOENT)
			return true;
		*err = $syserr(errno, "Cannot open dir {path:q}", path(path));
		return false;
	}

	while ((dent = readdir(dir)) != NULL)
	{
		if (strcmp(dent->d_name, ".") == 0)
			continue;
		if (strcmp(dent->d_name, "..") == 0)
			continue;
		is_empty = false;
		break;
	}

	if (errno)
		*err = $syserr(errno, "Couldn't read dir {path:q}", path(path));

	closedir(dir);

	return is_empty;
}

static void
pioLocalDrive_pioRemoveDir(VSelf, const char *root, bool root_as_well) {
    FOBJ_FUNC_ARP();
    Self(pioLocalDrive);
	char full_path[MAXPGPATH];
	ft_arr_cstr_t dirs = ft_arr_init();
	ft_arr_cstr_t files = ft_arr_init();
	char *dirname;
	char *filename;
	DIR* dir;
	struct dirent *dirent;
	struct stat st;
	size_t i;

	/* note: we don't dup root, so will not free it */
	ft_arr_cstr_push(&dirs, (char*)root);

	for (i = 0; i < dirs.len; i++) /* note that dirs.len will grow */
	{
		dirname = dirs.ptr[i];
		dir = opendir(dirname);

		if (dir == NULL)
		{
			if (errno == ENOENT)
			{
				elog(WARNING, "Dir \"%s\" disappeared", dirname);
				dirs.ptr[i] = NULL;
				if (i != 0)
					ft_free(dirname);
				continue;
			}
			else
				elog(ERROR, "Cannot open dir \"%s\": %m", dirname);
		}

		for(errno=0; (dirent = readdir(dir)) != NULL; errno=0)
		{
			if (strcmp(dirent->d_name, ".") == 0 ||
				strcmp(dirent->d_name, "..") == 0)
				continue;

			join_path_components(full_path, dirname, dirent->d_name);
			if (stat(full_path, &st))
			{
				if (errno == ENOENT)
				{
					elog(WARNING, "File \"%s\" disappeared", full_path);
					continue;
				}
				elog(ERROR, "Could not stat \"%s\": %m", full_path);
			}

			if (S_ISDIR(st.st_mode))
				ft_arr_cstr_push(&dirs, ft_cstrdup(full_path));
			else
				ft_arr_cstr_push(&files, ft_cstrdup(dirent->d_name));
		}
		if (errno)
			elog(ERROR, "Could not readdir \"%s\": %m", full_path);
		closedir(dir);

		while (files.len > 0)
		{
			filename = ft_arr_cstr_pop(&files);
			join_path_components(full_path, dirname, filename);
			ft_free(filename);

			if (progress)
				elog(INFO, "Progress: delete file \"%s\"", full_path);
			if (remove_file_or_dir(full_path) != 0)
			{
				if (errno == ENOENT)
					elog(WARNING, "File \"%s\" disappeared", full_path);
				else
					elog(ERROR, "Could not remove \"%s\": %m", full_path);
			}
		}
	}

	while (dirs.len > 0)
	{
		dirname = ft_arr_cstr_pop(&dirs);
		if (dirname == NULL)
			continue;

		if (dirs.len == 0 && !root_as_well)
			break;

		if (progress)
			elog(INFO, "Progress: delete dir \"%s\"", full_path);
		if (remove_file_or_dir(dirname) != 0)
		{
			if (errno == ENOENT)
				elog(WARNING, "Dir \"%s\" disappeared", full_path);
			else
				elog(ERROR, "Could not remove \"%s\": %m", full_path);
		}

		if (dirs.len != 0) /* we didn't dup root, so don't free it */
			ft_free(dirname);
	}

	ft_arr_cstr_free(&dirs);
	ft_arr_cstr_free(&files);
}

static ft_bytes_t
pioLocalDrive_pioReadFile(VSelf, path_t path, bool binary, err_i* err)
{
	Self(pioLocalDrive);
	FILE*       fl = NULL;
	pio_stat_t	st;
	ft_bytes_t	res = ft_bytes(NULL, 0);
	size_t		amount;

	fobj_reset_err(err);

	st = $(pioStat, self, .path = path, .follow_symlink = true, .err = err);
	if ($haserr(*err))
	{
		return res;
	}
	if (st.pst_kind != PIO_KIND_REGULAR)
	{
		*err = $err(RT, "File {path:q} is not regular: {kind}", path(path),
					kind(pio_file_kind2str(st.pst_kind, path)),
					errNo(EACCES));
		return res;
	}

	/* forbid too large file because of remote protocol */
	if (st.pst_size >= PIO_READ_WRITE_FILE_LIMIT)
	{
		*err = $err(RT, "File {path:q} is too large: {size}", path(path),
					size(st.pst_size), errNo(EFBIG));
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
	fl = fopen(path, binary ? "rb" : "r");
	if (fl == NULL)
	{
		*err = $syserr(errno, "Opening file {path:q}", path(path));
		ft_bytes_free(&res);
		return res;
	}

	amount = fread(res.ptr, 1, res.len, fl);
	if (ferror(fl))
	{
		*err = $syserr(errno, "Opening file {path:q}", path(path));
		fclose(fl);
		ft_bytes_free(&res);
		return res;
	}

	fclose(fl);

	if (amount != st.pst_size)
	{
		ft_bytes_free(&res);
		*err = $err(RT, "File {path:q} is truncated while reading",
					path(path), errNo(EBUSY));
		return res;
	}

	res.len = amount;
	if (!binary)
		res.ptr[amount] = 0;

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

	err = $i(pioWrite, fl, content);
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
pioLocalFile_pioClose(VSelf)
{
    Self(pioLocalFile);
    err_i	err = $noerr();
    int r;

    ft_assert(self->fd >= 0, "Closed file abused \"%s\"", self->p.path);

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

static err_i
pioLocalFile_pioSeek(VSelf, uint64_t offs)
{
	Self(pioLocalFile);

	ft_assert(self->fd >= 0, "Closed file abused \"%s\"", self->p.path);

	off_t pos = lseek(self->fd, offs, SEEK_SET);

	if (pos == (off_t)-1)
		return $syserr(errno, "Can not seek to {offs} in file {path:q}", offs(offs), path(self->p.path));
	ft_assert(pos == offs);
	return $noerr();
}

static fobjStr*
pioLocalFile_fobjRepr(VSelf)
{
    Self(pioLocalFile);
    return $fmt("pioLocalFile({path:q}, fd:{fd}",
                (path, $S(self->p.path)), (fd, $I(self->fd)));
}

static err_i
pioLocalWriteFile_pioWrite(VSelf, ft_bytes_t buf)
{
	Self(pioLocalWriteFile);
	size_t r;

	if (buf.len == 0)
		return $noerr();

	r = fwrite(buf.ptr, 1, buf.len, self->fl);
	if (r < buf.len)
		return $syserr(errno, "Writting file {path:q}",
					   path(self->path_tmp.ptr));
	return $noerr();
}

static err_i
pioLocalWriteFile_pioWriteCompressed(VSelf, ft_bytes_t buf, CompressAlg compress_alg)
{
	Self(pioLocalWriteFile);
	char   decbuf[BLCKSZ];
	const char *errormsg = NULL;
	int32  uncompressed_size;

	ft_assert(buf.len != 0);

	uncompressed_size = do_decompress(decbuf, BLCKSZ, buf.ptr, buf.len,
									  compress_alg, &errormsg);
	if (errormsg != NULL)
	{
		return $err(RT, "An error occured during decompressing block for {path:q}: {causeStr}",
					path(self->path.ptr), causeStr(errormsg));
	}

	if (uncompressed_size != BLCKSZ)
	{
		return $err(RT, "Page uncompressed to {size} bytes != BLCKSZ (for {path:q})",
					path(self->path.ptr), size(uncompressed_size));
	}

	return $(pioWrite, self, ft_bytes(decbuf, BLCKSZ));
}

static err_i
pioLocalWriteFile_pioSeek(VSelf, uint64_t offs)
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
pioLocalWriteFile_pioTruncate(VSelf, uint64_t sz)
{
	Self(pioLocalWriteFile);
	ft_assert(self->fl != NULL, "Closed file abused \"%s\"", self->path_tmp.ptr);

	/* it is better to flush before we will truncate */
	if (fflush(self->fl))
		return $syserr(errno, "Cannot flush file {path:q}",
					   path(self->path_tmp.ptr));

	if (ftruncate(fileno(self->fl), sz) < 0)
		return $syserr(errno, "Cannot truncate file {path:q}",
					   path(self->path_tmp.ptr));
	/* TODO: what to do with file position? */

	return $noerr();
}

static err_i
pioLocalWriteFile_pioClose(VSelf)
{
	Self(pioLocalWriteFile);
	int fd;
	int r;

	fd = fileno(self->fl);

	if (fflush(self->fl) != 0)
		return $syserr(errno, "Flushing file {path:q}",
					   path(self->path_tmp.ptr));

	if (ferror(self->fl))
	{
		fclose(self->fl);
		self->fl = NULL;
		return $noerr();
	}

	if (self->sync)
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
		self->delete_in_dispose = false;

		if (self->sync)
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
	else
		self->delete_in_dispose = false;

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
	if (self->delete_in_dispose)
	{
		remove(self->path_tmp.ptr);
	}
	ft_str_free(&self->path);
	ft_str_free(&self->path_tmp);
	ft_bytes_free(&self->buf);
}

static pio_dirent_t
pioLocalDir_pioDirNext(VSelf, err_i* err)
{
	Self(pioLocalDir);
	struct dirent* ent;
	pio_dirent_t   entry = {.stat={.pst_kind=PIO_KIND_UNKNOWN}};
	char           path[MAXPGPATH];
	fobj_reset_err(err);

	ft_assert(self->dir != NULL, "Abuse closed dir");

	ft_strbuf_reset_for_reuse(&self->name_buf);

	for (;;)
	{
		errno = 0;
		ent = readdir(self->dir);
		if (ent == NULL && errno != 0)
			*err = $syserr(errno, "Could not read dir {path:q}",
						   path(self->path.ptr));
		if (ent == NULL)
			return entry;

		/* Skip '.', '..' and all hidden files as well */
		if (ent->d_name[0] == '.')
			continue;

		join_path_components(path, self->path.ptr, ent->d_name);
		entry.stat = $i(pioStat, localDrive, path, true, .err = err);
		if ($haserr(*err))
			return entry;

		/*
		 * Add only files, directories and links. Skip sockets and other
		 * unexpected file formats.
		 */
		if (entry.stat.pst_kind != PIO_KIND_DIRECTORY &&
		    entry.stat.pst_kind != PIO_KIND_REGULAR)
		{
			elog(WARNING, "Skip '%s': unexpected file kind %s", path,
				 pio_file_kind2str(entry.stat.pst_kind, path));
			continue;
		}

		ft_strbuf_catc(&self->name_buf, ent->d_name);
		entry.name = ft_strbuf_ref(&self->name_buf);
		return entry;
	}
}

static err_i
pioLocalDir_pioClose(VSelf)
{
	Self(pioLocalDir);
	int rc;

	rc = closedir(self->dir);
	self->dir = NULL;
	if (rc)
		return $syserr(errno, "Could not close dir {path:q}",
					   path(self->path.ptr));
	return $noerr();
}

static void
pioLocalDir_fobjDispose(VSelf)
{
	Self(pioLocalDir);

	if (self->dir)
		closedir(self->dir);
	self->dir = NULL;
	ft_str_free(&self->path);
	ft_strbuf_free(&self->name_buf);
}

/* REMOTE DRIVE */

static pioReader_i
pioRemoteDrive_pioOpenRead(VSelf, path_t path, err_i *err)
{
    int handle;
    fio_header hdr;
    fobj_reset_err(err);
    fobj_t file;

	handle = find_free_handle();

    hdr.cop = FIO_OPEN;
    hdr.handle = handle;
    hdr.size = strlen(path) + 1;
    hdr.arg = O_RDONLY;
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
        return (pioReader_i){NULL};
    }
    file = $alloc(pioRemoteFile, .handle = handle,
                  .p = { .path = ft_cstrdup(path) });
    return $bind(pioReader, file);
}

static pioReadStream_i
pioRemoteDrive_pioOpenReadStream(VSelf, path_t path, err_i *err)
{
	Self(pioRemoteDrive);

	pioReader_i fl = $(pioOpenRead, self, path, err);
	if ($haserr(*err))
		return $null(pioReadStream);

	*err = $(pioSetAsync, fl.self, true);
	if ($haserr(*err))
	{
		$idel(&fl);
		return $null(pioReadStream);
	}

	return $reduce(pioReadStream, fl);
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
	ft_strbuf_catc_zt(&buf, file1);
	ft_strbuf_catc_zt(&buf, file2);
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

static pioDirIter_i
pioRemoteDrive_pioOpenDir(VSelf, path_t path, err_i* err)
{
	Self(pioRemoteDrive);
	fio_header hdr = {
		.cop = PIO_DIR_OPEN,
		.handle = find_free_handle(),
		.size = strlen(path)+1,
	};
	fobj_reset_err(err);

	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

	IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
	if (hdr.cop == FIO_PIO_ERROR)
	{
		*err = fio_receive_pio_err(&hdr);
		return $null(pioDirIter);
	}
	ft_assert(hdr.cop == PIO_DIR_OPEN);
	set_handle(hdr.handle);
	return $bind(pioDirIter,
				 $alloc(pioRemoteDir,
						.path = ft_strdupc(path),
						.handle = hdr.handle,
						.pos = 0));
}

static bool
pioRemoteDrive_pioIsDirEmpty(VSelf, path_t path, err_i* err)
{
	Self(pioRemoteDrive);
	fio_header hdr = {
			.cop = PIO_IS_DIR_EMPTY,
			.size = strlen(path)+1,
	};
	fobj_reset_err(err);

	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

	IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
	if (hdr.cop == FIO_PIO_ERROR)
	{
		*err = fio_receive_pio_err(&hdr);
		return false;
	}
	ft_assert(hdr.cop == PIO_IS_DIR_EMPTY);
	return hdr.arg;
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

	ft_strbuf_catc_zt(&buf, path);
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
pioRemoteFile_pioClose(VSelf)
{
	Self(pioRemoteFile);
	err_i err = $noerr();

	ft_assert(self->handle >= 0, "Remote closed file abused \"%s\"", self->p.path);

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

static err_i
pioRemoteFile_pioSeek(VSelf, uint64_t offs)
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
pioRemoteFile_pioSetAsync(VSelf, bool async)
{
    Self(pioRemoteFile);

    ft_assert(self->handle >= 0, "Remote closed file abused \"%s\"", self->p.path);

    if (!self->asyncMode && async)
    {
        self->asyncMode = true;
    }
    else if (self->asyncMode && !async)
    {
        self->asyncMode = false;
    }
    return $noerr();
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
							 bool binary, bool use_temp, bool sync, err_i *err)
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
			.use_temp = use_temp,
			.sync = sync,
	};

	fio_ensure_remote();

	ft_strbuf_catbytes(&buf, ft_bytes(&hdr, sizeof(hdr)));
	ft_strbuf_catbytes(&buf, ft_bytes(&req, sizeof(req)));
	ft_strbuf_catc_zt(&buf, path);

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
							bool exclusive, bool sync, err_i *err)
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
			.sync        = sync,
	};

	fio_ensure_remote();

	ft_strbuf_catbytes(&buf, ft_bytes(&hdr, sizeof(hdr)));
	ft_strbuf_catbytes(&buf, ft_bytes(&req, sizeof(req)));
	ft_strbuf_catc_zt(&buf, path);

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

static err_i
pioRemoteWriteFile_pioWrite(VSelf, ft_bytes_t buf)
{
	Self(pioRemoteWriteFile);
	fio_header hdr;

	ft_assert(self->handle >= 0);

	if (buf.len == 0)
		return $noerr();

	hdr = (fio_header){
			.cop = PIO_WRITE_ASYNC,
			.handle = self->handle,
			.size = buf.len,
			.arg = 0,
	};

	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	IO_CHECK(fio_write_all(fio_stdout, buf.ptr, buf.len), buf.len);

	self->did_async = true;

	return $noerr();
}

static err_i
pioRemoteWriteFile_pioWriteCompressed(VSelf, ft_bytes_t buf, CompressAlg compress_alg)
{
	Self(pioRemoteWriteFile);
	fio_header hdr = {
			.cop = PIO_WRITE_COMPRESSED_ASYNC,
			.handle = self->handle,
			.size = buf.len,
			.arg = compress_alg,
	};

	ft_assert(self->handle >= 0);

	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	IO_CHECK(fio_write_all(fio_stdout, buf.ptr, buf.len), buf.len);

	self->did_async = true;

	return $noerr();
}

static err_i
pioRemoteWriteFile_pioSeek(VSelf, uint64_t offs)
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

	self->did_async = true;

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

	self->did_async = false;

	if (hdr.cop == FIO_PIO_ERROR)
		return fio_receive_pio_err(&hdr);
	ft_dbg_assert(hdr.cop == PIO_GET_ASYNC_ERROR);

	return $noerr();
}

static err_i
pioRemoteWriteFile_pioTruncate(VSelf, uint64_t sz)
{
    Self(pioRemoteWriteFile);

	struct __attribute__((packed)) {
		fio_header hdr;
		uint64_t   off;
	} req = {
			.hdr = {
					.cop = PIO_TRUNCATE,
					.handle = self->handle,
					.size = sizeof(uint64_t),
			},
			.off = sz,
	};

	ft_assert(self->handle >= 0, "Remote closed file abused \"%s\"", self->path.ptr);

	IO_CHECK(fio_write_all(fio_stdout, &req, sizeof(req)), sizeof(req));

	self->did_async = true;

    return $noerr();
}

static err_i
pioRemoteWriteFile_pioClose(VSelf)
{
	Self(pioRemoteWriteFile);
	err_i	   err = $noerr();
	fio_header hdr = {.cop = PIO_CLOSE, .handle = self->handle };

	ft_assert(self->handle >= 0);

	if (self->did_async)
		err = $(pioWriteFinish, self);

	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

	unset_handle(self->handle);
	self->handle = -1;

	if (hdr.cop == FIO_PIO_ERROR)
		err = fobj_err_combine(err, fio_receive_pio_err(&hdr));
	return err;
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

static pio_dirent_t
pioRemoteDir_pioDirNext(VSelf, err_i *err)
{
	Self(pioRemoteDir);
	fio_header   hdr;
	pio_dirent_t entry = {.stat={.pst_kind=PIO_KIND_UNKNOWN}};
	ft_bytes_t   tofree = ft_bytes(NULL, 0);
	ft_bytes_t   buf;
	int          n;
	fobj_reset_err(err);

	ft_assert(self->handle >= 0, "Abuse closed dir");

	if (self->pos == self->entries.len)
	{
		ft_bytes_free(&self->names_buf);
		ft_arr_dirent_reset_for_reuse(&self->entries);
		self->pos = 0;

		hdr = (fio_header){
			.cop = PIO_DIR_NEXT,
			.handle = self->handle,
		};

		IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
		IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

		if (hdr.cop == FIO_PIO_ERROR)
		{
			*err = fio_receive_pio_err(&hdr);
			return entry;
		}
		ft_assert(hdr.cop == PIO_DIR_NEXT);

		if (hdr.arg == 0)
		{
			/* End of iteration */
			return entry;
		}

		buf = ft_bytes_alloc(hdr.size);
		tofree = buf;
		IO_CHECK(fio_read_all(fio_stdin, buf.ptr, buf.len), buf.len);

		for (n = 0; n < hdr.arg; n++)
		{
			ft_bytes_shift_must(&buf, FT_BYTES_FOR(entry.stat));
			ft_arr_dirent_push(&self->entries, entry);
		}

		self->names_buf = ft_bytes_dup(buf);
		buf = self->names_buf;

		for (n = 0; n < self->entries.len; n++)
			self->entries.ptr[n].name = ft_bytes_shift_zt(&buf);

		ft_bytes_free(&tofree);
	}

	entry = self->entries.ptr[self->pos];
	self->pos++;
	return entry;
}

static err_i
pioRemoteDir_pioClose(VSelf)
{
	Self(pioRemoteDir);
	err_i	   err = $noerr();
	fio_header hdr = {.cop = PIO_CLOSE, .handle = self->handle };

	ft_assert(self->handle >= 0);

	IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
	IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

	unset_handle(self->handle);
	self->handle = -1;

	if (hdr.cop == FIO_PIO_ERROR)
		err = fobj_err_combine(err, fio_receive_pio_err(&hdr));
	return err;
}

static void
pioRemoteDir_fobjDispose(VSelf)
{
	Self(pioRemoteDir);

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
	ft_bytes_free(&self->names_buf);
	ft_arr_dirent_reset_for_reuse(&self->entries);
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
        r = $i(pioRead, self->wrapped, wbuf, err);
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

		if (wbuf.len == 0)
			break;

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
pioReadFilter_pioClose(VSelf)
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
    if ($ifdef(errcl =, pioClose, self->wrapped.self))
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

static err_i
pioWriteFilter_pioWrite(VSelf, ft_bytes_t rbuf)
{
    Self(pioWriteFilter);
	err_i		err = $noerr();
    pioFltTransformResult tr;
    size_t      rlen = rbuf.len;
    ft_bytes_t	wbuf;

    if ($notNULL(self->inplace))
    {
        err = $i(pioFltInPlace, self->inplace, rbuf);
        if ($haserr(err))
            return err;
        return $i(pioWrite, self->wrapped, rbuf);
    }

    while (rbuf.len > 0)
    {
        wbuf = ft_bytes(self->buffer, self->capa);
        while (wbuf.len > 0)
        {
            tr = $i(pioFltTransform, self->filter, rbuf, wbuf, &err);
            if ($haserr(err))
                return err;
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
        err = $i(pioWrite, self->wrapped, wbuf);
        if ($haserr(err))
            return err;
    }

    if (rbuf.len)
    {
        return $err(SysErr, "short write: {writtenSz} < {wantedSz}",
                    writtenSz(rlen - rbuf.len), wantedSz(rbuf.len));
    }
    return $noerr();
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
        err = $i(pioWrite, self->wrapped, wbuf);
        if ($haserr(err))
            return err;
    }

    return $i(pioWriteFinish, self->wrapped);
}

static err_i
pioWriteFilter_pioClose(VSelf)
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
    if ($ifdef(errcl =, pioClose, self->wrapped.self))
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
#define MAX_WBITS            15 /* 32K LZ77 window */
#define DEF_MEM_LEVEL        8

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

pioWrapRead_i
pioGZDecompressWrapper(bool ignoreTruncate)
{
	return $bind(pioWrapRead, $alloc(pioGZDecompressWrapperObj,
									 .ignoreTruncate = ignoreTruncate));
}

static pioRead_i
pioGZDecompressWrapperObj_pioWrapRead(VSelf, pioRead_i rdr, err_i* err)
{
	Self(pioGZDecompressWrapperObj);
	pioFilter_i flt;
	fobj_reset_err(err);

	flt = pioGZDecompressFilter(self->ignoreTruncate);
	return pioWrapReadFilter(rdr, flt, CHUNK_SIZE);
}
#endif

extern pioReader_i
pioWrapForReSeek(pioReader_i fl, pioWrapRead_i wr)
{
	pioReSeekableReader* reseek;
	err_i	err;

	reseek = $alloc(pioReSeekableReader,
					.reader = $iref(fl),
					.wrapper = $iref(wr),
					);
	reseek->wrapped = $iref($i(pioWrapRead, wr,
							   .reader=$reduce(pioRead, fl),
							   .err=&err));
	if ($haserr(err))
		ft_logerr(FT_FATAL, $errmsg(err), "wrap failed");

	return $bind(pioReader, reseek);
}

static size_t
pioReSeekableReader_pioRead(VSelf, ft_bytes_t buf, err_i *err)
{
	Self(pioReSeekableReader);
	size_t r;

	ft_assert(!self->had_err, "use after error");
	ft_assert(!self->closed, "use after close");

	r = $i(pioRead, self->wrapped, buf, err);
	self->pos += r;
	if ($haserr(*err))
		self->had_err = true;
	return r;
}

static err_i
pioReSeekableReader_pioSeek(VSelf, uint64_t pos)
{
	FOBJ_FUNC_ARP();
	Self(pioReSeekableReader);
	char buf[4096];
	size_t need, r;
	err_i err;

	ft_assert(!self->had_err, "use after error");
	ft_assert(!self->closed, "use after close");

	if (pos < self->pos)
	{
		pioRead_i wrapped;
		/* had to read from the beginning and reset filter */
		self->had_err = true;
		err = $i(pioSeek, self->reader, 0);
		if ($haserr(err))
			return $iresult(err);
		self->pos = 0;

		wrapped = $i(pioWrapRead, self->wrapper,
					 .reader = $reduce(pioRead, self->reader),
					 .err = &err);
		if ($haserr(err))
			return $iresult(err);
		$iset(&self->wrapped, wrapped);
		self->had_err = false;
	}

	while (pos > self->pos)
	{
		need = ft_min(pos - self->pos, sizeof(buf));
		r = $(pioRead, self, ft_bytes(buf, need), .err = &err);
		if ($haserr(err))
		{
			self->had_err = true;
			return $iresult(err);
		}
		/* lseek/fseek seeks past the file end without error, so we do */
		if (r < need)
			break;
	}

	return $noerr();
}

static err_i
pioReSeekableReader_pioClose(VSelf)
{
	Self(pioReSeekableReader);
	err_i err;

	err = $i(pioClose, self->reader);
	self->closed = true;
	return err;
}

static void
pioReSeekableReader_fobjDispose(VSelf)
{
	Self(pioReSeekableReader);
	if (!self->closed)
		$i(pioClose, self->reader);
	$idel(&self->reader);
	$idel(&self->wrapper);
	$idel(&self->wrapped);
}

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

static err_i
pioDevNull_pioWrite(VSelf, ft_bytes_t buf)
{
	return $noerr();
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

    for (i = nfilters - 1; i >= 0; i--)
        dest = pioWrapWriteFilter(dest, filters[i], OUT_BUF_SIZE);

    buf = fobj_alloc_temp(OUT_BUF_SIZE);

    while (!$haserr(rerr) && !$haserr(werr))
    {
        size_t read_len = 0;

        read_len = $i(pioRead, src, ft_bytes(buf, OUT_BUF_SIZE), &rerr);

        if (read_len == 0)
            break;

        werr = $i(pioWrite, dest, ft_bytes(buf, read_len));
		if ($noerr(werr))
			*copied += read_len;
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
	ft_strbuf_catc_zt(&buf, from_fullpath);

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

/*
 * skip_drive
 *
 * On Windows, a path may begin with "C:" or "//network/".  Advance over
 * this and point to the effective start of the path.
 *
 * (copied from PostgreSQL's src/port/path.c)
 */
#ifdef WIN32

static char *
skip_drive(const char *path)
{
	if (IS_DIR_SEP(path[0]) && IS_DIR_SEP(path[1]))
	{
		path += 2;
		while (*path && !IS_DIR_SEP(*path))
			path++;
	}
	else if (isalpha((unsigned char) path[0]) && path[1] == ':')
	{
		path += 2;
	}
	return (char *) path;
}
#else
#define skip_drive(path)	(path)
#endif

bool
ft_strbuf_cat_path(ft_strbuf_t *buf, ft_str_t path)
{
	/* here we repeat join_path_components */
	if (buf->len > 0 && !IS_DIR_SEP(buf->ptr[buf->len-1]))
	{
		if (*(skip_drive(buf->ptr)) != '\0')
			if (!ft_strbuf_cat1(buf, '/'))
				return false;
	}

	return ft_strbuf_cat(buf, path);
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
fobj_klass_handle(pioLocalDir);
fobj_klass_handle(pioRemoteDir);
fobj_klass_handle(pioWriteFilter, mth(fobjDispose, fobjRepr));
fobj_klass_handle(pioReadFilter, mth(fobjDispose, fobjRepr));
fobj_klass_handle(pioDevNull);
fobj_klass_handle(pioCRC32Counter);
fobj_klass_handle(pioReSeekableReader);

#ifdef HAVE_LIBZ
fobj_klass_handle(pioGZCompress, mth(fobjRepr));
fobj_klass_handle(pioGZDecompress, mth(fobjRepr));
fobj_klass_handle(pioGZDecompressWrapperObj);
#endif

void
init_pio_objects(void)
{
    FOBJ_FUNC_ARP();

    localDrive = bindref_pioDBDrive($alloc(pioLocalDrive));
    remoteDrive = bindref_pioDBDrive($alloc(pioRemoteDrive));
}
