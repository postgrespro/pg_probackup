#ifndef __FILE__H__
#define __FILE__H__

#include "storage/bufpage.h"
#include <stdio.h>
#ifndef WIN32
#include <sys/stat.h>
#endif
#include <dirent.h>

#ifdef HAVE_LIBZ
#include <zlib.h>
#endif

#include <fo_obj.h>

/* Directory/File permission */
#define DIR_PERMISSION		(0700)
#define FILE_PERMISSION		(0600)

typedef enum
{
	/* message for compatibility check */
	FIO_AGENT_VERSION, /* never move this */
	FIO_OPEN,
	FIO_CLOSE,
	FIO_WRITE,
	FIO_SYNC,
	FIO_RENAME,
	FIO_SYMLINK,
	FIO_REMOVE,
	FIO_MKDIR,
	FIO_CHMOD,
	FIO_SEEK,
	FIO_TRUNCATE,
	FIO_PREAD,
	FIO_READ,
	FIO_LOAD,
	FIO_STAT,
	FIO_SEND,
	FIO_ACCESS,
	FIO_OPENDIR,
	FIO_READDIR,
	FIO_CLOSEDIR,
	FIO_PAGE,
	FIO_WRITE_COMPRESSED_ASYNC,
	FIO_GET_CRC32,
	/* used for incremental restore */
	FIO_GET_CHECKSUM_MAP,
	FIO_GET_LSN_MAP,
	/* used in fio_send_pages */
	FIO_SEND_PAGES,
	FIO_ERROR,
	FIO_SEND_FILE,
//	FIO_CHUNK,
	FIO_SEND_FILE_EOF,
	FIO_SEND_FILE_CORRUPTION,
	FIO_SEND_FILE_HEADERS,
	/* messages for closing connection */
	FIO_DISCONNECT,
	FIO_DISCONNECTED,
	FIO_LIST_DIR,
    FIO_REMOVE_DIR,
	FIO_CHECK_POSTMASTER,
	FIO_GET_ASYNC_ERROR,
	FIO_WRITE_ASYNC,
	FIO_READLINK,
    FIO_SYNC_FILE,
    FIO_SEND_FILE_CONTENT,
} fio_operations;

typedef struct
{
//	fio_operations cop;
//	16
	/* fio operation, see fio_operations enum */
	unsigned cop    : 32;
	/* */
	unsigned handle : 32;
	/* size of additional data sent after this header */
	unsigned size   : 32;
	/* additional small parameter for requests (varies between operations) or a result code for response */
	unsigned arg;
} fio_header;

typedef enum
{
	FIO_LOCAL_HOST,  /* data is locate at local host */
	FIO_DB_HOST,     /* data is located at Postgres server host */
	FIO_BACKUP_HOST, /* data is located at backup host */
	FIO_REMOTE_HOST,  /* date is located at remote host */
	FIO_CLOUD_HOST  /* date is located at cloud (S3) */
} fio_location;

extern fio_location MyLocation;

extern void    setMyLocation(ProbackupSubcmd const subcmd);
/* Check if specified location is local for current node */
extern bool    fio_is_remote(fio_location location);
extern bool    fio_is_remote_simple(fio_location location);

extern void    fio_communicate(int in, int out);
extern void    fio_disconnect(void);
extern int     fio_get_agent_version(void);

#define FIO_FDMAX 64
#define FIO_PIPE_MARKER 0x40000000

/* Check if FILE handle is local or remote (created by FIO) */
#define fio_is_remote_file(file) ((size_t)(file) <= FIO_FDMAX)

extern void    fio_redirect(int in, int out, int err);
extern void    fio_error(int rc, int size, const char* file, int line);

#define SYS_CHECK(cmd) do if ((cmd) < 0) { fprintf(stderr, "%s:%d: (%s) %s\n", __FILE__, __LINE__, #cmd, strerror(errno)); exit(EXIT_FAILURE); } while (0)
#define IO_CHECK(cmd, size) do { int _rc = (cmd); if (_rc != (size)) fio_error(_rc, size, __FILE__, __LINE__); } while (0)


/* fd-style functions */
extern int     fio_open(fio_location location, const char* name, int mode);
extern ssize_t fio_write(int fd, void const* buf, size_t size);
extern ssize_t fio_write_async(int fd, void const* buf, size_t size);
extern int     fio_check_error_fd(int fd, char **errmsg);
extern int     fio_check_error_fd_gz(gzFile f, char **errmsg);
extern ssize_t fio_read(int fd, void* buf, size_t size);
extern int     fio_flush(int fd);
extern int     fio_seek(int fd, off_t offs);
extern int     fio_fstat(int fd, struct stat* st);
extern int     fio_truncate(int fd, off_t size);
extern int     fio_close(int fd);

/* FILE-style functions */
extern FILE*   fio_fopen(fio_location location, const char* name, const char* mode);
extern size_t  fio_fwrite(FILE* f, void const* buf, size_t size);
extern ssize_t fio_fwrite_async_compressed(FILE* f, void const* buf, size_t size, int compress_alg);
extern size_t  fio_fwrite_async(FILE* f, void const* buf, size_t size);
extern int     fio_check_error_file(FILE* f, char **errmsg);
extern ssize_t fio_fread(FILE* f, void* buf, size_t size);
extern int     fio_pread(FILE* f, void* buf, off_t offs);
extern int     fio_fprintf(FILE* f, const char* arg, ...) pg_attribute_printf(2, 3);
extern int     fio_fflush(FILE* f);
extern int     fio_fseek(FILE* f, off_t offs);
extern int     fio_ftruncate(FILE* f, off_t size);
extern int     fio_fclose(FILE* f);
extern int     fio_ffstat(FILE* f, struct stat* st);

extern FILE*   fio_open_stream(fio_location location, const char* name);
extern int     fio_close_stream(FILE* f);

/* gzFile-style functions */
#ifdef HAVE_LIBZ
extern gzFile  fio_gzopen(fio_location location, const char* path, const char* mode, int level);
extern int     fio_gzclose(gzFile file);
extern int     fio_gzread(gzFile f, void *buf, unsigned size);
extern int     fio_gzwrite(gzFile f, void const* buf, unsigned size);
extern int     fio_gzeof(gzFile f);
extern z_off_t fio_gzseek(gzFile f, z_off_t offset, int whence);
extern const char* fio_gzerror(gzFile file, int *errnum);
#endif

/* DIR-style functions */
extern DIR*    fio_opendir(fio_location location, const char* path);
extern struct dirent * fio_readdir(DIR *dirp);
extern int     fio_closedir(DIR *dirp);

/* pathname-style functions */
extern int     fio_sync(fio_location location, const char* path);
extern pg_crc32
fio_get_crc32(fio_location location, const char *file_path,
			  bool decompress, bool missing_ok);

extern int     fio_rename(fio_location location, const char* old_path, const char* new_path);
extern int     fio_symlink(fio_location location, const char* target, const char* link_path, bool overwrite);
extern int     fio_remove(fio_location location, const char* path, bool missing_ok);
extern int     fio_mkdir(fio_location location, const char* path, int mode, bool strict);
extern int     fio_chmod(fio_location location, const char* path, int mode);
extern int     fio_access(fio_location location, const char* path, int mode);
extern int     fio_stat(fio_location location, const char* path, struct stat* st, bool follow_symlinks);
extern bool    fio_is_same_file(fio_location location, const char* filename1, const char* filename2, bool follow_symlink);
extern ssize_t fio_readlink(fio_location location, const char *path, char *value, size_t valsiz);
extern pid_t   fio_check_postmaster(fio_location location, const char *pgdata);

extern void db_list_dir(parray *files, const char *root, bool handle_tablespaces,
			bool backup_logs, int external_dir_num);
extern void backup_list_dir(parray *files, const char *root);

struct PageState; /* defined in pg_probackup.h */
extern struct PageState *fio_get_checksum_map(fio_location location, const char *fullpath, uint32 checksum_version,
									   int n_blocks, XLogRecPtr dest_stop_lsn, BlockNumber segmentno);
struct datapagemap; /* defined in datapagemap.h */
extern struct datapagemap *fio_get_lsn_map(fio_location location, const char *fullpath, uint32 checksum_version,
									  int n_blocks, XLogRecPtr horizonLsn, BlockNumber segmentno);


// OBJECTS

extern void init_pio_objects(void);

typedef const char* path_t;

fobj_error_cstr_key(remotemsg);
fobj_error_int_key(writtenSz);
fobj_error_int_key(wantedSz);

#ifdef HAVE_LIBZ
fobj_error_kind(GZ);
fobj_error_int_key(gzErrNo);
fobj_error_cstr_key(gzErrStr);
#endif

// File
#define mth__pioClose  		err_i, (bool, sync)
#define mth__pioClose__optional() (sync, false)
#define mth__pioRead  		size_t, (ft_bytes_t, buf), (err_i *, err)
#define mth__pioWrite  		size_t, (ft_bytes_t, buf), (err_i *, err)
#define mth__pioTruncate 	err_i, (size_t, sz)
#define mth__pioFlush		err_i
fobj_method(pioClose);
fobj_method(pioRead);
fobj_method(pioWrite);
fobj_method(pioTruncate);
fobj_method(pioFlush);

#define iface__pioFile				mth(pioWrite, pioFlush, pioRead, pioTruncate, pioClose)
#define iface__pioWriteFlush		mth(pioWrite, pioFlush)
#define iface__pioWriteCloser		mth(pioWrite, pioFlush, pioClose)
#define iface__pioReadCloser  		mth(pioRead, pioClose)
fobj_iface(pioFile);
fobj_iface(pioWriteFlush);
fobj_iface(pioWriteCloser);
fobj_iface(pioReadCloser);

typedef struct stat stat_t;

// Drive
#define mth__pioOpen 		pioFile_i, (path_t, path), (int, flags), \
									   (int, permissions), (err_i *, err)
#define mth__pioOpen__optional() (permissions, FILE_PERMISSION)
#define mth__pioStat 		stat_t, (path_t, path), (bool, follow_symlink), \
										 (err_i *, err)
#define mth__pioRemove 		err_i, (path_t, path), (bool, missing_ok)
#define mth__pioRename 		err_i, (path_t, old_path), (path_t, new_path)
#define mth__pioExists 		bool, (path_t, path), (err_i *, err)
#define mth__pioGetCRC32 	pg_crc32, (path_t, path), (bool, compressed), \
									  (err_i *, err)
#define mth__pioIsRemote 	bool
#define mth__pioListDir     void, (parray *, files), (const char *, root), \
                                (bool, handle_tablespaces), (bool, symlink_and_hidden), \
                                (bool, backup_logs), (bool, skip_hidden),  (int, external_dir_num)
#define mth__pioRemoveDir   void, (const char *, root), (bool, root_as_well)

fobj_method(pioOpen);
fobj_method(pioStat);
fobj_method(pioRemove);
fobj_method(pioRename);
fobj_method(pioExists);
fobj_method(pioIsRemote);
fobj_method(pioGetCRC32);
fobj_method(pioListDir);
fobj_method(pioRemoveDir);

#define iface__pioDrive 	mth(pioOpen, pioStat, pioRemove, pioRename), \
					        mth(pioExists, pioGetCRC32, pioIsRemote, pioListDir, pioRemoveDir)
fobj_iface(pioDrive);

#define kls__pioLocalDrive	iface__pioDrive, iface(pioDrive)
#define kls__pioRemoteDrive	iface__pioDrive, iface(pioDrive)
fobj_klass(pioLocalDrive);
fobj_klass(pioRemoteDrive);

extern pioDrive_i pioDriveForLocation(fio_location location);

#define pioFile__common_methods mth(pioRead, pioWrite, pioFlush, pioTruncate, pioClose)

#define kls__pioLocalFile	iface__pioFile, iface(pioFile)
fobj_klass(pioLocalFile);

#define mth__pioSetAsync    err_i, (bool, async)
#define mth__pioSetAsync__optional()  (async, true)
#define mth__pioAsyncRead   size_t, (ft_bytes_t, buf), (err_i*, err)
#define mth__pioAsyncWrite  size_t, (ft_bytes_t, buf), (err_i*, err)
#define mth__pioAsyncError  err_i
fobj_method(pioSetAsync);
fobj_method(pioAsyncRead);
fobj_method(pioAsyncWrite);
fobj_method(pioAsyncError);

#define kls__pioRemoteFile	iface__pioFile, iface(pioFile), \
                            mth(pioSetAsync, pioAsyncRead, pioAsyncWrite, pioAsyncError)
fobj_klass(pioRemoteFile);

// Filter
typedef struct pioTransformResult {
    size_t	consumed;
    size_t	produced;
} pioTransformResult;

#define mth__pioTransform	pioTransformResult, (ft_bytes_t, in), \
												(ft_bytes_t, out), \
												(err_i*, err)
fobj_method(pioTransform);
#define mth__pioFinish		size_t, (ft_bytes_t, out), (err_i*, err)
fobj_method(pioFinish);

#define iface__pioFilter	mth(pioTransform, pioFinish)
fobj_iface(pioFilter);

#define kls__pioReadFilter	mth(pioRead, pioClose)
#define kls__pioWriteFilter iface__pioWriteFlush, iface(pioWriteFlush), \
                            mth(pioClose)
fobj_klass(pioReadFilter);
fobj_klass(pioWriteFilter);

extern pioWriteFlush_i pioWrapWriteFilter(pioWriteFlush_i fl,
                                          pioFilter_i flt,
                                          size_t buf_size);
extern pioRead_i       pioWrapReadFilter(pioRead_i fl,
                                         pioFilter_i flt,
                                         size_t buf_size);

#ifdef HAVE_LIBZ
extern pioFilter_i	pioGZCompressFilter(int level);
extern pioFilter_i	pioGZDecompressFilter(bool ignoreTruncate);
#endif

extern err_i    pioCopyWithFilters(pioWriteFlush_i dest, pioRead_i src,
                                       pioFilter_i *filters, int nfilters, size_t *copied);
#define pioCopy(dest, src, ...) ({ \
        pioFilter_i _fltrs_[] = {__VA_ARGS__}; \
        pioCopyWithFilters((dest), (src), _fltrs_, ft_arrsz(_fltrs_), NULL); \
})
#endif
