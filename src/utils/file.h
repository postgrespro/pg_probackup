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

#include "datapagemap.h"

/* Directory/File permission */
#define DIR_PERMISSION		(0700)
#define FILE_PERMISSION		(0600)

/* define it now to avoid ugly mess of includes */
struct pgFile;
typedef struct pgFile pgFile;

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
	FIO_SEEK,
	FIO_READ,
	FIO_STAT,
	FIO_SEND,
	FIO_PAGE,
	FIO_GET_CRC32,
	/* used for incremental restore */
	FIO_GET_CHECKSUM_MAP,
	FIO_GET_LSN_MAP,
	FIO_ERROR,
	FIO_SEND_FILE,
	FIO_SEND_FILE_EOF,
	/* messages for closing connection */
	FIO_DISCONNECT,
	FIO_DISCONNECTED,
	FIO_LIST_DIR,
    FIO_REMOVE_DIR,
	FIO_CHECK_POSTMASTER,
	FIO_READLINK,
	FIO_SEND_FILE_CONTENT,
	FIO_PAGE_ZERO,
	FIO_FILES_ARE_SAME,
	FIO_READ_FILE_AT_ONCE,
	FIO_WRITE_FILE_AT_ONCE,
	FIO_PIO_ERROR, /* for sending err_i */
	FIO_ITERATE_PAGES,
	FIO_ITERATE_DATA,
	FIO_ITERATE_EOF,
	PIO_OPEN_REWRITE,
	PIO_OPEN_WRITE,
	PIO_WRITE_ASYNC,
	PIO_WRITE_COMPRESSED_ASYNC,
	PIO_SEEK,
	PIO_TRUNCATE,
	PIO_GET_ASYNC_ERROR,
	PIO_DIR_OPEN,
	PIO_DIR_NEXT,
	PIO_IS_DIR_EMPTY,
	PIO_CLOSE,
	PIO_DISPOSE,
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
} fio_location;

typedef enum pio_file_kind {
	PIO_KIND_UNKNOWN = 0,
	PIO_KIND_REGULAR = 1,
	PIO_KIND_DIRECTORY = 2,
	PIO_KIND_SYMLINK = 3,
	PIO_KIND_FIFO = 4,
	PIO_KIND_SOCK = 5,
	PIO_KIND_CHARDEV = 6,
	PIO_KIND_BLOCKDEV = 7,
} pio_file_kind_e;

typedef struct pio_stat {
	pio_file_kind_e pst_kind;
	uint32_t 		pst_mode;
	int64_t			pst_size;
	int64_t	 		pst_mtime;
} pio_stat_t;

typedef struct pio_dirent {
	pio_stat_t		stat;
	ft_str_t		name;
} pio_dirent_t;

#define FT_SLICE dirent
#define FT_SLICE_TYPE pio_dirent_t
#include <ft_array.inc.h>

#define FT_SORT dirent
#define FT_SORT_TYPE pio_dirent_t
#include <ft_sort.inc.h>

ft_inline int
compare_dirent_by_name(pio_dirent_t d1, pio_dirent_t d2)
{
	return ft_strcmp(d1.name, d2.name);
}

extern fio_location MyLocation;

extern void    setMyLocation(ProbackupSubcmd const subcmd);
/* Check if specified location is local for current node */
extern bool    fio_is_remote(fio_location location);
extern bool    fio_is_remote_simple(fio_location location);

extern void    fio_communicate(int in, int out);
extern void    fio_disconnect(void);

#define FIO_FDMAX 64

/* Check if FILE handle is local or remote (created by FIO) */
#define fio_is_remote_file(file) ((size_t)(file) <= FIO_FDMAX)

extern void    fio_redirect(int in, int out, int err);
extern void    fio_error(int rc, int size, const char* file, int line);

#define SYS_CHECK(cmd) do if ((cmd) < 0) { fprintf(stderr, "%s:%d: (%s) %s\n", __FILE__, __LINE__, #cmd, strerror(errno)); exit(EXIT_FAILURE); } while (0)
#define IO_CHECK(cmd, size) do { int _rc = (cmd); if (_rc != (size)) fio_error(_rc, size, __FILE__, __LINE__); } while (0)

extern void    fio_get_agent_version(int* protocol, char* payload_buf, size_t payload_buf_size);

/* pathname-style functions */
extern int     fio_sync(fio_location location, const char* path);
extern pg_crc32
fio_get_crc32(fio_location location, const char *file_path,
			  bool decompress, bool missing_ok);
extern pg_crc32
fio_get_crc32_truncated(fio_location location, const char *file_path,
			  bool missing_ok);

extern int     fio_symlink(fio_location location, const char* target, const char* link_path, bool overwrite);
extern int     fio_remove(fio_location location, const char* path, bool missing_ok);
extern ssize_t fio_readlink(fio_location location, const char *path, char *value, size_t valsiz);
extern pid_t   fio_check_postmaster(fio_location location, const char *pgdata);

extern PageState *fio_get_checksum_map(fio_location location, const char *fullpath, uint32 checksum_version,
									   int n_blocks, XLogRecPtr dest_stop_lsn, BlockNumber segmentno);
struct datapagemap; /* defined in datapagemap.h */
extern struct datapagemap *fio_get_lsn_map(fio_location location, const char *fullpath, uint32 checksum_version,
									  int n_blocks, XLogRecPtr horizonLsn, BlockNumber segmentno);

extern pg_crc32 pgFileGetCRC32C(const char *file_path, bool missing_ok);
extern pg_crc32 pgFileGetCRC32CTruncated(const char *file_path, bool missing_ok);
#if PG_VERSION_NUM < 120000
extern pg_crc32 pgFileGetCRC32(const char *file_path, bool missing_ok);
#endif
extern pg_crc32 pgFileGetCRC32Cgz(const char *file_path, bool missing_ok);

extern pio_file_kind_e pio_statmode2file_kind(mode_t mode, const char* path);
extern pio_file_kind_e pio_str2file_kind(const char* str, const char* path);
extern const char*	   pio_file_kind2str(pio_file_kind_e kind, const char* path);
extern mode_t		   pio_limit_mode(mode_t mode);

// OBJECTS

extern void init_pio_objects(void);

typedef const char* path_t;

fobj_error_cstr_key(remotemsg);
fobj_error_int_key(writtenSz);
fobj_error_int_key(wantedSz);
fobj_error_int_key(size);
fobj_error_cstr_key(kind);
fobj_error_int_key(offs);
fobj_error_int_key(blknum);

#ifdef HAVE_LIBZ
fobj_error_kind(GZ);
fobj_error_int_key(gzErrNo);
fobj_error_cstr_key(gzErrStr);
#endif

// File
#define mth__pioClose  		err_i
#define mth__pioRead  		size_t, (ft_bytes_t, buf), (err_i *, err)
#define mth__pioWrite  		err_i, (ft_bytes_t, buf)
#define mth__pioWriteCompressed  err_i, (ft_bytes_t, buf), (CompressAlg, compress_alg)
#define mth__pioTruncate 	err_i, (uint64_t, size)
#define mth__pioWriteFinish		err_i
#define mth__pioSeek		err_i, (uint64_t, offs)

fobj_method(pioClose);
fobj_method(pioRead);
fobj_method(pioWrite);
fobj_method(pioWriteCompressed);
fobj_method(pioTruncate);
fobj_method(pioWriteFinish);
fobj_method(pioSeek);

#define iface__pioReader			mth(pioRead, pioClose, pioSeek)
#define iface__pioReadStream		mth(pioRead, pioClose)
#define iface__pioWriteFlush		mth(pioWrite, pioWriteFinish)
#define iface__pioWriteCloser		mth(pioWrite, pioWriteFinish, pioClose)
#define iface__pioDBWriter			mth(pioWrite, pioSeek, pioWriteCompressed), \
									mth(pioWriteFinish, pioTruncate, pioClose)
#define iface__pioReadCloser  		mth(pioRead, pioClose)
fobj_iface(pioReader);
fobj_iface(pioReadStream);
fobj_iface(pioWriteFlush);
fobj_iface(pioWriteCloser);
fobj_iface(pioDBWriter);
fobj_iface(pioReadCloser);

// DIR
#define mth__pioDirNext				pio_dirent_t, (err_i*, err)
fobj_method(pioDirNext);
#define iface__pioDirIter			mth(pioDirNext, pioClose)
fobj_iface(pioDirIter);

// Pages iterator
typedef struct
{
	PageState	state;
	BlockNumber	blknum;
	int			page_result;
	size_t		compressed_size;
	char		compressed_page[BLCKSZ]; /* MUST be last */
} PageIteratorValue;

#define mth__pioNextPage		err_i, (PageIteratorValue *, value)
#define mth__pioFinalPageN		BlockNumber

fobj_method(pioNextPage);
fobj_method(pioFinalPageN);

#define iface__pioPagesIterator	mth(pioNextPage, pioFinalPageN)

fobj_iface(pioPagesIterator);

// Drive
#define mth__pioOpenRead	pioReader_i, (path_t, path), (err_i *, err)
#define mth__pioOpenReadStream	pioReadStream_i, (path_t, path), (err_i *, err)
#define mth__pioOpenWrite   pioDBWriter_i, (path_t, path), (int, permissions, FILE_PERMISSION), \
										   (bool, exclusive, false), (bool, sync, false), \
										   (err_i *, err)
#define mth__pioOpenRewrite pioWriteCloser_i, (path_t, path), (int, permissions, FILE_PERMISSION), \
                                              (bool, binary, true), (bool, use_temp, true),   \
											  (bool, sync, false), (err_i *, err)
#define mth__pioStat 		pio_stat_t, (path_t, path), (bool, follow_symlink), \
										 (err_i *, err)
#define mth__pioRemove 		err_i, (path_t, path), (bool, missing_ok)
#define mth__pioRename 		err_i, (path_t, old_path), (path_t, new_path)
#define mth__pioExists 		bool, (path_t, path), (pio_file_kind_e, expected_kind, PIO_KIND_REGULAR),	\
									(err_i *, err)
#define mth__pioGetCRC32 	pg_crc32, (path_t, path), (bool, compressed), \
									  (err_i *, err)
/* Compare, that filename1 and filename2 is the same file */
#define mth__pioFilesAreSame bool, (path_t, file1), (path_t, file2)
#define mth__pioIsRemote 	bool
#define mth__pioMakeDir	err_i, (path_t, path), (mode_t, mode), (bool, strict)
#define mth__pioOpenDir		pioDirIter_i, (path_t, path), (err_i*, err)
#define mth__pioIsDirEmpty	bool, (path_t, path), (err_i*, err)
#define mth__pioRemoveDir   void, (const char *, root), (bool, root_as_well)
/* pioReadFile and pioWriteFile should be used only for small files */
#define PIO_READ_WRITE_FILE_LIMIT (16*1024*1024)
#define mth__pioReadFile	ft_bytes_t, (path_t, path), (bool, binary, true), \
							(err_i *, err)
#define mth__pioWriteFile	err_i, (path_t, path), (ft_bytes_t, content), \
                                   (bool, binary, true)

#define mth__pioIteratePages pioPagesIterator_i, (path_t, path), \
		(int, segno), (datapagemap_t, pagemap), (XLogRecPtr, start_lsn), \
		(CompressAlg, calg), (int, clevel), \
		(uint32, checksum_version), (bool, just_validate), (err_i*, err)

fobj_method(pioOpenRead);
fobj_method(pioOpenReadStream);
fobj_method(pioOpenRewrite);
fobj_method(pioOpenWrite);
fobj_method(pioStat);
fobj_method(pioRemove);
fobj_method(pioRename);
fobj_method(pioExists);
fobj_method(pioIsRemote);
fobj_method(pioGetCRC32);
fobj_method(pioMakeDir);
fobj_method(pioFilesAreSame);
fobj_method(pioOpenDir);
fobj_method(pioIsDirEmpty);
fobj_method(pioRemoveDir);
fobj_method(pioReadFile);
fobj_method(pioWriteFile);
fobj_method(pioIteratePages);

#define iface__pioDrive 	mth(pioOpenRead, pioOpenReadStream), \
							mth(pioStat, pioRemove), \
					        mth(pioExists, pioGetCRC32, pioIsRemote),          \
							mth(pioMakeDir, pioOpenDir, pioIsDirEmpty, pioRemoveDir),  \
							mth(pioFilesAreSame, pioReadFile, pioWriteFile),   \
							mth(pioOpenRewrite)
fobj_iface(pioDrive);

#define iface__pioDBDrive	iface__pioDrive, mth(pioIteratePages, pioOpenWrite), \
                            mth(pioRename)
fobj_iface(pioDBDrive);

extern pioDrive_i pioDriveForLocation(fio_location location);
extern pioDBDrive_i pioDBDriveForLocation(fio_location location);

struct doIteratePages_params {
	path_t from_fullpath;
	pgFile *file;
	XLogRecPtr start_lsn;
	CompressAlg calg;
	int clevel;
	uint32 checksum_version;
	BackupMode backup_mode;
	bool just_validate;
	err_i *err;
};

extern pioPagesIterator_i
doIteratePages_impl(pioIteratePages_i drive, struct doIteratePages_params p);
#define doIteratePages(drive, ...) \
	doIteratePages_impl($reduce(pioIteratePages, drive), ((struct doIteratePages_params){ \
					.start_lsn = InvalidXLogRecPtr,        \
					.calg = NONE_COMPRESS, .clevel = 0,    \
					.just_validate = false,                \
					__VA_ARGS__}))

#define mth__pioSetAsync    err_i, (bool, async, true)
#define mth__pioAsyncRead   size_t, (ft_bytes_t, buf), (err_i*, err)
fobj_method(pioSetAsync);
fobj_method(pioAsyncRead);

// Filter
typedef struct pioFltTransformResult {
    size_t	consumed;
    size_t	produced;
} pioFltTransformResult;

#define mth__pioFltTransform	pioFltTransformResult, (ft_bytes_t, in), \
												(ft_bytes_t, out), \
												(err_i*, err)
fobj_method(pioFltTransform);
#define mth__pioFltFinish		size_t, (ft_bytes_t, out), (err_i*, err)
fobj_method(pioFltFinish);

#define mth__pioFltInPlace		err_i, (ft_bytes_t, buf)
fobj_method(pioFltInPlace);

#define iface__pioFilter	mth(pioFltTransform, pioFltFinish)
fobj_iface(pioFilter);

extern pioWriteFlush_i pioWrapWriteFilter(pioWriteFlush_i fl,
                                          pioFilter_i flt,
                                          size_t buf_size);
extern pioRead_i       pioWrapReadFilter(pioRead_i fl,
                                         pioFilter_i flt,
                                         size_t buf_size);

#define mth__pioWrapRead pioRead_i, (pioRead_i, reader), (err_i*, err)
fobj_method(pioWrapRead);

/*
 * Usefull for seek-able GZip reader - ie imitate gzseek.
 * Has same limitations: will read from start if seek-ed back.
 */
extern pioReader_i     pioWrapForReSeek(pioReader_i fl,
										pioWrapRead_i wr);

#ifdef HAVE_LIBZ
extern pioFilter_i	pioGZCompressFilter(int level);
extern pioFilter_i	pioGZDecompressFilter(bool ignoreTruncate);
extern pioWrapRead_i pioGZDecompressWrapper(bool ignoreTruncate);
#endif
extern pioFilter_i pioCRC32Filter(void);

typedef struct pioCRC32Counter pioCRC32Counter;
#define kls__pioCRC32Counter iface__pioFilter, mth(pioFltInPlace), iface(pioFilter)
fobj_klass(pioCRC32Counter);
extern pioCRC32Counter* pioCRC32Counter_alloc(void);
extern pg_crc32 pioCRC32Counter_getCRC32(pioCRC32Counter* flt);
extern int64_t pioCRC32Counter_getSize(pioCRC32Counter* flt);

typedef struct pioZeroTail pioZeroTail;
extern pioZeroTail* pioZeroTail_alloc(void);

extern pioWriteFlush_i pioDevNull_alloc(void);

extern err_i    pioCopyWithFilters(pioWriteFlush_i dest, pioRead_i src,
                                       pioFilter_i *filters, int nfilters, size_t *copied);
#define pioCopy(dest, src, ...) ({ \
        pioFilter_i _fltrs_[] = {__VA_ARGS__}; \
        pioCopyWithFilters((dest), (src), _fltrs_, ft_arrsz(_fltrs_), NULL); \
})

typedef struct pio_line_reader pio_line_reader;
struct pio_line_reader {
	pioRead_i   source;
	ft_bytes_t  buf;
	ft_bytes_t  rest;
};

extern void init_pio_line_reader(pio_line_reader *r, pioRead_i source, size_t max_length);
extern void deinit_pio_line_reader(pio_line_reader *r);
extern ft_bytes_t pio_line_reader_getline(pio_line_reader *r, err_i *err);

typedef struct pio_recursive_dir pio_recursive_dir_t;
extern pio_recursive_dir_t* pio_recursive_dir_alloc(pioDrive_i drive, path_t root, err_i *err);
extern pio_dirent_t pio_recursive_dir_next(pio_recursive_dir_t* dir, err_i* err);
extern void pio_recursive_dir_dont_recurse_current(pio_recursive_dir_t* dir);
extern void pio_recursive_dir_free(pio_recursive_dir_t* dir);

/* append path component */
extern bool    ft_strbuf_cat_path(ft_strbuf_t *buf, ft_str_t path);
ft_inline bool
ft_strbuf_cat_pathc(ft_strbuf_t *buf, const char *path)
{
	return ft_strbuf_cat_path(buf, ft_cstr(path));
}

#endif
