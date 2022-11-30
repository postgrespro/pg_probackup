#ifndef __EXTRA_H__
#define __EXTRA_H__

typedef enum CompressAlg
{
	NOT_DEFINED_COMPRESS = 0,
	NONE_COMPRESS,
	PGLZ_COMPRESS,
	ZLIB_COMPRESS,
} CompressAlg;

typedef struct PageState
{
	uint16  checksum;
	XLogRecPtr  lsn;
} PageState;

typedef enum BackupMode
{
	BACKUP_MODE_INVALID = 0,
	BACKUP_MODE_DIFF_PAGE,		/* incremental page backup */
	BACKUP_MODE_DIFF_PTRACK,	/* incremental page backup with ptrack system */
	BACKUP_MODE_DIFF_DELTA,		/* incremental page backup with lsn comparison */
	BACKUP_MODE_FULL			/* full backup */
} BackupMode;

typedef struct pgFile pgFile;

int compress_page(char *write_buffer, size_t buffer_size, BlockNumber blknum, void *page,
				  CompressAlg calg, int clevel, const char *from_fullpath);

#endif /* __EXTRA_H__ */
