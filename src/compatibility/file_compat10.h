#ifndef FILE_COMPAT10_H
#define FILE_COMPAT10_H

//for PG10

//#include "pg_bswap.h"


#ifndef DEFAULT_XLOG_SEG_SIZE
#define DEFAULT_XLOG_SEG_SIZE	(16*1024*1024)
#endif


#ifndef PG_FILE_MODE_OWNER

/*
 * Mode mask for data directory permissions that only allows the owner to
 * read/write directories and files.
 *
 * This is the default.
 */
#define PG_MODE_MASK_OWNER		    (S_IRWXG | S_IRWXO)

 /*
  * Mode mask for data directory permissions that also allows group read/execute.
  */
#define PG_MODE_MASK_GROUP			(S_IWGRP | S_IRWXO)



#define PG_FILE_MODE_OWNER		    (S_IRUSR | S_IWUSR)
//#define pg_file_create_mode PG_FILE_MODE_OWNER

/* Default mode for creating directories */
#define PG_DIR_MODE_OWNER			S_IRWXU

/* Mode for creating directories that allows group read/execute */
#define PG_DIR_MODE_GROUP			(S_IRWXU | S_IRGRP | S_IXGRP)

/* Default mode for creating files */
#define PG_FILE_MODE_OWNER		    (S_IRUSR | S_IWUSR)

/* Mode for creating files that allows group read */
#define PG_FILE_MODE_GROUP			(S_IRUSR | S_IWUSR | S_IRGRP)

/* Modes for creating directories and files in the data directory */
extern int	pg_dir_create_mode;
extern int	pg_file_create_mode;

/* Mode mask to pass to umask() */
extern int	pg_mode_mask;

/* Set permissions and mask based on the provided mode */
extern void SetDataDirectoryCreatePerm(int dataDirMode);

/* Set permissions and mask based on the mode of the data directory */
extern bool GetDataDirectoryCreatePerm(const char* dataDir);

#endif

/* Set permissions and mask based on the provided mode */
extern void SetDataDirectoryCreatePerm(int dataDirMode);

/* Set permissions and mask based on the mode of the data directory */
extern bool GetDataDirectoryCreatePerm(const char *dataDir);


/* wal_segment_size can range from 1MB to 1GB */
#define WalSegMinSize 1024 * 1024
#define WalSegMaxSize 1024 * 1024 * 1024


#define XLogSegmentOffset(xlogptr, wal_segsz_bytes)	\
	((xlogptr) & ((wal_segsz_bytes) - 1))

/* check that the given size is a valid wal_segment_size */
#define IsPowerOf2(x) (x > 0 && ((x) & ((x)-1)) == 0)

#define IsValidWalSegSize(size) \
	 (IsPowerOf2(size) && \
	 ((size) >= WalSegMinSize && (size) <= WalSegMaxSize))



/* From access/xlog_internal.h */

#undef XLByteToSeg
#undef XLogFileName
#undef XLogSegmentsPerXLogId
#undef XLogFromFileName
#undef XLogSegNoOffsetToRecPtr
#undef XLByteInSeg

#define XLByteToSeg(xlrp, logSegNo, wal_segsz_bytes) \
	logSegNo = (xlrp) / (wal_segsz_bytes)

#define XLogFileName(fname, tli, logSegNo, wal_segsz_bytes)	\
	snprintf(fname, MAXFNAMELEN, "%08X%08X%08X", tli,		\
			 (uint32) ((logSegNo) / XLogSegmentsPerXLogId(wal_segsz_bytes)), \
			 (uint32) ((logSegNo) % XLogSegmentsPerXLogId(wal_segsz_bytes)))

#define XLogSegmentsPerXLogId(wal_segsz_bytes)	\
	(UINT64CONST(0x100000000) / (wal_segsz_bytes))


/*
 * The XLog directory and control file (relative to $PGDATA)
 */
#define XLOGDIR				"pg_wal"
#define XLOG_CONTROL_FILE	"global/pg_control"

/*
 * These macros encapsulate knowledge about the exact layout of XLog file
 * names, timeline history file names, and archive-status file names.
 */
#define MAXFNAMELEN		64

/* Length of XLog file name */
#define XLOG_FNAME_LEN	   24

/*
 * Generate a WAL segment file name.  Do not use this macro in a helper
 * function allocating the result generated.
 */
#define XLogFileName(fname, tli, logSegNo, wal_segsz_bytes)	\
	snprintf(fname, MAXFNAMELEN, "%08X%08X%08X", tli,		\
			 (uint32) ((logSegNo) / XLogSegmentsPerXLogId(wal_segsz_bytes)), \
			 (uint32) ((logSegNo) % XLogSegmentsPerXLogId(wal_segsz_bytes)))

#define XLogFileNameById(fname, tli, log, seg)	\
	snprintf(fname, MAXFNAMELEN, "%08X%08X%08X", tli, log, seg)

#define IsXLogFileName(fname) \
	(strlen(fname) == XLOG_FNAME_LEN && \
	 strspn(fname, "0123456789ABCDEF") == XLOG_FNAME_LEN)


#define XLogFromFileName(fname, tli, logSegNo, wal_segsz_bytes)	\
	do {												\
		uint32 log;										\
		uint32 seg;										\
		sscanf(fname, "%08X%08X%08X", tli, &log, &seg); \
		*logSegNo = (uint64) log * XLogSegmentsPerXLogId(wal_segsz_bytes) + seg; \
	} while (0)

#define XLogSegNoOffsetToRecPtr(segno, offset, wal_segsz_bytes, dest) \
		(dest) = (segno) * (wal_segsz_bytes) + (offset)
/*
 * Is an XLogRecPtr within a particular XLOG segment?
 *
 * For XLByteInSeg, do the computation at face value.  For XLByteInPrevSeg,
 * a boundary byte is taken to be in the previous segment.
 */
#define XLByteInSeg(xlrp, logSegNo, wal_segsz_bytes) \
	(((xlrp) / (wal_segsz_bytes)) == (logSegNo))



/* logs restore point */
/*
typedef struct xl_restore_point
{
	TimestampTz rp_time;
	char		rp_name[MAXFNAMELEN];
} xl_restore_point;
*/


#endif							/* FILE_COMPAT10_H */



