#ifndef FILE_COMPAT_H
#define FILE_COMPAT_H

#include <sys/stat.h>

extern int fsync_parent_path_compat(const char* fname);
extern int fsync_fname_compat(const char* fname, bool isdir);
extern int durable_rename_compat(const char* oldfile, const char* newfile);

//for PG10
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



#endif							/* FILE_COMPAT_H */



