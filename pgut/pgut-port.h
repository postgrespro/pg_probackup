/*-------------------------------------------------------------------------
 *
 * pgut-port.h
 *
 * Copyright (c) 2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGUT_PORT_H
#define PGUT_PORT_H

#include <sys/stat.h>

#ifndef WIN32

#include <blkid/blkid.h>
#include <sys/utsname.h>
#include <sys/statfs.h>
#include <unistd.h>

#else

#include <time.h>

struct utsname
{
	char	sysname[32];
	char	nodename[256];
	char	release[128];
	char	version[128];
	char	machine[32];
};

#define NTFS_SB_MAGIC	0x5346544e

typedef struct { int val[2]; } fsid_t;

struct statfs
{
	long	f_type;
	long	f_bsize;
	long	f_blocks;
	long	f_bfree;
	long	f_bavail;
	long	f_files;
	long	f_ffree;
	fsid_t	f_fsid;
	long	f_namelen;
};

extern int uname(struct utsname *buf);
extern int statfs(const char *path, struct statfs *buf);
extern ssize_t readlink(const char *path, char *target, size_t size);
extern char *blkid_devno_to_devname(dev_t devno);

#define LOCK_SH		1	/* Shared lock.  */
#define LOCK_EX		2	/* Exclusive lock.  */
#define LOCK_UN		8	/* Unlock.  */
#define LOCK_NB		4	/* Don't block when locking.  */

extern int flock(int fd, int operation);

#define S_IFLNK			(0)
#define S_IRWXG			(0)
#define S_IRWXO			(0)
#define S_ISLNK(mode)	(0)

#endif

#endif   /* PGUT_PORT_H */
