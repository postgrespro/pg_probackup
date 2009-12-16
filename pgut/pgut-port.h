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

#ifdef WIN32

#define LOCK_SH		1	/* Shared lock.  */
#define LOCK_EX		2	/* Exclusive lock.  */
#define LOCK_UN		8	/* Unlock.  */
#define LOCK_NB		4	/* Don't block when locking.  */

#define S_IFLNK			(0)
#define S_IRWXG			(0)
#define S_IRWXO			(0)
#define S_ISLNK(mode)	(0)

extern int flock(int fd, int operation);
extern ssize_t readlink(const char *path, char *target, size_t size);

#endif

#endif   /* PGUT_PORT_H */
