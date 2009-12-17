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

/*
 * readlink ports
 */
#ifdef WIN32

#define S_IFLNK			(0)
#define S_IRWXG			(0)
#define S_IRWXO			(0)
#define S_ISLNK(mode)	(0)

extern ssize_t readlink(const char *path, char *target, size_t size);

#endif

/*
 * flock ports
 */
#ifndef LOCK_EX

#define PGUT_FLOCK

#undef LOCK_SH
#undef LOCK_EX
#undef LOCK_UN
#undef LOCK_NB

#define LOCK_SH		1	/* Shared lock.  */
#define LOCK_EX		2	/* Exclusive lock.  */
#define LOCK_UN		8	/* Unlock.  */
#define LOCK_NB		4	/* Don't block when locking.  */

extern int pgut_flock(int fd, int operation);

#define flock	pgut_flock

#endif

#endif   /* PGUT_PORT_H */
