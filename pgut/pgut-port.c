/*-------------------------------------------------------------------------
 *
 * pgut-port.c
 *
 * Copyright (c) 2009-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"
#include "pgut-port.h"

#undef flock

#include <unistd.h>
#include <fcntl.h>

#ifdef PGUT_FLOCK

#ifdef WIN32
int
pgut_flock(int fd, int operation)
{
	BOOL	ret;
	HANDLE	handle = (HANDLE) _get_osfhandle(fd);
	DWORD	lo = 0;
	DWORD	hi = 0;

	if (operation & LOCK_UN)
	{
		ret = UnlockFileEx(handle, 0, lo, hi, NULL);
	}
	else
	{
		DWORD	flags = 0;
		if (operation & LOCK_EX)
			flags |= LOCKFILE_EXCLUSIVE_LOCK;
		if (operation & LOCK_NB)
			flags |= LOCKFILE_FAIL_IMMEDIATELY;
		ret = LockFileEx(handle, flags, 0, lo, hi, NULL);
	}

	if (!ret)
	{
		_dosmaperr(GetLastError());
		return -1;
	}

	return 0;
}

#else

int
pgut_flock(int fd, int operation)
{
	struct flock	lck;
	int				cmd;

	memset(&lck, 0, sizeof(lck));
	lck.l_whence = SEEK_SET;
	lck.l_start = 0;
	lck.l_len = 0;
	lck.l_pid = getpid();

	if (operation & LOCK_UN)
		lck.l_type = F_UNLCK;
	else if (operation & LOCK_EX)
		lck.l_type = F_WRLCK;
	else
		lck.l_type = F_RDLCK;

	if (operation & LOCK_NB)
		cmd = F_SETLK;
	else
		cmd = F_SETLKW;

	return fcntl(fd, cmd, &lck);
}
#endif

#endif
