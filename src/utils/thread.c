/*-------------------------------------------------------------------------
 *
 * thread.c: - multi-platform pthread implementations.
 *
 * Copyright (c) 2018-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "thread.h"

bool thread_interrupted = false;

#ifdef WIN32
DWORD main_tid = 0;
#else
pthread_t main_tid = 0;
#endif
#ifdef WIN32
#include <errno.h>

typedef struct win32_pthread
{
	HANDLE		handle;
	void	   *(*routine) (void *);
	void	   *arg;
	void	   *result;
} win32_pthread;

static long mutex_initlock = 0;

static unsigned __stdcall
win32_pthread_run(void *arg)
{
	win32_pthread *th = (win32_pthread *)arg;

	th->result = th->routine(th->arg);

	return 0;
}

int
pthread_create(pthread_t *thread,
			   pthread_attr_t *attr,
			   void *(*start_routine) (void *),
			   void *arg)
{
	int			save_errno;
	win32_pthread *th;

	th = (win32_pthread *)pg_malloc(sizeof(win32_pthread));
	th->routine = start_routine;
	th->arg = arg;
	th->result = NULL;

	th->handle = (HANDLE)_beginthreadex(NULL, 0, win32_pthread_run, th, 0, NULL);
	if (th->handle == NULL)
	{
		save_errno = errno;
		free(th);
		return save_errno;
	}

	*thread = th;
	return 0;
}

int
pthread_join(pthread_t th, void **thread_return)
{
	if (th == NULL || th->handle == NULL)
	return errno = EINVAL;

	if (WaitForSingleObject(th->handle, INFINITE) != WAIT_OBJECT_0)
	{
		_dosmaperr(GetLastError());
		return errno;
	}

	if (thread_return)
		*thread_return = th->result;

	CloseHandle(th->handle);
	free(th);
	return 0;
}

#endif   /* WIN32 */

int
pthread_lock(pthread_mutex_t *mp)
{
#ifdef WIN32
	if (*mp == NULL)
	{
		while (InterlockedExchange(&mutex_initlock, 1) == 1)
			/* loop, another thread own the lock */ ;
		if (*mp == NULL)
		{
			if (pthread_mutex_init(mp, NULL))
				return -1;
		}
		InterlockedExchange(&mutex_initlock, 0);
	}
#endif
	return pthread_mutex_lock(mp);
}
