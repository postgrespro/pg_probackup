/*-------------------------------------------------------------------------
 *
 * thread.h: - multi-platform pthread implementations.
 *
 * Copyright (c) 2018-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#ifndef PROBACKUP_THREAD_H
#define PROBACKUP_THREAD_H

#ifdef WIN32
#include "postgres_fe.h"
#include "port/pthread-win32.h"

/* Use native win32 threads on Windows */
typedef struct win32_pthread *pthread_t;
typedef int pthread_attr_t;

#define PTHREAD_MUTEX_INITIALIZER NULL //{ NULL, 0 }
#define PTHREAD_ONCE_INIT false

extern int pthread_create(pthread_t *thread, pthread_attr_t *attr, void *(*start_routine) (void *), void *arg);
extern int pthread_join(pthread_t th, void **thread_return);
#else
/* Use platform-dependent pthread capability */
#include <pthread.h>
#endif

#ifdef WIN32
extern DWORD main_tid;
#else
extern pthread_t main_tid;
#endif

extern bool			thread_interrupted;

extern int pthread_lock(pthread_mutex_t *mp);

#endif   /* PROBACKUP_THREAD_H */
