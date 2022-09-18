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

#if defined(WIN32) && !(defined(__MINGW64__) || defined(__MINGW32__) || defined(HAVE_PTHREAD))
#error "Windows build supports only 'pthread' threading"
#endif

/* Use platform-dependent pthread capability */
#include <pthread.h>
extern pthread_t main_tid;
#define pthread_lock(mp)  pthread_mutex_lock(mp)

extern bool			thread_interrupted;

int my_thread_num(void);
void set_my_thread_num(int);

#endif   /* PROBACKUP_THREAD_H */
