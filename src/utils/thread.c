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

/*
 * Global var used to detect error condition (not signal interrupt!) in threads,
 * so if one thread errored out, then others may abort
 */
bool thread_interrupted = false;

pthread_t main_tid = 0;
static __thread int  my_thread_num_var = 1;

int
my_thread_num(void)
{
	return my_thread_num_var;
}

void
set_my_thread_num(int th)
{
	my_thread_num_var = th;
}
