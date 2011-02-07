/*-------------------------------------------------------------------------
 *
 * queue.c: Job queue with thread pooling.
 *
 * Copyright (c) 2009-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_rman.h"
#include "pgut/pgut-pthread.h"

struct JobQueue
{
	pthread_mutex_t		mutex;		/* protects the queue data */
	pthread_cond_t		anyjobs;	/* fired if any jobs */
	pthread_cond_t		nojobs;		/* fired if no jobs */
	List			   *threads;	/* list of worker thread handles */
	List			   *jobs;		/* pending jobs */
	volatile int		maximum;	/* maximum allowed threads */
	volatile int		idle;		/* number of idle threads */
	volatile bool		terminated;	/* in termination? */
};

static void *worker_thread(void *arg);

JobQueue *
JobQueue_new(int nthreads)
{
	JobQueue	*queue;

	Assert(nthreads >= 1);

	queue = pgut_new(JobQueue);
	pthread_mutex_init(&queue->mutex, NULL);
	pthread_cond_init(&queue->anyjobs, NULL);
	pthread_cond_init(&queue->nojobs, NULL);
	queue->threads = NIL;
	queue->jobs = NIL;
	queue->maximum = nthreads;
	queue->idle = 0;
	queue->terminated = false;

	return queue;
}

/*
 * Job must be allocated with malloc. The ownership will be granted to
 * the queue.
 */
void
JobQueue_push(JobQueue *queue, Job *job)
{
	Assert(queue);
	Assert(!queue->terminated);
	Assert(job);
	Assert(job->routine);

	pgut_mutex_lock(&queue->mutex);
	queue->jobs = lappend(queue->jobs, job);

	if (queue->idle > 0)
		pthread_cond_signal(&queue->anyjobs);
	else if (list_length(queue->threads) < queue->maximum)
	{
		pthread_t	th;

		if (pthread_create(&th, NULL, worker_thread, queue))
			ereport(ERROR,
				(errcode_errno(),
				 errmsg("could not create thread: ")));

		queue->threads = lappend(queue->threads, (void *) th);
		Assert(list_length(queue->threads) <= queue->maximum);
	}

	pthread_mutex_unlock(&queue->mutex);
}

/* wait for all job finished */
void
JobQueue_wait(JobQueue *queue)
{
	Assert(queue);
	Assert(!queue->terminated);

	pgut_mutex_lock(&queue->mutex);
	while (queue->jobs || queue->idle < list_length(queue->threads))
		pgut_cond_wait(&queue->nojobs, &queue->mutex);
	pthread_mutex_unlock(&queue->mutex);
}

/* Free job queue. All pending jobs are also discarded. */
void
JobQueue_free(JobQueue *queue)
{
	ListCell *cell;

	if (queue == NULL)
		return;

	Assert(!queue->terminated);

	/* Terminate all threads. */
	pgut_mutex_lock(&queue->mutex);
	queue->terminated = true;
	pthread_cond_broadcast(&queue->anyjobs);
	pthread_mutex_unlock(&queue->mutex);

	/*
	 * Wait for all threads.
	 * XXX: cancel thread for long running jobs?
	 */
	foreach(cell, queue->threads)
	{
		pthread_t	th = (pthread_t) lfirst(cell);

		pthread_join(th, NULL);
	}
	list_free(queue->threads);

	/* Free all pending jobs, though it must be avoided. */
	list_free_deep(queue->jobs);

	pthread_cond_destroy(&queue->nojobs);
	pthread_cond_destroy(&queue->anyjobs);
	pthread_mutex_destroy(&queue->mutex);
	free(queue);
}

static void *
worker_thread(void *arg)
{
	JobQueue *queue = (JobQueue *) arg;

	pgut_mutex_lock(&queue->mutex);
	while (!queue->terminated)
	{
		Job *job;

		if (queue->jobs == NIL)
		{
			queue->idle++;

			/* notify if done all jobs */
			if (queue->idle >= list_length(queue->threads))
				pthread_cond_broadcast(&queue->nojobs);

			pgut_cond_wait(&queue->anyjobs, &queue->mutex);

			queue->idle--;
			if (queue->terminated)
				break;
		}

		if (queue->jobs == NIL)
			continue;	/* job might have done by another worker */

		job = linitial(queue->jobs);
		queue->jobs = list_delete_first(queue->jobs);

		pthread_mutex_unlock(&queue->mutex);
		job->routine(job);
		free(job);
		pgut_mutex_lock(&queue->mutex);
	}
	pthread_mutex_unlock(&queue->mutex);

	return NULL;
}
