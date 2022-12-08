/*-------------------------------------------------------------------------
 *
 * datapagemap.h
 *
 * Copyright (c) 2013-2019, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef DATAPAGEMAP_H
#define DATAPAGEMAP_H

#if PG_VERSION_NUM < 160000
#include "storage/relfilenode.h"
#else
#include "storage/relfilelocator.h"
#define RelFileNode RelFileLocator
#endif
#include "storage/block.h"


struct datapagemap
{
	char	   *bitmap;
	int			bitmapsize;
};

typedef struct datapagemap datapagemap_t;
typedef struct datapagemap_iterator datapagemap_iterator_t;

extern void datapagemap_add(datapagemap_t *map, BlockNumber blkno);
extern bool datapagemap_first(datapagemap_t map, BlockNumber *start_and_result);
extern datapagemap_iterator_t *datapagemap_iterate(datapagemap_t *map);
extern bool datapagemap_next(datapagemap_iterator_t *iter, BlockNumber *blkno);

#endif							/* DATAPAGEMAP_H */
