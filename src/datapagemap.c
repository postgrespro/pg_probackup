/*-------------------------------------------------------------------------
 *
 * datapagemap.c
 *	  A data structure for keeping track of data pages that have changed.
 *
 * This is a fairly simple bitmap.
 *
 * Copyright (c) 2013-2019, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "datapagemap.h"

/*****
 * Public functions
 */

/*
 * Add a block to the bitmap.
 */
void
datapagemap_add(datapagemap_t *map, BlockNumber blkno)
{
	int			offset;
	int			bitno;
	int			oldsize = map->bitmapsize;

	offset = blkno / 8;
	bitno = blkno % 8;

	/* enlarge or create bitmap if needed */
	if (oldsize <= offset)
	{
		int			newsize;

		/*
		 * The minimum to hold the new bit is offset + 1. But add some
		 * headroom, so that we don't need to repeatedly enlarge the bitmap in
		 * the common case that blocks are modified in order, from beginning
		 * of a relation to the end.
		 */
		newsize = (oldsize == 0) ? 16 : oldsize;
		while (newsize <= offset) {
			newsize <<= 1;
		}

		map->bitmap = pg_realloc(map->bitmap, newsize);

		/* zero out the newly allocated region */
		memset(&map->bitmap[oldsize], 0, newsize - oldsize);

		map->bitmapsize = newsize;
	}

	/* Set the bit */
	map->bitmap[offset] |= (1 << bitno);
}

bool
datapagemap_first(datapagemap_t map, BlockNumber *start_and_result)
{
	BlockNumber blk = *start_and_result;
	for (;;)
	{
		int			nextoff = blk / 8;
		int			bitno = blk % 8;
		unsigned char c;

		if (nextoff >= map.bitmapsize)
			break;

		c = map.bitmap[nextoff] >> bitno;
		if (c == 0)
			blk += 8 - bitno;
		else if (c&1)
		{
			*start_and_result = blk;
			return true;
		}
		else
			blk += ffs(c)-1;
	}

	/* no more set bits in this bitmap. */
	*start_and_result = UINT32_MAX;
	return false;
}