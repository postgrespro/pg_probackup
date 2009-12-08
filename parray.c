/*-------------------------------------------------------------------------
 *
 * parray.c: pointer array collection.
 *
 * Copyright (c) 2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pg_rman.h"

/* members of struct parray are hidden from client. */
struct parray
{
	void **data;		/* poiter array, expanded if necessary */
	size_t alloced;		/* number of elements allocated */
	size_t used;		/* number of elements in use */
};

/*
 * Create new parray object.
 * Never returns NULL.
 */
parray *
parray_new(void)
{
	parray *a = pgut_malloc(sizeof(parray));

	a->data = NULL;
	a->used = 0;
	a->alloced = 0;

	parray_expand(a, 1024);

	return a;
}

/*
 * Expand array pointed by data to newsize.
 * Elements in expanded area are initialized to NULL.
 * Note: never returns NULL.
 */
void
parray_expand(parray *array, size_t newsize)
{
	void **p;

	/* already allocated */
	if (newsize <= array->alloced)
		return;

	p = pgut_realloc(array->data, sizeof(void *) * newsize);

	/* initialize expanded area to NULL */
	memset(p + array->alloced, 0, (newsize - array->alloced) * sizeof(void *));

	array->alloced = newsize;
	array->data = p;

	return; 
}

void
parray_free(parray *array)
{
	if (array == NULL)
		return;
	free(array->data);
	free(array);
}

void
parray_append(parray *array, void *elem)
{
	if (array->used + 1 > array->alloced)
		parray_expand(array, array->alloced * 2);

	array->data[array->used++] = elem;
}

void
parray_insert(parray *array, size_t index, void *elem)
{
	if (array->used + 1 > array->alloced)
		parray_expand(array, array->alloced * 2);

	memmove(array->data + index + 1, array->data + index,
		(array->alloced - index - 1) * sizeof(void *));
	array->data[index] = elem;

	/* adjust used count */
	if (array->used < index + 1)
		array->used = index + 1;
	else
		array->used++;
}

/*
 * Concatinate two parray.
 * parray_concat() appends the copy of the content of src to the end of dest.
 */
parray *
parray_concat(parray *dest, const parray *src)
{
	/* expand head array */
	parray_expand(dest, dest->used + src->used);

	/* copy content of src after content of dest */
	memcpy(dest->data + dest->used * sizeof(void *), src->data,
		src->used * sizeof(void *));
	dest->used += parray_num(src);

	return dest;
}

void
parray_set(parray *array, size_t index, void *elem)
{
	if (index > array->alloced - 1)
		parray_expand(array, index + 1);

	array->data[index] = elem;

	/* adjust used count */
	if (array->used < index + 1)
		array->used = index + 1;
}

void *
parray_get(const parray *array, size_t index)
{
	if (index > array->alloced - 1)
		return NULL;
	return array->data[index];
}

void *
parray_remove(parray *array, size_t index)
{
	void *val;

	/* removing unused element */
	if (index > array->used)
		return NULL;

	val = array->data[index];

	/* Do not move if the last element was removed. */
	if (index < array->alloced - 1)
		memmove(array->data + index, array->data + index + 1,
			(array->alloced - index - 1) * sizeof(void *));

	/* adjust used count */
	array->used--;

	return val;
}

size_t
parray_num(const parray *array)
{
	return array->used;
}

void
parray_qsort(parray *array, int(*compare)(const void *, const void *))
{
	qsort(array->data, array->used, sizeof(void *), compare);
}

void
parray_walk(parray *array, void (*action)(void *))
{
	int i;
	for (i = 0; i < array->used; i++)
		action(array->data[i]);
}

void *
parray_bsearch(parray *array, const void *key, int(*compare)(const void *, const void *))
{
	return bsearch(&key, array->data, array->used, sizeof(void *), compare);
}

