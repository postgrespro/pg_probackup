#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#define FU_MALLOC_RAW
#include "../ft_util.h"
#include "../ft_ss_examples.h"
#include "../ft_ar_examples.h"
#include <time.h>

static void
check_equal_fun(int *a, int *b, int len)
{
	for (len--; len >= 0; len--)
		ft_assert(a[len] == b[len]);
}

#define check_equal(_a_, ...) do { \
	int _cmp_[] = {__VA_ARGS__}; \
	int _len_ = ft_arrsz(_cmp_); \
	ft_assert((_a_)->len == _len_); \
	check_equal_fun((_a_)->ptr, _cmp_, _len_); \
} while (0)

static int wlkcnt = 0;
static FT_WALK_ACT
walk_simple(int *el) {
    wlkcnt++;
    if (*el > 8)
        return FT_WALK_BREAK;
    return FT_WALK_CONT;
}

static FT_WALK_ACT
walk_del(int *el, ft_arg_t v) {
    wlkcnt++;
    if (*el == ft_arg_i(v))
        return FT_WALK_DEL;
    return FT_WALK_CONT;
}

static FT_WALK_ACT
walk_del2(int *el, ft_arg_t v) {
    wlkcnt++;
    if (*el == ft_arg_i(v))
        return FT_WALK_DEL_BREAK;
    return FT_WALK_CONT;
}


int
main(void) {
    ft_arr_int_t arr = ft_arr_init();
	int v, i;
    ft_bsres_t bsres;

	ft_arr_int_push(&arr, 1);
	check_equal(&arr, 1);

	ft_arr_int_push(&arr, 10);
	ft_arr_int_push(&arr, 5);
	ft_arr_int_push(&arr, 25);
	ft_arr_int_push(&arr, 15);
	ft_arr_int_push(&arr, 2);

	check_equal(&arr, 1, 10, 5, 25, 15, 2);

    ft_arr_int_resize(&arr, 1);
	check_equal(&arr, 1);
    ft_arr_int_append(&arr, ((int[]){10, 5, 25, 15, 2}), 5);
	check_equal(&arr, 1, 10, 5, 25, 15, 2);

	ft_assert(ft_arr_int_at(&arr, 1) == 10);
	ft_assert(ft_arr_int_at(&arr, 5) == 2);

	ft_shsort_int(ft_2ptrlen(arr), ft_int_cmp);
	check_equal(&arr, 1, 2, 5, 10, 15, 25);
	ft_assert(ft_arr_int_at(&arr, 2) == 5);
	ft_assert(ft_arr_int_at(&arr, 5) == 25);

	ft_arr_int_set(&arr, 2, 8);
	check_equal(&arr, 1, 2, 8, 10, 15, 25);

	bsres = ft_bsearch_int(ft_2ptrlen(arr), 14, ft_int_cmp);
	ft_assert(bsres.ix == 4);
	ft_assert(!bsres.eq);
	bsres = ft_bsearch_int(ft_2ptrlen(arr), 2, ft_int_cmp);
	ft_assert(bsres.ix == 1);
	ft_assert(bsres.eq);

	i = ft_search_int(ft_2ptrlen(arr), 2, ft_int_cmp);
	ft_assert(i == 1);
	i = ft_search_int(ft_2ptrlen(arr), 3, ft_int_cmp);
	ft_assert(i == 6);

	v = ft_arr_int_pop(&arr);
	ft_assert(v == 25);
	check_equal(&arr, 1, 2, 8, 10, 15);

	v = ft_arr_int_del_at(&arr, 1);
	ft_assert(v == 2);
	check_equal(&arr, 1, 8, 10, 15);

	ft_arr_int_insert_at(&arr, 3, 11);
	check_equal(&arr, 1, 8, 10, 11, 15);
	ft_arr_int_insert_at(&arr, 5, 20);
	check_equal(&arr, 1, 8, 10, 11, 15, 20);

    ft_arr_int_del_slice(&arr, 3, 5);
	check_equal(&arr, 1, 8, 10, 20);

	ft_arr_int_insert_n(&arr, 1, (int[]){7, 7, 9, 9}, 4);
	check_equal(&arr, 1, 7, 7, 9, 9, 8, 10, 20);

    ft_arr_int_del_slice(&arr, -2, FT_SLICE_END);
	check_equal(&arr, 1, 7, 7, 9, 9, 8);

    wlkcnt = 0;
    ft_arr_int_walk(&arr, walk_simple);
    ft_assert(wlkcnt == 4);

    wlkcnt = 0;
    ft_arr_int_walk_r(&arr, walk_del, ft_mka_i(9));
    ft_assert(wlkcnt == 6);
	check_equal(&arr, 1, 7, 7, 8);

    wlkcnt = 0;
    ft_arr_int_walk_r(&arr, walk_del2, ft_mka_i(7));
    ft_assert(wlkcnt == 2);
	check_equal(&arr, 1, 7, 8);

	ft_arr_int_free(&arr);
	ft_assert(arr.len == 0);
	ft_assert(arr.ptr == NULL);

}
