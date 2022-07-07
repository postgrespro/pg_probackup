#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ft_util.h>
#include <ft_ss_examples.h>
#include <time.h>

int
main(void) {
	int ex[] = {1, 3, 5, 7, 8, 9};
	ft_bsres_t bs;

	bs = ft_bsearch_int(ex, 6, 0, ft_int_cmp);
	ft_assert(bs.ix == 0);
	ft_assert(!bs.eq);
	bs = ft_bsearch_int(ex, 6, 1, ft_int_cmp);
	ft_assert(bs.ix == 0);
	ft_assert(bs.eq);
	bs = ft_bsearch_int(ex, 6, 2, ft_int_cmp);
	ft_assert(bs.ix == 1);
	ft_assert(!bs.eq);
	bs = ft_bsearch_int(ex, 6, 3, ft_int_cmp);
	ft_assert(bs.ix == 1);
	ft_assert(bs.eq);
	bs = ft_bsearch_int(ex, 6, 4, ft_int_cmp);
	ft_assert(bs.ix == 2);
	ft_assert(!bs.eq);
	bs = ft_bsearch_int(ex, 6, 5, ft_int_cmp);
	ft_assert(bs.ix == 2);
	ft_assert(bs.eq);
	bs = ft_bsearch_int(ex, 6, 6, ft_int_cmp);
	ft_assert(bs.ix == 3);
	ft_assert(!bs.eq);
	bs = ft_bsearch_int(ex, 6, 7, ft_int_cmp);
	ft_assert(bs.ix == 3);
	ft_assert(bs.eq);
	bs = ft_bsearch_int(ex, 6, 8, ft_int_cmp);
	ft_assert(bs.ix == 4);
	ft_assert(bs.eq);
	bs = ft_bsearch_int(ex, 6, 9, ft_int_cmp);
	ft_assert(bs.ix == 5);
	ft_assert(bs.eq);
	bs = ft_bsearch_int(ex, 6, 10, ft_int_cmp);
	ft_assert(bs.ix == 6);
	ft_assert(!bs.eq);
}
