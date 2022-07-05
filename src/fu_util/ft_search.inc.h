/*
 * Sort template.
 * Accepts four macrosses:
 * - FT_SEARCH  - suffix for functions
 * - FT_SEARCH_TYPE    - type of array element
 * - FT_SEARCH_PATTERN - optional type of key element. Defined to FT_SEARCH_TYPE if not present
 *
 * Produces:
 *
 *  
 *
 * - binary search function
 *   It returns index of first element that is equal of greater to pattern in
 *   a sorted array.
 *
 *   ft_bsres_t
 *   ft_bsearch_FT_SEARCH(
 *          FT_SEARCH_TYPE *arr, size_t len, FT_SEARCH_PATTERN pat,
 *          FT_CMP_RES (*cmp)(FT_SEARCH_TYPE el, FT_SEARCH_PATTERN pat));
 *   ft_bsres_t
 *   ft_bsearch_FT_SEARCH_r(
 *          FT_SEARCH_TYPE *arr, size_t len, FT_SEARCH_PATTERN pat,
 *          FT_CMP_RES (*cmp)(FT_SEARCH_TYPE el, FT_SEARCH_PATTERN pat, ft_arg_t arg),
 *          ft_arg_t arg);
 *
 * - linear search function
 *   It returns index of first element that matches predicate, or len.
 *
 *   size_t
 *   ft_search_FT_SEARCH(
 *          FT_SEARCH_TYPE *arr, size_t len, FT_SEARCH_PATTERN pat,
 *          FT_CMP_RES (*eq)(FT_SEARCH_TYPE el, FT_SEARCH_PATTERN pat))
 *   or
 *   size_t
 *   ft_search_FT_SEARCH(
 *          FT_SEARCH_TYPE *arr, size_t len, FT_SEARCH_PATTERN pat,
 *          FT_CMP_RES (*eq)(FT_SEARCH_TYPE el, FT_SEARCH_PATTERN pat, ft_arg_t arg),
 *          ft_arg_t arg)
 *
 */

#include <ft_util.h>

#define ft_func_bsearch     fm_cat(ft_bsearch_, FT_SEARCH)
#define ft_func_bsearch_r   fm_cat3(ft_bsearch_, FT_SEARCH, _r)
#define ft_func_search      fm_cat(ft_search_, FT_SEARCH)
#define ft_func_search_r    fm_cat3(ft_search_, FT_SEARCH, _r)
#ifndef FT_SEARCH_PATTERN
#define FT_SEARCH_PATTERN FT_SEARCH_TYPE
#endif

#define _ft_cmp_def_r(x) \
    FT_CMP_RES (*x)(FT_SEARCH_TYPE a, FT_SEARCH_PATTERN b, ft_arg_t arg)
#define _ft_cmp_def(x) \
    FT_CMP_RES (*x)(FT_SEARCH_TYPE a, FT_SEARCH_PATTERN b)

ft_inline ft_bsres_t
ft_func_bsearch_r(FT_SEARCH_TYPE *arr, size_t len, FT_SEARCH_PATTERN pat,
                _ft_cmp_def_r(cmp), ft_arg_t arg) {
    ft_bsres_t res = {len, false};
    size_t l, r, m;
    int cmpres;
    l = 0;
    r = len;
    while (l < r) {
        m = l + (r - l) / 2;
        cmpres = cmp(arr[m], pat, arg);
        if (cmpres >= FT_CMP_EQ) {
            r = m;
            res.eq = cmpres == FT_CMP_EQ;
        } else {
            l = m + 1;
        }
    }
    res.ix = l;
    return res;
}

ft_inline ft_bsres_t
ft_func_bsearch(FT_SEARCH_TYPE *arr, size_t len, FT_SEARCH_PATTERN pat,
                _ft_cmp_def(cmp)) {
    return ft_func_bsearch_r(arr, len, pat, (_ft_cmp_def_r()) cmp, ft_mka_z());
}

ft_inline size_t
ft_func_search_r(FT_SEARCH_TYPE *arr, size_t len, FT_SEARCH_PATTERN pat,
                _ft_cmp_def_r(cmp), ft_arg_t arg) {
    size_t i;
    for (i = 0; i < len; i++) {
        if (cmp(arr[i], pat, arg) == FT_CMP_EQ)
            break;
    }
    return i;
}

ft_inline size_t
ft_func_search(FT_SEARCH_TYPE *arr, size_t len, FT_SEARCH_PATTERN pat,
                _ft_cmp_def(cmp)) {
    return ft_func_search_r(arr, len, pat, (_ft_cmp_def_r()) cmp, ft_mka_z());
}

#undef FT_SEARCH
#undef FT_SEARCH_TYPE
#undef FT_SEARCH_PATTERN
#ifdef FT_SEARCH_ARG
#undef FT_SEARCH_ARG
#endif
#undef ft_func_bsearch
#undef ft_func_bsearch_r
#undef ft_func_search
#undef ft_func_search_r
#undef _ft_cmp_def_r
#undef _ft_cmp_def
