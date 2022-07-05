/* vim: set expandtab autoindent cindent ts=4 sw=4 sts=4 */
#ifndef FT_SS_EXAMPLES_H
#define FT_SS_EXAMPLES_H

/*
 * Sort for integers.
 * Defines:
 *      void
 *      ft_shsort_int  (int *arr, size_t len, int (*cmp)(int, int));
 *      void
 *      ft_qsort_int   (int *arr, size_t len, int (*cmp)(int, int));
 *      void
 *      ft_shsort_int_r(int *arr, size_t len,
 *                      int (*cmp)(int, int, ft_arg_t),
 *                      ft_arg_t);
 *      void
 *      ft_qsort_int_r (int *arr, size_t len,
 *                      int (*cmp)(int, int, ft_arg_t),
 *                      ft_arg_t);
 */
#define FT_SORT         int
#define FT_SORT_TYPE    int
#include <ft_sort.inc.h>
ft_inline FT_CMP_RES ft_int_cmp(int a, int b) { return ft_cmp(a, b); }

/*
 * Sort for strings.
 * Defines:
 *      void
 *      ft_shsort_cstr  (const char **arr, size_t len,
 *                       int (*cmp)(const char*, const char*));
 *      void
 *      ft_qsort_cstr   (const char **arr, size_t len,
 *                       int (*cmp)(const char*, const char*, ft_arg_t),
 *                       ft_arg_t);
 *      void
 *      ft_shsort_cstr_r(const char **arr, size_t len,
 *                       int (*cmp)(const char*, const char*));
 *      void
 *      ft_qsort_cstr_r (const char **arr, size_t len,
 *                       int (*cmp)(const char*, const char*, ft_arg_t),
 *                       ft_arg_t);
 */
#define FT_SORT         cstr
#define FT_SORT_TYPE    const char*
#include <ft_sort.inc.h>
/*
 * While we could pass raw strcmp to sort and search functions,
 * lets define wrapper for clarity
 */
ft_inline FT_CMP_RES ft_cstr_cmp(const char *a, const char *b) {
    return ft_cmp(strcmp(a, b), 0);
}

/*
 * Sort for void*.
 * Defines:
 *      void
 *      ft_shsort_void  (void **arr, size_t len,
 *                       int (*cmp)(void*, void*));
 *      void
 *      ft_qsort_void   (void **arr, size_t len,
 *                       int (*cmp)(void*, void*, ft_arg_t),
 *                       ft_arg_t);
 *      void
 *      ft_shsort_void_r(void **arr, size_t len,
 *                       int (*cmp)(void*, void*));
 *      void
 *      ft_qsort_void_r (void **arr, size_t len,
 *                       int (*cmp)(void*, void*, ft_arg_t),
 *                       ft_arg_t);
 */
#define FT_SORT         void
#define FT_SORT_TYPE    void*
#include <ft_sort.inc.h>

/*
 * Search for integers.
 * Defines:
 *      ft_bsres_t
 *      ft_bsearch_int  (int *arr, size_t len,
 *                       int (*cmp)(int, int))
 *      ft_bsres_t
 *      ft_bsearch_int_r(int *arr, size_t len,
 *                       int (*cmp)(int, int, ft_arg_t),
 *                       ft_arg_t)
 *      size_t
 *      ft_qsort_int  (int *arr, size_t len,
 *                     bool (*eq)(int, int))
 *      ft_qsort_int_r(int *arr, size_t len,
 *                     bool (*eq)(int, int, ft_arg_t),
 *                     ft_arg_t)
 */
#define FT_SEARCH int
#define FT_SEARCH_TYPE   int
#include <ft_search.inc.h>

/*
 * Search for strings.
 * Defines:
 *      ft_bsres_t
 *      ft_bsearch_cstr  (const char **arr, size_t len,
 *                        int (*cmp)(const char*, const char*))
 *      ft_bsres_t
 *      ft_bsearch_cstr_r(const char **arr, size_t len,
 *                        int (*cmp)(const char*, const char*, ft_arg_t),
 *                        ft_arg_t)
 *      size_t
 *      ft_qsort_cstr  (const char **arr, size_t len,
 *                      int (*eq)(const char*, const char*))
 *      ft_qsort_cstr_r(const char **arr, size_t len,
 *                      int (*eq)(const char*, const char*, ft_arg_t),
 *                      ft_arg_t)
 */
#define FT_SEARCH cstr
#define FT_SEARCH_TYPE   const char*
#include <ft_search.inc.h>

/*
 * Search for void*.
 * Defines:
 *      ft_bsres_t ft_bsearch_void(void **arr, size_t len,
 *                                 int (*cmp)(void*, void*))
 *      ft_bsres_t ft_search_void(void **arr, size_t len,
 *                                 bool (*eq)(void*, void*))
 */
#define FT_SEARCH void
#define FT_SEARCH_TYPE   void*
#include <ft_search.inc.h>

#endif /* FT_SS_EXAMPLES_H */
