/* vim: set expandtab autoindent cindent ts=4 sw=4 sts=4 */
#ifndef FT_AR_EXAMPLES_H
#define FT_AR_EXAMPLES_H

/*
 * Slices and arrays for int.
 * Defines slice:
 *
 *  typedef struct { int *ptr; size_t len; } ft_slc_int_t;
 *
 *  ft_slc_int_t    ft_slc_int_make(int *ptr, size_t len);
 *  ft_slc_int_t    ft_slc_int_alloc(int *ptr, size_t len);
 *
 *  int     ft_slc_int_at(ft_slc_int_t *ptr, ssize_t at);
 *  int     ft_slc_int_set(ft_slc_int_t *ptr, ssize_t at, int v);
 *
 *  ft_slc_int_t    ft_slc_int_slice(ft_slc_int_t *ptr, ssize_t start, ssize_t end)
 *
 *  void    ft_slc_int_each(ft_slc_int_t *, void (*each)(int));
 *  void    ft_slc_int_each_r(ft_slc_int_t *, void (*each)(int, ft_arg_t), ft_arg_t);
 *
 * Defines array:
 *
 *  typedef struct { int *ptr; size_t len; size_t cap; } ft_arr_int_t;
 *  ft_arr_int_t    ft_arr_int_alloc(int *ptr, size_t len);
 *
 *  int     ft_arr_int_at (ft_arr_int_t *ptr, ssize_t at);
 *  int     ft_arr_int_set(ft_arr_int_t *ptr, ssize_t at, int v);
 *
 *  ft_slc_int_t    ft_arr_int_slice(ft_arr_int_t *ptr, ssize_t start, ssize_t end)
 *
 *  void    ft_arr_int_each  (ft_arr_int_t *, void (*each)(int));
 *  void    ft_arr_int_each_r(ft_arr_int_t *, void (*each)(int, ft_arg_t), ft_arg_t);
 *
 *  void    ft_arr_int_ensure(ft_arr_int_t *, size_t addcapa);
 *  void    ft_arr_int_recapa(ft_arr_int_t *, size_t newcapa);
 *  void    ft_arr_int_resize(ft_arr_int_t *, size_t newsize);
 *
 *  void    ft_arr_int_insert_at(ft_arr_int_t *, ssize_t at, int el);
 *  void    ft_arr_int_insert_n (ft_arr_int_t *, ssize_t at, int *el, size_t n);
 *  void    ft_arr_int_push     (ft_arr_int_t *, int el);
 *  void    ft_arr_int_append   (ft_arr_int_t *, int *el, size_t n);
 *
 *  int     ft_arr_int_del_at   (ft_arr_int_t *, ssize_t at);
 *  int     ft_arr_int_pop      (ft_arr_int_t *);
 *  void    ft_arr_int_del_slice(ft_arr_int_t *, ssize_t start, ssize_t end);
 *
 *  void    ft_array_walk  (ft_arr_int_t *,
 *                          FT_WALK_ACT (*walk)(intl))
 *  void    ft_array_walk_r(ft_arr_int_t *,
 *                          FT_WALK_ACT (*walk)(int, ft_arg_t), ft_arg_t)
 */
#define FT_SLICE        int
#define FT_SLICE_TYPE   int
#include <ft_array.inc.h>

/*
 * Slices and arrays for C strings
 */
#define FT_SLICE        cstr
#define FT_SLICE_TYPE   const char*
#include <ft_array.inc.h>

/*
 * Slices and arrays for C strings
 */
#define FT_SLICE        void
#define FT_SLICE_TYPE   void*
#include <ft_array.inc.h>

#endif
