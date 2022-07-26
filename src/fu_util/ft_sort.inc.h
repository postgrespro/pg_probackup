/*
 * Sort template.
 * Accepts three macrosses:
 * - FT_SORT - suffix for functions
 * - FT_SORT_TYPE - type of array element
 * - FT_SORT_ARG  - optionally - argument to comparison function.
 *
 * Produces:
 *
 * - shell sort function
 *   void ft_shsort_FT_SORT(FT_SORT_TYPE *arr, size_t len,
 *                          int (*cmp)(FT_SORT_TYPE a, FT_SORT_TYPE b))
 *   void ft_shsort_FT_SORT_r(FT_SORT_TYPE *arr, size_t len,
 *                          int (*cmp)(FT_SORT_TYPE a, FT_SORT_TYPE b, ft_arg_t arg),
 *                          ft_arg_t arg)
 *
 * - quick sort function
 *   void ft_qsort_FT_SORT(FT_SORT_TYPE *arr, size_t len,
 *                         int (*cmp)(FT_SORT_TYPE a, FT_SORT_TYPE b))
 *   void ft_qsort_FT_SORT_r(FT_SORT_TYPE *arr, size_t len,
 *                         int (*cmp)(FT_SORT_TYPE a, FT_SORT_TYPE b, ft_arg_t arg),
 *                         ft_arg_t arg)
 */

#include <ft_util.h>

#ifndef FT_SORT
#error "FT_SORT should be defined"
#endif

#ifndef FT_SORT_TYPE
#error "FT_SORT_TYPE should be defined"
#endif

#define ft_func_shsort      fm_cat(ft_shsort_, FT_SORT)
#define ft_func_shsort_r    fm_cat3(ft_shsort_, FT_SORT, _r)
#define ft_func_qsort       fm_cat(ft_qsort_, FT_SORT)
#define ft_func_qsort_r     fm_cat3(ft_qsort_, FT_SORT, _r)

#define _ft_cmp_def_r(x) int (*x)(FT_SORT_TYPE a, FT_SORT_TYPE b, ft_arg_t arg)
#define _ft_cmp_def(x)   int (*x)(FT_SORT_TYPE a, FT_SORT_TYPE b)

ft_inline ft_optimize3 void
ft_func_shsort_r(FT_SORT_TYPE *arr, size_t len, _ft_cmp_def_r(cmp), ft_arg_t arg) {
    FT_SORT_TYPE el;
    size_t m, n, d;
    ft_dbg_assert((ssize_t)len >= 0);
    if (len < 2) {}
    else if (len == 2) {
        if (cmp(arr[1], arr[0], arg) < 0) {
            ft_swap(&arr[1], &arr[0]);
        }
    } else {
        d = (size_t)(len / 1.4142135) | 1;
        for (;;) {
            for (m = d; m < len; m++) {
                n = m;
                el = arr[n];
                for (; n >= d && cmp(el, arr[n - d], arg) < 0; n -= d) {
                    arr[n] = arr[n-d];
                }
                arr[n] = el;
            }
            if (d == 1) break;
            else if (d < 10) d = 1;
            else if (d <= 24) d = (size_t)(d / 2.221);
            else d = (size_t)(d / 2.7182818) | 1;
        }
    }
}

ft_inline ft_optimize3 void
ft_func_shsort(FT_SORT_TYPE *arr, size_t len, _ft_cmp_def(cmp)) {
    ft_func_shsort_r(arr, len, (_ft_cmp_def_r())(void*) cmp, ft_mka_z());
}

ft_inline ft_optimize3 void
ft_func_qsort_r(FT_SORT_TYPE *arr_, size_t len_, _ft_cmp_def_r(cmp), ft_arg_t arg) {
    FT_SORT_TYPE    *arr = arr_;
    FT_SORT_TYPE     pivot;
    size_t      len = len_;
    size_t      m, n, mid[5];
#define STSZ 32
    int const   stsz = STSZ;
    int         sttop = 0;
    struct { FT_SORT_TYPE *ar; size_t ln; } stack[STSZ];
#undef STSZ

    ft_dbg_assert((ssize_t)len >= 0);

    stack[sttop].ar = arr; stack[sttop].ln = len; sttop++;
    while (sttop > 0) {
        sttop--;
        arr = stack[sttop].ar; len = stack[sttop].ln;
        /* check for fallback to shell sort */
        if (len < 24 || (sttop == stsz-1)) {
            ft_func_shsort_r(arr, len, cmp, arg);
            continue;
        }
        else {
            m = 1;
            while (m < len && cmp(arr[m-1], arr[m], arg) < 0) m++;
            if (m == len)
                continue;
        }
        /* find a pivot as median of 5 */
        mid[0] = 0;
        mid[2] = len/2;
        mid[1] = 1 + ft_randn(mid[2]-2);
        mid[3] = mid[2] + 1 + ft_randn(mid[2]-2);
        mid[4] = len-1;
        /* fast median of 5 */
        {
            static int const ix[] = {0, 1, 3, 4, 0, 3, 1, 4, 1, 2, 2, 3, 1, 2};
            for (int i = 0; i < ft_arrsz(ix); i += 2) {
                if (cmp(arr[mid[ix[i]]], arr[mid[ix[i+1]]], arg) < 0)
                    ft_swap(&mid[ix[i]], &mid[ix[i+1]]);
            }
        }
        /* make a[i] <= a[l] if i < m */
        pivot = arr[mid[2]];
        m = 0; n = len;
        for (;;) {
            while (m < n && cmp(pivot, arr[m], arg) >= 0) m++;
            while (m < n && cmp(pivot, arr[n-1], arg) < 0) n--;
            if (m == n) break;
            ft_swap(&arr[m], &arr[n-1]);
            m++; n--;
        }
        if (m < len) {
            /* lgr - left greater */
            bool    lgr = m > len - m;
            stack[sttop+(1-lgr)].ar = arr;
            stack[sttop+(1-lgr)].ln = m;
            stack[sttop+lgr].ar = arr + m;
            stack[sttop+lgr].ln = len - m;
            sttop += 2;
        } else {
            /* all <= pivot */
            /* make a[i] < a[l] if i < m*/
            ft_dbg_assert(n == len);
            for (;m > 0 && cmp(arr[m-1], pivot, arg) >= 0; m--);
            n = m;
            for (;m > 0;m--) {
                if (cmp(arr[m-1], pivot, arg) >= 0) {
                    if (m < n) {
                        ft_swap(&arr[m-1], &arr[n-1]);
                    }
                    n--;
                }
            }
            if (n > 0) {
                stack[sttop].ar = arr; stack[sttop].ln = n;
                sttop++;
            }
        }
    }
}

ft_inline ft_optimize3 void
ft_func_qsort(FT_SORT_TYPE *arr, size_t len, _ft_cmp_def(cmp)) {
    ft_func_qsort_r(arr, len, (_ft_cmp_def_r())(void*) cmp, ft_mka_z());
}


#undef FT_SORT
#undef FT_SORT_TYPE
#undef ft_func_shsort
#undef ft_func_shsort_r
#undef ft_func_qsort
#undef ft_func_qsort_r
#undef _ft_cmp_def_r
#undef _ft_cmp_def

