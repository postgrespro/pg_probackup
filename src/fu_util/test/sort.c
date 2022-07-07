/* vim: set expandtab autoindent cindent ts=4 sw=4 sts=0 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ft_util.h>
#include <ft_ss_examples.h>
#include <time.h>
#include "./qsort/qsort_pg.inc.c"

static void
check_sorted(int *a, int len) {
    for (; len > 1 ; len--) {
        ft_assert(a[len-2] <= a[len-1]);
    }
}

static void
fill_ascending(int *a, int len) {
    int i = 0;
    for (; i < len ; i++)
        a[i] = i;
}

static void
fill_descending(int *a, int len) {
    int i = 0;
    for (; i < len ; i++)
        a[i] = len - i;
}

static void
fill_saw_1(int *a, int len) {
    int i = 0;
    for (; i < len/2 ; i++)
        a[i] = i;
    for (; i < len ; i++)
        a[i] = len-i;
}

static void
fill_saw_2(int *a, int len) {
    int i = 0;
    for (; i < len/2 ; i++)
        a[i] = len-i;
    for (; i < len ; i++)
        a[i] = i;
}

#define rand_init(len) \
    uint32_t r, rand = 0xdeadbeef ^ (uint32_t)len
#define rand_step do { \
    r = rand; \
    rand = rand * 0xcafedead + 0xbeef; \
    r = (r ^ (rand >> 16)) * 0x51235599; \
} while(0)


static void
fill_flip(int *a, int len) {
    int i = 0;
    rand_init(len);
    for (; i < len ; i++)
    {
        rand_step;
        a[i] = r >> 31;
    }
}

static void
fill_several(int *a, int len) {
    int i = 0;
    rand_init(len);
    for (; i < len ; i++)
    {
        rand_step;
        a[i] = r >> 28;
    }
}

static void
fill_rand(int *a, int len) {
    int i = 0;
    uint32_t max = (uint32_t)len;
    rand_init(len);
    for (; i < len ; i++)
    {
        rand_step;
        a[i] = ((uint64_t)r * max) >> 32;
    }
}

static void
fill_rand_div5(int *a, int len) {
    int i = 0;
    uint32_t max = (uint32_t)len / 5 + 1;
    rand_init(len);
    for (; i < len ; i++)
    {
        rand_step;
        a[i] = ((uint64_t)r * max) >> 32;
    }
}

static void
fill_asc_swap_tail4(int *a, int len) {
    int i = 0, j;
    rand_init(len);
    fill_ascending(a, len);
    if (len < 8)
        return;
    for (; i < 4 ; i++)
    {
        rand_step;
        j = ((uint64_t)r * (uint32_t)(len-4)) >> 32;
        ft_swap(&a[len - 1 - i], &a[j]);
    }
}

static void
fill_asc_swap_head4(int *a, int len) {
    int i = 0, j;
    rand_init(len);
    fill_ascending(a, len);
    if (len < 8)
        return;
    for (; i < 4 ; i++)
    {
        rand_step;
        j = ((uint64_t)r * (uint32_t)(len-5)) >> 32;
        ft_swap(&a[i], &a[4+j]);
    }
}

static uint64_t ncomp = 0;

static int
int_cmp_raw2(int a, int b) {
    ncomp++;
    return ft_cmp(a, b);
}

static void ft_unused
sort_shell(int *a, int len) {
    ft_shsort_int(a, len, int_cmp_raw2);
}

static void ft_unused
sort_quick(int *a, int len) {
    ft_qsort_int(a, len, int_cmp_raw2);
}

static int
compare_int(const void *pa, const void *pb) {
    int a = *(const int *)pa;
    int b = *(const int *)pb;
    ncomp++;
    return a < b ? -1 : a > b;
}

static void ft_unused
sort_qsort(int *a, int len) {
    qsort(a, len, sizeof(len), compare_int);
}

static void ft_unused
sort_qsort_pg(int *a, int len) {
    pg_qsort(a, len, sizeof(len), compare_int);
}

#define ST_SORT sort_qsort_pg2_
#define ST_ELEMENT_TYPE int
#define ST_COMPARE(a, b) (ncomp++, *(a) < *(b) ? -1 : *(a) > *(b))
#define ST_SCOPE static
#define ST_DECLARE
#define ST_DEFINE
#include "./qsort/sort_template.h"

static void
sort_qsort_pg2(int *a, int len) {
    sort_qsort_pg2_(a, len);
}


typedef void (*tfiller)(int *, int);
typedef void (*tsorter)(int *, int);

static double
mtime(void) {
    struct timespec ts = {0, 0};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec/1e9;
}


int
main(void) {
    int verbose = getenv("VERBOSE") ? atoi(getenv("VERBOSE")) : 0;
    int ex[] = {8, 4, 0, 2, 6, 32, 12};
    ft_shsort_int(ex, 7, ft_int_cmp);
    check_sorted(ex, 7);

    const char *sex[] = {"hi", "ho", "no", "yes", "obhs", "dump", "vamp"};
    ft_shsort_cstr(sex, 7, strcmp);
    for (int i = 0; i < 6; i++)
        ft_assert(strcmp(sex[i], sex[i+1]) < 0);

#define VS(v) {v, #v}
    struct { tfiller f; const char *name; } fillers[] = {
        VS(fill_ascending),
        VS(fill_descending),
        VS(fill_rand),
        VS(fill_rand_div5),
        VS(fill_several),
        VS(fill_flip),
        VS(fill_saw_1),
        VS(fill_saw_2),
        VS(fill_asc_swap_head4),
        VS(fill_asc_swap_tail4),
    };
    struct { tsorter sorter; const char* name; } sorters[] = {
        VS(sort_shell),
        VS(sort_quick),
        VS(sort_qsort),
        VS(sort_qsort_pg),
        VS(sort_qsort_pg2),
    };
    int sizes[] = {1, 2, 3, 5, 10, 20, 50, 100, 500, 1000, 2000, 100000};
    int sz, fl, srt;
    int *ar, *cp;
    for (sz = 0; sz < ft_arrsz(sizes); sz++) {
        if (verbose)
            printf("sz: %d\n", sizes[sz]);
        ar = calloc(sizeof(int), sizes[sz]);
        cp = calloc(sizeof(int), sizes[sz]);
        for(fl = 0; fl < ft_arrsz(fillers); fl++) {
            fillers[fl].f(ar, sizes[sz]);
            if (verbose)
                printf("  filler: %s\n", fillers[fl].name);
            for (srt = 0; srt < ft_arrsz(sorters); srt++) {
                double tend, tstart;
                ncomp = 0;
                memcpy(cp, ar, sizeof(int)*sizes[sz]);
                tstart = mtime();
                sorters[srt].sorter(cp, sizes[sz]);
                tend = mtime();
                check_sorted(cp, sizes[sz]);
                if (verbose)
                    printf("    %s: %.6f\tcmp: %llu\n",
                            sorters[srt].name,
                            tend - tstart,
                            (unsigned long long)ncomp);
            }
        }
        free(ar);
        free(cp);
    }
}
