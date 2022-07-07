/* vim: set expandtab autoindent cindent ts=4 sw=4 sts=0 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <ft_util.h>
#include <ft_ss_examples.h>
#include "./qsort/qsort.inc.c"
#include "./qsort/qsort_pg.inc.c"

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
    if (len < 16)
        return;
    for (; i < 8 ; i++)
    {
        rand_step;
        j = ((uint64_t)r * (uint32_t)(len-9)) >> 32;
        ft_swap(&a[len - 1 - i], &a[j]);
    }
}

static void
fill_asc_swap_head4(int *a, int len) {
    int i = 0, j;
    rand_init(len);
    fill_ascending(a, len);
    if (len < 16)
        return;
    for (; i < 8 ; i++)
    {
        rand_step;
        j = ((uint64_t)r * (uint32_t)(len-9)) >> 32;
        ft_swap(&a[i], &a[8+j]);
    }
}

static const char **ref = NULL;

static void
fill_ref(int len) {
    int i = 0, ignore ft_unused;
    ref = calloc(sizeof(char*), len);
    for (; i < len; i++)
    {
        ref[i] = ft_asprintf("%08x", i).ptr;
    }
}

static void
clear_ref(int len) {
    int i = 0;
    ref = calloc(sizeof(char*), len);
    for (; i < len; i++)
        free((void*)ref[i]);
    free(ref);
    ref = NULL;
}

static uint64_t ncomp = 0;

static FT_CMP_RES
compare_int_raw(int a, int b) {
    ncomp++;
    return ft_cstr_cmp(ref[a], ref[b]);
}

static void ft_unused
sort_shell(int *a, int len) {
    ft_shsort_int(a, len, compare_int_raw);
}

static void ft_unused
sort_quick(int *a, int len) {
    ft_qsort_int(a, len, compare_int_raw);
}

static int
compare_int(const void *pa, const void *pb) {
    int a = *(const int *)pa;
    int b = *(const int *)pb;
    ncomp++;
    return strcmp(ref[a], ref[b]);
}

static int
compare_int_v(const void *pa, const void *pb, void *p) {
    int a = *(const int *)pa;
    int b = *(const int *)pb;
    ncomp++;
    return strcmp(ref[a], ref[b]);
}

static void ft_unused
sort_qsort(int *a, int len) {
    qsort(a, len, sizeof(len), compare_int);
}

static void ft_unused
sort_qsort_cpy(int *a, int len) {
    _quicksort(a, len, sizeof(len), compare_int_v, NULL);
}

static void ft_unused
sort_qsort_pg(int *a, int len) {
    pg_qsort(a, len, sizeof(len), compare_int);
}

static void
check_sorted(int *a, int len) {
    for (; len > 1 ; len--) {
        ft_assert(strcmp(ref[a[len-2]], ref[a[len-1]]) <= 0);
    }
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
        VS(sort_qsort_cpy),
        VS(sort_qsort_pg),
    };
    int sizes[] = {1, 2, 3, 5, 10, 20, 50, 100, 500, 1000, 2000, 100000};
    int sz, fl, srt;
    int *ar, *cp;
    for (sz = 0; sz < ft_arrsz(sizes); sz++) {
        if (verbose)
            printf("sz: %d\n", sizes[sz]);
        ar = calloc(sizeof(int), sizes[sz]);
        cp = calloc(sizeof(int), sizes[sz]);
        fill_ref(ft_max(sizes[sz]+2, 32));
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
        clear_ref(ft_max(sizes[sz]+2, 32));
    }
}
