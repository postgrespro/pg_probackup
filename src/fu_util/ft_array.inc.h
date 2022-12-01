/* vim: set expandtab autoindent cindent ts=4 sw=4 sts=4 */
#ifndef FU_UTIL_H
#error "ft_util.h should be included"
#endif

/*
 * Accepts 2 macroses:
 * - FT_SLICE - name suffix
 * - FT_SLICE_TYPE - element type.
 *
 * Produces:
 *   typedef struct {
 *      FT_SLICE_TYPE* ptr;
 *      size_t         len;
 *   } ft_slc_FT_SLICE_t;
 *
 *   typedef struct {
 *      FT_SLICE_TYPE* ptr;
 *      size_t         len;
 *      size_t         cap;
 *   } ft_arr_FT_SLICE_t;
 *
 * - create slice struct
 *
 *   ft_slc_FT_SLICE_t
 *   ft_slc_FT_SLICE_make(FT_SLICE_TYPE_t *ptr, size_t len);
 *
 * - allocate memory and copy data
 *
 *   ft_slc_FT_SLICE_t
 *   ft_slc_FT_SLICE_alloc(FT_SLICE_TYPE_t *ptr, size_t len);
 *
 *   ft_arr_FT_SLICE_t
 *   ft_arr_FT_SLICE_alloc(FT_SLICE_TYPE_t *ptr, size_t len);
 *
 * - take an element.
 *   if `at < 0`, then takes at `len - at`
 *
 *   FT_SLICE_TYPE
 *   ft_slc_FT_SLICE_at(ft_slc_FT_SLICE_t *sl, ssize_t at);
 *
 *   FT_SLICE_TYPE
 *   ft_arr_FT_SLICE_at(ft_arr_FT_SLICE_t *sl, ssize_t at);
 *
 * - set an element.
 *   if `at < 0`, then sets at `len - at`
 *
 *   FT_SLICE_TYPE
 *   ft_slc_FT_SLICE_set(ft_slc_FT_SLICE_t *sl, ssize_t at, FT_SLICE_TYPE el);
 *
 *   FT_SLICE_TYPE
 *   ft_arr_FT_SLICE_set(ft_arr_FT_SLICE_t *sl, ssize_t at, FT_SLICE_TYPE el);
 *
 * - take subslice
 *   `start` and `end` are normalized as index in `*_at`, `*_set` functions.
 *   Additionally, FT_SLICE_END could be used as a slice end.
 *
 *   ft_slc_FT_SLICE_t
 *   ft_slc_FT_SLICE_slice(ft_slc_FT_SLICE_t *sl, ssize_t start, ssize end);
 *
 *   ft_slc_FT_SLICE_t
 *   ft_arr_FT_SLICE_slice(ft_arr_FT_SLICE_t *sl, ssize_t start, ssize end);
 *
 * - call function for each element by value
 *
 *   void
 *   ft_slc_FT_SLICE_each(ft_slc_FT_SLICE_t *, void (*)(FT_SLICE_TYPE));
 *
 *   void
 *   ft_slc_FT_SLICE_each_r(ft_slc_FT_SLICE_t *,
 *                          void (*)(FT_SLICE_TYPE, ft_arg_t), ft_arg_t);
 *
 *   void
 *   ft_arr_FT_SLICE_each(ft_arr_FT_SLICE_t *, void (*)(FT_SLICE_TYPE));
 *
 *   void
 *   ft_arr_FT_SLICE_each_r(ft_arr_FT_SLICE_t *,
 *                          void (*)(FT_SLICE_TYPE, ft_arg_t), ft_arg_t);
 *
 * Following are only for ARRAY:
 *
 * - ensure array's capacity for additional elements
 *
 *   void
 *   ft_arr_FT_SLICE_ensure(ft_arr_FT_SLICE_t *arr, size_t addelems);
 *
 * - ensure array's total capacity (or decrease it)
 *   It rounds up capacity to power of 2.
 *   It will panic if new capacity is less than lenght.
 *
 *   void
 *   ft_arr_FT_SLICE_recapa(ft_arr_FT_SLICE_t *arr, size_t newcapa);
 *
 * - truncate or zero extend array.
 *
 *   void
 *   ft_arr_FT_SLICE_resize(ft_arr_FT_SLICE_t *arr, size_t newsize);
 *
 * - push one or many elements to end of array
 *
 *   void
 *   ft_arr_FT_SLICE_push(ft_arr_FT_SLICE_t *arr, FT_SLICE_TYPE el);
 *
 *   void
 *   ft_arr_FT_SLICE_append(ft_arr_FT_SLICE_t *arr, FT_SLICE_TYPE *el, size_t n);
 *
 * - insert one or many elements into middle of array
 *
 *   void
 *   ft_arr_FT_SLICE_insert_at(ft_arr_FT_SLICE_t *arr, size_t at, FT_SLICE_TYPE el);
 *
 *   void
 *   ft_arr_FT_SLICE_insert_n(ft_arr_FT_SLICE_t *arr, size_t at, FT_SLICE_TYPE *el, size_t n);
 *
 * - delete one or many elements
 *
 *   FT_SLICE_TYPE
 *   ft_arr_FT_SLICE_pop(ft_arr_FT_SLICE_t *arr);
 *
 *   FT_SLICE_TYPE
 *   ft_arr_FT_SLICE_del_at(ft_arr_FT_SLICE_t *arr, size_t at);
 *
 *   void
 *   ft_arr_FT_SLICE_del_slice(ft_arr_FT_SLICE_T *arr, ssize_t start, ssize_t end);
 *
 * - controllable array iteration.
 *   Callback may tell what to do:
 *     FT_WALK_CONT         - continue
 *     FT_WALK_BREAK        - break
 *     FT_WALK_DEL          - delete current element and continue
 *     FT_WALK_DEL_BREAK    - delete current element and break
 *
 *   void
 *   ft_arr_FT_SLICE_walk(ft_arr_FT_SLICE_T *arr,
 *                        FT_WALK_ACT (*walk)(FT_SLICE_TYPE *el)) 
 *
 *   void
 *   ft_arr_FT_SLICE_walk_r(ft_arr_FT_SLICE_T *arr,
 *                          FT_WALK_ACT (*walk)(FT_SLICE_TYPE *el, ft_arg_t arg),
 *                          ft_arg_t arg) 
 */

#define ft_slice_pref       fm_cat(ft_slc_, FT_SLICE)
#define ft_array_pref       fm_cat(ft_arr_, FT_SLICE)
#define ft_slice_type       fm_cat(ft_slice_pref,_t)
#define ft_array_type       fm_cat(ft_array_pref,_t)

#define ft_slice_make       fm_cat(ft_slice_pref, _make)
#define ft_slice_alloc      fm_cat(ft_slice_pref, _alloc)
#define ft_slice_at         fm_cat(ft_slice_pref, _at)
#define ft_slice_set        fm_cat(ft_slice_pref, _set)
#define ft_slice_slice      fm_cat(ft_slice_pref, _slice)
#define ft_slice_each       fm_cat(ft_slice_pref, _each)
#define ft_slice_each_r     fm_cat(ft_slice_pref, _each_r)

#define ft_array_alloc      fm_cat(ft_array_pref, _alloc)
#define ft_array_at         fm_cat(ft_array_pref, _at)
#define ft_array_set        fm_cat(ft_array_pref, _set)
#define ft_array_slice      fm_cat(ft_array_pref, _slice)
#define ft_array_each       fm_cat(ft_array_pref, _each)
#define ft_array_each_r     fm_cat(ft_array_pref, _each_r)

#define ft_array_ensure     fm_cat(ft_array_pref, _ensure)
#define ft_array_recapa     fm_cat(ft_array_pref, _recapa)
#define ft_array_resize     fm_cat(ft_array_pref, _resize)
#define ft_array_reset_for_reuse     fm_cat(ft_array_pref, _reset_for_reuse)
#define ft_array_free       fm_cat(ft_array_pref, _free)

#define ft_array_insert_at  fm_cat(ft_array_pref, _insert_at)
#define ft_array_insert_n   fm_cat(ft_array_pref, _insert_n)
#define ft_array_push       fm_cat(ft_array_pref, _push)
#define ft_array_push2      fm_cat(ft_array_pref, _push2)
#define ft_array_append     fm_cat(ft_array_pref, _append)

#define ft_array_del_at     fm_cat(ft_array_pref, _del_at)
#define ft_array_del_slice  fm_cat(ft_array_pref, _del_slice)
#define ft_array_pop        fm_cat(ft_array_pref, _pop)

#define ft_array_walk       fm_cat(ft_array_pref, _walk)
#define ft_array_walk_r     fm_cat(ft_array_pref, _walk_r)

#if __SIZEOF_SIZE_T__ < 8
#define HUGE_SIZE ((size_t)UINT_MAX >> 2)
#else
#define HUGE_SIZE ((size_t)UINT_MAX << 16)
#endif

#ifndef NDEBUG
/* try to catch uninitialized vars */
#define ft_slice_invariants(slc) \
    ft_dbg_assert(ft_mul_size(sizeof(FT_SLICE_TYPE), slc->len) < HUGE_SIZE); \
    ft_dbg_assert((slc->len == 0) || (slc->ptr != NULL))
#define ft_array_invariants(arr) \
    ft_dbg_assert(ft_mul_size(sizeof(FT_SLICE_TYPE), arr->len) < HUGE_SIZE); \
    ft_dbg_assert(ft_mul_size(sizeof(FT_SLICE_TYPE), arr->len) < HUGE_SIZE); \
    ft_dbg_assert(arr->cap >= arr->len); \
    ft_dbg_assert((arr->cap == 0) || (arr->ptr != NULL))
#else
#define ft_slice_invariants(slc) \
    ft_dbg_assert((slc->len == 0) || (slc->ptr != NULL))
#define ft_array_invariants(arr) \
    ft_dbg_assert(arr->cap >= arr->len); \
    ft_dbg_assert((arr->cap == 0) || (arr->ptr != NULL))
#endif

typedef struct ft_slice_type {
    FT_SLICE_TYPE    *ptr;
    size_t      len;
} ft_slice_type;

typedef struct ft_array_type {
    FT_SLICE_TYPE    *ptr;
    size_t      len;
    size_t      cap;
} ft_array_type;

ft_inline ft_slice_type
ft_slice_make(FT_SLICE_TYPE *ptr, size_t len) {
    return (ft_slice_type){.ptr = ptr, .len = len};
}

ft_inline ft_slice_type
ft_slice_alloc(FT_SLICE_TYPE *ptr, size_t len) {
    FT_SLICE_TYPE   *newptr = ft_malloc_arr(sizeof(FT_SLICE_TYPE), len);
    memcpy(newptr, ptr, sizeof(FT_SLICE_TYPE) * len);
    return (ft_slice_type){.ptr = newptr, .len = len};
}

ft_inline FT_SLICE_TYPE
ft_slice_at(const ft_slice_type *sl, ssize_t at) {
    ft_slice_invariants(sl);
    at = ft__index_unify(at, sl->len);
    return sl->ptr[at];
}

ft_inline FT_SLICE_TYPE
ft_slice_set(const ft_slice_type *sl, ssize_t at, FT_SLICE_TYPE val) {
    ft_slice_invariants(sl);
    at = ft__index_unify(at, sl->len);
    sl->ptr[at] = val;
    return val;
}

ft_inline ft_slice_type
ft_slice_slice(const ft_slice_type *sl, ssize_t start, ssize_t end) {
    ft_slice_invariants(sl);
    start = ft__slcindex_unify(start, sl->len);
    end = ft__slcindex_unify(end, sl->len);
    ft_assert(start <= end);
    return (ft_slice_type){.ptr = sl->ptr + start, .len = end - start};
}

ft_inline void
ft_slice_each_r(const ft_slice_type *sl,
        void (*each)(FT_SLICE_TYPE el, ft_arg_t arg),
        ft_arg_t arg) {
    size_t  i;
    ft_slice_invariants(sl);
    for (i = 0; i < sl->len; i++) {
        each(sl->ptr[i], arg);
    }
}

ft_inline void
ft_slice_each(const ft_slice_type *sl, void (*each)(FT_SLICE_TYPE el)) {
    size_t  i;
    ft_slice_invariants(sl);
    for (i = 0; i < sl->len; i++) {
        each(sl->ptr[i]);
    }
}

/* ARRAY */

ft_inline FT_SLICE_TYPE
ft_array_at(const ft_array_type *arr, ssize_t at) {
    ft_array_invariants(arr);
    at = ft__index_unify(at, arr->len);
    return arr->ptr[at];
}

ft_inline FT_SLICE_TYPE
ft_array_set(const ft_array_type *arr, ssize_t at, FT_SLICE_TYPE val) {
    ft_array_invariants(arr);
    at = ft__index_unify(at, arr->len);
    arr->ptr[at] = val;
    return val;
}

ft_inline ft_slice_type
ft_array_slice(const ft_array_type *arr, ssize_t start, ssize_t end) {
    ft_array_invariants(arr);
    start = ft__slcindex_unify(start, arr->len);
    end = ft__slcindex_unify(end, arr->len);
    ft_assert(start <= end);
    return (ft_slice_type){.ptr = arr->ptr + start, .len = end - start};
}

ft_inline void
ft_array_each_r(const ft_array_type *arr,
        void (*each)(FT_SLICE_TYPE el, ft_arg_t arg),
        ft_arg_t arg) {
    size_t  i;
    ft_array_invariants(arr);
    for (i = 0; i < arr->len; i++) {
        each(arr->ptr[i], arg);
    }
}

ft_inline void
ft_array_each(const ft_array_type *arr, void (*each)(FT_SLICE_TYPE el)) {
    size_t  i;
    ft_array_invariants(arr);
    for (i = 0; i < arr->len; i++) {
        each(arr->ptr[i]);
    }
}

ft_inline void
ft_array_ensure(ft_array_type *arr, size_t sz) {
    size_t newcap;
    size_t newlen;

    ft_array_invariants(arr);
    ft_assert(SIZE_MAX/2 - 1 - arr->len >= sz);

    newlen = arr->len + sz;
    if (arr->cap >= newlen)
        return;

    newcap = arr->cap ? arr->cap : 4;
    while (newcap < newlen)
        newcap *= 2;

    arr->ptr = ft_realloc_arr(arr->ptr, sizeof(FT_SLICE_TYPE), arr->cap, newcap);
    arr->cap = newcap;
}

ft_inline void
ft_array_recapa(ft_array_type *arr, size_t cap) {
    size_t newcap;

    ft_array_invariants(arr);
    ft_assert(cap >= arr->len);
    ft_dbg_assert(SIZE_MAX/2 - 1 >= cap);

    newcap = (arr->cap && arr->cap <= cap) ? arr->cap : 4;
    while (newcap < cap)
        newcap *= 2;

    if (newcap == cap)
        return;

    arr->ptr = ft_realloc_arr(arr->ptr, sizeof(FT_SLICE_TYPE), arr->cap, newcap);
    arr->cap = newcap;
}

ft_inline void
ft_array_resize(ft_array_type *arr, size_t len) {
    ft_array_invariants(arr);

    if (len > arr->cap)
        ft_array_recapa(arr, len);

    if (len < arr->len) {
        memset(&arr->ptr[len], 0, sizeof(FT_SLICE_TYPE) * (arr->len - len));
    } else if (len > arr->len) {
        memset(&arr->ptr[arr->len], 0, sizeof(FT_SLICE_TYPE) * (len - arr->len));
    }

    arr->len = len;

    if (arr->len < arr->cap / 4)
        ft_array_recapa(arr, arr->len);
}

ft_inline void
ft_array_reset_for_reuse(ft_array_type *arr) {
	arr->len = 0;
}

ft_inline ft_array_type
ft_array_alloc(FT_SLICE_TYPE *ptr, size_t len) {
    ft_array_type   arr = {NULL, 0, 0};

    if (len > 0) {
        ft_array_ensure(&arr, len);
        memcpy(arr.ptr, ptr, sizeof(FT_SLICE_TYPE) * len);
        arr.len = len;
    }
    return arr;
}

ft_inline void
ft_array_free(ft_array_type *arr) {
    ft_array_invariants(arr);
    ft_free(arr->ptr);
    arr->ptr = 0;
    arr->len = 0;
    arr->cap = 0;
}

ft_inline FT_SLICE_TYPE
ft_array_del_at(ft_array_type *arr, ssize_t at) {
    FT_SLICE_TYPE el;
    ft_array_invariants(arr);

    at = ft__index_unify(at, arr->len);
    el = arr->ptr[at];
    if (at+1 < arr->len) {
        memmove(&arr->ptr[at], &arr->ptr[at+1], sizeof(FT_SLICE_TYPE)*(arr->len-at-1));
    }
    memset(&arr->ptr[arr->len-1], 0, sizeof(FT_SLICE_TYPE));
    arr->len--;

    if (arr->len < arr->cap / 4)
        ft_array_recapa(arr, arr->len);

    return el;
}

ft_inline void
ft_array_del_slice(ft_array_type *arr, ssize_t start, ssize_t end) {
    ft_array_invariants(arr);

    start = ft__slcindex_unify(start, arr->len);
    end = ft__slcindex_unify(end, arr->len);
    ft_assert(end >= start);
    if (end == start)
        return;

    if (end < arr->len) {
        memmove(&arr->ptr[start], &arr->ptr[end], sizeof(FT_SLICE_TYPE)*(arr->len-end));
    }

    memset(&arr->ptr[arr->len-(end-start)], 0, sizeof(FT_SLICE_TYPE)*(end-start));
    arr->len -= end-start;

    if (arr->len < arr->cap / 4)
        ft_array_recapa(arr, arr->len);
}

ft_inline FT_SLICE_TYPE
ft_array_pop(ft_array_type *arr) {
    FT_SLICE_TYPE el;
    ft_array_invariants(arr);

    el = arr->ptr[arr->len-1];
    memset(&arr->ptr[arr->len-1], 0, sizeof(FT_SLICE_TYPE));
    arr->len--;

    if (arr->len < arr->cap / 4)
        ft_array_recapa(arr, arr->len);

    return el;
}

ft_inline void
ft_array_insert_at(ft_array_type *arr, ssize_t at, FT_SLICE_TYPE el) {
    ft_array_invariants(arr);
    at = ft__slcindex_unify(at, arr->len);
    ft_array_ensure(arr, 1);
    if (at != arr->len)
        memmove(&arr->ptr[at+1], &arr->ptr[at], sizeof(FT_SLICE_TYPE) * (arr->len - at));
    arr->ptr[at] = el;
    arr->len++;
}

ft_inline void
ft_array_push(ft_array_type *arr, FT_SLICE_TYPE el) {
    ft_array_invariants(arr);
    ft_array_ensure(arr, 1);
    arr->ptr[arr->len] = el;
    arr->len++;
}

ft_inline void
ft_array_push2(ft_array_type *arr, FT_SLICE_TYPE el1, FT_SLICE_TYPE el2) {
    ft_array_invariants(arr);
    ft_array_ensure(arr, 2);
    arr->ptr[arr->len+0] = el1;
    arr->ptr[arr->len+1] = el2;
    arr->len+=2;
}

ft_inline void
ft_array_insert_n(ft_array_type *arr, ssize_t at, FT_SLICE_TYPE *el, size_t n) {
    bool alloced = false;
    ft_array_invariants(arr);

    at = ft__slcindex_unify(at, arr->len);

    if (el + n >= arr->ptr && el < arr->ptr + arr->len) {
        /* oops, overlap.
         * Since we could reallocate array, we have to copy to allocated place */
        FT_SLICE_TYPE *cpy;
        cpy = ft_malloc_arr(sizeof(FT_SLICE_TYPE), n);
        memcpy(cpy, el, sizeof(FT_SLICE_TYPE) * n);
        el = cpy;
        alloced = true;
    }

    ft_array_ensure(arr, n);

    if (at != arr->len)
        memmove(&arr->ptr[at+n], &arr->ptr[at],
                sizeof(FT_SLICE_TYPE) * (arr->len - at));
    memmove(&arr->ptr[at], el, sizeof(FT_SLICE_TYPE) * n);
    arr->len += n;

    if (alloced)
        ft_free(el);
}

ft_inline void
ft_array_append(ft_array_type *arr, FT_SLICE_TYPE *el, size_t n) {
    ft_array_invariants(arr);
    ft_array_insert_n(arr, arr->len, el, n);
}

ft_inline void
ft_array_walk_r(ft_array_type *arr,
        FT_WALK_ACT (*walk)(FT_SLICE_TYPE *el, ft_arg_t arg),
        ft_arg_t arg) {
    size_t  i, j = 0;
    FT_WALK_ACT act = FT_WALK_CONT;
    ft_array_invariants(arr);

    for (i = 0; i < arr->len && (act & FT_WALK_BREAK) == 0; i++) {
        act = walk(&arr->ptr[i], arg);
        if ((act & FT_WALK_DEL) == 0) {
            if (i != j)
                arr->ptr[j] = arr->ptr[i];
            j++;
        }
    }
    /* move tail */
    if (i != arr->len) {
        if (i != j) {
            memmove(&arr->ptr[j], &arr->ptr[i], sizeof(FT_SLICE_TYPE)*(arr->len - i));
        }
        j += arr->len - i;
    }

    /* set length */
    if (j != arr->len) {
        memset(&arr->ptr[j], 0, sizeof(FT_SLICE_TYPE)*(arr->len - j));
        arr->len = j;
        if (arr->len < arr->cap / 4)
            ft_array_recapa(arr, arr->len);
    }
}

ft_inline void
ft_array_walk(ft_array_type *arr, FT_WALK_ACT (*walk)(FT_SLICE_TYPE *el)) {
    ft_array_walk_r(arr, (FT_WALK_ACT (*)(FT_SLICE_TYPE*, ft_arg_t))(void*)walk, ft_mka_z());
}

#undef FT_SLICE
#undef FT_SLICE_TYPE

#undef ft_slice_pref
#undef ft_array_pref
#undef ft_slice_type
#undef ft_array_type

#undef ft_slice_make
#undef ft_slice_alloc
#undef ft_slice_at
#undef ft_slice_set
#undef ft_slice_slice
#undef ft_slice_each
#undef ft_slice_each_r

#undef ft_array_alloc
#undef ft_array_at
#undef ft_array_set
#undef ft_array_slice
#undef ft_array_each
#undef ft_array_each_r

#undef ft_array_ensure
#undef ft_array_recapa
#undef ft_array_resize
#undef ft_array_reset_for_reuse
#undef ft_array_free

#undef ft_array_insert_at
#undef ft_array_insert_n
#undef ft_array_push
#undef ft_array_push2
#undef ft_array_append

#undef ft_array_del_at
#undef ft_array_del_slice
#undef ft_array_pop

#undef ft_array_walk
#undef ft_array_walk_r

#undef HUGE_SIZE
#undef ft_slice_invariants
#undef ft_array_invariants

