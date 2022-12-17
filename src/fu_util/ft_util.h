/* vim: set expandtab autoindent cindent ts=4 sw=4 sts=4 */
#ifndef FU_UTIL_H
#define FU_UTIL_H 1

#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdarg.h>
#include <sys/types.h>
/* trick to find ssize_t even on windows and strict ansi mode */
#if defined(_MSC_VER)
#include <BaseTsd.h>
typedef SSIZE_T ssize_t;
#define SSIZE_MAX ((ssize_t)((SIZE_MAX) >> 1))

#if !defined(WIN32) && defined(_WIN32)
#define WIN32 _WIN32
#endif

#endif
#include <memory.h>
#include <limits.h>
#include <fm_util.h>

#ifdef __GNUC__
#define ft_gcc_const __attribute__((const))
#define ft_gcc_pure __attribute__((pure))
#if __GNUC__ > 10 && !defined(__clang__)
#define ft_gcc_malloc(free, idx) __attribute__((malloc, malloc(free, idx)))
#else
#define ft_gcc_malloc(free, idx) __attribute__((malloc))
#endif
#define ft_unused __attribute__((unused))
#define ft_gnu_printf(fmt, arg) __attribute__((format(gnu_printf,fmt,arg)))
#define ft_likely(x)    __builtin_expect(!!(x), 1)
#define ft_unlikely(x)  __builtin_expect(!!(x), 0)
#define ft_always_inline __attribute__((always_inline))
#define ft_cleanup(func) __attribute__((cleanup(func)))
#else
#define ft_gcc_const
#define ft_gcc_pure
#define ft_gcc_malloc(free, idx)
#define ft_unused
#define ft_gnu_printf(fmt, arg)
#define ft_likely(x)    (x)
#define ft_unlikely(x)  (x)
#define ft_always_inline
#endif
#define ft_static static ft_unused
#define ft_inline static ft_unused inline

#if defined(__GNUC__) && !defined(__clang__)
#define ft_optimize3    __attribute__((optimize(3)))
#else
#define ft_optimize3
#endif

#if __STDC_VERSION__ >= 201112L
#elif defined(__GNUC__) && !defined(_Noreturn)
#define _Noreturn __attribute__((__noreturn__))
#elif !defined(_Noreturn)
#define _Noreturn
#endif

/* Logging and asserts */

#if defined(__GNUC__) && !defined(__clang__)
#define ft_FUNC __PRETTY_FUNCTION__
#else
#define ft_FUNC __func__
#endif

typedef struct ft_source_position {
    const char *file;
    int line;
    const char *func;
} ft_source_position_t;

#define ft__srcpos() ((ft_source_position_t){.file=__FILE__,.line=__LINE__,.func=ft_FUNC})

enum FT_LOG_LEVEL {
    FT_UNINITIALIZED = -100,
    FT_DEBUG    = -2,
    FT_LOG      = -1,
    FT_INFO     = 0,
    FT_WARNING  = 1,
    FT_ERROR    = 2,
    FT_OFF      = 3,
    FT_FATAL    = 98,
    FT_TRACE    = 100 /* for active debugging only */
};

enum FT_ASSERT_LEVEL { FT_ASSERT_RUNTIME = 0, FT_ASSERT_ALL };

ft_inline const char* ft_log_level_str(enum FT_LOG_LEVEL level);

/*
 * Hook type to plugin external logging.
 * Default loggin writes to stderr only.
 */
typedef void ft_gnu_printf(4, 0) (*ft_log_hook_t)(enum FT_LOG_LEVEL,
                                                  ft_source_position_t srcpos,
                                                  const char* error,
                                                  const char *fmt,
                                                  va_list args);
/*
 * Initialize logging in main executable file.
 * Pass custom hook or NULL.
 * In MinGW if built with libbacktrace, pass executable path (argv[0]).
 */
#define ft_init_log(hook) ft__init_log(hook, __FILE__)

/* Reset log level for all files */
extern void ft_log_level_reset(enum FT_LOG_LEVEL level);
extern void ft_assert_level_reset(enum FT_ASSERT_LEVEL level);
/* Adjust log level for concrete file or all files */
extern void ft_log_level_set(const char *file, enum FT_LOG_LEVEL level);
extern void ft_assert_level_set(const char *file, enum FT_ASSERT_LEVEL level);

/* truncates filename to source root */
const char* ft__truncate_log_filename(const char *file);

/* register source for fine tuned logging */
#define ft_register_source()   ft__register_source_impl()

/* log simple message */
#define ft_log(level, fmt_or_msg, ...) \
    ft__log_impl(level, NULL, fmt_or_msg, __VA_ARGS__)
/* log message with error. Error will be appended as ": %s". */
#define ft_logerr(level, error, fmt_or_msg, ...) \
    ft__log_impl(level, error, fmt_or_msg, __VA_ARGS__)

/*
 * Assertions uses standard logging for output.
 * Assertions are runtime enabled:
 * - ft_assert is enabled always.
 * - ft_dbg_assert is disabled be default, but will be enabled if `ft_assert_level` is set positive.
 */

#define ft_dbg_enabled()        ft__dbg_enabled()
#define ft_dbg_assert(x, ...)   ft__dbg_assert(x, #x, __VA_ARGS__)
#define ft_assert(x, ...)       ft__assert(x, #x, ##__VA_ARGS__)
#define ft_assyscall(syscall, ...)  ft__assyscall(syscall, fm_uniq(res), __VA_ARGS__)

/* threadsafe strerror */
extern const char* ft__strerror(int eno, char *buf, size_t len);
#ifndef __TINYC__
extern const char* ft_strerror(int eno);
#else
#define ft_strerror(eno) ft__strerror(eno, (char[256]){0}, 256)
#endif

// Memory

// Standartize realloc(p, 0)
// Realloc implementations differ in handling newsz == 0 case:
// some returns NULL, some returns unique allocation.
// This version always returns NULL.
extern void* ft_realloc(void* ptr, size_t new_sz);
extern void* ft_calloc(size_t sz);
extern void* ft_realloc_arr(void* ptr, size_t elem_sz, size_t old_elems, size_t new_elems);

extern void* ft_malloc(size_t sz);
extern void* ft_malloc_arr(size_t sz, size_t cnt);
extern void  ft_free(void* ptr);
extern void* ft_calloc_arr(size_t sz, size_t cnt);


extern void ft_set_allocators(void *(*_realloc)(void *, size_t),
                              void (*_free)(void*));

/* overflow checking size addition and multiplication */
ft_inline size_t ft_add_size(size_t a, size_t b);
ft_inline size_t ft_mul_size(size_t a, size_t b);

/* division 64->32 bit */
ft_inline int32_t ft_div_i64u32_to_i32(int64_t a, uint32_t b);

#define ft_new(type)        ft_calloc(sizeof(type))
#define ft_newar(type, cnt) ft_calloc(ft_mul_size(sizeof(type), (cnt)))

// Function to clear freshly allocated memory
extern void  ft_memzero(void* ptr, size_t sz);

// Comparison

/* ft_max - macro-safe calculation of maximum */
#define ft_max(a_, b_) ft__max((a_), (b_), fm_uniq(a), fm_uniq(b))
/* ft_min - macro-safe calculation of minimum */
#define ft_min(a_, b_) ft__min((a_), (b_), fm_uniq(a), fm_uniq(b))

/* Well, it is a bit fake enum. */
typedef enum FT_CMP_RES {
    FT_CMP_LT  = -1,
    FT_CMP_EQ  =  0,
    FT_CMP_GT  =  1,
    FT_CMP_NE  =  2,
} FT_CMP_RES;
/* ft_cmp - macro-safe comparison */
#define ft_cmp(a_, b_) ft__cmp((a_), (b_), fm_uniq(a), fm_uniq(b))
/* ft_swap - macro-safe swap of variables */
#define ft_swap(a_, b_) ft__swap((a_), (b_), fm_uniq(ap), fm_uniq(bp), fm_uniq(t))

/* ft_arrsz - geterminze size of static array */
#define ft_arrsz(ar) (sizeof(ar)/sizeof(ar[0]))

/* used in ft_*_foreach iterations to close implicit scope */
#define ft_end_foreach } while(0)

// Some Numeric Utils

ft_inline uint32_t ft_rol32(uint32_t x, unsigned n);
ft_inline uint32_t ft_ror32(uint32_t x, unsigned n);
ft_inline size_t   ft_nextpow2(size_t sz);

/*
 * Simple inline murmur hash implementation hashing a 32 bit integer, for
 * performance.
 */
ft_inline uint32_t ft_mix32(uint32_t data);


/* Dumb quality random */
extern uint32_t ft_rand(void);
/* Dumb quality random 0<=r<mod */
ft_inline uint32_t ft_randn(uint32_t mod);
/* Xorshift32 random */
ft_inline uint32_t ft_rand32(uint32_t *state, uint32_t mod);

/* hash for small c strings */
extern uint32_t ft_small_cstr_hash(const char *key);

/* Time */
extern double ft_time(void);

/* ARGUMENT */

/*
 * Type for *_r callback functions argument.
 * Could be one of type:
 *  z - value-less
 *  p - `void*` pointer
 *  s - `char*` pointer
 *  i - `int64_t`
 *  u - `uint64_t`
 *  f - `float` (really, double)
 *  b - `bool`
 *  o - `object` (see fo_obj.h)
 */
typedef struct ft_arg ft_arg_t;

/* make value */
ft_inline ft_arg_t ft_mka_z(void);
ft_inline ft_arg_t ft_mka_p(void*    p);
ft_inline ft_arg_t ft_mka_s(char*    s);
ft_inline ft_arg_t ft_mka_i(int64_t  i);
ft_inline ft_arg_t ft_mka_u(uint64_t u);
ft_inline ft_arg_t ft_mka_f(double   f);
ft_inline ft_arg_t ft_mka_b(bool     b);
#ifdef FOBJ_OBJ_H
ft_inline ft_arg_t ft_mka_o(fobj_t   o);
#endif

/* type of value */
ft_inline char     ft_arg_type(ft_arg_t v);

/* get value */
ft_inline void     ft_arg_z(ft_arg_t v);
ft_inline void*    ft_arg_p(ft_arg_t v);
ft_inline char*    ft_arg_s(ft_arg_t v);
ft_inline int64_t  ft_arg_i(ft_arg_t v);
ft_inline uint64_t ft_arg_u(ft_arg_t v);
ft_inline double   ft_arg_f(ft_arg_t v);
ft_inline bool     ft_arg_b(ft_arg_t v);
#ifdef FOBJ_OBJ_H
ft_inline fobj_t   ft_arg_o(ft_arg_t v);
#endif

/* SLICES */

/* Value to indicate end of slice in _slice operations */
static const ssize_t FT_SLICE_END = (-SSIZE_MAX-1);

/* Action in walk callback */
typedef enum FT_WALK_ACT {
    FT_WALK_CONT        = 0,
    FT_WALK_BREAK       = 1,
    FT_WALK_DEL         = 2,
    FT_WALK_DEL_BREAK   = FT_WALK_BREAK | FT_WALK_DEL,
} FT_WALK_ACT;

/* Variable initialization fields */
#define ft_slc_init() { .ptr = NULL, .len = 0 }
#define ft_arr_init() { .ptr = NULL, .len = 0, .cap = 0 }

/*
 * Transform slice or array into pointer and length pair.
 * It evaluates argument twice, so use it carefully.
 */
#define ft_2ptrlen(slice_or_array) (slice_or_array).ptr, (slice_or_array).len

/* Result if binary search */
typedef struct ft_bsearch_result {
    size_t  ix; /* index of first greater or equal element */
    bool    eq; /* is element equal or not */
} ft_bsres_t;

// Bytes

typedef struct ft_bytes_t {
	char*	ptr;
	size_t	len;
} ft_bytes_t;

ft_inline ft_bytes_t ft_bytes(void* ptr, size_t len) {
	return (ft_bytes_t){.ptr = (char*)ptr, .len = len};
}

ft_inline ft_bytes_t ft_bytesc(const char* ptr) {
	return (ft_bytes_t){.ptr = (char*)ptr, .len = strlen(ptr)};
}

#define FT_BYTES_FOR(var) ft_bytes(&(var), sizeof(var))

ft_inline ft_bytes_t ft_bytes_alloc(size_t sz) {
	return ft_bytes(ft_malloc(sz), sz);
}

ft_inline ft_bytes_t ft_bytes_dup(ft_bytes_t bytes) {
	ft_bytes_t r = ft_bytes_alloc(bytes.len);
	memmove(r.ptr, bytes.ptr, bytes.len);
	return r;
}

ft_inline void ft_bytes_free(ft_bytes_t* bytes) {
	ft_free(bytes->ptr);
	*bytes = ft_bytes(NULL, 0);
}

ft_inline void ft_bytes_consume(ft_bytes_t *bytes, size_t cut);
ft_inline size_t ft_bytes_move(ft_bytes_t *dest, ft_bytes_t *src);
ft_inline ft_bytes_t ft_bytes_split(ft_bytes_t *bytes, size_t n);

extern ft_bytes_t   ft_bytes_shift_line(ft_bytes_t *bytes);
ft_inline bool      ft_bytes_shift_to(ft_bytes_t *bytes, ft_bytes_t to);
ft_inline void      ft_bytes_shift_must(ft_bytes_t *bytes, ft_bytes_t to);

extern size_t       ft_bytes_find_bytes(ft_bytes_t haystack, ft_bytes_t needle);
ft_inline size_t    ft_bytes_find_cstr(ft_bytes_t haystack, const char *needle);
ft_inline bool      ft_bytes_has_cstr(ft_bytes_t haystack, const char *needle);

ft_inline bool    ft_bytes_starts_with(ft_bytes_t haystack, ft_bytes_t needle);
ft_inline bool    ft_bytes_starts_withc(ft_bytes_t haystack, const char* needle);

ft_inline size_t  ft_bytes_spn(ft_bytes_t bytes, ft_bytes_t chars);
ft_inline size_t  ft_bytes_notspn(ft_bytes_t bytes, ft_bytes_t chars);
ft_inline size_t  ft_bytes_spnc(ft_bytes_t bytes, const char* chars);
ft_inline size_t  ft_bytes_notspnc(ft_bytes_t bytes, const char* chars);

// String utils
extern size_t ft_strlcpy(char *dest, const char* src, size_t dest_size);
/*
 * Concat strings regarding destination buffer size.
 * Note: if dest already full and doesn't contain \0n character, then fatal log is issued.
 */
extern size_t ft_strlcat(char *dest, const char* src, size_t dest_size);

/* dup string using ft_malloc */
ft_inline char * ft_cstrdup(const char *str);
ft_inline char * ft_cstrdupn(const char *str, size_t n);

/****************
 * String
 */

typedef struct ft_str_t {
    char*   ptr;
    size_t	len;
} ft_str_t;

ft_inline ft_str_t  ft_str(const char* ptr, size_t len) {
    return (ft_str_t){.ptr = (char*)ptr, .len = len};
}

ft_inline ft_str_t  ft_cstr(const char* ptr) {
    return (ft_str_t){.ptr = (char*)ptr, .len = ptr ? strlen(ptr) : 0};
}

ft_inline ft_bytes_t ft_str2bytes(ft_str_t str) {
    return ft_bytes(str.ptr, str.len);
}

ft_inline ft_bytes_t ft_str2bytes_withzb(ft_str_t str) {
	return ft_bytes(str.ptr, str.len+1);
}

ft_inline ft_str_t  ft_strdup(ft_str_t str);
ft_inline ft_str_t  ft_strdupc(const char* str);
ft_inline ft_str_t  ft_strdup_bytes(ft_bytes_t bytes);
/* use only if string was allocated */
ft_inline void      ft_str_free(ft_str_t *str);

/* print string into ft_malloc-ed buffer */
extern ft_str_t     ft_asprintf(const char *fmt, ...) ft_gnu_printf(1,2);
extern ft_str_t     ft_vasprintf(const char *fmt, va_list args) ft_gnu_printf(1,0);

ft_inline bool          ft_streq  (ft_str_t str, ft_str_t oth);
ft_inline FT_CMP_RES    ft_strcmp (ft_str_t str, ft_str_t oth);
ft_inline bool          ft_streqc (ft_str_t str, const char* oth);
ft_inline FT_CMP_RES    ft_strcmpc(ft_str_t str, const char* oth);

ft_inline void			ft_str_consume(ft_str_t *str, size_t cut);

/* shift zero-terminated string. Will assert if no zero-byte found and it is not last */
extern ft_str_t     ft_bytes_shift_zt(ft_bytes_t *bytes);

/*
 * String buffer.
 * It could be growable or fixed.
 * Note: it is limited by 4GB-1 bytes.
 */
typedef struct ft_strbuf_t ft_strbuf_t;
struct ft_strbuf_t {
    char*       ptr;
    /* len doesn't encount last zero byte */
    uint32_t    len;
    /* cap is 1 byte less than real cap for zero bytes */
    uint32_t    cap;
    /* could buffer grow?
     * It could be set on initialization, of if buffer reaches 4GB limit */
    bool        fixed;
    bool		overflowed;
    /* does ptr points to malloced place? */
    /* if so, then ft_strbuf_finish would not strdup */
    bool        alloced;
};

/* empty growable buffer */
ft_inline ft_strbuf_t ft_strbuf_zero(void);
/*
 * Give buffer some non-freeable place, so it could use it until grows over.
 * It will not prevent buffer from growing. In this case, buffer will be allocated.
 * ft_strbuf_finish will duplicate string if it wasn't allocated.
 * `capa` will be decreased by 1 to ensure there's room for zero-termination.
 */
ft_inline ft_strbuf_t ft_strbuf_init_stack(char* buf, size_t capa);
/*
 * Give buffer some non-freeable place.
 * Buffer will not grow more than that, and will remember if was overflowed.
 * `capa` will be decreased by 1 to ensure there's room for zero-termination.
 */
ft_inline ft_strbuf_t ft_strbuf_init_fixed(char* buf, size_t capa);
/*
 * Give buffer some non-freeable place for possible concatenation.
 * `len` and `cap` are set to `str.len`, therefore it will reallocate on first addition.
 */
ft_inline ft_strbuf_t ft_strbuf_init_str(ft_str_t str);

/*
 * Ensure space for future addition.
 * Returns false if buffer is fixed and there's no enough space.
 */
ft_inline bool      ft_strbuf_ensure(ft_strbuf_t *buf, size_t n);

/* All functions below returns false if fixed buffer was overflowed */
ft_inline bool      ft_strbuf_may   (ft_strbuf_t *buf);
ft_inline bool      ft_strbuf_cat   (ft_strbuf_t *buf, ft_str_t s);
/* cat string together with zero-terminated byte */
ft_inline bool      ft_strbuf_cat_zt(ft_strbuf_t *buf, ft_str_t s);
ft_inline bool      ft_strbuf_catbytes(ft_strbuf_t *buf, ft_bytes_t b);
ft_inline bool      ft_strbuf_cat1  (ft_strbuf_t *buf, char c);
ft_inline bool      ft_strbuf_cat2  (ft_strbuf_t *buf, char c1, char c2);
ft_inline bool      ft_strbuf_catc  (ft_strbuf_t *buf, const char *s);
ft_inline bool      ft_strbuf_catc_zt(ft_strbuf_t *buf, const char *s);
ft_gnu_printf(2, 3)
extern bool         ft_strbuf_catf  (ft_strbuf_t *buf, const char *fmt, ...);
ft_gnu_printf(2, 0)
extern bool         ft_strbuf_vcatf (ft_strbuf_t *buf, const char *fmt, va_list args);
/*
 * err is filled with true, if vsnprintf returns error.
 * Use it if format string comes from user.
 */
ft_gnu_printf(3, 0)
extern bool         ft_strbuf_vcatf_err (ft_strbuf_t *buf, bool err[1],
                                         const char *fmt, va_list args);
/*
 * Returns string which points into the buffer.
 * Buffer still owns content.
 * Useful if `buf->alloced = false` and you will duplicate string in your own way.
 */
ft_inline ft_str_t  ft_strbuf_ref(ft_strbuf_t *buf);

/*
 * Reset buffer's len to 0 without deallocation.
 */
ft_inline void		ft_strbuf_reset_for_reuse(ft_strbuf_t *buf);

/*
 * Free buffer's buffer, if it was allocated
 */
ft_inline void      ft_strbuf_free(ft_strbuf_t *buf);

/*
 * Always return allocated string.
 * If buffer wasn't empty, returns it's ptr intact.
 * If buffer was empty, allocate 1 bytes string with zero end
 * Meaningless with fixed non-allocated buffer if you don't want to allocate.
 *
 * Buffer fields are cleared, therefore it will be unusable after.
 * You will have to initialize it again.
 */
ft_inline ft_str_t  ft_strbuf_steal(ft_strbuf_t *buf);

#include "./impl/ft_impl.h"

/* Include some examples for search and sort usages */
//#include "./ft_ss_examples.h"
//#include "./ft_ar_examples.h"

#endif
