/* vim: set expandtab autoindent cindent ts=4 sw=4 sts=4 : */
#ifndef FT_IMPL_H
#define FT_IMPL_H

#ifdef __TINYC__

#if defined(__attribute__)
#undef __attribute__
#define __attribute__ __attribute__
#endif

#include <stdatomic.h>
#define __atomic_add_fetch(x, y, z) ft__atomic_add_fetch((x), (y), z, fm_uniq(y))
#define ft__atomic_add_fetch(x, y_, z, y) ({ \
        __typeof(y_) y = y_; \
        __atomic_fetch_add((x), y, z) + y; \
})
#define __atomic_sub_fetch(x, y, z) ft__atomic_sub_fetch((x), (y), z, fm_uniq(y))
#define ft__atomic_sub_fetch(x, y_, z, y) ({ \
        __typeof(y_) y = y_; \
        __atomic_fetch_sub((x), y, z) - y; \
})
#define __atomic_load_n(x, z) __atomic_load((x), z)
#define __atomic_store_n(x, y, z) __atomic_store((x), (y), z)

#endif /* __TINYC__ */

/* Memory */

/* Logging */

static ft_unused inline const char*
ft_log_level_str(enum FT_LOG_LEVEL level) {
    switch (level) {
        case FT_DEBUG:      return "DEBUG";
        case FT_LOG:        return "LOG";
        case FT_INFO:       return "INFO";
        case FT_WARNING:    return "WARNING";
        case FT_ERROR:      return "ERROR";
        case FT_FATAL:      return "FATAL";
        case FT_OFF:        return "OFF";
        case FT_TRACE:      return "TRACE";
        default:            return "UNKNOWN";
    }
}

extern void ft__init_log(ft_log_hook_t hook, const char *file);

struct ft_log_and_assert_level {
    enum FT_LOG_LEVEL       log_level;
    enum FT_ASSERT_LEVEL    assert_level;
};

extern struct ft_log_and_assert_level   ft_log_assert_levels;

/* this variable is duplicated in every source as static variable */
static ft_unused
struct ft_log_and_assert_level *ft_local_lgas_levels = &ft_log_assert_levels;

#define ft_will_log(level) (level >= ft_local_lgas_levels->log_level)
extern void ft__register_source(const char *file,
                                struct ft_log_and_assert_level **local_levels);

#if defined(__GNUC__) || defined(__TINYC__)
#define ft__register_source_impl() \
    static __attribute__((constructor)) void \
    ft__register_source_(void) { \
        ft__register_source(__FILE__, &ft_local_lgas_levels); \
    } \
    fm__dumb_require_semicolon
#else
#define ft_register_source_impl() fm__dumb_require_semicolon
#endif

#define COMPARE_FT_FATAL(x) x
#define ft__log_impl(level, error, fmt_or_msg, ...) \
    fm_if(fm_equal(level, FT_FATAL), \
            ft__log_fatal(ft__srcpos(), error, ft__log_fmt_msg(fmt_or_msg, __VA_ARGS__)), \
            ft__log_common(level, error, fmt_or_msg, __VA_ARGS__))

#define ft__log_common(level, error, fmt_or_msg, ...) do {\
    if (level >= FT_ERROR || ft_unlikely(ft_will_log(level))) \
        ft__log(level, ft__srcpos(), error, ft__log_fmt_msg(fmt_or_msg, __VA_ARGS__)); \
} while(0)

#define ft__log_fmt_msg(fmt, ...) \
    fm_iif(fm_no_va(__VA_ARGS__))("%s", fmt)(fmt, __VA_ARGS__)

extern ft_gnu_printf(4, 5)
void ft__log(enum FT_LOG_LEVEL level, ft_source_position_t srcpos, const char* error, const char *fmt, ...);
extern _Noreturn ft_gnu_printf(3, 4)
void ft__log_fatal(ft_source_position_t srcpos, const char* error, const char *fmt, ...);

ft_inline bool ft__dbg_enabled(void) {
    return ft_unlikely(ft_local_lgas_levels->assert_level >= FT_ASSERT_ALL);
}

#define ft__dbg_assert(x, xs, ...) do { \
    if (ft__dbg_enabled() && ft_unlikely(!(x))) \
        ft__log_fatal(ft__srcpos(), xs, ft__assert_arg(__VA_ARGS__)); \
} while(0)

#define ft__assert(x, xs, ...) do { \
    if (ft_unlikely(!(x))) \
        ft__log_fatal(ft__srcpos(), xs, ft__assert_arg(__VA_ARGS__)); \
} while(0)

#define ft__assert_arg(...) \
    fm_if(fm_no_va(__VA_ARGS__), "Asserion failed", \
            ft__log_fmt_msg(__VA_ARGS__))

#define ft__assyscall(syscall, res, ...)  ({ \
        __typeof(syscall) res = (syscall); \
        ft__assert(res >= 0, ft_strerror(errno), #syscall __VA_ARGS__); \
        res; \
    })

/* Comparison */

#define ft__max(a_, b_, a, b) ({ \
        __typeof(a_) a = (a_); \
        __typeof(b_) b = (b_); \
        a < b ? b : a ; \
        })

#define ft__min(a_, b_, a, b) ({ \
        __typeof(a_) a = (a_); \
        __typeof(b_) b = (b_); \
        a > b ? b : a ; \
        })

#define ft__cmp(a_, b_, a, b) ({ \
        __typeof(a_) a = (a_); \
        __typeof(b_) b = (b_); \
        a < b ? FT_CMP_LT : (a > b ? FT_CMP_GT : FT_CMP_EQ); \
        })

#define ft__swap(a_, b_, ap, bp, t) do { \
    __typeof(a_) ap = a_; \
    __typeof(a_) bp = b_; \
    __typeof(*ap) t = *ap; \
    *ap = *bp; \
    *bp = t; \
} while (0)

#if defined(__has_builtin) || defined(__clang__)
#   if __has_builtin(__builtin_add_overflow) && __has_builtin(__builtin_mul_overflow)
#       define ft__has_builtin_int_overflow
#   endif
#elif __GNUC__ > 4 && !defined(__clang__) && !defined(__LCC__)
#       define ft__has_builtin_int_overflow
#endif

ft_inline size_t ft_add_size(size_t a, size_t b) {
    size_t r;
#ifdef ft__has_builtin_int_overflow
    if (ft_unlikely(__builtin_add_overflow(a, b, &r)))
        ft_assert(r >= a && r >= b);
#else
    r = a + b;
    ft_assert(r >= a && r >= b);
#endif
    return r;
}

ft_inline size_t ft_mul_size(size_t a, size_t b) {
    size_t r;
#ifdef ft__has_builtin_int_overflow
    if (ft_unlikely(__builtin_mul_overflow(a, b, &r)))
        ft_assert(r / a == b);
#else
    r = a * b;
    ft_assert(r / a == b);
#endif
    return r;
}

/* division 64->32 bit */
ft_inline int32_t ft_div_i64u32_to_i32(int64_t a, uint32_t b) {
	int64_t r;
	ft_assert(a >= 0);
	r = a / b;
	ft_assert(r <= INT32_MAX);
	return (int32_t)r;
}

extern ft_gcc_malloc(ft_realloc, 1) void* ft_realloc(void* ptr, size_t new_sz);
extern ft_gcc_malloc(ft_realloc, 1) void* ft_calloc(size_t sz);

// Some Numeric Utils

ft_inline uint32_t
ft_rol32(uint32_t x, unsigned n) {
    return n == 0 ? x : n >= 32 ? 0 : (x << n) | (x >> (32 - n));
}

ft_inline uint32_t
ft_ror32(uint32_t x, unsigned n) {
    return n == 0 ? x : n >= 32 ? 0 : (x << (32 - n)) | (x >> n);
}

ft_inline size_t
ft_nextpow2(size_t sz) {
    sz |= sz >> 1;
    sz |= sz >> 2;
    sz |= sz >> 4;
    sz |= sz >> 8;
    sz |= sz >> 16;
#if !defined(__SIZEOF_SIZE_T__)
    if (sizeof(sz) > 4)
        sz |= sz >> 32;
#elif __SIZEOF_SIZE_T__ > 4
    sz |= sz >> 32;
#endif
    return ft_add_size(sz, 1);
}

ft_inline uint32_t
ft_mix32(uint32_t h)
{
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

ft_inline uint32_t
ft_fast_randmod(uint32_t v, uint32_t mod) {
    return (uint32_t)(((uint64_t)v * mod) >> 32);
}

ft_inline uint32_t ft_randn(uint32_t mod) {
    return ft_fast_randmod(ft_rand(), mod);
}

ft_inline uint32_t ft_rand32(uint32_t* state, uint32_t mod) {
    uint32_t x = *state;
    uint32_t r = ft_rol32(x, 15);
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = x;
    r += x;
    return mod ? ft_fast_randmod(r, mod) : r;
}

/* ft_val_t */
struct ft_arg {
    union {
        void       *p;
        char       *s;
        int64_t     i;
        uint64_t    u;
        double      f;
        bool        b;
#ifdef FOBJ_OBJ_H
        fobj_t      o;
#endif
    } v;
    char t;
};

ft_inline ft_arg_t ft_mka_z(void)       { return (ft_arg_t){.v={.u = 0}, .t='z'};}
ft_inline ft_arg_t ft_mka_p(void*    p) { return (ft_arg_t){.v={.p = p}, .t='p'};}
ft_inline ft_arg_t ft_mka_s(char*    s) { return (ft_arg_t){.v={.s = s}, .t='s'};}
ft_inline ft_arg_t ft_mka_i(int64_t  i) { return (ft_arg_t){.v={.i = i}, .t='i'};}
ft_inline ft_arg_t ft_mka_u(uint64_t u) { return (ft_arg_t){.v={.u = u}, .t='u'};}
ft_inline ft_arg_t ft_mka_f(double   f) { return (ft_arg_t){.v={.f = f}, .t='f'};}
ft_inline ft_arg_t ft_mka_b(bool     b) { return (ft_arg_t){.v={.b = b}, .t='b'};}
#ifdef FOBJ_OBJ_H
ft_inline ft_arg_t ft_mka_o(fobj_t   o) { return (ft_arg_t){.v={.o = o}, .t='o'};}
#endif

ft_inline char     ft_arg_type(ft_arg_t v) { return v.t; }

ft_inline void     ft_arg_z(ft_arg_t v) { ft_dbg_assert(v.t=='z'); }
ft_inline void*    ft_arg_p(ft_arg_t v) { ft_dbg_assert(v.t=='p'); return v.v.p; }
ft_inline char*    ft_arg_s(ft_arg_t v) { ft_dbg_assert(v.t=='s'); return v.v.s; }
ft_inline int64_t  ft_arg_i(ft_arg_t v) { ft_dbg_assert(v.t=='i'); return v.v.i; }
ft_inline uint64_t ft_arg_u(ft_arg_t v) { ft_dbg_assert(v.t=='u'); return v.v.u; }
ft_inline double   ft_arg_f(ft_arg_t v) { ft_dbg_assert(v.t=='f'); return v.v.f; }
ft_inline bool     ft_arg_b(ft_arg_t v) { ft_dbg_assert(v.t=='b'); return v.v.b; }
#ifdef FOBJ_OBJ_H
ft_inline fobj_t   ft_arg_o(ft_arg_t v) { ft_dbg_assert(v.t=='o'); return v.v.o; }
#endif

/* slices and arrays */

ft_inline size_t
ft__index_unify(ssize_t at, size_t len) {
    if (at >= 0) {
        ft_assert(at < len);
        return at;
    } else {
        ft_assert((size_t)(-at) <= len);
        return (size_t)(len - (size_t)(-at));
    }
}

ft_inline size_t
ft__slcindex_unify(ssize_t end, size_t len) {
    if (end >= 0) {
        ft_assert(end <= len);
        return end;
    } else if (end == FT_SLICE_END) {
        return len;
    } else {
        ft_assert((size_t)(-end) <= len);
        return (size_t)(len - (size_t)(-end));
    }
}

// Bytes

ft_inline void
ft_bytes_consume(ft_bytes_t *bytes, size_t cut) {
	ft_dbg_assert(cut <= bytes->len);
	bytes->ptr = bytes->ptr + cut;
	bytes->len -= cut;
}

ft_inline void
ft_bytes_move(ft_bytes_t *dest, ft_bytes_t *src) {
    size_t  len = ft_min(dest->len, src->len);
    memmove(dest->ptr, src->ptr, len);
    ft_bytes_consume(dest, len);
    ft_bytes_consume(src, len);
}

ft_inline ft_bytes_t
ft_bytes_shift_line(ft_bytes_t *bytes)
{
	size_t i;
	char *p = bytes->ptr;

	for (i = 0; i < bytes->len; i++) {
		if (p[i] == '\r' || p[i] == '\n') {
			if (p[i] == '\r' && i+1 < bytes->len && p[i+1] == '\n')
				i++;
			ft_bytes_consume(bytes, i+1);
			return ft_bytes(p, i+1);
		}
	}

	ft_bytes_consume(bytes, bytes->len);
	return ft_bytes(p, i);
}

ft_inline size_t
ft_bytes_find_bytes(ft_bytes_t haystack, ft_bytes_t needle)
{
	// TODO use memmem if present
	size_t i;
	char   first;

	if (needle.len == 0)
		return 0;
	if (needle.len > haystack.len)
		return haystack.len;

	first = needle.ptr[0];
	for (i = 0; i < haystack.len - needle.len; i++)
	{
		if (haystack.ptr[i] != first)
			continue;
		if (memcmp(haystack.ptr + i, needle.ptr, needle.len) == 0)
			return i;
	}

	return haystack.len;
}

ft_inline size_t
ft_bytes_find_cstr(ft_bytes_t haystack, const char* needle)
{
	return ft_bytes_find_bytes(haystack, ft_str2bytes(ft_cstr(needle)));
}

ft_inline bool
ft_bytes_has_cstr(ft_bytes_t haystack, const char* needle)
{
	size_t pos = ft_bytes_find_cstr(haystack, needle);
	return pos != haystack.len;
}

// String utils

ft_inline ft_str_t
ft_bytes2str(ft_bytes_t bytes) {
	ft_dbg_assert(bytes.ptr[bytes.len-1] == '\0');
	return ft_str(bytes.ptr, bytes.len-1);
}

ft_inline char *
ft_cstrdup(const char *str) {
    return (char*)ft_strdupc(str).ptr;
}

ft_inline ft_str_t
ft_strdup(ft_str_t str) {
    char *mem = ft_malloc(str.len + 1);
    if (str.ptr != NULL)
        memcpy(mem, str.ptr, str.len+1);
    else
        mem[0] = '\0';
    str.ptr = mem;
    return str;
}

ft_inline ft_str_t
ft_strdupc(const char *str) {
    return ft_strdup(ft_cstr(str));
}

ft_inline void
ft_str_free(ft_str_t *str) {
    ft_free(str->ptr);
    str->ptr = NULL;
    str->len = 0;
}

ft_inline bool
ft_streq(ft_str_t str, ft_str_t oth) {
    return str.len == oth.len && strncmp(str.ptr, oth.ptr, str.len) == 0;
}

ft_inline FT_CMP_RES
ft_strcmp(ft_str_t str, ft_str_t oth) {
    size_t m = ft_min(str.len, oth.len);
    return strncmp(str.ptr, oth.ptr, m) ?: ft_cmp(str.len, oth.len);
}

ft_inline bool
ft_streqc(ft_str_t str, const char* oth) {
    return ft_streq(str, ft_cstr(oth));
}

ft_inline FT_CMP_RES
ft_strcmpc(ft_str_t str, const char* oth) {
    return ft_strcmp(str, ft_cstr(oth));
}

ft_inline void
ft_str_consume(ft_str_t *str, size_t cut) {
	ft_dbg_assert(cut <= str->len);
	str->ptr = str->ptr + cut;
	str->len -= cut;
}

ft_inline ft_bytes_t
ft_str_shift_line(ft_str_t *str)
{
	size_t i;
	char *p = str->ptr;

	for (i = 0; i < str->len; i++) {
		if (p[i] == '\r' || p[i] == '\n') {
			if (p[i] == '\r' && p[i+1] == '\n')
				i++;
			ft_str_consume(str, i+1);
			return ft_bytes(p, i+1);
		}
	}

	ft_str_consume(str, str->len);
	return ft_bytes(p, i);
}

ft_inline ft_strbuf_t
ft_strbuf_zero(void) {
    return (ft_strbuf_t){.ptr = "", .len = 0, .cap = 0};
}

ft_inline ft_strbuf_t
ft_strbuf_init_stack(char *buf, size_t capa) {
    if (capa == 0)
        return (ft_strbuf_t){.ptr = "", .len = 0, .cap = 0};
    ft_assert(capa <= UINT32_MAX);
    buf[0] = '\0';
    return (ft_strbuf_t){.ptr = buf, .len = 0, .cap = capa-1};
}

ft_inline ft_strbuf_t
ft_strbuf_continue(char *buf, size_t capa) {
    if (capa == 0)
        return (ft_strbuf_t){.ptr = "", .len = 0, .cap = 0};
    ft_assert(capa <= UINT32_MAX);
    buf[0] = '\0';
    return (ft_strbuf_t){.ptr = buf, .len = 0, .cap = capa-1};
}

ft_inline ft_strbuf_t
ft_strbuf_init_fixed(char *buf, size_t capa) {
    ft_assert(capa > 0 && capa <= UINT32_MAX);
    buf[0] = '\0';
    return (ft_strbuf_t){.ptr = buf, .len = 0, .cap = capa-1, .fixed = true};
}

ft_inline ft_strbuf_t
ft_strbuf_init_str(ft_str_t str) {
    ft_assert(str.len <= UINT32_MAX);
    return (ft_strbuf_t){.ptr = str.ptr, .len = str.len, .cap = str.len};
}

/*
 * always allocates space for 1 zero ending byte.
 * Returns false, if buffer reaches 4GB limit.
 */
extern bool     ft__strbuf_ensure(ft_strbuf_t *buf, size_t n);

ft_inline bool
ft_strbuf_may(ft_strbuf_t *buf) {
    return !buf->fixed || buf->len < buf->cap;
}

ft_inline bool
ft_strbuf_ensure(ft_strbuf_t *buf, size_t n) {
    if ((size_t)buf->cap < ft_add_size(buf->len, n)) {
        if (buf->fixed)
            return false;
        return ft__strbuf_ensure(buf, n);
    }
    return true;
}

ft_inline bool
ft_strbuf_cat(ft_strbuf_t *buf, ft_str_t s) {
	/* we could actually reuse ft_strbuf_catbytes */
	return ft_strbuf_catbytes(buf, ft_bytes(s.ptr, s.len));
}

ft_inline bool
ft_strbuf_catbytes(ft_strbuf_t *buf, ft_bytes_t s) {
	if (!ft_strbuf_may(buf))
		return false;
	if (s.len == 0)
		return true;
	if (!ft_strbuf_ensure(buf, s.len)) {
		s.len = buf->cap - buf->len;
		ft_assert(s.len > 0);
	}
	memmove(buf->ptr + buf->len, s.ptr, s.len);
	buf->len += s.len;
	buf->ptr[buf->len] = '\0';
	return ft_strbuf_may(buf);
}

ft_inline bool
ft_strbuf_cat1(ft_strbuf_t *buf, char c) {
    if (!ft_strbuf_may(buf))
        return false;
    if (ft_strbuf_ensure(buf, 1)) {
        buf->ptr[buf->len+0] = c;
        buf->ptr[buf->len+1] = '\0';
        buf->len++;
    }
    return ft_strbuf_may(buf);
}

ft_inline bool
ft_strbuf_cat2(ft_strbuf_t *buf, char c1, char c2) {
    if (!ft_strbuf_may(buf))
        return false;
    if (ft_strbuf_ensure(buf, 2)) {
        buf->ptr[buf->len+0] = c1;
        buf->ptr[buf->len+1] = c1;
        buf->ptr[buf->len+2] = '\0';
        buf->len+=2;
    } else {
        buf->ptr[buf->len+0] = c1;
        buf->ptr[buf->len+1] = '\0';
        buf->len++;
    }
    return ft_strbuf_may(buf);
}

ft_inline bool
ft_strbuf_catc(ft_strbuf_t *buf, const char *s) {
    return ft_strbuf_cat(buf, ft_cstr(s));
}

ft_inline void
ft_strbuf_free(ft_strbuf_t *buf) {
    if (buf->alloced) {
        ft_free(buf->ptr);
    }
    *buf = (ft_strbuf_t){NULL};
}

ft_inline ft_str_t
ft_strbuf_ref(ft_strbuf_t *buf) {
    return ft_str(buf->ptr, buf->len);
}

ft_inline ft_str_t
ft_strbuf_steal(ft_strbuf_t *buf) {
    ft_str_t res = ft_str(buf->ptr, buf->len);
    if (!buf->alloced) {
        res = ft_strdup(res);
    }
    *buf = (ft_strbuf_t){NULL};
    return res;
}

#endif
