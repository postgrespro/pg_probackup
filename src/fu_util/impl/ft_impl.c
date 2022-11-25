/* vim: set expandtab autoindent cindent ts=4 sw=4 sts=4 */
#include <ft_util.h>

#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#if !defined(WIN32) || defined(__MINGW64__) || defined(__MINGW32__)
#include <unistd.h>
#include <sys/time.h>
#else
#define WIN32_LEAN_AND_MEAN

#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#undef small
#include <process.h>
#include <signal.h>
#include <direct.h>
#undef near
#endif

#ifdef HAVE_LIBBACKTRACE
#include <backtrace.h>
#if defined(__MINGW32__) || defined(__MINGW64__)
#include <libloaderapi.h>
#endif
#elif HAVE_EXECINFO_H
#include <execinfo.h>
#endif

#include <pthread.h>

#define FT_LOG_MAX_FILES (1<<12)

static void * (*_ft_realloc) (void *, size_t) = realloc;
static void (*_ft_free) (void *) = free;

void ft_set_allocators(
        void *(*_realloc)(void *, size_t),
        void (*_free)(void*)) {
    _ft_realloc = _realloc ? _realloc : realloc;
    _ft_free = _free ? _free : free;
}

void*
ft_calloc(size_t size) {
    void * res = ft_malloc(size);
    ft_memzero(res, size);
    return res;
}

void*
ft_realloc(void *oldptr, size_t size) {
    if (size) {
        void *res = _ft_realloc(oldptr, size);
        ft_assert(res, "ft_realloc failed: oldptr=%p size=%zd", oldptr, size);
        return res;
    }
    if (oldptr)
        _ft_free(oldptr);
    return NULL;
}

void*
ft_realloc_arr(void* ptr, size_t elem_sz, size_t old_elems, size_t new_elems) {
    ptr = ft_realloc(ptr, ft_mul_size(elem_sz, new_elems));
    if (new_elems > old_elems)
        ft_memzero((char*)ptr + elem_sz * old_elems,
                   elem_sz * (new_elems - old_elems));
    return ptr;
}

#ifdef OPTIMIZE_FT_MEMZERO
#define MEMZERO_BLOCK 4096
static const uint8_t zero[4096] = {0};
void
ft_memzero(void *_ptr, size_t sz) {
    uint8_t*  ptr = _ptr;
    uintptr_t ptri = (uintptr_t)ptr;
    uintptr_t diff;

    if (ptri & (MEMZERO_BLOCK-1)) {
        diff = MEMZERO_BLOCK - (ptri & (MEMZERO_BLOCK-1));
        if (diff > sz)
            diff = sz;
        memset(ptr, 0, diff);
        ptr += diff;
        sz -= diff;
    }

    /* Do not dirty page if it clear */
    while (sz >= MEMZERO_BLOCK) {
        if (memcmp(ptr, zero, MEMZERO_BLOCK) != 0) {
            memset(ptr, 0, MEMZERO_BLOCK);
        }
        ptr += MEMZERO_BLOCK;
        sz -= MEMZERO_BLOCK;
    }

    if (sz)
        memset(ptr, 0, sz);
}
#else
void
ft_memzero(void *ptr, size_t sz) {
	memset(ptr, 0, sz);
}
#endif

/* String utils */

size_t
ft_strlcat(char *dest, const char* src, size_t dest_size) {
    char*   dest_null = memchr(dest, 0, dest_size);
    size_t  dest_len = dest_null ? dest_null - dest : dest_size;
    ft_assert(dest_null, "destination has no zero byte");
    if (dest_len < dest_size-1) {
        size_t cpy_len = dest_size - dest_len - 1;
        cpy_len = ft_min(cpy_len, strlen(src));
        memcpy(dest+dest_len, src, cpy_len);
        dest[dest_len + cpy_len] = '\0';
    }
    return dest_len + strlen(src);
}

size_t
ft_strlcpy(char *dest, const char* src, size_t dest_size) {
	size_t cpy_len = dest_size - 1;
	cpy_len = ft_min(cpy_len, strlen(src));
	memcpy(dest, src, cpy_len);
	dest[cpy_len] = '\0';
	return strlen(src);
}

ft_str_t
ft_vasprintf(const char *fmt, va_list args) {
    ft_strbuf_t buf = ft_strbuf_zero();
    bool    err;

    ft_strbuf_vcatf_err(&buf, &err, fmt, args);

    if (err) {
        ft_strbuf_free(&buf);
        return ft_str(NULL, 0);
    }
    return ft_strbuf_steal(&buf);
}

ft_str_t
ft_asprintf(const char *fmt, ...) {
    ft_strbuf_t buf = ft_strbuf_zero();
    bool    err;
    va_list args;

    va_start(args, fmt);
    ft_strbuf_vcatf_err(&buf, &err, fmt, args);
    va_end(args);

    if (err) {
        ft_strbuf_free(&buf);
        return ft_str(NULL, 0);
    }
    return ft_strbuf_steal(&buf);
}

bool
ft__strbuf_ensure(ft_strbuf_t *buf, size_t n) {
    size_t      new_len;
    size_t      new_cap;
    bool        overflowed = false;
    ft_assert(!buf->fixed);
    ft_assert(buf->cap < ft_add_size(buf->len, n));
    /* 4GB string limit */
    ft_assert(buf->len + n <= UINT32_MAX);
    new_len = buf->len + n;
    if (new_len > UINT32_MAX) {
        new_len = UINT32_MAX;
        overflowed = true;
    }
    new_cap = ft_nextpow2(new_len);
    if (buf->alloced)
        buf->ptr = ft_realloc(buf->ptr, new_cap);
    else {
        char*   newbuf = ft_malloc(new_cap);
        memcpy(newbuf, buf->ptr, (size_t)buf->len+1);
        buf->ptr = newbuf;
    }
    buf->cap = new_cap-1;
    buf->alloced = true;
    buf->fixed = overflowed;
    return !overflowed;
}

extern bool
ft_strbuf_vcatf_err(ft_strbuf_t *buf, bool err[1], const char *fmt, va_list args) {
    int     save_errno = errno;
    char    localbuf[256] = "";
    char   *str = NULL;
    size_t  init_len = buf->len;
    ssize_t  len, need_len;
    bool    overflowed = false;
    va_list argcpy;

    if (!ft_strbuf_may(buf))
        return false;

    err[0] = false;

    va_copy(argcpy, args);
    need_len = vsnprintf(localbuf, ft_arrsz(localbuf), fmt, argcpy);
    va_end(argcpy);

    if (need_len < 0) {
        err[0] = true;
        return true;
    }

    if (need_len < ft_arrsz(localbuf)) {
        return ft_strbuf_cat(buf, ft_str(localbuf, need_len));
    }

    for (;;) {
        len = need_len;
        if (!ft_strbuf_ensure(buf, len)) {
            len = buf->cap - buf->len;
            overflowed = true;
        }
        str = buf->ptr + init_len;

        errno = save_errno;
        va_copy(argcpy, args);
        need_len = vsnprintf(str, len+1, fmt, argcpy);
        va_end(argcpy);

        if (need_len < 0) {
            buf->ptr[buf->len] = '0';
            err[0] = true;
            return true;
        }

        if (need_len <= len) {
            buf->len += need_len;
            return ft_strbuf_may(buf);
        }
        if (overflowed) {
            buf->len = buf->cap;
            return false;
        }
    }
}

bool
ft_strbuf_vcatf(ft_strbuf_t *buf, const char *fmt, va_list args) {
    bool    err = false;
    bool    may_continue = ft_strbuf_vcatf_err(buf, &err, fmt, args);
    if (err)
        ft_log(FT_ERROR, "error printing format '%s'", fmt);
    return may_continue;
}

bool
ft_strbuf_catf(ft_strbuf_t *buf, const char *fmt, ...) {
    bool    err = false;
    bool    may_continue;
    va_list args;

    va_start(args, fmt);
    may_continue = ft_strbuf_vcatf_err(buf, &err, fmt, args);
    va_end(args);

    if (err)
        ft_log(FT_ERROR, "error printing format '%s'", fmt);

    return may_continue;
}

/* Time */
double
ft_time(void) {
    struct timeval tv = {0, 0};
    ft_assyscall(gettimeofday(&tv, NULL));
    return (double)tv.tv_sec + (double)tv.tv_usec/1e6;
}

/* Logging */

/*
static _Noreturn void
ft_quick_exit(const char* msg) {
    write(STDERR_FILENO, msg, strlen(msg));
    abort();
}
*/

static const char *ft_log_main_file = __FILE__;
const char*
ft__truncate_log_filename(const char *file) {
    const char *me = ft_log_main_file;
    const char *he = file;
    for (;*he && *me && *he==*me;he++, me++) {
#ifndef WIN32
        if (*he == '/')
            file = he+1;
#else
        if (*he == '/' || *he == '\\')
            file = he+1;
#endif
    }
    return file;
}

static const char*
ft__base_log_filename(const char *file) {
    const char *he = file;
    for (;*he;he++) {
#ifndef WIN32
        if (*he == '/')
            file = he+1;
#else
        if (*he == '/' || *he == '\\')
            file = he+1;
#endif
    }
    return file;
}

#ifdef HAVE_LIBBACKTRACE
static struct backtrace_state * volatile ft_btstate = NULL;
static pthread_once_t ft_btstate_once = PTHREAD_ONCE_INIT;


static void
ft_backtrace_err(void *data, const char *msg, int errnum)
{
	fprintf(stderr, "ft_backtrace_err %s %d\n", msg, errnum);
}

static void
ft_backtrace_init(void) {
    const char *app = NULL;
#if defined(__MINGW32__) || defined(__MINGW64__)
    static char appbuf[2048] = {0};
    /* 2048 should be enough, don't check error */
    GetModuleFileNameA(0, appbuf, sizeof(appbuf)-1);
    app = appbuf;
#endif
    __atomic_store_n(&ft_btstate, backtrace_create_state(app, 1, ft_backtrace_err, NULL),
					 __ATOMIC_RELEASE);
}

static int
ft_backtrace_add(void *data, uintptr_t pc,
              const char* filename, int lineno,
              const char *function) {
    struct ft_strbuf_t *buf = data;
    ssize_t sz;
    if (filename == NULL)
        return 0;
    return !ft_strbuf_catf(buf, "\n\t%s:%-4d\t%s",
                          ft__truncate_log_filename(filename), lineno, function ? function : "(unknown)");
}
#endif

static void ft_gnu_printf(4,0)
ft_default_log(enum FT_LOG_LEVEL level, ft_source_position_t srcpos,
                const char* error, const char *fmt, va_list args) {
#define LOGMSG_SIZE (1<<12)
    char buffer[LOGMSG_SIZE] = {0};
    ft_strbuf_t buf = ft_strbuf_init_fixed(buffer, LOGMSG_SIZE);
    bool   err;
    double now;

    now = ft_time();
    ft_strbuf_catf(&buf, "%.3f %d [%s]", now, getpid(), ft_log_level_str(level));

    if (level <= FT_DEBUG || level >= FT_ERROR) {
        ft_strbuf_catf(&buf, " (%s@%s:%d)", srcpos.func, srcpos.file, srcpos.line);
    }

    ft_strbuf_catc(&buf, " > ");
    ft_strbuf_vcatf_err(&buf, &err, fmt, args);
    if (err) {
        ft_strbuf_catc(&buf, "<<error during log message format>>");
    }

    if (error != NULL) {
        ft_strbuf_catc(&buf, ": ");
        ft_strbuf_catc(&buf, error);
    }

    if (!ft_strbuf_may(&buf))
        goto done;

    if (level == FT_ERROR || level == FT_FATAL) {
#ifdef HAVE_LIBBACKTRACE
        if (__atomic_load_n(&ft_btstate, __ATOMIC_ACQUIRE) == NULL)
            pthread_once(&ft_btstate_once, ft_backtrace_init);
        if (ft_btstate)
            backtrace_full(ft_btstate, 0, ft_backtrace_add, NULL, &buf);
#elif defined(HAVE_EXECINFO_H)
        void *backtr[32] = {0};
        char **syms = NULL;
        int i, n;
        n = backtrace(backtr, 32);
        syms = backtrace_symbols(backtr, n);
        if (syms != NULL) {
            for (i = 1; i < n; i++) {
                ft_strbuf_cat1(&buf, '\n');
                ft_strbuf_catc(&buf, syms[i]);
            }
            free(syms);
        }
#endif
    }

done:
    if (!ft_strbuf_may(&buf)) {
        buf.ptr[buf.len-3] = '.';
        buf.ptr[buf.len-2] = '.';
        buf.ptr[buf.len-1] = '.';
    }

    fprintf(stderr, "%s\n", buffer);
}

static ft_gnu_printf(4,0)
void (*ft_log_hook)(enum FT_LOG_LEVEL, ft_source_position_t, const char*, const char *fmt, va_list args) = ft_default_log;

void
ft__init_log(ft_log_hook_t hook, const char *file) {
    ft_log_hook = hook == NULL ? ft_default_log : hook;
    ft_log_main_file = file == NULL ? __FILE__ : file;
}

void
ft__log(enum FT_LOG_LEVEL level, ft_source_position_t srcpos,
        const char* error, const char *fmt, ...) {
    va_list args;
    srcpos.file = ft__truncate_log_filename(srcpos.file);
    va_start(args, fmt);
    ft_log_hook(level, srcpos, error, fmt, args);
    va_end(args);
}

extern _Noreturn void
ft__log_fatal(ft_source_position_t srcpos, const char* error,
        const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    ft_log_hook(FT_FATAL, srcpos, error, fmt, args);
    va_end(args);
    abort();
}

const char*
ft__strerror(int eno, char *buf, size_t len) {
#ifndef HAVE_STRERROR_R
	char	   *sbuf = strerror(eno);

	if (sbuf == NULL)			/* can this still happen anywhere? */
		return NULL;
	/* To minimize thread-unsafety hazard, copy into caller's buffer */
	ft_strlcpy(buf, sbuf, len);
	return buf;
#elif !_GNU_SOURCE && (_POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600)
    int saveno = errno;
    int e = strerror_r(eno, buf, len);
    if (e != 0) {
        if (e == -1) {
            e = errno;
        }
        if (e == EINVAL) {
            snprintf(buf, len, "Wrong errno %d", eno);
        } else if (e == ERANGE) {
            snprintf(buf, len, "errno = %d has huge message", eno);
        }
    }
    errno = saveno;
    return buf;
#else
    return strerror_r(eno, buf, len);
#endif
}

#ifndef __TINYC__
const char*
ft_strerror(int eno) {
    static __thread char buf[256];
    return ft__strerror(eno, buf, sizeof(buf));
}
#endif

struct ft_log_and_assert_level   ft_log_assert_levels = {
    .log_level = FT_INFO,
#ifndef NDEBUG
    .assert_level = FT_ASSERT_ALL,
#else
    .assert_level = FT_ASSERT_RUNTIME,
#endif
};

typedef struct {
    const char *file;
    uint32_t    next;
    struct ft_log_and_assert_level  local_levels;
} ft_log_file_registration;

#define FT_LOG_FILES_HASH (FT_LOG_MAX_FILES/4)
static ft_log_file_registration ft_log_file_regs[FT_LOG_MAX_FILES] = {{0}};
static uint32_t ft_log_file_reg_hash[FT_LOG_FILES_HASH] = {0};
static uint32_t ft_log_file_n = 0;

extern void
ft__register_source(
        const char *file,
        struct ft_log_and_assert_level **local_levels) {
    ft_log_file_registration *reg;
    uint32_t hash;

    ft_assert(ft_log_file_n < FT_LOG_MAX_FILES);
    ft_dbg_assert(file != NULL);

    reg = &ft_log_file_regs[ft_log_file_n++];

    reg->file = file;
    reg->local_levels = ft_log_assert_levels;

    *local_levels = &reg->local_levels;

    hash = ft_small_cstr_hash(ft__base_log_filename(reg->file));
    reg->next = ft_log_file_reg_hash[hash%FT_LOG_FILES_HASH];
    ft_log_file_reg_hash[hash%FT_LOG_FILES_HASH] = ft_log_file_n;
}

static void
ft__log_level_reset(int what, int level) {
    uint32_t i;

    if (what)
        ft_log_assert_levels.log_level = level;
    else
        ft_log_assert_levels.assert_level = level;

    for (i = 0; i < ft_log_file_n; i++) {
        if (what)
            ft_log_file_regs[i].local_levels.log_level = level;
        else
            ft_log_file_regs[i].local_levels.assert_level = level;
    }
}

static void
ft__log_level_set(const char *file, int what, int level) {
    ft_log_file_registration *reg;
    uint32_t    hash, i;
    bool        found = false; 
    size_t      len = strlen(file);

    ft_dbg_assert(file != NULL);

    if (strcmp(file, "ALL") == 0) {
        ft__log_level_reset(what, level);
        return;
    }

    hash = ft_small_cstr_hash(ft__base_log_filename(file));
    i = ft_log_file_reg_hash[hash%FT_LOG_FILES_HASH];
    while (i) {
        size_t reglen;
        reg = &ft_log_file_regs[i-1];
        ft_dbg_assert(reg->file != NULL);
        reglen = strlen(reg->file);
        if (reglen >= len && strcmp(reg->file + (reglen-len), file) == 0) {
            if (what)
                reg->local_levels.log_level = level;
            else
                reg->local_levels.assert_level = level;
            found = true;
        }
        i = reg->next;
    }
    if (found)
        return;
    /*
     * ooops... not found... pity...
     * ok, lets set global one, but without per-file setting
     */
    if (what)
        ft_log_assert_levels.log_level = level;
    else
        ft_log_assert_levels.assert_level = level;
}

void
ft_log_level_reset(enum FT_LOG_LEVEL level) {
    ft__log_level_reset(1, level);
}

void
ft_assert_level_reset(enum FT_ASSERT_LEVEL level) {
    ft__log_level_reset(0, level);
}

void
ft_log_level_set(const char *file, enum FT_LOG_LEVEL level) {
    ft__log_level_set(file, 1, level);
}

void
ft_assert_level_set(const char *file, enum FT_ASSERT_LEVEL level) {
    ft__log_level_set(file, 0, level);
}

uint32_t
ft_rand(void) {
    static volatile uint32_t rstate = 0xbeaf1234;
    uint32_t rand = __atomic_fetch_add(&rstate, 0x11, __ATOMIC_RELAXED);
    rand = ft_mix32(rand);
    return rand;
}

uint32_t
ft_small_cstr_hash(const char *key) {
    unsigned char  *str = (unsigned char *)key;
    uint32_t h1 = 0x3b00;
    uint32_t h2 = 0;
    for (;str[0]; str++) {
        h1 += str[0];
        h1 *= 9;
        h2 += h1;
        h2 = ft_rol32(h2, 7);
        h2 *= 5;
    }
    h1 ^= h2;
    h1 += ft_rol32(h2, 14);
    h2 ^= h1; h2 += ft_ror32(h1, 6);
    h1 ^= h2; h1 += ft_rol32(h2, 5);
    h2 ^= h1; h2 += ft_ror32(h1, 8);
    return h2;
}

