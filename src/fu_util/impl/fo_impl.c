/* vim: set expandtab autoindent cindent ts=4 sw=4 sts=4 */
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>

#include <fo_obj.h>

#include <pthread.h>

/*
 * We limits total number of methods, klasses and method implementations.
 * Restricted number allows to use uint16_t for id and doesn't bother with
 * smart structures for hashes.
 * If you need more, you have to change the way they are stored.
 */
#define FOBJ_OBJ_MAX_KLASSES (1<<10)
#define FOBJ_OBJ_MAX_METHODS (1<<10)
#define FOBJ_OBJ_MAX_METHOD_IMPLS (1<<15)

enum { FOBJ_DISPOSING = 1, FOBJ_DISPOSED = 2 };

typedef enum {
    FOBJ_RT_NOT_INITIALIZED,
    FOBJ_RT_INITIALIZED,
    FOBJ_RT_FROZEN
} FOBJ_GLOBAL_STATE;

typedef struct fobj_header {
#ifndef NDEBUG
#define FOBJ_HEADER_MAGIC UINT64_C(0x1234567890abcdef)
    uint64_t magic;
#endif
    volatile uint32_t rc;
    volatile uint16_t flags;
    fobj_klass_handle_t klass;
} fobj_header_t;

#define METHOD_PARTITIONS (16)

typedef struct fobj_klass_registration {
    const char *name;
    uint32_t    hash;
    uint32_t    hash_next;

    ssize_t     size;
    fobj_klass_handle_t parent;

    uint32_t    nmethods;

    /* common methods */
    fobj__nm_impl_t(fobjDispose)      dispose;

    volatile uint16_t method_lists[METHOD_PARTITIONS];
} fobj_klass_registration_t;

typedef struct fobj_method_registration {
    const char *name;
    uint32_t    hash;
    uint32_t    hash_next;

    uint32_t    nklasses;
    volatile uint32_t    first;
} fobj_method_registration_t;

typedef struct fobj_method_impl {
    uint16_t    method;
    uint16_t    next_for_klass;
    uint16_t    klass;
    uint16_t    next_for_method;
    void*       impl;
} fobj_method_impl_t;


static fobj_klass_registration_t  fobj_klasses[1<<10] = {{0}};
static fobj_method_registration_t fobj_methods[1<<10] = {{0}};
#define FOBJ_OBJ_HASH_SIZE (FOBJ_OBJ_MAX_METHODS/4)
static volatile uint16_t fobj_klasses_hash[FOBJ_OBJ_HASH_SIZE] = {0};
static volatile uint16_t fobj_methods_hash[FOBJ_OBJ_HASH_SIZE] = {0};
static fobj_method_impl_t fobj_method_impl[FOBJ_OBJ_MAX_METHOD_IMPLS] = {{0}};
static volatile uint32_t fobj_klasses_n = 0;
static volatile uint32_t fobj_methods_n = 0;
static volatile uint32_t fobj_impls_n = 0;

static fobj_t fobj_autorelease(fobj_t obj, fobj_autorelease_pool *pool);
static void   fobj_release(fobj_t self);
static fobj_autorelease_pool** fobj_AR_current_ptr(void);

static pthread_mutex_t fobj_runtime_mutex = PTHREAD_MUTEX_INITIALIZER;
static volatile uint32_t fobj_global_state = FOBJ_RT_NOT_INITIALIZED;

#define pth_assert(...) do { \
    int rc = __VA_ARGS__; \
    ft_assert(!rc, "fobj_runtime_mutex: %s", ft_strerror(rc)); \
} while(0)

#define atload(v) __atomic_load_n((v), __ATOMIC_ACQUIRE)

bool
fobj_method_init_impl(volatile fobj_method_handle_t *meth, const char *name) {
    uint32_t hash, mh;
    fobj_method_registration_t *reg;

    ft_dbg_assert(meth);

    pth_assert(pthread_mutex_lock(&fobj_runtime_mutex));
    if ((mh = *meth) != 0) {
        reg = &fobj_methods[mh];
        pth_assert(pthread_mutex_unlock(&fobj_runtime_mutex));
        ft_assert(mh <= atload(&fobj_methods_n));
        ft_assert(strcmp(reg->name, name) == 0);
        return true;
    }


    hash = ft_small_cstr_hash(name);
    mh = fobj_methods_hash[hash % FOBJ_OBJ_HASH_SIZE];
    for (; mh != 0; mh = reg->hash_next) {
        reg = &fobj_methods[mh];
        if (reg->hash == hash && strcmp(reg->name, name) == 0) {
            __atomic_store_n(meth, mh, __ATOMIC_RELEASE);
            pth_assert(pthread_mutex_unlock(&fobj_runtime_mutex));
            return true;
        }
    }

    ft_assert(fobj_global_state == FOBJ_RT_INITIALIZED);

    mh = fobj_methods_n + 1;
    ft_dbg_assert(mh > 0);
    ft_assert(*meth < FOBJ_OBJ_MAX_METHODS, "Too many methods defined");
    reg = &fobj_methods[mh];
    reg->name = name;
    reg->hash = hash;
    reg->hash_next = fobj_methods_hash[hash % FOBJ_OBJ_HASH_SIZE];
    fobj_methods_hash[hash % FOBJ_OBJ_HASH_SIZE] = mh;

    __atomic_store_n(&fobj_methods_n, mh, __ATOMIC_RELEASE);
    __atomic_store_n(meth, mh, __ATOMIC_RELEASE);

    pth_assert(pthread_mutex_unlock(&fobj_runtime_mutex));

    return false;
}

static inline void*
fobj_search_impl(fobj_method_handle_t meth, fobj_klass_handle_t klass) {
    uint32_t i;

    i = atload(&fobj_klasses[klass].method_lists[meth%METHOD_PARTITIONS]);
    while (i != 0) {
        if (fobj_method_impl[i].method == meth)
            return fobj_method_impl[i].impl;
        i = fobj_method_impl[i].next_for_klass;
    }

    return NULL;
}

void*
fobj_klass_method_search(fobj_klass_handle_t klass, fobj_method_handle_t meth) {
    ft_assert(fobj_global_state != FOBJ_RT_NOT_INITIALIZED);
    ft_dbg_assert(meth > 0 && meth <= atload(&fobj_methods_n));
    ft_dbg_assert(meth != fobj__nm_mhandle(fobjDispose)());
    ft_dbg_assert(klass > 0 && klass <= atload(&fobj_klasses_n));

    do {
        void *impl = fobj_search_impl(meth, klass);
        if (impl)
            return impl;
        klass = fobj_klasses[klass].parent;
    } while (klass != 0);
    return NULL;
}


fobj__method_callback_t
fobj_method_search(const fobj_t self, fobj_method_handle_t meth, fobj_klass_handle_t for_child, bool validate) {
    fobj_header_t              *h;
    fobj_klass_handle_t         klass;
    fobj_klass_handle_t         for_klass;
    fobj__method_callback_t     cb = {self, NULL};

    if (ft_unlikely(ft_dbg_enabled())) {
        ft_assert(fobj_global_state != FOBJ_RT_NOT_INITIALIZED);
        ft_assert(meth > 0 && meth <= atload(&fobj_methods_n));
        ft_assert(meth != fobj__nm_mhandle(fobjDispose)());
    }

    if (self == NULL) {
        if (validate)
            ft_assert(self != NULL, "Call '%s' on NULL object", fobj_methods[meth].name);
        return cb;
    }

    h = ((fobj_header_t*)self - 1);
    assert(h->magic == FOBJ_HEADER_MAGIC);
    klass = h->klass;
    if (ft_unlikely(ft_dbg_enabled())) {
        ft_assert(klass > 0 && klass <= atload(&fobj_klasses_n));
        ft_assert((h->flags & FOBJ_DISPOSED) == 0, "Call '%s' on disposed object '%s'",
                  fobj_methods[meth].name, fobj_klasses[klass].name);
    }

    if (for_child != 0) {
        if (ft_unlikely(ft_dbg_enabled())) {
            while (klass && klass != for_child) {
                klass = fobj_klasses[klass].parent;
            }
            ft_assert(klass == for_child);
        } else {
            klass = for_child;
        }
        klass = fobj_klasses[klass].parent;
    }

    for_klass = klass;

    do {
        cb.impl = fobj_search_impl(meth, klass);
        if (cb.impl != NULL)
            return cb;

        klass = fobj_klasses[klass].parent;
    } while (klass);
    if (validate)
        ft_assert(cb.impl != NULL, "Klass '%s' has no method '%s'",
                  fobj_klasses[for_klass].name,
                  fobj_methods[meth].name);
    cb.self = NULL;
    return cb;
}

bool
fobj_method_implements(const fobj_t self, fobj_method_handle_t meth) {
    fobj_header_t              *h;
    fobj_klass_handle_t         klass;

    if (self == NULL)
        return false;

    ft_assert(fobj_global_state != FOBJ_RT_NOT_INITIALIZED);
    if (ft_dbg_enabled()) {
        ft_assert(meth > 0 && meth <= atload(&fobj_methods_n));
        ft_assert(meth != fobj__nm_mhandle(fobjDispose)());
    }

    h = ((fobj_header_t*)self - 1);
    assert(h->magic == FOBJ_HEADER_MAGIC);
    klass = h->klass;
    ft_dbg_assert(klass > 0 && klass <= atload(&fobj_klasses_n));

    do {
        if (fobj_search_impl(meth, klass) != NULL)
            return true;

        klass = fobj_klasses[klass].parent;
    } while (klass);
    return false;
}

_Noreturn 
void fobj__validate_arg(const char* file, int line, const char *arg) {
	ft_log(FT_FATAL, "%s:%d: missing argument %s", file, line, arg);
}

const char *
fobj_klass_name(fobj_klass_handle_t klass) {
    fobj_klass_registration_t *reg;

    ft_assert(fobj_global_state != FOBJ_RT_NOT_INITIALIZED);
    ft_dbg_assert(klass && klass <= atload(&fobj_klasses_n));

    reg = &fobj_klasses[klass];

    return reg->name;
}

fobj_klass_handle_t
fobj_real_klass_of(fobj_t self) {
    fobj_header_t *h;

    ft_assert(fobj_global_state != FOBJ_RT_NOT_INITIALIZED);
    ft_assert(self != NULL);

    h = ((fobj_header_t*)self - 1);
    assert(h->magic == FOBJ_HEADER_MAGIC);
    return h->klass;
}

static void fobj_method_register_priv(fobj_klass_handle_t klass,
                                      fobj_method_handle_t meth,
                                      void* impl);

bool
fobj_klass_init_impl(volatile fobj_klass_handle_t *klass,
                     ssize_t size,
                     fobj_klass_handle_t parent,
                     fobj__method_impl_box_t *methods,
                     const char *name) {
    uint32_t hash, kl;
    fobj_klass_registration_t *reg;

    ft_assert(fobj_global_state == FOBJ_RT_INITIALIZED);
    ft_dbg_assert(klass);

    pth_assert(pthread_mutex_lock(&fobj_runtime_mutex));

    if ((kl = *klass) != 0) {
        reg = &fobj_klasses[kl];
        pth_assert(pthread_mutex_unlock(&fobj_runtime_mutex));
        ft_assert(kl <= atload(&fobj_klasses_n));
        ft_assert(strcmp(reg->name, name) == 0);
        ft_assert(reg->size ==  size);
        ft_assert(reg->parent == parent);
        return true;
    }

    hash = ft_small_cstr_hash(name);
    kl = fobj_klasses_hash[hash % FOBJ_OBJ_HASH_SIZE];
    for (; kl != 0; kl = reg->hash_next) {
        reg = &fobj_klasses[kl];
        if (reg->hash == hash && strcmp(reg->name, name) == 0) {
            __atomic_store_n(klass, kl, __ATOMIC_RELEASE);
            pth_assert(pthread_mutex_unlock(&fobj_runtime_mutex));
            ft_assert(reg->size == size);
            ft_assert(reg->parent == parent);
            return true;
        }
    }

    kl = fobj_klasses_n + 1;
    ft_dbg_assert(kl > 0);
    ft_assert(*klass < FOBJ_OBJ_MAX_KLASSES, "Too many klasses defined");
    reg = &fobj_klasses[kl];
    reg->size = size;
    reg->name = name;
    reg->parent = parent;
    reg->hash = hash;
    reg->hash_next = fobj_klasses_hash[hash % FOBJ_OBJ_HASH_SIZE];
    fobj_klasses_hash[hash % FOBJ_OBJ_HASH_SIZE] = kl;

    __atomic_store_n(&fobj_klasses_n, kl, __ATOMIC_RELEASE);
    /* declare methods before store klass */
    while (methods->meth != 0) {
        fobj_method_register_priv(kl, methods->meth, methods->impl);
        methods++;
    }

    __atomic_store_n(klass, kl, __ATOMIC_RELEASE);

    pth_assert(pthread_mutex_unlock(&fobj_runtime_mutex));

    return false;
}

static void
fobj_method_register_priv(fobj_klass_handle_t klass, fobj_method_handle_t meth, void* impl) {
    fobj_method_registration_t *mreg;
    fobj_klass_registration_t *kreg;
    void    *existed;
    uint32_t nom;

    mreg = &fobj_methods[meth];
    kreg = &fobj_klasses[klass];

    existed = fobj_search_impl(meth, klass);
    ft_dbg_assert(existed == NULL || existed == impl,
                "Method %s.%s is redeclared with different implementation",
                kreg->name, mreg->name);

    if (existed == impl) {
        return;
    }

    nom = fobj_impls_n + 1;
    ft_assert(nom < FOBJ_OBJ_MAX_METHOD_IMPLS);
    fobj_method_impl[nom].method = meth;
    fobj_method_impl[nom].klass = klass;
    fobj_method_impl[nom].next_for_method = mreg->first;
    fobj_method_impl[nom].next_for_klass = kreg->method_lists[meth%METHOD_PARTITIONS];
    fobj_method_impl[nom].impl = impl;
    __atomic_store_n(&mreg->first, nom, __ATOMIC_RELEASE);
    __atomic_store_n(&kreg->method_lists[meth%METHOD_PARTITIONS], nom,
                    __ATOMIC_RELEASE);

    if (meth == fobj__nm_mhandle(fobjDispose)())
        kreg->dispose = (fobj__nm_impl_t(fobjDispose)) impl;

    __atomic_store_n(&fobj_impls_n, nom, __ATOMIC_RELEASE);
}

void
fobj_method_register_impl(fobj_klass_handle_t klass, fobj_method_handle_t meth, void* impl) {
    ft_assert(fobj_global_state == FOBJ_RT_INITIALIZED);
    ft_dbg_assert(meth > 0 && meth <= atload(&fobj_methods_n));
    ft_dbg_assert(klass > 0 && klass <= atload(&fobj_klasses_n));

    pth_assert(pthread_mutex_lock(&fobj_runtime_mutex));

    fobj_method_register_priv(klass, meth, impl);

    pth_assert(pthread_mutex_unlock(&fobj_runtime_mutex));
}

void*
fobj__allocate(fobj_klass_handle_t klass, void *init, ssize_t size) {
    fobj_klass_registration_t *kreg;
    fobj_header_t  *hdr;
    fobj_t          self;
    ssize_t         copy_size;

    ft_assert(fobj_global_state != FOBJ_RT_NOT_INITIALIZED);
    ft_dbg_assert(klass > 0 && klass <= atload(&fobj_klasses_n));

    kreg = &fobj_klasses[klass];
    copy_size = kreg->size >= 0 ? kreg->size : -1-kreg->size;
    if (size < 0) {
        size = copy_size;
    } else {
        ft_assert(kreg->size < 0);
        size += copy_size;
    }
    hdr = ft_calloc(sizeof(fobj_header_t) + size);
#ifndef NDEBUG
    hdr->magic = FOBJ_HEADER_MAGIC;
#endif
    hdr->klass = klass;
    hdr->rc = 1;
    self = (fobj_t)(hdr + 1);
    if (init != NULL)
        memcpy(self, init, copy_size);
    fobj_autorelease(self, *fobj_AR_current_ptr());
    return self;
}

fobj_t
fobj_ref(fobj_t self) {
    fobj_header_t *h;
    if (self == NULL)
        return NULL;
    h = ((fobj_header_t*)self - 1);
    assert(h->magic == FOBJ_HEADER_MAGIC);
    ft_assert(h->klass > 0 && h->klass <= atload(&fobj_klasses_n));
    __atomic_fetch_add(&h->rc, 1, __ATOMIC_ACQ_REL);
    return self;
}

void
fobj_set(fobj_t *ptr, fobj_t val) {
    fobj_t oldval = *ptr;
    *ptr = val ? fobj_ref(val) : NULL;
    if (oldval) fobj_release(oldval);
}

fobj_t
fobj_swap(fobj_t *ptr, fobj_t val) {
    fobj_t oldval = *ptr;
    *ptr = val ? fobj_ref(val) : NULL;
    return oldval ? fobj_autorelease(oldval, *fobj_AR_current_ptr()) : NULL;
}

fobj_t
fobj_unref(fobj_t val) {
    return fobj_autorelease(val, *fobj_AR_current_ptr());
}

static void
fobj__dispose_req(fobj_t self, fobj_klass_registration_t *kreg) {
    if (kreg->dispose)
        kreg->dispose(self);
    if (kreg->parent) {
        fobj_klass_registration_t *preg;

        preg = &fobj_klasses[kreg->parent];
        fobj__dispose_req(self, preg);
    }
}

static void
fobj__do_dispose(fobj_t self, fobj_header_t *h, fobj_klass_registration_t *kreg) {
    uint32_t old = __atomic_fetch_or(&h->flags, FOBJ_DISPOSING, __ATOMIC_ACQ_REL);
    if (old & FOBJ_DISPOSING)
        return;
    fobj__dispose_req(self, kreg);
    __atomic_fetch_or(&h->flags, FOBJ_DISPOSED, __ATOMIC_ACQ_REL);

    if (atload(&h->rc) == 0)
    {
        *h = (fobj_header_t){0};
        ft_free(h);
    }
}

static void
fobj_release(fobj_t self) {
    fobj_header_t *h;
    fobj_klass_handle_t klass;
    fobj_klass_registration_t *kreg;

    ft_assert(fobj_global_state != FOBJ_RT_NOT_INITIALIZED);

    if (self == NULL)
        return;

    h = ((fobj_header_t*)self - 1);
    assert(h->magic == FOBJ_HEADER_MAGIC);
    klass = h->klass;
    ft_dbg_assert(klass > 0 && klass <= atload(&fobj_klasses_n));
    kreg = &fobj_klasses[klass];


    if (__atomic_sub_fetch(&h->rc, 1, __ATOMIC_ACQ_REL) != 0)
        return;
    if ((atload(&h->flags) & FOBJ_DISPOSING) != 0)
        return;
    fobj__do_dispose(self, h, kreg);
}

#if 0
void
fobj_dispose(fobj_t self) {
    fobj_header_t *h;
    fobj_klass_handle_t klass;
    fobj_klass_registration_t *kreg;

    ft_assert(fobj_global_state != FOBJ_RT_NOT_INITIALIZED);

    if (self == NULL)
        return;

    h = ((fobj_header_t*)self - 1);
    assert(h->magic == FOBJ_HEADER_MAGIC);
    klass = h->klass;
    ft_dbg_assert(klass > 0 && klass <= atload(&fobj_klasses_n));
    kreg = &fobj_klasses[klass];

    fobj__do_dispose(self, h, kreg);
}

bool
fobj_disposing(fobj_t self) {
    fobj_header_t *h;

    ft_assert(fobj_global_state != FOBJ_RT_NOT_INITIALIZED);
    ft_assert(self != NULL);

    h = ((fobj_header_t*)self - 1);
    assert(h->magic == FOBJ_HEADER_MAGIC);
    return (atload(&h->flags) & FOBJ_DISPOSING) != 0;
}

bool
fobj_disposed(fobj_t self) {
    fobj_header_t *h;

    ft_assert(fobj_global_state != FOBJ_RT_NOT_INITIALIZED);
    ft_assert(self != NULL);

    h = ((fobj_header_t*)self - 1);
    assert(h->magic == FOBJ_HEADER_MAGIC);
    return (atload(&h->flags) & FOBJ_DISPOSED) != 0;
}

#endif

static fobj_klass_handle_t
fobjBase_fobjKlass(fobj_t self) {
    return fobj_real_klass_of(self);
}

static fobjStr*
fobjBase_fobjRepr(VSelf) {
    Self(fobjBase);
    fobj_klass_handle_t klass = fobjKlass(self);
    return fobj_sprintf("%s@%p", fobj_klass_name(klass), self);
}

err_i
fobj_err_combine(err_i fst, err_i scnd) {
    fobjErr*    first = (fobjErr*)fst.self;
    fobjErr*    second = (fobjErr*)scnd.self;
    fobjErr   **tail;
    if (first == NULL)
        return scnd;
    if (second == NULL)
        return fst;
    ft_assert(fobj_real_klass_of(first) == fobjErr__kh());
    ft_assert(fobj_real_klass_of(second) == fobjErr__kh());
    if (first->sibling != NULL) {
        tail = &second->sibling;
        while (*tail != NULL) tail = &(*tail)->sibling;
        /* ownership is also transferred */
        *tail = first->sibling;
    }
    first->sibling = $ref(second);
    return fst;
}

static fobjStr*
fobj_reservestr(size_t size) {
    fobjStr *str;
#if __SIZEOF_POINTER__ < 8
    ft_assert(size < (1<<30)-2);
#else
    ft_assert(size < UINT32_MAX-2);
#endif
    if (size < FOBJ_STR_SMALL_SIZE) {
        if (size < FOBJ_STR_FREE_SPACE)
            str = fobj_alloc(fobjStr);
        else {
            size_t diff = size + 1 - FOBJ_STR_FREE_SPACE;
            str = fobj_alloc_sized(fobjStr, diff);
        }
        str->small.type = FOBJ_STR_SMALL;
        str->small.len = size;
        str->small.buf[size] = '\0';
    } else {
        str = fobj_alloc_sized(fobjStr, size + 1);
        str->ptr.type = FOBJ_STR_UNOWNED; // abuse it because we don't need separate deallocation
        str->ptr.len = size;
        str->ptr.ptr = (char*)(str+1);
        str->ptr.ptr[size] = '\0';
    }
    return str;
}

fobjStr*
fobj_newstr(ft_str_t s, enum FOBJ_STR_ALLOC ownership) {
    fobjStr *str;
#if __SIZEOF_POINTER__ < 8
    ft_assert(s.len < (1<<30)-2);
#else
    ft_assert(s.len < UINT32_MAX-2);
#endif
    if (s.len >= FOBJ_STR_FREE_SPACE &&
            (ownership == FOBJ_STR_GIFTED || ownership == FOBJ_STR_CONST)) {
        str = fobj_alloc(fobjStr);
        str->ptr.type = ownership == FOBJ_STR_GIFTED ? FOBJ_STR_PTR : FOBJ_STR_UNOWNED;
        str->ptr.len = s.len;
        str->ptr.ptr = s.ptr;
        return str;
    }
    str = fobj_reservestr(s.len);
    memcpy(fobj_getstr(str).ptr, s.ptr, s.len);
    if (ownership == FOBJ_STR_GIFTED)
        ft_str_free(&s);
    return str;
}

ft_inline   fobjStr*    fobj_str_const(const char* s);

static void
fobjStr_fobjDispose(VSelf) {
    Self(fobjStr);
    if (self->type == FOBJ_STR_PTR) {
        ft_free(self->ptr.ptr);
    }
}

fobjStr*
fobj_strcat(fobjStr *self, ft_str_t s) {
    fobjStr *newstr;
    ft_str_t news;
    ft_str_t selfs = fobj_getstr(self);
    size_t alloc_len = selfs.len + s.len + 1;
    ft_assert(alloc_len < UINT32_MAX-2);

    if (s.len == 0)
        return self;

    newstr = fobj_reservestr(alloc_len-1);
    news = fobj_getstr(newstr);
    memcpy(news.ptr, selfs.ptr, selfs.len);
    memcpy(news.ptr + selfs.len, s.ptr, s.len);
    return newstr;
}

fobjStr*
fobj_strcat2(fobjStr *self, ft_str_t s1, ft_str_t s2) {
    fobjStr *newstr;
    ft_str_t news;
    ft_str_t selfs = fobj_getstr(self);
    size_t alloc_len = selfs.len + s1.len + s2.len + 1;
    ft_assert(alloc_len < UINT32_MAX-2);

    if (s1.len + s2.len == 0)
        return self;

    newstr = fobj_reservestr(alloc_len-1);
    news = fobj_getstr(newstr);
    memcpy(news.ptr, selfs.ptr, selfs.len);
    memcpy(news.ptr + selfs.len, s1.ptr, s1.len);
    memcpy(news.ptr + selfs.len + s1.len, s2.ptr, s2.len);
    return newstr;
}

fobjStr*
fobj_sprintf(const char *fmt, ...) {
    char        buffer[256] = {0};
    ft_strbuf_t buf = ft_strbuf_init_stack(buffer, 256);
    va_list     args;

    va_start(args, fmt);
    ft_strbuf_vcatf(&buf, fmt, args);
    va_end(args);

    return fobj_strbuf_steal(&buf);
}

fobjStr*
fobj_strcatf(fobjStr *ostr, const char *fmt, ...) {
    ft_strbuf_t buf = ft_strbuf_init_str(fobj_getstr(ostr));
    bool    err;
    va_list args;

    va_start(args, fmt);
    ft_strbuf_vcatf_err(&buf, &err, fmt, args);
    va_end(args);

    if (err) {
        ft_log(FT_ERROR, "error printing format '%s'", fmt);
        return NULL;
    }

    /* empty print? */
    if (ft_strbuf_ref(&buf).ptr == fobj_getstr(ostr).ptr) {
        return ostr;
    }
    return fobj_strbuf_steal(&buf);
}

fobjStr*
fobj_tostr(fobj_t obj, const char *fmt) {
    char    buffer[32];
    ft_strbuf_t   buf = ft_strbuf_init_stack(buffer, 32);

    if (obj == NULL) {
        return fobj_str("<null>");
    }

    if (fobj_real_klass_of(obj) == fobjStr__kh() && (fmt == NULL || fmt[0] == '\0')) {
        return obj;
    }

    if (!$ifdef(, fobjFormat, obj, &buf, fmt)) {
        /* fallback to Repr */
        return $(fobjRepr, obj);
    }
    return fobj_strbuf_steal(&buf);
}

static void
fobj_format_string(ft_strbuf_t *buf, ft_str_t str, const char *fmt) {
    int     i;
    char    c;

    if (fmt == NULL || fmt[0] == '\0') {
        ft_strbuf_cat(buf, str);
        return;
    } else if (strcmp(fmt, "q") != 0) {
        char    realfmt[32] = "%";

        ft_assert(ft_strlcat(realfmt, fmt, 32) < 32);
        ft_strbuf_catf(buf, realfmt, str.ptr);

        return;
    }

    /* Ok, we're asked for quoted representation */
    if (str.ptr == NULL) {
        ft_strbuf_catc(buf, "NULL");
    }

    ft_strbuf_cat1(buf, '"');
    for (i = 0; i < str.len; i++) {
        c = str.ptr[i];
        switch (c) {
            case '\"': ft_strbuf_catc(buf, "\\\""); break;
            case '\t': ft_strbuf_catc(buf, "\\t"); break;
            case '\n': ft_strbuf_catc(buf, "\\n"); break;
            case '\r': ft_strbuf_catc(buf, "\\r"); break;
            case '\a': ft_strbuf_catc(buf, "\\a"); break;
            case '\b': ft_strbuf_catc(buf, "\\b"); break;
            case '\f': ft_strbuf_catc(buf, "\\f"); break;
            case '\v': ft_strbuf_catc(buf, "\\v"); break;
            case '\\': ft_strbuf_catc(buf, "\\\\"); break;
            default:
                if (c < 0x20) {
                    ft_strbuf_catc(buf, "\\x");
                    ft_strbuf_cat2(buf, '0'+(c>>4), ((c&0xf)<=9?'0':'a')+(c&0xf));
                } else {
                    ft_strbuf_cat1(buf, c);
                }
        }
    }
    ft_strbuf_cat1(buf, '"');
}

static fobjStr*
fobjStr_fobjRepr(VSelf) {
    Self(fobjStr);
    char        buffer[32] = {0};
    ft_strbuf_t buf = ft_strbuf_init_stack(buffer, 32);

    ft_strbuf_catc(&buf, "$S(");
    fobj_format_string(&buf, fobj_getstr(self), "q");
    ft_strbuf_cat1(&buf, ')');

    return fobj_strbuf_steal(&buf);
}

static void
fobjStr_fobjFormat(VSelf, ft_strbuf_t *out, const char *fmt) {
    Self(fobjStr);
    fobj_format_string(out, fobj_getstr(self), fmt);
}

static fobjStr*
fobjInt_fobjRepr(VSelf) {
    Self(fobjInt);
    return fobj_sprintf("$I(%"PRIi64")", self->i);
}

static void
fobj_format_int(ft_strbuf_t *buf, uint64_t i, bool _signed, const char *fmt) {
    char    tfmt[32] = "%";
    char    base;
    size_t  fmtlen;


    if (fmt == NULL || fmt[0] == 0) {
        if (_signed) {
            ft_strbuf_catf(buf, "%"PRIi64, (int64_t)i);
        } else {
            ft_strbuf_catf(buf, "%"PRIu64, (uint64_t)i);
        }
        return;
    }

    /* need to clean length specifiers ('l', 'll', 'z') */
    fmtlen = ft_strlcat(tfmt, fmt, 32);
    ft_assert(fmtlen<28);
    base = tfmt[fmtlen-1];
    ft_assert(base=='x' || base=='X' || base=='o' || base=='u' ||
              base=='d' || base=='i');
    do fmtlen--;
    while (tfmt[fmtlen-1] == 'l' || tfmt[fmtlen-1] == 'z');
    tfmt[fmtlen] = '\0';

    /* now add real suitable format */
    switch (base) {
        case 'x': ft_strlcat(tfmt, PRIx64, sizeof(tfmt)); break;
        case 'X': ft_strlcat(tfmt, PRIX64, sizeof(tfmt)); break;
        case 'o': ft_strlcat(tfmt, PRIo64, sizeof(tfmt)); break;
        case 'u': ft_strlcat(tfmt, PRIu64, sizeof(tfmt)); break;
        case 'd': ft_strlcat(tfmt, PRId64, sizeof(tfmt)); break;
        default:
        case 'i': ft_strlcat(tfmt, PRIi64, sizeof(tfmt)); break;
    }

    switch (base) {
        case 'd': case 'i':
            ft_strbuf_catf(buf, tfmt, (int64_t)i);
            break;
        default:
            ft_strbuf_catf(buf, tfmt, (uint64_t)i);
            break;
    }
}

static void
fobjInt_fobjFormat(VSelf, ft_strbuf_t *buf, const char *fmt) {
    Self(fobjInt);
    fobj_format_int(buf, self->i, true, fmt);
}

static fobjStr*
fobjUInt_fobjRepr(VSelf) {
    Self(fobjUInt);
    return fobj_sprintf("$U(%"PRIu64")", self->u);
}

static void
fobjUInt_fobjFormat(VSelf, ft_strbuf_t *buf, const char *fmt) {
    Self(fobjUInt);
    fobj_format_int(buf, self->u, false, fmt);
}

static fobjStr*
fobjFloat_fobjRepr(VSelf) {
    Self(fobjFloat);
    return fobj_sprintf("$F(%f)", self->f);
}

static void
fobj_format_float(ft_strbuf_t *buf, double f, const char *fmt) {
    char    tfmt[32] = "%";

    if (fmt == NULL || fmt[0] == 0) {
        ft_strbuf_catf(buf, "%f", f);
        return;
    }
    ft_strlcat(tfmt, fmt, 32);
    ft_strbuf_catf(buf, tfmt, f);
}

static void
fobjFloat_fobjFormat(VSelf, ft_strbuf_t *buf, const char *fmt) {
    Self(fobjFloat);
    fobj_format_float(buf, self->f, fmt);
}

static fobjBool*    fobjTrue = NULL;
static fobjBool*    fobjFalse = NULL;
static fobjStr*     trueRepr = NULL;
static fobjStr*     falseRepr = NULL;

fobjBool*
fobj_bool(bool b) {
    return b ? fobjTrue : fobjFalse;
}

static fobjStr*
fobjBool_fobjRepr(VSelf) {
    Self(fobjBool);
    return self->b ? trueRepr : falseRepr;
}

static void
fobj_format_bool(ft_strbuf_t *buf, bool b, const char *fmt) {
    char    tfmt[32] = "%";
    size_t  fmtlen;
    const char *repr = NULL;

    if (fmt == NULL || fmt[0] == 0) {
        if (b)
            ft_strbuf_catc(buf, "true");
        else
            ft_strbuf_catc(buf, "false");
        return;
    }
    fmtlen = ft_strlcat(tfmt, fmt, 32);
    switch (tfmt[fmtlen-1]) {
        case 'B': repr = b ? "TRUE" : "FALSE"; break;
        case 'b': repr = b ? "true" : "false"; break;
        case 'P': repr = b ? "True" : "False"; break;
        case 'Y': repr = b ? "Yes" : "No"; break;
        case 'y': repr = b ? "yes" : "no"; break;
    }
    if (repr != NULL) {
        tfmt[fmtlen-1] = 's';
        ft_strbuf_catf(buf, tfmt, repr);
    } else {
        ft_strbuf_catf(buf, tfmt, b);
    }
}

static void
fobjBool_fobjFormat(VSelf, ft_strbuf_t *buf, const char *fmt) {
    Self(fobjBool);
    fobj_format_bool(buf, self->b, fmt);
}

static void
fobj_format_arg(ft_strbuf_t *out, ft_arg_t arg, const char *fmt) {
    switch (ft_arg_type(arg)) {
        case 'i':
            fobj_format_int(out, (uint64_t)arg.v.i, true, fmt);
            break;
        case 'u':
            fobj_format_int(out, arg.v.i, false, fmt);
            break;
        case 'f':
            fobj_format_float(out, arg.v.f, fmt);
            break;
        case 's':
            fobj_format_string(out, ft_cstr(arg.v.s), fmt);
            break;
        case 'b':
            fobj_format_bool(out, arg.v.b, fmt);
            break;
        case 'o':
            if (arg.v.o == NULL) {
                ft_strbuf_catc(out, "(null)");
            } else if (!$ifdef(, fobjFormat, arg.v.o, out, fmt)) {
                fobjStr* repr = $(fobjRepr, arg.v.o);
                ft_strbuf_cat(out, fobj_getstr(repr));
            }
            break;
        default:
            ft_assert(false, "Could not format arg of type '%c'", ft_arg_type(arg));
    }
}

static void
fobj_repr_arg(ft_strbuf_t *out, ft_arg_t arg) {
    fobjStr*    repr;
    switch (ft_arg_type(arg)) {
        case 'i':
            fobj_format_int(out, (uint64_t)arg.v.i, true, "i");
            break;
        case 'u':
            fobj_format_int(out, arg.v.u, false, NULL);
            break;
        case 'f':
            fobj_format_float(out, arg.v.f, NULL);
            break;
        case 's':
            fobj_format_string(out, ft_cstr(arg.v.s), "q");
            break;
        case 'b':
            fobj_format_bool(out, arg.v.b, NULL);
            break;
        case 'o':
            if (arg.v.o == NULL) {
                ft_strbuf_catc(out, "NULL");
            } else {
                repr = $(fobjRepr, arg.v.o);
                ft_strbuf_cat(out, fobj_getstr(repr));
            }
            break;
        default:
            ft_assert(false, "Could not represent arg of type '%c'", ft_arg_type(arg));
    }
}

static const char*
fobj__format_errmsg(const char* msg, fobj_err_kv_t *kvs) {
    char            buf[128];
    ft_strbuf_t     out = ft_strbuf_init_stack(buf, 128);
    bool            found;
    const char*     cur;
    char*           closebrace;
    char*           formatdelim;
    size_t          identlen;
    size_t          formatlen;
    char            ident[32];
    char            format[32];
    fobj_err_kv_t*  kv;

    if (strchr(msg, '{') == NULL || strchr(msg, '}') == NULL)
        return ft_cstrdup(msg);

    for (cur = msg; *cur; cur++) {
        if (*cur != '{') {
            ft_strbuf_cat1(&out, *cur);
            continue;
        }
        if (cur[1] == '{') {
            ft_strbuf_cat1(&out, '{');
            cur++;
            continue;
        }
        cur++;
        closebrace = strchr(cur, '}');
        ft_assert(closebrace, "error format string braces unbalanced");
        formatdelim = memchr(cur, ':', closebrace - cur);
        identlen = (formatdelim ?: closebrace) - cur;
        ft_assert(identlen <= 31,
                  "ident is too long in message \"%s\"", msg);
        ft_assert(formatdelim == NULL || closebrace - formatdelim <= 31,
                  "format is too long in message \"%s\"", msg);
        memcpy(ident, cur, identlen);
        ident[identlen] = 0;
        formatlen = formatdelim ? closebrace - (formatdelim+1) : 0;
        if (formatlen > 0) {
            memcpy(format, formatdelim + 1, formatlen);
        }
        format[formatlen] = 0;
        kv = kvs;
        found = false;
        for (;kv->key != NULL; kv++) {
            if (strcmp(kv->key, ident) == 0) {
                found = true;
                fobj_format_arg(&out, kv->val, format);
                break;
            }
        }
        ft_dbg_assert(found, "ident '%s' is not found (message \"%s\")", ident, msg);
        cur = closebrace;
    }

    return ft_strbuf_steal(&out).ptr;
}

extern err_i
fobj__make_err(const char *type,
               ft_source_position_t src,
               const char *msg,
               fobj_err_kv_t *kvs,
               size_t kvn) {
    fobjErr*        err;
    fobj_err_kv_t*  kv;
    fobj_err_kv_t*  cpy;
    ft_strbuf_t     nmsg;

    err = fobj_alloc_sized(fobjErr,
                           ft_mul_size(sizeof(*kvs), kvn+1),
                           .type = type ?: "RT",
                           .src = src);
    err->src.file = ft__truncate_log_filename(err->src.file);
    msg = msg ?: err->type ?: "Unspecified Error";
    nmsg = ft_strbuf_init_str(ft_cstr(msg));
    /* search for suffix */
    if (kvn > 0) {
        memcpy(err->kv, kvs, sizeof(*kvs)*kvn);
        cpy = err->kv;
        for (kv = err->kv; kv->key; kv++) {
            if (strcmp(kv->key, "__msgSuffix") == 0) {
                ft_strbuf_catc(&nmsg, ft_arg_s(kv->val));
                continue;
            }
            switch (ft_arg_type(kv->val)) {
                case 'o':
                    $ref(ft_arg_o(kv->val));
                    break;
                case 's':
                    kv->val.v.s = kv->val.v.s ? ft_cstrdup(kv->val.v.s) : NULL;
                    break;
            }
            if (cpy != kv)
                *cpy = *kv;
            cpy++;
        }
        if (cpy != kv)
            *cpy = (fobj_err_kv_t){NULL, ft_mka_z()};
    }
    err->message = fobj__format_errmsg(ft_strbuf_ref(&nmsg).ptr, err->kv);
    ft_strbuf_free(&nmsg);
    return bind_err(err);
}

err_i
fobj__alloc_err(const char *type,
			   ft_source_position_t src,
			   const char *msg,
			   fobj_err_kv_t *kvs,
			   size_t kvn) {
	fobjErr*        err;
	fobj_err_kv_t*  kv;

	src.func = ft_cstrdup(src.func);
	src.file = ft_cstrdup(src.file);
	err = fobj_alloc_sized(fobjErr,
						   ft_mul_size(sizeof(*kvs), kvn+1),
						   .type = ft_cstrdup(type),
						   .message = ft_cstrdup(msg),
						   .src = src,
						   .free_type_and_src = true,
						   );
	memcpy(err->kv, kvs, sizeof(*kvs)*kvn);
	/* search for suffix */
	for (kv = err->kv; kv->key; kv++) {
		switch (ft_arg_type(kv->val)) {
			case 'o':
				$ref(ft_arg_o(kv->val));
				break;
			case 's':
				kv->val.v.s = kv->val.v.s ? ft_cstrdup(kv->val.v.s) : NULL;
				break;
		}
	}
	return bind_err(err);
}

static void
fobjErr__fobjErr_marker_DONT_IMPLEMENT_ME(VSelf) {
}

static void
fobjErr_fobjDispose(VSelf) {
    Self(fobjErr);
    fobj_err_kv_t *kv;
    for (kv = self->kv; kv->key != NULL; kv++) {
        switch (ft_arg_type(kv->val)) {
            case 'o':
                $del(&kv->val.v.o);
                break;
            case 's':
                ft_free(kv->val.v.s);
                break;
        }
    }
	if (self->free_type_and_src)
	{
		ft_free((void*)self->type);
		ft_free((void*)self->src.file);
		ft_free((void*)self->src.func);
	}
	ft_free((void*)self->message);
    $del(&self->sibling);
}

static fobjStr*
fobjErr_fobjRepr(VSelf) {
    Self(fobjErr);
    char        buffer[256];
    ft_strbuf_t buf = ft_strbuf_init_stack(buffer, 256);
    fobj_err_kv_t*  kv = self->kv;

    ft_strbuf_catc(&buf, "$err(");
    ft_strbuf_catc(&buf, self->type);
    ft_strbuf_catc(&buf, ", ");
    fobj_format_string(&buf, ft_cstr(self->message), "q");
    for (;kv->key; kv++) {
        ft_strbuf_catc(&buf, ", (");
        ft_strbuf_catc(&buf, kv->key);
        ft_strbuf_catc(&buf, ", ");
        fobj_repr_arg(&buf, kv->val);
        ft_strbuf_cat1(&buf, ')');
    }
    ft_strbuf_cat1(&buf, ')');
    return fobj_strbuf_steal(&buf);
}

static void
fobjErr_fobjFormat(VSelf, ft_strbuf_t *buf, const char *fmt) {
    Self(fobjErr);
    const char*     c;
    fobj_err_kv_t*  kv = self->kv;

    if (fmt == NULL || fmt[0] == 0) {
        // fmt = "$T: $M ($F@$f:$l)";
        ft_strbuf_catf(buf, "%s: %s (%s@%s:%d)",
                       self->type, self->message,
                       self->src.func, self->src.file, self->src.line);
        return;
    }

    for (c = fmt; *c; c++) {
        if (*c != '$') {
            ft_strbuf_cat1(buf, *c);
            continue;
        }
        c++;
        switch (*c) {
        case 0:     c--; break;
        case '$':   ft_strbuf_cat1(buf, '$'); break;
        case 'T':   ft_strbuf_catc(buf, self->type); break;
        case 'M':   ft_strbuf_catc(buf, self->message); break;
        case 'F':   ft_strbuf_catc(buf, self->src.func); break;
        case 'f':   ft_strbuf_catc(buf, self->src.file); break;
        case 'l':   ft_strbuf_catf(buf, "%d", self->src.line); break;
        case 'K':
            ft_strbuf_cat1(buf, '{');
            for (kv = self->kv; kv->key; kv++) {
                if (kv != self->kv)
                    ft_strbuf_catc(buf, ", ");
                fobj_format_string(buf, ft_cstr(kv->key), NULL);
                ft_strbuf_catc(buf, ": ");
                fobj_format_arg(buf, kv->val, NULL);
            }
            ft_strbuf_cat1(buf, '}');
            break;
        default:
            ft_log(FT_ERROR, "Unknown error format character '%c'", *c);
        }
    }
}

ft_arg_t
fobj_err_getkv(err_i err, const char *key, ft_arg_t dflt, bool *found) {
    fobjErr*        oerr = (fobjErr*)(err.self);
    fobj_err_kv_t*  kv;
    if (oerr == NULL) return dflt;
    ft_assert(fobj_real_klass_of(oerr) == fobjErr__kh());                                               \
    kv = oerr->kv;
    for (;kv->key != NULL; kv++) {
        if (strcmp(kv->key, key) == 0) {
            if (found) *found = true;
            return kv->val;
        }
    }
    if (found) found = false;
    return dflt;
}

fobjStr*
fobj_printkv(const char *fmt, ft_slc_fokv_t kvs) {
    char            buf[128];
    ft_strbuf_t     out = ft_strbuf_init_stack(buf, 128);
    size_t          i;
    const char*     cur;
    char*           closebrace;
    char*           formatdelim;
    size_t          identlen;
    size_t          formatlen;
    char            ident[32];
    char            format[32];

    if (strchr(fmt, '{') == NULL || strchr(fmt, '}') == NULL) {
        return fobj_str(fmt);
    }

    for (cur = fmt; *cur; cur++) {
        if (*cur != '{') {
            ft_strbuf_cat1(&out, *cur);
            continue;
        }
        if (cur[1] == '{') {
            ft_strbuf_cat1(&out, '{');
            cur++;
            continue;
        }
        cur++;
        closebrace = strchr(cur, '}');
        ft_assert(closebrace, "format string braces unbalanced");
        formatdelim = memchr(cur, ':', closebrace - cur);
        identlen = (formatdelim ?: closebrace) - cur;
        ft_assert(identlen <= 31,
                  "ident is too long in format \"%s\"", fmt);
        ft_assert(formatdelim == NULL || closebrace - formatdelim <= 31,
                  "format is too long in format \"%s\"", fmt);
        memcpy(ident, cur, identlen);
        ident[identlen] = 0;
        formatlen = formatdelim ? closebrace - (formatdelim+1) : 0;
        if (formatlen > 0) {
            memcpy(format, formatdelim + 1, formatlen);
        }
        format[formatlen] = 0;
        i = ft_search_fokv(kvs.ptr, kvs.len, ident, fobj_fokv_cmpc);
        if (ft_unlikely(i >= kvs.len)) {
            ft_log(FT_WARNING, "ident '%s' is not found (fmt \"%s\")", ident, fmt);
        } else if (kvs.ptr[i].value == NULL) {
            ft_strbuf_catc(&out, "NULL");
        } else if (!$ifdef(, fobjFormat, kvs.ptr[i].value, &out, format)) {
            /* fallback to repr */
            ft_strbuf_cat(&out, fobj_getstr(fobjRepr(kvs.ptr[i].value)));
        }
        cur = closebrace;
    }

    return fobj_strbuf_steal(&out);
}

#ifndef WIN32
static pthread_key_t fobj_AR_current_key = 0;
static void fobj_destroy_thread_AR(void *arg);
#endif

/* Custom fobjBase implementation */
fobj_klass_handle_t
fobjBase__kh(void) {
    static volatile fobj_klass_handle_t hndl = 0;
    fobj_klass_handle_t khandle = hndl;
    ssize_t kls_size = sizeof(fobjBase);
    if (khandle) return khandle;
    {
        fobj__method_impl_box_t methods[] = {
            fobj__klass_decl_methods(fobjBase, fobj__map_params(kls__fobjBase))
            { 0, NULL }
        };
        if (fobj_klass_init_impl(&hndl, kls_size, 0, methods, "fobjBase"))
            return hndl;
    }
    khandle = hndl;
    return khandle;
}

fobj_klass_handle(fobjErr, mth(fobjRepr, _fobjErr_marker_DONT_IMPLEMENT_ME), varsized(kv));
fobj_klass_handle(fobjStr, mth(fobjDispose), varsized());
fobj_klass_handle(fobjInt);
fobj_klass_handle(fobjUInt);
fobj_klass_handle(fobjFloat);
fobj_klass_handle(fobjBool);
fobj_klass_handle(fobjTempBuffer);

void
fobj_init(void) {
    ft_assert(fobj_global_state == FOBJ_RT_NOT_INITIALIZED);

#ifndef WIN32
    {
        int res = pthread_key_create(&fobj_AR_current_key, fobj_destroy_thread_AR);
        if (res != 0) {
            fprintf(stderr, "could not initialize autorelease thread key: %s",
                    strerror(res));
            abort();
        }
    }
#endif

    fobj_global_state = FOBJ_RT_INITIALIZED;

    fobj__consume(fobjDispose__mh());
    fobj_klass_init(fobjBase);
    fobj_klass_init(fobjErr);
    fobj_klass_init(fobjStr);
    fobj_klass_init(fobjInt);
    fobj_klass_init(fobjUInt);
    fobj_klass_init(fobjFloat);
    fobj_klass_init(fobjBool);

    FOBJ_FUNC_ARP();

    fobjTrue    = $alloc(fobjBool, .b = true);
    fobjFalse   = $alloc(fobjBool, .b = false);
    falseRepr   = $ref($S("$B(false)"));
    trueRepr    = $ref($S("$B(true)"));
}

void
fobj_freeze(void) {
    fobj_global_state = FOBJ_RT_FROZEN;
}

/* Without this function clang could commit initialization of klass without methods */
volatile uint16_t fobj__FAKE__x;
void
fobj__consume(uint16_t _) {
    fobj__FAKE__x += _;
}

// AUTORELEASE POOL

static void fobj_autorelease_pool_release_till(fobj_autorelease_pool **from, fobj_autorelease_pool *till);

#ifndef __TINYC__
static __thread fobj_autorelease_pool *fobj_AR_current = NULL;
#ifndef WIN32
static __thread bool fobj_AR_current_set = false;
#endif
static inline fobj_autorelease_pool**
fobj_AR_current_ptr(void) {
    ft_assert(fobj_global_state != FOBJ_RT_NOT_INITIALIZED);

#ifndef WIN32
    if (!fobj_AR_current_set)
        pthread_setspecific(fobj_AR_current_key, &fobj_AR_current);
#endif
    return &fobj_AR_current;
}
#ifndef WIN32
static void
fobj_destroy_thread_AR(void *arg) {
    ft_assert(arg == &fobj_AR_current);
    fobj_autorelease_pool_release_till(&fobj_AR_current, NULL);
}
#endif
#else
static fobj_autorelease_pool**
fobj_AR_current_ptr(void) {
    fobj_autorelease_pool **current;

    ft_assert(fobj_global_state != FOBJ_RT_NOT_INITIALIZED);

    current = pthread_getspecific(fobj_AR_current_key);
    if (current == NULL) {
        current = ft_calloc(sizeof(fobj_autorelease_pool*));
        pthread_setspecific(fobj_AR_current_key, current);
    }
    return current;
}

static void
fobj_destroy_thread_AR(void *arg) {
    fobj_autorelease_pool **current = arg;

    fobj_autorelease_pool_release_till(current, NULL);
    ft_free(current);
}
#endif

fobj__autorelease_pool_ref
fobj_autorelease_pool_init(fobj_autorelease_pool *pool) {
    fobj_autorelease_pool **parent = fobj_AR_current_ptr();
    pool->ref.parent = *parent;
    pool->ref.root = parent;
    pool->last = &pool->first;
    pool->first.prev = NULL;
    pool->first.cnt = 0;
    *parent = pool;
    return pool->ref;
}

void
fobj_autorelease_pool_release(fobj_autorelease_pool *pool) {
    fobj_autorelease_pool_release_till(pool->ref.root, pool->ref.parent);
}

static void
fobj_autorelease_pool_release_till(fobj_autorelease_pool **from, fobj_autorelease_pool *till) {
    fobj_autorelease_pool   *current;
    fobj_autorelease_chunk  *chunk;

    while (*from != till) {
        current = *from;
        while (current->last != &current->first || current->last->cnt != 0) {
            chunk = current->last;
            if (chunk->cnt == 0) {
                current->last = chunk->prev;
                ft_free(chunk);
                continue;
            }
            fobj_del(&chunk->refs[--chunk->cnt]);
        }
        ft_assert(*from == current);
        *from = (*from)->ref.parent;
    }
}

static fobj_t
fobj_autorelease(fobj_t obj, fobj_autorelease_pool *pool) {
    fobj_autorelease_chunk  *chunk, *new_chunk;

    if (obj == NULL)
        return NULL;

    ft_assert(pool != NULL);

    chunk = pool->last;
    if (chunk->cnt == FOBJ_AR_CHUNK_SIZE) {
        new_chunk = ft_calloc(sizeof(fobj_autorelease_chunk));
        new_chunk->prev = chunk;
        pool->last = chunk = new_chunk;
    }
    chunk->refs[chunk->cnt] = obj;
    chunk->cnt++;
    return obj;
}

fobj_t
fobj_store_to_parent_pool(fobj_t obj, fobj_autorelease_pool *child_pool_or_null) {
    if (obj == NULL)
        return NULL;
    return fobj_autorelease(obj,
            (child_pool_or_null ?: *fobj_AR_current_ptr())->ref.parent);
}

ft_register_source();
