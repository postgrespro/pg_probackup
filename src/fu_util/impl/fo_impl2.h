/* vim: set expandtab autoindent cindent ts=4 sw=4 sts=4 */
#ifndef FOBJ_OBJ_PRIV2_H
#define FOBJ_OBJ_PRIV2_H

#include <ft_util.h>
#include <fo_obj.h>
#include <impl/fo_impl.h>

enum fobjStrType {
    FOBJ_STR_SMALL = 1,
    FOBJ_STR_UNOWNED,
    FOBJ_STR_PTR,
};
#define FOBJ_STR_SMALL_SIZE ((1<<14)-1)
#define FOBJ_STR_FREE_SPACE (sizeof(fobjStr) - offsetof(fobjStr, small.buf))

union fobjStr {
    struct {
        uint16_t    type:2;
    };
    struct {
        uint16_t    type:2;
        uint16_t    len:14;
        char        buf[];
    } small;
    struct {
        uint16_t    type:2;
        uint32_t    len;
        char* ptr;
    } ptr;
};

ft_inline fobjStr*
fobj_str(const char* s) {
    return fobj_newstr(ft_cstr(s), FOBJ_STR_COPY);
}

ft_inline fobjStr*
fobj_str_const(const char* s) {
    return fobj_newstr(ft_cstr(s), FOBJ_STR_CONST);
}

ft_inline fobjStr*
fobj_strbuf_steal(ft_strbuf_t *buf) {
    if (buf->len < FOBJ_STR_FREE_SPACE && !buf->alloced)
        return fobj_newstr(ft_strbuf_ref(buf), FOBJ_STR_COPY);
    return fobj_newstr(ft_strbuf_steal(buf), FOBJ_STR_GIFTED);
}

ft_inline ft_str_t
fobj_getstr(fobjStr *str) {
    switch (str->type) {
    case FOBJ_STR_SMALL:
        return ft_str(str->small.buf, str->small.len);
    case FOBJ_STR_PTR:
    case FOBJ_STR_UNOWNED:
        return ft_str(str->ptr.ptr, str->ptr.len);
    default:
        ft_log(FT_FATAL, "Unknown fobj_str type %d", str->type);
    }
}

ft_inline fobjStr*
fobj_strcatc(fobjStr *ostr, const char *str) {
    return fobj_strcat(ostr, ft_cstr(str));
}

ft_inline fobjStr*
fobj_strcatc2(fobjStr *ostr, const char *str1, const char *str2) {
    /* a bit lazy to do it in a fast way */
    return fobj_strcat2(ostr, ft_cstr(str1), ft_cstr(str2));
}

ft_inline fobjStr*
fobj_stradd(fobjStr *ostr, fobjStr *other) {
    return fobj_strcat(ostr, fobj_getstr(other));
}

ft_inline bool
fobj_streq(fobjStr* self, fobjStr *oth) {
    return ft_streq(fobj_getstr(self), fobj_getstr(oth));
}

ft_inline FT_CMP_RES
fobj_strcmp(fobjStr* self, fobjStr *oth) {
    return ft_strcmp(fobj_getstr(self), fobj_getstr(oth));
}

ft_inline bool
fobj_streq_str(fobjStr* self, ft_str_t oth) {
    return ft_streq(fobj_getstr(self), oth);
}

ft_inline FT_CMP_RES
fobj_strcmp_str(fobjStr* self, ft_str_t oth) {
    return ft_strcmp(fobj_getstr(self), oth);
}

ft_inline bool
fobj_streq_c(fobjStr* self, const char *oth) {
    return ft_streqc(fobj_getstr(self), oth);
}

ft_inline FT_CMP_RES
fobj_strcmp_c(fobjStr* self, const char *oth) {
    return ft_strcmpc(fobj_getstr(self), oth);
}

ft_inline fobjInt*
fobj_int(int64_t i) {
    return $alloc(fobjInt, .i = i);
}

ft_inline fobjUInt*
fobj_uint(uint64_t u) {
    return $alloc(fobjUInt, .u = u);
}

ft_inline fobjFloat*
fobj_float(double f) {
    return $alloc(fobjFloat, .f = f);
}

typedef struct fobjErr fobjErr;
struct fobjErr {
    const char*     type;
    const char*     message;
    ft_source_position_t src;
    fobjErr*        sibling; /* sibling error */
    fobj_err_kv_t   kv[];
};

#define fobj_make_err(type, ...) \
        fm_cat(fobj_make_err_, fm_va_01n(__VA_ARGS__))(type, __VA_ARGS__)
#define fobj_make_err_0(type, ...) ({ \
    fobj__make_err(fobj_error_kind_##type(), \
                  ft__srcpos(), "Unspecified Error", NULL, 0); \
})
#define fobj_make_err_1(type, msg) ({ \
    fobj__make_err(fobj_error_kind_##type(), \
                  ft__srcpos(), msg, NULL, 0); \
})
#define fobj_make_err_n(type, msg, ...) ({ \
    fobj_err_kv_t  kvs[] = {            \
        fobj__err_transform_kv(__VA_ARGS__) \
    };                                     \
    fobj__make_err(fobj_error_kind_##type(), \
                  ft__srcpos(), msg, \
                  kvs, ft_arrsz(kvs)); \
})

#define fobj_make_syserr(erno_, ...) \
        fm_cat(fobj_make_syserr_, fm_va_01(__VA_ARGS__))((erno_), fm_uniq(erno), __VA_ARGS__)
#define fobj_make_syserr_0(erno_, erno, ...) ({ \
    int erno = erno_;                           \
    fobj_err_kv_t  kvs[] = {                \
        {"errNo", ft_mka_i(erno)},         \
        {"errNoStr", ft_mka_s((char*)ft_strerror(erno))}, \
    };                                \
    fobj__make_err(fobj_error_kind_SysErr(), \
                   ft__srcpos(), "System Error: {errNoStr}", \
                   kvs, ft_arrsz(kvs));\
})
#define fobj_make_syserr_1(erno_, erno, msg, ...) ({ \
    int erno = erno_;                           \
    fobj_err_kv_t  kvs[] = {                \
        {"errNo", ft_mka_i(erno)},         \
        {"errNoStr", ft_mka_s((char*)ft_strerror(erno))}, \
        {"__msgSuffix", ft_mka_s((char*)": {errNoStr}")},  \
        fobj__err_transform_kv(__VA_ARGS__) \
    };                                \
    fobj__make_err(fobj_error_kind_SysErr(), \
                   ft__srcpos(), msg, \
                   kvs, ft_arrsz(kvs));\
})

extern err_i fobj__make_err(const char *type,
                                ft_source_position_t src,
                                const char *msg,
                                fobj_err_kv_t *kvs,
                                size_t kvn);

#define fobj__err_transform_kv_do(v) \
    fobj__err_mkkv_##v
#define fobj__err_transform_kv(...) \
    fm_eval_foreach_comma(fobj__err_transform_kv_do, __VA_ARGS__)

#define fobj__err_getkey(key, err, ...) \
    fobj__err_getkv_##key(err, fm_or_default(__VA_ARGS__)(NULL))

ft_inline int
getErrno(err_i err) {
    return $errkey(errNo, err);
}

ft_inline const char*
getErrnoStr(err_i err) {
    return $errkey(errNoStr, err);
}

ft_inline const char*
fobj_errtype(err_i err) {
    fobjErr*    self = (fobjErr*)(err.self);
    ft_assert(fobj_real_klass_of(self) == fobjErr__kh());                                               \
    return self->type ? self->type : "RT";
}

ft_inline const char*
fobj_errmsg(err_i err) {
    fobjErr*    self = (fobjErr*)(err.self);
    ft_assert(fobj_real_klass_of(self) == fobjErr__kh());                                               \
    return self->message ? self->message : "Unspecified Error";
}

ft_inline ft_source_position_t
fobj_errsrc(err_i err) {
    fobjErr*    self = (fobjErr*)(err.self);
    ft_assert(fobj_real_klass_of(self) == fobjErr__kh());                                               \
    return self->src;
}

#define fobj__printkv(fmt, ...) \
    fm_cat(fobj__printkv_, fm_va_01(__VA_ARGS__))(fmt, __VA_ARGS__)

#define fobj__printkv_0(fmt, ...) \
    fobj_printkv(fmt, ft_slc_fokv_make(NULL, 0))

#define fobj__printkv_1(fmt, ...) ({ \
    fobj_kv kvs[] = {                \
        fobj__transform_fokv(__VA_ARGS__) \
    };                             \
    fobj_printkv(fmt, ft_slc_fokv_make(kvs, ft_arrsz(kvs))); \
})

#define fobj__transform_fokv_do(key, val) \
    { #key, val }
#define fobj__transform_fokv(...) \
    fm_eval_tuples_comma(fobj__transform_fokv_do, __VA_ARGS__)

#endif // FOBJ_OBJ_PRIV2_H
