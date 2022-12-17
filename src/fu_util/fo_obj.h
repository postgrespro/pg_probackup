/* vim: set expandtab autoindent cindent ts=4 sw=4 sts=4 */
#ifndef FOBJ_OBJ_H
#define FOBJ_OBJ_H

#include <assert.h>

typedef void* fobj_t;

#include <ft_util.h>

/*
 * Pointer to "object*.
 * In fact, it is just 'void *'.
 */
/*
 * First argument, representing method receiver.
 * Unfortunately, function couldn't have arbitrary typed receiver without issueing
 * compiller warning.
 * Use Self(Klass) to convert to concrete type.
 */
#define VSelf fobj_t Vself
/*
 * Self(Klass) initiate "self" variable with casted pointer.
 */
#define Self(Klass) Self_impl(Klass)

extern void fobj_init(void);
/*
 * fobj_freeze forbids further modifications to runtime.
 * It certainly should be called before additional threads are created.
 */
extern void fobj_freeze(void);

#define fobj_self_klass 0

#include "./impl/fo_impl.h"

/* Generate all method boilerplate. */
#define fobj_method(method) fobj__define_method(method)

/* Generates runtime privately called method boilerplate */
#define fobj_special_method(meth) fobj__special_method(meth)

/*
 * Ensure method initialized.
 * Calling fobj_method_init is not required,
 * unless you want search method by string name or use `fobj_freeze`
 */
#define fobj_method_init(method) fobj__method_init(method)

/* Declare klass handle */
#define fobj_klass(klass) fobj__klass_declare(klass)
/*
 * Implement klass handle.
 * Here all the binding are done, therefore it should be called
 * after method implementions or at least prototypes.
 * Additional declarations could be passed here.
 */
#define fobj_klass_handle(klass, ...) fobj__klass_handle(klass, __VA_ARGS__)
/*
 * Calling fobj_klass_init is not required,
 * unless you want search klass by string name or use `fobj_freeze`
 */
#define fobj_klass_init(klass) fobj__klass_init(klass)
#define fobj_add_methods(klass, ...) fobj__add_methods(klass, __VA_ARGS__)

#define fobj_iface(iface) fobj__iface_declare(iface)

/*
 * Allocate klass instance, and optionally copy fields.
 *
 * $alloc(klass)
 * fobj_alloc(klass)
 *      allocates instance
 * $alloc(klass, .field1 = val1, .field2 = val2) -
 * fobj_alloc(klass, .field1 = val1, .field2 = val2) -
 *      allocates instance
 *      copies `(klass){.field1 = val1, .field2 = val2}`
 */
#define fobj_alloc(klass, ...) \
    fobj__alloc(klass, __VA_ARGS__)
#define $alloc(klass, ...) \
    fobj__alloc(klass, __VA_ARGS__)

/*
 * Allocate variable sized instance with additional size.
 * Size should be set in bytes, not variable sized field elements count.
 * Don't pass variable sized fields as arguments, they will not be copied.
 * Fill variable sized fields later.
 *
 * fobj_alloc_sized(Klass, size)
 *      allocates instance with custom additional `size`
 *      returns obj
 * fobj_alloc_sized(Klass, size, .field1 = val1, .field2 = val2)
 *      allocates instance with custom additional `size`
 *      copies `(klass){.field1 = val1, .field2 = val2}`
 *      returns obj
 */
#define fobj_alloc_sized(klass, size, ...) \
    fobj__alloc_sized(klass, size, __VA_ARGS__)

/*
 * Object lifetime.
 *
 * $ref(obj)
 *      Add reference to object.
 *      Manually increments reference count. It will prevent object's destruction.
 * $unref(obj)
 *      Forget reference to object, but keep object alive (for some time).
 *      Will put object to AutoRelease Pool, so it will be destroyed later.
 * $del(&var)
 *      Drop reference to object and (probably) destroy it.
 *      Manually decrement reference count and clear variable.
 *      It will destroy object, if its reference count become zero.
 * $set(&var, obj)
 *      Replace value, pointed by first argument, with new value.
 *      New value will be passed to `$ref` and assigned to ptr.
 *      Then old value will be passed to `$del`, so it could be destroyed at this
 *      moment.
 * $swap(&var, obj)
 *      Replace value, pointed by first argument, with new value, and return old
 *      value.
 *      New value will be passed to `$ref` and assigned to ptr.
 *      Then old value will be passed to `$unref`, so preserved in ARP.
 *
 * Same routines for interfaces:
 *
 * $iref(obj)
 * $iunref(obj)
 * $idel(&var)
 * $iset(&var, iface)
 * $iswap(&var, iface)
 *
 * AutoRelease Pool.
 *
 * AutoRelease Pool holds references to objects "about to be destroyed".
 *
 * AutoRelease Pool is drained on scope exit using GCC's __attribute__((cleanup)),
 * and all objects stored in ARP are passed to $del.
 *
 * Newly created objects are always registered in current Autorelelease Pool,
 * and if no $ref(obj), $set(var, obj) or $swap is called with them, they will be
 * automatially destroyed on scope exit.
 *
 * As well, if you replace/delete object in some container and return old value, it
 * should be put into AutoRelease Pool to be preserved for value acceptor.
 *
 * FOBJ_FUNC_ARP()
 *      Declare autorelease pool for function scope.
 * FOBJ_LOOP_ARP()
 *      Declare autorelease pool for loop body.
 * FOBJ_BLOCK_ARP()
 *      Declare autorelease pool for block body.
 *
 * $save(obj), $isave(iface)
 *      Increment reference and store object in parent autorelease pool.
 *      It is used to preserve object on loop or block exit with autorelease pool
 *      declared (`FOBJ_LOOP_ARP()` or `FOBJ_BLOCK_ARP()`).
 * $result(obj), $iresult(iface)
 *      Increment reference and store object in autorelease pool parent to
 *      function's one.
 *      It is used to preserve object on exit from function with autorelease pool
 *      declared (`FOBJ_FUNC_ARP()`).
 * $return(obj), $ireturn(obj)
 *      is just `return $result(obj)`
 */
#define $ref(obj)               fobj_ref(obj)
#define $unref(obj)             fobj_unref(obj)
#define $del(var)               $set(var, NULL)
#define $set(var, obj)          fobj__set_impl((var), (obj))
#define $swap(var, obj)         fobj__swap_impl((var), (obj))

extern fobj_t fobj_ref(fobj_t obj);
extern fobj_t fobj_unref(fobj_t obj);
#define fobj_del(var)           fobj_set(var, NULL)
extern void   fobj_set(fobj_t* var, fobj_t newval);
extern fobj_t fobj_swap(fobj_t* var, fobj_t newval);

#define $iref(iface)            fobj__iref(iface)
#define $iunref(iface)          fobj__iunref(iface)
#define $idel(iface)            fobj__idel(iface)
#define $iset(ptr, iface)       fobj__iset((ptr), (iface))
#define $iswap(ptr, iface)      fobj__iswap((ptr), (iface))

#define FOBJ_FUNC_ARP() FOBJ_ARP_POOL(fobj__func_ar_pool)
#define FOBJ_LOOP_ARP() FOBJ_ARP_POOL(fobj__block_ar_pool)
#define FOBJ_BLOCK_ARP() FOBJ_ARP_POOL(fobj__block_ar_pool)

#define $save(obj)      fobj_store_to_parent_pool($ref(obj), &fobj__block_ar_pool)
#define $result(obj)    fobj_store_to_parent_pool($ref(obj), &fobj__func_ar_pool)
#define $return(obj)    return $result(obj)

#define $isave(iface)	fobj__isave(iface)
#define $iresult(iface)	fobj__iresult(iface)
#define $ireturn(iface)	fobj__ireturn(iface)

/*
 * fobjDispose should finish all object's activity and release resources.
 * It is called automatically before destroying object.
 */
#define mth__fobjDispose    void
fobj_special_method(fobjDispose);

/*
 * returns globally allocated klass name.
 * DO NOT modify it.
 */
extern const char *fobj_klass_name(fobj_klass_handle_t klass);

/*
 * Return real klass of object.
 *
 * Note: `fobjKlass` is a method, so it could return something dirrefent from
 * real klass. But if you have to cast pointer, you'd probably need real klass.
 *
 * But in other cases you'd better not abuse this function.
 */
extern fobj_klass_handle_t fobj_real_klass_of(fobj_t);

/*
 * Call method with named/optional args.
 *
 *      $(someMethod, object)
 *      $(someMethod, object, v1, v2)
 *      $(someMethod, object, .arg1=v1, .arg2=v2)
 *      $(someMethod, object, .arg2=v2, .arg1=v1)
 *      // Skip optional .arg3
 *      $(someMethod, object, v1, v2, .arg4=v4)
 *      $(someMethod, object, .arg1=v1, .arg2=v2, .arg4=v4)
 *      // Order isn't important with named args.
 *      $(someMethod, object, .arg4=v4, .arg1=v1, .arg2=v2)
 *      $(someMethod, object, .arg4=v4, .arg2=v2, .arg1=v1)
 *
 *      fobj_call(someMethod, object)
 *      fobj_call(someMethod, object, v1, v2)
 *      fobj_call(someMethod, object, .arg1=v1, .arg2=v2)
 *      fobj_call(someMethod, object, v1, v2, .arg4=v4)
 *      fobj_call(someMethod, object, .arg1=v1, .arg2=v2, .arg4=v4)
 */
#define $(meth, self, ...) \
    fobj_call(meth, self, __VA_ARGS__)

/*
 * Call parent klass method implementation with named/optional args.
 *
 *      $super(someMethod, object)
 *      $super(someMethod, object, v1, v2, .arg4=v4)
 *      $super(someMethod, object, .arg1=v1, .arg2=v2, .arg4=v4)
 *      fobj_call_super(someMethod, object)
 *      fobj_call_super(someMethod, object, v1, v2)
 *      fobj_call_super(someMethod, object, v1, v2, .arg4=v4)
 *      fobj_call_super(someMethod, object, .arg1=v1, .arg2=v2, .arg4=v4)
 *
 * It uses variable set inside of Self(klass) statement.
 */
#define $super(meth, self, ...) \
    fobj_call_super(meth, fobj__klassh, self, __VA_ARGS__)

/*
 * Call method stored in the interface struct.
 * Interface is passed by value, not pointer.
 *
 *      SomeIface_i someIface = bind_SomeIface(obj);
 *      $i(someMethod, someIface)
 *      $i(someMethod, someIface, v1, v2, .arg4=v4)
 *      $i(someMethod, someIface, .arg1=v1, .arg2=v2, .arg4=v4)
 *      fobj_iface_call(someMethod, someIface)
 *      fobj_iface_call(someMethod, someIface, v1, v2)
 *      fobj_iface_call(someMethod, someIface, v1, v2, .arg4=v4)
 *      fobj_iface_call(someMethod, someIface, .arg1=v1, .arg2=v2, .arg4=v4)
 */
#define $i(meth, iface, ...) \
    fobj_iface_call(meth, iface, __VA_ARGS__)

/*
 * Determine if object implements interface.
 *
 *      if ($implements(someIface, object, &iface_var)) {
 *          $i(someMethod, iface_var);
 *      }
 *
 *      if ($implements(someIface, object)) {
 *          workWith(object);
 *      }
 *
 *      if (fobj_implements(iface, object, &iface_var)) {
 *          fobj_iface_call(someMethod, iface_var);
 *      }
 *
 *      if (fobj_implements(iface, object)) {
 *          workWith(object);
 *      }
 *
 *  And without macroses:
 *
 *      if (implements_someIface(object, &iface_var)) {
 *          $i(someMethod, iface_var);
 *      }
 *
 *      if (implements_someIface(object, NULL)) {
 *          workWith(object);
 *      }
 */
#define $implements(iface, obj, ...) \
    fobj__implements(iface, obj, __VA_ARGS__)
#define fobj_implements(iface, obj, ...) \
    fobj__implements(iface, obj, __VA_ARGS__)

/*
 * Determine if optional method is filled in interface.
 * Note: required methods are certainly filled.
 *
 *      if ($ifilled(someMethod, iface)) {
 *          $i(someMethod, iface);
 *      }
 *
 *      if (fobj_iface_filled(someMethod, iface)) {
 *          fobj_iface_call(someMethod, iface);
 *      }
 */
#define $ifilled(meth, iface) \
    fobj_iface_filled(meth, iface)

/*
 * Call method if it is defined, and assign result.
 *
 *      value_t val;
 *      if ($ifdef(val =, someMethod, self, v1, v2, .arg4=v4)) {
 *          ...
 *      }
 *
 *  or doesn't assign result
 *
 *      if ($ifdef(, someMethod, self, v1, v2, .arg4=v4)) {
 *          ...
 *      }
 */
#define $ifdef(assignment, meth, self, ...) \
    fobj_ifdef(assignment, meth, (self), __VA_ARGS__)

#define $bind(iface_type, obj) 		fobj_bind(iface_type, (obj))
#define $reduce(newiface, iface)	fobj_reduce(newiface, (iface))

#define $isNULL(iface)     ((iface).self == NULL)
#define $notNULL(iface)    ((iface).self != NULL)
#define $setNULL(ifacep)   ((ifacep)->self = NULL)
#define $null(iface_type)  ((iface_type##_i){NULL})

/*
 * Base type
 */
#define iface__fobj     mth(fobjKlass, fobjRepr)
/* hardcoded instantiation because of fobj_iface always include iface__fobj */
fobj__iface_declare_i(fobj, (mth, fobjKlass, fobjRepr));

#define mth__fobjRepr   union fobjStr*
fobj__define_base_method(fobjRepr);
#define mth__fobjKlass  fobj_klass_handle_t
fobj__define_base_method(fobjKlass);

#define $repr(obj)      fobj_getstr(fobjRepr(obj)).ptr
#define $irepr(iface)   fobj_getstr(fobjRepr((iface).self)).ptr

typedef struct fobjBase {
    char fobj__base[0];
} fobjBase;
#define kls__fobjBase mth(fobjKlass, fobjRepr)
fobj_klass(fobjBase);

/*
 * fobjFormat should be defined for pretty printing
 */
#define mth__fobjFormat  void, (ft_strbuf_t*, out), (const char*, fmt, NULL)
fobj_method(fobjFormat);

/*********************************
 * String
 */

typedef union fobjStr fobjStr;

ft_inline   fobjStr*    fobj_str(const char* s);
ft_inline   fobjStr*    fobj_str_const(const char* s);
#define $S(s)           (__builtin_constant_p(s) ? fobj_str_const(s) : fobj_str(s))
enum FOBJ_STR_ALLOC {
    FOBJ_STR_GIFTED,
    FOBJ_STR_CONST,
    FOBJ_STR_COPY,
};
extern      fobjStr*    fobj_newstr(ft_str_t str, enum FOBJ_STR_ALLOC ownership);
ft_inline   ft_str_t    fobj_getstr(fobjStr *str);

/*
 * Steal if buffer is allocated, or copy otherwise.
 * Buffer is zeroed and should be re-initialized.
 */
ft_inline   fobjStr*    fobj_strbuf_steal(ft_strbuf_t *buf);

ft_gnu_printf(1, 2)
extern      fobjStr*    fobj_sprintf(const char* fmt, ...);
extern      fobjStr*    fobj_strcat(fobjStr *ostr, ft_str_t str);
extern      fobjStr*    fobj_strcat2(fobjStr *ostr, ft_str_t str1, ft_str_t str2);
ft_inline   fobjStr*    fobj_strcatc(fobjStr *ostr, const char *str);
ft_inline   fobjStr*    fobj_strcatc2(fobjStr *ostr, const char *str1, const char *str2);
ft_inline   fobjStr*    fobj_stradd(fobjStr *ostr, fobjStr *other);
ft_gnu_printf(2, 3)
extern      fobjStr*    fobj_strcatf(fobjStr *str, const char* fmt, ...);

/* String comparison */
ft_inline   bool        fobj_streq(fobjStr* self, fobjStr *oth);
ft_inline   FT_CMP_RES  fobj_strcmp(fobjStr* self, fobjStr *oth);
ft_inline   bool        fobj_streq_str(fobjStr* self, ft_str_t oth);
ft_inline   FT_CMP_RES  fobj_strcmp_str(fobjStr* self, ft_str_t oth);
ft_inline   bool        fobj_streq_c(fobjStr* self, const char *oth);
ft_inline   FT_CMP_RES  fobj_strcmp_c(fobjStr* self, const char *oth);

/* turn object to string using fobjFormat */
extern      fobjStr*    fobj_tostr(fobj_t obj, const char* fmt);
#define $tostr(obj, ...)    fobj_getstr(fobj_tostr((obj), fm_or_default(__VA_ARGS__)(NULL))).ptr
#define $itostr(obj, ...)   fobj_getstr(fobj_tostr((obj).self, fm_or_default(__VA_ARGS__)(NULL))).ptr

#define kls__fobjStr    mth(fobjRepr, fobjFormat)
fobj_klass(fobjStr);

/**********************************
 * Int
 */

typedef struct fobjInt {
    int64_t     i;
} fobjInt;

ft_inline   fobjInt*    fobj_int(int64_t i);
#define $I(i)           fobj_int(i)

#define kls__fobjInt    mth(fobjRepr, fobjFormat)
fobj_klass(fobjInt);

/**********************************
 * UInt
 */

typedef struct fobjUInt {
    uint64_t    u;
} fobjUInt;

ft_inline   fobjUInt*   fobj_uint(uint64_t u);
#define $U(i)           fobj_uint(i)

#define kls__fobjUInt   mth(fobjRepr, fobjFormat)
fobj_klass(fobjUInt);

/**********************************
 * Float
 */

typedef struct fobjFloat {
    double      f;
} fobjFloat;

ft_inline   fobjFloat*  fobj_float(double f);
#define $F(f)           fobj_float(f)

#define kls__fobjFloat  mth(fobjRepr, fobjFormat)
fobj_klass(fobjFloat);

/**********************************
 * Bool
 */

typedef struct fobjBool {
    bool        b;
} fobjBool;

extern fobjBool*        fobj_bool(bool f);
#define $B(f)           fobj_bool(f)

#define kls__fobjBool   mth(fobjRepr, fobjFormat)
fobj_klass(fobjBool);

/*
 * Allocate temporary blob.
 * It could be anything, and it will be automatically released.
 */
static inline void* fobj_alloc_temp(size_t buf_size);
/* get object pointer for temporary blob */
static inline fobj_t fobj_temp2obj(void* temp);
#define fobj_temp_save(ptr)		$save(fobj_temp2obj(ptr))
#define fobj_temp_result(ptr)   $result(fobj_temp2obj(ptr))
#define fobj_temp_return(ptr)   $return(fobj_temp2obj(ptr))

/**********************************
 * kv
 */
typedef struct fobj_kv {
    const char *    key;
    fobj_t          value;
} fobj_kv;

#define FT_SLICE        fokv
#define FT_SLICE_TYPE   fobj_kv
#include <ft_array.inc.h>
#define FT_SEARCH           fokv
#define FT_SEARCH_TYPE      fobj_kv
#define FT_SEARCH_PATTERN   const char*
#include <ft_search.inc.h>
ft_inline FT_CMP_RES fobj_fokv_cmpc(fobj_kv kv, const char* nm) {
    return strcmp(kv.key, nm);
}

extern  fobjStr*        fobj_printkv(const char *fmt, ft_slc_fokv_t kv);
#define $fmt(fmt, ...)  fobj__printkv(fmt, __VA_ARGS__)


/**********************************
 * ERRORS
 */

#define mth___fobjErr_marker_DONT_IMPLEMENT_ME void
fobj_special_method(_fobjErr_marker_DONT_IMPLEMENT_ME);

#define iface__err      mth(_fobjErr_marker_DONT_IMPLEMENT_ME)
fobj_iface(err);

#define fobj_error_kind(err)        fobj__error_kind(err)
#define fobj_error_flag_key(key)    fobj__error_flag_key(key)
#define fobj_error_int_key(key)     fobj__error_int_key(key)
#define fobj_error_uint_key(key)    fobj__error_uint_key(key)
#define fobj_error_cstr_key(key)    fobj__error_cstr_key(key)
#define fobj_error_float_key(key)   fobj__error_float_key(key)
#define fobj_error_bool_key(key)    fobj__error_bool_key(key)
#define fobj_error_object_key(key)  fobj__error_object_key(key)

extern ft_arg_t fobj_err_getkv(err_i err, const char *key, ft_arg_t dflt, bool *found);

fobj_error_kind(RT);
fobj_error_kind(SysErr);

fobj_error_object_key(cause);
fobj_error_cstr_key(causeStr);
fobj_error_int_key(errNo);
fobj_error_int_key(intCode);
fobj_error_cstr_key(errNoStr);
#define fobj_errno_keys(errno) (errNo, errno), (errNoStr, ft_strerror(errno))
fobj_error_cstr_key(path);
fobj_error_cstr_key(old_path);
fobj_error_cstr_key(new_path);

/* special key for raw appending to error message */
fobj_error_cstr_key(__msgSuffix);

/*
 * $err(Type)
 * $err(Type, "some error")
 * $err(Type, "Some bad thing happens at {path}", (path, filename))
 */
#define $err(type, ...)     fobj_make_err(type, __VA_ARGS__)
/*
 * $noerr() - empty error
 * $noerr(err) - true, if $isNULL(err)
 */
#define $noerr(...)         fm_if(fm_va_01(__VA_ARGS__), $isNULL(__VA_ARGS__), $null(err))
/*
 * $haserr(err) - true if $notNULL(err)
 */
#define $haserr(err)        $notNULL(err)

/*
 * $syserr(errno)
 * $syserr(errno, "allocation error")
 * $syserr(errno, "Could not open file {path}", (path, filename))
 */
#define $syserr(erno, ...)      fobj_make_syserr((erno), __VA_ARGS__)

/* fetch key back */
#define $errkey(key, err, ...)  fobj__err_getkey(key, err, __VA_ARGS__)
/*
 * Get errno stored in `errNo` error key
 */
ft_inline int           getErrno(err_i err);
/*
 * Get errno string stored in `errNoStr` error key
 */
ft_inline const char*   getErrnoStr(err_i err);

/*
 * Get error type
 */
#define $errtype(err)       fobj_errtype(err)

/*
 * Get error message
 */
#define $errmsg(err)        fobj_errmsg(err)
/*
 * Get error location
 */
#define $errsrc(err)        fobj_errsrc(err)

#define kls__fobjErr mth(fobjDispose, fobjRepr, fobjFormat)
fobj_klass(fobjErr);

/*
 * Combines two error by placing second into single linked list of siblings.
 * If either of error is NULL, other error is returned.
 * If both errors are NULL, then NULL is returned.
 * If second already has siblings, first's list of siblings is appended to
 * second's list, then second becames first sibling of first.
 */
extern err_i fobj_err_combine(err_i first, err_i second);

#define fobj_reset_err(err) do { ft_dbg_assert(err != NULL); *err = (err_i){NULL}; } while(0)

#include "./impl/fo_impl2.h"

#endif
