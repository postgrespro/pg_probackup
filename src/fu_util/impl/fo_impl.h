/* vim: set expandtab autoindent cindent ts=4 sw=4 sts=4 */
#ifndef FOBJ_OBJ_PRIV_H
#define FOBJ_OBJ_PRIV_H

#define Self_impl(Klass) \
    Klass * self ft_unused = Vself; fobj_klass_handle_t fobj__klassh ft_unused = fobj__nm_khandle(Klass)()

typedef uint16_t fobj_klass_handle_t;
typedef uint16_t fobj_method_handle_t;

#define FOBJ_ARGS_COMPLEX

typedef struct fobj__missing_argument_detector {
    char is_set;
} fobj__missing_argument_detector;
#define fobj__dumb_arg ((fobj__missing_argument_detector){1})
#define fobj__check_arg(name) fobj__nm_given(name).is_set

typedef struct {
    fobj_method_handle_t meth;
    void*                impl;
} fobj__method_impl_box_t;

/* params coversions */

/* map params to tuples */
#define fobj__map_params(...) \
    fm_eval_foreach_comma(fobj__map_param, __VA_ARGS__)
#define fobj__map_params_(...) \
    fm_foreach_comma(fobj__map_param, __VA_ARGS__)
#define fobj__map_param(param) \
    fm_cat(fobj__map_param_, param)
#define fobj__map_param_varsized(...)       (varsized, __VA_ARGS__)
#define fobj__map_param_mth(...)            (mth, __VA_ARGS__)
#define fobj__map_param_iface(...)          (iface, __VA_ARGS__)
#define fobj__map_param_inherits(parent)    (inherits, parent)

/* filter and flatten methods */
#define fobj__flat_methods(...) \
    fm_tail(fm_eval_tuples(fobj__fetch_methods, __VA_ARGS__))
#define fobj__fetch_methods(tag, ...) fobj__fetch_methods_##tag(__VA_ARGS__)
#define fobj__fetch_methods_mth(...) , __VA_ARGS__
#define fobj__fetch_methods_iface(...)
#define fobj__fetch_methods_inherits(...)
#define fobj__fetch_methods_varsized(...)

/* filter and flatten interfaces */
#define fobj__flat_ifaces(...) \
    fm_tail(fm_eval_tuples(fobj__fetch_ifaces, __VA_ARGS__))
#define fobj__fetch_ifaces(tag, ...) \
    fobj__fetch_ifaces_##tag(__VA_ARGS__)
#define fobj__fetch_ifaces_mth(...)
#define fobj__fetch_ifaces_iface(...) , __VA_ARGS__
#define fobj__fetch_ifaces_inherits(...)
#define fobj__fetch_ifaces_varsized(...)

/* Standard naming */

#define fobj__nm_mth(meth)          mth__##meth
#define fobj__nm_mthdflt(meth)      mth__##meth##__optional
#define fobj__nm_kls(klass)         kls__##klass
#define fobj__nm_iface(iface)       iface__##iface
#define fobj__nm_mhandle(meth)      meth##__mh
#define fobj__nm_do(meth)           meth##__do
#define fobj__nm_params_t(meth)     meth##__params_t
#define fobj__nm_impl_t(meth)       meth##__impl
#define fobj__nm_cb(meth)           meth##__fetch_cb
#define fobj__nm_cb_t(meth)         meth##__cb_t
#define fobj__nm_register(meth)     meth##__register
#define fobj__nm_wrap_decl(meth)    meth##__wrap_decl
#define fobj__nm_meth_i(meth)       meth##_i
#define fobj__nm_has(m)             has_##m
#define fobj__nm_bind(m_or_i)       bind_##m_or_i
#define fobj__nm_bindref(m_or_i)    bindref_##m_or_i
#define fobj__nm_implements(m_or_i) implements_##m_or_i
#define fobj__nm_khandle(klass)     klass##__kh
#define fobj__nm_klass_meth(klass, meth) klass##_##meth
#define fobj__nm_iface_i(iface)     iface##_i
#define fobj__nm_given(param)       param##__given
#define fobj__nm_kvalidate(m_or_i)  fobj__klass_validate_##m_or_i

/* Method definition */
#define fobj__predefine_method(method) \
    ft_static ft_gcc_const fobj_method_handle_t fobj__nm_mhandle(method)(void)

#define fobj__define_method(meth) \
    fobj__method_declare_i(meth, fobj__nm_mth(meth)) \
    fobj__iface_declare_i(meth, fobj__map_params(mth(meth), iface__fobj)) \
    fm__dumb_require_semicolon
#define fobj__define_base_method(meth) \
    fobj__method_declare_i(meth, fobj__nm_mth(meth)) \
    fobj__iface_declare_i(meth, iface__fobj) \
    fm__dumb_require_semicolon
#define fobj__method_declare_i(meth, ...) \
    fobj__method_declare(meth, __VA_ARGS__)
#define fobj__method_declare(meth, res, ...) \
    fobj__method_declare_impl(meth, \
            fobj__nm_mhandle(meth), \
            fobj__nm_params_t(meth), \
            fobj__nm_do(meth), \
            fobj__nm_impl_t(meth), \
            fobj__nm_cb(meth), \
            fobj__nm_cb_t(meth), \
            fobj__nm_register(meth), \
            fobj__nm_wrap_decl(meth), \
            fobj__nm_meth_i(meth), \
            fobj__nm_bind(meth), \
            fobj__nm_bindref(meth), \
            fobj__nm_implements(meth), \
            fobj__nm_kvalidate(meth), \
            fm_va_comma_fun(__VA_ARGS__), \
            res, __VA_ARGS__)

#define fobj__special_method(meth) \
    fobj__special_method_declare_i(meth, fobj__nm_mth(meth)) \
    fm__dumb_require_semicolon
#define fobj__special_method_declare_i(meth, ...) \
    fobj__special_method_declare(meth, __VA_ARGS__)
#define fobj__special_method_declare(meth, res, ...) \
    fobj__method_common(meth, \
            fobj__nm_mhandle(meth), \
            fobj__nm_impl_t(meth), \
            fobj__nm_register(meth), \
            fobj__nm_wrap_decl(meth), \
            fm_va_comma_fun(__VA_ARGS__), \
            res, __VA_ARGS__)

#define fobj__method_declare_impl(meth, handle, \
                                  params_t, \
                                  meth_do, \
                                  impl_meth_t, \
                                  cb_meth, cb_meth_t, \
                                  register_meth, wrap_decl, \
                                  meth_i, bind_meth, bindref_meth, \
                                  implements_meth, \
                                  kvalidate, comma, res, ...) \
    \
    fobj__method_common(meth, handle, impl_meth_t, register_meth, \
                        wrap_decl, comma, res, __VA_ARGS__) \
    \
    typedef struct params_t { \
        fobj__mapArgs_toFields(__VA_ARGS__) \
    } params_t;                             \
    \
    typedef struct cb_meth_t { \
        fobj_t      self; \
        impl_meth_t impl; \
    } cb_meth_t; \
    \
    ft_inline ft_always_inline cb_meth_t \
    cb_meth(fobj_t self, fobj_klass_handle_t parent, bool validate) { \
        fobj__method_callback_t fnd = {NULL, NULL}; \
        fnd = fobj_method_search(self, handle(), parent, validate); \
        return (cb_meth_t){fnd.self, fnd.impl}; \
    } \
    \
    ft_static res \
    meth_do(fobj_t self, fobj_klass_handle_t parent comma() fobj__mapArgs_toArgs(__VA_ARGS__)) { \
        cb_meth_t cb = cb_meth(self, parent, true); \
        return cb.impl(cb.self comma() fobj__mapArgs_toNames(__VA_ARGS__)); \
    } \
    \
    ft_inline ft_always_inline res \
    meth(fobj_t self comma() fobj__mapArgs_toArgs(__VA_ARGS__)) {  \
        return meth_do(self, fobj_self_klass comma() fobj__mapArgs_toNames(__VA_ARGS__)); \
    }

#define fobj__method_common(meth, handle, impl_meth_t, register_meth, \
                            wrap_decl, comma, res, ...) \
    \
    ft_inline ft_gcc_const fobj_method_handle_t handle(void) { \
        static volatile fobj_method_handle_t hndl = 0; \
        fobj_method_handle_t h = hndl; \
        if (ft_likely(h)) return h; \
        fobj_method_init_impl(&hndl, fm_str(meth)); \
        return hndl; \
    } \
    \
    typedef res (* impl_meth_t)(fobj_t self comma() fobj__mapArgs_toArgs(__VA_ARGS__)); \
    \
    ft_inline void \
    register_meth(fobj_klass_handle_t klass, impl_meth_t cb) { \
        fobj_method_register_impl(klass, handle(), (void *)cb); \
    } \
    \
    ft_inline fobj__method_impl_box_t \
    wrap_decl(impl_meth_t cb) { \
        return (fobj__method_impl_box_t) { handle(), cb }; \
    }

#define fobj__mapArgs_toArgs(...) \
    fm_eval_tuples_comma(fobj__mapArgs_toArgs_do, __VA_ARGS__)
#define fobj__mapArgs_toArgs_do(x, y, ...) x y

#define fobj__mapArgs_toFields(...) \
    fm_eval_tuples(fobj__mapArgs_toFields_do, __VA_ARGS__)
#define fobj__mapArgs_toFields_do(x, y, ...)  \
    x y; \
    fobj__missing_argument_detector fobj__nm_given(y);

#define fobj__mapArgs_toNames(...) \
    fm_eval_tuples_comma(fobj__mapArgs_toNames_do, __VA_ARGS__)
#define fobj__mapArgs_toNames_do(x, y, ...) y

#define fobj__mapArgs_toNamedParams(params, ...) \
    fm_eval_tuples_arg_comma(fobj__mapArgs_toNamedParams_do, params, __VA_ARGS__)
#define fobj__mapArgs_toNamedParams_do(params, x, y, ...) params.y

#define fobj__params_defaultsEach(params, ...) \
	fm_eval_tuples_arg(fobj__params_defaultsEach_do, params, __VA_ARGS__)
#define fobj__params_defaultsEach_do(params, x, y, ...) \
	if (!fobj__check_arg(params.y)) { \
		fm_if(fm_is_empty(__VA_ARGS__), \
				fobj__validate_arg(__FILE__, __LINE__, #y), \
				params.y = __VA_ARGS__); \
	}

/* Klass declarations */

#define fobj__klass_declare(klass) \
    extern fobj_klass_handle_t fobj__nm_khandle(klass)(void) ft_gcc_const; \
    fm__dumb_require_semicolon

#define fobj__klass_handle(klass, ...) \
    fobj__klass_handle_i(klass, \
            fobj__map_params(fobj__nm_kls(klass) fm_when_isnt_empty(__VA_ARGS__)(fm__comma __VA_ARGS__)))
#define fobj__klass_handle_i(klass, ...) \
    fobj__klass_handle_impl(klass, __VA_ARGS__)
#define fobj__klass_handle_impl(klass, ...) \
    fobj_klass_handle_t fobj__nm_khandle(klass) (void) { \
        static volatile fobj_klass_handle_t hndl = 0; \
        fobj_klass_handle_t khandle = hndl; \
        fobj_klass_handle_t kparent = fobjBase__kh(); \
        ssize_t kls_size = sizeof(klass); \
        if (khandle) return khandle; \
        fm_eval_tuples_arg(fobj__klass_detect_size, klass, __VA_ARGS__) \
        { \
            fobj__method_impl_box_t methods[] = { \
                fobj__klass_decl_methods(klass, __VA_ARGS__) \
                { 0, NULL } \
            }; \
            if (fobj_klass_init_impl(&hndl, kls_size, kparent, methods, fm_str(klass))) \
                return hndl; \
        } \
        khandle = hndl; \
        fobj__klass_check_ifaces(klass, __VA_ARGS__) \
        return khandle; \
    }

#define fobj__klass_detect_size(klass, tag, ...) \
    fobj__klass_detect_size_##tag (klass, __VA_ARGS__)
#define fobj__klass_detect_size_inherits(klass, parent) \
    kparent = fobj__nm_khandle(parent)();
#define fobj__klass_detect_size_varsized(klass, ...) \
    fm_iif(fm_va_01(__VA_ARGS__))                    \
        ( kls_size = -1-offsetof(klass,fm_head(__VA_ARGS__)); ) \
        ( kls_size = -1-sizeof(klass); )
#define fobj__klass_detect_size_mth(...)
#define fobj__klass_detect_size_iface(...)

#define fobj__klass_decl_methods(klass, ...) \
    fm_eval_foreach_arg(fobj__klass_decl_method, klass, fobj__flat_methods(__VA_ARGS__))
#define fobj__klass_decl_method(klass, meth) \
    fobj__nm_wrap_decl(meth)(fobj__nm_klass_meth(klass, meth)),

#define fobj__klass_check_ifaces(klass, ...) \
    fm_eval_foreach_arg(fobj__klass_check_iface, klass, fobj__flat_ifaces(__VA_ARGS__))
#define fobj__klass_check_iface(klass, iface) \
    fobj__nm_kvalidate(iface)(khandle);

#define fobj__method_init(meth) \
    fobj__consume(fobj__nm_mhandle(meth)())
#define fobj__klass_init(klass) \
    fobj__consume(fobj__nm_khandle(klass)())

/* add methods after class declaration */

#define fobj__add_methods(klass, ...) do { \
    fobj_klass_handle_t khandle = fobj__nm_khandle(klass)(); \
    fm_eval_foreach_arg(fobj__add_methods_do, klass, __VA_ARGS__) \
} while (0)
#define fobj__add_methods_do(klass, meth) \
    fobj__nm_register(meth)(khandle, fobj__nm_klass_meth(klass, meth));

/* Instance creation */
#define fobj__alloc(klass, ...) \
    fm_cat(fobj__alloc_, fm_va_01(__VA_ARGS__))(klass, fobj__nm_khandle(klass), -1, __VA_ARGS__)
#define fobj__alloc_sized(klass, size, ...) \
    fm_cat(fobj__alloc_, fm_va_01(__VA_ARGS__))(\
            klass, fobj__nm_khandle(klass),  (size), __VA_ARGS__)
#define fobj__alloc_0(klass, khandle, size, ...) \
    ((klass *)fobj__allocate(khandle(), NULL, size))
#define fobj__alloc_1(klass, khandle, size, ...) \
    ((klass *)fobj__allocate(khandle(), &(klass){__VA_ARGS__}, size))

/* Interface declaration */

#define fobj__iface_declare(iface) \
    fobj__iface_declare_i(iface, fobj__map_params(fobj__nm_iface(iface), iface__fobj)) \
    fm__dumb_require_semicolon

#define fobj__iface_declare_i(iface, ...) \
    fobj__iface_declare_impl(iface, \
            fobj__nm_iface_i(iface), fobj__nm_bind(iface), \
            fobj__nm_bindref(iface), fobj__nm_implements(iface), \
            fobj__nm_kvalidate(iface), (fobj__flat_methods(__VA_ARGS__)))

#define fobj__iface_declare_impl(iface, iface_i, \
                                bind_iface, bindref_iface, implements_iface, \
                                kvalidate, methods)                          \
    fobj__mapMethods_toHandlers methods \
    typedef union iface_i { \
        fobj_t self; \
        fobj__mapMethods_toFields methods \
    } iface_i; \
    \
    ft_inline iface_i \
    bind_iface(fobj_t self) { \
        iface_i _iface = (iface_i){ .self = self }; \
        fobj__mapMethods_toSetters methods \
        return _iface; \
    } \
    \
    ft_inline bool \
    implements_iface(fobj_t self, iface_i *ifacep) { \
        iface_i _iface = (iface_i){ .self = self }; \
        bool    all_ok = true; \
        fobj__mapMethods_toIfSetters methods \
        if (ifacep != NULL) \
            *ifacep = all_ok ? _iface : (iface_i){NULL}; \
        return all_ok; \
    } \
    \
    ft_inline iface_i \
    bindref_iface(fobj_t self) { \
        iface_i _iface = bind_iface(self); \
        fobj_ref(_iface.self); \
        return _iface; \
    } \
    \
    ft_inline void \
    kvalidate(fobj_klass_handle_t khandle) { \
        fobj__kvalidateMethods methods \
    }

#define fobj__mapMethods_toHandlers(...) \
    fm_eval_foreach(fobj__mapMethods_toHandlers_do, __VA_ARGS__)
#define fobj__mapMethods_toHandlers_do(m) \
    fobj__predefine_method(m);

#define fobj__mapMethods_toFields(...) \
    fm_eval_foreach(fobj__mapMethods_toFields_do, __VA_ARGS__)
#define fobj__mapMethods_toFields_do(m) \
    uintptr_t fobj__nm_has(m);

#define fobj__mapMethods_toSetters(...) \
    fm_eval_foreach(fobj__mapMethods_toSetters_do, __VA_ARGS__)
#define fobj__mapMethods_toSetters_do(meth) \
    ft_assert(fobj_method_implements(self, fobj__nm_mhandle(meth)()));

#define fobj__mapMethods_toIfSetters(...) \
    fm_eval_foreach(fobj__mapMethods_toIfSetters_do, __VA_ARGS__)
#define fobj__mapMethods_toIfSetters_do(meth) \
    all_ok &= fobj_method_implements(self, fobj__nm_mhandle(meth)());

#define fobj__kvalidateMethods(...) \
    fm_eval_foreach(fobj__kvalidateMethods_do, __VA_ARGS__)
#define fobj__kvalidateMethods_do(meth) \
    ft_assert(fobj_klass_method_search(khandle, fobj__nm_mhandle(meth)()) != NULL);

#ifndef NDEBUG
#define fobj_reduce(newifacetype, oldiface) ({ \
    if (0) {                                       \
		__typeof(oldiface) _old_iface_ ft_unused = {NULL}; \
		fobj__nm_iface_i(newifacetype) _new_iface_ ft_unused = {NULL}; \
		fobj__mapMethods_toCopyChecks(newifacetype) \
    }                                \
	((fobj__nm_iface_i(newifacetype)){.self = (oldiface).self}); \
})
#else
#define fobj_reduce(newifacetype, oldiface) \
	((fobj__nm_iface_i(newifacetype)){.self = (oldiface).self})
#endif

#define fobj__mapMethods_toCopyChecks(iface) \
	fobj__mapMethods_toCopyChecks_i(    \
		fm_iif(fobj__macroIsIface(iface)) \
			(fobj__map_params(fobj__nm_iface(iface))) \
			((mth, iface)))
#define fobj__mapMethods_toCopyChecks_i(...) \
    fm_eval_foreach(fobj__mapMethods_toCopyChecks_do, fobj__flat_methods(__VA_ARGS__))
#define fobj__mapMethods_toCopyChecks_do(meth) \
    _new_iface_.fobj__nm_has(meth) = _old_iface_.fobj__nm_has(meth);

#define fobj__macroIsIface(iface) \
	fm_is_empty(fobj__macroIsIface_i(fobj__nm_iface(iface)))
#define fobj__macroIsIface_i(...) \
	fm_eval_foreach(fobj__macroIsIface_do, __VA_ARGS__)
#define fobj__macroIsIface_do(x) \
    fobj__macroIsIface_##x
#define fobj__macroIsIface_mth(...)

/* Method invocation */

#define fobj_call(meth, self, ...) fobj__call_1(meth, self, fobj_self_klass, fm_uniq(params), __VA_ARGS__)
#define fobj__call_1(meth, self, parent, params, ...) ({\
	ft_unused fobj__nm_params_t(meth) params = fobj_pass_params(meth, __VA_ARGS__); \
	fobj__call_2(meth, (self), parent, params, fobj__nm_mth(meth)) \
	})
#define fobj__call_2(meth, self, parent, params, ...) \
	fobj__call_3(meth, self, parent, params, __VA_ARGS__)
#define fobj__call_3(meth, self, parent, params, res, ...) \
	fobj__params_defaultsEach(params, __VA_ARGS__);        \
	fobj__nm_do(meth)(self, parent fm_va_comma(__VA_ARGS__) fobj__mapArgs_toNamedParams(params, __VA_ARGS__));

#define fobj_call_super(meth, _klassh, self, ...) \
    fobj__call_1(meth, self, _klassh, fm_uniq(params), __VA_ARGS__)

#define fobj_iface_call(meth, iface, ...) \
    fobj_call(meth, (fobj_t)(iface).fobj__nm_has(meth), __VA_ARGS__)

#define fobj_cb_fastcall(cb, ...) \
    (cb).impl((cb).self, __VA_ARGS__)

#define fobj__implements(iface, self, ...) \
    (fobj__nm_implements(iface)(self, fm_if(fm_no_va(__VA_ARGS__), NULL, __VA_ARGS__)))

#define fobj_iface_filled(meth, iface) \
    (fobj__nm_implements(meth)((fobj_t)(iface).fobj__nm_has(meth), NULL))

#define fobj_ifdef(assignment, meth, self, ...) \
    fobj__ifdef_impl(assignment, meth, (self), \
            fm_uniq(cb), fm_uniq(_self), fobj__nm_cb(meth), \
			fobj__nm_cb_t(meth), __VA_ARGS__)
#define fobj__ifdef_impl(assignment, meth, self_, cb, self, \
                        cb_meth, cb_meth_t, ...) ({ \
            fobj_t self = (self_);                                                             \
            cb_meth_t cb = cb_meth(self, fobj_self_klass, false); \
            if (cb.impl != NULL) { \
                assignment fobj_call(meth, self, __VA_ARGS__); \
            } \
            cb.impl != NULL; \
            })

/* Named params passing hazzles with optional and defaults */

#define fobj_pass_params(meth, ...) \
    ((fobj__nm_params_t(meth)){fm_eval_foreach_comma(fobj__pass_params_each, __VA_ARGS__)})

#define fobj__pass_params_each(param) \
    param, fobj__dumb_arg

#define fobj_bind(iface, obj)    fobj__nm_bind(iface)(obj)

/* Declarations "private" implementation functions */
extern bool fobj_method_init_impl(volatile fobj_method_handle_t *meth,
                                  const char *name);
extern void fobj_method_register_impl(fobj_klass_handle_t klass,
                                      fobj_method_handle_t meth,
                                      void* impl);
extern bool fobj_klass_init_impl(volatile fobj_klass_handle_t *klass,
                                 ssize_t size,
                                 fobj_klass_handle_t parent,
                                 fobj__method_impl_box_t *methods,
                                 const char *name);
extern void* fobj__allocate(fobj_klass_handle_t klass,
                            void *init,
                            ssize_t size);

/* helper function to consume value to disable compiler optimizations */
extern void fobj__consume(uint16_t);

typedef struct fobj__method_callback {
    fobj_t  self;
    void*   impl;
} fobj__method_callback_t;
extern fobj__method_callback_t fobj_method_search(fobj_t self,
                                                  fobj_method_handle_t meth,
                                                  fobj_klass_handle_t for_child_take_parent,
                                                  bool validate);

extern bool fobj_method_implements(fobj_t self,
                                   fobj_method_handle_t meth);

extern void* fobj_klass_method_search(fobj_klass_handle_t klass,
                                      fobj_method_handle_t meth);

extern _Noreturn
#if __OPTIMIZE__ || defined(__clang__)
__attribute__((error("missing argument")))
#endif
void fobj__validate_arg(const char* file, int line, const char *arg);

/* Variable set helpers */

#ifndef NDEBUG
#define fobj__set_impl(ptr, obj) do { \
    __typeof(&(**ptr)) fm_uniq(_validate_ptrptr_) ft_unused = NULL; \
    fobj_set((void**)(ptr), (obj)); \
} while(0)
#define fobj__swap_impl(ptr, obj) ({ \
    __typeof(&(**ptr)) fm_uniq(_validate_ptrptr_) ft_unused = NULL; \
    fobj_swap((void**)(ptr), (obj)); \
})
#define fobj__del_impl(ptr) do { \
    __typeof(&(**ptr)) fm_uniq(_validate_ptrptr_) ft_unused = NULL; \
    fobj_del((void**)(ptr)); \
} while (0)
#else
#define fobj__set_impl(ptr, obj)    fobj_set((void**)(ptr), (obj))
#define fobj__swap_impl(ptr, obj)   fobj_swap((void**)(ptr), (obj))
#define fobj__del_impl(ptr)         fobj_del((void**)(ptr))
#endif

#define fobj__iref(iface)	({__typeof(iface) t = (iface); t.self=fobj_ref(t.self); t;})
#define fobj__iunref(iface)	({__typeof(iface) t = (iface); t.self=fobj_unref(t.self); t;})
#ifndef NDEBUG
#define fobj__iset(ptr, iface) do { \
	__typeof(*(ptr)) fm_uniq(_validate_ptr_) ft_unused = (__typeof(iface)){}; \
	fobj_set(&(ptr)->self, (iface).self);                                    \
} while (0)
#define fobj__iswap(ptr, iface) ({ \
	__typeof(*(ptr)) fm_uniq(_validate_ptr_) ft_unused = (__typeof(iface)){}; \
	(__typeof(iface)){.self=fobj_swap(&(ptr)->self, (iface).self)};           \
})
#else
#define fobj__iset(ptr, iface) \
	fobj_set(&(ptr)->self, (iface).self)
#define fobj__iswap(ptr, iface) \
	((__typeof(iface)){.self=fobj_swap(&(ptr)->self, (iface).self)})
#endif
#define fobj__idel(iface)      fobj_del((void*)&(iface)->self)

#define fobj__isave(iface)       ({__typeof(iface) t=(iface); $save(t.self); t;})
#define fobj__iresult(iface)     ({__typeof(iface) t=(iface); $result(t.self); t;})
#define fobj__ireturn(iface)     return $iresult(iface)

/* Autorelease pool handling */

#define FOBJ_AR_CHUNK_SIZE 14
typedef struct fobj_autorelease_chunk fobj_autorelease_chunk;
struct fobj_autorelease_chunk {
    fobj_autorelease_chunk  *prev;
    uint32_t    cnt;
    fobj_t      refs[FOBJ_AR_CHUNK_SIZE];
};
typedef struct fobj__autorelease_pool_ref fobj__autorelease_pool_ref;
typedef struct fobj_autorelease_pool fobj_autorelease_pool;
struct fobj__autorelease_pool_ref {
    fobj_autorelease_pool  *parent;
    fobj_autorelease_pool **root;
};
struct fobj_autorelease_pool {
    struct fobj__autorelease_pool_ref ref;
    fobj_autorelease_chunk *last;
    fobj_autorelease_chunk  first;
};

extern fobj__autorelease_pool_ref fobj_autorelease_pool_init(fobj_autorelease_pool *pool);
extern void fobj_autorelease_pool_release(fobj_autorelease_pool *pool);
extern fobj_t fobj_store_to_parent_pool(fobj_t obj,
        fobj_autorelease_pool *child_pool_or_null);

#define FOBJ_ARP_POOL(name) \
    fobj_autorelease_pool __attribute__((cleanup(fobj_autorelease_pool_release))) \
    name = {fobj_autorelease_pool_init(&name), &name.first}


/********************************
 * ERROR
 */
typedef struct fobj_err_kv {
    const char *key;
    ft_arg_t    val;
} fobj_err_kv_t;

#define fobj__error_kind(err) \
    ft_inline const char* fobj_error_kind_##err(void) { return #err; }

#define fobj__error_flag_key(key) \
    ft_inline fobj_err_kv_t fobj__err_mkkv_##key(void) { \
        return (fobj_err_kv_t){#key, ft_mka_z()};            \
    }                                                          \
    ft_inline bool fobj__err_getkv_##key(err_i err, bool *found) { \
        bool fnd;                          \
        fobj_err_getkv(err, #key, ft_mka_z(), &fnd));    \
        if (found) *found = fnd;  \
        return fnd;               \
    }

#define fobj__error_int_key(key) \
    ft_inline fobj_err_kv_t fobj__err_mkkv_##key(int64_t v) { \
        return (fobj_err_kv_t){#key, ft_mka_i(v)};            \
    }                                                          \
    ft_inline int64_t fobj__err_getkv_##key(err_i err, bool *found) { \
        return ft_arg_i(fobj_err_getkv(err, #key, ft_mka_i(0), found));   \
    }

#define fobj__error_uint_key(key) \
    ft_inline fobj_err_kv_t fobj__err_mkkv_##key(uint64_t v) { \
        return (fobj_err_kv_t){#key, ft_mka_u(v)};             \
    }                                                           \
    ft_inline uint64_t fobj__err_getkv_##key(err_i err, bool *found) { \
        return ft_arg_u(fobj_err_getkv(err, #key, ft_mka_u(0), found));   \
    }

#define fobj__error_cstr_key(key) \
    ft_inline fobj_err_kv_t fobj__err_mkkv_##key(const char* v) { \
        return (fobj_err_kv_t){#key, ft_mka_s((char*)v)};         \
    }                                                              \
    ft_inline const char* fobj__err_getkv_##key(err_i err, bool *found) { \
        return ft_arg_s(fobj_err_getkv(err, #key, ft_mka_s(NULL), found));    \
    }

#define fobj__error_float_key(key) \
    ft_inline fobj_err_kv_t fobj__err_mkkv_##key(double v) { \
        return (fobj_err_kv_t){#key, ft_mka_f(v)};           \
    }                                                         \
    ft_inline double fobj__err_getkv_##key(err_i err, bool *found) { \
        return ft_arg_f(fobj_err_getkv(err, #key, ft_mka_f(0), found));  \
    }

#define fobj__error_bool_key(key) \
    ft_inline fobj_err_kv_t fobj__err_mkkv_##key(bool v) { \
        return (fobj_err_kv_t){#key, ft_mka_b(v)};         \
    }                                                       \
    ft_inline bool fobj__err_getkv_##key(err_i err, bool *found) {      \
        return ft_arg_b(fobj_err_getkv(err, #key, ft_mka_b(false), found)); \
    }

#define fobj__error_object_key(key) \
    ft_inline fobj_err_kv_t fobj__err_mkkv_##key(fobj_t v) { \
        return (fobj_err_kv_t){#key, ft_mka_o(v)};             \
    }                                                           \
    ft_inline fobj_t fobj__err_getkv_##key(err_i err, bool *found) { \
        return ft_arg_o(fobj_err_getkv(err, #key, ft_mka_o(NULL), found)); \
    }

#endif
