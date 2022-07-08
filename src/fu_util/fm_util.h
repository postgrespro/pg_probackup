/* vim: set expandtab autoindent cindent ts=4 sw=4 sts=0 */
#ifndef FM_UTIL_H
#define FM_UTIL_H

#define fm_cat_impl(x, y) x##y
#define fm_cat(x, y) fm_cat_impl(x, y)
#define fm_cat3_impl(x, y, z) x##y##z
#define fm_cat3(x, y, z) fm_cat3_impl(x, y, z)
#define fm_cat4_impl(w, x, y, z) w##x##y##z
#define fm_cat4(w, x, y, z) fm_cat4_impl(w, x, y, z)
#define fm_str_impl(...) #__VA_ARGS__
#define fm_str(...) fm_str_impl(__VA_ARGS__)
#define fm_uniq(x) fm_cat(_##x##_, __COUNTER__)

#define fm_expand(...) __VA_ARGS__
#define fm_empty(...)

#define fm_compl(v) fm_cat(fm_compl_, v)
#define fm_compl_0 1
#define fm_compl_1 0
#define fm_and(x, y) fm_cat3(fm__and_, x, y)
#define fm__and_00 0
#define fm__and_01 0
#define fm__and_10 0
#define fm__and_11 1
#define fm_or(x, y) fm_cat3(fm__or_, x, y)
#define fm__or_00 0
#define fm__or_01 1
#define fm__or_10 1
#define fm__or_11 1
#define fm_nand(x, y) fm_cat3(fm__nand_, x, y)
#define fm__nand_00 1
#define fm__nand_01 1
#define fm__nand_10 1
#define fm__nand_11 0
#define fm_nor(x, y) fm_cat3(fm__nor_, x, y)
#define fm__nor_00 1
#define fm__nor_01 0
#define fm__nor_10 0
#define fm__nor_11 0
#define fm_xor(x, y) fm_cat3(fm__xor_, x, y)
#define fm__xor_00 0
#define fm__xor_01 1
#define fm__xor_10 1
#define fm__xor_11 0

#define fm_if(x, y, ...) fm_cat(fm__if_, x)(y, __VA_ARGS__)
#define fm_iif(x) fm_cat(fm__if_, x)
#define fm__if_1(y, ...) y
#define fm__if_0(y, ...) __VA_ARGS__
#define fm_when(x) fm_cat(fm__when_, x)
#define fm__when_1(...) __VA_ARGS__
#define fm__when_0(...)

#define fm_va_comma(...) \
    fm_cat(fm__va_comma_, fm_va_01(__VA_ARGS__))()
#define fm__va_comma_0()
#define fm__va_comma_1() ,

#define fm_or_default(...) \
    fm_cat(fm__or_default_, fm_va_01(__VA_ARGS__))(__VA_ARGS__)
#define fm__or_default_0(...) fm_expand
#define fm__or_default_1(...) __VA_ARGS__ fm_empty

#define fm__primitive_compare(x, y) fm_is_tuple(COMPARE_##x(COMPARE_##y)(()))
#define fm__is_comparable(x) fm_is_tuple(fm_cat(COMPARE_,x)(()))
#define fm_not_equal(x, y) \
    fm_iif(fm_and(fm__is_comparable(x),fm__is_comparable(y))) \
    (fm__primitive_compare, 1 fm_empty)(x, y)
#define fm_equal(x, y) \
    fm_compl(fm_not_equal(x, y))

#define fm_comma(...) ,
#define fm__comma ,
#define fm_va_single(...) fm__va_single(__VA_ARGS__, fm__comma)
#define fm_va_many(...) fm__va_many(__VA_ARGS__, fm__comma)
#define fm__va_single(x, y, ...) fm__va_result(y, 1, 0)
#define fm__va_many(x, y, ...) fm__va_result(y, 0, 1)
#define fm__va_result(x, y, res, ...) res

#if !__STRICT_ANSI__
#define fm_no_va(...) fm__no_va(__VA_ARGS__)
#define fm__no_va(...) fm_va_single(~, ##__VA_ARGS__)
#define fm_va_01(...) fm__va_01(__VA_ARGS__)
#define fm__va_01(...) fm_va_many(~, ##__VA_ARGS__)
#else
#define fm_no_va fm_is_empty
#define fm_va_01 fm_isnt_empty
#endif

#define fm__is_tuple_choose(a,b,x,...) x
#define fm__is_tuple_help(...) ,
#define fm__is_tuple_(...) fm__is_tuple_choose(__VA_ARGS__)
#define fm_is_tuple(x, ...) fm__is_tuple_(fm__is_tuple_help x, 1, 0)

#define fm_head(x, ...) x
#define fm_tail(x, ...) __VA_ARGS__

#define fm_apply_1(macro, x, ...) \
    macro(x)
#define fm_apply_2(macro, x, y, ...) \
    macro(x, y)
#define fm_apply_3(macro, x, y, z, ...) \
    macro(x, y, z)
#define fm_apply_tuple_1(macro, x, ...) \
    macro x
#define fm_apply_tuple_2(macro, x, y, ...) \
    fm__apply_tuple_2(macro, x, fm_expand y)
#define fm__apply_tuple_2(macro, x, ...) \
    macro(x, __VA_ARGS__)

#define fm_tuple_expand(x) fm_expand x
#define fm_tuple_tag(x) fm_head x
#define fm_tuple_data(x) fm_tail x
#define fm_tuple_0(x) fm_head x
#define fm_tuple_1(x) fm__tuple_1 x
#define fm__tuple_1(_0, _1, ...) _1
#define fm_tuple_2(x) fm__tuple_2 x
#define fm__tuple_2(_0, _1, _2, ...) _2

#define fm__tuple_tag_or_0_choose(a,x,...) x
#define fm__tuple_tag_or_0_help(tag, ...) , tag
#define fm__tuple_tag_or_0_(...) fm__tuple_tag_or_0_choose(__VA_ARGS__)
#define fm_tuple_tag_or_0(x) fm__tuple_tag_or_0_(fm__tuple_tag_or_0_help x, 0)

#define fm_dispatch_tag_or_0(prefix, x) \
    fm_cat(prefix, fm_tuple_tag_or_0(x))

#define fm_va_012(...) \
    fm_if(fm_no_va(__VA_ARGS__), 0, fm__va_12(__VA_ARGS__))
#define fm__va_12(...) \
    fm_if(fm_va_single(__VA_ARGS__), 1, 2)

// recursion handle
#define fm_defer(id) id fm_empty()
#define fm_recurs(id) id fm_defer(fm_empty)()

#if __STRICT_ANSI__
#define fm__is_emptyfirst(x, ...) fm_iif(fm_is_tuple(x))(0, fm__is_emptyfirst_impl(x))
#define fm__is_emptyfirst_impl(x,...) fm_tuple_2((\
            fm__is_emptyfirst_do1 x (fm__is_emptyfirst_do2), 1, 0))
#define fm__is_emptyfirst_do1(F) F()
#define fm__is_emptyfirst_do2(...) ,
#define fm_is_empty(...) fm_and(fm__is_emptyfirst(__VA_ARGS__), fm_va_single(__VA_ARGS__))
#define fm_isnt_empty(...) fm_nand(fm__is_emptyfirst(__VA_ARGS__), fm_va_single(__VA_ARGS__))
#else
#define fm_is_empty fm_no_va
#define fm_isnt_empty fm_va_01
#endif

#define fm_eval(...) fm__eval_0(__VA_ARGS__)
#ifdef FU_LONG_EVAL
#define fm__eval_0(...) fm__eval_1(fm__eval_1(fm__eval_1(fm__eval_1(__VA_ARGS__))))
#else
#define fm__eval_0(...) fm__eval_1(fm__eval_1(__VA_ARGS__))
#endif
#define fm__eval_1(...) fm__eval_2(fm__eval_2(__VA_ARGS__))
#define fm__eval_2(...) fm__eval_3(fm__eval_3(__VA_ARGS__))
#define fm__eval_3(...) __VA_ARGS__

#define fm_foreach(macro, ...) \
    fm_when(fm_va_01(__VA_ARGS__))( \
            fm_apply_1(macro, __VA_ARGS__) \
            fm_recurs(fm_cat) (fm_, foreach) (\
                macro, fm_tail(__VA_ARGS__) \
                ) \
            )

#define fm_foreach_arg(macro, arg, ...) \
    fm_when(fm_va_01(__VA_ARGS__))( \
            fm_apply_2(macro, arg, __VA_ARGS__) \
            fm_recurs(fm_cat) (fm_, foreach_arg) (\
                macro, arg, fm_tail(__VA_ARGS__) \
            ) \
        )

#define fm_catx(x, y) fm_cat_impl(x, y)
#define fm_foreach_comma(macro, ...) \
    fm_when(fm_va_01(__VA_ARGS__))( \
            fm_apply_1(macro, __VA_ARGS__\
            )fm_if(fm_va_single(__VA_ARGS__), , fm__comma)\
            fm_recurs(fm_catx) (fm_, foreach_comma) (\
                macro, fm_tail(__VA_ARGS__) \
            ) \
        )


#define fm_foreach_tuple(macro, ...) \
    fm_when(fm_va_01(__VA_ARGS__))( \
            fm_apply_tuple_1(macro, __VA_ARGS__) \
            fm_recurs(fm_cat) (fm_, foreach_tuple) (\
                macro, fm_tail(__VA_ARGS__) \
            ) \
        )

#define fm_foreach_tuple_arg(macro, arg, ...) \
    fm_when(fm_va_01(__VA_ARGS__))( \
            fm_apply_tuple_2(macro, arg, __VA_ARGS__) \
            fm_recurs(fm_cat) (fm_, foreach_tuple_arg) (\
                macro, arg, fm_tail(__VA_ARGS__) \
            ) \
        )

#define fm_foreach_tuple_comma(macro, ...) \
    fm_when(fm_va_01(__VA_ARGS__))( \
            fm_apply_tuple_1(macro, __VA_ARGS__\
            )fm_if(fm_va_single(__VA_ARGS__), fm_empty(), fm__comma)\
            fm_recurs(fm_cat) (fm_, foreach_tuple_comma) (\
                macro, fm_tail(__VA_ARGS__) \
            ) \
        )


#define fm_eval_foreach(macro, ...) \
        fm_eval(fm_foreach(macro, __VA_ARGS__))

#define fm_eval_foreach_arg(macro, arg, ...) \
        fm_eval(fm_foreach_arg(macro, arg, __VA_ARGS__))

#define fm_eval_tuples(macro, ...) \
        fm_eval(fm_foreach_tuple(macro, __VA_ARGS__))

#define fm_eval_tuples_arg(macro, arg, ...) \
        fm_eval(fm_foreach_tuple_arg(macro, arg, __VA_ARGS__))

#define fm_eval_tuples_comma(macro, ...) \
        fm_eval(fm_foreach_tuple_comma(macro, __VA_ARGS__))

#define fm__dumb_require_semicolon \
    struct __dumb_struct_declaration_for_semicolon

#endif
