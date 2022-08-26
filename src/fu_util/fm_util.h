/* vim: set expandtab autoindent cindent ts=4 sw=4 sts=0 */
#ifndef FM_UTIL_H
#define FM_UTIL_H

#define fm_cat(x, y) fm__cat(x, y)
#define fm__cat(x, y) x##y
#define fm_cat3(x, y, z) fm__cat3(x, y, z)
#define fm__cat3(x, y, z) x##y##z
#define fm_cat4(w, x, y, z) fm__cat4(w, x, y, z)
#define fm__cat4(w, x, y, z) w##x##y##z
#define fm_str(...) fm__str(__VA_ARGS__)
#define fm__str(...) #__VA_ARGS__
#define fm_uniq(x) fm_cat(_##x##_, __COUNTER__)

#define fm_expand(...) __VA_ARGS__
#define fm_empty(...)

#define fm_comma(...) ,
#define fm__comma ,

#define fm_apply(macro, ...) \
    macro(__VA_ARGS__)

/****************************************/
// LOGIC

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
#define fm__if_1(y, ...) y
#define fm__if_0(y, ...) __VA_ARGS__
#define fm_when(x) fm_cat(fm__when_, x)
#define fm__when_1(...) __VA_ARGS__
#define fm__when_0(...)
#define fm_iif(x) fm_cat(fm__iif_, x)
#define fm__iif_1(...) __VA_ARGS__ fm_empty
#define fm__iif_0(...) fm_expand

/****************************************/
// COMPARISON

#define fm_equal(x, y) \
    fm_compl(fm_not_equal(x, y))
#define fm_not_equal(x, y) \
    fm_if(fm_and(fm__is_comparable(x),fm__is_comparable(y)), fm__primitive_compare, 1 fm_empty)(x, y)
#define fm__primitive_compare(x, y) fm_is_tuple(COMPARE_##x(COMPARE_##y)(()))
#define fm__is_comparable(x) fm_is_tuple(fm_cat(COMPARE_,x)(()))

/****************************************/
// __VA_ARGS__

#define fm_head(...) fm__head(__VA_ARGS__)
#define fm__head(x, ...) x
#define fm_tail(...) fm__tail(__VA_ARGS__)
#define fm__tail(x, ...) __VA_ARGS__

#define fm_or_default(...) \
    fm_iif(fm_va_01(__VA_ARGS__))(__VA_ARGS__)
#define fm_va_single(...) fm__va_single(__VA_ARGS__, fm__comma)
#define fm_va_many(...) fm__va_many(__VA_ARGS__, fm__comma)
#define fm__va_single(x, y, ...) fm__va_result(y, 1, 0)
#define fm__va_many(x, y, ...) fm__va_result(y, 0, 1)
#define fm__va_result(x, y, res, ...) res

#define fm_no_va fm_is_empty
#define fm_va_01 fm_isnt_empty
#define fm_va_01n(...) fm_cat3(fm__va_01n_, fm__isnt_empty(__VA_ARGS__), fm_va_many(__VA_ARGS__))
#define fm__va_01n_00 0
#define fm__va_01n_10 1
#define fm__va_01n_11 n

#if !__STRICT_ANSI__
#define fm_is_empty(...) fm__is_empty(__VA_ARGS__)
#define fm__is_empty(...) fm_va_single(~, ##__VA_ARGS__)
#define fm_isnt_empty(...) fm__isnt_empty(__VA_ARGS__)
#define fm__isnt_empty(...) fm_va_many(~, ##__VA_ARGS__)
#else
#define fm_is_empty(...) fm_and(fm__is_emptyfirst(__VA_ARGS__), fm_va_single(__VA_ARGS__))
#define fm_isnt_empty(...) fm_nand(fm__is_emptyfirst(__VA_ARGS__), fm_va_single(__VA_ARGS__))

#define fm__is_emptyfirst(x, ...) fm_iif(fm_is_tuple(x))(0)(fm__is_emptyfirst_impl(x))
#define fm__is_emptyfirst_impl(x,...) fm_tuple_2((\
            fm__is_emptyfirst_do1 x (fm__is_emptyfirst_do2), 1, 0))
#define fm__is_emptyfirst_do1(F) F()
#define fm__is_emptyfirst_do2(...) ,
#endif

#define fm_when_isnt_empty(...) fm_cat(fm__when_, fm__isnt_empty(__VA_ARGS__))
#define fm_va_comma(...) \
    fm_when_isnt_empty(__VA_ARGS__)(fm__comma)
#define fm_va_comma_fun(...) \
    fm_if(fm_va_01(__VA_ARGS__), fm_comma, fm_empty)


/****************************************/
// Tuples

#define fm_is_tuple(x, ...) fm__is_tuple_(fm__is_tuple_help x, 1, 0)
#define fm__is_tuple_choose(a,b,x,...) x
#define fm__is_tuple_help(...) ,
#define fm__is_tuple_(...) fm__is_tuple_choose(__VA_ARGS__)

#define fm_tuple_expand(x) fm_expand x
#define fm_tuple_tag(x) fm_head x
#define fm_tuple_data(x) fm_tail x
#define fm_tuple_0(x) fm_head x
#define fm_tuple_1(x) fm__tuple_1 x
#define fm__tuple_1(_0, _1, ...) _1
#define fm_tuple_2(x) fm__tuple_2 x
#define fm__tuple_2(_0, _1, _2, ...) _2

#define fm_tuple_tag_or_0(x) fm__tuple_tag_or_0_(fm__tuple_tag_or_0_help x, 0)
#define fm__tuple_tag_or_0_(...) fm__tuple_tag_or_0_choose(__VA_ARGS__)
#define fm__tuple_tag_or_0_choose(a,x,...) x
#define fm__tuple_tag_or_0_help(tag, ...) , tag

#define fm_dispatch_tag_or_0(prefix, x) \
    fm_cat(prefix, fm_tuple_tag_or_0(x))

/****************************************/
// Iteration

/* recursion engine */
#define fm_eval(...) fm__eval_0(__VA_ARGS__)
#ifdef FU_LONG_EVAL
#define fm__eval_0(...) fm__eval_1(fm__eval_1(fm__eval_1(fm__eval_1(__VA_ARGS__))))
#else
#define fm__eval_0(...) fm__eval_1(fm__eval_1(__VA_ARGS__))
#endif
#define fm__eval_1(...) fm__eval_2(fm__eval_2(__VA_ARGS__))
#define fm__eval_2(...) fm__eval_3(fm__eval_3(__VA_ARGS__))
#define fm__eval_3(...) __VA_ARGS__

// recursion handle : delay macro expansion to next recursion iteration
#define fm_recurs(id) id fm_empty fm_empty() ()
#define fm_recurs2(a,b) fm_cat fm_empty fm_empty() () (a,b)
#define fm_defer(id) id fm_empty()

#define fm_foreach_join(join, macro, ...) \
    fm_foreach_join_(fm_empty, join, macro, __VA_ARGS__)
#define fm_foreach_join_(join1, join2, macro, ...) \
    fm_cat(fm_foreach_join_, fm_va_01n(__VA_ARGS__))(join1, join2, macro, __VA_ARGS__)
#define fm_foreach_join_0(join1, join2, macro, ...)
#define fm_foreach_join_1(join1, join2, macro, x) \
    join1() macro(x)
#define fm_foreach_join_n(join1, join2, macro, x, y, ...) \
    join1() macro(x) \
    join2() macro(y) \
    fm_recurs2(fm_, foreach_join_) (join2, join2, macro, __VA_ARGS__)

#define fm_foreach(macro, ...) \
    fm_foreach_join(fm_empty, macro, __VA_ARGS__)
#define fm_foreach_comma(macro, ...) \
    fm_foreach_join(fm_comma, macro, __VA_ARGS__)

#define fm_foreach_arg_join(join, macro, arg, ...) \
    fm_foreach_arg_join_(fm_empty, join, macro, arg, __VA_ARGS__)
#define fm_foreach_arg_join_(join1, join2, macro, arg, ...) \
    fm_cat(fm_foreach_arg_join_, fm_va_01n(__VA_ARGS__))(join1, join2, macro, arg, __VA_ARGS__)
#define fm_foreach_arg_join_0(join1, join2, macro, ...)
#define fm_foreach_arg_join_1(join1, join2, macro, arg, x) \
    join1() macro(arg, x)
#define fm_foreach_arg_join_n(join1, join2, macro, arg, x, y, ...) \
    join1() macro(arg, x) \
    join2() macro(arg, y) \
    fm_recurs2(fm_, foreach_arg_join_) (join2, join2, macro, arg, __VA_ARGS__)

#define fm_foreach_arg(macro, arg, ...) \
    fm_foreach_arg_join(fm_empty, macro, arg, __VA_ARGS__)
#define fm_foreach_arg_comma(macro, arg, ...) \
    fm_foreach_arg_join(fm_comma, macro, arg, __VA_ARGS__)

#define fm_foreach_tuple_join(join, macro, ...) \
    fm_foreach_tuple_join_(fm_empty, join, macro, __VA_ARGS__)
#define fm_foreach_tuple_join_(join1, join2, macro, ...) \
    fm_cat(fm_foreach_tuple_join_, fm_va_01n(__VA_ARGS__))(join1, join2, macro, __VA_ARGS__)
#define fm_foreach_tuple_join_0(join1, join2, macro, ...)
#define fm_foreach_tuple_join_1(join1, join2, macro, x) \
    join1() macro x
#define fm_foreach_tuple_join_n(join1, join2, macro, x, y, ...) \
    join1() macro x  \
    join2() macro y  \
    fm_recurs2(fm_, foreach_tuple_join_) (join2, join2, macro, __VA_ARGS__)

#define fm_foreach_tuple(macro, ...) \
    fm_foreach_tuple_join(fm_empty, macro, __VA_ARGS__)
#define fm_foreach_tuple_comma(macro, ...) \
    fm_foreach_tuple_join(fm_comma, macro, __VA_ARGS__)

#define fm_foreach_tuple_arg_join(join, macro, arg, ...) \
    fm_foreach_tuple_arg_join_(fm_empty, join, macro, arg, __VA_ARGS__)
#define fm_foreach_tuple_arg_join_(join1, join2, macro, arg, ...) \
    fm_cat(fm_foreach_tuple_arg_join_, fm_va_01n(__VA_ARGS__))(join1, join2, macro, arg, __VA_ARGS__)
#define fm_foreach_tuple_arg_join_0(join1, join2, macro, ...)
#define fm_foreach_tuple_arg_join_1(join1, join2, macro, arg, x) \
    join1() fm_apply(macro, arg, fm_expand x)
#define fm_foreach_tuple_arg_join_n(join1, join2, macro, arg, x, y, ...) \
    join1() fm_apply(macro, arg, fm_expand x) \
    join2() fm_apply(macro, arg, fm_expand y) \
    fm_recurs2(fm_, foreach_tuple_arg_join_) (join2, join2, macro, arg, __VA_ARGS__)

#define fm_foreach_tuple_arg(macro, arg, ...) \
    fm_foreach_tuple_arg_join(fm_empty, macro, arg, __VA_ARGS__)
#define fm_foreach_tuple_arg_comma(macro, arg, ...) \
    fm_foreach_tuple_arg_join(fm_comma, macro, arg, __VA_ARGS__)

#define fm_eval_foreach(macro, ...) \
    fm_eval(fm_foreach(macro, __VA_ARGS__))

#define fm_eval_foreach_comma(macro, ...) \
    fm_eval(fm_foreach_comma(macro, __VA_ARGS__))

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
