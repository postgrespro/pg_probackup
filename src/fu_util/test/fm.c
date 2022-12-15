#include <fm_util.h>

#ifndef __TINYC__
  #define AssertEq(x, v, name) _Static_assert(x == v, name)
  #define AssertEqStr(x, str, name) _Static_assert(__builtin_strcmp(x, str) == 0, name)
  #define AssertNqStr(x, str, name) _Static_assert(__builtin_strcmp(x, str) != 0, name)
#else
  #ifdef NDEBUG
    #undef NDEBUG
  #endif
  #include <assert.h>
  #include <string.h>
  #define AssertEq(x, v, name) assert(x == v)
  #define AssertEqStr(x, str, name) assert(strcmp(x, str) == 0)
  #define AssertNqStr(x, str, name) assert(strcmp(x, str) != 0)
#endif

int main(void) {

#define asdfhjkl 99
AssertEq(fm_cat(asdf,hjkl), 99, "fm_cat");
AssertEq(fm_cat(fm_cat(as,df),fm_cat(hj,kl)), 99, "fm_cat(fm_cat)");
AssertEq(fm_cat3(as,dfhj,kl), 99, "fm_cat3");
AssertEq(fm_cat4(as,df,hj,kl), 99, "fm_cat4");

AssertEqStr(fm_str(1), "1", "fm_str");
AssertEqStr(fm_str(1,2), "1,2", "fm_str");
AssertEqStr(fm_str(1, 2), "1, 2", "fm_str");
AssertEqStr(fm_str(1,  2), "1, 2", "fm_str");
AssertEqStr(fm_str(1 , 2), "1 , 2", "fm_str");
AssertEqStr(fm_str(1, 2 ), "1, 2", "fm_str");
AssertEqStr(fm_str(fm_cat(1,2)), "12", "fm_str");
AssertEqStr(fm_str(fm_cat(1, 2 )), "12", "fm_str");
AssertEqStr(fm_str(fm_cat(x, y )), "xy", "fm_str");
AssertEqStr(fm_str(fm_cat(x , y )), "xy", "fm_str");
AssertEqStr(fm_str(fm_cat3(1,2,3)), "123", "fm_str");
AssertEqStr(fm_str(fm_cat3(1, 2 , 3)), "123", "fm_str");
AssertEqStr(fm_str(fm_cat3(x, y , z)), "xyz", "fm_str");
AssertEqStr(fm_str(fm_cat3(x , y ,z)), "xyz", "fm_str");

AssertNqStr(fm_str(fm_uniq(x)), fm_str(fm_uniq(x)), "fm_uniq");

AssertEqStr(fm_str(fm_expand()), "", "fm_expand");
AssertEqStr(fm_str(fm_expand(1)), "1", "fm_expand");
AssertEqStr(fm_str(fm_expand( 1 )), "1", "fm_expand");
AssertEqStr(fm_str( fm_expand( 1 ) ), "1", "fm_expand");
AssertEqStr(fm_str(fm_expand(1,2)), "1,2", "fm_expand");
AssertEqStr(fm_str(fm_expand( 1 , 2 )), "1 , 2", "fm_expand");
AssertEqStr(fm_str( fm_expand( 1 , 2) ), "1 , 2", "fm_expand");

AssertEqStr(fm_str(fm_empty()), "", "fm_empty");
AssertEqStr(fm_str(fm_empty(1)), "", "fm_empty");
AssertEqStr(fm_str(fm_empty(1 , 3)), "", "fm_empty");

AssertEqStr(fm_str(fm_comma), "fm_comma", "fm_comma");
AssertEqStr(fm_str(fm_comma()), ",", "fm_comma");
AssertEqStr(fm_str(fm_comma(xx,xx)), ",", "fm_comma");
AssertEqStr(fm_str(fm__comma), ",", "fm_comma");

AssertEqStr(fm_str(fm_apply(fm_expand)), "", "fm_apply");
AssertEqStr(fm_str(fm_apply(fm_expand, 1)), "1", "fm_apply");
AssertEqStr(fm_str(fm_apply(fm_expand, 1, 2)), "1, 2", "fm_apply");
AssertEqStr(fm_str(fm_apply(fm_expand, 1,2)), "1,2", "fm_apply");
AssertEqStr(fm_str(fm_apply(fm_expand, 1 ,2)), "1 ,2", "fm_apply");
AssertEqStr(fm_str(fm_apply(fm_cat, 1 ,2)), "12", "fm_apply");
AssertEqStr(fm_str(fm_apply(fm_cat, 1, 2)), "12", "fm_apply");
AssertEqStr(fm_str(fm_apply(fm_cat3, x, y, z)), "xyz", "fm_apply");
AssertEqStr(fm_str(fm_apply(fm_comma, ())), ",", "fm_apply");

AssertEq(fm_compl(1), 0, "fm_compl");
AssertEq(fm_compl(0), 1, "fm_compl");
AssertEq(fm_compl(fm_true), 0, "fm_compl");
AssertEq(fm_compl(fm_false), 1, "fm_compl");

AssertEq(fm_and(0, 0), 0, "fm_and");
AssertEq(fm_and(0, 1), 0, "fm_and");
AssertEq(fm_and(1, 0), 0, "fm_and");
AssertEq(fm_and(1, 1), 1, "fm_and");
AssertEq(fm_and(fm_false, fm_false), fm_false, "fm_and");
AssertEq(fm_and(fm_false, fm_true), fm_false, "fm_and");
AssertEq(fm_and(fm_true, fm_false), fm_false, "fm_and");
AssertEq(fm_and(fm_true, fm_true), fm_true, "fm_and");

AssertEq(fm_or(0, 0), 0, "fm_or");
AssertEq(fm_or(0, 1), 1, "fm_or");
AssertEq(fm_or(1, 0), 1, "fm_or");
AssertEq(fm_or(1, 1), 1, "fm_or");
AssertEq(fm_or(fm_false, fm_false), fm_false, "fm_or");
AssertEq(fm_or(fm_false, fm_true), fm_true, "fm_or");
AssertEq(fm_or(fm_true, fm_false), fm_true, "fm_or");
AssertEq(fm_or(fm_true, fm_true), fm_true, "fm_or");

AssertEq(fm_nand(0, 0), 1, "fm_nand");
AssertEq(fm_nand(0, 1), 1, "fm_nand");
AssertEq(fm_nand(1, 0), 1, "fm_nand");
AssertEq(fm_nand(1, 1), 0, "fm_nand");
AssertEq(fm_nand(fm_false, fm_false), fm_true, "fm_nand");
AssertEq(fm_nand(fm_false, fm_true), fm_true, "fm_nand");
AssertEq(fm_nand(fm_true, fm_false), fm_true, "fm_nand");
AssertEq(fm_nand(fm_true, fm_true), fm_false, "fm_nand");

AssertEq(fm_nor(0, 0), 1, "fm_nor");
AssertEq(fm_nor(0, 1), 0, "fm_nor");
AssertEq(fm_nor(1, 0), 0, "fm_nor");
AssertEq(fm_nor(1, 1), 0, "fm_nor");
AssertEq(fm_nor(fm_false, fm_false), fm_true, "fm_nor");
AssertEq(fm_nor(fm_false, fm_true), fm_false, "fm_nor");
AssertEq(fm_nor(fm_true, fm_false), fm_false, "fm_nor");
AssertEq(fm_nor(fm_true, fm_true), fm_false, "fm_nor");

AssertEq(fm_xor(0, 0), 0, "fm_xor");
AssertEq(fm_xor(0, 1), 1, "fm_xor");
AssertEq(fm_xor(1, 0), 1, "fm_xor");
AssertEq(fm_xor(1, 1), 0, "fm_xor");
AssertEq(fm_xor(fm_false, fm_false), fm_false, "fm_xor");
AssertEq(fm_xor(fm_false, fm_true), fm_true, "fm_xor");
AssertEq(fm_xor(fm_true, fm_false), fm_true, "fm_xor");
AssertEq(fm_xor(fm_true, fm_true), fm_false, "fm_xor");

AssertEq(fm_if(fm_true, 3, 4), 3, "fm_if");
AssertEq(fm_if(fm_false, 3, 4), 4, "fm_if");
AssertEqStr(fm_str(fm_if(fm_false, 3, 4)), "4", "fm_if");
AssertEqStr(fm_str(fm_if(fm_false, 3, 4, 5)), "4, 5", "fm_if");

AssertEqStr(fm_str(fm_when(fm_true)(3, 4)), "3, 4", "fm_when");
AssertEqStr(fm_str(fm_when(fm_false)(3, 4)), "", "fm_when");

AssertEqStr(fm_str(fm_iif(fm_true)(3, 4)(5, 6)), "3, 4", "fm_iif");
AssertEqStr(fm_str(fm_iif(fm_false)(3, 4)(5, 6)), "5, 6", "fm_iif");

#define COMPARE_FOO(x) x
#define COMPARE_BAR(x) x
AssertEq(fm_equal(FOO, FOO), 1, "fm_equal");
AssertEq(fm_equal(BAR, BAR), 1, "fm_equal");
AssertEq(fm_equal(FOO, BAR), 0, "fm_equal");
AssertEq(fm_equal(BAR, FOO), 0, "fm_equal");
AssertEq(fm_equal(BAR, BAZ), 0, "fm_equal");
AssertEq(fm_equal(BAZ, BAR), 0, "fm_equal");
AssertEq(fm_equal(BAZ, BAD), 0, "fm_equal");

AssertEq(fm_not_equal(FOO, FOO), 0, "fm_not_equal");
AssertEq(fm_not_equal(BAR, BAR), 0, "fm_not_equal");
AssertEq(fm_not_equal(FOO, BAR), 1, "fm_not_equal");
AssertEq(fm_not_equal(BAR, FOO), 1, "fm_not_equal");
AssertEq(fm_not_equal(BAR, BAZ), 1, "fm_not_equal");
AssertEq(fm_not_equal(BAZ, BAR), 1, "fm_not_equal");
AssertEq(fm_not_equal(BAZ, BAD), 1, "fm_not_equal");

AssertEq(fm_head(2), 2, "fm_head");
AssertEq(fm_head(2, 3), 2, "fm_head");
AssertEq(fm_head(2, 3, 4), 2, "fm_head");
AssertEq(fm_head(2, 3, 4), 2, "fm_head");
AssertEqStr(fm_str(fm_head()), "", "fm_head");
AssertEqStr(fm_str(fm_head(, 1)), "", "fm_head");
AssertEqStr(fm_str(fm_head(fm__comma)), "", "fm_head");
AssertEqStr(fm_str(fm_head(fm__comma, 1)), "", "fm_head");
AssertEqStr(fm_str(fm_head(fm_comma(), 1)), "", "fm_head");
AssertEqStr(fm_str(fm_tail(2)), "", "fm_head");
AssertEqStr(fm_str(fm_tail(2, 3)), "3", "fm_head");
AssertEqStr(fm_str(fm_tail(2, 3, 4)), "3, 4", "fm_head");

AssertEq(fm_va_single(), 1, "fm_va_single");
AssertEq(fm_va_single(1), 1, "fm_va_single");
AssertEq(fm_va_single(,), 0, "fm_va_single");
AssertEq(fm_va_single(fm_expand()), 1, "fm_va_single");
AssertEq(fm_va_single(fm_expand(1)), 1, "fm_va_single");
AssertEq(fm_va_single(fm_expand(,)), 0, "fm_va_single");

AssertEq(fm_va_many(), 0, "fm_va_many");
AssertEq(fm_va_many(1), 0, "fm_va_many");
AssertEq(fm_va_many(,), 1, "fm_va_many");
AssertEq(fm_va_many(fm_expand()), 0, "fm_va_many");
AssertEq(fm_va_many(fm_expand(1)), 0, "fm_va_many");
AssertEq(fm_va_many(fm_expand(,)), 1, "fm_va_many");

AssertEq(fm_no_va(), 1, "fm_no_va");
AssertEq(fm_no_va(fm_empty()), 1, "fm_no_va");
AssertEq(fm_no_va(fm_empty(1)), 1, "fm_no_va");
AssertEq(fm_no_va(1), 0, "fm_no_va");
AssertEq(fm_no_va(,), 0, "fm_no_va");
AssertEq(fm_no_va(,1), 0, "fm_no_va");

AssertEq(fm_va_01(), 0, "fm_va_01");
AssertEq(fm_va_01(fm_empty()), 0, "fm_va_01");
AssertEq(fm_va_01(fm_empty(1)), 0, "fm_va_01");
AssertEq(fm_va_01(1), 1, "fm_va_01");
AssertEq(fm_va_01(,), 1, "fm_va_01");
AssertEq(fm_va_01(,1), 1, "fm_va_01");

AssertEq(fm_va_01n(), 0, "fm_va_01n");
AssertEq(fm_va_01n(fm_empty()), 0, "fm_va_01n");
AssertEq(fm_va_01n(x), 1, "fm_va_01n");
AssertEq(fm_va_01n(fm_cat(x, y)), 1, "fm_va_01n");
AssertEq(fm_va_01n(fm_head(x, y)), 1, "fm_va_01n");
AssertEq(fm_va_01n(fm_tail(x)), 0, "fm_va_01n");
AssertEq(fm_va_01n(fm_tail(x, y)), 1, "fm_va_01n");
AssertEqStr(fm_str(fm_va_01n(,)), "n", "fm_va_01n");
AssertEqStr(fm_str(fm_va_01n(x,)), "n", "fm_va_01n");
AssertEqStr(fm_str(fm_va_01n(1,2)), "n", "fm_va_01n");
AssertEqStr(fm_str(fm_va_01n(fm_tail(1,2,3))), "n", "fm_va_01n");

AssertEq(fm_or_default()(5), 5, "fm_or_default");
AssertEq(fm_or_default(4)(5), 4, "fm_or_default");
AssertEqStr(fm_str(fm_or_default()(5, 6)), "5, 6", "fm_or_default");
AssertEqStr(fm_str(fm_or_default(3, 4)(5, 6)), "3, 4", "fm_or_default");

AssertEqStr(fm_str(fm_when_isnt_empty()(5)), "", "fm_when_isnt_empty");
AssertEqStr(fm_str(fm_when_isnt_empty(fm_empty())(5)), "", "fm_when_isnt_empty");
AssertEqStr(fm_str(fm_when_isnt_empty(1)(5)), "5", "fm_when_isnt_empty");
AssertEqStr(fm_str(fm_when_isnt_empty(1)(5, 6)), "5, 6", "fm_when_isnt_empty");

AssertEq(fm_is_tuple(), 0, "fm_is_tuple");
AssertEq(fm_is_tuple(1), 0, "fm_is_tuple");
AssertEq(fm_is_tuple(1, 2), 0, "fm_is_tuple");
AssertEq(fm_is_tuple(,), 0, "fm_is_tuple");
AssertEq(fm_is_tuple(()), 1, "fm_is_tuple");
AssertEq(fm_is_tuple((,)), 1, "fm_is_tuple");
AssertEq(fm_is_tuple((fm_comma)), 1, "fm_is_tuple");

#define add_x(y) y##x
#define add_ax(a, y) y##a##x
AssertEqStr(fm_str(fm_eval_foreach(add_x)),
            "", "fm_eval_foreach");
AssertEqStr(fm_str(fm_eval_foreach(add_x, a)),
            "ax", "fm_eval_foreach");
AssertEqStr(fm_str(fm_eval_foreach(add_x, a, b)),
            "ax bx", "fm_eval_foreach");
AssertEqStr(fm_str(fm_eval_foreach(add_x, a, b, c)),
            "ax bx cx", "fm_eval_foreach");
AssertEqStr(fm_str(fm_eval_foreach(add_x, a, b, c, d)),
            "ax bx cx dx", "fm_eval_foreach");
AssertEqStr(fm_str(fm_eval_foreach(add_x, a, b, c, d, e)),
            "ax bx cx dx ex", "fm_eval_foreach");

AssertEqStr(fm_str(fm_eval_foreach_comma(add_x)),
            "", "fm_eval_foreach_comma");
AssertEqStr(fm_str(fm_eval_foreach_comma(add_x, a)),
            "ax", "fm_eval_foreach_comma");
AssertEqStr(fm_str(fm_eval_foreach_comma(add_x, a, b)),
            "ax , bx", "fm_eval_foreach_comma");
AssertEqStr(fm_str(fm_eval_foreach_comma(add_x, a, b, c)),
            "ax , bx , cx", "fm_eval_foreach_comma");
AssertEqStr(fm_str(fm_eval_foreach_comma(add_x, a, b, c, d)),
            "ax , bx , cx , dx", "fm_eval_foreach_comma");
AssertEqStr(fm_str(fm_eval_foreach_comma(add_x, a, b, c, d, e)),
            "ax , bx , cx , dx , ex", "fm_eval_foreach_comma");

AssertEqStr(fm_str(fm_eval_foreach_arg(add_ax, Z)),
            "", "fm_eval_foreach_arg");
AssertEqStr(fm_str(fm_eval_foreach_arg(add_ax, Z, a)),
            "aZx", "fm_eval_foreach_arg");
AssertEqStr(fm_str(fm_eval_foreach_arg(add_ax, Z, a, b)),
            "aZx bZx", "fm_eval_foreach_arg");
AssertEqStr(fm_str(fm_eval_foreach_arg(add_ax, Z, a, b, c)),
            "aZx bZx cZx", "fm_eval_foreach_arg");
AssertEqStr(fm_str(fm_eval_foreach_arg(add_ax, Z, a, b, c, d)),
            "aZx bZx cZx dZx", "fm_eval_foreach_arg");
AssertEqStr(fm_str(fm_eval_foreach_arg(add_ax, Z, a, b, c, d, e)),
            "aZx bZx cZx dZx eZx", "fm_eval_foreach_arg");

#define map_tuple(t, ...) map_tuple_##t(__VA_ARGS__)
#define map_tuple_k(x) fm_cat(x, K)
#define map_tuple_n(x, y) fm_cat3(x, y, N)

#define map_tuple_a(a, t, ...) map_tuple_a##t(a, __VA_ARGS__)
#define map_tuple_ak(a, x) fm_cat3(x, a, K)
#define map_tuple_an(a, x, y) fm_cat4(x, a, y, N)

AssertEqStr(fm_str(fm_eval_tuples(map_tuple)),
            "", "fm_eval_tuples");
AssertEqStr(fm_str(fm_eval_tuples(map_tuple, (k, a))),
            "aK", "fm_eval_tuples");
AssertEqStr(fm_str(fm_eval_tuples(map_tuple, (n, a, b))),
            "abN", "fm_eval_tuples");
AssertEqStr(fm_str(fm_eval_tuples(map_tuple, (k, a), (n, a, b))),
            "aK abN", "fm_eval_tuples");
AssertEqStr(fm_str(fm_eval_tuples(map_tuple, (k, a), (n, a, b), (k, a))),
            "aK abN aK", "fm_eval_tuples");
AssertEqStr(fm_str(fm_eval_tuples(map_tuple, (k, a), (n, a, b), (k, c))),
            "aK abN cK", "fm_eval_tuples");
AssertEqStr(fm_str(fm_eval_tuples(map_tuple, (k, a), (n, a, b), (k, c), (n, c, d))),
            "aK abN cK cdN", "fm_eval_tuples");
AssertEqStr(fm_str(fm_eval_tuples(map_tuple, (k, a), (n, a, b), (k, c), (n, c, d), (k, e))),
            "aK abN cK cdN eK", "fm_eval_tuples");

AssertEqStr(fm_str(fm_eval_tuples_comma(map_tuple)),
            "", "fm_eval_tuples_comma");
AssertEqStr(fm_str(fm_eval_tuples_comma(map_tuple, (k, a))),
            "aK", "fm_eval_tuples_comma");
AssertEqStr(fm_str(fm_eval_tuples_comma(map_tuple, (n, a, b))),
            "abN", "fm_eval_tuples_comma");
AssertEqStr(fm_str(fm_eval_tuples_comma(map_tuple, (k, a), (n, a, b))),
            "aK , abN", "fm_eval_tuples_comma");
AssertEqStr(fm_str(fm_eval_tuples_comma(map_tuple, (k, a), (n, a, b), (k, c))),
            "aK , abN , cK", "fm_eval_tuples_comma");
AssertEqStr(fm_str(fm_eval_tuples_comma(map_tuple, (k, a), (n, a, b), (k, c), (n, c, d))),
            "aK , abN , cK , cdN", "fm_eval_tuples_comma");
AssertEqStr(fm_str(fm_eval_tuples_comma(map_tuple, (k, a), (n, a, b), (k, c), (n, c, d), (k, e))),
            "aK , abN , cK , cdN , eK", "fm_eval_tuples_comma");

AssertEqStr(fm_str(fm_eval_tuples_arg(map_tuple_a, Y)),
            "", "fm_eval_tuples_arg");
AssertEqStr(fm_str(fm_eval_tuples_arg(map_tuple_a, Y, (k, a))),
            "aYK", "fm_eval_tuples_arg");
AssertEqStr(fm_str(fm_eval_tuples_arg(map_tuple_a, Y, (n, a, b))),
            "aYbN", "fm_eval_tuples_arg");
AssertEqStr(fm_str(fm_eval_tuples_arg(map_tuple_a, Y, (k, a), (n, a, b))),
            "aYK aYbN", "fm_eval_tuples_arg");
AssertEqStr(fm_str(fm_eval_tuples_arg(map_tuple_a, Y, (k, a), (n, a, b), (k, c))),
            "aYK aYbN cYK", "fm_eval_tuples_arg");
AssertEqStr(fm_str(fm_eval_tuples_arg(map_tuple_a, Y, (k, a), (n, a, b), (k, c), (n, c, d))),
            "aYK aYbN cYK cYdN", "fm_eval_tuples_arg");
AssertEqStr(fm_str(fm_eval_tuples_arg(map_tuple_a, Y, (k, a), (n, a, b), (k, c), (n, c, d), (k, e))),
            "aYK aYbN cYK cYdN eYK", "fm_eval_tuples_arg");

}