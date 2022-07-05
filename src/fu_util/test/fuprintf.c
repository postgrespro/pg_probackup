/* vim: set expandtab autoindent cindent ts=4 sw=4 sts=4 */
#include <stdlib.h>
#include <stdio.h>
#include <ft_util.h>

int main(void) {
    ft_str_t msg = ft_asprintf("asdf %d asdf\n", 123456);
    const char *cmp = "asdf 123456 asdf\n";

    ft_assert(msg.ptr != NULL);
    ft_assert(msg.len == strlen(cmp));
    ft_assert(strcmp(msg.ptr, cmp) == 0);

    for (int i = 0; i < 10; i++) {
        ft_str_t newmsg = ft_asprintf("%s%s", msg.ptr, msg.ptr);
        ft_free((char*)msg.ptr);
        msg = newmsg;
        ft_assert(msg.ptr != NULL);
        ft_assert(msg.len == strlen(cmp) * (1 << (i+1)));
    }

    ft_free((char*)msg.ptr);

    return 0;
}

