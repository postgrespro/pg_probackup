/* vim: set expandtab autoindent cindent ts=4 sw=4 sts=4 */
#include <stdlib.h>
#include <stdio.h>

#include <fo_obj.h>
#include <errno.h>

static int verbose = 0;
#define logf(...) ft_log(FT_DEBUG, __VA_ARGS__)

#define mth__ioRead   ssize_t, (void *, buf), (size_t, count)
#define mth__ioRead__optional() (count, 4)
#define mth__ioClose  int
#define mth__ioStatus int
#define mth__fobjGetError err_i
fobj_method(ioRead);
fobj_method(ioClose);
fobj_method(ioStatus);
fobj_method(fobjGetError);

#define iface__ioReader		mth(ioRead)
#define iface__ioReadCloser	iface__ioReader, mth(ioClose), opt(ioStatus)
#define	iface__obj
fobj_iface(ioReadCloser);
fobj_iface(ioReader);
fobj_iface(obj);


#define kls__Klass0   mth(fobjDispose), \
    iface__ioReader, mth(fobjGetError)
#define kls__KlassA   inherits(Klass0), \
    iface__ioReadCloser, \
    mth(ioStatus), iface(ioReadCloser, ioReader)

fobj_klass(Klass0);
fobj_klass(KlassA);

typedef struct Klass0 {
    int x;
} Klass0;


typedef struct KlassA {
    Klass0 p;
    size_t offset;
} KlassA;

static void
Klass0_fobjDispose(VSelf) {
    Self(Klass0);
    logf("{.x = %d}", self->x);
}

static ssize_t
Klass0_ioRead(VSelf, void *buf, size_t count) {
    Self(Klass0);
    logf("{.x = %d}, .count = %zd", self->x, count);
    self->x += 1;
    return count;
}

fobj_error_int_key(myx);
fobj_error_float_key(myy);

static err_i
Klass0_fobjGetError(VSelf) {
    Self(Klass0);
    return $err(RT, "WTF ERROR {myx:05d} {myy:9.4f}", (myx, self->x), (myy, 100.001));
}

static int
KlassA_ioClose(VSelf) {
    //Self(KlassA);
    return 0;
}

static ssize_t
KlassA_ioRead(VSelf, void *buf, size_t count) {
    Self(KlassA);
    logf("p{.offset = %zd}, .count = %zd",
            self->offset, count);
    self->offset += count;
    $super(ioRead, self, buf, count);
    return count;
}

static int
KlassA_ioStatus(VSelf) {
    Self(KlassA);
    logf("{.offset = %zd}", self->offset);
    return (int)self->offset;
}

static void
KlassA_fobjDispose(VSelf) {
    Self(KlassA);
    logf("{.offset = %zd}", self->offset);
}

fobj_klass_handle(KlassA, mth(fobjDispose), iface(obj));
fobj_klass_handle(Klass0);

int main(int argc, char** argv) {
    ft_init_log(NULL);
    fobj_init();

    FOBJ_FUNC_ARP();
    ft_assert(fobj__func_ar_pool.last != NULL);

    char b[1024];
    int benchmode = 0, benchcnt = 0;
    int i;

    verbose = atoi(getenv("VERBOSE") ?: "0");
    benchcnt = atoi(getenv("BENCHCNT") ?: "0");
    benchmode = atoi(getenv("BENCHMODE") ?: "0");

    if (verbose) {
        //ft_log_level_reset(FT_LOG);
        ft_log_level_set(__FILE__, FT_DEBUG);
    }

    fobj_klass_init(Klass0);
    fobj_klass_init(KlassA);

    fobj_freeze();

    KlassA *a = $alloc(KlassA, .offset = 1, .p.x = 2);
    logf("a=%s", fobjRepr(a)->ptr);

    logf("Before block 1 enter");
    {
        FOBJ_BLOCK_ARP();
        KlassA *d;
        fobj_t e;
        logf("Before block 2 enter");
        {
            FOBJ_BLOCK_ARP();
            KlassA *c = $alloc(KlassA, .p.x = 55555);
            d = $alloc(KlassA, .p.x = 12345);
            e = $alloc(KlassA, .p.x = 67890);
            $unref($ref(c)); /* incref and store in current ARP */
            $save(d); /* store in outter ARP */
            $ref(e);  /* explicit reference increment */
            logf("Before block 2 exits");
        }
        logf("After block 2 exited");
        /* $set is needed only if variable is explicitely managed with $ref/$del */
        $set(&e, $alloc(KlassA, .p.x = 67891));
		$swap(&e, $alloc(KlassA, .p.x = 78912));
		$del(&e); /* explicit reference decrement */
        logf("Before block 1 exits");
    }
    logf("After block 1 exited");

    ioRead_i aird = bind_ioRead(a);
    $i(ioRead, aird, b, 100);
    $i(ioRead, aird, b);
    // will fail in runtime with param.buf__given != NULL
    //$i(ioRead, aird, .count = 100);
    $i(ioRead, aird, .buf = b, .count = 100);
    $i(ioRead, aird, .buf = b);

    ioReader_i ard = bind_ioReader(a);
    $i(ioRead, ard, b, 100);
    $i(ioRead, ard, .buf = b, .count = 100);

	ard = $bind(ioReader, a);
	aird = $bind(ioRead, ard.self);
	aird = $reduce(ioRead, ard);
	ard = $reduce(ioReader, aird);

	ioReadCloser_i ardcl = bind_ioReadCloser(a);
	ardcl = $reduce(ioReadCloser, ardcl);
	ard = $reduce(ioReader, ardcl);
	aird = $reduce(ioRead, ardcl);

    ioRead(a, b, 100);
    $(ioRead, a, b, 100);
    $(ioRead, a, .buf = b, .count = 100);

    $(ioStatus, a);

    aird = (ioRead_i){NULL};
    ard = (ioReader_i){NULL};

    err_i err = $err(RT, "ha");

    ft_assert(!$implements(ioRead, err.self));
    ft_assert(!$implements(ioRead, err.self, &aird));
    ft_assert(!$ifilled(ioRead, aird));
    ft_assert(!$implements(ioReader, err.self));
    ft_assert(!$implements(ioReader, err.self, &ard));
    ft_assert(!$ifilled(ioRead, ard));

    ft_assert($implements(ioRead, a));
    ft_assert($implements(ioRead, a, &aird));
    ft_assert($ifilled(ioRead, aird));
    ft_assert($implements(ioReader, a));
    ft_assert($implements(ioReader, a, &ard));
    ft_assert($ifilled(ioRead, ard));

    i = ioStatus(a) - 1;
    ft_assert($ifdef(,ioStatus, a));
    ft_assert(i != ioStatus(a));
    ft_assert($ifdef(i =, ioStatus, a));
    ft_assert(i == ioStatus(a));
    ft_assert(!$ifdef(,fobjFormat, a));

    err = $(fobjGetError, a);
    logf("Error: %s", $errmsg(err));
    logf("Error: %s", $itostr(err, NULL)->ptr);
    logf("Error: %s", $itostr(err, "$T $M $K")->ptr);
    ioRead(a, b, strlen($errmsg(err)));
    $(ioRead, a, b, strlen($errmsg(err)));
    $(ioRead, a, b, $(ioRead, a, b, $(ioStatus, a)));
    logf("Error: %s", $errmsg($(fobjGetError, a)));

    errno = ENOENT;
    err = $syserr();
    logf("Error: %s", $errmsg(err));
    logf("Error: %s", $irepr(err)->ptr);
    errno = ENOENT;
    err = $syserr("Opening file");
    logf("Error: %s", $errmsg(err));
    logf("Error: %s", $irepr(err)->ptr);
    errno = ENOENT;
    err = $syserr("Opening file {path}", (path, "folder/read.me"));
    logf("Error: %s", $errmsg(err));
    logf("Error: %s", $irepr(err)->ptr);
    logf("Errno: %d", getErrno(err));

    Klass0 *k0 = $alloc(Klass0);
    aird = bind_ioRead(k0);
    ioRead__cb k0_ioRead = fetch_cb_ioRead(k0, fobj_self_klass);
    for (i = 0; i < benchcnt; i++) {
        switch (benchmode) {
            case 0: ioRead(k0, b, 100); break;
            case 1: $(ioRead, k0, b, 100); break;
            case 2: $i(ioRead, aird, b, 100); break;
            case 3: fobj_cb_fastcall(k0_ioRead, b, 100); break;
        }
    }

    $ref(a);
    { fobj_t b = a; $del(&b); }
    $(ioStatus, a);

	{
		ioRead_i bird = {NULL};
		$iset(&bird, aird);
		$iswap(&bird, aird);
		$iref(bird);
		$iunref(bird);
		$idel(&bird);
	}

    fobjStr *stra = $S("this is string a");
    fobjStr *strb = $S("this is b");

    ft_assert(fobj_streq_c(stra, "this is string a"));
    ft_assert(fobj_streq_c(strb, "this is b"));

    fobjStr *strc = fobj_strcatc(stra, "??????");
    fobjStr *strd = fobj_strcatc(strb, "!!");

    ft_assert(fobj_streq_c(strc, "this is string a??????"));
    ft_assert(fobj_streq_c(strd, "this is b!!"));

    fobjStr *stre = fobj_stradd(strc, strd);

    ft_assert(stre->len == strc->len + strd->len);
    ft_assert(fobj_streq_c(stre, "this is string a??????this is b!!"));

    stre = fobj_sprintf("%s:%d", "hello", 1);

    ft_assert(fobj_streq_c(stre, "hello:1"));

    stre = fobj_strcatf(stre, "/%d/%s", 100, "goodbye");

    ft_assert(fobj_streq_c(stre, "hello:1/100/goodbye"));

    fobjStr *strf = $fmt("Some {usual:8s} things cost > $${money:-8.4f}$$");
    ft_assert(fobj_streq_c(strf, "Some  things cost > $$$$"));
    strf = $fmt("Some {usual:8s} things cost > $${money:-8.4f}$$",
                (usual, $S("scary")), (money, $F(12.48)));
    ft_assert(fobj_streq_c(strf, "Some    scary things cost > $$12.4800 $$"),
              "String is '%s'", strf->ptr);

    logf("BEFORE EXIT");
}

ft_register_source();
