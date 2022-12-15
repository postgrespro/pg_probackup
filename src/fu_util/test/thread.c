/* vim: set expandtab autoindent cindent ts=4 sw=4 sts=4 */
#include <stdlib.h>
#include <stdio.h>

#include <fo_obj.h>
#include <errno.h>

#include <pthread.h>


typedef struct FlagShip {
	bool	*flag;
} FlagShip;
#define kls__FlagShip mth(fobjDispose)
fobj_klass(FlagShip);

static bool theFlag1 = false;
static bool theFlag2 = false;
static bool theFlag3 = false;

static void
FlagShip_fobjDispose(VSelf)
{
	Self(FlagShip);
	*self->flag = true;
}

fobj_klass_handle(FlagShip);

static
int thr_func3(FlagShip *f)
{
	FOBJ_FUNC_ARP();
	$unref($ref(f));
	$alloc(FlagShip, .flag = &theFlag3);
	pthread_exit(NULL);
	return 1;
}

static
int thr_func2(FlagShip *f)
{
	FOBJ_FUNC_ARP();
	$unref($ref(f));
	return theFlag2 + thr_func3($alloc(FlagShip, .flag = &theFlag2));
}

static
void* thr_func1(void *arg)
{
	FOBJ_FUNC_ARP();
	printf("%d\n", theFlag1 + thr_func2($alloc(FlagShip, .flag = &theFlag1)));
	return NULL;
}

int
main(int argc, char** argv)
{
	pthread_t th;
	void* res;
	fobj_init();
	if (pthread_create(&th, NULL, thr_func1, NULL))
		ft_log(FT_FATAL, "Can't create\n");
	if (pthread_join(th, &res))
		ft_log(FT_FATAL, "Can't join\n");
	ft_assert(theFlag1);
	ft_assert(theFlag2);
	ft_assert(theFlag3);
}