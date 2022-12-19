#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <CUnit/Basic.h>

#include <pg_probackup.h>
#include <utils/file.h>

#include "pgunit.h"

pioDrive_i drive;
pioDBDrive_i dbdrive;
pioDrive_i cloud_drive;
pioDBDrive_i local_drive;

int should_be_remote;

void
init_test_drives()
{
	local_drive = pioDBDriveForLocation(FIO_LOCAL_HOST);
}

int
USE_LOCAL()
{
	drive = $reduce(pioDrive, local_drive);
	dbdrive = local_drive;
	should_be_remote = 0;
	printf("USE_LOCAL\n");
	return 0;
}

static int
clean_basic_suite()
{
	return 0;
}

#define FNAMES "abcdefghijklmnopqrstuvwxyz0123456789"

static int rand_init=0;
char *
random_path(void)
{
	char name[MAXPGPATH];

	if(!rand_init) {
		srand(time(NULL));
		rand_init = 1;
	}

	int len = 3 + rand() % 20;
	int fnlen = strlen(FNAMES);
	name[0]=0;
	snprintf(name, MAXPGPATH, "/tmp/%d_", getpid());
	int i;
	int l=strlen(name);
	for(i=l; i < len+l; ++i)
	{
		name[i] = FNAMES[rand()%fnlen];
	}
	name[i] = 0;

	return strdup(name);
}

char *
random_name(void)
{
	char name[MAXPGPATH];

	if(!rand_init) {
		srand(time(NULL));
		rand_init = 1;
	}

	int len = 3 + rand() % 10;
	int fnlen = strlen(FNAMES);
	int i;
	for(i=0;i<len; ++i)
	{
		name[i] = FNAMES[rand()%fnlen];
	}
	name[i] = 0;

	return strdup(name);
}

void
copy_file(const char *from, const char *to)
{
	int fdin = open(from, O_RDONLY|PG_BINARY);
	CU_ASSERT_FATAL(fdin>=0);
	int fdout = open(to, O_CREAT|O_RDWR|O_TRUNC, FILE_PERMISSION);
	CU_ASSERT_FATAL(fdout>=0);
	while(1)
	{
		char buf[BUFSZ];
		int rc = read(fdin, buf, BUFSZ);
		CU_ASSERT_FATAL(rc>=0);
		if(rc==0) break;
		int written = write(fdout, buf, rc);
		CU_ASSERT_FATAL(written == rc);
	}
	close(fdin);
	fsync(fdout);
	close(fdout);
}

void
init_fake_server(const char *path)
{
	char global[8192];
	snprintf(global, 8192, "%s/global", path);
	int rc = mkdir(path, DIR_PERMISSION);
	CU_ASSERT_FATAL(rc == 0);
	rc = mkdir(global, DIR_PERMISSION);
	CU_ASSERT_FATAL(rc == 0);
	char global2[MAXPGPATH];
	snprintf(global2, MAXPGPATH, "%s/pg_control", global);
	copy_file("pg_control.TEST", global2);
}

void
pbk_add_tests(int (*init)(void), const char *suite_name, PBK_test_description *tests)
{
	CU_pSuite pSuite;
	int i;

	pSuite = CU_add_suite(suite_name, init, clean_basic_suite);
	if(pSuite==NULL)
	{
		fprintf(stderr, "Can't add a suite %s\n", suite_name);
		CU_cleanup_registry();
		abort();
	}

	for(i = 0; tests[i].name; ++i)
	{
		if(CU_add_test(pSuite, tests[i].name, tests[i].foo) == NULL)
		{
			fprintf(stderr, "Can't add test %s.%s\n", suite_name, tests[i].name);
			CU_cleanup_registry();
			abort();
		}
	}
}

void
pio_write(pioDrive_i drive, path_t path, const char *data)
{
	FOBJ_FUNC_ARP();
	err_i err=$noerr();
	err = $i(pioWriteFile, drive, .path = path, .content = ft_bytes((char *)data, strlen(data)), .binary = true);
	CU_ASSERT(!$haserr(err));
}

bool
pio_exists(pioDrive_i drive, path_t path)
{
	FOBJ_FUNC_ARP();
	err_i err=$noerr();

	bool exists = $i(pioExists, drive, .path = path, .expected_kind = PIO_KIND_REGULAR, &err);
	if ($haserr(err))
		fprintf(stderr, "pio_exists: %s\n", $errmsg(err));
	CU_ASSERT(!$haserr(err));
	return exists;
}
bool
pio_exists_d(pioDrive_i drive, path_t path)
{
	FOBJ_FUNC_ARP();
	err_i err=$noerr();

	bool exists = $i(pioExists, drive, .path = path, .expected_kind = PIO_KIND_DIRECTORY, &err);
	CU_ASSERT(!$haserr(err));
	return exists;
}
