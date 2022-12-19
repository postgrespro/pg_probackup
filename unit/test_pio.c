#include <stdio.h>
#include <unistd.h>
#include <time.h>

#include <CUnit/Basic.h>
#include <CUnit/Automated.h>

#include <pg_probackup.h>
#include <utils/file.h>

#include "pgunit.h"

#define TEST_STR "test\n"
#define BUFSZ 8192

#define XXX_STR "XXX"

static void
test_pioStat()
{
	FOBJ_FUNC_ARP();
	err_i err = $noerr();
	ft_str_t path = random_path();

	err = $i(pioWriteFile, drive, .path = path.ptr, .content = ft_bytes(TEST_STR, strlen(TEST_STR)), .binary = true);
	CU_ASSERT(!$haserr(err));
	time_t now = time(NULL);

	pio_stat_t pst = $i(pioStat, drive, .path = path.ptr, .follow_symlink = false, .err = &err);

	CU_ASSERT(!$haserr(err));

	CU_ASSERT(pst.pst_kind == PIO_KIND_REGULAR);
	CU_ASSERT(pst.pst_mode == FILE_PERMISSION);
	CU_ASSERT(abs(now-pst.pst_mtime) < 2);
	CU_ASSERT(pst.pst_size == 5);

	ft_str_free(&path);
}

static void
test_pioRemove()
{
	FOBJ_FUNC_ARP();

	ft_str_t path = random_path();
	pio_write(drive, path.ptr, TEST_STR);
	CU_ASSERT(pio_exists(drive, path.ptr));

	err_i err = $i(pioRemove, drive, .path = path.ptr, .missing_ok = false);

	CU_ASSERT(!$haserr(err));

	CU_ASSERT(!pio_exists(drive, path.ptr));

	ft_str_free(&path);
}

static void
test_pioExists()
{
	FOBJ_FUNC_ARP();

	err_i err = $noerr();
	bool exists = $i(pioExists, drive, .path = "/", .expected_kind = PIO_KIND_DIRECTORY, &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(exists);

	ft_str_t path = random_path();
	err = $noerr();
	exists = $i(pioExists, drive, .path = path.ptr, .expected_kind = PIO_KIND_REGULAR, &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(!exists);

	ft_str_t name = random_path();
	pio_write(drive, name.ptr, TEST_STR);
	exists = $i(pioExists, drive, .path = name.ptr, .expected_kind = PIO_KIND_REGULAR, &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(exists);

	ft_str_free(&path);
	ft_str_free(&name);
}

static void
test_pioIsRemote()
{
	FOBJ_FUNC_ARP();

	if(should_be_remote) {
		CU_ASSERT( $i(pioIsRemote, drive) );
	} else {
		CU_ASSERT( !$i(pioIsRemote, drive) );
	}
}

static void
test_pioWriteFile()
{
	FOBJ_FUNC_ARP();

	err_i err = $noerr();
	ft_str_t path = random_path();

	CU_ASSERT(!pio_exists(drive, path.ptr));

	err = $i(pioWriteFile, drive, .path = path.ptr, .content = ft_bytes(TEST_STR, strlen(TEST_STR)), .binary = true);
	CU_ASSERT(!$haserr(err));

	CU_ASSERT(pio_exists(drive, path.ptr));

	ft_bytes_t result = $i(pioReadFile, drive, .path = path.ptr, .binary = true, &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(result.len==strlen(TEST_STR));
	CU_ASSERT(!strncmp(result.ptr, TEST_STR, strlen(TEST_STR)));

	ft_bytes_free(&result);
	ft_str_free(&path);
}

static void
test_pioOpenRead()
{
	FOBJ_FUNC_ARP();
	err_i err = $noerr();
	ft_str_t path = random_path();
	pio_write(drive, path.ptr, TEST_STR);

	CU_ASSERT(pio_exists(drive, path.ptr));

	pioReader_i reader = $i(pioOpenRead, drive, .path = path.ptr, &err);
	CU_ASSERT(!$haserr(err));
	char B0[8192];
	ft_bytes_t buf = ft_bytes(B0, 8192);
	size_t ret = $i(pioRead, reader, .buf = buf, &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(ret==strlen(TEST_STR));
	CU_ASSERT(!strncmp(buf.ptr, TEST_STR, strlen(TEST_STR)));
	err = $i(pioSeek, reader, 0);
	CU_ASSERT(!$haserr(err));
	ft_bytes_t buf2 = ft_bytes(B0+100, 8192);
	ret = $i(pioRead, reader, .buf = buf2, &err);
	CU_ASSERT(ret==strlen(TEST_STR));
	CU_ASSERT(!strncmp(buf2.ptr, TEST_STR, strlen(TEST_STR)));

	$i(pioClose, reader);

	//ft_bytes_free(&result);

	ft_str_free(&path);
}

static void
test_pioOpenReadStream()
{
	// return enoent for non existent file. same for pioStat
	FOBJ_FUNC_ARP();
	err_i err = $noerr();
	ft_str_t path = random_path();

	pioReadStream_i stream;
	/* Crash in pioCloudDrive */
	stream  = $i(pioOpenReadStream, drive, .path = path.ptr, &err);
	CU_ASSERT($haserr(err));

	pio_write(drive, path.ptr, TEST_STR);

	stream  = $i(pioOpenReadStream, drive, .path = path.ptr, &err);
	CU_ASSERT(!$haserr(err));

	char B0[8192];
	ft_bytes_t buf = ft_bytes(B0, 8192);
	size_t ret = $i(pioRead, stream, .buf= buf, &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(ret==strlen(TEST_STR));
	CU_ASSERT(!strncmp(buf.ptr, TEST_STR, strlen(TEST_STR)));
	$i(pioClose, stream);
	ft_str_free(&path);
}

static void
test_pioGetCRC32()
{
	FOBJ_FUNC_ARP();
	err_i err = $noerr();
	ft_str_t path = random_path();
	pg_crc32 crc;

#if 0
	//crashes. should return errno in err
	crc = $i(pioGetCRC32, drive, .path = path.ptr, .compressed = false, .err = &err);
	CU_ASSERT($haserr(err));
#endif
	pio_write(drive, path.ptr, TEST_STR);
	crc = $i(pioGetCRC32, drive, .path = path.ptr, .compressed = false, .err = &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(crc==0xFA94FDDF)
}


static void
test_pioMakeDir()
{
	FOBJ_FUNC_ARP();

	ft_str_t path = random_path();

	CU_ASSERT(!pio_exists(drive, path.ptr));
	err_i err = $i(pioMakeDir, drive, .path = path.ptr, .mode = DIR_PERMISSION, .strict = true);
	CU_ASSERT(!$haserr(err));

	CU_ASSERT(pio_exists_d(drive, path.ptr));
}

static void
test_pioMakeDirWithParent()
{
	FOBJ_FUNC_ARP();
	char child[MAXPGPATH];
	ft_str_t parent = random_path();
	CU_ASSERT(!pio_exists(drive, parent.ptr));
	snprintf(child, MAXPGPATH, "%s/TEST", parent.ptr);

	err_i err = $i(pioMakeDir, drive, .path = child, .mode = DIR_PERMISSION, .strict = true);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(pio_exists_d(drive, parent.ptr));
	CU_ASSERT(pio_exists_d(drive, child));

	ft_str_free(&parent);
}

static void
test_pioListDirCanWithSlash()
{
	FOBJ_FUNC_ARP();
	err_i err = $noerr();
	ft_str_t root = random_path();
	ft_str_t slash = ft_asprintf("%s/", root.ptr);
	ft_str_t child = ft_asprintf("%s/sample.txt", root.ptr);

	CU_ASSERT(!pio_exists(drive, root.ptr));
	err = $i(pioMakeDir, drive, .path = root.ptr, .mode = DIR_PERMISSION, .strict = true);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(pio_exists_d(drive, root.ptr));

	err = $i(pioWriteFile, drive, .path = child.ptr, .content = ft_bytes(TEST_STR, strlen(TEST_STR)), .binary = true);
	CU_ASSERT(!$haserr(err));

	pioDirIter_i dir = $i(pioOpenDir, drive, .path = slash.ptr, .err = &err);
	CU_ASSERT(!$haserr(err));

	int count = 0;
	while (true)
	{
		pio_dirent_t entry = $i(pioDirNext, dir, &err);
		CU_ASSERT(!$haserr(err));
		if (entry.stat.pst_kind == PIO_KIND_UNKNOWN) break;
		CU_ASSERT(ft_strcmp(entry.name, ft_cstr("sample.txt")) == FT_CMP_EQ);
		count++;
	}
	CU_ASSERT(count == 1);
	err = $i(pioClose, dir);
	CU_ASSERT(!$haserr(err));

	ft_str_free(&root);
	ft_str_free(&slash);
	ft_str_free(&child);
}

static void
test_pioListDir()
{
	FOBJ_FUNC_ARP();
	ft_str_t root = random_path();
	ft_str_t child = ft_asprintf("%s/sample.txt", root.ptr);
	ft_str_t sub_dir = ft_asprintf("%s/subdir", root.ptr);
	ft_str_t sub_child = ft_asprintf("%s/subdir/xxx.txt", root.ptr);
	err_i err = $noerr();
	int i;

	CU_ASSERT(!pio_exists(drive, root.ptr));
	err = $i(pioMakeDir, drive, .path = root.ptr, .mode = DIR_PERMISSION, .strict = true);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(pio_exists_d(drive, root.ptr));

	err = $i(pioWriteFile, drive, .path = child.ptr, .content = ft_bytes(TEST_STR, strlen(TEST_STR)), .binary = true);
	CU_ASSERT(!$haserr(err));

	err = $i(pioMakeDir, drive, .path = sub_dir.ptr, .mode = DIR_PERMISSION, .strict = true);
	CU_ASSERT(!$haserr(err));

	err = $i(pioWriteFile, drive, .path = sub_child.ptr, .content = ft_bytes(TEST_STR, strlen(TEST_STR)), .binary = true);
	CU_ASSERT(!$haserr(err));

	pioDirIter_i dir = $i(pioOpenDir, drive, .path = root.ptr, .err = &err);
	CU_ASSERT(!$haserr(err));

#define NUM_EXPECTED 2
	const char *expected[NUM_EXPECTED] = {"sample.txt", "subdir"};
	int count = 0;
	for (count = 0; true; count++)
	{
		pio_dirent_t entry = $i(pioDirNext, dir, &err);
		CU_ASSERT(!$haserr(err));

		if (entry.stat.pst_kind == PIO_KIND_UNKNOWN) break;

		for(i = 0; i < NUM_EXPECTED; ++i)
		{
			if(ft_strcmp(entry.name, ft_cstr(expected[i])) != FT_CMP_EQ)
				continue;
			expected[i] = NULL;
		}
	}

	for(i = 0; i < NUM_EXPECTED; ++i)
	{
		CU_ASSERT(expected[i] == NULL);
	}

	CU_ASSERT(count == NUM_EXPECTED);

	err = $i(pioClose, dir);
	CU_ASSERT(!$haserr(err));

	dir = $i(pioOpenDir, drive, .path = sub_dir.ptr, .err = &err);
	CU_ASSERT(!$haserr(err));

	count = 0;
	for (count = 0; true; count++)
	{
		pio_dirent_t entry = $i(pioDirNext, dir, &err);
		CU_ASSERT(!$haserr(err));

		if (entry.stat.pst_kind == PIO_KIND_UNKNOWN) break;

		CU_ASSERT(ft_strcmp(entry.name, ft_cstr("xxx.txt")) == FT_CMP_EQ);
	}

	for(i = 0; i < NUM_EXPECTED; ++i)
	{
		CU_ASSERT(expected[i] == NULL);
	}

	CU_ASSERT(count == 1);

	err = $i(pioClose, dir);
	CU_ASSERT(!$haserr(err));

#undef NUM_EXPECTED
}

static void
test_pioListDirMTimeAndSize()
{
	FOBJ_FUNC_ARP();
	ft_str_t root = random_path();
	ft_str_t child = ft_asprintf("%s/sample.txt", root.ptr);
	err_i err = $noerr();
	int i;

	CU_ASSERT(!pio_exists(drive, root.ptr));
	err = $i(pioMakeDir, drive, .path = root.ptr, .mode = DIR_PERMISSION, .strict = true);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(pio_exists_d(drive, root.ptr));

	err = $i(pioWriteFile, drive, .path = child.ptr, .content = ft_bytes(TEST_STR, strlen(TEST_STR)), .binary = true);
	CU_ASSERT(!$haserr(err));
	time_t created = time(NULL);

	pioDirIter_i dir = $i(pioOpenDir, drive, .path = root.ptr, .err = &err);
	CU_ASSERT(!$haserr(err));

#define NUM_EXPECTED 1
	const char *expected[NUM_EXPECTED] = {"sample.txt"};
	int count = 0;
	for (count = 0; true; count++)
	{
		pio_dirent_t entry = $i(pioDirNext, dir, &err);
		CU_ASSERT(!$haserr(err));

		if (entry.stat.pst_kind == PIO_KIND_UNKNOWN) break;

		printf("XXX mtime=%ld, size=%ld created=%ld diff=%d\n", entry.stat.pst_mtime, entry.stat.pst_size, created, (int)created-(int)entry.stat.pst_mtime);
		CU_ASSERT(entry.stat.pst_mtime == created);
		//CU_ASSERT(entry.stat.pst_mtime == (created+3600*3));
		CU_ASSERT(entry.stat.pst_size == strlen(TEST_STR));

		for(i = 0; i < NUM_EXPECTED; ++i)
		{
			if(ft_strcmp(entry.name, ft_cstr(expected[i])) != FT_CMP_EQ)
				continue;
			expected[i] = NULL;
		}
	}

	for(i = 0; i < NUM_EXPECTED; ++i)
	{
		CU_ASSERT(expected[i] == NULL);
	}

	CU_ASSERT(count == NUM_EXPECTED);

	err = $i(pioClose, dir);
	CU_ASSERT(!$haserr(err));
}

static void
test_pioRemoveDir()
{
	FOBJ_FUNC_ARP();
	ft_str_t path = random_path();
	err_i err = $noerr();
	char path2[8192];
	snprintf(path2, 8192, "%s/%s", path.ptr, "sample.txt");

	CU_ASSERT(!pio_exists(drive, path.ptr));
	err = $i(pioMakeDir, drive, .path = path.ptr, .mode = DIR_PERMISSION, .strict = true);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(pio_exists_d(drive, path.ptr));

	err = $i(pioWriteFile, drive, .path = path2, .content = ft_bytes(TEST_STR, strlen(TEST_STR)), .binary = true);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(pio_exists(drive, path2));
	$i(pioRemoveDir, drive, path.ptr, .root_as_well=false);
	CU_ASSERT(!pio_exists(drive, path2));
	CU_ASSERT(pio_exists_d(drive, path.ptr));
}

static void
test_pioFilesAreSame()
{
	FOBJ_FUNC_ARP();

	err_i err = $noerr();
	ft_str_t path1 = random_path();
	ft_str_t path2 = random_path();

	CU_ASSERT(!pio_exists(drive, path1.ptr));
	CU_ASSERT(!pio_exists(drive, path2.ptr));

	err = $i(pioWriteFile, drive, .path = path1.ptr, .content = ft_bytes(TEST_STR, strlen(TEST_STR)), .binary = true);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(pio_exists(drive, path1.ptr));

	err = $i(pioWriteFile, drive, .path = path2.ptr, .content = ft_bytes(TEST_STR, strlen(TEST_STR)), .binary = true);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(pio_exists(drive, path2.ptr));

	ft_bytes_t result1 = $i(pioReadFile, drive, .path = path1.ptr, .binary = true, &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(result1.len==strlen(TEST_STR));
	CU_ASSERT(!strncmp(result1.ptr, TEST_STR, strlen(TEST_STR)));

	ft_bytes_t result2 = $i(pioReadFile, drive, .path = path2.ptr, .binary = true, &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(result2.len==strlen(TEST_STR));
	CU_ASSERT(!strncmp(result2.ptr, TEST_STR, strlen(TEST_STR)));

	CU_ASSERT(result1.len == result2.len);
	CU_ASSERT(!memcmp(result1.ptr, result2.ptr, result1.len));

	ft_bytes_free(&result1);
	ft_bytes_free(&result2);

	ft_str_free(&path1);
	ft_str_free(&path2);
}

static void
test_pioReadFile()
{
	FOBJ_FUNC_ARP();

	err_i err = $noerr();
	ft_str_t path = random_path();

	CU_ASSERT(!pio_exists(drive, path.ptr));

	err = $i(pioWriteFile, drive, .path = path.ptr, .content = ft_bytes(TEST_STR, strlen(TEST_STR)), .binary = true);
	CU_ASSERT(!$haserr(err));

	CU_ASSERT(pio_exists(drive, path.ptr));

	ft_bytes_t result = $i(pioReadFile, drive, .path = path.ptr, .binary = true, &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(result.len==strlen(TEST_STR));
	CU_ASSERT(!strncmp(result.ptr, TEST_STR, strlen(TEST_STR)));

	ft_bytes_free(&result);

	ft_str_free(&path);
}

static void
test_pioOpenRewrite()
{
	FOBJ_FUNC_ARP();
	err_i err = $noerr();
	ft_str_t path = random_path();
	pio_write(drive, path.ptr, TEST_STR);

	CU_ASSERT(pio_exists(drive, path.ptr));

	pioWriteCloser_i writer = $i(pioOpenRewrite, drive, .path = path.ptr,
								 .permissions = FILE_PERMISSION, .binary = true,
								 .use_temp=true, .sync = true, .err = &err);
	CU_ASSERT(!$haserr(err));
	char B0[8192];
	snprintf(B0, 8192, XXX_STR);
	ft_bytes_t buf = ft_bytes(B0, strlen(B0));
	err = $i(pioWrite, writer, .buf = buf);
	CU_ASSERT(!$haserr(err));
	$i(pioClose, writer);

	ft_bytes_t result = $i(pioReadFile, drive, .path = path.ptr, .binary = true, &err);
	CU_ASSERT(strlen(XXX_STR) == result.len);
	CU_ASSERT(!memcmp(XXX_STR, result.ptr, result.len));
	ft_bytes_free(&result);

	ft_str_free(&path);
}

static void
test_pioSeek()
{
	FOBJ_FUNC_ARP();
	err_i err = $noerr();
	ft_str_t path = random_path();
	pioWriteCloser_i writer = $i(pioOpenRewrite, drive, .path = path.ptr,
								 .permissions = FILE_PERMISSION, .binary = true,
								 .use_temp=true, .sync = true, .err = &err);
	CU_ASSERT(!$haserr(err));
	char B0[8192];
	snprintf(B0, 8192, "012345678901234567890123012345678901234567890123");
	ft_bytes_t buf = ft_bytes(B0, strlen(B0));
	err = $i(pioWrite, writer, .buf = buf);
	CU_ASSERT(!$haserr(err));
	$i(pioClose, writer);

	pioReader_i reader = $i(pioOpenRead, drive, .path = path.ptr, &err);
	CU_ASSERT (!$haserr(err));

#define TRY_OFFT 1
#define TRY_LEN 24
	err = $i(pioSeek, reader, TRY_OFFT);
	CU_ASSERT(!$haserr(err));

	ft_bytes_t read_buf = ft_bytes_alloc(TRY_LEN);
	size_t rc = $i(pioRead, reader, .buf = read_buf, .err = &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(rc == TRY_LEN);
	CU_ASSERT(!memcmp(B0+TRY_OFFT, read_buf.ptr, TRY_LEN));
}

/* pioDBDrive */
static void
test_pioRename()
{
	FOBJ_FUNC_ARP();
	pioDBDrive_i db_drive = pioDBDriveForLocation(FIO_LOCAL_HOST);
	ft_str_t name = random_path();
	ft_str_t another_name = random_path();

	pio_write(drive, name.ptr, TEST_STR);
	CU_ASSERT(pio_exists(drive, name.ptr));

	err_i err = $i(pioRename, db_drive, .old_path = name.ptr, .new_path = another_name.ptr);
	CU_ASSERT(!$haserr(err));

	CU_ASSERT(!pio_exists(drive, name.ptr));
	CU_ASSERT(pio_exists(drive, another_name.ptr));
}

PBK_test_description PIO_DRIVE_TESTS[] = {
	{"Test pioOpenRead",	test_pioOpenRead},
	{"Test pioOpenReadStream", test_pioOpenReadStream},
	{"Test pioStat",		test_pioStat},
	{"Test pioRemove",		test_pioRemove},
	{"Test pioExists",		test_pioExists},
	{"Test pioGetCRC32",	test_pioGetCRC32},
	{"Test pioIsRemote",	test_pioIsRemote},
	{"Test pioMakeDir",		test_pioMakeDir},
	{"Test pioMakeDirWithParent", test_pioMakeDirWithParent},
	{"Test pioListDir",		test_pioListDir},
	{"Test pioListDirCanWithSlash", test_pioListDirCanWithSlash},
	{"Test pioListDirMTimeAndSize",		test_pioListDirMTimeAndSize},
	{"Test pioRemoveDir",	test_pioRemoveDir},
	{"Test pioFilesAreSame", test_pioFilesAreSame},
	{"Test pioReadFile",	test_pioReadFile},
	{"Test pioWriteFile",	test_pioWriteFile},
	{"Test pioOpenRewrite",	test_pioOpenRewrite},
	{"Test pioSeek",		test_pioSeek},
	{NULL, NULL}
};

PBK_test_description PIO_DB_DRIVE_TESTS[] = {
	{"Test pioRename", test_pioRename},
	{NULL, NULL}
};

int
main(int argc, char *argv[])
{
	ft_init_log(elog_ft_log);
	fobj_init();
	FOBJ_FUNC_ARP();
	init_pio_objects();

	init_test_drives();
	if(CUE_SUCCESS != CU_initialize_registry())
		return CU_get_error();

	pbk_add_tests(USE_LOCAL, "Local pioDrive", PIO_DRIVE_TESTS);
	pbk_add_tests(USE_LOCAL, "LOcal pioDBDrive", PIO_DB_DRIVE_TESTS);

	CU_list_tests_to_file();

	CU_basic_set_mode(CU_BRM_VERBOSE);
	CU_set_output_filename("test_pio");

	CU_basic_run_tests();
	CU_automated_run_tests();

	CU_cleanup_registry();

	return CU_get_error();
}
