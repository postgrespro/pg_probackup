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
	char *path = random_path();

	err = $i(pioWriteFile, drive, .path = path, .content = ft_bytes(TEST_STR, strlen(TEST_STR)), .binary = true);
	CU_ASSERT(!$haserr(err));
	time_t now = time(NULL);

	pio_stat_t pst = $i(pioStat, drive, .path = path, .follow_symlink = false, .err = &err);

	CU_ASSERT(!$haserr(err));

	CU_ASSERT(pst.pst_kind == PIO_KIND_REGULAR);
	CU_ASSERT(pst.pst_mode == FILE_PERMISSION);
	CU_ASSERT(abs(now-pst.pst_mtime) < 2);
	CU_ASSERT(pst.pst_size == 5);
}

static void
test_pioRemove()
{
	FOBJ_FUNC_ARP();

	char *path = random_path();
	pio_write(drive, path, TEST_STR);
	CU_ASSERT(pio_exists(drive, path));

	err_i err = $i(pioRemove, drive, .path = path, .missing_ok = false);

	CU_ASSERT(!$haserr(err));

	CU_ASSERT(!pio_exists(drive, path));
}

static void
test_pioRename()
{
	FOBJ_FUNC_ARP();

	char *name = random_path();
	char *another_name = random_path();

	pio_write(drive, name, TEST_STR);
	CU_ASSERT(pio_exists(drive, name));

	err_i err = $i(pioRename, dbdrive, .old_path = name, .new_path = another_name);
	CU_ASSERT(!$haserr(err));

	CU_ASSERT(!pio_exists(drive, name));
	CU_ASSERT(pio_exists(drive, another_name));
}

static void
test_pioExists()
{
	FOBJ_FUNC_ARP();

	err_i err = $noerr();
	bool exists = $i(pioExists, drive, .path = "/", .expected_kind = PIO_KIND_DIRECTORY, &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(exists);

	const char *path = random_path();
	err = $noerr();
	exists = $i(pioExists, drive, .path = path, .expected_kind = PIO_KIND_REGULAR, &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(!exists);

	char *name = random_path();
	pio_write(drive, name, TEST_STR);
	exists = $i(pioExists, drive, .path = name, .expected_kind = PIO_KIND_REGULAR, &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(exists);
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
	char *path = random_path();

	CU_ASSERT(!pio_exists(drive, path));

	err = $i(pioWriteFile, drive, .path = path, .content = ft_bytes(TEST_STR, strlen(TEST_STR)), .binary = true);
	CU_ASSERT(!$haserr(err));

	CU_ASSERT(pio_exists(drive, path));

	ft_bytes_t result = $i(pioReadFile, drive, .path = path, .binary = true, &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(result.len==strlen(TEST_STR));
	CU_ASSERT(!strncmp(result.ptr, TEST_STR, strlen(TEST_STR)));

	ft_bytes_free(&result);

	free(path);
}

static void
test_pioOpenRead()
{
	FOBJ_FUNC_ARP();
	err_i err = $noerr();
	char *path = random_path();
	pio_write(drive, path, TEST_STR);

	CU_ASSERT(pio_exists(drive, path));

	pioReader_i reader = $i(pioOpenRead, drive, .path = path, &err);
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

	free(path);
}

static void
test_pioOpenReadStream()
{
	FOBJ_FUNC_ARP();
	err_i err = $noerr();
	char *path = random_path();

	pioReadStream_i stream;
	/* Crash in pioCloudDrive */
	stream  = $i(pioOpenReadStream, drive, .path = path, &err);
	CU_ASSERT($haserr(err));

	pio_write(drive, path, TEST_STR);

	stream  = $i(pioOpenReadStream, drive, .path = path, &err);
	CU_ASSERT(!$haserr(err));

	char B0[8192];
	ft_bytes_t buf = ft_bytes(B0, 8192);
	size_t ret = $i(pioRead, stream, .buf= buf, &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(ret==strlen(TEST_STR));
	CU_ASSERT(!strncmp(buf.ptr, TEST_STR, strlen(TEST_STR)));
	$i(pioClose, stream);
	free(path);
}

static void
test_pioGetCRC32()
{
	FOBJ_FUNC_ARP();
	err_i err = $noerr();
	char *path = random_path();
	pg_crc32 crc;

#if 0
	//crashes. should return errno in err
	crc = $i(pioGetCRC32, drive, .path = path, .compressed = false, .err = &err);
	CU_ASSERT($haserr(err));
#endif
	pio_write(drive, path, TEST_STR);
	crc = $i(pioGetCRC32, drive, .path = path, .compressed = false, .err = &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(crc==0xFA94FDDF)
}


static void
test_pioMakeDir()
{
	FOBJ_FUNC_ARP();

	char *path = random_path();

	CU_ASSERT(!pio_exists(drive, path));
	err_i err = $i(pioMakeDir, drive, .path = path, .mode = DIR_PERMISSION, .strict = true);
	CU_ASSERT(!$haserr(err));

	CU_ASSERT(pio_exists_d(drive, path));
}

static void
test_pioMakeDirWithParent()
{
	FOBJ_FUNC_ARP();
	char child[MAXPGPATH];
	char *parent = random_path();
	CU_ASSERT(!pio_exists(drive, parent));
	snprintf(child, MAXPGPATH, "%s/TEST", parent);

	err_i err = $i(pioMakeDir, drive, .path = child, .mode = DIR_PERMISSION, .strict = true);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(pio_exists_d(drive, parent));
	CU_ASSERT(pio_exists_d(drive, child));

	free(parent);
}

static void
test_pioRemoveDir()
{
	FOBJ_FUNC_ARP();
	char *path = random_path();
	err_i err = $noerr();
	char path2[8192];
	snprintf(path2, 8192, "%s/%s", path, "sample.txt");

	CU_ASSERT(!pio_exists(drive, path));
	err = $i(pioMakeDir, drive, .path = path, .mode = DIR_PERMISSION, .strict = true);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(pio_exists_d(drive, path));

	err = $i(pioWriteFile, drive, .path = path2, .content = ft_bytes(TEST_STR, strlen(TEST_STR)), .binary = true);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(pio_exists(drive, path2));
	$i(pioRemoveDir, drive, path, .root_as_well=false);
	CU_ASSERT(!pio_exists(drive, path2));
	CU_ASSERT(pio_exists_d(drive, path));
}

static void
test_pioFilesAreSame()
{
	FOBJ_FUNC_ARP();

	err_i err = $noerr();
	char *path1 = random_path();
	char *path2 = random_path();

	CU_ASSERT(!pio_exists(drive, path1));
	CU_ASSERT(!pio_exists(drive, path2));

	err = $i(pioWriteFile, drive, .path = path1, .content = ft_bytes(TEST_STR, strlen(TEST_STR)), .binary = true);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(pio_exists(drive, path1));

	err = $i(pioWriteFile, drive, .path = path2, .content = ft_bytes(TEST_STR, strlen(TEST_STR)), .binary = true);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(pio_exists(drive, path2));

	ft_bytes_t result1 = $i(pioReadFile, drive, .path = path1, .binary = true, &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(result1.len==strlen(TEST_STR));
	CU_ASSERT(!strncmp(result1.ptr, TEST_STR, strlen(TEST_STR)));

	ft_bytes_t result2 = $i(pioReadFile, drive, .path = path2, .binary = true, &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(result2.len==strlen(TEST_STR));
	CU_ASSERT(!strncmp(result2.ptr, TEST_STR, strlen(TEST_STR)));

	CU_ASSERT(result1.len == result2.len);
	CU_ASSERT(!memcmp(result1.ptr, result2.ptr, result1.len));

	ft_bytes_free(&result1);
	ft_bytes_free(&result2);

	free(path1);
	free(path2);
}

static void
test_pioReadFile()
{
	FOBJ_FUNC_ARP();

	err_i err = $noerr();
	char *path = random_path();

	CU_ASSERT(!pio_exists(drive, path));

	err = $i(pioWriteFile, drive, .path = path, .content = ft_bytes(TEST_STR, strlen(TEST_STR)), .binary = true);
	CU_ASSERT(!$haserr(err));

	CU_ASSERT(pio_exists(drive, path));

	ft_bytes_t result = $i(pioReadFile, drive, .path = path, .binary = true, &err);
	CU_ASSERT(!$haserr(err));
	CU_ASSERT(result.len==strlen(TEST_STR));
	CU_ASSERT(!strncmp(result.ptr, TEST_STR, strlen(TEST_STR)));

	ft_bytes_free(&result);

	free(path);
}

static void
test_pioOpenRewrite()
{
	FOBJ_FUNC_ARP();
	err_i err = $noerr();
	char *path = random_path();
	pio_write(drive, path, TEST_STR);

	CU_ASSERT(pio_exists(drive, path));

	pioWriteCloser_i writer = $i(pioOpenRewrite, drive, .path = path,
								 .permissions = FILE_PERMISSION, .binary = true,
								 .use_temp=true, .sync = true, .err = &err);
	CU_ASSERT(!$haserr(err));
	char B0[8192];
	snprintf(B0, 8192, XXX_STR);
	ft_bytes_t buf = ft_bytes(B0, strlen(B0));
	err = $i(pioWrite, writer, .buf = buf);
	CU_ASSERT(!$haserr(err));
	$i(pioClose, writer);

	ft_bytes_t result = $i(pioReadFile, drive, .path = path, .binary = true, &err);
	CU_ASSERT(strlen(XXX_STR) == result.len);
	CU_ASSERT(!memcmp(XXX_STR, result.ptr, result.len));
	ft_bytes_free(&result);

	free(path);
}

PBK_test_description PIO_DRIVE_TESTS[] = {
	{"Test pioOpenRead", test_pioOpenRead},
	{"Test pioOpenReadStream", test_pioOpenReadStream},
	{"Test pioStat", test_pioStat},
	{"Test pioRemove", test_pioRemove},
	{"Test pioRename", test_pioRename},
	{"Test pioExists", test_pioExists},
	{"Test pioGetCRC32", test_pioGetCRC32},
	{"Test pioIsRemote", test_pioIsRemote},
	{"Test pioMakeDir", test_pioMakeDir},
	{"Test pioMakeDirWithParent", test_pioMakeDirWithParent},
	{"Test pioRemoveDir", test_pioRemoveDir},
	{"Test pioFilesAreSame", test_pioFilesAreSame},
	{"Test pioReadFile", test_pioReadFile},
	{"Test pioWriteFile", test_pioWriteFile},
	{"Test pioOpenRewrite", test_pioOpenRewrite},
	{NULL, NULL}
};

PBK_test_description PIO_DB_DRIVE_TESTS[] = {
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
