#include <stdio.h>
#include <unistd.h>
#include <time.h>

#include <CUnit/Basic.h>
#include <CUnit/Automated.h>

#include <pg_probackup.h>
#include <utils/file.h>

#include "pgunit.h"

static void
test_do_init()
{
	FOBJ_FUNC_ARP();
	char *backup_path = random_path();

	CatalogState *catalogState = catalog_new(backup_path);

	int rc = do_init(catalogState);
	CU_ASSERT(rc == 0);
}

static void
test_do_add_instance()
{
	//FOBJ_FUNC_ARP();
	int rc;
	char *backup_path = random_path();
	char *instance_name = random_name();
	char *server_path = random_path();
	init_fake_server(server_path);

	CatalogState *catalogState = catalog_new(backup_path);
	catalogState->backup_location = drive;
	rc = do_init(catalogState);
	CU_ASSERT(rc == 0);

	//CU_ASSERT_FATAL(pio_exists_d(drive, backup_path));

	init_config(&instance_config, instance_name);
	instance_config.pgdata = server_path;
	InstanceState *instanceState = makeInstanceState(catalogState, instance_name);
	instanceState->database_location = drive;
	rc = do_add_instance(instanceState, &instance_config);
	CU_ASSERT(rc == 0);

	char buf[MAXPGPATH];
	snprintf(buf, MAXPGPATH, "%s/%s/%s", catalogState->backup_subdir_path, instance_name, BACKUP_CATALOG_CONF_FILE);
	CU_ASSERT(pio_exists(drive, buf));
}


PBK_test_description PIO_INIT_TESTS[] = {
	{"Test do_init", test_do_init},
	{"Test do_add_instance", test_do_add_instance},
	{NULL,NULL},
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

	pbk_add_tests(USE_LOCAL, "Local init", PIO_INIT_TESTS);

	CU_basic_set_mode(CU_BRM_VERBOSE);

	CU_basic_run_tests();
	CU_set_output_filename("test_probackup");
	//CU_list_tests_to_file();
	CU_automated_run_tests();

	CU_cleanup_registry();

	return CU_get_error();
}
