import os
import shutil
import stat
import unittest

from .helpers.ptrack_helpers import dir_files, ProbackupTest, fs_backup_class

DIR_PERMISSION = 0o700 if os.name != 'nt' else 0o777
CATALOG_DIRS = ['backups', 'wal']


class InitTest(ProbackupTest):

    def tearDown(self):
        super().tearDown()
        # Remove some additional backup dirs
        if hasattr(self, 'no_access_dir'):
            shutil.rmtree(self.no_access_dir, ignore_errors=True)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_basic_success(self):
        """Success normal init"""
        instance_name = 'node'
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple(instance_name)
        self.pb.init()

        if backup_dir.is_file_based:
            self.assertEqual(
                dir_files(backup_dir),
                CATALOG_DIRS
            )

            for subdir in CATALOG_DIRS:
                dirname = os.path.join(backup_dir, subdir)
                self.assertEqual(DIR_PERMISSION, stat.S_IMODE(os.stat(dirname).st_mode))

        self.pb.add_instance(instance_name, node)
        self.assertMessage(self.pb.del_instance(instance_name),
                           contains=f"INFO: Instance '{instance_name}' successfully deleted")

        # Show non-existing instance
        error_result = self.pb.show(instance_name, as_text=True, expect_error=True)
        self.assertMessage(error_result,
                           contains=f"ERROR: Instance '{instance_name}' does not exist in this backup catalog")

        # Delete non-existing instance
        error_result = self.pb.del_instance('node1', expect_error=True)
        self.assertMessage(error_result,
                           contains="ERROR: Instance 'node1' does not exist in this backup catalog")

        # Add instance without pgdata
        error_result = self.pb.run([
                                        "add-instance",
                                        "--instance=node1",
                                      ], expect_error=True)
        self.assertMessage(error_result,
                           contains="No postgres data directory specified.\n"
			 "Please specify it either using environment variable PGDATA or\n"
			 "command line option --pgdata (-D)")

    # @unittest.skip("skip")
    def test_basic_already_exist(self):
        """Failure with backup catalog already existed"""
        instance_name = 'node'
        self.pg_node.make_simple(instance_name, checksum=False)
        self.pb.init()
        error_result = self.pb.show(instance_name, expect_error=True)
        self.assertMessage(error_result, contains=f"ERROR: Instance '{instance_name}' "
                                                   f"does not exist in this backup catalog")

    # @unittest.skip("skip")
    def test_basic_abs_path(self):
        """failure with backup catalog should be given as absolute path"""
        self.pg_node.make_simple('node')
        error_result = self.pb.run(["init", "-B", "../backups_fake"], expect_error=True, use_backup_dir=None)
        self.assertMessage(error_result, regex="backup-path must be an absolute path")

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_basic_add_instance_idempotence(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/219
        """
        backup_dir = self.backup_dir
        instance_name = 'node'
        node = self.pg_node.make_simple(instance_name)
        self.pb.init()

        self.pb.add_instance(instance_name, node)
        self.remove_one_backup_instance(backup_dir, instance_name)

        self.write_instance_wal(backup_dir, instance_name, '0000', b'')

        error_message = self.pb.add_instance(instance_name, node, expect_error=True)
        self.assertMessage(error_message, regex=fr"'{instance_name}'.*WAL.*already exists")

        error_message = self.pb.add_instance(instance_name, node, expect_error=True)
        self.assertMessage(error_message, regex=fr"'{instance_name}'.*WAL.*already exists")

    def test_init_backup_catalog_no_access(self):
        """ Test pg_probackup init -B backup_dir to a dir with no read access. """
        if not self.backup_dir.is_file_based:
            self.skipTest("permission test is not implemented on cloud storage")
        backup_dir = self.build_backup_dir('noaccess/backup')
        # Store no_access_dir for the teardown in an instance variable
        self.no_access_dir = os.path.dirname(backup_dir)
        os.makedirs(self.no_access_dir)
        os.chmod(self.no_access_dir, stat.S_IREAD)

        expected = f'ERROR: Cannot open backup catalog directory: Cannot open dir "{backup_dir}": Permission denied'
        error_message = self.pb.init(use_backup_dir=backup_dir, expect_error=True)
        self.assertMessage(error_message, contains=expected)

    def test_init_backup_catalog_no_write(self):
        """ Test pg_probackup init -B backup_dir to a dir with no write access. """
        if not self.backup_dir.is_file_based:
            self.skipTest("permission test is not implemented on cloud storage")
        backup_dir = self.build_backup_dir('noaccess/backup')
        # Store no_access_dir for the teardown in an instance variable
        self.no_access_dir = os.path.dirname(backup_dir)
        os.makedirs(self.no_access_dir)
        os.chmod(self.no_access_dir, stat.S_IREAD|stat.S_IEXEC)

        expected = 'ERROR: Can not create backup catalog root directory: Cannot make dir "{0}": Permission denied'.format(backup_dir)
        error_message = self.pb.init(use_backup_dir=backup_dir, expect_error=True)
        self.assertMessage(error_message, contains=expected)

    def test_init_backup_catalog_no_create(self):
        """ Test pg_probackup init -B backup_dir to a dir when backup dir exists but not writeable. """
        if not self.backup_dir.is_file_based:
            self.skipTest("permission test is not implemented on cloud storage")
        os.makedirs(self.backup_dir)
        os.chmod(self.backup_dir, stat.S_IREAD|stat.S_IEXEC)

        expected = 'ERROR: Can not create backup catalog data directory: Cannot make dir "{0}": Permission denied'.format(os.path.join(self.backup_dir, 'backups'))
        error_message = self.pb.init(expect_error=True)
        self.assertMessage(error_message, contains=expected)

    def test_init_backup_catalog_exists_not_empty(self):
        """ Test pg_probackup init -B backup_dir which exists and not empty. """
        backup_dir = self.backup_dir
        if backup_dir.is_file_based:
            os.makedirs(backup_dir)
        backup_dir.write_file('somefile.txt', 'hello')

        error_message = self.pb.init(expect_error=True)
        self.assertMessage(error_message, contains=f"ERROR: Backup catalog '{backup_dir}' already exists and is not empty")

    def test_init_add_instance_with_special_symbols(self):
        """ Test pg_probackup init -B backup_dir which exists and not empty. """
        backup_dir = self.backup_dir
        instance_name = 'instance! -_.*(\')&$@=;:+,?\\{^}%`[\"]<>~#|'
        node = self.pg_node.make_simple(instance_name)
        self.pb.init()

        error_message = self.pb.add_instance(instance_name, node)
        self.assertMessage(error_message, regex=fr"'INFO: Instance {instance_name}' successfully initialized")

    @unittest.skipUnless(fs_backup_class.is_file_based, "AccessPath check is always true on s3")
    def test_basic_dry_run_init(self):
        """Success init with dry-run option"""
        instance_name = 'node'
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple(instance_name)
        self.pb.init(options=['--dry-run'])
        #Assertions
        self.assertFalse(os.path.exists(backup_dir),
                         "Directory should not exist: {0}".format(backup_dir))

        # Check existing backup directory
        self.pb.init()
        self.pb.init(options=['--dry-run', '--skip-if-exists'])

        # Access check suite if disk mounted as read_only
        dir_mode = os.stat(self.test_path).st_mode
        os.chmod(self.test_path, 0o500)

        error_message = self.pb.init(options=['--dry-run'], expect_error ='because of changed permissions')
        try:
            self.assertMessage(error_message, contains='ERROR: Check permissions')
        finally:
            # Cleanup
            os.chmod(self.test_path, dir_mode)

        node.cleanup()

    @unittest.skipUnless(fs_backup_class.is_file_based, "AccessPath check is always true on s3")
    def test_basic_dry_run_add_instance(self):
        """ Check --dry-run option for add_instance command"""
        instance_name = 'node'
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple(instance_name)
        self.pb.init()
        self.pb.add_instance(instance_name, node, options=['--dry-run'])
        # Assetions
        self.assertFalse(os.listdir(os.path.join(backup_dir, "wal")))
        self.assertFalse(os.listdir(os.path.join(backup_dir, "backups")))

        # Check existing backup directory
        self.pb.add_instance(instance_name, node)
        self.pb.add_instance(instance_name, node, options=['--dry-run', '--skip-if-exists'])

        # Check access suite - if disk mounted as read_only
        dir_path = os.path.join(self.test_path, 'backup')
        dir_mode = os.stat(dir_path).st_mode
        os.chmod(dir_path, 0o500)

        error_message = self.pb.add_instance(instance_name, node, options=['--dry-run'],
                                             expect_error ='because of changed permissions')
        try:
            self.assertMessage(error_message, contains='ERROR: Check permissions ')
        finally:
            # Cleanup
            os.chmod(dir_path, dir_mode)

        node.cleanup()
