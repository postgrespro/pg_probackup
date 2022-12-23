import os
import stat
import unittest
import shutil

from .helpers.ptrack_helpers import dir_files, ProbackupTest, ProbackupException

DIR_PERMISSION = 0o700 if os.name != 'nt' else 0o777
CATALOG_DIRS = ['backups', 'wal']

class InitTest(ProbackupTest, unittest.TestCase):
    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_success(self):
        """Success normal init"""
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(base_dir=os.path.join(self.module_name, self.fname, 'node'))
        self.init_pb(backup_dir)
        self.assertEqual(
            dir_files(backup_dir),
            CATALOG_DIRS
        )

        for subdir in CATALOG_DIRS:
            dirname = os.path.join(backup_dir, subdir)
            self.assertEqual(DIR_PERMISSION, stat.S_IMODE(os.stat(dirname).st_mode))

        self.add_instance(backup_dir, 'node', node)
        self.assertIn(
            "INFO: Instance 'node' successfully deleted",
            self.del_instance(backup_dir, 'node'),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        # Show non-existing instance
        try:
            self.show_pb(backup_dir, 'node')
            self.assertEqual(1, 0, 'Expecting Error due to show of non-existing instance. Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Instance 'node' does not exist in this backup catalog",
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(e.message, self.cmd))

        # Delete non-existing instance
        try:
            self.del_instance(backup_dir, 'node1')
            self.assertEqual(1, 0, 'Expecting Error due to delete of non-existing instance. Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Instance 'node1' does not exist in this backup catalog",
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(e.message, self.cmd))

        # Add instance without pgdata
        try:
            self.run_pb([
                "add-instance",
                "--instance=node1",
                "-B", backup_dir
            ])
            self.assertEqual(1, 0, 'Expecting Error due to adding instance without pgdata. Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Required parameter not specified: PGDATA (-D, --pgdata)",
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(e.message, self.cmd))

    # @unittest.skip("skip")
    def test_already_exist(self):
        """Failure with backup catalog already existed"""
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(base_dir=os.path.join(self.module_name, self.fname, 'node'))
        self.init_pb(backup_dir)
        try:
            self.show_pb(backup_dir, 'node')
            self.assertEqual(1, 0, 'Expecting Error due to initialization in non-empty directory. Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Instance 'node' does not exist in this backup catalog",
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

    # @unittest.skip("skip")
    def test_abs_path(self):
        """failure with backup catalog should be given as absolute path"""
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(base_dir=os.path.join(self.module_name, self.fname, 'node'))
        try:
            self.run_pb(["init", "-B", os.path.relpath("%s/backup" % node.base_dir, self.dir_path)])
            self.assertEqual(1, 0, 'Expecting Error due to initialization with non-absolute path in --backup-path. Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: -B, --backup-path must be an absolute path",
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_add_instance_idempotence(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/219
        """
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(base_dir=os.path.join(self.module_name, self.fname, 'node'))
        self.init_pb(backup_dir)

        self.add_instance(backup_dir, 'node', node)
        shutil.rmtree(os.path.join(backup_dir, 'backups', 'node'))

        dir_backups = os.path.join(backup_dir, 'backups', 'node')
        dir_wal = os.path.join(backup_dir, 'wal', 'node')

        with open(os.path.join(dir_wal, "0000"), 'w'):
            pass

        with self.assertRaisesRegex(ProbackupException, r"'node'.*WAL.*already exists"):
            self.add_instance(backup_dir, 'node', node)

        with self.assertRaisesRegex(ProbackupException, r"'node'.*WAL.*already exists"):
            self.add_instance(backup_dir, 'node', node)

    def test_init_backup_catalog_no_access(self):
        """ Test pg_probackup init -B backup_dir to a dir with no read access. """
        no_access_dir = os.path.join(self.tmp_path, self.module_name, self.fname,
                                   'noaccess')
        backup_dir = os.path.join(no_access_dir, 'backup')
        os.makedirs(no_access_dir)
        os.chmod(no_access_dir, stat.S_IREAD)

        expected = 'ERROR: cannot open backup catalog directory.*{0}.*Permission denied'.format(backup_dir)
        with self.assertRaisesRegex(ProbackupException, expected):
            self.init_pb(backup_dir)

    def test_init_backup_catalog_no_write(self):
        """ Test pg_probackup init -B backup_dir to a dir with no write access. """
        no_access_dir = os.path.join(self.tmp_path, self.module_name, self.fname,
                                   'noaccess')
        backup_dir = os.path.join(no_access_dir, 'backup')
        os.makedirs(no_access_dir)
        os.chmod(no_access_dir, stat.S_IREAD|stat.S_IEXEC)

        expected = 'ERROR: Can not create backup catalog root directory: Cannot make dir "{0}": Permission denied'.format(backup_dir)
        with self.assertRaisesRegex(ProbackupException, expected):
            self.init_pb(backup_dir)

    def test_init_backup_catalog_no_create(self):
        """ Test pg_probackup init -B backup_dir to a dir when backup dir exists but not writeable. """
        parent_dir = os.path.join(self.tmp_path, self.module_name, self.fname,
                                   'parent')
        backup_dir = os.path.join(parent_dir, 'backup')
        os.makedirs(backup_dir)
        os.chmod(backup_dir, stat.S_IREAD|stat.S_IEXEC)

        backups_dir = os.path.join(backup_dir, 'backups')
        expected = 'ERROR: Can not create backup catalog data directory: Cannot make dir "{0}": Permission denied'.format(backups_dir)
        with self.assertRaisesRegex(ProbackupException, expected):
            self.init_pb(backup_dir, cleanup=False)

    def test_init_backup_catalog_exists_not_empty(self):
        """ Test pg_probackup init -B backup_dir which exists and not empty. """
        parent_dir = os.path.join(self.tmp_path, self.module_name, self.fname,
                                   'parent')
        backup_dir = os.path.join(parent_dir, 'backup')
        os.makedirs(backup_dir)
        with open(os.path.join(backup_dir, 'somefile.txt'), 'wb'):
            pass

        with self.assertRaisesRegex(ProbackupException, "ERROR: backup catalog already exist and it's not empty"):
            self.init_pb(backup_dir, cleanup=False)
