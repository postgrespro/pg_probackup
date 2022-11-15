import os
import stat
import unittest
import shutil

from .helpers.ptrack_helpers import dir_files, ProbackupTest, ProbackupException


module_name = 'init'

DIR_PERMISSION = 0o700

CATALOG_DIRS = ['backups', 'wal']

class InitTest(ProbackupTest, unittest.TestCase):
    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_success(self):
        """Success normal init"""
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir=os.path.join(module_name, fname, 'node'))
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

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_already_exist(self):
        """Failure with backup catalog already existed"""
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir=os.path.join(module_name, fname, 'node'))
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

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_abs_path(self):
        """failure with backup catalog should be given as absolute path"""
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir=os.path.join(module_name, fname, 'node'))
        try:
            self.run_pb(["init", "-B", os.path.relpath("%s/backup" % node.base_dir, self.dir_path)])
            self.assertEqual(1, 0, 'Expecting Error due to initialization with non-absolute path in --backup-path. Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: -B, --backup-path must be an absolute path",
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_add_instance_idempotence(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/219
        """
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir=os.path.join(module_name, fname, 'node'))
        self.init_pb(backup_dir)

        self.add_instance(backup_dir, 'node', node)
        shutil.rmtree(os.path.join(backup_dir, 'backups', 'node'))

        dir_backups = os.path.join(backup_dir, 'backups', 'node')
        dir_wal = os.path.join(backup_dir, 'wal', 'node')

        try:
            self.add_instance(backup_dir, 'node', node)
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because page backup should not be possible "
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Instance 'node' WAL archive directory already exists: ",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        try:
            self.add_instance(backup_dir, 'node', node)
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because page backup should not be possible "
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Instance 'node' WAL archive directory already exists: ",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_init_backup_catalog_no_access(self):
        """ Test pg_probackup init -B backup_dir to a dir with no read access. """
        fname = self.id().split('.')[3]

        no_access_dir = os.path.join(self.tmp_path, module_name, fname,
                                   'noaccess')
        backup_dir = os.path.join(no_access_dir, 'backup')
        os.makedirs(no_access_dir)
        os.chmod(no_access_dir, stat.S_IREAD)

        try:
            self.init_pb(backup_dir, cleanup=False)
        except ProbackupException as e:
            self.assertEqual(f'ERROR: cannot open backup catalog directory "{backup_dir}": Permission denied\n',
                             e.message)
        finally:
            self.del_test_dir(module_name, fname)

    def test_init_backup_catalog_no_write(self):
        """ Test pg_probackup init -B backup_dir to a dir with no write access. """
        fname = self.id().split('.')[3]

        no_access_dir = os.path.join(self.tmp_path, module_name, fname,
                                   'noaccess')
        backup_dir = os.path.join(no_access_dir, 'backup')
        os.makedirs(no_access_dir)
        os.chmod(no_access_dir, stat.S_IREAD|stat.S_IEXEC)

        try:
            self.init_pb(backup_dir, cleanup=False)
        except ProbackupException as e:
            self.assertEqual(f'ERROR: Can not create backup catalog root directory: Cannot make dir "{backup_dir}": Permission denied\n',
                             e.message)
        finally:
            self.del_test_dir(module_name, fname)

    def test_init_backup_catalog_no_create(self):
        """ Test pg_probackup init -B backup_dir to a dir when backup dir exists but not writeable. """
        fname = self.id().split('.')[3]

        parent_dir = os.path.join(self.tmp_path, module_name, fname,
                                   'parent')
        backup_dir = os.path.join(parent_dir, 'backup')
        os.makedirs(backup_dir)
        os.chmod(backup_dir, stat.S_IREAD|stat.S_IEXEC)

        try:
            self.init_pb(backup_dir, cleanup=False)
        except ProbackupException as e:
            backups_dir = os.path.join(backup_dir, 'backups')
            self.assertEqual(f'ERROR: Can not create backup catalog data directory: Cannot make dir "{backups_dir}": Permission denied\n',
                             e.message)
        finally:
            self.del_test_dir(module_name, fname)

    def test_init_backup_catalog_exists_not_empty(self):
        """ Test pg_probackup init -B backup_dir which exists and not empty. """
        fname = self.id().split('.')[3]

        parent_dir = os.path.join(self.tmp_path, module_name, fname,
                                   'parent')
        backup_dir = os.path.join(parent_dir, 'backup')
        os.makedirs(backup_dir)
        with open(os.path.join(backup_dir, 'somefile.txt'), 'w') as fout:
            fout.write("42\n")

        try:
            self.init_pb(backup_dir, cleanup=False)
            self.fail("This should have failed due to non empty catalog dir.")
        except ProbackupException as e:
            self.assertEqual("ERROR: backup catalog already exist and it's not empty\n",
                             e.message)
        finally:
            self.del_test_dir(module_name, fname)
