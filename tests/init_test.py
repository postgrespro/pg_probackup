import unittest
from sys import exit
import os
from os import path
from .helpers.ptrack_helpers import dir_files, ProbackupTest, ProbackupException
from testgres import stop_all, clean_all
import shutil


class InitTest(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(InitTest, self).__init__(*args, **kwargs)
        self.module_name = 'init'

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_success(self):
        """Success normal init"""
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname))
        self.init_pb(backup_dir)
        self.assertEqual(
            dir_files(backup_dir),
            ['backups', 'wal']
        )
        self.add_instance(backup_dir, 'node', node)
        self.assertEqual("INFO: Instance 'node' successfully deleted\n", self.del_instance(backup_dir, 'node', node),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        try:
            self.show_pb(backup_dir, 'node')
            self.assertEqual(1, 0, 'Expecting Error due to show of non-existing instance. Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(e.message,
                "ERROR: Instance 'node' does not exist in this backup catalog\n",
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(e.message, self.cmd))

        # Clean after yourself
        self.del_test_dir(self.module_name, fname)

    # @unittest.skip("skip")
    def test_already_exist(self):
        """Failure with backup catalog already existed"""
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname))
        self.init_pb(backup_dir)
        try:
            self.show_pb(backup_dir, 'node')
            self.assertEqual(1, 0, 'Expecting Error due to initialization in non-empty directory. Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(e.message,
                "ERROR: Instance 'node' does not exist in this backup catalog\n",
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(self.module_name, fname)

    # @unittest.skip("skip")
    def test_abs_path(self):
        """failure with backup catalog should be given as absolute path"""
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname))
        try:
            self.run_pb(["init", "-B", path.relpath("%s/backup" % node.base_dir, self.dir_path)])
            self.assertEqual(1, 0, 'Expecting Error due to initialization with non-absolute path in --backup-path. Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(e.message,
                "ERROR: -B, --backup-path must be an absolute path\n",
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(self.module_name, fname)
