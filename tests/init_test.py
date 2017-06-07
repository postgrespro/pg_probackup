import unittest
from sys import exit
import os
from os import path
import six
from helpers.ptrack_helpers import dir_files, ProbackupTest, ProbackupException


class InitTest(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(InitTest, self).__init__(*args, **kwargs)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_success(self):
        """Success normal init"""
        fname = self.id().split(".")[3]
        node = self.make_simple_node(base_dir="tmp_dirs/init/{0}".format(fname))
        self.assertEqual(self.init_pb(node), six.b(""))
        self.assertEqual(
            dir_files(self.backup_dir(node)),
            ['backups', 'wal']
        )
        self.add_instance(node=node, instance='test')

        self.assertEqual("INFO: Instance 'test' deleted successfully\n",
            self.del_instance(node=node, instance='test'),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        try:
            self.show_pb(node, instance='test')
            self.assertEqual(1, 0, 'Expecting Error due to show of non-existing instance. Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertEqual(e.message,
                "ERROR: Instance 'test' does not exist in this backup catalog\n",
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(e.message, self.cmd))

    def test_already_exist(self):
        """Failure with backup catalog already existed"""
        fname = self.id().split(".")[3]
        node = self.make_simple_node(base_dir="tmp_dirs/init/{0}".format(fname))
        self.init_pb(node)
        try:
            self.init_pb(node)
            self.assertEqual(1, 0, 'Expecting Error due to initialization in non-empty directory. Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertEqual(e.message,
                "ERROR: backup catalog already exist and it's not empty\n",
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

    def test_abs_path(self):
        """failure with backup catalog should be given as absolute path"""
        fname = self.id().split(".")[3]
        node = self.make_simple_node(base_dir="tmp_dirs/init/{0}".format(fname))
        try:
            self.run_pb(["init", "-B", path.relpath("%s/backup" % node.base_dir, self.dir_path)])
            self.assertEqual(1, 0, 'Expecting Error due to initialization with non-absolute path in --backup-path. Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertEqual(e.message,
                "ERROR: -B, --backup-path must be an absolute path\n",
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))


if __name__ == '__main__':
    unittest.main()
