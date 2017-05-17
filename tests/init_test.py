import unittest
from sys import exit
import os
from os import path
import six
from .ptrack_helpers import dir_files, ProbackupTest, ProbackupException

#TODO 

class InitTest(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(InitTest, self).__init__(*args, **kwargs)

    def test_success_1(self):
        """Success normal init"""
        fname = self.id().split(".")[3]
        node = self.make_simple_node(base_dir="tmp_dirs/init/{0}".format(fname))
        self.assertEqual(self.init_pb(node), six.b(""))
        self.assertEqual(
            dir_files(self.backup_dir(node)),
            ['backups', 'pg_probackup.conf', 'wal']
        )

    def test_already_exist_2(self):
        """Failure with backup catalog already existed"""
        fname = self.id().split(".")[3]
        node = self.make_simple_node(base_dir="tmp_dirs/init/{0}".format(fname))
        self.init_pb(node)
        try:
            self.init_pb(node)
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                e.message,
                "ERROR: backup catalog already exist and it's not empty\n"
                )

    def test_abs_path_3(self):
        """failure with backup catalog should be given as absolute path"""
        fname = self.id().split(".")[3]
        node = self.make_simple_node(base_dir="tmp_dirs/init/{0}".format(fname))
        try:
            self.run_pb(["init", "-B", path.relpath("%s/backup" % node.base_dir, self.dir_path)])
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                e.message,
                "ERROR: -B, --backup-path must be an absolute path\n"
                )


if __name__ == '__main__':
    unittest.main()
