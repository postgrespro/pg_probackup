import unittest
import subprocess
import os
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from sys import exit

module_name = 'config'


class ConfigTest(ProbackupTest, unittest.TestCase):

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_remove_instance_config(self):
        """remove pg_probackup.conf"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.show_pb(backup_dir)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(backup_dir, 'node', node)

        self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        conf_file = os.path.join(
            backup_dir, 'backups','node', 'pg_probackup.conf')

        os.unlink(os.path.join(backup_dir, 'backups','node', 'pg_probackup.conf'))

        try:
            self.backup_node(
                backup_dir, 'node', node, backup_type='page')
            self.assertEqual(
                    1, 0,
                    "Expecting Error because pg_probackup.conf is missing. "
                    ".\n Output: {0} \n CMD: {1}".format(
                        repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: could not open file "{0}": '
                'No such file or directory'.format(conf_file),
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))
