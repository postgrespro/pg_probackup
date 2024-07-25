import unittest
import subprocess
import os
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from sys import exit
from shutil import copyfile


class ConfigTest(ProbackupTest, unittest.TestCase):

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_remove_instance_config(self):
        """remove pg_probackup.conself.f"""
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
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

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_corrupt_backup_content(self):
        """corrupt backup_content.control"""
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        full1_id = self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            'postgres',
            'create table t1()')

        full2_id = self.backup_node(backup_dir, 'node', node)

        full1_conf_file = os.path.join(
            backup_dir, 'backups','node', full1_id, 'backup_content.control')

        full2_conf_file = os.path.join(
            backup_dir, 'backups','node', full2_id, 'backup_content.control')

        copyfile(full2_conf_file, full1_conf_file)

        try:
            self.validate_pb(backup_dir, 'node')
            self.assertEqual(
                    1, 0,
                    "Expecting Error because pg_probackup.conf is missing. "
                    ".\n Output: {0} \n CMD: {1}".format(
                        repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "WARNING: Invalid CRC of backup control file '{0}':".format(full1_conf_file),
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

            self.assertIn(
                "WARNING: Failed to get file list for backup {0}".format(full1_id),
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

            self.assertIn(
                "WARNING: Backup {0} file list is corrupted".format(full1_id),
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        self.assertEqual(self.show_pb(backup_dir, 'node', full1_id)['status'], 'CORRUPT')
        self.assertEqual(self.show_pb(backup_dir, 'node', full2_id)['status'], 'OK')

        self.assertEqual(self.show_pb(backup_dir, 'node')[0]['status'], 'CORRUPT')
        self.assertEqual(self.show_pb(backup_dir, 'node')[1]['status'], 'OK')
