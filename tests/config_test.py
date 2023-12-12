import unittest
import subprocess
import os
from .helpers.ptrack_helpers import ProbackupTest
from sys import exit
from shutil import copyfile


class ConfigTest(ProbackupTest):

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_remove_instance_config(self):
        """remove pg_probackup.conself.f"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.show()
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node)

        self.pb.backup_node('node', node, backup_type='page')

        self.remove_backup_config(backup_dir, 'node')

        self.pb.backup_node('node', node, backup_type='page',
                         expect_error="because pg_probackup.conf is missing")
        self.assertMessage(regex=r'ERROR: Reading instance control.*No such file')

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_corrupt_backup_content(self):
        """corrupt backup_content.control"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        full1_id = self.pb.backup_node('node', node)

        node.safe_psql(
            'postgres',
            'create table t1()')

        fulle2_id = self.pb.backup_node('node', node)

        content = self.read_backup_file(backup_dir, 'node', fulle2_id,
                                        'backup_content.control')
        self.write_backup_file(backup_dir, 'node', full1_id,
                               'backup_content.control', content)

        self.pb.validate('node',
                         expect_error="because pg_probackup.conf is missing")
        self.assertMessage(regex="WARNING: Invalid CRC of backup control file "
                                 fr".*{full1_id}")
        self.assertMessage(contains=f"WARNING: Failed to get file list for backup {full1_id}")
        self.assertMessage(contains=f"WARNING: Backup {full1_id} file list is corrupted")

        self.pb.show('node', full1_id)['status']

        self.assertEqual(self.pb.show('node')[0]['status'], 'CORRUPT')
        self.assertEqual(self.pb.show('node')[1]['status'], 'OK')
