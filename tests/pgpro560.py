import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from datetime import datetime, timedelta
import subprocess
from time import sleep


class CheckSystemID(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_pgpro560_control_file_loss(self):
        """
        https://jira.postgrespro.ru/browse/PGPRO-560
        make node with stream support, delete control file
        make backup
        check that backup failed
        """
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        file = os.path.join(node.base_dir, 'data', 'global', 'pg_control')
        # Not delete this file permanently
        os.rename(file, os.path.join(node.base_dir, 'data', 'global', 'pg_control_copy'))

        try:
            self.backup_node(backup_dir, 'node', node, options=['--stream'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
               1, 0,
               "Expecting Error because pg_control was deleted.\n "
               "Output: {0} \n CMD: {1}".format(repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'ERROR: Could not open file' in e.message and
                'pg_control' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        # Return this file to avoid Postger fail
        os.rename(os.path.join(node.base_dir, 'data', 'global', 'pg_control_copy'), file)
        self.del_test_dir(self.module_name, self.fname)

    def test_pgpro560_systemid_mismatch(self):
        """
        https://jira.postgrespro.ru/browse/PGPRO-560
        make node1 and node2
        feed to backup PGDATA from node1 and PGPORT from node2
        check that backup failed
        """
        node1 = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node1'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        node1.slow_start()
        node2 = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node2'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        node2.slow_start()

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node1', node1)

        try:
            self.backup_node(backup_dir, 'node1', node2, options=['--stream'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because of SYSTEM ID mismatch.\n "
                "Output: {0} \n CMD: {1}".format(repr(self.output), self.cmd))
        except ProbackupException as e:
            if self.get_version(node1) > 90600:
                self.assertTrue(
                    'ERROR: Backup data directory was '
                    'initialized for system id' in e.message and
                    'but connected instance system id is' in e.message,
                    '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                        repr(e.message), self.cmd))
            else:
                self.assertIn(
                    'ERROR: System identifier mismatch. '
                    'Connected PostgreSQL instance has system id',
                    e.message,
                    '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                        repr(e.message), self.cmd))

        sleep(1)

        try:
            self.backup_node(
                backup_dir, 'node1', node2,
                data_dir=node1.data_dir, options=['--stream'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because of of SYSTEM ID mismatch.\n "
                "Output: {0} \n CMD: {1}".format(repr(self.output), self.cmd))
        except ProbackupException as e:
            if self.get_version(node1) > 90600:
                self.assertTrue(
                    'ERROR: Backup data directory was initialized '
                    'for system id' in e.message and
                    'but connected instance system id is' in e.message,
                    '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                        repr(e.message), self.cmd))
            else:
                self.assertIn(
                    'ERROR: System identifier mismatch. '
                    'Connected PostgreSQL instance has system id',
                    e.message,
                    '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                        repr(e.message), self.cmd))
