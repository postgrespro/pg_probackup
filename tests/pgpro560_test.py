import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest
from datetime import datetime, timedelta
import subprocess
from time import sleep


class CheckSystemID(ProbackupTest):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_pgpro560_control_file_loss(self):
        """
        https://jira.postgrespro.ru/browse/PGPRO-560
        make node with stream support, delete control file
        make backup
        check that backup failed
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        file = os.path.join(node.base_dir, 'data', 'global', 'pg_control')
        # Not delete this file permanently
        os.rename(file, os.path.join(node.base_dir, 'data', 'global', 'pg_control_copy'))

        self.pb.backup_node('node', node, options=['--stream'],
                         expect_error='because pg_control was deleted')
        self.assertMessage(regex=r'ERROR: Could not get control file:.*pg_control')

        # Return this file to avoid Postger fail
        os.rename(os.path.join(node.base_dir, 'data', 'global', 'pg_control_copy'), file)

    def test_pgpro560_systemid_mismatch(self):
        """
        https://jira.postgrespro.ru/browse/PGPRO-560
        make node1 and node2
        feed to backup PGDATA from node1 and PGPORT from node2
        check that backup failed
        """
        node1 = self.pg_node.make_simple('node1',
            set_replication=True)

        node1.slow_start()
        node2 = self.pg_node.make_simple('node2',
            set_replication=True)

        node2.slow_start()

        self.pb.init()
        self.pb.add_instance('node1', node1)

        self.pb.backup_node('node1', node2, options=['--stream'],
                         expect_error="because of SYSTEM ID mismatch")
        self.assertMessage(contains='ERROR: Backup data directory was '
                                    'initialized for system id')
        self.assertMessage(contains='but connected instance system id is')

        sleep(1)

        self.pb.backup_node('node1', node2,
                         data_dir=node1.data_dir, options=['--stream'],
                         expect_error="because of of SYSTEM ID mismatch")
        self.assertMessage(contains='ERROR: Backup data directory was '
                                    'initialized for system id')
        self.assertMessage(contains='but connected instance system id is')
