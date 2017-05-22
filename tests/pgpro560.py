import unittest
import os
import six
from .ptrack_helpers import ProbackupTest, ProbackupException, idx_ptrack
from datetime import datetime, timedelta
from testgres import stop_all
import subprocess
from sys import exit


class CheckSystemID(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(CheckSystemID, self).__init__(*args, **kwargs)

    @classmethod
    def tearDownClass(cls):
        stop_all()

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_pgpro560_control_file_loss(self):
        """
        https://jira.postgrespro.ru/browse/PGPRO-560
        make node with stream support, delete control file
        make backup
        check that backup failed
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/pgpro560/{0}/node".format(fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        node.start()

        self.assertEqual(self.init_pb(node), six.b(""))
        file = os.path.join(node.base_dir,'data', 'global', 'pg_control')
        os.remove(file)

        try:
            self.backup_pb(node, backup_type='full', options=['--stream'])
            assertEqual(1, 0, 'Error is expected because of control file loss')
        except ProbackupException, e:
            self.assertTrue(
                'ERROR: could not open file' and 'pg_control' in e.message,
                'Expected error is about control file loss')

    def test_pgpro560_systemid_mismatch(self):
        """
        https://jira.postgrespro.ru/browse/PGPRO-560
        make node1 and node2
        feed to backup PGDATA from node1 and PGPORT from node2
        check that backup failed
        """
        fname = self.id().split('.')[3]
        node1 = self.make_simple_node(base_dir="tmp_dirs/pgpro560/{0}/node1".format(fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        node1.start()
        node2 = self.make_simple_node(base_dir="tmp_dirs/pgpro560/{0}/node2".format(fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        node2.start()
        self.assertEqual(self.init_pb(node1), six.b(""))

        try:
            self.backup_pb(node1, data_dir=node2.data_dir, backup_type='full', options=['--stream'])
            assertEqual(1, 0, 'Error is expected because of SYSTEM ID mismatch')
        except ProbackupException, e:
            self.assertTrue(
                'ERROR: Backup data directory was initialized for system id' and
                'but target backup directory system id is' in e.message,
                'Expected error is about SYSTEM ID mismatch')
