import unittest
import os
import six
from .ptrack_helpers import ProbackupTest, ProbackupException, idx_ptrack
from datetime import datetime, timedelta
from testgres import stop_all
import subprocess
from sys import exit


class ArchiveCheck(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(ArchiveCheck, self).__init__(*args, **kwargs)

    @classmethod
    def tearDownClass(cls):
        stop_all()

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_archive_mode(self):
        """
        https://jira.postgrespro.ru/browse/PGPRO-589
        make node without archive support, make backup which should fail
        check ERROR text
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/pgpro589/{0}/node".format(fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        node.start()

        node.pgbench_init(scale=5)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        path = node.safe_psql("postgres", "select pg_relation_filepath('pgbench_accounts')").rstrip()

        self.assertEqual(self.init_pb(node), six.b(""))

        try:
            self.backup_pb(node, backup_type='full', options=['--archive-timeout=10'])
            assertEqual(1, 0, 'Error is expected because of disabled archive_mode')
        except ProbackupException, e:
            self.assertEqual(e.message, 'ERROR: Archiving must be enabled for archive backup\n')

    def test_pgpro589(self):
        """
        https://jira.postgrespro.ru/browse/PGPRO-589
        make node without archive support, make backup which should fail
        check that backup status equal to ERROR
        check that no files where copied to backup catalogue
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/pgpro589/{0}/node".format(fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        node.append_conf("postgresql.auto.conf", "archive_mode = on")
        node.append_conf("postgresql.auto.conf", "wal_level = archive")
        node.append_conf("postgresql.auto.conf", "archive_command = 'exit 0'")
        node.start()

        node.pgbench_init(scale=5)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        path = node.safe_psql("postgres", "select pg_relation_filepath('pgbench_accounts')").rstrip()
        self.assertEqual(self.init_pb(node), six.b(""))

        try:
            self.backup_pb(
                node, backup_type='full', options=['--archive-timeout=10'])
            assertEqual(1, 0, 'Error is expected because of missing archive wal segment with start_backup() LSN')
        except ProbackupException, e:
            self.assertTrue('INFO: wait for LSN' in e.message, "Expecting 'INFO: wait for LSN'")
            self.assertTrue('ERROR: switched WAL segment' and 'could not be archived' in e.message,
                "Expecting 'ERROR: switched WAL segment could not be archived'")

        id = self.show_pb(node)[0]['ID']
        self.assertEqual('ERROR', self.show_pb(node, id=id)['status'], 'Backup should have ERROR status')
        #print self.backup_dir(node)
        file = os.path.join(self.backup_dir(node), 'backups', id, 'database', path)
        self.assertFalse(os.path.isfile(file),
            '\n Start LSN was not found in archive but datafiles where copied to backup catalogue.\n For example: {0}\n It is not optimal'.format(file))
