import unittest
import os
import six
from .ptrack_helpers import ProbackupTest, ProbackupException, idx_ptrack
from datetime import datetime, timedelta
from testgres import stop_all
import subprocess
from sys import exit


class LsnCheck(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(LsnCheck, self).__init__(*args, **kwargs)

#    @classmethod
#    def tearDownClass(cls):
#        stop_all()
#    @unittest.expectedFailure
    def test_pgpro589(self):
        """
        https://jira.postgrespro.ru/browse/PGPRO-589
        make node without archive support, make backup which should fail
        check that no files where copied to backup catalogue
        EXPECTED TO FAIL
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
        proc = self.backup_pb(
            node, backup_type='full', options=['--archive-timeout=1'], async=True)

        content = proc.stderr.read()
        self.assertEqual(True, 'wait for LSN' in repr(content),
            'No Wait for LSN')
        self.assertEqual(True, 'could not be archived' in repr(content),
            'No Fail Archiving Message')

        id = self.show_pb(node)[0]['ID']
        self.assertEqual('ERROR', self.show_pb(node, id=id)['status'], 'Backup should have ERROR status')
        #print self.backup_dir(node)
        file = os.path.join(self.backup_dir(node), 'backups', id, 'database', path)
        self.assertEqual(False, os.path.isfile(file),
            '\n Start LSN was not found in archive but datafiles where copied to backup catalogue.\n For example: {0}\n It is not optimal'.format(file))
