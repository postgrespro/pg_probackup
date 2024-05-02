import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, idx_ptrack
from datetime import datetime, timedelta
import subprocess


class ArchiveCheck(ProbackupTest):

    def test_pgpro589(self):
        """
        https://jira.postgrespro.ru/browse/PGPRO-589
        make node without archive support, make backup which should fail
        check that backup status equal to ERROR
        check that no files where copied to backup catalogue
        """
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)

        # make erroneous archive_command
        node.set_auto_conf({'archive_command': 'exit 0'})
        node.slow_start()

        node.pgbench_init(scale=5)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )
        pgbench.wait()
        pgbench.stdout.close()
        path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('pgbench_accounts')").rstrip().decode(
            "utf-8")

        self.pb.backup_node('node', node,
                         options=['--archive-timeout=10'],
                         expect_error="because of missing archive wal segment "
                                      "with start_lsn")
        self.assertMessage(contains='INFO: Wait for WAL segment')
        self.assertMessage(regex='ERROR: WAL segment .* could not be archived in 10 seconds')

        backup_id = self.pb.show('node')[0]['id']
        self.assertEqual(
            'ERROR', self.pb.show('node', backup_id)['status'],
            'Backup should have ERROR status')
        file = os.path.join(
            backup_dir, 'backups', 'node',
            backup_id, 'database', path)
        self.assertFalse(
            os.path.isfile(file),
            "\n Start LSN was not found in archive but datafiles where "
            "copied to backup catalogue.\n For example: {0}\n "
            "It is not optimal".format(file))
