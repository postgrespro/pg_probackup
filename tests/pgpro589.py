import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException, idx_ptrack
from datetime import datetime, timedelta
import subprocess


module_name = 'pgpro589'


class ArchiveCheck(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_archive_mode(self):
        """
        https://jira.postgrespro.ru/browse/PGPRO-589
        make node without archive support, make backup which should fail
        check ERROR text
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.start()

        node.pgbench_init(scale=5)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        try:
            self.backup_node(backup_dir, 'node', node, options=['--archive-timeout=10'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because of disabled archive_mode.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(e.message, 'ERROR: Archiving must be enabled for archive backup\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_pgpro589(self):
        """
        https://jira.postgrespro.ru/browse/PGPRO-589
        make node without archive support, make backup which should fail
        check that backup status equal to ERROR
        check that no files where copied to backup catalogue
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)

        # make erroneus archive_command
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
        path = node.safe_psql("postgres", "select pg_relation_filepath('pgbench_accounts')").rstrip().decode("utf-8")

        try:
            self.backup_node(backup_dir, 'node', node, options=['--archive-timeout=10'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because of missing archive wal segment with start_lsn.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'INFO: wait for WAL segment' in e.message
                 and 'ERROR: switched WAL segment' in e.message
                 and 'could not be archived' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        backup_id = self.show_pb(backup_dir, 'node')[0]['ID']
        self.assertEqual('ERROR', self.show_pb(backup_dir, 'node', backup_id)['status'], 'Backup should have ERROR status')
        file = os.path.join(backup_dir, 'backups', 'node', backup_id, 'database', path)
        self.assertFalse(os.path.isfile(file),
            '\n Start LSN was not found in archive but datafiles where copied to backup catalogue.\n For example: {0}\n It is not optimal'.format(file))

        # Clean after yourself
        self.del_test_dir(module_name, fname)
