import unittest
import subprocess
import os
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException, archive_script
from sys import exit

module_name = 'compatibility'


class CompatibilityTest(ProbackupTest, unittest.TestCase):

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_show_previous_version_catalog(self):
        """Description in jira issue PGPRO-434"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'max_wal_senders': '2',
                'checkpoint_timeout': '30s'}
            )
        self.init_pb(backup_dir, old_binary=True)
        self.show_pb(backup_dir)

        self.add_instance(backup_dir, 'node', node, old_binary=True)
        self.show_pb(backup_dir)

        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=10)

        # FULL backup
        self.backup_node(
            backup_dir, 'node', node, old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.show_pb(backup_dir)
        self.validate_pb(backup_dir)

        # RESTORE
        node_restored = self.make_simple_node(
            base_dir="{0}/{1}/node_restored".format(module_name, fname))

        node_restored.cleanup()

        self.restore_node(
                backup_dir, 'node', node_restored,
                options=["-j", "4", "--recovery-target-action=promote"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Incremental BACKUP
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )

        self.backup_node(
            backup_dir, 'node', node, backup_type='page',
            old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.restore_node(
            backup_dir, 'node', node_restored,
            options=["-j", "4", "--recovery-target-action=promote"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Incremental BACKUP with new binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='page')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.restore_node(
            backup_dir, 'node', node_restored,
            options=["-j", "4", "--recovery-target-action=promote"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)
