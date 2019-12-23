import unittest
import subprocess
import os
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from sys import exit

module_name = 'compatibility'


class CompatibilityTest(ProbackupTest, unittest.TestCase):

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_backward_compatibility_page(self):
        """Description in jira issue PGPRO-434"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir, old_binary=True)
        self.show_pb(backup_dir)

        self.add_instance(backup_dir, 'node', node, old_binary=True)
        self.show_pb(backup_dir)

        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=10)

        # FULL backup with old binary
        self.backup_node(
            backup_dir, 'node', node, old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.show_pb(backup_dir)

        self.validate_pb(backup_dir)

        # RESTORE old FULL with new binary
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))

        node_restored.cleanup()

        self.restore_node(
                backup_dir, 'node', node_restored, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Page BACKUP with old binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "20"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node, backup_type='page',
            old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Page BACKUP with new binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "20"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_backward_compatibility_delta(self):
        """Description in jira issue PGPRO-434"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir, old_binary=True)
        self.show_pb(backup_dir)

        self.add_instance(backup_dir, 'node', node, old_binary=True)
        self.show_pb(backup_dir)

        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=10)

        # FULL backup with old binary
        self.backup_node(
            backup_dir, 'node', node, old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.show_pb(backup_dir)

        self.validate_pb(backup_dir)

        # RESTORE old FULL with new binary
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Delta BACKUP with old binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "20"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node, backup_type='delta',
            old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Delta BACKUP with new binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "20"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_backward_compatibility_ptrack(self):
        """Description in jira issue PGPRO-434"""

        if not self.ptrack:
            return unittest.skip('Skipped because ptrack support is disabled')

        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir, old_binary=True)
        self.show_pb(backup_dir)

        self.add_instance(backup_dir, 'node', node, old_binary=True)
        self.show_pb(backup_dir)

        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=10)

        # FULL backup with old binary
        self.backup_node(
            backup_dir, 'node', node, old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.show_pb(backup_dir)

        self.validate_pb(backup_dir)

        # RESTORE old FULL with new binary
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # ptrack BACKUP with old binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "20"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack',
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

        # Ptrack BACKUP with new binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "20"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=["-j", "4", "--recovery-target-action=promote"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_backward_compatibility_compression(self):
        """Description in jira issue PGPRO-434"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir, old_binary=True)
        self.add_instance(backup_dir, 'node', node, old_binary=True)

        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=10)

        # FULL backup with OLD binary
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            old_binary=True,
            options=['--compress'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # restore OLD FULL with new binary
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))

        node_restored.cleanup()

        self.restore_node(
                backup_dir, 'node', node_restored,
                options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # PAGE backup with OLD binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"])
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='page',
            old_binary=True,
            options=['--compress'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.restore_node(
            backup_dir, 'node', node_restored,
            options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # PAGE backup with new binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"])
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='page',
            options=['--compress'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Delta backup with old binary
        self.delete_pb(backup_dir, 'node', backup_id)

        self.backup_node(
            backup_dir, 'node', node,
            old_binary=True,
            options=['--compress'])

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"])

        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta',
            options=['--compress'],
            old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Delta backup with new binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"])

        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta',
            options=['--compress'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_backward_compatibility_merge(self):
        """
        Create node, take FULL and PAGE backups with old binary,
        merge them with new binary
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir, old_binary=True)
        self.add_instance(backup_dir, 'node', node, old_binary=True)

        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        # FULL backup with OLD binary
        self.backup_node(
            backup_dir, 'node', node,
            old_binary=True)

        node.pgbench_init(scale=1)

        # PAGE backup with OLD binary
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.merge_backup(backup_dir, "node", backup_id)

        print(self.show_pb(backup_dir, as_text=True, as_json=False))

        # restore OLD FULL with new binary
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)
