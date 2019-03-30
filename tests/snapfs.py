import unittest
import os
from time import sleep
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException


module_name = 'snapfs'


class SnapFSTest(ProbackupTest, unittest.TestCase):

    # @unittest.expectedFailure
    @unittest.skipUnless(ProbackupTest.enterprise, 'skip')
    def test_snapfs_simple(self):
        """standart backup modes with ARCHIVE WAL method"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            'postgres',
            'select pg_make_snapshot()')

        node.pgbench_init(scale=10)

        pgbench = node.pgbench(options=['-T', '50', '-c', '2', '--no-vacuum'])
        pgbench.wait()

        self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        node.safe_psql(
            'postgres',
            'select pg_remove_snapshot(1)')

        self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        pgdata = self.pgdata_content(node.data_dir)

        node.cleanup()

        self.restore_node(
            backup_dir, 'node',
            node, options=["-j", "4"])

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)
