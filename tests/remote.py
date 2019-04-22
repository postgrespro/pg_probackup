import unittest
import os
from time import sleep
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from .helpers.cfs_helpers import find_by_name


module_name = 'remote'


class RemoteTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_remote_1(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
#        self.set_archiving(backup_dir, 'node', node, remote=True)
        node.slow_start()

        self.backup_node(
            backup_dir, 'node', node,
            options=['--remote-proto=ssh', '--remote-host=localhost', '--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        node.cleanup()

        self.restore_node(
            backup_dir, 'node', node,
            options=[
                '--remote-proto=ssh',
                '--remote-host=localhost'])

        # Physical comparison
        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)
