import unittest
import os
from time import sleep
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException


module_name = 'locking'


class LockingTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_locking_simple(self):
        """"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(backup_dir, 'node', node)

        gdb = self.backup_node(
            backup_dir, 'node', node, gdb=True)

        gdb.set_breakpoint('copy_file')
        gdb.run_until_break()

        if gdb.continue_execution_until_break(20) != 'breakpoint-hit':
            self.AssertTrue(False, 'Failed to hit breakpoint')

        gdb._execute('signal SIGKILL')

        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'node')[0]['status'])

        self.assertEqual(
            'ERROR', self.show_pb(backup_dir, 'node')[1]['status'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)