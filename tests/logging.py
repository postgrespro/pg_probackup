import unittest
import os
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
import datetime

module_name = 'logging'


class LogTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    # PGPRO-2154
    def test_log_rotation(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        self.set_config(
            backup_dir, 'node',
            options=['--log-rotation-age=1s', '--log-rotation-size=1MB'])

        self.backup_node(
            backup_dir, 'node', node,
            options=['--stream', '--log-level-file=verbose'])

        gdb = self.backup_node(
            backup_dir, 'node', node,
            options=['--stream', '--log-level-file=verbose'], gdb=True)

        gdb.set_breakpoint('open_logfile')
        gdb.run_until_break()
        gdb.continue_execution_until_exit()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_log_filename_strftime(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        self.set_config(
            backup_dir, 'node',
            options=['--log-rotation-age=1d'])

        self.backup_node(
            backup_dir, 'node', node,
            options=[
                '--stream',
                '--log-level-file=VERBOSE',
                '--log-filename=pg_probackup-%a.log'])

        day_of_week = datetime.datetime.today().strftime("%a")

        path = os.path.join(
            backup_dir, 'log', 'pg_probackup-{0}.log'.format(day_of_week))

        self.assertTrue(os.path.isfile(path))

        # Clean after yourself
        self.del_test_dir(module_name, fname)
