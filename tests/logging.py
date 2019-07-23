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
            set_replication=True,
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
            set_replication=True,
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

    def test_truncate_rotation_file(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
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
                '--log-level-file=VERBOSE'])

        rotation_file_path = os.path.join(
            backup_dir, 'log', 'pg_probackup.log.rotation')

        log_file_path = os.path.join(
            backup_dir, 'log', 'pg_probackup.log')

        log_file_size = os.stat(log_file_path).st_size

        self.assertTrue(os.path.isfile(rotation_file_path))

        # truncate .rotation file
        with open(rotation_file_path, "rb+", 0) as f:
            f.truncate()
            f.flush()
            f.close

        output = self.backup_node(
            backup_dir, 'node', node,
            options=[
                '--stream',
                '--log-level-file=LOG'],
            return_id=False)

        # check that log file wasn`t rotated
        self.assertGreater(
            os.stat(log_file_path).st_size,
            log_file_size)

        self.assertIn(
            'WARNING: cannot read creation timestamp from rotation file',
            output)

        output = self.backup_node(
            backup_dir, 'node', node,
            options=[
                '--stream',
                '--log-level-file=LOG'],
            return_id=False)

        # check that log file wasn`t rotated
        self.assertGreater(
            os.stat(log_file_path).st_size,
            log_file_size)

        self.assertNotIn(
            'WARNING: cannot read creation timestamp from rotation file',
            output)

        self.assertTrue(os.path.isfile(rotation_file_path))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_unlink_rotation_file(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
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
                '--log-level-file=VERBOSE'])

        rotation_file_path = os.path.join(
            backup_dir, 'log', 'pg_probackup.log.rotation')

        log_file_path = os.path.join(
            backup_dir, 'log', 'pg_probackup.log')

        log_file_size = os.stat(log_file_path).st_size

        self.assertTrue(os.path.isfile(rotation_file_path))

        # unlink .rotation file
        os.unlink(rotation_file_path)

        output = self.backup_node(
            backup_dir, 'node', node,
            options=[
                '--stream',
                '--log-level-file=LOG'],
            return_id=False)

        # check that log file wasn`t rotated
        self.assertGreater(
            os.stat(log_file_path).st_size,
            log_file_size)

        self.assertIn(
            'WARNING: missing rotation file:',
            output)

        self.assertTrue(os.path.isfile(rotation_file_path))

        output = self.backup_node(
            backup_dir, 'node', node,
            options=[
                '--stream',
                '--log-level-file=VERBOSE'],
            return_id=False)

        self.assertNotIn(
            'WARNING: missing rotation file:',
            output)

        # check that log file wasn`t rotated
        self.assertGreater(
            os.stat(log_file_path).st_size,
            log_file_size)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_garbage_in_rotation_file(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
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
                '--log-level-file=VERBOSE'])

        rotation_file_path = os.path.join(
            backup_dir, 'log', 'pg_probackup.log.rotation')

        log_file_path = os.path.join(
            backup_dir, 'log', 'pg_probackup.log')

        log_file_size = os.stat(log_file_path).st_size

        self.assertTrue(os.path.isfile(rotation_file_path))

        # mangle .rotation file
        with open(rotation_file_path, "wt", 0) as f:
            f.write(b"blah")
            f.flush()
            f.close

        output = self.backup_node(
            backup_dir, 'node', node,
            options=[
                '--stream',
                '--log-level-file=LOG'],
            return_id=False)

        # check that log file wasn`t rotated
        self.assertGreater(
            os.stat(log_file_path).st_size,
            log_file_size)

        self.assertIn(
            'WARNING: rotation file',
            output)

        self.assertIn(
            'has wrong creation timestamp',
            output)

        self.assertTrue(os.path.isfile(rotation_file_path))

        output = self.backup_node(
            backup_dir, 'node', node,
            options=[
                '--stream',
                '--log-level-file=LOG'],
            return_id=False)

        self.assertNotIn(
            'WARNING: rotation file',
            output)

        # check that log file wasn`t rotated
        self.assertGreater(
            os.stat(log_file_path).st_size,
            log_file_size)

        # Clean after yourself
        self.del_test_dir(module_name, fname)
