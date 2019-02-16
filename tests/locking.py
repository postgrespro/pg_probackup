import unittest
import os
from time import sleep
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException


module_name = 'locking'


class LockingTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_locking_running_1(self):
        """ 
        make node, take full backup, stop it in the middle
        run validate, expect it to successfully executed,
        concurrect RUNNING backup with pid file and active process is legal
        """
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

        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'node')[0]['status'])

        self.assertEqual(
            'RUNNING', self.show_pb(backup_dir, 'node')[1]['status'])

        self.validate_pb(backup_dir)

        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'node')[0]['status'])

        self.assertEqual(
            'ERROR', self.show_pb(backup_dir, 'node')[1]['status'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_locking_running_2(self):
        """ 
        make node, take full backup, stop it in the middle,
        kill process so no cleanup is done - pid file is in place,
        run validate, expect it to not successfully executed,
        RUNNING backup with pid file AND without active pid is legal,
        but his status must be changed to ERROR and pid file is deleted
        """
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
        gdb.continue_execution_until_running()

        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'node')[0]['status'])

        self.assertEqual(
            'RUNNING', self.show_pb(backup_dir, 'node')[1]['status'])

        try:
            self.validate_pb(backup_dir)
        except:
            pass

        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'node')[0]['status'])

        self.assertEqual(
            'ERROR', self.show_pb(backup_dir, 'node')[1]['status'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_locking_running_3(self):
        """ 
        make node, take full backup, stop it in the middle,
        terminate process, delete pid file,
        run validate, expect it to not successfully executed,
        RUNNING backup without pid file AND without active pid is legal,
        his status must be changed to ERROR
        """
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
        gdb.continue_execution_until_running()

        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'node')[0]['status'])

        self.assertEqual(
            'RUNNING', self.show_pb(backup_dir, 'node')[1]['status'])

        backup_id = self.show_pb(backup_dir, 'node')[1]['id']

        os.remove(
            os.path.join(backup_dir, 'backups', 'node', backup_id, 'backup.pid'))

        try:
            self.validate_pb(backup_dir)
        except:
            pass

        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'node')[0]['status'])

        self.assertEqual(
            'ERROR', self.show_pb(backup_dir, 'node')[1]['status'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)