import unittest
import os
from time import sleep
from .helpers.ptrack_helpers import ProbackupTest, fs_backup_class
from .helpers.gdb import needs_gdb


class LockingTest(ProbackupTest):

    def setUp(self):
        super().setUp()
        if not self.backup_dir.is_file_based:
            self.skipTest("Locking is not implemented yet for cloud storage")

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    @needs_gdb
    def test_locking_running_validate_1(self):
        """
        make node, take full backup, stop it in the middle
        run validate, expect it to successfully executed,
        concurrent RUNNING backup with pid file and active process is legal
        """

        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node)

        gdb = self.pb.backup_node('node', node, gdb=True)

        gdb.set_breakpoint('backup_non_data_file')
        gdb.run_until_break()

        gdb.continue_execution_until_break(20)

        self.assertEqual(
            'OK', self.pb.show('node')[0]['status'])

        self.assertEqual(
            'RUNNING', self.pb.show('node')[1]['status'])

        validate_output = self.pb.validate(options=['--log-level-console=LOG'])

        backup_id = self.pb.show('node')[1]['id']

        self.assertIn(
            "is using backup {0}, and is still running".format(backup_id),
            validate_output,
            '\n Unexpected Validate Output: {0}\n'.format(repr(validate_output)))

        self.assertEqual(
            'OK', self.pb.show('node')[0]['status'])

        self.assertEqual(
            'RUNNING', self.pb.show('node')[1]['status'])

        # Clean after yourself
        gdb.kill()

    @needs_gdb
    def test_locking_running_validate_2(self):
        """
        make node, take full backup, stop it in the middle,
        kill process so no cleanup is done - pid file is in place,
        run validate, expect it to not successfully executed,
        RUNNING backup with pid file AND without active pid is legal,
        but his status must be changed to ERROR and pid file is deleted
        """

        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node)

        gdb = self.pb.backup_node('node', node, gdb=True)

        gdb.set_breakpoint('backup_non_data_file')
        gdb.run_until_break()

        gdb.continue_execution_until_break(20)

        gdb.signal('SIGKILL')
        gdb.continue_execution_until_error()

        self.assertEqual(
            'OK', self.pb.show('node')[0]['status'])

        self.assertEqual(
            'RUNNING', self.pb.show('node')[1]['status'])

        backup_id = self.pb.show('node')[1]['id']

        self.pb.validate(
                         expect_error="because RUNNING backup is no longer active")
        self.assertMessage(contains=f"which used backup {backup_id} no longer exists")
        self.assertMessage(contains=f"Backup {backup_id} has status RUNNING, change it "
                                    "to ERROR and skip validation")
        self.assertMessage(contains="WARNING: Some backups are not valid")

        self.assertEqual(
            'OK', self.pb.show('node')[0]['status'])

        self.assertEqual(
            'ERROR', self.pb.show('node')[1]['status'])

        # Clean after yourself
        gdb.kill()

    @needs_gdb
    def test_locking_running_validate_2_specific_id(self):
        """
        make node, take full backup, stop it in the middle,
        kill process so no cleanup is done - pid file is in place,
        run validate on this specific backup,
        expect it to not successfully executed,
        RUNNING backup with pid file AND without active pid is legal,
        but his status must be changed to ERROR and pid file is deleted
        """

        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node)

        gdb = self.pb.backup_node('node', node, gdb=True)

        gdb.set_breakpoint('backup_non_data_file')
        gdb.run_until_break()

        gdb.continue_execution_until_break(20)

        gdb.signal('SIGKILL')
        gdb.continue_execution_until_error()

        self.assertEqual(
            'OK', self.pb.show('node')[0]['status'])

        self.assertEqual(
            'RUNNING', self.pb.show('node')[1]['status'])

        backup_id = self.pb.show('node')[1]['id']

        self.pb.validate('node', backup_id, expect_error="because RUNNING backup is no longer active")
        self.assertMessage(contains=f"which used backup {backup_id} no longer exists")
        self.assertMessage(contains=f"Backup {backup_id} has status RUNNING, change it "
                                    "to ERROR and skip validation")
        self.assertMessage(contains=f"ERROR: Backup {backup_id} has status: ERROR")

        self.assertEqual(
            'OK', self.pb.show('node')[0]['status'])

        self.assertEqual(
            'ERROR', self.pb.show('node')[1]['status'])

        self.pb.validate('node', backup_id,
                         expect_error="because backup has status ERROR")
        self.assertMessage(contains=f"ERROR: Backup {backup_id} has status: ERROR")

        self.pb.validate(expect_error="because backup has status ERROR")
        self.assertMessage(contains=f"WARNING: Backup {backup_id} has status ERROR. Skip validation")
        self.assertMessage(contains="WARNING: Some backups are not valid")

        # Clean after yourself
        gdb.kill()

    @needs_gdb
    def test_locking_running_3(self):
        """
        make node, take full backup, stop it in the middle,
        terminate process, delete pid file,
        run validate, expect it to not successfully executed,
        RUNNING backup without pid file AND without active pid is legal,
        his status must be changed to ERROR
        """

        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node)

        gdb = self.pb.backup_node('node', node, gdb=True)

        gdb.set_breakpoint('backup_non_data_file')
        gdb.run_until_break()

        gdb.continue_execution_until_break(20)

        gdb.signal('SIGKILL')
        gdb.continue_execution_until_error()

        self.assertEqual(
            'OK', self.pb.show('node')[0]['status'])

        self.assertEqual(
            'RUNNING', self.pb.show('node')[1]['status'])

        backup_id = self.pb.show('node')[1]['id']

        os.remove(
            os.path.join(backup_dir, 'backups', 'node', backup_id, 'backup.pid'))

        self.pb.validate(
                         expect_error="because RUNNING backup is no longer active")
        self.assertMessage(contains=f"Backup {backup_id} has status RUNNING, change it "
                                    "to ERROR and skip validation")
        self.assertMessage(contains="WARNING: Some backups are not valid")

        self.assertEqual(
            'OK', self.pb.show('node')[0]['status'])

        self.assertEqual(
            'ERROR', self.pb.show('node')[1]['status'])

        # Clean after yourself
        gdb.kill()

    @needs_gdb
    def test_locking_restore_locked(self):
        """
        make node, take full backup, take two page backups,
        launch validate on PAGE1 and stop it in the middle,
        launch restore of PAGE2.
        Expect restore to sucseed because read-only locks
        do not conflict
        """

        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL
        full_id = self.pb.backup_node('node', node)

        # PAGE1
        backup_id = self.pb.backup_node('node', node, backup_type='page')

        # PAGE2
        self.pb.backup_node('node', node, backup_type='page')

        gdb = self.pb.validate('node', backup_id=backup_id, gdb=True)

        gdb.set_breakpoint('pgBackupValidate')
        gdb.run_until_break()

        node.cleanup()

        self.pb.restore_node('node', node=node)

        # Clean after yourself
        gdb.kill()

    @needs_gdb
    def test_concurrent_delete_and_restore(self):
        """
        make node, take full backup, take page backup,
        launch validate on FULL and stop it in the middle,
        launch restore of PAGE.
        Expect restore to fail because validation of
        intermediate backup is impossible
        """

        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL
        backup_id = self.pb.backup_node('node', node)

        # PAGE1
        restore_id = self.pb.backup_node('node', node, backup_type='page')

        gdb = self.pb.delete('node', backup_id=backup_id, gdb=True)

        # gdb.set_breakpoint('pgFileDelete')
        gdb.set_breakpoint('delete_backup_files')
        gdb.run_until_break()

        node.cleanup()

        self.pb.restore_node('node', node=node, options=['--no-validate'],
                          expect_error="because restore without whole chain validation "
                                       "is prohibited unless --no-validate provided.")
        self.assertMessage(contains=f"Backup {restore_id} is used without validation")
        self.assertMessage(contains=f'is using backup {backup_id}, and is still running')
        self.assertMessage(contains='ERROR: Cannot lock backup')

        # Clean after yourself
        gdb.kill()

    @needs_gdb
    def test_locking_concurrent_validate_and_backup(self):
        """
        make node, take full backup, launch validate
        and stop it in the middle, take page backup.
        Expect PAGE backup to be successfully executed
        """

        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL
        self.pb.backup_node('node', node)

        # PAGE2
        backup_id = self.pb.backup_node('node', node, backup_type='page')

        gdb = self.pb.validate('node', backup_id=backup_id, gdb=True)

        gdb.set_breakpoint('pgBackupValidate')
        gdb.run_until_break()

        # This PAGE backup is expected to be successfull
        self.pb.backup_node('node', node, backup_type='page')

        # Clean after yourself
        gdb.kill()

    @needs_gdb
    def test_locking_concurren_restore_and_delete(self):
        """
        make node, take full backup, launch restore
        and stop it in the middle, delete full backup.
        Expect it to fail.
        """

        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL
        full_id = self.pb.backup_node('node', node)

        node.cleanup()
        gdb = self.pb.restore_node('node', node=node, gdb=True)

        gdb.set_breakpoint('create_data_directories')
        gdb.run_until_break()

        self.pb.delete('node', full_id,
                       expect_error="because backup is locked")
        self.assertMessage(contains=f"ERROR: Cannot lock backup {full_id} directory")

        # Clean after yourself
        gdb.kill()

    def test_backup_directory_name(self):
        """
        """
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL
        full_id_1 = self.pb.backup_node('node', node)
        page_id_1 = self.pb.backup_node('node', node, backup_type='page')

        full_id_2 = self.pb.backup_node('node', node)
        page_id_2 = self.pb.backup_node('node', node, backup_type='page')

        node.cleanup()

        old_path = os.path.join(backup_dir, 'backups', 'node', full_id_1)
        new_path = os.path.join(backup_dir, 'backups', 'node', 'hello_kitty')

        os.rename(old_path, new_path)

        # This PAGE backup is expected to be successfull
        self.pb.show('node', full_id_1)

        self.pb.validate()
        self.pb.validate('node')
        self.pb.validate('node', full_id_1)

        self.pb.restore_node('node', node=node, backup_id=full_id_1)

        self.pb.delete('node', full_id_1)

        self.pb.set_backup('node', full_id_2, options=['--note=hello'])

        self.pb.merge_backup('node', page_id_2, options=["-j", "4"])

        self.assertNotIn(
            'note',
            self.pb.show('node', page_id_2))

        # Clean after yourself

    def test_empty_lock_file(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/308
        """
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Fill with data
        node.pgbench_init(scale=100)

        # FULL
        backup_id = self.pb.backup_node('node', node)

        lockfile = os.path.join(backup_dir, 'backups', 'node', backup_id, 'backup.pid')
        with open(lockfile, "w+") as f:
            f.truncate()

        out = self.pb.validate('node', backup_id)

        self.assertIn(
            "Waiting 30 seconds on empty exclusive lock for backup", out)

    @needs_gdb
    def test_shared_lock(self):
        """
        Make sure that shared lock leaves no files with pids
        """

        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Fill with data
        node.pgbench_init(scale=1)

        # FULL
        backup_id = self.pb.backup_node('node', node)

        lockfile_excl = os.path.join(backup_dir, 'backups', 'node', backup_id, 'backup.pid')
        lockfile_shr = os.path.join(backup_dir, 'backups', 'node', backup_id, 'backup_ro.pid')

        self.pb.validate('node', backup_id)

        self.assertFalse(
            os.path.exists(lockfile_excl),
            "File should not exist: {0}".format(lockfile_excl))

        self.assertFalse(
            os.path.exists(lockfile_shr),
            "File should not exist: {0}".format(lockfile_shr))
        
        gdb = self.pb.validate('node', backup_id, gdb=True)

        gdb.set_breakpoint('validate_one_page')
        gdb.run_until_break()
        gdb.kill()

        self.assertTrue(
            os.path.exists(lockfile_shr),
            "File should exist: {0}".format(lockfile_shr))
        
        self.pb.validate('node', backup_id)

        self.assertFalse(
            os.path.exists(lockfile_excl),
            "File should not exist: {0}".format(lockfile_excl))

        self.assertFalse(
            os.path.exists(lockfile_shr),
            "File should not exist: {0}".format(lockfile_shr))

    @needs_gdb
    def test_concurrent_merge_1(self):
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Fill with data
        node.pgbench_init(scale=1)

        full_id = self.pb.backup_node('node', node, backup_type="full")

        pgbench = node.pgbench(options=['-t', '2000', '-c', '2'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '2000', '-c', '2'])
        pgbench.wait()

        prev_id = self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '2000', '-c', '2'])
        pgbench.wait()

        last_id = self.pb.backup_node('node', node, backup_type="page")

        gdb = self.pb.merge_backup('node', prev_id, gdb=True)
        gdb.set_breakpoint("merge_chain")
        gdb.run_until_break()

        self.pb.merge_backup('node', last_id,
                          expect_error="because of concurrent merge")
        self.assertMessage(contains=f"ERROR: Cannot lock backup {full_id}")

    @needs_gdb
    def test_concurrent_merge_2(self):
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Fill with data
        node.pgbench_init(scale=1)

        full_id = self.pb.backup_node('node', node, backup_type="full")

        pgbench = node.pgbench(options=['-t', '2000', '-c', '2'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '2000', '-c', '2'])
        pgbench.wait()

        prev_id = self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '2000', '-c', '2'])
        pgbench.wait()

        last_id = self.pb.backup_node('node', node, backup_type="page")

        gdb = self.pb.merge_backup('node', prev_id, gdb=True)
        # pthread_create will be called after state changed to merging
        gdb.set_breakpoint("merge_files")
        gdb.run_until_break()

        print(self.pb.show('node', as_text=True, as_json=False))
        self.assertEqual(
            'MERGING', self.pb.show('node')[0]['status'])
        self.assertEqual(
            'MERGING', self.pb.show('node')[-2]['status'])

        self.pb.merge_backup('node', last_id,
                          expect_error="because of concurrent merge")
        self.assertMessage(contains=f"ERROR: Full backup {full_id} has unfinished merge")

    @needs_gdb
    def test_concurrent_merge_and_backup_1(self):
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Fill with data
        node.pgbench_init(scale=1)

        full_id = self.pb.backup_node('node', node, backup_type="full")

        pgbench = node.pgbench(options=['-t', '2000', '-c', '2'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '2000', '-c', '2'])
        pgbench.wait()

        prev_id = self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '2000', '-c', '2'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type="page")

        gdb = self.pb.merge_backup('node', prev_id, gdb=True)
        gdb.set_breakpoint("merge_chain")
        gdb.run_until_break()

        pgbench = node.pgbench(options=['-t', '2000', '-c', '2'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type="page")

    @needs_gdb
    def test_concurrent_merge_and_backup_2(self):
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Fill with data
        node.pgbench_init(scale=1)

        full_id = self.pb.backup_node('node', node, backup_type="full")

        pgbench = node.pgbench(options=['-t', '2000', '-c', '2'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '2000', '-c', '2'])
        pgbench.wait()

        prev_id = self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '2000', '-c', '2'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type="page")

        gdb = self.pb.merge_backup('node', prev_id, gdb=True)
        # pthread_create will be called after state changed to merging
        gdb.set_breakpoint("merge_files")
        gdb.run_until_break()

        pgbench = node.pgbench(options=['-t', '2000', '-c', '2'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type="page",
                         expect_error="because of concurrent merge")
        self.assertMessage(
            contains="WARNING: Valid full backup on current timeline 1 "
                     "is not found, trying to look up on previous timelines")
        self.assertMessage(
            contains="WARNING: Cannot find valid backup on previous timelines")
        self.assertMessage(
            contains="ERROR: Create new full backup before an incremental one")
