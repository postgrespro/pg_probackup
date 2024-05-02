import re
import time
import unittest
import os
from .helpers.ptrack_helpers import ProbackupTest, fs_backup_class
from pg_probackup2.gdb import needs_gdb


class LockingTest(ProbackupTest):

    def setUp(self):
        super().setUp()

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
            "WARNING: Lock waiting timeout reached. Deleting lock file",
            validate_output,
            '\n Unexpected Validate Output 1: {0}\n'.format(repr(
                validate_output)))

        self.assertIn(
            "WARNING: Cannot lock backup {0} directory, skip validation".format(backup_id),
            validate_output,
            '\n Unexpected Validate Output 3: {0}\n'.format(repr(
                validate_output)))

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
        kill process so no cleanup is done, then change lock file timestamp
        to expired time, run validate, expect it to not successfully
        executed, RUNNING backup with expired lock file is legal, but his
        status must be changed to ERROR
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

        self.expire_locks(backup_dir, 'node')

        self.pb.validate(options=["--log-level-console=VERBOSE"],
                         expect_error="because RUNNING backup is no longer active")
        self.assertMessage(regex=r"Lock \S* has expired")
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
        kill process so no cleanup is done, then change lock file timestamp
        to expired time, run validate on this specific backup, expect it to
        not successfully executed, RUNNING backup with expired lock file is
        legal, but his status must be changed to ERROR
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

        self.expire_locks(backup_dir, 'node')

        self.pb.validate('node', backup_id,
                         options=['--log-level-console=VERBOSE'],
                         expect_error="because RUNNING backup is no longer active")
        self.assertMessage(regex=r"Lock \S* has expired")
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
        make node, take full backup, stop it in the middle, kill process so
        no cleanup is done, delete lock file to expired time, run validate,
        expect it to not successfully executed, RUNNING backup with expired
        lock file is legal, but his status must be changed to ERROR
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

        # delete lock file
        self.expire_locks(backup_dir, 'node')

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
        Expect restore to succeed because read-only locks
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
        self.assertMessage(contains='WARNING: Lock waiting timeout reached.')
        self.assertMessage(contains=f'Cannot lock backup {backup_id} directory')
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
    def test_locking_concurrent_restore_and_delete(self):
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

    @unittest.skipIf(not fs_backup_class.is_file_based, "os.rename is not implemented in a cloud")
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


    @needs_gdb
    def test_locks_delete(self):
        """
        Make sure that shared and exclusive locks are deleted
        after end of pg_probackup operations
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
        gdb = self.pb.backup_node('node', node, gdb=True)

        gdb.set_breakpoint('do_backup_pg')
        gdb.run_until_break()

        backup_id = self.pb.show('node')[0]['id']

        locks = self.get_locks(backup_dir, 'node')

        self.assertEqual(len(locks), 1,
                         f"There should be just 1 lock, got {locks}")
        self.assertTrue(locks[0].startswith(backup_id+"_"),
                         f"Lock should be for backup {backup_id}, got {locks[0]}")
        self.assertTrue(locks[0].endswith("_w"),
                         f"Lock should be exclusive got {locks[0]}")

        gdb.continue_execution_until_exit()

        locks = self.get_locks(backup_dir, 'node')
        self.assertFalse(locks, f"Locks should not exist, got {locks}")

        gdb = self.pb.validate('node', backup_id, gdb=True)

        gdb.set_breakpoint('validate_one_page')
        gdb.run_until_break()

        locks = self.get_locks(backup_dir, 'node')

        self.assertEqual(len(locks), 1,
                         f"There should be just 1 lock, got {locks}")
        self.assertTrue(locks[0].startswith(backup_id+"_"),
                        f"Lock should be for backup {backup_id}, got {locks[0]}")
        self.assertTrue(locks[0].endswith("_r"),
                        f"Lock should be shared got {locks[0]}")

        gdb.continue_execution_until_exit()

        locks = self.get_locks(backup_dir, 'node')
        self.assertFalse(locks, f"Locks should not exist, got {locks}")


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


    @needs_gdb
    def test_locks_race_condition(self):
        """
        Make locks race condition happen and check that pg_probackup
        detected it and retried taking new lock.

        Run full backup. Set breakpoint on create_lock_file function,
        stop there. Then run 'pg_probackup delete' command on current full
        backup, stop it after taking a lock and before deleting its lock
        file. Then continue taking full backup -- it must encounter race
        condition because lock file of delete operation appeared between
        two checks of /locks directory for concurrent locks
        (scan_locks_directory function).
        """

        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # start full backup
        gdb_backup = self.pb.backup_node('node', node,
                                         options=['--log-level-console=LOG'], gdb=True)

        gdb_backup.set_breakpoint('create_lock_file')
        gdb_backup.run_until_break()

        backup_id = self.pb.show('node')[0]['id']

        gdb_delete = self.pb.delete('node', backup_id,
                                    options=['--log-level-console=LOG'], gdb=True)

        gdb_delete.set_breakpoint('create_lock_file')
        gdb_delete.run_until_break()

        # we scanned locks directory and found no concurrent locks
        # so we proceed to the second scan for race condition check
        gdb_backup.set_breakpoint('scan_locks_directory')
        gdb_delete.set_breakpoint('scan_locks_directory')

        # we create lock files with no contents
        gdb_backup.continue_execution_until_break()
        gdb_delete.continue_execution_until_break()

        # check that both exclusive lock files were created with empty contents
        locks_list = self.get_locks(backup_dir, 'node')
        locks_list_race = locks_list

        self.assertEqual(len(locks_list), 2)

        self.assertFalse(self.read_lock(backup_dir, 'node', locks_list[0]))
        self.assertFalse(self.read_lock(backup_dir, 'node', locks_list[1]))

        gdb_backup.set_breakpoint('pioRemove__do')
        gdb_delete.set_breakpoint('pioRemove__do')

        # we wait for message about race condition and stop right before we
        # delete concurrent lock files to make both processes encounter race
        # condition
        gdb_backup.continue_execution_until_break()
        self.assertIn("Lock race condition detected, taking lock attempt 1 "
                      "failed", gdb_backup.output)
        gdb_delete.continue_execution_until_break()
        self.assertIn("Lock race condition detected, taking lock attempt 1 "
                      "failed", gdb_delete.output)

        # run until next breakpoint ('scan_locks_directory') so old lock
        # files will be deleted
        gdb_backup.continue_execution_until_break()
        gdb_delete.continue_execution_until_break()

        locks_list = self.get_locks(backup_dir, 'node')
        self.assertFalse(locks_list)

        # continue backup execution until 'unlock_backup' at-exit util
        gdb_backup.remove_all_breakpoints()
        gdb_backup.set_breakpoint('unlock_backup')
        gdb_backup.continue_execution_until_break()

        locks_list = self.get_locks(backup_dir, 'node')
        self.assertTrue(locks_list,
                        f"Expecting at least 1 lock, got no")
        self.assertLessEqual(len(locks_list), 2,
                             f"Expecting 1 or 2 locks, got {locks_list}")
        if len(locks_list) == 2:
            id1 = "_".join(locks_list[0].split("_", 2)[:2])
            id2 = "_".join(locks_list[1].split("_", 2)[:2])
            self.assertEqual(id1, id2)

        lock_backup = locks_list[0]
        self.assertIn(f"{lock_backup} was taken",
                      gdb_backup.output)
        self.assertFalse(self.read_lock(backup_dir, 'node', lock_backup))
        self.assertNotIn(lock_backup, locks_list_race)

        # make sure that delete command keeps waiting for backup unlocking
        gdb_delete.remove_all_breakpoints()
        gdb_delete.set_breakpoint('wait_for_conflicting_locks')
        gdb_delete.continue_execution_until_break()

        locks_list = self.get_locks(backup_dir, 'node')
        self.assertEqual(len(locks_list), 2)
        self.assertNotEqual(locks_list[0], locks_list[1])
        lock_delete = (set(locks_list) - {lock_backup}).pop()

        self.assertNotIn(lock_delete, locks_list_race)
        self.assertTrue(self.read_lock(backup_dir, 'node', lock_delete))

        gdb_delete.remove_all_breakpoints()
        gdb_delete.set_breakpoint('sleep')
        gdb_delete.continue_execution_until_break()
        gdb_delete.remove_all_breakpoints()

        self.assertIn(f"Waiting to take lock for backup {backup_id}",
                      gdb_delete.output)

        # continue all commands
        gdb_backup.continue_execution_until_exit()
        gdb_delete.continue_execution_until_exit()

        # make sure that locks were deleted
        locks_list = self.get_locks(backup_dir, 'node')
        self.assertFalse(locks_list)


    @needs_gdb
    def test_expired_locks_delete(self):
        """
        check that if locks (shared or exclusive) have timestamp older than
        (<current_timestamp> - LOCK_LIFETIME) they are deleted by running
        pg_probackup process
        """

        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        gdb = self.pb.backup_node('node', node, gdb=True)

        gdb.set_breakpoint('backup_non_data_file')
        gdb.run_until_break()

        gdb.continue_execution_until_break(20)

        gdb.signal('SIGKILL')
        gdb.continue_execution_until_error()

        self.assertEqual(
            'RUNNING', self.pb.show('node')[0]['status'])

        self.expire_locks(backup_dir, 'node', seconds=3600)

        stale_locks_list = self.get_locks(backup_dir, 'node')
        self.assertEqual(len(stale_locks_list), 1)

        backup_id = self.pb.show('node')[0]['id']

        gdb = self.pb.validate('node', backup_id, gdb=True,
                               options=['--log-level-console=LOG'])
        gdb.set_breakpoint('pgBackupValidate')
        gdb.run_until_break()

        self.assertRegex(gdb.output,
                         r"Expired lock file \S* is older than 180 seconds, deleting")

        new_locks_list = self.get_locks(backup_dir, 'node')
        self.assertEqual(len(new_locks_list), 1)
        self.assertFalse(set(stale_locks_list) & set(new_locks_list))

        gdb.continue_execution_until_exit()

        self.assertEqual(
            'ERROR', self.pb.show('node')[0]['status'])


    @needs_gdb
    def test_locks_renovate_time(self):
        """
        check that daemon thread renovates locks (shared or exclusive)
        timestamps when they are about to expire
        """
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)

        node.slow_start()

        gdb = self.pb.backup_node('node', node, gdb=True, options=['-j', '1',
                                                                   '--write-rate-limit=1',
                                                                   '--log-level-file=LOG'])

        # we need to stop just main thread
        gdb._execute("set pagination off")
        gdb._execute("set non-stop on")
        gdb.set_breakpoint('do_backup')
        gdb.run_until_break()
        gdb._execute("set val_LOCK_RENOVATE_TIME=2")
        gdb.set_breakpoint('do_backup_pg')
        gdb.continue_execution_until_break()

        self.assertEqual(
            'RUNNING', self.pb.show('node')[0]['status'])

        locks_1 = self.get_locks(backup_dir, 'node')
        self.assertLessEqual(len(locks_1), 2)
        lock_id = '_'.join(locks_1[0].split('_', 2)[:2])

        for attempt in range(25):
            time.sleep(4)
            locks_2 = self.get_locks(backup_dir, 'node')
            if set(locks_1) != set(locks_2) and len(locks_2) == 2:
                new = (set(locks_2) - set(locks_1)).pop()
                self.assertTrue(new.startswith(lock_id))
                break
        else:
            self.fail("locks didn't renovate in 100 seconds")


        gdb.remove_all_breakpoints()
        gdb.continue_execution_until_exit()

        self.assertEqual(
            'OK', self.pb.show('node')[0]['status'])