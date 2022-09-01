import unittest
import os
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from datetime import datetime, timedelta
import subprocess


module_name = 'false_positive'


class FalsePositive(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    @unittest.expectedFailure
    def test_validate_wal_lost_segment(self):
        """
        Loose segment located between backups. ExpectedFailure. This is BUG
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(backup_dir, 'node', node)

        # make some wals
        node.pgbench_init(scale=5)

        # delete last wal segment
        wals_dir = os.path.join(backup_dir, "wal", 'node')
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(
            os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        wals = map(int, wals)
        os.remove(os.path.join(wals_dir, '0000000' + str(max(wals))))

        # We just lost a wal segment and know nothing about it
        self.backup_node(backup_dir, 'node', node)
        self.assertTrue(
            'validation completed successfully' in self.validate_pb(
                backup_dir, 'node'))
        ########

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    @unittest.expectedFailure
    # Need to force validation of ancestor-chain
    def test_incremental_backup_corrupt_full_1(self):
        """page-level backup with corrupted full backup"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        backup_id = self.backup_node(backup_dir, 'node', node)
        file = os.path.join(
            backup_dir, "backups", "node",
            backup_id.decode("utf-8"), "database", "postgresql.conf")
        os.remove(file)

        try:
            self.backup_node(backup_dir, 'node', node, backup_type="page")
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because page backup should not be "
                "possible without valid full backup.\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(
                e.message,
                'ERROR: Valid full backup on current timeline is not found. '
                'Create new FULL backup before an incremental one.\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertFalse(
                True,
                "Expecting Error because page backup should not be "
                "possible without valid full backup.\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(
                e.message,
                'ERROR: Valid full backup on current timeline is not found. '
                'Create new FULL backup before an incremental one.\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertEqual(
            self.show_pb(backup_dir, 'node')[0]['Status'], "ERROR")

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    @unittest.expectedFailure
    def test_pg_10_waldir(self):
        """
        test group access for PG >= 11
        """
        fname = self.id().split('.')[3]
        wal_dir = os.path.join(
            os.path.join(self.tmp_path, module_name, fname), 'wal_dir')
        shutil.rmtree(wal_dir, ignore_errors=True)
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=[
                '--data-checksums',
                '--waldir={0}'.format(wal_dir)])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        # take FULL backup
        self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        # restore backup
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored)

        # compare pgdata permissions
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        self.assertTrue(
            os.path.islink(os.path.join(node_restored.data_dir, 'pg_wal')),
            'pg_wal should be symlink')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_recovery_target_time_backup_victim(self):
        """
        Check that for validation to recovery target
        probackup chooses valid backup
        https://github.com/postgrespro/pg_probackup/issues/104
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # FULL backup
        self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")

        target_time = node.safe_psql(
            "postgres",
            "select now()").rstrip()

        node.safe_psql(
            "postgres",
            "create table t_heap1 as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,100) i")

        gdb = self.backup_node(backup_dir, 'node', node, gdb=True)

        gdb.set_breakpoint('pg_stop_backup')
        gdb.run_until_break()
        gdb.remove_all_breakpoints()
        gdb._execute('signal SIGINT')
        gdb.continue_execution_until_error()

        backup_id = self.show_pb(backup_dir, 'node')[1]['id']

        self.assertEqual(
            'ERROR',
            self.show_pb(backup_dir, 'node', backup_id)['status'],
            'Backup STATUS should be "ERROR"')

        self.validate_pb(
            backup_dir, 'node',
            options=['--recovery-target-time={0}'.format(target_time)])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_recovery_target_lsn_backup_victim(self):
        """
        Check that for validation to recovery target
        probackup chooses valid backup
        https://github.com/postgrespro/pg_probackup/issues/104
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # FULL backup
        self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")

        node.safe_psql(
            "postgres",
            "create table t_heap1 as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,100) i")

        gdb = self.backup_node(
            backup_dir, 'node', node,
            options=['--log-level-console=LOG'], gdb=True)

        gdb.set_breakpoint('pg_stop_backup')
        gdb.run_until_break()
        gdb.remove_all_breakpoints()
        gdb._execute('signal SIGINT')
        gdb.continue_execution_until_error()

        backup_id = self.show_pb(backup_dir, 'node')[1]['id']

        self.assertEqual(
            'ERROR',
            self.show_pb(backup_dir, 'node', backup_id)['status'],
            'Backup STATUS should be "ERROR"')

        self.switch_wal_segment(node)

        target_lsn = self.show_pb(backup_dir, 'node', backup_id)['start-lsn']

        self.validate_pb(
            backup_dir, 'node',
            options=['--recovery-target-lsn={0}'.format(target_lsn)])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    @unittest.expectedFailure
    def test_streaming_timeout(self):
        """
        Illustrate the problem of loosing exact error
        message because our WAL streaming engine is "borrowed"
        from pg_receivexlog
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '1h',
                'wal_sender_timeout': '5s'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        # FULL backup
        gdb = self.backup_node(
            backup_dir, 'node', node, gdb=True,
            options=['--stream', '--log-level-file=LOG'])

        gdb.set_breakpoint('pg_stop_backup')
        gdb.run_until_break()

        sleep(10)
        gdb.continue_execution_until_error()
        gdb._execute('detach')
        sleep(2)

        log_file_path = os.path.join(backup_dir, 'log', 'pg_probackup.log')
        with open(log_file_path) as f:
            log_content = f.read()

        self.assertIn(
            'could not receive data from WAL stream',
            log_content)

        self.assertIn(
            'ERROR: Problem in receivexlog',
            log_content)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    @unittest.expectedFailure
    def test_validate_all_empty_catalog(self):
        """
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)

        try:
            self.validate_pb(backup_dir)
            self.assertEqual(
                1, 0,
                "Expecting Error because backup_dir is empty.\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: This backup catalog contains no backup instances',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)
