import unittest
import os
from time import sleep

from .helpers.ptrack_helpers import ProbackupTest
from pg_probackup2.gdb import needs_gdb
from datetime import datetime


class FalsePositive(ProbackupTest):

    # @unittest.skip("skip")
    @unittest.expectedFailure
    def test_validate_wal_lost_segment(self):
        """
        Loose segment located between backups. ExpectedFailure. This is BUG
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node)

        # make some wals
        node.pgbench_init(scale=5)

        # delete last wal segment
        wals = self.get_instance_wal_list(backup_dir, 'node')
        self.remove_instance_wal(backup_dir, 'node', max(wals))

        # We just lost a wal segment and know nothing about it
        self.pb.backup_node('node', node)
        self.assertTrue(
            'validation completed successfully' in self.pb.validate('node'))
        ########

    @unittest.expectedFailure
    # Need to force validation of ancestor-chain
    def test_incremental_backup_corrupt_full_1(self):
        """page-level backup with corrupted full backup"""
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node)
        self.remove_backup_file(backup_dir, 'node', backup_id,
                                'database/postgresql.conf')

        self.pb.backup_node('node', node, backup_type="page",
                         expect_error="because page backup without full is impossible")
        self.assertMessage(contains=
                'ERROR: Valid full backup on current timeline is not found. '
                'Create new FULL backup before an incremental one.')

        self.assertEqual(
            self.pb.show('node')[0]['Status'], "ERROR")

    # @unittest.skip("skip")
    @unittest.expectedFailure
    def test_pg_10_waldir(self):
        """
        test group access for PG >= 11
        """
        wal_dir = os.path.join(self.test_path, 'wal_dir')
        import shutil
        shutil.rmtree(wal_dir, ignore_errors=True)
        node = self.pg_node.make_simple('node',
            set_replication=True,
            initdb_params=['--waldir={0}'.format(wal_dir)])

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # take FULL backup
        self.pb.backup_node('node', node, options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        # restore backup
        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored)

        # compare pgdata permissions
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        self.assertTrue(
            os.path.islink(os.path.join(node_restored.data_dir, 'pg_wal')),
            'pg_wal should be symlink')

    # @unittest.expectedFailure
    @needs_gdb
    # @unittest.skip("skip")
    def test_recovery_target_time_backup_victim(self):
        """
        Check that for validation to recovery target
        probackup chooses valid backup
        https://github.com/postgrespro/pg_probackup/issues/104

        @y.sokolov: looks like this test should pass.
        So I commented 'expectedFailure'
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL backup
        self.pb.backup_node('node', node)

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")

        target_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        node.safe_psql(
            "postgres",
            "create table t_heap1 as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,100) i")

        gdb = self.pb.backup_node('node', node, gdb=True)

        # Attention! This breakpoint is set to a probackup internal fuction, not a postgres core one
        gdb.set_breakpoint('pg_stop_backup')
        gdb.run_until_break()
        gdb.signal('SIGINT')
        gdb.continue_execution_until_error()

        backup_id = self.pb.show('node')[1]['id']

        self.assertEqual(
            'ERROR',
            self.pb.show('node', backup_id)['status'],
            'Backup STATUS should be "ERROR"')

        self.pb.validate(
            'node',
            options=['--recovery-target-time={0}'.format(target_time)])

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    @needs_gdb
    def test_recovery_target_lsn_backup_victim(self):
        """
        Check that for validation to recovery target
        probackup chooses valid backup
        https://github.com/postgrespro/pg_probackup/issues/104

        @y.sokolov: looks like this test should pass.
        So I commented 'expectedFailure'
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL backup
        self.pb.backup_node('node', node)

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

        gdb = self.pb.backup_node('node', node,
            options=['--log-level-console=LOG'], gdb=True)

        # Attention! This breakpoint is set to a probackup internal fuction, not a postgres core one
        gdb.set_breakpoint('pg_stop_backup')
        gdb.run_until_break()
        gdb.signal('SIGINT')
        gdb.continue_execution_until_error()

        backup_id = self.pb.show('node')[1]['id']

        self.assertEqual(
            'ERROR',
            self.pb.show('node', backup_id)['status'],
            'Backup STATUS should be "ERROR"')

        self.switch_wal_segment(node)

        target_lsn = self.pb.show('node', backup_id)['start-lsn']

        self.pb.validate(
            'node',
            options=['--recovery-target-lsn={0}'.format(target_lsn)])

    # @unittest.skip("skip")
    @needs_gdb
    def test_streaming_timeout(self):
        """
        Illustrate the problem of loosing exact error
        message because our WAL streaming engine is "borrowed"
        from pg_receivexlog
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '1h',
                'wal_sender_timeout': '5s'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # FULL backup
        gdb = self.pb.backup_node('node', node, gdb=True,
            options=['--stream', '--log-level-file=LOG'])

        # Attention! This breakpoint is set to a probackup internal fuction, not a postgres core one
        gdb.set_breakpoint('pg_stop_backup')
        gdb.run_until_break()

        sleep(10)
        gdb.continue_execution_until_error()
        gdb.detach()
        sleep(2)

        log_content = self.read_pb_log()

        self.assertIn(
            'ERROR: Problem in receivexlog',
            log_content)

    # @unittest.skip("skip")
    @unittest.expectedFailure
    def test_validate_all_empty_catalog(self):
        """
        """
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()

        self.pb.validate(
                         expect_error="because backup_dir is empty")
        self.assertMessage(contains=
                'ERROR: This backup catalog contains no backup instances')
