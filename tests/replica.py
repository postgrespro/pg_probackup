import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException, idx_ptrack
from datetime import datetime, timedelta
import subprocess
import time
from distutils.dir_util import copy_tree
from testgres import ProcessType
from time import sleep


module_name = 'replica'

class ReplicaTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_replica_switchover(self):
        """
        check that archiving on replica works correctly
        over the course of several switchovers
        https://www.postgresql.org/message-id/54b059d4-2b48-13a4-6f43-95a087c92367%40postgrespro.ru
        """

        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node1 = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node1'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node1', node1)

        node1.slow_start()

        # take full backup and restore it
        self.backup_node(backup_dir, 'node1', node1, options=['--stream'])
        node2 = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node2'))
        node2.cleanup()

        # create replica
        self.restore_node(backup_dir, 'node1', node2)

        # setup replica
        self.add_instance(backup_dir, 'node2', node2)
        self.set_archiving(backup_dir, 'node2', node2, replica=True)
        self.set_replica(node1, node2, synchronous=False)
        self.set_auto_conf(node2, {'port': node2.port})

        node2.slow_start(replica=True)

        # generate some data
        node1.pgbench_init(scale=5)

        # take full backup on replica
        self.backup_node(backup_dir, 'node2', node2, options=['--stream'])

        # first switchover
        node1.stop()
        node2.promote()

        self.set_replica(node2, node1, synchronous=False)
        node2.reload()
        node1.slow_start(replica=True)

        # take incremental backup from new master
        self.backup_node(
            backup_dir, 'node2', node2,
            backup_type='delta', options=['--stream'])

        # second switchover
        node2.stop()
        node1.promote()
        self.set_replica(node1, node2, synchronous=False)
        node1.reload()
        node2.slow_start(replica=True)

        # generate some more data
        node1.pgbench_init(scale=5)

        # take incremental backup from replica
        self.backup_node(
            backup_dir, 'node2', node2,
            backup_type='delta', options=['--stream'])

        # https://github.com/postgrespro/pg_probackup/issues/251
        self.validate_pb(backup_dir)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_replica_stream_ptrack_backup(self):
        """
        make node, take full backup, restore it and make replica from it,
        take full stream backup from replica
        """
        if not self.ptrack:
            return unittest.skip('Skipped because ptrack support is disabled')

        if self.pg_config_version > self.version_to_num('9.6.0'):
            return unittest.skip(
                'Skipped because backup from replica is not supported in PG 9.5')

        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)

        master.slow_start()

        if master.major_version >= 12:
            master.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        # CREATE TABLE
        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,256) i")
        before = master.safe_psql("postgres", "SELECT * FROM t_heap")

        # take full backup and restore it
        self.backup_node(backup_dir, 'master', master, options=['--stream'])
        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()
        self.restore_node(backup_dir, 'master', replica)
        self.set_replica(master, replica)

        # Check data correctness on replica
        replica.slow_start(replica=True)
        after = replica.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        # Change data on master, take FULL backup from replica,
        # restore taken backup and check that restored data equal
        # to original data
        master.psql(
            "postgres",
            "insert into t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(256,512) i")
        before = master.safe_psql("postgres", "SELECT * FROM t_heap")
        self.add_instance(backup_dir, 'replica', replica)

        backup_id = self.backup_node(
            backup_dir, 'replica', replica,
            options=[
                '--stream',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port)])
        self.validate_pb(backup_dir, 'replica')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'replica', backup_id)['status'])

        # RESTORE FULL BACKUP TAKEN FROM PREVIOUS STEP
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'))
        node.cleanup()
        self.restore_node(backup_dir, 'replica', data_dir=node.data_dir)

        self.set_auto_conf(node, {'port': node.port})

        node.slow_start()

        # CHECK DATA CORRECTNESS
        after = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        # Change data on master, take PTRACK backup from replica,
        # restore taken backup and check that restored data equal
        # to original data
        master.psql(
            "postgres",
            "insert into t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(512,768) i")

        before = master.safe_psql("postgres", "SELECT * FROM t_heap")

        backup_id = self.backup_node(
            backup_dir, 'replica', replica, backup_type='ptrack',
            options=[
                '--stream',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port)])
        self.validate_pb(backup_dir, 'replica')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'replica', backup_id)['status'])

        # RESTORE PTRACK BACKUP TAKEN FROM replica
        node.cleanup()
        self.restore_node(
            backup_dir, 'replica', data_dir=node.data_dir, backup_id=backup_id)

        self.set_auto_conf(node, {'port': node.port})

        node.slow_start()

        # CHECK DATA CORRECTNESS
        after = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_replica_archive_page_backup(self):
        """
        make archive master, take full and page archive backups from master,
        set replica, make archive backup from replica
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'archive_timeout': '10s',
                'checkpoint_timeout': '30s',
                'max_wal_size': '32MB'})

        if self.get_version(master) < self.version_to_num('9.6.0'):
            self.del_test_dir(module_name, fname)
            return unittest.skip(
                'Skipped because backup from replica is not supported in PG 9.5')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        self.set_archiving(backup_dir, 'master', master)
        master.slow_start()

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.backup_node(backup_dir, 'master', master)

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")

        before = master.safe_psql("postgres", "SELECT * FROM t_heap")

        backup_id = self.backup_node(
            backup_dir, 'master', master, backup_type='page')
        self.restore_node(backup_dir, 'master', replica)

        # Settings for Replica
        self.add_instance(backup_dir, 'replica', replica)
        self.set_replica(master, replica, synchronous=True)
        self.set_archiving(backup_dir, 'replica', replica, replica=True)

        replica.slow_start(replica=True)

        # Check data correctness on replica
        after = replica.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        # Change data on master, take FULL backup from replica,
        # restore taken backup and check that restored data
        # equal to original data
        master.psql(
            "postgres",
            "insert into t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(256,25120) i")

        before = master.safe_psql("postgres", "SELECT * FROM t_heap")

        self.wait_until_replica_catch_with_master(master, replica)

        backup_id = self.backup_node(
            backup_dir, 'replica', replica,
            options=[
                '--archive-timeout=60',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port)])

        self.validate_pb(backup_dir, 'replica')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'replica', backup_id)['status'])

        # RESTORE FULL BACKUP TAKEN FROM replica
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'))
        node.cleanup()
        self.restore_node(backup_dir, 'replica', data_dir=node.data_dir)

        self.set_auto_conf(node, {'port': node.port, 'archive_mode': 'off'})

        node.slow_start()

        # CHECK DATA CORRECTNESS
        after = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)
        node.cleanup()

        # Change data on master, make PAGE backup from replica,
        # restore taken backup and check that restored data equal
        # to original data
        master.pgbench_init(scale=5)

        pgbench = master.pgbench(
            options=['-T', '30', '-c', '2', '--no-vacuum'])

        backup_id = self.backup_node(
            backup_dir, 'replica',
            replica, backup_type='page',
            options=[
                '--archive-timeout=60',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port)])

        pgbench.wait()

        self.switch_wal_segment(master)

        before = master.safe_psql("postgres", "SELECT * FROM pgbench_accounts")

        self.validate_pb(backup_dir, 'replica')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'replica', backup_id)['status'])

        # RESTORE PAGE BACKUP TAKEN FROM replica
        self.restore_node(
            backup_dir, 'replica', data_dir=node.data_dir,
            backup_id=backup_id)

        self.set_auto_conf(node, {'port': node.port, 'archive_mode': 'off'})

        node.slow_start()

        # CHECK DATA CORRECTNESS
        after = node.safe_psql("postgres", "SELECT * FROM pgbench_accounts")
        self.assertEqual(
            before, after, 'Restored data is not equal to original')

        self.add_instance(backup_dir, 'node', node)
        self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_basic_make_replica_via_restore(self):
        """
        make archive master, take full and page archive backups from master,
        set replica, make archive backup from replica
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'archive_timeout': '10s'})

        if self.get_version(master) < self.version_to_num('9.6.0'):
            self.del_test_dir(module_name, fname)
            return unittest.skip(
                'Skipped because backup from replica is not supported in PG 9.5')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        self.set_archiving(backup_dir, 'master', master)
        master.slow_start()

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.backup_node(backup_dir, 'master', master)

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,8192) i")

        before = master.safe_psql("postgres", "SELECT * FROM t_heap")

        backup_id = self.backup_node(
            backup_dir, 'master', master, backup_type='page')
        self.restore_node(
            backup_dir, 'master', replica, options=['-R'])

        # Settings for Replica
        self.add_instance(backup_dir, 'replica', replica)
        self.set_archiving(backup_dir, 'replica', replica, replica=True)
        self.set_replica(master, replica, synchronous=True)

        replica.slow_start(replica=True)

        self.backup_node(
            backup_dir, 'replica', replica,
            options=['--archive-timeout=30s', '--stream'])

        # Clean after yourself
        self.del_test_dir(module_name, fname, [master, replica])

    # @unittest.skip("skip")
    def test_take_backup_from_delayed_replica(self):
        """
        make archive master, take full backups from master,
        restore full backup as delayed replica, launch pgbench,
        take FULL, PAGE and DELTA backups from replica
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'archive_timeout': '10s'})

        if self.get_version(master) < self.version_to_num('9.6.0'):
            self.del_test_dir(module_name, fname)
            return unittest.skip(
                'Skipped because backup from replica is not supported in PG 9.5')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        self.set_archiving(backup_dir, 'master', master)
        master.slow_start()

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.backup_node(backup_dir, 'master', master)

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,165000) i")

        master.psql(
            "postgres",
            "create table t_heap_1 as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,165000) i")

        self.restore_node(
            backup_dir, 'master', replica, options=['-R'])

        # Settings for Replica
        self.add_instance(backup_dir, 'replica', replica)
        self.set_archiving(backup_dir, 'replica', replica, replica=True)

        self.set_auto_conf(replica, {'port': replica.port})

        replica.slow_start(replica=True)

        self.wait_until_replica_catch_with_master(master, replica)

        if self.get_version(master) >= self.version_to_num('12.0'):
            self.set_auto_conf(
                replica, {'recovery_min_apply_delay': '300s'})
        else:
            replica.append_conf(
                'recovery.conf',
                'recovery_min_apply_delay = 300s')

        replica.stop()
        replica.slow_start(replica=True)

        master.pgbench_init(scale=10)

        pgbench = master.pgbench(
            options=['-T', '60', '-c', '2', '--no-vacuum'])

        self.backup_node(
            backup_dir, 'replica',
            replica, options=['--archive-timeout=60s'])

        self.backup_node(
            backup_dir, 'replica', replica,
            data_dir=replica.data_dir,
            backup_type='page', options=['--archive-timeout=60s'])

        sleep(1)

        self.backup_node(
            backup_dir, 'replica', replica,
            backup_type='delta', options=['--archive-timeout=60s'])

        pgbench.wait()

        pgbench = master.pgbench(
            options=['-T', '30', '-c', '2', '--no-vacuum'])

        self.backup_node(
            backup_dir, 'replica', replica,
            options=['--stream'])

        self.backup_node(
            backup_dir, 'replica', replica,
            backup_type='page', options=['--stream'])

        self.backup_node(
            backup_dir, 'replica', replica,
            backup_type='delta', options=['--stream'])

        pgbench.wait()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_replica_promote(self):
        """
        start backup from replica, during backup promote replica
        check that backup is failed
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'archive_timeout': '10s',
                'checkpoint_timeout': '30s',
                'max_wal_size': '32MB'})

        if self.get_version(master) < self.version_to_num('9.6.0'):
            self.del_test_dir(module_name, fname)
            return unittest.skip(
                'Skipped because backup from replica is not supported in PG 9.5')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        self.set_archiving(backup_dir, 'master', master)
        master.slow_start()

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.backup_node(backup_dir, 'master', master)

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,165000) i")

        self.restore_node(
            backup_dir, 'master', replica, options=['-R'])

        # Settings for Replica
        self.add_instance(backup_dir, 'replica', replica)
        self.set_archiving(backup_dir, 'replica', replica, replica=True)
        self.set_replica(
            master, replica, replica_name='replica', synchronous=True)

        replica.slow_start(replica=True)

        master.psql(
            "postgres",
            "create table t_heap_1 as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,165000) i")

        self.wait_until_replica_catch_with_master(master, replica)

        # start backup from replica
        gdb = self.backup_node(
            backup_dir, 'replica', replica, gdb=True,
            options=['--log-level-file=verbose'])

        gdb.set_breakpoint('backup_data_file')
        gdb.run_until_break()
        gdb.continue_execution_until_break(20)

        replica.promote()

        gdb.remove_all_breakpoints()
        gdb.continue_execution_until_exit()

        backup_id = self.show_pb(
            backup_dir, 'replica')[0]["id"]

        # read log file content
        with open(os.path.join(backup_dir, 'log', 'pg_probackup.log')) as f:
            log_content = f.read()
            f.close

        self.assertIn(
            'ERROR:  the standby was promoted during online backup',
            log_content)

        self.assertIn(
            'WARNING: Backup {0} is running, '
            'setting its status to ERROR'.format(backup_id),
            log_content)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_replica_stop_lsn_null_offset(self):
        """
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '1h',
                'wal_level': 'replica'})

        if self.get_version(master) < self.version_to_num('9.6.0'):
            self.del_test_dir(module_name, fname)
            return unittest.skip(
                'Skipped because backup from replica is not supported in PG 9.5')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', master)
        self.set_archiving(backup_dir, 'node', master)
        master.slow_start()

        # freeze bgwriter to get rid of RUNNING XACTS records
        bgwriter_pid = master.auxiliary_pids[ProcessType.BackgroundWriter][0]
        gdb_checkpointer = self.gdb_attach(bgwriter_pid)

        self.backup_node(backup_dir, 'node', master)

        # Create replica
        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()
        self.restore_node(backup_dir, 'node', replica)

        # Settings for Replica
        self.set_replica(master, replica, synchronous=True)
        self.set_archiving(backup_dir, 'node', replica, replica=True)

        replica.slow_start(replica=True)

        self.switch_wal_segment(master)
        self.switch_wal_segment(master)

        output = self.backup_node(
            backup_dir, 'node', replica, replica.data_dir,
            options=[
                '--archive-timeout=30',
                '--log-level-console=LOG',
                '--no-validate',
                '--stream'],
            return_id=False)

        self.assertIn(
            'LOG: Invalid offset in stop_lsn value 0/4000000',
            output)

        self.assertIn(
            'WARNING: WAL segment 000000010000000000000004 could not be streamed in 30 seconds',
            output)

        self.assertIn(
            'WARNING: Failed to get next WAL record after 0/4000000, looking for previous WAL record',
            output)

        self.assertIn(
            'LOG: Looking for LSN 0/4000000 in segment: 000000010000000000000003',
            output)

        self.assertIn(
            'has endpoint 0/4000000 which is '
            'equal or greater than requested LSN 0/4000000',
            output)

        self.assertIn(
            'LOG: Found prior LSN:',
            output)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_replica_stop_lsn_null_offset_next_record(self):
        """
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '1h',
                'wal_level': 'replica'})

        if self.get_version(master) < self.version_to_num('9.6.0'):
            self.del_test_dir(module_name, fname)
            return unittest.skip(
                'Skipped because backup from replica is not supported in PG 9.5')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        self.set_archiving(backup_dir, 'master', master)
        master.slow_start()

        # freeze bgwriter to get rid of RUNNING XACTS records
        bgwriter_pid = master.auxiliary_pids[ProcessType.BackgroundWriter][0]
        gdb_checkpointer = self.gdb_attach(bgwriter_pid)

        self.backup_node(backup_dir, 'master', master)

        # Create replica
        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()
        self.restore_node(backup_dir, 'master', replica)

        # Settings for Replica
        self.add_instance(backup_dir, 'replica', replica)
        self.set_replica(master, replica, synchronous=True)
        self.set_archiving(backup_dir, 'replica', replica, replica=True)

        copy_tree(
            os.path.join(backup_dir, 'wal', 'master'),
            os.path.join(backup_dir, 'wal', 'replica'))

        replica.slow_start(replica=True)

        self.switch_wal_segment(master)
        self.switch_wal_segment(master)

        # open connection to master
        conn = master.connect()

        gdb = self.backup_node(
            backup_dir, 'replica', replica,
            options=[
                '--archive-timeout=40',
                '--log-level-file=LOG',
                '--no-validate',
                '--stream'],
            gdb=True)

        gdb.set_breakpoint('pg_stop_backup')
        gdb.run_until_break()
        gdb.remove_all_breakpoints()
        gdb.continue_execution_until_running()

        sleep(5)

        conn.execute("create table t1()")
        conn.commit()

        while 'RUNNING' in self.show_pb(backup_dir, 'replica')[0]['status']:
            sleep(5)

        file = os.path.join(backup_dir, 'log', 'pg_probackup.log')

        with open(file) as f:
            log_content = f.read()

        self.assertIn(
            'LOG: Invalid offset in stop_lsn value 0/4000000',
            log_content)

        self.assertIn(
            'LOG: Looking for segment: 000000010000000000000004',
            log_content)

        self.assertIn(
            'LOG: First record in WAL segment "000000010000000000000004": 0/4000028',
            log_content)

        self.assertIn(
            'LOG: stop_lsn: 0/4000000',
            log_content)

        self.assertTrue(self.show_pb(backup_dir, 'replica')[0]['status'] == 'DONE')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_archive_replica_null_offset(self):
        """
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '1h',
                'wal_level': 'replica'})

        if self.get_version(master) < self.version_to_num('9.6.0'):
            self.del_test_dir(module_name, fname)
            return unittest.skip(
                'Skipped because backup from replica is not supported in PG 9.5')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', master)
        self.set_archiving(backup_dir, 'node', master)
        master.slow_start()

        self.backup_node(backup_dir, 'node', master)

        # Create replica
        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()
        self.restore_node(backup_dir, 'node', replica)

        # Settings for Replica
        self.set_replica(master, replica, synchronous=True)
        self.set_archiving(backup_dir, 'node', replica, replica=True)

        # freeze bgwriter to get rid of RUNNING XACTS records
        bgwriter_pid = master.auxiliary_pids[ProcessType.BackgroundWriter][0]
        gdb_checkpointer = self.gdb_attach(bgwriter_pid)

        replica.slow_start(replica=True)

        self.switch_wal_segment(master)
        self.switch_wal_segment(master)

        # take backup from replica
        output = self.backup_node(
            backup_dir, 'node', replica, replica.data_dir,
            options=[
                '--archive-timeout=30',
                '--log-level-console=LOG',
                '--no-validate'],
            return_id=False)

        self.assertIn(
            'LOG: Invalid offset in stop_lsn value 0/4000000',
            output)

        self.assertIn(
            'WARNING: WAL segment 000000010000000000000004 could not be archived in 30 seconds',
            output)

        self.assertIn(
            'WARNING: Failed to get next WAL record after 0/4000000, looking for previous WAL record',
            output)

        self.assertIn(
            'LOG: Looking for LSN 0/4000000 in segment: 000000010000000000000003',
            output)

        self.assertIn(
            'has endpoint 0/4000000 which is '
            'equal or greater than requested LSN 0/4000000',
            output)

        self.assertIn(
            'LOG: Found prior LSN:',
            output)

        print(output)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_archive_replica_not_null_offset(self):
        """
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '1h',
                'wal_level': 'replica'})

        if self.get_version(master) < self.version_to_num('9.6.0'):
            self.del_test_dir(module_name, fname)
            return unittest.skip(
                'Skipped because backup from replica is not supported in PG 9.5')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', master)
        self.set_archiving(backup_dir, 'node', master)
        master.slow_start()

        self.backup_node(backup_dir, 'node', master)

        # Create replica
        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()
        self.restore_node(backup_dir, 'node', replica)

        # Settings for Replica
        self.set_replica(master, replica, synchronous=True)
        self.set_archiving(backup_dir, 'node', replica, replica=True)

        replica.slow_start(replica=True)

        # take backup from replica
        self.backup_node(
            backup_dir, 'node', replica, replica.data_dir,
            options=[
                '--archive-timeout=30',
                '--log-level-console=LOG',
                '--no-validate'],
            return_id=False)

        try:
            self.backup_node(
                backup_dir, 'node', replica, replica.data_dir,
                options=[
                    '--archive-timeout=30',
                    '--log-level-console=LOG',
                    '--no-validate'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because of archive timeout. "
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'LOG: Looking for LSN 0/4000060 in segment: 000000010000000000000004',
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

            self.assertIn(
                'INFO: Wait for LSN 0/4000060 in archived WAL segment',
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

            self.assertIn(
                'ERROR: WAL segment 000000010000000000000004 could not be archived in 30 seconds',
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_replica_toast(self):
        """
        make archive master, take full and page archive backups from master,
        set replica, make archive backup from replica
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off',
                'checkpoint_timeout': '1h',
                'wal_level': 'replica',
                'shared_buffers': '128MB'})

        if self.get_version(master) < self.version_to_num('9.6.0'):
            self.del_test_dir(module_name, fname)
            return unittest.skip(
                'Skipped because backup from replica is not supported in PG 9.5')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        self.set_archiving(backup_dir, 'master', master)
        master.slow_start()

        # freeze bgwriter to get rid of RUNNING XACTS records
        bgwriter_pid = master.auxiliary_pids[ProcessType.BackgroundWriter][0]
        gdb_checkpointer = self.gdb_attach(bgwriter_pid)

        self.backup_node(backup_dir, 'master', master)

        # Create replica
        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()
        self.restore_node(backup_dir, 'master', replica)

        # Settings for Replica
        self.add_instance(backup_dir, 'replica', replica)
        self.set_replica(master, replica, synchronous=True)
        self.set_archiving(backup_dir, 'replica', replica, replica=True)

        copy_tree(
            os.path.join(backup_dir, 'wal', 'master'),
            os.path.join(backup_dir, 'wal', 'replica'))

        replica.slow_start(replica=True)

        self.switch_wal_segment(master)
        self.switch_wal_segment(master)

        master.safe_psql(
            'postgres',
            'CREATE TABLE t1 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,10) i')

        self.wait_until_replica_catch_with_master(master, replica)

        output = self.backup_node(
            backup_dir, 'replica', replica,
            options=[
                '--archive-timeout=30',
                '--log-level-console=LOG',
                '--no-validate',
                '--stream'],
            return_id=False)

        pgdata = self.pgdata_content(replica.data_dir)

        self.assertIn(
            'WARNING: Could not read WAL record at',
            output)

        self.assertIn(
            'LOG: Found prior LSN:',
            output)

        res1 = replica.safe_psql(
            'postgres',
            'select md5(fat_attr) from t1')

        replica.cleanup()

        self.restore_node(backup_dir, 'replica', replica)
        pgdata_restored = self.pgdata_content(replica.data_dir)

        replica.slow_start()

        res2 = replica.safe_psql(
            'postgres',
            'select md5(fat_attr) from t1')

        self.assertEqual(res1, res2)

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    @unittest.skip("skip")
    def test_replica_promote_1(self):
        """
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '1h',
                'wal_level': 'replica'})

        if self.get_version(master) < self.version_to_num('9.6.0'):
            self.del_test_dir(module_name, fname)
            return unittest.skip(
                'Skipped because backup from replica is not supported in PG 9.5')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        # set replica True, so archive_mode 'always' is used.
        self.set_archiving(backup_dir, 'master', master, replica=True)
        master.slow_start()

        self.backup_node(backup_dir, 'master', master)

        # Create replica
        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()
        self.restore_node(backup_dir, 'master', replica)

        # Settings for Replica
        self.set_replica(master, replica)

        replica.slow_start(replica=True)

        master.safe_psql(
            'postgres',
            'CREATE TABLE t1 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,10) i')

        self.wait_until_replica_catch_with_master(master, replica)

        wal_file = os.path.join(
            backup_dir, 'wal', 'master', '000000010000000000000004')

        wal_file_partial = os.path.join(
            backup_dir, 'wal', 'master', '000000010000000000000004.partial')

        self.assertFalse(os.path.exists(wal_file))

        replica.promote()

        while not os.path.exists(wal_file_partial):
            sleep(1)

        self.switch_wal_segment(master)

        # sleep to be sure, that any partial timeout is expired
        sleep(70)

        self.assertTrue(
            os.path.exists(wal_file_partial),
            "File {0} disappeared".format(wal_file))
        
        self.assertTrue(
            os.path.exists(wal_file_partial),
            "File {0} disappeared".format(wal_file_partial))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_replica_promote_2(self):
        """
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        # set replica True, so archive_mode 'always' is used.
        self.set_archiving(
            backup_dir, 'master', master, replica=True)
        master.slow_start()

        self.backup_node(backup_dir, 'master', master)

        # Create replica
        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()
        self.restore_node(backup_dir, 'master', replica)

        # Settings for Replica
        self.set_replica(master, replica)
        self.set_auto_conf(replica, {'port': replica.port})

        replica.slow_start(replica=True)

        master.safe_psql(
            'postgres',
            'CREATE TABLE t1 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,1) i')

        self.wait_until_replica_catch_with_master(master, replica)

        replica.promote()

        self.backup_node(
            backup_dir, 'master', replica, data_dir=replica.data_dir,
            backup_type='page')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_replica_promote_3(self):
        """
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)

        master.slow_start()

        self.backup_node(backup_dir, 'master', master, options=['--stream'])

        # Create replica
        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()
        self.restore_node(backup_dir, 'master', replica)

        # Settings for Replica
        self.set_replica(master, replica)
        self.set_auto_conf(replica, {'port': replica.port})

        replica.slow_start(replica=True)

        master.safe_psql(
            'postgres',
            'CREATE TABLE t1 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,20) i')
        self.wait_until_replica_catch_with_master(master, replica)

        self.add_instance(backup_dir, 'replica', replica)

        full_id = self.backup_node(
            backup_dir, 'replica',
            replica, options=['--stream'])

        master.safe_psql(
            'postgres',
            'CREATE TABLE t2 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,20) i')
        self.wait_until_replica_catch_with_master(master, replica)

        self.backup_node(
            backup_dir, 'replica', replica,
            backup_type='delta', options=['--stream'])

        replica.promote()

        # failing, because without archving, it is impossible to
        # take multi-timeline backup.
        try:
            self.backup_node(
                backup_dir, 'replica', replica,
                backup_type='delta', options=['--stream'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because of timeline switch "
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'WARNING: Cannot find valid backup on previous timelines, '
                'WAL archive is not available' in e.message and
                'ERROR: Create new full backup before an incremental one' in e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_replica_promote_archive_delta(self):
        """
        t3                    /---D3-->
        t2               /------->
        t1 --F---D1--D2--
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node1 = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node1'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s',
                'archive_timeout': '30s',
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node1)
        self.set_config(
            backup_dir, 'node', options=['--archive-timeout=60s'])
        self.set_archiving(backup_dir, 'node', node1)

        node1.slow_start()

        self.backup_node(backup_dir, 'node', node1, options=['--stream'])

        # Create replica
        node2 = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node2'))
        node2.cleanup()
        self.restore_node(backup_dir, 'node', node2, node2.data_dir)

        # Settings for Replica
        self.set_replica(node1, node2)
        self.set_auto_conf(node2, {'port': node2.port})
        self.set_archiving(backup_dir, 'node', node2, replica=True)

        node2.slow_start(replica=True)

        node1.safe_psql(
            'postgres',
            'CREATE TABLE t1 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,20) i')
        self.wait_until_replica_catch_with_master(node1, node2)

        node1.safe_psql(
            'postgres',
            'CREATE TABLE t2 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,20) i')
        self.wait_until_replica_catch_with_master(node1, node2)

        # delta backup on replica on timeline 1
        delta1_id = self.backup_node(
            backup_dir, 'node', node2, node2.data_dir,
            'delta', options=['--stream'])

        # delta backup on replica on timeline 1
        delta2_id = self.backup_node(
            backup_dir, 'node', node2, node2.data_dir, 'delta')

        self.change_backup_status(
            backup_dir, 'node', delta2_id, 'ERROR')

        # node2 is now master
        node2.promote()

        node2.safe_psql(
            'postgres',
            'CREATE TABLE t3 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,20) i')

        # node1 is now replica
        node1.cleanup()
        # kludge "backup_id=delta1_id"
        self.restore_node(
            backup_dir, 'node', node1, node1.data_dir,
            backup_id=delta1_id,
            options=[
                '--recovery-target-timeline=2',
                '--recovery-target=latest'])

        # Settings for Replica
        self.set_replica(node2, node1)
        self.set_auto_conf(node1, {'port': node1.port})
        self.set_archiving(backup_dir, 'node', node1, replica=True)

        node1.slow_start(replica=True)

        node2.safe_psql(
            'postgres',
            'CREATE TABLE t4 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,30) i')
        self.wait_until_replica_catch_with_master(node2, node1)

        # node1 is back to be a master
        node1.promote()

        sleep(5)

        # delta backup on timeline 3
        self.backup_node(
            backup_dir, 'node', node1, node1.data_dir, 'delta',
            options=['--archive-timeout=60'])

        pgdata = self.pgdata_content(node1.data_dir)

        node1.cleanup()
        self.restore_node(backup_dir, 'node', node1, node1.data_dir)

        pgdata_restored = self.pgdata_content(node1.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_replica_promote_archive_page(self):
        """
        t3                    /---P3-->
        t2               /------->
        t1 --F---P1--P2--
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node1 = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node1'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s',
                'archive_timeout': '30s',
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node1)
        self.set_archiving(backup_dir, 'node', node1)
        self.set_config(
            backup_dir, 'node', options=['--archive-timeout=60s'])

        node1.slow_start()

        self.backup_node(backup_dir, 'node', node1, options=['--stream'])

        # Create replica
        node2 = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node2'))
        node2.cleanup()
        self.restore_node(backup_dir, 'node', node2, node2.data_dir)

        # Settings for Replica
        self.set_replica(node1, node2)
        self.set_auto_conf(node2, {'port': node2.port})
        self.set_archiving(backup_dir, 'node', node2, replica=True)

        node2.slow_start(replica=True)

        node1.safe_psql(
            'postgres',
            'CREATE TABLE t1 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,20) i')
        self.wait_until_replica_catch_with_master(node1, node2)

        node1.safe_psql(
            'postgres',
            'CREATE TABLE t2 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,20) i')
        self.wait_until_replica_catch_with_master(node1, node2)

        # page backup on replica on timeline 1
        page1_id = self.backup_node(
            backup_dir, 'node', node2, node2.data_dir,
            'page', options=['--stream'])

        # page backup on replica on timeline 1
        page2_id = self.backup_node(
            backup_dir, 'node', node2, node2.data_dir, 'page')

        self.change_backup_status(
            backup_dir, 'node', page2_id, 'ERROR')

        # node2 is now master
        node2.promote()

        node2.safe_psql(
            'postgres',
            'CREATE TABLE t3 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,20) i')

        # node1 is now replica
        node1.cleanup()
        # kludge "backup_id=page1_id"
        self.restore_node(
            backup_dir, 'node', node1, node1.data_dir,
            backup_id=page1_id,
            options=[
                '--recovery-target-timeline=2',
                '--recovery-target=latest'])

        # Settings for Replica
        self.set_replica(node2, node1)
        self.set_auto_conf(node1, {'port': node1.port})
        self.set_archiving(backup_dir, 'node', node1, replica=True)

        node1.slow_start(replica=True)

        node2.safe_psql(
            'postgres',
            'CREATE TABLE t4 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,30) i')
        self.wait_until_replica_catch_with_master(node2, node1)

        # node1 is back to be a master
        node1.promote()
        self.switch_wal_segment(node1)

        sleep(5)

        # delta3_id = self.backup_node(
        #     backup_dir, 'node', node2, node2.data_dir, 'delta')
        # page backup on timeline 3
        page3_id = self.backup_node(
            backup_dir, 'node', node1, node1.data_dir, 'page',
            options=['--archive-timeout=60'])

        pgdata = self.pgdata_content(node1.data_dir)

        node1.cleanup()
        self.restore_node(backup_dir, 'node', node1, node1.data_dir)

        pgdata_restored = self.pgdata_content(node1.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_parent_choosing(self):
        """
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)

        master.slow_start()

        self.backup_node(backup_dir, 'master', master, options=['--stream'])

        # Create replica
        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()
        self.restore_node(backup_dir, 'master', replica)

        # Settings for Replica
        self.set_replica(master, replica)
        self.set_auto_conf(replica, {'port': replica.port})

        replica.slow_start(replica=True)

        master.safe_psql(
            'postgres',
            'CREATE TABLE t1 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,20) i')
        self.wait_until_replica_catch_with_master(master, replica)

        self.add_instance(backup_dir, 'replica', replica)

        full_id = self.backup_node(
            backup_dir, 'replica',
            replica, options=['--stream'])

        master.safe_psql(
            'postgres',
            'CREATE TABLE t2 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,20) i')
        self.wait_until_replica_catch_with_master(master, replica)

        self.backup_node(
            backup_dir, 'replica', replica,
            backup_type='delta', options=['--stream'])

        replica.promote()

        # failing, because without archving, it is impossible to
        # take multi-timeline backup.
        try:
            self.backup_node(
                backup_dir, 'replica', replica,
                backup_type='delta', options=['--stream'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because of timeline switch "
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'WARNING: Cannot find valid backup on previous timelines, '
                'WAL archive is not available' in e.message and
                'ERROR: Create new full backup before an incremental one' in e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_instance_from_the_past(self):
        """
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)

        node.slow_start()

        full_id = self.backup_node(backup_dir, 'node', node, options=['--stream'])

        node.pgbench_init(scale=10)
        self.backup_node(backup_dir, 'node', node, options=['--stream'])
        node.cleanup()

        self.restore_node(backup_dir, 'node', node, backup_id=full_id)
        node.slow_start()

        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='delta', options=['--stream'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because instance is from the past "
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'ERROR: Current START LSN' in e.message and
                'is lower than START LSN' in e.message and
                'It may indicate that we are trying to backup '
                'PostgreSQL instance from the past' in e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

# TODO:
# null offset STOP LSN and latest record in previous segment is conrecord (manual only)
# archiving from promoted delayed replica
