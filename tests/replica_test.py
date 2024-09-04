import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest
from pg_probackup2.gdb import needs_gdb
from testgres import ProcessType
from time import sleep


class ReplicaTest(ProbackupTest):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_replica_switchover(self):
        """
        check that archiving on replica works correctly
        over the course of several switchovers
        https://www.postgresql.org/message-id/54b059d4-2b48-13a4-6f43-95a087c92367%40postgrespro.ru
        """
        backup_dir = self.backup_dir
        node1 = self.pg_node.make_simple('node1',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node1', node1)

        node1.slow_start()

        # take full backup and restore it
        self.pb.backup_node('node1', node1, options=['--stream'])
        node2 = self.pg_node.make_simple('node2')
        node2.cleanup()

        # create replica
        self.pb.restore_node('node1', node=node2)

        # setup replica
        self.pb.add_instance('node2', node2)
        self.pb.set_archiving('node2', node2, replica=True)
        self.set_replica(node1, node2, synchronous=False)
        node2.set_auto_conf({'port': node2.port})

        node2.slow_start(replica=True)

        # generate some data
        node1.pgbench_init(scale=5)

        # take full backup on replica
        self.pb.backup_node('node2', node2, options=['--stream'])

        # first switchover
        node1.stop()
        node2.promote()

        self.set_replica(node2, node1, synchronous=False)
        node2.reload()
        node1.slow_start(replica=True)

        # take incremental backup from new master
        self.pb.backup_node('node2', node2,
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
        self.pb.backup_node('node2', node2,
            backup_type='delta', options=['--stream'])

        # https://github.com/postgrespro/pg_probackup/issues/251
        self.pb.validate()

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_replica_stream_ptrack_backup(self):
        """
        make node, take full backup, restore it and make replica from it,
        take full stream backup from replica
        """
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

        backup_dir = self.backup_dir
        master = self.pg_node.make_simple('master',
            set_replication=True,
            ptrack_enable=True)

        self.pb.init()
        self.pb.add_instance('master', master)

        master.slow_start()

        master.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        # CREATE TABLE
        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,256) i")
        before = master.table_checksum("t_heap")

        # take full backup and restore it
        self.pb.backup_node('master', master, options=['--stream'])
        replica = self.pg_node.make_simple('replica')
        replica.cleanup()
        self.pb.restore_node('master', node=replica)
        self.set_replica(master, replica)

        # Check data correctness on replica
        replica.slow_start(replica=True)
        after = replica.table_checksum("t_heap")
        self.assertEqual(before, after)

        # Change data on master, take FULL backup from replica,
        # restore taken backup and check that restored data equal
        # to original data
        master.psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(256,512) i")
        before = master.table_checksum("t_heap")
        self.pb.add_instance('replica', replica)

        backup_id = self.pb.backup_node('replica', replica,
            options=['--stream'])
        self.pb.validate('replica')
        self.assertEqual(
            'OK', self.pb.show('replica', backup_id)['status'])

        # RESTORE FULL BACKUP TAKEN FROM PREVIOUS STEP
        node = self.pg_node.make_simple('node')
        node.cleanup()
        self.pb.restore_node('replica', node=node)

        node.set_auto_conf({'port': node.port})

        node.slow_start()

        # CHECK DATA CORRECTNESS
        after = node.table_checksum("t_heap")
        self.assertEqual(before, after)

        # Change data on master, take PTRACK backup from replica,
        # restore taken backup and check that restored data equal
        # to original data
        master.psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(512,768) i")

        before = master.table_checksum("t_heap")

        backup_id = self.pb.backup_node('replica', replica, backup_type='ptrack',
            options=['--stream'])
        self.pb.validate('replica')
        self.assertEqual(
            'OK', self.pb.show('replica', backup_id)['status'])

        # RESTORE PTRACK BACKUP TAKEN FROM replica
        node.cleanup()
        self.pb.restore_node('replica', node, backup_id=backup_id)

        node.set_auto_conf({'port': node.port})

        node.slow_start()

        # CHECK DATA CORRECTNESS
        after = node.table_checksum("t_heap")
        self.assertEqual(before, after)

    # @unittest.skip("skip")
    @needs_gdb
    def test_replica_archive_page_backup(self):
        """
        make archive master, take full and page archive backups from master,
        set replica, make archive backup from replica
        """
        backup_dir = self.backup_dir
        master = self.pg_node.make_simple('master',
            set_replication=True,
            pg_options={
                'archive_timeout': '10s',
                'checkpoint_timeout': '30s',
                'max_wal_size': '32MB'})

        self.pb.init()
        self.pb.add_instance('master', master)
        self.pb.set_archiving('master', master)
        master.slow_start()

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.backup_node('master', master)

        master.pgbench_init(scale=5)

        before = master.table_checksum("pgbench_accounts")

        backup_id = self.pb.backup_node('master', master, backup_type='page')
        self.pb.restore_node('master', node=replica)

        # Settings for Replica
        self.pb.add_instance('replica', replica)
        self.set_replica(master, replica, synchronous=True)
        self.pb.set_archiving('replica', replica, replica=True)

        replica.slow_start(replica=True)

        # Check data correctness on replica
        after = replica.table_checksum("pgbench_accounts")
        self.assertEqual(before, after)

        # Change data on master, take FULL backup from replica,
        # restore taken backup and check that restored data
        # equal to original data
        pgbench = master.pgbench(options=['-T', '3', '-c', '2', '--no-vacuum'])
        pgbench.wait()

        before = master.table_checksum("pgbench_accounts")

        self.wait_until_replica_catch_with_master(master, replica)

        backup_id, _ = self.pb.backup_replica_node('replica', replica,
            master=master,
            options=['--archive-timeout=60'])

        self.pb.validate('replica')
        self.assertEqual(
            'OK', self.pb.show('replica', backup_id)['status'])

        # RESTORE FULL BACKUP TAKEN FROM replica
        node = self.pg_node.make_simple('node')
        node.cleanup()
        self.pb.restore_node('replica', node=node)

        node.set_auto_conf({'port': node.port, 'archive_mode': 'off'})

        node.slow_start()

        # CHECK DATA CORRECTNESS
        after = node.table_checksum("pgbench_accounts")
        self.assertEqual(before, after)
        node.cleanup()

        # Change data on master, make PAGE backup from replica,
        # restore taken backup and check that restored data equal
        # to original data
        pgbench = master.pgbench(
            options=['-T', '15', '-c', '1', '--no-vacuum'])

        backup_id, _ = self.pb.backup_replica_node('replica',
            replica, backup_type='page',
            master=master,
            options=['--archive-timeout=60'])

        pgbench.wait()

        lsn = self.switch_wal_segment(master)

        before = master.table_checksum("pgbench_accounts")

        self.pb.validate('replica')
        self.assertEqual(
            'OK', self.pb.show('replica', backup_id)['status'])

        # RESTORE PAGE BACKUP TAKEN FROM replica
        self.pb.restore_node('replica', node,
            backup_id=backup_id)

        node.set_auto_conf({'port': node.port, 'archive_mode': 'off'})

        node.slow_start()

        self.wait_until_lsn_replayed(node, lsn)

        # CHECK DATA CORRECTNESS
        after = node.table_checksum("pgbench_accounts")
        self.assertEqual(
            before, after, 'Restored data is not equal to original')

        self.pb.add_instance('node', node)
        self.pb.backup_node('node', node, options=['--stream'])

    # @unittest.skip("skip")
    def test_basic_make_replica_via_restore(self):
        """
        make archive master, take full and page archive backups from master,
        set replica, make archive backup from replica
        """
        backup_dir = self.backup_dir
        master = self.pg_node.make_simple('master',
            set_replication=True,
            pg_options={
                'archive_timeout': '10s'})

        self.pb.init()
        self.pb.add_instance('master', master)
        self.pb.set_archiving('master', master)
        master.slow_start()

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.backup_node('master', master)

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,8192) i")

        before = master.table_checksum("t_heap")

        backup_id = self.pb.backup_node('master', master, backup_type='page')
        self.pb.restore_node('master', replica, options=['-R'])

        # Settings for Replica
        self.pb.add_instance('replica', replica)
        self.pb.set_archiving('replica', replica, replica=True)
        self.set_replica(master, replica, synchronous=True)

        replica.slow_start(replica=True)

        self.pb.backup_node('replica', replica,
            options=['--archive-timeout=30s', '--stream'])

    # @unittest.skip("skip")
    def test_take_backup_from_delayed_replica(self):
        """
        make archive master, take full backups from master,
        restore full backup as delayed replica, launch pgbench,
        take FULL, PAGE and DELTA backups from replica
        """
        backup_dir = self.backup_dir
        master = self.pg_node.make_simple('master',
            set_replication=True,
            pg_options={'archive_timeout': '10s'})

        self.pb.init()
        self.pb.add_instance('master', master)
        self.pb.set_archiving('master', master)
        master.slow_start()

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.backup_node('master', master)

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

        self.pb.restore_node('master', replica, options=['-R'])

        # Settings for Replica
        self.pb.add_instance('replica', replica)
        self.pb.set_archiving('replica', replica, replica=True)

        replica.set_auto_conf({'port': replica.port})

        replica.slow_start(replica=True)

        self.wait_until_replica_catch_with_master(master, replica)

        if self.pg_config_version >= self.version_to_num('12.0'):
            replica.set_auto_conf({'recovery_min_apply_delay': '300s'})
        else:
            replica.append_conf(
                'recovery.conf',
                'recovery_min_apply_delay = 300s')

        replica.stop()
        replica.slow_start(replica=True)

        master.pgbench_init(scale=10)

        pgbench = master.pgbench(
            options=['-T', '60', '-c', '2', '--no-vacuum'])

        self.pb.backup_node('replica',
            replica, options=['--archive-timeout=60s'])

        self.pb.backup_node('replica', replica,
            data_dir=replica.data_dir,
            backup_type='page', options=['--archive-timeout=60s'])

        sleep(1)

        self.pb.backup_node('replica', replica,
            backup_type='delta', options=['--archive-timeout=60s'])

        pgbench.wait()

        pgbench = master.pgbench(
            options=['-T', '30', '-c', '2', '--no-vacuum'])

        self.pb.backup_node('replica', replica,
            options=['--stream'])

        self.pb.backup_node('replica', replica,
            backup_type='page', options=['--stream'])

        self.pb.backup_node('replica', replica,
            backup_type='delta', options=['--stream'])

        pgbench.wait()

    # @unittest.skip("skip")
    @needs_gdb
    def test_replica_promote(self):
        """
        start backup from replica, during backup promote replica
        check that backup is failed
        """

        backup_dir = self.backup_dir
        master = self.pg_node.make_simple('master',
            set_replication=True,
            pg_options={
                'archive_timeout': '10s',
                'checkpoint_timeout': '30s',
                'max_wal_size': '32MB'})

        self.pb.init()
        self.pb.add_instance('master', master)
        self.pb.set_archiving('master', master)
        master.slow_start()

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.backup_node('master', master)

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,165000) i")

        self.pb.restore_node('master', replica, options=['-R'])

        # Settings for Replica
        self.pb.add_instance('replica', replica)
        self.pb.set_archiving('replica', replica, replica=True)
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
        gdb = self.pb.backup_node('replica', replica, gdb=True,
            options=['--log-level-file=verbose'])

        gdb.set_breakpoint('backup_data_file')
        gdb.run_until_break()
        gdb.continue_execution_until_break(20)

        replica.promote()

        gdb.continue_execution_until_exit()

        backup_id = self.pb.show('replica')[0]["id"]

        # read log file content
        log_content = self.read_pb_log()

        self.assertIn(
            'ERROR:  the standby was promoted during online backup',
            log_content)

        self.assertIn(
            'WARNING: Backup {0} is running, '
            'setting its status to ERROR'.format(backup_id),
            log_content)

    # @unittest.skip("skip")
    @needs_gdb
    def test_replica_stop_lsn_null_offset(self):
        """
        """
        self.test_env["PGPROBACKUP_TESTS_SKIP_EMPTY_COMMIT"] = "ON"
        backup_dir = self.backup_dir
        master = self.pg_node.make_simple('master',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '1h',
                'wal_level': 'replica'})

        self.pb.init()
        self.pb.add_instance('node', master)
        self.pb.set_archiving('node', master)
        master.slow_start()

        # freeze bgwriter to get rid of RUNNING XACTS records
        bgwriter_pid = master.auxiliary_pids[ProcessType.BackgroundWriter][0]
        gdb_bgwriter = self.gdb_attach(bgwriter_pid)

        self.pb.backup_node('node', master)

        # Create replica
        replica = self.pg_node.make_simple('replica')
        replica.cleanup()
        self.pb.restore_node('node', node=replica)

        # Settings for Replica
        self.set_replica(master, replica, synchronous=True)
        self.pb.set_archiving('node', replica, replica=True)

        replica.slow_start(replica=True)

        self.switch_wal_segment(master)
        self.switch_wal_segment(master)

        output = self.pb.backup_node('node', replica, replica.data_dir,
            options=[
                '--archive-timeout=30',
                '--log-level-console=LOG',
                '--no-validate',
                '--stream'],
            return_id=False)

        self.assertIn(
            'has endpoint 0/4000000 which is '
            'equal or greater than requested LSN',
            output)

        self.assertIn(
            'LOG: Found prior LSN:',
            output)

        # Clean after yourself
        gdb_bgwriter.detach()

    # @unittest.skip("skip")
    @needs_gdb
    def test_replica_stop_lsn_null_offset_next_record(self):
        """
        """
        self.test_env["PGPROBACKUP_TESTS_SKIP_EMPTY_COMMIT"] = "ON"
        backup_dir = self.backup_dir
        master = self.pg_node.make_simple('master',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '1h',
                'wal_level': 'replica'})

        self.pb.init()
        self.pb.add_instance('master', master)
        self.pb.set_archiving('master', master)
        master.slow_start()

        # freeze bgwriter to get rid of RUNNING XACTS records
        bgwriter_pid = master.auxiliary_pids[ProcessType.BackgroundWriter][0]
        gdb_bgwriter = self.gdb_attach(bgwriter_pid)

        self.pb.backup_node('master', master)

        # Create replica
        replica = self.pg_node.make_simple('replica')
        replica.cleanup()
        self.pb.restore_node('master', node=replica)

        # Settings for Replica
        self.pb.add_instance('replica', replica)
        self.set_replica(master, replica, synchronous=True)
        self.pb.set_archiving('replica', replica, replica=True)

        replica.slow_start(replica=True)

        # open connection to master
        conn = master.connect()

        gdb = self.pb.backup_node('replica', replica,
            options=[
                '--archive-timeout=40',
                '--log-level-file=LOG',
                '--no-validate',
                '--stream'],
            gdb=True)

        # Attention! this breakpoint is set to a probackup internal function, not a postgres core one
        gdb.set_breakpoint('pg_stop_backup_consume')
        gdb.run_until_break()

        conn.execute("create table t1()")
        conn.commit()

        sleep(5)

        gdb.continue_execution_until_exit()

        log_content = self.read_pb_log()

        self.assertIn(
            'has endpoint 0/4000000 which is '
            'equal or greater than requested LSN',
            log_content)

        self.assertIn(
            'LOG: Found prior LSN:',
            log_content)

        self.assertIn(
            'INFO: backup->stop_lsn 0/4000000',
            log_content)

        self.assertTrue(self.pb.show('replica')[0]['status'] == 'DONE')

        gdb_bgwriter.detach()

    # @unittest.skip("skip")
    @needs_gdb
    def test_archive_replica_null_offset(self):
        """
        """

        backup_dir = self.backup_dir
        master = self.pg_node.make_simple('master',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '1h',
                'wal_level': 'replica'})

        self.pb.init()
        self.pb.add_instance('node', master)
        self.pb.set_archiving('node', master)
        master.slow_start()

        self.pb.backup_node('node', master)

        # Create replica
        replica = self.pg_node.make_simple('replica')
        replica.cleanup()
        self.pb.restore_node('node', node=replica)

        # Settings for Replica
        self.set_replica(master, replica, synchronous=True)
        self.pb.set_archiving('node', replica, replica=True)

        # freeze bgwriter to get rid of RUNNING XACTS records
        bgwriter_pid = master.auxiliary_pids[ProcessType.BackgroundWriter][0]
        gdb_bgwriter = self.gdb_attach(bgwriter_pid)

        replica.slow_start(replica=True)

        self.switch_wal_segment(master)

        # take backup from replica
        _, output = self.pb.backup_replica_node('node', replica, replica.data_dir,
            master=master,
            options=[
                '--archive-timeout=300',
                '--log-level-file=LOG',
                '--no-validate'],
        )

        self.assertRegex(
            output,
            r'LOG: Looking for LSN 0/[45]000000 in segment: 00000001000000000000000[34]')

        self.assertRegex(
            output,
            r'has endpoint 0/[45]000000 which is '
            r'equal or greater than requested LSN 0/[45]000000')

        self.assertIn(
            'LOG: Found prior LSN:',
            output)

        gdb_bgwriter.detach()

    # @unittest.skip("skip")
    @needs_gdb
    def test_archive_replica_not_null_offset(self):
        """
        """
        backup_dir = self.backup_dir
        master = self.pg_node.make_simple('master',
            set_replication=True,
            pg_options={
                'archive_timeout' : '1h',
                'checkpoint_timeout': '1h',
                'wal_level': 'replica'})

        self.pb.init()
        self.pb.add_instance('node', master)
        self.pb.set_archiving('node', master)
        master.slow_start()

        self.pb.backup_node('node', master)

        # Create replica
        replica = self.pg_node.make_simple('replica')
        replica.cleanup()
        self.pb.restore_node('node', node=replica)

        # Settings for Replica
        self.set_replica(master, replica, synchronous=True)
        self.pb.set_archiving('node', replica, replica=True)

        replica.slow_start(replica=True)

        # take backup from replica
        self.pb.backup_replica_node('node', replica, replica.data_dir,
            master=master,
            options=[
                '--archive-timeout=300',
                '--no-validate'],
        )

        master.execute('select txid_current()')
        self.wait_until_replica_catch_with_master(master, replica)

        output = self.pb.backup_node('node', replica, replica.data_dir,
                options=[
                    '--archive-timeout=10',
                    '--log-level-console=LOG',
                    '--log-level-file=LOG',
                    '--no-validate'],
                expect_error=True)

        self.assertMessage(output, regex=r'LOG: Looking for LSN 0/[45]0000(?!00)[A-F\d]{2} in segment: 0*10*[45]')

        self.assertMessage(output, regex=r'ERROR: WAL segment 0*10*[45] could not be archived in \d+ seconds')

    # @unittest.skip("skip")
    @needs_gdb
    def test_replica_toast(self):
        """
        make archive master, take full and page archive backups from master,
        set replica, make archive backup from replica
        """

        backup_dir = self.backup_dir
        master = self.pg_node.make_simple('master',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '1h',
                'wal_level': 'replica',
                'shared_buffers': '128MB'})

        self.pb.init()
        self.pb.add_instance('master', master)
        self.pb.set_archiving('master', master)
        master.slow_start()

        # freeze bgwriter to get rid of RUNNING XACTS records
        bgwriter_pid = master.auxiliary_pids[ProcessType.BackgroundWriter][0]
        gdb_bgwriter = self.gdb_attach(bgwriter_pid)

        self.pb.backup_node('master', master)

        # Create replica
        replica = self.pg_node.make_simple('replica')
        replica.cleanup()
        self.pb.restore_node('master', node=replica)

        # Settings for Replica
        self.pb.add_instance('replica', replica)
        self.set_replica(master, replica, synchronous=True)
        self.pb.set_archiving('replica', replica, replica=True)

        replica.slow_start(replica=True)

        master.safe_psql(
            'postgres',
            'CREATE TABLE t1 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,10) i')

        self.wait_until_replica_catch_with_master(master, replica)

        output = self.pb.backup_node('replica', replica,
            options=[
                '--archive-timeout=30',
                '--log-level-console=LOG',
                '--no-validate',
                '--stream'],
            return_id=False)

        pgdata = self.pgdata_content(replica.data_dir)

        self.assertIn(
            'LOG: Found prior LSN:',
            output)

        res1 = replica.safe_psql(
            'postgres',
            'select md5(fat_attr) from t1')

        replica.cleanup()

        self.pb.restore_node('replica', node=replica)
        pgdata_restored = self.pgdata_content(replica.data_dir)

        replica.slow_start()

        res2 = replica.safe_psql(
            'postgres',
            'select md5(fat_attr) from t1')

        self.assertEqual(res1, res2)

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        gdb_bgwriter.detach()

    # @unittest.skip("skip")
    @needs_gdb
    def test_start_stop_lsn_in_the_same_segno(self):
        """
        """

        backup_dir = self.backup_dir
        master = self.pg_node.make_simple('master',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '1h',
                'wal_level': 'replica',
                'shared_buffers': '128MB'})

        self.pb.init()
        self.pb.add_instance('master', master)
        master.slow_start()

        # freeze bgwriter to get rid of RUNNING XACTS records
        bgwriter_pid = master.auxiliary_pids[ProcessType.BackgroundWriter][0]
        gdb_bgwriter = self.gdb_attach(bgwriter_pid)

        self.pb.backup_node('master', master, options=['--stream'])

        # Create replica
        replica = self.pg_node.make_simple('replica')
        replica.cleanup()
        self.pb.restore_node('master', node=replica)

        # Settings for Replica
        self.pb.add_instance('replica', replica)
        self.set_replica(master, replica, synchronous=True)

        replica.slow_start(replica=True)

        self.switch_wal_segment(master)
        self.switch_wal_segment(master)

        master.safe_psql(
            'postgres',
            'CREATE TABLE t1 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,10) i')

        master.safe_psql(
            'postgres',
            'CHECKPOINT')

        self.wait_until_replica_catch_with_master(master, replica)

        sleep(60)

        self.pb.backup_node('replica', replica,
            options=[
                '--archive-timeout=30',
                '--log-level-console=LOG',
                '--no-validate',
                '--stream'],
            return_id=False)

        self.pb.backup_node('replica', replica,
            options=[
                '--archive-timeout=30',
                '--log-level-console=LOG',
                '--no-validate',
                '--stream'],
            return_id=False)

        gdb_bgwriter.detach()

    @unittest.skip("skip")
    def test_replica_promote_1(self):
        """
        """
        backup_dir = self.backup_dir
        master = self.pg_node.make_simple('master',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '1h',
                'wal_level': 'replica'})

        self.pb.init()
        self.pb.add_instance('master', master)
        # set replica True, so archive_mode 'always' is used.
        self.pb.set_archiving('master', master, replica=True)
        master.slow_start()

        self.pb.backup_node('master', master)

        # Create replica
        replica = self.pg_node.make_simple('replica')
        replica.cleanup()
        self.pb.restore_node('master', node=replica)

        # Settings for Replica
        self.set_replica(master, replica)

        replica.slow_start(replica=True)

        master.safe_psql(
            'postgres',
            'CREATE TABLE t1 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,10) i')

        self.wait_until_replica_catch_with_master(master, replica)

        self.assertFalse(
            self.instance_wal_exists(backup_dir, master, '000000010000000000000004'))

        wal_file_partial = '000000010000000000000004.partial'

        replica.promote()

        self.wait_instance_wal_exists(backup_dir, 'master', wal_file_partial)

        self.switch_wal_segment(master)

        # sleep to be sure, that any partial timeout is expired
        sleep(70)

        self.assertTrue(
            self.instance_wal_exists(backup_dir, 'master', wal_file_partial),
            "File {0} disappeared".format(wal_file_partial))

    # @unittest.skip("skip")
    def test_replica_promote_2(self):
        """
        """
        backup_dir = self.backup_dir
        master = self.pg_node.make_simple('master',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('master', master)
        # set replica True, so archive_mode 'always' is used.
        self.pb.set_archiving('master', master, replica=True)
        master.slow_start()

        self.pb.backup_node('master', master)

        # Create replica
        replica = self.pg_node.make_simple('replica')
        replica.cleanup()
        self.pb.restore_node('master', node=replica)

        # Settings for Replica
        self.set_replica(master, replica)
        replica.set_auto_conf({'port': replica.port})

        replica.slow_start(replica=True)

        master.safe_psql(
            'postgres',
            'CREATE TABLE t1 AS '
            'SELECT i,'
            '   (select string_agg(md5((i^j)::text), \',\')'
            '    from generate_series(1,5006056) j) AS fat_attr '
            'FROM generate_series(0,1) i')

        self.wait_until_replica_catch_with_master(master, replica)

        replica.promote()

        self.pb.backup_node('master', replica, data_dir=replica.data_dir,
            backup_type='page')

    # @unittest.skip("skip")
    def test_replica_promote_archive_delta(self):
        """
        t3                    /---D3-->
        t2               /------->
        t1 --F---D1--D2--
        """
        backup_dir = self.backup_dir
        node1 = self.pg_node.make_simple('node1',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '30s',
                'archive_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('node', node1)
        self.pb.set_config('node', options=['--archive-timeout=60s'])
        self.pb.set_archiving('node', node1)

        node1.slow_start()

        self.pb.backup_node('node', node1, options=['--stream'])

        # Create replica
        node2 = self.pg_node.make_simple('node2')
        node2.cleanup()
        self.pb.restore_node('node', node=node2)

        # Settings for Replica
        self.set_replica(node1, node2)
        node2.set_auto_conf({'port': node2.port})
        self.pb.set_archiving('node', node2, replica=True)

        node2.slow_start(replica=True)

        create_table(node1, 't1')
        self.wait_until_replica_catch_with_master(node1, node2)

        create_table(node1, 't2')
        self.wait_until_replica_catch_with_master(node1, node2)

        # delta backup on replica on timeline 1
        delta1_id = self.pb.backup_node('node', node2, node2.data_dir,
            'delta', options=['--stream'])

        # delta backup on replica on timeline 1
        delta2_id = self.pb.backup_node('node', node2, node2.data_dir, 'delta')

        self.change_backup_status(backup_dir, 'node', delta2_id, 'ERROR')

        # node2 is now master
        node2.promote()

        create_table(node2, 't3')

        # node1 is now replica
        node1.cleanup()
        # kludge "backup_id=delta1_id"
        self.pb.restore_node('node', node1,
            backup_id=delta1_id,
            options=[
                '--recovery-target-timeline=2',
                '--recovery-target=latest'])

        # Settings for Replica
        self.set_replica(node2, node1)
        node1.set_auto_conf({'port': node1.port})
        self.pb.set_archiving('node', node1, replica=True)

        node1.slow_start(replica=True)

        create_table(node2, 't4')
        self.wait_until_replica_catch_with_master(node2, node1)

        # node1 is back to be a master
        node1.promote()

        sleep(5)

        # delta backup on timeline 3
        self.pb.backup_node('node', node1, node1.data_dir, 'delta',
            options=['--archive-timeout=60'])

        pgdata = self.pgdata_content(node1.data_dir)

        node1.cleanup()
        self.pb.restore_node('node', node=node1)

        pgdata_restored = self.pgdata_content(node1.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_replica_promote_archive_page(self):
        """
        t3                    /---P3-->
        t2               /------->
        t1 --F---P1--P2--
        """
        backup_dir = self.backup_dir
        node1 = self.pg_node.make_simple('node1',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '30s',
                'archive_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('node', node1)
        self.pb.set_archiving('node', node1)
        self.pb.set_config('node', options=['--archive-timeout=60s'])

        node1.slow_start()

        self.pb.backup_node('node', node1, options=['--stream'])

        # Create replica
        node2 = self.pg_node.make_simple('node2')
        node2.cleanup()
        self.pb.restore_node('node', node=node2)

        # Settings for Replica
        self.set_replica(node1, node2)
        node2.set_auto_conf({'port': node2.port})
        self.pb.set_archiving('node', node2, replica=True)

        node2.slow_start(replica=True)

        create_table(node1, 't1')
        self.wait_until_replica_catch_with_master(node1, node2)

        create_table(node1, 't2')
        self.wait_until_replica_catch_with_master(node1, node2)

        # page backup on replica on timeline 1
        page1_id = self.pb.backup_node('node', node2, node2.data_dir,
            'page', options=['--stream'])

        # page backup on replica on timeline 1
        page2_id = self.pb.backup_node('node', node2, node2.data_dir, 'page')

        self.change_backup_status(backup_dir, 'node', page2_id, 'ERROR')

        # node2 is now master
        node2.promote()

        create_table(node2, 't3')

        # node1 is now replica
        node1.cleanup()
        # kludge "backup_id=page1_id"
        self.pb.restore_node('node', node1,
            backup_id=page1_id,
            options=[
                '--recovery-target-timeline=2',
                '--recovery-target=latest'])

        # Settings for Replica
        self.set_replica(node2, node1)
        node1.set_auto_conf({'port': node1.port})
        self.pb.set_archiving('node', node1, replica=True)

        node1.slow_start(replica=True)

        create_table(node2, 't4')
        self.wait_until_replica_catch_with_master(node2, node1)

        # node1 is back to be a master
        node1.promote()
        self.switch_wal_segment(node1)

        sleep(5)

        # delta3_id = self.pb.backup_node(
        #     'node', node2, node2.data_dir, 'delta')
        # page backup on timeline 3
        page3_id = self.pb.backup_node('node', node1, node1.data_dir, 'page',
            options=['--archive-timeout=60'])

        pgdata = self.pgdata_content(node1.data_dir)

        node1.cleanup()
        self.pb.restore_node('node', node=node1)

        pgdata_restored = self.pgdata_content(node1.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_parent_choosing(self):
        """
        """
        backup_dir = self.backup_dir
        master = self.pg_node.make_simple('master',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('master', master)

        master.slow_start()

        self.pb.backup_node('master', master, options=['--stream'])

        # Create replica
        replica = self.pg_node.make_simple('replica')
        replica.cleanup()
        self.pb.restore_node('master', node=replica)

        # Settings for Replica
        self.set_replica(master, replica)
        replica.set_auto_conf({'port': replica.port})

        replica.slow_start(replica=True)

        master.safe_psql(
            'postgres',
            'CREATE TABLE t1 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,20) i')
        self.wait_until_replica_catch_with_master(master, replica)

        self.pb.add_instance('replica', replica)

        full_id = self.pb.backup_node('replica',
            replica, options=['--stream'])

        master.safe_psql(
            'postgres',
            'CREATE TABLE t2 AS '
            'SELECT i, repeat(md5(i::text),5006056) AS fat_attr '
            'FROM generate_series(0,20) i')
        self.wait_until_replica_catch_with_master(master, replica)

        self.pb.backup_node('replica', replica,
            backup_type='delta', options=['--stream'])

        replica.promote()

        # failing, because without archving, it is impossible to
        # take multi-timeline backup.
        self.pb.backup_node('replica', replica,
            backup_type='delta', options=['--stream'])

    # @unittest.skip("skip")
    def test_instance_from_the_past(self):
        """
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)

        node.slow_start()

        full_id = self.pb.backup_node('node', node, options=['--stream'])

        node.pgbench_init(scale=10)
        self.pb.backup_node('node', node, options=['--stream'])
        node.cleanup()

        self.pb.restore_node('node', node=node, backup_id=full_id)
        node.slow_start()

        self.pb.backup_node('node', node, backup_type='delta',
                         options=['--stream'],
                         expect_error="because instance is from the past")
        self.assertMessage(regex='ERROR: Current START LSN .* is lower than START LSN')
        self.assertMessage(contains='It may indicate that we are trying to backup '
                                    'PostgreSQL instance from the past')

    # @unittest.skip("skip")
    def test_replica_via_basebackup(self):
        """
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={'hot_standby': 'on'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)

        node.slow_start()

        node.pgbench_init(scale=10)

        #FULL backup
        full_id = self.pb.backup_node('node', node)

        pgbench = node.pgbench(
            options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        node.cleanup()

        self.pb.restore_node('node', node,
            options=['--recovery-target=latest', '--recovery-target-action=promote'])
        node.slow_start()

        # Timeline 2
        # Take stream page backup from instance in timeline2
        self.pb.backup_node('node', node, backup_type='full',
            options=['--stream', '--log-level-file=verbose'])

        node.cleanup()

        # restore stream backup
        self.pb.restore_node('node', node=node)

        filepath = os.path.join(node.data_dir, 'pg_wal', "00000002.history")
        self.assertTrue(
            os.path.exists(filepath),
            "History file do not exists: {0}".format(filepath))

        node.slow_start()

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        pg_basebackup_path = self.get_bin_path('pg_basebackup')

        self.pb.run_binary(
            [
                pg_basebackup_path, '-p', str(node.port), '-h', 'localhost',
                '-R', '-X', 'stream', '-D', node_restored.data_dir
            ])

        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start(replica=True)

def call_repeat(times, func, *args):
    for i in range(times):
        func(*args)

def create_table(node, name):
    node.safe_psql(
        'postgres',
        f"CREATE TABLE {name} AS "
        "SELECT i, v as fat_attr "
        "FROM generate_series(0,3) i, "
        "     (SELECT string_agg(md5(j::text), '') as v"
        "      FROM generate_series(0,500605) as j) v")

# TODO:
# null offset STOP LSN and latest record in previous segment is conrecord (manual only)
# archiving from promoted delayed replica
