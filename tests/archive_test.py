import os
import shutil
import unittest
from .helpers.ptrack_helpers import ProbackupTest, fs_backup_class, get_relative_path
from pg_probackup2.gdb import needs_gdb
from .helpers.data_helpers import tail_file
import subprocess
from sys import exit
from time import sleep
from testgres import ProcessType


class ArchiveTest(ProbackupTest):

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_pgpro434_1(self):
        """Description in jira issue PGPRO-434"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector from "
            "generate_series(0,100) i")

        result = node.table_checksum("t_heap")
        self.pb.backup_node('node', node)
        node.cleanup()

        self.pb.restore_node('node', node=node)
        node.slow_start()

        # Recreate backup catalog
        backup_dir.cleanup()
        self.pb.init()
        self.pb.add_instance('node', node)

        # Make backup
        self.pb.backup_node('node', node)
        node.cleanup()

        # Restore Database
        self.pb.restore_node('node', node=node)
        node.slow_start()

        self.assertEqual(
            result, node.table_checksum("t_heap"),
            'data after restore not equal to original data')

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_pgpro434_2(self):
        """
        Check that timelines are correct.
        WAITING PGPRO-1053 for --immediate
        """
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '30s'}
            )

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FIRST TIMELINE
        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,100) i")
        backup_id = self.pb.backup_node('node', node)
        node.safe_psql(
            "postgres",
            "insert into t_heap select 100501 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1) i")

        # SECOND TIMELIN
        node.cleanup()
        self.pb.restore_node('node', node,
            options=['--immediate', '--recovery-target-action=promote'])
        node.slow_start()

        if self.verbose:
            print(node.safe_psql(
                "postgres",
                "select redo_wal_file from pg_control_checkpoint()"))
            self.assertFalse(
                node.execute(
                    "postgres",
                    "select exists(select 1 "
                    "from t_heap where id = 100501)")[0][0],
                'data after restore not equal to original data')

        node.safe_psql(
            "postgres",
            "insert into t_heap select 2 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(100,200) i")

        backup_id = self.pb.backup_node('node', node)

        node.safe_psql(
            "postgres",
            "insert into t_heap select 100502 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,256) i")

        # THIRD TIMELINE
        node.cleanup()
        self.pb.restore_node('node', node,
            options=['--immediate', '--recovery-target-action=promote'])
        node.slow_start()

        if self.verbose:
            print(
                node.safe_psql(
                    "postgres",
                    "select redo_wal_file from pg_control_checkpoint()"))

            node.safe_psql(
                "postgres",
                "insert into t_heap select 3 as id, md5(i::text) as text, "
                "md5(repeat(i::text,10))::tsvector as tsvector "
                "from generate_series(200,300) i")

        backup_id = self.pb.backup_node('node', node)

        result = node.table_checksum("t_heap")
        node.safe_psql(
            "postgres",
            "insert into t_heap select 100503 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,256) i")

        # FOURTH TIMELINE
        node.cleanup()
        self.pb.restore_node('node', node,
            options=['--immediate', '--recovery-target-action=promote'])
        node.slow_start()

        if self.verbose:
            print('Fourth timeline')
            print(node.safe_psql(
                "postgres",
                "select redo_wal_file from pg_control_checkpoint()"))

        # FIFTH TIMELINE
        node.cleanup()
        self.pb.restore_node('node', node,
            options=['--immediate', '--recovery-target-action=promote'])
        node.slow_start()

        if self.verbose:
            print('Fifth timeline')
            print(node.safe_psql(
                "postgres",
                "select redo_wal_file from pg_control_checkpoint()"))

        # SIXTH TIMELINE
        node.cleanup()
        self.pb.restore_node('node', node,
            options=['--immediate', '--recovery-target-action=promote'])
        node.slow_start()

        if self.verbose:
            print('Sixth timeline')
            print(node.safe_psql(
                "postgres",
                "select redo_wal_file from pg_control_checkpoint()"))

        self.assertFalse(
            node.execute(
                "postgres",
                "select exists(select 1 from t_heap where id > 100500)")[0][0],
            'data after restore not equal to original data')

        self.assertEqual(result, node.table_checksum("t_heap"),
            'data after restore not equal to original data')

    # @unittest.skip("skip")
    @needs_gdb
    def test_pgpro434_3(self):
        """
        Check pg_stop_backup_timeout, needed backup_timeout
        Fixed in commit d84d79668b0c139 and assert fixed by ptrack 1.7
        """

        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)

        node.slow_start()

        gdb = self.pb.backup_node('node', node,
                options=[
                    "--archive-timeout=10",
                    "--log-level-file=LOG"],
                gdb=True)

        # Attention! this breakpoint has been set on internal probackup function, not on a postgres core one
        gdb.set_breakpoint('pg_stop_backup')
        gdb.run_until_break()

        node.set_auto_conf({'archive_command': 'exit 1'})
        node.reload()

        sleep(1)

        gdb.continue_execution_until_exit()

        sleep(1)

        log_content = self.read_pb_log()

        self.assertIn(
            "ERROR: WAL segment 000000010000000000000003 could not be archived in 10 seconds",
            log_content)

        log_file = os.path.join(node.logs_dir, 'postgresql.log')
        with open(log_file, 'r') as f:
            log_content = f.read()

        self.assertNotIn(
            'FailedAssertion',
            log_content,
            'PostgreSQL crashed because of a failed assert')

    # @unittest.skip("skip")
    @needs_gdb
    def test_pgpro434_4(self):
        """
        Check pg_stop_backup_timeout, libpq-timeout requested.
        Fixed in commit d84d79668b0c139 and assert fixed by ptrack 1.7
        """

        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)

        node.slow_start()

        gdb = self.pb.backup_node('node', node,
                options=[
                    "--archive-timeout=10",
                    "--log-level-file=info"],
                gdb=True)

        # Attention! this breakpoint has been set on internal probackup function, not on a postgres core one
        gdb.set_breakpoint('pg_stop_backup')
        gdb.run_until_break()

        node.set_auto_conf({'archive_command': 'exit 1'})
        node.reload()

        os.environ["PGAPPNAME"] = "foo"

        pid = node.safe_psql(
            "postgres",
            "SELECT pid "
            "FROM pg_stat_activity "
            "WHERE application_name = 'pg_probackup'").decode('utf-8').rstrip()

        os.environ["PGAPPNAME"] = "pg_probackup"

        postgres_gdb = self.gdb_attach(pid)
        if self.pg_config_version < 150000:
            postgres_gdb.set_breakpoint('do_pg_stop_backup')
        else:
            postgres_gdb.set_breakpoint('do_pg_backup_stop')
        postgres_gdb.continue_execution_until_running()

        gdb.continue_execution_until_exit()

        log_content = self.read_pb_log()

        if self.pg_config_version < 150000:
            self.assertIn(
                "ERROR: pg_stop_backup doesn't answer in 10 seconds, cancel it",
                log_content)
        else:
            self.assertIn(
                "ERROR: pg_backup_stop doesn't answer in 10 seconds, cancel it",
                log_content)

        log_file = os.path.join(node.logs_dir, 'postgresql.log')
        with open(log_file, 'r') as f:
            log_content = f.read()

        self.assertNotIn(
            'FailedAssertion',
            log_content,
            'PostgreSQL crashed because of a failed assert')

    # @unittest.skip("skip")
    def test_archive_push_file_exists(self):
        """Archive-push if file exists"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)

        suffix = self.compress_suffix
        walfile = '000000010000000000000001'+suffix
        self.write_instance_wal(backup_dir, 'node', walfile,
                              b"blablablaadssaaaaaaaaaaaaaaa")

        node.slow_start()
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,100500) i")
        log_file = os.path.join(node.logs_dir, 'postgresql.log')

        self.switch_wal_segment(node)

        log = tail_file(log_file, linetimeout=30, totaltimeout=120,
                        collect=True)
        log.wait(contains = 'The failed archive command was')

        self.assertIn(
            'LOG:  archive command failed with exit code 1',
            log.content)

        self.assertIn(
            'DETAIL:  The failed archive command was:',
            log.content)

        self.assertIn(
            'pg_probackup archive-push WAL file',
            log.content)

        if self.archive_compress:
            self.assertIn(
                'WAL file already exists and looks like it is damaged',
                log.content)
        else:
            self.assertIn(
                'WAL file already exists in archive with different checksum',
                log.content)

        self.assertNotIn(
            'pg_probackup archive-push completed successfully', log.content)

        # btw check that console coloring codes are not slipped into log file
        self.assertNotIn('[0m', log.content)
        log.stop_collect()

        wal_src = os.path.join(
            node.data_dir, 'pg_wal', '000000010000000000000001')
        with open(wal_src, 'rb') as f_in:
            file_content = f_in.read()

        self.write_instance_wal(backup_dir, 'node', walfile, file_content,
                                compress = self.archive_compress)

        self.switch_wal_segment(node)

        log.wait(contains = 'pg_probackup archive-push completed successfully')

    # @unittest.skip("skip")
    def test_archive_push_file_exists_overwrite(self):
        """Archive-push if file exists"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={'checkpoint_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)

        suffix = self.compress_suffix
        walfile = '000000010000000000000001'+suffix
        self.write_instance_wal(backup_dir, 'node', walfile,
                              b"blablablaadssaaaaaaaaaaaaaaa")

        node.slow_start()
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,100500) i")
        log_file = os.path.join(node.logs_dir, 'postgresql.log')

        self.switch_wal_segment(node)

        log = tail_file(log_file, linetimeout=30, collect=True)
        log.wait(contains = 'The failed archive command was')

        self.assertIn(
            'LOG:  archive command failed with exit code 1', log.content)
        self.assertIn(
            'DETAIL:  The failed archive command was:', log.content)
        self.assertIn(
            'pg_probackup archive-push WAL file', log.content)
        self.assertNotIn('overwriting', log.content)
        if self.archive_compress:
            self.assertIn(
                'WAL file already exists and looks like '
                'it is damaged', log.content)
        else:
            self.assertIn(
                'WAL file already exists in archive with '
                'different checksum', log.content)

        self.assertNotIn(
            'pg_probackup archive-push completed successfully', log.content)

        self.pb.set_archiving('node', node, overwrite=True)
        node.reload()
        self.switch_wal_segment(node)

        log.drop_content()
        log.wait(contains = 'pg_probackup archive-push completed successfully')

        if self.archive_compress:
            self.assertIn(
                'WAL file already exists and looks like '
                'it is damaged, overwriting', log.content)
        else:
            self.assertIn(
                'WAL file already exists in archive with '
                'different checksum, overwriting', log.content)

    @unittest.skip("should be redone with file locking")
    def test_archive_push_part_file_exists_not_stale(self):
        """Archive-push if .part file exists and it is not stale"""
        # TODO: this test is not completely obsolete, but should be rewritten
        # with use of file locking when push_file_internal will use it.
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node, archive_timeout=60)

        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t1()")
        self.switch_wal_segment(node)

        node.safe_psql(
            "postgres",
            "create table t2()")

        filename_orig = node.safe_psql(
            "postgres",
            "SELECT file_name "
            "FROM pg_walfile_name_offset(pg_current_wal_flush_lsn());").rstrip()

        filename_orig = filename_orig.decode('utf-8')

        # form up path to next .part WAL segment
        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        filename = filename_orig + self.compress_suffix + '.part'
        file = os.path.join(wals_dir, filename)

        with open(file, 'a+b') as f:
            f.write(b"blahblah")
            f.flush()
            f.close()

        self.switch_wal_segment(node)
        sleep(30)

        with open(file, 'a+b') as f:
            f.write(b"blahblahblahblah")
            f.flush()
            f.close()

        sleep(40)

        # check that segment is NOT archived
        filename_orig += self.compress_suffix

        file = os.path.join(wals_dir, filename_orig)

        self.assertFalse(os.path.isfile(file))

        # log_file = os.path.join(node.logs_dir, 'postgresql.log')
        # with open(log_file, 'r') as f:
        #     log_content = f.read()
        #     self.assertIn(
        #         'is not stale',
        #         log_content)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    @needs_gdb
    def test_replica_archive(self):
        """
        make node without archiving, take stream backup and
        turn it into replica, set replica with archiving,
        make archive backup from replica
        """
        backup_dir = self.backup_dir
        master = self.pg_node.make_simple('master',
            set_replication=True,
            pg_options={
                'archive_timeout': '10s',
                'checkpoint_timeout': '30s',
                'max_wal_size': '32MB'})

        self.pb.init()
        # ADD INSTANCE 'MASTER'
        self.pb.add_instance('master', master)
        master.slow_start()

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")

        self.pb.backup_node('master', master, options=['--stream'])
        before = master.table_checksum("t_heap")

        # Settings for Replica
        self.pb.restore_node('master', node=replica)
        self.set_replica(master, replica, synchronous=True)

        self.pb.add_instance('replica', replica)
        self.pb.set_archiving('replica', replica, replica=True)
        replica.slow_start(replica=True)

        # Check data correctness on replica
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

        backup_id = self.pb.backup_node('replica', replica,
            options=[
                '--archive-timeout=30',
                '--stream'])

        self.pb.validate('replica')
        self.assertEqual(
            'OK', self.pb.show('replica', backup_id)['status'])

        # RESTORE FULL BACKUP TAKEN FROM replica
        node = self.pg_node.make_simple('node')
        node.cleanup()
        self.pb.restore_node('replica', node=node)

        node.set_auto_conf({'port': node.port})
        node.slow_start()
        # CHECK DATA CORRECTNESS
        after = node.table_checksum("t_heap")
        self.assertEqual(before, after)

        # Change data on master, make PAGE backup from replica,
        # restore taken backup and check that restored data equal
        # to original data
        master.psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(512,80680) i")

        before = master.table_checksum("t_heap")

        self.wait_until_replica_catch_with_master(master, replica)

        backup_id, _ = self.pb.backup_replica_node('replica',
            replica, backup_type='page',
            master=master,
            options=[
                '--archive-timeout=60',
                '--stream'])

        self.pb.validate('replica')
        self.assertEqual(
            'OK', self.pb.show('replica', backup_id)['status'])

        # RESTORE PAGE BACKUP TAKEN FROM replica
        node.cleanup()
        self.pb.restore_node('replica', node, backup_id=backup_id)

        node.set_auto_conf({'port': node.port})

        node.slow_start()
        # CHECK DATA CORRECTNESS
        after = node.table_checksum("t_heap")
        self.assertEqual(before, after)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_master_and_replica_parallel_archiving(self):
        """
            make node 'master 'with archiving,
            take archive backup and turn it into replica,
            set replica with archiving, make archive backup from replica,
            make archive backup from master
        """
        master = self.pg_node.make_simple('master',
            set_replication=True,
            pg_options={
                'archive_timeout': '10s'}
            )

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.init()
        # ADD INSTANCE 'MASTER'
        self.pb.add_instance('master', master)
        self.pb.set_archiving('master', master)
        master.slow_start()

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")

        # TAKE FULL ARCHIVE BACKUP FROM MASTER
        self.pb.backup_node('master', master)
        # GET LOGICAL CONTENT FROM MASTER
        before = master.table_checksum("t_heap")
        # GET PHYSICAL CONTENT FROM MASTER
        pgdata_master = self.pgdata_content(master.data_dir)

        # Settings for Replica
        self.pb.restore_node('master', node=replica)
        # CHECK PHYSICAL CORRECTNESS on REPLICA
        pgdata_replica = self.pgdata_content(replica.data_dir)
        self.compare_pgdata(pgdata_master, pgdata_replica)

        self.set_replica(master, replica)
        # ADD INSTANCE REPLICA
        self.pb.add_instance('replica', replica)
        # SET ARCHIVING FOR REPLICA
        self.pb.set_archiving('replica', replica, replica=True)
        replica.slow_start(replica=True)

        # CHECK LOGICAL CORRECTNESS on REPLICA
        after = replica.table_checksum("t_heap")
        self.assertEqual(before, after)

        master.psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0, 60000) i")

        backup_id = self.pb.backup_node('replica', replica,
            options=[
                '--archive-timeout=30',
                '--stream'])

        self.pb.validate('replica')
        self.assertEqual(
            'OK', self.pb.show('replica', backup_id)['status'])

        # TAKE FULL ARCHIVE BACKUP FROM MASTER
        backup_id = self.pb.backup_node('master', master)
        self.pb.validate('master')
        self.assertEqual(
            'OK', self.pb.show('master', backup_id)['status'])

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    @needs_gdb
    def test_basic_master_and_replica_concurrent_archiving(self):
        """
            make node 'master 'with archiving,
            take archive backup and turn it into replica,
            set replica with archiving,
            make sure that archiving on both node is working.
        """
        master = self.pg_node.make_simple('master',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '30s',
                'archive_timeout': '10s'})

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.init()
        # ADD INSTANCE 'MASTER'
        self.pb.add_instance('master', master)
        self.pb.set_archiving('master', master)
        master.slow_start()

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")

        master.pgbench_init(scale=5)

        # TAKE FULL ARCHIVE BACKUP FROM MASTER
        self.pb.backup_node('master', master)
        # GET PHYSICAL CONTENT FROM MASTER
        master.stop()
        pgdata_master = self.pgdata_content(master.data_dir, exclude_dirs = ['pg_stat'])
        master.start()

        # Settings for Replica
        self.pb.restore_node('master', node=replica)
        # CHECK PHYSICAL CORRECTNESS on REPLICA
        pgdata_replica = self.pgdata_content(replica.data_dir, exclude_dirs = ['pg_stat'])

        self.set_replica(master, replica, synchronous=False)
        # ADD INSTANCE REPLICA
        # self.pb.add_instance('replica', replica)
        # SET ARCHIVING FOR REPLICA
        self.pb.set_archiving('master', replica, replica=True)
        replica.slow_start(replica=True)


        # GET LOGICAL CONTENT FROM MASTER
        before = master.table_checksum("t_heap")
        # CHECK LOGICAL CORRECTNESS on REPLICA
        after = replica.table_checksum("t_heap")

        # self.assertEqual(before, after)
        if before != after:
            self.compare_pgdata(pgdata_master, pgdata_replica)

        master.psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")

        # freeze bgwriter to get rid of RUNNING XACTS records
        bgwriter_pid = master.auxiliary_pids[ProcessType.BackgroundWriter][0]
        gdb_bgwriter = self.gdb_attach(bgwriter_pid)

        # TAKE FULL ARCHIVE BACKUP FROM REPLICA
        backup_id = self.pb.backup_node('master', replica)

        self.pb.validate('master')
        self.assertEqual(
            'OK', self.pb.show('master', backup_id)['status'])

        # TAKE FULL ARCHIVE BACKUP FROM MASTER
        backup_id = self.pb.backup_node('master', master)
        self.pb.validate('master')
        self.assertEqual(
            'OK', self.pb.show('master', backup_id)['status'])

        master.pgbench_init(scale=10)

        sleep(10)

        replica.promote()

        master.pgbench_init(scale=10)
        replica.pgbench_init(scale=10)

        self.pb.backup_node('master', master)
        self.pb.backup_node('master', replica, data_dir=replica.data_dir)

        # Clean after yourself
        gdb_bgwriter.detach()

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_concurrent_archiving(self):
        """
        Concurrent archiving from master, replica and cascade replica
        https://github.com/postgrespro/pg_probackup/issues/327

        For PG >= 11 it is expected to pass this test
        """

        if self.pg_config_version < self.version_to_num('11.0'):
            self.skipTest('You need PostgreSQL >= 11 for this test')

        master = self.pg_node.make_simple('master',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', master)
        self.pb.set_archiving('node', master, replica=True)
        master.slow_start()

        master.pgbench_init(scale=10)

        # TAKE FULL ARCHIVE BACKUP FROM MASTER
        self.pb.backup_node('node', master)

        # Settings for Replica
        replica = self.pg_node.make_simple('replica')
        replica.cleanup()
        self.pb.restore_node('node', node=replica)

        self.set_replica(master, replica, synchronous=True)
        self.pb.set_archiving('node', replica, replica=True)
        replica.set_auto_conf({'port': replica.port})
        replica.slow_start(replica=True)

        # create cascade replicas
        replica1 = self.pg_node.make_simple('replica1')
        replica1.cleanup()

        # Settings for casaced replica
        self.pb.restore_node('node', node=replica1)
        self.set_replica(replica, replica1, synchronous=False)
        replica1.set_auto_conf({'port': replica1.port})
        replica1.slow_start(replica=True)

        # Take full backup from master
        self.pb.backup_node('node', master)

        pgbench = master.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            options=['-T', '30', '-c', '1'])

        # Take several incremental backups from master
        self.pb.backup_node('node', master, backup_type='page', options=['--no-validate'])

        self.pb.backup_node('node', master, backup_type='page', options=['--no-validate'])

        pgbench.wait()
        pgbench.stdout.close()

        with open(os.path.join(master.logs_dir, 'postgresql.log'), 'r') as f:
            log_content = f.read()
        self.assertNotIn('different checksum', log_content)

        with open(os.path.join(replica.logs_dir, 'postgresql.log'), 'r') as f:
            log_content = f.read()
        self.assertNotIn('different checksum', log_content)

        with open(os.path.join(replica1.logs_dir, 'postgresql.log'), 'r') as f:
            log_content = f.read()
        self.assertNotIn('different checksum', log_content)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_archive_pg_receivexlog(self):
        """Test backup with pg_receivexlog wal delivary method"""
        backup_dir = self.backup_dir
        if not backup_dir.is_file_based:
            self.skipTest('test has no meaning for cloud storage')

        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()
        pg_receivexlog_path = self.get_bin_path('pg_receivewal')

        pg_receivexlog = self.pb.run_binary(
            [
                pg_receivexlog_path, '-p', str(node.port), '--synchronous',
                '-D', os.path.join(backup_dir, 'wal', 'node')
            ], asynchronous=True)

        if pg_receivexlog.returncode:
            self.assertFalse(
                True,
                'Failed to start pg_receivexlog: {0}'.format(
                    pg_receivexlog.communicate()[1]))

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")

        self.pb.backup_node('node', node)

        # PAGE
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(10000,20000) i")

        self.pb.backup_node(
            'node',
            node,
            backup_type='page'
        )
        result = node.table_checksum("t_heap")
        self.pb.validate()

        # Check data correctness
        node.cleanup()
        self.pb.restore_node('node', node=node)
        node.slow_start()

        self.assertEqual(
            result,
            node.table_checksum("t_heap"),
            'data after restore not equal to original data')

        # Clean after yourself
        pg_receivexlog.kill()

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_archive_pg_receivexlog_compression_pg10(self):
        """Test backup with pg_receivewal compressed wal delivary method"""
        backup_dir = self.backup_dir
        if not backup_dir.is_file_based:
            self.skipTest('test has no meaning for cloud storage')

        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '30s'}
            )
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        pg_receivexlog_path = self.get_bin_path('pg_receivewal')
        pg_receivexlog = self.pb.run_binary(
            [
                pg_receivexlog_path, '-p', str(node.port), '--synchronous',
                '-Z', '9', '-D', os.path.join(backup_dir, 'wal', 'node')
            ], asynchronous=True)

        if pg_receivexlog.returncode:
            self.assertFalse(
                True,
                'Failed to start pg_receivexlog: {0}'.format(
                    pg_receivexlog.communicate()[1]))

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")

        self.pb.backup_node('node', node)

        # PAGE
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(10000,20000) i")

        self.pb.backup_node('node', node,
            backup_type='page'
            )
        result = node.table_checksum("t_heap")
        self.pb.validate()

        # Check data correctness
        node.cleanup()
        self.pb.restore_node('node', node=node)
        node.slow_start()

        self.assertEqual(
            result, node.table_checksum("t_heap"),
            'data after restore not equal to original data')

        # Clean after yourself
        pg_receivexlog.kill()

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_archive_catalog(self):
        """
        ARCHIVE replica:

        t6                     |-----------------------
        t5                     |                           |-------
                               |                           |
        t4                     |                      |-------------- 
                               |                      |
        t3                     |      |--B1--|/|--B2-|/|-B3---
                               |      |
        t2                  |--A1--------A2---
        t1  ---------Y1--Y2--

        ARCHIVE master:
        t1  -Z1--Z2---
        """
        backup_dir = self.backup_dir
        master = self.pg_node.make_simple('master',
            set_replication=True,
            pg_options={
                'archive_timeout': '30s',
                'checkpoint_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('master', master)
        self.pb.set_archiving('master', master)

        master.slow_start()

        # FULL
        master.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")

        self.pb.backup_node('master', master)

        # PAGE
        master.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(10000,20000) i")

        self.pb.backup_node('master', master, backup_type='page')

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()
        self.pb.restore_node('master', node=replica)
        self.set_replica(master, replica)

        self.pb.add_instance('replica', replica)
        self.pb.set_archiving('replica', replica, replica=True)

        replica.slow_start(replica=True)

        # FULL backup replica
        Y1 = self.pb.backup_node('replica', replica,
            options=['--stream', '--archive-timeout=60s'])

        master.pgbench_init(scale=5)

        # PAGE backup replica
        Y2 = self.pb.backup_node('replica', replica,
            backup_type='page', options=['--stream', '--archive-timeout=60s'])

        # create timeline t2
        replica.promote()

        # FULL backup replica
        A1 = self.pb.backup_node('replica', replica)

        replica.pgbench_init(scale=5)

        replica.safe_psql(
            'postgres',
            "CREATE TABLE t1 (a text)")

        target_xid = None
        with replica.connect("postgres") as con:
            res = con.execute(
                "INSERT INTO t1 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]

        # DELTA backup replica
        A2 = self.pb.backup_node('replica', replica, backup_type='delta')

        # create timeline t3
        replica.cleanup()
        self.pb.restore_node('replica', replica,
            options=[
                '--recovery-target-xid={0}'.format(target_xid),
                '--recovery-target-timeline=2',
                '--recovery-target-action=promote'])

        replica.slow_start()

        B1 = self.pb.backup_node('replica', replica)

        replica.pgbench_init(scale=2)

        B2 = self.pb.backup_node('replica', replica, backup_type='page')

        replica.pgbench_init(scale=2)

        target_xid = None
        with replica.connect("postgres") as con:
            res = con.execute(
                "INSERT INTO t1 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]

        B3 = self.pb.backup_node('replica', replica, backup_type='page')

        replica.pgbench_init(scale=2)

        # create timeline t4
        replica.cleanup()
        self.pb.restore_node('replica', replica,
            options=[
                '--recovery-target-xid={0}'.format(target_xid),
                '--recovery-target-timeline=3',
                '--recovery-target-action=promote'])

        replica.slow_start()

        replica.safe_psql(
            'postgres',
            'CREATE TABLE '
            't2 as select i, '
            'repeat(md5(i::text),5006056) as fat_attr '
            'from generate_series(0,6) i')

        target_xid = None
        with replica.connect("postgres") as con:
            res = con.execute(
                "INSERT INTO t1 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]

        replica.safe_psql(
            'postgres',
            'CREATE TABLE '
            't3 as select i, '
            'repeat(md5(i::text),5006056) as fat_attr '
            'from generate_series(0,10) i')

        # create timeline t5
        replica.cleanup()
        self.pb.restore_node('replica', replica,
            options=[
                '--recovery-target-xid={0}'.format(target_xid),
                '--recovery-target-timeline=4',
                '--recovery-target-action=promote'])

        replica.slow_start()

        replica.safe_psql(
            'postgres',
            'CREATE TABLE '
            't4 as select i, '
            'repeat(md5(i::text),5006056) as fat_attr '
            'from generate_series(0,6) i')

        # create timeline t6
        replica.cleanup()

        self.pb.restore_node('replica', replica, backup_id=A1,
            options=[
                '--recovery-target=immediate',
                '--recovery-target-action=promote'])
        replica.slow_start()

        replica.pgbench_init(scale=2)

        sleep(5)

        show = self.pb.show_archive(as_text=True)
        show = self.pb.show_archive()

        for instance in show:
            if instance['instance'] == 'replica':
                replica_timelines = instance['timelines']

            if instance['instance'] == 'master':
                master_timelines = instance['timelines']

        # check that all timelines are ok
        for timeline in replica_timelines:
            self.assertTrue(timeline['status'], 'OK')

        # check that all timelines are ok
        for timeline in master_timelines:
            self.assertTrue(timeline['status'], 'OK')

        # check that t3 is ok
        self.pb.show_archive()

        # create holes in t3
        suffix = self.compress_suffix
        self.remove_instance_wal(backup_dir, 'replica', '000000030000000000000017' + suffix)
        self.remove_instance_wal(backup_dir, 'replica', '000000030000000000000012' + suffix)
        self.remove_instance_wal(backup_dir, 'replica', '000000030000000000000013' + suffix)

        # check that t3 is not OK
        show = self.pb.show_archive()

        show = self.pb.show_archive()

        for instance in show:
            if instance['instance'] == 'replica':
                replica_timelines = instance['timelines']

        # sanity
        for timeline in replica_timelines:
            if timeline['tli'] == 1:
                timeline_1 = timeline
                continue

            if timeline['tli'] == 2:
                timeline_2 = timeline
                continue

            if timeline['tli'] == 3:
                timeline_3 = timeline
                continue

            if timeline['tli'] == 4:
                timeline_4 = timeline
                continue

            if timeline['tli'] == 5:
                timeline_5 = timeline
                continue

            if timeline['tli'] == 6:
                timeline_6 = timeline
                continue

        self.assertEqual(timeline_6['status'], "OK")
        self.assertEqual(timeline_5['status'], "OK")
        self.assertEqual(timeline_4['status'], "OK")
        self.assertEqual(timeline_3['status'], "DEGRADED")
        self.assertEqual(timeline_2['status'], "OK")
        self.assertEqual(timeline_1['status'], "OK")

        self.assertEqual(len(timeline_3['lost-segments']), 2)
        self.assertEqual(
            timeline_3['lost-segments'][0]['begin-segno'],
            '000000030000000000000012')
        self.assertEqual(
            timeline_3['lost-segments'][0]['end-segno'],
            '000000030000000000000013')
        self.assertEqual(
            timeline_3['lost-segments'][1]['begin-segno'],
            '000000030000000000000017')
        self.assertEqual(
            timeline_3['lost-segments'][1]['end-segno'],
            '000000030000000000000017')

        self.assertEqual(len(timeline_6['backups']), 0)
        self.assertEqual(len(timeline_5['backups']), 0)
        self.assertEqual(len(timeline_4['backups']), 0)
        self.assertEqual(len(timeline_3['backups']), 3)
        self.assertEqual(len(timeline_2['backups']), 2)
        self.assertEqual(len(timeline_1['backups']), 2)

        # check closest backup correctness
        self.assertEqual(timeline_6['closest-backup-id'], A1)
        self.assertEqual(timeline_5['closest-backup-id'], B2)
        self.assertEqual(timeline_4['closest-backup-id'], B2)
        self.assertEqual(timeline_3['closest-backup-id'], A1)
        self.assertEqual(timeline_2['closest-backup-id'], Y2)

        # check parent tli correctness
        self.assertEqual(timeline_6['parent-tli'], 2)
        self.assertEqual(timeline_5['parent-tli'], 4)
        self.assertEqual(timeline_4['parent-tli'], 3)
        self.assertEqual(timeline_3['parent-tli'], 2)
        self.assertEqual(timeline_2['parent-tli'], 1)
        self.assertEqual(timeline_1['parent-tli'], 0)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_archive_catalog_1(self):
        """
        double segment - compressed and not
        """
        if not self.archive_compress:
            self.skipTest('You need to enable ARCHIVE_COMPRESSION '
                          'for this test to run')

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'archive_timeout': '30s',
                'checkpoint_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node, compress=True)

        node.slow_start()

        # FULL
        self.pb.backup_node('node', node)
        node.pgbench_init(scale=2)
        tailer = tail_file(os.path.join(node.logs_dir, 'postgresql.log'))
        tailer.wait_archive_push_completed()
        node.stop()

        file_content = self.read_instance_wal(backup_dir, 'node',
                                            '000000010000000000000001'+self.compress_suffix,
                                              decompress=True)
        self.write_instance_wal(backup_dir, 'node', '000000010000000000000002',
                                file_content)

        show = self.pb.show_archive()

        for instance in show:
            timelines = instance['timelines']

        # sanity
        for timeline in timelines:
            self.assertEqual(
                timeline['min-segno'],
                '000000010000000000000001')
            self.assertEqual(timeline['status'], 'OK')

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_archive_catalog_2(self):
        """
        double segment - compressed and not
        """
        if not self.archive_compress:
            self.skipTest('You need to enable ARCHIVE_COMPRESSION '
                          'for this test to run')

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'archive_timeout': '30s',
                'checkpoint_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node, compress=True)

        node.slow_start()

        # FULL
        self.pb.backup_node('node', node)
        node.pgbench_init(scale=2)
        tailer = tail_file(os.path.join(node.logs_dir, "postgresql.log"))
        tailer.wait_archive_push_completed()
        node.stop()

        suffix = self.compress_suffix
        file_content = self.read_instance_wal(backup_dir, 'node',
                                            '000000010000000000000001'+suffix,
                                              decompress=True)
        self.write_instance_wal(backup_dir, 'node', '000000010000000000000002',
                                file_content)

        self.remove_instance_wal(backup_dir, 'node',
                               '000000010000000000000001'+suffix)

        show = self.pb.show_archive()

        for instance in show:
            timelines = instance['timelines']

        # sanity
        for timeline in timelines:
            self.assertEqual(
                timeline['min-segno'],
                '000000010000000000000002')
            self.assertEqual(timeline['status'], 'OK')

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_archive_options(self):
        """
        check that '--archive-host', '--archive-user', '--archiver-port'
        and '--restore-command' are working as expected.
        """
        if not self.remote:
            self.skipTest("You must enable PGPROBACKUP_SSH_REMOTE"
                          " for run this test")
        backup_dir = self.backup_dir
        if not backup_dir.is_file_based:
            self.skipTest("Test has no meaning for cloud storage")

        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node, compress=True)

        node.slow_start()

        # FULL
        self.pb.backup_node('node', node)
        node.pgbench_init(scale=1)

        node.cleanup()

        wal_dir = os.path.join(backup_dir, 'wal', 'node')
        self.pb.restore_node('node', node,
            options=[
                '--restore-command="cp {0}/%f %p"'.format(wal_dir),
                '--archive-host=localhost',
                '--archive-port=22',
                '--archive-user={0}'.format(self.username)
                ])

        if self.pg_config_version >= self.version_to_num('12.0'):
            recovery_conf = os.path.join(node.data_dir, 'postgresql.auto.conf')
        else:
            recovery_conf = os.path.join(node.data_dir, 'recovery.conf')

        with open(recovery_conf, 'r') as f:
            recovery_content = f.read()

        self.assertIn(
            'restore_command = \'"cp {0}/%f %p"\''.format(wal_dir),
            recovery_content)

        node.cleanup()

        self.pb.restore_node('node', node,
            options=[
                '--archive-host=localhost',
                '--archive-port=22',
                '--archive-user={0}'.format(self.username)])

        with open(recovery_conf, 'r') as f:
            recovery_content = f.read()

        self.assertIn(
            "restore_command = '\"{0}\" archive-get -B \"{1}\" --instance \"{2}\" "
            "--wal-file-path=%p --wal-file-name=%f --remote-host=localhost "
            "--remote-port=22 --remote-user={3}'".format(
                self.probackup_path, backup_dir, 'node', self.username),
            recovery_content)

        node.slow_start()

        node.safe_psql(
            'postgres',
            'select 1')

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_archive_options_1(self):
        """
        check that '--archive-host', '--archive-user', '--archiver-port'
        and '--restore-command' are working as expected with set-config
        """
        backup_dir = self.backup_dir
        if not backup_dir.is_file_based:
            self.skipTest("Test has no meaning for cloud storage")

        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node, compress=True)

        node.slow_start()

        # FULL
        self.pb.backup_node('node', node)
        node.pgbench_init(scale=1)

        node.cleanup()

        wal_dir = os.path.join(backup_dir, 'wal', 'node')
        self.pb.set_config('node',
            options=[
                '--restore-command="cp {0}/%f %p"'.format(wal_dir),
                '--archive-host=localhost',
                '--archive-port=22',
                '--archive-user={0}'.format(self.username)])
        self.pb.restore_node('node', node=node)

        if self.pg_config_version >= self.version_to_num('12.0'):
            recovery_conf = os.path.join(node.data_dir, 'postgresql.auto.conf')
        else:
            recovery_conf = os.path.join(node.data_dir, 'recovery.conf')

        with open(recovery_conf, 'r') as f:
            recovery_content = f.read()

        self.assertIn(
            'restore_command = \'"cp {0}/%f %p"\''.format(wal_dir),
            recovery_content)

        node.cleanup()

        self.pb.restore_node('node', node,
            options=[
                '--restore-command=none',
                '--archive-host=localhost1',
                '--archive-port=23',
                '--archive-user={0}'.format(self.username)
                ])

        with open(recovery_conf, 'r') as f:
            recovery_content = f.read()

        self.assertIn(
            "restore_command = '\"{0}\" archive-get -B \"{1}\" --instance \"{2}\" "
            "--wal-file-path=%p --wal-file-name=%f --remote-host=localhost1 "
            "--remote-port=23 --remote-user={3}'".format(
                self.probackup_path, backup_dir, 'node', self.username),
            recovery_content)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_undefined_wal_file_path(self):
        """
        check that archive-push works correct with undefined
        --wal-file-path
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        archive_command = " ".join([f'"{self.probackup_path}"', 'archive-push',
                                    *self.backup_dir.pb_args, '--instance=node',
                                    '--wal-file-name=%f'])
        if os.name == 'posix':
            # Dash produces a core dump when it gets a SIGQUIT from its
            # child process so replace the shell with pg_probackup
            archive_command = 'exec ' + archive_command
        elif os.name == "nt":
            archive_command = archive_command.replace("\\","\\\\")

        self.pb.set_archiving('node', node, custom_archive_command=archive_command)

        node.slow_start()
        node.safe_psql(
            "postgres",
            "create table t_heap as select i"
            " as id from generate_series(0, 10) i")
        self.switch_wal_segment(node)
        tailer = tail_file(os.path.join(node.logs_dir, "postgresql.log"))
        tailer.wait_archive_push_completed()
        node.stop()

        log = tail_file(os.path.join(node.logs_dir, 'postgresql.log'), collect=True)
        log.wait(contains='archive-push completed successfully')

        # check
        self.assertEqual(self.pb.show_archive(instance='node', tli=1)['min-segno'], '000000010000000000000001')

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_intermediate_archiving(self):
        """
        check that archive-push works correct with --wal-file-path setting by user
        """
        node = self.pg_node.make_simple('node')

        node_pg_options = {}
        if node.major_version >= 13:
            node_pg_options['wal_keep_size'] = '0MB'
        else:
            node_pg_options['wal_keep_segments'] = '0'
        node.set_auto_conf(node_pg_options)

        self.pb.init()
        self.pb.add_instance('node', node)

        wal_dir = os.path.join(self.test_path, 'intermediate_dir')
        shutil.rmtree(wal_dir, ignore_errors=True)
        os.makedirs(wal_dir)
        if os.name == 'posix':
            self.pb.set_archiving('node', node, custom_archive_command='cp -v %p {0}/%f'.format(wal_dir))
        elif os.name == 'nt':
            self.pb.set_archiving('node', node, custom_archive_command='copy /Y "%p" "{0}\\\\%f"'.format(wal_dir.replace("\\","\\\\")))
        else:
            self.assertTrue(False, 'Unexpected os family')

        node.slow_start()
        node.safe_psql(
            "postgres",
            "create table t_heap as select i"
            " as id from generate_series(0, 10) i")
        self.switch_wal_segment(node)

        wal_segment = '000000010000000000000001'

        self.pb.archive_push('node', node, wal_file_path="{0}/{1}".format(wal_dir, wal_segment),
                             wal_file_name=wal_segment)

        self.assertEqual(self.pb.show_archive(instance='node', tli=1)['min-segno'], wal_segment)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_waldir_outside_pgdata_archiving(self):
        """
        check that archive-push works correct with symlinked waldir
        """
        backup_dir = self.backup_dir
        external_wal_dir = os.path.join(self.test_path, 'ext_wal_dir')
        shutil.rmtree(external_wal_dir, ignore_errors=True)

        node = self.pg_node.make_simple('node', initdb_params=['--waldir={0}'.format(external_wal_dir)])

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)

        node.slow_start()
        node.safe_psql(
            "postgres",
            "create table t_heap as select i"
            " as id from generate_series(0, 10) i")
        self.switch_wal_segment(node)

        tailer = tail_file(os.path.join(node.logs_dir, 'postgresql.log'))
        tailer.wait_archive_push_completed()
        node.stop()

        # check
        self.assertEqual(self.pb.show_archive(instance='node', tli=1)['min-segno'], '000000010000000000000001')

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_hexadecimal_timeline(self):
        """
        Check that timelines are correct.
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node)
        node.pgbench_init(scale=2)

        # create timelines
        for i in range(1, 13):
            # print(i)
            node.cleanup()
            self.pb.restore_node('node', node,
                options=['--recovery-target-timeline={0}'.format(i)])
            node.slow_start()
            node.pgbench_init(scale=2)

        sleep(5)

        show = self.pb.show_archive()

        timelines = show[0]['timelines']

        print(timelines[0])

        tli13 = timelines[0]

        self.assertEqual(
            13,
            tli13['tli'])

        self.assertEqual(
            12,
            tli13['parent-tli'])

        self.assertEqual(
            backup_id,
            tli13['closest-backup-id'])

        self.assertEqual(
            '0000000D000000000000001C',
            tli13['max-segno'])

    @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_archiving_and_slots(self):
        """
        Check that archiving don`t break slot
        guarantee.
        """
        backup_dir = self.backup_dir
        if not backup_dir.is_file_based:
            self.skipTest("Test has no meaning for cloud storage")

        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '30s',
                'max_wal_size': '64MB'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        pg_receivexlog_path = self.get_bin_path('pg_receivewal')

        # "pg_receivewal --create-slot --slot archive_slot --if-not-exists "
        # "&& pg_receivewal --synchronous -Z 1 /tmp/wal --slot archive_slot --no-loop"

        self.pb.run_binary(
            [
                pg_receivexlog_path, '-p', str(node.port), '--synchronous',
                '--create-slot', '--slot', 'archive_slot', '--if-not-exists'
            ])

        node.pgbench_init(scale=10)

        pg_receivexlog = self.pb.run_binary(
            [
                pg_receivexlog_path, '-p', str(node.port), '--synchronous',
                '-D', os.path.join(backup_dir, 'wal', 'node'),
                '--no-loop', '--slot', 'archive_slot',
                '-Z', '1'
            ], asynchronous=True)

        if pg_receivexlog.returncode:
            self.assertFalse(
                True,
                'Failed to start pg_receivexlog: {0}'.format(
                    pg_receivexlog.communicate()[1]))

        sleep(2)

        pg_receivexlog.kill()

        backup_id = self.pb.backup_node('node', node)
        node.pgbench_init(scale=20)

        exit(1)

    def test_archive_push_sanity(self):
        """"""
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'archive_mode': 'on',
                'archive_command': 'exit 1'})

        self.pb.init()
        self.pb.add_instance('node', node)

        node.slow_start()

        node.pgbench_init(scale=50)
        node.stop()

        self.pb.set_archiving('node', node)
        os.remove(os.path.join(node.logs_dir, 'postgresql.log'))
        node.slow_start()

        self.pb.backup_node('node', node)

        with open(os.path.join(node.logs_dir, 'postgresql.log'), 'r') as f:
            postgres_log_content = cleanup_ptrack(f.read())

        # print(postgres_log_content)
        # make sure that .backup file is not compressed
        if self.archive_compress:
            self.assertNotIn('.backup'+self.compress_suffix, postgres_log_content)
        self.assertNotIn('WARNING', postgres_log_content)

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.restore_node('node', replica, options=['-R'])

        # self.pb.set_archiving('replica', replica, replica=True)
        replica.set_auto_conf({'port': replica.port})
        replica.set_auto_conf({'archive_mode': 'always'})
        replica.set_auto_conf({'hot_standby': 'on'})
        replica.slow_start(replica=True)

        self.wait_until_replica_catch_with_master(node, replica)

        node.pgbench_init(scale=5)

        replica.promote()
        replica.pgbench_init(scale=10)

        log = tail_file(os.path.join(replica.logs_dir, 'postgresql.log'),
                        collect=True, linetimeout=30)
        log.wait(regex=r"pushing file.*history")
        log.wait_archive_push_completed()
        log.wait(regex=r"pushing file.*partial")
        log.wait_archive_push_completed()

        if self.archive_compress:
            # make sure that .partial file is not compressed
            self.assertNotIn('.partial'+self.compress_suffix, log.content)
            # make sure that .history file is not compressed
            self.assertNotIn('.history'+self.compress_suffix, log.content)

        replica.stop()
        log.wait_shutdown()

        self.assertNotIn('WARNING', cleanup_ptrack(log.content))

        output = self.pb.show_archive(
            'node', as_json=False, as_text=True,
            options=['--log-level-console=INFO'])

        self.assertNotIn('WARNING', output)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_archive_pg_receivexlog_partial_handling(self):
        """check that archive-get delivers .partial and .gz.partial files"""
        backup_dir = self.backup_dir
        if not backup_dir.is_file_based:
            self.skipTest("Test has no meaning for cloud storage")

        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)

        node.slow_start()

        app_name = 'pg_receivewal'
        pg_receivexlog_path = self.get_bin_path('pg_receivewal')

        cmdline = [
            pg_receivexlog_path, '-p', str(node.port), '--synchronous',
            '-D', os.path.join(backup_dir, 'wal', 'node')]

        if self.archive_compress and node.major_version >= 10:
            cmdline += ['-Z', '1']

        env = self.test_env
        env["PGAPPNAME"] = app_name
        pg_receivexlog = self.pb.run_binary(cmdline, asynchronous=True, env=env)

        if pg_receivexlog.returncode:
            self.assertFalse(
                True,
                'Failed to start pg_receivexlog: {0}'.format(
                    pg_receivexlog.communicate()[1]))

        node.set_auto_conf({'synchronous_standby_names': app_name})
        node.set_auto_conf({'synchronous_commit': 'on'})
        node.reload()

        # FULL
        self.pb.backup_node('node', node, options=['--stream'])

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1000000) i")

        # PAGE
        self.pb.backup_node('node', node, backup_type='page', options=['--stream'])

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(1000000,2000000) i")

        pg_receivexlog.kill()

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored,
            options=['--recovery-target=latest', '--recovery-target-action=promote'])
        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.set_auto_conf({'hot_standby': 'off'})

        node_restored.slow_start()

        result = node.table_checksum("t_heap")
        result_new = node_restored.table_checksum("t_heap")

        self.assertEqual(result, result_new)

    @unittest.skip("skip")
    def test_multi_timeline_recovery_prefetching(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)

        node.slow_start()

        self.pb.backup_node('node', node)

        node.pgbench_init(scale=50)

        target_xid = node.safe_psql(
            'postgres',
            'select txid_current()').rstrip()

        node.pgbench_init(scale=20)

        node.stop()
        node.cleanup()

        self.pb.restore_node('node', node,
            options=[
                '--recovery-target-xid={0}'.format(target_xid),
                '--recovery-target-action=promote'])

        node.slow_start()

        node.pgbench_init(scale=20)

        target_xid = node.safe_psql(
            'postgres',
            'select txid_current()').rstrip()

        node.stop(['-m', 'immediate', '-D', node.data_dir])
        node.cleanup()

        self.pb.restore_node('node', node,
            options=[
#                '--recovery-target-xid={0}'.format(target_xid),
                '--recovery-target-timeline=2',
#                '--recovery-target-action=promote',
                '--no-validate'])
        node.slow_start()

        node.pgbench_init(scale=20)
        result = node.table_checksum("pgbench_accounts")
        node.stop()
        node.cleanup()

        self.pb.restore_node('node', node,
            options=[
#                '--recovery-target-xid=100500',
                '--recovery-target-timeline=3',
#                '--recovery-target-action=promote',
                '--no-validate'])
        os.remove(os.path.join(node.logs_dir, 'postgresql.log'))

        restore_command = self.get_restore_command(backup_dir, 'node')
        restore_command += ' -j 2 --batch-size=10 --log-level-console=VERBOSE'

        if node.major_version >= 12:
            node.append_conf(
                'postgresql.auto.conf', "restore_command = '{0}'".format(restore_command))
        else:
            node.append_conf(
                'recovery.conf', "restore_command = '{0}'".format(restore_command))

        node.slow_start()

        result_new = node.table_checksum("pgbench_accounts")

        self.assertEqual(result, result_new)

        with open(os.path.join(node.logs_dir, 'postgresql.log'), 'r') as f:
            postgres_log_content = f.read()

        # check that requesting of non-existing segment do not
        # throwns aways prefetch
        self.assertIn(
            'pg_probackup archive-get failed to '
            'deliver WAL file: 000000030000000000000006',
            postgres_log_content)

        self.assertIn(
            'pg_probackup archive-get failed to '
            'deliver WAL file: 000000020000000000000006',
            postgres_log_content)

        self.assertIn(
            'pg_probackup archive-get used prefetched '
            'WAL segment 000000010000000000000006, prefetch state: 5/10',
            postgres_log_content)

    def test_archive_get_batching_sanity(self):
        """
        Make sure that batching works.
        .gz file is corrupted and uncompressed is not, check that both
            corruption detected and uncompressed file is used.
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)

        node.slow_start()

        self.pb.backup_node('node', node, options=['--stream'])

        node.pgbench_init(scale=50)

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.restore_node('node', node=replica)
        self.set_replica(node, replica, log_shipping=True)

        if node.major_version >= 12:
            replica.set_auto_conf({'restore_command': 'exit 1'})
        else:
            replica.append_conf('recovery.conf', "restore_command = 'exit 1'")

        replica.slow_start(replica=True)

        # at this point replica is consistent
        restore_command = self.get_restore_command(backup_dir, 'node')

        restore_command += ' -j 2 --batch-size=10'

        # print(restore_command)

        if node.major_version >= 12:
            replica.set_auto_conf({'restore_command': restore_command})
        else:
            replica.append_conf(
                'recovery.conf', "restore_command = '{0}'".format(restore_command))

        replica.restart()

        sleep(5)

        with open(os.path.join(replica.logs_dir, 'postgresql.log'), 'r') as f:
            postgres_log_content = f.read()

        self.assertIn(
            'pg_probackup archive-get completed successfully, fetched: 10/10',
            postgres_log_content)
        self.assertIn('used prefetched WAL segment', postgres_log_content)
        self.assertIn('prefetch state: 9/10', postgres_log_content)
        self.assertIn('prefetch state: 8/10', postgres_log_content)

    def test_archive_get_prefetch_corruption(self):
        """
        Make sure that WAL corruption is detected.
        And --prefetch-dir is honored.
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)
        node.set_auto_conf({
            'wal_compression': 'off',
        })

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)

        node.slow_start()

        self.pb.backup_node('node', node, options=['--stream'])

        node.pgbench_init(scale=20)

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.restore_node('node', node=replica)
        self.set_replica(node, replica, log_shipping=True)

        if node.major_version >= 12:
            replica.set_auto_conf({'restore_command': 'exit 1'})
        else:
            replica.append_conf('recovery.conf', "restore_command = 'exit 1'")

        log = tail_file(os.path.join(node.logs_dir, 'postgresql.log'),
                        linetimeout=30)
        log.wait(regex=r"pushing file.*000000D")
        log.wait_archive_push_completed()

        replica.slow_start(replica=True)

        # at this point replica is consistent
        restore_command = self.get_restore_command(backup_dir, 'node')

        restore_command += ' -j5 --batch-size=10 --log-level-console=VERBOSE'
        #restore_command += ' --batch-size=2 --log-level-console=VERBOSE'

        if node.major_version >= 12:
            replica.set_auto_conf({'restore_command': restore_command})
        else:
            replica.append_conf(
                'recovery.conf', "restore_command = '{0}'".format(restore_command))

        replica.restart()

        sleep(5)

        with open(os.path.join(replica.logs_dir, 'postgresql.log'), 'r') as f:
            postgres_log_content = f.read()

        self.assertIn(
            'pg_probackup archive-get completed successfully, fetched: 10/10',
            postgres_log_content)
        self.assertIn('used prefetched WAL segment', postgres_log_content)
        self.assertIn('prefetch state: 9/10', postgres_log_content)
        self.assertIn('prefetch state: 8/10', postgres_log_content)

        replica.stop()

        # generate WAL, copy it into prefetch directory, then corrupt
        # some segment
        node.pgbench_init(scale=5)
        sleep(20)

        # now copy WAL files into prefetch directory and corrupt some of them
        files = self.get_instance_wal_list(backup_dir, 'node')

        suffix = self.compress_suffix
        for filename in [files[-4], files[-3], files[-2], files[-1]]:
            content = self.read_instance_wal(backup_dir, 'node', filename,
                                             decompress=True)
    
            if node.major_version >= 10:
                wal_dir = 'pg_wal'
            else:
                wal_dir = 'pg_xlog'
    
            if suffix and filename.endswith(suffix):
                filename = filename[:-len(suffix)]
            dst_file = os.path.join(replica.data_dir, wal_dir, 'pbk_prefetch', filename)
            with open(dst_file, 'wb') as f_out:
                f_out.write(content)

        # corrupt file
        if suffix and files[-2].endswith(suffix):
            filename = files[-2][:-len(suffix)]
        else:
            filename = files[-2]

        prefetched_file = os.path.join(replica.data_dir, wal_dir, 'pbk_prefetch', filename)

        with open(prefetched_file, "rb+", 0) as f:
            f.seek(8192*2)
            f.write(b"SURIKEN")
            f.flush()

        # enable restore_command
        restore_command = self.get_restore_command(backup_dir, 'node')
        restore_command += ' --batch-size=2 --log-level-console=VERBOSE'

        if node.major_version >= 12:
            replica.set_auto_conf({'restore_command': restore_command})
        else:
            replica.append_conf(
                'recovery.conf', "restore_command = '{0}'".format(restore_command))

        os.remove(os.path.join(replica.logs_dir, 'postgresql.log'))
        replica.slow_start(replica=True)

        prefetch_line = 'Prefetched WAL segment {0} is invalid, cannot use it'.format(filename)
        restored_line = 'LOG:  restored log file "{0}" from archive'.format(filename)

        self.wait_server_wal_exists(replica.data_dir, wal_dir, filename)

        tailer = tail_file(os.path.join(replica.logs_dir, 'postgresql.log'))
        tailer.wait(contains=prefetch_line)
        tailer.wait(contains=restored_line)

    # @unittest.skip("skip")
    def test_archive_show_partial_files_handling(self):
        """
        check that files with '.part', '.part.gz', '.partial' and '.partial.gz'
        siffixes are handled correctly
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node, compress=False)

        node.slow_start()

        self.pb.backup_node('node', node)

        # .part file
        if backup_dir.is_file_based:
            wals_dir = os.path.join(backup_dir, 'wal', 'node')

            node.safe_psql(
                "postgres",
                "create table t1()")

            filename = node.safe_psql(
                "postgres",
                "SELECT file_name "
                "FROM pg_walfile_name_offset(pg_current_wal_flush_lsn())").rstrip()

            filename = filename.decode('utf-8')

            self.switch_wal_segment(node)

            self.wait_instance_wal_exists(backup_dir, 'node', filename)
            os.rename(
                os.path.join(wals_dir, filename),
                os.path.join(wals_dir, '{0}~tmp123451'.format(filename)))

            # .gz.part file
            node.safe_psql(
                "postgres",
                "create table t2()")

            filename = node.safe_psql(
                "postgres",
                "SELECT file_name "
                "FROM pg_walfile_name_offset(pg_current_wal_flush_lsn())").rstrip()

            filename = filename.decode('utf-8')

            self.switch_wal_segment(node)

            self.wait_instance_wal_exists(backup_dir, 'node', filename)
            os.rename(
                os.path.join(wals_dir, filename),
                os.path.join(wals_dir, f'{filename}{self.compress_suffix}~tmp234513'))

        # .partial file
        node.safe_psql(
            "postgres",
            "create table t3()")

        filename = node.safe_psql(
            "postgres",
            "SELECT file_name "
            "FROM pg_walfile_name_offset(pg_current_wal_flush_lsn())").rstrip()

        filename = filename.decode('utf-8')

        self.switch_wal_segment(node)

        self.wait_instance_wal_exists(backup_dir, 'node', filename)
        file_content = self.read_instance_wal(backup_dir, 'node', filename)
        self.write_instance_wal(backup_dir, 'node', f'{filename}.partial',
                                file_content)
        self.remove_instance_wal(backup_dir, 'node', filename)

        # .gz.partial file
        node.safe_psql(
            "postgres",
            "create table t4()")

        filename = node.safe_psql(
            "postgres",
            "SELECT file_name "
            "FROM pg_walfile_name_offset(pg_current_wal_flush_lsn())").rstrip()

        filename = filename.decode('utf-8')

        self.switch_wal_segment(node)

        self.wait_instance_wal_exists(backup_dir, 'node', filename)
        file_content = self.read_instance_wal(backup_dir, 'node', filename)
        self.write_instance_wal(backup_dir, 'node', f'{filename}{self.compress_suffix}.partial',
                                file_content)
        self.remove_instance_wal(backup_dir, 'node', filename)

        self.pb.show_archive('node', options=['--log-level-file=VERBOSE'])

        log_content = self.read_pb_log()

        self.assertNotIn(
            'WARNING',
            log_content)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_archive_empty_history_file(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/326
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)

        node.slow_start()
        node.pgbench_init(scale=5)

        # FULL
        self.pb.backup_node('node', node)

        node.pgbench_init(scale=5)
        node.cleanup()

        self.pb.restore_node('node', node,
            options=[
                    '--recovery-target=latest',
                    '--recovery-target-action=promote'])

        # Node in timeline 2
        node.slow_start()

        node.pgbench_init(scale=5)
        node.cleanup()

        self.pb.restore_node('node', node,
            options=[
                    '--recovery-target=latest',
                    '--recovery-target-timeline=2',
                    '--recovery-target-action=promote'])

        # Node in timeline 3
        node.slow_start()

        node.pgbench_init(scale=5)
        node.cleanup()

        self.pb.restore_node('node', node,
            options=[
                    '--recovery-target=latest',
                    '--recovery-target-timeline=3',
                    '--recovery-target-action=promote'])

        # Node in timeline 4
        node.slow_start()
        node.pgbench_init(scale=5)

        # Truncate history files
        for tli in range(2, 5):
            self.write_instance_wal(backup_dir, 'node', f'0000000{tli}.history',
                                  b'')

        timelines = self.pb.show_archive('node', options=['--log-level-file=INFO'])

        # check that all timelines has zero switchpoint
        for timeline in timelines:
            self.assertEqual(timeline['switchpoint'], '0/0')

        log_content = self.read_pb_log()

        self.assertRegex(
            log_content,
            'WARNING: History file is corrupted or missing: "[^"]*00000002.history"')
        self.assertRegex(
            log_content,
            'WARNING: History file is corrupted or missing: "[^"]*00000003.history"')
        self.assertRegex(
            log_content,
            'WARNING: History file is corrupted or missing: "[^"]*00000004.history"')

    def test_archive_get_relative_path(self):
        """
        Take a backup in archive mode, restore it and run the cluster
        on it with relative pgdata path, archive-get should be ok with
        relative pgdata path as well.
        """

        # initialize basic node
        node = self.pg_node.make_simple(
            base_dir='node',
            pg_options={
                'archive_timeout': '10s'}
        )

        # initialize the node to restore to
        restored = self.pg_node.make_empty(base_dir='restored')

        # initialize pg_probackup setup including archiving
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)

        # the job
        node.slow_start()
        self.pb.backup_node('node', node)
        node.stop()
        self.pb.restore_node('node', restored)
        restored.set_auto_conf({"port": restored.port})

        run_path = os.getcwd()
        relative_pgdata = get_relative_path(run_path, restored.data_dir)

        restored.start(params=["-D", relative_pgdata])

        # cleanup
        restored.stop()

    def test_archive_push_alot_of_files(self):
        """
        Test archive-push pushes files in-order.
        PBCKP-911
        """
        if self.pg_config_version < 130000:
            self.skipTest("too costly to test with 16MB wal segment")

        node = self.pg_node.make_simple(base_dir='node',
                                        initdb_params=['--wal-segsize','1'],
                                        pg_options={
                                            'archive_mode': 'on',
                                        })

        self.pb.init()
        self.pb.add_instance('node', node)

        pg_wal_dir = os.path.join(node.data_dir, 'pg_wal')

        node.slow_start()
        # create many segments
        for i in range(30):
            node.execute("select pg_logical_emit_message(False, 'z', repeat('0', 1024*1024))")
        # EXT4 always stores in hash table, so test could skip following two
        # loops if it runs on EXT4.
        #
        # But for XFS we have to disturb file order manually.
        # 30-30-30 is empirically obtained: pg_wal/archive_status doesn't overflow
        # to B+Tree yet, but already reuses some of removed items
        for i in range(1,30):
            fl = f'{1:08x}{0:08x}{i:08X}'
            if os.path.exists(os.path.join(pg_wal_dir, fl)):
                os.remove(os.path.join(pg_wal_dir, fl))
                os.remove(os.path.join(pg_wal_dir, f'archive_status/{fl}.ready'))
        for i in range(30):
            node.execute("select pg_logical_emit_message(False, 'z', repeat('0', 1024*1024))")

        node.stop()

        files = os.listdir(pg_wal_dir)
        files.sort()
        n = int(len(files)/2)

        self.pb.archive_push("node", node, wal_file_name=files[0], wal_file_path=pg_wal_dir,
                             options=['--threads', '10',
                                      '--batch-size', str(n),
                                      '--log-level-file', 'VERBOSE'])

        archived = self.get_instance_wal_list(self.backup_dir, 'node')

        self.assertListEqual(files[:n], archived)

#################################################################
#                           dry-run
#################################################################

    @unittest.skipUnless(fs_backup_class.is_file_based, "AccessPath check is always true on s3")
    def test_dry_run_archive_push(self):
        """ Check archive-push command with dry_run option"""
        node = self.pg_node.make_simple('node',
                                        set_replication=True)
        self.pb.init()
        self.pb.add_instance('node', node)

        node.slow_start()
        node.pgbench_init(scale=10)

        walfile = node.safe_psql(
            'postgres',
            'select pg_walfile_name(pg_current_wal_lsn())').decode('utf-8').rstrip()
        self.pb.archive_push('node', node=node, wal_file_name=walfile, options=['--dry-run'])

        self.assertTrue(len(self.backup_dir.list_dirs((os.path.join(self.backup_dir, 'wal/node')))) == 0)
        # Access check suit if disk mounted as read_only
        if fs_backup_class.is_file_based:  #AccessPath check is always true on s3
            dir_path = os.path.join(self.backup_dir, 'wal/node')
            dir_mode = os.stat(dir_path).st_mode
            os.chmod(dir_path, 0o400)
            print(self.backup_dir)

            error_message = self.pb.archive_push('node', node=node, wal_file_name=walfile, options=['--dry-run'],
                                                                                    expect_error="because of changed permissions")
            try:
                self.assertMessage(error_message, contains='ERROR: Check permissions')
            finally:
                # Cleanup
                os.chmod(dir_path, dir_mode)

        node.stop()

    @unittest.skipUnless(fs_backup_class.is_file_based, "AccessPath check is always true on s3")
    def test_archive_get_dry_run(self):
        """
        Check archive-get command with dry-ryn option
        """
        # initialize basic node
        node = self.pg_node.make_simple(
            base_dir='node',
            pg_options={
                'archive_timeout': '3s'}
        )

        # initialize the node to restore to
        restored = self.pg_node.make_empty(base_dir='restored')

        # initialize pg_probackup setup including archiving
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)

        # the job
        node.slow_start()
        node.pgbench_init(scale=10)

        self.pb.backup_node('node', node)
        self.pb.restore_node('node', restored, options=['--recovery-target=latest'])
        restored.set_auto_conf({"port": restored.port})

        files = self.get_instance_wal_list(self.backup_dir, 'node')
        cwd = os.getcwd()
        os.chdir(restored.data_dir)
        wal_dir = self.pgdata_content(os.path.join(restored.data_dir, 'pg_wal'))
        self.pb.archive_get('node', wal_file_name=files[-1], wal_file_path="{0}/{1}".format('pg_wal', files[-1]),
                            options=['--dry-run', "-D", restored.data_dir])
        restored_wal = self.pgdata_content(os.path.join(restored.data_dir, 'pg_wal'))
        self.compare_pgdata(wal_dir, restored_wal)
        os.chdir(cwd)
        node.stop()

def cleanup_ptrack(log_content):
    # PBCKP-423 - need to clean ptrack warning
    ptrack_is_not = 'Ptrack 1.X is not supported anymore'
    if ptrack_is_not in log_content:
        lines = [line for line in log_content.splitlines()
                 if ptrack_is_not not in line]
        log_content = "".join(lines)
    return log_content


# TODO test with multiple not archived segments.
# TODO corrupted file in archive.

# important - switchpoint may be NullOffset LSN and not actually existing in archive to boot.
# so write WAL validation code accordingly

# change wal-seg-size
#
#
#t3          ----------------
#           /
#t2      ----------------
#       /
#t1 -A--------
#
#


#t3             ----------------
#               /
#t2      ----------------
#       /
#t1 -A--------
#
