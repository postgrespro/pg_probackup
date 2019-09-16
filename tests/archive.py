import os
import shutil
import gzip
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException, GdbException
from datetime import datetime, timedelta
import subprocess
from sys import exit
from time import sleep
from distutils.dir_util import copy_tree


module_name = 'archive'


class ArchiveTest(ProbackupTest, unittest.TestCase):

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_pgpro434_1(self):
        """Description in jira issue PGPRO-434"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector from "
            "generate_series(0,100) i")

        result = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.backup_node(
            backup_dir, 'node', node)
        node.cleanup()

        self.restore_node(
            backup_dir, 'node', node)
        node.slow_start()

        # Recreate backup catalog
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)

        # Make backup
        self.backup_node(
            backup_dir, 'node', node)
        node.cleanup()

        # Restore Database
        self.restore_node(
            backup_dir, 'node', node,
            options=["--recovery-target-action=promote"])
        node.slow_start()

        self.assertEqual(
            result, node.safe_psql("postgres", "SELECT * FROM t_heap"),
            'data after restore not equal to original data')
        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_pgpro434_2(self):
        """
        Check that timelines are correct.
        WAITING PGPRO-1053 for --immediate
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s'}
            )
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # FIRST TIMELINE
        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,100) i")
        backup_id = self.backup_node(backup_dir, 'node', node)
        node.safe_psql(
            "postgres",
            "insert into t_heap select 100501 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1) i")

        # SECOND TIMELIN
        node.cleanup()
        self.restore_node(
            backup_dir, 'node', node,
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

        backup_id = self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "insert into t_heap select 100502 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,256) i")

        # THIRD TIMELINE
        node.cleanup()
        self.restore_node(
            backup_dir, 'node', node,
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

        backup_id = self.backup_node(backup_dir, 'node', node)

        result = node.safe_psql("postgres", "SELECT * FROM t_heap")
        node.safe_psql(
            "postgres",
            "insert into t_heap select 100503 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,256) i")

        # FOURTH TIMELINE
        node.cleanup()
        self.restore_node(
            backup_dir, 'node', node,
            options=['--immediate', '--recovery-target-action=promote'])
        node.slow_start()

        if self.verbose:
            print('Fourth timeline')
            print(node.safe_psql(
                "postgres",
                "select redo_wal_file from pg_control_checkpoint()"))

        # FIFTH TIMELINE
        node.cleanup()
        self.restore_node(
            backup_dir, 'node', node,
            options=['--immediate', '--recovery-target-action=promote'])
        node.slow_start()

        if self.verbose:
            print('Fifth timeline')
            print(node.safe_psql(
                "postgres",
                "select redo_wal_file from pg_control_checkpoint()"))

        # SIXTH TIMELINE
        node.cleanup()
        self.restore_node(
            backup_dir, 'node', node,
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

        self.assertEqual(
            result,
            node.safe_psql(
                "postgres",
                "SELECT * FROM t_heap"),
            'data after restore not equal to original data')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_pgpro434_3(self):
        """
        Check pg_stop_backup_timeout, needed backup_timeout
        Fixed in commit d84d79668b0c139 and assert fixed by ptrack 1.7
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

        gdb = self.backup_node(
                backup_dir, 'node', node,
                options=[
                    "--archive-timeout=60",
                    "--log-level-file=info"],
                gdb=True)

        gdb.set_breakpoint('pg_stop_backup')
        gdb.run_until_break()

        node.append_conf(
            'postgresql.auto.conf', "archive_command = 'exit 1'")
        node.reload()

        gdb.continue_execution_until_exit()

        log_file = os.path.join(backup_dir, 'log', 'pg_probackup.log')
        with open(log_file, 'r') as f:
            log_content = f.read()

        # in PG =< 9.6 pg_stop_backup always wait
        if self.get_version(node) < 100000:
            self.assertIn(
                "ERROR: pg_stop_backup doesn't answer in 60 seconds, cancel it",
                log_content)
        else:
            self.assertIn(
                "ERROR: Switched WAL segment 000000010000000000000002 "
                "could not be archived in 60 seconds",
                log_content)

        log_file = os.path.join(node.logs_dir, 'postgresql.log')
        with open(log_file, 'r') as f:
            log_content = f.read()

        self.assertNotIn(
            'FailedAssertion',
            log_content,
            'PostgreSQL crashed because of a failed assert')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_pgpro434_4(self):
        """
        Check pg_stop_backup_timeout, needed backup_timeout
        Fixed in commit d84d79668b0c139 and assert fixed by ptrack 1.7
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

        gdb = self.backup_node(
                backup_dir, 'node', node,
                options=[
                    "--archive-timeout=60",
                    "--log-level-file=info"],
                gdb=True)

        gdb.set_breakpoint('pg_stop_backup')
        gdb.run_until_break()

        node.append_conf(
            'postgresql.auto.conf', "archive_command = 'exit 1'")
        node.reload()

        os.environ["PGAPPNAME"] = "foo"

        pid = node.safe_psql(
            "postgres",
            "SELECT pid "
            "FROM pg_stat_activity "
            "WHERE application_name = 'pg_probackup'").rstrip()

        os.environ["PGAPPNAME"] = "pg_probackup"

        postgres_gdb = self.gdb_attach(pid)
        postgres_gdb.set_breakpoint('do_pg_stop_backup')
        postgres_gdb.continue_execution_until_running()

        gdb.continue_execution_until_exit()
        # gdb._execute('detach')

        log_file = os.path.join(backup_dir, 'log', 'pg_probackup.log')
        with open(log_file, 'r') as f:
            log_content = f.read()

        self.assertIn(
            "ERROR: pg_stop_backup doesn't answer in 60 seconds, cancel it",
            log_content)

        log_file = os.path.join(node.logs_dir, 'postgresql.log')
        with open(log_file, 'r') as f:
            log_content = f.read()

        self.assertNotIn(
            'FailedAssertion',
            log_content,
            'PostgreSQL crashed because of a failed assert')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_archive_push_file_exists(self):
        """Archive-push if file exists"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)

        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        if self.archive_compress:
            filename = '000000010000000000000001.gz'
            file = os.path.join(wals_dir, filename)
        else:
            filename = '000000010000000000000001'
            file = os.path.join(wals_dir, filename)

        with open(file, 'a') as f:
            f.write(b"blablablaadssaaaaaaaaaaaaaaa")
            f.flush()
            f.close()

        node.slow_start()
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,100500) i")
        log_file = os.path.join(node.logs_dir, 'postgresql.log')

        self.switch_wal_segment(node)
        sleep(1)

        with open(log_file, 'r') as f:
            log_content = f.read()
        self.assertIn(
            'LOG:  archive command failed with exit code 1',
            log_content)

        self.assertIn(
            'DETAIL:  The failed archive command was:',
            log_content)

        self.assertIn(
            'INFO: pg_probackup archive-push from',
            log_content)

        self.assertIn(
            'ERROR: WAL segment ',
            log_content)

        self.assertIn(
            'already exists.',
            log_content)

        self.assertNotIn(
            'pg_probackup archive-push completed successfully', log_content)

        if self.get_version(node) < 100000:
            wal_src = os.path.join(
                node.data_dir, 'pg_xlog', '000000010000000000000001')
        else:
            wal_src = os.path.join(
                node.data_dir, 'pg_wal', '000000010000000000000001')

        if self.archive_compress:
            with open(wal_src, 'rb') as f_in, gzip.open(
                    file, 'wb', compresslevel=1) as f_out:
                shutil.copyfileobj(f_in, f_out)
        else:
            shutil.copyfile(wal_src, file)

        self.switch_wal_segment(node)
        sleep(5)

        with open(log_file, 'r') as f:
            log_content = f.read()

        self.assertIn(
            'pg_probackup archive-push completed successfully',
            log_content)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_archive_push_file_exists_overwrite(self):
        """Archive-push if file exists"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)

        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        if self.archive_compress:
            filename = '000000010000000000000001.gz'
            file = os.path.join(wals_dir, filename)
        else:
            filename = '000000010000000000000001'
            file = os.path.join(wals_dir, filename)

        with open(file, 'a') as f:
            f.write(b"blablablaadssaaaaaaaaaaaaaaa")
            f.flush()
            f.close()

        node.slow_start()
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,100500) i")
        log_file = os.path.join(node.logs_dir, 'postgresql.log')

        self.switch_wal_segment(node)
        sleep(1)

        with open(log_file, 'r') as f:
            log_content = f.read()

        self.assertIn(
            'LOG:  archive command failed with exit code 1', log_content)
        self.assertIn(
            'DETAIL:  The failed archive command was:', log_content)
        self.assertIn(
            'INFO: pg_probackup archive-push from', log_content)
        self.assertIn(
            '{0}" already exists.'.format(filename), log_content)

        self.assertNotIn(
            'pg_probackup archive-push completed successfully', log_content)

        self.set_archiving(backup_dir, 'node', node, overwrite=True)
        node.reload()
        self.switch_wal_segment(node)
        sleep(2)

        with open(log_file, 'r') as f:
            log_content = f.read()
            self.assertTrue(
                'pg_probackup archive-push completed successfully' in log_content,
                'Expecting messages about successfull execution archive_command')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_archive_push_partial_file_exists(self):
        """Archive-push if stale '.part' file exists"""
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

        # this backup is needed only for validation to xid
        self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "create table t1(a int)")

        xid = node.safe_psql(
            "postgres",
            "INSERT INTO t1 VALUES (1) RETURNING (xmin)").rstrip()

        if self.get_version(node) < 100000:
            filename_orig = node.safe_psql(
                "postgres",
                "SELECT file_name "
                "FROM pg_xlogfile_name_offset(pg_current_xlog_location());").rstrip()
        else:
            filename_orig = node.safe_psql(
                "postgres",
                "SELECT file_name "
                "FROM pg_walfile_name_offset(pg_current_wal_flush_lsn());").rstrip()

        # form up path to next .part WAL segment
        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        if self.archive_compress:
            filename = filename_orig + '.gz' + '.part'
            file = os.path.join(wals_dir, filename)
        else:
            filename = filename_orig + '.part'
            file = os.path.join(wals_dir, filename)

        # emulate stale .part file
        with open(file, 'a') as f:
            f.write(b"blahblah")
            f.flush()
            f.close()

        self.switch_wal_segment(node)
        sleep(70)

        # check that segment is archived
        if self.archive_compress:
            filename_orig = filename_orig + '.gz'

        file = os.path.join(wals_dir, filename_orig)
        self.assertTrue(os.path.isfile(file))

        # successful validate means that archive-push reused stale wal segment
        self.validate_pb(
            backup_dir, 'node',
            options=['--recovery-target-xid={0}'.format(xid)])

        log_file = os.path.join(node.logs_dir, 'postgresql.log')
        with open(log_file, 'r') as f:
            log_content = f.read()
            self.assertIn(
                'Reusing stale destination temporary WAL file',
                log_content)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_archive_push_partial_file_exists_not_stale(self):
        """Archive-push if .part file exists and it is not stale"""
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

        node.safe_psql(
            "postgres",
            "create table t1()")
        self.switch_wal_segment(node)

        node.safe_psql(
            "postgres",
            "create table t2()")

        if self.get_version(node) < 100000:
            filename_orig = node.safe_psql(
                "postgres",
                "SELECT file_name "
                "FROM pg_xlogfile_name_offset(pg_current_xlog_location());").rstrip()
        else:
            filename_orig = node.safe_psql(
                "postgres",
                "SELECT file_name "
                "FROM pg_walfile_name_offset(pg_current_wal_flush_lsn());").rstrip()

        # form up path to next .part WAL segment
        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        if self.archive_compress:
            filename = filename_orig + '.gz' + '.part'
            file = os.path.join(wals_dir, filename)
        else:
            filename = filename_orig + '.part'
            file = os.path.join(wals_dir, filename)

        with open(file, 'a') as f:
            f.write(b"blahblah")
            f.flush()
            f.close()

        self.switch_wal_segment(node)
        sleep(30)

        with open(file, 'a') as f:
            f.write(b"blahblahblahblah")
            f.flush()
            f.close()

        sleep(40)

        # check that segment is NOT archived
        if self.archive_compress:
            filename_orig = filename_orig + '.gz'

        file = os.path.join(wals_dir, filename_orig)

        self.assertFalse(os.path.isfile(file))

        # log_file = os.path.join(node.logs_dir, 'postgresql.log')
        # with open(log_file, 'r') as f:
        #     log_content = f.read()
        #     self.assertIn(
        #         'is not stale',
        #         log_content)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_replica_archive(self):
        """
        make node without archiving, take stream backup and
        turn it into replica, set replica with archiving,
        make archive backup from replica
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

        self.init_pb(backup_dir)
        # ADD INSTANCE 'MASTER'
        self.add_instance(backup_dir, 'master', master)
        master.slow_start()

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")

        self.backup_node(backup_dir, 'master', master, options=['--stream'])
        before = master.safe_psql("postgres", "SELECT * FROM t_heap")

        # Settings for Replica
        self.restore_node(backup_dir, 'master', replica)
        self.set_replica(master, replica, synchronous=True)

        self.add_instance(backup_dir, 'replica', replica)
        self.set_archiving(backup_dir, 'replica', replica, replica=True)
        replica.slow_start(replica=True)

        # Check data correctness on replica
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

        backup_id = self.backup_node(
            backup_dir, 'replica', replica,
            options=[
                '--archive-timeout=30',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port),
                '--stream'])

        self.validate_pb(backup_dir, 'replica')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'replica', backup_id)['status'])

        # RESTORE FULL BACKUP TAKEN FROM replica
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'))
        node.cleanup()
        self.restore_node(backup_dir, 'replica', data_dir=node.data_dir)
        node.append_conf(
            'postgresql.auto.conf', 'port = {0}'.format(node.port))
        node.slow_start()
        # CHECK DATA CORRECTNESS
        after = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        # Change data on master, make PAGE backup from replica,
        # restore taken backup and check that restored data equal
        # to original data
        master.psql(
            "postgres",
            "insert into t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(512,80680) i")

        before = master.safe_psql("postgres", "SELECT * FROM t_heap")

        master.safe_psql(
            "postgres",
            "CHECKPOINT")

        self.wait_until_replica_catch_with_master(master, replica)

        backup_id = self.backup_node(
            backup_dir, 'replica',
            replica, backup_type='page',
            options=[
                '--archive-timeout=60',
                '--master-db=postgres',
                '--master-host=localhost',
                '--master-port={0}'.format(master.port),
                '--stream'])

        self.validate_pb(backup_dir, 'replica')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'replica', backup_id)['status'])

        # RESTORE PAGE BACKUP TAKEN FROM replica
        node.cleanup()
        self.restore_node(
            backup_dir, 'replica', data_dir=node.data_dir, backup_id=backup_id)

        node.append_conf(
            'postgresql.auto.conf', 'port = {0}'.format(node.port))

        node.slow_start()
        # CHECK DATA CORRECTNESS
        after = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_master_and_replica_parallel_archiving(self):
        """
            make node 'master 'with archiving,
            take archive backup and turn it into replica,
            set replica with archiving, make archive backup from replica,
            make archive backup from master
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'archive_timeout': '10s'}
            )
        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.init_pb(backup_dir)
        # ADD INSTANCE 'MASTER'
        self.add_instance(backup_dir, 'master', master)
        self.set_archiving(backup_dir, 'master', master)
        master.slow_start()

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")

        # TAKE FULL ARCHIVE BACKUP FROM MASTER
        self.backup_node(backup_dir, 'master', master)
        # GET LOGICAL CONTENT FROM MASTER
        before = master.safe_psql("postgres", "SELECT * FROM t_heap")
        # GET PHYSICAL CONTENT FROM MASTER
        pgdata_master = self.pgdata_content(master.data_dir)

        # Settings for Replica
        self.restore_node(backup_dir, 'master', replica)
        # CHECK PHYSICAL CORRECTNESS on REPLICA
        pgdata_replica = self.pgdata_content(replica.data_dir)
        self.compare_pgdata(pgdata_master, pgdata_replica)

        self.set_replica(master, replica)
        # ADD INSTANCE REPLICA
        self.add_instance(backup_dir, 'replica', replica)
        # SET ARCHIVING FOR REPLICA
        self.set_archiving(backup_dir, 'replica', replica, replica=True)
        replica.slow_start(replica=True)

        # CHECK LOGICAL CORRECTNESS on REPLICA
        after = replica.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        master.psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0, 60000) i")

        master.psql(
            "postgres",
            "CHECKPOINT")

        backup_id = self.backup_node(
            backup_dir, 'replica', replica,
            options=[
                '--archive-timeout=30',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port),
                '--stream'])

        self.validate_pb(backup_dir, 'replica')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'replica', backup_id)['status'])

        # TAKE FULL ARCHIVE BACKUP FROM MASTER
        backup_id = self.backup_node(backup_dir, 'master', master)
        self.validate_pb(backup_dir, 'master')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'master', backup_id)['status'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_basic_master_and_replica_concurrent_archiving(self):
        """
            make node 'master 'with archiving,
            take archive backup and turn it into replica,
            set replica with archiving, make archive backup from replica,
            make archive backup from master
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s',
                'archive_timeout': '10s'}
            )
        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.init_pb(backup_dir)
        # ADD INSTANCE 'MASTER'
        self.add_instance(backup_dir, 'master', master)
        self.set_archiving(backup_dir, 'master', master)
        master.slow_start()

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")

        # TAKE FULL ARCHIVE BACKUP FROM MASTER
        self.backup_node(backup_dir, 'master', master)
        # GET LOGICAL CONTENT FROM MASTER
        before = master.safe_psql("postgres", "SELECT * FROM t_heap")
        # GET PHYSICAL CONTENT FROM MASTER
        pgdata_master = self.pgdata_content(master.data_dir)

        # Settings for Replica
        self.restore_node(
            backup_dir, 'master', replica)
        # CHECK PHYSICAL CORRECTNESS on REPLICA
        pgdata_replica = self.pgdata_content(replica.data_dir)
        self.compare_pgdata(pgdata_master, pgdata_replica)

        self.set_replica(master, replica, synchronous=True)
        # ADD INSTANCE REPLICA
        # self.add_instance(backup_dir, 'replica', replica)
        # SET ARCHIVING FOR REPLICA
        # self.set_archiving(backup_dir, 'replica', replica, replica=True)
        replica.slow_start(replica=True)

        # CHECK LOGICAL CORRECTNESS on REPLICA
        after = replica.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        master.psql(
            "postgres",
            "insert into t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")

        # TAKE FULL ARCHIVE BACKUP FROM REPLICA
        backup_id = self.backup_node(
            backup_dir, 'master', replica,
            options=[
                '--archive-timeout=30',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port)])

        self.validate_pb(backup_dir, 'master')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'master', backup_id)['status'])

        # TAKE FULL ARCHIVE BACKUP FROM MASTER
        backup_id = self.backup_node(backup_dir, 'master', master)
        self.validate_pb(backup_dir, 'master')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'master', backup_id)['status'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_archive_pg_receivexlog(self):
        """Test backup with pg_receivexlog wal delivary method"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()
        if self.get_version(node) < 100000:
            pg_receivexlog_path = self.get_bin_path('pg_receivexlog')
        else:
            pg_receivexlog_path = self.get_bin_path('pg_receivewal')

        pg_receivexlog = self.run_binary(
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

        self.backup_node(backup_dir, 'node', node)

        # PAGE
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(10000,20000) i")

        self.backup_node(
            backup_dir,
            'node',
            node,
            backup_type='page'
        )
        result = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.validate_pb(backup_dir)

        # Check data correctness
        node.cleanup()
        self.restore_node(backup_dir, 'node', node)
        node.slow_start()

        self.assertEqual(
            result,
            node.safe_psql(
                "postgres", "SELECT * FROM t_heap"
            ),
            'data after restore not equal to original data')

        # Clean after yourself
        pg_receivexlog.kill()
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_archive_pg_receivexlog_compression_pg10(self):
        """Test backup with pg_receivewal compressed wal delivary method"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s'}
            )
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()
        if self.get_version(node) < self.version_to_num('10.0'):
            return unittest.skip('You need PostgreSQL >= 10 for this test')
        else:
            pg_receivexlog_path = self.get_bin_path('pg_receivewal')

        pg_receivexlog = self.run_binary(
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

        self.backup_node(backup_dir, 'node', node)

        # PAGE
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(10000,20000) i")

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='page'
            )
        result = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.validate_pb(backup_dir)

        # Check data correctness
        node.cleanup()
        self.restore_node(backup_dir, 'node', node)
        node.slow_start()

        self.assertEqual(
            result, node.safe_psql("postgres", "SELECT * FROM t_heap"),
            'data after restore not equal to original data')

        # Clean after yourself
        pg_receivexlog.kill()
        self.del_test_dir(module_name, fname)

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
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'archive_timeout': '30s',
                'checkpoint_timeout': '30s',
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        self.set_archiving(backup_dir, 'master', master)

        master.slow_start()

        # FULL
        master.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")

        self.backup_node(backup_dir, 'master', master)

        # PAGE
        master.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(10000,20000) i")

        self.backup_node(
            backup_dir, 'master', master, backup_type='page')

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()
        self.restore_node(backup_dir, 'master', replica)
        self.set_replica(master, replica)

        self.add_instance(backup_dir, 'replica', replica)
        self.set_archiving(backup_dir, 'replica', replica, replica=True)

        copy_tree(
            os.path.join(backup_dir, 'wal', 'master'),
            os.path.join(backup_dir, 'wal', 'replica'))

        # Check data correctness on replica
        replica.slow_start(replica=True)

        # FULL backup replica
        Y1 = self.backup_node(
            backup_dir, 'replica', replica,
            options=['--stream', '--archive-timeout=60s'])

        master.pgbench_init(scale=5)

        # PAGE backup replica
        Y2 = self.backup_node(
            backup_dir, 'replica', replica,
            backup_type='page', options=['--stream', '--archive-timeout=60s'])

        # create timeline t2
        replica.promote()

        # do checkpoint to increment timeline ID in pg_control
        replica.safe_psql(
            'postgres',
            'CHECKPOINT')

        # FULL backup replica
        A1 = self.backup_node(
            backup_dir, 'replica', replica)

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
        A2 = self.backup_node(
            backup_dir, 'replica', replica, backup_type='delta')

        # create timeline t3
        replica.cleanup()
        self.restore_node(
            backup_dir, 'replica', replica,
            options=[
                '--recovery-target-xid={0}'.format(target_xid),
                '--recovery-target-timeline=2',
                '--recovery-target-action=promote'])

        replica.slow_start()

        B1 = self.backup_node(
            backup_dir, 'replica', replica)

        replica.pgbench_init(scale=2)

        B2 = self.backup_node(
            backup_dir, 'replica', replica, backup_type='page')

        replica.pgbench_init(scale=2)

        target_xid = None
        with replica.connect("postgres") as con:
            res = con.execute(
                "INSERT INTO t1 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]

        B3 = self.backup_node(
            backup_dir, 'replica', replica, backup_type='page')

        replica.pgbench_init(scale=2)

        # create timeline t4
        replica.cleanup()
        self.restore_node(
            backup_dir, 'replica', replica,
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
        self.restore_node(
            backup_dir, 'replica', replica,
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

        self.restore_node(
            backup_dir, 'replica', replica, backup_id=A1,
            options=[
                '--recovery-target=immediate',
                '--recovery-target-action=promote'])
        replica.slow_start()

        replica.pgbench_init(scale=2)

        show = self.show_archive(backup_dir, as_text=True)
        show = self.show_archive(backup_dir)

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

        # create holes in t3
        wals_dir = os.path.join(backup_dir, 'wal', 'replica')
        wals = [
                f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f))
                and not f.endswith('.backup') and not f.endswith('.history') and f.startswith('00000003')
            ]
        wals.sort()

        # check that t3 is ok
        self.show_archive(backup_dir)

        file = os.path.join(backup_dir, 'wal', 'replica', '000000030000000000000017')
        if self.archive_compress:
            file = file + '.gz'
        os.remove(file)

        file = os.path.join(backup_dir, 'wal', 'replica', '000000030000000000000012')
        if self.archive_compress:
            file = file + '.gz'
        os.remove(file)

        file = os.path.join(backup_dir, 'wal', 'replica', '000000030000000000000013')
        if self.archive_compress:
            file = file + '.gz'
        os.remove(file)

        # check that t3 is not OK
        show = self.show_archive(backup_dir)

        show = self.show_archive(backup_dir)

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
        self.assertEqual(timeline_3['lost-segments'][0]['begin-segno'], '0000000000000012')
        self.assertEqual(timeline_3['lost-segments'][0]['end-segno'], '0000000000000013')
        self.assertEqual(timeline_3['lost-segments'][1]['begin-segno'], '0000000000000017')
        self.assertEqual(timeline_3['lost-segments'][1]['end-segno'], '0000000000000017')

        self.assertEqual(len(timeline_6['backups']), 0)
        self.assertEqual(len(timeline_5['backups']), 0)
        self.assertEqual(len(timeline_4['backups']), 0)
        self.assertEqual(len(timeline_3['backups']), 3)
        self.assertEqual(len(timeline_2['backups']), 2)
        self.assertEqual(len(timeline_1['backups']), 2)

        # check prior backup correctness
        self.assertEqual(timeline_6['prior-backup-id'], A1)
        self.assertEqual(timeline_5['prior-backup-id'], B2)
        self.assertEqual(timeline_4['prior-backup-id'], B2)
        self.assertEqual(timeline_3['prior-backup-id'], A1)
        self.assertEqual(timeline_2['prior-backup-id'], Y2)

        # check parent tli correctness
        self.assertEqual(timeline_6['parent-tli'], 2)
        self.assertEqual(timeline_5['parent-tli'], 4)
        self.assertEqual(timeline_4['parent-tli'], 3)
        self.assertEqual(timeline_3['parent-tli'], 2)
        self.assertEqual(timeline_2['parent-tli'], 1)
        self.assertEqual(timeline_1['parent-tli'], 0)


        self.del_test_dir(module_name, fname)

    def test_archive_catalog_1(self):

        backup_dir = '/home/gsmol/git/postgres/contrib/pg_probackup/tests/tmp_dirs/archive/test_archive_catalog/backup'
        self.show_archive(backup_dir)

# important - switchpoint may be NullOffset LSN and not actually existing in archive to boot.
# so write validation code accordingly

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