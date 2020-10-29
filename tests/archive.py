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
        self.clean_pb(backup_dir)
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)

        # Make backup
        self.backup_node(backup_dir, 'node', node)
        node.cleanup()

        # Restore Database
        self.restore_node(backup_dir, 'node', node)
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
                    "--log-level-file=LOG"],
                gdb=True)

        gdb.set_breakpoint('pg_stop_backup')
        gdb.run_until_break()

        self.set_auto_conf(node, {'archive_command': 'exit 1'})
        node.reload()

        gdb.continue_execution_until_exit()

        sleep(1)

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
                "ERROR: WAL segment 000000010000000000000003 could not be archived in 60 seconds",
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
        Check pg_stop_backup_timeout, libpq-timeout requested.
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

        self.set_auto_conf(node, {'archive_command': "'exit 1'"})
        node.reload()

        os.environ["PGAPPNAME"] = "foo"

        pid = node.safe_psql(
            "postgres",
            "SELECT pid "
            "FROM pg_stat_activity "
            "WHERE application_name = 'pg_probackup'").decode('utf-8').rstrip()

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

        with open(file, 'a+b') as f:
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
            'pg_probackup archive-push WAL file',
            log_content)

        self.assertIn(
            'WAL file already exists in archive with different checksum',
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
            pg_options={'checkpoint_timeout': '30s'})

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

        with open(file, 'a+b') as f:
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
            'pg_probackup archive-push WAL file', log_content)
        self.assertNotIn(
            'WAL file already exists in archive with '
            'different checksum, overwriting', log_content)
        self.assertIn(
            'WAL file already exists in archive with '
            'different checksum', log_content)

        self.assertNotIn(
            'pg_probackup archive-push completed successfully', log_content)

        self.set_archiving(backup_dir, 'node', node, overwrite=True)
        node.reload()
        self.switch_wal_segment(node)
        sleep(5)

        with open(log_file, 'r') as f:
            log_content = f.read()
            self.assertTrue(
                'pg_probackup archive-push completed successfully' in log_content,
                'Expecting messages about successfull execution archive_command')

        self.assertIn(
            'WAL file already exists in archive with '
            'different checksum, overwriting', log_content)

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
        self.set_archiving(
            backup_dir, 'node', node,
            log_level='verbose', archive_timeout=60)

        node.slow_start()

        # this backup is needed only for validation to xid
        self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "create table t1(a int)")

        xid = node.safe_psql(
            "postgres",
            "INSERT INTO t1 VALUES (1) RETURNING (xmin)").decode('utf-8').rstrip()

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

        filename_orig = filename_orig.decode('utf-8')

        # form up path to next .part WAL segment
        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        if self.archive_compress:
            filename = filename_orig + '.gz' + '.part'
            file = os.path.join(wals_dir, filename)
        else:
            filename = filename_orig + '.part'
            file = os.path.join(wals_dir, filename)

        # emulate stale .part file
        with open(file, 'a+b') as f:
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
                'Reusing stale temp WAL file',
                log_content)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_archive_push_part_file_exists_not_stale(self):
        """Archive-push if .part file exists and it is not stale"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node, archive_timeout=60)

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

        filename_orig = filename_orig.decode('utf-8')

        # form up path to next .part WAL segment
        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        if self.archive_compress:
            filename = filename_orig + '.gz' + '.part'
            file = os.path.join(wals_dir, filename)
        else:
            filename = filename_orig + '.part'
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

        self.set_auto_conf(node, {'port': node.port})
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

        self.set_auto_conf(node, {'port': node.port})

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
            set replica with archiving,
            make sure that archiving on both node is working.
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s',
                'autovacuum': 'off',
                'archive_timeout': '10s'})

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

        master.pgbench_init(scale=5)

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

        self.set_replica(master, replica, synchronous=False)
        # ADD INSTANCE REPLICA
        # self.add_instance(backup_dir, 'replica', replica)
        # SET ARCHIVING FOR REPLICA
        self.set_archiving(backup_dir, 'master', replica, replica=True)
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
        backup_id = self.backup_node(backup_dir, 'master', replica)

        self.validate_pb(backup_dir, 'master')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'master', backup_id)['status'])

        # TAKE FULL ARCHIVE BACKUP FROM MASTER
        backup_id = self.backup_node(backup_dir, 'master', master)
        self.validate_pb(backup_dir, 'master')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'master', backup_id)['status'])

        master.pgbench_init(scale=10)

        sleep(10)

        replica.promote()

        master.pgbench_init(scale=10)
        replica.pgbench_init(scale=10)

        self.backup_node(backup_dir, 'master', master)
        self.backup_node(backup_dir, 'master', replica)

        # Clean after yourself
        self.del_test_dir(module_name, fname, nodes=[master, replica])

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

        if self.get_version(master) < self.version_to_num('9.6.0'):
            self.del_test_dir(module_name, fname)
            return unittest.skip(
                'Skipped because backup from replica is not supported in PG 9.5')

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

        sleep(5)

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

        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_archive_catalog_1(self):
        """
        double segment - compressed and not
        """
        if not self.archive_compress:
            return self.fail(
                'You need to enable ARCHIVE_COMPRESSION for this test to run')

        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'archive_timeout': '30s',
                'checkpoint_timeout': '30s',
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node, compress=True)

        node.slow_start()

        # FULL
        self.backup_node(backup_dir, 'node', node)
        node.pgbench_init(scale=2)

        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        original_file = os.path.join(wals_dir, '000000010000000000000001.gz')
        tmp_file = os.path.join(wals_dir, '000000010000000000000001')

        with gzip.open(original_file, 'rb') as f_in, open(tmp_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

        os.rename(
            os.path.join(wals_dir, '000000010000000000000001'),
            os.path.join(wals_dir, '000000010000000000000002'))

        show = self.show_archive(backup_dir)

        for instance in show:
            timelines = instance['timelines']

        # sanity
        for timeline in timelines:
            self.assertEqual(
                timeline['min-segno'],
                '000000010000000000000001')
            self.assertEqual(timeline['status'], 'OK')

        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_archive_catalog_2(self):
        """
        double segment - compressed and not
        """
        if not self.archive_compress:
            return self.fail(
                'You need to enable ARCHIVE_COMPRESSION for this test to run')

        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'archive_timeout': '30s',
                'checkpoint_timeout': '30s',
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node, compress=True)

        node.slow_start()

        # FULL
        self.backup_node(backup_dir, 'node', node)
        node.pgbench_init(scale=2)

        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        original_file = os.path.join(wals_dir, '000000010000000000000001.gz')
        tmp_file = os.path.join(wals_dir, '000000010000000000000001')

        with gzip.open(original_file, 'rb') as f_in, open(tmp_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

        os.rename(
            os.path.join(wals_dir, '000000010000000000000001'),
            os.path.join(wals_dir, '000000010000000000000002'))

        os.remove(original_file)

        show = self.show_archive(backup_dir)

        for instance in show:
            timelines = instance['timelines']

        # sanity
        for timeline in timelines:
            self.assertEqual(
                timeline['min-segno'],
                '000000010000000000000002')
            self.assertEqual(timeline['status'], 'OK')

        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_archive_options(self):
        """
        check that '--archive-host', '--archive-user', '--archiver-port'
        and '--restore-command' are working as expected.
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node, compress=True)

        node.slow_start()

        # FULL
        self.backup_node(backup_dir, 'node', node)
        node.pgbench_init(scale=1)

        node.cleanup()

        wal_dir = os.path.join(backup_dir, 'wal', 'node')
        self.restore_node(
            backup_dir, 'node', node,
            options=[
                '--restore-command="cp {0}/%f %p"'.format(wal_dir),
                '--archive-host=localhost',
                '--archive-port=22',
                '--archive-user={0}'.format(self.user)
                ])

        if self.get_version(node) >= self.version_to_num('12.0'):
            recovery_conf = os.path.join(node.data_dir, 'probackup_recovery.conf')
        else:
            recovery_conf = os.path.join(node.data_dir, 'recovery.conf')

        with open(recovery_conf, 'r') as f:
            recovery_content = f.read()

        self.assertIn(
            'restore_command = \'"cp {0}/%f %p"\''.format(wal_dir),
            recovery_content)

        node.cleanup()

        self.restore_node(
            backup_dir, 'node', node,
            options=[
                '--archive-host=localhost',
                '--archive-port=22',
                '--archive-user={0}'.format(self.user)])

        with open(recovery_conf, 'r') as f:
            recovery_content = f.read()

        self.assertIn(
            "restore_command = '{0} archive-get -B {1} --instance {2} "
            "--wal-file-path=%p --wal-file-name=%f --remote-host=localhost "
            "--remote-port=22 --remote-user={3}'".format(
                self.probackup_path, backup_dir, 'node', self.user),
            recovery_content)

        node.slow_start()

        node.safe_psql(
            'postgres',
            'select 1')

        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_archive_options_1(self):
        """
        check that '--archive-host', '--archive-user', '--archiver-port'
        and '--restore-command' are working as expected with set-config
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node, compress=True)

        node.slow_start()

        # FULL
        self.backup_node(backup_dir, 'node', node)
        node.pgbench_init(scale=1)

        node.cleanup()

        wal_dir = os.path.join(backup_dir, 'wal', 'node')
        self.set_config(
            backup_dir, 'node',
            options=[
                '--restore-command="cp {0}/%f %p"'.format(wal_dir),
                '--archive-host=localhost',
                '--archive-port=22',
                '--archive-user={0}'.format(self.user)])
        self.restore_node(backup_dir, 'node', node)

        if self.get_version(node) >= self.version_to_num('12.0'):
            recovery_conf = os.path.join(node.data_dir, 'probackup_recovery.conf')
        else:
            recovery_conf = os.path.join(node.data_dir, 'recovery.conf')

        with open(recovery_conf, 'r') as f:
            recovery_content = f.read()

        self.assertIn(
            'restore_command = \'"cp {0}/%f %p"\''.format(wal_dir),
            recovery_content)

        node.cleanup()

        self.restore_node(
            backup_dir, 'node', node,
            options=[
                '--restore-command=none'.format(wal_dir),
                '--archive-host=localhost1',
                '--archive-port=23',
                '--archive-user={0}'.format(self.user)
                ])

        with open(recovery_conf, 'r') as f:
            recovery_content = f.read()

        self.assertIn(
            "restore_command = '{0} archive-get -B {1} --instance {2} "
            "--wal-file-path=%p --wal-file-name=%f --remote-host=localhost1 "
            "--remote-port=23 --remote-user={3}'".format(
                self.probackup_path, backup_dir, 'node', self.user),
            recovery_content)

        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_hexadecimal_timeline(self):
        """
        Check that timelines are correct.
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node, log_level='verbose')
        node.slow_start()

        backup_id = self.backup_node(backup_dir, 'node', node)
        node.pgbench_init(scale=2)

        # create timelines
        for i in range(1, 13):
            # print(i)
            node.cleanup()
            self.restore_node(
                backup_dir, 'node', node,
                options=['--recovery-target-timeline={0}'.format(i)])
            node.slow_start()
            node.pgbench_init(scale=2)

        sleep(5)

        show = self.show_archive(backup_dir)

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

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_archiving_and_slots(self):
        """
        Check that archiving don`t break slot
        guarantee.
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off',
                'checkpoint_timeout': '30s',
                'max_wal_size': '64MB'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node, log_level='verbose')
        node.slow_start()

        if self.get_version(node) < 100000:
            pg_receivexlog_path = self.get_bin_path('pg_receivexlog')
        else:
            pg_receivexlog_path = self.get_bin_path('pg_receivewal')

        # "pg_receivewal --create-slot --slot archive_slot --if-not-exists "
        # "&& pg_receivewal --synchronous -Z 1 /tmp/wal --slot archive_slot --no-loop"

        self.run_binary(
            [
                pg_receivexlog_path, '-p', str(node.port), '--synchronous',
                '--create-slot', '--slot', 'archive_slot', '--if-not-exists'
            ])

        node.pgbench_init(scale=10)

        pg_receivexlog = self.run_binary(
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

        backup_id = self.backup_node(backup_dir, 'node', node)
        node.pgbench_init(scale=20)

        exit(1)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_archive_push_sanity(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'archive_mode': 'on',
                'archive_command': 'exit 1'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)

        node.slow_start()

        node.pgbench_init(scale=50)
        node.stop()

        self.set_archiving(backup_dir, 'node', node)
        os.remove(os.path.join(node.logs_dir, 'postgresql.log'))
        node.slow_start()

        self.backup_node(backup_dir, 'node', node)

        with open(os.path.join(node.logs_dir, 'postgresql.log'), 'r') as f:
            postgres_log_content = f.read()

        # print(postgres_log_content)
        # make sure that .backup file is not compressed
        self.assertNotIn('.backup.gz', postgres_log_content)
        self.assertNotIn('WARNING', postgres_log_content)

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.restore_node(
            backup_dir, 'node', replica,
            data_dir=replica.data_dir, options=['-R'])

        #self.set_archiving(backup_dir, 'replica', replica, replica=True)
        self.set_auto_conf(replica, {'port': replica.port})
        self.set_auto_conf(replica, {'archive_mode': 'always'})
        self.set_auto_conf(replica, {'hot_standby': 'on'})
        replica.slow_start(replica=True)

        self.wait_until_replica_catch_with_master(node, replica)

        node.pgbench_init(scale=5)

        replica.promote()
        replica.pgbench_init(scale=10)

        with open(os.path.join(replica.logs_dir, 'postgresql.log'), 'r') as f:
            replica_log_content = f.read()

        # make sure that .partial file is not compressed
        self.assertNotIn('.partial.gz', replica_log_content)
        # make sure that .history file is not compressed
        self.assertNotIn('.history.gz', replica_log_content)
        self.assertNotIn('WARNING', replica_log_content)

        output = self.show_archive(
            backup_dir, 'node', as_json=False, as_text=True,
            options=['--log-level-console=VERBOSE'])

        self.assertNotIn('WARNING', output)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_archive_pg_receivexlog_partial_handling(self):
        """check that archive-get delivers .partial and .gz.partial files"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'archive_timeout': '10s'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)

        node.slow_start()

        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.restore_node(
            backup_dir, 'node', replica, replica.data_dir, options=['-R'])
        self.set_auto_conf(replica, {'port': replica.port})
        self.set_replica(node, replica)

        self.add_instance(backup_dir, 'replica', replica)
        # self.set_archiving(backup_dir, 'replica', replica, replica=True)

        replica.slow_start(replica=True)

        if self.get_version(replica) < 100000:
            pg_receivexlog_path = self.get_bin_path('pg_receivexlog')
        else:
            pg_receivexlog_path = self.get_bin_path('pg_receivewal')

        cmdline = [
            pg_receivexlog_path, '-p', str(replica.port), '--synchronous',
            '-D', os.path.join(backup_dir, 'wal', 'replica')]

        if self.archive_compress and node.major_version >= 10:
            cmdline += ['-Z', '1']

        pg_receivexlog = self.run_binary(cmdline, asynchronous=True)

        if pg_receivexlog.returncode:
            self.assertFalse(
                True,
                'Failed to start pg_receivexlog: {0}'.format(
                    pg_receivexlog.communicate()[1]))

        # FULL
        self.backup_node(backup_dir, 'replica', replica, options=['--stream'])

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1000000) i")

        # PAGE
        self.backup_node(
            backup_dir, 'replica', replica, backup_type='delta', options=['--stream'])

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(1000000,2000000) i")

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'replica', node_restored,
            node_restored.data_dir, options=['--recovery-target=latest', '--recovery-target-action=promote'])
        self.set_auto_conf(node_restored, {'port': node_restored.port})
        self.set_auto_conf(node_restored, {'hot_standby': 'off'})

        # it will set node_restored as warm standby.
#        with open(os.path.join(node_restored.data_dir, "standby.signal"), 'w') as f:
#            f.flush()
#            f.close()

        node_restored.slow_start()

        result = node.safe_psql(
            "postgres",
            "select sum(id) from t_heap")

        result_new = node_restored.safe_psql(
            "postgres",
            "select sum(id) from t_heap")

        self.assertEqual(result, result_new)

        # Clean after yourself
        pg_receivexlog.kill()
        self.del_test_dir(
            module_name, fname, [node, replica, node_restored])

    @unittest.skip("skip")
    def test_multi_timeline_recovery_prefetching(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)

        node.slow_start()

        self.backup_node(backup_dir, 'node', node)

        node.pgbench_init(scale=50)

        target_xid = node.safe_psql(
            'postgres',
            'select txid_current()').rstrip()

        node.pgbench_init(scale=20)

        node.stop()
        node.cleanup()

        self.restore_node(
            backup_dir, 'node', node,
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

        self.restore_node(
            backup_dir, 'node', node,
            options=[
#                '--recovery-target-xid={0}'.format(target_xid),
                '--recovery-target-timeline=2',
#                '--recovery-target-action=promote',
                '--no-validate'])
        node.slow_start()

        node.pgbench_init(scale=20)
        result = node.safe_psql(
            'postgres',
            'select * from pgbench_accounts')
        node.stop()
        node.cleanup()

        self.restore_node(
            backup_dir, 'node', node,
            options=[
#                '--recovery-target-xid=100500',
                '--recovery-target-timeline=3',
#                '--recovery-target-action=promote',
                '--no-validate'])
        os.remove(os.path.join(node.logs_dir, 'postgresql.log'))

        restore_command = self.get_restore_command(backup_dir, 'node', node)
        restore_command += ' -j 2 --batch-size=10 --log-level-console=VERBOSE'

        if node.major_version >= 12:
            node.append_conf(
                'probackup_recovery.conf', "restore_command = '{0}'".format(restore_command))
        else:
            node.append_conf(
                'recovery.conf', "restore_command = '{0}'".format(restore_command))

        node.slow_start()

        result_new = node.safe_psql(
            'postgres',
            'select * from pgbench_accounts')

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

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_archive_get_batching_sanity(self):
        """
        Make sure that batching works.
        .gz file is corrupted and uncompressed is not, check that both
            corruption detected and uncompressed file is used.
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        if self.get_version(node) < self.version_to_num('9.6.0'):
            self.del_test_dir(module_name, fname)
            return unittest.skip(
                'Skipped because backup from replica is not supported in PG 9.5')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)

        node.slow_start()

        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        node.pgbench_init(scale=50)

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.restore_node(
            backup_dir, 'node', replica, replica.data_dir)
        self.set_replica(node, replica, log_shipping=True)

        if node.major_version >= 12:
            self.set_auto_conf(replica, {'restore_command': 'exit 1'})
        else:
            replica.append_conf('recovery.conf', "restore_command = 'exit 1'")

        replica.slow_start(replica=True)

        # at this point replica is consistent
        restore_command = self.get_restore_command(backup_dir, 'node', replica)

        restore_command += ' -j 2 --batch-size=10'

        print(restore_command)

        if node.major_version >= 12:
            self.set_auto_conf(replica, {'restore_command': restore_command})
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

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_archive_get_prefetch_corruption(self):
        """
        Make sure that WAL corruption is detected.
        And --prefetch-dir is honored.
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)

        node.slow_start()

        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        node.pgbench_init(scale=50)

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.restore_node(
            backup_dir, 'node', replica, replica.data_dir)
        self.set_replica(node, replica, log_shipping=True)

        if node.major_version >= 12:
            self.set_auto_conf(replica, {'restore_command': 'exit 1'})
        else:
            replica.append_conf('recovery.conf', "restore_command = 'exit 1'")

        replica.slow_start(replica=True)

        # at this point replica is consistent
        restore_command = self.get_restore_command(backup_dir, 'node', replica)

        restore_command += ' -j5 --batch-size=10 --log-level-console=VERBOSE'
        #restore_command += ' --batch-size=2 --log-level-console=VERBOSE'

        if node.major_version >= 12:
            self.set_auto_conf(replica, {'restore_command': restore_command})
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
        node.pgbench_init(scale=20)
        sleep(20)

        # now copy WAL files into prefetch directory and corrupt some of them
        archive_dir = os.path.join(backup_dir, 'wal', 'node')
        files = os.listdir(archive_dir)
        files.sort()

        for filename in [files[-4], files[-3], files[-2], files[-1]]:
            src_file = os.path.join(archive_dir, filename)
    
            if node.major_version >= 10:
                wal_dir = 'pg_wal'
            else:
                wal_dir = 'pg_xlog'
    
            if filename.endswith('.gz'):
                dst_file = os.path.join(replica.data_dir, wal_dir, 'pbk_prefetch', filename[:-3])
                with gzip.open(src_file, 'rb') as f_in, open(dst_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            else:
                dst_file = os.path.join(replica.data_dir, wal_dir, 'pbk_prefetch', filename)
                shutil.copyfile(src_file, dst_file)

            print(dst_file)

        # corrupt file
        if files[-2].endswith('.gz'):
            filename = files[-2][:-3]
        else:
            filename = files[-2]

        prefetched_file = os.path.join(replica.data_dir, wal_dir, 'pbk_prefetch', filename)

        with open(prefetched_file, "rb+", 0) as f:
            f.seek(8192*2)
            f.write(b"SURIKEN")
            f.flush()
            f.close

        # enable restore_command
        restore_command = self.get_restore_command(backup_dir, 'node', replica)
        restore_command += ' --batch-size=2 --log-level-console=VERBOSE'

        if node.major_version >= 12:
            self.set_auto_conf(replica, {'restore_command': restore_command})
        else:
            replica.append_conf(
                'recovery.conf', "restore_command = '{0}'".format(restore_command))

        os.remove(os.path.join(replica.logs_dir, 'postgresql.log'))
        replica.slow_start(replica=True)

        sleep(60)

        with open(os.path.join(replica.logs_dir, 'postgresql.log'), 'r') as f:
            postgres_log_content = f.read()

        self.assertIn(
            'Prefetched WAL segment {0} is invalid, cannot use it'.format(filename),
            postgres_log_content)

        self.assertIn(
            'LOG:  restored log file "{0}" from archive'.format(filename),
            postgres_log_content)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_archive_show_partial_files_handling(self):
        """
        check that files with '.part', '.part.gz', '.partial' and '.partial.gz'
        siffixes are handled correctly
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node, compress=False)

        node.slow_start()

        self.backup_node(backup_dir, 'node', node)

        wals_dir = os.path.join(backup_dir, 'wal', 'node')

        # .part file
        node.safe_psql(
            "postgres",
            "create table t1()")

        if self.get_version(node) < 100000:
            filename = node.safe_psql(
                "postgres",
                "SELECT file_name "
                "FROM pg_xlogfile_name_offset(pg_current_xlog_location())").rstrip()
        else:
            filename = node.safe_psql(
                "postgres",
                "SELECT file_name "
                "FROM pg_walfile_name_offset(pg_current_wal_flush_lsn())").rstrip()

        filename = filename.decode('utf-8')

        self.switch_wal_segment(node)

        os.rename(
            os.path.join(wals_dir, filename),
            os.path.join(wals_dir, '{0}.part'.format(filename)))

        # .gz.part file
        node.safe_psql(
            "postgres",
            "create table t2()")

        if self.get_version(node) < 100000:
            filename = node.safe_psql(
                "postgres",
                "SELECT file_name "
                "FROM pg_xlogfile_name_offset(pg_current_xlog_location())").rstrip()
        else:
            filename = node.safe_psql(
                "postgres",
                "SELECT file_name "
                "FROM pg_walfile_name_offset(pg_current_wal_flush_lsn())").rstrip()

        filename = filename.decode('utf-8')

        self.switch_wal_segment(node)

        os.rename(
            os.path.join(wals_dir, filename),
            os.path.join(wals_dir, '{0}.gz.part'.format(filename)))

        # .partial file
        node.safe_psql(
            "postgres",
            "create table t3()")

        if self.get_version(node) < 100000:
            filename = node.safe_psql(
                "postgres",
                "SELECT file_name "
                "FROM pg_xlogfile_name_offset(pg_current_xlog_location())").rstrip()
        else:
            filename = node.safe_psql(
                "postgres",
                "SELECT file_name "
                "FROM pg_walfile_name_offset(pg_current_wal_flush_lsn())").rstrip()

        filename = filename.decode('utf-8')

        self.switch_wal_segment(node)

        os.rename(
            os.path.join(wals_dir, filename),
            os.path.join(wals_dir, '{0}.partial'.format(filename)))

        # .gz.partial file
        node.safe_psql(
            "postgres",
            "create table t4()")

        if self.get_version(node) < 100000:
            filename = node.safe_psql(
                "postgres",
                "SELECT file_name "
                "FROM pg_xlogfile_name_offset(pg_current_xlog_location())").rstrip()
        else:
            filename = node.safe_psql(
                "postgres",
                "SELECT file_name "
                "FROM pg_walfile_name_offset(pg_current_wal_flush_lsn())").rstrip()

        filename = filename.decode('utf-8')

        self.switch_wal_segment(node)

        os.rename(
            os.path.join(wals_dir, filename),
            os.path.join(wals_dir, '{0}.gz.partial'.format(filename)))

        self.show_archive(backup_dir, 'node', options=['--log-level-file=VERBOSE'])

        with open(os.path.join(backup_dir, 'log', 'pg_probackup.log'), 'r') as f:
            log_content = f.read()

        self.assertNotIn(
            'WARNING',
            log_content)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

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