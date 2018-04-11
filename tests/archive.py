import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException, archive_script
from datetime import datetime, timedelta
import subprocess
from sys import exit
from time import sleep


module_name = 'archive'


class ArchiveTest(ProbackupTest, unittest.TestCase):

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_pgpro434_1(self):
        """Description in jira issue PGPRO-434"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'checkpoint_timeout': '30s'}
            )
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector from "
            "generate_series(0,100) i")

        result = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.backup_node(
            backup_dir, 'node', node,
            options=["--log-level-file=verbose"])
        node.cleanup()

        self.restore_node(
            backup_dir, 'node', node,
            options=["--recovery-target-action=promote"])
        node.start()
        while node.safe_psql(
            "postgres",
                "select pg_is_in_recovery()") == 't\n':
            sleep(1)

        # Recreate backup calagoue
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)

        # Make backup
        self.backup_node(
            backup_dir, 'node', node,
            options=["--log-level-file=verbose"])
        node.cleanup()

        # Restore Database
        self.restore_node(
            backup_dir, 'node', node,
            options=["--recovery-target-action=promote"])
        node.start()
        while node.safe_psql(
            "postgres",
                "select pg_is_in_recovery()") == 't\n':
            sleep(1)

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
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'checkpoint_timeout': '30s'}
            )
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

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
        node.start()
        while node.safe_psql(
            "postgres",
                "select pg_is_in_recovery()") == 't\n':
            sleep(1)
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
        node.start()
        while node.safe_psql(
            "postgres",
                "select pg_is_in_recovery()") == 't\n':
            sleep(1)

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
        node.start()
        while node.safe_psql(
                "postgres",
                "select pg_is_in_recovery()") == 't\n':
            sleep(1)
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
        node.start()
        while node.safe_psql(
                "postgres", "select pg_is_in_recovery()") == 't\n':
            sleep(1)
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
        node.start()
        while node.safe_psql(
                "postgres", "select pg_is_in_recovery()") == 't\n':
            sleep(1)
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
        """Check pg_stop_backup_timeout"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'checkpoint_timeout': '30s'}
            )
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)

        archive_script_path = os.path.join(backup_dir, 'archive_script.sh')
        with open(archive_script_path, 'w+') as f:
            f.write(
                archive_script.format(
                    backup_dir=backup_dir, node_name='node', count_limit=2))

        st = os.stat(archive_script_path)
        os.chmod(archive_script_path, st.st_mode | 0o111)
        node.append_conf(
            'postgresql.auto.conf', "archive_command  = '{0} %p %f'".format(
                archive_script_path))
        node.start()
        try:
            self.backup_node(
                backup_dir, 'node', node,
                options=[
                    "--archive-timeout=60",
                    "--log-level-file=verbose",
                    "--stream"]
                    )
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because pg_stop_backup failed to answer.\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                "ERROR: pg_stop_backup doesn't answer" in e.message and
                "cancel it" in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

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
    def test_arhive_push_file_exists(self):
        """Archive-push if file exists"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'checkpoint_timeout': '30s'}
            )
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)

        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        if self.archive_compress:
            file = os.path.join(wals_dir, '000000010000000000000001.gz')
        else:
            file = os.path.join(wals_dir, '000000010000000000000001')

        with open(file, 'a') as f:
            f.write(b"blablablaadssaaaaaaaaaaaaaaa")
            f.flush()
            f.close()

        node.start()
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,100500) i")
        log_file = os.path.join(node.logs_dir, 'postgresql.log')

        with open(log_file, 'r') as f:
            log_content = f.read()
            self.assertTrue(
                'LOG:  archive command failed with exit code 1' in log_content and
                'DETAIL:  The failed archive command was:' in log_content and
                'INFO: pg_probackup archive-push from' in log_content and
                'ERROR: WAL segment "{0}" already exists.'.format(file) in log_content,
                'Expecting error messages about failed archive_command'
            )
            self.assertFalse('pg_probackup archive-push completed successfully' in log_content)

        os.remove(file)
        self.switch_wal_segment(node)
        sleep(5)

        with open(log_file, 'r') as f:
            log_content = f.read()
            self.assertTrue(
                'pg_probackup archive-push completed successfully' in log_content,
                'Expecting messages about successfull execution archive_command')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_arhive_push_file_exists_overwrite(self):
        """Archive-push if file exists"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'checkpoint_timeout': '30s'}
            )
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)

        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        if self.archive_compress:
            file = os.path.join(wals_dir, '000000010000000000000001.gz')
        else:
            file = os.path.join(wals_dir, '000000010000000000000001')

        with open(file, 'a') as f:
            f.write(b"blablablaadssaaaaaaaaaaaaaaa")
            f.flush()
            f.close()

        node.start()
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,100500) i")
        log_file = os.path.join(node.logs_dir, 'postgresql.log')

        with open(log_file, 'r') as f:
            log_content = f.read()
            self.assertTrue(
                'LOG:  archive command failed with exit code 1' in log_content and
                'DETAIL:  The failed archive command was:' in log_content and
                'INFO: pg_probackup archive-push from' in log_content and
                'ERROR: WAL segment "{0}" already exists.'.format(file) in log_content,
                'Expecting error messages about failed archive_command'
            )
            self.assertFalse('pg_probackup archive-push completed successfully' in log_content)

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

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_replica_archive(self):
        """make node without archiving, take stream backup and turn it into replica, set replica with archiving, make archive backup from replica"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir="{0}/{1}/master".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'checkpoint_timeout': '30s',
                'max_wal_size': '1GB'}
            )
        self.init_pb(backup_dir)
        # ADD INSTANCE 'MASTER'
        self.add_instance(backup_dir, 'master', master)
        master.start()

        replica = self.make_simple_node(
            base_dir="{0}/{1}/replica".format(module_name, fname))
        replica.cleanup()

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,256) i")

        self.backup_node(backup_dir, 'master', master, options=['--stream'])
        before = master.safe_psql("postgres", "SELECT * FROM t_heap")

        # Settings for Replica
        self.restore_node(backup_dir, 'master', replica)
        self.set_replica(master, replica, synchronous=True)
        self.set_archiving(backup_dir, 'replica', replica, replica=True)
        replica.start()

        # Check data correctness on replica
        after = replica.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        # Change data on master, take FULL backup from replica, restore taken backup and check that restored data equal to original data
        master.psql(
            "postgres",
            "insert into t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(256,512) i")
        before = master.safe_psql("postgres", "SELECT * FROM t_heap")
        # ADD INSTANCE 'REPLICA'
        self.add_instance(backup_dir, 'replica', replica)
        backup_id = self.backup_node(
            backup_dir, 'replica', replica,
            options=[
                '--archive-timeout=30',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port)])
        self.validate_pb(backup_dir, 'replica')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'replica', backup_id)['status'])

        # RESTORE FULL BACKUP TAKEN FROM replica
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname))
        node.cleanup()
        self.restore_node(backup_dir, 'replica', data_dir=node.data_dir)
        node.append_conf(
            'postgresql.auto.conf', 'port = {0}'.format(node.port))
        node.start()
        # CHECK DATA CORRECTNESS
        after = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        # Change data on master, make PAGE backup from replica, restore taken backup and check that restored data equal to original data
        master.psql(
            "postgres",
            "insert into t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(512,768) i")
        before = master.safe_psql("postgres", "SELECT * FROM t_heap")
        backup_id = self.backup_node(
            backup_dir, 'replica',
            replica, backup_type='page',
            options=[
                '--archive-timeout=30', '--log-level-file=verbose',
                '--master-host=localhost', '--master-db=postgres',
                '--master-port={0}'.format(master.port)]
                )
        self.validate_pb(backup_dir, 'replica')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'replica', backup_id)['status'])

        # RESTORE PAGE BACKUP TAKEN FROM replica
        node.cleanup()
        self.restore_node(
            backup_dir, 'replica', data_dir=node.data_dir, backup_id=backup_id)
        node.append_conf(
            'postgresql.auto.conf', 'port = {0}'.format(node.port))
        node.start()
        # CHECK DATA CORRECTNESS
        after = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        # Clean after yourself
        # self.del_test_dir(module_name, fname)

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
            base_dir="{0}/{1}/master".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'checkpoint_timeout': '30s'}
            )
        replica = self.make_simple_node(
            base_dir="{0}/{1}/replica".format(module_name, fname))
        replica.cleanup()

        self.init_pb(backup_dir)
        # ADD INSTANCE 'MASTER'
        self.add_instance(backup_dir, 'master', master)
        self.set_archiving(backup_dir, 'master', master)
        master.start()

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

        self.set_replica(master, replica, synchronous=True)
        # ADD INSTANCE REPLICA
        self.add_instance(backup_dir, 'replica', replica)
        # SET ARCHIVING FOR REPLICA
        self.set_archiving(backup_dir, 'replica', replica, replica=True)
        replica.start()

        # CHECK LOGICAL CORRECTNESS on REPLICA
        after = replica.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        # TAKE FULL ARCHIVE BACKUP FROM REPLICA
        backup_id = self.backup_node(
            backup_dir, 'replica', replica,
            options=[
                '--archive-timeout=20',
                '--log-level-file=verbose',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port)]
                )
        self.validate_pb(backup_dir, 'replica')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'replica', backup_id)['status'])

        # TAKE FULL ARCHIVE BACKUP FROM MASTER
        backup_id = self.backup_node(backup_dir, 'master', master)
        self.validate_pb(backup_dir, 'master')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'master', backup_id)['status'])

        # Clean after yourself
        # self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_master_and_replica_concurrent_archiving(self):
        """
            make node 'master 'with archiving,
            take archive backup and turn it into replica,
            set replica with archiving, make archive backup from replica,
            make archive backup from master
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir="{0}/{1}/master".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'checkpoint_timeout': '30s'}
            )
        replica = self.make_simple_node(
            base_dir="{0}/{1}/replica".format(module_name, fname))
        replica.cleanup()

        self.init_pb(backup_dir)
        # ADD INSTANCE 'MASTER'
        self.add_instance(backup_dir, 'master', master)
        self.set_archiving(backup_dir, 'master', master)
        master.start()

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

        self.set_replica(master, replica, synchronous=True)
        # ADD INSTANCE REPLICA
        # self.add_instance(backup_dir, 'replica', replica)
        # SET ARCHIVING FOR REPLICA
        # self.set_archiving(backup_dir, 'replica', replica, replica=True)
        replica.start()

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
        # self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_archive_pg_receivexlog(self):
        """Test backup with pg_receivexlog wal delivary method"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'checkpoint_timeout': '30s'}
            )
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.start()
        if self.get_version(node) < 100000:
            pg_receivexlog_path = self.get_bin_path('pg_receivexlog')
        else:
            pg_receivexlog_path = self.get_bin_path('pg_receivewal')

        pg_receivexlog = self.run_binary(
            [
                pg_receivexlog_path, '-p', str(node.port), '--synchronous',
                '-D', os.path.join(backup_dir, 'wal', 'node')
            ], async=True)

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
        node.start()
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
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'checkpoint_timeout': '30s'}
            )
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.start()
        if self.get_version(node) < self.version_to_num('10.0'):
            return unittest.skip('You need PostgreSQL 10 for this test')
        else:
            pg_receivexlog_path = node.get_bin_path('pg_receivewal')

        pg_receivexlog = self.run_binary(
            [
                pg_receivexlog_path, '-p', str(node.port), '--synchronous',
                '-Z', '9', '-D', os.path.join(backup_dir, 'wal', 'node')
            ], async=True)

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
        node.start()
        self.assertEqual(
            result, node.safe_psql("postgres", "SELECT * FROM t_heap"),
            'data after restore not equal to original data')

        # Clean after yourself
        pg_receivexlog.kill()
        self.del_test_dir(module_name, fname)
