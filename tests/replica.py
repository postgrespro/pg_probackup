import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException, idx_ptrack
from datetime import datetime, timedelta
import subprocess
import time


module_name = 'replica'


class ReplicaTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_replica_stream_ptrack_backup(self):
        """
        make node, take full backup, restore it and make replica from it,
        take full stream backup from replica
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'ptrack_enable': 'on'}
            )
        master.slow_start()
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)

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

        node.append_conf(
            'postgresql.auto.conf', 'port = {0}'.format(node.port))
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

        node.append_conf(
            'postgresql.auto.conf', 'port = {0}'.format(node.port))
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
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'archive_timeout': '10s'}
            )
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

        master.psql(
            "postgres",
            "CHECKPOINT")

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

        node.append_conf(
            'postgresql.auto.conf', 'port = {0}'.format(node.port))

        node.append_conf(
            'postgresql.auto.conf', 'archive_mode = off'.format(node.port))

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

        node.append_conf(
            'postgresql.auto.conf', 'port = {0}'.format(node.port))

        node.append_conf(
            'postgresql.auto.conf', 'archive_mode = off')

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
    def test_make_replica_via_restore(self):
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
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'archive_timeout': '10s'}
            )
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        self.set_archiving(backup_dir, 'master', master)
        # force more frequent wal switch
        master.append_conf('postgresql.auto.conf', 'archive_timeout  = 10')
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
        replica.append_conf(
            'postgresql.auto.conf', 'port = {0}'.format(replica.port))
        replica.append_conf(
            'postgresql.auto.conf', 'hot_standby = on')

        replica.slow_start(replica=True)

        self.backup_node(
            backup_dir, 'replica', replica,
            options=['--archive-timeout=30s', '--stream'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

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
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'archive_timeout': '10s'}
            )
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
            "CHECKPOINT")

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

        replica.append_conf(
            'postgresql.auto.conf', 'port = {0}'.format(replica.port))

        replica.slow_start(replica=True)

        self.wait_until_replica_catch_with_master(master, replica)

        replica.append_conf(
            'recovery.conf', "recovery_min_apply_delay = '300s'")

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
