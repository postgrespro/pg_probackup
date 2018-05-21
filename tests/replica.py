import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException, idx_ptrack
from datetime import datetime, timedelta
import subprocess
from sys import exit
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
            base_dir="{0}/{1}/master".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica', 'max_wal_senders': '2',
                'checkpoint_timeout': '30s', 'ptrack_enable': 'on'}
            )
        master.start()
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
            base_dir="{0}/{1}/replica".format(module_name, fname))
        replica.cleanup()
        self.restore_node(backup_dir, 'master', replica)
        self.set_replica(master, replica)

        # Check data correctness on replica
        replica.start(["-t", "600"])
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
            base_dir="{0}/{1}/node".format(module_name, fname))
        node.cleanup()
        self.restore_node(backup_dir, 'replica', data_dir=node.data_dir)
        node.append_conf(
            'postgresql.auto.conf', 'port = {0}'.format(node.port))
        node.start()
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
        node.start()
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
            base_dir="{0}/{1}/master".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'checkpoint_timeout': '30s'}
            )
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        self.set_archiving(backup_dir, 'master', master)
        # force more frequent wal switch
        master.append_conf('postgresql.auto.conf', 'archive_timeout  = 10')
        master.start()

        replica = self.make_simple_node(
            base_dir="{0}/{1}/replica".format(module_name, fname))
        replica.cleanup()

        self.backup_node(backup_dir, 'master', master)

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,256) i")

        before = master.safe_psql("postgres", "SELECT * FROM t_heap")

        backup_id = self.backup_node(
            backup_dir, 'master', master, backup_type='page')
        self.restore_node(backup_dir, 'master', replica)

        # Settings for Replica
        self.set_replica(master, replica)
        self.set_archiving(backup_dir, 'replica', replica, replica=True)
        replica.start(["-t", "600"])

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
            "from generate_series(256,512) i")
        before = master.safe_psql("postgres", "SELECT * FROM t_heap")
        self.add_instance(backup_dir, 'replica', replica)
        backup_id = self.backup_node(
            backup_dir, 'replica', replica,
            options=[
                '--archive-timeout=300',
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

        # Change data on master, make PAGE backup from replica,
        # restore taken backup and check that restored data equal
        # to original data
        master.psql(
            "postgres",
            "insert into t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(512,768) i")
        before = master.safe_psql("postgres", "SELECT * FROM t_heap")
        backup_id = self.backup_node(
            backup_dir, 'replica', replica, backup_type='page',
            options=[
                '--archive-timeout=300',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port)])
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
            base_dir="{0}/{1}/master".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica', 'max_wal_senders': '2',
                'checkpoint_timeout': '30s'}
            )
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        self.set_archiving(backup_dir, 'master', master)
        # force more frequent wal switch
        master.append_conf('postgresql.auto.conf', 'archive_timeout  = 10')
        master.start()

        replica = self.make_simple_node(
            base_dir="{0}/{1}/replica".format(module_name, fname))
        replica.cleanup()

        self.backup_node(backup_dir, 'master', master)

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,256) i")

        before = master.safe_psql("postgres", "SELECT * FROM t_heap")

        backup_id = self.backup_node(
            backup_dir, 'master', master, backup_type='page')
        self.restore_node(
            backup_dir, 'master', replica,
            options=['-R', '--recovery-target-action=promote'])

        # Settings for Replica
        # self.set_replica(master, replica)
        self.set_archiving(backup_dir, 'replica', replica, replica=True)
        replica.append_conf(
            'postgresql.auto.conf', 'port = {0}'.format(replica.port))
        replica.start(["-t", "600"])

        time.sleep(1)
        self.assertEqual(
            master.safe_psql(
                "postgres",
                "select exists(select * from pg_stat_replication)"
                ).rstrip(),
            't')

        # Clean after yourself
        self.del_test_dir(module_name, fname)
