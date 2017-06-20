import unittest
import os
import six
from helpers.ptrack_helpers import ProbackupTest, ProbackupException
from datetime import datetime, timedelta
from testgres import stop_all, get_username
import subprocess
from sys import exit, _getframe
import shutil
import time


class ReplicaTest(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(ReplicaTest, self).__init__(*args, **kwargs)
        self.module_name = 'replica'
        self.instance_master = 'master'
        self.instance_replica = 'replica'

#    @classmethod
#    def tearDownClass(cls):
#        stop_all()

    @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_replica_stream_full_backup(self):
        """make full stream backup from replica"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        master = self.make_simple_node(base_dir="{0}/{1}/master".format(self.module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '5min'}
            )
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, self.instance_master, master)
        master.start()

        # Make empty Object 'replica' from new node
        replica = self.make_simple_node(base_dir="{0}/{1}/replica".format(self.module_name, fname))
        replica_port = replica.port
        replica.cleanup()

        # FULL STREAM backup of master
        self.backup_node(backup_dir, self.instance_master, master, backup_type='full', options=['--stream'])
        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")
        before = master.execute("postgres", "SELECT * FROM t_heap")

        # FULL STREAM backup of master
        self.backup_node(backup_dir, self.instance_master, master, backup_type='full', options=['--stream'])

        # Restore last backup from master to Replica directory
        self.restore_node(backup_dir, self.instance_master, replica.data_dir)
        # Set Replica
        replica.append_conf('postgresql.auto.conf', 'port = {0}'.format(replica.port))
        replica.append_conf('postgresql.auto.conf', 'hot_standby = on')
        replica.append_conf('recovery.conf', "standby_mode = 'on'")
        replica.append_conf('recovery.conf',
            "primary_conninfo = 'user={0} port={1} sslmode=prefer sslcompression=1'".format(get_username(), master.port))
        replica.start({"-t": "600"})

        # Check replica
        after = replica.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        # Add instance replica
        self.add_instance(backup_dir, self.instance_replica, replica)

        # FULL STREAM backup of replica
        self.assertTrue('INFO: Wait end of WAL streaming' and 'completed' in
            self.backup_node(backup_dir, self.instance_replica, replica, backup_type='full', options=[
                '--stream', '--log-level=verbose', '--master-host=localhost', '--master-db=postgres', '--master-port={0}'.format(master.port)]))

        # Validate instance replica
        self.validate_pb(backup_dir, self.instance_replica)
        self.assertEqual('OK', self.show_pb(backup_dir, self.instance_replica)[0]['Status'])

    def test_replica_archive_full_backup(self):
        """make page archive backup from replica"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        master = self.make_simple_node(base_dir="{0}/{1}/master".format(self.module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '5min'}
            )
        self.set_archiving(backup_dir, self.instance_master, master)
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, self.instance_master, master)
        master.start()

        # Make empty Object 'replica' from new node
        replica = self.make_simple_node(base_dir="{0}/{1}/replica".format(self.module_name, fname))
        replica_port = replica.port
        replica.cleanup()

        # FULL ARCHIVE backup of master
        self.backup_node(backup_dir, self.instance_master, master, backup_type='full')
        # Create table t_heap
        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")
        before = master.execute("postgres", "SELECT * FROM t_heap")

        # PAGE ARCHIVE backup of master
        self.backup_node(backup_dir, self.instance_master, master, backup_type='page')

        # Restore last backup from master to Replica directory
        self.restore_node(backup_dir, self.instance_master, replica.data_dir)

        # Set Replica
        self.set_archiving(backup_dir, self.instance_replica, replica, replica=True)
        replica.append_conf('postgresql.auto.conf', 'port = {0}'.format(replica.port))
        replica.append_conf('postgresql.auto.conf', 'hot_standby = on')

        replica.append_conf('recovery.conf', "standby_mode = 'on'")
        replica.append_conf('recovery.conf',
            "primary_conninfo = 'user={0} port={1} sslmode=prefer sslcompression=1'".format(get_username(), master.port))
        replica.start({"-t": "600"})

        # Check replica
        after = replica.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        # Make FULL ARCHIVE backup from replica
        self.add_instance(backup_dir, self.instance_replica, replica)
        self.assertTrue('INFO: Wait end of WAL streaming' and 'completed' in
            self.backup_node(backup_dir, self.instance_replica, replica, backup_type='full', options=[
                '--log-level=verbose', '--master-host=localhost', '--master-db=postgres', '--master-port={0}'.format(master.port)]))
        self.validate_pb(backup_dir, self.instance_replica)
        self.assertEqual('OK', self.show_pb(backup_dir, self.instance_replica)[0]['Status'])

        # Drop Table t_heap
        after = master.execute("postgres", "drop table t_heap")
        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,512) i")
        before = master.execute("postgres", "SELECT * FROM t_heap")

        # Make page backup from replica
        self.assertTrue('INFO: Wait end of WAL streaming' and 'completed' in
            self.backup_node(backup_dir, self.instance_replica, replica, backup_type='page', options=[
                '--log-level=verbose', '--master-host=localhost', '--master-db=postgres', '--master-port={0}'.format(master.port)]))
        self.validate_pb(backup_dir, self.instance_replica)
        self.assertEqual('OK', self.show_pb(backup_dir, self.instance_replica)[0]['Status'])

    @unittest.skip("skip")
    def test_replica_archive_full_backup_123(self):
        """
        make full archive backup from replica
        """
        fname = self.id().split('.')[3]
        master = self.make_simple_node(base_dir="tmp_dirs/replica/{0}/master".format(fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        master.append_conf('postgresql.auto.conf', 'archive_timeout  = 10')
        master.start()

        replica = self.make_simple_node(base_dir="tmp_dirs/replica/{0}/replica".format(fname))
        replica_port = replica.port
        replica.cleanup()

        self.assertEqual(self.init_pb(master), six.b(""))
        self.backup_pb(node=master, backup_type='full', options=['--stream'])

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")

        before = master.execute("postgres", "SELECT * FROM t_heap")

        id = self.backup_pb(master, backup_type='page', options=['--stream'])
        self.restore_pb(backup_dir=self.backup_dir(master), data_dir=replica.data_dir)

        # Settings for Replica
        replica.append_conf('postgresql.auto.conf', 'port = {0}'.format(replica.port))
        replica.append_conf('postgresql.auto.conf', 'hot_standby = on')
        # Set Archiving for replica
        self.set_archiving_conf(replica, replica=True)

        replica.append_conf('recovery.conf', "standby_mode = 'on'")
        replica.append_conf('recovery.conf',
            "primary_conninfo = 'user=gsmol port={0} sslmode=prefer sslcompression=1'".format(master.port))
        replica.start({"-t": "600"})
        # Replica Started

        # master.execute("postgres", "checkpoint")

        # Check replica
        after = replica.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        # Make backup from replica
        self.assertEqual(self.init_pb(replica), six.b(""))
        self.backup_pb(replica, backup_type='full', options=['--archive-timeout=30'])
        self.validate_pb(replica)
