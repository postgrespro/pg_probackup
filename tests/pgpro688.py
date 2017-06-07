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
        self.instance_1 = 'master'
        self.instance_2 = 'slave'

#    @classmethod
#    def tearDownClass(cls):
#        stop_all()

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_replica_stream_full_backup(self):
        """
        make full stream backup from replica
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        master = self.make_simple_node(base_dir="{0}/{1}/master".format(self.module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '5min'}
            )
        master.start()
        shutil.rmtree(backup_dir, ignore_errors=True)
        self.init_pb(backup_dir)
        instance = ''
        self.add_instance(backup_dir, self.instance_1, master)
        self.set_archiving(backup_dir, self.instance_1, master)
        master.restart()

        slave = self.make_simple_node(base_dir="{0}/{1}/slave".format(self.module_name, fname))
        slave_port = slave.port
        slave.cleanup()

        # FULL BACKUP
        self.backup_node(backup_dir, self.instance_1, master, backup_type='full', options=['--stream'])
        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")

        before = master.execute("postgres", "SELECT * FROM t_heap")

        #PAGE BACKUP
        self.backup_node(backup_dir, self.instance_1, master, backup_type='page', options=['--stream'])
        self.restore_node(backup_dir, self.instance_1, slave.data_dir)

        slave.append_conf('postgresql.auto.conf', 'port = {0}'.format(slave.port))
        slave.append_conf('postgresql.auto.conf', 'hot_standby = on')

        slave.append_conf('recovery.conf', "standby_mode = 'on'")
        slave.append_conf('recovery.conf',
            "primary_conninfo = 'user={0} port={1} sslmode=prefer sslcompression=1'".format(get_username(), master.port))
        slave.start({"-t": "600"})
        # Replica Ready

        # Check replica
        after = slave.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        # Make backup from replica
        self.add_instance(backup_dir, self.instance_2, slave)
        self.assertTrue('INFO: Wait end of WAL streaming' and 'completed' in
            self.backup_node(backup_dir, self.instance_2, slave, backup_type='full', options=[
                '--stream', '--log-level=verbose', '--master-host=localhost', '--master-db=postgres', '--master-port={0}'.format(master.port)]))
        self.validate_pb(backup_dir, self.instance_2)
        self.assertEqual('OK', self.show_pb(backup_dir, self.instance_2)[0]['Status'])

    @unittest.skip("skip")
    def test_replica_archive_full_backup(self):
        """
        make full archive backup from replica
        """
        fname = self.id().split('.')[3]
        master = self.make_simple_node(base_dir="tmp_dirs/replica/{0}/master".format(fname),
            set_archiving=True,
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        master.append_conf('postgresql.auto.conf', 'archive_timeout  = 10')
        master.start()

        slave = self.make_simple_node(base_dir="tmp_dirs/replica/{0}/slave".format(fname))
        slave_port = slave.port
        slave.cleanup()

        self.assertEqual(self.init_pb(master), six.b(""))
        self.backup_pb(node=master, backup_type='full', options=['--stream'])

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")

        before = master.execute("postgres", "SELECT * FROM t_heap")

        id = self.backup_pb(master, backup_type='page', options=['--stream'])
        self.restore_pb(backup_dir=self.backup_dir(master), data_dir=slave.data_dir)

        # Settings for Replica
        slave.append_conf('postgresql.auto.conf', 'port = {0}'.format(slave.port))
        slave.append_conf('postgresql.auto.conf', 'hot_standby = on')
        # Set Archiving for replica
        self.set_archiving_conf(slave, replica=True)

        slave.append_conf('recovery.conf', "standby_mode = 'on'")
        slave.append_conf('recovery.conf',
            "primary_conninfo = 'user=gsmol port={0} sslmode=prefer sslcompression=1'".format(master.port))
        slave.start({"-t": "600"})
        # Replica Started

        # master.execute("postgres", "checkpoint")

        # Check replica
        after = slave.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        # Make backup from replica
        self.assertEqual(self.init_pb(slave), six.b(""))
        self.backup_pb(slave, backup_type='full', options=['--archive-timeout=30'])
        self.validate_pb(slave)
