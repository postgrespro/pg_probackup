import unittest
import os
import six
from helpers.ptrack_helpers import ProbackupTest, ProbackupException, idx_ptrack
from datetime import datetime, timedelta
from testgres import stop_all
import subprocess
from sys import exit, _getframe
import shutil
import time


class ReplicaTest(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(ReplicaTest, self).__init__(*args, **kwargs)
        self.module_name = 'replica'

#    @classmethod
#    def tearDownClass(cls):
#        stop_all()

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_replica_stream_full_backup(self):
        """make full stream backup from replica"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        master = self.make_simple_node(base_dir="{0}/{1}/master".format(self.module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '30s'}
            )
        master.start()
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)

        slave = self.make_simple_node(base_dir="{0}/{1}/slave".format(self.module_name, fname))
        slave.cleanup()

        # FULL BACKUP
        self.backup_node(backup_dir, 'master', master, backup_type='full', options=['--stream'])
        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")

        before = master.execute("postgres", "SELECT * FROM t_heap")

        #FULL BACKUP
        self.backup_node(backup_dir, 'master', master, options=['--stream'])
        self.restore_node(backup_dir, 'master', slave)

        slave.append_conf('postgresql.auto.conf', 'port = {0}'.format(slave.port))
        slave.append_conf('postgresql.auto.conf', 'hot_standby = on')

        slave.append_conf('recovery.conf', "standby_mode = 'on'")
        slave.append_conf('recovery.conf',
            "primary_conninfo = 'user={0} port={1} sslmode=prefer sslcompression=1'".format(self.user, master.port))
        slave.start({"-t": "600"})
        # Replica Ready

        # Check replica
        after = slave.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        # Make backup from replica
        self.add_instance(backup_dir, 'slave', slave)
        #time.sleep(2)
        self.assertTrue('INFO: Wait end of WAL streaming' and 'completed' in
            self.backup_node(backup_dir, 'slave', slave, options=['--stream', '--log-level=verbose', 
                '--master-host=localhost', '--master-db=postgres','--master-port={0}'.format(master.port)]))
        self.validate_pb(backup_dir, 'slave')
        self.assertEqual('OK', self.show_pb(backup_dir, 'slave')[0]['Status'])

    # @unittest.skip("skip")
    def test_replica_archive_full_backup(self):
        """make full archive backup from replica"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        master = self.make_simple_node(base_dir="{0}/{1}/master".format(self.module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '30s'}
            )
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        self.set_archiving(backup_dir, 'master', master)
        # force more frequent wal switch
        master.append_conf('postgresql.auto.conf', 'archive_timeout  = 10')
        master.start()

        slave = self.make_simple_node(base_dir="{0}/{1}/slave".format(self.module_name, fname))
        slave.cleanup()

        self.backup_node(backup_dir, 'master', master)

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")

        before = master.execute("postgres", "SELECT * FROM t_heap")

        backup_id = self.backup_node(backup_dir, 'master', master, backup_type='page')
        self.restore_node(backup_dir, 'master', slave)

        # Settings for Replica
        slave.append_conf('postgresql.auto.conf', 'port = {0}'.format(slave.port))
        slave.append_conf('postgresql.auto.conf', 'hot_standby = on')
        # Set Archiving for replica
        #self.set_archiving_conf( slave, replica=True)
        self.set_archiving(backup_dir, 'slave', slave, replica=True)

        # Set Replica
        slave.append_conf('postgresql.auto.conf', 'port = {0}'.format(slave.port))
        slave.append_conf('postgresql.auto.conf', 'hot_standby = on')
        slave.append_conf('recovery.conf', "standby_mode = 'on'")
        slave.append_conf('recovery.conf',
            "primary_conninfo = 'user={0} port={1} sslmode=prefer sslcompression=1'".format(self.user, master.port))
        slave.start({"-t": "600"})

        # Check replica
        after = slave.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)

        # Make backup from replica
        self.add_instance(backup_dir, 'slave', slave)
        self.backup_node(backup_dir, 'slave', slave, options=['--archive-timeout=300',
            '--master-host=localhost', '--master-db=postgres','--master-port={0}'.format(master.port)])
        self.validate_pb(backup_dir, 'slave')
