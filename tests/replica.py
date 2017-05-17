import unittest
import os
import six
from .ptrack_helpers import ProbackupTest, ProbackupException, idx_ptrack
from datetime import datetime, timedelta
from testgres import stop_all
import subprocess
from sys import exit


class ReplicaTest(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(SomeTest, self).__init__(*args, **kwargs)

    @classmethod
    def tearDownClass(cls):
        stop_all()

    def test_make_simple_replica(self):
        """
        make node with archiving, make stream backup,
        get Recovery Time, try to make pitr to Recovery Time
        """
        fname = self.id().split('.')[3]
        master = self.make_simple_node(base_dir="tmp_dirs/pgpro561/{0}/master".format(fname),
            set_archiving=True,
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        master.start()

        slave = self.make_simple_node(base_dir="tmp_dirs/pgpro561/{0}/replica".format(fname),
            set_archiving=True,
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        slave_port = slave.port
        slave.cleanup()

        self.assertEqual(self.init_pb(master), six.b(""))
        self.backup_pb(master, backup_type='full')

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")
        # for i in idx_ptrack:
        #    if idx_ptrack[i]['type'] == 'heap':
        #        continue
        #    master.psql("postgres", "create index {0} on {1} using {2}({3})".format(
        #        i, idx_ptrack[i]['relation'], idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        before = master.execute("postgres", "SELECT * FROM t_heap")

        id = self.backup_pb(master, backup_type='page')
        self.restore_pb(backup_dir=self.backup_dir(master), data_dir=slave.data_dir)
        slave.append_conf('postgresql.auto.conf', 'port = {0}'.format(slave.port))
        slave.append_conf('postgresql.auto.conf', 'hot_standby = on')

        slave.append_conf('recovery.conf', "standby_mode = 'on'")
        slave.append_conf('recovery.conf',
            "primary_conninfo = 'user=gsmol port={0} sslmode=prefer sslcompression=1'".format(master.port))
        slave.start({"-t": "600"})

        after = slave.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(before, after)
