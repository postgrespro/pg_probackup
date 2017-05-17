import unittest
import os
import six
from .ptrack_helpers import ProbackupTest, ProbackupException, idx_ptrack
from datetime import datetime, timedelta
from testgres import stop_all
import subprocess
from sys import exit


class SomeTest(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(SomeTest, self).__init__(*args, **kwargs)

#    @classmethod
#    def tearDownClass(cls):
#        stop_all()

    def test_pgpro561(self):
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

        node1 = self.make_simple_node(base_dir="tmp_dirs/pgpro561/{0}/node1".format(fname))
        node2 = self.make_simple_node(base_dir="tmp_dirs/pgpro561/{0}/node2".format(fname))
        node1.cleanup()
        node2.cleanup()

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
        #before = master.execute("postgres", "SELECT * FROM t_heap")

        id = self.backup_pb(master, backup_type='page')
        self.restore_pb(backup_dir=self.backup_dir(master), data_dir=node1.data_dir)
        node1.append_conf('postgresql.auto.conf', 'port = {0}'.format(node1.port))

        self.restore_pb(backup_dir=self.backup_dir(master), data_dir=node2.data_dir)
        node2.append_conf('postgresql.auto.conf', 'port = {0}'.format(node2.port))

        node1.start({"-t": "600"})
        node2.start({"-t": "600"})

        timeline_node1 = node1.get_control_data()["Latest checkpoint's TimeLineID"]
        timeline_node2 = node2.get_control_data()["Latest checkpoint's TimeLineID"]
        self.assertEqual(timeline_node1, timeline_node2,
            "Node1 and Node2 timelines are different.\nWhich means that Node2 applied wals archived by Master AND Node1.\nWhich means that Master and Node1 have common archive.\nTHIS IS BAD\nCheck archive directory in {0}".format(os.path.join(self.backup_dir(master), "wal")))
#        node1.pgbench_init(scale=5)
#        node2.pgbench_init(scale=5)
