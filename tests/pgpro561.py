import unittest
import os
import six
from .ptrack_helpers import ProbackupTest, ProbackupException, idx_ptrack
from datetime import datetime, timedelta
from testgres import stop_all
import subprocess
from sys import exit


class CommonArchiveDir(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(CommonArchiveDir, self).__init__(*args, **kwargs)

#    @classmethod
#    def tearDownClass(cls):
#        stop_all()

    def test_pgpro561(self):
        """
        EXPECTED TO FAIL
        make node with archiving, make stream backup, restore it to node1,
        check that archiving is not successful on node1
        """
        fname = self.id().split('.')[3]
        master = self.make_simple_node(base_dir="tmp_dirs/pgpro561/{0}/master".format(fname),
            set_archiving=True,
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        master.start()

        self.assertEqual(self.init_pb(master), six.b(""))
        id = self.backup_pb(master, backup_type='full', options=["--stream"])

        node1 = self.make_simple_node(base_dir="tmp_dirs/pgpro561/{0}/node1".format(fname))
        node1.cleanup()

        master.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")

        self.backup_pb(master, backup_type='page', options=["--stream"])
        self.restore_pb(backup_dir=self.backup_dir(master), data_dir=node1.data_dir)
        node1.append_conf('postgresql.auto.conf', 'port = {0}'.format(node1.port))
        node1.start({"-t": "600"})

        timeline_master = master.get_control_data()["Latest checkpoint's TimeLineID"]
        timeline_node1 = node1.get_control_data()["Latest checkpoint's TimeLineID"]
        self.assertEqual(timeline_master, timeline_node1, "Timelines on Master and Node1 should be equal. This is unexpected")

        archive_command_master = master.safe_psql("postgres", "show archive_command")
        archive_command_node1 = node1.safe_psql("postgres", "show archive_command")
        self.assertEqual(archive_command_master, archive_command_node1, "Archive command on Master and Node should be equal. This is unexpected")

        res = node1.safe_psql("postgres", "select last_failed_wal from pg_stat_get_archiver() where last_failed_wal is not NULL")
        self.assertEqual(res, six.b(""), 'Restored Node1 failed to archive segment {0} due to having the same archive command as Master'.format(res.rstrip()))

        master.stop()
        node1.stop()
