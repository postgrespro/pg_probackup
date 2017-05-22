import unittest
import os
import six
from .ptrack_helpers import ProbackupTest, ProbackupException
from datetime import datetime, timedelta
from testgres import stop_all
import subprocess
from sys import exit


class FalsePositive(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(FalsePositive, self).__init__(*args, **kwargs)

    @classmethod
    def tearDownClass(cls):
        stop_all()

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_pgpro561(self):
        """
        make node with archiving, make stream backup, restore it to node1,
        check that archiving is not successful on node1
        """
        fname = self.id().split('.')[3]
        master = self.make_simple_node(base_dir="tmp_dirs/false_positive/{0}/master".format(fname),
            set_archiving=True,
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        master.start()

        self.assertEqual(self.init_pb(master), six.b(""))
        id = self.backup_pb(master, backup_type='full', options=["--stream"])

        node1 = self.make_simple_node(base_dir="tmp_dirs/false_positive/{0}/node1".format(fname))
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
        # self.assertEqual(res, six.b(""), 'Restored Node1 failed to archive segment {0} due to having the same archive command as Master'.format(res.rstrip()))
        if res == six.b(""):
            self.assertEqual(1, 0, 'Error is expected due to Master and Node1 having the common archive and archive_command')

        master.stop()
        node1.stop()

    def pgpro688(self):
        """
        make node with archiving, make backup,
        get Recovery Time, validate to Recovery Time
        Waiting PGPRO-688
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/false_positive/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        node.start()

        self.assertEqual(self.init_pb(node), six.b(""))
        id = self.backup_pb(node, backup_type='full')
        recovery_time = self.show_pb(node, id=id)['recovery-time']

        # Uncommenting this section will make this test True Positive
        #node.psql("postgres", "select pg_create_restore_point('123')")
        #node.psql("postgres", "select txid_current()")
        #node.psql("postgres", "select pg_switch_xlog()")
        ####

        try:
            self.validate_pb(node, options=["--time='{0}'".format(recovery_time)])
            self.assertEqual(1, 0, 'Error is expected because We should not be able safely validate "Recovery Time" without wal record with timestamp')
        except ProbackupException, e:
            self.assertTrue('WARNING: recovery can be done up to time {0}'.format(recovery_time) in e.message)

        node.stop()

    def pgpro702_688(self):
        """
        make node without archiving, make stream backup,
        get Recovery Time, validate to Recovery Time
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/false_positive/{0}".format(fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        node.start()

        self.assertEqual(self.init_pb(node), six.b(""))
        id = self.backup_pb(node, backup_type='full', options=["--stream"])
        recovery_time = self.show_pb(node, id=id)['recovery-time']

        self.assertIn(six.b("INFO: backup validation completed successfully on"),
            self.validate_pb(node, options=["--time='{0}'".format(recovery_time)]))

    def test_validate_wal_lost_segment(self):
        """Loose segment located between backups. ExpectedFailure. This is BUG """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/false_positive/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )

        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        self.backup_pb(node, backup_type='full')

        # make some wals
        node.pgbench_init(scale=2)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        # delete last wal segment
        wals_dir = os.path.join(self.backup_dir(node), "wal")
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        wals = map(int, wals)
        os.remove(os.path.join(self.backup_dir(node), "wal", '0000000' + str(max(wals))))


        ##### Hole Smokes, Batman! We just lost a wal segment and know nothing about it
        ##### We need archive-push ASAP
        self.backup_pb(node, backup_type='full')
        self.assertTrue('validation completed successfully' in self.validate_pb(node))
        ########
        node.stop()
