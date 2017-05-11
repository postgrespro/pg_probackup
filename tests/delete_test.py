import unittest
from os import path
import six
from .ptrack_helpers import ProbackupTest, ProbackupException
from testgres import stop_all
import subprocess


class DeleteTest(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(DeleteTest, self).__init__(*args, **kwargs)

#    @classmethod
#    def tearDownClass(cls):
#        stop_all()
#    @unittest.skip("123")
    def test_delete_full_backups(self):
        """delete full backups"""
        fname = self.id().split('.')[3]
        print '{0} started'.format(fname)
        node = self.make_simple_node(base_dir="tmp_dirs/delete/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        node.pgbench_init()

        # full backup mode
        with open(path.join(node.logs_dir, "backup_1.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, options=["--verbose"]))

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        with open(path.join(node.logs_dir, "backup_2.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, options=["--verbose"]))

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        with open(path.join(node.logs_dir, "backup_3.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, options=["--verbose"]))

        show_backups = self.show_pb(node)
        id_1 = show_backups[0]['ID']
        id_3 = show_backups[2]['ID']
        self.delete_pb(node, show_backups[1]['ID'])
        show_backups = self.show_pb(node)
        self.assertEqual(show_backups[0]['ID'], id_1)
        self.assertEqual(show_backups[1]['ID'], id_3)

        node.stop()

#    @unittest.skip("123")
    def test_delete_increment(self):
        """delete increment and all after him"""
        fname = self.id().split('.')[3]
        print '{0} started'.format(fname)
        node = self.make_simple_node(base_dir="tmp_dirs/delete/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))

        # full backup mode
        with open(path.join(node.logs_dir, "backup_1.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, options=["--verbose"]))

        # page backup mode
        with open(path.join(node.logs_dir, "backup_2.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="page", options=["--verbose"]))

        # page backup mode
        with open(path.join(node.logs_dir, "backup_3.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="page", options=["--verbose"]))

        # full backup mode
        self.backup_pb(node)

        show_backups = self.show_pb(node)

        self.assertEqual(len(show_backups), 4)

        # delete first page backup
        self.delete_pb(node, show_backups[1]['ID'])

        show_backups = self.show_pb(node)
        self.assertEqual(len(show_backups), 2)

        self.assertEqual(show_backups[0]['Mode'], six.b("FULL"))
        self.assertEqual(show_backups[0]['Status'], six.b("OK"))
        self.assertEqual(show_backups[1]['Mode'], six.b("FULL"))
        self.assertEqual(show_backups[1]['Status'], six.b("OK"))

        node.stop()
