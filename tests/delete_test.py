import unittest
import os
import six
from helpers.ptrack_helpers import ProbackupTest, ProbackupException
from testgres import stop_all
import subprocess


class DeleteTest(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(DeleteTest, self).__init__(*args, **kwargs)
        self.module_name = 'delete'

    @classmethod
    def tearDownClass(cls):
        stop_all()

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_delete_full_backups(self):
        """delete full backups"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # full backup
        self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(backup_dir, 'node', node)

        show_backups = self.show_pb(backup_dir, 'node')
        id_1 = show_backups[0]['ID']
        id_2 = show_backups[1]['ID']
        id_3 = show_backups[2]['ID']
        self.delete_pb(backup_dir, 'node', id_2)
        show_backups = self.show_pb(backup_dir, 'node')
        self.assertEqual(show_backups[0]['ID'], id_1)
        self.assertEqual(show_backups[1]['ID'], id_3)

        node.stop()

    def test_delete_increment_page(self):
        """delete increment and all after him"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # full backup mode
        self.backup_node(backup_dir, 'node', node)
        # page backup mode
        self.backup_node(backup_dir, 'node', node, backup_type="page")
        # page backup mode
        self.backup_node(backup_dir, 'node', node, backup_type="page")
        # full backup mode
        self.backup_node(backup_dir, 'node', node)

        show_backups = self.show_pb(backup_dir, 'node')
        self.assertEqual(len(show_backups), 4)

        # delete first page backup
        self.delete_pb(backup_dir, 'node', show_backups[1]['ID'])

        show_backups = self.show_pb(backup_dir, 'node')
        self.assertEqual(len(show_backups), 2)

        self.assertEqual(show_backups[0]['Mode'], six.b("FULL"))
        self.assertEqual(show_backups[0]['Status'], six.b("OK"))
        self.assertEqual(show_backups[1]['Mode'], six.b("FULL"))
        self.assertEqual(show_backups[1]['Status'], six.b("OK"))

        node.stop()

    def test_delete_increment_ptrack(self):
        """delete increment and all after him"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on'}
            )
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # full backup mode
        self.backup_node(backup_dir, 'node', node)
        # page backup mode
        self.backup_node(backup_dir, 'node', node, backup_type="ptrack")
        # page backup mode
        self.backup_node(backup_dir, 'node', node, backup_type="ptrack")
        # full backup mode
        self.backup_node(backup_dir, 'node', node)

        show_backups = self.show_pb(backup_dir, 'node')
        self.assertEqual(len(show_backups), 4)

        # delete first page backup
        self.delete_pb(backup_dir, 'node', show_backups[1]['ID'])

        show_backups = self.show_pb(backup_dir, 'node')
        self.assertEqual(len(show_backups), 2)

        self.assertEqual(show_backups[0]['Mode'], six.b("FULL"))
        self.assertEqual(show_backups[0]['Status'], six.b("OK"))
        self.assertEqual(show_backups[1]['Mode'], six.b("FULL"))
        self.assertEqual(show_backups[1]['Status'], six.b("OK"))

        node.stop()
