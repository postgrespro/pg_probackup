import unittest
import os
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
import subprocess
from sys import exit


module_name = 'delete'


class DeleteTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_delete_full_backups(self):
        """delete full backups"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # full backup
        self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(backup_dir, 'node', node)

        show_backups = self.show_pb(backup_dir, 'node')
        id_1 = show_backups[0]['id']
        id_2 = show_backups[1]['id']
        id_3 = show_backups[2]['id']
        self.delete_pb(backup_dir, 'node', id_2)
        show_backups = self.show_pb(backup_dir, 'node')
        self.assertEqual(show_backups[0]['id'], id_1)
        self.assertEqual(show_backups[1]['id'], id_3)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_delete_increment_page(self):
        """delete increment and all after him"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
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
        self.delete_pb(backup_dir, 'node', show_backups[1]['id'])

        show_backups = self.show_pb(backup_dir, 'node')
        self.assertEqual(len(show_backups), 2)

        self.assertEqual(show_backups[0]['backup-mode'], "FULL")
        self.assertEqual(show_backups[0]['status'], "OK")
        self.assertEqual(show_backups[1]['backup-mode'], "FULL")
        self.assertEqual(show_backups[1]['status'], "OK")

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_delete_increment_ptrack(self):
        """delete increment and all after him"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
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
        self.delete_pb(backup_dir, 'node', show_backups[1]['id'])

        show_backups = self.show_pb(backup_dir, 'node')
        self.assertEqual(len(show_backups), 2)

        self.assertEqual(show_backups[0]['backup-mode'], "FULL")
        self.assertEqual(show_backups[0]['status'], "OK")
        self.assertEqual(show_backups[1]['backup-mode'], "FULL")
        self.assertEqual(show_backups[1]['status'], "OK")

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_delete_orphaned_wal_segments(self):
        """make archive node, make three full backups, delete second backup without --wal option, then delete orphaned wals via --wal option"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,10000) i")
        # first full backup
        backup_1_id = self.backup_node(backup_dir, 'node', node)
        # second full backup
        backup_2_id = self.backup_node(backup_dir, 'node', node)
        # third full backup
        backup_3_id = self.backup_node(backup_dir, 'node', node)
        node.stop()

        # Check wals
        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        original_wal_quantity = len(wals)

        # delete second full backup
        self.delete_pb(backup_dir, 'node', backup_2_id)
        # check wal quantity
        self.validate_pb(backup_dir)
        self.assertEqual(self.show_pb(backup_dir, 'node', backup_1_id)['status'], "OK")
        self.assertEqual(self.show_pb(backup_dir, 'node', backup_3_id)['status'], "OK")
        # try to delete wals for second backup
        self.delete_pb(backup_dir, 'node', options=['--wal'])
        # check wal quantity
        self.validate_pb(backup_dir)
        self.assertEqual(self.show_pb(backup_dir, 'node', backup_1_id)['status'], "OK")
        self.assertEqual(self.show_pb(backup_dir, 'node', backup_3_id)['status'], "OK")

        # delete first full backup
        self.delete_pb(backup_dir, 'node', backup_1_id)
        self.validate_pb(backup_dir)
        self.assertEqual(self.show_pb(backup_dir, 'node', backup_3_id)['status'], "OK")

        result = self.delete_pb(backup_dir, 'node', options=['--wal'])
        # delete useless wals
        self.assertTrue('INFO: removed min WAL segment' in result
            and 'INFO: removed max WAL segment' in result)
        self.validate_pb(backup_dir)
        self.assertEqual(self.show_pb(backup_dir, 'node', backup_3_id)['status'], "OK")

        # Check quantity, it should be lower than original
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        self.assertTrue(original_wal_quantity > len(wals), "Number of wals not changed after 'delete --wal' which is illegal")

        # Delete last backup
        self.delete_pb(backup_dir, 'node', backup_3_id, options=['--wal'])
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        self.assertEqual (0, len(wals), "Number of wals should be equal to 0")

        # Clean after yourself
        self.del_test_dir(module_name, fname)
