import unittest
import os
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
import subprocess


class DeleteTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_delete_full_backups(self):
        """delete full backups"""
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

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

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_del_instance_archive(self):
        """delete full backups"""
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # full backup
        self.backup_node(backup_dir, 'node', node)

        # full backup
        self.backup_node(backup_dir, 'node', node)

        # restore
        node.cleanup()
        self.restore_node(backup_dir, 'node', node)
        node.slow_start()

        # Delete instance
        self.del_instance(backup_dir, 'node')

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_delete_archive_mix_compress_and_non_compressed_segments(self):
        """delete full backups"""
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(self.module_name, self.fname),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(
            backup_dir, 'node', node, compress=False)
        node.slow_start()

        # full backup
        self.backup_node(backup_dir, 'node', node)

        node.pgbench_init(scale=10)

        # Restart archiving with compression
        self.set_archiving(backup_dir, 'node', node, compress=True)

        node.restart()

        # full backup
        self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        self.backup_node(
            backup_dir, 'node', node,
            options=[
                '--retention-redundancy=3',
                '--delete-expired'])

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        self.backup_node(
            backup_dir, 'node', node,
            options=[
                '--retention-redundancy=3',
                '--delete-expired'])

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        self.backup_node(
            backup_dir, 'node', node,
            options=[
                '--retention-redundancy=3',
                '--delete-expired'])

    # @unittest.skip("skip")
    def test_delete_increment_page(self):
        """delete increment and all after him"""
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

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

    # @unittest.skip("skip")
    def test_delete_increment_ptrack(self):
        """delete increment and all after him"""
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            ptrack_enable=self.ptrack,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            'postgres',
            'CREATE EXTENSION ptrack')

        # full backup mode
        self.backup_node(backup_dir, 'node', node)
        # ptrack backup mode
        self.backup_node(backup_dir, 'node', node, backup_type="ptrack")
        # ptrack backup mode
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

    # @unittest.skip("skip")
    def test_delete_orphaned_wal_segments(self):
        """
        make archive node, make three full backups,
        delete second backup without --wal option,
        then delete orphaned wals via --wal option
        """
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

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
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f))]
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
        self.assertTrue('On timeline 1 WAL segments between ' in result
            and 'will be removed' in result)

        self.validate_pb(backup_dir)
        self.assertEqual(self.show_pb(backup_dir, 'node', backup_3_id)['status'], "OK")

        # Check quantity, it should be lower than original
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f))]
        self.assertTrue(original_wal_quantity > len(wals), "Number of wals not changed after 'delete --wal' which is illegal")

        # Delete last backup
        self.delete_pb(backup_dir, 'node', backup_3_id, options=['--wal'])
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f))]
        self.assertEqual (0, len(wals), "Number of wals should be equal to 0")

    # @unittest.skip("skip")
    def test_delete_wal_between_multiple_timelines(self):
        """
                    /-------B1--
        A1----------------A2----

        delete A1 backup, check that WAL segments on [A1, A2) and
        [A1, B1) are deleted and backups B1 and A2 keep
        their WAL
        """
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        A1 = self.backup_node(backup_dir, 'node', node)

        # load some data to node
        node.pgbench_init(scale=3)

        node2 = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node2'))
        node2.cleanup()

        self.restore_node(backup_dir, 'node', node2)
        self.set_auto_conf(node2, {'port': node2.port})
        node2.slow_start()

        # load some more data to node
        node.pgbench_init(scale=3)

        # take A2
        A2 = self.backup_node(backup_dir, 'node', node)

        # load some more data to node2
        node2.pgbench_init(scale=2)

        B1 = self.backup_node(
            backup_dir, 'node',
            node2, data_dir=node2.data_dir)

        self.delete_pb(backup_dir, 'node', backup_id=A1, options=['--wal'])

        self.validate_pb(backup_dir)

    # @unittest.skip("skip")
    def test_delete_backup_with_empty_control_file(self):
        """
        take backup, truncate its control file,
        try to delete it via 'delete' command
        """
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'],
            set_replication=True)

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        # full backup mode
        self.backup_node(
            backup_dir, 'node', node, options=['--stream'])
        # page backup mode
        self.backup_node(
            backup_dir, 'node', node, backup_type="delta", options=['--stream'])
        # page backup mode
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type="delta", options=['--stream'])

        with open(
            os.path.join(backup_dir, 'backups', 'node', backup_id, 'backup.control'),
            'wt') as f:
                f.flush()
                f.close()

        show_backups = self.show_pb(backup_dir, 'node')
        self.assertEqual(len(show_backups), 3)

        self.delete_pb(backup_dir, 'node', backup_id=backup_id)

    # @unittest.skip("skip")
    def test_delete_interleaved_incremental_chains(self):
        """complicated case of interleaved backup chains"""
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # Take FULL BACKUPs
        backup_id_a = self.backup_node(backup_dir, 'node', node)
        backup_id_b = self.backup_node(backup_dir, 'node', node)

        # Change FULLb to ERROR
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # FULLb  ERROR
        # FULLa  OK

        # Take PAGEa1 backup
        page_id_a1 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        # Change FULLb to OK
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        # Change PAGEa1 to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'ERROR')

        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  OK

        page_id_b1 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGEb1 OK
        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  OK

        # Now we start to play with first generation of PAGE backups
        # Change PAGEb1 and FULLb status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # Change PAGEa1 status to OK
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'OK')

        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        page_id_a2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGEa2 OK
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        # Change PAGEa2 and FULla to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'ERROR')

        # Change PAGEb1 and FULlb to OK
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        # PAGEa2 ERROR
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  ERROR

        page_id_b2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Change PAGEa2 and FULLa status to OK
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        # PAGEb2 OK
        # PAGEa2 OK
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        self.backup_node(backup_dir, 'node', node)
        self.backup_node(backup_dir, 'node', node, backup_type='page')

        # PAGEc1 OK
        # FULLc  OK
        # PAGEb2 OK
        # PAGEa2 OK
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        # Delete FULLb
        self.delete_pb(
            backup_dir, 'node', backup_id_b)

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 5)

        print(self.show_pb(
            backup_dir, 'node', as_json=False, as_text=True))

    # @unittest.skip("skip")
    def test_delete_multiple_descendants(self):
        r"""
        PAGEb3
          |                 PAGEa3
        PAGEb2               /
          |       PAGEa2    /
        PAGEb1       \     /
          |           PAGEa1
        FULLb           |
                      FULLa  should be deleted
        """
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # Take FULL BACKUPs
        backup_id_a = self.backup_node(backup_dir, 'node', node)
        backup_id_b = self.backup_node(backup_dir, 'node', node)

        # Change FULLb to ERROR
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        page_id_a1 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Change FULLb to OK
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        # Change PAGEa1 backup status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'ERROR')

        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  OK

        page_id_b1 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGEb1 OK
        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  OK

        # Change PAGEa1 to OK
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'OK')

        # Change PAGEb1 and FULLb backup status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        page_id_a2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGEa2 OK
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        # Change PAGEb1 and FULLb to OK
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        # Change PAGEa2 and FULLa to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'ERROR')

        # PAGEa2 ERROR
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  ERROR

        page_id_b2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGEb2 OK
        # PAGEa2 ERROR
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  ERROR

        # Change PAGEb2, PAGEb1 and FULLb to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_b2, 'ERROR')
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # Change FULLa to OK
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'OK')

        # PAGEb2 ERROR
        # PAGEa2 ERROR
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        page_id_a3 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGEa3 OK
        # PAGEb2 ERROR
        # PAGEa2 ERROR
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        # Change PAGEa3 status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a3, 'ERROR')

        # Change PAGEb2 and FULLb to OK
        self.change_backup_status(backup_dir, 'node', page_id_b2, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        page_id_b3 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGEb3 OK
        # PAGEa3 ERROR
        # PAGEb2 OK
        # PAGEa2 ERROR
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        # Change PAGEa3, PAGEa2 and PAGEb1 to OK
        self.change_backup_status(backup_dir, 'node', page_id_a3, 'OK')
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'OK')
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'OK')

        # PAGEb3 OK
        # PAGEa3 OK
        # PAGEb2 OK
        # PAGEa2 OK
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        # Check that page_id_a3 and page_id_a2 are both direct descendants of page_id_a1
        self.assertEqual(
            self.show_pb(backup_dir, 'node', backup_id=page_id_a3)['parent-backup-id'],
            page_id_a1)

        self.assertEqual(
            self.show_pb(backup_dir, 'node', backup_id=page_id_a2)['parent-backup-id'],
            page_id_a1)

        # Delete FULLa
        self.delete_pb(backup_dir, 'node', backup_id_a)

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 4)

    # @unittest.skip("skip")
    def test_delete_multiple_descendants_dry_run(self):
        r"""
                 PAGEa3
        PAGEa2    /
           \     /
            PAGEa1 (delete target)
              |
            FULLa
        """
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # Take FULL BACKUP
        node.pgbench_init(scale=1)
        backup_id_a = self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()
        page_id_a1 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()
        page_id_a2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')


        # Change PAGEa2 to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'ERROR')

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()
        page_id_a3 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Change PAGEa2 to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'OK')

        # Delete PAGEa1
        output = self.delete_pb(
            backup_dir, 'node', page_id_a1,
            options=['--dry-run', '--log-level-console=LOG', '--delete-wal'])

        print(output)
        self.assertIn(
            'LOG: Backup {0} can be deleted'.format(page_id_a3),
            output)
        self.assertIn(
            'LOG: Backup {0} can be deleted'.format(page_id_a2),
            output)
        self.assertIn(
            'LOG: Backup {0} can be deleted'.format(page_id_a1),
            output)

        self.assertIn(
            'INFO: Resident data size to free by '
            'delete of backup {0} :'.format(page_id_a1),
            output)

        self.assertIn(
            'On timeline 1 WAL segments between 000000010000000000000001 '
            'and 000000010000000000000003 can be removed',
            output)

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 4)

        output = self.delete_pb(
            backup_dir, 'node', page_id_a1,
            options=['--log-level-console=LOG', '--delete-wal'])

        self.assertIn(
            'LOG: Backup {0} will be deleted'.format(page_id_a3),
            output)
        self.assertIn(
            'LOG: Backup {0} will be deleted'.format(page_id_a2),
            output)
        self.assertIn(
            'LOG: Backup {0} will be deleted'.format(page_id_a1),
            output)
        self.assertIn(
            'INFO: Resident data size to free by '
            'delete of backup {0} :'.format(page_id_a1),
            output)

        self.assertIn(
            'On timeline 1 WAL segments between 000000010000000000000001 '
            'and 000000010000000000000003 will be removed',
            output)

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 1)

        self.validate_pb(backup_dir, 'node')

    def test_delete_error_backups(self):
        """delete increment and all after him"""
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # full backup mode
        self.backup_node(backup_dir, 'node', node)
        # page backup mode
        self.backup_node(backup_dir, 'node', node, backup_type="page")

        # Take FULL BACKUP
        backup_id_a = self.backup_node(backup_dir, 'node', node)
        # Take PAGE BACKUP
        backup_id_b = self.backup_node(backup_dir, 'node', node, backup_type="page")

        backup_id_c = self.backup_node(backup_dir, 'node', node, backup_type="page")

        backup_id_d = self.backup_node(backup_dir, 'node', node, backup_type="page")

        # full backup mode
        self.backup_node(backup_dir, 'node', node)
        self.backup_node(backup_dir, 'node', node, backup_type="page")
        backup_id_e = self.backup_node(backup_dir, 'node', node, backup_type="page")
        self.backup_node(backup_dir, 'node', node, backup_type="page")

        # Change status to ERROR
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_c, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_e, 'ERROR')

        print(self.show_pb(backup_dir, as_text=True, as_json=False))

        show_backups = self.show_pb(backup_dir, 'node')
        self.assertEqual(len(show_backups), 10)

        # delete error backups
        output = self.delete_pb(backup_dir, 'node', options=['--status=ERROR', '--dry-run'])
        show_backups = self.show_pb(backup_dir, 'node')
        self.assertEqual(len(show_backups), 10)

        self.assertIn(
            "Deleting all backups with status 'ERROR' in dry run mode",
            output)

        self.assertIn(
            "INFO: Backup {0} with status OK can be deleted".format(backup_id_d),
            output)

        print(self.show_pb(backup_dir, as_text=True, as_json=False))

        show_backups = self.show_pb(backup_dir, 'node')
        output = self.delete_pb(backup_dir, 'node', options=['--status=ERROR'])
        print(output)
        show_backups = self.show_pb(backup_dir, 'node')
        self.assertEqual(len(show_backups), 4)

        self.assertEqual(show_backups[0]['status'], "OK")
        self.assertEqual(show_backups[1]['status'], "OK")
        self.assertEqual(show_backups[2]['status'], "OK")
        self.assertEqual(show_backups[3]['status'], "OK")
