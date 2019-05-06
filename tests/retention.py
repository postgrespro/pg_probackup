import os
import unittest
from datetime import datetime, timedelta
from .helpers.ptrack_helpers import ProbackupTest


module_name = 'retention'


class RetentionTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_retention_redundancy_1(self):
        """purge backups using redundancy-based retention policy"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        with open(os.path.join(
                backup_dir, 'backups', 'node',
                "pg_probackup.conf"), "a") as conf:
            conf.write("retention-redundancy = 1\n")

        # Make backups to be purged
        self.backup_node(backup_dir, 'node', node)
        self.backup_node(backup_dir, 'node', node, backup_type="page")
        # Make backups to be keeped
        self.backup_node(backup_dir, 'node', node)
        self.backup_node(backup_dir, 'node', node, backup_type="page")

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 4)

        # Purge backups
        log = self.delete_expired(
            backup_dir, 'node', options=['--expired', '--wal'])
        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 2)

        # Check that WAL segments were deleted
        min_wal = None
        max_wal = None
        for line in log.splitlines():
            if line.startswith("INFO: removed min WAL segment"):
                min_wal = line[31:-1]
            elif line.startswith("INFO: removed max WAL segment"):
                max_wal = line[31:-1]

        if not min_wal:
            self.assertTrue(False, "min_wal is empty")

        if not max_wal:
            self.assertTrue(False, "max_wal is not set")

        for wal_name in os.listdir(os.path.join(backup_dir, 'wal', 'node')):
            if not wal_name.endswith(".backup"):
                # wal_name_b = wal_name.encode('ascii')
                self.assertEqual(wal_name[8:] > min_wal[8:], True)
                self.assertEqual(wal_name[8:] > max_wal[8:], True)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_retention_window_2(self):
        """purge backups using window-based retention policy"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        with open(
            os.path.join(
                backup_dir,
                'backups',
                'node',
                "pg_probackup.conf"), "a") as conf:
            conf.write("retention-redundancy = 1\n")
            conf.write("retention-window = 1\n")

        # Make backups to be purged
        self.backup_node(backup_dir, 'node', node)
        self.backup_node(backup_dir, 'node', node, backup_type="page")
        # Make backup to be keeped
        self.backup_node(backup_dir, 'node', node)

        backups = os.path.join(backup_dir, 'backups', 'node')
        days_delta = 5
        for backup in os.listdir(backups):
            if backup == 'pg_probackup.conf':
                continue
            with open(
                    os.path.join(
                        backups, backup, "backup.control"), "a") as conf:
                conf.write("recovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=days_delta)))
                days_delta -= 1

        # Make backup to be keeped
        self.backup_node(backup_dir, 'node', node, backup_type="page")

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 4)

        # Purge backups
        self.delete_expired(backup_dir, 'node', options=['--expired'])
        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 2)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_retention_window_3(self):
        """purge all backups using window-based retention policy"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # take FULL BACKUP
        backup_id_1 = self.backup_node(backup_dir, 'node', node)

        # Take second FULL BACKUP
        backup_id_2 = self.backup_node(backup_dir, 'node', node)

        # Take third FULL BACKUP
        backup_id_3 = self.backup_node(backup_dir, 'node', node)

        backups = os.path.join(backup_dir, 'backups', 'node')
        for backup in os.listdir(backups):
            if backup == 'pg_probackup.conf':
                continue
            with open(
                    os.path.join(
                        backups, backup, "backup.control"), "a") as conf:
                conf.write("recovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=3)))

        # Purge backups
        self.delete_expired(
            backup_dir, 'node', options=['--retention-window=1', '--expired'])

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 0)

        print(self.show_pb(
            backup_dir, 'node', as_json=False, as_text=True))

        # count wal files in ARCHIVE

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_retention_window_4(self):
        """purge all backups using window-based retention policy"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # take FULL BACKUPs
        backup_id_1 = self.backup_node(backup_dir, 'node', node)

        backup_id_2 = self.backup_node(backup_dir, 'node', node)

        backup_id_3 = self.backup_node(backup_dir, 'node', node)

        backups = os.path.join(backup_dir, 'backups', 'node')
        for backup in os.listdir(backups):
            if backup == 'pg_probackup.conf':
                continue
            with open(
                    os.path.join(
                        backups, backup, "backup.control"), "a") as conf:
                conf.write("recovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=3)))

        self.delete_pb(backup_dir, 'node', backup_id_2)
        self.delete_pb(backup_dir, 'node', backup_id_3)

        # Purge backups
        self.delete_expired(
            backup_dir, 'node',
            options=['--retention-window=1', '--expired', '--wal'])

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 0)

        print(self.show_pb(
            backup_dir, 'node', as_json=False, as_text=True))

        # count wal files in ARCHIVE
        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        # n_wals = len(os.listdir(wals_dir))

        # self.assertTrue(n_wals > 0)

        # self.delete_expired(
        #     backup_dir, 'node',
        #     options=['--retention-window=1', '--expired', '--wal'])

        # count again
        n_wals = len(os.listdir(wals_dir))
        self.assertTrue(n_wals == 0)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_window_expire_interleaved_incremental_chains(self):
        """complicated case of interleaved backup chains"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # take FULL BACKUPs
        backup_id_a = self.backup_node(backup_dir, 'node', node)
        backup_id_b = self.backup_node(backup_dir, 'node', node)

        # Change FULL B backup status to ERROR
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # FULLb  ERROR
        # FULLa  OK
        # Take PAGEa1 backup
        page_id_a1 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK
        # Change FULL B backup status to OK
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
        # Now we start to play with first generation of PAGE backups
        # Change PAGEb1 status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'ERROR')

        # Change PAGEa1 status to OK
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'OK')

        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK
        page_id_a2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGEa2 OK
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK
        # Change PAGEa2 status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'ERROR')

        # Change PAGEb1 status to OK
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'OK')

        # PAGEa2 ERROR
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK
        page_id_b2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Change PAGEa2 status to OK
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'OK')

        # PAGEb2 OK
        # PAGEa2 OK
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        # Purge backups
        backups = os.path.join(backup_dir, 'backups', 'node')
        for backup in os.listdir(backups):
            if backup in [page_id_a2, page_id_b2, 'pg_probackup.conf']:
                continue

            with open(
                    os.path.join(
                        backups, backup, "backup.control"), "a") as conf:
                conf.write("recovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=3)))

        self.delete_expired(
            backup_dir, 'node',
            options=['--retention-window=1', '--expired'])

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 6)

        print(self.show_pb(
            backup_dir, 'node', as_json=False, as_text=True))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_redundancy_expire_interleaved_incremental_chains(self):
        """complicated case of interleaved backup chains"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # take FULL BACKUPs
        backup_id_a = self.backup_node(backup_dir, 'node', node)
        backup_id_b = self.backup_node(backup_dir, 'node', node)

        # Change FULL B backup status to ERROR
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # FULLb  ERROR
        # FULLa  OK
        # Take PAGEa1 backup
        page_id_a1 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK
        # Change FULL B backup status to OK
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
        # Now we start to play with first generation of PAGE backups
        # Change PAGEb1 status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'ERROR')

        # Change PAGEa1 status to OK
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'OK')

        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK
        page_id_a2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGEa2 OK
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK
        # Change PAGEa2 status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'ERROR')

        # Change PAGEb1 status to OK
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'OK')

        # PAGEa2 ERROR
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK
        page_id_b2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Change PAGEa2 status to OK
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'OK')

        # PAGEb2 OK
        # PAGEa2 OK
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        self.delete_expired(
            backup_dir, 'node',
            options=['--retention-redundancy=1', '--expired'])

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 3)

        print(self.show_pb(
            backup_dir, 'node', as_json=False, as_text=True))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_window_merge_interleaved_incremental_chains(self):
        """complicated case of interleaved backup chains"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # Take FULL BACKUPs
        backup_id_a = self.backup_node(backup_dir, 'node', node)
        backup_id_b = self.backup_node(backup_dir, 'node', node)

        # Change FULL B backup status to ERROR
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # FULLb  ERROR
        # FULLa  OK
        # Take PAGEa1 backup
        page_id_a1 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK
        # Change FULL B backup status to OK
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
        # Now we start to play with first generation of PAGE backups
        # Change PAGEb1 status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'ERROR')

        # Change PAGEa1 status to OK
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'OK')

        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK
        page_id_a2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGEa2 OK
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK
        # Change PAGEa2 status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'ERROR')

        # Change PAGEb1 status to OK
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'OK')

        # PAGEa2 ERROR
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK
        page_id_b2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Change PAGEa2 status to OK
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'OK')

        # PAGEb2 OK
        # PAGEa2 OK
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        # Purge backups
        backups = os.path.join(backup_dir, 'backups', 'node')
        for backup in os.listdir(backups):
            if backup in [page_id_a2, page_id_b2, 'pg_probackup.conf']:
                continue

            with open(
                    os.path.join(
                        backups, backup, "backup.control"), "a") as conf:
                conf.write("recovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=3)))

        output = self.delete_expired(
            backup_dir, 'node',
            options=['--retention-window=1', '--expired', '--merge-expired'])

        self.assertIn(
            "Merge incremental chain between FULL backup {0} and backup {1}".format(
                backup_id_a, page_id_a2),
            output)

        self.assertIn(
            "Merging backup {0} with backup {1}".format(
                page_id_a1, backup_id_a), output)

        self.assertIn(
            "Rename {0} to {1}".format(
                backup_id_a, page_id_a1), output)

        self.assertIn(
            "Merging backup {0} with backup {1}".format(
                page_id_a2, page_id_a1), output)

        self.assertIn(
            "Rename {0} to {1}".format(
                page_id_a1, page_id_a2), output)

        self.assertIn(
            "Merge incremental chain between FULL backup {0} and backup {1}".format(
                backup_id_b, page_id_b2),
            output)

        self.assertIn(
            "Merging backup {0} with backup {1}".format(
                page_id_b1, backup_id_b), output)

        self.assertIn(
            "Rename {0} to {1}".format(
                backup_id_b, page_id_b1), output)

        self.assertIn(
            "Merging backup {0} with backup {1}".format(
                page_id_b2, page_id_b1), output)

        self.assertIn(
            "Rename {0} to {1}".format(
                page_id_b1, page_id_b2), output)

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 2)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_window_merge_interleaved_incremental_chains_1(self):
        """
            PAGEb3
            PAGEb2
            PAGEb1
            PAGEa1
            FULLb
            FULLa
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum':'off'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        # Take FULL BACKUPs
        backup_id_a = self.backup_node(backup_dir, 'node', node)
        pgbench = node.pgbench(options=['-t', '10', '-c', '2'])
        pgbench.wait()

        backup_id_b = self.backup_node(backup_dir, 'node', node)
        pgbench = node.pgbench(options=['-t', '10', '-c', '2'])
        pgbench.wait()

        # Change FULL B backup status to ERROR
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        page_id_a1 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        pgdata_a1 = self.pgdata_content(node.data_dir)

        pgbench = node.pgbench(options=['-t', '10', '-c', '2'])
        pgbench.wait()

        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK
        # Change FULL B backup status to OK
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        # Change PAGEa1 backup status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'ERROR')

        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  OK
        page_id_b1 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        pgbench = node.pgbench(options=['-t', '10', '-c', '2'])
        pgbench.wait()

        page_id_b2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        pgbench = node.pgbench(options=['-t', '10', '-c', '2'])
        pgbench.wait()

        page_id_b3 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        pgdata_b3 = self.pgdata_content(node.data_dir)

        pgbench = node.pgbench(options=['-t', '10', '-c', '2'])
        pgbench.wait()

        # PAGEb3 OK
        # PAGEb2 OK
        # PAGEb1 OK
        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  OK

        # Change PAGEa1 backup status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'OK')

        # PAGEb3 OK
        # PAGEb2 OK
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        # Purge backups
        backups = os.path.join(backup_dir, 'backups', 'node')
        for backup in os.listdir(backups):
            if backup in [page_id_a1, page_id_b3, 'pg_probackup.conf']:
                continue

            with open(
                    os.path.join(
                        backups, backup, "backup.control"), "a") as conf:
                conf.write("recovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=3)))

        output = self.delete_expired(
            backup_dir, 'node',
            options=['--retention-window=1', '--expired', '--merge-expired'])

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 2)

        self.assertEqual(
            self.show_pb(backup_dir, 'node')[1]['id'],
            page_id_b3)

        self.assertEqual(
            self.show_pb(backup_dir, 'node')[0]['id'],
            page_id_a1)

        self.assertEqual(
            self.show_pb(backup_dir, 'node')[1]['backup-mode'],
            'FULL')

        self.assertEqual(
            self.show_pb(backup_dir, 'node')[0]['backup-mode'],
            'FULL')

        node.cleanup()

        # Data correctness of PAGEa3
        self.restore_node(backup_dir, 'node', node, backup_id=page_id_a1)
        pgdata_restored_a1 = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata_a1, pgdata_restored_a1)

        node.cleanup()

        # Data correctness of PAGEb3
        self.restore_node(backup_dir, 'node', node, backup_id=page_id_b3)
        pgdata_restored_b3 = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata_b3, pgdata_restored_b3)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_window_merge_multiple_descendants(self):
        """
        PAGEb3
          |                 PAGEa3
        -----------------------------retention window
        PAGEb2               /
          |       PAGEa2    /        should be deleted
        PAGEb1       \     /
          |           PAGEa1
        FULLb           |
                      FULLa
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        # Take FULL BACKUPs
        backup_id_a = self.backup_node(backup_dir, 'node', node)
        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        backup_id_b = self.backup_node(backup_dir, 'node', node)
        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        # Change FULLb backup status to ERROR
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        page_id_a1 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        # Change FULLb backup status to OK
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

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        # Change PAGEa1 backup status to OK
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'OK')

        # Change PAGEb1 backup status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'ERROR')

        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        page_id_a2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        # PAGEa2 OK
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        # Change PAGEb1 backup status to OK
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'OK')

        # Change PAGEa2 backup status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'ERROR')

        # PAGEa2 ERROR
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        page_id_b2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        # PAGEb2 OK
        # PAGEa2 ERROR
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        # Change PAGEb2 and PAGEb1  status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_b2, 'ERROR')
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'ERROR')

        # PAGEb2 ERROR
        # PAGEa2 ERROR
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        page_id_a3 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')
        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        # PAGEa3 OK
        # PAGEb2 ERROR
        # PAGEa2 ERROR
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        # Change PAGEa3 status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a3, 'ERROR')

        # Change PAGEb2 status to OK
        self.change_backup_status(backup_dir, 'node', page_id_b2, 'OK')

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

        # Change PAGEa3, PAGEa2 and PAGEb1 status to OK
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

        print("Backups {0} and {1} are children of {2}".format(
            page_id_a3, page_id_a2, page_id_a1))

        # Purge backups
        backups = os.path.join(backup_dir, 'backups', 'node')
        for backup in os.listdir(backups):
            if backup in [page_id_a3, page_id_b3, 'pg_probackup.conf']:
                continue

            with open(
                    os.path.join(
                        backups, backup, "backup.control"), "a") as conf:
                conf.write("recovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=3)))

        output = self.delete_expired(
            backup_dir, 'node',
            options=[
                '--retention-window=1', '--expired',
                '--merge-expired', '--log-level-console=log'])

        print(output)

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 3)

        # Merging chain A
        self.assertIn(
            "Merge incremental chain between FULL backup {0} and backup {1}".format(
                backup_id_a, page_id_a3),
            output)

        self.assertIn(
            "Merging backup {0} with backup {1}".format(
                page_id_a1, backup_id_a), output)

        self.assertIn(
            "INFO: Rename {0} to {1}".format(
                backup_id_a, page_id_a1), output)

        self.assertIn(
            "WARNING: Backup {0} has multiple valid descendants. "
            "Automatic merge is not possible.".format(
                page_id_a1), output)

        # Merge chain B
        self.assertIn(
            "Merge incremental chain between FULL backup {0} and backup {1}".format(
                backup_id_b, page_id_b3),
            output)

        self.assertIn(
            "Merging backup {0} with backup {1}".format(
                page_id_b1, backup_id_b), output)

        self.assertIn(
            "INFO: Rename {0} to {1}".format(
                backup_id_b, page_id_b1), output)

        self.assertIn(
            "Merging backup {0} with backup {1}".format(
                page_id_b2, page_id_b1), output)

        self.assertIn(
            "INFO: Rename {0} to {1}".format(
                page_id_b1, page_id_b2), output)

        self.assertIn(
            "Merging backup {0} with backup {1}".format(
                page_id_b3, page_id_b2), output)

        self.assertIn(
            "INFO: Rename {0} to {1}".format(
                page_id_b2, page_id_b3), output)

        # this backup deleted because it is not guarded by retention
        self.assertIn(
            "INFO: Delete: {0}".format(
                page_id_a1), output)

        self.assertEqual(
            self.show_pb(backup_dir, 'node')[2]['id'],
            page_id_b3)

        self.assertEqual(
            self.show_pb(backup_dir, 'node')[1]['id'],
            page_id_a3)

        self.assertEqual(
            self.show_pb(backup_dir, 'node')[0]['id'],
            page_id_a1)

        self.assertEqual(
            self.show_pb(backup_dir, 'node')[2]['backup-mode'],
            'FULL')

        self.assertEqual(
            self.show_pb(backup_dir, 'node')[1]['backup-mode'],
            'PAGE')

        self.assertEqual(
            self.show_pb(backup_dir, 'node')[0]['backup-mode'],
            'FULL')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_window_chains(self):
        """
        PAGE
        -------window
        PAGE
        PAGE
        FULL
        PAGE
        PAGE
        FULL
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        # Chain A
        backup_id_a = self.backup_node(backup_dir, 'node', node)
        page_id_a1 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        page_id_a2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Chain B
        backup_id_b = self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        page_id_b1 = self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        page_id_b2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        page_id_b3 = self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        pgdata = self.pgdata_content(node.data_dir)

        # Purge backups
        backups = os.path.join(backup_dir, 'backups', 'node')
        for backup in os.listdir(backups):
            if backup in [page_id_b3, 'pg_probackup.conf']:
                continue

            with open(
                    os.path.join(
                        backups, backup, "backup.control"), "a") as conf:
                conf.write("recovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=3)))

        output = self.delete_expired(
            backup_dir, 'node',
            options=[
                '--retention-window=1', '--expired',
                '--merge-expired', '--log-level-console=log'])

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 1)

        node.cleanup()

        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_window_chains_1(self):
        """
        PAGE
        -------window
        PAGE
        PAGE
        FULL
        PAGE
        PAGE
        FULL
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        # Chain A
        backup_id_a = self.backup_node(backup_dir, 'node', node)
        page_id_a1 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        page_id_a2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Chain B
        backup_id_b = self.backup_node(backup_dir, 'node', node)

        page_id_b1 = self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        page_id_b2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        page_id_b3 = self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        pgdata = self.pgdata_content(node.data_dir)

        # Purge backups
        backups = os.path.join(backup_dir, 'backups', 'node')
        for backup in os.listdir(backups):
            if backup in [page_id_b3, 'pg_probackup.conf']:
                continue

            with open(
                    os.path.join(
                        backups, backup, "backup.control"), "a") as conf:
                conf.write("recovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=3)))

        output = self.delete_expired(
            backup_dir, 'node',
            options=[
                '--retention-window=1',
                '--merge-expired', '--log-level-console=log'])

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 4)

        self.assertIn(
            "There are no backups to delete by retention policy",
            output)

        self.assertIn(
            "Retention merging finished",
            output)

        output = self.delete_expired(
            backup_dir, 'node',
            options=[
                '--retention-window=1',
                '--expired', '--log-level-console=log'])

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 1)

        self.assertIn(
            "There are no backups to merge by retention policy",
            output)

        self.assertIn(
            "Purging finished",
            output)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_window_error_backups(self):
        """
        PAGE ERROR
        -------window
        PAGE ERROR
        PAGE ERROR
        PAGE ERROR
        FULL ERROR
        FULL
        -------redundancy
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        # Take FULL BACKUPs
        backup_id_a1 = self.backup_node(backup_dir, 'node', node)
        page_id_a2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Change FULLb backup status to ERROR
        # self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')
