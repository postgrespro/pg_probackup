import unittest
from datetime import datetime, timedelta
from .helpers.ptrack_helpers import ProbackupTest
from .helpers.ptrack_helpers import fs_backup_class
from pg_probackup2.gdb import needs_gdb
from .helpers.data_helpers import tail_file
from time import sleep
import os.path


class RetentionTest(ProbackupTest):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_retention_redundancy_1(self):
        """purge backups using redundancy-based retention policy"""
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.set_config('node', options=['--retention-redundancy=1'])

        # Make backups to be purged
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type="page")
        # Make backups to be keeped
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type="page")

        self.assertEqual(len(self.pb.show('node')), 4)

        output_before = self.pb.show_archive('node', tli=1)

        # Purge backups
        self.pb.delete_expired('node', options=['--expired', '--wal'])
        self.assertEqual(len(self.pb.show('node')), 2)

        output_after = self.pb.show_archive('node', tli=1)

        self.assertEqual(
            output_before['max-segno'],
            output_after['max-segno'])

        self.assertNotEqual(
            output_before['min-segno'],
            output_after['min-segno'])

        # Check that WAL segments were deleted
        min_wal = output_after['min-segno']
        max_wal = output_after['max-segno']

        wals = self.get_instance_wal_list(backup_dir, 'node')
        for wal_name in wals:
            if self.archive_compress and wal_name.endswith(self.compress_suffix):
                wal_name = wal_name[:-len(self.compress_suffix)]
            self.assertGreaterEqual(wal_name, min_wal)
            self.assertLessEqual(wal_name, max_wal)

    # @unittest.skip("skip")
    def test_retention_window_2(self):
        """purge backups using window-based retention policy"""
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        with self.modify_backup_config(backup_dir, 'node') as cf:
            cf.data += "retention-redundancy = 1\n"
            cf.data += "retention-window = 1\n"

        # Make backups to be purged
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type="page")
        # Make backup to be keeped
        self.pb.backup_node('node', node)

        days_delta = 5
        for backup_id in backup_dir.list_instance_backups('node'):
            with self.modify_backup_control(backup_dir, 'node', backup_id) as cf:
                cf.data += "\nrecovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                            datetime.now() - timedelta(days=days_delta))
            days_delta -= 1

        # Make backup to be keeped
        self.pb.backup_node('node', node, backup_type="page")

        self.assertEqual(len(self.pb.show('node')), 4)

        # Purge backups
        self.pb.delete_expired('node', options=['--expired'])
        self.assertEqual(len(self.pb.show('node')), 2)

    # @unittest.skip("skip")
    def test_retention_window_3(self):
        """purge all backups using window-based retention policy"""
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # take FULL BACKUP
        self.pb.backup_node('node', node)

        # Take second FULL BACKUP
        self.pb.backup_node('node', node)

        # Take third FULL BACKUP
        self.pb.backup_node('node', node)

        for backup in backup_dir.list_instance_backups('node'):
            with self.modify_backup_control(backup_dir, 'node', backup) as cf:
                cf.data += "\nrecovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                            datetime.now() - timedelta(days=3))

        # Purge backups
        self.pb.delete_expired('node', options=['--retention-window=1', '--expired'])

        self.assertEqual(len(self.pb.show('node')), 0)

        print(self.pb.show('node', as_json=False, as_text=True))

        # count wal files in ARCHIVE

    # @unittest.skip("skip")
    def test_retention_window_4(self):
        """purge all backups using window-based retention policy"""
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # take FULL BACKUPs
        self.pb.backup_node('node', node)

        backup_id_2 = self.pb.backup_node('node', node)

        backup_id_3 = self.pb.backup_node('node', node)

        for backup in backup_dir.list_instance_backups('node'):
            with self.modify_backup_control(backup_dir, 'node', backup) as cf:
                cf.data += "\nrecovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=3))

        self.pb.delete('node', backup_id_2)
        self.pb.delete('node', backup_id_3)

        # Purge backups
        self.pb.delete_expired(
            'node',
            options=['--retention-window=1', '--expired', '--wal'])

        self.assertEqual(len(self.pb.show('node')), 0)

        print(self.pb.show('node', as_json=False, as_text=True))

        # count wal files in ARCHIVE
        wals = self.get_instance_wal_list(backup_dir, 'node')
        self.assertFalse(wals)

    @unittest.skipIf(not fs_backup_class.is_file_based, "Locks are not implemented in cloud")
    @needs_gdb
    def test_concurrent_retention_1(self):
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        with self.modify_backup_config(backup_dir, 'node') as cf:
            cf.data += "retention-redundancy = 1\n"
            cf.data += "retention-window = 2\n"

        # Fill with data
        node.pgbench_init(scale=1)

        full_id = self.pb.backup_node('node', node, backup_type="full")

        pgbench = node.pgbench(options=['-t', '20', '-c', '2'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '20', '-c', '2'])
        pgbench.wait()

        prev_id = self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '20', '-c', '2'])
        pgbench.wait()

        last_id = self.pb.backup_node('node', node, backup_type="page")

        days_delta = 4
        for backup_id in backup_dir.list_instance_backups('node'):
            with self.modify_backup_control(backup_dir, 'node', backup_id) as cf:
                cf.data += "\nrecovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=days_delta))
            days_delta -= 1

        gdb = self.pb.backup_node('node', node, gdb=True,
                               options=['--merge-expired'])
        gdb.set_breakpoint("merge_chain")
        gdb.run_until_break()

        self.pb.backup_node('node', node,
                         options=['--merge-expired'],
                         expect_error="because of concurrent merge")
        self.assertMessage(contains=f"ERROR: Cannot lock backup {full_id}")

    @needs_gdb
    def test_concurrent_retention_2(self):
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        with self.modify_backup_config(backup_dir, 'node') as cf:
            cf.data += "retention-redundancy = 1\n"
            cf.data += "retention-window = 2\n"

        # Fill with data
        node.pgbench_init(scale=1)

        full_id = self.pb.backup_node('node', node, backup_type="full")

        pgbench = node.pgbench(options=['-t', '20', '-c', '2'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '20', '-c', '2'])
        pgbench.wait()

        prev_id = self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '20', '-c', '2'])
        pgbench.wait()

        last_id = self.pb.backup_node('node', node, backup_type="page")

        days_delta = 4
        for backup_id in backup_dir.list_instance_backups('node'):
            with self.modify_backup_control(backup_dir, 'node', backup_id) as cf:
                cf.data += "\nrecovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=days_delta))
            days_delta -= 1

        gdb = self.pb.backup_node('node', node, gdb=True,
                               options=['--merge-expired'])
        gdb.set_breakpoint("merge_files")
        gdb.run_until_break()

        out = self.pb.backup_node('node', node,
                         options=['--merge-expired'],return_id=False)
                         #expect_error="because of concurrent merge")
        self.assertMessage(out, contains=f"WARNING: Backup {full_id} is not in stable state")
        self.assertMessage(out, contains=f"There are no backups to merge by retention policy")

    # @unittest.skip("skip")
    def test_window_expire_interleaved_incremental_chains(self):
        """complicated case of interleaved backup chains"""
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # take FULL BACKUPs
        backup_id_a = self.pb.backup_node('node', node)
        backup_id_b = self.pb.backup_node('node', node)

        # Change FULLb backup status to ERROR
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # FULLb  ERROR
        # FULLa  OK

        # Take PAGEa1 backup
        page_id_a1 = self.pb.backup_node('node', node, backup_type='page')

        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        # Change FULLb backup status to OK
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        # Change PAGEa1 and FULLa to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'ERROR')

        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  ERROR

        page_id_b1 = self.pb.backup_node('node', node, backup_type='page')

        # PAGEb1 OK
        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  ERROR

        # Now we start to play with first generation of PAGE backups
        # Change PAGEb1 and FULLb to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # Change PAGEa1 and FULLa to OK
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'OK')

        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        page_id_a2 = self.pb.backup_node('node', node, backup_type='page')

        # PAGEa2 OK
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        # Change PAGEa2 and FULLa to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'ERROR')

        # Change PAGEb1 and FULLb to OK
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        # PAGEa2 ERROR
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  ERROR

        page_id_b2 = self.pb.backup_node('node', node, backup_type='page')

        # Change PAGEa2 and FULla to OK
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'OK')

        # PAGEb2 OK
        # PAGEa2 OK
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        # Purge backups
        for backup in backup_dir.list_instance_backups('node'):
            if backup in [page_id_a2, page_id_b2]:
                continue
            with self.modify_backup_control(backup_dir, 'node', backup) as cf:
                cf.data += "\nrecovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=3))

        self.pb.delete_expired(
            'node',
            options=['--retention-window=1', '--expired'])

        self.assertEqual(len(self.pb.show('node')), 6)

        print(self.pb.show('node', as_json=False, as_text=True))

    # @unittest.skip("skip")
    def test_redundancy_expire_interleaved_incremental_chains(self):
        """complicated case of interleaved backup chains"""
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # take FULL BACKUPs
        backup_id_a = self.pb.backup_node('node', node)
        backup_id_b = self.pb.backup_node('node', node)

        # Change FULL B backup status to ERROR
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # FULLb  ERROR
        # FULLa  OK
        # Take PAGEa1 backup
        page_id_a1 = self.pb.backup_node('node', node, backup_type='page')

        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        # Change FULLb backup status to OK
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        # Change PAGEa1 and FULLa backup status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'ERROR')

        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  ERROR

        page_id_b1 = self.pb.backup_node('node', node, backup_type='page')

        # PAGEb1 OK
        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  ERROR

        # Now we start to play with first generation of PAGE backups
        # Change PAGEb1 status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # Change PAGEa1 status to OK
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'OK')

        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK
        page_id_a2 = self.pb.backup_node('node', node, backup_type='page')

        # PAGEa2 OK
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        # Change PAGEa2 and FULLa status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'ERROR')

        # Change PAGEb1 and FULLb status to OK
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        # PAGEa2 ERROR
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  ERROR
        self.pb.backup_node('node', node, backup_type='page')

        # Change PAGEa2 and FULLa status to OK
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'OK')

        # PAGEb2 OK
        # PAGEa2 OK
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        self.pb.delete_expired(
            'node',
            options=['--retention-redundancy=1', '--expired'])

        self.assertEqual(len(self.pb.show('node')), 3)

        print(self.pb.show('node', as_json=False, as_text=True))

    # @unittest.skip("skip")
    def test_window_merge_interleaved_incremental_chains(self):
        """complicated case of interleaved backup chains"""
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Take FULL BACKUPs
        backup_id_a = self.pb.backup_node('node', node)
        backup_id_b = self.pb.backup_node('node', node)

        # Change FULLb backup status to ERROR
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # FULLb  ERROR
        # FULLa  OK

        # Take PAGEa1 backup
        page_id_a1 = self.pb.backup_node('node', node, backup_type='page')

        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        # Change FULLb to OK
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        # Change PAGEa1 backup status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'ERROR')

        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  OK

        page_id_b1 = self.pb.backup_node('node', node, backup_type='page')

        # PAGEb1 OK
        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  OK

        # Now we start to play with first generation of PAGE backups
        # Change PAGEb1 and FULLb to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # Change PAGEa1 to OK
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'OK')

        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        page_id_a2 = self.pb.backup_node('node', node, backup_type='page')

        # PAGEa2 OK
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        # Change PAGEa2 and FULLa to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'ERROR')

        # Change PAGEb1 and FULLb to OK
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        # PAGEa2 ERROR
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  ERROR

        page_id_b2 = self.pb.backup_node('node', node, backup_type='page')

        # Change PAGEa2 and FULLa to OK
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'OK')

        # PAGEb2 OK
        # PAGEa2 OK
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        # Purge backups
        for backup in backup_dir.list_instance_backups('node'):
            if backup in [page_id_a2, page_id_b2]:
                continue
            with self.modify_backup_control(backup_dir, 'node', backup) as cf:
                cf.data += "\nrecovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=3))

        output = self.pb.delete_expired(
            'node',
            options=['--retention-window=1', '--expired', '--merge-expired'])

        self.assertIn(
            "Merge incremental chain between full backup {0} and backup {1}".format(
                backup_id_a, page_id_a2),
            output)

        self.assertIn(
            "Rename merged full backup {0} to {1}".format(
                backup_id_a, page_id_a2), output)

        self.assertIn(
            "Merge incremental chain between full backup {0} and backup {1}".format(
                backup_id_b, page_id_b2),
            output)

        self.assertIn(
            "Rename merged full backup {0} to {1}".format(
                backup_id_b, page_id_b2), output)

        self.assertEqual(len(self.pb.show('node')), 2)

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
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=5)

        # Take FULL BACKUPs
        self.pb.backup_node('node', node)
        pgbench = node.pgbench(options=['-t', '20', '-c', '1'])
        pgbench.wait()

        backup_id_b = self.pb.backup_node('node', node)
        pgbench = node.pgbench(options=['-t', '20', '-c', '1'])
        pgbench.wait()

        # Change FULL B backup status to ERROR
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        page_id_a1 = self.pb.backup_node('node', node, backup_type='page')

        pgdata_a1 = self.pgdata_content(node.data_dir)

        pgbench = node.pgbench(options=['-t', '20', '-c', '1'])
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
        self.pb.backup_node('node', node, backup_type='page')

        pgbench = node.pgbench(options=['-t', '20', '-c', '1'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type='page')

        pgbench = node.pgbench(options=['-t', '20', '-c', '1'])
        pgbench.wait()

        page_id_b3 = self.pb.backup_node('node', node, backup_type='page')
        pgdata_b3 = self.pgdata_content(node.data_dir)

        pgbench = node.pgbench(options=['-t', '20', '-c', '1'])
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
        for backup_id in backup_dir.list_instance_backups('node'):
            if backup_id in [page_id_a1, page_id_b3]:
                continue

            with self.modify_backup_control(backup_dir, 'node', backup_id) as cf:
                cf.data += "\nrecovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                            datetime.now() - timedelta(days=3))

        self.pb.delete_expired(
            'node',
            options=['--retention-window=1', '--expired', '--merge-expired'])

        self.assertEqual(len(self.pb.show('node')), 2)

        self.assertEqual(
            self.pb.show('node')[1]['id'],
            page_id_b3)

        self.assertEqual(
            self.pb.show('node')[0]['id'],
            page_id_a1)

        self.assertEqual(
            self.pb.show('node')[1]['backup-mode'],
            'FULL')

        self.assertEqual(
            self.pb.show('node')[0]['backup-mode'],
            'FULL')

        node.cleanup()

        # Data correctness of PAGEa3
        self.pb.restore_node('node', node, backup_id=page_id_a1)
        pgdata_restored_a1 = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata_a1, pgdata_restored_a1)

        node.cleanup()

        # Data correctness of PAGEb3
        self.pb.restore_node('node', node, backup_id=page_id_b3)
        pgdata_restored_b3 = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata_b3, pgdata_restored_b3)

    def test_basic_window_merge_multiple_descendants(self):
        r"""
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
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        # Take FULL BACKUPs
        backup_id_a = self.pb.backup_node('node', node)
        # pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        # pgbench.wait()

        backup_id_b = self.pb.backup_node('node', node)
        # pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        # pgbench.wait()

        # Change FULLb backup status to ERROR
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        page_id_a1 = self.pb.backup_node('node', node, backup_type='page')

        # pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        # pgbench.wait()

        # Change FULLb  to OK
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        # Change PAGEa1 to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'ERROR')

        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  OK

        page_id_b1 = self.pb.backup_node('node', node, backup_type='page')

        # PAGEb1 OK
        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  OK

        # pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        # pgbench.wait()

        # Change PAGEa1 to OK
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'OK')

        # Change PAGEb1 and FULLb to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        page_id_a2 = self.pb.backup_node('node', node, backup_type='page')

        # pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        # pgbench.wait()

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

        page_id_b2 = self.pb.backup_node('node', node, backup_type='page')

        # pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        # pgbench.wait()

        # PAGEb2 OK
        # PAGEa2 ERROR
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  ERROR

        # Change PAGEb2 and PAGEb1 to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_b2, 'ERROR')
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'ERROR')

        # and FULL stuff
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # PAGEb2 ERROR
        # PAGEa2 ERROR
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        page_id_a3 = self.pb.backup_node('node', node, backup_type='page')
        # pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        # pgbench.wait()

        # PAGEa3 OK
        # PAGEb2 ERROR
        # PAGEa2 ERROR
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        # Change PAGEa3 to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a3, 'ERROR')

        # Change PAGEb2, PAGEb1 and FULLb to OK
        self.change_backup_status(backup_dir, 'node', page_id_b2, 'OK')
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        page_id_b3 = self.pb.backup_node('node', node, backup_type='page')

        # PAGEb3 OK
        # PAGEa3 ERROR
        # PAGEb2 OK
        # PAGEa2 ERROR
        # PAGEb1 OK
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
            self.pb.show('node', backup_id=page_id_a3)['parent-backup-id'],
            page_id_a1)

        self.assertEqual(
            self.pb.show('node', backup_id=page_id_a2)['parent-backup-id'],
            page_id_a1)

        # Purge backups
        for backup in backup_dir.list_instance_backups('node'):
            if backup in [page_id_a3, page_id_b3]:
                continue
            with self.modify_backup_control(backup_dir, 'node', backup) as cf:
                cf.data += "\nrecovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=3))

        output = self.pb.delete_expired(
            'node',
            options=[
                '--retention-window=1', '--delete-expired',
                '--merge-expired', '--log-level-console=log'])

        self.assertEqual(len(self.pb.show('node')), 2)

        # Merging chain A
        self.assertIn(
            "Merge incremental chain between full backup {0} and backup {1}".format(
                backup_id_a, page_id_a3),
            output)

        self.assertIn(
            "INFO: Rename merged full backup {0} to {1}".format(
                backup_id_a, page_id_a3), output)

#        self.assertIn(
#            "WARNING: Backup {0} has multiple valid descendants. "
#            "Automatic merge is not possible.".format(
#                page_id_a1), output)

        self.assertIn(
            "LOG: Consider backup {0} for purge".format(
                page_id_a2), output)

        # Merge chain B
        self.assertIn(
            "Merge incremental chain between full backup {0} and backup {1}".format(
                backup_id_b, page_id_b3),
            output)

        self.assertIn(
            "INFO: Rename merged full backup {0} to {1}".format(
                backup_id_b, page_id_b3), output)

        self.assertIn(
            "Delete: {0}".format(page_id_a2), output)

        self.assertEqual(
            self.pb.show('node')[1]['id'],
            page_id_b3)

        self.assertEqual(
            self.pb.show('node')[0]['id'],
            page_id_a3)

        self.assertEqual(
            self.pb.show('node')[1]['backup-mode'],
            'FULL')

        self.assertEqual(
            self.pb.show('node')[0]['backup-mode'],
            'FULL')

    # @unittest.skip("skip")
    def test_basic_window_merge_multiple_descendants_1(self):
        r"""
        PAGEb3
          |                 PAGEa3
        -----------------------------retention window
        PAGEb2               /
          |       PAGEa2    /
        PAGEb1       \     /
          |           PAGEa1
        FULLb           |
                      FULLa
        """
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        # Take FULL BACKUPs
        backup_id_a = self.pb.backup_node('node', node)
        # pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        # pgbench.wait()

        backup_id_b = self.pb.backup_node('node', node)
        # pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        # pgbench.wait()

        # Change FULLb backup status to ERROR
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        page_id_a1 = self.pb.backup_node('node', node, backup_type='page')

        # pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        # pgbench.wait()

        # Change FULLb to OK
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        # Change PAGEa1 to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'ERROR')

        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  OK

        page_id_b1 = self.pb.backup_node('node', node, backup_type='page')

        # PAGEb1 OK
        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  OK

        # pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        # pgbench.wait()

        # Change PAGEa1 to OK
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'OK')

        # Change PAGEb1 and FULLb to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        page_id_a2 = self.pb.backup_node('node', node, backup_type='page')

        # pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        # pgbench.wait()

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

        page_id_b2 = self.pb.backup_node('node', node, backup_type='page')

        # pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        # pgbench.wait()

        # PAGEb2 OK
        # PAGEa2 ERROR
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  ERROR

        # Change PAGEb2 and PAGEb1 to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_b2, 'ERROR')
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'ERROR')

        # and FULL stuff
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # PAGEb2 ERROR
        # PAGEa2 ERROR
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        page_id_a3 = self.pb.backup_node('node', node, backup_type='page')
        # pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        # pgbench.wait()

        # PAGEa3 OK
        # PAGEb2 ERROR
        # PAGEa2 ERROR
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        # Change PAGEa3 to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a3, 'ERROR')

        # Change PAGEb2, PAGEb1 and FULLb to OK
        self.change_backup_status(backup_dir, 'node', page_id_b2, 'OK')
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        page_id_b3 = self.pb.backup_node('node', node, backup_type='page')

        # PAGEb3 OK
        # PAGEa3 ERROR
        # PAGEb2 OK
        # PAGEa2 ERROR
        # PAGEb1 OK
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
            self.pb.show('node', backup_id=page_id_a3)['parent-backup-id'],
            page_id_a1)

        self.assertEqual(
            self.pb.show('node', backup_id=page_id_a2)['parent-backup-id'],
            page_id_a1)

        # Purge backups
        for backup in backup_dir.list_instance_backups('node'):
            if backup in [page_id_a3, page_id_b3]:
                continue
            with self.modify_backup_control(backup_dir, 'node', backup) as cf:
                cf.data += "\nrecovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=3))

        output = self.pb.delete_expired(
            'node',
            options=[
                '--retention-window=1',
                '--merge-expired', '--log-level-console=log'])

        self.assertEqual(len(self.pb.show('node')), 3)

        # Merging chain A
        self.assertIn(
            "Merge incremental chain between full backup {0} and backup {1}".format(
                backup_id_a, page_id_a3),
            output)

        self.assertIn(
            "INFO: Rename merged full backup {0} to {1}".format(
                backup_id_a, page_id_a3), output)

#        self.assertIn(
#            "WARNING: Backup {0} has multiple valid descendants. "
#            "Automatic merge is not possible.".format(
#                page_id_a1), output)

        # Merge chain B
        self.assertIn(
            "Merge incremental chain between full backup {0} and backup {1}".format(
                backup_id_b, page_id_b3), output)

        self.assertIn(
            "INFO: Rename merged full backup {0} to {1}".format(
                backup_id_b, page_id_b3), output)

        self.assertEqual(
            self.pb.show('node')[2]['id'],
            page_id_b3)

        self.assertEqual(
            self.pb.show('node')[1]['id'],
            page_id_a3)

        self.assertEqual(
            self.pb.show('node')[0]['id'],
            page_id_a2)

        self.assertEqual(
            self.pb.show('node')[2]['backup-mode'],
            'FULL')

        self.assertEqual(
            self.pb.show('node')[1]['backup-mode'],
            'FULL')

        self.assertEqual(
            self.pb.show('node')[0]['backup-mode'],
            'PAGE')

        output = self.pb.delete_expired(
            'node',
            options=[
                '--retention-window=1',
                '--delete-expired', '--log-level-console=log'])

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
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        # Chain A
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')

        self.pb.backup_node('node', node, backup_type='page')

        # Chain B
        self.pb.backup_node('node', node)

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type='delta')

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type='page')

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        page_id_b3 = self.pb.backup_node('node', node, backup_type='delta')

        pgdata = self.pgdata_content(node.data_dir)

        # Purge backups
        for backup in backup_dir.list_instance_backups('node'):
            if backup in [page_id_b3]:
                continue
            with self.modify_backup_control(backup_dir, 'node', backup) as cf:
                cf.data += "\nrecovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=3))

        self.pb.delete_expired(
            'node',
            options=[
                '--retention-window=1', '--expired',
                '--merge-expired', '--log-level-console=log'])

        self.assertEqual(len(self.pb.show('node')), 1)

        node.cleanup()

        self.pb.restore_node('node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

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
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        # Chain A
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')

        self.pb.backup_node('node', node, backup_type='page')

        # Chain B
        self.pb.backup_node('node', node)

        self.pb.backup_node('node', node, backup_type='delta')

        self.pb.backup_node('node', node, backup_type='page')

        page_id_b3 = self.pb.backup_node('node', node, backup_type='delta')

        self.pgdata_content(node.data_dir)

        # Purge backups
        for backup in backup_dir.list_instance_backups('node'):
            if backup in [page_id_b3]:
                continue
            with self.modify_backup_control(backup_dir, 'node', backup) as cf:
                cf.data += "\nrecovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=3))

        output = self.pb.delete_expired(
            'node',
            options=[
                '--retention-window=1',
                '--merge-expired', '--log-level-console=log'])

        self.assertEqual(len(self.pb.show('node')), 4)

        self.assertIn(
            "There are no backups to delete by retention policy",
            output)

        self.assertIn(
            "Retention merging finished",
            output)

        output = self.pb.delete_expired(
            'node',
            options=[
                '--retention-window=1',
                '--expired', '--log-level-console=log'])

        self.assertEqual(len(self.pb.show('node')), 1)

        self.assertIn(
            "There are no backups to merge by retention policy",
            output)

        self.assertIn(
            "Purging finished",
            output)

    @unittest.skip("skip")
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
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Take FULL BACKUPs
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')

        self.pb.backup_node('node', node, backup_type='page')

        # Change FULLb backup status to ERROR
        # self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

    # @unittest.skip("skip")
    @needs_gdb
    def test_window_error_backups_1(self):
        """
        DELTA
        PAGE ERROR
        FULL
        -------window
        """

        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Take FULL BACKUP
        self.pb.backup_node('node', node)

        # Take PAGE BACKUP
        gdb = self.pb.backup_node('node', node, backup_type='page', gdb=True)

        # Attention! this breakpoint has been set on internal probackup function, not on a postgres core one
        gdb.set_breakpoint('pg_stop_backup')
        gdb.run_until_break()
        gdb.signal('SIGINT')
        gdb.continue_execution_until_error()

        self.pb.show('node')[1]['id']

        # Take DELTA backup
        self.pb.backup_node('node', node, backup_type='delta',
            options=['--retention-window=2', '--delete-expired'])

        # Take FULL BACKUP
        self.pb.backup_node('node', node)

        self.assertEqual(len(self.pb.show('node')), 4)

    # @unittest.skip("skip")
    @needs_gdb
    def test_window_error_backups_2(self):
        """
        DELTA
        PAGE ERROR
        FULL
        -------window
        """

        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Take FULL BACKUP
        self.pb.backup_node('node', node)

        # Take PAGE BACKUP
        gdb = self.pb.backup_node('node', node, backup_type='page', gdb=True)

        # Attention! this breakpoint has been set on internal probackup function, not on a postgres core one
        gdb.set_breakpoint('pg_stop_backup')
        gdb.run_until_break()
        gdb.signal('SIGKILL')
        gdb.continue_execution_until_error()

        self.pb.show('node')[1]['id']

        # Take DELTA backup
        self.pb.backup_node('node', node, backup_type='delta',
            options=['--retention-window=2', '--delete-expired'])

        self.assertEqual(len(self.pb.show('node')), 3)

    @needs_gdb
    def test_retention_redundancy_overlapping_chains(self):
        """"""

        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.set_config('node', options=['--retention-redundancy=1'])

        # Make backups to be purged
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type="page")

        # Make backups to be keeped
        gdb = self.pb.backup_node('node', node, gdb=True)
        gdb.set_breakpoint('backup_files')
        gdb.run_until_break()

        sleep(1)

        self.pb.backup_node('node', node, backup_type="page")

        gdb.continue_execution_until_exit()

        self.pb.backup_node('node', node, backup_type="page")

        # Purge backups
        self.pb.delete_expired(
            'node', options=['--expired', '--wal'])
        self.assertEqual(len(self.pb.show('node')), 2)

        self.pb.validate('node')

    @needs_gdb
    def test_retention_redundancy_overlapping_chains_1(self):
        """"""

        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.set_config('node', options=['--retention-redundancy=1'])

        # Make backups to be purged
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type="page")

        # Make backups to be keeped
        gdb = self.pb.backup_node('node', node, gdb=True)
        gdb.set_breakpoint('backup_files')
        gdb.run_until_break()

        sleep(1)

        self.pb.backup_node('node', node, backup_type="page")

        gdb.continue_execution_until_exit()

        self.pb.backup_node('node', node, backup_type="page")

        # Purge backups
        self.pb.delete_expired(
            'node', options=['--expired', '--wal'])
        self.assertEqual(len(self.pb.show('node')), 2)

        self.pb.validate('node')

    def test_wal_purge_victim(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/103
        """
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Make ERROR incremental backup
        self.pb.backup_node('node', node, backup_type='page',
                         expect_error="because page backup should not be "
                                      "possible without valid full backup")
        self.assertMessage(contains="WARNING: Valid full backup on current timeline 1 is not found")
        self.assertMessage(contains="ERROR: Create new full backup before an incremental one")

        page_id = self.pb.show('node')[0]['id']

        sleep(1)

        # Make FULL backup
        full_id = self.pb.backup_node('node', node, options=['--delete-wal'])

        self.pb.validate('node',
                         expect_error="because page backup should not be "
                                      "possible without valid full backup")
        self.assertMessage(contains=f"INFO: Backup {full_id} WAL segments are valid")
        self.assertMessage(contains=f"WARNING: Backup {page_id} has missing parent 0")

    # @unittest.skip("skip")
    @needs_gdb
    def test_failed_merge_redundancy_retention(self):
        """
        Check that retention purge works correctly with MERGING backups
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL1 backup
        full_id = self.pb.backup_node('node', node)

        # DELTA BACKUP
        delta_id = self.pb.backup_node('node', node, backup_type='delta')

        # DELTA BACKUP
        self.pb.backup_node('node', node, backup_type='delta')

        # DELTA BACKUP
        self.pb.backup_node('node', node, backup_type='delta')

        # FULL2 backup
        self.pb.backup_node('node', node)

        # DELTA BACKUP
        self.pb.backup_node('node', node, backup_type='delta')

        # DELTA BACKUP
        self.pb.backup_node('node', node, backup_type='delta')

        # FULL3 backup
        self.pb.backup_node('node', node)

        # DELTA BACKUP
        self.pb.backup_node('node', node, backup_type='delta')

        # DELTA BACKUP
        self.pb.backup_node('node', node, backup_type='delta')

        self.pb.set_config('node', options=['--retention-redundancy=2'])

        self.pb.set_config('node', options=['--retention-window=2'])

        # create pair of MERGING backup as a result of failed merge 
        gdb = self.pb.merge_backup('node', delta_id, gdb=True)
        gdb.set_breakpoint('backup_non_data_file')
        gdb.run_until_break()
        gdb.continue_execution_until_break(2)
        gdb.signal('SIGKILL')

        # "expire" first full backup
        with self.modify_backup_control(backup_dir, 'node', full_id) as cf:
            cf.data += "\nrecovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                datetime.now() - timedelta(days=3))

        # run retention merge
        self.pb.delete_expired(
            'node', options=['--delete-expired'])

        self.assertEqual(
            'MERGING',
            self.pb.show('node', full_id)['status'],
            'Backup STATUS should be "MERGING"')

        self.assertEqual(
            'MERGING',
            self.pb.show('node', delta_id)['status'],
            'Backup STATUS should be "MERGING"')

        self.assertEqual(len(self.pb.show('node')), 10)

    def test_wal_depth_1(self):
        """
                        |-------------B5----------> WAL timeline3
                  |-----|-------------------------> WAL timeline2
        B1   B2---|        B3     B4-------B6-----> WAL timeline1

        wal-depth=2
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'archive_timeout': '30s',
                'checkpoint_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)

        self.pb.set_config('node', options=['--archive-timeout=60s'])

        node.slow_start()

        # FULL
        node.pgbench_init(scale=1)
        self.pb.backup_node('node', node)

        # PAGE
        node.pgbench_init(scale=1)
        B2 = self.pb.backup_node('node', node, backup_type='page')

        # generate_some more data
        node.pgbench_init(scale=1)

        target_xid = node.safe_psql(
            "postgres",
            "select txid_current()").decode('utf-8').rstrip()

        node.pgbench_init(scale=1)

        self.pb.backup_node('node', node, backup_type='page')

        node.pgbench_init(scale=1)

        self.pb.backup_node('node', node, backup_type='page')

        # Timeline 2
        node_restored = self.pg_node.make_simple('node_restored')

        node_restored.cleanup()

        output = self.pb.restore_node('node', node_restored,
            options=[
                '--recovery-target-xid={0}'.format(target_xid),
                '--recovery-target-action=promote'])

        self.assertIn(
            'Restore of backup {0} completed'.format(B2),
            output)

        node_restored.set_auto_conf(options={'port': node_restored.port})

        node_restored.slow_start()

        node_restored.pgbench_init(scale=1)

        target_xid = node_restored.safe_psql(
            "postgres",
            "select txid_current()").decode('utf-8').rstrip()

        node_restored.pgbench_init(scale=2)

        # Timeline 3
        node_restored.cleanup()

        output = self.pb.restore_node('node', node_restored,
            options=[
                '--recovery-target-xid={0}'.format(target_xid),
                '--recovery-target-timeline=2',
                '--recovery-target-action=promote'])

        self.assertIn(
            'Restore of backup {0} completed'.format(B2),
            output)

        node_restored.set_auto_conf(options={'port': node_restored.port})

        node_restored.slow_start()

        node_restored.pgbench_init(scale=1)
        self.pb.backup_node('node', node_restored, data_dir=node_restored.data_dir)

        node.pgbench_init(scale=1)
        self.pb.backup_node('node', node)

        lsn = self.pb.show_archive('node', tli=2)['switchpoint']

        self.pb.validate(
            'node', backup_id=B2,
            options=['--recovery-target-lsn={0}'.format(lsn)])

        self.pb.validate('node')

    def test_wal_purge(self):
        """
         -------------------------------------> tli5
         ---------------------------B6--------> tli4
                            S2`---------------> tli3
             S1`------------S2---B4-------B5--> tli2
        B1---S1-------------B2--------B3------> tli1

        B* - backups
        S* - switchpoints

        Expected result:
                    TLI5 will be purged entirely
                                    B6--------> tli4
                            S2`---------------> tli3
             S1`------------S2---B4-------B5--> tli2
        B1---S1-------------B2--------B3------> tli1

        wal-depth=2
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_config('node', options=['--archive-timeout=60s'])

        node.slow_start()

        # STREAM FULL
        stream_id = self.pb.backup_node('node', node, options=['--stream'])

        node.stop()
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL
        B1 = self.pb.backup_node('node', node)
        node.pgbench_init(scale=1)

        target_xid = node.safe_psql(
            "postgres",
            "select txid_current()").decode('utf-8').rstrip()
        node.pgbench_init(scale=5)

        # B2 FULL on TLI1
        self.pb.backup_node('node', node)
        node.pgbench_init(scale=4)
        self.pb.backup_node('node', node)
        node.pgbench_init(scale=4)

        self.pb.delete('node', options=['--delete-wal'])

        # TLI 2
        node_tli2 = self.pg_node.make_simple('node_tli2')
        node_tli2.cleanup()

        output = self.pb.restore_node('node', node_tli2,
            options=[
                '--recovery-target-xid={0}'.format(target_xid),
                '--recovery-target-timeline=1',
                '--recovery-target-action=promote'])

        self.assertIn(
            'INFO: Restore of backup {0} completed'.format(B1),
            output)

        node_tli2.set_auto_conf(options={'port': node_tli2.port})
        node_tli2.slow_start()
        node_tli2.pgbench_init(scale=4)

        target_xid = node_tli2.safe_psql(
            "postgres",
            "select txid_current()").decode('utf-8').rstrip()
        node_tli2.pgbench_init(scale=1)

        self.pb.backup_node('node', node_tli2, data_dir=node_tli2.data_dir)
        node_tli2.pgbench_init(scale=3)

        self.pb.backup_node('node', node_tli2, data_dir=node_tli2.data_dir)
        node_tli2.pgbench_init(scale=1)
        node_tli2.cleanup()

        # TLI3
        node_tli3 = self.pg_node.make_simple('node_tli3')
        node_tli3.cleanup()

        # Note, that successful validation here is a happy coincidence 
        output = self.pb.restore_node('node', node_tli3,
            options=[
                '--recovery-target-xid={0}'.format(target_xid),
                '--recovery-target-timeline=2',
                '--recovery-target-action=promote'])

        self.assertIn(
            'INFO: Restore of backup {0} completed'.format(B1),
            output)
        node_tli3.set_auto_conf(options={'port': node_tli3.port})
        node_tli3.slow_start()
        node_tli3.pgbench_init(scale=5)
        node_tli3.cleanup()

        # TLI4
        node_tli4 = self.pg_node.make_simple('node_tli4')
        node_tli4.cleanup()

        self.pb.restore_node('node', node_tli4, backup_id=stream_id,
            options=[
                '--recovery-target=immediate',
                '--recovery-target-action=promote'])

        node_tli4.set_auto_conf(options={'port': node_tli4.port})
        self.pb.set_archiving('node', node_tli4)
        node_tli4.slow_start()

        node_tli4.pgbench_init(scale=5)

        self.pb.backup_node('node', node_tli4, data_dir=node_tli4.data_dir)
        node_tli4.pgbench_init(scale=5)
        node_tli4.cleanup()

        # TLI5
        node_tli5 = self.pg_node.make_simple('node_tli5')
        node_tli5.cleanup()

        self.pb.restore_node('node', node_tli5, backup_id=stream_id,
            options=[
                '--recovery-target=immediate',
                '--recovery-target-action=promote'])

        node_tli5.set_auto_conf(options={'port': node_tli5.port})
        self.pb.set_archiving('node', node_tli5)
        node_tli5.slow_start()
        node_tli5.pgbench_init(scale=10)

        # delete '.history' file of TLI4
        self.remove_instance_wal(backup_dir, 'node', '00000004.history')
        # delete '.history' file of TLI5
        self.wait_instance_wal_exists(backup_dir, 'node', '00000005.history')
        self.remove_instance_wal(backup_dir, 'node', '00000005.history')

        tailer = tail_file(os.path.join(node_tli5.logs_dir, 'postgresql.log'))
        tailer.wait(contains='LOG: pushing file "000000050000000000000007')
        tailer.wait_archive_push_completed()
        del tailer
        node_tli5.stop()

        output = self.pb.delete('node',
            options=[
                '--delete-wal', '--dry-run',
                '--log-level-console=verbose'])

        self.assertIn(
            'INFO: On timeline 4 WAL segments between 000000040000000000000002 '
            'and 000000040000000000000006 can be removed',
            output)

        self.assertIn(
            'INFO: On timeline 5 all files can be removed',
            output)

        show_tli1_before = self.pb.show_archive('node', tli=1)
        show_tli2_before = self.pb.show_archive('node', tli=2)
        show_tli3_before = self.pb.show_archive('node', tli=3)
        show_tli4_before = self.pb.show_archive('node', tli=4)
        show_tli5_before = self.pb.show_archive('node', tli=5)

        self.assertTrue(show_tli1_before)
        self.assertTrue(show_tli2_before)
        self.assertTrue(show_tli3_before)
        self.assertTrue(show_tli4_before)
        self.assertTrue(show_tli5_before)

        output = self.pb.delete('node',
            options=['--delete-wal', '--log-level-console=verbose'])

        self.assertIn(
            'INFO: On timeline 4 WAL segments between 000000040000000000000002 '
            'and 000000040000000000000006 will be removed',
            output)

        self.assertIn(
            'INFO: On timeline 5 all files will be removed',
            output)

        show_tli1_after = self.pb.show_archive('node', tli=1)
        show_tli2_after = self.pb.show_archive('node', tli=2)
        show_tli3_after = self.pb.show_archive('node', tli=3)
        show_tli4_after = self.pb.show_archive('node', tli=4)
        show_tli5_after = self.pb.show_archive('node', tli=5)

        self.assertEqual(show_tli1_before, show_tli1_after)
        self.assertEqual(show_tli2_before, show_tli2_after)
        self.assertEqual(show_tli3_before, show_tli3_after)
        self.assertNotEqual(show_tli4_before, show_tli4_after)
        self.assertNotEqual(show_tli5_before, show_tli5_after)

        self.assertEqual(
            show_tli4_before['min-segno'],
            '000000040000000000000002')

        self.assertEqual(
            show_tli4_after['min-segno'],
            '000000040000000000000006')

        self.assertFalse(show_tli5_after)

        self.pb.validate('node')

    def test_wal_depth_2(self):
        """
         -------------------------------------> tli5
         ---------------------------B6--------> tli4
                            S2`---------------> tli3
             S1`------------S2---B4-------B5--> tli2
        B1---S1-------------B2--------B3------> tli1

        B* - backups
        S* - switchpoints
        wal-depth=2

        Expected result:
                    TLI5 will be purged entirely
                                    B6--------> tli4
                            S2`---------------> tli3
             S1`------------S2   B4-------B5--> tli2
        B1---S1             B2--------B3------> tli1

        wal-depth=2
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_config('node', options=['--archive-timeout=60s'])

        node.slow_start()

        # STREAM FULL
        stream_id = self.pb.backup_node('node', node, options=['--stream'])

        node.stop()
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL
        B1 = self.pb.backup_node('node', node)
        node.pgbench_init(scale=1)

        target_xid = node.safe_psql(
            "postgres",
            "select txid_current()").decode('utf-8').rstrip()
        node.pgbench_init(scale=5)

        # B2 FULL on TLI1
        B2 = self.pb.backup_node('node', node)
        node.pgbench_init(scale=4)
        self.pb.backup_node('node', node)
        node.pgbench_init(scale=4)

        # TLI 2
        node_tli2 = self.pg_node.make_simple('node_tli2')
        node_tli2.cleanup()

        output = self.pb.restore_node('node', node_tli2,
            options=[
                '--recovery-target-xid={0}'.format(target_xid),
                '--recovery-target-timeline=1',
                '--recovery-target-action=promote'])

        self.assertIn(
            'INFO: Restore of backup {0} completed'.format(B1),
            output)

        node_tli2.set_auto_conf(options={'port': node_tli2.port})
        node_tli2.slow_start()
        node_tli2.pgbench_init(scale=4)

        target_xid = node_tli2.safe_psql(
            "postgres",
            "select txid_current()").decode('utf-8').rstrip()
        node_tli2.pgbench_init(scale=1)

        B4 = self.pb.backup_node('node', node_tli2, data_dir=node_tli2.data_dir)
        node_tli2.pgbench_init(scale=3)

        self.pb.backup_node('node', node_tli2, data_dir=node_tli2.data_dir)
        node_tli2.pgbench_init(scale=1)
        node_tli2.cleanup()

        # TLI3
        node_tli3 = self.pg_node.make_simple('node_tli3')
        node_tli3.cleanup()

        # Note, that successful validation here is a happy coincidence 
        output = self.pb.restore_node('node', node_tli3,
            options=[
                '--recovery-target-xid={0}'.format(target_xid),
                '--recovery-target-timeline=2',
                '--recovery-target-action=promote'])

        self.assertIn(
            'INFO: Restore of backup {0} completed'.format(B1),
            output)
        node_tli3.set_auto_conf(options={'port': node_tli3.port})
        node_tli3.slow_start()
        node_tli3.pgbench_init(scale=5)
        node_tli3.cleanup()

        # TLI4
        node_tli4 = self.pg_node.make_simple('node_tli4')
        node_tli4.cleanup()

        self.pb.restore_node('node', node_tli4, backup_id=stream_id,
            options=[
                '--recovery-target=immediate',
                '--recovery-target-action=promote'])

        node_tli4.set_auto_conf(options={'port': node_tli4.port})
        self.pb.set_archiving('node', node_tli4)
        node_tli4.slow_start()

        node_tli4.pgbench_init(scale=5)

        self.pb.backup_node('node', node_tli4, data_dir=node_tli4.data_dir)
        node_tli4.pgbench_init(scale=5)
        node_tli4.cleanup()

        # TLI5
        node_tli5 = self.pg_node.make_simple('node_tli5')
        node_tli5.cleanup()

        self.pb.restore_node('node', node_tli5, backup_id=stream_id,
            options=[
                '--recovery-target=immediate',
                '--recovery-target-action=promote'])

        node_tli5.set_auto_conf(options={'port': node_tli5.port})
        self.pb.set_archiving('node', node_tli5)
        node_tli5.slow_start()
        node_tli5.pgbench_init(scale=10)

        # delete '.history' file of TLI4
        self.remove_instance_wal(backup_dir, 'node', '00000004.history')
        # delete '.history' file of TLI5
        self.wait_instance_wal_exists(backup_dir, 'node', '00000005.history')
        self.remove_instance_wal(backup_dir, 'node', '00000005.history')

        output = self.pb.delete('node',
            options=[
                '--delete-wal', '--dry-run',
                '--wal-depth=2', '--log-level-console=verbose'])

        start_lsn_B2 = self.pb.show('node', B2)['start-lsn']
        self.assertIn(
            'On timeline 1 WAL is protected from purge at {0}'.format(start_lsn_B2),
            output)

        self.assertIn(
            'LOG: Archive backup {0} to stay consistent protect from '
            'purge WAL interval between 000000010000000000000004 '
            'and 000000010000000000000005 on timeline 1'.format(B1), output)

        start_lsn_B4 = self.pb.show('node', B4)['start-lsn']
        self.assertIn(
            'On timeline 2 WAL is protected from purge at {0}'.format(start_lsn_B4),
            output)

        self.assertIn(
            'LOG: Timeline 3 to stay reachable from timeline 1 protect '
            'from purge WAL interval between 000000020000000000000006 and '
            '000000020000000000000009 on timeline 2', output)

        self.assertIn(
            'LOG: Timeline 3 to stay reachable from timeline 1 protect '
            'from purge WAL interval between 000000010000000000000004 and '
            '000000010000000000000006 on timeline 1', output)

        show_tli1_before = self.pb.show_archive('node', tli=1)
        show_tli2_before = self.pb.show_archive('node', tli=2)
        show_tli3_before = self.pb.show_archive('node', tli=3)
        show_tli4_before = self.pb.show_archive('node', tli=4)
        show_tli5_before = self.pb.show_archive('node', tli=5)

        self.assertTrue(show_tli1_before)
        self.assertTrue(show_tli2_before)
        self.assertTrue(show_tli3_before)
        self.assertTrue(show_tli4_before)
        self.assertTrue(show_tli5_before)

        sleep(5)

        output = self.pb.delete('node',
            options=['--delete-wal', '--wal-depth=2', '--log-level-console=verbose'])

#        print(output)

        show_tli1_after = self.pb.show_archive('node', tli=1)
        show_tli2_after = self.pb.show_archive('node', tli=2)
        show_tli3_after = self.pb.show_archive('node', tli=3)
        show_tli4_after = self.pb.show_archive('node', tli=4)
        show_tli5_after = self.pb.show_archive('node', tli=5)

        self.assertNotEqual(show_tli1_before, show_tli1_after)
        self.assertNotEqual(show_tli2_before, show_tli2_after)
        self.assertEqual(show_tli3_before, show_tli3_after)
        self.assertNotEqual(show_tli4_before, show_tli4_after)
        self.assertNotEqual(show_tli5_before, show_tli5_after)

        self.assertEqual(
            show_tli4_before['min-segno'],
            '000000040000000000000002')

        self.assertEqual(
            show_tli4_after['min-segno'],
            '000000040000000000000006')

        self.assertFalse(show_tli5_after)

        self.assertTrue(show_tli1_after['lost-segments'])
        self.assertTrue(show_tli2_after['lost-segments'])
        self.assertFalse(show_tli3_after['lost-segments'])
        self.assertFalse(show_tli4_after['lost-segments'])
        self.assertFalse(show_tli5_after)

        self.assertEqual(len(show_tli1_after['lost-segments']), 1)
        self.assertEqual(len(show_tli2_after['lost-segments']), 1)

        self.assertEqual(
            show_tli1_after['lost-segments'][0]['begin-segno'],
            '000000010000000000000007')

        self.assertEqual(
            show_tli1_after['lost-segments'][0]['end-segno'],
            '00000001000000000000000A')

        self.assertEqual(
            show_tli2_after['lost-segments'][0]['begin-segno'],
            '00000002000000000000000A')

        self.assertEqual(
            show_tli2_after['lost-segments'][0]['end-segno'],
            '00000002000000000000000A')

        self.pb.validate('node')

    def test_basic_wal_depth(self):
        """
        B1---B1----B3-----B4----B5------> tli1

        Expected result with wal-depth=1:
        B1   B1    B3     B4    B5------> tli1

        wal-depth=1
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_config('node', options=['--archive-timeout=60s'])
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL
        node.pgbench_init(scale=1)
        B1 = self.pb.backup_node('node', node)


        # B2
        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()
        B2 = self.pb.backup_node('node', node, backup_type='page')

        # B3
        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()
        B3 = self.pb.backup_node('node', node, backup_type='page')

        # B4
        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()
        B4 = self.pb.backup_node('node', node, backup_type='page')

        # B5
        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()
        B5 = self.pb.backup_node('node', node, backup_type='page',
            options=['--wal-depth=1', '--delete-wal'])

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        target_xid = node.safe_psql(
            "postgres",
            "select txid_current()").decode('utf-8').rstrip()

        self.switch_wal_segment(node)

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        tli1 = self.pb.show_archive('node', tli=1,
                                 options=['--log-level-file=VERBOSE'])

        # check that there are 4 lost_segments intervals
        self.assertEqual(len(tli1['lost-segments']), 4)

        output = self.pb.validate(
            'node', B5,
            options=['--recovery-target-xid={0}'.format(target_xid)])

        self.assertIn(
            'INFO: Backup validation completed successfully on time',
            output)

        self.assertIn(
            'xid {0} and LSN'.format(target_xid),
            output)

        for backup_id in [B1, B2, B3, B4]:
            self.pb.validate('node', backup_id,
                             options=['--recovery-target-xid', target_xid],
                             expect_error="because page backup should not be "
                                          "possible without valid full backup")
            self.assertMessage(contains=f"ERROR: Not enough WAL records to xid {target_xid}")

        self.pb.validate('node')

    @needs_gdb
    def test_concurrent_running_full_backup(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/328
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL
        self.pb.backup_node('node', node)

        gdb = self.pb.backup_node('node', node, gdb=True)
        gdb.set_breakpoint('backup_data_file')
        gdb.run_until_break()
        gdb.kill()

        self.assertTrue(
            self.pb.show('node')[0]['status'],
            'RUNNING')

        self.pb.backup_node('node', node, backup_type='delta',
            options=['--retention-redundancy=2', '--delete-expired'])

        self.assertTrue(
            self.pb.show('node')[1]['status'],
            'RUNNING')

        self.pb.backup_node('node', node)

        gdb = self.pb.backup_node('node', node, gdb=True)
        gdb.set_breakpoint('backup_data_file')
        gdb.run_until_break()
        gdb.kill()

        gdb = self.pb.backup_node('node', node, gdb=True)
        gdb.set_breakpoint('backup_data_file')
        gdb.run_until_break()
        gdb.kill()

        self.pb.backup_node('node', node)

        gdb = self.pb.backup_node('node', node, gdb=True)
        gdb.set_breakpoint('backup_data_file')
        gdb.run_until_break()
        gdb.kill()

        self.expire_locks(backup_dir, 'node')

        self.pb.backup_node('node', node, backup_type='delta',
            options=['--retention-redundancy=2', '--delete-expired'],
            return_id=False)

        self.assertTrue(
            self.pb.show('node')[0]['status'],
            'OK')

        self.assertTrue(
            self.pb.show('node')[1]['status'],
            'RUNNING')

        self.assertTrue(
            self.pb.show('node')[2]['status'],
            'OK')

        self.assertEqual(
            len(self.pb.show('node')),
            6)
