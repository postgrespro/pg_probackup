from .helpers.ptrack_helpers import ProbackupTest
import subprocess


class DeleteTest(ProbackupTest):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_delete_full_backups(self):
        """delete full backups"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # full backup
        id_1 = self.pb.backup_node('node', node)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        id_2 = self.pb.backup_node('node', node)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        id_2_1 = self.pb.backup_node('node', node, backup_type = "delta")

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        id_3 = self.pb.backup_node('node', node)

        show_backups = self.pb.show('node')
        self.assertEqual(show_backups[0]['id'], id_1)
        self.assertEqual(show_backups[1]['id'], id_2)
        self.assertEqual(show_backups[2]['id'], id_2_1)
        self.assertEqual(show_backups[3]['id'], id_3)

        self.pb.delete('node', id_2)
        show_backups = self.pb.show('node')
        self.assertEqual(len(show_backups), 2)
        self.assertEqual(show_backups[0]['id'], id_1)
        self.assertEqual(show_backups[1]['id'], id_3)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_del_instance_archive(self):
        """delete full backups"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # full backup
        self.pb.backup_node('node', node)

        # full backup
        self.pb.backup_node('node', node)

        # restore
        node.cleanup()
        self.pb.restore_node('node', node=node)
        node.slow_start()

        # Delete instance
        self.pb.del_instance('node')

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_delete_archive_mix_compress_and_non_compressed_segments(self):
        """delete full backups"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node, compress=False)
        node.slow_start()

        # full backup
        self.pb.backup_node('node', node)

        node.pgbench_init(scale=10)

        # Restart archiving with compression
        self.pb.set_archiving('node', node, compress=True)

        node.restart()

        # full backup
        self.pb.backup_node('node', node)

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        self.pb.backup_node('node', node,
            options=[
                '--retention-redundancy=3',
                '--delete-expired'])

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        self.pb.backup_node('node', node,
            options=[
                '--retention-redundancy=3',
                '--delete-expired'])

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()

        self.pb.backup_node('node', node,
            options=[
                '--retention-redundancy=3',
                '--delete-expired'])

    # @unittest.skip("skip")
    def test_basic_delete_increment_page(self):
        """delete increment and all after him"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # full backup mode
        self.pb.backup_node('node', node)
        # page backup mode
        self.pb.backup_node('node', node, backup_type="page")
        # page backup mode
        self.pb.backup_node('node', node, backup_type="page")
        # full backup mode
        self.pb.backup_node('node', node)

        show_backups = self.pb.show('node')
        self.assertEqual(len(show_backups), 4)

        # delete first page backup
        self.pb.delete('node', show_backups[1]['id'])

        show_backups = self.pb.show('node')
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

        node = self.pg_node.make_simple('node',
            ptrack_enable=self.ptrack)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            'postgres',
            'CREATE EXTENSION ptrack')

        # full backup mode
        self.pb.backup_node('node', node)
        # ptrack backup mode
        self.pb.backup_node('node', node, backup_type="ptrack")
        # ptrack backup mode
        self.pb.backup_node('node', node, backup_type="ptrack")
        # full backup mode
        self.pb.backup_node('node', node)

        show_backups = self.pb.show('node')
        self.assertEqual(len(show_backups), 4)

        # delete first page backup
        self.pb.delete('node', show_backups[1]['id'])

        show_backups = self.pb.show('node')
        self.assertEqual(len(show_backups), 2)

        self.assertEqual(show_backups[0]['backup-mode'], "FULL")
        self.assertEqual(show_backups[0]['status'], "OK")
        self.assertEqual(show_backups[1]['backup-mode'], "FULL")
        self.assertEqual(show_backups[1]['status'], "OK")

    # @unittest.skip("skip")
    def test_basic_delete_orphaned_wal_segments(self):
        """
        make archive node, make three full backups,
        delete second backup without --wal option,
        then delete orphaned wals via --wal option
        """
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,10000) i")
        # first full backup
        backup_1_id = self.pb.backup_node('node', node)
        # second full backup
        backup_2_id = self.pb.backup_node('node', node)
        # third full backup
        backup_3_id = self.pb.backup_node('node', node)
        node.stop()

        # Check wals
        wals = self.get_instance_wal_list(backup_dir, 'node')
        original_wal_quantity = len(wals)

        # delete second full backup
        self.pb.delete('node', backup_2_id)
        # check wal quantity
        self.pb.validate()
        self.assertEqual(self.pb.show('node', backup_1_id)['status'], "OK")
        self.assertEqual(self.pb.show('node', backup_3_id)['status'], "OK")
        # try to delete wals for second backup
        self.pb.delete('node', options=['--wal'])
        # check wal quantity
        self.pb.validate()
        self.assertEqual(self.pb.show('node', backup_1_id)['status'], "OK")
        self.assertEqual(self.pb.show('node', backup_3_id)['status'], "OK")

        # delete first full backup
        self.pb.delete('node', backup_1_id)
        self.pb.validate()
        self.assertEqual(self.pb.show('node', backup_3_id)['status'], "OK")

        result = self.pb.delete('node', options=['--wal'])
        # delete useless wals
        self.assertTrue('On timeline 1 WAL segments between ' in result
            and 'will be removed' in result)

        self.pb.validate()
        self.assertEqual(self.pb.show('node', backup_3_id)['status'], "OK")

        # Check quantity, it should be lower than original
        wals = self.get_instance_wal_list(backup_dir, 'node')
        self.assertGreater(original_wal_quantity, len(wals), "Number of wals not changed after 'delete --wal' which is illegal")

        # Delete last backup
        self.pb.delete('node', backup_3_id, options=['--wal'])
        wals = self.get_instance_wal_list(backup_dir, 'node')
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
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        A1 = self.pb.backup_node('node', node)

        # load some data to node
        node.pgbench_init(scale=3)

        node2 = self.pg_node.make_simple('node2')
        node2.cleanup()

        self.pb.restore_node('node', node=node2)
        node2.set_auto_conf({'port': node2.port})
        node2.slow_start()

        # load some more data to node
        node.pgbench_init(scale=3)

        # take A2
        A2 = self.pb.backup_node('node', node)

        # load some more data to node2
        node2.pgbench_init(scale=2)

        B1 = self.pb.backup_node('node',
            node2, data_dir=node2.data_dir)

        self.pb.delete('node', backup_id=A1, options=['--wal'])

        self.pb.validate()

    # @unittest.skip("skip")
    def test_delete_backup_with_empty_control_file(self):
        """
        take backup, truncate its control file,
        try to delete it via 'delete' command
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # full backup mode
        self.pb.backup_node('node', node, options=['--stream'])
        # page backup mode
        self.pb.backup_node('node', node, backup_type="delta", options=['--stream'])
        # page backup mode
        backup_id = self.pb.backup_node('node', node, backup_type="delta", options=['--stream'])

        with self.modify_backup_control(backup_dir, 'node', backup_id) as cf:
            cf.data = ''

        show_backups = self.pb.show('node')
        self.assertEqual(len(show_backups), 3)

        self.pb.delete('node', backup_id=backup_id)

    # @unittest.skip("skip")
    def test_delete_interleaved_incremental_chains(self):
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

        # Change FULLb to ERROR
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

        page_id_a2 = self.pb.backup_node('node', node, backup_type='page')

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

        page_id_b2 = self.pb.backup_node('node', node, backup_type='page')

        # Change PAGEa2 and FULLa status to OK
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        # PAGEb2 OK
        # PAGEa2 OK
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')

        # PAGEc1 OK
        # FULLc  OK
        # PAGEb2 OK
        # PAGEa2 OK
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        # Delete FULLb
        self.pb.delete('node', backup_id_b)

        self.assertEqual(len(self.pb.show('node')), 5)

        print(self.pb.show('node', as_json=False, as_text=True))

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
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Take FULL BACKUPs
        backup_id_a = self.pb.backup_node('node', node)
        backup_id_b = self.pb.backup_node('node', node)

        # Change FULLb to ERROR
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        page_id_a1 = self.pb.backup_node('node', node, backup_type='page')

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

        # Change PAGEa1 to OK
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'OK')

        # Change PAGEb1 and FULLb backup status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

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

        page_id_a3 = self.pb.backup_node('node', node, backup_type='page')

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

        page_id_b3 = self.pb.backup_node('node', node, backup_type='page')

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
            self.pb.show('node', backup_id=page_id_a3)['parent-backup-id'],
            page_id_a1)

        self.assertEqual(
            self.pb.show('node', backup_id=page_id_a2)['parent-backup-id'],
            page_id_a1)

        # Delete FULLa
        self.pb.delete('node', backup_id_a)

        self.assertEqual(len(self.pb.show('node')), 4)

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
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Take FULL BACKUP
        node.pgbench_init(scale=1)
        backup_id_a = self.pb.backup_node('node', node)

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()
        page_id_a1 = self.pb.backup_node('node', node, backup_type='page')

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()
        page_id_a2 = self.pb.backup_node('node', node, backup_type='page')


        # Change PAGEa2 to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'ERROR')

        pgbench = node.pgbench(options=['-T', '10', '-c', '2'])
        pgbench.wait()
        page_id_a3 = self.pb.backup_node('node', node, backup_type='page')

        # Change PAGEa2 to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'OK')

        # Delete PAGEa1
        output = self.pb.delete('node', page_id_a1,
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

        self.assertRegex(output,
            r'On timeline 1 WAL segments between 000000010000000000000001 '
            r'and 00000001000000000000000\d can be removed')

        self.assertEqual(len(self.pb.show('node')), 4)

        output = self.pb.delete('node', page_id_a1,
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

        self.assertRegex(output,
            r'On timeline 1 WAL segments between 000000010000000000000001 '
            r'and 00000001000000000000000\d will be removed')

        self.assertEqual(len(self.pb.show('node')), 1)

        self.pb.validate('node')

    def test_delete_error_backups(self):
        """delete increment and all after him"""
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # full backup mode
        self.pb.backup_node('node', node)
        # page backup mode
        self.pb.backup_node('node', node, backup_type="page")

        # Take FULL BACKUP
        backup_id_a = self.pb.backup_node('node', node)
        # Take PAGE BACKUP
        backup_id_b = self.pb.backup_node('node', node, backup_type="page")

        backup_id_c = self.pb.backup_node('node', node, backup_type="page")

        backup_id_d = self.pb.backup_node('node', node, backup_type="page")

        # full backup mode
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type="page")
        backup_id_e = self.pb.backup_node('node', node, backup_type="page")
        self.pb.backup_node('node', node, backup_type="page")

        # Change status to ERROR
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_c, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_e, 'ERROR')

        print(self.pb.show(as_text=True, as_json=False))

        show_backups = self.pb.show('node')
        self.assertEqual(len(show_backups), 10)

        # delete error backups
        output = self.pb.delete('node', options=['--status=ERROR', '--dry-run'])
        show_backups = self.pb.show('node')
        self.assertEqual(len(show_backups), 10)

        self.assertIn(
            "Deleting all backups with status 'ERROR' in dry run mode",
            output)

        self.assertIn(
            "INFO: Backup {0} with status OK can be deleted".format(backup_id_d),
            output)

        print(self.pb.show(as_text=True, as_json=False))

        show_backups = self.pb.show('node')
        output = self.pb.delete('node', options=['--status=ERROR'])
        print(output)
        show_backups = self.pb.show('node')
        self.assertEqual(len(show_backups), 4)

        self.assertEqual(show_backups[0]['status'], "OK")
        self.assertEqual(show_backups[1]['status'], "OK")
        self.assertEqual(show_backups[2]['status'], "OK")
        self.assertEqual(show_backups[3]['status'], "OK")

###########################################################################
#                             dry-run
###########################################################################

    def test_basic_dry_run_del_instance(self):
        """ Check del-instance command with dry-run option"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # full backup
        self.pb.backup_node('node', node, options=['--stream'])

        content_before = self.pgdata_content(self.backup_dir)
        # Delete instance
        self.pb.del_instance('node', options=['--dry-run'])

        self.compare_instance_dir(
            content_before,
            self.pgdata_content(self.backup_dir)
        )

        node.cleanup()