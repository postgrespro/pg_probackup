import copy
import unittest
import os

from .compression_test import have_alg
from .helpers.ptrack_helpers import ProbackupTest, fs_backup_class
from .helpers.state_helper import get_program_version
from .helpers.validators.show_validator import ShowJsonResultValidator


class ShowTest(ProbackupTest):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_show_1(self):
        """Status DONE and OK"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.assertEqual(
            self.pb.backup_node('node', node,
                options=["--log-level-console=off"]),
            None
        )
        self.assertIn("OK", self.pb.show('node', as_text=True))

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_show_json(self):
        """Status DONE and OK"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.assertEqual(
            self.pb.backup_node('node', node,
                options=["--log-level-console=off"]),
            None
        )
        self.pb.backup_node('node', node)
        self.assertIn("OK", self.pb.show('node', as_text=True))

    # @unittest.skip("skip")
    def test_corrupt_2(self):
        """Status CORRUPT"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node)

        # delete file which belong to backup
        self.remove_backup_file(backup_dir, 'node', backup_id, "database/postgresql.conf")

        error_result = self.pb.validate('node', backup_id, expect_error=True)

        self.assertMessage(error_result, contains='data files are corrupted')
        self.assertIn("CORRUPT", self.pb.show(as_text=True))

    def test_failed_backup_status(self):
        """Status ERROR - showing recovery-time for faield backup"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node, backup_type='delta', expect_error=True)

        show_res = self.pb.show('node', as_text=True, as_json=False)
        self.assertIn("ERROR", show_res)
        self.assertIn("Recovery Time", show_res)
        self.assertNotIn("----  DELTA", show_res)

    # @unittest.skip("skip")
    def test_no_control_file(self):
        """backup.control doesn't exist"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node)

        # delete backup.control file
        self.remove_backup_file(backup_dir, "node", backup_id, "backup.control")

        output = self.pb.show('node', as_text=True, as_json=False)

        self.assertIn(
            'Control file',
            output)

        self.assertIn(
            'doesn\'t exist',
            output)

    # @unittest.skip("skip")
    def test_empty_control_file(self):
        """backup.control is empty"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node)

        # truncate backup.control file
        with self.modify_backup_control(self.backup_dir, 'node', backup_id) as cf:
            cf.data = ''

        output = self.pb.show('node', as_text=True, as_json=False)

        self.assertIn(
            'Control file',
            output)

        self.assertIn(
            'is empty',
            output)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_corrupt_control_file(self):
        """backup.control contains invalid option"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node)

        # corrupt backup.control file
        with self.modify_backup_control(self.backup_dir, 'node', backup_id) as cf:
            cf.data += "\nstatuss = OK"

        self.assertIn(
            'WARNING: Invalid option "statuss" in file',
            self.pb.show('node', as_json=False, as_text=True))

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_corrupt_correctness(self):
        """backup.control contains invalid option"""
        if not self.remote:
            self.skipTest("You must enable PGPROBACKUP_SSH_REMOTE"
                          " for run this test")
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=1)

        # FULL
        backup_local_id = self.pb.backup_node('node', node, no_remote=True)

        backup_remote_id = self.pb.backup_node('node', node)

        # check correctness
        self.check_backup_size_in_show(backup_local_id, backup_remote_id, 'node', compressed=True)
        self.check_backup_size_in_show(backup_local_id, backup_remote_id, 'node', compressed=False)

        # DELTA
        backup_local_id = self.pb.backup_node('node', node,
            backup_type='delta', no_remote=True)

        output_local = self.pb.show('node', as_json=False, backup_id=backup_local_id)
        self.pb.delete('node', backup_local_id)

        backup_remote_id = self.pb.backup_node('node', node, backup_type='delta')

        output_remote = self.pb.show('node', as_json=False, backup_id=backup_remote_id)
        self.pb.delete('node', backup_remote_id)

        # check correctness
        self.assertAlmostEqual(
            int(output_local['data-bytes']),
            int(output_remote['data-bytes']), delta=2)

        self.assertAlmostEqual(
            int(output_local['uncompressed-bytes']),
            int(output_remote['uncompressed-bytes']), delta=2)

        # PAGE
        backup_local_id = self.pb.backup_node('node', node,
            backup_type='page', no_remote=True)

        output_local = self.pb.show('node', as_json=False, backup_id=backup_local_id)
        self.pb.delete('node', backup_local_id)

        backup_remote_id = self.pb.backup_node('node', node, backup_type='page')

        output_remote = self.pb.show('node', as_json=False, backup_id=backup_remote_id)
        self.pb.delete('node', backup_remote_id)

        # check correctness
        self.assertAlmostEqual(
            int(output_local['data-bytes']),
            int(output_remote['data-bytes']), delta=2)

        self.assertAlmostEqual(
            int(output_local['uncompressed-bytes']),
            int(output_remote['uncompressed-bytes']), delta=2)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_corrupt_correctness_1(self):
        """backup.control contains invalid option"""
        if not self.remote:
            self.skipTest("You must enable PGPROBACKUP_SSH_REMOTE"
                          " for run this test")
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # stabilize test
        # there were situation that due to internal wal segment switches
        # backup_label differed in size:
        # - first page backup had 0/E000028 location, and
        # - second page backup - 0/10000028
        # Stabilize by adding more segments therefore it is always long
        for i in range(8):
            self.switch_wal_segment(node)

        node.pgbench_init(scale=1)

        # FULL
        backup_local_id = self.pb.backup_node('node', node, no_remote=True)

        backup_remote_id = self.pb.backup_node('node', node)

        # check correctness
        self.check_backup_size_in_show(backup_local_id, backup_remote_id, 'node', compressed=True)
        self.check_backup_size_in_show(backup_local_id, backup_remote_id, 'node', compressed=False)

        # change data
        pgbench = node.pgbench(options=['-T', '10', '--no-vacuum'])
        pgbench.wait()

        # DELTA
        backup_local_id = self.pb.backup_node('node', node,
            backup_type='delta', no_remote=True)

        output_local = self.pb.show('node', as_json=False, backup_id=backup_local_id)
        self.pb.delete('node', backup_local_id)

        backup_remote_id = self.pb.backup_node('node', node, backup_type='delta')

        output_remote = self.pb.show('node', as_json=False, backup_id=backup_remote_id)
        self.pb.delete('node', backup_remote_id)

        # check correctness
        self.assertAlmostEqual(
            int(output_local['data-bytes']),
            int(output_remote['data-bytes']), delta=2)

        self.assertAlmostEqual(
            int(output_local['uncompressed-bytes']),
            int(output_remote['uncompressed-bytes']), delta=2)

        # PAGE
        backup_local_id = self.pb.backup_node('node', node,
            backup_type='page', no_remote=True)

        output_local = self.pb.show('node', as_json=False, backup_id=backup_local_id)
        self.pb.delete('node', backup_local_id)

        backup_remote_id = self.pb.backup_node('node', node, backup_type='page')

        output_remote = self.pb.show('node', as_json=False, backup_id=backup_remote_id)
        self.pb.delete('node', backup_remote_id)

        # check correctness
        self.assertAlmostEqual(
            int(output_local['data-bytes']),
            int(output_remote['data-bytes']), delta=2)

        self.assertAlmostEqual(
            int(output_local['uncompressed-bytes']),
            int(output_remote['uncompressed-bytes']), delta=2)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_corrupt_correctness_2(self):
        """backup.control contains invalid option"""
        if not self.remote:
            self.skipTest("You must enable PGPROBACKUP_SSH_REMOTE"
                          " for run this test")
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # stabilize test
        # there were situation that due to internal wal segment switches
        # backup_label differed in size:
        # - first page backup had 0/E000028 location, and
        # - second page backup - 0/10000028
        # Stabilize by adding more segments therefore it is always long
        for i in range(8):
            self.switch_wal_segment(node)

        node.pgbench_init(scale=1)

        # FULL
        backup_local_id = self.pb.backup_node('node', node,
            options=['--compress'], no_remote=True)

        if self.remote:
            backup_remote_id = self.pb.backup_node('node', node, options=['--compress'])
        else:
            backup_remote_id = self.pb.backup_node('node', node,
                options=['--remote-proto=ssh', '--remote-host=localhost', '--compress'])

        # check correctness
        self.check_backup_size_in_show(backup_local_id, backup_remote_id, 'node', compressed=True)
        self.check_backup_size_in_show(backup_local_id, backup_remote_id, 'node', compressed=False)

        # change data
        pgbench = node.pgbench(options=['-T', '10', '--no-vacuum'])
        pgbench.wait()

        # DELTA
        backup_local_id = self.pb.backup_node('node', node,
            backup_type='delta', options=['--compress'], no_remote=True)

        output_local = self.pb.show('node', as_json=False, backup_id=backup_local_id)
        self.pb.delete('node', backup_local_id)

        if self.remote:
            backup_remote_id = self.pb.backup_node('node', node, backup_type='delta', options=['--compress'])
        else:
            backup_remote_id = self.pb.backup_node('node', node, backup_type='delta',
                options=['--remote-proto=ssh', '--remote-host=localhost', '--compress'])

        output_remote = self.pb.show('node', as_json=False, backup_id=backup_remote_id)
        self.pb.delete('node', backup_remote_id)

        # check correctness
        self.assertAlmostEqual(
            int(output_local['data-bytes']),
            int(output_remote['data-bytes']), delta=2)

        self.assertAlmostEqual(
            int(output_local['uncompressed-bytes']),
            int(output_remote['uncompressed-bytes']), delta=2)

        # PAGE
        backup_local_id = self.pb.backup_node('node', node,
            backup_type='page', options=['--compress'], no_remote=True)

        output_local = self.pb.show('node', as_json=False, backup_id=backup_local_id)
        self.pb.delete('node', backup_local_id)

        if self.remote:
            backup_remote_id = self.pb.backup_node('node', node, backup_type='page', options=['--compress'])
        else:
            backup_remote_id = self.pb.backup_node('node', node, backup_type='page',
                options=['--remote-proto=ssh', '--remote-host=localhost', '--compress'])

        output_remote = self.pb.show('node', as_json=False, backup_id=backup_remote_id)
        self.pb.delete('node', backup_remote_id)

        # check correctness
        self.assertAlmostEqual(
            int(output_local['data-bytes']),
            int(output_remote['data-bytes']), delta=2)

        self.assertAlmostEqual(
            int(output_local['uncompressed-bytes']),
            int(output_remote['uncompressed-bytes']), delta=2)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_color_with_no_terminal(self):
        """backup.control contains invalid option"""
        node = self.pg_node.make_simple('node',
            pg_options={'autovacuum': 'off'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.pgbench_init(scale=1)

        # FULL
        error_result = self.pb.backup_node('node', node, options=['--archive-timeout=1s'], expect_error=True)
        self.assertNotIn('[0m', error_result)

    @unittest.skipIf(not (have_alg('lz4') and have_alg('zstd')),
                     "pg_probackup is not compiled with lz4 or zstd support")
    def test_show_command_as_text(self):
        instance_name = 'node'
        node = self.pg_node.make_simple(
            base_dir=instance_name)

        self.pb.init()
        self.pb.add_instance(instance_name, node)
        self.pb.set_archiving(instance_name, node)
        node.slow_start()

        self.pb.backup_node(instance_name, node, backup_type="full", options=['--compress-level', '1',
                                                                                       '--compress-algorithm', 'pglz'])

        self.pb.backup_node(instance_name, node, backup_type="delta", options=['--compress-level', '3',
                                                                                        '--compress-algorithm', 'lz4'])

        self.pb.backup_node(instance_name, node, backup_type="page", options=['--compress-level', '9',
                                                                                       '--compress-algorithm', 'zstd'])
        self.pb.backup_node(instance_name, node, backup_type="page")

        show_backups = self.pb.show(instance_name, as_text=True, as_json=False)
        self.assertIn(" FULL   ARCHIVE ", show_backups)  # Mode, Wal mode
        self.assertIn(" DELTA  ARCHIVE ", show_backups)  # Mode, Wal mode
        self.assertIn(" PAGE   ARCHIVE ", show_backups)  # Mode, Wal mode
        self.assertIn(" pglz ", show_backups)
        self.assertIn(" lz4 ", show_backups)
        self.assertIn(" zstd ", show_backups)
        self.assertIn(" none ", show_backups)
        self.assertIn(" OK ", show_backups)  # Status

    def test_show_command_as_json(self):
        instance_name = 'node'
        node = self.pg_node.make_simple(
            base_dir=instance_name)

        self.pb.init()
        self.pb.add_instance(instance_name, node)
        self.pb.set_archiving(instance_name, node)
        node.slow_start()

        pg_version = int(self.pg_config_version/10000)

        full_backup_id = self.pb.backup_node(instance_name, node, backup_type="full")

        delta_backup_id = self.pb.backup_node(instance_name, node, backup_type="delta")

        page_backup_id = self.pb.backup_node(instance_name, node, backup_type="page")

        show_backups = self.pb.show(instance_name, as_text=False, as_json=True)

        common_show_result = ShowJsonResultValidator()
        common_show_result.wal = "ARCHIVE"
        common_show_result.compress_alg = "none"
        common_show_result.compress_level = 1
        common_show_result.from_replica = "false"
        common_show_result.block_size = 8192
        common_show_result.xlog_block_size = 8192
        common_show_result.checksum_version = 1
        common_show_result.program_version = get_program_version()
        common_show_result.server_version = pg_version
        common_show_result.status = "OK"

        full_show_result = copy.deepcopy(common_show_result)
        full_show_result.backup_mode = "FULL"
        full_show_result.set_backup_id = full_backup_id

        delta_show_result = copy.deepcopy(common_show_result)
        delta_show_result.backup_mode = "DELTA"
        delta_show_result.backup_id = delta_backup_id
        delta_show_result.parent_backup_id = full_backup_id

        page_show_result = copy.deepcopy(common_show_result)
        page_show_result.backup_mode = "PAGE"
        page_show_result.backup_id = page_backup_id
        page_show_result.parent_backup_id = delta_backup_id

        full_show_result.check_show_json(show_backups[0])
        delta_show_result.check_show_json(show_backups[1])
        page_show_result.check_show_json(show_backups[2])

    def test_tablespace_print_issue_431(self):
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Create tablespace
        tblspc_path = os.path.join(node.base_dir, "tblspc")
        os.makedirs(tblspc_path)
        with node.connect("postgres") as con:
            con.connection.autocommit = True
            con.execute("CREATE TABLESPACE tblspc LOCATION '%s'" % tblspc_path)
            con.connection.autocommit = False
            con.execute("CREATE TABLE test (id int) TABLESPACE tblspc")
            con.execute("INSERT INTO test VALUES (1)")
            con.commit()

        full_backup_id = self.pb.backup_node('node', node)
        self.assertIn("OK", self.pb.show('node', as_text=True))
        # Check that tablespace info exists. JSON
        self.assertIn("tablespace_map", self.pb.show('node', as_text=True))
        self.assertIn("oid", self.pb.show('node', as_text=True))
        self.assertIn("path", self.pb.show('node', as_text=True))
        self.assertIn(tblspc_path, self.pb.show('node', as_text=True))
        # Check that tablespace info exists. PLAIN
        self.assertIn("tablespace_map", self.pb.show('node', backup_id=full_backup_id, as_text=True, as_json=False))
        self.assertIn(tblspc_path, self.pb.show('node', backup_id=full_backup_id, as_text=True, as_json=False))
        # Check that tablespace info NOT exists if backup id not provided. PLAIN
        self.assertNotIn("tablespace_map", self.pb.show('node', as_text=True, as_json=False))

    def test_show_hidden_merged_dirs_as_json(self):
        instance_name = 'node'
        node = self.pg_node.make_simple(
            base_dir=instance_name)

        self.pb.init()
        self.pb.add_instance(instance_name, node)
        self.pb.set_archiving(instance_name, node)
        node.slow_start()

        pg_version = int(self.pg_config_version/10000)

        full_backup_id = self.pb.backup_node(instance_name, node, backup_type="full")

        delta_backup_id = self.pb.backup_node(instance_name, node, backup_type="delta")

        page_backup_id = self.pb.backup_node(instance_name, node, backup_type="page")

        self.pb.merge_backup(instance_name, delta_backup_id)
        show_backups = self.pb.show(instance_name, as_text=False, as_json=True, options=["--show-symlinks"])

        self.assertEqual(show_backups[0]['backup-mode'], "FULL")
        self.assertEqual(show_backups[0]['id'], delta_backup_id)
        self.assertEqual(show_backups[0]['id'], show_backups[1]['id'])
        self.assertEqual(show_backups[0]['dir'], full_backup_id)

        self.assertEqual(show_backups[1]['status'], "SYMLINK")
        self.assertEqual(show_backups[1]['id'], show_backups[0]['id'])
        self.assertEqual(show_backups[1]['symlink'], full_backup_id)


    def test_show_hidden_merged_dirs_as_plain(self):
        instance_name = 'node'
        node = self.pg_node.make_simple(
            base_dir=instance_name)

        self.pb.init()
        self.pb.add_instance(instance_name, node)
        self.pb.set_archiving(instance_name, node)
        node.slow_start()

        pg_version = int(self.pg_config_version/10000)

        full_backup_id = self.pb.backup_node(instance_name, node, backup_type="full")

        delta_backup_id = self.pb.backup_node(instance_name, node, backup_type="delta")

        page_backup_id = self.pb.backup_node(instance_name, node, backup_type="page")

        self.pb.merge_backup(instance_name, delta_backup_id)
        show_backups = self.pb.show(instance_name, as_text=True, as_json=False, options=["--show-symlinks"])

        self.assertIn(" PAGE  ARCHIVE ", show_backups)  # Mode, Wal mode
        self.assertIn(" FULL  ARCHIVE ", show_backups)  # Mode, Wal mode

    def get_backup_label_size(self, backup_id, instance_name):
        """Get backup_label size from file backup_content.control"""
        content_control_json = self.read_backup_content_control(backup_id, instance_name)
        for item in content_control_json:
            if item.get('path') == 'backup_label':
                return item['size']

    def check_backup_size_in_show(self, first_backup_id, second_backup_id, instance_name, compressed=True):
        """Use show command to check backup size. If we have difference,
        try to compare size without backuo_label file"""
        first_out = self.pb.show('node', as_json=False, backup_id=first_backup_id)

        second_out = self.pb.show('node', as_json=False, backup_id=second_backup_id)

        # check correctness
        if compressed:
            first_size = first_out['data-bytes']
            second_size = second_out['data-bytes']
        else:
            first_size = first_out['uncompressed-bytes']
            second_size = second_out['uncompressed-bytes']
        if fs_backup_class.is_file_based:
            local_label_size = self.get_backup_label_size(first_backup_id, instance_name)
            remote_label_size = self.get_backup_label_size(second_backup_id, instance_name)
            # If we have difference in full size check without backup_label file
            self.assertTrue(first_size == second_size or
                            first_size - local_label_size == second_size - remote_label_size)
            self.assertAlmostEqual(int(local_label_size), int(remote_label_size), delta=2)
        else:
            self.assertAlmostEqual(int(first_size), int(second_size), delta=2)
