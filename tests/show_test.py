import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException


class ShowTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_show_1(self):
        """Status DONE and OK"""
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        self.assertEqual(
            self.backup_node(
                backup_dir, 'node', node,
                options=["--log-level-console=off"]),
            None
        )
        self.assertIn("OK", self.show_pb(backup_dir, 'node', as_text=True))

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_show_json(self):
        """Status DONE and OK"""
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        self.assertEqual(
            self.backup_node(
                backup_dir, 'node', node,
                options=["--log-level-console=off"]),
            None
        )
        self.backup_node(backup_dir, 'node', node)
        self.assertIn("OK", self.show_pb(backup_dir, 'node', as_text=True))

    # @unittest.skip("skip")
    def test_corrupt_2(self):
        """Status CORRUPT"""
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        backup_id = self.backup_node(backup_dir, 'node', node)

        # delete file which belong to backup
        file = os.path.join(
            backup_dir, "backups", "node",
            backup_id, "database", "postgresql.conf")
        os.remove(file)

        try:
            self.validate_pb(backup_dir, 'node', backup_id)
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because backup corrupted."
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd
                )
            )
        except ProbackupException as e:
            self.assertIn(
                'data files are corrupted',
                e.message,
                '\n Unexpected Error Message: {0}\n'
                ' CMD: {1}'.format(repr(e.message), self.cmd)
            )
        self.assertIn("CORRUPT", self.show_pb(backup_dir, as_text=True))

    # @unittest.skip("skip")
    def test_no_control_file(self):
        """backup.control doesn't exist"""
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        backup_id = self.backup_node(backup_dir, 'node', node)

        # delete backup.control file
        file = os.path.join(
            backup_dir, "backups", "node",
            backup_id, "backup.control")
        os.remove(file)

        output = self.show_pb(backup_dir, 'node', as_text=True, as_json=False)

        self.assertIn(
            'Control file',
            output)

        self.assertIn(
            'doesn\'t exist',
            output)

    # @unittest.skip("skip")
    def test_empty_control_file(self):
        """backup.control is empty"""
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        backup_id = self.backup_node(backup_dir, 'node', node)

        # truncate backup.control file
        file = os.path.join(
            backup_dir, "backups", "node",
            backup_id, "backup.control")
        fd = open(file, 'w')
        fd.close()

        output = self.show_pb(backup_dir, 'node', as_text=True, as_json=False)

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
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        backup_id = self.backup_node(backup_dir, 'node', node)

        # corrupt backup.control file
        file = os.path.join(
            backup_dir, "backups", "node",
            backup_id, "backup.control")
        fd = open(file, 'a')
        fd.write("statuss = OK")
        fd.close()

        self.assertIn(
            'WARNING: Invalid option "statuss" in file',
            self.show_pb(backup_dir, 'node', as_json=False, as_text=True))

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_corrupt_correctness(self):
        """backup.control contains invalid option"""
        if not self.remote:
            self.skipTest("You must enable PGPROBACKUP_SSH_REMOTE"
                          " for run this test")
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=1)

        # FULL
        backup_local_id = self.backup_node(
            backup_dir, 'node', node, no_remote=True)

        output_local = self.show_pb(
            backup_dir, 'node', as_json=False, backup_id=backup_local_id)

        backup_remote_id = self.backup_node(backup_dir, 'node', node)

        output_remote = self.show_pb(
            backup_dir, 'node', as_json=False, backup_id=backup_remote_id)

        # check correctness
        self.assertEqual(
            output_local['data-bytes'],
            output_remote['data-bytes'])

        self.assertEqual(
            output_local['uncompressed-bytes'],
            output_remote['uncompressed-bytes'])

        # DELTA
        backup_local_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta', no_remote=True)

        output_local = self.show_pb(
            backup_dir, 'node', as_json=False, backup_id=backup_local_id)
        self.delete_pb(backup_dir, 'node', backup_local_id)

        backup_remote_id = self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        output_remote = self.show_pb(
            backup_dir, 'node', as_json=False, backup_id=backup_remote_id)
        self.delete_pb(backup_dir, 'node', backup_remote_id)

        # check correctness
        self.assertEqual(
            output_local['data-bytes'],
            output_remote['data-bytes'])

        self.assertEqual(
            output_local['uncompressed-bytes'],
            output_remote['uncompressed-bytes'])

        # PAGE
        backup_local_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', no_remote=True)

        output_local = self.show_pb(
            backup_dir, 'node', as_json=False, backup_id=backup_local_id)
        self.delete_pb(backup_dir, 'node', backup_local_id)

        backup_remote_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        output_remote = self.show_pb(
            backup_dir, 'node', as_json=False, backup_id=backup_remote_id)
        self.delete_pb(backup_dir, 'node', backup_remote_id)

        # check correctness
        self.assertEqual(
            output_local['data-bytes'],
            output_remote['data-bytes'])

        self.assertEqual(
            output_local['uncompressed-bytes'],
            output_remote['uncompressed-bytes'])

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_corrupt_correctness_1(self):
        """backup.control contains invalid option"""
        if not self.remote:
            self.skipTest("You must enable PGPROBACKUP_SSH_REMOTE"
                          " for run this test")
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=1)

        # FULL
        backup_local_id = self.backup_node(
            backup_dir, 'node', node, no_remote=True)

        output_local = self.show_pb(
            backup_dir, 'node', as_json=False, backup_id=backup_local_id)

        backup_remote_id = self.backup_node(backup_dir, 'node', node)

        output_remote = self.show_pb(
            backup_dir, 'node', as_json=False, backup_id=backup_remote_id)

        # check correctness
        self.assertEqual(
            output_local['data-bytes'],
            output_remote['data-bytes'])

        self.assertEqual(
            output_local['uncompressed-bytes'],
            output_remote['uncompressed-bytes'])

        # change data
        pgbench = node.pgbench(options=['-T', '10', '--no-vacuum'])
        pgbench.wait()

        # DELTA
        backup_local_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta', no_remote=True)

        output_local = self.show_pb(
            backup_dir, 'node', as_json=False, backup_id=backup_local_id)
        self.delete_pb(backup_dir, 'node', backup_local_id)

        backup_remote_id = self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        output_remote = self.show_pb(
            backup_dir, 'node', as_json=False, backup_id=backup_remote_id)
        self.delete_pb(backup_dir, 'node', backup_remote_id)

        # check correctness
        self.assertEqual(
            output_local['data-bytes'],
            output_remote['data-bytes'])

        self.assertEqual(
            output_local['uncompressed-bytes'],
            output_remote['uncompressed-bytes'])

        # PAGE
        backup_local_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', no_remote=True)

        output_local = self.show_pb(
            backup_dir, 'node', as_json=False, backup_id=backup_local_id)
        self.delete_pb(backup_dir, 'node', backup_local_id)

        backup_remote_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        output_remote = self.show_pb(
            backup_dir, 'node', as_json=False, backup_id=backup_remote_id)
        self.delete_pb(backup_dir, 'node', backup_remote_id)

        # check correctness
        self.assertEqual(
            output_local['data-bytes'],
            output_remote['data-bytes'])

        self.assertEqual(
            output_local['uncompressed-bytes'],
            output_remote['uncompressed-bytes'])

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_corrupt_correctness_2(self):
        """backup.control contains invalid option"""
        if not self.remote:
            self.skipTest("You must enable PGPROBACKUP_SSH_REMOTE"
                          " for run this test")
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=1)

        # FULL
        backup_local_id = self.backup_node(
            backup_dir, 'node', node,
            options=['--compress'], no_remote=True)

        output_local = self.show_pb(
            backup_dir, 'node', as_json=False, backup_id=backup_local_id)

        if self.remote:
            backup_remote_id = self.backup_node(
                backup_dir, 'node', node, options=['--compress'])
        else:
            backup_remote_id = self.backup_node(
                backup_dir, 'node', node,
                options=['--remote-proto=ssh', '--remote-host=localhost', '--compress'])

        output_remote = self.show_pb(
            backup_dir, 'node', as_json=False, backup_id=backup_remote_id)

        # check correctness
        self.assertEqual(
            output_local['data-bytes'],
            output_remote['data-bytes'])

        self.assertEqual(
            output_local['uncompressed-bytes'],
            output_remote['uncompressed-bytes'])

        # change data
        pgbench = node.pgbench(options=['-T', '10', '--no-vacuum'])
        pgbench.wait()

        # DELTA
        backup_local_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta', options=['--compress'], no_remote=True)

        output_local = self.show_pb(
            backup_dir, 'node', as_json=False, backup_id=backup_local_id)
        self.delete_pb(backup_dir, 'node', backup_local_id)

        if self.remote:
            backup_remote_id = self.backup_node(
                backup_dir, 'node', node, backup_type='delta', options=['--compress'])
        else:
            backup_remote_id = self.backup_node(
                backup_dir, 'node', node, backup_type='delta',
                options=['--remote-proto=ssh', '--remote-host=localhost', '--compress'])

        output_remote = self.show_pb(
            backup_dir, 'node', as_json=False, backup_id=backup_remote_id)
        self.delete_pb(backup_dir, 'node', backup_remote_id)

        # check correctness
        self.assertEqual(
            output_local['data-bytes'],
            output_remote['data-bytes'])

        self.assertEqual(
            output_local['uncompressed-bytes'],
            output_remote['uncompressed-bytes'])

        # PAGE
        backup_local_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', options=['--compress'], no_remote=True)

        output_local = self.show_pb(
            backup_dir, 'node', as_json=False, backup_id=backup_local_id)
        self.delete_pb(backup_dir, 'node', backup_local_id)

        if self.remote:
            backup_remote_id = self.backup_node(
                backup_dir, 'node', node, backup_type='page', options=['--compress'])
        else:
            backup_remote_id = self.backup_node(
                backup_dir, 'node', node, backup_type='page',
                options=['--remote-proto=ssh', '--remote-host=localhost', '--compress'])

        output_remote = self.show_pb(
            backup_dir, 'node', as_json=False, backup_id=backup_remote_id)
        self.delete_pb(backup_dir, 'node', backup_remote_id)

        # check correctness
        self.assertEqual(
            output_local['data-bytes'],
            output_remote['data-bytes'])

        self.assertEqual(
            output_local['uncompressed-bytes'],
            output_remote['uncompressed-bytes'])

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_color_with_no_terminal(self):
        """backup.control contains invalid option"""
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=1)

        # FULL
        try:
            self.backup_node(
                backup_dir, 'node', node, options=['--archive-timeout=1s'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because archiving is disabled\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertNotIn(
                '[0m', e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

    # @unittest.skip("skip")
    def test_tablespace_print_issue_431(self):
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
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

        full_backup_id =  self.backup_node(backup_dir, 'node', node)
        self.assertIn("OK", self.show_pb(backup_dir,'node', as_text=True))
        # Check that tablespace info exists. JSON
        self.assertIn("tablespace_map", self.show_pb(backup_dir, 'node', as_text=True))
        self.assertIn("oid", self.show_pb(backup_dir, 'node', as_text=True))
        self.assertIn("path", self.show_pb(backup_dir, 'node', as_text=True))
        self.assertIn(tblspc_path, self.show_pb(backup_dir, 'node', as_text=True))
        # Check that tablespace info exists. PLAIN
        self.assertIn("tablespace_map", self.show_pb(backup_dir, 'node', backup_id=full_backup_id, as_text=True, as_json=False))
        self.assertIn(tblspc_path, self.show_pb(backup_dir, 'node', backup_id=full_backup_id, as_text=True, as_json=False))
        # Check that tablespace info NOT exists if backup id not provided. PLAIN
        self.assertNotIn("tablespace_map", self.show_pb(backup_dir, 'node', as_text=True, as_json=False))
