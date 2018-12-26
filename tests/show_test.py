import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException


module_name = 'show'


class OptionTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_show_1(self):
        """Status DONE and OK"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )

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

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_show_json(self):
        """Status DONE and OK"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )

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

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_corrupt_2(self):
        """Status CORRUPT"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )

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
                "Expecting Error because backup corrupted.\n"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd
                )
            )
        except ProbackupException as e:
            self.assertIn(
                'data files are corrupted\n',
                e.message,
                '\n Unexpected Error Message: {0}\n'
                ' CMD: {1}'.format(repr(e.message), self.cmd)
            )
        self.assertIn("CORRUPT", self.show_pb(backup_dir, as_text=True))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_no_control_file(self):
        """backup.control doesn't exist"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )

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

        self.assertIn('Control file "{0}" doesn\'t exist'.format(file), self.show_pb(backup_dir, 'node', as_text=True))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_empty_control_file(self):
        """backup.control is empty"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )

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

        self.assertIn('Control file "{0}" is empty'.format(file), self.show_pb(backup_dir, 'node', as_text=True))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_corrupt_control_file(self):
        """backup.control contains invalid option"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )

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
            'WARNING: Invalid option "statuss" in file'.format(file),
            self.show_pb(backup_dir, 'node', as_text=True))

        # Clean after yourself
        # self.del_test_dir(module_name, fname)
