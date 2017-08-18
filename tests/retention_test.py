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
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        with open(os.path.join(backup_dir, 'backups', 'node', "pg_probackup.conf"), "a") as conf:
            conf.write("retention-redundancy = 1\n")

        # Make backups to be purged
        self.backup_node(backup_dir, 'node', node)
        self.backup_node(backup_dir, 'node', node, backup_type="page")
        # Make backups to be keeped
        self.backup_node(backup_dir, 'node', node)
        self.backup_node(backup_dir, 'node', node, backup_type="page")

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 4)

        # Purge backups
        log = self.delete_expired(backup_dir, 'node')
        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 2)

        # Check that WAL segments were deleted
        min_wal = None
        max_wal = None
        for line in log.splitlines():
            if line.startswith("INFO: removed min WAL segment"):
                min_wal = line[31:-1]
            elif line.startswith("INFO: removed max WAL segment"):
                max_wal = line[31:-1]
        for wal_name in os.listdir(os.path.join(backup_dir, 'wal', 'node')):
            if not wal_name.endswith(".backup"):
                #wal_name_b = wal_name.encode('ascii')
                self.assertEqual(wal_name[8:] > min_wal[8:], True)
                self.assertEqual(wal_name[8:] > max_wal[8:], True)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

#    @unittest.skip("123")
    def test_retention_window_2(self):
        """purge backups using window-based retention policy"""
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

        with open(os.path.join(backup_dir, 'backups', 'node', "pg_probackup.conf"), "a") as conf:
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
            with open(os.path.join(backups, backup, "backup.control"), "a") as conf:
                conf.write("recovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=days_delta)))
                days_delta -= 1

        # Make backup to be keeped
        self.backup_node(backup_dir, 'node', node, backup_type="page")

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 4)

        # Purge backups
        self.delete_expired(backup_dir, 'node')
        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 2)

        # Clean after yourself
        self.del_test_dir(module_name, fname)
