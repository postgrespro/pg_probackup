import unittest
import os
from datetime import datetime, timedelta
from os import path, listdir
from .ptrack_helpers import ProbackupTest
from testgres import stop_all


class RetentionTest(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(RetentionTest, self).__init__(*args, **kwargs)

    @classmethod
    def tearDownClass(cls):
        stop_all()

#    @unittest.skip("123")
    def test_retention_redundancy_1(self):
        """purge backups using redundancy-based retention policy"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/retention/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )

        node.start()

        self.init_pb(node)
        with open(path.join(self.backup_dir(node), "pg_probackup.conf"), "a") as conf:
            conf.write("retention-redundancy = 1\n")

        # Make backups to be purged
        self.backup_pb(node)
        self.backup_pb(node, backup_type="page")
        # Make backups to be keeped
        self.backup_pb(node)
        self.backup_pb(node, backup_type="page")

        self.assertEqual(len(self.show_pb(node)), 4)

        # Purge backups
        log = self.delete_expired(node)
        self.assertEqual(len(self.show_pb(node)), 2)

        # Check that WAL segments were deleted
        min_wal = None
        max_wal = None
        for line in log.splitlines():
            if line.startswith(b"INFO: removed min WAL segment"):
                min_wal = line[31:-1]
            elif line.startswith(b"INFO: removed max WAL segment"):
                max_wal = line[31:-1]
        for wal_name in listdir(path.join(self.backup_dir(node), "wal")):
            if not wal_name.endswith(".backup"):
                wal_name_b = wal_name.encode('ascii')
                self.assertEqual(wal_name_b[8:] > min_wal[8:], True)
                self.assertEqual(wal_name_b[8:] > max_wal[8:], True)

        node.stop()

#    @unittest.skip("123")
    def test_retention_window_2(self):
        """purge backups using window-based retention policy"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/retention/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )

        node.start()

        self.init_pb(node)
        with open(path.join(self.backup_dir(node), "pg_probackup.conf"), "a") as conf:
            conf.write("retention-redundancy = 1\n")
            conf.write("retention-window = 1\n")

        # Make backups to be purged
        self.backup_pb(node)
        self.backup_pb(node, backup_type="page")
        # Make backup to be keeped
        self.backup_pb(node)

        backups = path.join(self.backup_dir(node), "backups")
        days_delta = 5
        for backup in listdir(backups):
            with open(path.join(backups, backup, "backup.control"), "a") as conf:
                conf.write("recovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=days_delta)))
                days_delta -= 1

        # Make backup to be keeped
        self.backup_pb(node, backup_type="page")

        self.assertEqual(len(self.show_pb(node)), 4)

        # Purge backups
        self.delete_expired(node)
        self.assertEqual(len(self.show_pb(node)), 2)

        node.stop()
