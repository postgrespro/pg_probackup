import unittest
import os
from os import path
import six
from .ptrack_helpers import ProbackupTest
from testgres import stop_all


class OptionTest(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(OptionTest, self).__init__(*args, **kwargs)

    @classmethod
    def tearDownClass(cls):
        stop_all()

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def show_test_1(self):
        """Status DONE and OK"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/show/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))

        self.assertEqual(
            self.backup_pb(node, options=["--quiet"]),
            None
        )
        self.assertIn(six.b("OK"), self.show_pb(node, as_text=True))
        node.stop()

    def test_corrupt_2(self):
        """Status CORRUPT"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/show/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        id_backup = self.backup_pb(node)

        path.join(self.backup_dir(node), "backups", id_backup.decode("utf-8"), "database", "postgresql.conf")
        os.remove(path.join(self.backup_dir(node), "backups", id_backup.decode("utf-8"), "database", "postgresql.conf"))

        self.validate_pb(node, id_backup)
        self.assertIn(six.b("CORRUPT"), self.show_pb(node, as_text=True))
        node.stop()
