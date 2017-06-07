import unittest
from os import path, listdir
import six
from helpers.ptrack_helpers import ProbackupTest, ProbackupException
from testgres import stop_all


class BackupTest(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(BackupTest, self).__init__(*args, **kwargs)

    @classmethod
    def tearDownClass(cls):
        stop_all()

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    # PGPRO-707
    def test_backup_modes_archive(self):
        """standart backup modes with ARCHIVE WAL method"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/backup/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))

        # full backup mode
        #with open(path.join(node.logs_dir, "backup_full.log"), "wb") as backup_log:
        #    backup_log.write(self.backup_pb(node, options=["--verbose"]))

        self.backup_pb(node)

        show_backup = self.show_pb(node)[0]
        full_backup_id = show_backup['ID']
        self.assertEqual(show_backup['Status'], six.b("OK"))
        self.assertEqual(show_backup['Mode'], six.b("FULL"))

        # postmaster.pid and postmaster.opts shouldn't be copied
        excluded = True
        backups_dir = path.join(self.backup_dir(node), "backups")
        for backup in listdir(backups_dir):
            db_dir = path.join(backups_dir, backup, "database")
            for f in listdir(db_dir):
                if path.isfile(path.join(db_dir, f)) and \
                    (f == "postmaster.pid" or f == "postmaster.opts"):
                    excluded = False
        self.assertEqual(excluded, True)

        # page backup mode
        self.backup_pb(node, backup_type="page")

        # print self.show_pb(node)
        show_backup = self.show_pb(node)[1]
        self.assertEqual(show_backup['Status'], six.b("OK"))
        self.assertEqual(show_backup['Mode'], six.b("PAGE"))

        # Check parent backup
        self.assertEqual(
            full_backup_id,
            self.show_pb(node, id=show_backup['ID'])["parent-backup-id"])

        # ptrack backup mode
        self.backup_pb(node, backup_type="ptrack")

        show_backup = self.show_pb(node)[2]
        self.assertEqual(show_backup['Status'], six.b("OK"))
        self.assertEqual(show_backup['Mode'], six.b("PTRACK"))

        node.stop()

    def test_smooth_checkpoint(self):
        """full backup with smooth checkpoint"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/backup/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))

        self.backup_pb(node, options=["-C"])

        self.assertEqual(self.show_pb(node)[0]['Status'], six.b("OK"))

        node.stop()

    def test_page_backup_without_full(self):
        """page-level backup without validated full backup"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/backup/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))

        try:
            self.backup_pb(node, backup_type="page")
        except ProbackupException, e:
            pass
        self.assertEqual(self.show_pb(node)[0]['Status'], six.b("ERROR"))

        node.stop()

    def test_ptrack_threads(self):
        """ptrack multi thread backup mode"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/backup/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', "ptrack_enable": "on", 'max_wal_senders': '2'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))

        self.backup_pb(node, backup_type="full", options=["-j", "4"])

        self.assertEqual(self.show_pb(node)[0]['Status'], six.b("OK"))

        with open(path.join(node.logs_dir, "backup_ptrack.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="ptrack", options=["-j", "4"]))

        self.assertEqual(self.show_pb(node)[0]['Status'], six.b("OK"))

        node.stop()

    def test_ptrack_threads_stream(self):
        """ptrack multi thread backup mode and stream"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/backup/{0}".format(fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on', 'max_wal_senders': '2'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))

        self.backup_pb(node, backup_type="full", options=["-j", "4", "--stream"])

        self.assertEqual(self.show_pb(node)[0]['Status'], six.b("OK"))

        self.backup_pb(node, backup_type="ptrack", options=["-j", "4", "--stream"])

        self.assertEqual(self.show_pb(node)[1]['Status'], six.b("OK"))
        node.stop()
