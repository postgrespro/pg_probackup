import unittest
import os
import six
from time import sleep
from helpers.ptrack_helpers import ProbackupTest, ProbackupException
from testgres import stop_all


class BackupTest(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(BackupTest, self).__init__(*args, **kwargs)
        self.module_name = 'backup'

    @classmethod
    def tearDownClass(cls):
        stop_all()

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    # PGPRO-707
    def test_backup_modes_archive(self):
        """standart backup modes with ARCHIVE WAL method"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on'}
            )
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # full backup mode
        #with open(path.join(node.logs_dir, "backup_full.log"), "wb") as backup_log:
        #    backup_log.write(self.backup_node(node, options=["--verbose"]))

        backup_id = self.backup_node(backup_dir, 'node', node)
        show_backup = self.show_pb(backup_dir, 'node')[0]

        self.assertEqual(show_backup['Status'], six.b("OK"))
        self.assertEqual(show_backup['Mode'], six.b("FULL"))

        # postmaster.pid and postmaster.opts shouldn't be copied
        excluded = True
        db_dir = os.path.join(backup_dir, "backups", 'node', backup_id, "database")
        for f in os.listdir(db_dir):
            if os.path.isfile(os.path.join(db_dir, f)) \
            and (f == "postmaster.pid" or f == "postmaster.opts"):
                    excluded = False
        self.assertEqual(excluded, True)

        # page backup mode
        page_backup_id = self.backup_node(backup_dir, 'node', node, backup_type="page")

        # print self.show_pb(node)
        show_backup = self.show_pb(backup_dir, 'node')[1]
        self.assertEqual(show_backup['Status'], six.b("OK"))
        self.assertEqual(show_backup['Mode'], six.b("PAGE"))

        # Check parent backup
        self.assertEqual(
            backup_id,
            self.show_pb(backup_dir, 'node', backup_id=show_backup['ID'])["parent-backup-id"])

        # ptrack backup mode
        self.backup_node(backup_dir, 'node', node, backup_type="ptrack")

        show_backup = self.show_pb(backup_dir, 'node')[2]
        self.assertEqual(show_backup['Status'], six.b("OK"))
        self.assertEqual(show_backup['Mode'], six.b("PTRACK"))

        # Check parent backup
        self.assertEqual(
            page_backup_id,
            self.show_pb(backup_dir, 'node', backup_id=show_backup['ID'])["parent-backup-id"])

        node.stop()

    # @unittest.skip("skip")
    def test_smooth_checkpoint(self):
        """full backup with smooth checkpoint"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        self.backup_node(backup_dir, 'node' ,node, options=["-C"])
        self.assertEqual(self.show_pb(backup_dir, 'node')[0]['Status'], six.b("OK"))
        node.stop()

    #@unittest.skip("skip")
    def test_incremental_backup_without_full(self):
        """page-level backup without validated full backup"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on'}
            )
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        try:
            self.backup_node(backup_dir, 'node', node, backup_type="page")
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because page backup should not be possible without valid full backup.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertEqual(e.message,
                'ERROR: Valid backup on current timeline is not found. Create new FULL backup before an incremental one.\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        sleep(1)

        try:
            self.backup_node(backup_dir, 'node', node, backup_type="ptrack")
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because page backup should not be possible without valid full backup.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertEqual(e.message,
                'ERROR: Valid backup on current timeline is not found. Create new FULL backup before an incremental one.\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        self.assertEqual(self.show_pb(backup_dir, 'node')[0]['Status'], six.b("ERROR"))
        node.stop()

    @unittest.expectedFailure
    # Need to forcibly validate parent
    def test_incremental_backup_corrupt_full(self):
        """page-level backup with corrupted full backup"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on'}
            )
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        backup_id = self.backup_node(backup_dir, 'node', node)
        file = os.path.join(backup_dir, "backups", "node", backup_id.decode("utf-8"), "database", "postgresql.conf")
        os.remove(file)

        try:
            self.backup_node(backup_dir, 'node', node, backup_type="page")
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because page backup should not be possible without valid full backup.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertEqual(e.message,
                'ERROR: Valid backup on current timeline is not found. Create new FULL backup before an incremental one.\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

            sleep(1)
            self.assertEqual(1, 0, "Expecting Error because page backup should not be possible without valid full backup.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertEqual(e.message,
                'ERROR: Valid backup on current timeline is not found. Create new FULL backup before an incremental one.\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        self.assertEqual(self.show_pb(backup_dir, 'node')[0]['Status'], six.b("ERROR"))
        node.stop()

    # @unittest.skip("skip")
    def test_ptrack_threads(self):
        """ptrack multi thread backup mode"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on'}
            )
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        self.backup_node(backup_dir, 'node', node, backup_type="full", options=["-j", "4"])
        self.assertEqual(self.show_pb(backup_dir, 'node')[0]['Status'], six.b("OK"))

        self.backup_node(backup_dir, 'node', node, backup_type="ptrack", options=["-j", "4"])
        self.assertEqual(self.show_pb(backup_dir, 'node')[0]['Status'], six.b("OK"))

        node.stop()

    # @unittest.skip("skip")
    def test_ptrack_threads_stream(self):
        """ptrack multi thread backup mode and stream"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on', 'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.start()

        self.backup_node(backup_dir, 'node', node, backup_type="full", options=["-j", "4", "--stream"])

        self.assertEqual(self.show_pb(backup_dir, 'node')[0]['Status'], six.b("OK"))
        self.backup_node(backup_dir, 'node', node, backup_type="ptrack", options=["-j", "4", "--stream"])
        self.assertEqual(self.show_pb(backup_dir, 'node')[1]['Status'], six.b("OK"))
        node.stop()
