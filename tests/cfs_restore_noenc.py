"""
restore
    Syntax:

    pg_probackup restore -B backupdir --instance instance_name
        [-D datadir]
        [ -i backup_id | [{--time=time | --xid=xid } [--inclusive=boolean]]][--timeline=timeline] [-T OLDDIR=NEWDIR]
        [-j num_threads] [--progress] [-q] [-v]

"""
import os
import unittest
import shutil

from .helpers.cfs_helpers import find_by_extensions, find_by_name
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException


module_name = 'cfs_restore_noenc'

tblspace_name = 'cfs_tblspace_noenc'
tblspace_name_new = 'cfs_tblspace_new'


class CfsRestoreNoencEmptyTablespaceTest(ProbackupTest, unittest.TestCase):
    fname = None
    backup_dir = None
    node = None

    def setUp(self):
        global fname
        global backup_dir
        global node

        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')

        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'ptrack_enable': 'on',
                'cfs_encryption': 'off',
                'max_wal_senders': '2'
            }
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)

        node.start()

        self.create_tblspace_in_node(node, tblspace_name, True)
        self.backup_id = None
        try:
            self.backup_id = self.backup_node(backup_dir, 'node', node, backup_type='full')
        except ProbackupException as e:
            self.fail(
                "ERROR: Full backup failed \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_restore_empty_tablespace_from_fullbackup(self):
        """
        Case: Restore empty tablespace from valid full backup.
        """
        node.stop({"-m": "immediate"})
        node.cleanup()
        shutil.rmtree(self.get_tblspace_path(node, tblspace_name))

        try:
            self.restore_node(backup_dir, 'node', node, backup_id=self.backup_id)
        except ProbackupException as e:
            self.fail(
                "ERROR: Restore failed. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )
        self.assertTrue(
            find_by_name([self.get_tblspace_path(node, tblspace_name)], ["pg_compression"]),
            "ERROR: Restored data is not valid. pg_compression not found in tablespace dir."
        )

        try:
            node.start()
        except ProbackupException as e:
            self.fail(
                "ERROR: Instance not started after restore. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )
        tblspace = node.safe_psql(
            "postgres",
            "SELECT * FROM pg_tablespace WHERE spcname='{0}'".format(tblspace_name)
        )
        self.assertTrue(
            tblspace_name in tblspace and "compression=true" in tblspace,
            "ERROR: The tablespace not restored or it restored without compressions"
        )

    def tearDown(self):
        node.cleanup()
        self.del_test_dir(module_name, fname)


class CfsRestoreNoencTest(ProbackupTest, unittest.TestCase):
    fname = None
    backup_dir = None
    node = None

    def setUp(self):
        global fname
        global backup_dir
        global node

        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')

        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'ptrack_enable': 'on',
                'cfs_encryption': 'off',
                'max_wal_senders': '2'
            }
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)

        node.start()

        self.create_tblspace_in_node(node, tblspace_name, True)

        node.safe_psql(
            "postgres",
            'CREATE TABLE {0} TABLESPACE {1} \
                AS SELECT i AS id, MD5(i::text) AS text, \
                MD5(repeat(i::text,10))::tsvector AS tsvector \
                FROM generate_series(0,1e5) i'.format('t1', tblspace_name)
        )
        self.table_t1 = node.safe_psql(
            "postgres",
            "SELECT * FROM t1"
        )
        self.backup_id = None
        try:
            self.backup_id = self.backup_node(backup_dir, 'node', node, backup_type='full')
        except ProbackupException as e:
            self.fail(
                "ERROR: Full backup failed \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )

# --- Restore from full backup ---#
    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_restore_from_fullbackup_to_old_location(self):
        """
        Case: Restore instance from valid full backup to old location.
        """
        node.stop(['-m', 'immediate'])
        node.cleanup()
        shutil.rmtree(self.get_tblspace_path(node, tblspace_name))

        try:
            self.restore_node(backup_dir, 'node', node, backup_id=self.backup_id)
        except ProbackupException as e:
            self.fail(
                "ERROR: Restore from full backup failed. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )
        self.assertTrue(
            find_by_name([self.get_tblspace_path(node, tblspace_name)], ['pg_compression']),
            "ERROR: File pg_compression not found in backup dir"
        )
        try:
            node.start()
        except ProbackupException as e:
            self.fail(
                "ERROR: Instance not started after restore. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )

        self.assertEqual(
            repr(node.safe_psql("postgres", "SELECT * FROM %s" % 't1')),
            repr(self.table_t1)
        )

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_restore_from_fullbackup_to_old_location_3_jobs(self):
        """
        Case: Restore instance from valid full backup to old location.
        """
        node.stop(['-m', 'immediate'])
        node.cleanup()
        shutil.rmtree(self.get_tblspace_path(node, tblspace_name))

        try:
            self.restore_node(backup_dir, 'node', node, backup_id=self.backup_id, options=['-j', '3'])
        except ProbackupException as e:
            self.fail(
                "ERROR: Restore from full backup failed. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )
        self.assertTrue(
            find_by_name([self.get_tblspace_path(node,tblspace_name)], ['pg_compression']),
            "ERROR: File pg_compression not found in backup dir"
        )
        try:
            node.start()
        except ProbackupException as e:
            self.fail(
                "ERROR: Instance not started after restore. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )

        self.assertEqual(
            repr(node.safe_psql("postgres", "SELECT * FROM %s" % 't1')),
            repr(self.table_t1)
        )

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_restore_from_fullbackup_to_new_location(self):
        """
        Case: Restore instance from valid full backup to new location.
        """
        node.stop(['-m', 'immediate'])
        node.cleanup()
        shutil.rmtree(self.get_tblspace_path(node, tblspace_name))

        node_new = self.make_simple_node(base_dir="{0}/{1}/node_new_location".format(module_name, fname))

        try:
            self.restore_node(backup_dir, 'node', node_new, backup_id=self.backup_id)
        except ProbackupException as e:
            self.fail(
                "ERROR: Restore from full backup failed. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )
        self.assertTrue(
            find_by_name([self.get_tblspace_path(node_new,tblspace_name)], ['pg_compression']),
            "ERROR: File pg_compression not found in backup dir"
        )
        try:
            node_new.start()
        except ProbackupException as e:
            self.fail(
                "ERROR: Instance not started after restore. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )

        self.assertEqual(
            repr(node.safe_psql("postgres", "SELECT * FROM %s" % 't1')),
            repr(self.table_t1)
        )
        node_new.cleanup()

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_restore_from_fullbackup_to_new_location_5_jobs(self):
        """
        Case: Restore instance from valid full backup to new location.
        """
        node.stop(['-m', 'immediate'])
        node.cleanup()
        shutil.rmtree(self.get_tblspace_path(node, tblspace_name))

        node_new = self.make_simple_node(base_dir="{0}/{1}/node_new_location".format(module_name, fname))

        try:
            self.restore_node(backup_dir, 'node', node_new, backup_id=self.backup_id, options=['-j', '5'])
        except ProbackupException as e:
            self.fail(
                "ERROR: Restore from full backup failed. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )
        self.assertTrue(
            find_by_name([self.get_tblspace_path(node_new,tblspace_name)], ['pg_compression']),
            "ERROR: File pg_compression not found in backup dir"
        )
        try:
            node_new.start()
        except ProbackupException as e:
            self.fail(
                "ERROR: Instance not started after restore. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )

        self.assertEqual(
            repr(node.safe_psql("postgres", "SELECT * FROM %s" % 't1')),
            repr(self.table_t1)
        )
        node_new.cleanup()

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_restore_from_fullbackup_to_old_location_tablespace_new_location(self):
        pass

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_restore_from_fullbackup_to_old_location_tablespace_new_location_3_jobs(self):
        pass

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_restore_from_fullbackup_to_new_location_tablespace_new_location(self):
        pass

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_restore_from_fullbackup_to_new_location_tablespace_new_location_5_jobs(self):
        pass

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_restore_from_ptrack(self):
        """
        Case: Restore from backup to old location
        """
        pass

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_restore_from_ptrack_jobs(self):
        """
        Case: Restore from backup to old location, four jobs
        """
        pass

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_restore_from_ptrack_new_jobs(self):
        pass

# --------------------------------------------------------- #
    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_restore_from_page(self):
        """
        Case: Restore from backup to old location
        """
        pass

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_restore_from_page_jobs(self):
        """
        Case: Restore from backup to old location, four jobs
        """
        pass

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_restore_from_page_new_jobs(self):
        """
        Case: Restore from backup to new location, four jobs
        """
        pass

    def tearDown(self):
        node.cleanup()
        self.del_test_dir(module_name, fname)
