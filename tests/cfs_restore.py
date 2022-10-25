"""
restore
    Syntax:

    pg_probackup restore -B backupdir --instance instance_name
        [-D datadir]
        [ -i backup_id | [{--time=time | --xid=xid | --lsn=lsn } [--inclusive=boolean]]][--timeline=timeline] [-T OLDDIR=NEWDIR]
        [-j num_threads] [--progress] [-q] [-v]

"""
import os
import unittest
import shutil

from .helpers.cfs_helpers import find_by_name
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException, is_test_result_ok


module_name = 'cfs_restore'

tblspace_name = 'cfs_tblspace'
tblspace_name_new = 'cfs_tblspace_new'


class CfsRestoreBase(ProbackupTest, unittest.TestCase):
    def setUp(self):
        self.fname = self.id().split('.')[3]
        self.backup_dir = os.path.join(self.tmp_path, module_name, self.fname, 'backup')

        self.node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, self.fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
#                'ptrack_enable': 'on',
                'cfs_encryption': 'off',
            }
        )

        self.init_pb(self.backup_dir)
        self.add_instance(self.backup_dir, 'node', self.node)
        self.set_archiving(self.backup_dir, 'node', self.node)

        self.node.slow_start()
        self.create_tblspace_in_node(self.node, tblspace_name, cfs=True)

        self.add_data_in_cluster()

        self.backup_id = None
        try:
            self.backup_id = self.backup_node(self.backup_dir, 'node', self.node, backup_type='full')
        except ProbackupException as e:
            self.fail(
                "ERROR: Full backup failed \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )

    def add_data_in_cluster(self):
        pass

    @unittest.skipUnless(ProbackupTest.enterprise, 'skip')
    def tearDown(self):
        module_name = self.id().split('.')[1]
        fname = self.id().split('.')[3]
        if is_test_result_ok(self):
            self.del_test_dir(module_name, fname)


class CfsRestoreNoencEmptyTablespaceTest(CfsRestoreBase):
    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_restore_empty_tablespace_from_fullbackup(self):
        """
        Case: Restore empty tablespace from valid full backup.
        """
        self.node.stop(["-m", "immediate"])
        self.node.cleanup()
        shutil.rmtree(self.get_tblspace_path(self.node, tblspace_name))

        try:
            self.restore_node(self.backup_dir, 'node', self.node, backup_id=self.backup_id)
        except ProbackupException as e:
            self.fail(
                "ERROR: Restore failed. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )
        self.assertTrue(
            find_by_name([self.get_tblspace_path(self.node, tblspace_name)], ["pg_compression"]),
            "ERROR: Restored data is not valid. pg_compression not found in tablespace dir."
        )

        try:
            self.node.slow_start()
        except ProbackupException as e:
            self.fail(
                "ERROR: Instance not started after restore. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )
        tblspace = self.node.safe_psql(
            "postgres",
            "SELECT * FROM pg_tablespace WHERE spcname='{0}'".format(tblspace_name)
        ).decode("UTF-8")
        self.assertTrue(
            tblspace_name in tblspace and "compression=true" in tblspace,
            "ERROR: The tablespace not restored or it restored without compressions"
        )


class CfsRestoreNoencTest(CfsRestoreBase):
    def add_data_in_cluster(self):
        self.node.safe_psql(
            "postgres",
            'CREATE TABLE {0} TABLESPACE {1} \
                AS SELECT i AS id, MD5(i::text) AS text, \
                MD5(repeat(i::text,10))::tsvector AS tsvector \
                FROM generate_series(0,1e5) i'.format('t1', tblspace_name)
        )
        self.table_t1 = self.node.safe_psql(
            "postgres",
            "SELECT * FROM t1"
        )

    # --- Restore from full backup ---#
    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_restore_from_fullbackup_to_old_location(self):
        """
        Case: Restore instance from valid full backup to old location.
        """
        self.node.stop()
        self.node.cleanup()
        shutil.rmtree(self.get_tblspace_path(self.node, tblspace_name))

        try:
            self.restore_node(self.backup_dir, 'node', self.node, backup_id=self.backup_id)
        except ProbackupException as e:
            self.fail(
                "ERROR: Restore from full backup failed. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )

        self.assertTrue(
            find_by_name([self.get_tblspace_path(self.node, tblspace_name)], ['pg_compression']),
            "ERROR: File pg_compression not found in tablespace dir"
        )
        try:
            self.node.slow_start()
        except ProbackupException as e:
            self.fail(
                "ERROR: Instance not started after restore. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )

        self.assertEqual(
            repr(self.node.safe_psql("postgres", "SELECT * FROM %s" % 't1')),
            repr(self.table_t1)
        )

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_restore_from_fullbackup_to_old_location_3_jobs(self):
        """
        Case: Restore instance from valid full backup to old location.
        """
        self.node.stop()
        self.node.cleanup()
        shutil.rmtree(self.get_tblspace_path(self.node, tblspace_name))

        try:
            self.restore_node(self.backup_dir, 'node', self.node, backup_id=self.backup_id, options=['-j', '3'])
        except ProbackupException as e:
            self.fail(
                "ERROR: Restore from full backup failed. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )
        self.assertTrue(
            find_by_name([self.get_tblspace_path(self.node, tblspace_name)], ['pg_compression']),
            "ERROR: File pg_compression not found in backup dir"
        )
        try:
            self.node.slow_start()
        except ProbackupException as e:
            self.fail(
                "ERROR: Instance not started after restore. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )

        self.assertEqual(
            repr(self.node.safe_psql("postgres", "SELECT * FROM %s" % 't1')),
            repr(self.table_t1)
        )

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_restore_from_fullbackup_to_new_location(self):
        """
        Case: Restore instance from valid full backup to new location.
        """
        self.node.stop()
        self.node.cleanup()
        shutil.rmtree(self.get_tblspace_path(self.node, tblspace_name))

        node_new = self.make_simple_node(base_dir="{0}/{1}/node_new_location".format(module_name, self.fname))
        node_new.cleanup()

        try:
            self.restore_node(self.backup_dir, 'node', node_new, backup_id=self.backup_id)
            self.set_auto_conf(node_new, {'port': node_new.port})
        except ProbackupException as e:
            self.fail(
                "ERROR: Restore from full backup failed. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )
        self.assertTrue(
            find_by_name([self.get_tblspace_path(self.node, tblspace_name)], ['pg_compression']),
            "ERROR: File pg_compression not found in backup dir"
        )
        try:
            node_new.slow_start()
        except ProbackupException as e:
            self.fail(
                "ERROR: Instance not started after restore. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )

        self.assertEqual(
            repr(node_new.safe_psql("postgres", "SELECT * FROM %s" % 't1')),
            repr(self.table_t1)
        )
        node_new.cleanup()

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_restore_from_fullbackup_to_new_location_5_jobs(self):
        """
        Case: Restore instance from valid full backup to new location.
        """
        self.node.stop()
        self.node.cleanup()
        shutil.rmtree(self.get_tblspace_path(self.node, tblspace_name))

        node_new = self.make_simple_node(base_dir="{0}/{1}/node_new_location".format(module_name, self.fname))
        node_new.cleanup()

        try:
            self.restore_node(self.backup_dir, 'node', node_new, backup_id=self.backup_id, options=['-j', '5'])
            self.set_auto_conf(node_new, {'port': node_new.port})
        except ProbackupException as e:
            self.fail(
                "ERROR: Restore from full backup failed. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )
        self.assertTrue(
            find_by_name([self.get_tblspace_path(self.node, tblspace_name)], ['pg_compression']),
            "ERROR: File pg_compression not found in backup dir"
        )
        try:
            node_new.slow_start()
        except ProbackupException as e:
            self.fail(
                "ERROR: Instance not started after restore. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )

        self.assertEqual(
            repr(node_new.safe_psql("postgres", "SELECT * FROM %s" % 't1')),
            repr(self.table_t1)
        )
        node_new.cleanup()

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_restore_from_fullbackup_to_old_location_tablespace_new_location(self):
        self.node.stop()
        self.node.cleanup()
        shutil.rmtree(self.get_tblspace_path(self.node, tblspace_name))

        os.mkdir(self.get_tblspace_path(self.node, tblspace_name_new))

        try:
            self.restore_node(
                self.backup_dir,
                'node', self.node,
                backup_id=self.backup_id,
                options=["-T", "{0}={1}".format(
                        self.get_tblspace_path(self.node, tblspace_name),
                        self.get_tblspace_path(self.node, tblspace_name_new)
                    )
                ]
            )
        except ProbackupException as e:
            self.fail(
                "ERROR: Restore from full backup failed. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )
        self.assertTrue(
            find_by_name([self.get_tblspace_path(self.node, tblspace_name_new)], ['pg_compression']),
            "ERROR: File pg_compression not found in new tablespace location"
        )
        try:
            self.node.slow_start()
        except ProbackupException as e:
            self.fail(
                "ERROR: Instance not started after restore. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )

        self.assertEqual(
            repr(self.node.safe_psql("postgres", "SELECT * FROM %s" % 't1')),
            repr(self.table_t1)
        )

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_restore_from_fullbackup_to_old_location_tablespace_new_location_3_jobs(self):
        self.node.stop()
        self.node.cleanup()
        shutil.rmtree(self.get_tblspace_path(self.node, tblspace_name))

        os.mkdir(self.get_tblspace_path(self.node, tblspace_name_new))

        try:
            self.restore_node(
                self.backup_dir,
                'node', self.node,
                backup_id=self.backup_id,
                options=["-j", "3", "-T", "{0}={1}".format(
                        self.get_tblspace_path(self.node, tblspace_name),
                        self.get_tblspace_path(self.node, tblspace_name_new)
                    )
                ]
            )
        except ProbackupException as e:
            self.fail(
                "ERROR: Restore from full backup failed. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )
        self.assertTrue(
            find_by_name([self.get_tblspace_path(self.node, tblspace_name_new)], ['pg_compression']),
            "ERROR: File pg_compression not found in new tablespace location"
        )
        try:
            self.node.slow_start()
        except ProbackupException as e:
            self.fail(
                "ERROR: Instance not started after restore. \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )

        self.assertEqual(
            repr(self.node.safe_psql("postgres", "SELECT * FROM %s" % 't1')),
            repr(self.table_t1)
        )

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


#class CfsRestoreEncEmptyTablespaceTest(CfsRestoreNoencEmptyTablespaceTest):
#    # --- Begin --- #
#    def setUp(self):
#        os.environ["PG_CIPHER_KEY"] = "super_secret_cipher_key"
#        super(CfsRestoreNoencEmptyTablespaceTest, self).setUp()
#
#
#class CfsRestoreEncTest(CfsRestoreNoencTest):
#    # --- Begin --- #
#    def setUp(self):
#        os.environ["PG_CIPHER_KEY"] = "super_secret_cipher_key"
#        super(CfsRestoreNoencTest, self).setUp()
