import os
import unittest

from .helpers.cfs_helpers import find_by_extensions, find_by_name
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException

module_name = 'cfs_backup_enc'
tblspace_name = 'cfs_tblspace'


class CfsBackupEncTest(ProbackupTest, unittest.TestCase):
    fname = None
    backup_dir = None
    node = None

# --- Begin --- #
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

# --- Section: Prepare --- #
    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_create_tblspace_compression(self):
        """
        Case: Check to create compression tablespace
        """
        self.create_tblspace_in_node(node, tblspace_name, True)
        tblspace = node.safe_psql(
            "postgres",
            "SELECT * FROM pg_tablespace WHERE spcname='{0}'".format(tblspace_name)
        )
        node.safe_psql(
            "postgres",
            'create table {0} tablespace {1} \
                as select i as id, md5(i::text) as text, \
                md5(repeat(i::text,10))::tsvector as tsvector \
                from generate_series(0,256) i'.format('t1', tblspace_name)
        )
        self.assertTrue(
            tblspace_name in tblspace and "compression=true" in tblspace,
            "ERROR: The tablespace not created or it create without compressions"
        )
        self.assertTrue(
            find_by_name([self.get_tblspace_path(node, tblspace_name)], ['pg_compression']),
            "ERROR: File pg_compression not found"
        )

# --- Section: Full --- #
    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_fullbackup_empty_tablespace(self):
        """
        Case: Check fullbackup empty compressed tablespace
        """
        self.create_tblspace_in_node(node, tblspace_name, True)

        backup_id = None
        try:
            backup_id = self.backup_node(backup_dir, 'node', node, backup_type='full')
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Full backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )
        show_backup = self.show_pb(backup_dir, 'node', backup_id)
        self.assertEqual(
            "OK",
            show_backup["status"],
            "ERROR: Full backup status is not valid. \n Current backup status={0}".format(show_backup["status"])
        )
        self.assertTrue(
            find_by_name([os.path.join(backup_dir, 'backups', 'node', backup_id)], ['pg_compression']),
            "ERROR: File pg_compression not found in backup dir"
        )

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_fullbackup_empty_tablespace_stream(self):
        """
        Case: Check fullbackup empty compressed tablespace with options stream
        """
        self.create_tblspace_in_node(node, tblspace_name, True)

        backup_id = None
        try:
            backup_id = self.backup_node(backup_dir, 'node', node, backup_type='full', options=['--stream'])
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Full backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )
        show_backup = self.show_pb(backup_dir, 'node', backup_id)
        self.assertEqual(
            "OK",
            show_backup["status"],
            "ERROR: Full backup status is not valid. \n Current backup status={0}".format(show_backup["status"])
        )
        self.assertTrue(
            find_by_name([os.path.join(backup_dir, 'backups', 'node', backup_id)], ['pg_compression']),
            "ERROR: File pg_compression not found in backup dir"
        )
        self.assertFalse(
            find_by_extensions([os.path.join(backup_dir, 'node', backup_id)], ['_ptrack']),
            "ERROR: _ptrack files found in backup dir"
        )

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    # PGPRO-1018 invalid file size
    def test_fullbackup_after_create_table(self):
        """
        Case: Make full backup after created table in the tablespace
        """

        self.create_tblspace_in_node(node, tblspace_name, True)

        node.safe_psql(
            "postgres",
            'CREATE TABLE {0} TABLESPACE {1} \
                AS SELECT i AS id, MD5(i::text) AS text, \
                MD5(repeat(i::text,10))::tsvector AS tsvector \
                FROM generate_series(0,256) i'.format('t1', tblspace_name)
        )

        backup_id = None
        try:
            backup_id = self.backup_node(backup_dir, 'node', node, backup_type='full')
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Full backup wasn't created.\n {0} \n {0}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )
        show_backup = self.show_pb(backup_dir, 'node', backup_id)
        self.assertEqual(
            "OK",
            show_backup["status"],
            "ERROR: Full backup status is not valid. \n Current backup status={0}".format(show_backup["status"])
        )
        self.assertTrue(
            find_by_name([os.path.join(backup_dir, 'backups', 'node', backup_id)], ['pg_compression']),
            "ERROR: File pg_compression not found in {0}".format(os.path.join(backup_dir, 'node', backup_id))
        )
        self.assertTrue(
            find_by_extensions([os.path.join(backup_dir, 'node', backup_id)], ['.cmf']),
            "ERROR: .cmf files not found in backup dir"
        )
        self.assertFalse(
            find_by_extensions([os.path.join(backup_dir, 'node', backup_id)], ['_ptrack']),
            "ERROR: _ptrack files was found in backup dir"
        )

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    # PGPRO-1018 invalid file size
    def test_fullbackup_after_create_table_stream(self):
        """
        Case: Make full backup after created table in the tablespace with option --stream
        """

        self.create_tblspace_in_node(node, tblspace_name, True)

        node.safe_psql(
            "postgres",
            'CREATE TABLE {0} TABLESPACE {1} \
                AS SELECT i AS id, MD5(i::text) AS text, \
                MD5(repeat(i::text,10))::tsvector AS tsvector \
                FROM generate_series(0,256) i'.format('t1', tblspace_name)
        )

        backup_id = None
        try:
            backup_id = self.backup_node(backup_dir, 'node', node, backup_type='full', options=['--stream'])
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Full backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )
        show_backup = self.show_pb(backup_dir, 'node', backup_id)
        self.assertEqual(
            "OK",
            show_backup["status"],
            "ERROR: Full backup status is not valid. \n Current backup status={0}".format(show_backup["status"])
        )
        self.assertTrue(
            find_by_name([os.path.join(backup_dir, 'backups', 'node', backup_id)], ['pg_compression']),
            "ERROR: File pg_compression not found in backup dir"
        )
        self.assertTrue(
            find_by_extensions([os.path.join(backup_dir, 'node', backup_id)], ['.cmf']),
            "ERROR: .cmf files not found in backup dir"
        )
        self.assertFalse(
            find_by_extensions([os.path.join(backup_dir, 'node', backup_id)], ['_ptrack']),
            "ERROR: _ptrack files was found in backup dir"
        )

# --- Section: Incremental from empty tablespace --- #
    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_fullbackup_empty_tablespace_ptrack_after_create_table(self):
        """
        Case: Make full backup before created table in the tablespace.
                Make ptrack backup after create table
        """

        self.create_tblspace_in_node(node, tblspace_name, True)
        try:
            self.backup_node(backup_dir, 'node', node, backup_type='full')
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Full backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )

        node.safe_psql(
            "postgres",
            'CREATE TABLE {0} TABLESPACE {1} \
                AS SELECT i AS id, MD5(i::text) AS text, \
                MD5(repeat(i::text,10))::tsvector AS tsvector \
                FROM generate_series(0,256) i'.format('t1', tblspace_name)
        )

        backup_id = None
        try:
            backup_id = self.backup_node(backup_dir, 'node', node, backup_type='ptrack')
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Incremental backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )
        show_backup = self.show_pb(backup_dir, 'node', backup_id)
        self.assertEqual(
            "OK",
            show_backup["status"],
            "ERROR: Incremental backup status is not valid. \n Current backup status={0}".format(show_backup["status"])
        )
        self.assertTrue(
            find_by_name([self.get_tblspace_path(node, tblspace_name)], ['pg_compression']),
            "ERROR: File pg_compression not found"
        )
        self.assertTrue(
            find_by_extensions([os.path.join(backup_dir, 'node', backup_id)], ['.cmf']),
            "ERROR: .cmf files not found in backup dir"
        )
        self.assertFalse(
            find_by_extensions([os.path.join(backup_dir, 'node', backup_id)], ['_ptrack']),
            "ERROR: _ptrack files was found in backup dir"
        )

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_fullbackup_empty_tablespace_ptrack_after_create_table_stream(self):
        """
        Case: Make full backup before created table in the tablespace.
                Make ptrack backup after create table
        """

        self.create_tblspace_in_node(node, tblspace_name, True)
        try:
            self.backup_node(backup_dir, 'node', node, backup_type='full', options=['--stream'])
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Full backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )

        node.safe_psql(
            "postgres",
            'CREATE TABLE {0} TABLESPACE {1} \
                AS SELECT i AS id, MD5(i::text) AS text, \
                MD5(repeat(i::text,10))::tsvector AS tsvector \
                FROM generate_series(0,256) i'.format('t1', tblspace_name)
        )

        backup_id = None
        try:
            backup_id = self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=['--stream'])
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Incremental backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )
        show_backup = self.show_pb(backup_dir, 'node', backup_id)
        self.assertEqual(
            "OK",
            show_backup["status"],
            "ERROR: Incremental backup status is not valid. \n Current backup status={0}".format(show_backup["status"])
        )
        self.assertTrue(
            find_by_name([self.get_tblspace_path(node, tblspace_name)], ['pg_compression']),
            "ERROR: File pg_compression not found"
        )
        self.assertTrue(
            find_by_extensions([os.path.join(backup_dir, 'node', backup_id)], ['.cmf']),
            "ERROR: .cmf files not found in backup dir"
        )
        self.assertFalse(
            find_by_extensions([os.path.join(backup_dir, 'node', backup_id)], ['_ptrack']),
            "ERROR: _ptrack files was found in backup dir"
        )

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_fullbackup_empty_tablespace_page_after_create_table(self):
        """
        Case: Make full backup before created table in the tablespace.
                Make page backup after create table
        """

        self.create_tblspace_in_node(node, tblspace_name, True)
        try:
            self.backup_node(backup_dir, 'node', node, backup_type='full')
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Full backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )

        node.safe_psql(
            "postgres",
            'CREATE TABLE {0} TABLESPACE {1} \
                AS SELECT i AS id, MD5(i::text) AS text, \
                MD5(repeat(i::text,10))::tsvector AS tsvector \
                FROM generate_series(0,256) i'.format('t1', tblspace_name)
        )

        backup_id = None
        try:
            backup_id = self.backup_node(backup_dir, 'node', node, backup_type='page')
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Incremental backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )
        show_backup = self.show_pb(backup_dir, 'node', backup_id)
        self.assertEqual(
            "OK",
            show_backup["status"],
            "ERROR: Incremental backup status is not valid. \n Current backup status={0}".format(show_backup["status"])
        )
        self.assertTrue(
            find_by_name([self.get_tblspace_path(node, tblspace_name)], ['pg_compression']),
            "ERROR: File pg_compression not found"
        )
        self.assertTrue(
            find_by_extensions([os.path.join(backup_dir, 'node', backup_id)], ['.cmf']),
            "ERROR: .cmf files not found in backup dir"
        )
        self.assertFalse(
            find_by_extensions([os.path.join(backup_dir, 'node', backup_id)], ['_ptrack']),
            "ERROR: _ptrack files was found in backup dir"
        )

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_fullbackup_empty_tablespace_page_after_create_table_stream(self):
        """
        Case: Make full backup before created table in the tablespace.
                Make page backup after create table
        """

        self.create_tblspace_in_node(node, tblspace_name, True)
        try:
            self.backup_node(backup_dir, 'node', node, backup_type='full', options=['--stream'])
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Full backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )

        node.safe_psql(
            "postgres",
            'CREATE TABLE {0} TABLESPACE {1} \
                AS SELECT i AS id, MD5(i::text) AS text, \
                MD5(repeat(i::text,10))::tsvector AS tsvector \
                FROM generate_series(0,256) i'.format('t1', tblspace_name)
        )

        backup_id = None
        try:
            backup_id = self.backup_node(backup_dir, 'node', node, backup_type='page', options=['--stream'])
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Incremental backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )
        show_backup = self.show_pb(backup_dir, 'node', backup_id)
        self.assertEqual(
            "OK",
            show_backup["status"],
            "ERROR: Incremental backup status is not valid. \n Current backup status={0}".format(show_backup["status"])
        )
        self.assertTrue(
            find_by_name([self.get_tblspace_path(node, tblspace_name)], ['pg_compression']),
            "ERROR: File pg_compression not found"
        )
        self.assertTrue(
            find_by_extensions([os.path.join(backup_dir, 'node', backup_id)], ['.cmf']),
            "ERROR: .cmf files not found in backup dir"
        )
        self.assertFalse(
            find_by_extensions([os.path.join(backup_dir, 'node', backup_id)], ['_ptrack']),
            "ERROR: _ptrack files was found in backup dir"
        )

# --- Section: Incremental from fill tablespace --- #
    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_fullbackup_after_create_table_ptrack_after_create_table(self):
        """
        Case:   Make full backup before created table in the tablespace.
                Make ptrack backup after create table.
                Check: incremental backup will not greater as full
        """

        self.create_tblspace_in_node(node, tblspace_name, True)
        node.safe_psql(
            "postgres",
            'CREATE TABLE {0} TABLESPACE {1} \
                AS SELECT i AS id, MD5(i::text) AS text, \
                MD5(repeat(i::text,10))::tsvector AS tsvector \
                FROM generate_series(0,256) i'.format('t1', tblspace_name)
        )

        backup_id_full = None
        try:
            backup_id_full = self.backup_node(backup_dir, 'node', node, backup_type='full')
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Full backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )

        node.safe_psql(
            "postgres",
            'CREATE TABLE {0} TABLESPACE {1} \
                AS SELECT i AS id, MD5(i::text) AS text, \
                MD5(repeat(i::text,10))::tsvector AS tsvector \
                FROM generate_series(0,25) i'.format('t2', tblspace_name)
        )

        backup_id_ptrack = None
        try:
            backup_id_ptrack = self.backup_node(backup_dir, 'node', node, backup_type='ptrack')
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Incremental backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )

        show_backup_full = self.show_pb(backup_dir, 'node', backup_id_full)
        show_backup_ptrack = self.show_pb(backup_dir, 'node', backup_id_ptrack)
        self.assertGreater(
            show_backup_ptrack["data-bytes"],
            show_backup_full["data-bytes"],
            "ERROR: Size of incremental backup greater as full. \n INFO: {0} >{1}".format(
                show_backup_ptrack["data-bytes"],
                show_backup_full["data-bytes"]
            )
        )

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_fullbackup_after_create_table_ptrack_after_create_table_stream(self):
        """
        Case:   Make full backup before created table in the tablespace (--stream).
                Make ptrack backup after create table (--stream).
                Check: incremental backup will not greater as full
        """

        self.create_tblspace_in_node(node, tblspace_name, True)
        node.safe_psql(
            "postgres",
            'CREATE TABLE {0} TABLESPACE {1} \
                AS SELECT i AS id, MD5(i::text) AS text, \
                MD5(repeat(i::text,10))::tsvector AS tsvector \
                FROM generate_series(0,256) i'.format('t1', tblspace_name)
        )

        backup_id_full = None
        try:
            backup_id_full = self.backup_node(backup_dir, 'node', node, backup_type='full', options=['--stream'])
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Full backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )

        node.safe_psql(
            "postgres",
            'CREATE TABLE {0} TABLESPACE {1} \
                AS SELECT i AS id, MD5(i::text) AS text, \
                MD5(repeat(i::text,10))::tsvector AS tsvector \
                FROM generate_series(0,25) i'.format('t2', tblspace_name)
        )

        backup_id_ptrack = None
        try:
            backup_id_ptrack = self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=['--stream'])
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Incremental backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )

        show_backup_full = self.show_pb(backup_dir, 'node', backup_id_full)
        show_backup_ptrack = self.show_pb(backup_dir, 'node', backup_id_ptrack)
        self.assertGreater(
            show_backup_ptrack["data-bytes"],
            show_backup_full["data-bytes"],
            "ERROR: Size of incremental backup greater as full. \n INFO: {0} >{1}".format(
                show_backup_ptrack["data-bytes"],
                show_backup_full["data-bytes"]
            )
        )

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_fullbackup_after_create_table_page_after_create_table(self):
        """
        Case:   Make full backup before created table in the tablespace.
                Make ptrack backup after create table.
                Check: incremental backup will not greater as full
        """

        self.create_tblspace_in_node(node, tblspace_name, True)
        node.safe_psql(
            "postgres",
            'CREATE TABLE {0} TABLESPACE {1} \
                AS SELECT i AS id, MD5(i::text) AS text, \
                MD5(repeat(i::text,10))::tsvector AS tsvector \
                FROM generate_series(0,256) i'.format('t1', tblspace_name)
        )

        backup_id_full = None
        try:
            backup_id_full = self.backup_node(backup_dir, 'node', node, backup_type='full')
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Full backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )

        node.safe_psql(
            "postgres",
            'CREATE TABLE {0} TABLESPACE {1} \
                AS SELECT i AS id, MD5(i::text) AS text, \
                MD5(repeat(i::text,10))::tsvector AS tsvector \
                FROM generate_series(0,25) i'.format('t2', tblspace_name)
        )

        backup_id_page = None
        try:
            backup_id_page = self.backup_node(backup_dir, 'node', node, backup_type='page')
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Incremental backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )

        show_backup_full = self.show_pb(backup_dir, 'node', backup_id_full)
        show_backup_page = self.show_pb(backup_dir, 'node', backup_id_page)
        self.assertGreater(
            show_backup_page["data-bytes"],
            show_backup_full["data-bytes"],
            "ERROR: Size of incremental backup greater as full. \n INFO: {0} >{1}".format(
                show_backup_page["data-bytes"],
                show_backup_full["data-bytes"]
            )
        )

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_fullbackup_after_create_table_page_after_create_table_stream(self):
        """
        Case:   Make full backup before created table in the tablespace (--stream).
                Make ptrack backup after create table (--stream).
                Check: incremental backup will not greater as full
        """

        self.create_tblspace_in_node(node, tblspace_name, True)
        node.safe_psql(
            "postgres",
            'CREATE TABLE {0} TABLESPACE {1} \
                AS SELECT i AS id, MD5(i::text) AS text, \
                MD5(repeat(i::text,10))::tsvector AS tsvector \
                FROM generate_series(0,256) i'.format('t1', tblspace_name)
        )

        backup_id_full = None
        try:
            backup_id_full = self.backup_node(backup_dir, 'node', node, backup_type='full', options=['--stream'])
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Full backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )

        node.safe_psql(
            "postgres",
            'CREATE TABLE {0} TABLESPACE {1} \
                AS SELECT i AS id, MD5(i::text) AS text, \
                MD5(repeat(i::text,10))::tsvector AS tsvector \
                FROM generate_series(0,25) i'.format('t2', tblspace_name)
        )

        backup_id_page = None
        try:
            backup_id_page = self.backup_node(backup_dir, 'node', node, backup_type='page', options=['--stream'])
        except ProbackupException as e:
            self.assertTrue(
                False,
                "ERROR: Incremental backup wasn't created.\n {0} \n {1}".format(
                    str(e.cmd),
                    str(e.message)
                )
            )

        show_backup_full = self.show_pb(backup_dir, 'node', backup_id_full)
        show_backup_page = self.show_pb(backup_dir, 'node', backup_id_page)
        self.assertGreater(
            show_backup_page["data-bytes"],
            show_backup_full["data-bytes"],
            "ERROR: Size of incremental backup greater as full. \n INFO: {0} >{1}".format(
                show_backup_page["data-bytes"],
                show_backup_full["data-bytes"]
            )
        )

# --- Make backup with not valid data (broken .cfm) --- #
    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_delete_random_cfm_file_from_tablespace_dir(self):
        pass

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_delete_file_pg_compression_from_tablespace_dir(self):
        pass

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_delete_random_data_file_from_tablespace_dir(self):
        pass

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_broken_random_cfm_file_into_tablespace_dir(self):
        pass

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_broken_random_data_file_into_tablespace_dir(self):
        pass

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_broken_file_pg_compression_into_tablespace_dir(self):
        pass

# --- Validation backup --- #
    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_delete_random_cfm_file_from_backup_dir(self):
        pass

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_delete_file_pg_compression_from_backup_dir(self):
        pass

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_delete_random_data_file_from_backup_dir(self):
        pass

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_broken_random_cfm_file_into_backup_dir(self):
        pass

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_broken_random_data_file_into_backup_dir(self):
        pass

    # @unittest.expectedFailure
    @unittest.skip("skip")
    def test_broken_file_pg_compression_into_backup_dir(self):
        pass

# --- end ---#
    def tearDown(self):
        node.cleanup()
        self.del_test_dir(module_name, fname)
