import unittest
import os
from time import sleep
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
import shutil
from distutils.dir_util import copy_tree
from testgres import ProcessType


module_name = 'backup'


class BackupTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    # PGPRO-707
    def test_backup_modes_archive(self):
        """standart backup modes with ARCHIVE WAL method"""
        if not self.ptrack:
            return unittest.skip('Skipped because ptrack support is disabled')

        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={
                'ptrack_enable': 'on'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        backup_id = self.backup_node(backup_dir, 'node', node)
        show_backup = self.show_pb(backup_dir, 'node')[0]

        self.assertEqual(show_backup['status'], "OK")
        self.assertEqual(show_backup['backup-mode'], "FULL")

        # postmaster.pid and postmaster.opts shouldn't be copied
        excluded = True
        db_dir = os.path.join(
            backup_dir, "backups", 'node', backup_id, "database")

        for f in os.listdir(db_dir):
            if (
                os.path.isfile(os.path.join(db_dir, f)) and
                (
                    f == "postmaster.pid" or
                    f == "postmaster.opts"
                )
            ):
                excluded = False
                self.assertEqual(excluded, True)

        # page backup mode
        page_backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type="page")

        # print self.show_pb(node)
        show_backup = self.show_pb(backup_dir, 'node')[1]
        self.assertEqual(show_backup['status'], "OK")
        self.assertEqual(show_backup['backup-mode'], "PAGE")

        # Check parent backup
        self.assertEqual(
            backup_id,
            self.show_pb(
                backup_dir, 'node',
                backup_id=show_backup['id'])["parent-backup-id"])

        # ptrack backup mode
        self.backup_node(backup_dir, 'node', node, backup_type="ptrack")

        show_backup = self.show_pb(backup_dir, 'node')[2]
        self.assertEqual(show_backup['status'], "OK")
        self.assertEqual(show_backup['backup-mode'], "PTRACK")

        # Check parent backup
        self.assertEqual(
            page_backup_id,
            self.show_pb(
                backup_dir, 'node',
                backup_id=show_backup['id'])["parent-backup-id"])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_smooth_checkpoint(self):
        """full backup with smooth checkpoint"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(
            backup_dir, 'node', node,
            options=["-C"])
        self.assertEqual(self.show_pb(backup_dir, 'node')[0]['status'], "OK")
        node.stop()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_incremental_backup_without_full(self):
        """page-level backup without validated full backup"""
        if not self.ptrack:
            return unittest.skip('Skipped because ptrack support is disabled')

        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'ptrack_enable': 'on'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        try:
            self.backup_node(backup_dir, 'node', node, backup_type="page")
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because page backup should not be possible "
                "without valid full backup.\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Valid backup on current timeline 1 is not found. "
                "Create new FULL backup before an incremental one.",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        sleep(1)

        try:
            self.backup_node(backup_dir, 'node', node, backup_type="ptrack")
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because page backup should not be possible "
                "without valid full backup.\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Valid backup on current timeline 1 is not found. "
                "Create new FULL backup before an incremental one.",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        self.assertEqual(
            self.show_pb(backup_dir, 'node')[0]['status'],
            "ERROR")

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_incremental_backup_corrupt_full(self):
        """page-level backup with corrupted full backup"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        backup_id = self.backup_node(backup_dir, 'node', node)
        file = os.path.join(
            backup_dir, "backups", "node", backup_id,
            "database", "postgresql.conf")
        os.remove(file)

        try:
            self.validate_pb(backup_dir, 'node')
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because of validation of corrupted backup.\n"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                "INFO: Validate backups of the instance 'node'" in e.message and
                "WARNING: Backup file".format(
                    file) in e.message and
                "is not found".format(file) in e.message and
                "WARNING: Backup {0} data files are corrupted".format(
                    backup_id) in e.message and
                "WARNING: Some backups are not valid" in e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        try:
            self.backup_node(backup_dir, 'node', node, backup_type="page")
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because page backup should not be possible "
                "without valid full backup.\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Valid backup on current timeline 1 is not found. "
                "Create new FULL backup before an incremental one.",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        self.assertEqual(
            self.show_pb(backup_dir, 'node', backup_id)['status'], "CORRUPT")
        self.assertEqual(
            self.show_pb(backup_dir, 'node')[1]['status'], "ERROR")

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_threads(self):
        """ptrack multi thread backup mode"""
        if not self.ptrack:
            return unittest.skip('Skipped because ptrack support is disabled')

        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'ptrack_enable': 'on'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(
            backup_dir, 'node', node,
            backup_type="full", options=["-j", "4"])
        self.assertEqual(self.show_pb(backup_dir, 'node')[0]['status'], "OK")

        self.backup_node(
            backup_dir, 'node', node,
            backup_type="ptrack", options=["-j", "4"])
        self.assertEqual(self.show_pb(backup_dir, 'node')[0]['status'], "OK")

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_threads_stream(self):
        """ptrack multi thread backup mode and stream"""
        if not self.ptrack:
            return unittest.skip('Skipped because ptrack support is disabled')

        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'ptrack_enable': 'on'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(
            backup_dir, 'node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        self.assertEqual(self.show_pb(backup_dir, 'node')[0]['status'], "OK")
        self.backup_node(
            backup_dir, 'node', node,
            backup_type="ptrack", options=["-j", "4", "--stream"])
        self.assertEqual(self.show_pb(backup_dir, 'node')[1]['status'], "OK")

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_page_corruption_heal_via_ptrack_1(self):
        """make node, corrupt some page, check that backup failed"""
        if not self.ptrack:
            return unittest.skip('Skipped because ptrack support is disabled')

        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(
            backup_dir, 'node', node,
            backup_type="full", options=["-j", "4", "--stream"])

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1000) i")
        node.safe_psql(
            "postgres",
            "CHECKPOINT;")

        heap_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").rstrip()

        with open(os.path.join(node.data_dir, heap_path), "rb+", 0) as f:
                f.seek(9000)
                f.write(b"bla")
                f.flush()
                f.close

        self.backup_node(
            backup_dir, 'node', node, backup_type="full",
            options=["-j", "4", "--stream", "--log-level-file=verbose"])

        # open log file and check
        with open(os.path.join(backup_dir, 'log', 'pg_probackup.log')) as f:
            log_content = f.read()
            self.assertIn('block 1, try to fetch via SQL', log_content)
            self.assertIn('SELECT pg_catalog.pg_ptrack_get_block', log_content)
            f.close

        self.assertTrue(
            self.show_pb(backup_dir, 'node')[1]['status'] == 'OK',
            "Backup Status should be OK")

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_page_corruption_heal_via_ptrack_2(self):
        """make node, corrupt some page, check that backup failed"""
        if not self.ptrack:
            return unittest.skip('Skipped because ptrack support is disabled')

        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(
            backup_dir, 'node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1000) i")
        node.safe_psql(
            "postgres",
            "CHECKPOINT;")

        heap_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").rstrip()
        node.stop()

        with open(os.path.join(node.data_dir, heap_path), "rb+", 0) as f:
                f.seek(9000)
                f.write(b"bla")
                f.flush()
                f.close
        node.slow_start()

        try:
            self.backup_node(
                backup_dir, 'node', node, backup_type="full",
                options=["-j", "4", "--stream", '--log-level-console=LOG'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because of page "
                "corruption in PostgreSQL instance.\n"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            if self.remote:
                self.assertTrue(
                    "WARNING: File" in e.message and
                    "try to fetch via SQL" in e.message and
                    "WARNING:  page verification failed, "
                    "calculated checksum" in e.message and
                    "ERROR: query failed: "
                    "ERROR:  invalid page in block" in e.message and
                    "query was: SELECT pg_catalog.pg_ptrack_get_block_2" in e.message,
                    "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                        repr(e.message), self.cmd))
            else:
                self.assertTrue(
                    "LOG: File" in e.message and
                    "blknum" in e.message and
                    "have wrong checksum" in e.message and
                    "try to fetch via SQL" in e.message and
                    "WARNING:  page verification failed, "
                    "calculated checksum" in e.message and
                    "ERROR: query failed: "
                    "ERROR:  invalid page in block" in e.message and
                    "query was: SELECT pg_catalog.pg_ptrack_get_block_2" in e.message,
                    "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                        repr(e.message), self.cmd))

        self.assertTrue(
             self.show_pb(backup_dir, 'node')[1]['status'] == 'ERROR',
             "Backup Status should be ERROR")

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_backup_detect_corruption(self):
        """make node, corrupt some page, check that backup failed"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1000) i")
        node.safe_psql(
            "postgres",
            "CHECKPOINT;")

        heap_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").rstrip()

        node.stop()

        with open(os.path.join(node.data_dir, heap_path), "rb+", 0) as f:
                f.seek(9000)
                f.write(b"bla")
                f.flush()
                f.close

        node.slow_start()

        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type="full", options=["-j", "4", "--stream"])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because tablespace mapping is incorrect"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            if self.ptrack:
                self.assertTrue(
                    'WARNING:  page verification failed, '
                    'calculated checksum' in e.message and
                    'ERROR: query failed: ERROR:  '
                    'invalid page in block 1 of relation' in e.message and
                    'ERROR: Data files transferring failed' in e.message,
                    '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                        repr(e.message), self.cmd))
            else:
                if self.remote:
                    self.assertTrue(
                        "ERROR: Failed to read file" in e.message and
                        "data file checksum mismatch" in e.message,
                        '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                            repr(e.message), self.cmd))
                else:
                    self.assertIn(
                        'WARNING: Corruption detected in file',
                        e.message,
                        '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                            repr(e.message), self.cmd))
                    self.assertIn(
                        'ERROR: Data file corruption',
                        e.message,
                        '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                            repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_backup_truncate_misaligned(self):
        """
        make node, truncate file to size not even to BLCKSIZE,
        take backup
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,100000) i")

        node.safe_psql(
            "postgres",
            "CHECKPOINT;")

        heap_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").rstrip()

        heap_size = node.safe_psql(
            "postgres",
            "select pg_relation_size('t_heap')")

        with open(os.path.join(node.data_dir, heap_path), "rb+", 0) as f:
            f.truncate(int(heap_size) - 4096)
            f.flush()
            f.close

        output = self.backup_node(
            backup_dir, 'node', node, backup_type="full",
            options=["-j", "4", "--stream"], return_id=False)

        self.assertIn("WARNING: File", output)
        self.assertIn("invalid file size", output)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_tablespace_in_pgdata_pgpro_1376(self):
        """PGPRO-1376 """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        self.create_tblspace_in_node(
            node, 'tblspace1',
            tblspc_path=(
                os.path.join(
                    node.data_dir, 'somedirectory', '100500'))
            )

        self.create_tblspace_in_node(
            node, 'tblspace2',
            tblspc_path=(os.path.join(node.data_dir))
            )

        node.safe_psql(
            "postgres",
            "create table t_heap1 tablespace tblspace1 as select 1 as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1000) i")

        node.safe_psql(
            "postgres",
            "create table t_heap2 tablespace tblspace2 as select 1 as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1000) i")

        backup_id_1 = self.backup_node(
            backup_dir, 'node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        node.safe_psql(
            "postgres",
            "drop table t_heap2")
        node.safe_psql(
            "postgres",
            "drop tablespace tblspace2")

        self.backup_node(
                backup_dir, 'node', node, backup_type="full",
                options=["-j", "4", "--stream"])

        pgdata = self.pgdata_content(node.data_dir)

        relfilenode = node.safe_psql(
            "postgres",
            "select 't_heap1'::regclass::oid"
            ).rstrip()

        list = []
        for root, dirs, files in os.walk(os.path.join(
                backup_dir, 'backups', 'node', backup_id_1)):
            for file in files:
                if file == relfilenode:
                    path = os.path.join(root, file)
                    list = list + [path]

        # We expect that relfilenode can be encountered only once
        if len(list) > 1:
            message = ""
            for string in list:
                message = message + string + "\n"
            self.assertEqual(
                1, 0,
                "Following file copied twice by backup:\n {0}".format(
                    message)
                )

        node.cleanup()

        self.restore_node(
            backup_dir, 'node', node, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_basic_tablespace_handling(self):
        """
        make node, take full backup, check that restore with
        tablespace mapping will end with error, take page backup,
        check that restore with tablespace mapping will end with
        success
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(
            backup_dir, 'node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        tblspace1_old_path = self.get_tblspace_path(node, 'tblspace1_old')
        tblspace2_old_path = self.get_tblspace_path(node, 'tblspace2_old')

        self.create_tblspace_in_node(
            node, 'some_lame_tablespace')

        self.create_tblspace_in_node(
            node, 'tblspace1',
            tblspc_path=tblspace1_old_path)

        self.create_tblspace_in_node(
            node, 'tblspace2',
            tblspc_path=tblspace2_old_path)

        node.safe_psql(
            "postgres",
            "create table t_heap_lame tablespace some_lame_tablespace "
            "as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1000) i")

        node.safe_psql(
            "postgres",
            "create table t_heap2 tablespace tblspace2 as select 1 as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1000) i")

        tblspace1_new_path = self.get_tblspace_path(node, 'tblspace1_new')
        tblspace2_new_path = self.get_tblspace_path(node, 'tblspace2_new')

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        try:
            self.restore_node(
                backup_dir, 'node', node_restored,
                options=[
                    "-j", "4",
                    "-T", "{0}={1}".format(
                        tblspace1_old_path, tblspace1_new_path),
                    "-T", "{0}={1}".format(
                        tblspace2_old_path, tblspace2_new_path)])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because tablespace mapping is incorrect"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'ERROR: --tablespace-mapping option' in e.message and
                'have an entry in tablespace_map file' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        node.safe_psql(
            "postgres",
            "drop table t_heap_lame")

        node.safe_psql(
            "postgres",
            "drop tablespace some_lame_tablespace")

        self.backup_node(
            backup_dir, 'node', node, backup_type="delta",
            options=["-j", "4", "--stream"])

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=[
                "-j", "4",
                "-T", "{0}={1}".format(
                    tblspace1_old_path, tblspace1_new_path),
                "-T", "{0}={1}".format(
                    tblspace2_old_path, tblspace2_new_path)])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_tablespace_handling_1(self):
        """
        make node with tablespace A, take full backup, check that restore with
        tablespace mapping of tablespace B will end with error
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        tblspace1_old_path = self.get_tblspace_path(node, 'tblspace1_old')
        tblspace2_old_path = self.get_tblspace_path(node, 'tblspace2_old')

        tblspace_new_path = self.get_tblspace_path(node, 'tblspace_new')

        self.create_tblspace_in_node(
            node, 'tblspace1',
            tblspc_path=tblspace1_old_path)

        self.backup_node(
            backup_dir, 'node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        try:
            self.restore_node(
                backup_dir, 'node', node_restored,
                options=[
                    "-j", "4",
                    "-T", "{0}={1}".format(
                        tblspace2_old_path, tblspace_new_path)])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because tablespace mapping is incorrect"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'ERROR: --tablespace-mapping option' in e.message and
                'have an entry in tablespace_map file' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_tablespace_handling_2(self):
        """
        make node without tablespaces, take full backup, check that restore with
        tablespace mapping will end with error
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        tblspace1_old_path = self.get_tblspace_path(node, 'tblspace1_old')
        tblspace_new_path = self.get_tblspace_path(node, 'tblspace_new')

        self.backup_node(
            backup_dir, 'node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        try:
            self.restore_node(
                backup_dir, 'node', node_restored,
                options=[
                    "-j", "4",
                    "-T", "{0}={1}".format(
                        tblspace1_old_path, tblspace_new_path)])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because tablespace mapping is incorrect"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'ERROR: --tablespace-mapping option' in e.message and
                'have an entry in tablespace_map file' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_drop_rel_during_backup_delta(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i"
            " as id from generate_series(0,100) i")

        relative_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").rstrip()

        absolute_path = os.path.join(node.data_dir, relative_path)

        # FULL backup
        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        # DELTA backup
        gdb = self.backup_node(
            backup_dir, 'node', node, backup_type='delta',
            gdb=True, options=['--log-level-file=verbose'])

        gdb.set_breakpoint('backup_files')
        gdb.run_until_break()

        # REMOVE file
        node.safe_psql(
            "postgres",
            "DROP TABLE t_heap")

        node.safe_psql(
            "postgres",
            "CHECKPOINT")

        # File removed, we can proceed with backup
        gdb.continue_execution_until_exit()

        pgdata = self.pgdata_content(node.data_dir)

        with open(os.path.join(backup_dir, 'log', 'pg_probackup.log')) as f:
            log_content = f.read()
            self.assertTrue(
                'LOG: File "{0}" is not found'.format(absolute_path) in log_content,
                'File "{0}" should be deleted but it`s not'.format(absolute_path))

        node.cleanup()
        self.restore_node(backup_dir, 'node', node, options=["-j", "4"])

        # Physical comparison
        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_drop_rel_during_backup_page(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i"
            " as id from generate_series(0,100) i")

        relative_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").rstrip()

        absolute_path = os.path.join(node.data_dir, relative_path)

        # FULL backup
        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        # PAGE backup
        gdb = self.backup_node(
            backup_dir, 'node', node, backup_type='page',
            gdb=True, options=['--log-level-file=verbose'])

        gdb.set_breakpoint('backup_files')
        gdb.run_until_break()

        # REMOVE file
        os.remove(absolute_path)

        # File removed, we can proceed with backup
        gdb.continue_execution_until_exit()

        pgdata = self.pgdata_content(node.data_dir)

        with open(os.path.join(backup_dir, 'log', 'pg_probackup.log')) as f:
            log_content = f.read()
            self.assertTrue(
                'LOG: File "{0}" is not found'.format(absolute_path) in log_content,
                'File "{0}" should be deleted but it`s not'.format(absolute_path))

        node.cleanup()
        self.restore_node(backup_dir, 'node', node, options=["-j", "4"])

        # Physical comparison
        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_drop_rel_during_backup_ptrack(self):
        """"""
        if not self.ptrack:
            return unittest.skip('Skipped because ptrack support is disabled')

        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'ptrack_enable': 'on'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i"
            " as id from generate_series(0,100) i")

        relative_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").rstrip()

        absolute_path = os.path.join(node.data_dir, relative_path)

        # FULL backup
        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        # PTRACK backup
        gdb = self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack',
            gdb=True, options=['--log-level-file=verbose'])

        gdb.set_breakpoint('backup_files')
        gdb.run_until_break()

        # REMOVE file
        os.remove(absolute_path)

        # File removed, we can proceed with backup
        gdb.continue_execution_until_exit()

        pgdata = self.pgdata_content(node.data_dir)

        with open(os.path.join(backup_dir, 'log', 'pg_probackup.log')) as f:
            log_content = f.read()
            self.assertTrue(
                'LOG: File "{0}" is not found'.format(absolute_path) in log_content,
                'File "{0}" should be deleted but it`s not'.format(absolute_path))

        node.cleanup()
        self.restore_node(backup_dir, 'node', node, options=["-j", "4"])

        # Physical comparison
        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_persistent_slot_for_stream_backup(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'max_wal_size': '40MB'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "SELECT pg_create_physical_replication_slot('slot_1')")

        # FULL backup
        self.backup_node(
            backup_dir, 'node', node,
            options=['--stream', '--slot=slot_1'])

        # FULL backup
        self.backup_node(
            backup_dir, 'node', node,
            options=['--stream', '--slot=slot_1'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_basic_temp_slot_for_stream_backup(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'max_wal_size': '40MB'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # FULL backup
        self.backup_node(
            backup_dir, 'node', node,
            options=['--stream', '--temp-slot'])

        if self.get_version(node) < self.version_to_num('10.0'):
            return unittest.skip('You need PostgreSQL >= 10 for this test')
        else:
            pg_receivexlog_path = self.get_bin_path('pg_receivewal')

        # FULL backup
        self.backup_node(
            backup_dir, 'node', node,
            options=['--stream', '--slot=slot_1', '--temp-slot'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_backup_concurrent_drop_table(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=1)

        # FULL backup
        gdb = self.backup_node(
            backup_dir, 'node', node,
            options=['--stream', '--compress'],
            gdb=True)

        gdb.set_breakpoint('backup_data_file')
        gdb.run_until_break()

        node.safe_psql(
            'postgres',
            'DROP TABLE pgbench_accounts')

        # do checkpoint to guarantee filenode removal
        node.safe_psql(
            'postgres',
            'CHECKPOINT')

        gdb.remove_all_breakpoints()
        gdb.continue_execution_until_exit()

        show_backup = self.show_pb(backup_dir, 'node')[0]

        self.assertEqual(show_backup['status'], "OK")

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_pg_11_adjusted_wal_segment_size(self):
        """"""
        if self.pg_config_version < self.version_to_num('11.0'):
            return unittest.skip('You need PostgreSQL >= 11 for this test')

        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=[
                '--data-checksums',
                '--wal-segsize=64'],
            pg_options={
                'min_wal_size': '128MB',
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=5)

        # FULL STREAM backup
        self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        pgbench = node.pgbench(options=['-T', '5', '-c', '2'])
        pgbench.wait()

        # PAGE STREAM backup
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', options=['--stream'])

        pgbench = node.pgbench(options=['-T', '5', '-c', '2'])
        pgbench.wait()

        # DELTA STREAM backup
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta', options=['--stream'])

        pgbench = node.pgbench(options=['-T', '5', '-c', '2'])
        pgbench.wait()

        # FULL ARCHIVE backup
        self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(options=['-T', '5', '-c', '2'])
        pgbench.wait()

        # PAGE ARCHIVE backup
        self.backup_node(backup_dir, 'node', node, backup_type='page')

        pgbench = node.pgbench(options=['-T', '5', '-c', '2'])
        pgbench.wait()

        # DELTA ARCHIVE backup
        backup_id = self.backup_node(backup_dir, 'node', node, backup_type='delta')
        pgdata = self.pgdata_content(node.data_dir)

        # delete
        output = self.delete_pb(
            backup_dir, 'node',
            options=[
                '--expired',
                '--delete-wal',
                '--retention-redundancy=1'])

        # validate
        self.validate_pb(backup_dir)

        # merge
        self.merge_backup(backup_dir, 'node', backup_id=backup_id)

        # restore
        node.cleanup()
        self.restore_node(
            backup_dir, 'node', node, backup_id=backup_id)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_sigint_handling(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        # FULL backup
        gdb = self.backup_node(
            backup_dir, 'node', node, gdb=True,
            options=['--stream', '--log-level-file=verbose'])

        gdb.set_breakpoint('copy_file')
        gdb.run_until_break()

        gdb.continue_execution_until_break(20)
        gdb.remove_all_breakpoints()

        gdb._execute('signal SIGINT')
        gdb.continue_execution_until_error()

        backup_id = self.show_pb(backup_dir, 'node')[0]['id']

        self.assertEqual(
            'ERROR',
            self.show_pb(backup_dir, 'node', backup_id)['status'],
            'Backup STATUS should be "ERROR"')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_sigterm_handling(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        # FULL backup
        gdb = self.backup_node(
            backup_dir, 'node', node, gdb=True,
            options=['--stream', '--log-level-file=verbose'])

        gdb.set_breakpoint('copy_file')
        gdb.run_until_break()

        gdb.continue_execution_until_break(20)
        gdb.remove_all_breakpoints()

        gdb._execute('signal SIGTERM')
        gdb.continue_execution_until_error()

        backup_id = self.show_pb(backup_dir, 'node')[0]['id']

        self.assertEqual(
            'ERROR',
            self.show_pb(backup_dir, 'node', backup_id)['status'],
            'Backup STATUS should be "ERROR"')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_sigquit_handling(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        # FULL backup
        gdb = self.backup_node(
            backup_dir, 'node', node, gdb=True,
            options=['--stream', '--log-level-file=verbose'])

        gdb.set_breakpoint('copy_file')
        gdb.run_until_break()

        gdb.continue_execution_until_break(20)
        gdb.remove_all_breakpoints()

        gdb._execute('signal SIGQUIT')
        gdb.continue_execution_until_error()

        backup_id = self.show_pb(backup_dir, 'node')[0]['id']

        self.assertEqual(
            'ERROR',
            self.show_pb(backup_dir, 'node', backup_id)['status'],
            'Backup STATUS should be "ERROR"')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_drop_table(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        connect_1 = node.connect("postgres")
        connect_1.execute(
            "create table t_heap as select i"
            " as id from generate_series(0,100) i")
        connect_1.commit()

        connect_2 = node.connect("postgres")
        connect_2.execute("SELECT * FROM t_heap")
        connect_2.commit()

        # DROP table
        connect_2.execute("DROP TABLE t_heap")
        connect_2.commit()

        # FULL backup
        self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_basic_missing_file_permissions(self):
        """"""
        if os.name == 'nt':
            return unittest.skip('Skipped because it is POSIX only test')

        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        relative_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('pg_class')").rstrip()

        full_path = os.path.join(node.data_dir, relative_path)

        os.chmod(full_path, 000)

        try:
            # FULL backup
            self.backup_node(
                backup_dir, 'node', node, options=['--stream'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because of missing permissions"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: cannot open file',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        os.chmod(full_path, 700)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_basic_missing_dir_permissions(self):
        """"""
        if os.name == 'nt':
            return unittest.skip('Skipped because it is POSIX only test')

        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        full_path = os.path.join(node.data_dir, 'pg_twophase')

        os.chmod(full_path, 000)

        try:
            # FULL backup
            self.backup_node(
                backup_dir, 'node', node, options=['--stream'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because of missing permissions"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Cannot open directory',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        os.chmod(full_path, 700)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_backup_with_least_privileges_role(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'archive_timeout': '30s'})

        if self.ptrack:
            node.append_conf('postgresql.auto.conf', 'ptrack_enable = on')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            'postgres',
            'CREATE DATABASE backupdb')

        # PG 9.5
        if self.get_version(node) < 90600:
            node.safe_psql(
                'backupdb',
                "REVOKE ALL ON DATABASE backupdb from PUBLIC; "
                "REVOKE ALL ON SCHEMA public from PUBLIC; "
                "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC; "
                "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC; "
                "REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC; "
                "REVOKE ALL ON SCHEMA pg_catalog from PUBLIC; "
                "REVOKE ALL ON ALL TABLES IN SCHEMA pg_catalog FROM PUBLIC; "
                "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pg_catalog FROM PUBLIC; "
                "REVOKE ALL ON ALL SEQUENCES IN SCHEMA pg_catalog FROM PUBLIC; "
                "REVOKE ALL ON SCHEMA information_schema from PUBLIC; "
                "REVOKE ALL ON ALL TABLES IN SCHEMA information_schema FROM PUBLIC; "
                "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA information_schema FROM PUBLIC; "
                "REVOKE ALL ON ALL SEQUENCES IN SCHEMA information_schema FROM PUBLIC; "
                "CREATE ROLE backup WITH LOGIN REPLICATION; "
                "GRANT CONNECT ON DATABASE backupdb to backup; "
                "GRANT USAGE ON SCHEMA pg_catalog TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_proc TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_database TO backup; " # for partial restore, checkdb and ptrack
                "GRANT EXECUTE ON FUNCTION pg_catalog.nameeq(name, name) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.textout(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.timestamptz(timestamp with time zone, integer) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_start_backup(text, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_stop_backup() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_current_snapshot() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_snapshot_xmax(txid_snapshot) TO backup;"
            )
        # PG 9.6
        elif self.get_version(node) > 90600 and self.get_version(node) < 100000:
            node.safe_psql(
                'backupdb',
                "REVOKE ALL ON DATABASE backupdb from PUBLIC; "
                "REVOKE ALL ON SCHEMA public from PUBLIC; "
                "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC; "
                "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC; "
                "REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC; "
                "REVOKE ALL ON SCHEMA pg_catalog from PUBLIC; "
                "REVOKE ALL ON ALL TABLES IN SCHEMA pg_catalog FROM PUBLIC; "
                "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pg_catalog FROM PUBLIC; "
                "REVOKE ALL ON ALL SEQUENCES IN SCHEMA pg_catalog FROM PUBLIC; "
                "REVOKE ALL ON SCHEMA information_schema from PUBLIC; "
                "REVOKE ALL ON ALL TABLES IN SCHEMA information_schema FROM PUBLIC; "
                "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA information_schema FROM PUBLIC; "
                "REVOKE ALL ON ALL SEQUENCES IN SCHEMA information_schema FROM PUBLIC; "
                "CREATE ROLE backup WITH LOGIN REPLICATION; "
                "GRANT CONNECT ON DATABASE backupdb to backup; "
                "GRANT USAGE ON SCHEMA pg_catalog TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_proc TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_database TO backup; " # for partial restore, checkdb and ptrack
                "GRANT EXECUTE ON FUNCTION pg_catalog.nameeq(name, name) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.textout(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.timestamptz(timestamp with time zone, integer) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_start_backup(text, boolean, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_stop_backup(boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_create_restore_point(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_xlog() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_last_xlog_replay_location() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_current_snapshot() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_snapshot_xmax(txid_snapshot) TO backup;"
            )
        # >= 10
        else:
            node.safe_psql(
                'backupdb',
                "REVOKE ALL ON DATABASE backupdb from PUBLIC; "
                "REVOKE ALL ON SCHEMA public from PUBLIC; "
                "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC; "
                "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC; "
                "REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC; "
                "REVOKE ALL ON SCHEMA pg_catalog from PUBLIC; "
                "REVOKE ALL ON ALL TABLES IN SCHEMA pg_catalog FROM PUBLIC; "
                "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pg_catalog FROM PUBLIC; "
                "REVOKE ALL ON ALL SEQUENCES IN SCHEMA pg_catalog FROM PUBLIC; "
                "REVOKE ALL ON SCHEMA information_schema from PUBLIC; "
                "REVOKE ALL ON ALL TABLES IN SCHEMA information_schema FROM PUBLIC; "
                "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA information_schema FROM PUBLIC; "
                "REVOKE ALL ON ALL SEQUENCES IN SCHEMA information_schema FROM PUBLIC; "
                "CREATE ROLE backup WITH LOGIN REPLICATION; "
                "GRANT CONNECT ON DATABASE backupdb to backup; "
                "GRANT USAGE ON SCHEMA pg_catalog TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_proc TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_database TO backup; " # for partial restore, checkdb and ptrack
                "GRANT EXECUTE ON FUNCTION pg_catalog.nameeq(name, name) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_start_backup(text, boolean, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_stop_backup(boolean, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_create_restore_point(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_wal() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_last_wal_replay_lsn() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_current_snapshot() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_snapshot_xmax(txid_snapshot) TO backup;"
            )

        if self.ptrack:
            for fname in [
                    'pg_catalog.oideq(oid, oid)',
                    'pg_catalog.ptrack_version()',
                    'pg_catalog.pg_ptrack_clear()',
                    'pg_catalog.pg_ptrack_control_lsn()',
                    'pg_catalog.pg_ptrack_get_and_clear_db(oid, oid)',
                    'pg_catalog.pg_ptrack_get_and_clear(oid, oid)',
                    'pg_catalog.pg_ptrack_get_block_2(oid, oid, oid, bigint)',
                    'pg_catalog.pg_stop_backup()']:
                # try:
                node.safe_psql(
                    "backupdb",
                    "GRANT EXECUTE ON FUNCTION {0} "
                    "TO backup".format(fname))
                # except:
                #     pass

        # FULL backup
        self.backup_node(
            backup_dir, 'node', node,
            datname='backupdb', options=['--stream', '-U', 'backup'])
        self.backup_node(
            backup_dir, 'node', node,
            datname='backupdb', options=['-U', 'backup'])

        # PAGE
        self.backup_node(
            backup_dir, 'node', node, backup_type='page',
            datname='backupdb', options=['-U', 'backup'])
        self.backup_node(
            backup_dir, 'node', node, backup_type='page', datname='backupdb',
            options=['--stream', '-U', 'backup'])

        # DELTA
        self.backup_node(
            backup_dir, 'node', node, backup_type='delta',
            datname='backupdb', options=['-U', 'backup'])
        self.backup_node(
            backup_dir, 'node', node, backup_type='delta',
            datname='backupdb', options=['--stream', '-U', 'backup'])

        # PTRACK
        if self.ptrack:
            self.backup_node(
                backup_dir, 'node', node, backup_type='ptrack',
                datname='backupdb', options=['-U', 'backup'])
            self.backup_node(
                backup_dir, 'node', node, backup_type='ptrack',
                datname='backupdb', options=['--stream', '-U', 'backup'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_parent_choosing(self):
        """
        PAGE3 <- RUNNING(parent should be FULL)
        PAGE2 <- OK
        PAGE1 <- CORRUPT
        FULL
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        full_id = self.backup_node(backup_dir, 'node', node)

        # PAGE1
        page1_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGE2
        page2_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Change PAGE1 to ERROR
        self.change_backup_status(backup_dir, 'node', page1_id, 'ERROR')

        # PAGE3
        page3_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', options=['--log-level-file=LOG'])

        log_file_path = os.path.join(backup_dir, 'log', 'pg_probackup.log')
        with open(log_file_path) as f:
            log_file_content = f.read()

        self.assertIn(
            "WARNING: Backup {0} has invalid parent: {1}. "
            "Cannot be a parent".format(page2_id, page1_id),
            log_file_content)

        self.assertIn(
            "WARNING: Backup {0} has status: ERROR. "
            "Cannot be a parent".format(page1_id),
            log_file_content)

        self.assertIn(
            "Parent backup: {0}".format(full_id),
            log_file_content)

        self.assertEqual(
            self.show_pb(
                backup_dir, 'node', backup_id=page3_id)['parent-backup-id'],
            full_id)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_parent_choosing_1(self):
        """
        PAGE3 <- RUNNING(parent should be FULL)
        PAGE2 <- OK
        PAGE1 <- (missing)
        FULL
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        full_id = self.backup_node(backup_dir, 'node', node)

        # PAGE1
        page1_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGE2
        page2_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Delete PAGE1
        shutil.rmtree(
            os.path.join(backup_dir, 'backups', 'node', page1_id))

        # PAGE3
        page3_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', options=['--log-level-file=LOG'])

        log_file_path = os.path.join(backup_dir, 'log', 'pg_probackup.log')
        with open(log_file_path) as f:
            log_file_content = f.read()

        self.assertIn(
            "WARNING: Backup {0} has missing parent: {1}. "
            "Cannot be a parent".format(page2_id, page1_id),
            log_file_content)

        self.assertIn(
            "Parent backup: {0}".format(full_id),
            log_file_content)

        self.assertEqual(
            self.show_pb(
                backup_dir, 'node', backup_id=page3_id)['parent-backup-id'],
            full_id)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_parent_choosing_2(self):
        """
        PAGE3 <- RUNNING(backup should fail)
        PAGE2 <- OK
        PAGE1 <- OK
        FULL  <- (missing)
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        full_id = self.backup_node(backup_dir, 'node', node)

        # PAGE1
        page1_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGE2
        page2_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Delete FULL
        shutil.rmtree(
            os.path.join(backup_dir, 'backups', 'node', full_id))

        # PAGE3
        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='page', options=['--log-level-file=LOG'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because FULL backup is missing"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Valid backup on current timeline 1 is not found. '
                'Create new FULL backup before an incremental one.',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertEqual(
            self.show_pb(
                backup_dir, 'node')[2]['status'],
            'ERROR')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_backup_with_less_privileges_role(self):
        """
        check permissions correctness from documentation:
        https://github.com/postgrespro/pg_probackup/blob/master/Documentation.md#configuring-the-database-cluster
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'archive_timeout': '30s',
                'checkpoint_timeout': '30s'})

        if self.ptrack:
            node.append_conf('postgresql.auto.conf', 'ptrack_enable = on')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            'postgres',
            'CREATE DATABASE backupdb')

        # PG 9.5
        if self.get_version(node) < 90600:
            node.safe_psql(
                'backupdb',
                "BEGIN; "
                "CREATE ROLE backup WITH LOGIN; "
                "GRANT USAGE ON SCHEMA pg_catalog TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_start_backup(text, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_stop_backup() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_create_restore_point(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_xlog() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_current() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_current_snapshot() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_snapshot_xmax(txid_snapshot) TO backup; "
                "COMMIT;"
            )
        # PG 9.6
        elif self.get_version(node) > 90600 and self.get_version(node) < 100000:
            node.safe_psql(
                'backupdb',
                "BEGIN; "
                "CREATE ROLE backup WITH LOGIN; "
                "GRANT USAGE ON SCHEMA pg_catalog TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_start_backup(text, boolean, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_stop_backup(boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_create_restore_point(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_xlog() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_last_xlog_replay_location() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_current() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_current_snapshot() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_snapshot_xmax(txid_snapshot) TO backup; "
                "COMMIT;"
            )
        # >= 10
        else:
            node.safe_psql(
                'backupdb',
                "BEGIN; "
                "CREATE ROLE backup WITH LOGIN; "
                "GRANT USAGE ON SCHEMA pg_catalog TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_start_backup(text, boolean, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_stop_backup(boolean, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_create_restore_point(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_wal() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_last_wal_replay_lsn() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_current() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_current_snapshot() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_snapshot_xmax(txid_snapshot) TO backup; "
                "COMMIT;"
            )

        # enable STREAM backup
        node.safe_psql(
            'backupdb',
            'ALTER ROLE backup WITH REPLICATION;')

        # FULL backup
        self.backup_node(
            backup_dir, 'node', node,
            datname='backupdb', options=['--stream', '-U', 'backup'])
        self.backup_node(
            backup_dir, 'node', node,
            datname='backupdb', options=['-U', 'backup'])

        # PAGE
        self.backup_node(
            backup_dir, 'node', node, backup_type='page',
            datname='backupdb', options=['-U', 'backup'])
        self.backup_node(
            backup_dir, 'node', node, backup_type='page', datname='backupdb',
            options=['--stream', '-U', 'backup'])

        # DELTA
        self.backup_node(
            backup_dir, 'node', node, backup_type='delta',
            datname='backupdb', options=['-U', 'backup'])
        self.backup_node(
            backup_dir, 'node', node, backup_type='delta',
            datname='backupdb', options=['--stream', '-U', 'backup'])

        # Restore as replica
        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.restore_node(backup_dir, 'node', replica)
        self.set_replica(node, replica)
        self.add_instance(backup_dir, 'replica', replica)
        self.set_archiving(backup_dir, 'replica', replica, replica=True)

        copy_tree(
            os.path.join(backup_dir, 'wal', 'node'),
            os.path.join(backup_dir, 'wal', 'replica'))

        replica.slow_start(replica=True)

        # freeze bgwriter to get rid of RUNNING XACTS records
        bgwriter_pid = node.auxiliary_pids[ProcessType.BackgroundWriter][0]
        gdb_checkpointer = self.gdb_attach(bgwriter_pid)

        self.switch_wal_segment(node)

        # FULL backup from replica
        self.backup_node(
            backup_dir, 'replica', replica,
            datname='backupdb', options=['--stream', '-U', 'backup'])

        self.switch_wal_segment(node)

        self.backup_node(
            backup_dir, 'replica', replica, datname='backupdb',
            options=['-U', 'backup', '--log-level-file=verbose', '--archive-timeout=100s'])

        # PAGE backup from replica
        self.backup_node(
            backup_dir, 'replica', replica, backup_type='page',
            datname='backupdb', options=['-U', 'backup', '--archive-timeout=100s'])

        self.backup_node(
            backup_dir, 'replica', replica, backup_type='page',
            datname='backupdb', options=['--stream', '-U', 'backup'])

        # DELTA backup from replica
        self.backup_node(
            backup_dir, 'replica', replica, backup_type='delta',
            datname='backupdb', options=['-U', 'backup', '--archive-timeout=100s'])
        self.backup_node(
            backup_dir, 'replica', replica, backup_type='delta',
            datname='backupdb', options=['--stream', '-U', 'backup'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)
