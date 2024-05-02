import unittest
import os
import re
from time import sleep, time
from datetime import datetime

from pg_probackup2.gdb import needs_gdb

from .helpers.ptrack_helpers import base36enc, ProbackupTest
from .helpers.ptrack_helpers import fs_backup_class
import subprocess


class BackupTest(ProbackupTest):

    def test_full_backup(self):
        """
        Just test full backup with at least two segments
        """
        node = self.pg_node.make_simple('node',
            # we need to write a lot. Lets speedup a bit.
            pg_options={"fsync": "off", "synchronous_commit": "off"})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Fill with data
        # Have to use scale=100 to create second segment.
        node.pgbench_init(scale=100, no_vacuum=True)

        # FULL
        backup_id = self.pb.backup_node('node', node)

        out = self.pb.validate('node', backup_id)
        self.assertIn(
            "INFO: Backup {0} is valid".format(backup_id),
            out)

    def test_full_backup_stream(self):
        """
        Just test full backup with at least two segments in stream mode
        """
        node = self.pg_node.make_simple('node',
            # we need to write a lot. Lets speedup a bit.
            pg_options={"fsync": "off", "synchronous_commit": "off"})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # Fill with data
        # Have to use scale=100 to create second segment.
        node.pgbench_init(scale=100, no_vacuum=True)

        # FULL
        backup_id = self.pb.backup_node('node', node,
                                     options=["--stream"])

        out = self.pb.validate('node', backup_id)
        self.assertIn(
            "INFO: Backup {0} is valid".format(backup_id),
            out)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    # PGPRO-707
    def test_backup_modes_archive(self):
        """standart backup modes with ARCHIVE WAL method"""
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()
        
        full_backup_id = self.pb.backup_node('node', node)
        show_backup = self.pb.show('node')[0]

        self.assertEqual(show_backup['status'], "OK")
        self.assertEqual(show_backup['backup-mode'], "FULL")

        # postmaster.pid and postmaster.opts shouldn't be copied
        pms = {"postmaster.pid", "postmaster.opts"}
        files = self.get_backup_listdir(backup_dir, 'node', full_backup_id,
                                        'database')
        self.assertFalse(pms.intersection(files))
        files = self.get_backup_filelist(backup_dir, 'node', full_backup_id)
        self.assertFalse(pms.intersection(files.keys()))

        # page backup mode
        page_backup_id = self.pb.backup_node('node', node, backup_type="page")

        show_backup_1 = self.pb.show('node')[1]
        self.assertEqual(show_backup_1['status'], "OK")
        self.assertEqual(show_backup_1['backup-mode'], "PAGE")

        # delta backup mode
        delta_backup_id = self.pb.backup_node('node', node, backup_type="delta")

        show_backup_2 = self.pb.show('node')[2]
        self.assertEqual(show_backup_2['status'], "OK")
        self.assertEqual(show_backup_2['backup-mode'], "DELTA")

        # Check parent backup
        self.assertEqual(
            full_backup_id,
            self.pb.show('node',
                backup_id=show_backup_1['id'])["parent-backup-id"])

        self.assertEqual(
            page_backup_id,
            self.pb.show('node',
                backup_id=show_backup_2['id'])["parent-backup-id"])

    # @unittest.skip("skip")
    def test_smooth_checkpoint(self):
        """full backup with smooth checkpoint"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node,
            options=["-C"])
        self.assertEqual(self.pb.show('node')[0]['status'], "OK")
        node.stop()

    # @unittest.skip("skip")
    def test_incremental_backup_without_full(self):
        """page backup without validated full backup"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node, backup_type="page",
                         expect_error="because page backup should not be possible")
        self.assertMessage(contains="WARNING: Valid full backup on current timeline 1 is not found")
        self.assertMessage(contains="ERROR: Create new full backup before an incremental one")

        self.assertEqual(
            self.pb.show('node')[0]['status'],
            "ERROR")

    # @unittest.skip("skip")
    def test_incremental_backup_corrupt_full(self):
        """page-level backup with corrupted full backup"""
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node)
        self.remove_backup_file(backup_dir, "node", backup_id, "database/postgresql.conf")

        self.pb.validate('node',
                         expect_error="because of validation of corrupted backup")
        self.assertMessage(contains="INFO: Validate backups of the instance 'node'")
        self.assertMessage(contains="WARNING: Validating ")
        self.assertMessage(contains="No such file")
        self.assertMessage(contains=f"WARNING: Backup {backup_id} data files are corrupted")
        self.assertMessage(contains="WARNING: Some backups are not valid")

        self.pb.backup_node('node', node, backup_type="page",
                         expect_error="because page backup should not be possible")
        self.assertMessage(contains="WARNING: Valid full backup on current timeline 1 is not found")
        self.assertMessage(contains="ERROR: Create new full backup before an incremental one")

        self.assertEqual(
            self.pb.show('node', backup_id)['status'], "CORRUPT")
        self.assertEqual(
            self.pb.show('node')[1]['status'], "ERROR")

    # @unittest.skip("skip")
    def test_delta_threads_stream(self):
        """delta multi thread backup mode and stream"""
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        self.pb.backup_node('node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        self.assertEqual(self.pb.show('node')[0]['status'], "OK")
        self.pb.backup_node('node', node,
            backup_type="delta", options=["-j", "4", "--stream"])
        self.assertEqual(self.pb.show('node')[1]['status'], "OK")

    # @unittest.skip("skip")
    def test_page_detect_corruption(self):
        """make node, corrupt some page, check that backup failed"""

        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=self.ptrack)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        self.pb.backup_node('node', node,
            backup_type="full", options=["-j", "4", "--stream"])

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1000) i")

        node.safe_psql(
            "postgres",
            "CHECKPOINT")

        heap_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()

        path = os.path.join(node.data_dir, heap_path)
        with open(path, "rb+", 0) as f:
                f.seek(9000)
                f.write(b"bla")
                f.flush()

        self.pb.backup_node('node', node, backup_type="full",
                options=["-j", "4", "--stream", "--log-level-file=VERBOSE"],
                expect_error="because data file is corrupted")
        self.assertMessage(contains=f'ERROR: Corruption detected in file "{path}", '
                                    'block 1: page verification failed, calculated checksum')

        self.assertEqual(
            self.pb.show('node')[1]['status'],
            'ERROR',
            "Backup Status should be ERROR")

    # @unittest.skip("skip")
    def test_backup_detect_corruption(self):
        """make node, corrupt some page, check that backup failed"""
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=self.ptrack)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        if self.ptrack:
            node.safe_psql(
                "postgres",
                "create extension ptrack")

        self.pb.backup_node('node', node,
            backup_type="full", options=["-j", "4", "--stream"])

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")

        heap_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()

        self.pb.backup_node('node', node,
            backup_type="full", options=["-j", "4", "--stream"])

        node.safe_psql(
            "postgres",
            "select count(*) from t_heap")

        node.safe_psql(
            "postgres",
            "update t_heap set id = id + 10000")

        node.stop()

        heap_fullpath = os.path.join(node.data_dir, heap_path)

        with open(heap_fullpath, "rb+", 0) as f:
                f.seek(9000)
                f.write(b"bla")
                f.flush()
                f.close

        node.slow_start()

        modes = "full,delta,page"
        if self.ptrack:
            modes += ",ptrack"
        for mode in modes.split(','):
            with self.subTest(mode):
                self.pb.backup_node('node', node,
                                 backup_type=mode,
                                 options=["-j", "4", "--stream"],
                                 expect_error="because of block corruption")
                self.assertMessage(contains=
                        'ERROR: Corruption detected in file "{0}", block 1: '
                        'page verification failed, calculated checksum'.format(
                            heap_fullpath))
            sleep(1)

    # @unittest.skip("skip")
    def test_backup_detect_invalid_block_header(self):
        """make node, corrupt some page, check that backup failed"""
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=self.ptrack)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        if self.ptrack:
            node.safe_psql(
                "postgres",
                "create extension ptrack")

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")

        heap_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()

        self.pb.backup_node('node', node,
            backup_type="full", options=["-j", "4", "--stream"])

        node.safe_psql(
            "postgres",
            "select count(*) from t_heap")

        node.safe_psql(
            "postgres",
            "update t_heap set id = id + 10000")

        node.stop()

        heap_fullpath = os.path.join(node.data_dir, heap_path)
        with open(heap_fullpath, "rb+", 0) as f:
                f.seek(8193)
                f.write(b"blahblahblahblah")
                f.flush()

        node.slow_start()

        modes = "full,delta,page"
        if self.ptrack:
            modes += ",ptrack"
        for mode in modes.split(','):
            with self.subTest(mode):
                self.pb.backup_node('node', node,
                                 backup_type=mode, options=["-j", "4", "--stream"],
                                 expect_error="because of block corruption")
                self.assertMessage(contains='ERROR: Corruption detected in file '
                                            f'"{heap_fullpath}", block 1: '
                                            'page header invalid, pd_lower')
            sleep(1)

    # @unittest.skip("skip")
    def test_backup_truncate_misaligned(self):
        """
        make node, truncate file to size not even to BLCKSIZE,
        take backup
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
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
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()

        heap_size = node.safe_psql(
            "postgres",
            "select pg_relation_size('t_heap')")

        with open(os.path.join(node.data_dir, heap_path), "rb+", 0) as f:
            f.truncate(int(heap_size) - 4096)
            f.flush()
            f.close

        output = self.pb.backup_node('node', node, backup_type="full",
            options=["-j", "4", "--stream"], return_id=False)

        self.assertIn("WARNING: File", output)
        self.assertIn("invalid file size", output)

    # @unittest.skip("skip")
    def test_tablespace_in_pgdata_pgpro_1376(self):
        """PGPRO-1376 """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir

        self.pb.init()
        self.pb.add_instance('node', node)
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

        backup_id_1 = self.pb.backup_node('node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        node.safe_psql(
            "postgres",
            "drop table t_heap2")
        node.safe_psql(
            "postgres",
            "drop tablespace tblspace2")

        self.pb.backup_node('node', node, backup_type="full",
                options=["-j", "4", "--stream"])

        pgdata = self.pgdata_content(node.data_dir)

        relfilenode = node.safe_psql(
            "postgres",
            "select 't_heap1'::regclass::oid"
            ).decode('utf-8').rstrip()

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

        self.pb.restore_node('node', node, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_basic_tablespace_handling(self):
        """
        make node, take full backup, check that restore with
        tablespace mapping will end with error, take page backup,
        check that restore with tablespace mapping will end with
        success
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node, backup_type="full",
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

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node=node_restored,
                          options=[
                              "-j", "4",
                              "-T", "{0}={1}".format(
                                  tblspace1_old_path, tblspace1_new_path),
                              "-T", "{0}={1}".format(
                                  tblspace2_old_path, tblspace2_new_path)],
                          expect_error="because tablespace mapping is incorrect")
        self.assertMessage(contains=
                f'ERROR: Backup {backup_id} has no tablespaceses, '
                'nothing to remap')

        node.safe_psql(
            "postgres",
            "drop table t_heap_lame")

        node.safe_psql(
            "postgres",
            "drop tablespace some_lame_tablespace")

        self.pb.backup_node('node', node, backup_type="delta",
            options=["-j", "4", "--stream"])

        self.pb.restore_node('node', node_restored,
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

    # @unittest.skip("skip")
    def test_tablespace_handling_1(self):
        """
        make node with tablespace A, take full backup, check that restore with
        tablespace mapping of tablespace B will end with error
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        tblspace1_old_path = self.get_tblspace_path(node, 'tblspace1_old')
        tblspace2_old_path = self.get_tblspace_path(node, 'tblspace2_old')

        tblspace_new_path = self.get_tblspace_path(node, 'tblspace_new')

        self.create_tblspace_in_node(
            node, 'tblspace1',
            tblspc_path=tblspace1_old_path)

        self.pb.backup_node('node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node=node_restored,
                          options=[
                              "-j", "4",
                              "-T", "{0}={1}".format(
                                  tblspace2_old_path, tblspace_new_path)],
                          expect_error="because tablespace mapping is incorrect")
        self.assertMessage(contains='ERROR: --tablespace-mapping option')
        self.assertMessage(contains='have an entry in tablespace_map file')

    # @unittest.skip("skip")
    def test_tablespace_handling_2(self):
        """
        make node without tablespaces, take full backup, check that restore with
        tablespace mapping will end with error
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        tblspace1_old_path = self.get_tblspace_path(node, 'tblspace1_old')
        tblspace_new_path = self.get_tblspace_path(node, 'tblspace_new')

        backup_id = self.pb.backup_node('node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node=node_restored,
                          options=[
                              "-j", "4",
                              "-T", "{0}={1}".format(
                                  tblspace1_old_path, tblspace_new_path)],
                          expect_error="because tablespace mapping is incorrect")
        self.assertMessage(contains=f'ERROR: Backup {backup_id} has no tablespaceses, '
                                    'nothing to remap')

    # @unittest.skip("skip")
    @needs_gdb
    def test_drop_rel_during_full_backup(self):
        """"""
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        for i in range(1, 512):
            node.safe_psql(
                "postgres",
                "create table t_heap_{0} as select i"
                " as id from generate_series(0,100) i".format(i))

        node.safe_psql(
            "postgres",
            "VACUUM")

        node.pgbench_init(scale=10)

        relative_path_1 = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap_1')").decode('utf-8').rstrip()

        relative_path_2 = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap_1')").decode('utf-8').rstrip()

        absolute_path_1 = os.path.join(node.data_dir, relative_path_1)
        absolute_path_2 = os.path.join(node.data_dir, relative_path_2)

        # FULL backup
        gdb = self.pb.backup_node('node', node,
            options=['--stream', '--log-level-console=LOG', '--progress'],
            gdb=True)

        gdb.set_breakpoint('backup_files')
        gdb.run_until_break()

        # REMOVE file
        for i in range(1, 512):
            node.safe_psql(
                "postgres",
                "drop table t_heap_{0}".format(i))

        node.safe_psql(
            "postgres",
            "CHECKPOINT")

        node.safe_psql(
            "postgres",
            "CHECKPOINT")

        # File removed, we can proceed with backup
        gdb.continue_execution_until_exit()

        pgdata = self.pgdata_content(node.data_dir)

        #log_content = self.read_pb_log()
        #self.assertTrue(
        #        'LOG: File "{0}" is not found'.format(absolute_path) in log_content,
        #        'File "{0}" should be deleted but it`s not'.format(absolute_path))

        node.cleanup()
        self.pb.restore_node('node', node=node)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    @unittest.skip("skip")
    def test_drop_db_during_full_backup(self):
        """"""
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        for i in range(1, 2):
            node.safe_psql(
                "postgres",
                "create database t_heap_{0}".format(i))

        node.safe_psql(
            "postgres",
            "VACUUM")

        # FULL backup
        gdb = self.pb.backup_node('node', node, gdb=True,
            options=[
                '--stream', '--log-level-file=LOG',
                '--log-level-console=LOG', '--progress'])

        gdb.set_breakpoint('backup_files')
        gdb.run_until_break()

        # REMOVE file
        for i in range(1, 2):
            node.safe_psql(
                "postgres",
                "drop database t_heap_{0}".format(i))

        node.safe_psql(
            "postgres",
            "CHECKPOINT")

        node.safe_psql(
            "postgres",
            "CHECKPOINT")

        # File removed, we can proceed with backup
        gdb.continue_execution_until_exit()

        pgdata = self.pgdata_content(node.data_dir)

        #log_content = self.read_pb_log()
        #self.assertTrue(
        #        'LOG: File "{0}" is not found'.format(absolute_path) in log_content,
        #        'File "{0}" should be deleted but it`s not'.format(absolute_path))

        node.cleanup()
        self.pb.restore_node('node', node=node)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    @needs_gdb
    def test_drop_rel_during_backup_delta(self):
        """"""
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=10)

        node.safe_psql(
            "postgres",
            "create table t_heap as select i"
            " as id from generate_series(0,100) i")

        relative_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()

        absolute_path = os.path.join(node.data_dir, relative_path)

        # FULL backup
        self.pb.backup_node('node', node, options=['--stream'])

        # DELTA backup
        gdb = self.pb.backup_node('node', node, backup_type='delta',
            gdb=True, options=['--log-level-file=LOG'])

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

        log_content = self.read_pb_log()
        self.assertTrue(
                'LOG: File not found: "{0}"'.format(absolute_path) in log_content,
                'File "{0}" should be deleted but it`s not'.format(absolute_path))

        node.cleanup()
        self.pb.restore_node('node', node=node, options=["-j", "4"])

        # Physical comparison
        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    @needs_gdb
    def test_drop_rel_during_backup_page(self):
        """"""

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i"
            " as id from generate_series(0,100) i")

        relative_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()

        absolute_path = os.path.join(node.data_dir, relative_path)

        # FULL backup
        self.pb.backup_node('node', node, options=['--stream'])

        node.safe_psql(
            "postgres",
            "insert into t_heap select i"
            " as id from generate_series(101,102) i")

        # PAGE backup
        gdb = self.pb.backup_node('node', node, backup_type='page',
            gdb=True, options=['--log-level-file=LOG'])

        gdb.set_breakpoint('backup_files')
        gdb.run_until_break()

        # REMOVE file
        os.remove(absolute_path)

        # File removed, we can proceed with backup
        gdb.continue_execution_until_exit()
        gdb.kill()

        pgdata = self.pgdata_content(node.data_dir)

        backup_id = self.pb.show('node')[1]['id']

        filelist = self.get_backup_filelist(backup_dir, 'node', backup_id)
        self.assertNotIn(relative_path, filelist)

        node.cleanup()
        self.pb.restore_node('node', node=node, options=["-j", "4"])

        # Physical comparison
        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_persistent_slot_for_stream_backup(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'max_wal_size': '40MB'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "SELECT pg_create_physical_replication_slot('slot_1')")

        # FULL backup. By default, --temp-slot=true.
        self.pb.backup_node('node', node,
            options=['--stream', '--slot=slot_1'],
            expect_error="because replication slot already exist")
        self.assertMessage(contains='ERROR:  replication slot "slot_1" already exists')

        # FULL backup
        self.pb.backup_node('node', node,
            options=['--stream', '--slot=slot_1', '--temp-slot=false'])

        # FULL backup
        self.pb.backup_node('node', node,
            options=['--stream', '--slot=slot_1', '--temp-slot=false'])

    # @unittest.skip("skip")
    def test_basic_temp_slot_for_stream_backup(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={'max_wal_size': '40MB'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL backup
        self.pb.backup_node('node', node,
            options=['--stream', '--temp-slot'])

        # FULL backup. By default, --temp-slot=true.
        self.pb.backup_node('node', node,
            options=['--stream', '--slot=slot_1'])

        # FULL backup
        self.pb.backup_node('node', node,
            options=['--stream', '--slot=slot_1', '--temp-slot=true'])

    # @unittest.skip("skip")
    @needs_gdb
    def test_backup_concurrent_drop_table(self):
        """"""
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=1)

        # FULL backup
        gdb = self.pb.backup_node('node', node,
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

        gdb.continue_execution_until_exit()
        gdb.kill()

        show_backup = self.pb.show('node')[0]

        self.assertEqual(show_backup['status'], "OK")

    def test_pg_11_adjusted_wal_segment_size(self):
        """"""
        if self.pg_config_version < self.version_to_num('11.0'):
            self.skipTest('You need PostgreSQL >= 11 for this test')

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            initdb_params=['--wal-segsize=64'],
            pg_options={
                'min_wal_size': '128MB'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=5)

        # FULL STREAM backup
        self.pb.backup_node('node', node, options=['--stream'])

        pgbench = node.pgbench(options=['-T', '5', '-c', '2'])
        pgbench.wait()

        # PAGE STREAM backup
        self.pb.backup_node('node', node,
            backup_type='page', options=['--stream'])

        pgbench = node.pgbench(options=['-T', '5', '-c', '2'])
        pgbench.wait()

        # DELTA STREAM backup
        self.pb.backup_node('node', node,
            backup_type='delta', options=['--stream'])

        pgbench = node.pgbench(options=['-T', '5', '-c', '2'])
        pgbench.wait()

        # FULL ARCHIVE backup
        self.pb.backup_node('node', node)

        pgbench = node.pgbench(options=['-T', '5', '-c', '2'])
        pgbench.wait()

        # PAGE ARCHIVE backup
        self.pb.backup_node('node', node, backup_type='page')

        pgbench = node.pgbench(options=['-T', '5', '-c', '2'])
        pgbench.wait()

        # DELTA ARCHIVE backup
        backup_id = self.pb.backup_node('node', node, backup_type='delta')
        pgdata = self.pgdata_content(node.data_dir)

        # delete
        output = self.pb.delete('node',
            options=[
                '--expired',
                '--delete-wal',
                '--retention-redundancy=1'])

        # validate
        self.pb.validate()

        # merge
        self.pb.merge_backup('node', backup_id=backup_id)

        # restore
        node.cleanup()
        self.pb.restore_node('node', node, backup_id=backup_id)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    @needs_gdb
    def test_sigint_handling(self):
        """"""

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # FULL backup
        gdb = self.pb.backup_node('node', node, gdb=True,
            options=['--stream', '--log-level-file=LOG'])

        gdb.set_breakpoint('backup_non_data_file')
        gdb.run_until_break()
        gdb.continue_execution_until_break(200)

        gdb.remove_all_breakpoints()
        gdb.signal('SIGINT')
        gdb.continue_execution_until_error()
        gdb.kill()

        backup_id = self.pb.show('node')[0]['id']

        self.assertEqual(
            'ERROR',
            self.pb.show('node', backup_id)['status'],
            'Backup STATUS should be "ERROR"')

    # @unittest.skip("skip")
    @needs_gdb
    def test_sigterm_handling(self):
        """"""

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # FULL backup
        gdb = self.pb.backup_node('node', node, gdb=True,
            options=['--stream', '--log-level-file=LOG'])

        gdb.set_breakpoint('backup_non_data_file')
        gdb.run_until_break()
        gdb.continue_execution_until_break(200)

        gdb.signal('SIGTERM')
        gdb.continue_execution_until_error()

        backup_id = self.pb.show('node')[0]['id']

        self.assertEqual(
            'ERROR',
            self.pb.show('node', backup_id)['status'],
            'Backup STATUS should be "ERROR"')

    # @unittest.skip("skip")
    @needs_gdb
    def test_sigquit_handling(self):
        """"""

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # FULL backup
        gdb = self.pb.backup_node('node', node, gdb=True, options=['--stream'])

        gdb.set_breakpoint('backup_non_data_file')
        gdb.run_until_break()
        gdb.continue_execution_until_break(200)

        gdb.signal('SIGQUIT')
        gdb.continue_execution_until_error()

        backup_id = self.pb.show('node')[0]['id']

        self.assertEqual(
            'ERROR',
            self.pb.show('node', backup_id)['status'],
            'Backup STATUS should be "ERROR"')

    # @unittest.skip("skip")
    def test_drop_table(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
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
        self.pb.backup_node('node', node, options=['--stream'])

    # @unittest.skip("skip")
    def test_basic_missing_file_permissions(self):
        """"""
        if os.name == 'nt':
            self.skipTest('Skipped because it is POSIX only test')

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        relative_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('pg_class')").decode('utf-8').rstrip()

        full_path = os.path.join(node.data_dir, relative_path)

        os.chmod(full_path, 000)

        # FULL backup
        self.pb.backup_node('node', node, options=['--stream'],
                         expect_error="because of missing permissions")
        self.assertMessage(regex=r"ERROR: [^\n]*: Permission denied")

        os.chmod(full_path, 700)

    # @unittest.skip("skip")
    def test_basic_missing_dir_permissions(self):
        """"""
        if os.name == 'nt':
            self.skipTest('Skipped because it is POSIX only test')

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        full_path = os.path.join(node.data_dir, 'pg_twophase')

        os.chmod(full_path, 000)

            # FULL backup
        self.pb.backup_node('node', node, options=['--stream'],
                         expect_error="because of missing permissions")
        self.assertMessage(regex=r'ERROR:[^\n]*Cannot open dir')

        os.rmdir(full_path)

    # @unittest.skip("skip")
    def test_backup_with_least_privileges_role(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=self.ptrack,
            pg_options={'archive_timeout': '10s'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            'postgres',
            'CREATE DATABASE backupdb')

        if self.ptrack:
            node.safe_psql(
                "backupdb",
                "CREATE SCHEMA ptrack; "
                "CREATE EXTENSION ptrack WITH SCHEMA ptrack")

        if self.pg_config_version < 150000:
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
                "GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_proc TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_database TO backup; " # for partial restore, checkdb and ptrack
                "GRANT EXECUTE ON FUNCTION pg_catalog.oideq(oid, oid) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.nameeq(name, name) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.set_config(text, text, boolean) TO backup; "
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
        # >= 15
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
                "GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_proc TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_database TO backup; " # for partial restore, checkdb and ptrack
                "GRANT EXECUTE ON FUNCTION pg_catalog.oideq(oid, oid) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.nameeq(name, name) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.set_config(text, text, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_backup_start(text, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_backup_stop(boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_create_restore_point(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_wal() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_last_wal_replay_lsn() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_current_snapshot() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_snapshot_xmax(txid_snapshot) TO backup;"
            )

        if self.ptrack:
            node.safe_psql(
                 "backupdb",
                 "GRANT USAGE ON SCHEMA ptrack TO backup")

            node.safe_psql(
                "backupdb",
                "GRANT EXECUTE ON FUNCTION ptrack.ptrack_get_pagemapset(pg_lsn) TO backup; "
                "GRANT EXECUTE ON FUNCTION ptrack.ptrack_init_lsn() TO backup;")

        if ProbackupTest.pgpro:
            node.safe_psql(
                "backupdb",
                "GRANT EXECUTE ON FUNCTION pg_catalog.pgpro_version() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pgpro_edition() TO backup;")

        # FULL backup
        self.pb.backup_node('node', node,
            datname='backupdb', options=['--stream', '-U', 'backup'])
        self.pb.backup_node('node', node,
            datname='backupdb', options=['-U', 'backup'])

        # PAGE
        self.pb.backup_node('node', node, backup_type='page',
            datname='backupdb', options=['-U', 'backup'])
        self.pb.backup_node('node', node, backup_type='page', datname='backupdb',
            options=['--stream', '-U', 'backup'])

        # DELTA
        self.pb.backup_node('node', node, backup_type='delta',
            datname='backupdb', options=['-U', 'backup'])
        self.pb.backup_node('node', node, backup_type='delta',
            datname='backupdb', options=['--stream', '-U', 'backup'])

        # PTRACK
        if self.ptrack:
            self.pb.backup_node('node', node, backup_type='ptrack',
                datname='backupdb', options=['-U', 'backup'])
            self.pb.backup_node('node', node, backup_type='ptrack',
                datname='backupdb', options=['--stream', '-U', 'backup'])

    # @unittest.skip("skip")
    def test_parent_choosing(self):
        """
        PAGE3 <- RUNNING(parent should be FULL)
        PAGE2 <- OK
        PAGE1 <- CORRUPT
        FULL
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        full_id = self.pb.backup_node('node', node)

        # PAGE1
        page1_id = self.pb.backup_node('node', node, backup_type='page')

        # PAGE2
        page2_id = self.pb.backup_node('node', node, backup_type='page')

        # Change PAGE1 to ERROR
        self.change_backup_status(backup_dir, 'node', page1_id, 'ERROR')

        # PAGE3
        page3_id = self.pb.backup_node('node', node,
            backup_type='page', options=['--log-level-file=LOG'])

        log_file_content = self.read_pb_log()

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
            self.pb.show('node', backup_id=page3_id)['parent-backup-id'],
            full_id)

    # @unittest.skip("skip")
    def test_parent_choosing_1(self):
        """
        PAGE3 <- RUNNING(parent should be FULL)
        PAGE2 <- OK
        PAGE1 <- (missing)
        FULL
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        full_id = self.pb.backup_node('node', node)

        # PAGE1
        page1_id = self.pb.backup_node('node', node, backup_type='page')

        # PAGE2
        page2_id = self.pb.backup_node('node', node, backup_type='page')

        # Delete PAGE1
        self.remove_one_backup(backup_dir, 'node', page1_id)

        # PAGE3
        page3_id = self.pb.backup_node('node', node,
            backup_type='page', options=['--log-level-file=LOG'])

        log_file_content = self.read_pb_log()

        self.assertIn(
            "WARNING: Backup {0} has missing parent: {1}. "
            "Cannot be a parent".format(page2_id, page1_id),
            log_file_content)

        self.assertIn(
            "Parent backup: {0}".format(full_id),
            log_file_content)

        self.assertEqual(
            self.pb.show('node', backup_id=page3_id)['parent-backup-id'],
            full_id)

    # @unittest.skip("skip")
    def test_parent_choosing_2(self):
        """
        PAGE3 <- RUNNING(backup should fail)
        PAGE2 <- OK
        PAGE1 <- OK
        FULL  <- (missing)
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        full_id = self.pb.backup_node('node', node)

        # PAGE1
        page1_id = self.pb.backup_node('node', node, backup_type='page')

        # PAGE2
        page2_id = self.pb.backup_node('node', node, backup_type='page')

        # Delete FULL
        self.remove_one_backup(backup_dir, 'node', full_id)

        # PAGE3
        self.pb.backup_node('node', node,
                backup_type='page', options=['--log-level-file=LOG'],
                expect_error="because FULL backup is missing")
        self.assertMessage(contains='WARNING: Valid full backup on current timeline 1 is not found')
        self.assertMessage(contains='ERROR: Create new full backup before an incremental one')

        self.assertEqual(
            self.pb.show('node')[2]['status'],
            'ERROR')

    # @unittest.skip("skip")
    @needs_gdb
    def test_backup_with_less_privileges_role(self):
        """
        check permissions correctness from documentation:
        https://github.com/postgrespro/pg_probackup/blob/master/Documentation.md#configuring-the-database-cluster
        """
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=self.ptrack,
            pg_options={
                'archive_timeout': '10s',
                'archive_mode': 'always',
                'checkpoint_timeout': '30s',
                'wal_level': 'logical'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_config('node', options=['--archive-timeout=30s'])
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            'postgres',
            'CREATE DATABASE backupdb')

        if self.ptrack:
            node.safe_psql(
                'backupdb',
                'CREATE EXTENSION ptrack')

        if self.pg_config_version < 150000:
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
        # >= 15
        else:
            node.safe_psql(
                'backupdb',
                "BEGIN; "
                "CREATE ROLE backup WITH LOGIN; "
                "GRANT USAGE ON SCHEMA pg_catalog TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_backup_start(text, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_backup_stop(boolean) TO backup; "
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
        self.pb.backup_node('node', node,
            datname='backupdb', options=['--stream', '-U', 'backup'])
        self.pb.backup_node('node', node,
            datname='backupdb', options=['-U', 'backup'])

        # PAGE
        self.pb.backup_node('node', node, backup_type='page',
            datname='backupdb', options=['-U', 'backup'])
        self.pb.backup_node('node', node, backup_type='page', datname='backupdb',
            options=['--stream', '-U', 'backup'])

        # DELTA
        self.pb.backup_node('node', node, backup_type='delta',
            datname='backupdb', options=['-U', 'backup'])
        self.pb.backup_node('node', node, backup_type='delta',
            datname='backupdb', options=['--stream', '-U', 'backup'])

        # PTRACK
        if self.ptrack:
            self.pb.backup_node('node', node, backup_type='ptrack',
                datname='backupdb', options=['-U', 'backup'])
            self.pb.backup_node('node', node, backup_type='ptrack',
                datname='backupdb', options=['--stream', '-U', 'backup'])

        # Restore as replica
        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.restore_node('node', node=replica)
        self.set_replica(node, replica)
        self.pb.add_instance('replica', replica)
        self.pb.set_config('replica',
            options=['--archive-timeout=60s', '--log-level-console=LOG'])
        self.pb.set_archiving('replica', replica, replica=True)
        replica.set_auto_conf({'hot_standby': 'on'})

        # freeze bgwriter to get rid of RUNNING XACTS records
        # bgwriter_pid = node.auxiliary_pids[ProcessType.BackgroundWriter][0]
        # gdb_checkpointer = self.gdb_attach(bgwriter_pid)

        replica.slow_start(replica=True)

        # make sure replica will archive wal segment with backup start point
        lsn = self.switch_wal_segment(node, and_tx=True)
        self.wait_until_lsn_replayed(replica, lsn)
        replica.execute('CHECKPOINT')
        replica.poll_query_until(f"select redo_lsn >= '{lsn}' from pg_control_checkpoint()")

        self.pb.backup_replica_node('replica', replica, master=node,
                datname='backupdb', options=['-U', 'backup'])

        # stream full backup from replica
        self.pb.backup_node('replica', replica,
            datname='backupdb', options=['--stream', '-U', 'backup'])

#        self.switch_wal_segment(node)

        # PAGE backup from replica
        self.pb.backup_replica_node('replica', replica, master=node,
                backup_type='page', datname='backupdb',
                options=['-U', 'backup'])

        self.pb.backup_node('replica', replica, backup_type='page',
            datname='backupdb', options=['--stream', '-U', 'backup'])

        # DELTA backup from replica
        self.pb.backup_replica_node('replica', replica, master=node,
                backup_type='delta', datname='backupdb',
                options=['-U', 'backup'])
        self.pb.backup_node('replica', replica, backup_type='delta',
            datname='backupdb', options=['--stream', '-U', 'backup'])

        # PTRACK backup from replica
        if self.ptrack:
            self.pb.backup_replica_node('replica', replica, master=node,
                    backup_type='ptrack', datname='backupdb',
                    options=['-U', 'backup'])
            self.pb.backup_node('replica', replica, backup_type='ptrack',
                datname='backupdb', options=['--stream', '-U', 'backup'])

    @unittest.skip("skip")
    def test_issue_132(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/132
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        with node.connect("postgres") as conn:
            for i in range(50000):
                conn.execute(
                    "CREATE TABLE t_{0} as select 1".format(i))
                conn.commit()

        self.pb.backup_node('node', node, options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        node.cleanup()
        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        exit(1)

    @unittest.skip("skip")
    def test_issue_132_1(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/132
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        # TODO: check version of old binary, it should be 2.1.4, 2.1.5 or 2.2.1

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        with node.connect("postgres") as conn:
            for i in range(30000):
                conn.execute(
                    "CREATE TABLE t_{0} as select 1".format(i))
                conn.commit()

        full_id = self.pb.backup_node('node', node, options=['--stream'], old_binary=True)

        delta_id = self.pb.backup_node('node', node, backup_type='delta',
            options=['--stream'], old_binary=True)

        node.cleanup()

        # make sure that new binary can detect corruption
        self.pb.validate('node', backup_id=full_id,
                         expect_error="because FULL backup is CORRUPT")
        self.assertMessage(contains=
                f'WARNING: Backup {full_id} is a victim of metadata corruption')

        self.pb.validate('node', backup_id=delta_id,
                         expect_error="because FULL backup is CORRUPT")
        self.assertMessage(contains=
                           f'WARNING: Backup {full_id} is a victim of metadata corruption')

        self.assertEqual(
            'CORRUPT', self.pb.show('node', full_id)['status'],
            'Backup STATUS should be "CORRUPT"')

        self.assertEqual(
            'ORPHAN', self.pb.show('node', delta_id)['status'],
            'Backup STATUS should be "ORPHAN"')

        # check that revalidation is working correctly
        self.pb.validate('node', backup_id=delta_id,
                         expect_error="because FULL backup is CORRUPT")
        self.assertMessage(contains=
                           f'WARNING: Backup {full_id} is a victim of metadata corruption')

        self.assertEqual(
            'CORRUPT', self.pb.show('node', full_id)['status'],
            'Backup STATUS should be "CORRUPT"')

        self.assertEqual(
            'ORPHAN', self.pb.show('node', delta_id)['status'],
            'Backup STATUS should be "ORPHAN"')

        # check that '--no-validate' do not allow to restore ORPHAN backup
#       self.pb.restore_node('node', node=node, backup_id=delta_id,
#                         options=['--no-validate'],
#                         expect_error="because FULL backup is CORRUPT")
#       self.assertMessage(contains='Insert data')

        node.cleanup()

        output = self.pb.restore_node('node', node, backup_id=full_id, options=['--force'])

        self.assertIn(
            'WARNING: Backup {0} has status: CORRUPT'.format(full_id),
            output)

        self.assertIn(
            'WARNING: Backup {0} is corrupt.'.format(full_id),
            output)

        self.assertIn(
            'WARNING: Backup {0} is not valid, restore is forced'.format(full_id),
            output)

        self.assertIn(
            'INFO: Restore of backup {0} completed.'.format(full_id),
            output)

        node.cleanup()

        output = self.pb.restore_node('node', node, backup_id=delta_id, options=['--force'])

        self.assertIn(
            'WARNING: Backup {0} is orphan.'.format(delta_id),
            output)

        self.assertIn(
            'WARNING: Backup {0} is not valid, restore is forced'.format(full_id),
            output)

        self.assertIn(
            'WARNING: Backup {0} is not valid, restore is forced'.format(delta_id),
            output)

        self.assertIn(
            'INFO: Restore of backup {0} completed.'.format(delta_id),
            output)

    def test_note_sanity(self):
        """
        test that adding note to backup works as expected
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL backup
        backup_id = self.pb.backup_node('node', node,
            options=['--stream', '--log-level-file=LOG', '--note=test_note'])

        show_backups = self.pb.show('node')

        print(self.pb.show(as_text=True, as_json=True))

        self.assertEqual(show_backups[0]['note'], "test_note")

        self.pb.set_backup('node', backup_id, options=['--note=none'])

        backup_meta = self.pb.show('node', backup_id)

        self.assertNotIn(
            'note',
            backup_meta)

    # @unittest.skip("skip")
    def test_parent_backup_made_by_newer_version(self):
        """incremental backup with parent made by newer version"""
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node)

        version = self.probackup_version
        fake_new_version = str(int(version.split('.')[0]) + 1) + '.0.0'

        with self.modify_backup_control(backup_dir, "node", backup_id) as cf:
            cf.data = cf.data.replace(version, fake_new_version)

        self.pb.backup_node('node', node, backup_type="page",
                         expect_error="because incremental backup should not be possible")
        self.assertMessage(contains=
                "pg_probackup do not guarantee to be forward compatible. "
                "Please upgrade pg_probackup binary.")

        self.assertEqual(
            self.pb.show('node')[1]['status'], "ERROR")

    # @unittest.skip("skip")
    def test_issue_289(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/289
        """
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)

        node.slow_start()

        self.pb.backup_node('node', node,
                backup_type='page', options=['--archive-timeout=10s'],
                         expect_error="because full backup is missing")
        self.assertMessage(has_no="INFO: Wait for WAL segment")
        self.assertMessage(contains="ERROR: Create new full backup before an incremental one")

        self.assertEqual(
            self.pb.show('node')[0]['status'], "ERROR")

    # @unittest.skip("skip")
    def test_issue_290(self):
        """
        For archive backup make sure that archive dir exists.

        https://github.com/postgrespro/pg_probackup/issues/290
        """
        backup_dir = self.backup_dir

        if not backup_dir.is_file_based:
            self.skipTest("directories are not implemented on cloud storage")

        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)

        self.remove_instance_waldir(backup_dir, 'node')

        node.slow_start()

        self.pb.backup_node('node', node,
                         options=['--archive-timeout=10s'],
                         expect_error="because full backup is missing")
        self.assertMessage(has_no="INFO: Wait for WAL segment")
        self.assertMessage(contains="WAL archive directory is not accessible")

        self.assertEqual(
            self.pb.show('node')[0]['status'], "ERROR")

    @unittest.skip("skip")
    def test_issue_203(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/203
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        with node.connect("postgres") as conn:
            for i in range(1000000):
                conn.execute(
                    "CREATE TABLE t_{0} as select 1".format(i))
                conn.commit()

        full_id = self.pb.backup_node('node', node, options=['--stream', '-j2'])

        pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node=node_restored)

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_issue_231(self):
        """
        Backups get the same ID if they are created within the same second.
        https://github.com/postgrespro/pg_probackup/issues/231
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)

        datadir = os.path.join(node.data_dir, '123')

        t0 = time()
        while True:
            output = self.pb.backup_node('node', node,
                                      expect_error=True)
            pb1 = re.search(r' backup ID: ([^\s,]+),', output).groups()[0]

            t = time()
            if int(pb1, 36) == int(t) and t % 1 < 0.5:
                # ok, we have a chance to start next backup in same second
                break
            elif t - t0 > 20:
                # Oops, we are waiting for too long. Looks like this runner
                # is too slow. Lets skip the test.
                self.skipTest("runner is too slow")
            # sleep to the second's end so backup will not sleep for a second.
            sleep(1 - t % 1)

        output = self.pb.backup_node('node', node, expect_error=True)
        pb2 = re.search(r' backup ID: ([^\s,]+),', output).groups()[0]

        self.assertNotEqual(pb1, pb2)

    def test_incr_backup_filenode_map(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/320
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node1 = self.pg_node.make_simple('node1')
        node1.cleanup()

        node.pgbench_init(scale=5)

        # FULL backup
        backup_id = self.pb.backup_node('node', node)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            options=['-T', '10', '-c', '1'])

        backup_id = self.pb.backup_node('node', node, backup_type='delta')

        node.safe_psql(
            'postgres',
            'reindex index pg_type_oid_index')

        backup_id = self.pb.backup_node('node', node, backup_type='delta')

        # incremental restore into node1
        node.cleanup()

        self.pb.restore_node('node', node=node)
        node.slow_start()

        node.safe_psql(
            'postgres',
            'select 1')

    # @unittest.skip("skip")
    @needs_gdb
    def test_missing_wal_segment(self):
        """"""

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=self.ptrack,
            pg_options={'archive_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=10)

        node.safe_psql(
            'postgres',
            'CREATE DATABASE backupdb')

        # get segments in pg_wal, sort then and remove all but the latest
        pg_wal_dir = os.path.join(node.data_dir, 'pg_wal')

        if node.major_version >= 10:
            pg_wal_dir = os.path.join(node.data_dir, 'pg_wal')
        else:
            pg_wal_dir = os.path.join(node.data_dir, 'pg_xlog')

        # Full backup in streaming mode
        gdb = self.pb.backup_node('node', node, datname='backupdb',
            options=['--stream', '--log-level-file=INFO'], gdb=True)

        # break at streaming start
        gdb.set_breakpoint('start_WAL_streaming')
        gdb.run_until_break()

        # generate some more data
        node.pgbench_init(scale=3)

        # remove redundant WAL segments in pg_wal
        files = os.listdir(pg_wal_dir)
        files.sort(reverse=True)

        # leave first two files in list
        del files[:2]
        for filename in files:
            os.remove(os.path.join(pg_wal_dir, filename))

        gdb.continue_execution_until_exit()

        self.assertIn(
            'unexpected termination of replication stream: ERROR:  requested WAL segment',
            gdb.output)

        self.assertIn(
            'has already been removed',
            gdb.output)

        self.assertIn(
            'ERROR: Interrupted during waiting for WAL streaming',
            gdb.output)

        self.assertIn(
            'WARNING: A backup is in progress, stopping it',
            gdb.output)

        # TODO: check the same for PAGE backup

    # @unittest.skip("skip")
    def test_missing_replication_permission(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=self.ptrack)

        self.pb.init()
        self.pb.add_instance('node', node)
#        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL backup
        self.pb.backup_node('node', node, options=['--stream'])

        # Create replica
        replica = self.pg_node.make_simple('replica')
        replica.cleanup()
        self.pb.restore_node('node', node=replica)

        # Settings for Replica
        self.set_replica(node, replica)
        replica.slow_start(replica=True)

        node.safe_psql(
            'postgres',
            'CREATE DATABASE backupdb')

        if self.pg_config_version < 150000:
            node.safe_psql(
                'backupdb',
                "CREATE ROLE backup WITH LOGIN; "
                "GRANT CONNECT ON DATABASE backupdb to backup; "
                "GRANT USAGE ON SCHEMA pg_catalog TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_proc TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup; "
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
        # >= 15
        else:
            node.safe_psql(
                'backupdb',
                "CREATE ROLE backup WITH LOGIN; "
                "GRANT CONNECT ON DATABASE backupdb to backup; "
                "GRANT USAGE ON SCHEMA pg_catalog TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_proc TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_database TO backup; " # for partial restore, checkdb and ptrack
                "GRANT EXECUTE ON FUNCTION pg_catalog.nameeq(name, name) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_backup_start(text, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_backup_stop(boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_create_restore_point(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_wal() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_last_wal_replay_lsn() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_current_snapshot() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_snapshot_xmax(txid_snapshot) TO backup;"
            )

        if ProbackupTest.pgpro:
            node.safe_psql(
                "backupdb",
                "GRANT EXECUTE ON FUNCTION pg_catalog.pgpro_version() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pgpro_edition() TO backup;")

        sleep(2)
        replica.promote()

        # Delta backup
        self.pb.backup_node('node', replica, backup_type='delta',
                         data_dir=replica.data_dir, datname='backupdb',
                         options=['--stream', '-U', 'backup'],
                         expect_error="because incremental backup should not be possible")

        if self.pg_config_version < 160000:
            self.assertMessage(
                contains=r"FATAL:  must be superuser or replication role to start walsender")
        else:
            self.assertMessage(
                contains="FATAL:  permission denied to start WAL sender\n"
                         "DETAIL:  Only roles with the REPLICATION")

    # @unittest.skip("skip")
    def test_missing_replication_permission_1(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=self.ptrack)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL backup
        self.pb.backup_node('node', node, options=['--stream'])

        # Create replica
        replica = self.pg_node.make_simple('replica')
        replica.cleanup()
        self.pb.restore_node('node', node=replica)

        # Settings for Replica
        self.set_replica(node, replica)
        replica.slow_start(replica=True)

        node.safe_psql(
            'postgres',
            'CREATE DATABASE backupdb')

        # >= 10 && < 15
        if self.pg_config_version >= 100000 and self.pg_config_version < 150000:
            node.safe_psql(
                'backupdb',
                "CREATE ROLE backup WITH LOGIN; "
                "GRANT CONNECT ON DATABASE backupdb to backup; "
                "GRANT USAGE ON SCHEMA pg_catalog TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_proc TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup; "
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
        # > 15
        else:
            node.safe_psql(
                'backupdb',
                "CREATE ROLE backup WITH LOGIN; "
                "GRANT CONNECT ON DATABASE backupdb to backup; "
                "GRANT USAGE ON SCHEMA pg_catalog TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_proc TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_database TO backup; " # for partial restore, checkdb and ptrack
                "GRANT EXECUTE ON FUNCTION pg_catalog.nameeq(name, name) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_backup_start(text, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_backup_stop(boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_create_restore_point(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_wal() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_last_wal_replay_lsn() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_current_snapshot() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_snapshot_xmax(txid_snapshot) TO backup;"
            )

        if ProbackupTest.pgpro:
            node.safe_psql(
                "backupdb",
                "GRANT EXECUTE ON FUNCTION pg_catalog.pgpro_version() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pgpro_edition() TO backup;")

        replica.promote()

        # PAGE
        output = self.pb.backup_node('node', replica, backup_type='page',
            data_dir=replica.data_dir, datname='backupdb', options=['-U', 'backup'],
            return_id=False)

        self.assertIn(
            'WARNING: Valid full backup on current timeline 2 is not found, trying to look up on previous timelines',
            output)

        # Messages before 14
        # 'WARNING: could not connect to database backupdb: FATAL:  must be superuser or replication role to start walsender'
        # Messages for >=14
        # 'WARNING: could not connect to database backupdb: connection to server on socket "/tmp/.s.PGSQL.30983" failed: FATAL:  must be superuser or replication role to start walsender'
        # 'WARNING: could not connect to database backupdb: connection to server at "localhost" (127.0.0.1), port 29732 failed: FATAL:  must be superuser or replication role to start walsender'
        # OS-dependant messages:
        # 'WARNING: could not connect to database backupdb: connection to server at "localhost" (::1), port 12101 failed: Connection refused\n\tIs the server running on that host and accepting TCP/IP connections?\nconnection to server at "localhost" (127.0.0.1), port 12101 failed: FATAL:  must be superuser or replication role to start walsender'
        if self.pg_config_version < 160000:
            self.assertRegex(
                output,
                r'WARNING: could not connect to database backupdb:[\s\S]*?'
                r'FATAL:  must be superuser or replication role to start walsender')
        else:
            self.assertRegex(
                output,
                r'WARNING: could not connect to database backupdb:[\s\S]*?'
                r'FATAL:  permission denied to start WAL sender')

    # @unittest.skip("skip")
    def test_basic_backup_default_transaction_read_only(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={'default_transaction_read_only': 'on'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        error_result = node.safe_psql('postgres',
                                      'create temp table t1()', expect_error=True)

        self.assertMessage(error_result, contains="cannot execute CREATE TABLE in a read-only transaction")

        # FULL backup
        self.pb.backup_node('node', node,
            options=['--stream'])

        # DELTA backup
        self.pb.backup_node('node', node, backup_type='delta', options=['--stream'])

        # PAGE backup
        self.pb.backup_node('node', node, backup_type='page')

    # @unittest.skip("skip")
    @needs_gdb
    def test_backup_atexit(self):
        """"""

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=self.ptrack)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=5)

        # Full backup in streaming mode
        gdb = self.pb.backup_node('node', node,
            options=['--stream', '--log-level-file=VERBOSE'], gdb=True)

        # break at streaming start
        gdb.set_breakpoint('backup_data_file')
        gdb.run_until_break()

        gdb.remove_all_breakpoints()
        gdb.signal('SIGINT')

        timeout = 60
        status = self.pb.show('node')[0]['status']
        while status == 'RUNNING' or timeout > 0:
            sleep(1)
            timeout = timeout - 1
            status = self.pb.show('node')[0]['status']

        self.assertEqual(status, 'ERROR')

        log_content = self.read_pb_log()

        self.assertIn(
                'WARNING: A backup is in progress, stopping it',
                log_content)

        if self.pg_config_version < 150000:
            self.assertIn(
                    'FROM pg_catalog.pg_stop_backup',
                    log_content)
        else:
            self.assertIn(
                    'FROM pg_catalog.pg_backup_stop',
                    log_content)

        self.assertIn(
                'setting its status to ERROR',
                log_content)

    # @unittest.skip("skip")
    def test_pg_stop_backup_missing_permissions(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=self.ptrack)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=5)

        self.simple_bootstrap(node, 'backup')

        if self.pg_config_version < 150000:
            node.safe_psql(
                'postgres',
                'REVOKE EXECUTE ON FUNCTION pg_catalog.pg_stop_backup(boolean, boolean) FROM backup')
        else:
            node.safe_psql(
                'postgres',
                'REVOKE EXECUTE ON FUNCTION pg_catalog.pg_backup_stop(boolean) FROM backup')

        if self.pg_config_version < 150000:
            stop_backup = "pg_stop_backup"
        else:
            stop_backup = "pg_backup_stop"
        # Full backup in streaming mode
        self.pb.backup_node('node', node,
                         options=['--stream', '-U', 'backup'],
                         expect_error=f"because of missing permissions on {stop_backup}")
        self.assertMessage(contains=f"ERROR:  permission denied for function {stop_backup}")
        self.assertMessage(contains="query was: SELECT pg_catalog.txid_snapshot_xmax")

    # @unittest.skip("skip")
    def test_start_time(self):
        """Test, that option --start-time allows to set backup_id and restore"""
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=self.ptrack)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL backup
        startTimeFull = int(time())
        self.pb.backup_node('node', node, backup_type='full',
            options=['--stream', '--start-time={0}'.format(startTimeFull)])
        # restore FULL backup by backup_id calculated from start-time
        # cleanup it if we have leftover from a failed test
        node_restored_full = self.pg_node.make_empty('node_restored_full')
        self.pb.restore_node('node', node_restored_full,
            backup_id=base36enc(startTimeFull))

        #FULL backup with incorrect start time
        startTime = startTimeFull-100000
        self.pb.backup_node('node', node, backup_type='full',
                options=['--stream', '--start-time={0}'.format(startTime)],
                         expect_error="because start time for new backup must be newer")
        self.assertMessage(
            regex=r"ERROR: Can't assign backup_id from requested start_time "
                  r"\(\w*\), this time must be later that backup \w*\n")

        # DELTA backup
        startTime = max(int(time()), startTimeFull+1)
        self.pb.backup_node('node', node, backup_type='delta',
            options=['--stream', '--start-time={0}'.format(str(startTime))])
        # restore DELTA backup by backup_id calculated from start-time
        node_restored_delta = self.pg_node.make_empty('node_restored_delta')
        self.pb.restore_node('node', node_restored_delta,
            backup_id=base36enc(startTime))

        # PAGE backup
        startTime = max(int(time()), startTime+1)
        self.pb.backup_node('node', node, backup_type='page',
            options=['--stream', '--start-time={0}'.format(str(startTime))])
        # restore PAGE backup by backup_id calculated from start-time
        node_restored_page = self.pg_node.make_empty('node_restored_page')
        self.pb.restore_node('node', node=node_restored_page,
                          backup_id=base36enc(startTime))

        # PTRACK backup
        if self.ptrack:
            node.safe_psql(
                'postgres',
                'create extension ptrack')

            startTime = max(int(time()), startTime+1)
            self.pb.backup_node('node', node, backup_type='ptrack',
                options=['--stream', '--start-time={0}'.format(str(startTime))])
            # restore PTRACK backup by backup_id calculated from start-time
            node_restored_ptrack = self.pg_node.make_empty('node_restored_ptrack')
            self.pb.restore_node('node', node_restored_ptrack,
                backup_id=base36enc(startTime))

    # @unittest.skip("skip")
    def test_start_time_few_nodes(self):
        """Test, that we can synchronize backup_id's for different DBs"""
        node1 = self.pg_node.make_simple('node1',
            set_replication=True,
            ptrack_enable=self.ptrack)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node1', node1)
        self.pb.set_archiving('node1', node1)
        node1.slow_start()

        node2 = self.pg_node.make_simple('node2',
            set_replication=True,
            ptrack_enable=self.ptrack)

        self.pb.add_instance('node2', node2)
        self.pb.set_archiving('node2', node2)
        node2.slow_start()

        # FULL backup
        startTime = int(time())
        self.pb.backup_node('node1', node1, backup_type='full',
            options=['--stream', '--start-time={0}'.format(startTime)])
        self.pb.backup_node('node2', node2, backup_type='full',
            options=['--stream', '--start-time={0}'.format(startTime)])
        show_backup1 = self.pb.show('node1')[0]
        show_backup2 = self.pb.show('node2')[0]
        self.assertEqual(show_backup1['id'], show_backup2['id'])

        # DELTA backup
        startTime = max(int(time()), startTime+1)
        self.pb.backup_node('node1', node1, backup_type='delta',
            options=['--stream', '--start-time={0}'.format(startTime)])
        self.pb.backup_node('node2', node2, backup_type='delta',
            options=['--stream', '--start-time={0}'.format(startTime)])
        show_backup1 = self.pb.show('node1')[1]
        show_backup2 = self.pb.show('node2')[1]
        self.assertEqual(show_backup1['id'], show_backup2['id'])

        # PAGE backup
        startTime = max(int(time()), startTime+1)
        self.pb.backup_node('node1', node1, backup_type='page',
            options=['--stream', '--start-time={0}'.format(startTime)])
        self.pb.backup_node('node2', node2, backup_type='page',
            options=['--stream', '--start-time={0}'.format(startTime)])
        show_backup1 = self.pb.show('node1')[2]
        show_backup2 = self.pb.show('node2')[2]
        self.assertEqual(show_backup1['id'], show_backup2['id'])

        # PTRACK backup
        if self.ptrack:
            node1.safe_psql(
                'postgres',
                'create extension ptrack')
            node2.safe_psql(
                'postgres',
                'create extension ptrack')

            startTime = max(int(time()), startTime+1)
            self.pb.backup_node(
                'node1', node1, backup_type='ptrack',
                options=['--stream', '--start-time={0}'.format(startTime)])
            self.pb.backup_node('node2', node2, backup_type='ptrack',
                options=['--stream', '--start-time={0}'.format(startTime)])
            show_backup1 = self.pb.show('node1')[3]
            show_backup2 = self.pb.show('node2')[3]
            self.assertEqual(show_backup1['id'], show_backup2['id'])

    def test_regress_issue_585(self):
        """https://github.com/postgrespro/pg_probackup/issues/585"""
        node = self.pg_node.make_simple(
            base_dir='node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # create couple of files that looks like db files
        with open(os.path.join(node.data_dir, 'pg_multixact/offsets/1000'),'wb') as f:
            pass
        with open(os.path.join(node.data_dir, 'pg_multixact/members/1000'),'wb') as f:
            pass

        self.pb.backup_node('node', node, backup_type='full',
            options=['--stream'])

        output = self.pb.backup_node('node', node, backup_type='delta',
            options=['--stream'],
            return_id=False,
        )
        self.assertNotRegex(output, r'WARNING: [^\n]* was stored as .* but looks like')

        node.cleanup()

        output = self.pb.restore_node('node', node)
        self.assertNotRegex(output, r'WARNING: [^\n]* was stored as .* but looks like')

    def test_2_delta_backups(self):
        """https://github.com/postgrespro/pg_probackup/issues/596"""
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir

        self.pb.init()
        self.pb.add_instance('node', node)
        # self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL
        full_backup_id = self.pb.backup_node('node', node, options=["--stream"])

        # delta backup mode
        delta_backup_id1 = self.pb.backup_node('node', node, backup_type="delta", options=["--stream"])

        delta_backup_id2 = self.pb.backup_node('node', node, backup_type="delta", options=["--stream"])

        # postgresql.conf and pg_hba.conf shouldn't be copied
        conf_file = os.path.join(backup_dir, 'backups', 'node', delta_backup_id1, 'database', 'postgresql.conf')
        self.assertFalse(
            os.path.exists(conf_file),
            "File should not exist: {0}".format(conf_file))
        conf_file = os.path.join(backup_dir, 'backups', 'node', delta_backup_id2, 'database', 'postgresql.conf')
        print(conf_file)
        self.assertFalse(
            os.path.exists(conf_file),
            "File should not exist: {0}".format(conf_file))


    #########################################
    # --dry-run
    #########################################

    def test_dry_run_backup(self):
        """
        Test dry-run option for full backup
        """
        node = self.pg_node.make_simple('node',
                                        ptrack_enable=self.ptrack,
                                        # we need to write a lot. Lets speedup a bit.
                                        pg_options={"fsync": "off", "synchronous_commit": "off"})
        external_dir = self.get_tblspace_path(node, 'somedirectory')
        os.mkdir(external_dir)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Fill with data
        # Have to use scale=100 to create second segment.
        node.pgbench_init(scale=50, no_vacuum=True)

        backup_dir = self.backup_dir

        content_before = self.pgdata_content(os.path.join(backup_dir, 'backups', 'node'))

        # FULL archive
        backup_id = self.pb.backup_node('node', node, options=['--dry-run', '--note=test_note',
                                                               '--external-dirs={0}'.format(external_dir)])

        show_backups = self.pb.show('node')
        self.assertEqual(len(show_backups), 0)

        self.compare_pgdata(
            content_before,
            self.pgdata_content(os.path.join(backup_dir, 'backups', 'node'))
        )

        # FULL stream
        backup_id = self.pb.backup_node('node', node, options=['--stream', '--dry-run', '--note=test_note',
                                                               '--external-dirs={0}'.format(external_dir)])

        show_backups = self.pb.show('node')
        self.assertEqual(len(show_backups), 0)

        self.compare_pgdata(
            content_before,
            self.pgdata_content(os.path.join(backup_dir, 'backups', 'node'))
        )

        # do FULL
        backup_id = self.pb.backup_node('node', node,
                                        options=['--stream', '--external-dirs={0}'.format(external_dir),
                                                 '--note=test_note'])
        # Add some data changes to better testing
        pgbench = node.pgbench(options=['-T', '2', '--no-vacuum'])
        pgbench.wait()

        content_before = self.pgdata_content(os.path.join(backup_dir, 'backups', 'node'))

        # DELTA
        delta_backup_id = self.pb.backup_node('node', node, backup_type="delta",
                                              options=['--stream', '--external-dirs={0}'.format(external_dir),
                                                       '--note=test_note', '--dry-run'])
        # DELTA
        delta_backup_id = self.pb.backup_node('node', node, backup_type="delta",
                                              options=['--external-dirs={0}'.format(external_dir),
                                                       '--note=test_note', '--dry-run'])
        show_backups = self.pb.show('node')
        self.assertEqual(len(show_backups), 1)

        self.compare_pgdata(
            content_before,
            self.pgdata_content(os.path.join(backup_dir, 'backups', 'node'))
        )

        # do DELTA
        delta_backup_id = self.pb.backup_node('node', node, backup_type="delta",
                                              options=['--stream', '--external-dirs={0}'.format(external_dir),
                                                       '--note=test_note'])
        # Add some data changes
        pgbench = node.pgbench(options=['-T', '2', '--no-vacuum'])
        pgbench.wait()

        instance_before = self.pgdata_content(os.path.join(backup_dir, 'backups', 'node'))

        # PAGE
        page_backup_id = self.pb.backup_node('node', node, backup_type="page",
                                             options=['--external-dirs={0}'.format(external_dir),
                                                      '--note=test_note', '--dry-run'])
        show_backups = self.pb.show('node')
        self.assertEqual(len(show_backups), 2)

        self.compare_pgdata(
            instance_before,
            self.pgdata_content(os.path.join(backup_dir, 'backups', 'node'))
        )

        # do PAGE
        page_backup_id = self.pb.backup_node('node', node, backup_type="page",
                                             options=['--external-dirs={0}'.format(external_dir),
                                                      '--note=test_note'])
        instance_before = self.pgdata_content(os.path.join(backup_dir, 'backups', 'node'))

        # Add some data changes
        pgbench = node.pgbench(options=['-T', '2', '--no-vacuum'])
        pgbench.wait()

        if self.ptrack:
            node.safe_psql(
                "postgres",
                "create extension ptrack")

        if self.ptrack:
            backup_id = self.pb.backup_node('node', node, backup_type='ptrack',
                                            options=['--stream', '--external-dirs={0}'.format(external_dir),
                                                     '--note=test_note', '--dry-run'])
        if self.ptrack:
            backup_id = self.pb.backup_node('node', node, backup_type='ptrack',
                                            options=['--external-dirs={0}'.format(external_dir),
                                                     '--note=test_note', '--dry-run'])

        show_backups = self.pb.show('node')
        self.assertEqual(len(show_backups), 3)

        self.compare_pgdata(
            instance_before,
            self.pgdata_content(os.path.join(backup_dir, 'backups', 'node'))
        )

        # do PTRACK
        if self.ptrack:
            backup_id = self.pb.backup_node('node', node, backup_type='ptrack',
                                            options=['--stream', '--external-dirs={0}'.format(external_dir),
                                                     '--note=test_note'])

        out = self.pb.validate('node', backup_id)
        self.assertIn(
            "INFO: Backup {0} is valid".format(backup_id),
            out)
        # Cleanup
        node.stop()

    @unittest.skipIf(not fs_backup_class.is_file_based, "AccessPath check is always true on s3")
    def test_basic_dry_run_check_backup_with_access(self):
        """
        Access check suite if disk mounted as read_only
        """
        node = self.pg_node.make_simple('node',
                                        # we need to write a lot. Lets speedup a bit.
                                        pg_options={"fsync": "off", "synchronous_commit": "off"})
        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Fill with data
        # Have to use scale=100 to create second segment.
        node.pgbench_init(scale=20, no_vacuum=True)

        # FULL backup
        self.pb.backup_node('node', node, options=['--dry-run', '--stream', '--log-level-file=verbose'])

        check_permissions_dir = ['backups', 'wal']
        for dir in check_permissions_dir:
            # Access check suit if disk mounted as read_only
            dir_path = os.path.join(backup_dir, dir)
            dir_mode = os.stat(dir_path).st_mode
            os.chmod(dir_path, 0o400)
            print(backup_dir)

            try:
                error_message = self.pb.backup_node('node', node, backup_type='delta',
                                                options=['--stream', '--dry-run'],
                                                expect_error="because of changed permissions")

                self.assertMessage(error_message, contains='Permission denied')
            finally:
                # Cleanup
                os.chmod(dir_path, dir_mode)
            os.chmod(dir_path, 0o500)
            print(backup_dir)

            try:
                error_message = self.pb.backup_node('node', node, backup_type='delta',
                                                options=['--stream', '--dry-run'],
                                                expect_error="because of changed permissions")


                self.assertMessage(error_message, contains='ERROR: Check permissions')
            finally:
                # Cleanup
                os.chmod(dir_path, dir_mode)

        node.stop()
        node.cleanup()

    def string_in_file(self, file_path, str):
        with open(file_path, 'r') as file:
            # read all content of a file
            content = file.read()
            # check if string present in a file
            if str in content:
                return True
            else:
                return False

    def test_dry_run_restore_point_absence(self):
        node = self.pg_node.make_simple('node',
                                        # we need to write a lot. Lets speedup a bit.
                                        pg_options={"fsync": "off", "synchronous_commit": "off"})
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Fill with data
        # Have to use scale=100 to create second segment.
        node.pgbench_init(scale=100, no_vacuum=True)

        data_dir = node.data_dir

        backup_id = self.pb.backup_node('node', node, options=['--dry-run'])

        node.stop()

        restore_point = self.string_in_file(os.path.join(node.logs_dir, "postgresql.log"), "restore point")
        self.assertFalse(restore_point, "String should not exist: {0}".format("restore point"))

    @needs_gdb
    def test_dry_run_backup_kill_process(self):
        node = self.pg_node.make_simple('node',
                                        # we need to write a lot. Lets speedup a bit.
                                        pg_options={"fsync": "off", "synchronous_commit": "off"})
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Fill with data
        # Have to use scale=100 to create second segment.
        node.pgbench_init(scale=20, no_vacuum=True)

        backup_dir = self.backup_dir

        content_before = self.pgdata_content(os.path.join(backup_dir, 'backups', 'node'))
        # FULL backup
        gdb = self.pb.backup_node('node', node, options=['--dry-run', '--stream', '--log-level-file=verbose'],
                                  gdb=True)

        gdb.set_breakpoint('backup_files')
        gdb.run_until_break()
        gdb.signal('SIGTERM')
        gdb.continue_execution_until_error()

        self.compare_pgdata(
            content_before,
            self.pgdata_content(os.path.join(backup_dir, 'backups', 'node'))
        )

        gdb.kill()
        node.stop()

    def test_limit_rate_full_backup(self):
        """
        Test full backup with slow down to 8MBps speed
        """
        set_rate_limit = 8
        node = self.pg_node.make_simple('node',
            # we need to write a lot. Lets speedup a bit.
            pg_options={"fsync": "off", "synchronous_commit": "off"})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Fill with data
        node.pgbench_init(scale=5, no_vacuum=True)

        # FULL backup with rate limit
        backup_id = self.pb.backup_node("node", node, options=['--write-rate-limit='+str(set_rate_limit)])

        # Validate backup
        out = self.pb.validate('node', backup_id)
        self.assertIn(
            "INFO: Backup {0} is valid".format(backup_id),
            out)

        # Calculate time from start to end of backup
        show_backup = self.pb.show("node")
        backup_time = (datetime.strptime(show_backup[0]["end-time"]+"00", "%Y-%m-%d %H:%M:%S%z") -
                     datetime.strptime(show_backup[0]["start-time"]+"00", "%Y-%m-%d %H:%M:%S%z")
                     ).seconds

        # Calculate rate limit we've got in MBps and round it down
        get_rate_limit = int(show_backup[0]["data-bytes"] / (1024 * 1024 * backup_time))

        # Check that we are NOT faseter than expexted
        self.assertLessEqual(get_rate_limit, set_rate_limit)
