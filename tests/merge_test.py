# coding: utf-8

import unittest
import os

from .compression_test import have_alg
from .helpers.ptrack_helpers import ProbackupTest
from .helpers.ptrack_helpers import fs_backup_class
from .helpers.ptrack_helpers import base36enc, base36dec
from pg_probackup2.gdb import needs_gdb
from testgres import QueryException
import shutil
from datetime import datetime, timedelta
import time
import subprocess


class MergeTest(ProbackupTest):

    def test_basic_merge_full_2page(self):
        """
        1. Full backup -> fill data
        2. First Page backup -> fill data
        3. Second Page backup
        4. Merge 2 "Page" backups
        Restore and compare
        """
        # Initialize instance and backup directory
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance("node", node)
        self.pb.set_archiving("node", node)
        node.slow_start()

        # Do full backup
        self.pb.backup_node("node", node, options=['--compress'])
        show_backup = self.pb.show("node")[0]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "FULL")

        # Fill with data
        with node.connect() as conn:
            conn.execute("create table test (id int)")
            conn.execute(
                "insert into test select i from generate_series(1,10) s(i)")
            conn.commit()

        # Do first page backup
        self.pb.backup_node("node", node, backup_type="page", options=['--compress'])
        show_backup = self.pb.show("node")[1]

        # sanity check
        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "PAGE")

        # Fill with data
        with node.connect() as conn:
            conn.execute(
                "insert into test select i from generate_series(1,10) s(i)")
            count1 = conn.execute("select count(*) from test")
            conn.commit()

        # Do second page backup
        self.pb.backup_node("node", node,
            backup_type="page", options=['--compress'])
        show_backup = self.pb.show("node")[2]
        page_id = show_backup["id"]

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # sanity check
        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "PAGE")

        # Merge all backups
        output = self.pb.merge_backup("node", page_id,
                                      options=["-j", "4"])
        self.assertNotIn("WARNING", output)

        show_backups = self.pb.show("node")

        # sanity check
        self.assertEqual(len(show_backups), 1)
        self.assertEqual(show_backups[0]["status"], "OK")
        self.assertEqual(show_backups[0]["backup-mode"], "FULL")

        # Drop node and restore it
        node.cleanup()
        self.pb.restore_node('node', node=node)

        # Check physical correctness
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        node.slow_start()

        # Check restored node
        count2 = node.execute("postgres", "select count(*) from test")
        self.assertEqual(count1, count2)

    @unittest.skipIf(not (have_alg('lz4') and have_alg('zstd')),
                     "pg_probackup is not compiled with lz4 or zstd support")
    def test_merge_compressed_delta_page_ptrack(self):
        """
        1. Full compressed [zlib, 3] backup -> change data
        2. Delta compressed [pglz, 5] -> change data
        3. Page compressed [lz4, 9] -> change data
        4. Ptrack compressed [zstd, default]
        5. Merge all backups in one
        Restore and compare
        """
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

        # Initialize instance and backup directory
        node = self.pg_node.make_simple('node', set_replication=True, ptrack_enable=True)

        self.pb.init()
        self.pb.add_instance("node", node)
        self.pb.set_archiving("node", node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        # Fill with data
        node.pgbench_init(scale=10)

        # Do compressed FULL backup
        self.pb.backup_node("node", node, options=['--stream',
                                                            '--compress-level', '3',
                                                            '--compress-algorithm', 'zlib'])
        show_backup = self.pb.show("node")[0]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "FULL")

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do compressed DELTA backup
        self.pb.backup_node("node", node,
            backup_type="delta", options=['--stream',
                                          '--compress-level', '5',
                                          '--compress-algorithm', 'pglz'])

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do compressed PAGE backup
        self.pb.backup_node("node", node, backup_type="page", options=['--compress-level', '9',
                                                                   '--compress-algorithm', 'lz4'])

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do compressed PTRACK backup
        self.pb.backup_node("node", node, backup_type='ptrack', options=['--compress-algorithm', 'zstd'])

        pgdata = self.pgdata_content(node.data_dir)

        # Check backups
        show_backup = self.pb.show("node")
        self.assertEqual(show_backup[0]["status"], "OK")
        self.assertEqual(show_backup[0]["backup-mode"], "FULL")

        self.assertEqual(show_backup[1]["status"], "OK")
        self.assertEqual(show_backup[1]["backup-mode"], "DELTA")

        self.assertEqual(show_backup[2]["status"], "OK")
        self.assertEqual(show_backup[2]["backup-mode"], "PAGE")

        self.assertEqual(show_backup[3]["status"], "OK")
        self.assertEqual(show_backup[3]["backup-mode"], "PTRACK")

        ptrack_id = show_backup[3]["id"]

        # Merge all backups
        self.pb.merge_backup("node", ptrack_id, options=['-j2'])
        show_backups = self.pb.show("node")

        # Check number of backups and status
        self.assertEqual(len(show_backups), 1)
        self.assertEqual(show_backups[0]["status"], "OK")
        self.assertEqual(show_backups[0]["backup-mode"], "FULL")

        # Drop node and restore it
        node.cleanup()
        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        node.cleanup()

    def test_merge_uncompressed_ptrack_page_delta(self):
        """
        1. Full uncompressed backup -> change data
        2. uncompressed Ptrack -> change data
        3. uncompressed Page -> change data
        4. uncompressed Delta
        5. Merge all backups in one
        Restore and compare
        """
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

        backup_dir = self.backup_dir

        # Initialize instance and backup directory
        node = self.pg_node.make_simple('node', set_replication=True, ptrack_enable=True)

        self.pb.init()
        self.pb.add_instance("node", node)
        self.pb.set_archiving("node", node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        # Fill with data
        node.pgbench_init(scale=10)

        # Do uncompressed FULL backup
        self.pb.backup_node("node", node, options=['--stream'])
        show_backup = self.pb.show("node")[0]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "FULL")

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do uncompressed PTRACK backup
        self.pb.backup_node("node", node,
            backup_type="ptrack", options=['--stream'])

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do uncompressed PAGE backup
        self.pb.backup_node("node", node, backup_type="page")

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do uncompressed DELTA backup
        self.pb.backup_node("node", node, backup_type='delta')

        pgdata = self.pgdata_content(node.data_dir)

        # Check backups
        show_backup = self.pb.show("node")
        self.assertEqual(show_backup[0]["status"], "OK")
        self.assertEqual(show_backup[0]["backup-mode"], "FULL")

        self.assertEqual(show_backup[1]["status"], "OK")
        self.assertEqual(show_backup[1]["backup-mode"], "PTRACK")

        self.assertEqual(show_backup[2]["status"], "OK")
        self.assertEqual(show_backup[2]["backup-mode"], "PAGE")

        self.assertEqual(show_backup[3]["status"], "OK")
        self.assertEqual(show_backup[3]["backup-mode"], "DELTA")

        ptrack_id = show_backup[3]["id"]

        # Merge all backups
        self.pb.merge_backup("node", ptrack_id, options=['-j2'])
        show_backups = self.pb.show("node")

        # Check number of backups and status
        self.assertEqual(len(show_backups), 1)
        self.assertEqual(show_backups[0]["status"], "OK")
        self.assertEqual(show_backups[0]["backup-mode"], "FULL")

        # Drop node and restore it
        node.cleanup()
        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        node.cleanup()

    # @unittest.skip("skip")
    def test_merge_tablespaces(self):
        """
        Create tablespace with table, take FULL backup,
        create another tablespace with another table and drop previous
        tablespace, take page backup, merge it and restore

        """
        node = self.pg_node.make_simple('node', set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'somedata')
        node.safe_psql(
            "postgres",
            "create table t_heap tablespace somedata as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,100) i"
        )
        # FULL backup
        self.pb.backup_node('node', node)

        # Create new tablespace
        self.create_tblspace_in_node(node, 'somedata1')

        node.safe_psql(
            "postgres",
            "create table t_heap1 tablespace somedata1 as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,100) i"
        )

        node.safe_psql(
            "postgres",
            "drop table t_heap"
        )

        # Drop old tablespace
        node.safe_psql(
            "postgres",
            "drop tablespace somedata"
        )

        # PAGE backup
        backup_id = self.pb.backup_node('node', node, backup_type="page")

        pgdata = self.pgdata_content(node.data_dir)

        node.stop()
        shutil.rmtree(
            self.get_tblspace_path(node, 'somedata'),
            ignore_errors=True)
        shutil.rmtree(
            self.get_tblspace_path(node, 'somedata1'),
            ignore_errors=True)
        node.cleanup()

        self.pb.merge_backup('node', backup_id)

        self.pb.restore_node('node', node, options=["-j", "4"])

        pgdata_restored = self.pgdata_content(node.data_dir)

        # this compare should fall because we lost some directories
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_merge_tablespaces_1(self):
        """
        Create tablespace with table, take FULL backup,
        create another tablespace with another table, take page backup,
        drop first tablespace and take delta backup,
        merge it and restore
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node', set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'somedata')

        # FULL backup
        self.pb.backup_node('node', node)
        node.safe_psql(
            "postgres",
            "create table t_heap tablespace somedata as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,100) i"
        )

        # CREATE NEW TABLESPACE
        self.create_tblspace_in_node(node, 'somedata1')

        node.safe_psql(
            "postgres",
            "create table t_heap1 tablespace somedata1 as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,100) i"
        )

        # PAGE backup
        self.pb.backup_node('node', node, backup_type="page")

        node.safe_psql(
            "postgres",
            "drop table t_heap"
        )
        node.safe_psql(
            "postgres",
            "drop tablespace somedata"
        )

        # DELTA backup
        backup_id = self.pb.backup_node('node', node, backup_type="delta")

        pgdata = self.pgdata_content(node.data_dir)

        node.stop()
        shutil.rmtree(
            self.get_tblspace_path(node, 'somedata'),
            ignore_errors=True)
        shutil.rmtree(
            self.get_tblspace_path(node, 'somedata1'),
            ignore_errors=True)
        node.cleanup()

        self.pb.merge_backup('node', backup_id)

        self.pb.restore_node('node', node,
            options=["-j", "4"])

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    def test_merge_page_truncate(self):
        """
        make node, create table, take full backup,
        delete last 3 pages, vacuum relation,
        take page backup, merge full and page,
        restore last page backup and check data correctness
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '300s'})

        node_restored = self.pg_node.make_simple('node_restored')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node_restored.cleanup()
        node.slow_start()
        self.create_tblspace_in_node(node, 'somedata')

        node.safe_psql(
            "postgres",
            "create sequence t_seq; "
            "create table t_heap tablespace somedata as select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1024) i;")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.pb.backup_node('node', node)

        node.safe_psql(
            "postgres",
            "delete from t_heap where ctid >= '(11,0)'")
        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.pb.backup_node('node', node, backup_type='page')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        page_id = self.pb.show("node")[1]["id"]
        self.pb.merge_backup("node", page_id)

        self.pb.validate()

        old_tablespace = self.get_tblspace_path(node, 'somedata')
        new_tablespace = self.get_tblspace_path(node_restored, 'somedata_new')

        self.pb.restore_node('node', node_restored,
            options=[
                "-j", "4",
                "-T", "{0}={1}".format(old_tablespace, new_tablespace)])

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

        # Logical comparison
        result1 = node.table_checksum("t_heap")
        result2 = node_restored.table_checksum("t_heap")

        self.assertEqual(result1, result2)

    def test_merge_delta_truncate(self):
        """
        make node, create table, take full backup,
        delete last 3 pages, vacuum relation,
        take page backup, merge full and page,
        restore last page backup and check data correctness
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '300s'})

        node_restored = self.pg_node.make_simple('node_restored')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node_restored.cleanup()
        node.slow_start()
        self.create_tblspace_in_node(node, 'somedata')

        node.safe_psql(
            "postgres",
            "create sequence t_seq; "
            "create table t_heap tablespace somedata as select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1024) i;")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.pb.backup_node('node', node)

        node.safe_psql(
            "postgres",
            "delete from t_heap where ctid >= '(11,0)'")
        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.pb.backup_node('node', node, backup_type='delta')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        page_id = self.pb.show("node")[1]["id"]
        self.pb.merge_backup("node", page_id)

        self.pb.validate()

        old_tablespace = self.get_tblspace_path(node, 'somedata')
        new_tablespace = self.get_tblspace_path(node_restored, 'somedata_new')

        self.pb.restore_node('node', node_restored,
            options=[
                "-j", "4",
                "-T", "{0}={1}".format(old_tablespace, new_tablespace)])

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

        # Logical comparison
        result1 = node.table_checksum("t_heap")
        result2 = node_restored.table_checksum("t_heap")

        self.assertEqual(result1, result2)

    def test_merge_ptrack_truncate(self):
        """
        make node, create table, take full backup,
        delete last 3 pages, vacuum relation,
        take page backup, merge full and page,
        restore last page backup and check data correctness
        """
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        self.create_tblspace_in_node(node, 'somedata')

        node.safe_psql(
            "postgres",
            "create sequence t_seq; "
            "create table t_heap tablespace somedata as select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1024) i;")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.pb.backup_node('node', node)

        node.safe_psql(
            "postgres",
            "delete from t_heap where ctid >= '(11,0)'")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        ptrack_id = self.pb.backup_node('node', node, backup_type='ptrack')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.pb.merge_backup("node", ptrack_id)

        self.pb.validate()

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        old_tablespace = self.get_tblspace_path(node, 'somedata')
        new_tablespace = self.get_tblspace_path(node_restored, 'somedata_new')

        self.pb.restore_node('node', node_restored,
            options=[
                "-j", "4",
                "-T", "{0}={1}".format(old_tablespace, new_tablespace)])

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

        # Logical comparison
        result1 = node.table_checksum("t_heap")
        result2 = node_restored.table_checksum("t_heap")

        self.assertEqual(result1, result2)

    # @unittest.skip("skip")
    def test_merge_delta_delete(self):
        """
        Make node, create tablespace with table, take full backup,
        alter tablespace location, take delta backup, merge full and delta,
        restore database.
        """
        node = self.pg_node.make_simple('node', set_replication=True,
            pg_options={
                'checkpoint_timeout': '30s',
            }
        )

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'somedata')

        # FULL backup
        self.pb.backup_node('node', node, options=["--stream"])

        node.safe_psql(
            "postgres",
            "create table t_heap tablespace somedata as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,100) i"
        )

        node.safe_psql(
            "postgres",
            "delete from t_heap"
        )

        node.safe_psql(
            "postgres",
            "vacuum t_heap"
        )

        # DELTA BACKUP
        self.pb.backup_node('node', node,
            backup_type='delta',
            options=["--stream"]
        )

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        backup_id = self.pb.show("node")[1]["id"]
        self.pb.merge_backup("node", backup_id, options=["-j", "4"])

        # RESTORE
        node_restored = self.pg_node.make_simple('node_restored'
        )
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored,
            options=[
                "-j", "4",
                "-T", "{0}={1}".format(
                    self.get_tblspace_path(node, 'somedata'),
                    self.get_tblspace_path(node_restored, 'somedata')
                )
            ]
        )

        # GET RESTORED PGDATA AND COMPARE
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

    # @unittest.skip("skip")
    @needs_gdb
    def test_continue_failed_merge(self):
        """
        Check that failed MERGE can be continued
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL backup
        self.pb.backup_node('node', node)

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,1000) i"
        )

        # DELTA BACKUP
        self.pb.backup_node('node', node, backup_type='delta'
        )

        node.safe_psql(
            "postgres",
            "delete from t_heap"
        )

        node.safe_psql(
            "postgres",
            "vacuum t_heap"
        )

        # DELTA BACKUP
        self.pb.backup_node('node', node, backup_type='delta'
        )

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        backup_id = self.pb.show("node")[2]["id"]

        gdb = self.pb.merge_backup("node", backup_id, gdb=True)

        gdb.set_breakpoint('backup_non_data_file_internal')
        gdb.run_until_break()

        gdb.continue_execution_until_break(5)

        gdb.signal('SIGKILL')
        gdb.detach()
        time.sleep(1)

        self.expire_locks(backup_dir, 'node')

        print(self.pb.show(as_text=True, as_json=False))

        # Try to continue failed MERGE
        self.pb.merge_backup("node", backup_id)

        # Drop node and restore it
        node.cleanup()
        self.pb.restore_node('node', node=node)

    # @unittest.skip("skip")
    @needs_gdb
    def test_continue_failed_merge_with_corrupted_delta_backup(self):
        """
        Fail merge via gdb, corrupt DELTA backup, try to continue merge
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL backup
        self.pb.backup_node('node', node)

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,1000) i")

        old_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()

        # DELTA BACKUP
        self.pb.backup_node('node', node, backup_type='delta')

        node.safe_psql(
            "postgres",
            "update t_heap set id = 100500")

        node.safe_psql(
            "postgres",
            "vacuum full t_heap")

        new_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()

        # DELTA BACKUP
        backup_id_2 = self.pb.backup_node('node', node, backup_type='delta')

        backup_id = self.pb.show("node")[1]["id"]

        # Failed MERGE
        gdb = self.pb.merge_backup("node", backup_id, gdb=True)
        gdb.set_breakpoint('backup_non_data_file_internal')
        gdb.run_until_break()

        gdb.continue_execution_until_break(2)

        gdb.signal('SIGKILL')

        self.expire_locks(backup_dir, 'node')

        # CORRUPT incremental backup
        # read block from future
        # block_size + backup_header = 8200
        file_content2 = self.read_backup_file(backup_dir, 'node', backup_id_2,
                                             f'database/{new_path}')[:16400]
        # write block from future
        self.corrupt_backup_file(backup_dir, 'node', backup_id,
                                 f'database/{old_path}',
                                 damage=(8200, file_content2[8200:16400]))

        # Try to continue failed MERGE
        self.pb.merge_backup("node", backup_id,
                          expect_error="because of incremental backup corruption")
        self.assertMessage(contains=f"ERROR: Backup {backup_id} has status CORRUPT, merge is aborted")

    @needs_gdb
    def test_continue_failed_merge_2(self):
        """
        Check that failed MERGE on delete can be continued
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL backup
        self.pb.backup_node('node', node)

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,1000) i")

        # DELTA BACKUP
        self.pb.backup_node('node', node, backup_type='delta')

        node.safe_psql(
            "postgres",
            "delete from t_heap")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        # DELTA BACKUP
        self.pb.backup_node('node', node, backup_type='delta')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        backup_id = self.pb.show("node")[2]["id"]

        gdb = self.pb.merge_backup("node", backup_id, gdb=True)

        gdb.set_breakpoint('lock_backup')

        gdb.run_until_break()

        gdb._execute('thread apply all bt')
        gdb.remove_all_breakpoints()

        gdb.set_breakpoint('pioRemoveDir__do')

        gdb.continue_execution_until_break(20)

        gdb._execute('thread apply all bt')

        gdb.signal('SIGKILL')

        self.expire_locks(backup_dir, 'node')

        print(self.pb.show(as_text=True, as_json=False))

        backup_id_deleted = self.pb.show("node")[1]["id"]

        # TODO check that full backup has meta info is equal to DELETTING

        # Try to continue failed MERGE
        self.pb.merge_backup("node", backup_id)

    @needs_gdb
    def test_continue_failed_merge_3(self):
        """
        Check that failed MERGE cannot be continued if intermediate
        backup is missing.
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Create test data
        node.safe_psql("postgres", "create sequence t_seq")
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, nextval('t_seq')"
            " as t_seq, md5(i::text) as text, md5(i::text)::tsvector"
            " as tsvector from generate_series(0,100000) i"
        )

        # FULL backup
        self.pb.backup_node('node', node)

        # CREATE FEW PAGE BACKUP
        i = 0

        while i < 2:

            node.safe_psql(
                "postgres",
                "delete from t_heap"
            )

            node.safe_psql(
                "postgres",
                "vacuum t_heap"
            )
            node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, nextval('t_seq') as t_seq,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(100,200000) i"
            )

            # PAGE BACKUP
            self.pb.backup_node('node', node, backup_type='page'
            )
            i = i + 1

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        backup_id_merge = self.pb.show("node")[2]["id"]
        backup_id_delete = self.pb.show("node")[1]["id"]

        print(self.pb.show(as_text=True, as_json=False))

        gdb = self.pb.merge_backup("node", backup_id_merge, gdb=True)

        gdb.set_breakpoint('backup_non_data_file_internal')
        gdb.run_until_break()
        gdb.continue_execution_until_break(2)

        gdb.signal('SIGKILL')

        self.expire_locks(backup_dir, 'node')

        print(self.pb.show(as_text=True, as_json=False))
        # print(os.path.join(backup_dir, "backups", "node", backup_id_delete))

        # DELETE PAGE1
        self.remove_one_backup(backup_dir, 'node', backup_id_delete)

        # Try to continue failed MERGE
        self.pb.merge_backup("node", backup_id_merge,
                          expect_error="because of backup corruption")
        self.assertMessage(contains="ERROR: Incremental chain is broken, "
                                    "merge is impossible to finish")

    def test_merge_different_compression_algo(self):
        """
        Check that backups with different compression algorithms can be merged
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL backup
        self.pb.backup_node('node', node, options=['--compress-algorithm=zlib'])

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,1000) i")

        # DELTA BACKUP
        self.pb.backup_node('node', node,
            backup_type='delta', options=['--compress-algorithm=pglz'])

        node.safe_psql(
            "postgres",
            "delete from t_heap")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        # DELTA BACKUP
        self.pb.backup_node('node', node, backup_type='delta')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        backup_id = self.pb.show("node")[2]["id"]

        self.pb.merge_backup("node", backup_id)

    def test_merge_different_wal_modes(self):
        """
        Check that backups with different wal modes can be merged
        correctly
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL stream backup
        self.pb.backup_node('node', node, options=['--stream'])

        # DELTA archive backup
        backup_id = self.pb.backup_node('node', node, backup_type='delta')

        self.pb.merge_backup('node', backup_id=backup_id)

        self.assertEqual(
            'ARCHIVE', self.pb.show('node', backup_id)['wal'])

        # DELTA stream backup
        backup_id = self.pb.backup_node('node', node,
            backup_type='delta', options=['--stream'])

        self.pb.merge_backup('node', backup_id=backup_id)

        self.assertEqual(
            'STREAM', self.pb.show('node', backup_id)['wal'])

    def test_merge_A_B_C_removes_internal_B(self):
        """
        check that A->B->C merge removes B merge stub dir.
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
                                     set_replication=True,
                                     initdb_params=['--data-checksums'])

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL
        full_id = self.pb.backup_node('node', node, options=['--stream'])

        # DELTA 1
        delta_1_id = self.pb.backup_node('node', node, backup_type='delta')

        # DELTA 2
        delta_2_id = self.pb.backup_node('node', node, backup_type='delta')

        self.pb.merge_backup('node', backup_id=delta_1_id)

        self.assertEqual(
            'ARCHIVE', self.pb.show('node', delta_1_id)['wal'])

        self.pb.merge_backup('node', backup_id=delta_2_id)

        backups_dirs_list = self.get_backups_dirs(backup_dir, "node")

        self.assertIn(full_id, backups_dirs_list)
        self.assertNotIn(delta_1_id, backups_dirs_list)
        self.assertIn(delta_2_id, backups_dirs_list)

    @needs_gdb
    def test_merge_A_B_C_broken_on_B_removal(self):
        """
        check that A->B->C merge removes B merge stub dir
        on second merge try after first merge is killed on B removal.
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
                                     set_replication=True,
                                     initdb_params=['--data-checksums'])

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL
        full_id = self.pb.backup_node('node', node, options=['--stream'])

        # DELTA 1
        delta_1_id = self.pb.backup_node('node', node, backup_type='delta')

        # DELTA 2
        delta_2_id = self.pb.backup_node('node', node, backup_type='delta')

        self.pb.merge_backup('node', backup_id=delta_1_id)

        self.assertEqual('ARCHIVE', self.pb.show('node', delta_1_id)['wal'])

        gdb = self.pb.merge_backup('node', backup_id=delta_2_id, gdb=True)

        gdb.set_breakpoint('renameBackupToDir')
        gdb.run_until_break()

        gdb.set_breakpoint("pgBackupInit")
        # breaks after removing interim B dir, before recreating "C" dir as merged_to
        gdb.continue_execution_until_break()
        # killing merge on in-critical-section broken inside BACKUP_STATUS_MERGES critical section
        gdb.kill()

        self.expire_locks(backup_dir, 'node')

        # rerun merge to C, it should merge fine
        self.pb.merge_backup('node', backup_id=delta_2_id)#, gdb=("suspend", 30303))
        backups_dirs_list = self.get_backups_dirs(backup_dir, "node")

        self.assertIn(full_id, backups_dirs_list)
        self.assertNotIn(delta_1_id, backups_dirs_list)
        self.assertIn(delta_2_id, backups_dirs_list)

    def test_merge_A_B_and_remove_A_removes_B(self):
        """
        Check, that after A->B merge and remove of B removes both A and B dirs.
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
                                        set_replication=True,
                                        initdb_params=['--data-checksums'])

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # PRE FULL
        pre_full_id = self.pb.backup_node('node', node, options=['--stream'])

        # FULL
        full_id = self.pb.backup_node('node', node, options=['--stream'])

        # DELTA 1
        delta_1_id = self.pb.backup_node('node', node, backup_type='delta')

        # DELTA 2
        delta_2_id = self.pb.backup_node('node', node, backup_type='delta')

        # POST FULL
        post_full_id = self.pb.backup_node('node', node, options=['--stream'])

        self.pb.merge_backup('node', backup_id=delta_1_id)

        self.assertEqual(
            'ARCHIVE', self.pb.show('node', delta_1_id)['wal'])

        self.pb.delete('node', backup_id=delta_1_id)

        backups_dirs_list = self.get_backups_dirs(backup_dir, "node")

        # these should be deleted obviously
        self.assertNotIn(full_id, backups_dirs_list)
        self.assertNotIn(delta_2_id, backups_dirs_list)
        # these should not be deleted obviously
        self.assertIn(pre_full_id, backups_dirs_list)
        self.assertIn(post_full_id, backups_dirs_list)
        # and actual check for PBCKP-710: deleted symlink directory
        self.assertNotIn(delta_1_id, backups_dirs_list)


    def test_validate_deleted_dirs_merged_from(self):
        """
        checks validate fails if we miss full dir
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
                                        set_replication=True,
                                        initdb_params=['--data-checksums'])

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL 1
        full_1 = self.pb.backup_node('node', node, options=['--stream'])

        # DELTA 1-1
        delta_1_1 = self.pb.backup_node('node', node, backup_type='delta')

        #FULL 2
        full_2 = self.pb.backup_node('node', node, options=['--stream'])

        # DELTA 2-1
        delta_2_1 = self.pb.backup_node('node', node, backup_type='delta')

        # validate is ok
        self.pb.merge_backup('node', backup_id=delta_2_1)
        self.pb.validate('node')

        # changing DELTA_2_1 backup.control symlink fields
        # validate should find problems
        with self.modify_backup_control(backup_dir, "node", delta_2_1) as cf:
            cf.data += "\nsymlink = " + base36enc(base36dec(delta_2_1)+1)
        self.pb.validate('node', expect_error=True)
        self.assertMessage(contains="no linked backup")

        with self.modify_backup_control(backup_dir, "node", delta_2_1) as cf:
            cf.data = "\n".join(cf.data.splitlines()[:-1])

        # validate should find previous backup is not FULL
        self.remove_one_backup(backup_dir, 'node', full_2)
        self.pb.validate('node', expect_error=True)
        self.assertMessage(contains="no linked backup")

        # validate should find there's previous FULL backup has bad id or cross references
        self.remove_one_backup(backup_dir, 'node', delta_1_1)
        with self.modify_backup_control(backup_dir, "node", delta_2_1) as cf:
            cf.data += f"\nsymlink = {full_1}"
        self.pb.validate('node', expect_error=True)
        self.assertMessage(contains="has different 'backup-id'")

        # validate should find there's previous FULL backup has bad id or cross references
        self.remove_one_backup(backup_dir, 'node', full_1)
        self.pb.validate('node', expect_error=True)
        self.assertMessage(contains="no linked backup")

    @needs_gdb
    def test_crash_after_opening_backup_control_1(self):
        """
        check that crashing after opening backup.control
        for writing will not result in losing backup metadata
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL stream backup
        self.pb.backup_node('node', node, options=['--stream'])

        # DELTA archive backup
        backup_id = self.pb.backup_node('node', node, backup_type='delta')

        print(self.pb.show('node', as_json=False, as_text=True))

        gdb = self.pb.merge_backup("node", backup_id, gdb=True)
        gdb.set_breakpoint('write_backup_filelist')
        gdb.run_until_break()

        gdb.set_breakpoint('write_backup')
        gdb.continue_execution_until_break()
        gdb.set_breakpoint('pgBackupWriteControl')
        gdb.continue_execution_until_break()

        gdb.signal('SIGKILL')

        self.expire_locks(backup_dir, 'node')

        print(self.pb.show('node', as_json=False, as_text=True))

        self.assertEqual(
            'MERGING', self.pb.show('node')[0]['status'])

        self.assertEqual(
            'MERGING', self.pb.show('node')[1]['status'])

    # @unittest.skip("skip")
    @needs_gdb
    def test_crash_after_opening_backup_control_2(self):
        """
        check that crashing after opening backup_content.control
        for writing will not result in losing metadata about backup files
        TODO: rewrite
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Add data
        node.pgbench_init(scale=3)

        # FULL backup
        full_id = self.pb.backup_node('node', node, options=['--stream'])

        # Change data
        pgbench = node.pgbench(options=['-T', '20', '-c', '2'])
        pgbench.wait()

        path = node.safe_psql(
            'postgres',
            "select pg_relation_filepath('pgbench_accounts')").decode('utf-8').rstrip()

        fsm_path = path + '_fsm'

        node.safe_psql(
            'postgres',
            'vacuum pgbench_accounts')

        # DELTA backup
        backup_id = self.pb.backup_node('node', node, backup_type='delta')

        pgdata = self.pgdata_content(node.data_dir)

        print(self.pb.show('node', as_json=False, as_text=True))

        gdb = self.pb.merge_backup("node", backup_id, gdb=True)
        gdb.set_breakpoint('write_backup_filelist')
        gdb.run_until_break()

#        gdb.set_breakpoint('sprintf')
#        gdb.continue_execution_until_break(1)

        gdb.signal('SIGKILL')

        self.expire_locks(backup_dir, 'node')

        print(self.pb.show('node', as_json=False, as_text=True))

        self.assertEqual(
            'MERGING', self.pb.show('node')[0]['status'])

        self.assertEqual(
            'MERGING', self.pb.show('node')[1]['status'])

        # In to_backup drop file that comes from from_backup
        # emulate crash during previous merge
        self.remove_backup_file(backup_dir, 'node', full_id,
                                f'database/{fsm_path}')

        # Continue failed merge
        self.pb.merge_backup("node", backup_id)

        node.cleanup()

        # restore merge backup
        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(node.data_dir)

        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    @needs_gdb
    def test_losing_file_after_failed_merge(self):
        """
        check that crashing after opening backup_content.control
        for writing will not result in losing metadata about backup files
        TODO: rewrite
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Add data
        node.pgbench_init(scale=1)

        # FULL backup
        full_id = self.pb.backup_node('node', node, options=['--stream'])

        # Change data
        node.safe_psql(
            'postgres',
            "update pgbench_accounts set aid = aid + 1005000")

        path = node.safe_psql(
            'postgres',
            "select pg_relation_filepath('pgbench_accounts')").decode('utf-8').rstrip()

        node.safe_psql(
            'postgres',
            "VACUUM pgbench_accounts")

        vm_path = path + '_vm'

        # DELTA backup
        backup_id = self.pb.backup_node('node', node, backup_type='delta')

        pgdata = self.pgdata_content(node.data_dir)

        print(self.pb.show('node', as_json=False, as_text=True))

        gdb = self.pb.merge_backup("node", backup_id, gdb=True)
        gdb.set_breakpoint('write_backup_filelist')
        gdb.run_until_break()

#        gdb.set_breakpoint('sprintf')
#        gdb.continue_execution_until_break(20)

        gdb.signal('SIGKILL')

        self.expire_locks(backup_dir, 'node')

        print(self.pb.show('node', as_json=False, as_text=True))

        self.assertEqual(
            'MERGING', self.pb.show('node')[0]['status'])

        self.assertEqual(
            'MERGING', self.pb.show('node')[1]['status'])

        # In to_backup drop file that comes from from_backup
        # emulate crash during previous merge
        self.remove_backup_file(backup_dir, 'node', full_id,
                                f'database/{vm_path}')

        # Try to continue failed MERGE
        self.pb.merge_backup("node", backup_id)

        self.assertEqual(
            'OK', self.pb.show('node')[0]['status'])

        node.cleanup()

        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    @needs_gdb
    def test_failed_merge_after_delete(self):
        """
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # add database
        node.safe_psql(
            'postgres',
            'CREATE DATABASE testdb')

        dboid = node.safe_psql(
            "postgres",
            "select oid from pg_database where datname = 'testdb'").decode('utf-8').rstrip()

        # take FULL backup
        full_id = self.pb.backup_node('node', node, options=['--stream'])

        # drop database
        node.safe_psql(
            'postgres',
            'DROP DATABASE testdb')

        # take PAGE backup
        page_id = self.pb.backup_node('node', node, backup_type='page')

        page_id_2 = self.pb.backup_node('node', node, backup_type='page')

        gdb = self.pb.merge_backup('node', page_id,
            gdb=True, options=['--log-level-console=verbose'])

        gdb.set_breakpoint('delete_backup_files')
        gdb.run_until_break()

        gdb.set_breakpoint('lock_backup')
        gdb.continue_execution_until_break()

        gdb.signal('SIGKILL')

        self.expire_locks(backup_dir, 'node')

        # backup half-merged
        self.assertEqual(
            'MERGED', self.pb.show('node')[0]['status'])

        self.assertEqual(
            full_id, self.pb.show('node')[0]['id'])

        self.pb.merge_backup('node', page_id_2,
                          options=['--log-level-console=verbose'],
                          expect_error="because of missing parent")
        self.assertMessage(contains=f"ERROR: Full backup {full_id} has "
                                    f"unfinished merge with backup {page_id}")

    @needs_gdb
    def test_failed_merge_after_delete_1(self):
        """
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # take FULL backup
        full_id = self.pb.backup_node('node', node, options=['--stream'])

        node.pgbench_init(scale=1)

        page_1 = self.pb.backup_node('node', node, backup_type='page')

        # Change PAGE1 backup status to ERROR
        self.change_backup_status(backup_dir, 'node', page_1, 'ERROR')

        pgdata = self.pgdata_content(node.data_dir)

        # add data
        pgbench = node.pgbench(options=['-T', '10', '-c', '2', '--no-vacuum'])
        pgbench.wait()

        # take PAGE2 backup
        page_id = self.pb.backup_node('node', node, backup_type='page')

        # Change PAGE1 backup status to OK
        self.change_backup_status(backup_dir, 'node', page_1, 'OK')

        gdb = self.pb.merge_backup('node', page_id,
            gdb=True, options=['--log-level-console=verbose'])

        gdb.set_breakpoint('delete_backup_files')
        gdb.run_until_break()

#        gdb.set_breakpoint('parray_bsearch')
#        gdb.continue_execution_until_break()

        gdb.set_breakpoint('lock_backup')
        gdb.continue_execution_until_break(30)
        gdb.signal('SIGKILL')

        self.expire_locks(backup_dir, 'node')

        self.assertEqual(
            full_id, self.pb.show('node')[0]['id'])

        # restore
        node.cleanup()
        self.pb.restore_node('node', node=node,
                          expect_error="because of orphan status")
        self.assertMessage(contains=f"ERROR: Backup {page_1} is orphan")

    @needs_gdb
    def test_failed_merge_after_delete_2(self):
        """
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # take FULL backup
        full_id = self.pb.backup_node('node', node, options=['--stream'])

        node.pgbench_init(scale=1)

        page_1 = self.pb.backup_node('node', node, backup_type='page')

        # add data
        pgbench = node.pgbench(options=['-T', '10', '-c', '2', '--no-vacuum'])
        pgbench.wait()

        # take PAGE2 backup
        page_2 = self.pb.backup_node('node', node, backup_type='page')

        gdb = self.pb.merge_backup('node', page_2, gdb=True,
            options=['--log-level-console=VERBOSE'])

        gdb.set_breakpoint('delete_backup_files')
        gdb.run_until_break()
        gdb.set_breakpoint('lock_backup')
        gdb.continue_execution_until_break(2)
        gdb.signal('SIGKILL')

        self.expire_locks(backup_dir, 'node')

        self.pb.delete('node', backup_id=page_2)

        # rerun merge
        self.pb.merge_backup('node', page_1,
                          expect_error="because backup is missing")
        self.assertMessage(contains=f"ERROR: Full backup {full_id} has unfinished merge "
                                    f"with backup {page_2}")

    @needs_gdb
    def test_failed_merge_after_delete_3(self):
        """
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # add database
        node.safe_psql(
            'postgres',
            'CREATE DATABASE testdb')

        dboid = node.safe_psql(
            "postgres",
            "select oid from pg_database where datname = 'testdb'").rstrip()

        # take FULL backup
        full_id = self.pb.backup_node('node', node, options=['--stream'])

        # drop database
        node.safe_psql(
            'postgres',
            'DROP DATABASE testdb')

        # take PAGE backup
        page_id = self.pb.backup_node('node', node, backup_type='page')

        # create database
        node.safe_psql(
            'postgres',
            'create DATABASE testdb')

        page_id_2 = self.pb.backup_node('node', node, backup_type='page')

        gdb = self.pb.merge_backup('node', page_id,
            gdb=True, options=['--log-level-console=verbose'])

        gdb.set_breakpoint('delete_backup_files')
        gdb.run_until_break()

        gdb.set_breakpoint('lock_backup')
        gdb.continue_execution_until_break()

        gdb.signal('SIGKILL')

        self.expire_locks(backup_dir, 'node')

        # backup half-merged
        self.assertEqual(
            'MERGED', self.pb.show('node')[0]['status'])

        self.assertEqual(
            full_id, self.pb.show('node')[0]['id'])

        # FULL backup is missing now
        self.remove_one_backup(backup_dir, 'node', full_id)

        self.pb.merge_backup('node', page_id_2,
                          options=['--log-level-console=verbose'],
                          expect_error="because of missing parent")
        self.assertMessage(contains=f"ERROR: Failed to find parent full backup for {page_id_2}")

    # Skipped, because backups from the future are invalid.
    # This cause a "ERROR: Can't assign backup_id, there is already a backup in future"
    # now (PBCKP-259). We can conduct such a test again when we
    # untie 'backup_id' from 'start_time'
    @unittest.skip("skip")
    def test_merge_backup_from_future(self):
        """
        take FULL backup, table PAGE backup from future,
        try to merge page with FULL
        """
        backup_dir = self.backup_dir
        if not backup_dir.is_file_based:
            self.skipTest("test uses rename which is hard for cloud")

        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Take FULL
        self.pb.backup_node('node', node)

        node.pgbench_init(scale=5)

        # Take PAGE from future
        backup_id = self.pb.backup_node('node', node, backup_type='page')

        with self.modify_backup_control(backup_dir, 'node', backup_id) as cf:
            cf.data += "\nstart-time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                        datetime.now() + timedelta(days=3))

        # rename directory
        new_id = self.pb.show('node')[1]['id']

        os.rename(
            os.path.join(backup_dir, 'backups', 'node', backup_id),
            os.path.join(backup_dir, 'backups', 'node', new_id))

        pgbench = node.pgbench(options=['-T', '5', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        backup_id = self.pb.backup_node('node', node, backup_type='page')
        pgdata = self.pgdata_content(node.data_dir)

        result = node.table_checksum("pgbench_accounts")

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node',
            node_restored, backup_id=backup_id)

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # check that merged backup has the same state as
        node_restored.cleanup()
        self.pb.merge_backup('node', backup_id=backup_id)
        self.pb.restore_node('node',
            node_restored, backup_id=backup_id)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)

        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

        result_new = node_restored.table_checksum("pgbench_accounts")

        self.assertEqual(result, result_new)

        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_merge_multiple_descendants(self):
        r"""
        PAGEb3
          |                 PAGEa3
        PAGEb2               /
          |       PAGEa2    /
        PAGEb1       \     /
          |           PAGEa1
        FULLb           |
                      FULLa
        """
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Take FULL BACKUPs
        backup_id_a = self.pb.backup_node('node', node)

        backup_id_b = self.pb.backup_node('node', node)

        # Change FULLb backup status to ERROR
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        page_id_a1 = self.pb.backup_node('node', node, backup_type='page')

        # Change FULLb backup status to OK
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        # Change PAGEa1 backup status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'ERROR')

        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  OK

        page_id_b1 = self.pb.backup_node('node', node, backup_type='page')

        # PAGEb1 OK
        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  OK

        # Change PAGEa1 to OK
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'OK')

        # Change PAGEb1 and FULLb to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        page_id_a2 = self.pb.backup_node('node', node, backup_type='page')

        # PAGEa2 OK
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        # Change PAGEb1 and FULLb to OK
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        # Change PAGEa2 and FULL to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'ERROR')

        # PAGEa2 ERROR
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  ERROR

        page_id_b2 = self.pb.backup_node('node', node, backup_type='page')

        # PAGEb2 OK
        # PAGEa2 ERROR
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  ERROR

        # Change PAGEb2, PAGEb1 and FULLb to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_b2, 'ERROR')
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'ERROR')
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        # Change FULLa to OK
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'OK')

        # PAGEb2 ERROR
        # PAGEa2 ERROR
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        page_id_a3 = self.pb.backup_node('node', node, backup_type='page')

        # PAGEa3 OK
        # PAGEb2 ERROR
        # PAGEa2 ERROR
        # PAGEb1 ERROR
        # PAGEa1 OK
        # FULLb  ERROR
        # FULLa  OK

        # Change PAGEa3 and FULLa to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a3, 'ERROR')

        # Change PAGEb2, PAGEb1 and FULLb to OK
        self.change_backup_status(backup_dir, 'node', page_id_b2, 'OK')
        self.change_backup_status(backup_dir, 'node', page_id_b1, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'OK')

        page_id_b3 = self.pb.backup_node('node', node, backup_type='page')

        # PAGEb3 OK
        # PAGEa3 ERROR
        # PAGEb2 OK
        # PAGEa2 ERROR
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  ERROR

        # Change PAGEa3, PAGEa2 and FULLa status to OK
        self.change_backup_status(backup_dir, 'node', page_id_a3, 'OK')
        self.change_backup_status(backup_dir, 'node', page_id_a2, 'OK')
        self.change_backup_status(backup_dir, 'node', backup_id_a, 'OK')

        # PAGEb3 OK
        # PAGEa3 OK
        # PAGEb2 OK
        # PAGEa2 OK
        # PAGEb1 OK
        # PAGEa1 OK
        # FULLb  OK
        # FULLa  OK

        # Check that page_id_a3 and page_id_a2 are both direct descendants of page_id_a1
        self.assertEqual(
            self.pb.show('node', backup_id=page_id_a3)['parent-backup-id'],
            page_id_a1)

        self.assertEqual(
            self.pb.show('node', backup_id=page_id_a2)['parent-backup-id'],
            page_id_a1)

        self.pb.merge_backup('node', page_id_a2,
            options=['--merge-expired', '--log-level-console=log'])

        self.pb.merge_backup('node', page_id_a3,
                          options=['--merge-expired', '--log-level-console=log'],
                          expect_error="parent FULL backup is missing")
        self.assertMessage(contains=f"ERROR: Failed to find parent full backup for {page_id_a3}")

    # @unittest.skip("skip")
    def test_smart_merge(self):
        """
        make node, create database, take full backup, drop database,
        take PAGE backup and merge it into FULL,
        make sure that files from dropped database are not
        copied during restore
        https://github.com/postgrespro/pg_probackup/issues/63
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # create database
        node.safe_psql(
            "postgres",
            "CREATE DATABASE testdb")

        # take FULL backup
        full_id = self.pb.backup_node('node', node)

        # drop database
        node.safe_psql(
            "postgres",
            "DROP DATABASE testdb")

        # take PAGE backup
        page_id = self.pb.backup_node('node', node, backup_type='page')

        # get delta between FULL and PAGE filelists
        filelist_full = self.get_backup_filelist(backup_dir, 'node', full_id)

        # merge PAGE backup
        self.pb.merge_backup('node', page_id,
            options=['--log-level-file=VERBOSE'])

        filelist_full_after_merge = self.get_backup_filelist(backup_dir, 'node', full_id)

        filelist_diff = self.get_backup_filelist_diff(
            filelist_full, filelist_full_after_merge)

        logfile_content = self.read_pb_log()

        self.assertTrue(filelist_diff, "There should be deleted files")
        for file in filelist_diff:
            self.assertIn(file, logfile_content)


    @needs_gdb
    def test_idempotent_merge(self):
        """
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # add database
        node.safe_psql(
            'postgres',
            'CREATE DATABASE testdb')

        # take FULL backup
        full_id = self.pb.backup_node('node', node, options=['--stream'])

        # create database
        node.safe_psql(
            'postgres',
            'create DATABASE testdb1')

        # take PAGE backup
        page_id = self.pb.backup_node('node', node, backup_type='delta')

        # create database
        node.safe_psql(
            'postgres',
            'create DATABASE testdb2')

        page_id_2 = self.pb.backup_node('node', node, backup_type='delta')

        gdb = self.pb.merge_backup('node', page_id_2,
            gdb=True, options=['--log-level-console=log'])

        gdb.set_breakpoint('delete_backup_files')
        gdb.run_until_break()
        gdb.remove_all_breakpoints()

        gdb.set_breakpoint("renameBackupToDir")
        gdb.continue_execution_until_break()
        gdb.set_breakpoint("write_backup")
        gdb.continue_execution_until_break()
        gdb.set_breakpoint("pgBackupFree")
        gdb.continue_execution_until_break()


        gdb.signal('SIGKILL')

        self.expire_locks(backup_dir, 'node')

        show_backups = self.pb.show("node")
        self.assertEqual(len(show_backups), 1)

        self.assertEqual(
            'MERGED', self.pb.show('node')[0]['status'])

        self.assertEqual(
            full_id, self.pb.show('node')[0]['id'])

        self.pb.merge_backup('node', page_id_2)

        self.assertEqual(
            'OK', self.pb.show('node')[0]['status'])

        self.assertEqual(
            page_id_2, self.pb.show('node')[0]['id'])

    def test_merge_correct_inheritance(self):
        """
        Make sure that backup metainformation fields
        'note' and 'expire-time' are correctly inherited
        during merge
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # add database
        node.safe_psql(
            'postgres',
            'CREATE DATABASE testdb')

        # take FULL backup
        self.pb.backup_node('node', node, options=['--stream'])

        # create database
        node.safe_psql(
            'postgres',
            'create DATABASE testdb1')

        # take PAGE backup
        page_id = self.pb.backup_node('node', node, backup_type='page')

        self.pb.set_backup('node', page_id, options=['--note=hello', '--ttl=20d'])

        page_meta = self.pb.show('node', page_id)

        self.pb.merge_backup('node', page_id)

        print(self.pb.show('node', page_id))

        self.assertEqual(
            page_meta['note'],
            self.pb.show('node', page_id)['note'])

        self.assertEqual(
            page_meta['expire-time'],
            self.pb.show('node', page_id)['expire-time'])

    def test_merge_correct_inheritance_1(self):
        """
        Make sure that backup metainformation fields
        'note' and 'expire-time' are correctly inherited
        during merge
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # add database
        node.safe_psql(
            'postgres',
            'CREATE DATABASE testdb')

        # take FULL backup
        self.pb.backup_node('node', node,
            options=['--stream', '--note=hello', '--ttl=20d'])

        # create database
        node.safe_psql(
            'postgres',
            'create DATABASE testdb1')

        # take PAGE backup
        page_id = self.pb.backup_node('node', node, backup_type='page')

        self.pb.merge_backup('node', page_id)

        self.assertNotIn(
            'note',
            self.pb.show('node', page_id))

        self.assertNotIn(
            'expire-time',
            self.pb.show('node', page_id))

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_multi_timeline_merge(self):
        """
        Check that backup in PAGE mode choose
        parent backup correctly:
        t12        /---P-->
        ...
        t3      /---->
        t2   /---->
        t1 -F-----D->

        P must have F as parent
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql("postgres", "create extension pageinspect")

        try:
            node.safe_psql(
                "postgres",
                "create extension amcheck")
        except QueryException as e:
            node.safe_psql(
                "postgres",
                "create extension amcheck_next")

        node.pgbench_init(scale=20)
        full_id = self.pb.backup_node('node', node)

        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type='delta')

        node.cleanup()
        self.pb.restore_node('node', node, backup_id=full_id,
            options=[
                '--recovery-target=immediate',
                '--recovery-target-action=promote'])

        node.slow_start()

        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # create timelines
        for i in range(2, 7):
            node.cleanup()
            self.pb.restore_node('node', node,
                options=[
                    '--recovery-target=latest',
                    '--recovery-target-action=promote',
                    '--recovery-target-timeline={0}'.format(i)])
            node.slow_start()

            # at this point there is i+1 timeline
            pgbench = node.pgbench(options=['-T', '20', '-c', '1', '--no-vacuum'])
            pgbench.wait()

            # create backup at 2, 4 and 6 timeline
            if i % 2 == 0:
                self.pb.backup_node('node', node, backup_type='page')

        page_id = self.pb.backup_node('node', node, backup_type='page')
        pgdata = self.pgdata_content(node.data_dir)

        self.pb.merge_backup('node', page_id)

        result = node.table_checksum("pgbench_accounts")

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node=node_restored)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)

        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

        result_new = node_restored.table_checksum("pgbench_accounts")

        self.assertEqual(result, result_new)

        self.compare_pgdata(pgdata, pgdata_restored)

        self.pb.checkdb_node(
            use_backup_dir=True,
            instance='node',
            options=[
                '--amcheck',
                '-d', 'postgres', '-p', str(node.port)])

        self.pb.checkdb_node(
            use_backup_dir=True,
            instance='node',
            options=[
                '--amcheck',
                '-d', 'postgres', '-p', str(node_restored.port)])

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    @needs_gdb
    def test_merge_page_header_map_retry(self):
        """
        page header map cannot be trusted when
        running retry
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.pgbench_init(scale=20)
        self.pb.backup_node('node', node, options=['--stream'])

        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        delta_id = self.pb.backup_node('node', node,
            backup_type='delta', options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        gdb = self.pb.merge_backup('node', delta_id, gdb=True)

        # our goal here is to get full backup with merged data files,
        # but with old page header map
        gdb.set_breakpoint('cleanup_header_map')
        gdb.run_until_break()
        gdb.signal('SIGKILL')

        self.expire_locks(backup_dir, 'node')

        self.pb.merge_backup('node', delta_id)

        node.cleanup()

        self.pb.restore_node('node', node=node)
        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    @needs_gdb
    def test_missing_data_file(self):
        """
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Add data
        node.pgbench_init(scale=1)

        # FULL backup
        self.pb.backup_node('node', node)

        # Change data
        pgbench = node.pgbench(options=['-T', '5', '-c', '1'])
        pgbench.wait()

        # DELTA backup
        delta_id = self.pb.backup_node('node', node, backup_type='delta')

        path = node.safe_psql(
            'postgres',
            "select pg_relation_filepath('pgbench_accounts')").decode('utf-8').rstrip()

        gdb = self.pb.merge_backup("node", delta_id,
            options=['--log-level-file=VERBOSE'], gdb=True)
        gdb.set_breakpoint('merge_files')
        gdb.run_until_break()

        # remove data file in incremental backup
        self.remove_backup_file(backup_dir, 'node', delta_id,
                                f'database/{path}')

        gdb.continue_execution_until_error()

        logfile_content = self.read_pb_log()

        if fs_backup_class.is_file_based:
            self.assertRegex(
                logfile_content,
                'ERROR: Open backup file: Cannot open file "[^"]*{0}": No such file or directory'.format(path))
        else:  # suggesting S3 for minio, S3TestBackupDir
            regex = 'ERROR: Open backup file: S3 error [0-9a-fA-F]+:[^:]+:/[^\\n].*{0}:NoSuchKey:404: No such file'.format(
                path)
            self.assertRegex(
                logfile_content,
                regex)

    # @unittest.skip("skip")
    @needs_gdb
    def test_missing_non_data_file(self):
        """
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL backup
        self.pb.backup_node('node', node)

        # DELTA backup
        delta_id = self.pb.backup_node('node', node, backup_type='delta')

        gdb = self.pb.merge_backup("node", delta_id,
            options=['--log-level-file=VERBOSE'], gdb=True)
        gdb.set_breakpoint('merge_files')
        gdb.run_until_break()

        # remove data file in incremental backup
        self.remove_backup_file(backup_dir, 'node', delta_id,
                                'database/backup_label')

        gdb.continue_execution_until_error()

        logfile_content = self.read_pb_log()

        self.assertRegex(
            logfile_content,
            'ERROR: File "[^"]*backup_label" is not found')

        self.assertIn(
            'ERROR: Backup files merging failed',
            logfile_content)

        self.assertEqual(
            'MERGING', self.pb.show('node')[0]['status'])

        self.assertEqual(
            'MERGING', self.pb.show('node')[1]['status'])

    # @unittest.skip("skip")
    @needs_gdb
    def test_merge_remote_mode(self):
        """
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL backup
        full_id = self.pb.backup_node('node', node)

        # DELTA backup
        delta_id = self.pb.backup_node('node', node, backup_type='delta')

        self.pb.set_config('node', options=['--retention-window=1'])

        with self.modify_backup_control(backup_dir, 'node', full_id) as cf:
            cf.data += "\nrecovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                        datetime.now() - timedelta(days=5))

        gdb = self.pb.backup_node("node", node,
            options=['--log-level-file=VERBOSE', '--merge-expired'], gdb=True)
        gdb.set_breakpoint('merge_files')
        gdb.run_until_break()

        logfile_content_pre_len = len(self.read_pb_log())

        gdb.continue_execution_until_exit()

        logfile_content = self.read_pb_log()[logfile_content_pre_len:]

        self.assertNotIn(
            'SSH', logfile_content)

        self.assertEqual(
            'OK', self.pb.show('node')[0]['status'])

    def test_merge_pg_filenode_map(self):
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
        self.pb.backup_node('node', node)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            options=['-T', '10', '-c', '1'])

        self.pb.backup_node('node', node, backup_type='delta')

        node.safe_psql(
            'postgres',
            'reindex index pg_type_oid_index')

        backup_id = self.pb.backup_node('node', node, backup_type='delta')

        self.pb.merge_backup('node', backup_id)

        node.cleanup()

        self.pb.restore_node('node', node=node)
        node.slow_start()

        node.safe_psql(
            'postgres',
            'select 1')

    @needs_gdb
    def test_unfinished_merge(self):
        """ Test when parent has unfinished merge with a different backup. """
        cases = [('fail_merged', 'delete_backup_files', ['MERGED', 'MERGING', 'OK']),
                 ('fail_merging', 'create_directories_in_full', ['MERGING', 'MERGING', 'OK'])]

        for name, terminate_at, states in cases:
            node_name = 'node_' + name
            backup_dir = self.backup_dir
            self.backup_dir.cleanup()
            node = self.pg_node.make_simple(
                base_dir=os.path.join(self.module_name, self.fname, node_name),
                set_replication=True)

            self.pb.init()
            self.pb.add_instance(node_name, node)
            self.pb.set_archiving(node_name, node)
            node.slow_start()

            full_id=self.pb.backup_node(node_name, node, options=['--stream'])

            backup_id = self.pb.backup_node(node_name, node, backup_type='delta')
            second_backup_id = self.pb.backup_node(node_name, node, backup_type='delta')

            gdb = self.pb.merge_backup(node_name, backup_id, gdb=True)
            gdb.set_breakpoint(terminate_at)
            gdb.run_until_break()

            gdb.remove_all_breakpoints()
            gdb._execute('signal SIGINT')
            gdb.continue_execution_until_error()

            print(self.pb.show(node_name, as_json=False, as_text=True))

            backup_infos = self.pb.show(node_name)
            self.assertEqual(len(backup_infos), len(states))
            for expected, real in zip(states, backup_infos):
                self.assertEqual(expected, real['status'])

            with self.assertRaisesRegex(Exception,
                                        f"Full backup {full_id} has unfinished merge with backup {backup_id}"):
                self.pb.merge_backup(node_name, second_backup_id, gdb=False)

    @needs_gdb
    def test_continue_failed_merge_with_corrupted_full_backup(self):
        """
        Fail merge via gdb with corrupted FULL backup
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
                                     set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,1000) i")

        old_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()

        # FULL backup
        self.pb.backup_node('node', node)

        node.safe_psql(
            "postgres",
            "update t_heap set id = 100500")

        node.safe_psql(
            "postgres",
            "vacuum full t_heap")

        new_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()

        # DELTA BACKUP
        backup_id_1 = self.pb.backup_node('node', node, backup_type='delta')

        full_id = self.pb.show("node")[0]["id"]

        # CORRUPT full backup
        # read block from future
        # block_size + backup_header = 8200
        file_content2 = self.read_backup_file(backup_dir, 'node', backup_id_1,
                                              f'database/{new_path}')[:16400]
        # write block from future
        self.corrupt_backup_file(backup_dir, 'node', full_id,
                                 f'database/{old_path}',
                                 damage=(8200, file_content2[8200:16400]))

        # Try to continue failed MERGE
        self.pb.merge_backup("node", backup_id_1,
                          expect_error=f"WARNING: Backup {full_id} data files are corrupted")
        self.assertMessage(contains=f"ERROR: Backup {full_id} has status CORRUPT, merge is aborted")

        # Check number of backups
        show_res = self.pb.show("node")
        self.assertEqual(len(show_res), 2)

    @unittest.skipIf(not (have_alg('lz4') and have_alg('zstd')),
                     "pg_probackup is not compiled with lz4 or zstd support")
    def test_merge_compressed_and_uncompressed(self):
        """
        1. Full compressed [zlib, 3] backup -> change data
        2. Delta uncompressed -> change data
        3. Page compressed [lz4, 2] -> change data
        5. Merge all backups in one
        Restore and compare
        """
        backup_dir = self.backup_dir

        # Initialize instance and backup directory
        node = self.pg_node.make_simple('node', set_replication=True)

        self.pb.init()
        self.pb.add_instance("node", node)
        self.pb.set_archiving("node", node)
        node.slow_start()

        # Fill with data
        node.pgbench_init(scale=10)

        # Do compressed FULL backup
        self.pb.backup_node("node", node, options=['--stream',
                                                            '--compress-level', '3',
                                                            '--compress-algorithm', 'zlib'])
        show_backup = self.pb.show("node")[0]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "FULL")

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do uncompressed DELTA backup
        self.pb.backup_node("node", node,
            backup_type="delta")

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do compressed PAGE backup
        self.pb.backup_node("node", node, backup_type="page", options=['--compress-level', '2',
                                                                   '--compress-algorithm', 'lz4'])

        pgdata = self.pgdata_content(node.data_dir)

        # Check backups
        show_backup = self.pb.show("node")
        self.assertEqual(len(show_backup), 3)
        self.assertEqual(show_backup[0]["status"], "OK")
        self.assertEqual(show_backup[0]["backup-mode"], "FULL")

        self.assertEqual(show_backup[1]["status"], "OK")
        self.assertEqual(show_backup[1]["backup-mode"], "DELTA")

        self.assertEqual(show_backup[2]["status"], "OK")
        self.assertEqual(show_backup[2]["backup-mode"], "PAGE")

        page_id = show_backup[2]["id"]

        # Merge all backups
        self.pb.merge_backup("node", page_id, options=['-j5'])
        show_backups = self.pb.show("node")

        # Check number of backups and status
        self.assertEqual(len(show_backups), 1)
        self.assertEqual(show_backups[0]["status"], "OK")
        self.assertEqual(show_backups[0]["backup-mode"], "FULL")

        # Drop node and restore it
        node.cleanup()
        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        node.cleanup()

    def test_merge_with_error_backup_in_the_middle(self):
        """
        1. Full uncompressed backup -> change data
        2. Delta with error (stop node) -> change data
        3. Page -> change data
        4. Delta -> change data
        5. Merge all backups in one
        Restore and compare
        """
        backup_dir = self.backup_dir

        # Initialize instance and backup directory
        node = self.pg_node.make_simple('node', set_replication=True)

        self.pb.init()
        self.pb.add_instance("node", node)
        self.pb.set_archiving("node", node)
        node.slow_start()

        # Fill with data
        node.pgbench_init(scale=10)

        # Do uncompressed FULL backup
        self.pb.backup_node("node", node)
        show_backup = self.pb.show("node")[0]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "FULL")

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        node.stop()

        # Try to create a DELTA backup with disabled archiving (expecting error)
        self.pb.backup_node("node", node, backup_type="delta", expect_error=True)

        # Enable archiving
        node.slow_start()

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do PAGE backup
        self.pb.backup_node("node", node, backup_type="page")

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do DELTA backup
        self.pb.backup_node("node", node, backup_type="delta")

        # Check backups
        show_backup = self.pb.show("node")
        self.assertEqual(len(show_backup), 4)
        self.assertEqual(show_backup[0]["status"], "OK")
        self.assertEqual(show_backup[0]["backup-mode"], "FULL")

        self.assertEqual(show_backup[1]["status"], "ERROR")
        self.assertEqual(show_backup[1]["backup-mode"], "DELTA")

        self.assertEqual(show_backup[2]["status"], "OK")
        self.assertEqual(show_backup[2]["backup-mode"], "PAGE")

        self.assertEqual(show_backup[3]["status"], "OK")
        self.assertEqual(show_backup[3]["backup-mode"], "DELTA")

        delta_id = show_backup[3]["id"]

        # Merge all backups
        self.pb.merge_backup("node", delta_id, options=['-j5'])
        show_backup = self.pb.show("node")

        # Check number of backups and status
        self.assertEqual(len(show_backup), 2)
        self.assertEqual(show_backup[0]["status"], "ERROR")
        self.assertEqual(show_backup[0]["backup-mode"], "DELTA")

        self.assertEqual(show_backup[1]["status"], "OK")
        self.assertEqual(show_backup[1]["backup-mode"], "FULL")

    def test_merge_with_deleted_backup_in_the_middle(self):
        """
        1. Full uncompressed backup -> change data
        2. Delta uncompressed -> change data
        3. 1 Page uncompressed -> change data
        4. 2 Page uncompressed
        5. Remove 1 Page backup
        5. Merge all backups in one
        Restore and compare
        """
        backup_dir = self.backup_dir

        # Initialize instance and backup directory
        node = self.pg_node.make_simple('node', set_replication=True)

        self.pb.init()
        self.pb.add_instance("node", node)
        self.pb.set_archiving("node", node)
        node.slow_start()

        # Fill with data
        node.pgbench_init(scale=10)

        # Do uncompressed FULL backup
        self.pb.backup_node("node", node)
        show_backup = self.pb.show("node")[0]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "FULL")

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do uncompressed DELTA backup
        self.pb.backup_node("node", node,
            backup_type="delta")

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do uncompressed 1 PAGE backup
        self.pb.backup_node("node", node, backup_type="page")

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do uncompressed 2 PAGE backup
        self.pb.backup_node("node", node, backup_type="page")

        pgdata = self.pgdata_content(node.data_dir)

        # Check backups
        show_backup = self.pb.show("node")
        self.assertEqual(len(show_backup), 4)
        self.assertEqual(show_backup[0]["status"], "OK")
        self.assertEqual(show_backup[0]["backup-mode"], "FULL")

        self.assertEqual(show_backup[1]["status"], "OK")
        self.assertEqual(show_backup[1]["backup-mode"], "DELTA")

        self.assertEqual(show_backup[2]["status"], "OK")
        self.assertEqual(show_backup[2]["backup-mode"], "PAGE")

        self.assertEqual(show_backup[3]["status"], "OK")
        self.assertEqual(show_backup[3]["backup-mode"], "PAGE")

        first_page_id = show_backup[2]["id"]
        second_page_id = show_backup[3]["id"]

        # Remove backup in the middle
        self.remove_one_backup(backup_dir, "node", first_page_id)

        # Merge all backups
        error = self.pb.merge_backup("node", second_page_id, options=['-j5'], expect_error=True)
        self.assertMessage(error, contains=f"WARNING: Backup {first_page_id} is missing\n"
                                           f"ERROR: Failed to find parent full backup for {second_page_id}")

    def test_merge_with_multiple_full_backups(self):
        """
            1. Full backup -> change data
            2. Delta -> change data
            3. Page -> change data
            4. Full -> change data
            5. Page -> change data
            6. Delta
            7. Merge all backups in one
            Restore and compare
            """
        # Initialize instance and backup directory
        node = self.pg_node.make_simple('node', set_replication=True)

        self.pb.init()
        self.pb.add_instance("node", node)
        self.pb.set_archiving("node", node)
        node.slow_start()

        # Fill with data
        node.pgbench_init(scale=10)

        # Do uncompressed FULL backup
        self.pb.backup_node("node", node)
        show_backup = self.pb.show("node")[0]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "FULL")

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do uncompressed DELTA backup
        self.pb.backup_node("node", node,
            backup_type="delta")

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do uncompressed PAGE backup
        self.pb.backup_node("node", node, backup_type="page")

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do uncompressed FULL backup
        self.pb.backup_node("node", node)

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do uncompressed PAGE backup
        self.pb.backup_node("node", node, backup_type="page")

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do uncompressed DELTA backup
        self.pb.backup_node("node", node,
            backup_type="delta")

        pgdata = self.pgdata_content(node.data_dir)

        # Check backups
        show_backup = self.pb.show("node")
        self.assertEqual(len(show_backup), 6)
        self.assertEqual(show_backup[0]["status"], "OK")
        self.assertEqual(show_backup[0]["backup-mode"], "FULL")

        self.assertEqual(show_backup[1]["status"], "OK")
        self.assertEqual(show_backup[1]["backup-mode"], "DELTA")

        self.assertEqual(show_backup[2]["status"], "OK")
        self.assertEqual(show_backup[2]["backup-mode"], "PAGE")

        self.assertEqual(show_backup[3]["status"], "OK")
        self.assertEqual(show_backup[3]["backup-mode"], "FULL")

        self.assertEqual(show_backup[4]["status"], "OK")
        self.assertEqual(show_backup[4]["backup-mode"], "PAGE")

        self.assertEqual(show_backup[5]["status"], "OK")
        self.assertEqual(show_backup[5]["backup-mode"], "DELTA")

        last_backup_id = show_backup[5]["id"]

        # Merge all backups
        self.pb.merge_backup("node", last_backup_id, options=['-j5'])
        show_backups = self.pb.show("node")

        # Check number of backups and status
        self.assertEqual(len(show_backups), 4)
        self.assertEqual(show_backup[0]["status"], "OK")
        self.assertEqual(show_backup[0]["backup-mode"], "FULL")

        self.assertEqual(show_backup[1]["status"], "OK")
        self.assertEqual(show_backup[1]["backup-mode"], "DELTA")

        self.assertEqual(show_backup[2]["status"], "OK")
        self.assertEqual(show_backup[2]["backup-mode"], "PAGE")

        self.assertEqual(show_backup[3]["status"], "OK")
        self.assertEqual(show_backup[3]["backup-mode"], "FULL")

        # Drop node and restore it
        node.cleanup()
        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        node.cleanup()

    def test_merge_with_logical_corruption(self):
        """
        1. Full backup -> change data
        2. Break logic (remove a referenced row from parent table)
        3. Mark foreign key constraint as NOT VALID
        4. Perform PAGE backup
        5. Merge all backups into one
        6. Restore and compare data directories
        7. Validate foreign key constraint to check that the logical corruption is also restored
        """
        backup_dir = self.backup_dir

        # Initialize instance and backup directory
        node = self.pg_node.make_simple('node', set_replication=True)

        self.pb.init()
        self.pb.add_instance("node", node)
        self.pb.set_archiving("node", node)
        node.slow_start()

        # Create a table and fill
        node.safe_psql("postgres", """
            CREATE TABLE parent_table (
                         id serial PRIMARY KEY,
                         name varchar(100) NOT NULL
            );

            CREATE TABLE child_table (
                         id serial PRIMARY KEY,
                         parent_id integer REFERENCES parent_table (id),
                         value varchar(100) NOT NULL
            );

            INSERT INTO parent_table (name) VALUES ('Parent 1'), ('Parent 2'), ('Parent 3');

            INSERT INTO child_table (parent_id, value) VALUES (1, 'Child 1.1'), (1, 'Child 1.2'), (2, 'Child 2.1'), 
                        (2, 'Child 2.2'), (3, 'Child 3.1'), (3, 'Child 3.2');
            """)

        # Do Full backup
        self.pb.backup_node("node", node)

        # Break logic
        node.safe_psql("postgres", """
            ALTER TABLE child_table DROP CONSTRAINT child_table_parent_id_fkey;
            DELETE FROM parent_table WHERE id = 2;
            ALTER TABLE child_table ADD CONSTRAINT child_table_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES parent_table (id) NOT VALID;
        """)

        # Do PAGE backup
        self.pb.backup_node("node", node, backup_type="page")

        pgdata = self.pgdata_content(node.data_dir)

        # Check backups
        show_backup = self.pb.show("node")
        self.assertEqual(len(show_backup), 2)
        self.assertEqual(show_backup[0]["status"], "OK")
        self.assertEqual(show_backup[0]["backup-mode"], "FULL")

        self.assertEqual(show_backup[1]["status"], "OK")
        self.assertEqual(show_backup[1]["backup-mode"], "PAGE")

        page_id = show_backup[1]["id"]

        # Merge all backups
        self.pb.merge_backup("node", page_id, options=['-j5'])
        show_backups = self.pb.show("node")

        # Check number of backups and status
        self.assertEqual(len(show_backups), 1)
        self.assertEqual(show_backups[0]["status"], "OK")
        self.assertEqual(show_backups[0]["backup-mode"], "FULL")

        # Drop node and restore it
        node.cleanup()
        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        node.slow_start()

        # Check that logic of restored table also broken
        error = node.safe_psql("postgres", """
            ALTER TABLE child_table VALIDATE CONSTRAINT child_table_parent_id_fkey;
        """, expect_error=True)
        self.assertMessage(error, contains='Key (parent_id)=(2) is not present in table "parent_table"')

        # Clean after yourself
        node.cleanup()

    def test_two_merges_1(self):
        """
        Test two merges for one full backup.
        """
        node = self.pg_node.make_simple('node',
                                        initdb_params=['--data-checksums'])
        node.set_auto_conf({'shared_buffers': '2GB',
                            'autovacuum': 'off'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Fill with data
        node.pgbench_init(scale=100)

        self.pb.backup_node('node', node, backup_type="full")

        pgbench = node.pgbench(options=['-t', '20000', '-c', '3', '-n'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '20000', '-c', '3', '-n'])
        pgbench.wait()

        prev_id = self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '20000', '-c', '3', '-n'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '20000', '-c', '3', '-n'])
        pgbench.wait()

        last_id = self.pb.backup_node('node', node, backup_type="page")

        checksum = node.pgbench_table_checksums()
        node.stop()

        self.pb.merge_backup('node', prev_id)

        self.pb.merge_backup('node', last_id)

        restored = self.pg_node.make_empty('restored')
        self.pb.restore_node('node', restored)
        restored.set_auto_conf({'port': restored.port})
        restored.slow_start()

        restored_checksum = restored.pgbench_table_checksums()
        self.assertEqual(checksum, restored_checksum,
                         "data are not equal")

    def test_two_merges_2(self):
        """
        Test two merges for one full backup with data in tablespace.
        """
        node = self.pg_node.make_simple('node', initdb_params=['--data-checksums'])
        node.set_auto_conf({'shared_buffers': '2GB',
                            'autovacuum': 'off'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'somedata')

        # Fill with data
        node.pgbench_init(scale=100, options=['--tablespace=somedata'])

        self.pb.backup_node('node', node, backup_type="full")

        pgbench = node.pgbench(options=['-t', '20000', '-c', '3', '-n'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '20000', '-c', '3', '-n'])
        pgbench.wait()

        prev_id = self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '20000', '-c', '3', '-n'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '20000', '-c', '3', '-n'])
        pgbench.wait()

        last_id = self.pb.backup_node('node', node, backup_type="page")

        checksum = node.pgbench_table_checksums()
        node.stop()

        self.pb.merge_backup('node', prev_id)

        self.pb.merge_backup('node', last_id)

        restored = self.pg_node.make_empty('restored')
        node_ts = os.path.join(node.base_dir, 'somedata')
        restored_ts = os.path.join(restored.base_dir, 'somedata')
        os.mkdir(restored_ts)
        self.pb.restore_node('node', restored,
                        options=['-T', f"{node_ts}={restored_ts}"])
        restored.set_auto_conf({'port': restored.port})
        restored.slow_start()

        restored_checksum = restored.pgbench_table_checksums()
        self.assertEqual(checksum, restored_checksum,
                         "data are not equal")

    @needs_gdb
    def test_backup_while_merge(self):
        """
        Test backup is not possible while closest full backup is in merge.
        (PBCKP-626_)
        TODO: fix it if possible.
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=1)

        self.pb.backup_node('node', node, backup_type="full")

        pgbench = node.pgbench(options=['-t', '100', '-c', '3', '-n'])
        pgbench.wait()

        first_page = self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '100', '-c', '3', '-n'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type="page")

        pgbench = node.pgbench(options=['-t', '100', '-c', '3', '-n'])
        pgbench.wait()

        gdb = self.pb.merge_backup('node', first_page, gdb=True)
        gdb.set_breakpoint('create_directories_in_full')
        gdb.run_until_break()

        self.pb.backup_node('node', node, backup_type="page",
                            expect_error="just because it goes this way yet.")

        gdb.kill()

############################################################################
#                                 dry-run
############################################################################

    def test_basic_dry_run_merge_full_2page(self):
        """
        1. Full backup -> fill data
        2. First Page backup -> fill data
        3. Second Page backup
        4. Merge 2 "Page" backups with dry-run
        Compare instance directory before and after merge
        """
        # Initialize instance and backup directory
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance("node", node)
        self.pb.set_archiving("node", node)
        node.slow_start()

        # Do full backup
        self.pb.backup_node("node", node, options=['--compress'])
        show_backup = self.pb.show("node")[0]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "FULL")

        # Fill with data
        with node.connect() as conn:
            conn.execute("create table test (id int);")
            conn.execute(
                "insert into test select i from generate_series(1,10) s(i);")
            conn.commit()

        # Do first page backup
        self.pb.backup_node("node", node, backup_type="page", options=['--compress'])
        show_backup = self.pb.show("node")[1]

        # sanity check
        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "PAGE")

        # Fill with data
        with node.connect() as conn:
            conn.execute(
                "insert into test select i from generate_series(1,10) s(i);")
            count1 = conn.execute("select count(*) from test")
            conn.commit()

        # take PAGE backup with external directory pointing to a file
        external_dir = self.get_tblspace_path(node, 'somedirectory')
        os.mkdir(external_dir)

        # Do second page backup
        self.pb.backup_node("node", node,
                            backup_type="page", options=['--compress', '--external-dirs={0}'.format(external_dir)])
        show_backup = self.pb.show("node")[2]
        page_id = show_backup["id"]

        # sanity check
        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "PAGE")

        # Check data changes absence
        instance_before = self.pgdata_content(os.path.join(self.backup_dir, 'backups/node'))

        # Merge all backups
        output = self.pb.merge_backup("node", page_id,
                                      options=['--dry-run'])
        self.assertNotIn("WARNING", output)
        instance_after = self.pgdata_content(os.path.join(self.backup_dir, 'backups/node'))
        self.compare_pgdata(instance_before, instance_after)

        show_backups = self.pb.show("node")
        node.cleanup()

    @unittest.skipIf(not fs_backup_class.is_file_based, "AccessPath check is always true on s3")
    def test_basic_dry_run_check_merge_with_access(self):
        """
        Access check suite if disk mounted as read_only
        """
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance("node", node)
        self.pb.set_archiving("node", node)
        node.slow_start()

        # Do full backup
        self.pb.backup_node("node", node, options=['--compress'])
        show_backup = self.pb.show("node")[0]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "FULL")

        # Fill with data
        with node.connect() as conn:
            conn.execute("create table test (id int);")
            conn.execute(
                "insert into test select i from generate_series(1,10) s(i);")
            conn.commit()

        # Do first page backup
        self.pb.backup_node("node", node, backup_type="page", options=['--compress'])
        show_backup = self.pb.show("node")[1]

        # sanity check
        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "PAGE")

        # Fill with data
        with node.connect() as conn:
            conn.execute(
                "insert into test select i from generate_series(1,10) s(i);")
            count1 = conn.execute("select count(*) from test")
            conn.commit()

        # take PAGE backup with external directory pointing to a file
        external_dir = self.get_tblspace_path(node, 'somedirectory')
        os.mkdir(external_dir)

        # Do second page backup
        self.pb.backup_node("node", node,
                            backup_type="page", options=['--compress', '--external-dirs={0}'.format(external_dir)])
        show_backup = self.pb.show("node")[2]
        page_id = show_backup["id"]

        # sanity check
        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "PAGE")

        # Access check
        dir_path = os.path.join(self.backup_dir, 'backups/node')
        dir_mode = os.stat(dir_path).st_mode
        os.chmod(dir_path, 0o500)

        error_message = self.pb.merge_backup("node", page_id,
                                             options=['--dry-run'], expect_error ='because of changed permissions')
        try:
            self.assertMessage(error_message, contains='ERROR: Check permissions')
        finally:
            # Cleanup
            os.chmod(dir_path, dir_mode)

        dir_path = os.path.join(self.backup_dir, 'backups/node/', page_id)
        dir_mode = os.stat(dir_path).st_mode
        os.chmod(dir_path, 0o500)

        error_message = self.pb.merge_backup("node", page_id,
                                         options=['--dry-run'], expect_error ='because of changed permissions')
        try:
            self.assertMessage(error_message, contains='ERROR: Check permissions')
        finally:
            # Cleanup
            os.chmod(dir_path, dir_mode)




class BenchMerge(ProbackupTest):

    def setUp(self):
        super().setUp()

        node = self.pg_node.make_simple('node',
                                        set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.execute("""
        do $$
            declare
                i int;
            begin
                for i in 0..2000 loop
                    execute 'create table x'||i||'(i int primary key, j text);';
                    commit;
                end loop;
            end;
        $$;
        """)

        start = int(time.time())
        self.pb.backup_node('node', node,
                            options=['--start-time', str(start)])
        for i in range(50):
            start += 1
            self.pb.backup_node('node', node, backup_type='page',
                                options=['--start-time', str(start)])
        start += 1
        self.backup_id = self.pb.backup_node('node', node, backup_type='page',
                                             options=['--start-time', str(start)])

    def test_bench_merge_long_chain(self):
        """
        test long incremental chain with a lot of tables
        """

        start = time.time()
        self.pb.merge_backup('node', self.backup_id)
        stop = time.time()
        print(f"LASTS FOR {stop - start}")
