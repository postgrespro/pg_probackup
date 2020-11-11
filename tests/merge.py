# coding: utf-8

import unittest
import os
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from testgres import QueryException
import shutil
from datetime import datetime, timedelta
import time

module_name = "merge"


class MergeTest(ProbackupTest, unittest.TestCase):

    def test_basic_merge_full_page(self):
        """
        Test MERGE command, it merges FULL backup with target PAGE backups
        """
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, "backup")

        # Initialize instance and backup directory
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=["--data-checksums"])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # Do full backup
        self.backup_node(backup_dir, "node", node, options=['--compress'])
        show_backup = self.show_pb(backup_dir, "node")[0]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "FULL")

        # Fill with data
        with node.connect() as conn:
            conn.execute("create table test (id int)")
            conn.execute(
                "insert into test select i from generate_series(1,10) s(i)")
            conn.commit()

        # Do first page backup
        self.backup_node(backup_dir, "node", node, backup_type="page", options=['--compress'])
        show_backup = self.show_pb(backup_dir, "node")[1]

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
        self.backup_node(
            backup_dir, "node", node,
            backup_type="page", options=['--compress'])
        show_backup = self.show_pb(backup_dir, "node")[2]
        page_id = show_backup["id"]

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # sanity check
        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "PAGE")

        # Merge all backups
        self.merge_backup(backup_dir, "node", page_id,
                          options=["-j", "4"])
        show_backups = self.show_pb(backup_dir, "node")

        # sanity check
        self.assertEqual(len(show_backups), 1)
        self.assertEqual(show_backups[0]["status"], "OK")
        self.assertEqual(show_backups[0]["backup-mode"], "FULL")

        # Drop node and restore it
        node.cleanup()
        self.restore_node(backup_dir, 'node', node)

        # Check physical correctness
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        node.slow_start()

        # Check restored node
        count2 = node.execute("postgres", "select count(*) from test")
        self.assertEqual(count1, count2)

        # Clean after yourself
        self.del_test_dir(module_name, fname, [node])

    def test_merge_compressed_backups(self):
        """
        Test MERGE command with compressed backups
        """
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, "backup")

        # Initialize instance and backup directory
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=["--data-checksums"])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # Do full compressed backup
        self.backup_node(backup_dir, "node", node, options=['--compress'])
        show_backup = self.show_pb(backup_dir, "node")[0]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "FULL")

        # Fill with data
        with node.connect() as conn:
            conn.execute("create table test (id int)")
            conn.execute(
                "insert into test select i from generate_series(1,10) s(i)")
            count1 = conn.execute("select count(*) from test")
            conn.commit()

        # Do compressed page backup
        self.backup_node(
            backup_dir, "node", node, backup_type="page", options=['--compress'])
        show_backup = self.show_pb(backup_dir, "node")[1]
        page_id = show_backup["id"]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "PAGE")

        # Merge all backups
        self.merge_backup(backup_dir, "node", page_id, options=['-j2'])
        show_backups = self.show_pb(backup_dir, "node")

        self.assertEqual(len(show_backups), 1)
        self.assertEqual(show_backups[0]["status"], "OK")
        self.assertEqual(show_backups[0]["backup-mode"], "FULL")

        # Drop node and restore it
        node.cleanup()
        self.restore_node(backup_dir, 'node', node)
        node.slow_start()

        # Check restored node
        count2 = node.execute("postgres", "select count(*) from test")
        self.assertEqual(count1, count2)

        # Clean after yourself
        node.cleanup()
        self.del_test_dir(module_name, fname)

    def test_merge_compressed_backups_1(self):
        """
        Test MERGE command with compressed backups
        """
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, "backup")

        # Initialize instance and backup directory
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True, initdb_params=["--data-checksums"],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # Fill with data
        node.pgbench_init(scale=10)

        # Do compressed FULL backup
        self.backup_node(backup_dir, "node", node, options=['--compress', '--stream'])
        show_backup = self.show_pb(backup_dir, "node")[0]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "FULL")

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do compressed DELTA backup
        self.backup_node(
            backup_dir, "node", node,
            backup_type="delta", options=['--compress', '--stream'])

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do compressed PAGE backup
        self.backup_node(
            backup_dir, "node", node, backup_type="page", options=['--compress'])

        pgdata = self.pgdata_content(node.data_dir)

        show_backup = self.show_pb(backup_dir, "node")[2]
        page_id = show_backup["id"]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "PAGE")

        # Merge all backups
        self.merge_backup(backup_dir, "node", page_id, options=['-j2'])
        show_backups = self.show_pb(backup_dir, "node")

        self.assertEqual(len(show_backups), 1)
        self.assertEqual(show_backups[0]["status"], "OK")
        self.assertEqual(show_backups[0]["backup-mode"], "FULL")

        # Drop node and restore it
        node.cleanup()
        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        node.cleanup()
        self.del_test_dir(module_name, fname)

    def test_merge_compressed_and_uncompressed_backups(self):
        """
        Test MERGE command with compressed and uncompressed backups
        """
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, "backup")

        # Initialize instance and backup directory
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True, initdb_params=["--data-checksums"],
            pg_options={
                'autovacuum': 'off'
            }
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # Fill with data
        node.pgbench_init(scale=10)

        # Do compressed FULL backup
        self.backup_node(backup_dir, "node", node, options=[
            '--compress-algorithm=zlib', '--stream'])
        show_backup = self.show_pb(backup_dir, "node")[0]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "FULL")

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do compressed DELTA backup
        self.backup_node(
            backup_dir, "node", node, backup_type="delta",
            options=['--compress', '--stream'])

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do uncompressed PAGE backup
        self.backup_node(backup_dir, "node", node, backup_type="page")

        pgdata = self.pgdata_content(node.data_dir)

        show_backup = self.show_pb(backup_dir, "node")[2]
        page_id = show_backup["id"]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "PAGE")

        # Merge all backups
        self.merge_backup(backup_dir, "node", page_id, options=['-j2'])
        show_backups = self.show_pb(backup_dir, "node")

        self.assertEqual(len(show_backups), 1)
        self.assertEqual(show_backups[0]["status"], "OK")
        self.assertEqual(show_backups[0]["backup-mode"], "FULL")

        # Drop node and restore it
        node.cleanup()
        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        node.cleanup()
        self.del_test_dir(module_name, fname)

    def test_merge_compressed_and_uncompressed_backups_1(self):
        """
        Test MERGE command with compressed and uncompressed backups
        """
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, "backup")

        # Initialize instance and backup directory
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True, initdb_params=["--data-checksums"],
            pg_options={
                'autovacuum': 'off'
            }
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # Fill with data
        node.pgbench_init(scale=5)

        # Do compressed FULL backup
        self.backup_node(backup_dir, "node", node, options=[
            '--compress-algorithm=zlib', '--stream'])
        show_backup = self.show_pb(backup_dir, "node")[0]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "FULL")

        # Change data
        pgbench = node.pgbench(options=['-T', '20', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do uncompressed DELTA backup
        self.backup_node(
            backup_dir, "node", node, backup_type="delta",
            options=['--stream'])

        # Change data
        pgbench = node.pgbench(options=['-T', '20', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do compressed PAGE backup
        self.backup_node(
            backup_dir, "node", node, backup_type="page",
            options=['--compress-algorithm=zlib'])

        pgdata = self.pgdata_content(node.data_dir)

        show_backup = self.show_pb(backup_dir, "node")[2]
        page_id = show_backup["id"]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "PAGE")

        # Merge all backups
        self.merge_backup(backup_dir, "node", page_id)
        show_backups = self.show_pb(backup_dir, "node")

        self.assertEqual(len(show_backups), 1)
        self.assertEqual(show_backups[0]["status"], "OK")
        self.assertEqual(show_backups[0]["backup-mode"], "FULL")

        # Drop node and restore it
        node.cleanup()
        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        node.cleanup()
        self.del_test_dir(module_name, fname)

    def test_merge_compressed_and_uncompressed_backups_2(self):
        """
        Test MERGE command with compressed and uncompressed backups
        """
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, "backup")

        # Initialize instance and backup directory
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True, initdb_params=["--data-checksums"],
            pg_options={
                'autovacuum': 'off'
            }
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # Fill with data
        node.pgbench_init(scale=20)

        # Do uncompressed FULL backup
        self.backup_node(backup_dir, "node", node)
        show_backup = self.show_pb(backup_dir, "node")[0]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "FULL")

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do compressed DELTA backup
        self.backup_node(
            backup_dir, "node", node, backup_type="delta",
            options=['--compress-algorithm=zlib', '--stream'])

        # Change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Do uncompressed PAGE backup
        self.backup_node(
            backup_dir, "node", node, backup_type="page")

        pgdata = self.pgdata_content(node.data_dir)

        show_backup = self.show_pb(backup_dir, "node")[2]
        page_id = show_backup["id"]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "PAGE")

        # Merge all backups
        self.merge_backup(backup_dir, "node", page_id)
        show_backups = self.show_pb(backup_dir, "node")

        self.assertEqual(len(show_backups), 1)
        self.assertEqual(show_backups[0]["status"], "OK")
        self.assertEqual(show_backups[0]["backup-mode"], "FULL")

        # Drop node and restore it
        node.cleanup()
        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        node.cleanup()
        self.del_test_dir(module_name, fname)


    # @unittest.skip("skip")
    def test_merge_tablespaces(self):
        """
        Create tablespace with table, take FULL backup,
        create another tablespace with another table and drop previous
        tablespace, take page backup, merge it and restore

        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True, initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'
            }
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'somedata')
        node.safe_psql(
            "postgres",
            "create table t_heap tablespace somedata as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,100) i"
        )
        # FULL backup
        self.backup_node(backup_dir, 'node', node)

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
        backup_id = self.backup_node(backup_dir, 'node', node, backup_type="page")

        pgdata = self.pgdata_content(node.data_dir)

        node.stop()
        shutil.rmtree(
            self.get_tblspace_path(node, 'somedata'),
            ignore_errors=True)
        shutil.rmtree(
            self.get_tblspace_path(node, 'somedata1'),
            ignore_errors=True)
        node.cleanup()

        self.merge_backup(backup_dir, 'node', backup_id)

        self.restore_node(
            backup_dir, 'node', node, options=["-j", "4"])

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
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True, initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'
            }
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'somedata')

        # FULL backup
        self.backup_node(backup_dir, 'node', node)
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
        self.backup_node(backup_dir, 'node', node, backup_type="page")

        node.safe_psql(
            "postgres",
            "drop table t_heap"
        )
        node.safe_psql(
            "postgres",
            "drop tablespace somedata"
        )

        # DELTA backup
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type="delta")

        pgdata = self.pgdata_content(node.data_dir)

        node.stop()
        shutil.rmtree(
            self.get_tblspace_path(node, 'somedata'),
            ignore_errors=True)
        shutil.rmtree(
            self.get_tblspace_path(node, 'somedata1'),
            ignore_errors=True)
        node.cleanup()

        self.merge_backup(backup_dir, 'node', backup_id)

        self.restore_node(
            backup_dir, 'node', node,
            options=["-j", "4"])

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_merge_page_truncate(self):
        """
        make node, create table, take full backup,
        delete last 3 pages, vacuum relation,
        take page backup, merge full and page,
        restore last page backup and check data correctness
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '300s',
                'autovacuum': 'off'})

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
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

        self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "delete from t_heap where ctid >= '(11,0)'")
        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        page_id = self.show_pb(backup_dir, "node")[1]["id"]
        self.merge_backup(backup_dir, "node", page_id)

        self.validate_pb(backup_dir)

        old_tablespace = self.get_tblspace_path(node, 'somedata')
        new_tablespace = self.get_tblspace_path(node_restored, 'somedata_new')

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=[
                "-j", "4",
                "-T", "{0}={1}".format(old_tablespace, new_tablespace)])

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()

        # Logical comparison
        result1 = node.safe_psql(
            "postgres",
            "select * from t_heap")

        result2 = node_restored.safe_psql(
            "postgres",
            "select * from t_heap")

        self.assertEqual(result1, result2)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_merge_delta_truncate(self):
        """
        make node, create table, take full backup,
        delete last 3 pages, vacuum relation,
        take page backup, merge full and page,
        restore last page backup and check data correctness
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '300s',
                'autovacuum': 'off'})

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
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

        self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "delete from t_heap where ctid >= '(11,0)'")
        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        page_id = self.show_pb(backup_dir, "node")[1]["id"]
        self.merge_backup(backup_dir, "node", page_id)

        self.validate_pb(backup_dir)

        old_tablespace = self.get_tblspace_path(node, 'somedata')
        new_tablespace = self.get_tblspace_path(node_restored, 'somedata_new')

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=[
                "-j", "4",
                "-T", "{0}={1}".format(old_tablespace, new_tablespace)])

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()

        # Logical comparison
        result1 = node.safe_psql(
            "postgres",
            "select * from t_heap")

        result2 = node_restored.safe_psql(
            "postgres",
            "select * from t_heap")

        self.assertEqual(result1, result2)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_merge_ptrack_truncate(self):
        """
        make node, create table, take full backup,
        delete last 3 pages, vacuum relation,
        take page backup, merge full and page,
        restore last page backup and check data correctness
        """
        if not self.ptrack:
            return unittest.skip('Skipped because ptrack support is disabled')

        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            ptrack_enable=True,
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 12:
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

        self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "delete from t_heap where ctid >= '(11,0)'")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        page_id = self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.merge_backup(backup_dir, "node", page_id)

        self.validate_pb(backup_dir)

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        old_tablespace = self.get_tblspace_path(node, 'somedata')
        new_tablespace = self.get_tblspace_path(node_restored, 'somedata_new')

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=[
                "-j", "4",
                "-T", "{0}={1}".format(old_tablespace, new_tablespace)])

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()

        # Logical comparison
        result1 = node.safe_psql(
            "postgres",
            "select * from t_heap")

        result2 = node_restored.safe_psql(
            "postgres",
            "select * from t_heap")

        self.assertEqual(result1, result2)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_merge_delta_delete(self):
        """
        Make node, create tablespace with table, take full backup,
        alter tablespace location, take delta backup, merge full and delta,
        restore database.
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True, initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s',
                'autovacuum': 'off'
            }
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'somedata')

        # FULL backup
        self.backup_node(backup_dir, 'node', node, options=["--stream"])

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
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta',
            options=["--stream"]
        )

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        backup_id = self.show_pb(backup_dir, "node")[1]["id"]
        self.merge_backup(backup_dir, "node", backup_id, options=["-j", "4"])

        # RESTORE
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored')
        )
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored,
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
        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_continue_failed_merge(self):
        """
        Check that failed MERGE can be continued
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(
                module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # FULL backup
        self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,1000) i"
        )

        # DELTA BACKUP
        self.backup_node(
            backup_dir, 'node', node, backup_type='delta'
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
        self.backup_node(
            backup_dir, 'node', node, backup_type='delta'
        )

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        backup_id = self.show_pb(backup_dir, "node")[2]["id"]

        gdb = self.merge_backup(backup_dir, "node", backup_id, gdb=True)

        gdb.set_breakpoint('backup_non_data_file_internal')
        gdb.run_until_break()

        gdb.continue_execution_until_break(5)

        gdb._execute('signal SIGKILL')
        gdb._execute('detach')
        time.sleep(1)

        print(self.show_pb(backup_dir, as_text=True, as_json=False))

        # Try to continue failed MERGE
        self.merge_backup(backup_dir, "node", backup_id)

        # Drop node and restore it
        node.cleanup()
        self.restore_node(backup_dir, 'node', node)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_continue_failed_merge_with_corrupted_delta_backup(self):
        """
        Fail merge via gdb, corrupt DELTA backup, try to continue merge
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True, initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # FULL backup
        self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,1000) i")

        old_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()

        # DELTA BACKUP
        self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

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
        backup_id_2 = self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        backup_id = self.show_pb(backup_dir, "node")[1]["id"]

        # Failed MERGE
        gdb = self.merge_backup(backup_dir, "node", backup_id, gdb=True)
        gdb.set_breakpoint('backup_non_data_file_internal')
        gdb.run_until_break()

        gdb.continue_execution_until_break(2)

        gdb._execute('signal SIGKILL')

        # CORRUPT incremental backup
        # read block from future
        # block_size + backup_header = 8200
        file = os.path.join(
            backup_dir, 'backups', 'node',
            backup_id_2, 'database', new_path)
        with open(file, 'rb') as f:
            f.seek(8200)
            block_1 = f.read(8200)
            f.close

        # write block from future
        file = os.path.join(
            backup_dir, 'backups', 'node',
            backup_id, 'database', old_path)
        with open(file, 'r+b') as f:
            f.seek(8200)
            f.write(block_1)
            f.close

        # Try to continue failed MERGE
        try:
            print(self.merge_backup(backup_dir, "node", backup_id))
            self.assertEqual(
                1, 0,
                "Expecting Error because of incremental backup corruption.\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                "ERROR: Backup {0} has status CORRUPT, merge is aborted".format(
                    backup_id) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_continue_failed_merge_2(self):
        """
        Check that failed MERGE on delete can be continued
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True, initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # FULL backup
        self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,1000) i")

        # DELTA BACKUP
        self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        node.safe_psql(
            "postgres",
            "delete from t_heap")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        # DELTA BACKUP
        self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        backup_id = self.show_pb(backup_dir, "node")[2]["id"]

        gdb = self.merge_backup(backup_dir, "node", backup_id, gdb=True)

        gdb.set_breakpoint('pgFileDelete')

        gdb.run_until_break()

        gdb._execute('thread apply all bt')

        gdb.continue_execution_until_break(20)

        gdb._execute('thread apply all bt')

        gdb._execute('signal SIGKILL')

        print(self.show_pb(backup_dir, as_text=True, as_json=False))

        backup_id_deleted = self.show_pb(backup_dir, "node")[1]["id"]

        # TODO check that full backup has meta info is equal to DELETTING

        # Try to continue failed MERGE
        self.merge_backup(backup_dir, "node", backup_id)
        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_continue_failed_merge_3(self):
        """
        Check that failed MERGE cannot be continued if intermediate
        backup is missing.
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True, initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
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
        self.backup_node(backup_dir, 'node', node)

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
            self.backup_node(
                backup_dir, 'node', node, backup_type='page'
            )
            i = i + 1

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        backup_id_merge = self.show_pb(backup_dir, "node")[2]["id"]
        backup_id_delete = self.show_pb(backup_dir, "node")[1]["id"]

        print(self.show_pb(backup_dir, as_text=True, as_json=False))

        gdb = self.merge_backup(backup_dir, "node", backup_id_merge, gdb=True)

        gdb.set_breakpoint('backup_non_data_file_internal')
        gdb.run_until_break()
        gdb.continue_execution_until_break(2)

        gdb._execute('signal SIGKILL')

        print(self.show_pb(backup_dir, as_text=True, as_json=False))
        # print(os.path.join(backup_dir, "backups", "node", backup_id_delete))

        # DELETE PAGE1
        shutil.rmtree(
            os.path.join(backup_dir, "backups", "node", backup_id_delete))

        # Try to continue failed MERGE
        try:
            self.merge_backup(backup_dir, "node", backup_id_merge)
            self.assertEqual(
                1, 0,
                "Expecting Error because of backup corruption.\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                "ERROR: Incremental chain is broken, "
                "merge is impossible to finish" in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_merge_different_compression_algo(self):
        """
        Check that backups with different compression algorithms can be merged
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

        # FULL backup
        self.backup_node(
            backup_dir, 'node', node, options=['--compress-algorithm=zlib'])

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,1000) i")

        # DELTA BACKUP
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta', options=['--compress-algorithm=pglz'])

        node.safe_psql(
            "postgres",
            "delete from t_heap")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        # DELTA BACKUP
        self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        backup_id = self.show_pb(backup_dir, "node")[2]["id"]

        self.merge_backup(backup_dir, "node", backup_id)

        self.del_test_dir(module_name, fname)

    def test_merge_different_wal_modes(self):
        """
        Check that backups with different wal modes can be merged
        correctly
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

        # FULL stream backup
        self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        # DELTA archive backup
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        self.merge_backup(backup_dir, 'node', backup_id=backup_id)

        self.assertEqual(
            'ARCHIVE', self.show_pb(backup_dir, 'node', backup_id)['wal'])

        # DELTA stream backup
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta', options=['--stream'])

        self.merge_backup(backup_dir, 'node', backup_id=backup_id)

        self.assertEqual(
            'STREAM', self.show_pb(backup_dir, 'node', backup_id)['wal'])

        self.del_test_dir(module_name, fname)

    def test_crash_after_opening_backup_control_1(self):
        """
        check that crashing after opening backup.control
        for writing will not result in losing backup metadata
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # FULL stream backup
        self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        # DELTA archive backup
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        print(self.show_pb(
            backup_dir, 'node', as_json=False, as_text=True))

        gdb = self.merge_backup(backup_dir, "node", backup_id, gdb=True)
        gdb.set_breakpoint('write_backup_filelist')
        gdb.run_until_break()

        gdb.set_breakpoint('write_backup')
        gdb.continue_execution_until_break()
        gdb.set_breakpoint('pgBackupWriteControl')
        gdb.continue_execution_until_break()

        gdb._execute('signal SIGKILL')

        print(self.show_pb(
            backup_dir, 'node', as_json=False, as_text=True))

        self.assertEqual(
            'MERGING', self.show_pb(backup_dir, 'node')[0]['status'])

        self.assertEqual(
            'MERGING', self.show_pb(backup_dir, 'node')[1]['status'])

        self.del_test_dir(module_name, fname)

    @unittest.skip("skip")
    def test_crash_after_opening_backup_control_2(self):
        """
        check that crashing after opening backup_content.control
        for writing will not result in losing metadata about backup files
        TODO: rewrite
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # Add data
        node.pgbench_init(scale=3)

        # FULL backup
        full_id = self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

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
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        pgdata = self.pgdata_content(node.data_dir)

        print(self.show_pb(
            backup_dir, 'node', as_json=False, as_text=True))

        gdb = self.merge_backup(backup_dir, "node", backup_id, gdb=True)
        gdb.set_breakpoint('write_backup_filelist')
        gdb.run_until_break()

        gdb.set_breakpoint('sprintf')
        gdb.continue_execution_until_break(1)

        gdb._execute('signal SIGKILL')

        print(self.show_pb(
            backup_dir, 'node', as_json=False, as_text=True))

        self.assertEqual(
            'MERGING', self.show_pb(backup_dir, 'node')[0]['status'])

        self.assertEqual(
            'MERGING', self.show_pb(backup_dir, 'node')[1]['status'])

        # In to_backup drop file that comes from from_backup
        # emulate crash during previous merge
        file_to_remove = os.path.join(
            backup_dir, 'backups',
            'node', full_id, 'database', fsm_path)

        # print(file_to_remove)

        os.remove(file_to_remove)

        # Continue failed merge
        self.merge_backup(backup_dir, "node", backup_id)

        node.cleanup()

        # restore merge backup
        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)

        self.compare_pgdata(pgdata, pgdata_restored)

        self.del_test_dir(module_name, fname)

    @unittest.skip("skip")
    def test_losing_file_after_failed_merge(self):
        """
        check that crashing after opening backup_content.control
        for writing will not result in losing metadata about backup files
        TODO: rewrite
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # Add data
        node.pgbench_init(scale=1)

        # FULL backup
        full_id = self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

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
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        pgdata = self.pgdata_content(node.data_dir)

        print(self.show_pb(
            backup_dir, 'node', as_json=False, as_text=True))

        gdb = self.merge_backup(backup_dir, "node", backup_id, gdb=True)
        gdb.set_breakpoint('write_backup_filelist')
        gdb.run_until_break()

        gdb.set_breakpoint('sprintf')
        gdb.continue_execution_until_break(20)

        gdb._execute('signal SIGKILL')

        print(self.show_pb(
            backup_dir, 'node', as_json=False, as_text=True))

        self.assertEqual(
            'MERGING', self.show_pb(backup_dir, 'node')[0]['status'])

        self.assertEqual(
            'MERGING', self.show_pb(backup_dir, 'node')[1]['status'])

        # In to_backup drop file that comes from from_backup
        # emulate crash during previous merge
        file_to_remove = os.path.join(
            backup_dir, 'backups',
            'node', full_id, 'database', vm_path)

        os.remove(file_to_remove)

        # Try to continue failed MERGE
        self.merge_backup(backup_dir, "node", backup_id)

        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'node')[0]['status'])

        node.cleanup()

        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        self.del_test_dir(module_name, fname)

    def test_failed_merge_after_delete(self):
        """
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # add database
        node.safe_psql(
            'postgres',
            'CREATE DATABASE testdb')

        dboid = node.safe_psql(
            "postgres",
            "select oid from pg_database where datname = 'testdb'").decode('utf-8').rstrip()

        # take FULL backup
        full_id = self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        # drop database
        node.safe_psql(
            'postgres',
            'DROP DATABASE testdb')

        # take PAGE backup
        page_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        page_id_2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        gdb = self.merge_backup(
            backup_dir, 'node', page_id,
            gdb=True, options=['--log-level-console=verbose'])

        gdb.set_breakpoint('delete_backup_files')
        gdb.run_until_break()

        gdb.set_breakpoint('pgFileDelete')
        gdb.continue_execution_until_break(20)

        gdb._execute('signal SIGKILL')

        # backup half-merged
        self.assertEqual(
            'MERGED', self.show_pb(backup_dir, 'node')[0]['status'])

        self.assertEqual(
            full_id, self.show_pb(backup_dir, 'node')[0]['id'])

        db_path = os.path.join(
            backup_dir, 'backups', 'node',
            full_id, 'database', 'base', dboid)

        try:
            self.merge_backup(
                backup_dir, 'node', page_id_2,
                options=['--log-level-console=verbose'])
            self.assertEqual(
                    1, 0,
                    "Expecting Error because of missing parent.\n "
                    "Output: {0} \n CMD: {1}".format(
                        repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                "ERROR: Full backup {0} has unfinished merge with backup {1}".format(
                    full_id, page_id) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.del_test_dir(module_name, fname)

    def test_failed_merge_after_delete_1(self):
        """
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # take FULL backup
        full_id = self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        node.pgbench_init(scale=1)

        page_1 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Change PAGE1 backup status to ERROR
        self.change_backup_status(backup_dir, 'node', page_1, 'ERROR')

        pgdata = self.pgdata_content(node.data_dir)

        # add data
        pgbench = node.pgbench(options=['-T', '10', '-c', '2', '--no-vacuum'])
        pgbench.wait()

        # take PAGE2 backup
        page_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Change PAGE1 backup status to OK
        self.change_backup_status(backup_dir, 'node', page_1, 'OK')

        gdb = self.merge_backup(
            backup_dir, 'node', page_id,
            gdb=True, options=['--log-level-console=verbose'])

        gdb.set_breakpoint('delete_backup_files')
        gdb.run_until_break()

#        gdb.set_breakpoint('parray_bsearch')
#        gdb.continue_execution_until_break()

        gdb.set_breakpoint('pgFileDelete')
        gdb.continue_execution_until_break(30)
        gdb._execute('signal SIGKILL')

        self.assertEqual(
            full_id, self.show_pb(backup_dir, 'node')[0]['id'])

        # restore
        node.cleanup()
        try:
            #self.restore_node(backup_dir, 'node', node, backup_id=page_1)
            self.restore_node(backup_dir, 'node', node)
            self.assertEqual(
                    1, 0,
                    "Expecting Error because of orphan status.\n "
                    "Output: {0} \n CMD: {1}".format(
                        repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Backup {0} is orphan".format(page_1),
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.del_test_dir(module_name, fname)

    def test_failed_merge_after_delete_2(self):
        """
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # take FULL backup
        full_id = self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        node.pgbench_init(scale=1)

        page_1 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # add data
        pgbench = node.pgbench(options=['-T', '10', '-c', '2', '--no-vacuum'])
        pgbench.wait()

        # take PAGE2 backup
        page_2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        gdb = self.merge_backup(
            backup_dir, 'node', page_2, gdb=True,
            options=['--log-level-console=VERBOSE'])

        gdb.set_breakpoint('pgFileDelete')
        gdb.run_until_break()
        gdb.continue_execution_until_break(2)
        gdb._execute('signal SIGKILL')

        self.delete_pb(backup_dir, 'node', backup_id=page_2)

        # rerun merge
        try:
            #self.restore_node(backup_dir, 'node', node, backup_id=page_1)
            self.merge_backup(backup_dir, 'node', page_1)
            self.assertEqual(
                    1, 0,
                    "Expecting Error because of backup is missing.\n "
                    "Output: {0} \n CMD: {1}".format(
                        repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Full backup {0} has unfinished merge "
                "with backup {1}".format(full_id, page_2),
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.del_test_dir(module_name, fname)

    def test_failed_merge_after_delete_3(self):
        """
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # add database
        node.safe_psql(
            'postgres',
            'CREATE DATABASE testdb')

        dboid = node.safe_psql(
            "postgres",
            "select oid from pg_database where datname = 'testdb'").rstrip()

        # take FULL backup
        full_id = self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        # drop database
        node.safe_psql(
            'postgres',
            'DROP DATABASE testdb')

        # take PAGE backup
        page_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # create database
        node.safe_psql(
            'postgres',
            'create DATABASE testdb')

        page_id_2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        gdb = self.merge_backup(
            backup_dir, 'node', page_id,
            gdb=True, options=['--log-level-console=verbose'])

        gdb.set_breakpoint('delete_backup_files')
        gdb.run_until_break()

        gdb.set_breakpoint('pgFileDelete')
        gdb.continue_execution_until_break(20)

        gdb._execute('signal SIGKILL')

        # backup half-merged
        self.assertEqual(
            'MERGED', self.show_pb(backup_dir, 'node')[0]['status'])

        self.assertEqual(
            full_id, self.show_pb(backup_dir, 'node')[0]['id'])

        db_path = os.path.join(
            backup_dir, 'backups', 'node', full_id)

        # FULL backup is missing now
        shutil.rmtree(db_path)

        try:
            self.merge_backup(
                backup_dir, 'node', page_id_2,
                options=['--log-level-console=verbose'])
            self.assertEqual(
                    1, 0,
                    "Expecting Error because of missing parent.\n "
                    "Output: {0} \n CMD: {1}".format(
                        repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                "ERROR: Failed to find parent full backup for {0}".format(
                    page_id_2) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_merge_backup_from_future(self):
        """
        take FULL backup, table PAGE backup from future,
        try to merge page with FULL
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # Take FULL
        self.backup_node(backup_dir, 'node', node)

        node.pgbench_init(scale=5)

        # Take PAGE from future
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        with open(
                os.path.join(
                    backup_dir, 'backups', 'node',
                    backup_id, "backup.control"), "a") as conf:
            conf.write("start-time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                datetime.now() + timedelta(days=3)))

        # rename directory
        new_id = self.show_pb(backup_dir, 'node')[1]['id']

        os.rename(
            os.path.join(backup_dir, 'backups', 'node', backup_id),
            os.path.join(backup_dir, 'backups', 'node', new_id))

        pgbench = node.pgbench(options=['-T', '5', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        backup_id = self.backup_node(backup_dir, 'node', node, backup_type='page')
        pgdata = self.pgdata_content(node.data_dir)

        result = node.safe_psql(
            'postgres',
            'SELECT * from pgbench_accounts')

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node',
            node_restored, backup_id=backup_id)

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # check that merged backup has the same state as
        node_restored.cleanup()
        self.merge_backup(backup_dir, 'node', backup_id=backup_id)
        self.restore_node(
            backup_dir, 'node',
            node_restored, backup_id=backup_id)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)

        self.set_auto_conf(
            node_restored,
            {'port': node_restored.port})
        node_restored.slow_start()

        result_new = node_restored.safe_psql(
            'postgres',
            'SELECT * from pgbench_accounts')

        self.assertTrue(result, result_new)

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_merge_multiple_descendants(self):
        """
        PAGEb3
          |                 PAGEa3
        PAGEb2               /
          |       PAGEa2    /
        PAGEb1       \     /
          |           PAGEa1
        FULLb           |
                      FULLa
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # Take FULL BACKUPs
        backup_id_a = self.backup_node(backup_dir, 'node', node)

        backup_id_b = self.backup_node(backup_dir, 'node', node)

        # Change FULLb backup status to ERROR
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'ERROR')

        page_id_a1 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Change FULLb backup status to OK
        self.change_backup_status(backup_dir, 'node', backup_id_b, 'OK')

        # Change PAGEa1 backup status to ERROR
        self.change_backup_status(backup_dir, 'node', page_id_a1, 'ERROR')

        # PAGEa1 ERROR
        # FULLb  OK
        # FULLa  OK

        page_id_b1 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

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

        page_id_a2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

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

        page_id_b2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

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

        page_id_a3 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

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

        page_id_b3 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

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
            self.show_pb(backup_dir, 'node', backup_id=page_id_a3)['parent-backup-id'],
            page_id_a1)

        self.assertEqual(
            self.show_pb(backup_dir, 'node', backup_id=page_id_a2)['parent-backup-id'],
            page_id_a1)

        self.merge_backup(
            backup_dir, 'node', page_id_a2,
            options=['--merge-expired', '--log-level-console=log'])

        try:
            self.merge_backup(
                backup_dir, 'node', page_id_a3,
                options=['--merge-expired', '--log-level-console=log'])
            self.assertEqual(
                1, 0,
                "Expecting Error because of parent FULL backup is missing.\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                "ERROR: Failed to find parent full backup for {0}".format(
                    page_id_a3) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname, [node])

    # @unittest.skip("skip")
    def test_smart_merge(self):
        """
        make node, create database, take full backup, drop database,
        take PAGE backup and merge it into FULL,
        make sure that files from dropped database are not
        copied during restore
        https://github.com/postgrespro/pg_probackup/issues/63
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # create database
        node.safe_psql(
            "postgres",
            "CREATE DATABASE testdb")

        # take FULL backup
        full_id = self.backup_node(backup_dir, 'node', node)

        # drop database
        node.safe_psql(
            "postgres",
            "DROP DATABASE testdb")

        # take PAGE backup
        page_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # get delta between FULL and PAGE filelists
        filelist_full = self.get_backup_filelist(
            backup_dir, 'node', full_id)

        filelist_page = self.get_backup_filelist(
            backup_dir, 'node', page_id)

        filelist_diff = self.get_backup_filelist_diff(
            filelist_full, filelist_page)

        # merge PAGE backup
        self.merge_backup(
            backup_dir, 'node', page_id,
            options=['--log-level-file=VERBOSE'])

        logfile = os.path.join(backup_dir, 'log', 'pg_probackup.log')
        with open(logfile, 'r') as f:
                logfile_content = f.read()

        # Clean after yourself
        self.del_test_dir(module_name, fname, [node])

    def test_idempotent_merge(self):
        """
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # add database
        node.safe_psql(
            'postgres',
            'CREATE DATABASE testdb')

        # take FULL backup
        full_id = self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        # create database
        node.safe_psql(
            'postgres',
            'create DATABASE testdb1')

        # take PAGE backup
        page_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # create database
        node.safe_psql(
            'postgres',
            'create DATABASE testdb2')

        page_id_2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        gdb = self.merge_backup(
            backup_dir, 'node', page_id_2,
            gdb=True, options=['--log-level-console=verbose'])

        gdb.set_breakpoint('delete_backup_files')
        gdb.run_until_break()
        gdb.remove_all_breakpoints()

        gdb.set_breakpoint('rename')
        gdb.continue_execution_until_break()
        gdb.continue_execution_until_break(2)

        gdb._execute('signal SIGKILL')

        show_backups = self.show_pb(backup_dir, "node")
        self.assertEqual(len(show_backups), 1)

        self.assertEqual(
            'MERGED', self.show_pb(backup_dir, 'node')[0]['status'])

        self.assertEqual(
            full_id, self.show_pb(backup_dir, 'node')[0]['id'])

        self.merge_backup(backup_dir, 'node', page_id_2)

        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'node')[0]['status'])

        self.assertEqual(
            page_id_2, self.show_pb(backup_dir, 'node')[0]['id'])

        self.del_test_dir(module_name, fname, [node])

    def test_merge_correct_inheritance(self):
        """
        Make sure that backup metainformation fields
        'note' and 'expire-time' are correctly inherited
        during merge
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # add database
        node.safe_psql(
            'postgres',
            'CREATE DATABASE testdb')

        # take FULL backup
        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        # create database
        node.safe_psql(
            'postgres',
            'create DATABASE testdb1')

        # take PAGE backup
        page_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        self.set_backup(
            backup_dir, 'node', page_id, options=['--note=hello', '--ttl=20d'])

        page_meta = self.show_pb(backup_dir, 'node', page_id)

        self.merge_backup(backup_dir, 'node', page_id)

        print(self.show_pb(backup_dir, 'node', page_id))

        self.assertEqual(
            page_meta['note'],
            self.show_pb(backup_dir, 'node', page_id)['note'])

        self.assertEqual(
            page_meta['expire-time'],
            self.show_pb(backup_dir, 'node', page_id)['expire-time'])

        self.del_test_dir(module_name, fname, [node])

    def test_merge_correct_inheritance_1(self):
        """
        Make sure that backup metainformation fields
        'note' and 'expire-time' are correctly inherited
        during merge
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # add database
        node.safe_psql(
            'postgres',
            'CREATE DATABASE testdb')

        # take FULL backup
        self.backup_node(
            backup_dir, 'node', node,
            options=['--stream', '--note=hello', '--ttl=20d'])

        # create database
        node.safe_psql(
            'postgres',
            'create DATABASE testdb1')

        # take PAGE backup
        page_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        self.merge_backup(backup_dir, 'node', page_id)

        self.assertNotIn(
            'note',
            self.show_pb(backup_dir, 'node', page_id))

        self.assertNotIn(
            'expire-time',
            self.show_pb(backup_dir, 'node', page_id))

        self.del_test_dir(module_name, fname, [node])

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
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
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
        full_id = self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        self.backup_node(backup_dir, 'node', node, backup_type='delta')

        node.cleanup()
        self.restore_node(
            backup_dir, 'node', node, backup_id=full_id,
            options=[
                '--recovery-target=immediate',
                '--recovery-target-action=promote'])

        node.slow_start()

        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # create timelines
        for i in range(2, 7):
            node.cleanup()
            self.restore_node(
                backup_dir, 'node', node,
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
                self.backup_node(backup_dir, 'node', node, backup_type='page')

        page_id = self.backup_node(backup_dir, 'node', node, backup_type='page')
        pgdata = self.pgdata_content(node.data_dir)

        self.merge_backup(backup_dir, 'node', page_id)

        result = node.safe_psql(
            "postgres", "select * from pgbench_accounts")

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(backup_dir, 'node', node_restored)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)

        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()

        result_new = node_restored.safe_psql(
            "postgres", "select * from pgbench_accounts")

        self.assertEqual(result, result_new)

        self.compare_pgdata(pgdata, pgdata_restored)

        self.checkdb_node(
            backup_dir,
            'node',
            options=[
                '--amcheck',
                '-d', 'postgres', '-p', str(node.port)])

        self.checkdb_node(
            backup_dir,
            'node',
            options=[
                '--amcheck',
                '-d', 'postgres', '-p', str(node_restored.port)])

        # Clean after yourself
        self.del_test_dir(module_name, fname, [node, node_restored])

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_merge_page_header_map_retry(self):
        """
        page header map cannot be trusted when
        running retry
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=20)
        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        delta_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta', options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        gdb = self.merge_backup(backup_dir, 'node', delta_id, gdb=True)

        # our goal here is to get full backup with merged data files,
        # but with old page header map
        gdb.set_breakpoint('cleanup_header_map')
        gdb.run_until_break()
        gdb._execute('signal SIGKILL')

        self.merge_backup(backup_dir, 'node', delta_id)

        node.cleanup()

        self.restore_node(backup_dir, 'node', node)
        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_missing_data_file(self):
        """
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # Add data
        node.pgbench_init(scale=1)

        # FULL backup
        self.backup_node(backup_dir, 'node', node)

        # Change data
        pgbench = node.pgbench(options=['-T', '5', '-c', '1'])
        pgbench.wait()

        # DELTA backup
        delta_id = self.backup_node(backup_dir, 'node', node, backup_type='delta')

        path = node.safe_psql(
            'postgres',
            "select pg_relation_filepath('pgbench_accounts')").decode('utf-8').rstrip()

        gdb = self.merge_backup(
            backup_dir, "node", delta_id,
            options=['--log-level-file=VERBOSE'], gdb=True)
        gdb.set_breakpoint('merge_files')
        gdb.run_until_break()

        # remove data file in incremental backup
        file_to_remove = os.path.join(
            backup_dir, 'backups',
            'node', delta_id, 'database', path)

        os.remove(file_to_remove)

        gdb.continue_execution_until_error()

        logfile = os.path.join(backup_dir, 'log', 'pg_probackup.log')
        with open(logfile, 'r') as f:
                logfile_content = f.read()

        self.assertIn(
            'ERROR: Cannot open backup file "{0}": No such file or directory'.format(file_to_remove),
            logfile_content)

        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_missing_non_data_file(self):
        """
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # FULL backup
        self.backup_node(backup_dir, 'node', node)

        # DELTA backup
        delta_id = self.backup_node(backup_dir, 'node', node, backup_type='delta')

        gdb = self.merge_backup(
            backup_dir, "node", delta_id,
            options=['--log-level-file=VERBOSE'], gdb=True)
        gdb.set_breakpoint('merge_files')
        gdb.run_until_break()

        # remove data file in incremental backup
        file_to_remove = os.path.join(
            backup_dir, 'backups',
            'node', delta_id, 'database', 'backup_label')

        os.remove(file_to_remove)

        gdb.continue_execution_until_error()

        logfile = os.path.join(backup_dir, 'log', 'pg_probackup.log')
        with open(logfile, 'r') as f:
                logfile_content = f.read()

        self.assertIn(
            'ERROR: File "{0}" is not found'.format(file_to_remove),
            logfile_content)

        self.assertIn(
            'ERROR: Backup files merging failed',
            logfile_content)

        self.assertEqual(
            'MERGING', self.show_pb(backup_dir, 'node')[0]['status'])

        self.assertEqual(
            'MERGING', self.show_pb(backup_dir, 'node')[1]['status'])

        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_merge_remote_mode(self):
        """
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # FULL backup
        full_id = self.backup_node(backup_dir, 'node', node)

        # DELTA backup
        delta_id = self.backup_node(backup_dir, 'node', node, backup_type='delta')

        self.set_config(backup_dir, 'node', options=['--retention-window=1'])

        backups = os.path.join(backup_dir, 'backups', 'node')
        with open(
                os.path.join(
                    backups, full_id, "backup.control"), "a") as conf:
            conf.write("recovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                datetime.now() - timedelta(days=5)))

        gdb = self.backup_node(
            backup_dir, "node", node,
            options=['--log-level-file=VERBOSE', '--merge-expired'], gdb=True)
        gdb.set_breakpoint('merge_files')
        gdb.run_until_break()

        logfile = os.path.join(backup_dir, 'log', 'pg_probackup.log')

        with open(logfile, "w+") as f:
            f.truncate()

        gdb.continue_execution_until_exit()

        logfile = os.path.join(backup_dir, 'log', 'pg_probackup.log')
        with open(logfile, 'r') as f:
                logfile_content = f.read()

        self.assertNotIn(
            'SSH', logfile_content)

        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'node')[0]['status'])

        self.del_test_dir(module_name, fname)

# 1. Need new test with corrupted FULL backup
# 2. different compression levels
