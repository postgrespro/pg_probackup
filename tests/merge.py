# coding: utf-8

import unittest
import os
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
import shutil
from datetime import datetime, timedelta

module_name = "merge"


class MergeTest(ProbackupTest, unittest.TestCase):

    def test_merge_full_page(self):
        """
        Test MERGE command, it merges FULL backup with target PAGE backups
        """
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, "backup")

        # Initialize instance and backup directory
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=["--data-checksums"]
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # Do full backup
        self.backup_node(backup_dir, "node", node)
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
        self.backup_node(backup_dir, "node", node, backup_type="page")
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
        self.backup_node(backup_dir, "node", node, backup_type="page")
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
        node.cleanup()
        self.del_test_dir(module_name, fname)

    def test_merge_compressed_backups(self):
        """
        Test MERGE command with compressed backups
        """
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, "backup")

        # Initialize instance and backup directory
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=["--data-checksums"]
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # Do full compressed backup
        self.backup_node(backup_dir, "node", node, options=[
            '--compress-algorithm=zlib'])
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
            backup_dir, "node", node, backup_type="page",
            options=['--compress-algorithm=zlib'])
        show_backup = self.show_pb(backup_dir, "node")[1]
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
            initdb_params=["--data-checksums"],
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
        pgbench = node.pgbench(options=['-T', '20', '-c', '2', '--no-vacuum'])
        pgbench.wait()

        # Do compressed DELTA backup
        self.backup_node(
            backup_dir, "node", node, backup_type="delta",
            options=['--compress-algorithm=zlib', '--stream'])

        # Change data
        pgbench = node.pgbench(options=['-T', '20', '-c', '2', '--no-vacuum'])
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

    def test_merge_compressed_and_uncompressed_backups(self):
        """
        Test MERGE command with compressed and uncompressed backups
        """
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, "backup")

        # Initialize instance and backup directory
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=["--data-checksums"],
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
        pgbench = node.pgbench(options=['-T', '20', '-c', '2', '--no-vacuum'])
        pgbench.wait()

        # Do compressed DELTA backup
        self.backup_node(
            backup_dir, "node", node, backup_type="delta",
            options=['--compress-algorithm=zlib', '--stream'])

        # Change data
        pgbench = node.pgbench(options=['-T', '20', '-c', '2', '--no-vacuum'])
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

    def test_merge_compressed_and_uncompressed_backups_1(self):
        """
        Test MERGE command with compressed and uncompressed backups
        """
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, "backup")

        # Initialize instance and backup directory
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=["--data-checksums"],
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
        pgbench = node.pgbench(options=['-T', '20', '-c', '2', '--no-vacuum'])
        pgbench.wait()

        # Do uncompressed DELTA backup
        self.backup_node(
            backup_dir, "node", node, backup_type="delta",
            options=['--stream'])

        # Change data
        pgbench = node.pgbench(options=['-T', '20', '-c', '2', '--no-vacuum'])
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
            initdb_params=["--data-checksums"],
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

        # Do uncompressed FULL backup
        self.backup_node(backup_dir, "node", node)
        show_backup = self.show_pb(backup_dir, "node")[0]

        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "FULL")

        # Change data
        pgbench = node.pgbench(options=['-T', '20', '-c', '2', '--no-vacuum'])
        pgbench.wait()

        # Do compressed DELTA backup
        self.backup_node(
            backup_dir, "node", node, backup_type="delta",
            options=['--compress-algorithm=zlib', '--stream'])

        # Change data
        pgbench = node.pgbench(options=['-T', '20', '-c', '2', '--no-vacuum'])
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
                'wal_level': 'replica',
                'max_wal_senders': '2',
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
                'wal_level': 'replica',
                'max_wal_senders': '2',
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
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'checkpoint_timeout': '300s',
                'autovacuum': 'off'
            }
        )
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
                "-T", "{0}={1}".format(old_tablespace, new_tablespace),
                "--recovery-target-action=promote"])

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        node_restored.append_conf(
            "postgresql.auto.conf", "port = {0}".format(node_restored.port))
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
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'checkpoint_timeout': '300s',
                'autovacuum': 'off'
            }
        )
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
                "-T", "{0}={1}".format(old_tablespace, new_tablespace),
                "--recovery-target-action=promote"])

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        node_restored.append_conf(
            "postgresql.auto.conf", "port = {0}".format(node_restored.port))
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
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'checkpoint_timeout': '300s',
                'autovacuum': 'off',
                'ptrack_enable': 'on'
            }
        )
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

        page_id = self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.merge_backup(backup_dir, "node", page_id)

        self.validate_pb(backup_dir)

        old_tablespace = self.get_tblspace_path(node, 'somedata')
        new_tablespace = self.get_tblspace_path(node_restored, 'somedata_new')

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=[
                "-j", "4",
                "-T", "{0}={1}".format(old_tablespace, new_tablespace),
                "--recovery-target-action=promote"])

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        node_restored.append_conf(
            "postgresql.auto.conf", "port = {0}".format(node_restored.port))
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
                'wal_level': 'replica',
                'max_wal_senders': '2',
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
        node_restored.append_conf(
            'postgresql.auto.conf', 'port = {0}'.format(node_restored.port))
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
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True, initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica'
            }
        )

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

        gdb.set_breakpoint('copy_file')
        gdb.run_until_break()

        gdb.continue_execution_until_break(20)

        gdb._execute('signal SIGKILL')

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
            set_replication=True, initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica'
            }
        )

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

        old_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").rstrip()

        # DELTA BACKUP
        self.backup_node(
            backup_dir, 'node', node, backup_type='delta'
        )

        node.safe_psql(
            "postgres",
            "update t_heap set id = 100500"
        )

        node.safe_psql(
            "postgres",
            "vacuum full t_heap"
        )

        new_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").rstrip()

        # DELTA BACKUP
        backup_id_2 = self.backup_node(
            backup_dir, 'node', node, backup_type='delta'
        )

        backup_id = self.show_pb(backup_dir, "node")[1]["id"]

        # Failed MERGE
        gdb = self.merge_backup(backup_dir, "node", backup_id, gdb=True)
        gdb.set_breakpoint('copy_file')
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
            self.merge_backup(backup_dir, "node", backup_id)
            self.assertEqual(
                1, 0,
                "Expecting Error because of incremental backup corruption.\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                "ERROR: Merging of backup {0} failed".format(
                    backup_id) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_continue_failed_merge_2(self):
        """
        Check that failed MERGE on delete can`t be continued
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True, initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica'
            }
        )

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

        gdb.continue_execution_until_break(20)

        gdb._execute('signal SIGKILL')

        print(self.show_pb(backup_dir, as_text=True, as_json=False))

        backup_id_deleted = self.show_pb(backup_dir, "node")[1]["id"]

        # Try to continue failed MERGE
        try:
            self.merge_backup(backup_dir, "node", backup_id)
            self.assertEqual(
                1, 0,
                "Expecting Error because of backup corruption.\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                "ERROR: Backup {0} has status: DELETING".format(
                    backup_id_deleted) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_continue_failed_merge_3(self):
        """
        Check that failed MERGE can`t be continued after target backup deleting
        Create FULL and 2 PAGE backups
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

        gdb.set_breakpoint('copy_file')
        gdb.run_until_break()
        gdb.continue_execution_until_break(2)

        gdb._execute('signal SIGKILL')

        print(self.show_pb(backup_dir, as_text=True, as_json=False))
        print(os.path.join(backup_dir, "backups", "node", backup_id_delete))

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
                "ERROR: Parent full backup for the given backup {0} was not found".format(
                    backup_id_merge) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_merge_different_compression_algo(self):
        """
        Check that backups with different compression algorihtms can be merged
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'})

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

    def test_crash_after_opening_backup_control_2(self):
        """
        check that crashing after opening backup_content.control
        for writing will not result in losing metadata about backup files
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
            "select pg_relation_filepath('pgbench_accounts')").rstrip()

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

        gdb.set_breakpoint('print_file_list')
        gdb.continue_execution_until_break()

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

        print(file_to_remove)

        os.remove(file_to_remove)

        # Continue failed merge
        self.merge_backup(backup_dir, "node", backup_id)

        node.cleanup()

        # restore merge backup
        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)

        self.compare_pgdata(pgdata, pgdata_restored)

        self.del_test_dir(module_name, fname)

    def test_losing_file_after_failed_merge(self):
        """
        check that crashing after opening backup_content.control
        for writing will not result in losing metadata about backup files
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

        # Add data
        node.pgbench_init(scale=3)

        # FULL backup
        full_id = self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        # Change data
        pgbench = node.pgbench(options=['-T', '20', '-c', '2', '--no-vacuum'])
        pgbench.wait()

        path = node.safe_psql(
            'postgres',
            "select pg_relation_filepath('pgbench_accounts')").rstrip()

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

        gdb.set_breakpoint('print_file_list')
        gdb.continue_execution_until_break()

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

        print(file_to_remove)

        os.remove(file_to_remove)

        # Try to continue failed MERGE
        try:
            self.merge_backup(backup_dir, "node", backup_id)
            self.assertEqual(
                1, 0,
                "Expecting Error because of backup corruption.\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                "ERROR: Merging of backup {0} failed".format(
                    backup_id) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertEqual(
            'CORRUPT', self.show_pb(backup_dir, 'node')[0]['status'])

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
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'autovacuum': 'off'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # Take FULL
        self.backup_node(backup_dir, 'node', node)

        node.pgbench_init(scale=3)

        # Take PAGE from future
        backup_id = self.backup_node(backup_dir, 'node', node, backup_type='page')

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

        pgbench = node.pgbench(options=['-T', '3', '-c', '2', '--no-vacuum'])
        pgbench.wait()

        backup_id = self.backup_node(backup_dir, 'node', node, backup_type='page')
        pgdata = self.pgdata_content(node.data_dir)

        node.cleanup()
        self.merge_backup(backup_dir, 'node', backup_id=backup_id)

        self.restore_node(backup_dir, 'node', node, backup_id=backup_id)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

# 1. always use parent link when merging (intermediates may be from different chain)
# 2. page backup we are merging with may disappear after failed merge,
# it should not be possible to continue merge after that
#   PAGE_A  MERGING (disappear)
#   FULL    MERGING

#   FULL    MERGING

#   PAGE_B  OK      (new backup)
#   FULL    MERGING

# 3. Need new test with corrupted FULL backup
# 4. different compression levels
