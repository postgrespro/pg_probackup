import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from datetime import datetime, timedelta
import subprocess

module_name = 'page'


class PageBackupTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    def test_page_vacuum_truncate(self):
        """
        make node, create table, take full backup,
        delete last 3 pages, vacuum relation,
        take page backup, take second page backup,
        restore last page backup and check data correctness
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
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
            base_dir="{0}/{1}/node_restored".format(module_name, fname))

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node_restored.cleanup()
        node.start()
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
            backup_dir, 'node', node, backup_type='page',
            options=['--log-level-file=verbose'])

        self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

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
    def test_page_stream(self):
        """
        make archive node, take full and page stream backups,
        restore them and check data correctness
        """
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'checkpoint_timeout': '30s'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(0,100) i")

        full_result = node.execute("postgres", "SELECT * FROM t_heap")
        full_backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='full', options=['--stream'])

        # PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(100,200) i")
        page_result = node.execute("postgres", "SELECT * FROM t_heap")
        page_backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', options=['--stream'])

        # Drop Node
        node.cleanup()

        # Check full backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(
                backup_dir, 'node', node,
                backup_id=full_backup_id, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n'
            ' CMD: {1}'.format(repr(self.output), self.cmd))
        node.slow_start()
        full_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check page backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(page_backup_id),
            self.restore_node(
                backup_dir, 'node', node,
                backup_id=page_backup_id, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n'
            ' CMD: {1}'.format(repr(self.output), self.cmd))
        node.slow_start()
        page_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(page_result, page_result_new)
        node.cleanup()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_page_archive(self):
        """
        make archive node, take full and page archive backups,
        restore them and check data correctness
        """
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'checkpoint_timeout': '30s'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,1) i")
        full_result = node.execute("postgres", "SELECT * FROM t_heap")
        full_backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='full')

        # PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, "
            "md5(i::text) as text, md5(i::text)::tsvector as tsvector "
            "from generate_series(0,2) i")
        page_result = node.execute("postgres", "SELECT * FROM t_heap")
        page_backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Drop Node
        node.cleanup()

        # Restore and check full backup
        self.assertIn("INFO: Restore of backup {0} completed.".format(
            full_backup_id),
            self.restore_node(
                backup_dir, 'node', node,
                backup_id=full_backup_id,
                options=[
                    "-j", "4",
                    "--immediate",
                    "--recovery-target-action=promote"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                repr(self.output), self.cmd))
        node.slow_start()

        full_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Restore and check page backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(page_backup_id),
            self.restore_node(
                backup_dir, 'node', node,
                backup_id=page_backup_id,
                options=[
                    "-j", "4",
                    "--immediate",
                    "--recovery-target-action=promote"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                repr(self.output), self.cmd))
        node.slow_start()

        page_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(page_result, page_result_new)
        node.cleanup()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_page_multiple_segments(self):
        """
        Make node, create table with multiple segments,
        write some data to it, check page and data correctness
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'fsync': 'off',
                'shared_buffers': '1GB',
                'maintenance_work_mem': '1GB',
                'autovacuum': 'off',
                'full_page_writes': 'off'
                }
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        self.create_tblspace_in_node(node, 'somedata')

        # CREATE TABLE
        node.pgbench_init(scale=100, options=['--tablespace=somedata'])
        # FULL BACKUP
        self.backup_node(backup_dir, 'node', node)

        # PGBENCH STUFF
        pgbench = node.pgbench(options=['-T', '50', '-c', '1', '--no-vacuum'])
        pgbench.wait()
        node.safe_psql("postgres", "checkpoint")

        # GET LOGICAL CONTENT FROM NODE
        result = node.safe_psql("postgres", "select * from pgbench_accounts")
        # PAGE BACKUP
        self.backup_node(
            backup_dir, 'node', node, backup_type='page',
            options=["--log-level-file=verbose"])
        # GET PHYSICAL CONTENT FROM NODE
        pgdata = self.pgdata_content(node.data_dir)

        # RESTORE NODE
        restored_node = self.make_simple_node(
            base_dir="{0}/{1}/restored_node".format(module_name, fname))
        restored_node.cleanup()
        tblspc_path = self.get_tblspace_path(node, 'somedata')
        tblspc_path_new = self.get_tblspace_path(
            restored_node, 'somedata_restored')

        self.restore_node(
            backup_dir, 'node', restored_node,
            options=[
                "-j", "4",
                "--recovery-target-action=promote",
                "-T", "{0}={1}".format(tblspc_path, tblspc_path_new)])

        # GET PHYSICAL CONTENT FROM NODE_RESTORED
        pgdata_restored = self.pgdata_content(restored_node.data_dir)

        # START RESTORED NODE
        restored_node.append_conf(
            "postgresql.auto.conf", "port = {0}".format(restored_node.port))
        restored_node.slow_start()

        result_new = restored_node.safe_psql(
            "postgres", "select * from pgbench_accounts")

        # COMPARE RESTORED FILES
        self.assertEqual(result, result_new, 'data is lost')

        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_page_delete(self):
        """
        Make node, create tablespace with table, take full backup,
        delete everything from table, vacuum table, take page backup,
        restore page backup, compare .
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
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
        node.start()

        self.create_tblspace_in_node(node, 'somedata')
        # FULL backup
        self.backup_node(backup_dir, 'node', node)
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

        # PAGE BACKUP
        self.backup_node(
            backup_dir, 'node', node, backup_type='page')
        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # RESTORE
        node_restored = self.make_simple_node(
            base_dir="{0}/{1}/node_restored".format(module_name, fname)
        )
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=[
                "-j", "4",
                "-T", "{0}={1}".format(
                    self.get_tblspace_path(node, 'somedata'),
                    self.get_tblspace_path(node_restored, 'somedata'))
            ]
        )

        # GET RESTORED PGDATA AND COMPARE
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        node_restored.append_conf(
            'postgresql.auto.conf', 'port = {0}'.format(node_restored.port))
        node_restored.start()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_page_delete_1(self):
        """
        Make node, create tablespace with table, take full backup,
        delete everything from table, vacuum table, take page backup,
        restore page backup, compare .
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
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
        node.start()

        self.create_tblspace_in_node(node, 'somedata')

        node.safe_psql(
            "postgres",
            "create table t_heap tablespace somedata as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,100) i"
        )
        # FULL backup
        self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "delete from t_heap"
        )

        node.safe_psql(
            "postgres",
            "vacuum t_heap"
        )

        # PAGE BACKUP
        self.backup_node(
            backup_dir, 'node', node, backup_type='page')
        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # RESTORE
        node_restored = self.make_simple_node(
            base_dir="{0}/{1}/node_restored".format(module_name, fname)
        )
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=[
                "-j", "4",
                "-T", "{0}={1}".format(
                    self.get_tblspace_path(node, 'somedata'),
                    self.get_tblspace_path(node_restored, 'somedata'))
            ]
        )

        # GET RESTORED PGDATA AND COMPARE
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        node_restored.append_conf(
            'postgresql.auto.conf', 'port = {0}'.format(node_restored.port))
        node_restored.start()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_parallel_pagemap(self):
        """
        Test for parallel WAL segments reading, during which pagemap is built
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')

        # Initialize instance and backup directory
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={
                "hot_standby": "on"
            }
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # Do full backup
        self.backup_node(backup_dir, 'node', node)
        show_backup = self.show_pb(backup_dir, 'node')[0]

        self.assertEqual(show_backup['status'], "OK")
        self.assertEqual(show_backup['backup-mode'], "FULL")

        # Fill instance with data and make several WAL segments ...
        node.safe_psql("postgres", "create table test (id int)")
        for x in range(0, 8):
            node.safe_psql(
                "postgres",
                "insert into test select i from generate_series(1,100) s(i)")
            self.switch_wal_segment(node)
        count1 = node.safe_psql("postgres", "select count(*) from test")

        # ... and do page backup with parallel pagemap
        self.backup_node(
            backup_dir, 'node', node, backup_type="page", options=["-j", "4"])
        show_backup = self.show_pb(backup_dir, 'node')[1]

        self.assertEqual(show_backup['status'], "OK")
        self.assertEqual(show_backup['backup-mode'], "PAGE")

        # Drop node and restore it
        node.cleanup()
        self.restore_node(backup_dir, 'node', node)
        node.start()

        # Check restored node
        count2 = node.safe_psql("postgres", "select count(*) from test")

        self.assertEqual(count1, count2)

        # Clean after yourself
        node.cleanup()
        self.del_test_dir(module_name, fname)

    def test_parallel_pagemap_1(self):
        """
        Test for parallel WAL segments reading, during which pagemap is built
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')

        # Initialize instance and backup directory
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={}
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # Do full backup
        self.backup_node(backup_dir, 'node', node)
        show_backup = self.show_pb(backup_dir, 'node')[0]

        self.assertEqual(show_backup['status'], "OK")
        self.assertEqual(show_backup['backup-mode'], "FULL")

        # Fill instance with data and make several WAL segments ...
        node.pgbench_init(scale=10)

        # do page backup in single thread
        page_id = self.backup_node(
            backup_dir, 'node', node, backup_type="page")

        self.delete_pb(backup_dir, 'node', page_id)

        # ... and do page backup with parallel pagemap
        self.backup_node(
            backup_dir, 'node', node, backup_type="page", options=["-j", "4"])
        show_backup = self.show_pb(backup_dir, 'node')[1]

        self.assertEqual(show_backup['status'], "OK")
        self.assertEqual(show_backup['backup-mode'], "PAGE")

        # Drop node and restore it
        node.cleanup()
        self.restore_node(backup_dir, 'node', node)
        node.start()

        # Clean after yourself
        node.cleanup()
        self.del_test_dir(module_name, fname)
