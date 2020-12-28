import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from testgres import QueryException
from datetime import datetime, timedelta
import subprocess
import gzip
import shutil

module_name = 'page'


class PageTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    def test_basic_page_vacuum_truncate(self):
        """
        make node, create table, take full backup,
        delete last 3 pages, vacuum relation,
        take page backup, take second page backup,
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

        # TODO: make it dynamic
        node.safe_psql(
            "postgres",
            "delete from t_heap where ctid >= '(11,0)'")
        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.backup_node(
            backup_dir, 'node', node, backup_type='page')

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
        self.del_test_dir(module_name, fname, [node, node_restored])

    # @unittest.skip("skip")
    def test_page_vacuum_truncate_1(self):
        """
        make node, create table, take full backup,
        delete all data, vacuum relation,
        take page backup, insert some data,
        take second page backup and check data correctness
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

        node.safe_psql(
            "postgres",
            "create sequence t_seq; "
            "create table t_heap as select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1024) i")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "delete from t_heap")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1) i")

        self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(backup_dir, 'node', node_restored)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()

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
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

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
            backup_type='page', options=['--stream', '-j', '4'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

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

        # GET RESTORED PGDATA AND COMPARE
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

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
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,100) i")
        full_result = node.execute("postgres", "SELECT * FROM t_heap")
        full_backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='full')

        # PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, "
            "md5(i::text) as text, md5(i::text)::tsvector as tsvector "
            "from generate_series(100, 200) i")
        page_result = node.execute("postgres", "SELECT * FROM t_heap")
        page_backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', options=["-j", "4"])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

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

         # GET RESTORED PGDATA AND COMPARE
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

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
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'fsync': 'off',
                'shared_buffers': '1GB',
                'maintenance_work_mem': '1GB',
                'autovacuum': 'off',
                'full_page_writes': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'somedata')

        # CREATE TABLE
        node.pgbench_init(scale=100, options=['--tablespace=somedata'])
        # FULL BACKUP
        self.backup_node(backup_dir, 'node', node)

        # PGBENCH STUFF
        pgbench = node.pgbench(options=['-T', '50', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # GET LOGICAL CONTENT FROM NODE
        result = node.safe_psql("postgres", "select count(*) from pgbench_accounts")
        # PAGE BACKUP
        self.backup_node(backup_dir, 'node', node, backup_type='page')

        # GET PHYSICAL CONTENT FROM NODE
        pgdata = self.pgdata_content(node.data_dir)

        # RESTORE NODE
        restored_node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'restored_node'))
        restored_node.cleanup()
        tblspc_path = self.get_tblspace_path(node, 'somedata')
        tblspc_path_new = self.get_tblspace_path(
            restored_node, 'somedata_restored')

        self.restore_node(
            backup_dir, 'node', restored_node,
            options=[
                "-j", "4",
                "-T", "{0}={1}".format(tblspc_path, tblspc_path_new)])

        # GET PHYSICAL CONTENT FROM NODE_RESTORED
        pgdata_restored = self.pgdata_content(restored_node.data_dir)

        # START RESTORED NODE
        self.set_auto_conf(restored_node, {'port': restored_node.port})
        restored_node.slow_start()

        result_new = restored_node.safe_psql(
            "postgres", "select count(*) from pgbench_accounts")

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
        self.backup_node(backup_dir, 'node', node)
        node.safe_psql(
            "postgres",
            "create table t_heap tablespace somedata as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,100) i")

        node.safe_psql(
            "postgres",
            "delete from t_heap")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        # PAGE BACKUP
        self.backup_node(
            backup_dir, 'node', node, backup_type='page')
        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # RESTORE
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
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
        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()

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
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
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
            base_dir=os.path.join(module_name, fname, 'node_restored')
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
        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()

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
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={
                "hot_standby": "on"
            }
        )
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'),
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node_restored.cleanup()
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # Do full backup
        self.backup_node(backup_dir, 'node', node)
        show_backup = self.show_pb(backup_dir, 'node')[0]

        self.assertEqual(show_backup['status'], "OK")
        self.assertEqual(show_backup['backup-mode'], "FULL")

        # Fill instance with data and make several WAL segments ...
        with node.connect() as conn:
            conn.execute("create table test (id int)")
            for x in range(0, 8):
                conn.execute(
                    "insert into test select i from generate_series(1,100) s(i)")
                conn.commit()
                self.switch_wal_segment(conn)
            count1 = conn.execute("select count(*) from test")

        # ... and do page backup with parallel pagemap
        self.backup_node(
            backup_dir, 'node', node, backup_type="page", options=["-j", "4"])
        show_backup = self.show_pb(backup_dir, 'node')[1]

        self.assertEqual(show_backup['status'], "OK")
        self.assertEqual(show_backup['backup-mode'], "PAGE")

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # Restore it
        self.restore_node(backup_dir, 'node', node_restored)

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()

        # Check restored node
        count2 = node_restored.execute("postgres", "select count(*) from test")

        self.assertEqual(count1, count2)

        # Clean after yourself
        node.cleanup()
        node_restored.cleanup()
        self.del_test_dir(module_name, fname)

    def test_parallel_pagemap_1(self):
        """
        Test for parallel WAL segments reading, during which pagemap is built
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')

        # Initialize instance and backup directory
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={}
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

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
        node.slow_start()

        # Clean after yourself
        node.cleanup()
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_page_backup_with_lost_wal_segment(self):
        """
        make node with archiving
        make archive backup, then generate some wals with pgbench,
        delete latest archived wal segment
        run page backup, expecting error because of missing wal segment
        make sure that backup status is 'ERROR'
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

        self.backup_node(backup_dir, 'node', node)

        # make some wals
        node.pgbench_init(scale=3)

        # delete last wal segment
        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(
            wals_dir, f)) and not f.endswith('.backup') and not f.endswith('.part')]
        wals = map(str, wals)
        file = os.path.join(wals_dir, max(wals))
        os.remove(file)
        if self.archive_compress:
            file = file[:-3]

        # Single-thread PAGE backup
        try:
            self.backup_node(
                backup_dir, 'node', node, backup_type='page')
            self.assertEqual(
                1, 0,
                "Expecting Error because of wal segment disappearance.\n "
                "Output: {0} \n CMD: {1}".format(
                    self.output, self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'INFO: Wait for WAL segment' in e.message and
                'to be archived' in e.message and
                'Could not read WAL record at' in e.message and
                'is absent' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertEqual(
            'ERROR',
            self.show_pb(backup_dir, 'node')[1]['status'],
            'Backup {0} should have STATUS "ERROR"')

        # Multi-thread PAGE backup
        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='page',
                options=["-j", "4"])
            self.assertEqual(
                1, 0,
                "Expecting Error because of wal segment disappearance.\n "
                "Output: {0} \n CMD: {1}".format(
                    self.output, self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'INFO: Wait for WAL segment' in e.message and
                'to be archived' in e.message and
                'Could not read WAL record at' in e.message and
                'is absent' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertEqual(
            'ERROR',
            self.show_pb(backup_dir, 'node')[2]['status'],
            'Backup {0} should have STATUS "ERROR"')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_page_backup_with_corrupted_wal_segment(self):
        """
        make node with archiving
        make archive backup, then generate some wals with pgbench,
        corrupt latest archived wal segment
        run page backup, expecting error because of missing wal segment
        make sure that backup status is 'ERROR'
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

        self.backup_node(backup_dir, 'node', node)

        # make some wals
        node.pgbench_init(scale=10)

        # delete last wal segment
        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(
            wals_dir, f)) and not f.endswith('.backup')]
        wals = map(str, wals)
 #       file = os.path.join(wals_dir, max(wals))

        if self.archive_compress:
            original_file = os.path.join(wals_dir, '000000010000000000000004.gz')
            tmp_file = os.path.join(backup_dir, '000000010000000000000004')

            with gzip.open(original_file, 'rb') as f_in, open(tmp_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

            # drop healthy file
            os.remove(original_file)
            file = tmp_file

        else:
            file = os.path.join(wals_dir, '000000010000000000000004')

        # corrupt file
        print(file)
        with open(file, "rb+", 0) as f:
            f.seek(42)
            f.write(b"blah")
            f.flush()
            f.close

        if self.archive_compress:
            # compress corrupted file and replace with it old file
            with open(file, 'rb') as f_in, gzip.open(original_file, 'wb', compresslevel=1) as f_out:
                shutil.copyfileobj(f_in, f_out)

            file = os.path.join(wals_dir, '000000010000000000000004.gz')

        #if self.archive_compress:
        #    file = file[:-3]

        # Single-thread PAGE backup
        try:
            self.backup_node(
                backup_dir, 'node', node, backup_type='page')
            self.assertEqual(
                1, 0,
                "Expecting Error because of wal segment disappearance.\n "
                "Output: {0} \n CMD: {1}".format(
                    self.output, self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'INFO: Wait for WAL segment' in e.message and
                'to be archived' in e.message and
                'Could not read WAL record at' in e.message and
                'Possible WAL corruption. Error has occured during reading WAL segment' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertEqual(
            'ERROR',
            self.show_pb(backup_dir, 'node')[1]['status'],
            'Backup {0} should have STATUS "ERROR"')

        # Multi-thread PAGE backup
        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='page', options=["-j", "4"])
            self.assertEqual(
                1, 0,
                "Expecting Error because of wal segment disappearance.\n "
                "Output: {0} \n CMD: {1}".format(
                    self.output, self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'INFO: Wait for WAL segment' in e.message and
                'to be archived' in e.message and
                'Could not read WAL record at' in e.message and
                'Possible WAL corruption. Error has occured during reading WAL segment "{0}"'.format(
                    file) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertEqual(
            'ERROR',
            self.show_pb(backup_dir, 'node')[2]['status'],
            'Backup {0} should have STATUS "ERROR"')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_page_backup_with_alien_wal_segment(self):
        """
        make two nodes with archiving
        take archive full backup from both nodes,
        generate some wals with pgbench on both nodes,
        move latest archived wal segment from second node to first node`s archive
        run page backup on first node
        expecting error because of alien wal segment
        make sure that backup status is 'ERROR'
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        alien_node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'alien_node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        self.add_instance(backup_dir, 'alien_node', alien_node)
        self.set_archiving(backup_dir, 'alien_node', alien_node)
        alien_node.slow_start()

        self.backup_node(
            backup_dir, 'node', node, options=['--stream'])
        self.backup_node(
            backup_dir, 'alien_node', alien_node, options=['--stream'])

        # make some wals
        node.safe_psql(
            "postgres",
            "create sequence t_seq; "
            "create table t_heap as select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1000) i;")

        alien_node.safe_psql(
            "postgres",
            "create database alien")

        alien_node.safe_psql(
            "alien",
            "create sequence t_seq; "
            "create table t_heap_alien as select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,100000) i;")

        # copy latest wal segment
        wals_dir = os.path.join(backup_dir, 'wal', 'alien_node')
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(
            wals_dir, f)) and not f.endswith('.backup')]
        wals = map(str, wals)
        filename = max(wals)
        file = os.path.join(wals_dir, filename)
        file_destination = os.path.join(
            os.path.join(backup_dir, 'wal', 'node'), filename)
#        file = os.path.join(wals_dir, '000000010000000000000004')
        print(file)
        print(file_destination)
        os.remove(file_destination)
        os.rename(file, file_destination)

        # Single-thread PAGE backup
        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='page')
            self.assertEqual(
                1, 0,
                "Expecting Error because of alien wal segment.\n "
                "Output: {0} \n CMD: {1}".format(
                    self.output, self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'INFO: Wait for WAL segment' in e.message and
                'to be archived' in e.message and
                'Could not read WAL record at' in e.message and
                'Possible WAL corruption. Error has occured during reading WAL segment' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertEqual(
            'ERROR',
            self.show_pb(backup_dir, 'node')[1]['status'],
            'Backup {0} should have STATUS "ERROR"')

        # Multi-thread PAGE backup
        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='page', options=["-j", "4"])
            self.assertEqual(
                1, 0,
                "Expecting Error because of alien wal segment.\n "
                "Output: {0} \n CMD: {1}".format(
                    self.output, self.cmd))
        except ProbackupException as e:
            self.assertIn('INFO: Wait for WAL segment', e.message)
            self.assertIn('to be archived', e.message)
            self.assertIn('Could not read WAL record at', e.message)
            self.assertIn('WAL file is from different database system: '
                'WAL file database system identifier is', e.message)
            self.assertIn('pg_control database system identifier is', e.message)
            self.assertIn('Possible WAL corruption. Error has occured '
                'during reading WAL segment', e.message)

        self.assertEqual(
            'ERROR',
            self.show_pb(backup_dir, 'node')[2]['status'],
            'Backup {0} should have STATUS "ERROR"')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_multithread_page_backup_with_toast(self):
        """
        make node, create toast, do multithread PAGE backup
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

        self.backup_node(backup_dir, 'node', node)

        # make some wals
        node.safe_psql(
            "postgres",
            "create table t3 as select i, "
            "repeat(md5(i::text),5006056) as fat_attr "
            "from generate_series(0,70) i")

        # Multi-thread PAGE backup
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', options=["-j", "4"])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_page_create_db(self):
        """
        Make node, take full backup, create database db1, take page backup,
        restore database and check it presense
        """
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'max_wal_size': '10GB',
                'checkpoint_timeout': '5min',
                'autovacuum': 'off'
            }
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,100) i")

        self.backup_node(
            backup_dir, 'node', node)

        # CREATE DATABASE DB1
        node.safe_psql("postgres", "create database db1")
        node.safe_psql(
            "db1",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,1000) i")

        # PAGE BACKUP
        backup_id = self.backup_node(backup_dir, 'node', node, backup_type='page')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # RESTORE
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))

        node_restored.cleanup()
        self.restore_node(
            backup_dir, 'node', node_restored,
            backup_id=backup_id, options=["-j", "4"])

        # COMPARE PHYSICAL CONTENT
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()

        node_restored.safe_psql('db1', 'select 1')
        node_restored.cleanup()

        # DROP DATABASE DB1
        node.safe_psql(
            "postgres", "drop database db1")
        # SECOND PAGE BACKUP
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # RESTORE SECOND PAGE BACKUP
        self.restore_node(
            backup_dir, 'node', node_restored,
            backup_id=backup_id, options=["-j", "4"]
        )

        # COMPARE PHYSICAL CONTENT
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node_restored.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()

        try:
            node_restored.safe_psql('db1', 'select 1')
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because we are connecting to deleted database"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd)
            )
        except QueryException as e:
            self.assertTrue(
                'FATAL:  database "db1" does not exist' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd)
            )

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_multi_timeline_page(self):
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

        page_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page',
            options=['--log-level-file=VERBOSE'])

        pgdata = self.pgdata_content(node.data_dir)

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

        backup_list = self.show_pb(backup_dir, 'node')

        self.assertEqual(
            backup_list[2]['parent-backup-id'],
            backup_list[0]['id'])
        self.assertEqual(backup_list[2]['current-tli'], 3)

        self.assertEqual(
            backup_list[3]['parent-backup-id'],
            backup_list[2]['id'])
        self.assertEqual(backup_list[3]['current-tli'], 5)

        self.assertEqual(
            backup_list[4]['parent-backup-id'],
            backup_list[3]['id'])
        self.assertEqual(backup_list[4]['current-tli'], 7)

        self.assertEqual(
            backup_list[5]['parent-backup-id'],
            backup_list[4]['id'])
        self.assertEqual(backup_list[5]['current-tli'], 7)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_multitimeline_page_1(self):
        """
        Check that backup in PAGE mode choose
        parent backup correctly:
        t2        /---->
        t1 -F--P---D->

        P must have F as parent
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off', 'wal_log_hints': 'on'})

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

        pgbench = node.pgbench(options=['-T', '20', '-c', '1'])
        pgbench.wait()

        page1 = self.backup_node(backup_dir, 'node', node, backup_type='page')

        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        page1 = self.backup_node(backup_dir, 'node', node, backup_type='delta')

        node.cleanup()
        self.restore_node(
            backup_dir, 'node', node, backup_id=page1,
            options=[
                '--recovery-target=immediate',
                '--recovery-target-action=promote'])

        node.slow_start()

        pgbench = node.pgbench(options=['-T', '20', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        print(self.backup_node(
            backup_dir, 'node', node, backup_type='page',
            options=['--log-level-console=LOG'], return_id=False))

        pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(backup_dir, 'node', node_restored)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)

        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_page_pg_resetxlog(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off',
                'shared_buffers': '512MB',
                'max_wal_size': '3GB'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # Create table
        node.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap "
            "as select nextval('t_seq')::int as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
#            "from generate_series(0,25600) i")
            "from generate_series(0,2560) i")

        self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            'postgres',
            "update t_heap set id = nextval('t_seq'), text = md5(text), "
            "tsvector = md5(repeat(tsvector::text, 10))::tsvector")

        self.switch_wal_segment(node)

        # kill the bastard
        if self.verbose:
            print('Killing postmaster. Losing Ptrack changes')
        node.stop(['-m', 'immediate', '-D', node.data_dir])

        # now smack it with sledgehammer
        if node.major_version >= 10:
            pg_resetxlog_path = self.get_bin_path('pg_resetwal')
            wal_dir = 'pg_wal'
        else:
            pg_resetxlog_path = self.get_bin_path('pg_resetxlog')
            wal_dir = 'pg_xlog'

        self.run_binary(
            [
                pg_resetxlog_path,
                '-D',
                node.data_dir,
                '-o 42',
                '-f'
            ],
            asynchronous=False)

        if not node.status():
            node.slow_start()
        else:
            print("Die! Die! Why won't you die?... Why won't you die?")
            exit(1)

        # take ptrack backup
#        self.backup_node(
#                backup_dir, 'node', node,
#                backup_type='page', options=['--stream'])

        try:
            self.backup_node(
                backup_dir, 'node', node, backup_type='page')
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because instance was brutalized by pg_resetxlog"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd)
            )
        except ProbackupException as e:
            self.assertIn(
                'Insert error message',
                e.message,
                '\n Unexpected Error Message: {0}\n'
                ' CMD: {1}'.format(repr(e.message), self.cmd))

#        pgdata = self.pgdata_content(node.data_dir)
#
#        node_restored = self.make_simple_node(
#            base_dir=os.path.join(module_name, fname, 'node_restored'))
#        node_restored.cleanup()
#
#        self.restore_node(
#            backup_dir, 'node', node_restored)
#
#        pgdata_restored = self.pgdata_content(node_restored.data_dir)
#        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)
