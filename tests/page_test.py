import unittest
from .helpers.ptrack_helpers import ProbackupTest
from testgres import QueryException
import time

class PageTest(ProbackupTest):

    # @unittest.skip("skip")
    def test_basic_page_vacuum_truncate(self):
        """
        make node, create table, take full backup,
        delete last 3 pages, vacuum relation,
        take page backup, take second page backup,
        restore last page backup and check data correctness
        """
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

        # TODO: make it dynamic
        node.safe_psql(
            "postgres",
            "delete from t_heap where ctid >= '(11,0)'")
        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.pb.backup_node('node', node, backup_type='page')

        self.pb.backup_node('node', node, backup_type='page')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

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
    def test_page_vacuum_truncate_1(self):
        """
        make node, create table, take full backup,
        delete all data, vacuum relation,
        take page backup, insert some data,
        take second page backup and check data correctness
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
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

        self.pb.backup_node('node', node)

        node.safe_psql(
            "postgres",
            "delete from t_heap")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.pb.backup_node('node', node, backup_type='page')

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1) i")

        self.pb.backup_node('node', node, backup_type='page')

        pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node=node_restored)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

    # @unittest.skip("skip")
    def test_page_stream(self):
        """
        make archive node, take full and page stream backups,
        restore them and check data correctness
        """
        self.maxDiff = None
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '30s'}
            )

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(0,100) i")

        full_result = node.table_checksum("t_heap")
        full_backup_id = self.pb.backup_node('node', node,
            backup_type='full', options=['--stream'])

        # PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(100,200) i")
        page_result = node.table_checksum("t_heap")
        page_backup_id = self.pb.backup_node('node', node,
            backup_type='page', options=['--stream', '-j', '4'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # Drop Node
        node.cleanup()

        # Check full backup
        restore_result = self.pb.restore_node('node', node,
                backup_id=full_backup_id, options=["-j", "4"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(full_backup_id))

        node.slow_start()
        full_result_new = node.table_checksum("t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check page backup
        restore_result = self.pb.restore_node('node', node,
                backup_id=page_backup_id, options=["-j", "4"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(page_backup_id))

        # GET RESTORED PGDATA AND COMPARE
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        node.slow_start()
        page_result_new = node.table_checksum("t_heap")
        self.assertEqual(page_result, page_result_new)
        node.cleanup()

    # @unittest.skip("skip")
    def test_page_archive(self):
        """
        make archive node, take full and page archive backups,
        restore them and check data correctness
        """
        self.maxDiff = None
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '30s'}
            )

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,100) i")
        full_result = node.table_checksum("t_heap")
        full_backup_id = self.pb.backup_node('node', node, backup_type='full')

        # PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, "
            "md5(i::text) as text, md5(i::text)::tsvector as tsvector "
            "from generate_series(100, 200) i")
        page_result = node.table_checksum("t_heap")
        page_backup_id = self.pb.backup_node('node', node,
            backup_type='page', options=["-j", "4"])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # Drop Node
        node.cleanup()

        # Restore and check full backup
        restore_result = self.pb.restore_node('node', node,
                backup_id=full_backup_id,
                options=[
                    "-j", "4",
                    "--immediate",
                    "--recovery-target-action=promote"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(full_backup_id))
        node.slow_start()

        full_result_new = node.table_checksum("t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Restore and check page backup
        restore_result = self.pb.restore_node('node', node,
                backup_id=page_backup_id,
                options=[
                    "-j", "4",
                    "--immediate",
                    "--recovery-target-action=promote"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(page_backup_id))

        # GET RESTORED PGDATA AND COMPARE
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        node.slow_start()

        page_result_new = node.table_checksum("t_heap")
        self.assertEqual(page_result, page_result_new)
        node.cleanup()

    # @unittest.skip("skip")
    def test_page_multiple_segments(self):
        """
        Make node, create table with multiple segments,
        write some data to it, check page and data correctness
        """
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'fsync': 'off',
                'shared_buffers': '1GB',
                'maintenance_work_mem': '1GB',
                'full_page_writes': 'off'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'somedata')

        # CREATE TABLE
        node.pgbench_init(scale=100, options=['--tablespace=somedata'])
        # FULL BACKUP
        self.pb.backup_node('node', node)

        # PGBENCH STUFF
        pgbench = node.pgbench(options=['-T', '50', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # GET LOGICAL CONTENT FROM NODE
        result = node.table_checksum("pgbench_accounts")
        # PAGE BACKUP
        self.pb.backup_node('node', node, backup_type='page')

        # GET PHYSICAL CONTENT FROM NODE
        pgdata = self.pgdata_content(node.data_dir)

        # RESTORE NODE
        restored_node = self.pg_node.make_simple('restored_node')
        restored_node.cleanup()
        tblspc_path = self.get_tblspace_path(node, 'somedata')
        tblspc_path_new = self.get_tblspace_path(
            restored_node, 'somedata_restored')

        self.pb.restore_node('node', restored_node,
            options=[
                "-j", "4",
                "-T", "{0}={1}".format(tblspc_path, tblspc_path_new)])

        # GET PHYSICAL CONTENT FROM NODE_RESTORED
        pgdata_restored = self.pgdata_content(restored_node.data_dir)

        # START RESTORED NODE
        restored_node.set_auto_conf({'port': restored_node.port})
        restored_node.slow_start()

        result_new = restored_node.table_checksum("pgbench_accounts")

        # COMPARE RESTORED FILES
        self.assertEqual(result, result_new, 'data is lost')

        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_page_delete(self):
        """
        Make node, create tablespace with table, take full backup,
        delete everything from table, vacuum table, take page backup,
        restore page backup, compare .
        """
        node = self.pg_node.make_simple('node',
            set_replication=True,
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
        self.pb.backup_node('node', node)
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
        self.pb.backup_node('node', node, backup_type='page')
        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # RESTORE
        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored,
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
        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

    # @unittest.skip("skip")
    def test_page_delete_1(self):
        """
        Make node, create tablespace with table, take full backup,
        delete everything from table, vacuum table, take page backup,
        restore page backup, compare .
        """
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '30s',
            }
        )

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

        node.safe_psql(
            "postgres",
            "delete from t_heap"
        )

        node.safe_psql(
            "postgres",
            "vacuum t_heap"
        )

        # PAGE BACKUP
        self.pb.backup_node('node', node, backup_type='page')
        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # RESTORE
        node_restored = self.pg_node.make_simple('node_restored'
        )
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored,
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
        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

    def test_parallel_pagemap(self):
        """
        Test for parallel WAL segments reading, during which pagemap is built
        """
        # Initialize instance and backup directory
        node = self.pg_node.make_simple('node',
            pg_options={
                "hot_standby": "on"
            }
        )
        node_restored = self.pg_node.make_simple('node_restored',
        )

        self.pb.init()
        self.pb.add_instance('node', node)
        node_restored.cleanup()
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Do full backup
        self.pb.backup_node('node', node)
        show_backup = self.pb.show('node')[0]

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
        self.pb.backup_node('node', node, backup_type="page", options=["-j", "4"])
        show_backup = self.pb.show('node')[1]

        self.assertEqual(show_backup['status'], "OK")
        self.assertEqual(show_backup['backup-mode'], "PAGE")

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # Restore it
        self.pb.restore_node('node', node=node_restored)

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

        # Check restored node
        count2 = node_restored.execute("postgres", "select count(*) from test")

        self.assertEqual(count1, count2)

        # Clean after yourself
        node.cleanup()
        node_restored.cleanup()

    def test_parallel_pagemap_1(self):
        """
        Test for parallel WAL segments reading, during which pagemap is built
        """
        # Initialize instance and backup directory
        node = self.pg_node.make_simple('node',
            pg_options={}
        )

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Do full backup
        self.pb.backup_node('node', node)
        show_backup = self.pb.show('node')[0]

        self.assertEqual(show_backup['status'], "OK")
        self.assertEqual(show_backup['backup-mode'], "FULL")

        # Fill instance with data and make several WAL segments ...
        node.pgbench_init(scale=10)

        # do page backup in single thread
        page_id = self.pb.backup_node('node', node, backup_type="page")

        self.pb.delete('node', page_id)

        # ... and do page backup with parallel pagemap
        self.pb.backup_node('node', node, backup_type="page", options=["-j", "4"])
        show_backup = self.pb.show('node')[1]

        self.assertEqual(show_backup['status'], "OK")
        self.assertEqual(show_backup['backup-mode'], "PAGE")

        # Drop node and restore it
        node.cleanup()
        self.pb.restore_node('node', node=node)
        node.slow_start()

        # Clean after yourself
        node.cleanup()

    # @unittest.skip("skip")
    def test_page_backup_with_lost_wal_segment(self):
        """
        make node with archiving
        make archive backup, then generate some wals with pgbench,
        delete latest archived wal segment
        run page backup, expecting error because of missing wal segment
        make sure that backup status is 'ERROR'
        """
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node)

        # make some wals
        node.pgbench_init(scale=3)

        # delete last wal segment
        walfile = '000000010000000000000004'+self.compress_suffix
        self.wait_instance_wal_exists(backup_dir, 'node', walfile)
        self.remove_instance_wal(backup_dir, 'node', walfile)

        # Single-thread PAGE backup
        self.pb.backup_node('node', node, backup_type='page',
                         expect_error="because of wal segment disappearance")
        self.assertMessage(contains='Could not read WAL record at')
        self.assertMessage(contains='is absent')

        self.assertEqual(
            'ERROR',
            self.pb.show('node')[1]['status'],
            'Backup {0} should have STATUS "ERROR"')

        # Multi-thread PAGE backup
        self.pb.backup_node('node', node, backup_type='page',
                         options=["-j", "4"],
                         expect_error="because of wal segment disappearance")
        self.assertMessage(contains='Could not read WAL record at')
        self.assertMessage(contains='is absent')

        self.assertEqual(
            'ERROR',
            self.pb.show('node')[2]['status'],
            'Backup {0} should have STATUS "ERROR"')

    # @unittest.skip("skip")
    def test_page_backup_with_corrupted_wal_segment(self):
        """
        make node with archiving
        make archive backup, then generate some wals with pgbench,
        corrupt latest archived wal segment
        run page backup, expecting error because of missing wal segment
        make sure that backup status is 'ERROR'
        """
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node)

        # make some wals
        node.pgbench_init(scale=10)

        # delete last wal segment
        file = '000000010000000000000004' + self.compress_suffix
        self.wait_instance_wal_exists(backup_dir, 'node', file)
        self.corrupt_instance_wal(backup_dir, 'node', file, 42, b"blah",
                                  decompressed=self.archive_compress)

        # Single-thread PAGE backup
        self.pb.backup_node('node', node, backup_type='page',
                         expect_error="because of wal segment disappearance")
        self.assertMessage(contains='Could not read WAL record at')
        self.assertMessage(contains='Possible WAL corruption. Error has occured during reading WAL segment')

        self.assertEqual(
            'ERROR',
            self.pb.show('node')[1]['status'],
            'Backup {0} should have STATUS "ERROR"')

        # Multi-thread PAGE backup
        self.pb.backup_node('node', node, backup_type='page',
                         options=["-j", "4"],
                         expect_error="because of wal segment disappearance")

        self.assertEqual(
            'ERROR',
            self.pb.show('node')[2]['status'],
            'Backup {0} should have STATUS "ERROR"')

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
        node = self.pg_node.make_simple('node',
            set_replication=True)

        alien_node = self.pg_node.make_simple('alien_node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.add_instance('alien_node', alien_node)
        self.pb.set_archiving('alien_node', alien_node)
        alien_node.slow_start()

        self.pb.backup_node('node', node, options=['--stream'])
        self.pb.backup_node('alien_node', alien_node, options=['--stream'])

        # make some wals
        node.safe_psql(
            "postgres",
            "create sequence t_seq; "
            "create table t_heap as select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i;")

        alien_node.execute(
            "postgres",
            "create database alien")

        wal_file = alien_node.execute(
            "alien",
            "SELECT pg_walfile_name(pg_current_wal_lsn());"
        )
        filename = wal_file[0][0] + self.compress_suffix

        alien_node.execute(
            "alien",
            "create sequence t_seq; "
            "create table t_heap_alien as select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i;")
        alien_node.execute(
            "alien",
            "select pg_switch_wal()")
        node.execute(
            "postgres",
            "select pg_switch_wal()")

        # wait nodes archived same file
        self.wait_instance_wal_exists(backup_dir, 'alien_node', filename)
        self.wait_instance_wal_exists(backup_dir, 'node', filename)
        file_content = self.read_instance_wal(backup_dir, 'alien_node', filename)
        self.write_instance_wal(backup_dir, 'node', filename, file_content)

        # Single-thread PAGE backup
        self.pb.backup_node('node', node, backup_type='page',
                         expect_error="because of alien wal segment")
        self.assertMessage(contains='Could not read WAL record at')
        self.assertMessage(contains='Possible WAL corruption. Error has occured during reading WAL segment')

        self.assertEqual(
            'ERROR',
            self.pb.show('node')[1]['status'],
            'Backup {0} should have STATUS "ERROR"')

        # Multi-thread PAGE backup
        self.pb.backup_node('node', node, backup_type='page',
                         options=["-j", "4"],
                         expect_error="because of alien wal segment")
        self.assertMessage(contains='Could not read WAL record at')
        self.assertMessage(contains='WAL file is from different database system: '
                                    'WAL file database system identifier is')
        self.assertMessage(contains='pg_control database system identifier is')
        self.assertMessage(contains='Possible WAL corruption. Error has occured '
                                    'during reading WAL segment')

        self.assertEqual(
            'ERROR',
            self.pb.show('node')[2]['status'],
            'Backup {0} should have STATUS "ERROR"')

    # @unittest.skip("skip")
    def test_multithread_page_backup_with_toast(self):
        """
        make node, create toast, do multithread PAGE backup
        """
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node)

        # make some wals
        node.safe_psql(
            "postgres",
            "create table t3 as select i, "
            "repeat(md5(i::text),5006056) as fat_attr "
            "from generate_series(0,70) i")

        # Multi-thread PAGE backup
        self.pb.backup_node('node', node,
            backup_type='page', options=["-j", "4"])

    # @unittest.skip("skip")
    def test_page_create_db(self):
        """
        Make node, take full backup, create database db1, take page backup,
        restore database and check it presense
        """
        self.maxDiff = None
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'max_wal_size': '10GB',
                'checkpoint_timeout': '5min',
            }
        )

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,100) i")

        self.pb.backup_node('node', node)

        # CREATE DATABASE DB1
        node.safe_psql("postgres", "create database db1")
        node.safe_psql(
            "db1",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,1000) i")

        # PAGE BACKUP
        backup_id = self.pb.backup_node('node', node, backup_type='page')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # RESTORE
        node_restored = self.pg_node.make_simple('node_restored')

        node_restored.cleanup()
        self.pb.restore_node('node', node_restored,
            backup_id=backup_id, options=["-j", "4"])

        # COMPARE PHYSICAL CONTENT
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

        node_restored.safe_psql('db1', 'select 1')
        node_restored.cleanup()

        # DROP DATABASE DB1
        node.safe_psql(
            "postgres", "drop database db1")
        # SECOND PAGE BACKUP
        backup_id = self.pb.backup_node('node', node, backup_type='page')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # RESTORE SECOND PAGE BACKUP
        self.pb.restore_node('node', node_restored,
            backup_id=backup_id, options=["-j", "4"]
        )

        # COMPARE PHYSICAL CONTENT
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node_restored.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

        error_result = node_restored.safe_psql('db1', 'select 1', expect_error=True)
        self.assertMessage(error_result, contains='FATAL:  database "db1" does not exist')

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

        page_id = self.pb.backup_node('node', node, backup_type='page',
            options=['--log-level-file=VERBOSE'])

        pgdata = self.pgdata_content(node.data_dir)

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

        backup_list = self.pb.show('node')

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
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={'wal_log_hints': 'on'})

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

        pgbench = node.pgbench(options=['-T', '20', '-c', '1'])
        pgbench.wait()

        page1 = self.pb.backup_node('node', node, backup_type='page')

        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        page1 = self.pb.backup_node('node', node, backup_type='delta')

        node.cleanup()
        self.pb.restore_node('node', node, backup_id=page1,
            options=[
                '--recovery-target=immediate',
                '--recovery-target-action=promote'])

        node.slow_start()

        pgbench = node.pgbench(options=['-T', '20', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        print(self.pb.backup_node('node', node, backup_type='page',
            options=['--log-level-console=LOG'], return_id=False))

        pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node=node_restored)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)

        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

        self.compare_pgdata(pgdata, pgdata_restored)

    @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_page_pg_resetxlog(self):
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'shared_buffers': '512MB',
                'max_wal_size': '3GB'})

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
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

        self.pb.backup_node('node', node)

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

        self.pb.run_binary(
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
#        self.pb.backup_node(
#                'node', node,
#                backup_type='page', options=['--stream'])

        self.pb.backup_node('node', node, backup_type='page',
                         expect_error="because instance was brutalized by pg_resetxlog")
        self.assertMessage(contains='Insert error message')

#        pgdata = self.pgdata_content(node.data_dir)
#
#        node_restored = self.pg_node.make_simple('node_restored')
#        node_restored.cleanup()
#
#        self.pb.restore_node(
#            'node', node_restored)
#
#        pgdata_restored = self.pgdata_content(node_restored.data_dir)
#        self.compare_pgdata(pgdata, pgdata_restored)

    def test_page_huge_xlog_record(self):
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'max_locks_per_transaction': '1000',
                'work_mem': '100MB',
                'temp_buffers': '100MB',
                'wal_buffers': '128MB',
                'wal_level' : 'logical',
                })

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        # Do full backup
        self.pb.backup_node('node', node, backup_type='full')

        show_backup = self.pb.show('node')[0]
        self.assertEqual(show_backup['status'], "OK")
        self.assertEqual(show_backup['backup-mode'], "FULL")

        # Originally client had the problem at the transaction that (supposedly)
        # deletes a lot of temporary tables (probably it was client disconnect).
        # It generated ~40MB COMMIT WAL record.
        #
        # `pg_logical_emit_message` is much simpler and faster way to generate
        # such huge record.
        node.safe_psql(
            "postgres",
            "select pg_logical_emit_message(False, 'z', repeat('o', 60*1000*1000))")

        # Do page backup with 1 thread
        backup_id = self.pb.backup_node('node', node, backup_type='page', options=['-j', '1'])

        show_backup = self.pb.show('node')[1]
        self.assertEqual(show_backup['status'], "OK")
        self.assertEqual(show_backup['backup-mode'], "PAGE")

        self.pb.delete('node', backup_id)

        # Repeat backup with multiple threads
        self.pb.backup_node('node', node, backup_type='page', options=['-j', '10'])

        show_backup = self.pb.show('node')[1]
        self.assertEqual(show_backup['status'], "OK")
        self.assertEqual(show_backup['backup-mode'], "PAGE")
