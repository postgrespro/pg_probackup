import os
import signal
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException

module_name = 'catchup'

class CatchupTest(ProbackupTest, unittest.TestCase):
    def setUp(self):
        self.fname = self.id().split('.')[3]

#########################################
# Basic tests
#########################################
    def test_basic_full_catchup(self):
        """
        Test 'multithreaded basebackup' mode (aka FULL catchup)
        """
        # preparation
        src_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, self.fname, 'src'),
            set_replication = True
            )
        src_pg.slow_start()
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question AS SELECT 42 AS answer")
        src_query_result = src_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")

        # do full catchup
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst'))
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        # 1st check: compare data directories
        self.compare_pgdata(
            self.pgdata_content(src_pg.data_dir),
            self.pgdata_content(dst_pg.data_dir)
            )

        # run&recover catchup'ed instance
        src_pg.stop()
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start()

        # 2nd check: run verification query
        dst_query_result = dst_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # Cleanup
        dst_pg.stop()
        #self.assertEqual(1, 0, 'Stop test')
        self.del_test_dir(module_name, self.fname)

    def test_full_catchup_with_tablespace(self):
        """
        Test tablespace transfers
        """
        # preparation
        src_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, self.fname, 'src'),
            set_replication = True
            )
        src_pg.slow_start()
        tblspace1_old_path = self.get_tblspace_path(src_pg, 'tblspace1_old')
        self.create_tblspace_in_node(src_pg, 'tblspace1', tblspc_path = tblspace1_old_path)
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question TABLESPACE tblspace1 AS SELECT 42 AS answer")
        src_query_result = src_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")

        # do full catchup with tablespace mapping
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst'))
        tblspace1_new_path = self.get_tblspace_path(dst_pg, 'tblspace1_new')
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = [
                '-d', 'postgres',
                '-p', str(src_pg.port),
                '--stream',
                '-T', '{0}={1}'.format(tblspace1_old_path, tblspace1_new_path)
                ]
            )

        # 1st check: compare data directories
        self.compare_pgdata(
            self.pgdata_content(src_pg.data_dir),
            self.pgdata_content(dst_pg.data_dir)
            )

        # make changes in master tablespace
        src_pg.safe_psql(
            "postgres",
            "UPDATE ultimate_question SET answer = -1")
        src_pg.stop()

        # run&recover catchup'ed instance
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start()

        # 2nd check: run verification query
        dst_query_result = dst_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # Cleanup
        dst_pg.stop()
        self.del_test_dir(module_name, self.fname)

    def test_basic_delta_catchup(self):
        """
        Test delta catchup
        """
        # preparation 1: source
        src_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, self.fname, 'src'),
            set_replication = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question(answer int)")

        # preparation 2: make clean shutdowned lagging behind replica
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst'))
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        self.set_replica(src_pg, dst_pg)
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start(replica = True)
        dst_pg.stop()

        # preparation 3: make changes on master (source)
        src_pg.pgbench_init(scale = 10)
        pgbench = src_pg.pgbench(options=['-T', '10', '--no-vacuum'])
        pgbench.wait()
        src_pg.safe_psql("postgres", "INSERT INTO ultimate_question VALUES(42)")
        src_query_result = src_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")

        # do delta catchup
        self.catchup_node(
            backup_mode = 'DELTA',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        # 1st check: compare data directories
        self.compare_pgdata(
            self.pgdata_content(src_pg.data_dir),
            self.pgdata_content(dst_pg.data_dir)
            )

        # run&recover catchup'ed instance
        src_pg.stop()
        self.set_replica(master = src_pg, replica = dst_pg)
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start(replica = True)

        # 2nd check: run verification query
        dst_query_result = dst_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # Cleanup
        dst_pg.stop()
        #self.assertEqual(1, 0, 'Stop test')
        self.del_test_dir(module_name, self.fname)

    def test_basic_ptrack_catchup(self):
        """
        Test ptrack catchup
        """
        if not self.ptrack:
            return unittest.skip('Skipped because ptrack support is disabled')

        # preparation 1: source
        src_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, self.fname, 'src'),
            set_replication = True,
            ptrack_enable = True,
            initdb_params = ['--data-checksums']
            )
        src_pg.slow_start()
        src_pg.safe_psql("postgres", "CREATE EXTENSION ptrack")
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question(answer int)")

        # preparation 2: make clean shutdowned lagging behind replica
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst'))
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        self.set_replica(src_pg, dst_pg)
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start(replica = True)
        dst_pg.stop()

        # preparation 3: make changes on master (source)
        src_pg.pgbench_init(scale = 10)
        pgbench = src_pg.pgbench(options=['-T', '10', '--no-vacuum'])
        pgbench.wait()
        src_pg.safe_psql("postgres", "INSERT INTO ultimate_question VALUES(42)")
        src_query_result = src_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")

        # do ptrack catchup
        self.catchup_node(
            backup_mode = 'PTRACK',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        # 1st check: compare data directories
        self.compare_pgdata(
            self.pgdata_content(src_pg.data_dir),
            self.pgdata_content(dst_pg.data_dir)
            )

        # run&recover catchup'ed instance
        src_pg.stop()
        self.set_replica(master = src_pg, replica = dst_pg)
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start(replica = True)

        # 2nd check: run verification query
        dst_query_result = dst_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # Cleanup
        dst_pg.stop()
        #self.assertEqual(1, 0, 'Stop test')
        self.del_test_dir(module_name, self.fname)

    def test_tli_delta_catchup(self):
        """
        Test that we correctly follow timeline change with delta catchup
        """
        # preparation 1: source
        src_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, self.fname, 'src'),
            set_replication = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()

        # preparation 2: destination
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst'))
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start()
        dst_pg.stop()

        # preparation 3: promote source
        src_pg.stop()
        self.set_replica(dst_pg, src_pg) # fake replication
        src_pg.slow_start(replica = True)
        src_pg.promote()
        src_pg.safe_psql("postgres", "CREATE TABLE ultimate_question AS SELECT 42 AS answer")
        src_query_result = src_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")

        # do catchup
        self.catchup_node(
            backup_mode = 'DELTA',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        # 1st check: compare data directories
        self.compare_pgdata(
            self.pgdata_content(src_pg.data_dir),
            self.pgdata_content(dst_pg.data_dir)
            )

        # run&recover catchup'ed instance
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start()

        # 2nd check: run verification query
        dst_query_result = dst_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # Cleanup
        src_pg.stop()
        dst_pg.stop()
        self.del_test_dir(module_name, self.fname)

    def test_tli_ptrack_catchup(self):
        """
        Test that we correctly follow timeline change with ptrack catchup
        """
        if not self.ptrack:
            return unittest.skip('Skipped because ptrack support is disabled')

        # preparation 1: source
        src_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, self.fname, 'src'),
            set_replication = True,
            ptrack_enable = True,
            initdb_params = ['--data-checksums']
            )
        src_pg.slow_start()
        src_pg.safe_psql("postgres", "CREATE EXTENSION ptrack")

        # preparation 2: destination
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst'))
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start()
        dst_pg.stop()

        # preparation 3: promote source
        src_pg.stop()
        self.set_replica(dst_pg, src_pg) # fake replication
        src_pg.slow_start(replica = True)
        src_pg.promote()
        src_pg.safe_psql("postgres", "CREATE TABLE ultimate_question AS SELECT 42 AS answer")
        src_query_result = src_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")

        # do catchup
        self.catchup_node(
            backup_mode = 'PTRACK',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        # 1st check: compare data directories
        self.compare_pgdata(
            self.pgdata_content(src_pg.data_dir),
            self.pgdata_content(dst_pg.data_dir)
            )

        # run&recover catchup'ed instance
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start()

        # 2nd check: run verification query
        dst_query_result = dst_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # Cleanup
        src_pg.stop()
        dst_pg.stop()
        self.del_test_dir(module_name, self.fname)

#########################################
# Test various corner conditions
#########################################
    def test_table_drop_with_delta(self):
        """
        Test that dropped table in source will be dropped in delta catchup'ed instance too
        """
        # preparation 1: source
        src_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, self.fname, 'src'),
            set_replication = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question AS SELECT 42 AS answer")

        # preparation 2: make clean shutdowned lagging behind replica
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst'))
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start()
        dst_pg.stop()

        # preparation 3: make changes on master (source)
        # perform checkpoint twice to ensure, that datafile is actually deleted on filesystem
        src_pg.safe_psql("postgres", "DROP TABLE ultimate_question")
        src_pg.safe_psql("postgres", "CHECKPOINT")
        src_pg.safe_psql("postgres", "CHECKPOINT")

        # do delta catchup
        self.catchup_node(
            backup_mode = 'DELTA',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        # Check: compare data directories
        self.compare_pgdata(
            self.pgdata_content(src_pg.data_dir),
            self.pgdata_content(dst_pg.data_dir)
            )

        # Cleanup
        src_pg.stop()
        self.del_test_dir(module_name, self.fname)

    def test_table_drop_with_ptrack(self):
        """
        Test that dropped table in source will be dropped in ptrack catchup'ed instance too
        """
        if not self.ptrack:
            return unittest.skip('Skipped because ptrack support is disabled')

        # preparation 1: source
        src_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, self.fname, 'src'),
            set_replication = True,
            ptrack_enable = True,
            initdb_params = ['--data-checksums']
            )
        src_pg.slow_start()
        src_pg.safe_psql("postgres", "CREATE EXTENSION ptrack")
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question AS SELECT 42 AS answer")

        # preparation 2: make clean shutdowned lagging behind replica
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst'))
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start()
        dst_pg.stop()

        # preparation 3: make changes on master (source)
        # perform checkpoint twice to ensure, that datafile is actually deleted on filesystem
        src_pg.safe_psql("postgres", "DROP TABLE ultimate_question")
        src_pg.safe_psql("postgres", "CHECKPOINT")
        src_pg.safe_psql("postgres", "CHECKPOINT")

        # do ptrack catchup
        self.catchup_node(
            backup_mode = 'PTRACK',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        # Check: compare data directories
        self.compare_pgdata(
            self.pgdata_content(src_pg.data_dir),
            self.pgdata_content(dst_pg.data_dir)
            )

        # Cleanup
        src_pg.stop()
        self.del_test_dir(module_name, self.fname)

    def test_tablefile_truncation_with_delta(self):
        """
        Test that truncated table in source will be truncated in delta catchup'ed instance too
        """
        # preparation 1: source
        src_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, self.fname, 'src'),
            set_replication = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()
        src_pg.safe_psql(
            "postgres",
            "CREATE SEQUENCE t_seq; "
            "CREATE TABLE t_heap AS SELECT i AS id, "
            "md5(i::text) AS text, "
            "md5(repeat(i::text, 10))::tsvector AS tsvector "
            "FROM generate_series(0, 1024) i")
        src_pg.safe_psql("postgres", "VACUUM t_heap")

        # preparation 2: make clean shutdowned lagging behind replica
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst'))
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dest_options = {}
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start()
        dst_pg.stop()

        # preparation 3: make changes on master (source)
        src_pg.safe_psql("postgres", "DELETE FROM t_heap WHERE ctid >= '(11,0)'")
        src_pg.safe_psql("postgres", "VACUUM t_heap")

        # do delta catchup
        self.catchup_node(
            backup_mode = 'DELTA',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        # Check: compare data directories
        self.compare_pgdata(
            self.pgdata_content(src_pg.data_dir),
            self.pgdata_content(dst_pg.data_dir)
            )

        # Cleanup
        src_pg.stop()
        self.del_test_dir(module_name, self.fname)

    def test_tablefile_truncation_with_ptrack(self):
        """
        Test that truncated table in source will be truncated in ptrack catchup'ed instance too
        """
        if not self.ptrack:
            return unittest.skip('Skipped because ptrack support is disabled')

        # preparation 1: source
        src_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, self.fname, 'src'),
            set_replication = True,
            ptrack_enable = True,
            initdb_params = ['--data-checksums']
            )
        src_pg.slow_start()
        src_pg.safe_psql("postgres", "CREATE EXTENSION ptrack")
        src_pg.safe_psql(
            "postgres",
            "CREATE SEQUENCE t_seq; "
            "CREATE TABLE t_heap AS SELECT i AS id, "
            "md5(i::text) AS text, "
            "md5(repeat(i::text, 10))::tsvector AS tsvector "
            "FROM generate_series(0, 1024) i")
        src_pg.safe_psql("postgres", "VACUUM t_heap")

        # preparation 2: make clean shutdowned lagging behind replica
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst'))
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dest_options = {}
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start()
        dst_pg.stop()

        # preparation 3: make changes on master (source)
        src_pg.safe_psql("postgres", "DELETE FROM t_heap WHERE ctid >= '(11,0)'")
        src_pg.safe_psql("postgres", "VACUUM t_heap")

        # do ptrack catchup
        self.catchup_node(
            backup_mode = 'PTRACK',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        # Check: compare data directories
        self.compare_pgdata(
            self.pgdata_content(src_pg.data_dir),
            self.pgdata_content(dst_pg.data_dir)
            )

        # Cleanup
        src_pg.stop()
        self.del_test_dir(module_name, self.fname)

#########################################
# Test reaction on user errors
#########################################
    def test_local_tablespace_without_mapping(self):
        """
        Test that we detect absence of needed --tablespace-mapping option
        """
        if self.remote:
            return unittest.skip('Skipped because this test tests local catchup error handling')

        src_pg = self.make_simple_node(base_dir = os.path.join(module_name, self.fname, 'src'))
        src_pg.slow_start()

        tblspace_path = self.get_tblspace_path(src_pg, 'tblspace')
        self.create_tblspace_in_node(
            src_pg, 'tblspace',
            tblspc_path = tblspace_path)

        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question TABLESPACE tblspace AS SELECT 42 AS answer")

        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst'))
        try:
            self.catchup_node(
                backup_mode = 'FULL',
                source_pgdata = src_pg.data_dir,
                destination_node = dst_pg,
                options = [
                    '-d', 'postgres',
                    '-p', str(src_pg.port),
                    '--stream',
                    ]
                )
            self.assertEqual(1, 0, "Expecting Error because '-T' parameter is not specified.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Local catchup executed, but source database contains tablespace',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Cleanup
        src_pg.stop()
        self.del_test_dir(module_name, self.fname)

    def test_running_dest_postmaster(self):
        """
        Test that we detect running postmaster in destination
        """
        # preparation 1: source
        src_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, self.fname, 'src'),
            set_replication = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()

        # preparation 2: destination
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst'))
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start()
        # leave running destination postmaster
        # so don't call dst_pg.stop()

        # try delta catchup
        try:
            self.catchup_node(
                backup_mode = 'DELTA',
                source_pgdata = src_pg.data_dir,
                destination_node = dst_pg,
                options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
                )
            self.assertEqual(1, 0, "Expecting Error because postmaster in destination is running.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Postmaster with pid ',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Cleanup
        src_pg.stop()
        self.del_test_dir(module_name, self.fname)

    def test_same_db_id(self):
        """
        Test that we detect different id's of source and destination
        """
        # preparation:
        #   source
        src_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, self.fname, 'src'),
            set_replication = True
            )
        src_pg.slow_start()
        #   destination
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst'))
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start()
        dst_pg.stop()
        #   fake destination
        fake_dst_pg = self.make_simple_node(base_dir = os.path.join(module_name, self.fname, 'fake_dst'))
        #   fake source
        fake_src_pg = self.make_simple_node(base_dir = os.path.join(module_name, self.fname, 'fake_src'))

        # try delta catchup (src (with correct src conn), fake_dst)
        try:
            self.catchup_node(
                backup_mode = 'DELTA',
                source_pgdata = src_pg.data_dir,
                destination_node = fake_dst_pg,
                options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
                )
            self.assertEqual(1, 0, "Expecting Error because database identifiers mismatch.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Database identifiers mismatch: ',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # try delta catchup (fake_src (with wrong src conn), dst)
        try:
            self.catchup_node(
                backup_mode = 'DELTA',
                source_pgdata = fake_src_pg.data_dir,
                destination_node = dst_pg,
                options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
                )
            self.assertEqual(1, 0, "Expecting Error because database identifiers mismatch.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Database identifiers mismatch: ',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Cleanup
        src_pg.stop()
        self.del_test_dir(module_name, self.fname)

    def test_tli_destination_mismatch(self):
        """
        Test that we detect TLI mismatch in destination
        """
        # preparation 1: source
        src_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, self.fname, 'src'),
            set_replication = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()

        # preparation 2: destination
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst'))
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        self.set_replica(src_pg, dst_pg)
        dst_pg.slow_start(replica = True)
        dst_pg.promote()
        dst_pg.stop()

        # preparation 3: "useful" changes
        src_pg.safe_psql("postgres", "CREATE TABLE ultimate_question AS SELECT 42 AS answer")
        src_query_result = src_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")

        # try catchup
        try:
            self.catchup_node(
                backup_mode = 'DELTA',
                source_pgdata = src_pg.data_dir,
                destination_node = dst_pg,
                options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
                )
            dst_options = {}
            dst_options['port'] = str(dst_pg.port)
            self.set_auto_conf(dst_pg, dst_options)
            dst_pg.slow_start()
            dst_query_result = dst_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")
            dst_pg.stop()
            self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Source is behind destination in timeline history',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Cleanup
        src_pg.stop()
        self.del_test_dir(module_name, self.fname)

    def test_tli_source_mismatch(self):
        """
        Test that we detect TLI mismatch in source history
        """
        # preparation 1: source
        src_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, self.fname, 'src'),
            set_replication = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()

        # preparation 2: fake source (promouted copy)
        fake_src_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'fake_src'))
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = fake_src_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        fake_src_options = {}
        fake_src_options['port'] = str(fake_src_pg.port)
        self.set_auto_conf(fake_src_pg, fake_src_options)
        self.set_replica(src_pg, fake_src_pg)
        fake_src_pg.slow_start(replica = True)
        fake_src_pg.promote()
        self.switch_wal_segment(fake_src_pg)
        fake_src_pg.safe_psql(
            "postgres",
            "CREATE TABLE t_heap AS SELECT i AS id, "
            "md5(i::text) AS text, "
            "md5(repeat(i::text, 10))::tsvector AS tsvector "
            "FROM generate_series(0, 256) i")
        self.switch_wal_segment(fake_src_pg)
        fake_src_pg.safe_psql("postgres", "CREATE TABLE ultimate_question AS SELECT 'trash' AS garbage")

        # preparation 3: destination
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst'))
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start()
        dst_pg.stop()

        # preparation 4: "useful" changes
        src_pg.safe_psql("postgres", "CREATE TABLE ultimate_question AS SELECT 42 AS answer")
        src_query_result = src_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")

        # try catchup
        try:
            self.catchup_node(
                backup_mode = 'DELTA',
                source_pgdata = fake_src_pg.data_dir,
                destination_node = dst_pg,
                options = ['-d', 'postgres', '-p', str(fake_src_pg.port), '--stream']
                )
            dst_options = {}
            dst_options['port'] = str(dst_pg.port)
            self.set_auto_conf(dst_pg, dst_options)
            dst_pg.slow_start()
            dst_query_result = dst_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")
            dst_pg.stop()
            self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Destination is not in source timeline history',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Cleanup
        src_pg.stop()
        fake_src_pg.stop()
        self.del_test_dir(module_name, self.fname)

#########################################
# Test unclean destination
#########################################
    def test_unclean_delta_catchup(self):
        """
        Test that we correctly recover uncleanly shutdowned destination
        """
        # preparation 1: source
        src_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, self.fname, 'src'),
            set_replication = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question(answer int)")

        # preparation 2: destination
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst'))
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        # try #1
        try:
            self.catchup_node(
                backup_mode = 'DELTA',
                source_pgdata = src_pg.data_dir,
                destination_node = dst_pg,
                options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
                )
            self.assertEqual(1, 0, "Expecting Error because destination pg is not cleanly shutdowned.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Destination directory contains "backup_label" file',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # try #2
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start()
        self.assertNotEqual(dst_pg.pid, 0, "Cannot detect pid of running postgres")
        os.kill(dst_pg.pid, signal.SIGKILL)

        # preparation 3: make changes on master (source)
        src_pg.pgbench_init(scale = 10)
        pgbench = src_pg.pgbench(options=['-T', '10', '--no-vacuum'])
        pgbench.wait()
        src_pg.safe_psql("postgres", "INSERT INTO ultimate_question VALUES(42)")
        src_query_result = src_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")

        # do delta catchup
        self.catchup_node(
            backup_mode = 'DELTA',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        # 1st check: compare data directories
        self.compare_pgdata(
            self.pgdata_content(src_pg.data_dir),
            self.pgdata_content(dst_pg.data_dir)
            )

        # run&recover catchup'ed instance
        src_pg.stop()
        self.set_replica(master = src_pg, replica = dst_pg)
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start(replica = True)

        # 2nd check: run verification query
        dst_query_result = dst_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # Cleanup
        dst_pg.stop()
        self.del_test_dir(module_name, self.fname)

    def test_unclean_ptrack_catchup(self):
        """
        Test that we correctly recover uncleanly shutdowned destination
        """
        if not self.ptrack:
            return unittest.skip('Skipped because ptrack support is disabled')

        # preparation 1: source
        src_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, self.fname, 'src'),
            set_replication = True,
            ptrack_enable = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()
        src_pg.safe_psql("postgres", "CREATE EXTENSION ptrack")
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question(answer int)")

        # preparation 2: destination
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst'))
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        # try #1
        try:
            self.catchup_node(
                backup_mode = 'PTRACK',
                source_pgdata = src_pg.data_dir,
                destination_node = dst_pg,
                options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
                )
            self.assertEqual(1, 0, "Expecting Error because destination pg is not cleanly shutdowned.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Destination directory contains "backup_label" file',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # try #2
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start()
        self.assertNotEqual(dst_pg.pid, 0, "Cannot detect pid of running postgres")
        os.kill(dst_pg.pid, signal.SIGKILL)

        # preparation 3: make changes on master (source)
        src_pg.pgbench_init(scale = 10)
        pgbench = src_pg.pgbench(options=['-T', '10', '--no-vacuum'])
        pgbench.wait()
        src_pg.safe_psql("postgres", "INSERT INTO ultimate_question VALUES(42)")
        src_query_result = src_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")

        # do delta catchup
        self.catchup_node(
            backup_mode = 'PTRACK',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        # 1st check: compare data directories
        self.compare_pgdata(
            self.pgdata_content(src_pg.data_dir),
            self.pgdata_content(dst_pg.data_dir)
            )

        # run&recover catchup'ed instance
        src_pg.stop()
        self.set_replica(master = src_pg, replica = dst_pg)
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start(replica = True)

        # 2nd check: run verification query
        dst_query_result = dst_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # Cleanup
        dst_pg.stop()
        self.del_test_dir(module_name, self.fname)

#########################################
# Test replication slot logic
#
#   -S, --slot=SLOTNAME                        replication slot to use
#       --temp-slot                                    use temporary replication slot
#   -P  --create-permanent-slot              create permanent replication slot
#       --primary-slot-name=SLOTNAME value for primary_slot_name parameter
#
# 1. if "--slot" is used - try to use already existing slot with given name
# 2. if "--slot" and "--create-permanent-slot" used - try to create permanent slot and use it.
# 3. If "--create-permanent-slot " flag is used without "--slot" option - use generic slot name like "pg_probackup_perm_slot"
# 4. If "--create-permanent-slot " flag is used and permanent slot already exists - fail with error.
# 5. "--create-permanent-slot" and "--temp-slot" flags cannot be used together.
# 6. "--primary-slot-name" and `-R` are used to create replication configuration ( as in restore command )
#########################################
    def test_catchup_with_replication_slot(self):
        """
        """
        # preparation
        src_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, self.fname, 'src'),
            set_replication = True
            )
        src_pg.slow_start()

        # 1a. --slot option
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst_1a'))
        try:
            self.catchup_node(
                backup_mode = 'FULL',
                source_pgdata = src_pg.data_dir,
                destination_node = dst_pg,
                options = [
                    '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                    '--slot=nonexistentslot_1a'
                    ]
                )
            self.assertEqual(1, 0, "Expecting Error because replication slot does not exist.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR:  replication slot "nonexistentslot_1a" does not exist',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

	# 1b. --slot option
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst_1b'))
        src_pg.safe_psql("postgres", "SELECT pg_catalog.pg_create_physical_replication_slot('existentslot_1b')")
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = [
                '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                '--slot=existentslot_1b'
                ]
            )

        # 2a. --slot --create-permanent-slot
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst_2a'))
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = [
                '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                '--slot=nonexistentslot_2a',
                '--create-permanent-slot'
                ]
            )

        # 2b. and 4. --slot --create-permanent-slot
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst_2b'))
        src_pg.safe_psql("postgres", "SELECT pg_catalog.pg_create_physical_replication_slot('existentslot_2b')")
        try:
            self.catchup_node(
                backup_mode = 'FULL',
                source_pgdata = src_pg.data_dir,
                destination_node = dst_pg,
                options = [
                    '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                    '--slot=existentslot_2b',
                    '--create-permanent-slot'
                    ]
                )
            self.assertEqual(1, 0, "Expecting Error because replication slot already exist.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR:  replication slot "existentslot_2b" already exists',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # 3. --create-permanent-slot --slot
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst_3'))
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = [
                '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                '--create-permanent-slot'
                ]
            )
        slot_name = src_pg.safe_psql(
            "postgres",
            "SELECT slot_name FROM pg_catalog.pg_replication_slots "
            "WHERE slot_name NOT LIKE '%existentslot%' "
            "AND slot_type = 'physical'"
            ).decode('utf-8').rstrip()
        self.assertEqual(slot_name, 'pg_probackup_perm_slot', 'Slot name mismatch')

        # 5. --create-permanent-slot --temp-slot
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst_5'))
        try:
            self.catchup_node(
                backup_mode = 'FULL',
                source_pgdata = src_pg.data_dir,
                destination_node = dst_pg,
                options = [
                    '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                    '--create-permanent-slot',
                    '--temp-slot'
                    ]
                )
            self.assertEqual(1, 0, "Expecting Error because conflicting options --create-permanent-slot and --temp-slot used together\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: You cannot specify "--create-permanent-slot" option with the "--temp-slot" option',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        #self.assertEqual(1, 0, 'Stop test')
        self.del_test_dir(module_name, self.fname)
