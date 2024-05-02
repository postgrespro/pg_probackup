import os
import subprocess
from pathlib import Path
from .helpers.ptrack_helpers import ProbackupTest
from parameterized import parameterized

module_name = 'catchup'


class CatchupTest(ProbackupTest):
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
        src_pg = self.pg_node.make_simple('src',
            set_replication = True
            )
        src_pg.slow_start()
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question AS SELECT 42 AS answer")
        src_query_result = src_pg.table_checksum("ultimate_question")

        # do full catchup
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
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
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start()

        # 2nd check: run verification query
        dst_query_result = dst_pg.table_checksum("ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # Cleanup
        dst_pg.stop()
        #self.assertEqual(1, 0, 'Stop test')


    @parameterized.expand(("DELTA", "PTRACK"))
    def test_cascade_catchup(self, test_input):
        """
        Test catchup of catchup'ed node
        """
        # preparation

        if test_input == "PTRACK" and not self.ptrack:
            self.skipTest("Ptrack is disabled, test_cascade_catchup")
        elif test_input == "PTRACK" and self.ptrack:
            db1 = self.pg_node.make_simple('db1', set_replication = True, ptrack_enable=True)
        else:
            db1 = self.pg_node.make_simple('db1', set_replication = True)

        db1.slow_start()

        if test_input == "PTRACK":
            db1.safe_psql("postgres", "CREATE EXTENSION ptrack")

        db1.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question AS SELECT 42 AS answer")
        db1_query_result = db1.table_checksum("ultimate_question")

        # full catchup db1 -> db2
        db2 = self.pg_node.make_empty('db2')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = db1.data_dir,
            destination_node = db2,
            options = ['-d', 'postgres', '-p', str(db1.port), '--stream']
        )

        # 1st check: compare data directories
        self.compare_pgdata(
            self.pgdata_content(db1.data_dir),
            self.pgdata_content(db2.data_dir)
        )

        # run&recover catchup'ed instance
        self.set_replica(db1, db2)
        db2_options = {}
        db2_options['port'] = str(db2.port)
        db2.set_auto_conf(db2_options)
        db2.slow_start(replica = True)

        # 2nd check: run verification query
        db2_query_result = db2.table_checksum("ultimate_question")
        self.assertEqual(db1_query_result, db2_query_result, 'Different answer from copy 2')

        # full catchup db2 -> db3
        db3 = self.pg_node.make_empty('db3')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = db2.data_dir,
            destination_node = db3,
            options = ['-d', 'postgres', '-p', str(db2.port), '--stream']
        )

        # 1st check: compare data directories
        self.compare_pgdata(
            self.pgdata_content(db2.data_dir),
            self.pgdata_content(db3.data_dir)
        )

        # run&recover catchup'ed instance
        self.set_replica(db2, db3)
        db3_options = {}
        db3_options['port'] = str(db3.port)
        db3.set_auto_conf(db3_options)
        db3.slow_start(replica = True)

        db3_query_result = db3.table_checksum("ultimate_question")
        self.assertEqual(db2_query_result, db3_query_result, 'Different answer from copy 3')

        db2.stop()
        db3.stop()

        # data modifications before incremental catchups
        db1.safe_psql(
            "postgres",
            "UPDATE ultimate_question SET answer = -1")
        db1.safe_psql("postgres", "CHECKPOINT")

        # do first incremental catchup
        self.pb.catchup_node(
            backup_mode = test_input,
            source_pgdata = db1.data_dir,
            destination_node = db2,
            options = ['-d', 'postgres', '-p', str(db1.port), '--stream']
        )

        self.compare_pgdata(
            self.pgdata_content(db1.data_dir),
            self.pgdata_content(db2.data_dir)
        )

        self.set_replica(db1, db2)
        db2_options = {}
        db2_options['port'] = str(db2.port)
        db2.set_auto_conf(db2_options)
        db2.slow_start(replica = True)

        # do second incremental catchup
        self.pb.catchup_node(
            backup_mode = test_input,
            source_pgdata = db2.data_dir,
            destination_node = db3,
            options = ['-d', 'postgres', '-p', str(db2.port), '--stream']
        )

        self.compare_pgdata(
            self.pgdata_content(db2.data_dir),
            self.pgdata_content(db3.data_dir)
        )

        self.set_replica(db2, db3)
        db3_options = {}
        db3_options['port'] = str(db3.port)
        db3.set_auto_conf(db3_options)
        self.pb.set_archiving('db3', db3, replica=True)
        db3.slow_start(replica = True)

        # data modification for checking continuous archiving
        db1.safe_psql(
            "postgres",
            "DROP TABLE ultimate_question")
        db1.safe_psql("postgres", "CHECKPOINT")

        self.wait_until_replica_catch_with_master(db1, db2)
        self.wait_until_replica_catch_with_master(db1, db3)

        db1_query_result = db1.table_checksum("pg_class")
        db2_query_result = db2.table_checksum("pg_class")
        db3_query_result = db3.table_checksum("pg_class")

        self.assertEqual(db1_query_result, db2_query_result, 'Different answer from copy 2')
        self.assertEqual(db2_query_result, db3_query_result, 'Different answer from copy 3')

        db1_query_result = db1.table_checksum("pg_depend")
        db2_query_result = db2.table_checksum("pg_depend")
        db3_query_result = db3.table_checksum("pg_depend")

        self.assertEqual(db1_query_result, db2_query_result, 'Different answer from copy 2')
        self.assertEqual(db2_query_result, db3_query_result, 'Different answer from copy 3')
        
        # cleanup
        db3.stop()
        db2.stop()
        db1.stop()


    def test_full_catchup_with_tablespace(self):
        """
        Test tablespace transfers
        """
        # preparation
        src_pg = self.pg_node.make_simple('src',
            set_replication = True
            )
        src_pg.slow_start()
        tblspace1_old_path = self.get_tblspace_path(src_pg, 'tblspace1_old')
        self.create_tblspace_in_node(src_pg, 'tblspace1', tblspc_path = tblspace1_old_path)
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question TABLESPACE tblspace1 AS SELECT 42 AS answer")
        src_query_result = src_pg.table_checksum("ultimate_question")

        # do full catchup with tablespace mapping
        dst_pg = self.pg_node.make_empty('dst')
        tblspace1_new_path = self.get_tblspace_path(dst_pg, 'tblspace1_new')
        self.pb.catchup_node(
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
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start()

        # 2nd check: run verification query
        dst_query_result = dst_pg.table_checksum("ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # Cleanup
        dst_pg.stop()

    def test_basic_delta_catchup(self):
        """
        Test delta catchup
        """
        # preparation 1: source
        src_pg = self.pg_node.make_simple('src',
            set_replication = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question(answer int)")

        # preparation 2: make clean shutdowned lagging behind replica
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        self.set_replica(src_pg, dst_pg)
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start(replica = True)
        dst_pg.stop()

        # preparation 3: make changes on master (source)
        src_pg.pgbench_init(scale = 10)
        pgbench = src_pg.pgbench(options=['-T', '10', '--no-vacuum'])
        pgbench.wait()
        src_pg.safe_psql("postgres", "INSERT INTO ultimate_question VALUES(42)")
        src_query_result = src_pg.table_checksum("ultimate_question")

        # do delta catchup
        self.pb.catchup_node(
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
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start(replica = True)

        # 2nd check: run verification query
        dst_query_result = dst_pg.table_checksum("ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # Cleanup
        dst_pg.stop()
        #self.assertEqual(1, 0, 'Stop test')

    def test_basic_ptrack_catchup(self):
        """
        Test ptrack catchup
        """
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

        # preparation 1: source
        src_pg = self.pg_node.make_simple('src', set_replication=True, ptrack_enable=True)
        src_pg.slow_start()
        src_pg.safe_psql("postgres", "CREATE EXTENSION ptrack")
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question(answer int)")

        # preparation 2: make clean shutdowned lagging behind replica
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        self.set_replica(src_pg, dst_pg)
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start(replica = True)
        dst_pg.stop()

        # preparation 3: make changes on master (source)
        src_pg.pgbench_init(scale = 10)
        pgbench = src_pg.pgbench(options=['-T', '10', '--no-vacuum'])
        pgbench.wait()
        src_pg.safe_psql("postgres", "INSERT INTO ultimate_question VALUES(42)")
        src_query_result = src_pg.table_checksum("ultimate_question")

        # do ptrack catchup
        self.pb.catchup_node(
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
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start(replica = True)

        # 2nd check: run verification query
        dst_query_result = dst_pg.table_checksum("ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # Cleanup
        dst_pg.stop()
        #self.assertEqual(1, 0, 'Stop test')

    def test_tli_delta_catchup(self):
        """
        Test that we correctly follow timeline change with delta catchup
        """
        # preparation 1: source
        src_pg = self.pg_node.make_simple('src',
            set_replication = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()

        # preparation 2: destination
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start()
        dst_pg.stop()

        # preparation 3: promote source
        src_pg.stop()
        self.set_replica(dst_pg, src_pg) # fake replication
        src_pg.slow_start(replica = True)
        src_pg.promote()
        src_pg.safe_psql("postgres", "CREATE TABLE ultimate_question AS SELECT 42 AS answer")
        src_query_result = src_pg.table_checksum("ultimate_question")

        # do catchup (src_tli = 2, dst_tli = 1)
        self.pb.catchup_node(
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
        dst_pg.set_auto_conf(dst_options)
        self.set_replica(master = src_pg, replica = dst_pg)
        dst_pg.slow_start(replica = True)

        # 2nd check: run verification query
        dst_query_result = dst_pg.table_checksum("ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        dst_pg.stop()

        # do catchup (src_tli = 2, dst_tli = 2)
        self.pb.catchup_node(
            backup_mode = 'DELTA',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        # Cleanup
        src_pg.stop()

    def test_tli_ptrack_catchup(self):
        """
        Test that we correctly follow timeline change with ptrack catchup
        """
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

        # preparation 1: source
        src_pg = self.pg_node.make_simple('src', set_replication=True, ptrack_enable=True)
        src_pg.slow_start()
        src_pg.safe_psql("postgres", "CREATE EXTENSION ptrack")

        # preparation 2: destination
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start()
        dst_pg.stop()

        # preparation 3: promote source
        src_pg.stop()
        self.set_replica(dst_pg, src_pg) # fake replication
        src_pg.slow_start(replica = True)
        src_pg.promote()

        src_pg.safe_psql("postgres", "CHECKPOINT") # force postgres to update tli in 'SELECT timeline_id FROM pg_catalog.pg_control_checkpoint()'
        src_tli = src_pg.safe_psql("postgres", "SELECT timeline_id FROM pg_catalog.pg_control_checkpoint()").decode('utf-8').rstrip()
        self.assertEqual(src_tli, "2", "Postgres didn't update TLI after promote")

        src_pg.safe_psql("postgres", "CREATE TABLE ultimate_question AS SELECT 42 AS answer")
        src_query_result = src_pg.table_checksum("ultimate_question")

        # do catchup (src_tli = 2, dst_tli = 1)
        self.pb.catchup_node(
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
        dst_pg.set_auto_conf(dst_options)
        self.set_replica(master = src_pg, replica = dst_pg)
        dst_pg.slow_start(replica = True)

        # 2nd check: run verification query
        dst_query_result = dst_pg.table_checksum("ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        dst_pg.stop()

        # do catchup (src_tli = 2, dst_tli = 2)
        self.pb.catchup_node(
            backup_mode = 'PTRACK',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        # Cleanup
        src_pg.stop()

#########################################
# Test various corner conditions
#########################################
    def test_table_drop_with_delta(self):
        """
        Test that dropped table in source will be dropped in delta catchup'ed instance too
        """
        # preparation 1: source
        src_pg = self.pg_node.make_simple('src',
            set_replication = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question AS SELECT 42 AS answer")

        # preparation 2: make clean shutdowned lagging behind replica
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start()
        dst_pg.stop()

        # preparation 3: make changes on master (source)
        # perform checkpoint twice to ensure, that datafile is actually deleted on filesystem
        src_pg.safe_psql("postgres", "DROP TABLE ultimate_question")
        src_pg.safe_psql("postgres", "CHECKPOINT")
        src_pg.safe_psql("postgres", "CHECKPOINT")

        # do delta catchup
        self.pb.catchup_node(
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

    def test_table_drop_with_ptrack(self):
        """
        Test that dropped table in source will be dropped in ptrack catchup'ed instance too
        """
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

        # preparation 1: source
        src_pg = self.pg_node.make_simple('src',
            set_replication = True,
            ptrack_enable = True)
        src_pg.slow_start()
        src_pg.safe_psql("postgres", "CREATE EXTENSION ptrack")
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question AS SELECT 42 AS answer")

        # preparation 2: make clean shutdowned lagging behind replica
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start()
        dst_pg.stop()

        # preparation 3: make changes on master (source)
        # perform checkpoint twice to ensure, that datafile is actually deleted on filesystem
        src_pg.safe_psql("postgres", "DROP TABLE ultimate_question")
        src_pg.safe_psql("postgres", "CHECKPOINT")
        src_pg.safe_psql("postgres", "CHECKPOINT")

        # do ptrack catchup
        self.pb.catchup_node(
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

    def test_tablefile_truncation_with_delta(self):
        """
        Test that truncated table in source will be truncated in delta catchup'ed instance too
        """
        # preparation 1: source
        src_pg = self.pg_node.make_simple('src',
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
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dest_options = {}
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start()
        dst_pg.stop()

        # preparation 3: make changes on master (source)
        src_pg.safe_psql("postgres", "DELETE FROM t_heap WHERE ctid >= '(11,0)'")
        src_pg.safe_psql("postgres", "VACUUM t_heap")

        # do delta catchup
        self.pb.catchup_node(
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

    def test_tablefile_truncation_with_ptrack(self):
        """
        Test that truncated table in source will be truncated in ptrack catchup'ed instance too
        """
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

        # preparation 1: source
        src_pg = self.pg_node.make_simple('src',
            set_replication = True,
            ptrack_enable = True)
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
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dest_options = {}
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start()
        dst_pg.stop()

        # preparation 3: make changes on master (source)
        src_pg.safe_psql("postgres", "DELETE FROM t_heap WHERE ctid >= '(11,0)'")
        src_pg.safe_psql("postgres", "VACUUM t_heap")

        # do ptrack catchup
        self.pb.catchup_node(
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

#########################################
# Test reaction on user errors
#########################################
    def test_local_tablespace_without_mapping(self):
        """
        Test that we detect absence of needed --tablespace-mapping option
        """
        if self.remote:
            self.skipTest('Skipped because this test tests local catchup error handling')

        src_pg = self.pg_node.make_simple('src')
        src_pg.slow_start()

        tblspace_path = self.get_tblspace_path(src_pg, 'tblspace')
        self.create_tblspace_in_node(
            src_pg, 'tblspace',
            tblspc_path = tblspace_path)

        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question TABLESPACE tblspace AS SELECT 42 AS answer")

        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
                backup_mode = 'FULL',
                source_pgdata = src_pg.data_dir,
                destination_node = dst_pg,
                options = [
                    '-d', 'postgres',
                    '-p', str(src_pg.port),
                    '--stream',
                    ],
                expect_error="because '-T' parameter is not specified"
                )
        self.assertMessage(contains='ERROR: Local catchup executed, but source '
                                    'database contains tablespace')

        # Cleanup
        src_pg.stop()

    def test_running_dest_postmaster(self):
        """
        Test that we detect running postmaster in destination
        """
        # preparation 1: source
        src_pg = self.pg_node.make_simple('src',
            set_replication = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()

        # preparation 2: destination
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start()
        # leave running destination postmaster
        # so don't call dst_pg.stop()

        # try delta catchup
        self.pb.catchup_node(
                backup_mode = 'DELTA',
                source_pgdata = src_pg.data_dir,
                destination_node = dst_pg,
                options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream'],
                expect_error="because postmaster in destination is running"
                )
        self.assertMessage(contains='ERROR: Postmaster with pid ')

        # Cleanup
        src_pg.stop()

    def test_same_db_id(self):
        """
        Test that we detect different id's of source and destination
        """
        # preparation:
        #   source
        src_pg = self.pg_node.make_simple('src',
            set_replication = True
            )
        src_pg.slow_start()
        #   destination
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start()
        dst_pg.stop()
        #   fake destination
        fake_dst_pg = self.pg_node.make_simple('fake_dst')
        #   fake source
        fake_src_pg = self.pg_node.make_simple('fake_src')

        # try delta catchup (src (with correct src conn), fake_dst)
        self.pb.catchup_node(
                backup_mode = 'DELTA',
                source_pgdata = src_pg.data_dir,
                destination_node = fake_dst_pg,
                options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream'],
                expect_error="because database identifiers mismatch"
                )
        self.assertMessage(contains='ERROR: Database identifiers mismatch: ')

        # try delta catchup (fake_src (with wrong src conn), dst)
        self.pb.catchup_node(
                backup_mode = 'DELTA',
                source_pgdata = fake_src_pg.data_dir,
                destination_node = dst_pg,
                options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream'],
                expect_error="because database identifiers mismatch"
                )
        self.assertMessage(contains='ERROR: Database identifiers mismatch: ')

        # Cleanup
        src_pg.stop()

    def test_tli_destination_mismatch(self):
        """
        Test that we detect TLI mismatch in destination
        """
        # preparation 1: source
        src_pg = self.pg_node.make_simple('src',
            set_replication = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()

        # preparation 2: destination
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        self.set_replica(src_pg, dst_pg)
        dst_pg.slow_start(replica = True)
        dst_pg.promote()
        dst_pg.stop()

        # preparation 3: "useful" changes
        src_pg.safe_psql("postgres", "CREATE TABLE ultimate_question AS SELECT 42 AS answer")

        # try catchup
        self.pb.catchup_node(
                backup_mode = 'DELTA',
                source_pgdata = src_pg.data_dir,
                destination_node = dst_pg,
                options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream'],
                expect_error="because of stale timeline",
                )
        self.assertMessage(contains='ERROR: Source is behind destination in timeline history')

        # Cleanup
        src_pg.stop()

    def test_tli_source_mismatch(self):
        """
        Test that we detect TLI mismatch in source history
        """
        # preparation 1: source
        src_pg = self.pg_node.make_simple('src',
            set_replication = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()

        # preparation 2: fake source (promouted copy)
        fake_src_pg = self.pg_node.make_empty('fake_src')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = fake_src_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        fake_src_options = {}
        fake_src_options['port'] = str(fake_src_pg.port)
        fake_src_pg.set_auto_conf(fake_src_options)
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
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start()
        dst_pg.stop()

        # preparation 4: "useful" changes
        src_pg.safe_psql("postgres", "CREATE TABLE ultimate_question AS SELECT 42 AS answer")

        # try catchup
        self.pb.catchup_node(
            backup_mode = 'DELTA',
            source_pgdata = fake_src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(fake_src_pg.port), '--stream'],
            expect_error="because of future timeline",
            )
        self.assertMessage(contains='ERROR: Destination is not in source timeline history')

        # Cleanup
        src_pg.stop()
        fake_src_pg.stop()

#########################################
# Test unclean destination
#########################################
    def test_unclean_delta_catchup(self):
        """
        Test that we correctly recover uncleanly shutdowned destination
        """
        # preparation 1: source
        src_pg = self.pg_node.make_simple('src',
            set_replication = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question(answer int)")

        # preparation 2: destination
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        # try #1
        self.pb.catchup_node(
                backup_mode = 'DELTA',
                source_pgdata = src_pg.data_dir,
                destination_node = dst_pg,
                options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream'],
                expect_error="because destination pg is not cleanly shutdowned"
                )
        self.assertMessage(contains='ERROR: Destination directory contains "backup_label" file')

        # try #2
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start()
        self.assertNotEqual(dst_pg.pid, 0, "Cannot detect pid of running postgres")
        dst_pg.kill()

        # preparation 3: make changes on master (source)
        src_pg.pgbench_init(scale = 10)
        pgbench = src_pg.pgbench(options=['-T', '10', '--no-vacuum'])
        pgbench.wait()
        src_pg.safe_psql("postgres", "INSERT INTO ultimate_question VALUES(42)")
        src_query_result = src_pg.table_checksum("ultimate_question")

        # do delta catchup
        self.pb.catchup_node(
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
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start(replica = True)

        # 2nd check: run verification query
        dst_query_result = dst_pg.table_checksum("ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # Cleanup
        dst_pg.stop()

    def test_unclean_ptrack_catchup(self):
        """
        Test that we correctly recover uncleanly shutdowned destination
        """
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

        # preparation 1: source
        src_pg = self.pg_node.make_simple('src',
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
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        # try #1
        self.pb.catchup_node(
                backup_mode = 'PTRACK',
                source_pgdata = src_pg.data_dir,
                destination_node = dst_pg,
                options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream'],
                expect_error="because destination pg is not cleanly shutdowned"
                )
        self.assertMessage(contains='ERROR: Destination directory contains "backup_label" file')

        # try #2
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start()
        self.assertNotEqual(dst_pg.pid, 0, "Cannot detect pid of running postgres")
        dst_pg.kill()

        # preparation 3: make changes on master (source)
        src_pg.pgbench_init(scale = 10)
        pgbench = src_pg.pgbench(options=['-T', '10', '--no-vacuum'])
        pgbench.wait()
        src_pg.safe_psql("postgres", "INSERT INTO ultimate_question VALUES(42)")
        src_query_result = src_pg.table_checksum("ultimate_question")

        # do delta catchup
        self.pb.catchup_node(
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
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start(replica = True)

        # 2nd check: run verification query
        dst_query_result = dst_pg.table_checksum("ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # Cleanup
        dst_pg.stop()

#########################################
# Test replication slot logic
#
#   -S, --slot=SLOTNAME                        replication slot to use
#       --temp-slot                                    use temporary replication slot
#   -P  --perm-slot              create permanent replication slot
#       --primary-slot-name=SLOTNAME value for primary_slot_name parameter
#
# 1. if "--slot" is used - try to use already existing slot with given name
# 2. if "--slot" and "--perm-slot" used - try to create permanent slot and use it.
# 3. If "--perm-slot " flag is used without "--slot" option - use generic slot name like "pg_probackup_perm_slot"
# 4. If "--perm-slot " flag is used and permanent slot already exists - fail with error.
# 5. "--perm-slot" and "--temp-slot" flags cannot be used together.
#########################################
    def test_catchup_with_replication_slot(self):
        """
        """
        # preparation
        src_pg = self.pg_node.make_simple('src',
            set_replication = True
            )
        src_pg.slow_start()

        # 1a. --slot option
        dst_pg = self.pg_node.make_empty('dst_1a')
        self.pb.catchup_node(
                backup_mode = 'FULL',
                source_pgdata = src_pg.data_dir,
                destination_node = dst_pg,
                options = [
                    '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                    '--slot=nonexistentslot_1a', '--temp-slot=false'
                    ],
                expect_error="because replication slot does not exist"
                )
        self.assertMessage(contains='ERROR:  replication slot "nonexistentslot_1a" does not exist')

	# 1b. --slot option
        dst_pg = self.pg_node.make_empty('dst_1b')
        src_pg.safe_psql("postgres", "SELECT pg_catalog.pg_create_physical_replication_slot('existentslot_1b')")
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = [
                '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                '--slot=existentslot_1b', '--temp-slot=false'
                ]
            )

        # 2a. --slot --perm-slot
        dst_pg = self.pg_node.make_empty('dst_2a')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = [
                '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                '--slot=nonexistentslot_2a',
                '--perm-slot'
                ]
            )

        # 2b. and 4. --slot --perm-slot
        dst_pg = self.pg_node.make_empty('dst_2b')
        src_pg.safe_psql("postgres", "SELECT pg_catalog.pg_create_physical_replication_slot('existentslot_2b')")
        self.pb.catchup_node(
                backup_mode = 'FULL',
                source_pgdata = src_pg.data_dir,
                destination_node = dst_pg,
                options = [
                    '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                    '--slot=existentslot_2b',
                    '--perm-slot'
                    ],
                expect_error="because replication slot already exist"
                )
        self.assertMessage(contains='ERROR:  replication slot "existentslot_2b" already exists')

        # 3. --perm-slot --slot
        dst_pg = self.pg_node.make_empty('dst_3')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = [
                '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                '--perm-slot'
                ]
            )
        slot_name = src_pg.safe_psql(
            "postgres",
            "SELECT slot_name FROM pg_catalog.pg_replication_slots "
            "WHERE slot_name NOT LIKE '%existentslot%' "
            "AND slot_type = 'physical'"
            ).decode('utf-8').rstrip()
        self.assertEqual(slot_name, 'pg_probackup_perm_slot', 'Slot name mismatch')

        # 5. --perm-slot --temp-slot
        dst_pg = self.pg_node.make_empty('dst_5a')
        self.pb.catchup_node(
                backup_mode = 'FULL',
                source_pgdata = src_pg.data_dir,
                destination_node = dst_pg,
                options = [
                    '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                    '--perm-slot',
                    '--temp-slot'
                    ],
                expect_error="because conflicting options --perm-slot and --temp-slot used together"
                )
        self.assertMessage(contains='ERROR: You cannot specify "--perm-slot" option with the "--temp-slot" option')

        dst_pg = self.pg_node.make_empty('dst_5b')
        self.pb.catchup_node(
                backup_mode = 'FULL',
                source_pgdata = src_pg.data_dir,
                destination_node = dst_pg,
                options = [
                    '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                    '--perm-slot',
                    '--temp-slot=true'
                    ],
                expect_error="because conflicting options --perm-slot and --temp-slot used together"
                )
        self.assertMessage(contains='ERROR: You cannot specify "--perm-slot" option with the "--temp-slot" option')

        dst_pg = self.pg_node.make_empty('dst_5c')
        self.pb.catchup_node(
                backup_mode = 'FULL',
                source_pgdata = src_pg.data_dir,
                destination_node = dst_pg,
                options = [
                    '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                    '--perm-slot',
                    '--temp-slot=false',
                    '--slot=dst_5c'
                    ],
                )

#########################################
# --exclude-path
#########################################
    def test_catchup_with_exclude_path(self):
        """
        various syntetic tests for --exclude-path option
        """
        # preparation
        src_pg = self.pg_node.make_simple('src',
            set_replication = True
            )
        src_pg.slow_start()

        # test 1
        os.mkdir(os.path.join(src_pg.data_dir, 'src_usefull_dir'))
        with open(os.path.join(os.path.join(src_pg.data_dir, 'src_usefull_dir', 'src_garbage_file')), 'w') as f:
            f.write('garbage')
            f.flush()
            f.close
        os.mkdir(os.path.join(src_pg.data_dir, 'src_garbage_dir'))
        with open(os.path.join(os.path.join(src_pg.data_dir, 'src_garbage_dir', 'src_garbage_file')), 'w') as f:
            f.write('garbage')
            f.flush()
            f.close

        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = [
                '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                '--exclude-path={0}'.format(os.path.join(src_pg.data_dir, 'src_usefull_dir', 'src_garbage_file')),
                '-x', '{0}'.format(os.path.join(src_pg.data_dir, 'src_garbage_dir')),
                ]
            )

        self.assertTrue(Path(os.path.join(dst_pg.data_dir, 'src_usefull_dir')).exists())
        self.assertFalse(Path(os.path.join(dst_pg.data_dir, 'src_usefull_dir', 'src_garbage_file')).exists())
        self.assertFalse(Path(os.path.join(dst_pg.data_dir, 'src_garbage_dir')).exists())

        self.set_replica(src_pg, dst_pg)
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start(replica = True)
        dst_pg.stop()

        # test 2
        os.mkdir(os.path.join(dst_pg.data_dir, 'dst_garbage_dir'))
        os.mkdir(os.path.join(dst_pg.data_dir, 'dst_usefull_dir'))
        with open(os.path.join(os.path.join(dst_pg.data_dir, 'dst_usefull_dir', 'dst_usefull_file')), 'w') as f:
            f.write('gems')
            f.flush()
            f.close

        self.pb.catchup_node(
            backup_mode = 'DELTA',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = [
                '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                '--exclude-path=src_usefull_dir/src_garbage_file',
                '--exclude-path=src_garbage_dir',
                '--exclude-path={0}'.format(os.path.join(dst_pg.data_dir, 'dst_usefull_dir')),
                ]
            )

        self.assertTrue(Path(os.path.join(dst_pg.data_dir, 'src_usefull_dir')).exists())
        self.assertFalse(Path(os.path.join(dst_pg.data_dir, 'src_usefull_dir', 'src_garbage_file')).exists())
        self.assertFalse(Path(os.path.join(dst_pg.data_dir, 'src_garbage_dir')).exists())
        self.assertFalse(Path(os.path.join(dst_pg.data_dir, 'dst_garbage_dir')).exists())
        self.assertTrue(Path(os.path.join(dst_pg.data_dir, 'dst_usefull_dir', 'dst_usefull_file')).exists())

        #self.assertEqual(1, 0, 'Stop test')
        src_pg.stop()

    def test_config_exclusion(self):
        """
        Test that catchup can preserve dest replication config
        """
        # preparation 1: source
        src_pg = self.pg_node.make_simple('src',
            set_replication = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question(answer int)")

        # preparation 2: make lagging behind replica
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        self.set_replica(src_pg, dst_pg)
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg._assign_master(src_pg)
        dst_pg.slow_start(replica = True)
        dst_pg.stop()

        # preparation 3: make changes on master (source)
        src_pg.pgbench_init(scale = 10)
        pgbench = src_pg.pgbench(options=['-T', '2', '--no-vacuum'])
        pgbench.wait()

        # test 1: do delta catchup with relative exclusion paths
        self.pb.catchup_node(
            backup_mode = 'DELTA',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = [
                '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                '--exclude-path=postgresql.conf',
                '--exclude-path=postgresql.auto.conf',
                '--exclude-path=recovery.conf',
                ]
            )

        # run&recover catchup'ed instance
        # don't set destination db port and recover options
        dst_pg.slow_start(replica = True)

        # check: run verification query
        src_pg.safe_psql("postgres", "INSERT INTO ultimate_question VALUES(42)")
        src_query_result = src_pg.table_checksum("ultimate_question")
        dst_pg.catchup() # wait for replication
        dst_query_result = dst_pg.table_checksum("ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # preparation 4: make changes on master (source)
        dst_pg.stop()
        #src_pg.pgbench_init(scale = 10)
        pgbench = src_pg.pgbench(options=['-T', '2', '--no-vacuum'])
        pgbench.wait()

        # test 2: do delta catchup with absolute source exclusion paths
        self.pb.catchup_node(
            backup_mode = 'DELTA',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = [
                '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                '--exclude-path={0}/postgresql.conf'.format(src_pg.data_dir),
                '--exclude-path={0}/postgresql.auto.conf'.format(src_pg.data_dir),
                '--exclude-path={0}/recovery.conf'.format(src_pg.data_dir),
                ]
            )

        # run&recover catchup'ed instance
        # don't set destination db port and recover options
        dst_pg.slow_start(replica = True)

        # check: run verification query
        src_pg.safe_psql("postgres", "INSERT INTO ultimate_question VALUES(2*42)")
        src_query_result = src_pg.table_checksum("ultimate_question")
        dst_pg.catchup() # wait for replication
        dst_query_result = dst_pg.table_checksum("ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # preparation 5: make changes on master (source)
        dst_pg.stop()
        pgbench = src_pg.pgbench(options=['-T', '2', '--no-vacuum'])
        pgbench.wait()

        # test 3: do delta catchup with absolute destination exclusion paths
        self.pb.catchup_node(
            backup_mode = 'DELTA',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = [
                '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                '--exclude-path={0}/postgresql.conf'.format(dst_pg.data_dir),
                '--exclude-path={0}/postgresql.auto.conf'.format(dst_pg.data_dir),
                '--exclude-path={0}/recovery.conf'.format(dst_pg.data_dir),
                ]
            )

        # run&recover catchup'ed instance
        # don't set destination db port and recover options
        dst_pg.slow_start(replica = True)

        # check: run verification query
        src_pg.safe_psql("postgres", "INSERT INTO ultimate_question VALUES(3*42)")
        src_query_result = src_pg.table_checksum("ultimate_question")
        dst_pg.catchup() # wait for replication
        dst_query_result = dst_pg.table_checksum("ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # Cleanup
        src_pg.stop()
        dst_pg.stop()
        #self.assertEqual(1, 0, 'Stop test')

#########################################
# --dry-run
#########################################
    def test_dry_run_catchup_full(self):
        """
        Test dry-run option for full catchup
        """
        # preparation 1: source
        src_pg = self.pg_node.make_simple('src',
            set_replication = True
            )
        src_pg.slow_start()

        # preparation 2: make clean shutdowned lagging behind replica
        dst_pg = self.pg_node.make_empty('dst')

        src_pg.pgbench_init(scale = 10)
        pgbench = src_pg.pgbench(options=['-T', '10', '--no-vacuum'])
        pgbench.wait()

        # save the condition before dry-run
        content_before = self.pgdata_content(dst_pg.data_dir)

        # do full catchup
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream', '--dry-run']
            )

        # compare data dirs before and after catchup
        self.compare_pgdata(
            content_before,
            self.pgdata_content(dst_pg.data_dir)
            )

        # Cleanup
        src_pg.stop()

    def test_dry_run_catchup_ptrack(self):
        """
        Test dry-run option for catchup in incremental ptrack mode
        """
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

         # preparation 1: source
        src_pg = self.pg_node.make_simple('src',
            set_replication = True,
            ptrack_enable = True)
        src_pg.slow_start()
        src_pg.safe_psql("postgres", "CREATE EXTENSION ptrack")

        src_pg.pgbench_init(scale = 10)
        pgbench = src_pg.pgbench(options=['-T', '10', '--no-vacuum'])
        pgbench.wait()

        # preparation 2: make clean shutdowned lagging behind replica
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        self.set_replica(src_pg, dst_pg)
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start(replica = True)
        dst_pg.stop()

        # save the condition before dry-run
        content_before = self.pgdata_content(dst_pg.data_dir)

        # do incremental catchup
        self.pb.catchup_node(
            backup_mode = 'PTRACK',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream', '--dry-run']
            )

        # compare data dirs before and after cathup
        self.compare_pgdata(
            content_before,
            self.pgdata_content(dst_pg.data_dir)
            )

        # Cleanup
        src_pg.stop()

    def test_dry_run_catchup_delta(self):
        """
        Test dry-run option for catchup in incremental delta mode
        """

        # preparation 1: source
        src_pg = self.pg_node.make_simple('src',
            set_replication = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()

        src_pg.pgbench_init(scale = 10)
        pgbench = src_pg.pgbench(options=['-T', '10', '--no-vacuum'])
        pgbench.wait()

         # preparation 2: make clean shutdowned lagging behind replica
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )
        self.set_replica(src_pg, dst_pg)
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start(replica = True)
        dst_pg.stop()

        # save the condition before dry-run
        content_before = self.pgdata_content(dst_pg.data_dir)

        # do delta catchup
        self.pb.catchup_node(
            backup_mode = 'DELTA',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream', "--dry-run"]
            )

        # compare data dirs before and after cathup
        self.compare_pgdata(
            content_before,
            self.pgdata_content(dst_pg.data_dir)
            )

        # Cleanup
        src_pg.stop()

    def test_pgdata_is_ignored(self):
        """ In catchup we still allow PGDATA to be set either from command line
        or from the env var. This test that PGDATA is actually ignored and
        --source-pgadta is used instead
        """
        node = self.pg_node.make_simple('node',
            set_replication = True
            )
        node.slow_start()

        # do full catchup
        dest = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = node.data_dir,
            destination_node = dest,
            options = ['-d', 'postgres', '-p', str(node.port), '--stream', '--pgdata=xxx']
            )

        self.compare_pgdata(
            self.pgdata_content(node.data_dir),
            self.pgdata_content(dest.data_dir)
            )

        self.test_env['PGDATA']='xxx'

        dest2 = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = node.data_dir,
            destination_node = dest2,
            options = ['-d', 'postgres', '-p', str(node.port), '--stream']
            )

        self.compare_pgdata(
            self.pgdata_content(node.data_dir),
            self.pgdata_content(dest2.data_dir)
            )

    def test_catchup_from_standby_single_wal(self):
        """ Make a standby node, with a single WAL file in it and try to catchup """
        node = self.pg_node.make_simple('node',
                                        pg_options={'hot_standby': 'on'})
        node.set_auto_conf({}, 'postgresql.conf', ['max_worker_processes'])
        standby_signal = os.path.join(node.data_dir, 'standby.signal')
        with open(standby_signal, 'w') as fout:
            fout.flush()
            fout.close()
        node.start()

        # No inserts to keep WAL size small

        dest = self.pg_node.make_empty('dst')

        self.pb.catchup_node(
            backup_mode='FULL',
            source_pgdata=node.data_dir,
            destination_node=dest,
            options = ['-d', 'postgres', '-p', str(node.port), '--stream']
        )

        dst_options = {}
        dst_options['port'] = str(dest.port)
        dest.set_auto_conf(dst_options)

        dest.slow_start()
        res = dest.safe_psql("postgres", "select 1").decode('utf-8').strip()
        self.assertEqual(res, "1")

    def test_catchup_ptrack_unlogged(self):
        """ catchup + ptrack when unlogged tables exist """
        node = self.pg_node.make_simple('node', ptrack_enable = True)
        node.slow_start()
        node.safe_psql("postgres", "CREATE EXTENSION ptrack")

        dest = self.pg_node.make_empty('dst')

        self.pb.catchup_node(
            backup_mode='FULL',
            source_pgdata=node.data_dir,
            destination_node=dest,
            options = ['-d', 'postgres', '-p', str(node.port), '--stream']
        )

        for i in range(1,7):
            node.safe_psql('postgres', 'create unlogged table t' + str(i) + ' (id int, name text);')

        dst_options = {}
        dst_options['port'] = str(dest.port)
        dest.set_auto_conf(dst_options)

        dest.slow_start()
        dest.stop()

        self.pb.catchup_node(
            backup_mode = 'PTRACK',
            source_pgdata = node.data_dir,
            destination_node = dest,
            options = ['-d', 'postgres', '-p', str(node.port), '--stream', '--dry-run']
            )

        return

    def test_catchup_instance_from_the_past(self):
        src_pg = self.pg_node.make_simple('src',
                                          set_replication=True
                                          )
        src_pg.slow_start()
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode='FULL',
            source_pgdata=src_pg.data_dir,
            destination_node=dst_pg,
            options=['-d', 'postgres', '-p', str(src_pg.port), '--stream']
        )
        dst_options = {'port': str(dst_pg.port)}
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start()
        dst_pg.pgbench_init(scale=10)
        pgbench = dst_pg.pgbench(
            stdout=subprocess.PIPE,
            options=["-c", "4", "-T", "20"])
        pgbench.wait()
        pgbench.stdout.close()
        dst_pg.stop()
        self.pb.catchup_node(
            backup_mode='DELTA',
            source_pgdata=src_pg.data_dir,
            destination_node=dst_pg,
            options=[
                '-d', 'postgres',
                '-p', str(src_pg.port),
                '--stream'
            ],
            expect_error="because instance is from the past"
        )

        self.assertMessage(regex='ERROR: Current START LSN .* is lower than SYNC LSN')
        self.assertMessage(contains='it may indicate that we are trying to catchup '
                                    'with PostgreSQL instance from the past')


#########################################
# --waldir
#########################################

    def test_waldir_option(self):
        """
        Test waldir option for full catchup
        """
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')
        # preparation: source
        src_pg = self.pg_node.make_simple('src',
                                        set_replication = True,
                                        ptrack_enable = True
            )
        src_pg.slow_start()
        src_pg.safe_psql("postgres", "CREATE EXTENSION ptrack")

        # do full catchup
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = [
                '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                '--waldir={0}'.format(os.path.join(self.test_path, 'tmp_new_wal_dir')),
                ]
            )

        # 1st check: compare data directories
        self.compare_pgdata(
            self.pgdata_content(src_pg.data_dir),
            self.pgdata_content(dst_pg.data_dir)
            )

        # 2nd check new waldir exists
        self.assertTrue(Path(os.path.join(self.test_path, 'tmp_new_wal_dir')).exists())

        #3rd check pg_wal is symlink
        if src_pg.major_version >= 10:
            wal_path = os.path.join(dst_pg.data_dir, "pg_wal")
        else:
            wal_path = os.path.join(dst_pg.data_dir, "pg_xlog")

        self.assertEqual(os.path.islink(wal_path), True)
        print("FULL DONE ----------------------------------------------------------")

        """
        Test waldir otion for delta catchup to different directory from full catchup's wal directory
        """
        # preparation 2: make clean shutdowned lagging behind replica

        self.set_replica(src_pg, dst_pg)
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start(replica=True)
        dst_pg.stop()

        # do delta catchup
        self.pb.catchup_node(
            backup_mode='DELTA',
            source_pgdata=src_pg.data_dir,
            destination_node=dst_pg,
            options=['-d', 'postgres', '-p', str(src_pg.port), '--stream',
                     '--waldir={0}'.format(os.path.join(self.test_path, 'tmp_another_wal_dir')),
                     ],
            expect_error="because we perform DELTA catchup's WAL in a different dir from FULL catchup's WAL dir",
        )
        self.assertMessage(contains='ERROR: WAL directory does not egual to symlinked pg_wal path')

        print("ANOTHER DIR DONE ------------------------------------------------")

        """
        Test waldir otion to delta catchup
        """

        self.set_replica(src_pg, dst_pg)
        dst_pg._assign_master(src_pg)
        dst_pg.slow_start(replica = True)
        dst_pg.stop()

        # preparation 3: make changes on master (source)
        src_pg.pgbench_init(scale = 10)
        pgbench = src_pg.pgbench(options=['-T', '2', '--no-vacuum'])
        pgbench.wait()

        # do delta catchup
        self.pb.catchup_node(
            backup_mode = 'DELTA',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream',
                       '--waldir={0}'.format(os.path.join(self.test_path, 'tmp_new_wal_dir')),
                       ],
        )

        # 1st check: compare data directories
        self.compare_pgdata(
            self.pgdata_content(src_pg.data_dir),
            self.pgdata_content(dst_pg.data_dir)
        )

        # 2nd check new waldir exists
        self.assertTrue(Path(os.path.join(self.test_path, 'tmp_new_wal_dir')).exists())

        #3rd check pg_wal is symlink
        if src_pg.major_version >= 10:
            wal_path = os.path.join(dst_pg.data_dir, "pg_wal")
        else:
            wal_path = os.path.join(dst_pg.data_dir, "pg_xlog")

        self.assertEqual(os.path.islink(wal_path), True)

        print ("DELTA DONE---------------------------------------------------------")

        """
        Test waldir option for catchup in incremental ptrack mode
        """
        self.set_replica(src_pg, dst_pg)
        dst_pg.slow_start(replica = True)
        dst_pg.stop()

        # preparation 3: make changes on master (source)
        src_pg.pgbench_init(scale = 10)
        pgbench = src_pg.pgbench(options=['-T', '2', '--no-vacuum'])
        pgbench.wait()

        # do incremental catchup
        self.pb.catchup_node(
            backup_mode = 'PTRACK',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream',
                       '--waldir={0}'.format(os.path.join(self.test_path, 'tmp_new_wal_dir')),
                       ]
        )

        # 1st check: compare data directories
        self.compare_pgdata(
            self.pgdata_content(src_pg.data_dir),
            self.pgdata_content(dst_pg.data_dir)
        )

        # 2nd check new waldir exists
        self.assertTrue(Path(os.path.join(self.test_path, 'tmp_new_wal_dir')).exists())

        #3rd check pg_wal is symlink
        if src_pg.major_version >= 10:
            wal_path = os.path.join(dst_pg.data_dir, "pg_wal")
        else:
            wal_path = os.path.join(dst_pg.data_dir, "pg_xlog")

        self.assertEqual(os.path.islink(wal_path), True)

        print ("PTRACK DONE -----------------------------------------------------------")

        """
        Test waldir option for full catchup to not empty WAL directory
        """

        dst_pg = self.pg_node.make_empty('dst2')
        self.pb.catchup_node(
            backup_mode='FULL',
            source_pgdata=src_pg.data_dir,
            destination_node=dst_pg,
            options=[
                '-d', 'postgres', '-p', str(src_pg.port), '--stream',
                '--waldir={0}'.format(os.path.join(self.test_path, 'tmp_new_wal_dir')),
            ],

            expect_error="because full catchup's WAL must be perform into empty directory",
        )
        self.assertMessage(contains='ERROR: Can\'t perform FULL catchup with non-empty pg_wal directory')

        print ("ANOTHER FULL DONE -----------------------------------------------------")
        # Cleanup
        src_pg.stop()


    def test_waldir_delta_catchup_without_full(self):
        """
        Test waldir otion with delta catchup without using it doing full
        """
        # preparation 1: source
        src_pg = self.pg_node.make_simple('src',
            set_replication = True,
            pg_options = { 'wal_log_hints': 'on' }
            )
        src_pg.slow_start()

        # preparation 2: make clean shutdowned lagging behind replica
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream',
                       ],
            )
        self.set_replica(src_pg, dst_pg)
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start(replica = True)
        dst_pg.stop()

        # do delta catchup
        self.pb.catchup_node(
            backup_mode = 'DELTA',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream',
                '--waldir={0}'.format(os.path.join(self.test_path, 'tmp_new_wal_dir')),
            ],
            expect_error="because we didn't perform FULL catchup's WAL before DELTA catchup",
        )
        self.assertMessage(contains='ERROR: Unable to read pg_wal symbolic link')

        # Cleanup
        src_pg.stop()


    def test_waldir_dry_run_catchup_full(self):
        """
        Test waldir with dry-run option for full catchup
        """
        # preparation 1: source
        src_pg = self.pg_node.make_simple('src',
                                          set_replication = True,
                                          pg_options = { 'wal_log_hints': 'on' }
                                          )
        src_pg.slow_start()

        # preparation 2: make clean shutdowned lagging behind replica
        dst_pg = self.pg_node.make_empty('dst')

        src_pg.pgbench_init(scale = 10)
        pgbench = src_pg.pgbench(options=['-T', '10', '--no-vacuum'])
        pgbench.wait()

        # save the condition before dry-run
        content_before = self.pgdata_content(dst_pg.data_dir)

        # do full catchup
        self.pb.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = ['-d', 'postgres', '-p', str(src_pg.port), '--stream', '--dry-run',
                       '--waldir={0}'.format(os.path.join(self.test_path, 'tmp_new_wal_dir')),
                       ]
        )

        # compare data dirs before and after catchup
        self.compare_pgdata(
            content_before,
            self.pgdata_content(dst_pg.data_dir)
        )
        self.assertFalse(Path(os.path.join(self.test_path, 'tmp_new_wal_dir')).exists())
        # Cleanup
        src_pg.stop()

