import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from datetime import datetime, timedelta
from testgres import QueryException
import subprocess
import time
from threading import Thread


module_name = 'delta'


class DeltaTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    def test_basic_delta_vacuum_truncate(self):
        """
        make node, create table, take full backup,
        delete last 3 pages, vacuum relation,
        take delta backup, take second delta backup,
        restore latest delta backup and check data correctness
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node_restored.cleanup()
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create sequence t_seq; "
            "create table t_heap as select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1024) i;")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        node.safe_psql(
            "postgres",
            "delete from t_heap where ctid >= '(11,0)'")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        pgdata = self.pgdata_content(node.data_dir)

        self.restore_node(
            backup_dir, 'node', node_restored)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()

        # Clean after yourself
        self.del_test_dir(module_name, fname, [node, node_restored])

    # @unittest.skip("skip")
    def test_delta_vacuum_truncate_1(self):
        """
        make node, create table, take full backup,
        delete last 3 pages, vacuum relation,
        take delta backup, take second delta backup,
        restore latest delta backup and check data correctness
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'
            }
        )
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'),
        )

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
            "from generate_series(0,1024) i;"
        )

        node.safe_psql(
            "postgres",
            "vacuum t_heap"
        )

        self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "delete from t_heap where ctid >= '(11,0)'"
        )

        node.safe_psql(
            "postgres",
            "vacuum t_heap"
        )

        self.backup_node(
            backup_dir, 'node', node, backup_type='delta'
        )

        self.backup_node(
            backup_dir, 'node', node, backup_type='delta'
        )

        pgdata = self.pgdata_content(node.data_dir)

        old_tablespace = self.get_tblspace_path(node, 'somedata')
        new_tablespace = self.get_tblspace_path(node_restored, 'somedata_new')

        self.restore_node(
            backup_dir,
            'node',
            node_restored,
            options=[
                "-T", "{0}={1}".format(
                    old_tablespace, new_tablespace)]
        )

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_delta_vacuum_truncate_2(self):
        """
        make node, create table, take full backup,
        delete last 3 pages, vacuum relation,
        take delta backup, take second delta backup,
        restore latest delta backup and check data correctness
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'
            }
        )
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'),
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node_restored.cleanup()
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10100000) i;"
        )
        filepath = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')"
        ).decode('utf-8').rstrip()

        self.backup_node(backup_dir, 'node', node)

        print(os.path.join(node.data_dir, filepath + '.1'))
        os.unlink(os.path.join(node.data_dir, filepath + '.1'))

        self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        pgdata = self.pgdata_content(node.data_dir)

        self.restore_node(
            backup_dir, 'node', node_restored)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_delta_stream(self):
        """
        make archive node, take full and delta stream backups,
        restore them and check data correctness
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s'
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
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(0,100) i")

        full_result = node.execute("postgres", "SELECT * FROM t_heap")
        full_backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='full', options=['--stream'])

        # delta BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(100,200) i")
        delta_result = node.execute("postgres", "SELECT * FROM t_heap")
        delta_backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta', options=['--stream'])

        # Drop Node
        node.cleanup()

        # Check full backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(
                backup_dir, 'node', node,
                backup_id=full_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"]),
            '\n Unexpected Error Message: {0}\n'
            ' CMD: {1}'.format(repr(self.output), self.cmd))
        node.slow_start()
        full_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check delta backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(delta_backup_id),
            self.restore_node(
                backup_dir, 'node', node,
                backup_id=delta_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"]),
            '\n Unexpected Error Message: {0}\n'
            ' CMD: {1}'.format(repr(self.output), self.cmd))
        node.slow_start()
        delta_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(delta_result, delta_result_new)
        node.cleanup()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_delta_archive(self):
        """
        make archive node, take full and delta archive backups,
        restore them and check data correctness
        """
        self.maxDiff = None
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

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,1) i")
        full_result = node.execute("postgres", "SELECT * FROM t_heap")
        full_backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='full')

        # delta BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,2) i")
        delta_result = node.execute("postgres", "SELECT * FROM t_heap")
        delta_backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        # Drop Node
        node.cleanup()

        # Restore and check full backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(
                backup_dir, 'node', node,
                backup_id=full_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                repr(self.output), self.cmd))
        node.slow_start()
        full_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Restore and check delta backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(delta_backup_id),
            self.restore_node(
                backup_dir, 'node', node,
                backup_id=delta_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                repr(self.output), self.cmd))
        node.slow_start()
        delta_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(delta_result, delta_result_new)
        node.cleanup()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_delta_multiple_segments(self):
        """
        Make node, create table with multiple segments,
        write some data to it, check delta and data correctness
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
                'full_page_writes': 'off'
            }
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        # self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'somedata')

        # CREATE TABLE
        node.pgbench_init(
            scale=100,
            options=['--tablespace=somedata', '--no-vacuum'])
        # FULL BACKUP
        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        # PGBENCH STUFF
        pgbench = node.pgbench(options=['-T', '50', '-c', '1', '--no-vacuum'])
        pgbench.wait()
        node.safe_psql("postgres", "checkpoint")

        # GET LOGICAL CONTENT FROM NODE
        result = node.safe_psql("postgres", "select count(*) from pgbench_accounts")
        # delta BACKUP
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta', options=['--stream'])
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
                "-j", "4", "-T", "{0}={1}".format(
                    tblspc_path, tblspc_path_new)])

        # GET PHYSICAL CONTENT FROM NODE_RESTORED
        pgdata_restored = self.pgdata_content(restored_node.data_dir)

        # START RESTORED NODE
        self.set_auto_conf(restored_node, {'port': restored_node.port})
        restored_node.slow_start()

        result_new = restored_node.safe_psql(
            "postgres",
            "select count(*) from pgbench_accounts")

        # COMPARE RESTORED FILES
        self.assertEqual(result, result_new, 'data is lost')

        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_delta_vacuum_full(self):
        """
        make node, make full and delta stream backups,
        restore them and check data correctness
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()
        self.create_tblspace_in_node(node, 'somedata')

        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        node.safe_psql(
            "postgres",
            "create table t_heap tablespace somedata as select i"
            " as id from generate_series(0,1000000) i"
            )

        pg_connect = node.connect("postgres", autocommit=True)

        gdb = self.gdb_attach(pg_connect.pid)
        gdb.set_breakpoint('reform_and_rewrite_tuple')

        gdb.continue_execution_until_running()

        process = Thread(
            target=pg_connect.execute, args=["VACUUM FULL t_heap"])
        process.start()

        while not gdb.stopped_in_breakpoint:
            sleep(1)

        gdb.continue_execution_until_break(20)

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta', options=['--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        gdb.remove_all_breakpoints()
        gdb._execute('detach')
        process.join()

        old_tablespace = self.get_tblspace_path(node, 'somedata')
        new_tablespace = self.get_tblspace_path(node_restored, 'somedata_new')

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=["-j", "4", "-T", "{0}={1}".format(
                old_tablespace, new_tablespace)])

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        self.set_auto_conf(node_restored, {'port': node_restored.port})

        node_restored.slow_start()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_create_db(self):
        """
        Make node, take full backup, create database db1, take delta backup,
        restore database and check it presense
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'max_wal_size': '10GB',
                'autovacuum': 'off'
            }
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,100) i")

        node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.backup_node(
            backup_dir, 'node', node,
            options=["--stream"])

        # CREATE DATABASE DB1
        node.safe_psql("postgres", "create database db1")
        node.safe_psql(
            "db1",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,100) i")

        # DELTA BACKUP
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta',
            options=["--stream"]
        )

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # RESTORE
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored')
        )

        node_restored.cleanup()
        self.restore_node(
            backup_dir,
            'node',
            node_restored,
            backup_id=backup_id,
            options=[
                "-j", "4",
                "--immediate",
                "--recovery-target-action=promote"])

        # COMPARE PHYSICAL CONTENT
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()

        # DROP DATABASE DB1
        node.safe_psql(
            "postgres", "drop database db1")
        # SECOND DELTA BACKUP
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta', options=["--stream"]
        )

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # RESTORE SECOND DELTA BACKUP
        node_restored.cleanup()
        self.restore_node(
            backup_dir,
            'node',
            node_restored,
            backup_id=backup_id,
            options=[
                "-j", "4",
                "--immediate",
                "--recovery-target-action=promote"]
        )

        # COMPARE PHYSICAL CONTENT
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
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
                    repr(self.output), self.cmd))
        except QueryException as e:
            self.assertTrue(
                'FATAL:  database "db1" does not exist' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_exists_in_previous_backup(self):
        """
        Make node, take full backup, create table, take page backup,
        take delta backup, check that file is no fully copied to delta backup
        """
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

        node.safe_psql("postgres", "SELECT * FROM t_heap")
        filepath = node.safe_psql(
            "postgres",
            "SELECT pg_relation_filepath('t_heap')").decode('utf-8').rstrip()
        self.backup_node(
            backup_dir,
            'node',
            node,
            options=["--stream"])

        # PAGE BACKUP
        backup_id = self.backup_node(
            backup_dir,
            'node',
            node,
            backup_type='page'
        )

        fullpath = os.path.join(
            backup_dir, 'backups', 'node', backup_id, 'database', filepath)
        self.assertFalse(os.path.exists(fullpath))

#        if self.paranoia:
#            pgdata_page = self.pgdata_content(
#                os.path.join(
#                    backup_dir, 'backups',
#                    'node', backup_id, 'database'))

        # DELTA BACKUP
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta',
            options=["--stream"]
        )
#        if self.paranoia:
#            pgdata_delta = self.pgdata_content(
#                os.path.join(
#                    backup_dir, 'backups',
#                    'node', backup_id, 'database'))
#            self.compare_pgdata(
#                pgdata_page, pgdata_delta)

        fullpath = os.path.join(
            backup_dir, 'backups', 'node', backup_id, 'database', filepath)
        self.assertFalse(os.path.exists(fullpath))

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # RESTORE
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored')
        )

        node_restored.cleanup()
        self.restore_node(
            backup_dir,
            'node',
            node_restored,
            backup_id=backup_id,
            options=[
                "-j", "4",
                "--immediate",
                "--recovery-target-action=promote"])

        # COMPARE PHYSICAL CONTENT
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_alter_table_set_tablespace_delta(self):
        """
        Make node, create tablespace with table, take full backup,
        alter tablespace location, take delta backup, restore database.
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
        node.slow_start()

        # FULL BACKUP
        self.create_tblspace_in_node(node, 'somedata')
        node.safe_psql(
            "postgres",
            "create table t_heap tablespace somedata as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,100) i")

        # FULL backup
        self.backup_node(backup_dir, 'node', node, options=["--stream"])

        # ALTER TABLESPACE
        self.create_tblspace_in_node(node, 'somedata_new')
        node.safe_psql(
            "postgres",
            "alter table t_heap set tablespace somedata_new")

        # DELTA BACKUP
        result = node.safe_psql(
            "postgres", "select * from t_heap")
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta',
            options=["--stream"])

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
                    self.get_tblspace_path(node_restored, 'somedata')
                ),
                "-T", "{0}={1}".format(
                    self.get_tblspace_path(node, 'somedata_new'),
                    self.get_tblspace_path(node_restored, 'somedata_new')
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

        result_new = node_restored.safe_psql(
            "postgres", "select * from t_heap")

        self.assertEqual(result, result_new, 'lost some data after restore')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_alter_database_set_tablespace_delta(self):
        """
        Make node, take full backup, create database,
        take delta backup, alter database tablespace location,
        take delta backup restore last delta backup.
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'
            }
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()
        self.create_tblspace_in_node(node, 'somedata')

        # FULL backup
        self.backup_node(backup_dir, 'node', node, options=["--stream"])

        # CREATE DATABASE DB1
        node.safe_psql(
            "postgres",
            "create database db1 tablespace = 'somedata'")
        node.safe_psql(
            "db1",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,100) i")

        # DELTA BACKUP
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta',
            options=["--stream"]
        )

        # ALTER TABLESPACE
        self.create_tblspace_in_node(node, 'somedata_new')
        node.safe_psql(
            "postgres",
            "alter database db1 set tablespace somedata_new"
        )

        # DELTA BACKUP
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta',
            options=["--stream"]
        )

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
                    self.get_tblspace_path(node_restored, 'somedata')
                ),
                "-T", "{0}={1}".format(
                    self.get_tblspace_path(node, 'somedata_new'),
                    self.get_tblspace_path(node_restored, 'somedata_new')
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
    def test_delta_delete(self):
        """
        Make node, create tablespace with table, take full backup,
        alter tablespace location, take delta backup, restore database.
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

    def test_delta_nullified_heap_page_backup(self):
        """
        make node, take full backup, nullify some heap block,
        take delta backup, restore, physically compare pgdata`s
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

        node.pgbench_init(scale=1)

        file_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('pgbench_accounts')").decode('utf-8').rstrip()

        node.safe_psql(
            "postgres",
            "CHECKPOINT")

        self.backup_node(
            backup_dir, 'node', node)

        # Nullify some block in PostgreSQL
        file = os.path.join(node.data_dir, file_path).replace("\\", "/")
        if os.name == 'nt':
            file = file.replace("\\", "/")

        with open(file, 'r+b', 0) as f:
            f.seek(8192)
            f.write(b"\x00"*8192)
            f.flush()
            f.close

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta', options=["--log-level-file=verbose"])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        if not self.remote:
            log_file_path = os.path.join(backup_dir, "log", "pg_probackup.log")
            with open(log_file_path) as f:
                content = f.read()

            self.assertIn(
                'VERBOSE: File: "{0}" blknum 1, empty page'.format(file),
                content)
            self.assertNotIn(
                "Skipping blknum 1 in file: {0}".format(file),
                content)

        # Restore DELTA backup
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored)

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_delta_backup_from_past(self):
        """
        make node, take FULL stream backup, take DELTA stream backup,
        restore FULL backup, try to take second DELTA stream backup
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

        backup_id = self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        node.pgbench_init(scale=3)

        # First DELTA
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta', options=['--stream'])

        # Restore FULL backup
        node.cleanup()
        self.restore_node(backup_dir, 'node', node, backup_id=backup_id)
        node.slow_start()

        # Second DELTA backup
        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='delta', options=['--stream'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because we are backing up an instance from the past"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'ERROR: Current START LSN ' in e.message and
                'is lower than START LSN ' in e.message and
                'of previous backup ' in e.message and
                'It may indicate that we are trying '
                'to backup PostgreSQL instance from the past' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_delta_pg_resetxlog(self):
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

        self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        node.safe_psql(
            'postgres',
            "update t_heap set id = nextval('t_seq'), text = md5(text), "
            "tsvector = md5(repeat(tsvector::text, 10))::tsvector")

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
#                backup_type='delta', options=['--stream'])

        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='delta', options=['--stream'])
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
