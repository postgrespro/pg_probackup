import os

from .helpers.ptrack_helpers import ProbackupTest
from pg_probackup2.gdb import needs_gdb
from threading import Thread


class DeltaTest(ProbackupTest):

    # @unittest.skip("skip")
    def test_basic_delta_vacuum_truncate(self):
        """
        make node, create table, take full backup,
        delete last 3 pages, vacuum relation,
        take delta backup, take second delta backup,
        restore the latest delta backup and check data correctness
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        node_restored = self.pg_node.make_simple('node_restored')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
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

        self.pb.backup_node('node', node, options=['--stream'])

        node.safe_psql(
            "postgres",
            "delete from t_heap where ctid >= '(11,0)'")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.pb.backup_node('node', node, backup_type='delta')

        self.pb.backup_node('node', node, backup_type='delta')

        pgdata = self.pgdata_content(node.data_dir)

        self.pb.restore_node('node', node_restored)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

    # @unittest.skip("skip")
    def test_delta_vacuum_truncate_1(self):
        """
        make node, create table, take full backup,
        delete last 3 pages, vacuum relation,
        take delta backup, take second delta backup,
        restore latest delta backup and check data correctness
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
        )
        node_restored = self.pg_node.make_simple('node_restored',
        )

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
            "from generate_series(0,1024) i;"
        )

        node.safe_psql(
            "postgres",
            "vacuum t_heap"
        )

        self.pb.backup_node('node', node)

        node.safe_psql(
            "postgres",
            "delete from t_heap where ctid >= '(11,0)'"
        )

        node.safe_psql(
            "postgres",
            "vacuum t_heap"
        )

        self.pb.backup_node('node', node, backup_type='delta'
        )

        self.pb.backup_node('node', node, backup_type='delta'
        )

        pgdata = self.pgdata_content(node.data_dir)

        old_tablespace = self.get_tblspace_path(node, 'somedata')
        new_tablespace = self.get_tblspace_path(node_restored, 'somedata_new')

        self.pb.restore_node(
            'node',
            node_restored,
            options=[
                "-T", "{0}={1}".format(
                    old_tablespace, new_tablespace)]
        )

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

    # @unittest.skip("skip")
    def test_delta_vacuum_truncate_2(self):
        """
        make node, create table, take full backup,
        delete last 3 pages, vacuum relation,
        take delta backup, take second delta backup,
        restore latest delta backup and check data correctness
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
        )
        node_restored = self.pg_node.make_simple('node_restored',
        )

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
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

        self.pb.backup_node('node', node)

        print(os.path.join(node.data_dir, filepath + '.1'))
        os.unlink(os.path.join(node.data_dir, filepath + '.1'))

        self.pb.backup_node('node', node, backup_type='delta')

        self.pb.backup_node('node', node, backup_type='delta')

        pgdata = self.pgdata_content(node.data_dir)

        self.pb.restore_node('node', node_restored)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

    # @unittest.skip("skip")
    def test_delta_stream(self):
        """
        make archive node, take full and delta stream backups,
        restore them and check data correctness
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '30s'
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
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(0,100) i")

        full_result = node.table_checksum("t_heap")
        full_backup_id = self.pb.backup_node('node', node,
            backup_type='full', options=['--stream'])

        # delta BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(100,200) i")
        delta_result = node.table_checksum("t_heap")
        delta_backup_id = self.pb.backup_node('node', node,
            backup_type='delta', options=['--stream'])

        # Drop Node
        node.cleanup()

        # Check full backup
        restore_result = self.pb.restore_node('node', node,
                backup_id=full_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(full_backup_id))
        node.slow_start()
        full_result_new = node.table_checksum("t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check delta backup
        restore_result = self.pb.restore_node('node', node,
                backup_id=delta_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(delta_backup_id))

        node.slow_start()
        delta_result_new = node.table_checksum("t_heap")
        self.assertEqual(delta_result, delta_result_new)
        node.cleanup()

    # @unittest.skip("skip")
    def test_delta_archive(self):
        """
        make archive node, take full and delta archive backups,
        restore them and check data correctness
        """
        self.maxDiff = None
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,1) i")
        full_result = node.table_checksum("t_heap")
        full_backup_id = self.pb.backup_node('node', node, backup_type='full')

        # delta BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,2) i")
        delta_result = node.table_checksum("t_heap")
        delta_backup_id = self.pb.backup_node('node', node, backup_type='delta')

        # Drop Node
        node.cleanup()

        # Restore and check full backup
        restore_result = self.pb.restore_node('node', node,
                backup_id=full_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(full_backup_id))
        node.slow_start()
        full_result_new = node.table_checksum("t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Restore and check delta backup
        restore_result = self.pb.restore_node('node', node,
                backup_id=delta_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(delta_backup_id))
        node.slow_start()
        delta_result_new = node.table_checksum("t_heap")
        self.assertEqual(delta_result, delta_result_new)
        node.cleanup()

    # @unittest.skip("skip")
    def test_delta_multiple_segments(self):
        """
        Make node, create table with multiple segments,
        write some data to it, check delta and data correctness
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'fsync': 'off',
                'shared_buffers': '1GB',
                'maintenance_work_mem': '1GB',
                'full_page_writes': 'off'
            }
        )

        self.pb.init()
        self.pb.add_instance('node', node)
        # self.pb.set_archiving('node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'somedata')

        # CREATE TABLE
        node.pgbench_init(
            scale=100,
            options=['--tablespace=somedata', '--no-vacuum'])
        # FULL BACKUP
        self.pb.backup_node('node', node, options=['--stream'])

        # PGBENCH STUFF
        pgbench = node.pgbench(options=['-T', '50', '-c', '1', '--no-vacuum'])
        pgbench.wait()
        node.safe_psql("postgres", "checkpoint")

        # GET LOGICAL CONTENT FROM NODE
        result = node.table_checksum("pgbench_accounts")
        # delta BACKUP
        self.pb.backup_node('node', node,
            backup_type='delta', options=['--stream'])
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
                "-j", "4", "-T", "{0}={1}".format(
                    tblspc_path, tblspc_path_new)])

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
    @needs_gdb
    def test_delta_vacuum_full(self):
        """
        make node, make full and delta stream backups,
        restore them and check data correctness
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()
        self.create_tblspace_in_node(node, 'somedata')

        self.pb.backup_node('node', node, options=['--stream'])

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

        gdb.stopped_in_breakpoint()

        gdb.continue_execution_until_break(20)

        self.pb.backup_node('node', node,
            backup_type='delta', options=['--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        gdb.remove_all_breakpoints()
        gdb.detach()
        process.join()

        old_tablespace = self.get_tblspace_path(node, 'somedata')
        new_tablespace = self.get_tblspace_path(node_restored, 'somedata_new')

        self.pb.restore_node('node', node_restored,
            options=["-j", "4", "-T", "{0}={1}".format(
                old_tablespace, new_tablespace)])

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})

        node_restored.slow_start()

    # @unittest.skip("skip")
    def test_create_db(self):
        """
        Make node, take full backup, create database db1, take delta backup,
        restore database and check it presense
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'max_wal_size': '10GB',
            }
        )

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,100) i")

        node.table_checksum("t_heap")
        self.pb.backup_node('node', node,
            options=["--stream"])

        # CREATE DATABASE DB1
        node.safe_psql("postgres", "create database db1")
        node.safe_psql(
            "db1",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,100) i")

        # DELTA BACKUP
        backup_id = self.pb.backup_node('node', node,
            backup_type='delta',
            options=["--stream"]
        )

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # RESTORE
        node_restored = self.pg_node.make_simple('node_restored')

        node_restored.cleanup()
        self.pb.restore_node(
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
        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

        # DROP DATABASE DB1
        node.safe_psql(
            "postgres", "drop database db1")
        # SECOND DELTA BACKUP
        backup_id = self.pb.backup_node('node', node,
            backup_type='delta', options=["--stream"]
        )

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # RESTORE SECOND DELTA BACKUP
        node_restored.cleanup()
        self.pb.restore_node(
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
        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

        error_result = node_restored.safe_psql('db1', 'select 1', expect_error=True)
        self.assertMessage(error_result, contains='FATAL:  database "db1" does not exist')

    # @unittest.skip("skip")
    def test_exists_in_previous_backup(self):
        """
        Make node, take full backup, create table, take page backup,
        take delta backup, check that file is no fully copied to delta backup
        """
        backup_dir = self.backup_dir
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

        node.table_checksum("t_heap")
        filepath = node.safe_psql(
            "postgres",
            "SELECT pg_relation_filepath('t_heap')").decode('utf-8').rstrip()
        self.pb.backup_node(
            'node',
            node,
            options=["--stream"])

        # PAGE BACKUP
        backup_id = self.pb.backup_node(
            'node',
            node,
            backup_type='page'
        )

        self.assertFalse(self.backup_file_exists(backup_dir, 'node', backup_id,
                                                 f'database/{filepath}'))

#        if self.paranoia:
#            pgdata_page = self.pgdata_content(
#                os.path.join(
#                    backup_dir, 'backups',
#                    'node', backup_id, 'database'))

        # DELTA BACKUP
        backup_id = self.pb.backup_node('node', node,
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

        self.assertFalse(self.backup_file_exists(backup_dir, 'node', backup_id,
                                                 f'database/{filepath}'))

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # RESTORE
        node_restored = self.pg_node.make_simple('node_restored'
        )

        node_restored.cleanup()
        self.pb.restore_node(
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
        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

    # @unittest.skip("skip")
    def test_alter_table_set_tablespace_delta(self):
        """
        Make node, create tablespace with table, take full backup,
        alter tablespace location, take delta backup, restore database.
        """
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '30s',
            }
        )

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # FULL BACKUP
        self.create_tblspace_in_node(node, 'somedata')
        node.safe_psql(
            "postgres",
            "create table t_heap tablespace somedata as select i as id,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(0,100) i")

        # FULL backup
        self.pb.backup_node('node', node, options=["--stream"])

        # ALTER TABLESPACE
        self.create_tblspace_in_node(node, 'somedata_new')
        node.safe_psql(
            "postgres",
            "alter table t_heap set tablespace somedata_new")

        # DELTA BACKUP
        result = node.table_checksum("t_heap")
        self.pb.backup_node('node', node,
            backup_type='delta',
            options=["--stream"])

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
        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

        result_new = node_restored.table_checksum("t_heap")

        self.assertEqual(result, result_new, 'lost some data after restore')

    # @unittest.skip("skip")
    def test_alter_database_set_tablespace_delta(self):
        """
        Make node, take full backup, create database,
        take delta backup, alter database tablespace location,
        take delta backup restore last delta backup.
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
        )

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()
        self.create_tblspace_in_node(node, 'somedata')

        # FULL backup
        self.pb.backup_node('node', node, options=["--stream"])

        # CREATE DATABASE DB1
        node.safe_psql(
            "postgres",
            "create database db1 tablespace = 'somedata'")
        node.safe_psql(
            "db1",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,100) i")

        # DELTA BACKUP
        self.pb.backup_node('node', node,
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
        self.pb.backup_node('node', node,
            backup_type='delta',
            options=["--stream"]
        )

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
        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

    # @unittest.skip("skip")
    def test_delta_delete(self):
        """
        Make node, create tablespace with table, take full backup,
        alter tablespace location, take delta backup, restore database.
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

        # RESTORE
        node_restored = self.pg_node.make_simple('node_restored')
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

    def test_delta_nullified_heap_page_backup(self):
        """
        make node, take full backup, nullify some heap block,
        take delta backup, restore, physically compare pgdata`s
        """
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=1)

        file_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('pgbench_accounts')").decode('utf-8').rstrip()

        node.safe_psql(
            "postgres",
            "CHECKPOINT")

        self.pb.backup_node('node', node)

        # Nullify some block in PostgreSQL
        file = os.path.join(node.data_dir, file_path).replace("\\", "/")
        if os.name == 'nt':
            file = file.replace("\\", "/")

        with open(file, 'r+b', 0) as f:
            f.seek(8192)
            f.write(b"\x00"*8192)
            f.flush()

        self.pb.backup_node('node', node,
            backup_type='delta', options=["--log-level-file=verbose"])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        content = self.read_pb_log()
        self.assertIn(
            'VERBOSE: File: {0} blknum 1, empty zeroed page'.format(file_path),
            content)
        if not self.remote:
            self.assertIn(
                'VERBOSE: File: "{0}" blknum 1, empty page'.format(file),
                content)
            self.assertNotIn(
                "Skipping blknum 1 in file: {0}".format(file),
                content)

        # Restore DELTA backup
        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored)

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

    def test_delta_backup_from_past(self):
        """
        make node, take FULL stream backup, take DELTA stream backup,
        restore FULL backup, try to take second DELTA stream backup
        """
        instance_name = 'node'
        node = self.pg_node.make_simple(instance_name,
                                     set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance(instance_name, node)
        node.slow_start()

        backup_id = self.pb.backup_node(instance_name, node, options=['--stream'])

        node.pgbench_init(scale=3)

        # First DELTA
        self.pb.backup_node(instance_name, node,
            backup_type='delta', options=['--stream'])

        # Restore FULL backup
        node.cleanup()
        self.pb.restore_node(instance_name, node, backup_id=backup_id)
        node.slow_start()

        # Second DELTA backup
        error_result = self.pb.backup_node( instance_name, node,
                                        backup_type='delta',
                                        options=['--stream'],
                                        expect_error=True)

        self.assertMessage(error_result, regex=r'Current START LSN (\d+)/(\d+) is lower than START LSN (\d+)/(\d+) '
                                                r'of previous backup \w{6}. It may indicate that we are trying '
                                                r'to backup PostgreSQL instance from the past.')

    # @unittest.expectedFailure
    def test_delta_pg_resetxlog(self):
        instance_name = 'node'
        node = self.pg_node.make_simple(instance_name,
                                     set_replication=True,
                                     pg_options={'shared_buffers': '512MB',
                                                 'max_wal_size': '3GB'})

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance(instance_name, node)
        node.slow_start()

        # Create table
        node.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap "
            "as select nextval('t_seq')::int as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")

        self.pb.backup_node(instance_name, node, options=['--stream'])

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
        else:
            pg_resetxlog_path = self.get_bin_path('pg_resetxlog')

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

        backup_id = self.pb.backup_node(instance_name,
                                     node,
                                     backup_type='delta',
                                     options=['--stream'])
        self.pb.validate(instance_name, backup_id)

    def test_delta_backup_before_full_will_fail(self):
        instance_name = 'node'
        node = self.pg_node.make_simple(
            base_dir=instance_name)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance(instance_name, node)
        self.pb.set_archiving(instance_name, node)
        node.slow_start()

        error_result = self.pb.backup_node(instance_name, node, backup_type="delta", expect_error=True)
        self.assertMessage(error_result,
                           contains='ERROR:  could not open file "pg_wal/00000001.history": No such file or directory')
