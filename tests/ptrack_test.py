import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, idx_ptrack
from pg_probackup2.gdb import needs_gdb
from testgres import StartNodeException
import shutil
from time import sleep
from threading import Thread


class PtrackTest(ProbackupTest):
    def setUp(self):
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

    # @unittest.skip("skip")
    @needs_gdb
    def test_drop_rel_during_backup_ptrack(self):
        """
        drop relation during ptrack backup
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=self.ptrack)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        node.safe_psql(
            "postgres",
            "create table t_heap as select i"
            " as id from generate_series(0,100) i")

        relative_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()

        absolute_path = os.path.join(node.data_dir, relative_path)

        # FULL backup
        self.pb.backup_node('node', node, options=['--stream'])

        # PTRACK backup
        gdb = self.pb.backup_node('node', node, backup_type='ptrack',
            gdb=True, options=['--log-level-file=LOG'])

        gdb.set_breakpoint('backup_files')
        gdb.run_until_break()

        # REMOVE file
        os.remove(absolute_path)

        # File removed, we can proceed with backup
        gdb.continue_execution_until_exit()

        pgdata = self.pgdata_content(node.data_dir)

        log_content = self.read_pb_log()
        self.assertTrue(
                'LOG: File not found: "{0}"'.format(absolute_path) in log_content,
                'File "{0}" should be deleted but it`s not'.format(absolute_path))

        node.cleanup()
        self.pb.restore_node('node', node=node, options=["-j", "4"])

        # Physical comparison
        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_ptrack_without_full(self):
        """ptrack backup without validated full backup"""
        node = self.pg_node.make_simple('node',
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        self.pb.backup_node('node', node, backup_type="ptrack",
                         expect_error="because page backup should not be "
                                      "possible without valid full backup")
        self.assertMessage(contains="WARNING: Valid full backup on current timeline 1 is not found")
        self.assertMessage(contains="ERROR: Create new full backup before an incremental one")

        self.assertEqual(
            self.pb.show('node')[0]['status'],
            "ERROR")

    # @unittest.skip("skip")
    def test_ptrack_threads(self):
        """ptrack multi thread backup mode"""
        node = self.pg_node.make_simple('node',
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        self.pb.backup_node('node', node,
            backup_type="full", options=["-j", "4"])
        self.assertEqual(self.pb.show('node')[0]['status'], "OK")

        self.pb.backup_node('node', node,
            backup_type="ptrack", options=["-j", "4"])
        self.assertEqual(self.pb.show('node')[0]['status'], "OK")

    # @unittest.skip("skip")
    def test_ptrack_stop_pg(self):
        """
        create node, take full backup,
        restart node, check that ptrack backup
        can be taken
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        node.pgbench_init(scale=1)

        # FULL backup
        self.pb.backup_node('node', node, options=['--stream'])

        node.stop()
        node.slow_start()

        self.pb.backup_node('node', node,
            backup_type='ptrack', options=['--stream'])

        # @unittest.skip("skip")
    def test_ptrack_multi_timeline_backup(self):
        """
        t2            /------P2
        t1 ------F---*-----P1
        """
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

        node.pgbench_init(scale=5)

        # FULL backup
        full_id = self.pb.backup_node('node', node)

        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        sleep(15)

        xid = node.safe_psql(
            'postgres',
            'SELECT txid_current()').decode('utf-8').rstrip()
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type='ptrack')

        node.cleanup()

        # Restore from full backup to create Timeline 2
        print(self.pb.restore_node('node', node,
            options=[
                '--recovery-target-xid={0}'.format(xid),
                '--recovery-target-action=promote']))

        node.slow_start()

        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        self.pb.backup_node('node', node, backup_type='ptrack')

        pgdata = self.pgdata_content(node.data_dir)

        node.cleanup()

        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        node.slow_start()

        balance = node.safe_psql(
            'postgres',
            'select (select sum(tbalance) from pgbench_tellers) - '
            '( select sum(bbalance) from pgbench_branches) + '
            '( select sum(abalance) from pgbench_accounts ) - '
            '(select sum(delta) from pgbench_history) as must_be_zero').decode('utf-8').rstrip()

        self.assertEqual('0', balance)

        # @unittest.skip("skip")
    def test_ptrack_multi_timeline_backup_1(self):
        """
        t2              /------
        t1 ---F---P1---*

        # delete P1
        t2              /------P2
        t1 ---F--------*
        """
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

        node.pgbench_init(scale=5)

        # FULL backup
        full_id = self.pb.backup_node('node', node)

        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        ptrack_id = self.pb.backup_node('node', node, backup_type='ptrack')
        node.cleanup()

        self.pb.restore_node('node', node=node)

        node.slow_start()

        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # delete old PTRACK backup
        self.pb.delete('node', backup_id=ptrack_id)

        # take new PTRACK backup
        self.pb.backup_node('node', node, backup_type='ptrack')

        pgdata = self.pgdata_content(node.data_dir)

        node.cleanup()

        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        node.slow_start()

        balance = node.safe_psql(
            'postgres',
            'select (select sum(tbalance) from pgbench_tellers) - '
            '( select sum(bbalance) from pgbench_branches) + '
            '( select sum(abalance) from pgbench_accounts ) - '
            '(select sum(delta) from pgbench_history) as must_be_zero').\
            decode('utf-8').rstrip()

        self.assertEqual('0', balance)

    # @unittest.skip("skip")
    def test_ptrack_eat_my_data(self):
        """
        PGPRO-4051
        """
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

        node.pgbench_init(scale=50)

        self.pb.backup_node('node', node)

        node_restored = self.pg_node.make_simple('node_restored')

        pgbench = node.pgbench(options=['-T', '300', '-c', '1', '--no-vacuum'])

        for i in range(10):
            print("Iteration: {0}".format(i))

            sleep(2)

            self.pb.backup_node('node', node, backup_type='ptrack')
#            pgdata = self.pgdata_content(node.data_dir)
#
#            node_restored.cleanup()
#
#            self.pb.restore_node('node', node=node_restored)
#            pgdata_restored = self.pgdata_content(node_restored.data_dir)
#
#            self.compare_pgdata(pgdata, pgdata_restored)

        pgbench.terminate()
        pgbench.wait()

        self.switch_wal_segment(node)

        result = node.table_checksum("pgbench_accounts")

        node_restored.cleanup()
        self.pb.restore_node('node', node=node_restored)
        node_restored.set_auto_conf({'port': node_restored.port})

        node_restored.slow_start()

        balance = node_restored.safe_psql(
            'postgres',
            'select (select sum(tbalance) from pgbench_tellers) - '
            '( select sum(bbalance) from pgbench_branches) + '
            '( select sum(abalance) from pgbench_accounts ) - '
            '(select sum(delta) from pgbench_history) as must_be_zero').decode('utf-8').rstrip()

        self.assertEqual('0', balance)

        # Logical comparison
        self.assertEqual(
            result,
            node.table_checksum("pgbench_accounts"),
            'Data loss')

    # @unittest.skip("skip")
    def test_ptrack_simple(self):
        """make node, make full and ptrack stream backups,"
        " restore them and check data correctness"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        self.pb.backup_node('node', node, options=['--stream'])

        node.safe_psql(
            "postgres",
            "create table t_heap as select i"
            " as id from generate_series(0,1) i")

        self.pb.backup_node('node', node, backup_type='ptrack',
            options=['--stream'])

        node.safe_psql(
            "postgres",
            "update t_heap set id = 100500")

        self.pb.backup_node('node', node,
            backup_type='ptrack', options=['--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        result = node.table_checksum("t_heap")

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored, options=["-j", "4"])

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node_restored.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})

        node_restored.slow_start()

        # Logical comparison
        self.assertEqual(
            result,
            node_restored.table_checksum("t_heap"))

    # @unittest.skip("skip")
    def test_ptrack_unprivileged(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        # self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE DATABASE backupdb")

        # PG < 15
        if self.pg_config_version < 150000:
            node.safe_psql(
                'backupdb',
                "REVOKE ALL ON DATABASE backupdb from PUBLIC; "
                "REVOKE ALL ON SCHEMA public from PUBLIC; "
                "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC; "
                "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC; "
                "REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC; "
                "REVOKE ALL ON SCHEMA pg_catalog from PUBLIC; "
                "REVOKE ALL ON ALL TABLES IN SCHEMA pg_catalog FROM PUBLIC; "
                "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pg_catalog FROM PUBLIC; "
                "REVOKE ALL ON ALL SEQUENCES IN SCHEMA pg_catalog FROM PUBLIC; "
                "REVOKE ALL ON SCHEMA information_schema from PUBLIC; "
                "REVOKE ALL ON ALL TABLES IN SCHEMA information_schema FROM PUBLIC; "
                "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA information_schema FROM PUBLIC; "
                "REVOKE ALL ON ALL SEQUENCES IN SCHEMA information_schema FROM PUBLIC; "
                "CREATE ROLE backup WITH LOGIN REPLICATION; "
                "GRANT CONNECT ON DATABASE backupdb to backup; "
                "GRANT USAGE ON SCHEMA pg_catalog TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_proc TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_database TO backup; " # for partial restore, checkdb and ptrack
                "GRANT EXECUTE ON FUNCTION pg_catalog.oideq(oid, oid) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.nameeq(name, name) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.set_config(text, text, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_start_backup(text, boolean, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_stop_backup(boolean, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_create_restore_point(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_wal() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_last_wal_replay_lsn() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_current_snapshot() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_snapshot_xmax(txid_snapshot) TO backup;"
            )
        # >= 15
        else:
            node.safe_psql(
                'backupdb',
                "REVOKE ALL ON DATABASE backupdb from PUBLIC; "
                "REVOKE ALL ON SCHEMA public from PUBLIC; "
                "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC; "
                "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC; "
                "REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC; "
                "REVOKE ALL ON SCHEMA pg_catalog from PUBLIC; "
                "REVOKE ALL ON ALL TABLES IN SCHEMA pg_catalog FROM PUBLIC; "
                "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pg_catalog FROM PUBLIC; "
                "REVOKE ALL ON ALL SEQUENCES IN SCHEMA pg_catalog FROM PUBLIC; "
                "REVOKE ALL ON SCHEMA information_schema from PUBLIC; "
                "REVOKE ALL ON ALL TABLES IN SCHEMA information_schema FROM PUBLIC; "
                "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA information_schema FROM PUBLIC; "
                "REVOKE ALL ON ALL SEQUENCES IN SCHEMA information_schema FROM PUBLIC; "
                "CREATE ROLE backup WITH LOGIN REPLICATION; "
                "GRANT CONNECT ON DATABASE backupdb to backup; "
                "GRANT USAGE ON SCHEMA pg_catalog TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_proc TO backup; "
                "GRANT SELECT ON TABLE pg_catalog.pg_database TO backup; " # for partial restore, checkdb and ptrack
                "GRANT EXECUTE ON FUNCTION pg_catalog.oideq(oid, oid) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.nameeq(name, name) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.set_config(text, text, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_backup_start(text, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_backup_stop(boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_create_restore_point(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_wal() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_last_wal_replay_lsn() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_current_snapshot() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_snapshot_xmax(txid_snapshot) TO backup;"
            )

        node.safe_psql(
            "backupdb",
            "CREATE SCHEMA ptrack")
        node.safe_psql(
            "backupdb",
            "CREATE EXTENSION ptrack WITH SCHEMA ptrack")
        node.safe_psql(
            "backupdb",
            "GRANT USAGE ON SCHEMA ptrack TO backup")

        node.safe_psql(
            "backupdb",
            "GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup")

        if ProbackupTest.pgpro:
            node.safe_psql(
                "backupdb",
                "GRANT EXECUTE ON FUNCTION pg_catalog.pgpro_version() TO backup; "
                'GRANT EXECUTE ON FUNCTION pg_catalog.pgpro_edition() TO backup;')

        self.pb.backup_node('node', node,
            datname='backupdb', options=['--stream', "-U", "backup"])

        self.pb.backup_node('node', node, datname='backupdb',
            backup_type='ptrack', options=['--stream', "-U", "backup"])


    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_enable(self):
        """make ptrack without full backup, should result in error"""
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'checkpoint_timeout': '30s',
                'shared_preload_libraries': 'ptrack'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        # PTRACK BACKUP
        self.pb.backup_node('node', node, backup_type='ptrack',
                         options=["--stream"],
                         expect_error="because ptrack disabled")
        self.assertMessage(contains='ERROR: Ptrack is disabled')

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_disable(self):
        """
        Take full backup, disable ptrack restart postgresql,
        enable ptrack, restart postgresql, take ptrack backup
        which should fail
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True,
            pg_options={'checkpoint_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        # FULL BACKUP
        self.pb.backup_node('node', node, options=['--stream'])

        # DISABLE PTRACK
        node.safe_psql('postgres', "alter system set ptrack.map_size to 0")
        node.stop()
        node.slow_start()

        # ENABLE PTRACK
        node.safe_psql('postgres', "alter system set ptrack.map_size to '128'")
        node.safe_psql('postgres', "alter system set shared_preload_libraries to 'ptrack'")
        node.stop()
        node.slow_start()

        # PTRACK BACKUP
        self.pb.backup_node('node', node, backup_type='ptrack',
                         options=["--stream"],
                         expect_error="because ptrack_enable was set to OFF "
                                      "at some point after previous backup")
        self.assertMessage(contains='ERROR: LSN from ptrack_control')

    # @unittest.skip("skip")
    def test_ptrack_uncommitted_xact(self):
        """make ptrack backup while there is uncommitted open transaction"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True,
            pg_options={
                'wal_level': 'replica'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        self.pb.backup_node('node', node, options=['--stream'])

        con = node.connect("postgres")
        con.execute(
            "create table t_heap as select i"
            " as id from generate_series(0,1) i")

        self.pb.backup_node('node', node, backup_type='ptrack',
            options=['--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                    node_restored.data_dir, ignore_ptrack=False)

        node_restored.set_auto_conf({'port': node_restored.port})

        node_restored.slow_start()

        # Physical comparison
        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    @needs_gdb
    def test_ptrack_vacuum_full_1(self):
        """make node, make full and ptrack stream backups,
          restore them and check data correctness"""

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'somedata')

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

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

        self.pb.backup_node('node', node, backup_type='ptrack', options=['--stream'])

        self.pb.backup_node('node', node, backup_type='ptrack', options=['--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        gdb.remove_all_breakpoints()
        gdb.detach()
        process.join()

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        old_tablespace = self.get_tblspace_path(node, 'somedata')
        new_tablespace = self.get_tblspace_path(node_restored, 'somedata_new')

        self.pb.restore_node('node', node_restored,
            options=["-j", "4", "-T", "{0}={1}".format(
                old_tablespace, new_tablespace)]
        )

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node_restored.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})

        node_restored.slow_start()

    # @unittest.skip("skip")
    def test_ptrack_vacuum_truncate(self):
        """make node, create table, take full backup,
           delete last 3 pages, vacuum relation,
           take ptrack backup, take second ptrack backup,
           restore last ptrack backup and check data correctness"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'somedata')

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

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

        self.pb.backup_node('node', node, options=['--stream'])

        node.safe_psql(
            "postgres",
            "delete from t_heap where ctid >= '(11,0)'")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.pb.backup_node('node', node, backup_type='ptrack', options=['--stream'])

        self.pb.backup_node('node', node, backup_type='ptrack', options=['--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        old_tablespace = self.get_tblspace_path(node, 'somedata')
        new_tablespace = self.get_tblspace_path(node_restored, 'somedata_new')

        self.pb.restore_node('node', node_restored,
            options=["-j", "4", "-T", "{0}={1}".format(
                old_tablespace, new_tablespace)]
        )

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node_restored.data_dir,
                ignore_ptrack=False
                )
            self.compare_pgdata(pgdata, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})

        node_restored.slow_start()

    # @unittest.skip("skip")
    @needs_gdb
    def test_ptrack_get_block(self):
        """
        make node, make full and ptrack stream backups,
        restore them and check data correctness
        """

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        node.safe_psql(
            "postgres",
            "create table t_heap as select i"
            " as id from generate_series(0,1) i")

        self.pb.backup_node('node', node, options=['--stream'])
        gdb = self.pb.backup_node('node', node, backup_type='ptrack',
            options=['--stream'],
            gdb=True)

        gdb.set_breakpoint('make_pagemap_from_ptrack_2')
        gdb.run_until_break()

        node.safe_psql(
            "postgres",
            "update t_heap set id = 100500")

        gdb.continue_execution_until_exit()

        self.pb.backup_node('node', node,
            backup_type='ptrack', options=['--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        result = node.table_checksum("t_heap")
        node.cleanup()
        self.pb.restore_node('node', node=node, options=["-j", "4"])

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        node.slow_start()
        # Logical comparison
        self.assertEqual(
            result,
            node.table_checksum("t_heap"))

    # @unittest.skip("skip")
    def test_ptrack_stream(self):
        """make node, make full and ptrack stream backups,
         restore them and check data correctness"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True,
            pg_options={
                'checkpoint_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        # FULL BACKUP
        node.safe_psql("postgres", "create sequence t_seq")
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, nextval('t_seq')"
            " as t_seq, md5(i::text) as text, md5(i::text)::tsvector"
            " as tsvector from generate_series(0,100) i")

        full_result = node.table_checksum("t_heap")
        full_backup_id = self.pb.backup_node('node', node, options=['--stream'])

        # PTRACK BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, nextval('t_seq') as t_seq,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(100,200) i")

        ptrack_result = node.table_checksum("t_heap")
        ptrack_backup_id = self.pb.backup_node('node', node,
            backup_type='ptrack', options=['--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # Drop Node
        node.cleanup()

        # Restore and check full backup
        restore_result = self.pb.restore_node('node', node,
                backup_id=full_backup_id, options=["-j", "4"]
            )
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(full_backup_id))
        node.slow_start()
        full_result_new = node.table_checksum("t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Restore and check ptrack backup
        restore_result = self.pb.restore_node('node', node,
                backup_id=ptrack_backup_id, options=["-j", "4"]
            )
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(ptrack_backup_id))

        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        node.slow_start()
        ptrack_result_new = node.table_checksum("t_heap")
        self.assertEqual(ptrack_result, ptrack_result_new)

    # @unittest.skip("skip")
    def test_ptrack_archive(self):
        """make archive node, make full and ptrack backups,
            check data correctness in restored instance"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True,
            pg_options={
                'checkpoint_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as"
            " select i as id,"
            " md5(i::text) as text,"
            " md5(i::text)::tsvector as tsvector"
            " from generate_series(0,100) i")

        full_result = node.table_checksum("t_heap")
        full_backup_id = self.pb.backup_node('node', node)
        full_target_time = self.pb.show('node', full_backup_id)['recovery-time']

        # PTRACK BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id,"
            " md5(i::text) as text,"
            " md5(i::text)::tsvector as tsvector"
            " from generate_series(100,200) i")

        ptrack_result = node.table_checksum("t_heap")
        ptrack_backup_id = self.pb.backup_node('node', node, backup_type='ptrack')
        ptrack_target_time = self.pb.show('node', ptrack_backup_id)['recovery-time']
        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id,"
            " md5(i::text) as text,"
            " md5(i::text)::tsvector as tsvector"
            " from generate_series(200, 300) i")

        # Drop Node
        node.cleanup()

        # Check full backup
        restore_result = self.pb.restore_node('node', node,
                backup_id=full_backup_id,
                options=[
                    "-j", "4", "--recovery-target-action=promote",
                    "--recovery-target-time={0}".format(full_target_time)]
            )
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(full_backup_id))
        node.slow_start()

        full_result_new = node.table_checksum("t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check ptrack backup
        restore_result = self.pb.restore_node('node', node,
                backup_id=ptrack_backup_id,
                options=[
                    "-j", "4",
                    "--recovery-target-time={0}".format(ptrack_target_time),
                    "--recovery-target-action=promote"]
            )
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(ptrack_backup_id))

        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        node.slow_start()
        ptrack_result_new = node.table_checksum("t_heap")
        self.assertEqual(ptrack_result, ptrack_result_new)

        node.cleanup()

    # @unittest.skip("skip")
    def test_create_db(self):
        """
        Make node, take full backup, create database db1, take ptrack backup,
        restore database and check it presense
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True,
            pg_options={
                'max_wal_size': '10GB'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

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

        # PTRACK BACKUP
        backup_id = self.pb.backup_node('node', node,
            backup_type='ptrack', options=["--stream"])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # RESTORE
        node_restored = self.pg_node.make_simple('node_restored')

        node_restored.cleanup()
        self.pb.restore_node('node', node_restored,
            backup_id=backup_id, options=["-j", "4"])

        # COMPARE PHYSICAL CONTENT
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node_restored.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

        # DROP DATABASE DB1
        node.safe_psql(
            "postgres", "drop database db1")
        # SECOND PTRACK BACKUP
        backup_id = self.pb.backup_node('node', node,
            backup_type='ptrack', options=["--stream"]
        )

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # RESTORE SECOND PTRACK BACKUP
        node_restored.cleanup()
        self.pb.restore_node('node', node_restored,
            backup_id=backup_id, options=["-j", "4"])

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
    def test_create_db_on_replica(self):
        """
        Make node, take full backup, create replica from it,
        take full backup from replica,
        create database db1, take ptrack backup from replica,
        restore database and check it presense
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True,
            pg_options={
                'checkpoint_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,100) i")

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.backup_node('node', node, options=['-j10', '--stream'])

        self.pb.restore_node('node', node=replica)

        # Add replica
        self.pb.add_instance('replica', replica)
        self.set_replica(node, replica, 'replica', synchronous=True)
        replica.slow_start(replica=True)

        self.pb.backup_node('replica', replica,
            options=['-j10', '--stream']
            )

        # CREATE DATABASE DB1
        node.safe_psql("postgres", "create database db1")
        node.safe_psql(
            "db1",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,100) i")

        # Wait until replica catch up with master
        self.wait_until_replica_catch_with_master(node, replica)
        replica.safe_psql('postgres', 'checkpoint')

        # PTRACK BACKUP
        backup_id = self.pb.backup_node('replica',
            replica, backup_type='ptrack',
            options=['-j10', '--stream']
            )

        if self.paranoia:
            pgdata = self.pgdata_content(replica.data_dir)

        # RESTORE
        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('replica', node_restored,
            backup_id=backup_id, options=["-j", "4"])

        # COMPARE PHYSICAL CONTENT
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_alter_table_set_tablespace_ptrack(self):
        """Make node, create tablespace with table, take full backup,
         alter tablespace location, take ptrack backup, restore database."""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True,
            pg_options={
                'checkpoint_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

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

        # sys.exit(1)
        # PTRACK BACKUP
        #result = node.table_checksum("t_heap")
        self.pb.backup_node('node', node,
            backup_type='ptrack',
            options=["--stream"]
        )
        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)
        # node.stop()
        # node.cleanup()

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
            pgdata_restored = self.pgdata_content(
                node_restored.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

#        result_new = node_restored.table_checksum("t_heap")
#
#        self.assertEqual(result, result_new, 'lost some data after restore')

    # @unittest.skip("skip")
    def test_alter_database_set_tablespace_ptrack(self):
        """Make node, create tablespace with database,"
        " take full backup, alter tablespace location,"
        " take ptrack backup, restore database."""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True,
            pg_options={
                'checkpoint_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        # FULL BACKUP
        self.pb.backup_node('node', node, options=["--stream"])

        # CREATE TABLESPACE
        self.create_tblspace_in_node(node, 'somedata')

        # ALTER DATABASE
        node.safe_psql(
            "template1",
            "alter database postgres set tablespace somedata")

        # PTRACK BACKUP
        self.pb.backup_node('node', node, backup_type='ptrack',
            options=["--stream"])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)
        node.stop()

        # RESTORE
        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()
        self.pb.restore_node('node',
            node_restored,
            options=[
                "-j", "4",
                "-T", "{0}={1}".format(
                    self.get_tblspace_path(node, 'somedata'),
                    self.get_tblspace_path(node_restored, 'somedata'))])

        # GET PHYSICAL CONTENT and COMPARE PHYSICAL CONTENT
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node_restored.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        node_restored.port = node.port
        node_restored.slow_start()

    # @unittest.skip("skip")
    def test_drop_tablespace(self):
        """
        Make node, create table, alter table tablespace, take ptrack backup,
        move table from tablespace, take ptrack backup
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True,
            pg_options={
                'checkpoint_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        self.create_tblspace_in_node(node, 'somedata')

        # CREATE TABLE
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,100) i")

        result = node.table_checksum("t_heap")
        # FULL BACKUP
        self.pb.backup_node('node', node, options=["--stream"])

        # Move table to tablespace 'somedata'
        node.safe_psql(
            "postgres", "alter table t_heap set tablespace somedata")
        # PTRACK BACKUP
        self.pb.backup_node('node', node,
            backup_type='ptrack', options=["--stream"])

        # Move table back to default tablespace
        node.safe_psql(
            "postgres", "alter table t_heap set tablespace pg_default")
        # SECOND PTRACK BACKUP
        self.pb.backup_node('node', node,
            backup_type='ptrack', options=["--stream"])

        # DROP TABLESPACE 'somedata'
        node.safe_psql(
            "postgres", "drop tablespace somedata")
        # THIRD PTRACK BACKUP
        self.pb.backup_node('node', node,
            backup_type='ptrack', options=["--stream"])

        if self.paranoia:
            pgdata = self.pgdata_content(
                node.data_dir, ignore_ptrack=True)

        tblspace = self.get_tblspace_path(node, 'somedata')
        node.cleanup()
        shutil.rmtree(tblspace, ignore_errors=True)
        self.pb.restore_node('node', node=node, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node.data_dir, ignore_ptrack=True)

        node.slow_start()

        tblspc_exist = node.safe_psql(
            "postgres",
            "select exists(select 1 from "
            "pg_tablespace where spcname = 'somedata')")

        if tblspc_exist.rstrip() == 't':
            self.assertEqual(
                1, 0,
                "Expecting Error because "
                "tablespace 'somedata' should not be present")

        result_new = node.table_checksum("t_heap")
        self.assertEqual(result, result_new)

        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_ptrack_alter_tablespace(self):
        """
        Make node, create table, alter table tablespace, take ptrack backup,
        move table from tablespace, take ptrack backup
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True,
            pg_options={
                'checkpoint_timeout': '30s'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        self.create_tblspace_in_node(node, 'somedata')
        tblspc_path = self.get_tblspace_path(node, 'somedata')

        # CREATE TABLE
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,100) i")

        result = node.table_checksum("t_heap")
        # FULL BACKUP
        self.pb.backup_node('node', node, options=["--stream"])

        # Move table to separate tablespace
        node.safe_psql(
            "postgres",
            "alter table t_heap set tablespace somedata")
        # GET LOGICAL CONTENT FROM NODE
        result = node.table_checksum("t_heap")

        # FIRTS PTRACK BACKUP
        self.pb.backup_node('node', node, backup_type='ptrack',
            options=["--stream"])

        # GET PHYSICAL CONTENT FROM NODE
        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # Restore ptrack backup
        restored_node = self.pg_node.make_simple('restored_node')
        restored_node.cleanup()
        tblspc_path_new = self.get_tblspace_path(
            restored_node, 'somedata_restored')
        self.pb.restore_node('node', node=restored_node, options=[
            "-j", "4", "-T", "{0}={1}".format(tblspc_path, tblspc_path_new)])

        # GET PHYSICAL CONTENT FROM RESTORED NODE and COMPARE PHYSICAL CONTENT
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                restored_node.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        restored_node.set_auto_conf({'port': restored_node.port})
        restored_node.slow_start()

        # COMPARE LOGICAL CONTENT
        result_new = restored_node.table_checksum("t_heap")
        self.assertEqual(result, result_new)

        restored_node.cleanup()
        shutil.rmtree(tblspc_path_new, ignore_errors=True)

        # Move table to default tablespace
        node.safe_psql(
            "postgres", "alter table t_heap set tablespace pg_default")
        # SECOND PTRACK BACKUP
        self.pb.backup_node('node', node, backup_type='ptrack',
            options=["--stream"])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # Restore second ptrack backup and check table consistency
        self.pb.restore_node('node', restored_node,
            options=[
                "-j", "4", "-T", "{0}={1}".format(tblspc_path, tblspc_path_new)])

        # GET PHYSICAL CONTENT FROM RESTORED NODE and COMPARE PHYSICAL CONTENT
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                restored_node.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        restored_node.set_auto_conf({'port': restored_node.port})
        restored_node.slow_start()

        result_new = restored_node.table_checksum("t_heap")
        self.assertEqual(result, result_new)

    # @unittest.skip("skip")
    def test_ptrack_multiple_segments(self):
        """
        Make node, create table, alter table tablespace,
        take ptrack backup, move table from tablespace, take ptrack backup
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True,
            pg_options={
                'full_page_writes': 'off'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        self.create_tblspace_in_node(node, 'somedata')

        # CREATE TABLE
        node.pgbench_init(scale=100, options=['--tablespace=somedata'])
        result = node.table_checksum("pgbench_accounts")
        # FULL BACKUP
        self.pb.backup_node('node', node, options=['--stream'])

        # PTRACK STUFF
        if node.major_version < 11:
            idx_ptrack = {'type': 'heap'}
            idx_ptrack['path'] = self.get_fork_path(node, 'pgbench_accounts')
            idx_ptrack['old_size'] = self.get_fork_size(node, 'pgbench_accounts')
            idx_ptrack['old_pages'] = self.get_md5_per_page_for_fork(
                idx_ptrack['path'], idx_ptrack['old_size'])

        pgbench = node.pgbench(
            options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        node.safe_psql("postgres", "checkpoint")

        if node.major_version < 11:
            idx_ptrack['new_size'] = self.get_fork_size(
                node,
                'pgbench_accounts')

            idx_ptrack['new_pages'] = self.get_md5_per_page_for_fork(
                idx_ptrack['path'],
                idx_ptrack['new_size'])

            idx_ptrack['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                node,
                idx_ptrack['path'])

            if not self.check_ptrack_sanity(idx_ptrack):
                self.assertTrue(
                    False, 'Ptrack has failed to register changes in data files')

        # GET LOGICAL CONTENT FROM NODE
        # it`s stupid, because hint`s are ignored by ptrack
        result = node.table_checksum("pgbench_accounts")
        # FIRTS PTRACK BACKUP
        self.pb.backup_node('node', node, backup_type='ptrack', options=['--stream'])

        # GET PHYSICAL CONTENT FROM NODE
        pgdata = self.pgdata_content(node.data_dir)

        # RESTORE NODE
        restored_node = self.pg_node.make_simple('restored_node')
        restored_node.cleanup()
        tblspc_path = self.get_tblspace_path(node, 'somedata')
        tblspc_path_new = self.get_tblspace_path(
            restored_node,
            'somedata_restored')

        self.pb.restore_node('node', restored_node,
            options=[
                "-j", "4", "-T", "{0}={1}".format(
                    tblspc_path, tblspc_path_new)])

        # GET PHYSICAL CONTENT FROM NODE_RESTORED
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                restored_node.data_dir, ignore_ptrack=False)

        # START RESTORED NODE
        restored_node.set_auto_conf({'port': restored_node.port})
        restored_node.slow_start()

        result_new = restored_node.table_checksum("pgbench_accounts")

        # COMPARE RESTORED FILES
        self.assertEqual(result, result_new, 'data is lost')

        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_cluster_on_btree(self):
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        self.create_tblspace_in_node(node, 'somedata')

        # Create table and indexes
        node.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap tablespace somedata "
            "as select i as id, nextval('t_seq') as t_seq, "
            "md5(i::text) as text, md5(repeat(i::text,10))::tsvector "
            "as tsvector from generate_series(0,2560) i")
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                node.safe_psql(
                    "postgres",
                    "create index {0} on {1} using {2}({3}) "
                    "tablespace somedata".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        node.safe_psql('postgres', 'vacuum t_heap')
        node.safe_psql('postgres', 'checkpoint')

        if node.major_version < 11:
            for i in idx_ptrack:
                # get size of heap and indexes. size calculated in pages
                idx_ptrack[i]['old_size'] = self.get_fork_size(node, i)
                # get path to heap and index files
                idx_ptrack[i]['path'] = self.get_fork_path(node, i)
                # calculate md5sums of pages
                idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                    idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        self.pb.backup_node('node', node, options=['-j10', '--stream'])

        node.safe_psql('postgres', 'delete from t_heap where id%2 = 1')
        node.safe_psql('postgres', 'cluster t_heap using t_btree')
        node.safe_psql('postgres', 'checkpoint')

        # CHECK PTRACK SANITY
        if node.major_version < 11:
            self.check_ptrack_map_sanity(node, idx_ptrack)

    # @unittest.skip("skip")
    def test_ptrack_cluster_on_gist(self):
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        # Create table and indexes
        node.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap as select i as id, "
            "nextval('t_seq') as t_seq, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                node.safe_psql(
                    "postgres",
                    "create index {0} on {1} using {2}({3})".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        node.safe_psql('postgres', 'vacuum t_heap')
        node.safe_psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get size of heap and indexes. size calculated in pages
            idx_ptrack[i]['old_size'] = self.get_fork_size(node, i)
            # get path to heap and index files
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # calculate md5sums of pages
            idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        self.pb.backup_node('node', node, options=['-j10', '--stream'])

        node.safe_psql('postgres', 'delete from t_heap where id%2 = 1')
        node.safe_psql('postgres', 'cluster t_heap using t_gist')
        node.safe_psql('postgres', 'checkpoint')

        # CHECK PTRACK SANITY
        if node.major_version < 11:
            self.check_ptrack_map_sanity(node, idx_ptrack)

        self.pb.backup_node('node', node,
            backup_type='ptrack', options=['-j10', '--stream'])

        pgdata = self.pgdata_content(node.data_dir)
        node.cleanup()

        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_ptrack_cluster_on_btree_replica(self):
        master = self.pg_node.make_simple('master',
            set_replication=True,
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('master', master)
        master.slow_start()

        if master.major_version >= 11:
            master.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.pb.backup_node('master', master, options=['--stream'])

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.restore_node('master', node=replica)

        self.pb.add_instance('replica', replica)
        self.set_replica(master, replica, synchronous=True)
        replica.slow_start(replica=True)

        # Create table and indexes
        master.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap as select i as id, "
            "nextval('t_seq') as t_seq, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")

        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                master.safe_psql(
                    "postgres",
                    "create index {0} on {1} using {2}({3})".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'],
                        idx_ptrack[i]['column']))

        master.safe_psql('postgres', 'vacuum t_heap')
        master.safe_psql('postgres', 'checkpoint')

        self.pb.backup_node('replica', replica, options=['-j10', '--stream'])

        for i in idx_ptrack:
            # get size of heap and indexes. size calculated in pages
            idx_ptrack[i]['old_size'] = self.get_fork_size(replica, i)
            # get path to heap and index files
            idx_ptrack[i]['path'] = self.get_fork_path(replica, i)
            # calculate md5sums of pages
            idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        master.safe_psql('postgres', 'delete from t_heap where id%2 = 1')
        master.safe_psql('postgres', 'cluster t_heap using t_btree')
        master.safe_psql('postgres', 'checkpoint')

        # Sync master and replica
        self.wait_until_replica_catch_with_master(master, replica)
        replica.safe_psql('postgres', 'checkpoint')

        # CHECK PTRACK SANITY
        if master.major_version < 11:
            self.check_ptrack_map_sanity(replica, idx_ptrack)

        self.pb.backup_node('replica', replica,
            backup_type='ptrack', options=['-j10', '--stream'])

        pgdata = self.pgdata_content(replica.data_dir)

        node = self.pg_node.make_simple('node')
        node.cleanup()

        self.pb.restore_node('replica', node=node)

        pgdata_restored = self.pgdata_content(replica.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_ptrack_cluster_on_gist_replica(self):
        master = self.pg_node.make_simple('master',
            set_replication=True,
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('master', master)
        master.slow_start()

        if master.major_version >= 11:
            master.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.pb.backup_node('master', master, options=['--stream'])

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.restore_node('master', node=replica)

        self.pb.add_instance('replica', replica)
        self.set_replica(master, replica, 'replica', synchronous=True)
        replica.slow_start(replica=True)

        # Create table and indexes
        master.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap as select i as id, "
            "nextval('t_seq') as t_seq, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")

        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                master.safe_psql(
                    "postgres",
                    "create index {0} on {1} using {2}({3})".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'],
                        idx_ptrack[i]['column']))

        master.safe_psql('postgres', 'vacuum t_heap')
        master.safe_psql('postgres', 'checkpoint')

        # Sync master and replica
        self.wait_until_replica_catch_with_master(master, replica)
        replica.safe_psql('postgres', 'checkpoint')

        self.pb.backup_node('replica', replica, options=[
                '-j10', '--stream'])

        for i in idx_ptrack:
            # get size of heap and indexes. size calculated in pages
            idx_ptrack[i]['old_size'] = self.get_fork_size(replica, i)
            # get path to heap and index files
            idx_ptrack[i]['path'] = self.get_fork_path(replica, i)
            # calculate md5sums of pages
            idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        master.safe_psql('postgres', 'DELETE FROM t_heap WHERE id%2 = 1')
        master.safe_psql('postgres', 'CLUSTER t_heap USING t_gist')

        if master.major_version < 11:
            master.safe_psql('postgres', 'CHECKPOINT')

        # Sync master and replica
        self.wait_until_replica_catch_with_master(master, replica)

        if master.major_version < 11:
            replica.safe_psql('postgres', 'CHECKPOINT')
            self.check_ptrack_map_sanity(replica, idx_ptrack)

        self.pb.backup_node('replica', replica,
            backup_type='ptrack', options=['-j10', '--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(replica.data_dir)

        node = self.pg_node.make_simple('node')
        node.cleanup()

        self.pb.restore_node('replica', node)

        if self.paranoia:
            pgdata_restored = self.pgdata_content(replica.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_empty(self):
        """Take backups of every available types and check that PTRACK is clean"""
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        self.create_tblspace_in_node(node, 'somedata')

        # Create table
        node.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap "
            "(id int DEFAULT nextval('t_seq'), text text, tsvector tsvector) "
            "tablespace somedata")

        # Take FULL backup to clean every ptrack
        self.pb.backup_node('node', node,
            options=['-j10', '--stream'])

        # Create indexes
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                node.safe_psql(
                    "postgres",
                    "create index {0} on {1} using {2}({3}) "
                    "tablespace somedata".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'],
                        idx_ptrack[i]['column']))

        node.safe_psql('postgres', 'checkpoint')

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        tblspace1 = self.get_tblspace_path(node, 'somedata')
        tblspace2 = self.get_tblspace_path(node_restored, 'somedata')

        # Take PTRACK backup
        backup_id = self.pb.backup_node('node', node, backup_type='ptrack',
            options=['-j10', '--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.pb.restore_node('node', node_restored,
            backup_id=backup_id,
            options=[
                "-j", "4",
                "-T{0}={1}".format(tblspace1, tblspace2)])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_empty_replica(self):
        """
        Take backups of every available types from master
        and check that PTRACK on replica is clean
        """
        master = self.pg_node.make_simple('master',
            set_replication=True,
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('master', master)
        master.slow_start()

        if master.major_version >= 11:
            master.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.pb.backup_node('master', master, options=['--stream'])

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.restore_node('master', node=replica)

        self.pb.add_instance('replica', replica)
        self.set_replica(master, replica, synchronous=True)
        replica.slow_start(replica=True)

        # Create table
        master.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap "
            "(id int DEFAULT nextval('t_seq'), text text, tsvector tsvector)")
        self.wait_until_replica_catch_with_master(master, replica)

        # Take FULL backup
        self.pb.backup_node(
            'replica',
            replica,
            options=['-j10', '--stream'])

        # Create indexes
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                master.safe_psql(
                    "postgres",
                    "create index {0} on {1} using {2}({3})".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'],
                        idx_ptrack[i]['column']))

        self.wait_until_replica_catch_with_master(master, replica)

        # Take PTRACK backup
        backup_id = self.pb.backup_node(
            'replica',
            replica,
            backup_type='ptrack',
            options=['-j1', '--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(replica.data_dir)

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('replica', node_restored,
            backup_id=backup_id, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_truncate(self):
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        self.create_tblspace_in_node(node, 'somedata')

        # Create table and indexes
        node.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap tablespace somedata "
            "as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")

        if node.major_version < 11:
            for i in idx_ptrack:
                if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                    node.safe_psql(
                        "postgres",
                        "create index {0} on {1} using {2}({3}) "
                        "tablespace somedata".format(
                            i, idx_ptrack[i]['relation'],
                            idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        self.pb.backup_node('node', node, options=['--stream'])

        node.safe_psql('postgres', 'truncate t_heap')
        node.safe_psql('postgres', 'checkpoint')

        if node.major_version < 11:
            for i in idx_ptrack:
                # get fork size and calculate it in pages
                idx_ptrack[i]['old_size'] = self.get_fork_size(node, i)
                # get path to heap and index files
                idx_ptrack[i]['path'] = self.get_fork_path(node, i)
                # calculate md5sums for every page of this fork
                idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                    idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        # Make backup to clean every ptrack
        self.pb.backup_node('node', node,
            backup_type='ptrack', options=['-j10', '--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        if node.major_version < 11:
            for i in idx_ptrack:
                idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                    node, idx_ptrack[i]['path'], [idx_ptrack[i]['old_size']])
                self.check_ptrack_clean(idx_ptrack[i], idx_ptrack[i]['old_size'])

        node.cleanup()
        shutil.rmtree(
            self.get_tblspace_path(node, 'somedata'),
            ignore_errors=True)
    
        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_basic_ptrack_truncate_replica(self):
        master = self.pg_node.make_simple('master',
            set_replication=True,
            ptrack_enable=True,
            pg_options={
                'max_wal_size': '32MB',
                'archive_timeout': '10s',
                'checkpoint_timeout': '5min'})

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('master', master)
        master.slow_start()

        if master.major_version >= 11:
            master.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.pb.backup_node('master', master, options=['--stream'])

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.restore_node('master', node=replica)

        self.pb.add_instance('replica', replica)
        self.set_replica(master, replica, 'replica', synchronous=True)
        replica.slow_start(replica=True)

        # Create table and indexes
        master.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap "
            "as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")

        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                master.safe_psql(
                    "postgres",
                    "create index {0} on {1} using {2}({3}) ".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        # Sync master and replica
        self.wait_until_replica_catch_with_master(master, replica)
        replica.safe_psql('postgres', 'checkpoint')

        if replica.major_version < 11:
            for i in idx_ptrack:
                # get fork size and calculate it in pages
                idx_ptrack[i]['old_size'] = self.get_fork_size(replica, i)
                # get path to heap and index files
                idx_ptrack[i]['path'] = self.get_fork_path(replica, i)
                # calculate md5sums for every page of this fork
                idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                    idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        # Make backup to clean every ptrack
        self.pb.backup_node('replica', replica,
            options=['-j10', '--stream'])

        if replica.major_version < 11:
            for i in idx_ptrack:
                idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                    replica, idx_ptrack[i]['path'], [idx_ptrack[i]['old_size']])
                self.check_ptrack_clean(idx_ptrack[i], idx_ptrack[i]['old_size'])

        master.safe_psql('postgres', 'truncate t_heap')

        # Sync master and replica
        self.wait_until_replica_catch_with_master(master, replica)

        if replica.major_version < 10:
            replica.safe_psql(
                "postgres",
                "select pg_xlog_replay_pause()")
        else:
            replica.safe_psql(
                "postgres",
                "select pg_wal_replay_pause()")

        self.pb.backup_node('replica', replica, backup_type='ptrack',
            options=['-j10', '--stream'])

        pgdata = self.pgdata_content(replica.data_dir)

        node = self.pg_node.make_simple('node')
        node.cleanup()

        self.pb.restore_node('replica', node)

        pgdata_restored = self.pgdata_content(node.data_dir)

        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)

        node.set_auto_conf({'port': node.port})

        node.slow_start()

        node.safe_psql(
            'postgres',
            'select 1')

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_vacuum(self):
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        self.create_tblspace_in_node(node, 'somedata')

        # Create table and indexes
        node.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap tablespace somedata "
            "as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                node.safe_psql(
                    "postgres",
                    "create index {0} on {1} using {2}({3}) "
                    "tablespace somedata".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'],
                        idx_ptrack[i]['column']))

        comparision_exclusion = self.get_known_bugs_comparision_exclusion_dict(node)

        node.safe_psql('postgres', 'vacuum t_heap')
        node.safe_psql('postgres', 'checkpoint')

        # Make full backup to clean every ptrack
        self.pb.backup_node('node', node, options=['-j10', '--stream'])

        if node.major_version < 11:
            for i in idx_ptrack:
                # get fork size and calculate it in pages
                idx_ptrack[i]['old_size'] = self.get_fork_size(node, i)
                # get path to heap and index files
                idx_ptrack[i]['path'] = self.get_fork_path(node, i)
                # calculate md5sums for every page of this fork
                idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                    idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])
                idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                    node, idx_ptrack[i]['path'], [idx_ptrack[i]['old_size']])
                self.check_ptrack_clean(idx_ptrack[i], idx_ptrack[i]['old_size'])

        # Delete some rows, vacuum it and make checkpoint
        node.safe_psql('postgres', 'delete from t_heap where id%2 = 1')
        node.safe_psql('postgres', 'vacuum t_heap')
        node.safe_psql('postgres', 'checkpoint')

        # CHECK PTRACK SANITY
        if node.major_version < 11:
            self.check_ptrack_map_sanity(node, idx_ptrack)

        self.pb.backup_node('node', node,
            backup_type='ptrack', options=['-j10', '--stream'])

        pgdata = self.pgdata_content(node.data_dir)
        node.cleanup()

        shutil.rmtree(
            self.get_tblspace_path(node, 'somedata'),
            ignore_errors=True)
    
        self.pb.restore_node('node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored, comparision_exclusion)

    # @unittest.skip("skip")
    def test_ptrack_vacuum_replica(self):
        master = self.pg_node.make_simple('master',
            set_replication=True,
            ptrack_enable=True,
            pg_options={
                'checkpoint_timeout': '30'})

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('master', master)
        master.slow_start()

        if master.major_version >= 11:
            master.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.pb.backup_node('master', master, options=['--stream'])

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.restore_node('master', node=replica)

        self.pb.add_instance('replica', replica)
        self.set_replica(master, replica, 'replica', synchronous=True)
        replica.slow_start(replica=True)

        # Create table and indexes
        master.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap as select i as id, "
            "md5(i::text) as text, md5(repeat(i::text,10))::tsvector "
            "as tsvector from generate_series(0,2560) i")

        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                master.safe_psql(
                    "postgres",
                    "create index {0} on {1} using {2}({3})".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        master.safe_psql('postgres', 'vacuum t_heap')
        master.safe_psql('postgres', 'checkpoint')

        # Sync master and replica
        self.wait_until_replica_catch_with_master(master, replica)
        replica.safe_psql('postgres', 'checkpoint')

        # Make FULL backup to clean every ptrack
        self.pb.backup_node('replica', replica, options=['-j10', '--stream'])

        if replica.major_version < 11:
            for i in idx_ptrack:
                # get fork size and calculate it in pages
                idx_ptrack[i]['old_size'] = self.get_fork_size(replica, i)
                # get path to heap and index files
                idx_ptrack[i]['path'] = self.get_fork_path(replica, i)
                # calculate md5sums for every page of this fork
                idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                    idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])
                idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                    replica, idx_ptrack[i]['path'], [idx_ptrack[i]['old_size']])
                self.check_ptrack_clean(idx_ptrack[i], idx_ptrack[i]['old_size'])

        # Delete some rows, vacuum it and make checkpoint
        master.safe_psql('postgres', 'delete from t_heap where id%2 = 1')
        master.safe_psql('postgres', 'vacuum t_heap')
        master.safe_psql('postgres', 'checkpoint')

        # Sync master and replica
        self.wait_until_replica_catch_with_master(master, replica)
        replica.safe_psql('postgres', 'checkpoint')

        # CHECK PTRACK SANITY
        if replica.major_version < 11:
            self.check_ptrack_map_sanity(master, idx_ptrack)

        self.pb.backup_node('replica', replica,
            backup_type='ptrack', options=['-j10', '--stream'])

        pgdata = self.pgdata_content(replica.data_dir)

        node = self.pg_node.make_simple('node')
        node.cleanup()
    
        self.pb.restore_node('replica', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_vacuum_bits_frozen(self):
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        self.create_tblspace_in_node(node, 'somedata')

        # Create table and indexes
        res = node.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap tablespace somedata "
            "as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                node.safe_psql(
                    "postgres",
                    "create index {0} on {1} using {2}({3}) "
                    "tablespace somedata".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'],
                        idx_ptrack[i]['column']))

        comparision_exclusion = self.get_known_bugs_comparision_exclusion_dict(node)
        node.safe_psql('postgres', 'checkpoint')

        self.pb.backup_node('node', node, options=['-j10', '--stream'])

        node.safe_psql('postgres', 'vacuum freeze t_heap')
        node.safe_psql('postgres', 'checkpoint')

        if node.major_version < 11:
            for i in idx_ptrack:
                # get size of heap and indexes. size calculated in pages
                idx_ptrack[i]['old_size'] = self.get_fork_size(node, i)
                # get path to heap and index files
                idx_ptrack[i]['path'] = self.get_fork_path(node, i)
                # calculate md5sums of pages
                idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                    idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        # CHECK PTRACK SANITY
        if node.major_version < 11:
            self.check_ptrack_map_sanity(node, idx_ptrack)

        self.pb.backup_node('node', node,
            backup_type='ptrack', options=['-j10', '--stream'])

        pgdata = self.pgdata_content(node.data_dir)
        node.cleanup()
        shutil.rmtree(
            self.get_tblspace_path(node, 'somedata'),
            ignore_errors=True)
    
        self.pb.restore_node('node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored, comparision_exclusion)

    # @unittest.skip("skip")
    def test_ptrack_vacuum_bits_frozen_replica(self):
        master = self.pg_node.make_simple('master',
            set_replication=True,
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('master', master)
        master.slow_start()

        if master.major_version >= 11:
            master.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.pb.backup_node('master', master, options=['--stream'])

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.restore_node('master', node=replica)

        self.pb.add_instance('replica', replica)
        self.set_replica(master, replica, synchronous=True)
        replica.slow_start(replica=True)

        # Create table and indexes
        master.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap as select i as id, "
            "md5(i::text) as text, md5(repeat(i::text,10))::tsvector "
            "as tsvector from generate_series(0,2560) i")
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                master.safe_psql(
                    "postgres",
                    "create index {0} on {1} using {2}({3})".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'],
                        idx_ptrack[i]['column']))

        master.safe_psql('postgres', 'checkpoint')

        # Sync master and replica
        self.wait_until_replica_catch_with_master(master, replica)
        replica.safe_psql('postgres', 'checkpoint')

        # Take backup to clean every ptrack
        self.pb.backup_node('replica', replica,
            options=['-j10', '--stream'])

        if replica.major_version < 11:
            for i in idx_ptrack:
                # get size of heap and indexes. size calculated in pages
                idx_ptrack[i]['old_size'] = self.get_fork_size(replica, i)
                # get path to heap and index files
                idx_ptrack[i]['path'] = self.get_fork_path(replica, i)
                # calculate md5sums of pages
                idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                    idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        master.safe_psql('postgres', 'vacuum freeze t_heap')
        master.safe_psql('postgres', 'checkpoint')

        # Sync master and replica
        self.wait_until_replica_catch_with_master(master, replica)
        replica.safe_psql('postgres', 'checkpoint')

        # CHECK PTRACK SANITY
        if replica.major_version < 11:
            self.check_ptrack_map_sanity(master, idx_ptrack)

        self.pb.backup_node('replica', replica, backup_type='ptrack',
            options=['-j10', '--stream'])

        pgdata = self.pgdata_content(replica.data_dir)
        replica.cleanup()
    
        self.pb.restore_node('replica', node=replica)

        pgdata_restored = self.pgdata_content(replica.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_vacuum_bits_visibility(self):
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        self.create_tblspace_in_node(node, 'somedata')

        # Create table and indexes
        res = node.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap tablespace somedata "
            "as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")

        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                node.safe_psql(
                    "postgres",
                    "create index {0} on {1} using {2}({3}) "
                    "tablespace somedata".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        comparision_exclusion = self.get_known_bugs_comparision_exclusion_dict(node)
        node.safe_psql('postgres', 'checkpoint')

        self.pb.backup_node('node', node, options=['-j10', '--stream'])

        if node.major_version < 11:
            for i in idx_ptrack:
                # get size of heap and indexes. size calculated in pages
                idx_ptrack[i]['old_size'] = self.get_fork_size(node, i)
                # get path to heap and index files
                idx_ptrack[i]['path'] = self.get_fork_path(node, i)
                # calculate md5sums of pages
                idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                    idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        node.safe_psql('postgres', 'vacuum t_heap')
        node.safe_psql('postgres', 'checkpoint')

        # CHECK PTRACK SANITY
        if node.major_version < 11:
            self.check_ptrack_map_sanity(node, idx_ptrack)

        self.pb.backup_node('node', node,
            backup_type='ptrack', options=['-j10', '--stream'])

        pgdata = self.pgdata_content(node.data_dir)
        node.cleanup()
        shutil.rmtree(
            self.get_tblspace_path(node, 'somedata'),
            ignore_errors=True)
    
        self.pb.restore_node('node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored, comparision_exclusion)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_vacuum_full_2(self):
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True,
            pg_options={ 'wal_log_hints': 'on' })

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        self.create_tblspace_in_node(node, 'somedata')

        # Create table and indexes
        res = node.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap tablespace somedata "
            "as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                node.safe_psql(
                    "postgres", "create index {0} on {1} "
                    "using {2}({3}) tablespace somedata".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        node.safe_psql('postgres', 'vacuum t_heap')
        node.safe_psql('postgres', 'checkpoint')

        self.pb.backup_node('node', node, options=['-j10', '--stream'])

        if node.major_version < 11:
            for i in idx_ptrack:
                # get size of heap and indexes. size calculated in pages
                idx_ptrack[i]['old_size'] = self.get_fork_size(node, i)
                # get path to heap and index files
                idx_ptrack[i]['path'] = self.get_fork_path(node, i)
                # calculate md5sums of pages
                idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                    idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        node.safe_psql('postgres', 'delete from t_heap where id%2 = 1')
        node.safe_psql('postgres', 'vacuum full t_heap')
        node.safe_psql('postgres', 'checkpoint')

        if node.major_version < 11:
            self.check_ptrack_map_sanity(node, idx_ptrack)

        self.pb.backup_node('node', node,
            backup_type='ptrack', options=['-j10', '--stream'])

        pgdata = self.pgdata_content(node.data_dir)
        node.cleanup()

        shutil.rmtree(
            self.get_tblspace_path(node, 'somedata'),
            ignore_errors=True)
    
        self.pb.restore_node('node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_vacuum_full_replica(self):
        master = self.pg_node.make_simple('master',
            set_replication=True,
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('master', master)
        master.slow_start()

        if master.major_version >= 11:
            master.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.pb.backup_node('master', master, options=['--stream'])
        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.restore_node('master', node=replica)

        self.pb.add_instance('replica', replica)
        self.set_replica(master, replica, 'replica', synchronous=True)
        replica.slow_start(replica=True)

        # Create table and indexes
        master.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap as select i as id, "
            "md5(i::text) as text, md5(repeat(i::text,10))::tsvector as "
            "tsvector from generate_series(0,256000) i")

        if master.major_version < 11:
            for i in idx_ptrack:
                if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                    master.safe_psql(
                        "postgres",
                        "create index {0} on {1} using {2}({3})".format(
                            i, idx_ptrack[i]['relation'],
                            idx_ptrack[i]['type'],
                            idx_ptrack[i]['column']))

        master.safe_psql('postgres', 'vacuum t_heap')
        master.safe_psql('postgres', 'checkpoint')

        # Sync master and replica
        self.wait_until_replica_catch_with_master(master, replica)
        replica.safe_psql('postgres', 'checkpoint')

        # Take FULL backup to clean every ptrack
        self.pb.backup_node('replica', replica,
            options=['-j10', '--stream'])

        if replica.major_version < 11:
            for i in idx_ptrack:
                # get size of heap and indexes. size calculated in pages
                idx_ptrack[i]['old_size'] = self.get_fork_size(replica, i)
                # get path to heap and index files
                idx_ptrack[i]['path'] = self.get_fork_path(replica, i)
                # calculate md5sums of pages
                idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                    idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        master.safe_psql('postgres', 'delete from t_heap where id%2 = 1')
        master.safe_psql('postgres', 'vacuum full t_heap')
        master.safe_psql('postgres', 'checkpoint')

        # Sync master and replica
        self.wait_until_replica_catch_with_master(master, replica)
        replica.safe_psql('postgres', 'checkpoint')

        if replica.major_version < 11:
            self.check_ptrack_map_sanity(master, idx_ptrack)

        self.pb.backup_node('replica', replica,
            backup_type='ptrack', options=['-j10', '--stream'])

        pgdata = self.pgdata_content(replica.data_dir)
        replica.cleanup()
    
        self.pb.restore_node('replica', node=replica)

        pgdata_restored = self.pgdata_content(replica.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_vacuum_truncate_2(self):
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        # Create table and indexes
        res = node.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap "
            "as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")

        if node.major_version < 11:
            for i in idx_ptrack:
                if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                    node.safe_psql(
                        "postgres", "create index {0} on {1} using {2}({3})".format(
                            i, idx_ptrack[i]['relation'],
                            idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        node.safe_psql('postgres', 'VACUUM t_heap')

        self.pb.backup_node('node', node, options=['-j10', '--stream'])

        if node.major_version < 11:
            for i in idx_ptrack:
                # get size of heap and indexes. size calculated in pages
                idx_ptrack[i]['old_size'] = self.get_fork_size(node, i)
                # get path to heap and index files
                idx_ptrack[i]['path'] = self.get_fork_path(node, i)
                # calculate md5sums of pages
                idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                    idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        node.safe_psql('postgres', 'DELETE FROM t_heap WHERE id > 128')
        node.safe_psql('postgres', 'VACUUM t_heap')
        node.safe_psql('postgres', 'CHECKPOINT')

        # CHECK PTRACK SANITY
        if node.major_version < 11:
            self.check_ptrack_map_sanity(node, idx_ptrack)

        self.pb.backup_node('node', node,
            backup_type='ptrack', options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored)

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_vacuum_truncate_replica(self):
        master = self.pg_node.make_simple('master',
            set_replication=True,
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('master', master)
        master.slow_start()

        if master.major_version >= 11:
            master.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.pb.backup_node('master', master, options=['--stream'])

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.restore_node('master', node=replica)

        self.pb.add_instance('replica', replica)
        self.set_replica(master, replica, 'replica', synchronous=True)
        replica.slow_start(replica=True)

        # Create table and indexes
        master.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap as select i as id, "
            "md5(i::text) as text, md5(repeat(i::text,10))::tsvector "
            "as tsvector from generate_series(0,2560) i")

        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                master.safe_psql(
                    "postgres", "create index {0} on {1} "
                    "using {2}({3})".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        master.safe_psql('postgres', 'vacuum t_heap')
        master.safe_psql('postgres', 'checkpoint')

        # Take FULL backup to clean every ptrack
        self.pb.backup_node('replica', replica,
            options=['-j10', '--stream']
            )

        if master.major_version < 11:
            for i in idx_ptrack:
                # get size of heap and indexes. size calculated in pages
                idx_ptrack[i]['old_size'] = self.get_fork_size(replica, i)
                # get path to heap and index files
                idx_ptrack[i]['path'] = self.get_fork_path(replica, i)
                # calculate md5sums of pages
                idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                    idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        master.safe_psql('postgres', 'DELETE FROM t_heap WHERE id > 128;')
        master.safe_psql('postgres', 'VACUUM t_heap')
        master.safe_psql('postgres', 'CHECKPOINT')

        # Sync master and replica
        self.wait_until_replica_catch_with_master(master, replica)
        replica.safe_psql('postgres', 'CHECKPOINT')

        # CHECK PTRACK SANITY
        if master.major_version < 11:
            self.check_ptrack_map_sanity(master, idx_ptrack)

        self.pb.backup_node('replica', replica, backup_type='ptrack',
            options=[
                '--stream',
                '--log-level-file=INFO',
                '--archive-timeout=30'])

        pgdata = self.pgdata_content(replica.data_dir)

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('replica', node_restored)

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    @unittest.skip("skip")
    def test_ptrack_recovery(self):
        """
        Check that ptrack map contain correct bits after recovery.
        Actual only for PTRACK 1.x
        """
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'somedata')

        # Create table
        node.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap tablespace somedata "
            "as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")

        # Create indexes
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                node.safe_psql(
                    "postgres", "create index {0} on {1} using {2}({3}) "
                    "tablespace somedata".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'], idx_ptrack[i]['column']))

            # get size of heap and indexes. size calculated in pages
            idx_ptrack[i]['size'] = int(self.get_fork_size(node, i))
            # get path to heap and index files
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)

        if self.verbose:
            print('Killing postmaster. Losing Ptrack changes')
        node.stop(['-m', 'immediate', '-D', node.data_dir])
        if not node.status():
            node.slow_start()
        else:
            print("Die! Die! Why won't you die?... Why won't you die?")
            exit(1)

        for i in idx_ptrack:
            # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                node, idx_ptrack[i]['path'], [idx_ptrack[i]['size']])
            # check that ptrack has correct bits after recovery
            self.check_ptrack_recovery(idx_ptrack[i])

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_recovery_1(self):
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True,
            pg_options={
                'shared_buffers': '512MB',
                'max_wal_size': '3GB'})

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        # Create table
        node.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap "
            "as select nextval('t_seq')::int as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
#            "from generate_series(0,25600) i")
            "from generate_series(0,2560) i")

        self.pb.backup_node('node', node, options=['--stream'])

        # Create indexes
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                node.safe_psql(
                    "postgres",
                    "CREATE INDEX {0} ON {1} USING {2}({3})".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        node.safe_psql(
            'postgres',
            "update t_heap set id = nextval('t_seq'), text = md5(text), "
            "tsvector = md5(repeat(tsvector::text, 10))::tsvector")

        node.safe_psql(
            'postgres',
            "create extension pg_buffercache")

        #print(node.safe_psql(
        #    'postgres',
        #    "SELECT count(*) FROM pg_buffercache WHERE isdirty"))

        if self.verbose:
            print('Killing postmaster. Losing Ptrack changes')
        node.stop(['-m', 'immediate', '-D', node.data_dir])

        if not node.status():
            node.slow_start()
        else:
            print("Die! Die! Why won't you die?... Why won't you die?")
            exit(1)

        self.pb.backup_node('node', node,
            backup_type='ptrack', options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored)

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_zero_changes(self):
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        # Create table
        node.safe_psql(
            "postgres",
            "create table t_heap "
            "as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")

        self.pb.backup_node('node', node, options=['--stream'])

        self.pb.backup_node('node', node,
            backup_type='ptrack', options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)
        node.cleanup()

        self.pb.restore_node('node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_pg_resetxlog(self):
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True,
            pg_options={
                'shared_buffers': '512MB',
                'max_wal_size': '3GB'})

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        # Create table
        node.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap "
            "as select nextval('t_seq')::int as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
#            "from generate_series(0,25600) i")
            "from generate_series(0,2560) i")

        self.pb.backup_node('node', node, options=['--stream'])

        # Create indexes
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                node.safe_psql(
                    "postgres",
                    "CREATE INDEX {0} ON {1} USING {2}({3})".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        node.safe_psql(
            'postgres',
            "update t_heap set id = nextval('t_seq'), text = md5(text), "
            "tsvector = md5(repeat(tsvector::text, 10))::tsvector")

#        node.safe_psql(
#            'postgres',
#            "create extension pg_buffercache")
#
#        print(node.safe_psql(
#            'postgres',
#            "SELECT count(*) FROM pg_buffercache WHERE isdirty"))

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
#                backup_type='ptrack', options=['--stream'])

        self.pb.backup_node('node', node, backup_type='ptrack',
                         options=['--stream'],
                         expect_error="because instance was brutalized by pg_resetxlog")
        self.assertMessage(regex='ERROR: LSN from ptrack_control .* '
                                 'is greater than Start LSN of previous backup')

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

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_corrupt_ptrack_map(self):
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        ptrack_version = self.get_ptrack_version(node)

        # Create table
        node.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap "
            "as select nextval('t_seq')::int as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")

        self.pb.backup_node('node', node, options=['--stream'])

        node.safe_psql(
            'postgres',
            "update t_heap set id = nextval('t_seq'), text = md5(text), "
            "tsvector = md5(repeat(tsvector::text, 10))::tsvector")

        # kill the bastard
        if self.verbose:
            print('Killing postmaster. Losing Ptrack changes')

        node.stop(['-m', 'immediate', '-D', node.data_dir])

        ptrack_map = os.path.join(node.data_dir, 'global', 'ptrack.map')

        # Let`s do index corruption. ptrack.map
        with open(ptrack_map, "rb+", 0) as f:
            f.seek(42)
            f.write(b"blablahblahs")
            f.flush()
            f.close

#        os.remove(os.path.join(node.logs_dir, node.pg_log_name))

        if self.verbose:
            print('Ptrack version:', ptrack_version)
        if ptrack_version >= self.version_to_num("2.3"):
            node.slow_start()

            log_file = os.path.join(node.logs_dir, 'postgresql.log')
            with open(log_file, 'r') as f:
                log_content = f.read()

            self.assertIn(
                'WARNING:  ptrack read map: incorrect checksum of file "{0}"'.format(ptrack_map),
                log_content)

            node.stop(['-D', node.data_dir])
        else:
            try:
                node.slow_start()
                # we should die here because exception is what we expect to happen
                self.assertEqual(
                    1, 0,
                    "Expecting Error because ptrack.map is corrupted"
                    "\n Output: {0} \n CMD: {1}".format(
                        repr(self.output), self.cmd))
            except StartNodeException as e:
                self.assertIn(
                    'Cannot start node',
                    e.message,
                    '\n Unexpected Error Message: {0}\n'
                    ' CMD: {1}'.format(repr(e.message), self.cmd))

            log_file = os.path.join(node.logs_dir, 'postgresql.log')
            with open(log_file, 'r') as f:
                log_content = f.read()

            self.assertIn(
               'FATAL:  ptrack init: incorrect checksum of file "{0}"'.format(ptrack_map),
                log_content)

        node.set_auto_conf({'ptrack.map_size': '0'})
        node.slow_start()

        self.pb.backup_node('node', node, backup_type='ptrack',
                         options=['--stream'],
                         expect_error="because instance ptrack is disabled")
        self.assertMessage(contains='ERROR: Ptrack is disabled')

        node.safe_psql(
            'postgres',
            "update t_heap set id = nextval('t_seq'), text = md5(text), "
            "tsvector = md5(repeat(tsvector::text, 10))::tsvector")

        node.stop(['-m', 'immediate', '-D', node.data_dir])

        node.set_auto_conf({'ptrack.map_size': '32', 'shared_preload_libraries': 'ptrack'})
        node.slow_start()

        self.pb.backup_node('node', node, backup_type='ptrack',
                         options=['--stream'],
                         expect_error="because ptrack map is from future")
        self.assertMessage(contains='ERROR: LSN from ptrack_control')

        self.pb.backup_node('node', node,
            backup_type='delta', options=['--stream'])

        node.safe_psql(
            'postgres',
            "update t_heap set id = nextval('t_seq'), text = md5(text), "
            "tsvector = md5(repeat(tsvector::text, 10))::tsvector")

        self.pb.backup_node('node', node,
            backup_type='ptrack', options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        node.cleanup()

        self.pb.restore_node('node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_horizon_lsn_ptrack(self):
        """
        https://github.com/postgrespro/pg_probackup/pull/386
        """
        if not self.probackup_old_path:
            self.skipTest("You must specify PGPROBACKUPBIN_OLD"
                          " for run this test")
        self.assertLessEqual(
            self.version_to_num(self.old_probackup_version),
            self.version_to_num('2.4.15'),
            'You need pg_probackup old_binary =< 2.4.15 for this test')

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        self.assertGreaterEqual(
            self.get_ptrack_version(node),
            self.version_to_num("2.1"),
            "You need ptrack >=2.1 for this test")

        # set map_size to a minimal value
        node.set_auto_conf({'ptrack.map_size': '1'})
        node.restart()

        node.pgbench_init(scale=100)

        # FULL backup
        full_id = self.pb.backup_node('node', node, options=['--stream'], old_binary=True)

        # enable archiving so the WAL size to do interfere with data bytes comparison later
        self.pb.set_archiving('node', node)
        node.restart()

        # change data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # DELTA is exemplar
        delta_id = self.pb.backup_node('node', node, backup_type='delta')
        delta_bytes = self.pb.show('node', backup_id=delta_id)["data-bytes"]
        self.pb.delete('node', backup_id=delta_id)

        # PTRACK with current binary
        ptrack_id = self.pb.backup_node('node', node, backup_type='ptrack')
        ptrack_bytes = self.pb.show('node', backup_id=ptrack_id)["data-bytes"]

        # make sure that backup size is exactly the same
        self.assertEqual(delta_bytes, ptrack_bytes)
