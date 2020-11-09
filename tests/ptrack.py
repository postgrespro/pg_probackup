import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException, idx_ptrack
from datetime import datetime, timedelta
import subprocess
from testgres import QueryException, StartNodeException
import shutil
import sys
from time import sleep
from threading import Thread


module_name = 'ptrack'


class PtrackTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    def test_ptrack_stop_pg(self):
        """
        create node, take full backup,
        restart node, check that ptrack backup
        can be taken
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
            node.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        node.pgbench_init(scale=1)

        # FULL backup
        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        node.stop()
        node.slow_start()

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=['--stream'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

        # @unittest.skip("skip")
    def test_ptrack_multi_timeline_backup(self):
        """
        t2            /------P2
        t1 ------F---*-----P1
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
            node.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        node.pgbench_init(scale=5)

        # FULL backup
        full_id = self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        sleep(15)

        xid = node.safe_psql(
            'postgres',
            'SELECT txid_current()').decode('utf-8').rstrip()
        pgbench.wait()

        self.backup_node(backup_dir, 'node', node, backup_type='ptrack')

        node.cleanup()

        # Restore from full backup to create Timeline 2
        print(self.restore_node(
            backup_dir, 'node', node,
            options=[
                '--recovery-target-xid={0}'.format(xid),
                '--recovery-target-action=promote']))

        node.slow_start()

        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        self.backup_node(backup_dir, 'node', node, backup_type='ptrack')

        pgdata = self.pgdata_content(node.data_dir)

        node.cleanup()

        self.restore_node(backup_dir, 'node', node)

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

        # Clean after yourself
        self.del_test_dir(module_name, fname)

        # @unittest.skip("skip")
    def test_ptrack_multi_timeline_backup_1(self):
        """
        t2              /------
        t1 ---F---P1---*

        # delete P1
        t2              /------P2
        t1 ---F--------*
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
            node.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        node.pgbench_init(scale=5)

        # FULL backup
        full_id = self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        ptrack_id = self.backup_node(backup_dir, 'node', node, backup_type='ptrack')
        node.cleanup()

        self.restore_node(backup_dir, 'node', node)

        node.slow_start()

        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # delete old PTRACK backup
        self.delete_pb(backup_dir, 'node', backup_id=ptrack_id)

        # take new PTRACK backup
        self.backup_node(backup_dir, 'node', node, backup_type='ptrack')

        pgdata = self.pgdata_content(node.data_dir)

        node.cleanup()

        self.restore_node(backup_dir, 'node', node)

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

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_eat_my_data(self):
        """
        PGPRO-4051
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
            node.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        node.pgbench_init(scale=50)

        self.backup_node(backup_dir, 'node', node)

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))

        pgbench = node.pgbench(options=['-T', '300', '-c', '1', '--no-vacuum'])

        for i in range(10):
            print("Iteration: {0}".format(i))

            sleep(2)

            self.backup_node(backup_dir, 'node', node, backup_type='ptrack')
#            pgdata = self.pgdata_content(node.data_dir)
#
#            node_restored.cleanup()
#
#            self.restore_node(backup_dir, 'node', node_restored)
#            pgdata_restored = self.pgdata_content(node_restored.data_dir)
#
#            self.compare_pgdata(pgdata, pgdata_restored)

        pgbench.terminate()
        pgbench.wait()

        self.switch_wal_segment(node)

        result = node.safe_psql("postgres", "SELECT * FROM pgbench_accounts")

        node_restored.cleanup()
        self.restore_node(backup_dir, 'node', node_restored)
        self.set_auto_conf(
            node_restored, {'port': node_restored.port})

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
            node_restored.safe_psql(
                'postgres',
                'SELECT * FROM pgbench_accounts'),
            'Data loss')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_simple(self):
        """make node, make full and ptrack stream backups,"
        " restore them and check data correctness"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
            node.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        node.safe_psql(
            "postgres",
            "create table t_heap as select i"
            " as id from generate_series(0,1) i")

        self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack',
            options=['--stream'])

        node.safe_psql(
            "postgres",
            "update t_heap set id = 100500")

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=['--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        result = node.safe_psql("postgres", "SELECT * FROM t_heap")

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node_restored.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        self.set_auto_conf(
            node_restored, {'port': node_restored.port})

        node_restored.slow_start()

        # Logical comparison
        self.assertEqual(
            result,
            node_restored.safe_psql("postgres", "SELECT * FROM t_heap"))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_unprivileged(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        # self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE DATABASE backupdb")

        # PG 9.5
        if self.get_version(node) < 90600:
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
                "GRANT EXECUTE ON FUNCTION pg_catalog.nameeq(name, name) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.textout(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.timestamptz(timestamp with time zone, integer) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_start_backup(text, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_stop_backup() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_current_snapshot() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_snapshot_xmax(txid_snapshot) TO backup;"
            )
        # PG 9.6
        elif self.get_version(node) > 90600 and self.get_version(node) < 100000:
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
                "GRANT EXECUTE ON FUNCTION pg_catalog.nameeq(name, name) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.textout(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.timestamptz(timestamp with time zone, integer) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_start_backup(text, boolean, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_stop_backup(boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_create_restore_point(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_xlog() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_last_xlog_replay_location() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_current_snapshot() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.txid_snapshot_xmax(txid_snapshot) TO backup;"
            )
        # >= 10
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
                "GRANT EXECUTE ON FUNCTION pg_catalog.nameeq(name, name) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; "
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

        if node.major_version < 11:
            fnames = [
                'pg_catalog.oideq(oid, oid)',
                'pg_catalog.ptrack_version()',
                'pg_catalog.pg_ptrack_clear()',
                'pg_catalog.pg_ptrack_control_lsn()',
                'pg_catalog.pg_ptrack_get_and_clear_db(oid, oid)',
                'pg_catalog.pg_ptrack_get_and_clear(oid, oid)',
                'pg_catalog.pg_ptrack_get_block_2(oid, oid, oid, bigint)'
                ]

            for fname in fnames:
                node.safe_psql(
                    "backupdb",
                    "GRANT EXECUTE ON FUNCTION {0} TO backup".format(fname))

        else:
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

        if ProbackupTest.enterprise:
            node.safe_psql(
                "backupdb",
                "GRANT EXECUTE ON FUNCTION pg_catalog.pgpro_edition() TO backup")

            node.safe_psql(
                "backupdb",
                "GRANT EXECUTE ON FUNCTION pg_catalog.pgpro_version() TO backup")

        self.backup_node(
            backup_dir, 'node', node,
            datname='backupdb', options=['--stream', "-U", "backup"])

        self.backup_node(
            backup_dir, 'node', node, datname='backupdb',
            backup_type='ptrack', options=['--stream', "-U", "backup"])


    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_enable(self):
        """make ptrack without full backup, should result in error"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True, initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s',
                'shared_preload_libraries': 'ptrack'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
            node.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        # PTRACK BACKUP
        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='ptrack', options=["--stream"]
            )
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because ptrack disabled.\n"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd
                )
            )
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Ptrack is disabled\n',
                e.message,
                '\n Unexpected Error Message: {0}\n'
                ' CMD: {1}'.format(repr(e.message), self.cmd)
            )

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_disable(self):
        """
        Take full backup, disable ptrack restart postgresql,
        enable ptrack, restart postgresql, take ptrack backup
        which should fail
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={'checkpoint_timeout': '30s'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
            node.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        # FULL BACKUP
        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        # DISABLE PTRACK
        if node.major_version >= 11:
            node.safe_psql('postgres', "alter system set ptrack.map_size to 0")
        else:
            node.safe_psql('postgres', "alter system set ptrack_enable to off")
        node.stop()
        node.slow_start()

        # ENABLE PTRACK
        if node.major_version >= 11:
            node.safe_psql('postgres', "alter system set ptrack.map_size to '128'")
            node.safe_psql('postgres', "alter system set shared_preload_libraries to 'ptrack'")
        else:
            node.safe_psql('postgres', "alter system set ptrack_enable to on")
        node.stop()
        node.slow_start()

        # PTRACK BACKUP
        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='ptrack', options=["--stream"]
            )
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because ptrack_enable was set to OFF at some"
                " point after previous backup.\n"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd
                )
            )
        except ProbackupException as e:
            self.assertIn(
                'ERROR: LSN from ptrack_control',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd
                )
            )

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_uncommitted_xact(self):
        """make ptrack backup while there is uncommitted open transaction"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
            node.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        con = node.connect("postgres")
        con.execute(
            "create table t_heap as select i"
            " as id from generate_series(0,1) i")

        self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack',
            options=['--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored,
            node_restored.data_dir, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                    node_restored.data_dir, ignore_ptrack=False)

        self.set_auto_conf(
            node_restored, {'port': node_restored.port})

        node_restored.slow_start()

        # Physical comparison
        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_vacuum_full(self):
        """make node, make full and ptrack stream backups,
          restore them and check data correctness"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'somedata')

        if node.major_version >= 11:
            node.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

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
            backup_dir, 'node', node, backup_type='ptrack', options=['--stream'])

        self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack', options=['--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        gdb.remove_all_breakpoints()
        gdb._execute('detach')
        process.join()

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        old_tablespace = self.get_tblspace_path(node, 'somedata')
        new_tablespace = self.get_tblspace_path(node_restored, 'somedata_new')

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=["-j", "4", "-T", "{0}={1}".format(
                old_tablespace, new_tablespace)]
        )

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node_restored.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        self.set_auto_conf(
            node_restored, {'port': node_restored.port})

        node_restored.slow_start()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_vacuum_truncate(self):
        """make node, create table, take full backup,
           delete last 3 pages, vacuum relation,
           take ptrack backup, take second ptrack backup,
           restore last ptrack backup and check data correctness"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'somedata')

        if node.major_version >= 11:
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

        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        node.safe_psql(
            "postgres",
            "delete from t_heap where ctid >= '(11,0)'")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack', options=['--stream'])

        self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack', options=['--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        old_tablespace = self.get_tblspace_path(node, 'somedata')
        new_tablespace = self.get_tblspace_path(node_restored, 'somedata_new')

        self.restore_node(
            backup_dir, 'node', node_restored,
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

        self.set_auto_conf(
            node_restored, {'port': node_restored.port})

        node_restored.slow_start()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_get_block(self):
        """make node, make full and ptrack stream backups,"
        " restore them and check data correctness"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
            self.skipTest("skip --- we do not need ptrack_get_block for ptrack 2.*")
            node.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        node.safe_psql(
            "postgres",
            "create table t_heap as select i"
            " as id from generate_series(0,1) i")

        self.backup_node(backup_dir, 'node', node, options=['--stream'])
        gdb = self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack',
            options=['--stream'],
            gdb=True)

        if node.major_version > 11:
            gdb.set_breakpoint('make_pagemap_from_ptrack_2')
        else:
            gdb.set_breakpoint('make_pagemap_from_ptrack_1')
        gdb.run_until_break()

        node.safe_psql(
            "postgres",
            "update t_heap set id = 100500")

        gdb.continue_execution_until_exit()

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=['--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        result = node.safe_psql("postgres", "SELECT * FROM t_heap")
        node.cleanup()
        self.restore_node(backup_dir, 'node', node, options=["-j", "4"])

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        node.slow_start()
        # Logical comparison
        self.assertEqual(
            result,
            node.safe_psql("postgres", "SELECT * FROM t_heap")
        )

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_stream(self):
        """make node, make full and ptrack stream backups,
         restore them and check data correctness"""
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s',
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
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

        full_result = node.safe_psql("postgres", "SELECT * FROM t_heap")
        full_backup_id = self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        # PTRACK BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, nextval('t_seq') as t_seq,"
            " md5(i::text) as text, md5(i::text)::tsvector as tsvector"
            " from generate_series(100,200) i")

        ptrack_result = node.safe_psql("postgres", "SELECT * FROM t_heap")
        ptrack_backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=['--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # Drop Node
        node.cleanup()

        # Restore and check full backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(
                backup_dir, 'node', node,
                backup_id=full_backup_id, options=["-j", "4"]
            ),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                repr(self.output), self.cmd)
            )
        node.slow_start()
        full_result_new = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Restore and check ptrack backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(ptrack_backup_id),
            self.restore_node(
                backup_dir, 'node', node,
                backup_id=ptrack_backup_id, options=["-j", "4"]
            ),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                repr(self.output), self.cmd))

        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        node.slow_start()
        ptrack_result_new = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(ptrack_result, ptrack_result_new)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_archive(self):
        """make archive node, make full and ptrack backups,
            check data correctness in restored instance"""
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s',
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
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

        full_result = node.safe_psql("postgres", "SELECT * FROM t_heap")
        full_backup_id = self.backup_node(backup_dir, 'node', node)
        full_target_time = self.show_pb(
            backup_dir, 'node', full_backup_id)['recovery-time']

        # PTRACK BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id,"
            " md5(i::text) as text,"
            " md5(i::text)::tsvector as tsvector"
            " from generate_series(100,200) i")

        ptrack_result = node.safe_psql("postgres", "SELECT * FROM t_heap")
        ptrack_backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack')
        ptrack_target_time = self.show_pb(
            backup_dir, 'node', ptrack_backup_id)['recovery-time']
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
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(
                backup_dir, 'node', node,
                backup_id=full_backup_id,
                options=[
                    "-j", "4", "--recovery-target-action=promote",
                    "--time={0}".format(full_target_time)]
            ),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                repr(self.output), self.cmd)
        )
        node.slow_start()

        full_result_new = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check ptrack backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(ptrack_backup_id),
            self.restore_node(
                backup_dir, 'node', node,
                backup_id=ptrack_backup_id,
                options=[
                    "-j", "4",
                    "--time={0}".format(ptrack_target_time),
                    "--recovery-target-action=promote"]
            ),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                repr(self.output), self.cmd)
        )

        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        node.slow_start()
        ptrack_result_new = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(ptrack_result, ptrack_result_new)

        node.cleanup()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_pgpro417(self):
        """Make  node, take full backup, take ptrack backup,
            delete ptrack backup. Try to take ptrack backup,
            which should fail. Actual only for PTRACK 1.x"""
        if self.pg_config_version > self.version_to_num('11.0'):
            return unittest.skip('You need PostgreSQL =< 11 for this test')

        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s',
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,100) i")
        node.safe_psql(
            "postgres",
            "SELECT * FROM t_heap")

        backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='full', options=["--stream"])

        start_lsn_full = self.show_pb(
            backup_dir, 'node', backup_id)['start-lsn']

        # PTRACK BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(100,200) i")
        node.safe_psql("postgres", "SELECT * FROM t_heap")
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=["--stream"])

        start_lsn_ptrack = self.show_pb(
            backup_dir, 'node', backup_id)['start-lsn']

        self.delete_pb(backup_dir, 'node', backup_id)

        # SECOND PTRACK BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(200,300) i")

        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='ptrack', options=["--stream"])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because of LSN mismatch from ptrack_control "
                "and previous backup start_lsn.\n"
                " Output: {0} \n CMD: {1}".format(repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'ERROR: LSN from ptrack_control' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_page_pgpro417(self):
        """
        Make archive node, take full backup, take page backup,
        delete page backup. Try to take ptrack backup, which should fail.
        Actual only for PTRACK 1.x
        """
        if self.pg_config_version > self.version_to_num('11.0'):
            return unittest.skip('You need PostgreSQL =< 11 for this test')

        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s',
                'autovacuum': 'off'})

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

        # PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(100,200) i")
        node.safe_psql("postgres", "SELECT * FROM t_heap")
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        self.delete_pb(backup_dir, 'node', backup_id)
#        sys.exit(1)

        # PTRACK BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(200,300) i")

        try:
            self.backup_node(backup_dir, 'node', node, backup_type='ptrack')
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because of LSN mismatch from ptrack_control "
                "and previous backup start_lsn.\n "
                "Output: {0}\n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'ERROR: LSN from ptrack_control' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_full_pgpro417(self):
        """
        Make node, take two full backups, delete full second backup.
        Try to take ptrack backup, which should fail.
        Relevant only for PTRACK 1.x
        """
        if self.pg_config_version > self.version_to_num('11.0'):
            return unittest.skip('You need PostgreSQL =< 11 for this test')

        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s',
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text,"
            " md5(i::text)::tsvector as tsvector "
            " from generate_series(0,100) i"
        )
        node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.backup_node(backup_dir, 'node', node, options=["--stream"])

        # SECOND FULL BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text,"
            " md5(i::text)::tsvector as tsvector"
            " from generate_series(100,200) i"
        )
        node.safe_psql("postgres", "SELECT * FROM t_heap")
        backup_id = self.backup_node(
            backup_dir, 'node', node, options=["--stream"])

        self.delete_pb(backup_dir, 'node', backup_id)

        # PTRACK BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(200,300) i")
        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='ptrack', options=["--stream"])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because of LSN mismatch from ptrack_control "
                "and previous backup start_lsn.\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd)
            )
        except ProbackupException as e:
            self.assertTrue(
                "ERROR: LSN from ptrack_control" in e.message and
                "Create new full backup before "
                "an incremental one" in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_create_db(self):
        """
        Make node, take full backup, create database db1, take ptrack backup,
        restore database and check it presense
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'max_wal_size': '10GB',
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
            node.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

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

        # PTRACK BACKUP
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=["--stream"])

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
            pgdata_restored = self.pgdata_content(
                node_restored.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        self.set_auto_conf(
            node_restored, {'port': node_restored.port})
        node_restored.slow_start()

        # DROP DATABASE DB1
        node.safe_psql(
            "postgres", "drop database db1")
        # SECOND PTRACK BACKUP
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=["--stream"]
        )

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # RESTORE SECOND PTRACK BACKUP
        node_restored.cleanup()
        self.restore_node(
            backup_dir, 'node', node_restored,
            backup_id=backup_id, options=["-j", "4"])

        # COMPARE PHYSICAL CONTENT
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node_restored.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        self.set_auto_conf(
            node_restored, {'port': node_restored.port})
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
    def test_create_db_on_replica(self):
        """
        Make node, take full backup, create replica from it,
        take full backup from replica,
        create database db1, take ptrack backup from replica,
        restore database and check it presense
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s',
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
            node.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,100) i")

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.backup_node(
            backup_dir, 'node', node, options=['-j10', '--stream'])

        self.restore_node(backup_dir, 'node', replica)

        # Add replica
        self.add_instance(backup_dir, 'replica', replica)
        self.set_replica(node, replica, 'replica', synchronous=True)
        replica.slow_start(replica=True)

        self.backup_node(
            backup_dir, 'replica', replica,
            options=[
                '-j10',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(node.port),
                '--stream'
                ]
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
        backup_id = self.backup_node(
            backup_dir, 'replica',
            replica, backup_type='ptrack',
            options=[
                '-j10',
                '--stream',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(node.port)
                ]
            )

        if self.paranoia:
            pgdata = self.pgdata_content(replica.data_dir)

        # RESTORE
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'replica', node_restored,
            backup_id=backup_id, options=["-j", "4"])

        # COMPARE PHYSICAL CONTENT
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_alter_table_set_tablespace_ptrack(self):
        """Make node, create tablespace with table, take full backup,
         alter tablespace location, take ptrack backup, restore database."""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s',
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
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
        self.backup_node(backup_dir, 'node', node, options=["--stream"])

        # ALTER TABLESPACE
        self.create_tblspace_in_node(node, 'somedata_new')
        node.safe_psql(
            "postgres",
            "alter table t_heap set tablespace somedata_new")

        # sys.exit(1)
        # PTRACK BACKUP
        #result = node.safe_psql(
        #    "postgres", "select * from t_heap")
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack',
            options=["--stream"]
        )
        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)
        # node.stop()
        # node.cleanup()

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
            pgdata_restored = self.pgdata_content(
                node_restored.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        self.set_auto_conf(
            node_restored, {'port': node_restored.port})
        node_restored.slow_start()

#        result_new = node_restored.safe_psql(
#            "postgres", "select * from t_heap")
#
#        self.assertEqual(result, result_new, 'lost some data after restore')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_alter_database_set_tablespace_ptrack(self):
        """Make node, create tablespace with database,"
        " take full backup, alter tablespace location,"
        " take ptrack backup, restore database."""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s',
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
            node.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        # FULL BACKUP
        self.backup_node(backup_dir, 'node', node, options=["--stream"])

        # CREATE TABLESPACE
        self.create_tblspace_in_node(node, 'somedata')

        # ALTER DATABASE
        node.safe_psql(
            "template1",
            "alter database postgres set tablespace somedata")

        # PTRACK BACKUP
        self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack',
            options=["--stream"])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)
        node.stop()

        # RESTORE
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()
        self.restore_node(
            backup_dir, 'node',
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

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_drop_tablespace(self):
        """
        Make node, create table, alter table tablespace, take ptrack backup,
        move table from tablespace, take ptrack backup
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s',
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
            node.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.create_tblspace_in_node(node, 'somedata')

        # CREATE TABLE
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,100) i")

        result = node.safe_psql("postgres", "select * from t_heap")
        # FULL BACKUP
        self.backup_node(backup_dir, 'node', node, options=["--stream"])

        # Move table to tablespace 'somedata'
        node.safe_psql(
            "postgres", "alter table t_heap set tablespace somedata")
        # PTRACK BACKUP
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=["--stream"])

        # Move table back to default tablespace
        node.safe_psql(
            "postgres", "alter table t_heap set tablespace pg_default")
        # SECOND PTRACK BACKUP
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=["--stream"])

        # DROP TABLESPACE 'somedata'
        node.safe_psql(
            "postgres", "drop tablespace somedata")
        # THIRD PTRACK BACKUP
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=["--stream"])

        if self.paranoia:
            pgdata = self.pgdata_content(
                node.data_dir, ignore_ptrack=True)

        tblspace = self.get_tblspace_path(node, 'somedata')
        node.cleanup()
        shutil.rmtree(tblspace, ignore_errors=True)
        self.restore_node(backup_dir, 'node', node, options=["-j", "4"])

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

        result_new = node.safe_psql("postgres", "select * from t_heap")
        self.assertEqual(result, result_new)

        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_alter_tablespace(self):
        """
        Make node, create table, alter table tablespace, take ptrack backup,
        move table from tablespace, take ptrack backup
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30s',
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
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

        result = node.safe_psql("postgres", "select * from t_heap")
        # FULL BACKUP
        self.backup_node(backup_dir, 'node', node, options=["--stream"])

        # Move table to separate tablespace
        node.safe_psql(
            "postgres",
            "alter table t_heap set tablespace somedata")
        # GET LOGICAL CONTENT FROM NODE
        result = node.safe_psql("postgres", "select * from t_heap")

        # FIRTS PTRACK BACKUP
        self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack',
            options=["--stream"])

        # GET PHYSICAL CONTENT FROM NODE
        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # Restore ptrack backup
        restored_node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'restored_node'))
        restored_node.cleanup()
        tblspc_path_new = self.get_tblspace_path(
            restored_node, 'somedata_restored')
        self.restore_node(backup_dir, 'node', restored_node, options=[
            "-j", "4", "-T", "{0}={1}".format(tblspc_path, tblspc_path_new)])

        # GET PHYSICAL CONTENT FROM RESTORED NODE and COMPARE PHYSICAL CONTENT
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                restored_node.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        self.set_auto_conf(
            restored_node, {'port': restored_node.port})
        restored_node.slow_start()

        # COMPARE LOGICAL CONTENT
        result_new = restored_node.safe_psql(
            "postgres", "select * from t_heap")
        self.assertEqual(result, result_new)

        restored_node.cleanup()
        shutil.rmtree(tblspc_path_new, ignore_errors=True)

        # Move table to default tablespace
        node.safe_psql(
            "postgres", "alter table t_heap set tablespace pg_default")
        # SECOND PTRACK BACKUP
        self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack',
            options=["--stream"])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # Restore second ptrack backup and check table consistency
        self.restore_node(
            backup_dir, 'node', restored_node,
            options=[
                "-j", "4", "-T", "{0}={1}".format(tblspc_path, tblspc_path_new)])

        # GET PHYSICAL CONTENT FROM RESTORED NODE and COMPARE PHYSICAL CONTENT
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                restored_node.data_dir, ignore_ptrack=False)
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        self.set_auto_conf(
            restored_node, {'port': restored_node.port})
        restored_node.slow_start()

        result_new = restored_node.safe_psql(
            "postgres", "select * from t_heap")
        self.assertEqual(result, result_new)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_multiple_segments(self):
        """
        Make node, create table, alter table tablespace,
        take ptrack backup, move table from tablespace, take ptrack backup
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off',
                'full_page_writes': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
            node.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.create_tblspace_in_node(node, 'somedata')

        # CREATE TABLE
        node.pgbench_init(scale=100, options=['--tablespace=somedata'])
        # FULL BACKUP
        self.backup_node(backup_dir, 'node', node, options=['--stream'])

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
        result = node.safe_psql("postgres", "select * from pgbench_accounts")
        # FIRTS PTRACK BACKUP
        self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack', options=['--stream'])

        # GET PHYSICAL CONTENT FROM NODE
        pgdata = self.pgdata_content(node.data_dir)

        # RESTORE NODE
        restored_node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'restored_node'))
        restored_node.cleanup()
        tblspc_path = self.get_tblspace_path(node, 'somedata')
        tblspc_path_new = self.get_tblspace_path(
            restored_node,
            'somedata_restored')

        self.restore_node(
            backup_dir, 'node', restored_node,
            options=[
                "-j", "4", "-T", "{0}={1}".format(
                    tblspc_path, tblspc_path_new)])

        # GET PHYSICAL CONTENT FROM NODE_RESTORED
        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                restored_node.data_dir, ignore_ptrack=False)

        # START RESTORED NODE
        self.set_auto_conf(
            restored_node, {'port': restored_node.port})
        restored_node.slow_start()

        result_new = restored_node.safe_psql(
            "postgres",
            "select * from pgbench_accounts")

        # COMPARE RESTORED FILES
        self.assertEqual(result, result_new, 'data is lost')

        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_atexit_fail(self):
        """
        Take backups of every available types and check that PTRACK is clean.
        Relevant only for PTRACK 1.x
        """
        if self.pg_config_version > self.version_to_num('11.0'):
            return unittest.skip('You need PostgreSQL =< 11 for this test')

        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'max_connections': '15'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        # Take FULL backup to clean every ptrack
        self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        try:
            self.backup_node(
                backup_dir, 'node', node, backup_type='ptrack',
                options=["--stream", "-j 30"])

            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because we are opening too many connections"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd)
            )
        except ProbackupException as e:
            self.assertIn(
                'setting its status to ERROR',
                e.message,
                '\n Unexpected Error Message: {0}\n'
                ' CMD: {1}'.format(repr(e.message), self.cmd)
            )

        self.assertEqual(
            node.safe_psql(
                "postgres",
                "select * from pg_is_in_backup()").rstrip(),
            "f")

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_clean(self):
        """
        Take backups of every available types and check that PTRACK is clean
        Relevant only for PTRACK 1.x
        """
        if self.pg_config_version > self.version_to_num('11.0'):
            return unittest.skip('You need PostgreSQL =< 11 for this test')

        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'somedata')

        # Create table and indexes
        node.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap tablespace somedata "
            "as select i as id, nextval('t_seq') as t_seq, "
            "md5(i::text) as text, "
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

        # Take FULL backup to clean every ptrack
        self.backup_node(
            backup_dir, 'node', node,
            options=['-j10', '--stream'])
        node.safe_psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get fork size and calculate it in pages
            idx_ptrack[i]['size'] = self.get_fork_size(node, i)
            # get path to heap and index files
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                node, idx_ptrack[i]['path'], [idx_ptrack[i]['size']])
            self.check_ptrack_clean(idx_ptrack[i], idx_ptrack[i]['size'])

        # Update everything and vacuum it
        node.safe_psql(
            'postgres',
            "update t_heap set t_seq = nextval('t_seq'), "
            "text = md5(text), "
            "tsvector = md5(repeat(tsvector::text, 10))::tsvector;")
        node.safe_psql('postgres', 'vacuum t_heap')

        # Take PTRACK backup to clean every ptrack
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack', options=['-j10', '--stream'])

        node.safe_psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get new size of heap and indexes and calculate it in pages
            idx_ptrack[i]['size'] = self.get_fork_size(node, i)
            # update path to heap and index files in case they`ve changed
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                node, idx_ptrack[i]['path'], [idx_ptrack[i]['size']])
            # check that ptrack bits are cleaned
            self.check_ptrack_clean(idx_ptrack[i], idx_ptrack[i]['size'])

        # Update everything and vacuum it
        node.safe_psql(
            'postgres',
            "update t_heap set t_seq = nextval('t_seq'), "
            "text = md5(text), "
            "tsvector = md5(repeat(tsvector::text, 10))::tsvector;")
        node.safe_psql('postgres', 'vacuum t_heap')

        # Take PAGE backup to clean every ptrack
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', options=['-j10', '--stream'])
        node.safe_psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get new size of heap and indexes and calculate it in pages
            idx_ptrack[i]['size'] = self.get_fork_size(node, i)
            # update path to heap and index files in case they`ve changed
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                node, idx_ptrack[i]['path'], [idx_ptrack[i]['size']])
            # check that ptrack bits are cleaned
            self.check_ptrack_clean(idx_ptrack[i], idx_ptrack[i]['size'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_clean_replica(self):
        """
        Take backups of every available types from
        master and check that PTRACK on replica is clean.
        Relevant only for PTRACK 1.x
        """
        if self.pg_config_version > self.version_to_num('11.0'):
            return unittest.skip('You need PostgreSQL =< 11 for this test')

        fname = self.id().split('.')[3]
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'archive_timeout': '30s'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        master.slow_start()

        self.backup_node(backup_dir, 'master', master, options=['--stream'])

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.restore_node(backup_dir, 'master', replica)

        self.add_instance(backup_dir, 'replica', replica)
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

        # Take FULL backup to clean every ptrack
        self.backup_node(
            backup_dir,
            'replica',
            replica,
            options=[
                '-j10', '--stream',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port)])
        master.safe_psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get fork size and calculate it in pages
            idx_ptrack[i]['size'] = self.get_fork_size(replica, i)
            # get path to heap and index files
            idx_ptrack[i]['path'] = self.get_fork_path(replica, i)
            # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                replica, idx_ptrack[i]['path'], [idx_ptrack[i]['size']])
            self.check_ptrack_clean(idx_ptrack[i], idx_ptrack[i]['size'])

        # Update everything and vacuum it
        master.safe_psql(
            'postgres',
            "update t_heap set t_seq = nextval('t_seq'), "
            "text = md5(text), "
            "tsvector = md5(repeat(tsvector::text, 10))::tsvector;")
        master.safe_psql('postgres', 'vacuum t_heap')

        # Take PTRACK backup to clean every ptrack
        backup_id = self.backup_node(
            backup_dir,
            'replica',
            replica,
            backup_type='ptrack',
            options=[
                '-j10', '--stream',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port)])
        master.safe_psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get new size of heap and indexes and calculate it in pages
            idx_ptrack[i]['size'] = self.get_fork_size(replica, i)
            # update path to heap and index files in case they`ve changed
            idx_ptrack[i]['path'] = self.get_fork_path(replica, i)
            # # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                replica, idx_ptrack[i]['path'], [idx_ptrack[i]['size']])
            # check that ptrack bits are cleaned
            self.check_ptrack_clean(idx_ptrack[i], idx_ptrack[i]['size'])

        # Update everything and vacuum it
        master.safe_psql(
            'postgres',
            "update t_heap set t_seq = nextval('t_seq'), text = md5(text), "
            "tsvector = md5(repeat(tsvector::text, 10))::tsvector;")
        master.safe_psql('postgres', 'vacuum t_heap')
        master.safe_psql('postgres', 'checkpoint')

        # Take PAGE backup to clean every ptrack
        self.backup_node(
            backup_dir,
            'replica',
            replica,
            backup_type='page',
            options=[
                '-j10', '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port),
                '--stream'])
        master.safe_psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get new size of heap and indexes and calculate it in pages
            idx_ptrack[i]['size'] = self.get_fork_size(replica, i)
            # update path to heap and index files in case they`ve changed
            idx_ptrack[i]['path'] = self.get_fork_path(replica, i)
            # # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                replica, idx_ptrack[i]['path'], [idx_ptrack[i]['size']])
            # check that ptrack bits are cleaned
            self.check_ptrack_clean(idx_ptrack[i], idx_ptrack[i]['size'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)
    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_cluster_on_btree(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
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

        self.backup_node(
            backup_dir, 'node', node, options=['-j10', '--stream'])

        node.safe_psql('postgres', 'delete from t_heap where id%2 = 1')
        node.safe_psql('postgres', 'cluster t_heap using t_btree')
        node.safe_psql('postgres', 'checkpoint')

        # CHECK PTRACK SANITY
        if node.major_version < 11:
            self.check_ptrack_map_sanity(node, idx_ptrack)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_cluster_on_gist(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
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

        self.backup_node(
            backup_dir, 'node', node, options=['-j10', '--stream'])

        node.safe_psql('postgres', 'delete from t_heap where id%2 = 1')
        node.safe_psql('postgres', 'cluster t_heap using t_gist')
        node.safe_psql('postgres', 'checkpoint')

        # CHECK PTRACK SANITY
        if node.major_version < 11:
            self.check_ptrack_map_sanity(node, idx_ptrack)

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=['-j10', '--stream'])

        pgdata = self.pgdata_content(node.data_dir)
        node.cleanup()

        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_cluster_on_btree_replica(self):
        fname = self.id().split('.')[3]
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        master.slow_start()

        if master.major_version >= 11:
            master.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.backup_node(backup_dir, 'master', master, options=['--stream'])

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.restore_node(backup_dir, 'master', replica)

        self.add_instance(backup_dir, 'replica', replica)
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

        self.backup_node(
            backup_dir, 'replica', replica, options=[
                '-j10', '--stream', '--master-host=localhost',
                '--master-db=postgres', '--master-port={0}'.format(
                    master.port)])

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

        self.backup_node(
            backup_dir, 'replica', replica,
            backup_type='ptrack', options=['-j10', '--stream'])

        pgdata = self.pgdata_content(replica.data_dir)

        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'))
        node.cleanup()

        self.restore_node(backup_dir, 'replica', node)

        pgdata_restored = self.pgdata_content(replica.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_cluster_on_gist_replica(self):
        fname = self.id().split('.')[3]
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            ptrack_enable=True)

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        master.slow_start()

        if master.major_version >= 11:
            master.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.backup_node(backup_dir, 'master', master, options=['--stream'])

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.restore_node(backup_dir, 'master', replica)

        self.add_instance(backup_dir, 'replica', replica)
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

        self.backup_node(
            backup_dir, 'replica', replica, options=[
                '-j10', '--stream', '--master-host=localhost',
                '--master-db=postgres', '--master-port={0}'.format(
                    master.port)])

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

        self.backup_node(
            backup_dir, 'replica', replica,
            backup_type='ptrack', options=['-j10', '--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(replica.data_dir)

        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'))
        node.cleanup()

        self.restore_node(backup_dir, 'replica', node)

        if self.paranoia:
            pgdata_restored = self.pgdata_content(replica.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_empty(self):
        """Take backups of every available types and check that PTRACK is clean"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
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
        self.backup_node(
            backup_dir, 'node', node,
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

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        tblspace1 = self.get_tblspace_path(node, 'somedata')
        tblspace2 = self.get_tblspace_path(node_restored, 'somedata')

        # Take PTRACK backup
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack',
            options=['-j10', '--stream'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.restore_node(
            backup_dir, 'node', node_restored,
            backup_id=backup_id,
            options=[
                "-j", "4",
                "-T{0}={1}".format(tblspace1, tblspace2)])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_empty_replica(self):
        """
        Take backups of every available types from master
        and check that PTRACK on replica is clean
        """
        fname = self.id().split('.')[3]
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            ptrack_enable=True,
            pg_options={'autovacuum': 'off'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        master.slow_start()

        if master.major_version >= 11:
            master.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.backup_node(backup_dir, 'master', master, options=['--stream'])

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.restore_node(backup_dir, 'master', replica)

        self.add_instance(backup_dir, 'replica', replica)
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
        self.backup_node(
            backup_dir,
            'replica',
            replica,
            options=[
                '-j10', '--stream',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port)])

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
        backup_id = self.backup_node(
            backup_dir,
            'replica',
            replica,
            backup_type='ptrack',
            options=[
                '-j1', '--stream',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port)])

        if self.paranoia:
            pgdata = self.pgdata_content(replica.data_dir)

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'replica', node_restored,
            backup_id=backup_id, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_truncate(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
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

        self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

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
        self.backup_node(
            backup_dir, 'node', node,
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
    
        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_basic_ptrack_truncate_replica(self):
        fname = self.id().split('.')[3]
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'max_wal_size': '32MB',
                'archive_timeout': '10s',
                'checkpoint_timeout': '30s',
                'autovacuum': 'off'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        master.slow_start()

        if master.major_version >= 11:
            master.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.backup_node(backup_dir, 'master', master, options=['--stream'])

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.restore_node(backup_dir, 'master', replica)

        self.add_instance(backup_dir, 'replica', replica)
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
        self.backup_node(
            backup_dir, 'replica', replica,
            options=[
                '-j10',
                '--stream',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port)])

        if replica.major_version < 11:
            for i in idx_ptrack:
                idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                    replica, idx_ptrack[i]['path'], [idx_ptrack[i]['old_size']])
                self.check_ptrack_clean(idx_ptrack[i], idx_ptrack[i]['old_size'])

        master.safe_psql('postgres', 'truncate t_heap')

        # Sync master and replica
        self.wait_until_replica_catch_with_master(master, replica)

        self.backup_node(
            backup_dir, 'replica', replica, backup_type='ptrack',
            options=[
                '-j10',
                '--stream',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port)])

        pgdata = self.pgdata_content(replica.data_dir)

        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'))
        node.cleanup()

        self.restore_node(backup_dir, 'replica', node, data_dir=node.data_dir)

        pgdata_restored = self.pgdata_content(node.data_dir)

        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname, [master, replica])

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_vacuum(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
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

        node.safe_psql('postgres', 'vacuum t_heap')
        node.safe_psql('postgres', 'checkpoint')

        # Make full backup to clean every ptrack
        self.backup_node(
            backup_dir, 'node', node, options=['-j10', '--stream'])

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

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=['-j10', '--stream'])

        pgdata = self.pgdata_content(node.data_dir)
        node.cleanup()

        shutil.rmtree(
            self.get_tblspace_path(node, 'somedata'),
            ignore_errors=True)
    
        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_vacuum_replica(self):
        fname = self.id().split('.')[3]
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'checkpoint_timeout': '30'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        master.slow_start()

        if master.major_version >= 11:
            master.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.backup_node(backup_dir, 'master', master, options=['--stream'])

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.restore_node(backup_dir, 'master', replica)

        self.add_instance(backup_dir, 'replica', replica)
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
        self.backup_node(
            backup_dir, 'replica', replica, options=[
                '-j10', '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port),
                '--stream'])

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

        self.backup_node(
            backup_dir, 'replica', replica,
            backup_type='ptrack', options=['-j10', '--stream'])

        pgdata = self.pgdata_content(replica.data_dir)

        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'))
        node.cleanup()
    
        self.restore_node(backup_dir, 'replica', node, data_dir=node.data_dir)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_vacuum_bits_frozen(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
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

        node.safe_psql('postgres', 'checkpoint')

        self.backup_node(
            backup_dir, 'node', node, options=['-j10', '--stream'])

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

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=['-j10', '--stream'])

        pgdata = self.pgdata_content(node.data_dir)
        node.cleanup()
        shutil.rmtree(
            self.get_tblspace_path(node, 'somedata'),
            ignore_errors=True)
    
        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_vacuum_bits_frozen_replica(self):
        fname = self.id().split('.')[3]
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        master.slow_start()

        if master.major_version >= 11:
            master.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.backup_node(backup_dir, 'master', master, options=['--stream'])

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.restore_node(backup_dir, 'master', replica)

        self.add_instance(backup_dir, 'replica', replica)
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
        self.backup_node(
            backup_dir, 'replica', replica,
            options=[
                '-j10',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port),
                '--stream'])

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

        self.backup_node(
            backup_dir, 'replica', replica, backup_type='ptrack',
            options=['-j10', '--stream'])

        pgdata = self.pgdata_content(replica.data_dir)
        replica.cleanup()
    
        self.restore_node(backup_dir, 'replica', replica)

        pgdata_restored = self.pgdata_content(replica.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_vacuum_bits_visibility(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
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

        node.safe_psql('postgres', 'checkpoint')

        self.backup_node(
            backup_dir, 'node', node, options=['-j10', '--stream'])

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

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=['-j10', '--stream'])

        pgdata = self.pgdata_content(node.data_dir)
        node.cleanup()
        shutil.rmtree(
            self.get_tblspace_path(node, 'somedata'),
            ignore_errors=True)
    
        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_vacuum_full(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True)

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
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

        self.backup_node(
            backup_dir, 'node', node, options=['-j10', '--stream'])

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

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=['-j10', '--stream'])

        pgdata = self.pgdata_content(node.data_dir)
        node.cleanup()

        shutil.rmtree(
            self.get_tblspace_path(node, 'somedata'),
            ignore_errors=True)
    
        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_vacuum_full_replica(self):
        fname = self.id().split('.')[3]
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        master.slow_start()

        if master.major_version >= 11:
            master.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.backup_node(backup_dir, 'master', master, options=['--stream'])
        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.restore_node(backup_dir, 'master', replica)

        self.add_instance(backup_dir, 'replica', replica)
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
        self.backup_node(
            backup_dir, 'replica', replica,
            options=[
                '-j10',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port),
                '--stream'])

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

        self.backup_node(
            backup_dir, 'replica', replica,
            backup_type='ptrack', options=['-j10', '--stream'])

        pgdata = self.pgdata_content(replica.data_dir)
        replica.cleanup()
    
        self.restore_node(backup_dir, 'replica', replica)

        pgdata_restored = self.pgdata_content(replica.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_vacuum_truncate(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
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

        self.backup_node(
            backup_dir, 'node', node, options=['-j10', '--stream'])

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

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(backup_dir, 'node', node_restored)

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_vacuum_truncate_replica(self):
        fname = self.id().split('.')[3]
        master = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'master'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        master.slow_start()

        if master.major_version >= 11:
            master.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        self.backup_node(backup_dir, 'master', master, options=['--stream'])

        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.restore_node(backup_dir, 'master', replica)

        self.add_instance(backup_dir, 'replica', replica)
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
        self.backup_node(
            backup_dir, 'replica', replica,
            options=[
                '-j10',
                '--stream',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port)
                ]
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

        self.backup_node(
            backup_dir, 'replica', replica, backup_type='ptrack',
            options=[
                '--stream',
                '--log-level-file=INFO',
                '--archive-timeout=30'])

        pgdata = self.pgdata_content(replica.data_dir)

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(backup_dir, 'replica', node_restored)

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_recovery(self):
        if self.pg_config_version > self.version_to_num('11.0'):
            return unittest.skip('You need PostgreSQL =< 11 for this test')

        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
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

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_recovery_1(self):
        if self.pg_config_version < self.version_to_num('12.0'):
            return unittest.skip('You need PostgreSQL >= 12 for this test')

        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off',
                'shared_buffers': '512MB',
                'max_wal_size': '3GB'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
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

        self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

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

        print(node.safe_psql(
            'postgres',
            "SELECT count(*) FROM pg_buffercache WHERE isdirty"))

        if self.verbose:
            print('Killing postmaster. Losing Ptrack changes')
        node.stop(['-m', 'immediate', '-D', node.data_dir])

        if not node.status():
            node.slow_start()
        else:
            print("Die! Die! Why won't you die?... Why won't you die?")
            exit(1)

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored)

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_zero_changes(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
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

        self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)
        node.cleanup()

        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_pg_resetxlog(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off',
                'shared_buffers': '512MB',
                'max_wal_size': '3GB'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
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

        self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

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
#                backup_type='ptrack', options=['--stream'])

        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='ptrack', options=['--stream'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because instance was brutalized by pg_resetxlog"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd)
            )
        except ProbackupException as e:
            self.assertTrue(
                'ERROR: LSN from ptrack_control ' in e.message and
                'is greater than Start LSN of previous backup' in e.message,
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

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_corrupt_ptrack_map(self):

        if self.pg_config_version < self.version_to_num('12.0'):
            return unittest.skip('You need PostgreSQL >= 12 for this test')

        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        if node.major_version >= 11:
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

        ptrack_map = os.path.join(node.data_dir, 'global', 'ptrack.map')
        ptrack_map_mmap = os.path.join(node.data_dir, 'global', 'ptrack.map.mmap')

        # Let`s do index corruption. ptrack.map, ptrack.map.mmap
        with open(ptrack_map, "rb+", 0) as f:
            f.seek(42)
            f.write(b"blablahblahs")
            f.flush()
            f.close

        with open(ptrack_map_mmap, "rb+", 0) as f:
            f.seek(42)
            f.write(b"blablahblahs")
            f.flush()
            f.close

#        os.remove(os.path.join(node.logs_dir, node.pg_log_name))

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

        self.set_auto_conf(node, {'ptrack.map_size': '0'})

        node.slow_start()

        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='ptrack', options=['--stream'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because instance ptrack is disabled"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Ptrack is disabled',
                e.message,
                '\n Unexpected Error Message: {0}\n'
                ' CMD: {1}'.format(repr(e.message), self.cmd))

        node.safe_psql(
            'postgres',
            "update t_heap set id = nextval('t_seq'), text = md5(text), "
            "tsvector = md5(repeat(tsvector::text, 10))::tsvector")

        node.stop(['-m', 'immediate', '-D', node.data_dir])

        self.set_auto_conf(node, {'ptrack.map_size': '32', 'shared_preload_libraries': 'ptrack'})

        node.slow_start()

        sleep(1)

        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='ptrack', options=['--stream'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because ptrack map is from future"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: LSN from ptrack_control',
                e.message,
                '\n Unexpected Error Message: {0}\n'
                ' CMD: {1}'.format(repr(e.message), self.cmd))

        sleep(1)

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta', options=['--stream'])

        node.safe_psql(
            'postgres',
            "update t_heap set id = nextval('t_seq'), text = md5(text), "
            "tsvector = md5(repeat(tsvector::text, 10))::tsvector")

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        node.cleanup()

        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)
