import unittest
import os
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from datetime import datetime, timedelta
import subprocess


module_name = 'false_positive'


class FalsePositive(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    @unittest.expectedFailure
    def test_validate_wal_lost_segment(self):
        """Loose segment located between backups. ExpectedFailure. This is BUG """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        self.backup_node(backup_dir, 'node', node)

        # make some wals
        node.pgbench_init(scale=2)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        # delete last wal segment
        wals_dir = os.path.join(backup_dir, "wal", 'node')
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(
            os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        wals = map(int, wals)
        os.remove(os.path.join(wals_dir, '0000000' + str(max(wals))))

        # We just lost a wal segment and know nothing about it
        self.backup_node(backup_dir, 'node', node)
        self.assertTrue(
            'validation completed successfully' in self.validate_pb(
                backup_dir, 'node'))
        ########

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    @unittest.expectedFailure
    # Need to force validation of ancestor-chain
    def test_incremental_backup_corrupt_full_1(self):
        """page-level backup with corrupted full backup"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'ptrack_enable': 'on'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        backup_id = self.backup_node(backup_dir, 'node', node)
        file = os.path.join(
            backup_dir, "backups", "node",
            backup_id.decode("utf-8"), "database", "postgresql.conf")
        os.remove(file)

        try:
            self.backup_node(backup_dir, 'node', node, backup_type="page")
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because page backup should not be "
                "possible without valid full backup.\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(
                e.message,
                'ERROR: Valid backup on current timeline is not found. '
                'Create new FULL backup before an incremental one.\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

            sleep(1)
            self.assertFalse(
                True,
                "Expecting Error because page backup should not be "
                "possible without valid full backup.\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(
                e.message,
                'ERROR: Valid backup on current timeline is not found. '
                'Create new FULL backup before an incremental one.\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertEqual(
            self.show_pb(backup_dir, 'node')[0]['Status'], "ERROR")

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    @unittest.expectedFailure
    def test_ptrack_concurrent_get_and_clear_1(self):
        """make node, make full and ptrack stream backups,"
        " restore them and check data correctness"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'max_wal_senders': '2',
                'checkpoint_timeout': '300s',
                'ptrack_enable': 'on'
            }
        )
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i"
            " as id from generate_series(0,1) i"
        )

        self.backup_node(backup_dir, 'node', node, options=['--stream'])
        gdb = self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack',
            options=['--stream', '--log-level-file=verbose'],
            gdb=True
        )

        gdb.set_breakpoint('make_pagemap_from_ptrack')
        gdb.run_until_break()

        node.safe_psql(
            "postgres",
            "update t_heap set id = 100500")

        tablespace_oid = node.safe_psql(
            "postgres",
            "select oid from pg_tablespace where spcname = 'pg_default'").rstrip()

        relfilenode = node.safe_psql(
            "postgres",
            "select 't_heap'::regclass::oid").rstrip()

        node.safe_psql(
            "postgres",
            "SELECT pg_ptrack_get_and_clear({0}, {1})".format(
                tablespace_oid, relfilenode))

        gdb.continue_execution_until_exit()

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='ptrack', options=['--stream']
        )
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
    @unittest.expectedFailure
    def test_ptrack_concurrent_get_and_clear_2(self):
        """make node, make full and ptrack stream backups,"
        " restore them and check data correctness"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'max_wal_senders': '2',
                'checkpoint_timeout': '300s',
                'ptrack_enable': 'on'
            }
        )
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i"
            " as id from generate_series(0,1) i"
        )

        self.backup_node(backup_dir, 'node', node, options=['--stream'])
        gdb = self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack',
            options=['--stream', '--log-level-file=verbose'],
            gdb=True
        )

        gdb.set_breakpoint('pthread_create')
        gdb.run_until_break()

        node.safe_psql(
            "postgres",
            "update t_heap set id = 100500")

        tablespace_oid = node.safe_psql(
            "postgres",
            "select oid from pg_tablespace "
            "where spcname = 'pg_default'").rstrip()

        relfilenode = node.safe_psql(
            "postgres",
            "select 't_heap'::regclass::oid").rstrip()

        node.safe_psql(
            "postgres",
            "SELECT pg_ptrack_get_and_clear({0}, {1})".format(
                tablespace_oid, relfilenode))

        gdb._execute("delete breakpoints")
        gdb.continue_execution_until_exit()

        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='ptrack', options=['--stream']
            )
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because of LSN mismatch from ptrack_control "
                "and previous backup ptrack_lsn.\n"
                " Output: {0} \n CMD: {1}".format(repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'ERROR: LSN from ptrack_control' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

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
    @unittest.expectedFailure
    def test_multiple_delete(self):
        """delete multiple backups"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,10000) i")
        # first full backup
        backup_1_id = self.backup_node(backup_dir, 'node', node)
        # second full backup
        backup_2_id = self.backup_node(backup_dir, 'node', node)
        # third full backup
        backup_3_id = self.backup_node(backup_dir, 'node', node)
        node.stop()

        self.delete_pb(backup_dir, 'node', options=
            ["-i {0}".format(backup_1_id), "-i {0}".format(backup_2_id), "-i {0}".format(backup_3_id)])

        # Clean after yourself
        self.del_test_dir(module_name, fname)
