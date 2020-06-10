import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
import subprocess
from datetime import datetime
import sys
from time import sleep
from datetime import datetime, timedelta
import hashlib
import shutil
import json
from testgres import QueryException


module_name = 'restore'


class IncrRestoreTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    def test_basic_incr_restore(self):
        """recovery to target timeline"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=10)

        self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(backup_dir, 'node', node, backup_type='page')

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            options=['-T', '10', '-c', '1'])
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(backup_dir, 'node', node, backup_type='page')

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()
        pgbench.stdout.close()

        backup_id = self.backup_node(backup_dir, 'node', node, backup_type='page')

        pgdata = self.pgdata_content(node.data_dir)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()
        pgbench.stdout.close()

        node.stop()

        self.restore_node(
            backup_dir, 'node', node,
            options=["-j", "4", "--incremental-mode=checksum"])

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)


    # @unittest.skip("skip")
    def test_checksum_corruption_detection(self):
        """recovery to target timeline"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=10)

        self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(backup_dir, 'node', node, backup_type='delta')

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            options=['-T', '10', '-c', '1'])
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(backup_dir, 'node', node, backup_type='delta')

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()
        pgbench.stdout.close()

        backup_id = self.backup_node(backup_dir, 'node', node, backup_type='delta')

        pgdata = self.pgdata_content(node.data_dir)

        node.stop()

        # corrupt block

        self.restore_node(
            backup_dir, 'node', node, options=["-j", "4", "--incremental-mode=checksum"])

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_incr_restore_with_tablespace(self):
        """
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        tblspace = self.get_tblspace_path(node, 'tblspace')
        some_directory = self.get_tblspace_path(node, 'some_directory')

        # stuff new destination with garbage
        self.restore_node(backup_dir, 'node', node, data_dir=some_directory)

        self.create_tblspace_in_node(node, 'tblspace')
        node.pgbench_init(scale=10, tablespace='tblspace')

        self.backup_node(backup_dir, 'node', node, options=['--stream'])
        pgdata = self.pgdata_content(node.data_dir)

        node.stop()

        self.restore_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4", "--incremental-mode=checksum",
                "-T{0}={1}".format(tblspace, some_directory)])

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        exit(1)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_incr_restore_with_tablespace_1(self):
        """recovery to target timeline"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        tblspace = self.get_tblspace_path(node, 'tblspace')
        some_directory = self.get_tblspace_path(node, 'some_directory')

        self.restore_node(backup_dir, 'node', node, data_dir=some_directory)

        self.create_tblspace_in_node(node, 'tblspace')
        node.pgbench_init(scale=10, tablespace='tblspace')

        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node, backup_type='delta', options=['--stream'])

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node, backup_type='delta', options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        node.stop()

        self.restore_node(
            backup_dir, 'node', node,
            options=["-j", "4", "--incremental-mode=checksum"])

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_incr_restore_with_tablespace_2(self):
        """
        If "--tablespace-mapping" option is used with incremental restore,
        then new directory must be empty.
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        tblspace = self.get_tblspace_path(node, 'tblspace')
        self.create_tblspace_in_node(node, 'tblspace')
        node.pgbench_init(scale=10, tablespace='tblspace')

        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        node_1 = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_1'))

        node_1.cleanup()

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=node_1.data_dir,
            options=['--incremental-mode=checksum'])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=node_1.data_dir,
            options=['--incremental-mode=checksum', '-T{0}={1}'.format(tblspace, tblspace)])

        pgdata_restored = self.pgdata_content(node_1.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_incr_restore_sanity(self):
        """recovery to target timeline"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        try:
            self.restore_node(
                backup_dir, 'node', node,
                options=["-j", "4", "--incremental-mode=checksum"])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because there is running postmaster "
                "process in destination directory.\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'WARNING: Postmaster with pid',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertIn(
                'ERROR: Incremental restore is impossible',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        node_1 = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_1'))

        try:
            self.restore_node(
                backup_dir, 'node', node_1, data_dir=node_1.data_dir,
                options=["-j", "4", "--incremental-mode=checksum"])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because destination directory has wrong system id.\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'WARNING: Backup catalog was initialized for system id',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertIn(
                'ERROR: Incremental restore is impossible',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_incr_checksum_restore(self):
        """
                        /----C-----D
        ------A----B---*--------X

        X - is instance, we want to return it to C state.
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off', 'wal_log_hints': 'on'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=50)
        self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        self.backup_node(backup_dir, 'node', node, backup_type='page')

        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        xid = node.safe_psql(
            'postgres',
            'select txid_current()').rstrip()

        # --A-----B--------X
        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()
        node.stop(['-m', 'immediate', '-D', node.data_dir])

        node_1 = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_1'))
        node_1.cleanup()

        self.restore_node(
            backup_dir, 'node', node_1, data_dir=node_1.data_dir,
            options=[
                '--recovery-target-action=promote',
                '--recovery-target-xid={0}'.format(xid)])

        self.set_auto_conf(node_1, {'port': node_1.port})
        node_1.slow_start()

        #               /--
        # --A-----B----*----X
        pgbench = node_1.pgbench(options=['-T', '10', '-c', '1'])
        pgbench.wait()

        #               /--C
        # --A-----B----*----X
        self.backup_node(backup_dir, 'node', node_1,
            data_dir=node_1.data_dir, backup_type='page')

        #               /--C------
        # --A-----B----*----X
        pgbench = node_1.pgbench(options=['-T', '50', '-c', '1'])
        pgbench.wait()

        #               /--C------D
        # --A-----B----*----X
        self.backup_node(backup_dir, 'node', node_1,
            data_dir=node_1.data_dir, backup_type='page')

        pgdata = self.pgdata_content(node_1.data_dir)

        print(self.restore_node(
            backup_dir, 'node', node,
            options=["-j", "4", "--incremental-mode=checksum"]))

        pgdata_restored = self.pgdata_content(node.data_dir)

        self.set_auto_conf(node, {'port': node.port})
        node.slow_start()

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)


    # @unittest.skip("skip")
    def test_incr_lsn_restore(self):
        """
                        /----C-----D
        ------A----B---*--------X

        X - is instance, we want to return it to C state.
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off', 'wal_log_hints': 'on'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=50)
        self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        self.backup_node(backup_dir, 'node', node, backup_type='page')

        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        xid = node.safe_psql(
            'postgres',
            'select txid_current()').rstrip()

        # --A-----B--------X
        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()
        node.stop(['-m', 'immediate', '-D', node.data_dir])

        node_1 = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_1'))
        node_1.cleanup()

        self.restore_node(
            backup_dir, 'node', node_1, data_dir=node_1.data_dir,
            options=[
                '--recovery-target-action=promote',
                '--recovery-target-xid={0}'.format(xid)])

        self.set_auto_conf(node_1, {'port': node_1.port})
        node_1.slow_start()

        #               /--
        # --A-----B----*----X
        pgbench = node_1.pgbench(options=['-T', '10', '-c', '1'])
        pgbench.wait()

        #               /--C
        # --A-----B----*----X
        self.backup_node(backup_dir, 'node', node_1,
            data_dir=node_1.data_dir, backup_type='page')

        #               /--C------
        # --A-----B----*----X
        pgbench = node_1.pgbench(options=['-T', '50', '-c', '1'])
        pgbench.wait()

        #               /--C------D
        # --A-----B----*----X
        self.backup_node(backup_dir, 'node', node_1,
            data_dir=node_1.data_dir, backup_type='page')

        pgdata = self.pgdata_content(node_1.data_dir)

        print(self.restore_node(
            backup_dir, 'node', node, options=["-j", "4", "--incremental-mode=lsn"]))

        pgdata_restored = self.pgdata_content(node.data_dir)

        self.set_auto_conf(node, {'port': node.port})
        node.slow_start()

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_incr_shift_sanity(self):
        """
                /----A-----B
        F------*--------X

        X - is instance, we want to return it to state B.
        fail is expected behaviour in case of shift restore.
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off', 'wal_log_hints': 'on'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(backup_dir, 'node', node)
        node.pgbench_init(scale=10)

        node_1 = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_1'))
        node_1.cleanup()

        self.restore_node(
            backup_dir, 'node', node_1, data_dir=node_1.data_dir)

        self.set_auto_conf(node_1, {'port': node_1.port})
        node_1.slow_start()

        pgbench = node_1.pgbench(options=['-T', '10', '-c', '1'])
        pgbench.wait()

        self.backup_node(backup_dir, 'node', node_1,
            data_dir=node_1.data_dir, backup_type='full')

        pgbench = node_1.pgbench(options=['-T', '10', '-c', '1'])
        pgbench.wait()

        page_id = self.backup_node(backup_dir, 'node', node_1,
            data_dir=node_1.data_dir, backup_type='page')

        node.stop()

        try:
            self.restore_node(
                backup_dir, 'node', node, data_dir=node.data_dir,
                options=["-j", "4", "--incremental-mode=lsn"])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because incremental restore in shift mode is impossible\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Cannot perform incremental restore of "
                "backup chain {0} in 'lsn' mode".format(page_id),
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

        # @unittest.skip("skip")
    def test_incr_checksum_sanity(self):
        """
                /----A-----B
        F------*--------X

        X - is instance, we want to return it to state B.
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(backup_dir, 'node', node)
        node.pgbench_init(scale=20)

        node_1 = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_1'))
        node_1.cleanup()

        self.restore_node(
            backup_dir, 'node', node_1, data_dir=node_1.data_dir)

        self.set_auto_conf(node_1, {'port': node_1.port})
        node_1.slow_start()

        pgbench = node_1.pgbench(options=['-T', '10', '-c', '1'])
        pgbench.wait()

        self.backup_node(backup_dir, 'node', node_1,
            data_dir=node_1.data_dir, backup_type='full')

        pgbench = node_1.pgbench(options=['-T', '10', '-c', '1'])
        pgbench.wait()

        page_id = self.backup_node(backup_dir, 'node', node_1,
            data_dir=node_1.data_dir, backup_type='page')
        pgdata = self.pgdata_content(node_1.data_dir)

        node.stop()

        self.restore_node(
            backup_dir, 'node', node, data_dir=node.data_dir,
            options=["-j", "4", "--incremental-mode=checksum"])

        pgdata_restored = self.pgdata_content(node.data_dir)

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)


        # @unittest.skip("skip")
    def test_incr_checksum_corruption_detection(self):
        """
        check that corrupted page got detected and replaced
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
#            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off', 'wal_log_hints': 'on'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(backup_dir, 'node', node)
        node.pgbench_init(scale=20)

        pgbench = node.pgbench(options=['-T', '10', '-c', '1'])
        pgbench.wait()

        self.backup_node(backup_dir, 'node', node,
            data_dir=node.data_dir, backup_type='full')

        heap_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('pgbench_accounts')").rstrip()

        pgbench = node.pgbench(options=['-T', '10', '-c', '1'])
        pgbench.wait()

        page_id = self.backup_node(backup_dir, 'node', node,
            data_dir=node.data_dir, backup_type='page')

        pgdata = self.pgdata_content(node.data_dir)

        node.stop()

        path = os.path.join(node.data_dir, heap_path)
        with open(path, "rb+", 0) as f:
                f.seek(22000)
                f.write(b"bla")
                f.flush()
                f.close

        print(self.restore_node(
            backup_dir, 'node', node, data_dir=node.data_dir,
            options=["-j", "4", "--incremental-mode=checksum"]))

        pgdata_restored = self.pgdata_content(node.data_dir)

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

        # @unittest.skip("skip")
    def test_incr_shift_corruption_detection(self):
        """
        check that corrupted page got detected and replaced
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off', 'wal_log_hints': 'on'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(backup_dir, 'node', node)
        node.pgbench_init(scale=20)

        pgbench = node.pgbench(options=['-T', '10', '-c', '1'])
        pgbench.wait()

        self.backup_node(backup_dir, 'node', node,
            data_dir=node.data_dir, backup_type='full')

        heap_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('pgbench_accounts')").rstrip()

        pgbench = node.pgbench(options=['-T', '10', '-c', '1'])
        pgbench.wait()

        page_id = self.backup_node(backup_dir, 'node', node,
            data_dir=node.data_dir, backup_type='page')

        pgdata = self.pgdata_content(node.data_dir)

        node.stop()

        path = os.path.join(node.data_dir, heap_path)
        with open(path, "rb+", 0) as f:
                f.seek(22000)
                f.write(b"bla")
                f.flush()
                f.close

        self.restore_node(
            backup_dir, 'node', node, data_dir=node.data_dir,
            options=["-j", "4", "--incremental-mode=lsn"])

        pgdata_restored = self.pgdata_content(node.data_dir)

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_incr_restore_multiple_external(self):
        """check that cmdline has priority over config"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # FULL backup
        node.pgbench_init(scale=20)
        self.backup_node(
            backup_dir, 'node', node,
            backup_type="full", options=["-j", "4"])

        # fill external directories with data
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2, options=["-j", "4"])

        self.set_config(
            backup_dir, 'node',
            options=['-E{0}:{1}'.format(external_dir1, external_dir2)])

        # cmdline option MUST override options in config
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='full', options=["-j", "4"])

        pgbench = node.pgbench(options=['-T', '10', '-c', '1'])
        pgbench.wait()

        # cmdline option MUST override options in config
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', options=["-j", "4"])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        pgbench = node.pgbench(options=['-T', '10', '-c', '1'])
        pgbench.wait()

        node.stop()

        print(self.restore_node(
            backup_dir, 'node', node,
            options=["-j", "4", '--incremental-mode=checksum', '--log-level-console=VERBOSE']))

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_incr_shift_restore_multiple_external(self):
        """check that cmdline has priority over config"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # FULL backup
        node.pgbench_init(scale=20)
        self.backup_node(
            backup_dir, 'node', node,
            backup_type="full", options=["-j", "4"])

        # fill external directories with data
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2, options=["-j", "4"])

        self.set_config(
            backup_dir, 'node',
            options=['-E{0}:{1}'.format(external_dir1, external_dir2)])

        # cmdline option MUST override options in config
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='full', options=["-j", "4"])

        pgbench = node.pgbench(options=['-T', '10', '-c', '1'])
        pgbench.wait()

        # cmdline option MUST override options in config
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', options=["-j", "4"])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        pgbench = node.pgbench(options=['-T', '10', '-c', '1'])
        pgbench.wait()

        node.stop()

        print(self.restore_node(
            backup_dir, 'node', node,
            options=["-j", "4", '--incremental-mode=lsn']))

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

# check that MinRecPoint and BackupStartLsn are correctly used in case of --incrementa-lsn
# incremental restore + partial restore.
