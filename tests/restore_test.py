import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, fs_backup_class
from pg_probackup2.gdb import needs_gdb
import subprocess
from time import sleep
from datetime import datetime, timedelta, timezone
import shutil
import json
from testgres import QueryException, StartNodeException
import testgres.utils as testgres_utils

import re


MAGIC_COUNT = 107183


class RestoreTest(ProbackupTest):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_restore_full_to_latest(self):
        """recovery to latest from full backup"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=2)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()
        before = node.table_checksum("pgbench_branches")
        backup_id = self.pb.backup_node('node', node)

        node.stop()
        node.cleanup()

        # 1 - Test recovery from latest
        restore_result = self.pb.restore_node('node', node, options=["-j", "4"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

        # 2 - Test that recovery.conf was created
        # TODO update test
        if self.pg_config_version >= self.version_to_num('12.0'):
            recovery_conf = os.path.join(node.data_dir, 'postgresql.auto.conf')
            with open(recovery_conf, 'r') as f:
                print(f.read())
        else:
            recovery_conf = os.path.join(node.data_dir, 'recovery.conf')
        self.assertEqual(os.path.isfile(recovery_conf), True)

        node.slow_start()

        after = node.table_checksum("pgbench_branches")
        self.assertEqual(before, after)

    # @unittest.skip("skip")
    def test_restore_full_page_to_latest(self):
        """recovery to latest from full + page backups"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=2)

        self.pb.backup_node('node', node)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        backup_id = self.pb.backup_node('node', node, backup_type="page")

        before = node.table_checksum("pgbench_branches")

        node.stop()
        node.cleanup()

        restore_result = self.pb.restore_node('node', node, options=["-j", "4"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

        node.slow_start()

        after = node.table_checksum("pgbench_branches")
        self.assertEqual(before, after)

    # @unittest.skip("skip")
    def test_restore_to_specific_timeline(self):
        """recovery to target timeline"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=2)

        before = node.table_checksum("pgbench_branches")

        backup_id = self.pb.backup_node('node', node)

        target_tli = int(
            node.get_control_data()["Latest checkpoint's TimeLineID"])
        node.stop()
        node.cleanup()

        restore_result = self.pb.restore_node('node', node, options=["-j", "4"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

        node.slow_start()
        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            options=['-T', '10', '-c', '2', '--no-vacuum'])
        pgbench.wait()
        pgbench.stdout.close()

        self.pb.backup_node('node', node)

        node.stop()
        node.cleanup()

        # Correct Backup must be choosen for restore
        restore_result = self.pb.restore_node('node', node,
                options=[
                    "-j", "4", "--timeline={0}".format(target_tli)]
            )
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

        recovery_target_timeline = self.get_recovery_conf(node)["recovery_target_timeline"]
        self.assertEqual(int(recovery_target_timeline), target_tli)

        node.slow_start()
        after = node.table_checksum("pgbench_branches")
        self.assertEqual(before, after)

    # @unittest.skip("skip")
    def test_restore_to_time(self):
        """recovery to target time"""
        node = self.pg_node.make_simple('node',
            pg_options={'TimeZone': 'GMT'})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=2)
        before = node.table_checksum("pgbench_branches")

        backup_id = self.pb.backup_node('node', node)

        node.safe_psql("postgres", "select txid_current()")
        target_time = node.safe_psql("postgres", "SELECT current_timestamp").decode('utf-8').strip()
        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        node.stop()
        node.cleanup()

        restore_result = self.pb.restore_node('node', node,
                options=[
                    "-j", "4", '--recovery-target-time={0}'.format(target_time),
                    "--recovery-target-action=promote"
                    ]
                )
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

        node.slow_start()
        after = node.table_checksum("pgbench_branches")
        self.assertEqual(before, after)

    # @unittest.skip("skip")
    def test_restore_to_xid_inclusive(self):
        """recovery to target xid"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=2)
        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        backup_id = self.pb.backup_node('node', node)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        before = node.table_checksum("pgbench_branches")
        with node.connect("postgres") as con:
            res = con.execute("INSERT INTO tbl0005 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        node.stop()
        node.cleanup()

        restore_result = self.pb.restore_node('node', node,
                options=[
                    "-j", "4", '--xid={0}'.format(target_xid),
                    "--recovery-target-action=promote"]
            )
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))
        node.slow_start()
        after = node.table_checksum("pgbench_branches")
        self.assertEqual(before, after)
        self.assertEqual(
            len(node.execute("postgres", "SELECT * FROM tbl0005")), 1)

    # @unittest.skip("skip")
    def test_restore_to_xid_not_inclusive(self):
        """recovery with target inclusive false"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=2)
        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        backup_id = self.pb.backup_node('node', node)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        before = node.table_checksum("pgbench_branches")
        with node.connect("postgres") as con:
            result = con.execute("INSERT INTO tbl0005 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = result[0][0]

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        node.stop()
        node.cleanup()

        restore_result = self.pb.restore_node('node', node,
                options=[
                    "-j", "4",
                    '--xid={0}'.format(target_xid),
                    "--inclusive=false",
                    "--recovery-target-action=promote"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

        node.slow_start()
        after = node.table_checksum("pgbench_branches")
        self.assertEqual(before, after)
        self.assertEqual(
            len(node.execute("postgres", "SELECT * FROM tbl0005")), 0)

    # @unittest.skip("skip")
    def test_restore_to_lsn_inclusive(self):
        """recovery to target lsn"""
        node = self.pg_node.make_simple('node')
        node.set_auto_conf({"autovacuum": "off"})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=2)
        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a int)")
            con.commit()

        backup_id = self.pb.backup_node('node', node)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        before = node.table_checksum("pgbench_branches")
        with node.connect("postgres") as con:
            con.execute("INSERT INTO tbl0005 VALUES (1)")
            con.commit()
            con.execute("INSERT INTO tbl0005 VALUES (2)")
            # With high probability, returned lsn will point at COMMIT start
            # If this test still will be flappy, get lsn after commit and add
            # one more xlog record (for example, with txid_current()+abort).
            res = con.execute("SELECT pg_current_wal_insert_lsn()")
            con.commit()
            target_lsn = res[0][0]

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        node.stop()
        node.cleanup()

        restore_result = self.pb.restore_node('node', node,
                options=[
                    "-j", "4", '--lsn={0}'.format(target_lsn),
                    "--recovery-target-action=promote"]
            )
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

        node.slow_start()

        after = node.table_checksum("pgbench_branches")
        self.assertEqual(before, after)
        self.assertEqual(
            len(node.execute("postgres", "SELECT * FROM tbl0005")), 2)

    # @unittest.skip("skip")
    def test_restore_to_lsn_not_inclusive(self):
        """recovery to target lsn"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=2)
        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a int)")
            con.commit()

        backup_id = self.pb.backup_node('node', node)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        before = node.table_checksum("pgbench_branches")
        with node.connect("postgres") as con:
            con.execute("INSERT INTO tbl0005 VALUES (1)")
            con.commit()
            con.execute("INSERT INTO tbl0005 VALUES (2)")
            # Returned lsn will certainly point at COMMIT start OR BEFORE IT,
            # if some background activity wrote record in between INSERT and
            # COMMIT. Any way, test should succeed.
            res = con.execute("SELECT pg_current_wal_insert_lsn()")
            con.commit()
            target_lsn = res[0][0]

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        node.stop()
        node.cleanup()

        restore_result = self.pb.restore_node('node', node,
                options=[
                    "--inclusive=false",
                    "-j", "4", '--lsn={0}'.format(target_lsn),
                    "--recovery-target-action=promote"]
            )
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

        node.slow_start()

        after = node.table_checksum("pgbench_branches")
        self.assertEqual(before, after)
        self.assertEqual(
            len(node.execute("postgres", "SELECT * FROM tbl0005")), 1)

    # @unittest.skip("skip")
    def test_restore_full_ptrack_archive(self):
        """recovery to latest from archive full+ptrack backups"""
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

        node = self.pg_node.make_simple('node',
            ptrack_enable=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        node.pgbench_init(scale=2)

        self.pb.backup_node('node', node)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        backup_id = self.pb.backup_node('node', node, backup_type="ptrack")

        before = node.table_checksum("pgbench_branches")

        node.stop()
        node.cleanup()

        restore_result = self.pb.restore_node('node', node,
                options=["-j", "4"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))
        node.slow_start()
        after = node.table_checksum("pgbench_branches")
        self.assertEqual(before, after)

    # @unittest.skip("skip")
    def test_restore_ptrack(self):
        """recovery to latest from archive full+ptrack+ptrack backups"""
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

        node = self.pg_node.make_simple('node',
            ptrack_enable=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        node.pgbench_init(scale=2)

        self.pb.backup_node('node', node)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        self.pb.backup_node('node', node, backup_type="ptrack")

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        backup_id = self.pb.backup_node('node', node, backup_type="ptrack")

        before = node.table_checksum("pgbench_branches")

        node.stop()
        node.cleanup()

        restore_result = self.pb.restore_node('node', node,
                options=["-j", "4"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

        node.slow_start()
        after = node.table_checksum("pgbench_branches")
        self.assertEqual(before, after)

    # @unittest.skip("skip")
    def test_restore_full_ptrack_stream(self):
        """recovery in stream mode to latest from full + ptrack backups"""
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

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

        node.pgbench_init(scale=2)

        self.pb.backup_node('node', node, options=["--stream"])

        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        backup_id = self.pb.backup_node('node', node,
            backup_type="ptrack", options=["--stream"])

        before = node.table_checksum("pgbench_branches")

        node.stop()
        node.cleanup()

        restore_result = self.pb.restore_node('node', node, options=["-j", "4"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

        node.slow_start()
        after = node.table_checksum("pgbench_branches")
        self.assertEqual(before, after)

    # @unittest.skip("skip")
    def test_restore_full_ptrack_under_load(self):
        """
        recovery to latest from full + ptrack backups
        with loads when ptrack backup do
        """
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

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

        node.pgbench_init(scale=2)

        self.pb.backup_node('node', node)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "8"]
        )

        backup_id = self.pb.backup_node('node', node,
            backup_type="ptrack", options=["--stream"])

        pgbench.wait()
        pgbench.stdout.close()

        bbalance = node.execute(
            "postgres", "SELECT sum(bbalance) FROM pgbench_branches")
        delta = node.execute(
            "postgres", "SELECT sum(delta) FROM pgbench_history")

        self.assertEqual(bbalance, delta)
        node.stop()
        node.cleanup()

        restore_result = self.pb.restore_node('node', node, options=["-j", "4"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

        node.slow_start()
        bbalance = node.execute(
            "postgres", "SELECT sum(bbalance) FROM pgbench_branches")
        delta = node.execute(
            "postgres", "SELECT sum(delta) FROM pgbench_history")
        self.assertEqual(bbalance, delta)

    # @unittest.skip("skip")
    def test_restore_full_under_load_ptrack(self):
        """
        recovery to latest from full + page backups
        with loads when full backup do
        """
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

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

        # wal_segment_size = self.guc_wal_segment_size(node)
        node.pgbench_init(scale=2)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "8"]
        )

        self.pb.backup_node('node', node)

        pgbench.wait()
        pgbench.stdout.close()

        backup_id = self.pb.backup_node('node', node,
            backup_type="ptrack", options=["--stream"])

        bbalance = node.execute(
            "postgres", "SELECT sum(bbalance) FROM pgbench_branches")
        delta = node.execute(
            "postgres", "SELECT sum(delta) FROM pgbench_history")

        self.assertEqual(bbalance, delta)

        node.stop()
        node.cleanup()

        restore_result = self.pb.restore_node('node', node, options=["-j", "4"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))
        node.slow_start()
        bbalance = node.execute(
            "postgres", "SELECT sum(bbalance) FROM pgbench_branches")
        delta = node.execute(
            "postgres", "SELECT sum(delta) FROM pgbench_history")
        self.assertEqual(bbalance, delta)

    # @unittest.skip("skip")
    def test_restore_with_tablespace_mapping_1(self):
        """recovery using tablespace-mapping option"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Create tablespace
        tblspc_path = os.path.join(node.base_dir, "tblspc")
        os.makedirs(tblspc_path)
        with node.connect("postgres") as con:
            con.connection.autocommit = True
            con.execute("CREATE TABLESPACE tblspc LOCATION '%s'" % tblspc_path)
            con.connection.autocommit = False
            con.execute("CREATE TABLE test (id int) TABLESPACE tblspc")
            con.execute("INSERT INTO test VALUES (1)")
            con.commit()

        backup_id = self.pb.backup_node('node', node)
        self.assertEqual(self.pb.show('node')[0]['status'], "OK")

        # 1 - Try to restore to existing directory
        node.stop()
        self.pb.restore_node('node', node=node,
                          expect_error="because restore destination is not empty")
        self.assertMessage(contains='ERROR: Restore destination is not empty:')

        # 2 - Try to restore to existing tablespace directory
        tblspc_path_tmp = os.path.join(node.base_dir, "tblspc_tmp")
        os.rename(tblspc_path, tblspc_path_tmp)
        shutil.rmtree(node.data_dir)
        os.rename(tblspc_path_tmp, tblspc_path)
        self.pb.restore_node('node', node=node,
                          expect_error="because restore tablespace destination is not empty")
        self.assertMessage(contains='ERROR: Restore tablespace destination is not empty:')

        # 3 - Restore using tablespace-mapping to not empty directory
        tblspc_path_temp = os.path.join(node.base_dir, "tblspc_temp")
        os.mkdir(tblspc_path_temp)
        with open(os.path.join(tblspc_path_temp, 'file'), 'w+') as f:
            f.close()

        self.pb.restore_node('node', node=node,
                          options=["-T", f"{tblspc_path}={tblspc_path_temp}"],
                          expect_error="because restore tablespace destination is not empty")
        self.assertMessage(contains='ERROR: Restore tablespace destination is not empty:')

        # 4 - Restore using tablespace-mapping
        tblspc_path_new = os.path.join(node.base_dir, "tblspc_new")
        restore_result = self.pb.restore_node('node', node,
                options=[
                    "-T", "%s=%s" % (tblspc_path, tblspc_path_new)]
            )
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

        node.slow_start()

        result = node.execute("postgres", "SELECT id FROM test")
        self.assertEqual(result[0][0], 1)

        # 4 - Restore using tablespace-mapping using page backup
        self.pb.backup_node('node', node)
        with node.connect("postgres") as con:
            con.execute("INSERT INTO test VALUES (2)")
            con.commit()
        backup_id = self.pb.backup_node('node', node, backup_type="page")

        show_pb = self.pb.show('node')
        self.assertEqual(show_pb[1]['status'], "OK")
        self.assertEqual(show_pb[2]['status'], "OK")

        node.stop()
        shutil.rmtree(node.data_dir)
        tblspc_path_page = os.path.join(node.base_dir, "tblspc_page")

        restore_result = self.pb.restore_node('node', node,
                options=[
                    "-T", "%s=%s" % (tblspc_path_new, tblspc_path_page)])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

        node.slow_start()
        result = node.execute("postgres", "SELECT id FROM test OFFSET 1")
        self.assertEqual(result[0][0], 2)

    # @unittest.skip("skip")
    def test_restore_with_tablespace_mapping_2(self):
        """recovery using tablespace-mapping option and page backup"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Full backup
        self.pb.backup_node('node', node)
        self.assertEqual(self.pb.show('node')[0]['status'], "OK")

        # Create tablespace
        tblspc_path = os.path.join(node.base_dir, "tblspc")
        os.makedirs(tblspc_path)
        with node.connect("postgres") as con:
            con.connection.autocommit = True
            con.execute("CREATE TABLESPACE tblspc LOCATION '%s'" % tblspc_path)
            con.connection.autocommit = False
            con.execute(
                "CREATE TABLE tbl AS SELECT * "
                "FROM generate_series(0,3) AS integer")
            con.commit()

        # First page backup
        self.pb.backup_node('node', node, backup_type="page")
        self.assertEqual(self.pb.show('node')[1]['status'], "OK")
        self.assertEqual(
            self.pb.show('node')[1]['backup-mode'], "PAGE")

        # Create tablespace table
        with node.connect("postgres") as con:
            con.connection.autocommit = True
            con.execute("CHECKPOINT")
            con.connection.autocommit = False
            con.execute("CREATE TABLE tbl1 (a int) TABLESPACE tblspc")
            con.execute(
                "INSERT INTO tbl1 SELECT * "
                "FROM generate_series(0,3) AS integer")
            con.commit()

        # Second page backup
        backup_id = self.pb.backup_node('node', node, backup_type="page")
        self.assertEqual(self.pb.show('node')[2]['status'], "OK")
        self.assertEqual(
            self.pb.show('node')[2]['backup-mode'], "PAGE")

        node.stop()
        node.cleanup()

        tblspc_path_new = os.path.join(node.base_dir, "tblspc_new")

        restore_result = self.pb.restore_node('node', node,
                options=[
                    "-T", "%s=%s" % (tblspc_path, tblspc_path_new)])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))
        node.slow_start()

        count = node.execute("postgres", "SELECT count(*) FROM tbl")
        self.assertEqual(count[0][0], 4)
        count = node.execute("postgres", "SELECT count(*) FROM tbl1")
        self.assertEqual(count[0][0], 4)

    # @unittest.skip("skip")
    def test_restore_with_missing_or_corrupted_tablespace_map(self):
        """restore backup with missing or corrupted tablespace_map"""
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Create tablespace
        self.create_tblspace_in_node(node, 'tblspace')
        node.pgbench_init(scale=1, tablespace='tblspace')

        # Full backup
        self.pb.backup_node('node', node)

        # Change some data
        pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Page backup
        page_id = self.pb.backup_node('node', node, backup_type="page")

        pgdata = self.pgdata_content(node.data_dir)

        node2 = self.pg_node.make_simple('node2')
        node2.cleanup()

        olddir = self.get_tblspace_path(node, 'tblspace')
        newdir = self.get_tblspace_path(node2, 'tblspace')

        # drop tablespace_map
        tablespace_map = self.read_backup_file(backup_dir, 'node', page_id,
                                               'database/tablespace_map', text=True)
        self.remove_backup_file(backup_dir, 'node', page_id, 'database/tablespace_map')

        self.pb.restore_node('node', node=node2,
                          options=["-T", "{0}={1}".format(olddir, newdir)],
                          expect_error="because tablespace_map is missing")
        self.assertMessage(regex=
                rf'ERROR: Tablespace map is missing: "[^"]*{page_id}[^"]*tablespace_map", '
                rf'probably backup {page_id} is corrupt, validate it')

        self.pb.restore_node('node', node=node2,
                          expect_error="because tablespace_map is missing")
        self.assertMessage(regex=
                rf'ERROR: Tablespace map is missing: "[^"]*{page_id}[^"]*tablespace_map", '
                rf'probably backup {page_id} is corrupt, validate it')

        self.corrupt_backup_file(backup_dir, 'node', page_id, 'database/tablespace_map',
                                 overwrite=tablespace_map + "HELLO\n", text=True)

        self.pb.restore_node('node', node=node2,
                          options=["-T", f"{olddir}={newdir}"],
                          expect_error="because tablespace_map is corupted")
        self.assertMessage(regex=r'ERROR: Invalid CRC of tablespace map file '
                                 rf'"[^"]*{page_id}[^"]*tablespace_map"')

        self.pb.restore_node('node', node=node2,
                          expect_error="because tablespace_map is corupted")
        self.assertMessage(regex=r'ERROR: Invalid CRC of tablespace map file '
                                 rf'"[^"]*{page_id}[^"]*tablespace_map"')

        # write correct back
        self.write_backup_file(backup_dir, 'node', page_id, 'database/tablespace_map',
                               tablespace_map, text=True)

        print(self.pb.restore_node('node', node2,
            options=["-T", "{0}={1}".format(olddir, newdir)]))

        pgdata_restored = self.pgdata_content(node2.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_archive_node_backup_stream_restore_to_recovery_time(self):
        """
        make node with archiving, make stream backup,
        make PITR to Recovery Time
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node, options=["--stream"])
        node.safe_psql("postgres", "create table t_heap(a int)")

        node.stop()
        node.cleanup()

        recovery_time = self.pb.show('node', backup_id)['recovery-time']

        restore_result = self.pb.restore_node('node', node,
            options=[
                "-j", "4", '--time={0}'.format(recovery_time),
                "--recovery-target-action=promote"
            ]
        )
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))
        node.slow_start()

        result = node.psql("postgres", 'select * from t_heap')
        self.assertTrue('does not exist' in result[2].decode("utf-8"))

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_archive_node_backup_stream_restore_to_recovery_time(self):
        """
        make node with archiving, make stream backup,
        make PITR to Recovery Time
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node, options=["--stream"])
        node.safe_psql("postgres", "create table t_heap(a int)")
        node.stop()
        node.cleanup()

        recovery_time = self.pb.show('node', backup_id)['recovery-time']

        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(backup_id),
            self.pb.restore_node('node', node,
                options=[
                    "-j", "4", '--time={0}'.format(recovery_time),
                    "--recovery-target-action=promote"
                ]
            ),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                repr(self.output), self.cmd))

        node.slow_start()
        result = node.psql("postgres", 'select * from t_heap')
        self.assertTrue('does not exist' in result[2].decode("utf-8"))

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_archive_node_backup_stream_pitr(self):
        """
        make node with archiving, make stream backup,
        create table t_heap, make pitr to Recovery Time,
        check that t_heap does not exist
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node, options=["--stream"])
        node.safe_psql("postgres", "create table t_heap(a int)")
        node.cleanup()

        recovery_time = self.pb.show('node', backup_id)['recovery-time']

        restore_result = self.pb.restore_node('node', node,
                options=[
                    "-j", "4", '--time={0}'.format(recovery_time),
                    "--recovery-target-action=promote"
                    ]
            )
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

        node.slow_start()

        result = node.psql("postgres", 'select * from t_heap')
        self.assertEqual(True, 'does not exist' in result[2].decode("utf-8"))

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_archive_node_backup_archive_pitr_2(self):
        """
        make node with archiving, make archive backup,
        create table t_heap, make pitr to Recovery Time,
        check that t_heap do not exist
        """
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node)
        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node.safe_psql("postgres", "create table t_heap(a int)")
        node.stop()

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        recovery_time = self.pb.show('node', backup_id)['recovery-time']

        resotre_result = self.pb.restore_node('node', node_restored,
                options=[
                    "-j", "4", '--time={0}'.format(recovery_time),
                    "--recovery-target-action=promote"]
            )
        self.assertMessage(resotre_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})

        node_restored.slow_start()

        result = node_restored.psql("postgres", 'select * from t_heap')
        self.assertTrue('does not exist' in result[2].decode("utf-8"))

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_archive_restore_to_restore_point(self):
        """
        make node with archiving, make archive backup,
        create table t_heap, make pitr to Recovery Time,
        check that t_heap do not exists
        """
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node)

        node.safe_psql(
            "postgres",
            "create table t_heap as select generate_series(0,10000)")
        result = node.table_checksum("t_heap")
        node.safe_psql(
            "postgres", "select pg_create_restore_point('savepoint')")
        node.safe_psql(
            "postgres",
            "create table t_heap_1 as select generate_series(0,10000)")
        node.cleanup()

        self.pb.restore_node('node', node,
            options=[
                "--recovery-target-name=savepoint",
                "--recovery-target-action=promote"])

        node.slow_start()

        result_new = node.table_checksum("t_heap")
        res = node.psql("postgres", "select * from t_heap_1")
        self.assertEqual(
            res[0], 1,
            "Table t_heap_1 should not exist in restored instance")

        self.assertEqual(result, result_new)

    @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_zags_block_corrupt(self):
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node)

        conn = node.connect()
        with node.connect("postgres") as conn:

            conn.execute(
                "create table tbl(i int)")
            conn.commit()
            conn.execute(
                "create index idx ON tbl (i)")
            conn.commit()
            conn.execute(
                "insert into tbl select i from generate_series(0,400) as i")
            conn.commit()
            conn.execute(
                "select pg_relation_size('idx')")
            conn.commit()
            conn.execute(
                "delete from tbl where i < 100")
            conn.commit()
            conn.execute(
                "explain analyze select i from tbl order by i")
            conn.commit()
            conn.execute(
                "select i from tbl order by i")
            conn.commit()
            conn.execute(
                "create extension pageinspect")
            conn.commit()
            print(conn.execute(
                "select * from bt_page_stats('idx',1)"))
            conn.commit()
            conn.execute(
                "insert into tbl select i from generate_series(0,100) as i")
            conn.commit()
            conn.execute(
                "insert into tbl select i from generate_series(0,100) as i")
            conn.commit()
            conn.execute(
                "insert into tbl select i from generate_series(0,100) as i")
            conn.commit()
            conn.execute(
                "insert into tbl select i from generate_series(0,100) as i")

        node_restored = self.pg_node.make_simple('node_restored')

        node_restored.cleanup()

        self.pb.restore_node('node', node_restored)

        node_restored.set_auto_conf({'archive_mode': 'off', 'hot_standby': 'on', 'port': node_restored.port})

        node_restored.slow_start()

    @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_zags_block_corrupt_1(self):
        node = self.pg_node.make_simple('node',
            pg_options={
                'full_page_writes': 'on'}
            )
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node)

        node.safe_psql('postgres', 'create table tbl(i int)')

        node.safe_psql('postgres', 'create index idx ON tbl (i)')

        node.safe_psql(
            'postgres',
            'insert into tbl select i from generate_series(0,100000) as i')

        node.safe_psql(
            'postgres',
            'delete from tbl where i%2 = 0')

        node.safe_psql(
            'postgres',
            'explain analyze select i from tbl order by i')

        node.safe_psql(
            'postgres',
            'select i from tbl order by i')

        node.safe_psql(
            'postgres',
            'create extension pageinspect')

        node.safe_psql(
            'postgres',
            'insert into tbl select i from generate_series(0,100) as i')

        node.safe_psql(
            'postgres',
            'insert into tbl select i from generate_series(0,100) as i')

        node.safe_psql(
            'postgres',
            'insert into tbl select i from generate_series(0,100) as i')

        node.safe_psql(
            'postgres',
            'insert into tbl select i from generate_series(0,100) as i')

        self.switch_wal_segment(node)

        node_restored = self.pg_node.make_simple('node_restored')

        pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.pb.restore_node('node', node_restored)

        node_restored.set_auto_conf({'archive_mode': 'off', 'hot_standby': 'on', 'port': node_restored.port})

        node_restored.slow_start()

        while True:
            with open(node_restored.pg_log_file, 'r') as f:
                if 'selected new timeline ID' in f.read():
                    break

        # with open(node_restored.pg_log_file, 'r') as f:
        #        print(f.read())

        pgdata_restored = self.pgdata_content(node_restored.data_dir)

        self.compare_pgdata(pgdata, pgdata_restored)

#        pg_xlogdump_path = self.get_bin_path('pg_xlogdump')

#        pg_xlogdump = self.pb.run_binary(
#            [
#                pg_xlogdump_path, '-b',
#                os.path.join(backup_dir, 'wal', 'node', '000000010000000000000003'),
#                ' | ', 'grep', 'Btree', ''
#            ], async=False)

        if pg_xlogdump.returncode:
            self.assertFalse(
                True,
                'Failed to start pg_wal_dump: {0}'.format(
                    pg_receivexlog.communicate()[1]))

    # @unittest.skip("skip")
    def test_restore_chain(self):
        """
        make node, take full backup, take several
        ERROR delta backups, take valid delta backup,
        restore must be successfull
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Take FULL
        self.pb.backup_node('node', node)

        # Take DELTA
        self.pb.backup_node('node', node, backup_type='delta')

        # Take ERROR DELTA
        self.pb.backup_node('node', node, backup_type='delta',
                         options=['-U', 'wrong_name'],
                         expect_error=True)

        # Take ERROR DELTA
        self.pb.backup_node('node', node, backup_type='delta',
                         options=['-U', 'wrong_name'],
                         expect_error=True)

        # Take DELTA
        self.pb.backup_node('node', node, backup_type='delta')

        # Take ERROR DELTA
        self.pb.backup_node('node', node, backup_type='delta',
                         options=['-U', 'wrong_name'],
                         expect_error=True)

        self.assertEqual(
            'OK',
            self.pb.show('node')[0]['status'],
            'Backup STATUS should be "OK"')

        self.assertEqual(
            'OK',
            self.pb.show('node')[1]['status'],
            'Backup STATUS should be "OK"')

        self.assertEqual(
            'ERROR',
            self.pb.show('node')[2]['status'],
            'Backup STATUS should be "ERROR"')

        self.assertEqual(
            'ERROR',
            self.pb.show('node')[3]['status'],
            'Backup STATUS should be "ERROR"')

        self.assertEqual(
            'OK',
            self.pb.show('node')[4]['status'],
            'Backup STATUS should be "OK"')

        self.assertEqual(
            'ERROR',
            self.pb.show('node')[5]['status'],
            'Backup STATUS should be "ERROR"')

        node.cleanup()

        self.pb.restore_node('node', node=node)

    # @unittest.skip("skip")
    def test_restore_chain_with_corrupted_backup(self):
        """more complex test_restore_chain()"""
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Take FULL
        self.pb.backup_node('node', node)

        # Take DELTA
        self.pb.backup_node('node', node, backup_type='page')

        # Take ERROR DELTA
        self.pb.backup_node('node', node, backup_type='page',
                         options=['-U', 'wrong_name'],
                         expect_error=True)

        # Take 1 DELTA
        self.pb.backup_node('node', node, backup_type='delta')

        # Take ERROR DELTA
        self.pb.backup_node('node', node, backup_type='delta',
                         options=['-U', 'wrong_name'],
                         expect_error=True)

        # Take 2 DELTA
        self.pb.backup_node('node', node, backup_type='delta')

        # Take ERROR DELTA
        self.pb.backup_node('node', node, backup_type='delta',
                         options=['-U', 'wrong_name'],
                         expect_error=True)

        # Take 3 DELTA
        self.pb.backup_node('node', node, backup_type='delta')

        # Corrupted 4 DELTA
        corrupt_id = self.pb.backup_node('node', node, backup_type='delta')

        # ORPHAN 5 DELTA
        restore_target_id = self.pb.backup_node('node', node, backup_type='delta')

        # ORPHAN 6 DELTA
        self.pb.backup_node('node', node, backup_type='delta')

        # NEXT FULL BACKUP
        self.pb.backup_node('node', node, backup_type='full')

        # Next Delta
        self.pb.backup_node('node', node, backup_type='delta')

        # do corrupt 6 DELTA backup
        self.remove_backup_file(backup_dir, 'node', corrupt_id,
                                'database/global/pg_control')

        # RESTORE BACKUP
        node.cleanup()

        self.pb.restore_node('node', node=node, backup_id=restore_target_id,
                          expect_error="because restore backup is corrupted")
        self.assertMessage(contains=f'ERROR: Backup {restore_target_id} is orphan')

        self.assertEqual(
            'OK',
            self.pb.show('node')[0]['status'],
            'Backup STATUS should be "OK"')

        self.assertEqual(
            'OK',
            self.pb.show('node')[1]['status'],
            'Backup STATUS should be "OK"')

        self.assertEqual(
            'ERROR',
            self.pb.show('node')[2]['status'],
            'Backup STATUS should be "ERROR"')

        self.assertEqual(
            'OK',
            self.pb.show('node')[3]['status'],
            'Backup STATUS should be "OK"')

        self.assertEqual(
            'ERROR',
            self.pb.show('node')[4]['status'],
            'Backup STATUS should be "ERROR"')

        self.assertEqual(
            'OK',
            self.pb.show('node')[5]['status'],
            'Backup STATUS should be "OK"')

        self.assertEqual(
            'ERROR',
            self.pb.show('node')[6]['status'],
            'Backup STATUS should be "ERROR"')

        self.assertEqual(
            'OK',
            self.pb.show('node')[7]['status'],
            'Backup STATUS should be "OK"')

        # corruption victim
        self.assertEqual(
            'CORRUPT',
            self.pb.show('node')[8]['status'],
            'Backup STATUS should be "CORRUPT"')

        # orphaned child
        self.assertEqual(
            'ORPHAN',
            self.pb.show('node')[9]['status'],
            'Backup STATUS should be "ORPHAN"')

        # orphaned child
        self.assertEqual(
            'ORPHAN',
            self.pb.show('node')[10]['status'],
            'Backup STATUS should be "ORPHAN"')

        # next FULL
        self.assertEqual(
            'OK',
            self.pb.show('node')[11]['status'],
            'Backup STATUS should be "OK"')

        # next DELTA
        self.assertEqual(
            'OK',
            self.pb.show('node')[12]['status'],
            'Backup STATUS should be "OK"')

        node.cleanup()

    # Skipped, because backups from the future are invalid.
    # This cause a "ERROR: Can't assign backup_id, there is already a backup in future"
    # now (PBCKP-259). We can conduct such a test again when we
    # untie 'backup_id' from 'start_time'
    @unittest.skip("skip")
    def test_restore_backup_from_future(self):
        """more complex test_restore_chain()"""
        backup_dir = self.backup_dir
        if not backup_dir.is_file_based:
            self.skipTest("test uses 'rename' in backup directory")

        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Take FULL
        self.pb.backup_node('node', node)

        node.pgbench_init(scale=5)
        # pgbench = node.pgbench(options=['-T', '20', '-c', '2'])
        # pgbench.wait()

        # Take PAGE from future
        backup_id = self.pb.backup_node('node', node, backup_type='page')

        with self.modify_backup_control(backup_dir, 'node', backup_id) as cf:
            cf.data += "\nstart-time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                        datetime.now() + timedelta(days=3))

        # rename directory
        new_id = self.pb.show('node')[1]['id']

        os.rename(
            os.path.join(backup_dir, 'backups', 'node', backup_id),
            os.path.join(backup_dir, 'backups', 'node', new_id))

        pgbench = node.pgbench(options=['-T', '7', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        backup_id = self.pb.backup_node('node', node, backup_type='page')
        pgdata = self.pgdata_content(node.data_dir)

        node.cleanup()
        self.pb.restore_node('node', node=node, backup_id=backup_id)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_restore_target_immediate_stream(self):
        """
        correct handling of immediate recovery target
        for STREAM backups
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # Take FULL
        self.pb.backup_node('node', node, options=['--stream'])

        # Take delta
        backup_id = self.pb.backup_node('node', node,
            backup_type='delta', options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        # TODO update test
        if self.pg_config_version >= self.version_to_num('12.0'):
            recovery_conf = os.path.join(node.data_dir, 'postgresql.auto.conf')
            with open(recovery_conf, 'r') as f:
                print(f.read())
        else:
            recovery_conf = os.path.join(node.data_dir, 'recovery.conf')

        # restore delta backup
        node.cleanup()
        self.pb.restore_node('node', node, options=['--immediate'])

        self.assertTrue(
            os.path.isfile(recovery_conf),
            "File {0} do not exists".format(recovery_conf))

        # restore delta backup
        node.cleanup()
        self.pb.restore_node('node', node, options=['--recovery-target=immediate'])

        self.assertTrue(
            os.path.isfile(recovery_conf),
            "File {0} do not exists".format(recovery_conf))

    # @unittest.skip("skip")
    def test_restore_target_immediate_archive(self):
        """
        correct handling of immediate recovery target
        for ARCHIVE backups
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Take FULL
        self.pb.backup_node('node', node)

        # Take delta
        backup_id = self.pb.backup_node('node', node,
            backup_type='delta')

        pgdata = self.pgdata_content(node.data_dir)

        # TODO update test
        if self.pg_config_version >= self.version_to_num('12.0'):
            recovery_conf = os.path.join(node.data_dir, 'postgresql.auto.conf')
            with open(recovery_conf, 'r') as f:
                print(f.read())
        else:
            recovery_conf = os.path.join(node.data_dir, 'recovery.conf')

        # restore page backup
        node.cleanup()
        self.pb.restore_node('node', node, options=['--immediate'])

        # For archive backup with immediate recovery target
        # recovery.conf is mandatory
        with open(recovery_conf, 'r') as f:
            self.assertIn("recovery_target = 'immediate'", f.read())

        # restore page backup
        node.cleanup()
        self.pb.restore_node('node', node, options=['--recovery-target=immediate'])

        # For archive backup with immediate recovery target
        # recovery.conf is mandatory
        with open(recovery_conf, 'r') as f:
            self.assertIn("recovery_target = 'immediate'", f.read())

    # Skipped, because default recovery_target_timeline is 'current'
    # Before PBCKP-598 the --recovery-target=latest' option did not work and this test allways passed
    @unittest.skip("skip")
    def test_restore_target_latest_archive(self):
        """
        make sure that recovery_target 'latest'
        is default recovery target
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Take FULL
        self.pb.backup_node('node', node)

        if self.pg_config_version >= self.version_to_num('12.0'):
            recovery_conf = os.path.join(node.data_dir, 'postgresql.auto.conf')
        else:
            recovery_conf = os.path.join(node.data_dir, 'recovery.conf')

        # restore
        node.cleanup()
        self.pb.restore_node('node', node=node)

        # hash_1 = hashlib.md5(
        #     open(recovery_conf, 'rb').read()).hexdigest()

        with open(recovery_conf, 'r') as f:
            content_1 = ''
            while True:
                line = f.readline()

                if not line:
                    break
                if line.startswith("#"):
                    continue
                content_1 += line

        node.cleanup()
        self.pb.restore_node('node', node=node, options=['--recovery-target=latest'])

        # hash_2 = hashlib.md5(
        #     open(recovery_conf, 'rb').read()).hexdigest()

        with open(recovery_conf, 'r') as f:
            content_2 = ''
            while True:
                line = f.readline()

                if not line:
                    break
                if line.startswith("#"):
                    continue
                content_2 += line

        self.assertEqual(content_1, content_2)

    # @unittest.skip("skip")
    def test_restore_target_new_options(self):
        """
        check that new --recovery-target-*
        options are working correctly
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Take FULL
        self.pb.backup_node('node', node)

        # TODO update test
        if self.pg_config_version >= self.version_to_num('12.0'):
            recovery_conf = os.path.join(node.data_dir, 'postgresql.auto.conf')
            with open(recovery_conf, 'r') as f:
                print(f.read())
        else:
            recovery_conf = os.path.join(node.data_dir, 'recovery.conf')

        node.pgbench_init(scale=2)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        node.safe_psql(
            "postgres",
            "CREATE TABLE tbl0005 (a text)")

        node.safe_psql(
            "postgres", "select pg_create_restore_point('savepoint')")

        target_name = 'savepoint'

        # in python-3.6+ it can be ...now()..astimezone()...
        target_time = datetime.utcnow().replace(tzinfo=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S.%f%z")
        with node.connect("postgres") as con:
            res = con.execute(
                "INSERT INTO tbl0005 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]

        with node.connect("postgres") as con:
            con.execute("INSERT INTO tbl0005 VALUES (1)")
            con.commit()
            res = con.execute("SELECT pg_current_wal_lsn()")

            con.commit()
            con.execute("INSERT INTO tbl0005 VALUES (2)")
            con.commit()
            xlogid, xrecoff = res[0][0].split('/')
            xrecoff = hex(int(xrecoff, 16) + 1)[2:]
            target_lsn = "{0}/{1}".format(xlogid, xrecoff)

        # Restore with recovery target time
        node.cleanup()
        self.pb.restore_node('node', node,
            options=[
                '--recovery-target-time={0}'.format(target_time),
                "--recovery-target-action=promote",
                '--recovery-target-timeline=1',
                ])

        with open(recovery_conf, 'r') as f:
            recovery_conf_content = f.read()

        self.assertIn(
            "recovery_target_time = '{0}'".format(target_time),
            recovery_conf_content)

        self.assertIn(
            "recovery_target_action = 'promote'",
            recovery_conf_content)

        self.assertIn(
            "recovery_target_timeline = '1'",
            recovery_conf_content)

        node.slow_start()

        # Restore with recovery target xid
        node.cleanup()
        self.pb.restore_node('node', node,
            options=[
                '--recovery-target-xid={0}'.format(target_xid),
                "--recovery-target-action=promote",
                '--recovery-target-timeline=1',
                ])

        with open(recovery_conf, 'r') as f:
            recovery_conf_content = f.read()

        self.assertIn(
            "recovery_target_xid = '{0}'".format(target_xid),
            recovery_conf_content)

        self.assertIn(
            "recovery_target_action = 'promote'",
            recovery_conf_content)

        self.assertIn(
            "recovery_target_timeline = '1'",
            recovery_conf_content)

        node.slow_start()

        # Restore with recovery target name
        node.cleanup()
        self.pb.restore_node('node', node,
            options=[
                '--recovery-target-name={0}'.format(target_name),
                "--recovery-target-action=promote",
                '--recovery-target-timeline=1',
                ])

        with open(recovery_conf, 'r') as f:
            recovery_conf_content = f.read()

        self.assertIn(
            "recovery_target_name = '{0}'".format(target_name),
            recovery_conf_content)

        self.assertIn(
            "recovery_target_action = 'promote'",
            recovery_conf_content)

        self.assertIn(
            "recovery_target_timeline = '1'",
            recovery_conf_content)

        node.slow_start()

        # Restore with recovery target lsn

        node.cleanup()
        self.pb.restore_node('node', node,
            options=[
                '--recovery-target-lsn={0}'.format(target_lsn),
                "--recovery-target-action=promote",
                '--recovery-target-timeline=1',
                ])

        with open(recovery_conf, 'r') as f:
            recovery_conf_content = f.read()

        self.assertIn(
            "recovery_target_lsn = '{0}'".format(target_lsn),
            recovery_conf_content)

        self.assertIn(
            "recovery_target_action = 'promote'",
            recovery_conf_content)

        self.assertIn(
            "recovery_target_timeline = '1'",
            recovery_conf_content)

        node.slow_start()

    # @unittest.skip("skip")
    def test_smart_restore(self):
        """
        make node, create database, take full backup, drop database,
        take incremental backup and restore it,
        make sure that files from dropped database are not
        copied during restore
        https://github.com/postgrespro/pg_probackup/issues/63
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # create database
        node.safe_psql(
            "postgres",
            "CREATE DATABASE testdb")

        # take FULL backup
        full_id = self.pb.backup_node('node', node)

        # drop database
        node.safe_psql(
            "postgres",
            "DROP DATABASE testdb")

        # take PAGE backup
        page_id = self.pb.backup_node('node', node, backup_type='page')

        # restore PAGE backup
        node.cleanup()
        self.pb.restore_node('node', node, backup_id=page_id,
            options=['--no-validate', '--log-level-file=VERBOSE'])

        logfile_content = self.read_pb_log()

        # get delta between FULL and PAGE filelists
        filelist_full = self.get_backup_filelist(backup_dir, 'node', full_id)

        filelist_page = self.get_backup_filelist(backup_dir, 'node', page_id)

        filelist_diff = self.get_backup_filelist_diff(
            filelist_full, filelist_page)

        self.assertTrue(filelist_diff, 'There should be deleted files')
        for file in filelist_diff:
            self.assertNotIn(file, logfile_content)

    # @unittest.skip("skip")
    def test_pg_11_group_access(self):
        """
        test group access for PG >= 11
        """
        if self.pg_config_version < self.version_to_num('11.0'):
            self.skipTest('You need PostgreSQL >= 11 for this test')

        node = self.pg_node.make_simple('node',
            set_replication=True,
            initdb_params=['--allow-group-access'])

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # take FULL backup
        self.pb.backup_node('node', node, options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        # restore backup
        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored)

        # compare pgdata permissions
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    @needs_gdb
    def test_restore_concurrent_drop_table(self):
        """"""

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=1)

        # FULL backup
        self.pb.backup_node('node', node,
            options=['--stream', '--compress'])

        # DELTA backup
        gdb = self.pb.backup_node('node', node, backup_type='delta',
            options=['--stream', '--compress', '--no-validate'],
            gdb=True)

        gdb.set_breakpoint('backup_data_file')
        gdb.run_until_break()

        node.safe_psql(
            'postgres',
            'DROP TABLE pgbench_accounts')

        # do checkpoint to guarantee filenode removal
        node.safe_psql(
            'postgres',
            'CHECKPOINT')

        gdb.continue_execution_until_exit()

        pgdata = self.pgdata_content(node.data_dir)
        node.cleanup()

        self.pb.restore_node('node', node, options=['--no-validate'])

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_lost_non_data_file(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # FULL backup
        backup_id = self.pb.backup_node('node', node, options=['--stream'])

        self.remove_backup_file(backup_dir, 'node', backup_id,
                                'database/postgresql.auto.conf')

        node.cleanup()

        self.pb.restore_node('node', node=node, options=['--no-validate'],
                          expect_error="because of non-data file dissapearance")
        self.assertMessage(contains='No such file')
        self.assertMessage(contains='ERROR: Backup files restoring failed')

    def test_partial_restore_exclude(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        for i in range(1, 10, 1):
            node.safe_psql(
                'postgres',
                'CREATE database db{0}'.format(i))

        db_list_raw = node.safe_psql(
            'postgres',
            'SELECT to_json(a) '
            'FROM (SELECT oid, datname FROM pg_database) a').decode('utf-8').rstrip()

        db_list_splitted = db_list_raw.splitlines()

        db_list = {}
        for line in db_list_splitted:
            line = json.loads(line)
            db_list[line['datname']] = line['oid']

        # FULL backup
        backup_id = self.pb.backup_node('node', node)
        pgdata = self.pgdata_content(node.data_dir)

        # restore FULL backup
        node_restored_1 = self.pg_node.make_simple('node_restored_1')
        node_restored_1.cleanup()

        self.pb.restore_node('node', node=node_restored_1,
                          options=["--db-include=db1", "--db-exclude=db2"],
                          expect_error="because of 'db-exclude' and 'db-include'")
        self.assertMessage(contains="ERROR: You cannot specify '--db-include' "
                                    "and '--db-exclude' together")

        self.pb.restore_node('node', node_restored_1)

        pgdata_restored_1 = self.pgdata_content(node_restored_1.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored_1)

        db1_path = os.path.join(
            node_restored_1.data_dir, 'base', db_list['db1'])
        db5_path = os.path.join(
            node_restored_1.data_dir, 'base', db_list['db5'])

        self.truncate_every_file_in_dir(db1_path)
        self.truncate_every_file_in_dir(db5_path)
        pgdata_restored_1 = self.pgdata_content(node_restored_1.data_dir)

        node_restored_2 = self.pg_node.make_simple('node_restored_2')
        node_restored_2.cleanup()

        self.pb.restore_node('node',
            node_restored_2, options=[
                "--db-exclude=db1",
                "--db-exclude=db5"])

        pgdata_restored_2 = self.pgdata_content(node_restored_2.data_dir)
        self.compare_pgdata(pgdata_restored_1, pgdata_restored_2)

        node_restored_2.set_auto_conf({'port': node_restored_2.port})

        node_restored_2.slow_start()

        node_restored_2.safe_psql(
            'postgres',
            'select 1')

        try:
            node_restored_2.safe_psql(
                'db1',
                'select 1')
        except QueryException as e:
            self.assertIn('FATAL', e.message)

        try:
            node_restored_2.safe_psql(
                'db5',
                'select 1')
        except QueryException as e:
            self.assertIn('FATAL', e.message)

        with open(node_restored_2.pg_log_file, 'r') as f:
            output = f.read()

        self.assertNotIn('PANIC', output)

    def test_partial_restore_exclude_tablespace(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        cat_version = node.get_control_data()["Catalog version number"]
        version_specific_dir = 'PG_' + node.major_version_str + '_' + cat_version

        # PG_10_201707211
        # pg_tblspc/33172/PG_9.5_201510051/16386/

        self.create_tblspace_in_node(node, 'somedata')

        node_tablespace = self.get_tblspace_path(node, 'somedata')

        tbl_oid = node.safe_psql(
            'postgres',
            "SELECT oid "
            "FROM pg_tablespace "
            "WHERE spcname = 'somedata'").decode('utf-8').rstrip()

        for i in range(1, 10, 1):
            node.safe_psql(
                'postgres',
                'CREATE database db{0} tablespace somedata'.format(i))

        db_list_raw = node.safe_psql(
            'postgres',
            'SELECT to_json(a) '
            'FROM (SELECT oid, datname FROM pg_database) a').decode('utf-8').rstrip()

        db_list_splitted = db_list_raw.splitlines()

        db_list = {}
        for line in db_list_splitted:
            line = json.loads(line)
            db_list[line['datname']] = line['oid']

        # FULL backup
        backup_id = self.pb.backup_node('node', node)
        pgdata = self.pgdata_content(node.data_dir)

        # restore FULL backup
        node_restored_1 = self.pg_node.make_simple('node_restored_1')
        node_restored_1.cleanup()

        node1_tablespace = self.get_tblspace_path(node_restored_1, 'somedata')

        self.pb.restore_node('node',
            node_restored_1, options=[
                "-T", "{0}={1}".format(
                    node_tablespace, node1_tablespace)])

        pgdata_restored_1 = self.pgdata_content(node_restored_1.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored_1)

        # truncate every db
        for db in db_list:
            # with exception below
            if db in ['db1', 'db5']:
                self.truncate_every_file_in_dir(
                    os.path.join(
                        node_restored_1.data_dir, 'pg_tblspc',
                        tbl_oid, version_specific_dir, db_list[db]))

        pgdata_restored_1 = self.pgdata_content(node_restored_1.data_dir)

        node_restored_2 = self.pg_node.make_simple('node_restored_2')
        node_restored_2.cleanup()
        node2_tablespace = self.get_tblspace_path(node_restored_2, 'somedata')

        self.pb.restore_node('node',
            node_restored_2, options=[
                "--db-exclude=db1",
                "--db-exclude=db5",
                "-T", "{0}={1}".format(
                    node_tablespace, node2_tablespace)])

        pgdata_restored_2 = self.pgdata_content(node_restored_2.data_dir)
        self.compare_pgdata(pgdata_restored_1, pgdata_restored_2)

        node_restored_2.set_auto_conf({'port': node_restored_2.port})

        node_restored_2.slow_start()

        node_restored_2.safe_psql(
            'postgres',
            'select 1')

        try:
            node_restored_2.safe_psql(
                'db1',
                'select 1')
        except QueryException as e:
            self.assertIn('FATAL', e.message)

        try:
            node_restored_2.safe_psql(
                'db5',
                'select 1')
        except QueryException as e:
            self.assertIn('FATAL', e.message)

        with open(node_restored_2.pg_log_file, 'r') as f:
            output = f.read()

        self.assertNotIn('PANIC', output)

    def test_partial_restore_include(self):
        """
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        for i in range(1, 10, 1):
            node.safe_psql(
                'postgres',
                'CREATE database db{0}'.format(i))

        db_list_raw = node.safe_psql(
            'postgres',
            'SELECT to_json(a) '
            'FROM (SELECT oid, datname FROM pg_database) a').decode('utf-8').rstrip()

        db_list_splitted = db_list_raw.splitlines()

        db_list = {}
        for line in db_list_splitted:
            line = json.loads(line)
            db_list[line['datname']] = line['oid']

        # FULL backup
        backup_id = self.pb.backup_node('node', node)
        pgdata = self.pgdata_content(node.data_dir)

        # restore FULL backup
        node_restored_1 = self.pg_node.make_simple('node_restored_1')
        node_restored_1.cleanup()

        self.pb.restore_node('node', node=node_restored_1,
                          options=["--db-include=db1", "--db-exclude=db2"],
                          expect_error="because of 'db-exclude' and 'db-include'")
        self.assertMessage(contains="ERROR: You cannot specify '--db-include' "
                                    "and '--db-exclude' together")

        self.pb.restore_node('node', node_restored_1)

        pgdata_restored_1 = self.pgdata_content(node_restored_1.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored_1)

        # truncate every db
        for db in db_list:
            # with exception below
            if db in ['template0', 'template1', 'postgres', 'db1', 'db5']:
                continue
            self.truncate_every_file_in_dir(
                os.path.join(
                    node_restored_1.data_dir, 'base', db_list[db]))

        pgdata_restored_1 = self.pgdata_content(node_restored_1.data_dir)

        node_restored_2 = self.pg_node.make_simple('node_restored_2')
        node_restored_2.cleanup()

        self.pb.restore_node('node',
            node_restored_2, options=[
                "--db-include=db1",
                "--db-include=db5",
                "--db-include=postgres"])

        pgdata_restored_2 = self.pgdata_content(node_restored_2.data_dir)
        self.compare_pgdata(pgdata_restored_1, pgdata_restored_2)

        node_restored_2.set_auto_conf({'port': node_restored_2.port})
        node_restored_2.slow_start()

        node_restored_2.safe_psql(
            'db1',
            'select 1')

        node_restored_2.safe_psql(
            'db5',
            'select 1')

        node_restored_2.safe_psql(
            'template1',
            'select 1')

        try:
            node_restored_2.safe_psql(
                'db2',
                'select 1')
        except QueryException as e:
            self.assertIn('FATAL', e.message)

        try:
            node_restored_2.safe_psql(
                'db10',
                'select 1')
        except QueryException as e:
            self.assertIn('FATAL', e.message)

        with open(node_restored_2.pg_log_file, 'r') as f:
            output = f.read()

        self.assertNotIn('PANIC', output)

    def test_partial_restore_backward_compatibility_1(self):
        """
        old binary should be of version < 2.2.0
        """
        if not self.probackup_old_path:
            self.skipTest("You must specify PGPROBACKUPBIN_OLD"
                          " for run this test")
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init(old_binary=True)
        self.pb.add_instance('node', node, old_binary=True)

        node.slow_start()

        # create databases
        for i in range(1, 10, 1):
            node.safe_psql(
                'postgres',
                'CREATE database db{0}'.format(i))

        # FULL backup with old binary, without partial restore support
        backup_id = self.pb.backup_node('node', node,
            old_binary=True, options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node=node_restored,
                          options=["--db-exclude=db5"],
                          expect_error="because backup do not support partial restore")
        self.assertMessage(contains=f"ERROR: Backup {backup_id} doesn't contain "
                                    "a database_map, partial restore is impossible")

        self.pb.restore_node('node', node=node_restored)

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # incremental backup with partial restore support
        for i in range(11, 15, 1):
            node.safe_psql(
                'postgres',
                'CREATE database db{0}'.format(i))

        # get db list
        db_list_raw = node.safe_psql(
            'postgres',
            'SELECT to_json(a) '
            'FROM (SELECT oid, datname FROM pg_database) a').rstrip()
        db_list_splitted = db_list_raw.splitlines()
        db_list = {}
        for line in db_list_splitted:
            line = json.loads(line)
            db_list[line['datname']] = line['oid']

        backup_id = self.pb.backup_node('node', node,
            backup_type='delta', options=['--stream'])

        # get etalon
        node_restored.cleanup()
        self.pb.restore_node('node', node=node_restored)
        self.truncate_every_file_in_dir(
                os.path.join(
                    node_restored.data_dir, 'base', db_list['db5']))
        self.truncate_every_file_in_dir(
                os.path.join(
                    node_restored.data_dir, 'base', db_list['db14']))
        pgdata_restored = self.pgdata_content(node_restored.data_dir)

        # get new node
        node_restored_1 = self.pg_node.make_simple('node_restored_1')
        node_restored_1.cleanup()

        self.pb.restore_node('node',
                node_restored_1, options=[
                    "--db-exclude=db5",
                    "--db-exclude=db14"])

        pgdata_restored_1 = self.pgdata_content(node_restored_1.data_dir)

        self.compare_pgdata(pgdata_restored, pgdata_restored_1)

    def test_partial_restore_backward_compatibility_merge(self):
        """
        old binary should be of version < 2.2.0
        """
        if not self.probackup_old_path:
            self.skipTest("You must specify PGPROBACKUPBIN_OLD"
                          " for run this test")
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init(old_binary=True)
        self.pb.add_instance('node', node, old_binary=True)

        node.slow_start()

        # create databases
        for i in range(1, 10, 1):
            node.safe_psql(
                'postgres',
                'CREATE database db{0}'.format(i))

        # FULL backup with old binary, without partial restore support
        backup_id = self.pb.backup_node('node', node,
            old_binary=True, options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node=node_restored,
                          options=["--db-exclude=db5"],
                          expect_error="because backup do not support partial restore")
        self.assertMessage(contains=f"ERROR: Backup {backup_id} doesn't contain a database_map, "
                                    "partial restore is impossible.")

        self.pb.restore_node('node', node=node_restored)

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # incremental backup with partial restore support
        for i in range(11, 15, 1):
            node.safe_psql(
                'postgres',
                'CREATE database db{0}'.format(i))

        # get db list
        db_list_raw = node.safe_psql(
            'postgres',
            'SELECT to_json(a) '
            'FROM (SELECT oid, datname FROM pg_database) a').rstrip()
        db_list_splitted = db_list_raw.splitlines()
        db_list = {}
        for line in db_list_splitted:
            line = json.loads(line)
            db_list[line['datname']] = line['oid']

        backup_id = self.pb.backup_node('node', node,
            backup_type='delta', options=['--stream'])

        # get etalon
        node_restored.cleanup()
        self.pb.restore_node('node', node_restored)
        self.truncate_every_file_in_dir(
                os.path.join(
                    node_restored.data_dir, 'base', db_list['db5']))
        self.truncate_every_file_in_dir(
                os.path.join(
                    node_restored.data_dir, 'base', db_list['db14']))
        pgdata_restored = self.pgdata_content(node_restored.data_dir)

        # get new node
        node_restored_1 = self.pg_node.make_simple('node_restored_1')
        node_restored_1.cleanup()

        # merge
        self.pb.merge_backup('node', backup_id=backup_id)

        self.pb.restore_node('node',
                node_restored_1, options=[
                    "--db-exclude=db5",
                    "--db-exclude=db14"])
        pgdata_restored_1 = self.pgdata_content(node_restored_1.data_dir)

        self.compare_pgdata(pgdata_restored, pgdata_restored_1)

    def test_empty_and_mangled_database_map(self):
        """
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)

        node.slow_start()

        # create databases
        for i in range(1, 10, 1):
            node.safe_psql(
                'postgres',
                'CREATE database db{0}'.format(i))

        # FULL backup with database_map
        backup_id = self.pb.backup_node('node', node, options=['--stream'])
        pgdata = self.pgdata_content(node.data_dir)

        self.corrupt_backup_file(backup_dir, 'node', backup_id,
                               'database/database_map', truncate=0)

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored,
                          options=["--db-include=db1", '--no-validate'],
                          expect_error="because database_map is empty")
        self.assertMessage(contains=f"ERROR: Backup {backup_id} has empty or "
                                    "mangled database_map, partial restore "
                                    "is impossible")

        self.pb.restore_node('node', node_restored,
                          options=["--db-exclude=db1", '--no-validate'],
                          expect_error="because database_map is empty")
        self.assertMessage(contains=f"ERROR: Backup {backup_id} has empty or "
                                    "mangled database_map, partial restore "
                                    "is impossible")

        self.corrupt_backup_file(backup_dir, 'node', backup_id,
                               'database/database_map', overwrite=b'42')

        self.pb.restore_node('node', node_restored,
                          options=["--db-include=db1", '--no-validate'],
                          expect_error="because database_map is corrupted")
        self.assertMessage(contains='ERROR: backup_content.control file has '
                                    'invalid format in line 42')

        self.pb.restore_node('node', node_restored,
                          options=["--db-exclude=db1", '--no-validate'],
                          expect_error="because database_map is corrupted")
        self.assertMessage(contains='ERROR: backup_content.control file has '
                                    'invalid format in line 42')

        # check that simple restore is still possible
        self.pb.restore_node('node', node_restored, options=['--no-validate'])

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    def test_missing_database_map(self):
        """
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=self.ptrack)

        self.pb.init()
        self.pb.add_instance('node', node)

        node.slow_start()

        # create databases
        for i in range(1, 10, 1):
            node.safe_psql(
                'postgres',
                'CREATE database db{0}'.format(i))

        node.safe_psql(
            "postgres",
            "CREATE DATABASE backupdb")

        # PG < 15
        if self.pg_config_version >= 100000 and self.pg_config_version < 150000:
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
                "GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup; "
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
                "GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup; "
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

        if self.ptrack:
            # TODO why backup works without these grants ?
            #    'pg_ptrack_get_pagemapset(pg_lsn)',
            #    'pg_ptrack_control_lsn()',
            # because PUBLIC
            node.safe_psql(
                "backupdb",
                "CREATE SCHEMA ptrack; "
                "GRANT USAGE ON SCHEMA ptrack TO backup; "
                "CREATE EXTENSION ptrack WITH SCHEMA ptrack")

        if ProbackupTest.pgpro:

            node.safe_psql(
                "backupdb",
                "GRANT EXECUTE ON FUNCTION pg_catalog.pgpro_version() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_catalog.pgpro_edition() TO backup;")

        # FULL backup without database_map
        backup_id = self.pb.backup_node('node', node, datname='backupdb',
            options=['--stream', "-U", "backup", '--log-level-file=verbose'])

        pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        # backup has missing database_map and that is legal
        self.pb.restore_node('node', node_restored,
                          options=["--db-exclude=db5", "--db-exclude=db9"],
                          expect_error="because user do not have pg_database access")
        self.assertMessage(contains=f"ERROR: Backup {backup_id} doesn't contain a database_map, "
                                    "partial restore is impossible.")

        self.pb.restore_node('node', node_restored,
                          options=["--db-include=db1"],
                          expect_error="because user do not have pg_database access")
        self.assertMessage(contains=f"ERROR: Backup {backup_id} doesn't contain a database_map, "
                                    "partial restore is impossible.")

        # check that simple restore is still possible
        self.pb.restore_node('node', node_restored)

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_stream_restore_command_option(self):
        """
        correct handling of restore command options
        when restoring STREAM backup

        1. Restore STREAM backup with --restore-command only
        parameter, check that PostgreSQL recovery uses
        restore_command to obtain WAL from archive.

        2. Restore STREAM backup wuth --restore-command
        as replica, check that PostgreSQL recovery uses
        restore_command to obtain WAL from archive.
        """
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={'max_wal_size': '32MB'})

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # TODO update test
        if self.pg_config_version >= self.version_to_num('12.0'):
            recovery_conf = os.path.join(node.data_dir, 'postgresql.auto.conf')
            with open(recovery_conf, 'r') as f:
                print(f.read())
        else:
            recovery_conf = os.path.join(node.data_dir, 'recovery.conf')

        # Take FULL
        self.pb.backup_node('node', node, options=['--stream'])

        node.pgbench_init(scale=5)

        node.safe_psql(
            'postgres',
            'create table t1()')

        # restore backup
        node.cleanup()
        shutil.rmtree(os.path.join(node.logs_dir))

        restore_cmd = self.get_restore_command(backup_dir, 'node')

        self.pb.restore_node('node', node,
            options=[
                '--restore-command={0}'.format(restore_cmd)])

        self.assertTrue(
            os.path.isfile(recovery_conf),
            "File '{0}' do not exists".format(recovery_conf))

        if self.pg_config_version >= self.version_to_num('12.0'):
            recovery_signal = os.path.join(node.data_dir, 'recovery.signal')
            self.assertTrue(
                os.path.isfile(recovery_signal),
                "File '{0}' do not exists".format(recovery_signal))

        node.slow_start()

        node.safe_psql(
            'postgres',
            'select * from t1')

        timeline_id = node.safe_psql(
            'postgres',
            'select timeline_id from pg_control_checkpoint()').decode('utf-8').rstrip()

        self.assertEqual('2', timeline_id)

    # @unittest.skip("skip")
    def test_restore_primary_conninfo(self):
        """
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # Take FULL
        self.pb.backup_node('node', node, options=['--stream'])

        node.pgbench_init(scale=1)

        #primary_conninfo = 'host=192.168.1.50 port=5432 user=foo password=foopass'

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()
        str_conninfo='host=192.168.1.50 port=5432 user=foo password=foopass'

        self.pb.restore_node('node', replica,
            options=['-R', '--primary-conninfo={0}'.format(str_conninfo)])

        if self.pg_config_version >= self.version_to_num('12.0'):
            standby_signal = os.path.join(replica.data_dir, 'standby.signal')
            self.assertTrue(
                os.path.isfile(standby_signal),
                "File '{0}' do not exists".format(standby_signal))

        # TODO update test
        if self.pg_config_version >= self.version_to_num('12.0'):
            recovery_conf = os.path.join(replica.data_dir, 'postgresql.auto.conf')
            with open(recovery_conf, 'r') as f:
                print(f.read())
        else:
            recovery_conf = os.path.join(replica.data_dir, 'recovery.conf')

        with open(os.path.join(replica.data_dir, recovery_conf), 'r') as f:
            recovery_conf_content = f.read()

        self.assertIn(str_conninfo, recovery_conf_content)

    # @unittest.skip("skip")
    def test_restore_primary_slot_info(self):
        """
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # Take FULL
        self.pb.backup_node('node', node, options=['--stream'])

        node.pgbench_init(scale=1)

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        node.safe_psql(
            "SELECT pg_create_physical_replication_slot('master_slot')")

        self.pb.restore_node('node', replica,
            options=['-R', '--primary-slot-name=master_slot'])

        replica.set_auto_conf({'port': replica.port})
        replica.set_auto_conf({'hot_standby': 'on'})

        if self.pg_config_version >= self.version_to_num('12.0'):
            standby_signal = os.path.join(replica.data_dir, 'standby.signal')
            self.assertTrue(
                os.path.isfile(standby_signal),
                "File '{0}' do not exists".format(standby_signal))

        replica.slow_start(replica=True)

    def test_issue_249(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/249
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            'postgres',
            'CREATE database db1')

        node.pgbench_init(scale=5)

        node.safe_psql(
            'postgres',
            'CREATE TABLE t1 as SELECT * from pgbench_accounts where aid > 200000 and aid < 450000')

        node.safe_psql(
            'postgres',
            'DELETE from pgbench_accounts where aid > 200000 and aid < 450000')

        node.safe_psql(
            'postgres',
            'select * from pgbench_accounts')

        # FULL backup
        self.pb.backup_node('node', node)

        node.safe_psql(
            'postgres',
            'INSERT INTO pgbench_accounts SELECT * FROM t1')

        # restore FULL backup
        node_restored_1 = self.pg_node.make_simple('node_restored_1')
        node_restored_1.cleanup()

        self.pb.restore_node('node',
            node_restored_1, options=["--db-include=db1"])

        node_restored_1.set_auto_conf({'port': node_restored_1.port, 'hot_standby': 'off'})

        node_restored_1.slow_start()

        node_restored_1.safe_psql(
            'db1',
            'select 1')

        try:
            node_restored_1.safe_psql(
                'postgres',
                'select 1')
        except QueryException as e:
            self.assertIn('FATAL', e.message)

    def test_pg_12_probackup_recovery_conf_compatibility(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/249

        pg_probackup version must be 12 or greater
        """
        if not self.probackup_old_path:
            self.skipTest("You must specify PGPROBACKUPBIN_OLD"
                          " for run this test")
        if self.pg_config_version < self.version_to_num('12.0'):
           self.skipTest('You need PostgreSQL >= 12 for this test')

        if self.version_to_num(self.old_probackup_version) >= self.version_to_num('2.4.5'):
            self.assertTrue(False, 'You need pg_probackup < 2.4.5 for this test')

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL backup
        self.pb.backup_node('node', node, old_binary=True)

        node.pgbench_init(scale=5)

        node.safe_psql(
            'postgres',
            'CREATE TABLE t1 as SELECT * from pgbench_accounts where aid > 200000 and aid < 450000')

        time = node.safe_psql(
            'SELECT current_timestamp(0)::timestamptz;').decode('utf-8').rstrip()

        node.safe_psql(
            'postgres',
            'DELETE from pgbench_accounts where aid > 200000 and aid < 450000')

        node.cleanup()

        self.pb.restore_node('node',node,
            options=[
                    "--recovery-target-time={0}".format(time),
                    "--recovery-target-action=promote"],
            old_binary=True)

        node.slow_start()

        self.pb.backup_node('node', node, old_binary=True)

        node.pgbench_init(scale=5)

        xid = node.safe_psql(
            'SELECT txid_current()').decode('utf-8').rstrip()
        node.pgbench_init(scale=1)

        node.cleanup()

        self.pb.restore_node('node',node,
            options=[
                    "--recovery-target-xid={0}".format(xid),
                    "--recovery-target-action=promote"])

        node.slow_start()

    def test_drop_postgresql_auto_conf(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/249

        pg_probackup version must be 12 or greater
        """

        if self.pg_config_version < self.version_to_num('12.0'):
           self.skipTest('You need PostgreSQL >= 12 for this test')

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL backup
        self.pb.backup_node('node', node)

        # drop postgresql.auto.conf
        auto_path = os.path.join(node.data_dir, "postgresql.auto.conf")
        os.remove(auto_path)

        self.pb.backup_node('node', node, backup_type='page')

        node.cleanup()

        self.pb.restore_node('node',node,
            options=[
                    "--recovery-target=latest",
                    "--recovery-target-action=promote"])

        node.slow_start()

        self.assertTrue(os.path.exists(auto_path))

    def test_truncate_postgresql_auto_conf(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/249

        pg_probackup version must be 12 or greater
        """

        if self.pg_config_version < self.version_to_num('12.0'):
           self.skipTest('You need PostgreSQL >= 12 for this test')

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL backup
        self.pb.backup_node('node', node)

        # truncate postgresql.auto.conf
        auto_path = os.path.join(node.data_dir, "postgresql.auto.conf")
        with open(auto_path, "w+") as f:
            f.truncate()

        self.pb.backup_node('node', node, backup_type='page')

        node.cleanup()

        self.pb.restore_node('node',node,
            options=[
                    "--recovery-target=latest",
                    "--recovery-target-action=promote"])
        node.slow_start()

        self.assertTrue(os.path.exists(auto_path))

    # @unittest.skip("skip")
    @needs_gdb
    def test_concurrent_restore(self):
        """"""

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=1)

        # FULL backup
        self.pb.backup_node('node', node,
            options=['--stream', '--compress'])

        pgbench = node.pgbench(options=['-T', '7', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # DELTA backup
        self.pb.backup_node('node', node, backup_type='delta',
            options=['--stream', '--compress', '--no-validate'])

        pgdata1 = self.pgdata_content(node.data_dir)

        node_restored = self.pg_node.make_simple('node_restored')

        node.cleanup()
        node_restored.cleanup()

        gdb = self.pb.restore_node('node', node, options=['--no-validate'], gdb=True)

        gdb.set_breakpoint('restore_data_file')
        gdb.run_until_break()

        self.pb.restore_node('node', node_restored, options=['--no-validate'])

        gdb.continue_execution_until_exit()

        pgdata2 = self.pgdata_content(node.data_dir)
        pgdata3 = self.pgdata_content(node_restored.data_dir)

        self.compare_pgdata(pgdata1, pgdata2)
        self.compare_pgdata(pgdata2, pgdata3)


    # @unittest.skip("skip")
    def test_restore_with_waldir(self):
        """recovery using tablespace-mapping option and page backup"""
        node = self.pg_node.make_simple('node')

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        with node.connect("postgres") as con:
            con.execute(
                "CREATE TABLE tbl AS SELECT * "
                "FROM generate_series(0,3) AS integer")
            con.commit()

        # Full backup
        backup_id = self.pb.backup_node('node', node)

        node.stop()
        node.cleanup()

        # Create waldir
        waldir_path = os.path.join(node.base_dir, "waldir")
        os.makedirs(waldir_path)

        # Test recovery from latest
        restore_result = self.pb.restore_node('node', node,
                options=[
                    "-X", "%s" % (waldir_path)])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))
        node.slow_start()

        count = node.execute("postgres", "SELECT count(*) FROM tbl")
        self.assertEqual(count[0][0], 4)

        # check pg_wal is symlink
        if node.major_version >= 10:
            wal_path=os.path.join(node.data_dir, "pg_wal")
        else:
            wal_path=os.path.join(node.data_dir, "pg_xlog")

        self.assertEqual(os.path.islink(wal_path), True)

    def test_restore_with_sync(self):
        """
        By default our tests use --no-sync to speed up.
        This test runs full backup and then `restore' both with fsync enabled.
        """
        node = self.pg_node.make_simple('node')
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.execute("postgres", "CREATE TABLE tbl AS SELECT i as id FROM generate_series(0,3) AS i")

        backup_id = self.pb.backup_node('node', node, options=["--stream", "-j", "10"], sync=True)

        node.stop()
        node.cleanup()

        restore_result = self.pb.restore_node('node', node, options=["-j", "10"], sync=True)
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

        node.slow_start()

        count = node.execute("postgres", "SELECT count(*) FROM tbl")
        self.assertEqual(count[0][0], 4)

    def test_restore_target_time(self):
        """
        Test that we can restore to the time which we list
        as a recovery time for a backup.
        """
        node = self.pg_node.make_simple('node')
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql("postgres", "CREATE TABLE table_1(i int)")
        node.safe_psql("postgres", "INSERT INTO table_1 values (42)")
        node.safe_psql("postgres", "select pg_create_restore_point('savepoint');")

        backup_id = self.pb.backup_node('node', node)
        node.safe_psql("postgres", "select txid_current();")

        node.cleanup()

        backup = self.pb.show('node', backup_id)
        target_time = backup['recovery-time']

        self.pb.restore_node('node', node, options=[f'--recovery-target-time={target_time}',
                                   '--recovery-target-action=promote',])

        node.slow_start()

        with node.connect("postgres") as con:
            res = con.execute("SELECT * from table_1")[0][0]
            self.assertEqual(42, res)

    def test_restore_after_failover(self):
        """
        PITR: Check that we are able to restore to a correct timeline by replaying
        the WALs even though the backup was made on a different timeline.

        Insert some data on master (D0). Take a backup. Failover to replica.
        Insert some more data (D1). Record this moment as a PITR target. Insert some more data (D2).
        Recover to PITR target. Make sure D1 exists, while D2 does not.

        JIRA: PBCKP-588
        """
        master = self.pg_node.make_simple('master', set_replication=True)
        self.pb.init()
        self.pb.add_instance('master', master)
        # Streaming is not enough. We need full WALs to restore to a point in time later than the backup itself
        self.pb.set_archiving('master', master)
        master.slow_start()

        self.pb.backup_node('master', master, backup_type='full', options=['--stream'])

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        master.safe_psql("SELECT pg_create_physical_replication_slot('master_slot')")

        self.pb.restore_node(
            'master', replica,
            options=['-R', '--primary-slot-name=master_slot'])

        replica.set_auto_conf({'port': replica.port})
        replica.set_auto_conf({'hot_standby': 'on'})

        if self.pg_config_version >= self.version_to_num('12.0'):
            standby_signal = os.path.join(replica.data_dir, 'standby.signal')
            self.assertTrue(
                os.path.isfile(standby_signal),
                f"File '{standby_signal}' does not exist")

        replica.slow_start(replica=True)
        with master.connect("postgres") as con:
            master_timeline = con.execute("SELECT timeline_id FROM pg_control_checkpoint()")[0][0]
            self.assertNotEqual(master_timeline, 0)

        # Now we have master<=>standby setup.
        master.safe_psql("postgres", "CREATE TABLE t1 (a int, b int)")
        master.safe_psql("postgres", "INSERT INTO t1 SELECT i/100, i/500 FROM generate_series(1,100000) s(i)")

        # Make a backup on timeline 1 with most of the data missing
        self.pb.backup_node('master', master, backup_type='full', options=['--stream'])

        # For debugging purpose it was useful to have an incomplete commit in WAL. Might not be needed anymore
        psql_path = testgres_utils.get_bin_path("psql")
        os.spawnlp(os.P_NOWAIT, psql_path, psql_path, "-p", str(master.port), "-h", master.host, "-d", "postgres",
                        "-X", "-A", "-t", "-q", "-c",
                        "INSERT INTO t1 SELECT i/100, i/500 FROM generate_series(1,1000000) s(i)"
                        )

        master.stop(["-m", "immediate"])
        sleep(1)
        replica.promote()

        with replica.connect("postgres") as con:
            replica_timeline = con.execute("SELECT min_recovery_end_timeline FROM pg_control_recovery()")[0][0]
        self.assertNotEqual(master_timeline, replica_timeline)

        # Add some more on timeline 2
        replica.safe_psql("postgres", "CREATE TABLE t2 (a int, b int)")
        replica.safe_psql("postgres", f"INSERT INTO t2 SELECT i/100, i/500 FROM generate_series(1,{MAGIC_COUNT}) s(i)")

        # Find out point-in-time where we would like to restore to
        with replica.connect("postgres") as con:
            restore_time = con.execute("SELECT now(), txid_current();")[0][0]

        # Break MAGIC_COUNT. An insert which should not be restored
        replica.safe_psql("postgres", "INSERT INTO t2 SELECT i/100, i/500 FROM generate_series(1,100000) s(i)")

        replica.safe_psql("postgres", "SELECT pg_switch_wal();")

        # Final restore. We expect to find only the data up to {restore_time} and nothing else
        node_restored = self.pg_node.make_simple("node_restored")
        node_restored.cleanup()
        self.pb.restore_node('master', node_restored, options=[
            '--no-validate',
            f'--recovery-target-time={restore_time}',
            f'--recovery-target-timeline={replica_timeline}', # As per ticket we do not parse WALs. User supplies timeline manually
            '--recovery-target-action=promote',
            '-j', '4',
        ])
        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()

        with node_restored.connect("postgres") as con:
            nrows = con.execute("SELECT COUNT(*) from t2")[0][0]
        self.assertEqual(MAGIC_COUNT, nrows)

    # @unittest.skip("skip")
    @needs_gdb
    def test_restore_issue_313(self):
        """
        Check that partially restored PostgreSQL instance cannot be started
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL backup
        backup_id = self.pb.backup_node('node', node)
        node.cleanup()

        count = 0
        filelist = self.get_backup_filelist(backup_dir, 'node', backup_id)
        for file in filelist:
            # count only nondata files
            if int(filelist[file]['is_datafile']) == 0 and \
                    filelist[file]['kind'] != 'dir' and \
                    file != 'database_map':
                count += 1

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()
        self.pb.restore_node('node', node_restored)

        gdb = self.pb.restore_node('node', node, gdb=True, options=['--progress'])
        gdb.verbose = False
        gdb.set_breakpoint('restore_non_data_file')
        gdb.run_until_break()
        gdb.continue_execution_until_break(count - 1)
        gdb.quit()

        # emulate the user or HA taking care of PG configuration
        for fname in os.listdir(node_restored.data_dir):
            if fname.endswith('.conf'):
                os.rename(
                    os.path.join(node_restored.data_dir, fname),
                    os.path.join(node.data_dir, fname))

        try:
            node.slow_start()
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because backup is not fully restored")
        except StartNodeException as e:
            self.assertIn(
                'Cannot start node',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        with open(os.path.join(node.logs_dir, 'postgresql.log'), 'r') as f:
            self.assertIn(
                "postgres: could not find the database system",
                f.read())


    # @unittest.skip("skip")
    def test_restore_to_latest_timeline(self):
        """recovery to latest timeline"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=2)

        before1 = node.table_checksum("pgbench_branches")
        backup_id = self.pb.backup_node('node', node)

        node.stop()
        node.cleanup()

        restore_result = self.pb.restore_node('node', node, options=["-j", "4"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

        node.slow_start()
        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            options=['-T', '10', '-c', '2', '--no-vacuum'])
        pgbench.wait()
        pgbench.stdout.close()

        before2 = node.table_checksum("pgbench_branches")
        self.pb.backup_node('node', node)

        node.stop()
        node.cleanup()
        # restore from first backup
        restore_result = self.pb.restore_node('node', node,
                options=[
                    "-j", "4", "--recovery-target-timeline=latest", "-i", backup_id]
            )
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))
        # check recovery_target_timeline option in the recovery_conf
        recovery_target_timeline = self.get_recovery_conf(node)["recovery_target_timeline"]
        self.assertEqual(recovery_target_timeline, "latest")
        # check recovery-target=latest option for compatibility with previous versions
        node.cleanup()
        restore_result = self.pb.restore_node('node', node,
                options=[
                    "-j", "4", "--recovery-target=latest", "-i", backup_id]
            )
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))
        # check recovery_target_timeline option in the recovery_conf
        recovery_target_timeline = self.get_recovery_conf(node)["recovery_target_timeline"]
        self.assertEqual(recovery_target_timeline, "latest")

        # start postgres and promote wal files to latest timeline
        node.slow_start()

        # check for the latest updates
        after = node.table_checksum("pgbench_branches")
        self.assertEqual(before2, after)

        # checking recovery_target_timeline=current is the default option
        if self.pg_config_version >= self.version_to_num('12.0'):
            node.stop()
            node.cleanup()

            # restore from first backup
            restore_result = self.pb.restore_node('node', node,
                    options=[
                        "-j", "4", "-i", backup_id]
                )
            self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

            # check recovery_target_timeline option in the recovery_conf
            recovery_target_timeline = self.get_recovery_conf(node)["recovery_target_timeline"]
            self.assertEqual(recovery_target_timeline, "current")

            # start postgres with current timeline
            node.slow_start()

            # check for the current updates
            after = node.table_checksum("pgbench_branches")
            self.assertEqual(before1, after)

################################################
#               dry-run
###############################################
    @unittest.skipUnless(fs_backup_class.is_file_based, "AccessPath check is always true on s3")
    def test_basic_dry_run_restore(self):
        """recovery dry-run """
        node = self.pg_node.make_simple('node')

        # check external directory with dry_run
        external_dir = os.path.join(self.test_path, 'somedirectory')
        os.mkdir(external_dir)

        new_external_dir=os.path.join(self.test_path, "restored_external_dir")
        # fill external directory with data
        f = open(os.path.join(external_dir, "very_important_external_file"), 'x')
        f.close()

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=2)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()
        backup_id = self.pb.backup_node('node', node, options=["--external-dirs={0}".format(external_dir)])

        node.stop()

        # check data absence
        restore_dir = os.path.join(self.test_path, "restored_dir")
        if fs_backup_class.is_file_based:  #AccessPath check is always true on s3
            dir_mode = os.stat(self.test_path).st_mode
            os.chmod(self.test_path, 0o500)

            # 1 - Test recovery from latest without permissions
            error_message = self.pb.restore_node('node', restore_dir=restore_dir,
                                                  options=["-j", "4",
                                                           "--external-mapping={0}={1}".format(external_dir, new_external_dir),
                                                           "--dry-run"], expect_error ='because of changed permissions')
            try:
                self.assertMessage(error_message, contains='ERROR: Check permissions')
            finally:
                # Cleanup
                os.chmod(self.test_path, dir_mode)

        instance_before = self.pgdata_content(self.backup_dir)
        # 2 - Test recovery from latest
        restore_result = self.pb.restore_node('node', restore_dir=restore_dir,
                                              options=["-j", "4",
                                                       "--external-mapping={0}={1}".format(external_dir, new_external_dir),
                                                       "--dry-run"])

        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed in dry-run mode".format(backup_id))

        instance_after = self.pgdata_content(self.backup_dir)
        pgdata_after = self.pgdata_content(restore_dir)

        self.compare_instance_dir(
            instance_before,
            instance_after
        )

        # check external directory absence
        self.assertFalse(os.path.exists(new_external_dir))

        self.assertFalse(os.path.exists(restore_dir))


    @unittest.skipUnless(fs_backup_class.is_file_based, "AccessPath check is always true on s3")
    def test_basic_dry_run_incremental_restore(self):
        """incremental recovery with system_id mismatch and --force flag in --dry-run mode"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=2)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()
        backup_id = self.pb.backup_node('node', node)

        node.stop()
        # check data absence
        restore_dir = os.path.join(self.test_path, "restored_dir")

        # 1 - recovery from latest
        restore_result = self.pb.restore_node('node',
                                              restore_dir=restore_dir,
                                              options=["-j", "4"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(backup_id))

        # Make some changes
        node.slow_start()

        node.pgbench(
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()
        backup_id = self.pb.backup_node('node', node, options=["--stream",
                                                               "-b DELTA"])
        node.stop()

        pg_probackup_conf = os.path.join(self.backup_dir, "backups/node/pg_probackup.conf")

        # make system_id mismatch
        with open(pg_probackup_conf, 'r') as file:
            data = file.read()

            match = re.search(r'(system-identifier)( = )([0-9]+)(\n)', data)
            if match:
                data = data.replace(match.group(3), '1111111111111111111')

        with open(pg_probackup_conf, 'w') as file:

            file.write(data)

        instance_before = self.pgdata_content(self.backup_dir)
        pgdata_before = self.pgdata_content(restore_dir)
        if fs_backup_class.is_file_based:  #AccessPath check is always true on s3
            # Access check suite if disk mounted as read_only
            dir_mode = os.stat(restore_dir).st_mode
            os.chmod(restore_dir, 0o500)

            # 2 - incremetal recovery from latest without permissions
            try:
                error_message = self.pb.restore_node('node',
                                                  restore_dir=restore_dir,
                                                  options=["-j", "4",
                                                           "--dry-run",
                                                           "--force",
                                                           "-I", "checksum"], expect_error='because of changed permissions')
                self.assertMessage(error_message, contains='ERROR: Check permissions')
            finally:
                # Cleanup
                os.chmod(restore_dir, dir_mode)

        self.pb.restore_node('node',
                             restore_dir=restore_dir,
                             options=["-j", "4",
                                      "--dry-run",
                                      "--force",
                                      "-I", "checksum"])
        instance_after = self.pgdata_content(self.backup_dir)
        pgdata_after = self.pgdata_content(restore_dir)

        self.compare_instance_dir(
            instance_before,
            instance_after
        )
        self.compare_pgdata(
            pgdata_before,
            pgdata_after
        )

        node.stop()
