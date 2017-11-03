import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
import subprocess
from datetime import datetime
import sys, time


module_name = 'restore'


class RestoreTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_restore_full_to_latest(self):
        """recovery to latest from full backup"""
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

        node.pgbench_init(scale=2)
        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()
        before = node.execute("postgres", "SELECT * FROM pgbench_branches")
        backup_id = self.backup_node(backup_dir, 'node', node)

        node.stop()
        node.cleanup()

        # 1 - Test recovery from latest
        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        # 2 - Test that recovery.conf was created
        recovery_conf = os.path.join(node.data_dir, "recovery.conf")
        self.assertEqual(os.path.isfile(recovery_conf), True)

        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        after = node.execute("postgres", "SELECT * FROM pgbench_branches")
        self.assertEqual(before, after)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_restore_full_page_to_latest(self):
        """recovery to latest from full + page backups"""
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

        node.pgbench_init(scale=2)

        self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        backup_id = self.backup_node(backup_dir, 'node', node, backup_type="page")

        before = node.execute("postgres", "SELECT * FROM pgbench_branches")

        node.stop()
        node.cleanup()

        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        after = node.execute("postgres", "SELECT * FROM pgbench_branches")
        self.assertEqual(before, after)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_restore_to_specific_timeline(self):
        """recovery to target timeline"""
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

        node.pgbench_init(scale=2)

        before = node.execute("postgres", "SELECT * FROM pgbench_branches")

        backup_id = self.backup_node(backup_dir, 'node', node)

        for key, value in node.get_control_data().items():
            print(key)

        target_tli = int(node.get_control_data()["Latest checkpoint's TimeLineID"])
        node.stop()
        node.cleanup()

        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        node.start(params={'-t':'10'})
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            options=['-T', '10', '-c', '2', '--no-vacuum'])
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(backup_dir, 'node', node)

        node.stop()
        node.cleanup()

        # Correct Backup must be choosen for restore
        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node, options=["-j", "4", "--timeline={0}".format(target_tli)]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        recovery_target_timeline = self.get_recovery_conf(node)["recovery_target_timeline"]
        self.assertEqual(int(recovery_target_timeline), target_tli)

        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        after = node.execute("postgres", "SELECT * FROM pgbench_branches")
        self.assertEqual(before, after)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_restore_to_time(self):
        """recovery to target time"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.append_conf("postgresql.auto.conf", "TimeZone = Europe/Moscow")
        node.start()

        node.pgbench_init(scale=2)
        before = node.execute("postgres", "SELECT * FROM pgbench_branches")

        backup_id = self.backup_node(backup_dir, 'node', node)

        target_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        node.stop()
        node.cleanup()

        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node, options=["-j", "4", '--time={0}'.format(target_time)]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        after = node.execute("postgres", "SELECT * FROM pgbench_branches")
        self.assertEqual(before, after)

        # Clean after yourself
        # self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_restore_to_xid_inclusive(self):
        """recovery to target xid"""
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

        node.pgbench_init(scale=2)
        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        backup_id = self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        before = node.safe_psql("postgres", "SELECT * FROM pgbench_branches")
        with node.connect("postgres") as con:
            res = con.execute("INSERT INTO tbl0005 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        node.stop()
        node.cleanup()

        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node, options=["-j", "4", '--xid={0}'.format(target_xid)]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        after = node.safe_psql("postgres", "SELECT * FROM pgbench_branches")
        self.assertEqual(before, after)
        self.assertEqual(len(node.execute("postgres", "SELECT * FROM tbl0005")), 1)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_restore_to_xid_not_inclusive(self):
        """recovery with target inclusive false"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on', 'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        node.pgbench_init(scale=2)
        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        backup_id = self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        before = node.execute("postgres", "SELECT * FROM pgbench_branches")
        with node.connect("postgres") as con:
            result = con.execute("INSERT INTO tbl0005 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = result[0][0]

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        node.stop()
        node.cleanup()

        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node,
                options=["-j", "4", '--xid={0}'.format(target_xid), "--inclusive=false"]),
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        after = node.execute("postgres", "SELECT * FROM pgbench_branches")
        self.assertEqual(before, after)
        self.assertEqual(len(node.execute("postgres", "SELECT * FROM tbl0005")), 0)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_restore_full_ptrack_archive(self):
        """recovery to latest from archive full+ptrack backups"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        node.pgbench_init(scale=2)

        self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        backup_id = self.backup_node(backup_dir, 'node', node, backup_type="ptrack")

        before = node.execute("postgres", "SELECT * FROM pgbench_branches")

        node.stop()
        node.cleanup()

        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        after = node.execute("postgres", "SELECT * FROM pgbench_branches")
        self.assertEqual(before, after)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_restore_ptrack(self):
        """recovery to latest from archive full+ptrack+ptrack backups"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        node.pgbench_init(scale=2)

        self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(backup_dir, 'node', node, backup_type="ptrack")

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        backup_id = self.backup_node(backup_dir, 'node', node, backup_type="ptrack")

        before = node.execute("postgres", "SELECT * FROM pgbench_branches")

        node.stop()
        node.cleanup()

        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        after = node.execute("postgres", "SELECT * FROM pgbench_branches")
        self.assertEqual(before, after)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_restore_full_ptrack_stream(self):
        """recovery in stream mode to latest from full + ptrack backups"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on', 'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        node.pgbench_init(scale=2)

        self.backup_node(backup_dir, 'node', node, options=["--stream"])

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        backup_id = self.backup_node(backup_dir, 'node', node, backup_type="ptrack", options=["--stream"])

        before = node.execute("postgres", "SELECT * FROM pgbench_branches")

        node.stop()
        node.cleanup()

        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        after = node.execute("postgres", "SELECT * FROM pgbench_branches")
        self.assertEqual(before, after)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_restore_full_ptrack_under_load(self):
        """recovery to latest from full + ptrack backups with loads when ptrack backup do"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on', 'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        node.pgbench_init(scale=2)

        self.backup_node(backup_dir, 'node', node)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "8"]
        )

        backup_id = self.backup_node(backup_dir, 'node', node, backup_type="ptrack", options=["--stream"])

        pgbench.wait()
        pgbench.stdout.close()

        bbalance = node.execute("postgres", "SELECT sum(bbalance) FROM pgbench_branches")
        delta = node.execute("postgres", "SELECT sum(delta) FROM pgbench_history")

        self.assertEqual(bbalance, delta)
        node.stop()
        node.cleanup()

        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        bbalance = node.execute("postgres", "SELECT sum(bbalance) FROM pgbench_branches")
        delta = node.execute("postgres", "SELECT sum(delta) FROM pgbench_history")
        self.assertEqual(bbalance, delta)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_restore_full_under_load_ptrack(self):
        """recovery to latest from full + page backups with loads when full backup do"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on', 'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # wal_segment_size = self.guc_wal_segment_size(node)
        node.pgbench_init(scale=2)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "8"]
        )

        self.backup_node(backup_dir, 'node', node)

        pgbench.wait()
        pgbench.stdout.close()

        backup_id = self.backup_node(backup_dir, 'node', node, backup_type="ptrack", options=["--stream"])

        bbalance = node.execute("postgres", "SELECT sum(bbalance) FROM pgbench_branches")
        delta = node.execute("postgres", "SELECT sum(delta) FROM pgbench_history")

        self.assertEqual(bbalance, delta)

        node.stop()
        node.cleanup()
        #self.wrong_wal_clean(node, wal_segment_size)

        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        bbalance = node.execute("postgres", "SELECT sum(bbalance) FROM pgbench_branches")
        delta = node.execute("postgres", "SELECT sum(delta) FROM pgbench_history")
        self.assertEqual(bbalance, delta)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_restore_with_tablespace_mapping_1(self):
        """recovery using tablespace-mapping option"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on', 'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

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

        backup_id = self.backup_node(backup_dir, 'node', node)
        self.assertEqual(self.show_pb(backup_dir, 'node')[0]['Status'], "OK")

        # 1 - Try to restore to existing directory
        node.stop()
        try:
            self.restore_node(backup_dir, 'node', node)
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because restore destionation is not empty.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(e.message,
                'ERROR: restore destination is not empty: "{0}"\n'.format(node.data_dir),
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # 2 - Try to restore to existing tablespace directory
        node.cleanup()
        try:
            self.restore_node(backup_dir, 'node', node)
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because restore tablespace destination is not empty.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(e.message,
                'ERROR: restore tablespace destination is not empty: "{0}"\n'.format(tblspc_path),
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # 3 - Restore using tablespace-mapping
        tblspc_path_new = os.path.join(node.base_dir, "tblspc_new")
        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node, options=["-T", "%s=%s" % (tblspc_path, tblspc_path_new)]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)
        result = node.execute("postgres", "SELECT id FROM test")
        self.assertEqual(result[0][0], 1)

        # 4 - Restore using tablespace-mapping using page backup
        self.backup_node(backup_dir, 'node', node)
        with node.connect("postgres") as con:
            con.execute("INSERT INTO test VALUES (2)")
            con.commit()
        backup_id = self.backup_node(backup_dir, 'node', node, backup_type="page")

        show_pb = self.show_pb(backup_dir, 'node')
        self.assertEqual(show_pb[1]['Status'], "OK")
        self.assertEqual(show_pb[2]['Status'], "OK")

        node.stop()
        node.cleanup()
        tblspc_path_page = os.path.join(node.base_dir, "tblspc_page")

        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node, options=["-T", "%s=%s" % (tblspc_path_new, tblspc_path_page)]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)
        result = node.execute("postgres", "SELECT id FROM test OFFSET 1")
        self.assertEqual(result[0][0], 2)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_restore_with_tablespace_mapping_2(self):
        """recovery using tablespace-mapping option and page backup"""
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

        # Full backup
        self.backup_node(backup_dir, 'node', node)
        self.assertEqual(self.show_pb(backup_dir, 'node')[0]['Status'], "OK")

        # Create tablespace
        tblspc_path = os.path.join(node.base_dir, "tblspc")
        os.makedirs(tblspc_path)
        with node.connect("postgres") as con:
            con.connection.autocommit = True
            con.execute("CREATE TABLESPACE tblspc LOCATION '%s'" % tblspc_path)
            con.connection.autocommit = False
            con.execute("CREATE TABLE tbl AS SELECT * FROM generate_series(0,3) AS integer")
            con.commit()

        # First page backup
        self.backup_node(backup_dir, 'node', node, backup_type="page")
        self.assertEqual(self.show_pb(backup_dir, 'node')[1]['Status'], "OK")
        self.assertEqual(self.show_pb(backup_dir, 'node')[1]['Mode'], "PAGE")

        # Create tablespace table
        with node.connect("postgres") as con:
            con.connection.autocommit = True
            con.execute("CHECKPOINT")
            con.connection.autocommit = False
            con.execute("CREATE TABLE tbl1 (a int) TABLESPACE tblspc")
            con.execute("INSERT INTO tbl1 SELECT * FROM generate_series(0,3) AS integer")
            con.commit()

        # Second page backup
        backup_id = self.backup_node(backup_dir, 'node', node, backup_type="page")
        self.assertEqual(self.show_pb(backup_dir, 'node')[2]['Status'], "OK")
        self.assertEqual(self.show_pb(backup_dir, 'node')[2]['Mode'], "PAGE")

        node.stop()
        node.cleanup()

        tblspc_path_new = os.path.join(node.base_dir, "tblspc_new")

        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node, options=["-T", "%s=%s" % (tblspc_path, tblspc_path_new)]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        count = node.execute("postgres", "SELECT count(*) FROM tbl")
        self.assertEqual(count[0][0], 4)
        count = node.execute("postgres", "SELECT count(*) FROM tbl1")
        self.assertEqual(count[0][0], 4)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_archive_node_backup_stream_restore_to_recovery_time(self):
        """make node with archiving, make stream backup, make PITR to Recovery Time"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        backup_id = self.backup_node(backup_dir, 'node', node, options=["--stream"])
        node.safe_psql("postgres", "create table t_heap(a int)")
        node.safe_psql("postgres", "select pg_switch_xlog()")
        node.stop()
        node.cleanup()

        recovery_time = self.show_pb(backup_dir, 'node', backup_id)['recovery-time']

        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node, options=["-j", "4", '--time={0}'.format(recovery_time)]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        result = node.psql("postgres", 'select * from t_heap')
        self.assertTrue('does not exist' in result[2].decode("utf-8"))

        # Clean after yourself
        self.del_test_dir(module_name, fname)


    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_archive_node_backup_stream_restore_to_recovery_time(self):
        """make node with archiving, make stream backup, make PITR to Recovery Time"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        backup_id = self.backup_node(backup_dir, 'node', node, options=["--stream"])
        node.safe_psql("postgres", "create table t_heap(a int)")
        node.stop()
        node.cleanup()

        recovery_time = self.show_pb(backup_dir, 'node', backup_id)['recovery-time']

        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node, options=["-j", "4", '--time={0}'.format(recovery_time)]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        result = node.psql("postgres", 'select * from t_heap')
        self.assertTrue('does not exist' in result[2].decode("utf-8"))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_archive_node_backup_stream_pitr(self):
        """make node with archiving, make stream backup, create table t_heap, make pitr to Recovery Time, check that t_heap do not exists"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        backup_id = self.backup_node(backup_dir, 'node', node, options=["--stream"])
        node.safe_psql("postgres", "create table t_heap(a int)")
        node.cleanup()

        recovery_time = self.show_pb(backup_dir, 'node', backup_id)['recovery-time']

        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node,
                options=["-j", "4", '--time={0}'.format(recovery_time)]),
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        result = node.psql("postgres", 'select * from t_heap')
        self.assertEqual(True, 'does not exist' in result[2].decode("utf-8"))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_archive_node_backup_archive_pitr_2(self):
        """make node with archiving, make archive backup, create table t_heap, make pitr to Recovery Time, check that t_heap do not exists"""
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

        backup_id = self.backup_node(backup_dir, 'node', node)
        node.safe_psql("postgres", "create table t_heap(a int)")
        node.stop()
        node.cleanup()

        recovery_time = self.show_pb(backup_dir, 'node', backup_id)['recovery-time']

        self.assertIn("INFO: Restore of backup {0} completed.".format(backup_id),
            self.restore_node(backup_dir, 'node', node, 
                options=["-j", "4", '--time={0}'.format(recovery_time)]),
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        result = node.psql("postgres", 'select * from t_heap')
        self.assertTrue('does not exist' in result[2].decode("utf-8"))

        # Clean after yourself
        self.del_test_dir(module_name, fname)
