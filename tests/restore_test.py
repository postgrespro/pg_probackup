import unittest
import os
from os import path
import six
from .ptrack_helpers import ProbackupTest, ProbackupException
from testgres import stop_all
import subprocess
from datetime import datetime
import shutil


class RestoreTest(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(RestoreTest, self).__init__(*args, **kwargs)

    @classmethod
    def tearDownClass(cls):
        stop_all()

#    @unittest.skip("123")
    def test_restore_full_to_latest(self):
        """recovery to latest from full backup"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/restore/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        node.pgbench_init(scale=2)
        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()
        before = node.execute("postgres", "SELECT * FROM pgbench_branches")
        with open(path.join(node.logs_dir, "backup_1.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, options=["--verbose"]))

        node.stop({"-m": "immediate"})
        node.cleanup()

        # 1 - Test recovery from latest
#        TODO WAITING FIX FOR RESTORE
#        self.assertIn(six.b("INFO: restore complete"),
        self.restore_pb(node, options=["-j", "4", "--verbose"])
#           )

        # 2 - Test that recovery.conf was created
        recovery_conf = path.join(node.data_dir, "recovery.conf")
        self.assertEqual(path.isfile(recovery_conf), True)

        node.start({"-t": "600"})

        after = node.execute("postgres", "SELECT * FROM pgbench_branches")
        self.assertEqual(before, after)

        node.stop()

    def test_restore_full_page_to_latest(self):
        """recovery to latest from full + page backups"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/restore/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        node.pgbench_init(scale=2)

        with open(path.join(node.logs_dir, "backup_1.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, options=["--verbose"]))

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        with open(path.join(node.logs_dir, "backup_2.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="page", options=["--verbose"]))

        before = node.execute("postgres", "SELECT * FROM pgbench_branches")

        node.stop({"-m": "immediate"})
        node.cleanup()

#        TODO WAITING FIX FOR RESTORE
#        self.assertIn(six.b("INFO: restore complete"),
        self.restore_pb(node, options=["-j", "4", "--verbose"])
#            )

        node.start({"-t": "600"})

        after = node.execute("postgres", "SELECT * FROM pgbench_branches")
        self.assertEqual(before, after)

        node.stop()

    def test_restore_to_timeline(self):
        """recovery to target timeline"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/restore/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        node.pgbench_init(scale=2)

        before = node.execute("postgres", "SELECT * FROM pgbench_branches")

        with open(path.join(node.logs_dir, "backup_1.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="full", options=["--verbose"]))

        target_tli = int(node.get_control_data()[six.b("Latest checkpoint's TimeLineID")])
        node.stop({"-m": "immediate"})
        node.cleanup()

#        TODO WAITING FIX FOR RESTORE
#        self.assertIn(six.b("INFO: restore complete"),
        self.restore_pb(node, options=["-j", "4", "--verbose"])
#            )

        node.start({"-t": "600"})

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        with open(path.join(node.logs_dir, "backup_2.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="full", options=["--verbose"]))

        node.stop({"-m": "immediate"})
        node.cleanup()

#        TODO WAITING FIX FOR RESTORE
#        self.assertIn(six.b("INFO: restore complete"),
        self.restore_pb(node,
            options=["-j", "4", "--verbose", "--timeline=%i" % target_tli])
#        )

        recovery_target_timeline = self.get_recovery_conf(node)["recovery_target_timeline"]
        self.assertEqual(int(recovery_target_timeline), target_tli)

        node.start({"-t": "600"})

        after = node.execute("postgres", "SELECT * FROM pgbench_branches")
        self.assertEqual(before, after)

        node.stop()

    def test_restore_to_time(self):
        """recovery to target timeline"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/restore/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        node.pgbench_init(scale=2)

        before = node.execute("postgres", "SELECT * FROM pgbench_branches")

        with open(path.join(node.logs_dir, "backup_1.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="full", options=["--verbose"]))

        target_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        node.stop({"-m": "immediate"})
        node.cleanup()

#        TODO WAITING FIX FOR RESTORE
#        self.assertIn(six.b("INFO: restore complete"),
        self.restore_pb(node,
            options=["-j", "4", "--verbose", '--time="%s"' % target_time])
#            )

        node.start({"-t": "600"})

        after = node.execute("postgres", "SELECT * FROM pgbench_branches")
        self.assertEqual(before, after)

        node.stop()

    def test_restore_to_xid(self):
        """recovery to target xid"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/restore/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        node.pgbench_init(scale=2)
        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        with open(path.join(node.logs_dir, "backup_1.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="full", options=["--verbose"]))

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        before = node.execute("postgres", "SELECT * FROM pgbench_branches")
        with node.connect("postgres") as con:
            res = con.execute("INSERT INTO tbl0005 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        # Enforce segment to be archived to ensure that recovery goes up to the
        # wanted point. There is no way to ensure that all segments needed have
        # been archived up to the xmin point saved earlier without that.
        node.execute("postgres", "SELECT pg_switch_xlog()")

        node.stop({"-m": "fast"})
        node.cleanup()

#        TODO WAITING FIX FOR RESTORE
#        self.assertIn(six.b("INFO: restore complete"),
        self.restore_pb(node,
            options=["-j", "4", "--verbose", '--xid=%s' % target_xid])
#            )

        node.start({"-t": "600"})

        after = node.execute("postgres", "SELECT * FROM pgbench_branches")
        self.assertEqual(before, after)

        node.stop()

    def test_restore_full_ptrack(self):
        """recovery to latest from full + ptrack backups"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/restore/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        node.pgbench_init(scale=2)
        is_ptrack = node.execute("postgres", "SELECT proname FROM pg_proc WHERE proname='pg_ptrack_clear'")
        if not is_ptrack:
            node.stop()
            self.skipTest("ptrack not supported")
            return

        node.append_conf("postgresql.conf", "ptrack_enable = on")
        node.restart()

        with open(path.join(node.logs_dir, "backup_1.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="full", options=["--verbose"]))

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        with open(path.join(node.logs_dir, "backup_2.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="ptrack", options=["--verbose"]))

        before = node.execute("postgres", "SELECT * FROM pgbench_branches")

        node.stop({"-m": "immediate"})
        node.cleanup()

#        TODO WAITING FIX FOR RESTORE
#        self.assertIn(six.b("INFO: restore complete"),
        self.restore_pb(node, options=["-j", "4", "--verbose"])
#        )

        node.start({"-t": "600"})

        after = node.execute("postgres", "SELECT * FROM pgbench_branches")
        self.assertEqual(before, after)

        node.stop()

    def test_restore_full_ptrack_ptrack(self):
        """recovery to latest from full + ptrack + ptrack backups"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/restore/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        node.pgbench_init(scale=2)
        is_ptrack = node.execute("postgres", "SELECT proname FROM pg_proc WHERE proname='pg_ptrack_clear'")
        if not is_ptrack:
            node.stop()
            self.skipTest("ptrack not supported")
            return

        node.append_conf("postgresql.conf", "ptrack_enable = on")
        node.restart()

        with open(path.join(node.logs_dir, "backup_1.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="full", options=["--verbose"]))

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        with open(path.join(node.logs_dir, "backup_2.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="ptrack", options=["--verbose"]))

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        with open(path.join(node.logs_dir, "backup_3.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="ptrack", options=["--verbose"]))

        before = node.execute("postgres", "SELECT * FROM pgbench_branches")

        node.stop({"-m": "immediate"})
        node.cleanup()

#        TODO WAITING FIX FOR RESTORE
#        self.assertIn(six.b("INFO: restore complete"),
        self.restore_pb(node, options=["-j", "4", "--verbose"])
#            )

        node.start({"-t": "600"})

        after = node.execute("postgres", "SELECT * FROM pgbench_branches")
        self.assertEqual(before, after)

        node.stop()

    def test_restore_full_ptrack_stream(self):
        """recovery in stream mode to latest from full + ptrack backups"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/restore/{0}".format(fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on', 'max_wal_senders': '2'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        node.pgbench_init(scale=2)
        is_ptrack = node.execute("postgres", "SELECT proname FROM pg_proc WHERE proname='pg_ptrack_clear'")
        if not is_ptrack:
            node.stop()
            self.skipTest("ptrack not supported")
            return

        with open(path.join(node.logs_dir, "backup_1.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="full", options=["--verbose", "--stream"]))

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        with open(path.join(node.logs_dir, "backup_2.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="ptrack", options=["--verbose", "--stream"]))

        before = node.execute("postgres", "SELECT * FROM pgbench_branches")

        node.stop({"-m": "immediate"})
        node.cleanup()

#        TODO WAITING FIX FOR RESTORE
#        self.assertIn(six.b("INFO: restore complete"),
        self.restore_pb(node, options=["-j", "4", "--verbose"])
#            )

        node.start({"-t": "600"})

        after = node.execute("postgres", "SELECT * FROM pgbench_branches")
        self.assertEqual(before, after)

        node.stop()

    def test_restore_full_ptrack_under_load(self):
        """recovery to latest from full + ptrack backups with loads when ptrack backup do"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/restore/{0}".format(fname),
            set_archiving=True,
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on', 'max_wal_senders': '2'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        wal_segment_size = self.guc_wal_segment_size(node)
        node.pgbench_init(scale=2)
        is_ptrack = node.execute("postgres", "SELECT proname FROM pg_proc WHERE proname='pg_ptrack_clear'")
        if not is_ptrack:
            node.stop()
            self.skipTest("ptrack not supported")
            return
        node.restart()

        with open(path.join(node.logs_dir, "backup_1.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="full", options=["--verbose"]))

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "8"]
        )

        with open(path.join(node.logs_dir, "backup_2.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="ptrack", options=["--verbose", "--stream"]))

        pgbench.wait()
        pgbench.stdout.close()

        bbalance = node.execute("postgres", "SELECT sum(bbalance) FROM pgbench_branches")
        delta = node.execute("postgres", "SELECT sum(delta) FROM pgbench_history")

        self.assertEqual(bbalance, delta)
        node.stop({"-m": "immediate"})
        node.cleanup()

        self.wrong_wal_clean(node, wal_segment_size)

#        TODO WAITING FIX FOR RESTORE
#        self.assertIn(six.b("INFO: restore complete"),
        self.restore_pb(node, options=["-j", "4", "--verbose"])
#            )

        node.start({"-t": "600"})

        bbalance = node.execute("postgres", "SELECT sum(bbalance) FROM pgbench_branches")
        delta = node.execute("postgres", "SELECT sum(delta) FROM pgbench_history")

        self.assertEqual(bbalance, delta)

        node.stop()

    def test_restore_full_under_load_ptrack(self):
        """recovery to latest from full + page backups with loads when full backup do"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/restore/{0}".format(fname),
            set_archiving=True,
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on', 'max_wal_senders': '2'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        wal_segment_size = self.guc_wal_segment_size(node)
        node.pgbench_init(scale=2)
        is_ptrack = node.execute("postgres", "SELECT proname FROM pg_proc WHERE proname='pg_ptrack_clear'")
        if not is_ptrack:
            node.stop()
            self.skipTest("ptrack not supported")
            return

        node.restart()

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "8"]
        )

        with open(path.join(node.logs_dir, "backup_1.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="full", options=["--verbose"]))

        pgbench.wait()
        pgbench.stdout.close()

        with open(path.join(node.logs_dir, "backup_2.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="ptrack", options=["--verbose", "--stream"]))

        bbalance = node.execute("postgres", "SELECT sum(bbalance) FROM pgbench_branches")
        delta = node.execute("postgres", "SELECT sum(delta) FROM pgbench_history")

        self.assertEqual(bbalance, delta)

        node.stop({"-m": "immediate"})
        node.cleanup()
        self.wrong_wal_clean(node, wal_segment_size)

#        TODO WAITING FIX FOR RESTORE
#        self.assertIn(six.b("INFO: restore complete"),
        self.restore_pb(node, options=["-j", "4", "--verbose"])
#            )

        node.start({"-t": "600"})

        bbalance = node.execute("postgres", "SELECT sum(bbalance) FROM pgbench_branches")
        delta = node.execute("postgres", "SELECT sum(delta) FROM pgbench_history")

        self.assertEqual(bbalance, delta)

        node.stop()

    def test_restore_to_xid_inclusive(self):
        """recovery with target inclusive false"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/restore/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        node.pgbench_init(scale=2)
        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        with open(path.join(node.logs_dir, "backup_1.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, backup_type="full", options=["--verbose"]))

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        before = node.execute("postgres", "SELECT * FROM pgbench_branches")
        with node.connect("postgres") as con:
            res = con.execute("INSERT INTO tbl0005 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]

        pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pgbench.wait()
        pgbench.stdout.close()

        # Enforce segment to be archived to ensure that recovery goes up to the
        # wanted point. There is no way to ensure that all segments needed have
        # been archived up to the xmin point saved earlier without that.
        node.execute("postgres", "SELECT pg_switch_xlog()")

        node.stop({"-m": "fast"})
        node.cleanup()

#        TODO WAITING FIX FOR RESTORE
#        self.assertIn(six.b("INFO: restore complete"),
        self.restore_pb(node,
            options=[
                "-j", "4",
                "--verbose",
                '--xid=%s' % target_xid,
                "--inclusive=false"])
#        )

        node.start({"-t": "600"})

        after = node.execute("postgres", "SELECT * FROM pgbench_branches")
        self.assertEqual(before, after)
        self.assertEqual(len(node.execute("postgres", "SELECT * FROM tbl0005")), 0)

        node.stop()

    def test_restore_with_tablespace_mapping_1(self):
        """recovery using tablespace-mapping option"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/restore/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))

        # Create tablespace
        tblspc_path = path.join(node.base_dir, "tblspc")
        os.makedirs(tblspc_path)
        with node.connect("postgres") as con:
            con.connection.autocommit = True
            con.execute("CREATE TABLESPACE tblspc LOCATION '%s'" % tblspc_path)
            con.connection.autocommit = False
            con.execute("CREATE TABLE test (id int) TABLESPACE tblspc")
            con.execute("INSERT INTO test VALUES (1)")
            con.commit()

        self.backup_pb(node)
        self.assertEqual(self.show_pb(node)[0]['Status'], six.b("OK"))

        # 1 - Try to restore to existing directory
        node.stop()
        try:
            self.restore_pb(node)
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                e.message,
                'ERROR: restore destination is not empty: "{0}"\n'.format(node.data_dir)
                )

        # 2 - Try to restore to existing tablespace directory
        shutil.rmtree(node.data_dir)
        try:
            self.restore_pb(node)
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                e.message,
                'ERROR: restore tablespace destination is not empty: "{0}"\n'.format(tblspc_path)
                )

        # 3 - Restore using tablespace-mapping
        tblspc_path_new = path.join(node.base_dir, "tblspc_new")
#        TODO WAITING FIX FOR RESTORE
#        self.assertIn(six.b("INFO: restore complete."),
        self.restore_pb(node,
            options=["-T", "%s=%s" % (tblspc_path, tblspc_path_new)])
#        )

        node.start()
        id = node.execute("postgres", "SELECT id FROM test")
        self.assertEqual(id[0][0], 1)

        # 4 - Restore using tablespace-mapping using page backup
        self.backup_pb(node)
        with node.connect("postgres") as con:
            con.execute("INSERT INTO test VALUES (2)")
            con.commit()
        self.backup_pb(node, backup_type="page")

        show_pb = self.show_pb(node)
        self.assertEqual(show_pb[1]['Status'], six.b("OK"))
        self.assertEqual(show_pb[2]['Status'], six.b("OK"))

        node.stop()
        node.cleanup()
        tblspc_path_page = path.join(node.base_dir, "tblspc_page")
#        TODO WAITING FIX FOR RESTORE
#        self.assertIn(six.b("INFO: restore complete."),
        self.restore_pb(node,
            options=["-T", "%s=%s" % (tblspc_path_new, tblspc_path_page)])
#        )

        node.start()
        id = node.execute("postgres", "SELECT id FROM test OFFSET 1")
        self.assertEqual(id[0][0], 2)

        node.stop()

    def test_restore_with_tablespace_mapping_2(self):
        """recovery using tablespace-mapping option and page backup"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/restore/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))

        # Full backup
        self.backup_pb(node)
        self.assertEqual(self.show_pb(node)[0]['Status'], six.b("OK"))

        # Create tablespace
        tblspc_path = path.join(node.base_dir, "tblspc")
        os.makedirs(tblspc_path)
        with node.connect("postgres") as con:
            con.connection.autocommit = True
            con.execute("CREATE TABLESPACE tblspc LOCATION '%s'" % tblspc_path)
            con.connection.autocommit = False
            con.execute("CREATE TABLE tbl AS SELECT * FROM generate_series(0,3) AS integer")
            con.commit()

        # First page backup
        self.backup_pb(node, backup_type="page")
        self.assertEqual(self.show_pb(node)[1]['Status'], six.b("OK"))
        self.assertEqual(self.show_pb(node)[1]['Mode'], six.b("PAGE"))

        # Create tablespace table
        with node.connect("postgres") as con:
            con.connection.autocommit = True
            con.execute("CHECKPOINT")
            con.connection.autocommit = False
            con.execute("CREATE TABLE tbl1 (a int) TABLESPACE tblspc")
            con.execute("INSERT INTO tbl1 SELECT * FROM generate_series(0,3) AS integer")
            con.commit()

        # Second page backup
        self.backup_pb(node, backup_type="page")
        self.assertEqual(self.show_pb(node)[2]['Status'], six.b("OK"))
        self.assertEqual(self.show_pb(node)[2]['Mode'], six.b("PAGE"))

        node.stop()
        node.cleanup()

        tblspc_path_new = path.join(node.base_dir, "tblspc_new")
#        exit(1)
#        TODO WAITING FIX FOR RESTORE
#        self.assertIn(six.b("INFO: restore complete."),
        self.restore_pb(node,
            options=["-T", "%s=%s" % (tblspc_path, tblspc_path_new)])
#        )

        # Check tables
        node.start()

        count = node.execute("postgres", "SELECT count(*) FROM tbl")
        self.assertEqual(count[0][0], 4)

        count = node.execute("postgres", "SELECT count(*) FROM tbl1")
        self.assertEqual(count[0][0], 4)

        node.stop()
