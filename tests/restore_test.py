import unittest
import os
from os import path
import six
from .pb_lib import ProbackupTest
from testgres import stop_all
import subprocess
from datetime import datetime


class RestoreTest(ProbackupTest, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		super(RestoreTest, self).__init__(*args, **kwargs)

	@classmethod
	def tearDownClass(cls):
		stop_all()

	def test_restore_to_latest_1(self):
		"""recovery to latest from full backup"""
		node = self.make_bnode('restore_to_latest_1', base_dir="tmp_dirs/restore/restore_to_latest_1")
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

		self.assertIn(six.b("INFO: restore complete"),
			self.restore_pb(node, options=["-j", "4", "--verbose"]))

		node.start({"-t": "600"})

		after = node.execute("postgres", "SELECT * FROM pgbench_branches")
		self.assertEqual(before, after)

		node.stop()

	def test_restore_to_latest_2(self):
		"""recovery to latest from full + page backups"""
		node = self.make_bnode('restore_to_latest_2', base_dir="tmp_dirs/restore/restore_to_latest_2")
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

		self.assertIn(six.b("INFO: restore complete"),
			self.restore_pb(node, options=["-j", "4", "--verbose"]));

		node.start({"-t": "600"})

		after = node.execute("postgres", "SELECT * FROM pgbench_branches")
		self.assertEqual(before, after)

		node.stop()

	def test_restore_to_timeline_3(self):
		"""recovery to target timeline"""
		node = self.make_bnode('restore_to_timeline_3', base_dir="tmp_dirs/restore/restore_to_timeline_3")
		node.start()
		self.assertEqual(self.init_pb(node), six.b(""))
		node.pgbench_init(scale=2)

		before = node.execute("postgres", "SELECT * FROM pgbench_branches")

		with open(path.join(node.logs_dir, "backup_1.log"), "wb") as backup_log:
			backup_log.write(self.backup_pb(node, backup_type="full", options=["--verbose"]))

		target_tli = int(self.get_control_data(node)[six.b("Latest checkpoint's TimeLineID")])
		node.stop({"-m": "immediate"})
		node.cleanup()

		self.assertIn(six.b("INFO: restore complete"),
			self.restore_pb(node, options=["-j", "4", "--verbose"]))

		node.start({"-t": "600"})

		pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
		pgbench.wait()
		pgbench.stdout.close()

		with open(path.join(node.logs_dir, "backup_2.log"), "wb") as backup_log:
			backup_log.write(self.backup_pb(node, backup_type="full", options=["--verbose"]))

		node.stop({"-m": "immediate"})
		node.cleanup()

		self.assertIn(six.b("INFO: restore complete"),
			self.restore_pb(node,
				options=["-j", "4", "--verbose", "--timeline=%i" % target_tli]))

		recovery_target_timeline = self.get_recovery_conf(node)["recovery_target_timeline"]
		self.assertEqual(int(recovery_target_timeline), target_tli)

		node.start({"-t": "600"})

		after = node.execute("postgres", "SELECT * FROM pgbench_branches")
		self.assertEqual(before, after)

		node.stop()

	def test_restore_to_time_4(self):
		"""recovery to target timeline"""
		node = self.make_bnode('restore_to_time_4', base_dir="tmp_dirs/restore/restore_to_time_4")
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

		self.assertIn(six.b("INFO: restore complete"),
			self.restore_pb(node,
				options=["-j", "4", "--verbose", '--time="%s"' % target_time]))

		node.start({"-t": "600"})

		after = node.execute("postgres", "SELECT * FROM pgbench_branches")
		self.assertEqual(before, after)

		node.stop()

	def test_restore_to_xid_5(self):
		"""recovery to target xid"""
		node = self.make_bnode('restore_to_xid_5', base_dir="tmp_dirs/restore/restore_to_xid_5")
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

		self.assertIn(six.b("INFO: restore complete"),
			self.restore_pb(node,
				options=["-j", "4", "--verbose", '--xid=%s' % target_xid]))

		node.start({"-t": "600"})

		after = node.execute("postgres", "SELECT * FROM pgbench_branches")
		self.assertEqual(before, after)

		node.stop()

	def test_restore_full_ptrack_6(self):
		"""recovery to latest from full + ptrack backups"""
		node = self.make_bnode('restore_full_ptrack_6', base_dir="tmp_dirs/restore/full_ptrack_6")
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

		self.assertIn(six.b("INFO: restore complete"),
			self.restore_pb(node, options=["-j", "4", "--verbose"]))

		node.start({"-t": "600"})

		after = node.execute("postgres", "SELECT * FROM pgbench_branches")
		self.assertEqual(before, after)

		node.stop()

	def test_restore_full_ptrack_ptrack_7(self):
		"""recovery to latest from full + ptrack + ptrack backups"""
		node = self.make_bnode('restore_full_ptrack_ptrack_7', base_dir="tmp_dirs/restore/full_ptrack_ptrack_7")
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

		self.assertIn(six.b("INFO: restore complete"),
			self.restore_pb(node, options=["-j", "4", "--verbose"]))

		node.start({"-t": "600"})

		after = node.execute("postgres", "SELECT * FROM pgbench_branches")
		self.assertEqual(before, after)

		node.stop()

	def test_restore_full_ptrack_stream_8(self):
		"""recovery in stream mode to latest from full + ptrack backups"""
		node = self.make_bnode('restore_full_ptrack_stream_8', base_dir="tmp_dirs/restore/full_ptrack_stream_8")
		node.start()
		self.assertEqual(self.init_pb(node), six.b(""))
		node.pgbench_init(scale=2)
		is_ptrack = node.execute("postgres", "SELECT proname FROM pg_proc WHERE proname='pg_ptrack_clear'")
		if not is_ptrack:
			node.stop()
			self.skipTest("ptrack not supported")
			return

		node.append_conf("pg_hba.conf", "local replication all trust")
		node.append_conf("pg_hba.conf", "host replication all 127.0.0.1/32 trust")
		node.append_conf("postgresql.conf", "ptrack_enable = on")
		node.append_conf("postgresql.conf", "max_wal_senders = 1")
		node.restart()

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

		self.assertIn(six.b("INFO: restore complete"),
			self.restore_pb(node, options=["-j", "4", "--verbose"]))

		node.start({"-t": "600"})

		after = node.execute("postgres", "SELECT * FROM pgbench_branches")
		self.assertEqual(before, after)

		node.stop()

	def test_restore_full_ptrack_under_load_9(self):
		"""recovery to latest from full + page backups with loads when ptrack backup do"""
		node = self.make_bnode('restore_full_ptrack_under_load_9', base_dir="tmp_dirs/restore/full_ptrack_under_load_9")
		node.start()
		self.assertEqual(self.init_pb(node), six.b(""))
		wal_segment_size = self.guc_wal_segment_size(node)
		node.pgbench_init(scale=2)
		is_ptrack = node.execute("postgres", "SELECT proname FROM pg_proc WHERE proname='pg_ptrack_clear'")
		if not is_ptrack:
			node.stop()
			self.skipTest("ptrack not supported")
			return

		node.append_conf("pg_hba.conf", "local replication all trust")
		node.append_conf("pg_hba.conf", "host replication all 127.0.0.1/32 trust")
		node.append_conf("postgresql.conf", "ptrack_enable = on")
		node.append_conf("postgresql.conf", "max_wal_senders = 1")
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

		node.execute("postgres", "SELECT pg_switch_xlog()")

		bbalance = node.execute("postgres", "SELECT sum(bbalance) FROM pgbench_branches")
		delta = node.execute("postgres", "SELECT sum(delta) FROM pgbench_history")

		self.assertEqual(bbalance, delta)
		node.stop({"-m": "immediate"})
		node.cleanup()

		self.wrong_wal_clean(node, wal_segment_size)

		self.assertIn(six.b("INFO: restore complete"),
			self.restore_pb(node, options=["-j", "4", "--verbose"]))

		node.start({"-t": "600"})

		bbalance = node.execute("postgres", "SELECT sum(bbalance) FROM pgbench_branches")
		delta = node.execute("postgres", "SELECT sum(delta) FROM pgbench_history")

		self.assertEqual(bbalance, delta)

		node.stop()

	def test_restore_full_under_load_ptrack_10(self):
		"""recovery to latest from full + page backups with loads when full backup do"""
		node = self.make_bnode('restore_full_under_load_ptrack_10', base_dir="tmp_dirs/restore/full_under_load_ptrack_10")
		node.start()
		self.assertEqual(self.init_pb(node), six.b(""))
		wal_segment_size = self.guc_wal_segment_size(node)
		node.pgbench_init(scale=2)
		is_ptrack = node.execute("postgres", "SELECT proname FROM pg_proc WHERE proname='pg_ptrack_clear'")
		if not is_ptrack:
			node.stop()
			self.skipTest("ptrack not supported")
			return

		node.append_conf("pg_hba.conf", "local replication all trust")
		node.append_conf("pg_hba.conf", "host replication all 127.0.0.1/32 trust")
		node.append_conf("postgresql.conf", "ptrack_enable = on")
		node.append_conf("postgresql.conf", "max_wal_senders = 1")
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

		node.execute("postgres", "SELECT pg_switch_xlog()")

		bbalance = node.execute("postgres", "SELECT sum(bbalance) FROM pgbench_branches")
		delta = node.execute("postgres", "SELECT sum(delta) FROM pgbench_history")

		self.assertEqual(bbalance, delta)

		node.stop({"-m": "immediate"})
		node.cleanup()
		self.wrong_wal_clean(node, wal_segment_size)

		self.assertIn(six.b("INFO: restore complete"),
			self.restore_pb(node, options=["-j", "4", "--verbose"]))

		node.start({"-t": "600"})

		bbalance = node.execute("postgres", "SELECT sum(bbalance) FROM pgbench_branches")
		delta = node.execute("postgres", "SELECT sum(delta) FROM pgbench_history")

		self.assertEqual(bbalance, delta)

		node.stop()

	def test_restore_to_xid_inclusive_11(self):
		"""recovery with target inclusive false"""
		node = self.make_bnode('restore_to_xid_inclusive_11', base_dir="tmp_dirs/restore/restore_to_xid_inclusive_11")
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

		self.assertIn(six.b("INFO: restore complete"),
			self.restore_pb(node,
				options=[
					"-j", "4",
					"--verbose",
					'--xid=%s' % target_xid,
					"--inclusive=false"]))

		node.start({"-t": "600"})

		after = node.execute("postgres", "SELECT * FROM pgbench_branches")
		self.assertEqual(before, after)
		self.assertEqual(len(node.execute("postgres", "SELECT * FROM tbl0005")), 0)

		node.stop()

	def test_restore_with_tablespace_mapping_12(self):
		"""recovery using tablespace-mapping option"""
		node = self.make_bnode('restore_with_tablespace_mapping_12',
			base_dir="tmp_dirs/restore/restore_with_tablespace_mapping_12")
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
		self.assertEqual(self.show_pb(node)[0].status, six.b("OK"))

		# 1 - Try to restore to existing directory
		node.stop()
		self.assertEqual(six.b("ERROR: restore destination is not empty\n"),
			self.restore_pb(node))

		# 2 - Try to restore to existing tablespace directory
		node.cleanup()
		self.assertIn(six.b("ERROR: restore destination is not empty"),
			self.restore_pb(node))

		# 3 - Restore using tablespace-mapping
		node.cleanup()
		tblspc_path_new = path.join(node.base_dir, "tblspc_new")
		self.assertIn(six.b("INFO: restore complete."),
			self.restore_pb(node,
				options=["-T", "%s=%s" % (tblspc_path, tblspc_path_new)]))

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
		self.assertEqual(show_pb[1].status, six.b("OK"))
		self.assertEqual(show_pb[2].status, six.b("OK"))

		node.stop()
		node.cleanup()
		tblspc_path_page = path.join(node.base_dir, "tblspc_page")
		self.assertIn(six.b("INFO: restore complete."),
			self.restore_pb(node,
				options=["-T", "%s=%s" % (tblspc_path_new, tblspc_path_page)]))

		node.start()
		id = node.execute("postgres", "SELECT id FROM test OFFSET 1")
		self.assertEqual(id[0][0], 2)

		node.stop()
