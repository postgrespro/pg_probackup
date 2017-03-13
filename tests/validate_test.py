import unittest
from os import path, listdir
import six
from .pb_lib import ProbackupTest
from datetime import datetime, timedelta
from testgres import stop_all
import subprocess


class ValidateTest(ProbackupTest, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		super(ValidateTest, self).__init__(*args, **kwargs)

	@classmethod
	def tearDownClass(cls):
		try:
			stop_all()
		except:
			pass

	def test_validate_wal_1(self):
		"""recovery to latest from full backup"""
		node = self.make_bnode('test_validate_wal_1', base_dir="tmp_dirs/validate/wal_1")
		node.start()
		self.assertEqual(self.init_pb(node), six.b(""))
		node.pgbench_init(scale=2)
		with node.connect("postgres") as con:
			con.execute("CREATE TABLE tbl0005 (a text)")
			con.commit()

		with open(path.join(node.logs_dir, "backup_1.log"), "wb") as backup_log:
			backup_log.write(self.backup_pb(node, options=["--verbose"]))

		pgbench = node.pgbench(
			stdout=subprocess.PIPE,
			stderr=subprocess.STDOUT,
			options=["-c", "4", "-T", "10"]
		)

		pgbench.wait()
		pgbench.stdout.close()

		# Save time to validate
		target_time = datetime.now()

		target_xid = None
		with node.connect("postgres") as con:
			res = con.execute("INSERT INTO tbl0005 VALUES ('inserted') RETURNING (xmin)")
			con.commit()
			target_xid = res[0][0]

		node.execute("postgres", "SELECT pg_switch_xlog()")
		node.stop({"-m": "smart"})

		id_backup = self.show_pb(node)[0].id

		# Validate to real time
		self.assertIn(six.b("INFO: backup validation completed successfully on"),
			self.validate_pb(node, options=["--time='{:%Y-%m-%d %H:%M:%S}'".format(
				target_time)]))

		# Validate to unreal time
		self.assertIn(six.b("ERROR: no full backup found, cannot validate."),
			self.validate_pb(node, options=["--time='{:%Y-%m-%d %H:%M:%S}'".format(
				target_time - timedelta(days=2))]))

		# Validate to unreal time #2
		self.assertIn(six.b("ERROR: not enough WAL records to time"),
			self.validate_pb(node, options=["--time='{:%Y-%m-%d %H:%M:%S}'".format(
				target_time + timedelta(days=2))]))

		# Validate to real xid
		self.assertIn(six.b("INFO: backup validation completed successfully on"),
			self.validate_pb(node, options=["--xid=%s" % target_xid]))

		# Validate to unreal xid
		self.assertIn(six.b("ERROR: not enough WAL records to xid"),
			self.validate_pb(node, options=["--xid=%d" % (int(target_xid) + 1000)]))

		# Validate with backup ID
		self.assertIn(six.b("INFO: backup validation completed successfully on"),
			self.validate_pb(node, id_backup))

		# Validate broken WAL
		wals_dir = path.join(self.backup_dir(node), "wal")
		wals = [f for f in listdir(wals_dir) if path.isfile(path.join(wals_dir, f))]
		wals.sort()
		with open(path.join(wals_dir, wals[-3]), "rb+") as f:
			f.seek(256)
			f.write(six.b("blablabla"))

		res = self.validate_pb(node, id_backup, options=['--xid=%s' % target_xid])
		self.assertIn(six.b("not enough WAL records to xid"), res)
