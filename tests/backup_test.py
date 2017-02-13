import unittest
from os import path, listdir
import six
from .pb_lib import ProbackupTest
from testgres import stop_all


class BackupTest(ProbackupTest, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		super(BackupTest, self).__init__(*args, **kwargs)

	@classmethod
	def tearDownClass(cls):
		stop_all()

	def test_backup_modes_1(self):
		"""standart backup modes"""
		node = self.make_bnode('backup_modes_', base_dir="tmp_dirs/backup/backup_modes_1")
		node.start()
		self.assertEqual(self.init_pb(node), six.b(""))

		# detect ptrack
		is_ptrack = node.execute("postgres", "SELECT proname FROM pg_proc WHERE proname='pg_ptrack_clear'")
		if len(is_ptrack):
			node.append_conf("postgresql.conf", "ptrack_enable = on")
			node.restart()

		# full backup mode
		with open(path.join(node.logs_dir, "backup_full.log"), "wb") as backup_log:
			backup_log.write(self.backup_pb(node, options=["--verbose"]))

		show_backup = self.show_pb(node)[0]
		full_backup_id = show_backup.id
		self.assertEqual(show_backup.status, six.b("OK"))
		self.assertEqual(show_backup.mode, six.b("FULL"))

		# postmaster.pid and postmaster.opts shouldn't be copied
		excluded = True
		backups_dir = path.join(self.backup_dir(node), "backups")
		for backup in listdir(backups_dir):
			db_dir = path.join(backups_dir, backup, "database")
			for f in listdir(db_dir):
				if path.isfile(path.join(db_dir, f)) and \
					(f == "postmaster.pid" or f == "postmaster.opts"):
					excluded = False
		self.assertEqual(excluded, True)

		# page backup mode
		with open(path.join(node.logs_dir, "backup_page.log"), "wb") as backup_log:
			backup_log.write(self.backup_pb(node, backup_type="page", options=["--verbose"]))

		show_backup = self.show_pb(node)[0]
		self.assertEqual(show_backup.status, six.b("OK"))
		self.assertEqual(show_backup.mode, six.b("PAGE"))

		# Check parent backup
		self.assertEqual(
			full_backup_id,
			self.show_pb(node, show_backup.id)[six.b("PARENT_BACKUP")].strip(six.b(" '"))
		)

		# ptrack backup mode
		if len(is_ptrack):
			with open(path.join(node.logs_dir, "backup_ptrack.log"), "wb") as backup_log:
				backup_log.write(self.backup_pb(node, backup_type="ptrack", options=["--verbose"]))

			show_backup = self.show_pb(node)[0]
			self.assertEqual(show_backup.status, six.b("OK"))
			self.assertEqual(show_backup.mode, six.b("PTRACK"))

		node.stop()

	def test_smooth_checkpoint_2(self):
		"""full backup with smooth checkpoint"""
		node = self.make_bnode('smooth_checkpoint_2', base_dir="tmp_dirs/backup/smooth_checkpoint_2")
		node.start()
		self.assertEqual(self.init_pb(node), six.b(""))

		with open(path.join(node.logs_dir, "backup.log"), "wb") as backup_log:
			backup_log.write(self.backup_pb(node, options=["--verbose", "-C"]))

		self.assertEqual(self.show_pb(node)[0].status, six.b("OK"))

		node.stop()

	def test_page_backup_without_full_3(self):
		"""page-level backup without validated full backup"""
		node = self.make_bnode('without_full_3', base_dir="tmp_dirs/backup/without_full_3")
		node.start()
		self.assertEqual(self.init_pb(node), six.b(""))

		with open(path.join(node.logs_dir, "backup.log"), "wb") as backup_log:
			backup_log.write(self.backup_pb(node, backup_type="page", options=["--verbose"]))

		self.assertEqual(self.show_pb(node)[0].status, six.b("ERROR"))

		node.stop()

	def test_ptrack_threads_4(self):
		"""ptrack multi thread backup mode"""
		node = self.make_bnode(
			'ptrack_threads_4',
			base_dir="tmp_dirs/backup/ptrack_threads_4",
			options={"ptrack_enable": "on"}
		)
		node.start()
		self.assertEqual(self.init_pb(node), six.b(""))

		with open(path.join(node.logs_dir, "backup_full.log"), "wb") as backup_log:
			backup_log.write(self.backup_pb(node, backup_type="full", options=["--verbose", "-j", "4"]))

		self.assertEqual(self.show_pb(node)[0].status, six.b("OK"))

		with open(path.join(node.logs_dir, "backup_ptrack.log"), "wb") as backup_log:
			backup_log.write(self.backup_pb(node, backup_type="ptrack", options=["--verbose", "-j", "4"]))

		self.assertEqual(self.show_pb(node)[0].status, six.b("OK"))

		node.stop()

	def test_ptrack_threads_stream_5(self):
		"""ptrack multi thread backup mode and stream"""
		node = self.make_bnode(
			'ptrack_threads_stream_5',
			base_dir="tmp_dirs/backup/ptrack_threads_stream_5",
			options={
				"ptrack_enable": "on",
				"max_wal_senders": "5"
			}
		)
		node.append_conf("pg_hba.conf", "local replication all trust")
		node.append_conf("pg_hba.conf", "host replication all 127.0.0.1/32 trust")
		node.start()
		self.assertEqual(self.init_pb(node), six.b(""))

		with open(path.join(node.logs_dir, "backup_full.log"), "wb") as backup_log:
			backup_log.write(self.backup_pb(
				node,
				backup_type="full",
				options=["--verbose", "-j", "4", "--stream"]
			))

		self.assertEqual(self.show_pb(node)[0].status, six.b("OK"))

		with open(path.join(node.logs_dir, "backup_ptrack.log"), "wb") as backup_log:
			backup_log.write(self.backup_pb(
				node,
				backup_type="ptrack",
				options=["--verbose", "-j", "4", "--stream"]
			))

		self.assertEqual(self.show_pb(node)[0].status, six.b("OK"))

		node.stop()
