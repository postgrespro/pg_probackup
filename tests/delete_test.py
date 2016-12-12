import unittest
from os import path
import six
from .pb_lib import ProbackupTest
from testgres import stop_all
import subprocess


class DeleteTest(ProbackupTest, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		super(DeleteTest, self).__init__(*args, **kwargs)

	@classmethod
	def tearDownClass(cls):
		stop_all()

	def test_delete_full_backups_1(self):
		"""delete full backups"""
		node = self.make_bnode('delete_full_backups_1', base_dir="tmp_dirs/delete/delete_full_backups_1")
		node.start()
		self.assertEqual(self.init_pb(node), six.b(""))
		node.pgbench_init()

		# full backup mode
		with open(path.join(node.logs_dir, "backup_1.log"), "wb") as backup_log:
			backup_log.write(self.backup_pb(node, options=["--verbose"]))

		pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
		pgbench.wait()
		pgbench.stdout.close()

		with open(path.join(node.logs_dir, "backup_2.log"), "wb") as backup_log:
			backup_log.write(self.backup_pb(node, options=["--verbose"]))

		pgbench = node.pgbench(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
		pgbench.wait()
		pgbench.stdout.close()

		with open(path.join(node.logs_dir, "backup_3.log"), "wb") as backup_log:
			backup_log.write(self.backup_pb(node, options=["--verbose"]))

		show_backups = self.show_pb(node)
		id_1 = show_backups[0].id
		id_2 = show_backups[2].id
		self.delete_pb(node, show_backups[1].id)
		show_backups = self.show_pb(node)
		self.assertEqual(show_backups[0].id, id_1)
		self.assertEqual(show_backups[1].id, id_2)

		node.stop()

	def test_delete_increment_2(self):
		"""delete increment and all after him"""
		node = self.make_bnode('delete_increment_2', base_dir="tmp_dirs/delete/delete_increment_2")
		node.start()
		self.assertEqual(self.init_pb(node), six.b(""))

		# full backup mode
		with open(path.join(node.logs_dir, "backup_1.log"), "wb") as backup_log:
			backup_log.write(self.backup_pb(node, options=["--verbose"]))

		# page backup mode
		with open(path.join(node.logs_dir, "backup_2.log"), "wb") as backup_log:
			backup_log.write(self.backup_pb(node, backup_type="page", options=["--verbose"]))

		# page backup mode
		with open(path.join(node.logs_dir, "backup_3.log"), "wb") as backup_log:
			backup_log.write(self.backup_pb(node, backup_type="page", options=["--verbose"]))

		show_backups = self.show_pb(node)
		self.assertEqual(len(show_backups), 3)
		self.delete_pb(node, show_backups[1].id)
		show_backups = self.show_pb(node)
		self.assertEqual(len(show_backups), 1)
		self.assertEqual(show_backups[0].mode, six.b("FULL"))
		self.assertEqual(show_backups[0].status, six.b("OK"))

		node.stop()
