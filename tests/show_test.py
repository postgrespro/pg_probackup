import unittest
import os
from os import path
import six
from .pb_lib import ProbackupTest
from testgres import stop_all


class OptionTest(ProbackupTest, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		super(OptionTest, self).__init__(*args, **kwargs)

	@classmethod
	def tearDownClass(cls):
		stop_all()

	def test_ok_1(self):
		"""Status DONE and OK"""
		node = self.make_bnode('done_ok', base_dir="tmp_dirs/show/ok_1")
		node.start()
		self.assertEqual(self.init_pb(node), six.b(""))

		self.assertEqual(
			self.backup_pb(node, options=["--quiet"]),
			six.b("")
		)
		self.assertIn(six.b("OK"), self.show_pb(node, as_text=True))

		node.stop()

	def test_corrupt_2(self):
		"""Status DONE and OK"""
		node = self.make_bnode('corrupt', base_dir="tmp_dirs/show/corrupt_2")
		node.start()
		self.assertEqual(self.init_pb(node), six.b(""))

		self.assertEqual(
			self.backup_pb(node, options=["--quiet"]),
			six.b("")
		)

		id_backup = self.show_pb(node)[0].id
		os.remove(path.join(self.backup_dir(node), "backups", id_backup.decode("utf-8"), "database", "postgresql.conf"))

		self.validate_pb(node, id_backup)
		self.assertIn(six.b("CORRUPT"), self.show_pb(node, as_text=True))

		node.stop()
