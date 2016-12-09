import unittest
import os
from os import path
import six
from .pb_lib import dir_files, ProbackupTest


class InitTest(ProbackupTest, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		super(InitTest, self).__init__(*args, **kwargs)

	def test_success_1(self):
		"""Success normal init"""
		node = self.make_bnode('test_success_1', base_dir="tmp_dirs/init/success_1")
		self.assertEqual(self.init_pb(node), six.b(""))
		self.assertEqual(
			dir_files(self.backup_dir(node)),
			['backups', 'pg_probackup.conf', 'wal']
		)

	def test_already_exist_2(self):
		"""Failure with backup catalog already existed"""
		node = self.make_bnode('test_already_exist_2', base_dir="tmp_dirs/init/already_exist_2")
		self.init_pb(node)
		self.assertEqual(
			self.init_pb(node),
			six.b("ERROR: backup catalog already exist and it's not empty\n")
		)

	def test_abs_path_3(self):
		"""failure with backup catalog should be given as absolute path"""
		node = self.make_bnode('test_abs_path_3', base_dir="tmp_dirs/init/abs_path_3")
		self.assertEqual(
			self.run_pb(["init", "-B", path.relpath("%s/backup" % node.base_dir, self.dir_path)]),
			six.b("ERROR: -B, --backup-path must be an absolute path\n")
		)


if __name__ == '__main__':
	unittest.main()
