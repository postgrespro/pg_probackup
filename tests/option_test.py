import unittest
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

	def test_help_1(self):
		"""help options"""
		with open(path.join(self.dir_path, "expected/option_help.out"), "rb") as help_out:
			self.assertEqual(
				self.run_pb(["--help"]),
				help_out.read()
			)

	def test_version_2(self):
		"""help options"""
		with open(path.join(self.dir_path, "expected/option_version.out"), "rb") as version_out:
			self.assertEqual(
				self.run_pb(["--version"]),
				version_out.read()
			)

	def test_without_backup_path_3(self):
		"""backup command failure without backup mode option"""
		self.assertEqual(
			self.run_pb(["backup", "-b", "full"]),
			six.b("ERROR: required parameter not specified: BACKUP_PATH (-B, --backup-path)\n")
		)

	def test_options_4(self):
		node = self.make_bnode('test_options_4', base_dir="tmp_dirs/option/option_common")
		try:
			node.stop()
		except:
			pass
		self.assertEqual(self.init_pb(node), six.b(""))

		# backup command failure without backup mode option
		self.assertEqual(
			self.run_pb(["backup", "-B", self.backup_dir(node), "-D", node.data_dir]),
			six.b("ERROR: Required parameter not specified: BACKUP_MODE (-b, --backup-mode)\n")
		)

		# backup command failure with invalid backup mode option
		self.assertEqual(
			self.run_pb(["backup", "-b", "bad", "-B", self.backup_dir(node)]),
			six.b('ERROR: invalid backup-mode "bad"\n')
		)

		# delete failure without ID
		self.assertEqual(
			self.run_pb(["delete", "-B", self.backup_dir(node)]),
			six.b("ERROR: required backup ID not specified\n")
		)

		node.start()

		# syntax error in pg_probackup.conf
		with open(path.join(self.backup_dir(node), "pg_probackup.conf"), "a") as conf:
			conf.write(" = INFINITE\n")

		self.assertEqual(
			self.backup_pb(node),
			six.b('ERROR: syntax error in " = INFINITE"\n')
		)

		self.clean_pb(node)
		self.assertEqual(self.init_pb(node), six.b(""))

		# invalid value in pg_probackup.conf
		with open(path.join(self.backup_dir(node), "pg_probackup.conf"), "a") as conf:
			conf.write("BACKUP_MODE=\n")

		self.assertEqual(
			self.backup_pb(node, backup_type=None),
			six.b('ERROR: invalid backup-mode ""\n')
		)

		self.clean_pb(node)
		self.assertEqual(self.init_pb(node), six.b(""))

		# TODO: keep data generations

		# invalid value in pg_probackup.conf
		with open(path.join(self.backup_dir(node), "pg_probackup.conf"), "a") as conf:
			conf.write("SMOOTH_CHECKPOINT=FOO\n")

		self.assertEqual(
			self.backup_pb(node),
			six.b("ERROR: option -C, --smooth-checkpoint should be a boolean: 'FOO'\n")
		)

		self.clean_pb(node)
		self.assertEqual(self.init_pb(node), six.b(""))

		# invalid option in pg_probackup.conf
		with open(path.join(self.backup_dir(node), "pg_probackup.conf"), "a") as conf:
			conf.write("TIMELINEID=1\n")

		self.assertEqual(
			self.backup_pb(node),
			six.b('ERROR: invalid option "TIMELINEID"\n')
		)

		self.clean_pb(node)
		self.assertEqual(self.init_pb(node), six.b(""))

		node.stop()
