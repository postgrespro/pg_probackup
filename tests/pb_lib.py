import os
from os import path
import subprocess
import shutil
import six
from testgres import get_new_node


def dir_files(base_dir):
	out_list = []
	for dir_name, subdir_list, file_list in os.walk(base_dir):
		if dir_name != base_dir:
			out_list.append(path.relpath(dir_name, base_dir))
		for fname in file_list:
			out_list.append(path.relpath(path.join(dir_name, fname), base_dir))
	out_list.sort()
	return out_list


class ProbackupTest(object):
	def __init__(self, *args, **kwargs):
		super(ProbackupTest, self).__init__(*args, **kwargs)
		self.dir_path = path.dirname(os.path.realpath(__file__))
		try:
			os.makedirs(path.join(self.dir_path, "tmp_dirs"))
		except:
			pass
		self.probackup_path = os.path.abspath(path.join(
			self.dir_path,
			"../pg_probackup"
		))

	def arcwal_dir(self, node):
		return "%s/backup/wal" % node.base_dir

	def backup_dir(self, node):
		return os.path.abspath("%s/backup" % node.base_dir)

	def make_bnode(self, name, base_dir=None):
		node = get_new_node('test', base_dir=path.join(self.dir_path, base_dir))
		try:
			node.cleanup()
		except:
			pass
		shutil.rmtree(self.backup_dir(node), ignore_errors=True)
		node.init()

		node.append_conf("postgresql.conf", "wal_level = hot_standby")
		node.append_conf("postgresql.conf", "archive_mode = on")
		node.append_conf(
			"postgresql.conf",
			"""archive_command = 'cp "%%p" "%s/%%f"'""" % os.path.abspath(self.arcwal_dir(node))
		)
		return node

	def run_pb(self, command):
		try:
			return subprocess.check_output(
				[self.probackup_path] + command,
				stderr=subprocess.STDOUT
			)
		except subprocess.CalledProcessError as err:
			return err.output

	def init_pb(self, node):
		return self.run_pb([
			"init",
			"-B", self.backup_dir(node),
			"-D", node.data_dir
		])

	def clean_pb(self, node):
		shutil.rmtree(self.backup_dir(node), ignore_errors=True)

	def backup_pb(self, node, backup_type="full", options=[]):
		cmd_list = [
			"backup",
			"-D", node.data_dir,
			"-B", self.backup_dir(node),
			"-p", "%i" % node.port,
			"-d", "postgres"
		]
		if backup_type:
			cmd_list += ["-b", backup_type]

		# print(cmd_list)
		return self.run_pb(cmd_list + options)

	def show_pb(self, node, id=None, options=[]):
		cmd_list = [
			"-B", self.backup_dir(node),
			"show",
		]
		if id:
			cmd_list += [id]

		# print(cmd_list)
		return self.run_pb(options + cmd_list)

	def validate_pb(self, node, id=None, options=[]):
		cmd_list = [
			"-B", self.backup_dir(node),
			"validate",
		]
		if id:
			cmd_list += [id]

		# print(cmd_list)
		return self.run_pb(options + cmd_list)
