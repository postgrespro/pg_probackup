import os
from os import path, listdir
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


class ShowBackup(object):
	def __init__(self, split_line):
		self.id = split_line[0]
		# TODO: parse to datetime
		self.recovery_time = "%s %s" % (split_line[1], split_line[2])
		self.mode = split_line[3]
		self.cur_tli = split_line[4]
		self.parent_tli = split_line[6]
		# TODO: parse to interval
		self.time = split_line[7]
		# TODO: maybe rename to size?
		self.data = split_line[8]
		self.status = split_line[9]


class ProbackupTest(object):
	def __init__(self, *args, **kwargs):
		super(ProbackupTest, self).__init__(*args, **kwargs)
		self.test_env = os.environ.copy()
		envs_list = [
			"LANGUAGE",
			"LC_ALL",
			"PGCONNECT_TIMEOUT",
			"PGDATA",
			"PGDATABASE",
			"PGHOSTADDR",
			"PGREQUIRESSL",
			"PGSERVICE",
			"PGSSLMODE",
			"PGUSER",
			"PGPORT",
			"PGHOST"
		]

		for e in envs_list:
			try:
				del self.test_env[e]
			except:
				pass

		self.test_env["LC_MESSAGES"] = "C"
		self.test_env["LC_TIME"] = "C"

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

	def make_bnode(self, name, base_dir=None, options={}):
		real_base_dir = path.join(self.dir_path, base_dir)
		shutil.rmtree(real_base_dir, ignore_errors=True)
		node = get_new_node('test', base_dir=real_base_dir)
		node.init()
		# node.init(initdb_params=[
		# 	"-x", "0x123456789ABCDEF0",
		# 	"-m", "0x23456789ABCDEF01",
		# 	"-o", "0x3456789ABCDEF012"
		# ])

		node.append_conf("postgresql.conf", "wal_level = hot_standby")
		node.append_conf("postgresql.conf", "archive_mode = on")
		node.append_conf(
			"postgresql.conf",
			"""archive_command = 'cp "%%p" "%s/%%f"'""" % os.path.abspath(self.arcwal_dir(node))
		)

		for key, value in six.iteritems(options):
			node.append_conf("postgresql.conf", "%s = %s" % (key, value))

		return node

	def run_pb(self, command):
		try:
			return subprocess.check_output(
				[self.probackup_path] + command,
				stderr=subprocess.STDOUT,
				env=self.test_env
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

	def restore_pb(self, node, id=None, options=[]):
		cmd_list = [
			"-D", node.data_dir,
			"-B", self.backup_dir(node),
			"restore"
		]
		if id:
			cmd_list.append(id)

		# print(cmd_list)
		return self.run_pb(cmd_list + options)

	def show_pb(self, node, id=None, options=[], as_text=False):
		cmd_list = [
			"-B", self.backup_dir(node),
			"show",
		]
		if id:
			cmd_list += [id]

		# print(cmd_list)
		if as_text:
			return self.run_pb(options + cmd_list)
		elif id is None:
			return [ShowBackup(line.split()) for line in self.run_pb(options + cmd_list).splitlines()[3:]]
		else:
			return dict([
				line.split(six.b("="))
				for line in self.run_pb(options + cmd_list).splitlines()
				if line[0] != six.b("#")[0]
			])

	def validate_pb(self, node, id, options=[]):
		cmd_list = [
			"-B", self.backup_dir(node),
			"validate",
		]
		if id:
			cmd_list += [id]

		# print(cmd_list)
		return self.run_pb(options + cmd_list)

	def delete_pb(self, node, id=None, options=[]):
		cmd_list = [
			"-B", self.backup_dir(node),
			"delete",
		]
		if id:
			cmd_list += [id]

		# print(cmd_list)
		return self.run_pb(options + cmd_list)

	def retention_purge_pb(self, node, options=[]):
		cmd_list = [
			"-B", self.backup_dir(node),
			"retention", "purge",
		]

		return self.run_pb(options + cmd_list)

	def get_control_data(self, node):
		pg_controldata = node.get_bin_path("pg_controldata")
		out_data = {}
		lines = subprocess.check_output(
			[pg_controldata] + ["-D", node.data_dir],
			stderr=subprocess.STDOUT,
			env=self.test_env
		).splitlines()
		for l in lines:
			key, value = l.split(b":", maxsplit=1)
			out_data[key.strip()] = value.strip()
		return out_data

	def get_recovery_conf(self, node):
		out_dict = {}
		with open(path.join(node.data_dir, "recovery.conf"), "r") as recovery_conf:
			for line in recovery_conf:
				try:
					key, value = line.split("=")
				except:
					continue
				out_dict[key.strip()] = value.strip(" '").replace("'\n", "")

		return out_dict

	def wrong_wal_clean(self, node, wal_size):
		wals_dir = path.join(self.backup_dir(node), "wal")
		wals = [f for f in listdir(wals_dir) if path.isfile(path.join(wals_dir, f))]
		wals.sort()
		file_path = path.join(wals_dir, wals[-1])
		if path.getsize(file_path) != wal_size:
			os.remove(file_path)

	def guc_wal_segment_size(self, node):
		var = node.execute("postgres", "select setting from pg_settings where name = 'wal_segment_size'")
		return int(var[0][0]) * self.guc_wal_block_size(node)

	def guc_wal_block_size(self, node):
		var = node.execute("postgres", "select setting from pg_settings where name = 'wal_block_size'")
		return int(var[0][0])
