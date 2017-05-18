# you need os for unittest to work
import os
from sys import exit
import subprocess
import shutil
import six
from testgres import get_new_node
import hashlib
import re


idx_ptrack = {
't_heap': {
    'type': 'heap'
    },
't_btree': {
    'type': 'btree',
    'column': 'text',
    'relation': 't_heap'
    },
't_spgist': {
    'type': 'spgist',
    'column': 'text',
    'relation': 't_heap'
    },
't_brin': {
    'type': 'brin',
    'column': 'text',
    'relation': 't_heap'
    },
't_gist': {
    'type': 'gist',
    'column': 'tsvector',
    'relation': 't_heap'
    },
't_gin': {
    'type': 'gin',
    'column': 'tsvector',
    'relation': 't_heap'
    },
}

warning = """
Wrong splint in show_pb
Original Header:
{header}
Original Body:
{body}
Splitted Header
{header_split}
Splitted Body
{body_split}
"""

# You can lookup error message and cmdline in exception object attributes
class ProbackupException(Exception):
    def __init__(self, message, cmd):
#        print message
#        self.message = repr(message).strip("'")
        self.message = message
        self.cmd = cmd
    #need that to make second raise
    def __str__(self):
        return '\n ERROR: {0}\n CMD: {1}'.format(repr(self.message), self.cmd)


def dir_files(base_dir):
    out_list = []
    for dir_name, subdir_list, file_list in os.walk(base_dir):
        if dir_name != base_dir:
            out_list.append(os.path.relpath(dir_name, base_dir))
        for fname in file_list:
            out_list.append(os.path.relpath(os.path.join(dir_name, fname), base_dir))
    out_list.sort()
    return out_list


class ShowBackup(object):
    def __init__(self, line):
        self.counter = 0

        print split_line
        self.id = self.get_inc(split_line)
        # TODO: parse to datetime
        if len(split_line) == 12:
            self.recovery_time = "%s %s" % (self.get_inc(split_line), self.get_inc(split_line))
        # if recovery time is '----'
        else:
            self.recovery_time = self.get_inc(split_line)
        self.mode = self.get_inc(split_line)
#        print self.mode
        self.wal = self.get_inc(split_line)
        self.cur_tli = self.get_inc(split_line)
        # slash
        self.counter += 1
        self.parent_tli = self.get_inc(split_line)
        # TODO: parse to interval
        self.time = self.get_inc(split_line)
        # TODO: maybe rename to size?
        self.data = self.get_inc(split_line)
        self.start_lsn = self.get_inc(split_line)
        self.stop_lsn = self.get_inc(split_line)
        self.status = self.get_inc(split_line)

    def get_inc(self, split_line):
#        self.counter += 1
#        return split_line[self.counter - 1]
         return split_line


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

        self.dir_path = os.path.dirname(os.path.realpath(__file__))
        try:
            os.makedirs(os.path.join(self.dir_path, "tmp_dirs"))
        except:
            pass
        self.probackup_path = os.path.abspath(os.path.join(
            self.dir_path,
            "../pg_probackup"
        ))

    def arcwal_dir(self, node):
        return "%s/backup/wal" % node.base_dir

    def backup_dir(self, node):
        return os.path.abspath("%s/backup" % node.base_dir)

    def make_simple_node(self, base_dir=None, set_replication=False,
                        set_archiving=False, initdb_params=[], pg_options={}):
        real_base_dir = os.path.join(self.dir_path, base_dir)
        shutil.rmtree(real_base_dir, ignore_errors=True)

        node = get_new_node('test', base_dir=real_base_dir)
        node.init(initdb_params=initdb_params)

        # Sane default parameters, not a shit with fsync = off from testgres
        node.append_conf("postgresql.auto.conf", "{0} = {1}".format('fsync', 'on'))
        node.append_conf("postgresql.auto.conf", "{0} = {1}".format('wal_level', 'minimal'))

        # Apply given parameters
        for key, value in six.iteritems(pg_options):
            node.append_conf("postgresql.auto.conf", "%s = %s" % (key, value))

        # Allow replication in pg_hba.conf
        if set_replication:
            node.set_replication_conf()
        # Setup archiving for node
        if set_archiving:
            self.set_archiving_conf(node, self.arcwal_dir(node))
        return node

    def create_tblspace_in_node(self, node, tblspc_name, cfs=False):
        res = node.execute(
            "postgres", "select exists (select 1 from pg_tablespace where spcname = '{0}')".format(
                tblspc_name))
        # Check that tablespace with name 'tblspc_name' do not exists already
        self.assertEqual(res[0][0], False, 'Tablespace "{0}" already exists'.format(tblspc_name))

        tblspc_path = os.path.join(node.base_dir, '{0}'.format(tblspc_name))
        cmd = "CREATE TABLESPACE {0} LOCATION '{1}'".format(tblspc_name, tblspc_path)
        if cfs:
            cmd += " with (compression=true)"
        os.makedirs(tblspc_path)
        res = node.psql("postgres", cmd)
        # Check that tablespace was successfully created
        self.assertEqual(res[0], 0, 'Failed to create tablespace with cmd: {0}'.format(cmd))


    def get_fork_size(self, node, fork_name):
        return node.execute("postgres",
            "select pg_relation_size('{0}')/8192".format(fork_name))[0][0]

    def get_fork_path(self, node, fork_name):
        return os.path.join(node.base_dir, 'data',
            node.execute("postgres", "select pg_relation_filepath('{0}')".format(fork_name))[0][0])

    def get_md5_per_page_for_fork(self, file, size):
        file = os.open(file, os.O_RDONLY)
        offset = 0
        md5_per_page = {}
        for page in range(size):
            md5_per_page[page] = hashlib.md5(os.read(file, 8192)).hexdigest()
            offset += 8192
            os.lseek(file, offset, 0)
        os.close(file)
        return md5_per_page

    def get_ptrack_bits_per_page_for_fork(self, file, size):
        ptrack_bits_for_fork = []
        byte_size = os.path.getsize(file + '_ptrack')
        byte_size_minus_header = byte_size - 24
        file = os.open(file + '_ptrack', os.O_RDONLY)
        os.lseek(file, 24, 0)
        lot_of_bytes = os.read(file, byte_size_minus_header)
        for byte in lot_of_bytes:
            byte_inverted = bin(ord(byte))[2:].rjust(8, '0')[::-1]
#            byte_to_bits = (byte >> x) & 1 for x in range(7, -1, -1)
            for bit in byte_inverted:
                if len(ptrack_bits_for_fork) < size:
                    ptrack_bits_for_fork.append(int(bit))
        os.close(file)
        return ptrack_bits_for_fork

    def check_ptrack_sanity(self, idx_dict):
        success = True
        if idx_dict['new_size'] > idx_dict['old_size']:
            size = idx_dict['new_size']
        else:
            size = idx_dict['old_size']
        for PageNum in range(size):
            if PageNum not in idx_dict['old_pages']:
                # Page was not present before, meaning that relation got bigger
                # Ptrack should be equal to 1
                if idx_dict['ptrack'][PageNum] != 1:
                    print 'Page Number {0} of type {1} was added, but ptrack value is {2}. THIS IS BAD'.format(
                        PageNum, idx_dict['type'], idx_dict['ptrack'][PageNum])
                    print idx_dict
                    success = False
                continue
            if PageNum not in idx_dict['new_pages']:
                # Page is not present now, meaning that relation got smaller
                # Ptrack should be equal to 0, We are not freaking out about false positive stuff
                #if idx_dict['ptrack'][PageNum] != 0:
                #    print 'Page Number {0} of type {1} was deleted, but ptrack value is {2}'.format(
                #        PageNum, idx_dict['type'], idx_dict['ptrack'][PageNum])
                continue
            # Ok, all pages in new_pages that do not have corresponding page in old_pages
            # are been dealt with. We can now safely proceed to comparing old and new pages 
            if idx_dict['new_pages'][PageNum] != idx_dict['old_pages'][PageNum]:
                # Page has been changed, meaning that ptrack should be equal to 1
                if idx_dict['ptrack'][PageNum] != 1:
                    print 'Page Number {0} of type {1} was changed, but ptrack value is {2}. THIS IS BAD'.format(
                        PageNum, idx_dict['type'], idx_dict['ptrack'][PageNum])
                    print idx_dict
                    if PageNum == 0 and idx_dict['type'] == 'spgist':
                        print 'SPGIST is a special snowflake, so don`t fret about losing ptrack for blknum 0'
                        continue
                    success = False
            else:
                # Page has not been changed, meaning that ptrack should be equal to 0
                if idx_dict['ptrack'][PageNum] != 0:
                    print 'Page Number {0} of type {1} was not changed, but ptrack value is {2}'.format(
                        PageNum, idx_dict['type'], idx_dict['ptrack'][PageNum])
                    print idx_dict
            self.assertEqual(success, True)

    def check_ptrack_recovery(self, idx_dict):
        success = True
        size = idx_dict['size']
        for PageNum in range(size):
            if idx_dict['ptrack'][PageNum] != 1:
                print 'Recovery for Page Number {0} of Type {1} was conducted, but ptrack value is {2}. THIS IS BAD'.format(
                    PageNum, idx_dict['type'], idx_dict['ptrack'][PageNum])
                print idx_dict
                success = False
            self.assertEqual(success, True)

    def check_ptrack_clean(self, idx_dict, size):
        success = True
        for PageNum in range(size):
            if idx_dict['ptrack'][PageNum] != 0:
                print 'Ptrack for Page Number {0} of Type {1} should be clean, but ptrack value is {2}. THIS IS BAD'.format(
                    PageNum, idx_dict['type'], idx_dict['ptrack'][PageNum])
                print idx_dict
                success = False
            self.assertEqual(success, True)

    def run_pb(self, command, async=False):
        try:
            # print [self.probackup_path] + command
            if async is True:
                return subprocess.Popen(
                    [self.probackup_path] + command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=self.test_env
                )
            else:
                output = subprocess.check_output(
                    [self.probackup_path] + command,
                    stderr=subprocess.STDOUT,
                    env=self.test_env
                )
            if command[0] == 'backup':
                if '-q' in command or '--quiet' in command:
                    return None
                elif '-v' in command or '--verbose' in command:
                    return output
                else:
                    # return backup ID
                    return output.split()[2]
            else:
                return output
        except subprocess.CalledProcessError as e:
            raise  ProbackupException(e.output, e.cmd)

    def init_pb(self, node):

        return self.run_pb([
            "init",
            "-B", self.backup_dir(node),
            "-D", node.data_dir
        ])

    def clean_pb(self, node):
        shutil.rmtree(self.backup_dir(node), ignore_errors=True)

    def backup_pb(self, node=None, data_dir=None, backup_dir=None, backup_type="full", options=[], async=False):
        if data_dir is None:
            data_dir = node.data_dir
        if backup_dir is None:
            backup_dir = self.backup_dir(node)

        cmd_list = [
            "backup",
            "-B", backup_dir,
            "-D", data_dir,
            "-p", "%i" % node.port,
            "-d", "postgres"
        ]
        if backup_type:
            cmd_list += ["-b", backup_type]

        return self.run_pb(cmd_list + options, async)

    def restore_pb(self, node=None, backup_dir=None, data_dir=None, id=None, options=[]):
        if data_dir is None:
            data_dir = node.data_dir
        if backup_dir is None:
            backup_dir = self.backup_dir(node)

        cmd_list = [
            "restore",
            "-B", backup_dir,
            "-D", data_dir
        ]
        if id:
            cmd_list += ["-i", id]

        return self.run_pb(cmd_list + options)

    def show_pb(self, node, id=None, options=[], as_text=False):
        backup_list = []
        specific_record = {}
        cmd_list = [
            "show",
            "-B", self.backup_dir(node),
        ]
        if id:
            cmd_list += ["-i", id]

        if as_text:
            # You should print it when calling as_text=true
            return self.run_pb(cmd_list + options)

        # get show result as list of lines
        show_splitted = self.run_pb(cmd_list + options).splitlines()
        if id is None:
            # cut header(ID, Mode, etc) from show as single string
            header = show_splitted[1:2][0]
            # cut backup records from show as single list with string for every backup record
            body = show_splitted[3:]
            # inverse list so oldest record come first
            body = body[::-1]
            # split string in list with string for every header element
            header_split = re.split("  +", header)
            # CRUNCH, remove last item, because it`s empty, like that ''
            header_split.pop()
            for backup_record in body:
                # split string in list with string for every backup record element
                backup_record_split = re.split("  +", backup_record)
                # CRUNCH, remove last item, because it`s empty, like that ''
                backup_record_split.pop()
                if len(header_split) != len(backup_record_split):
                    print warning.format(
                        header=header, body=body,
                        header_split=header_split, body_split=backup_record_split)
                    exit(1)
                new_dict = dict(zip(header_split, backup_record_split))
                backup_list.append(new_dict)
            return backup_list
        else:
            # cut out empty lines and lines started with #
            # and other garbage then reconstruct it as dictionary
            # print show_splitted
            sanitized_show = [item for item in show_splitted if item]
            sanitized_show = [item for item in sanitized_show if not item.startswith('#')]
            # print sanitized_show
            for line in sanitized_show:
                name, var = line.partition(" = ")[::2]
                var = var.strip('"')
                var = var.strip("'")
                specific_record[name.strip()] = var
            return specific_record

    def validate_pb(self, node, id=None, options=[]):
        cmd_list = [
            "validate",
            "-B", self.backup_dir(node),
        ]
        if id:
            cmd_list += ["-i", id]

        # print(cmd_list)
        return self.run_pb(cmd_list + options)

    def delete_pb(self, node, id=None, options=[]):
        cmd_list = [
            "delete",
            "-B", self.backup_dir(node),
        ]
        if id:
            cmd_list += ["-i", id]

        # print(cmd_list)
        return self.run_pb(cmd_list + options)

    def delete_expired(self, node, options=[]):
        cmd_list = [
            "delete", "--expired",
            "-B", self.backup_dir(node),
        ]
        return self.run_pb(cmd_list + options)

    def show_config(self, node):
        out_dict = {}
        cmd_list = [
            "show-config",
            "-B", self.backup_dir(node),
        ]
        res = self.run_pb(cmd_list).splitlines()
        for line in res:
            if not line.startswith('#'):
                name, var = line.partition(" = ")[::2]
                out_dict[name] = var
        return out_dict


    def get_recovery_conf(self, node):
        out_dict = {}
        with open(os.path.join(node.data_dir, "recovery.conf"), "r") as recovery_conf:
            for line in recovery_conf:
                try:
                    key, value = line.split("=")
                except:
                    continue
                out_dict[key.strip()] = value.strip(" '").replace("'\n", "")
        return out_dict

    def set_archiving_conf(self, node, archive_dir):
        node.append_conf(
                "postgresql.auto.conf",
                "wal_level = archive"
                )
        node.append_conf(
                "postgresql.auto.conf",
                "archive_mode = on"
                )
        if os.name == 'posix':
            node.append_conf(
                    "postgresql.auto.conf",
                    "archive_command = 'test ! -f {0}/%f && cp %p {0}/%f'".format(archive_dir)
                    )
        elif os.name == 'nt':
            node.append_conf(
                    "postgresql.auto.conf",
                    "archive_command = 'copy %p {0}\\%f'".format(archive_dir)
                    )

    def wrong_wal_clean(self, node, wal_size):
        wals_dir = os.path.join(self.backup_dir(node), "wal")
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f))]
        wals.sort()
        file_path = os.path.join(wals_dir, wals[-1])
        if os.path.getsize(file_path) != wal_size:
            os.remove(file_path)

    def guc_wal_segment_size(self, node):
        var = node.execute("postgres", "select setting from pg_settings where name = 'wal_segment_size'")
        return int(var[0][0]) * self.guc_wal_block_size(node)

    def guc_wal_block_size(self, node):
        var = node.execute("postgres", "select setting from pg_settings where name = 'wal_block_size'")
        return int(var[0][0])

#    def ptrack_node(self, ptrack_enable=False, wal_level='minimal', max_wal_senders='2', allow_replication=True)
