# you need os for unittest to work
import os
from sys import exit, argv, version_info
import subprocess
import shutil
import six
import testgres
import hashlib
import re
import getpass
import select
import psycopg2
from time import sleep
import re
import json

idx_ptrack = {
    't_heap': {
        'type': 'heap'
    },
    't_btree': {
        'type': 'btree',
        'column': 'text',
        'relation': 't_heap'
    },
    't_seq': {
        'type': 'seq',
        'column': 't_seq',
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
    't_hash': {
        'type': 'hash',
        'column': 'id',
        'relation': 't_heap'
    },
    't_bloom': {
        'type': 'bloom',
        'column': 'id',
        'relation': 't_heap'
    }
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


def dir_files(base_dir):
    out_list = []
    for dir_name, subdir_list, file_list in os.walk(base_dir):
        if dir_name != base_dir:
            out_list.append(os.path.relpath(dir_name, base_dir))
        for fname in file_list:
            out_list.append(
                os.path.relpath(os.path.join(
                    dir_name, fname), base_dir)
                )
    out_list.sort()
    return out_list


def is_enterprise():
    # pg_config --help
    if os.name == 'posix':
        cmd = [os.environ['PG_CONFIG'], '--help']

    elif os.name == 'nt':
        cmd = [[os.environ['PG_CONFIG']], ['--help']]

    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    if b'postgrespro.ru' in p.communicate()[0]:
        return True
    else:
        return False


class ProbackupException(Exception):
    def __init__(self, message, cmd):
        self.message = message
        self.cmd = cmd

    def __str__(self):
        return '\n ERROR: {0}\n CMD: {1}'.format(repr(self.message), self.cmd)


def slow_start(self, replica=False):

    # wait for https://github.com/postgrespro/testgres/pull/50
#    self.start()
#    self.poll_query_until(
#       "postgres",
#       "SELECT not pg_is_in_recovery()",
#       suppress={testgres.NodeConnection})
    if replica:
        query = 'SELECT pg_is_in_recovery()'
    else:
        query = 'SELECT not pg_is_in_recovery()'

    self.start()
    while True:
        try:
            if self.safe_psql('postgres', query) == 't\n':
                break
        except testgres.QueryException as e:
            if 'database system is starting up' in e[0]:
                continue
            else:
                raise e


class ProbackupTest(object):
    # Class attributes
    enterprise = is_enterprise()

    def __init__(self, *args, **kwargs):
        super(ProbackupTest, self).__init__(*args, **kwargs)
        if '-v' in argv or '--verbose' in argv:
            self.verbose = True
        else:
            self.verbose = False

        self.test_env = os.environ.copy()
        envs_list = [
            'LANGUAGE',
            'LC_ALL',
            'PGCONNECT_TIMEOUT',
            'PGDATA',
            'PGDATABASE',
            'PGHOSTADDR',
            'PGREQUIRESSL',
            'PGSERVICE',
            'PGSSLMODE',
            'PGUSER',
            'PGPORT',
            'PGHOST'
        ]

        for e in envs_list:
            try:
                del self.test_env[e]
            except:
                pass

        self.test_env['LC_MESSAGES'] = 'C'
        self.test_env['LC_TIME'] = 'C'

        self.paranoia = False
        if 'PG_PROBACKUP_PARANOIA' in self.test_env:
            if self.test_env['PG_PROBACKUP_PARANOIA'] == 'ON':
                self.paranoia = True

        self.archive_compress = False
        if 'ARCHIVE_COMPRESSION' in self.test_env:
            if self.test_env['ARCHIVE_COMPRESSION'] == 'ON':
                self.archive_compress = True
        try:
            testgres.configure_testgres(
                cache_initdb=False,
                cached_initdb_dir=False,
                cache_pg_config=False,
                node_cleanup_full=False)
        except:
            pass

        self.helpers_path = os.path.dirname(os.path.realpath(__file__))
        self.dir_path = os.path.abspath(
            os.path.join(self.helpers_path, os.pardir)
            )
        self.tmp_path = os.path.abspath(
            os.path.join(self.dir_path, 'tmp_dirs')
            )
        try:
            os.makedirs(os.path.join(self.dir_path, 'tmp_dirs'))
        except:
            pass

        self.user = self.get_username()
        self.probackup_path = None
        if 'PGPROBACKUPBIN' in self.test_env:
            if (
                os.path.isfile(self.test_env["PGPROBACKUPBIN"]) and
                os.access(self.test_env["PGPROBACKUPBIN"], os.X_OK)
            ):
                self.probackup_path = self.test_env["PGPROBACKUPBIN"]
            else:
                if self.verbose:
                    print('PGPROBACKUPBIN is not an executable file')

        if not self.probackup_path:
            probackup_path_tmp = os.path.join(
                testgres.get_pg_config()['BINDIR'], 'pg_probackup')

            if os.path.isfile(probackup_path_tmp):
                if not os.access(probackup_path_tmp, os.X_OK):
                    print('{0} is not an executable file'.format(
                        probackup_path_tmp))
                else:
                    self.probackup_path = probackup_path_tmp

        if not self.probackup_path:
            probackup_path_tmp = os.path.abspath(os.path.join(
                self.dir_path, '../pg_probackup'))

            if os.path.isfile(probackup_path_tmp):
                if not os.access(probackup_path_tmp, os.X_OK):
                    print('{0} is not an executable file'.format(
                        probackup_path_tmp))
                else:
                    self.probackup_path = probackup_path_tmp

        if not self.probackup_path:
            print('pg_probackup binary is not found')
            exit(1)

        if os.name == 'posix':
            os.environ['PATH'] = os.path.dirname(
                self.probackup_path) + ':' + os.environ['PATH']

        elif os.name == 'nt':
            os.environ['PATH'] = os.path.dirname(
                self.probackup_path) + ';' + os.environ['PATH']

        self.probackup_old_path = None

        if 'PGPROBACKUPBIN_OLD' in self.test_env:
            if (
                os.path.isfile(self.test_env['PGPROBACKUPBIN_OLD']) and
                os.access(self.test_env['PGPROBACKUPBIN_OLD'], os.X_OK)
            ):
                self.probackup_old_path = self.test_env['PGPROBACKUPBIN_OLD']
            else:
                if self.verbose:
                    print('PGPROBACKUPBIN_OLD is not an executable file')

        self.remote = False
        self.remote_host = None
        self.remote_port = None
        self.remote_user = None

        if 'PGPROBACKUP_SSH_REMOTE' in self.test_env:
            self.remote = True

#            if 'PGPROBACKUP_SSH_HOST' in self.test_env:
#                self.remote_host = self.test_env['PGPROBACKUP_SSH_HOST']
#            else
#                print('PGPROBACKUP_SSH_HOST is not set')
#                exit(1)
#
#            if 'PGPROBACKUP_SSH_PORT' in self.test_env:
#                self.remote_port = self.test_env['PGPROBACKUP_SSH_PORT']
#            else
#                print('PGPROBACKUP_SSH_PORT is not set')
#                exit(1)
#
#            if 'PGPROBACKUP_SSH_USER' in self.test_env:
#                self.remote_user = self.test_env['PGPROBACKUP_SSH_USER']
#            else
#                print('PGPROBACKUP_SSH_USER is not set')
#                exit(1)



    def make_simple_node(
            self,
            base_dir=None,
            set_replication=False,
            initdb_params=[],
            pg_options={}):

        real_base_dir = os.path.join(self.tmp_path, base_dir)
        shutil.rmtree(real_base_dir, ignore_errors=True)
        os.makedirs(real_base_dir)

        node = testgres.get_new_node('test', base_dir=real_base_dir)
        # bound method slow_start() to 'node' class instance
        node.slow_start = slow_start.__get__(node)
        node.should_rm_dirs = True
        node.init(
           initdb_params=initdb_params, allow_streaming=set_replication)

        # Sane default parameters
        node.append_conf('postgresql.auto.conf', 'max_connections = 100')
        node.append_conf('postgresql.auto.conf', 'shared_buffers = 10MB')
        node.append_conf('postgresql.auto.conf', 'fsync = off')
        node.append_conf('postgresql.auto.conf', 'wal_level = logical')
        node.append_conf('postgresql.auto.conf', 'hot_standby = off')

        node.append_conf(
            'postgresql.auto.conf', "log_line_prefix = '%t [%p]: [%l-1] '")
        node.append_conf('postgresql.auto.conf', 'log_statement = none')
        node.append_conf('postgresql.auto.conf', 'log_duration = on')
        node.append_conf(
            'postgresql.auto.conf', 'log_min_duration_statement = 0')
        node.append_conf('postgresql.auto.conf', 'log_connections = on')
        node.append_conf('postgresql.auto.conf', 'log_disconnections = on')

        # Apply given parameters
        for key, value in six.iteritems(pg_options):
            node.append_conf('postgresql.auto.conf', '%s = %s' % (key, value))

        # Allow replication in pg_hba.conf
        if set_replication:
            node.append_conf(
                'postgresql.auto.conf',
                'max_wal_senders = 10')

        return node

    def create_tblspace_in_node(self, node, tblspc_name, tblspc_path=None, cfs=False):
        res = node.execute(
            'postgres',
            'select exists'
            " (select 1 from pg_tablespace where spcname = '{0}')".format(
                tblspc_name)
            )
        # Check that tablespace with name 'tblspc_name' do not exists already
        self.assertFalse(
            res[0][0],
            'Tablespace "{0}" already exists'.format(tblspc_name)
            )

        if not tblspc_path:
            tblspc_path = os.path.join(
                node.base_dir, '{0}'.format(tblspc_name))
        cmd = "CREATE TABLESPACE {0} LOCATION '{1}'".format(
            tblspc_name, tblspc_path)
        if cfs:
            cmd += ' with (compression=true)'

        if not os.path.exists(tblspc_path):
            os.makedirs(tblspc_path)
        res = node.safe_psql('postgres', cmd)
        # Check that tablespace was successfully created
        # self.assertEqual(
        #     res[0], 0,
        #     'Failed to create tablespace with cmd: {0}'.format(cmd))

    def get_tblspace_path(self, node, tblspc_name):
        return os.path.join(node.base_dir, tblspc_name)

    def get_fork_size(self, node, fork_name):
        return node.execute(
            'postgres',
            "select pg_relation_size('{0}')/8192".format(fork_name))[0][0]

    def get_fork_path(self, node, fork_name):
        return os.path.join(
            node.base_dir, 'data', node.execute(
                'postgres',
                "select pg_relation_filepath('{0}')".format(
                    fork_name))[0][0]
            )

    def get_md5_per_page_for_fork(self, file, size_in_pages):
        pages_per_segment = {}
        md5_per_page = {}
        nsegments = size_in_pages/131072
        if size_in_pages % 131072 != 0:
            nsegments = nsegments + 1

        size = size_in_pages
        for segment_number in range(nsegments):
            if size - 131072 > 0:
                pages_per_segment[segment_number] = 131072
            else:
                pages_per_segment[segment_number] = size
            size = size - 131072

        for segment_number in range(nsegments):
            offset = 0
            if segment_number == 0:
                file_desc = os.open(file, os.O_RDONLY)
                start_page = 0
                end_page = pages_per_segment[segment_number]
            else:
                file_desc = os.open(
                    file+'.{0}'.format(segment_number), os.O_RDONLY
                    )
                start_page = max(md5_per_page)+1
                end_page = end_page + pages_per_segment[segment_number]

            for page in range(start_page, end_page):
                md5_per_page[page] = hashlib.md5(
                    os.read(file_desc, 8192)).hexdigest()
                offset += 8192
                os.lseek(file_desc, offset, 0)
            os.close(file_desc)

        return md5_per_page

    def get_ptrack_bits_per_page_for_fork(self, node, file, size=[]):

        if self.get_pgpro_edition(node) == 'enterprise':
            if self.get_version(node) < self.version_to_num('10.0'):
                header_size = 48
            else:
                header_size = 24
        else:
            header_size = 24
        ptrack_bits_for_fork = []

        page_body_size = 8192-header_size
        byte_size = os.path.getsize(file + '_ptrack')
        npages = byte_size/8192
        if byte_size % 8192 != 0:
            print('Ptrack page is not 8k aligned')
            sys.exit(1)

        file = os.open(file + '_ptrack', os.O_RDONLY)

        for page in range(npages):
            offset = 8192*page+header_size
            os.lseek(file, offset, 0)
            lots_of_bytes = os.read(file, page_body_size)
            byte_list = [
                lots_of_bytes[i:i+1] for i in range(len(lots_of_bytes))
                ]
            for byte in byte_list:
                # byte_inverted = bin(int(byte, base=16))[2:][::-1]
                # bits = (byte >> x) & 1 for x in range(7, -1, -1)
                byte_inverted = bin(ord(byte))[2:].rjust(8, '0')[::-1]
                for bit in byte_inverted:
                    # if len(ptrack_bits_for_fork) < size:
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
                    if self.verbose:
                        print(
                            'File: {0}\n Page Number {1} of type {2} was added,'
                            ' but ptrack value is {3}. THIS IS BAD'.format(
                                idx_dict['path'],
                                PageNum, idx_dict['type'],
                                idx_dict['ptrack'][PageNum])
                        )
                        # print(idx_dict)
                    success = False
                continue
            if PageNum not in idx_dict['new_pages']:
                # Page is not present now, meaning that relation got smaller
                # Ptrack should be equal to 1,
                # We are not freaking out about false positive stuff
                if idx_dict['ptrack'][PageNum] != 1:
                    if self.verbose:
                        print(
                            'File: {0}\n Page Number {1} of type {2} was deleted,'
                            ' but ptrack value is {3}. THIS IS BAD'.format(
                                idx_dict['path'],
                                PageNum, idx_dict['type'],
                                idx_dict['ptrack'][PageNum])
                        )
                continue

            # Ok, all pages in new_pages that do not have
            # corresponding page in old_pages are been dealt with.
            # We can now safely proceed to comparing old and new pages
            if idx_dict['new_pages'][
                    PageNum] != idx_dict['old_pages'][PageNum]:
                # Page has been changed,
                # meaning that ptrack should be equal to 1
                if idx_dict['ptrack'][PageNum] != 1:
                    if self.verbose:
                        print(
                            'File: {0}\n Page Number {1} of type {2} was changed,'
                            ' but ptrack value is {3}. THIS IS BAD'.format(
                                idx_dict['path'],
                                PageNum, idx_dict['type'],
                                idx_dict['ptrack'][PageNum])
                        )
                        print(
                            '  Old checksumm: {0}\n'
                            '  New checksumm: {1}'.format(
                                idx_dict['old_pages'][PageNum],
                                idx_dict['new_pages'][PageNum])
                        )

                    if PageNum == 0 and idx_dict['type'] == 'spgist':
                        if self.verbose:
                            print(
                                'SPGIST is a special snowflake, so don`t '
                                'fret about losing ptrack for blknum 0'
                            )
                        continue
                    success = False
            else:
                # Page has not been changed,
                # meaning that ptrack should be equal to 0
                if idx_dict['ptrack'][PageNum] != 0:
                    if self.verbose:
                        print(
                            'File: {0}\n Page Number {1} of type {2} was not changed,'
                            ' but ptrack value is {3}'.format(
                                idx_dict['path'],
                                PageNum, idx_dict['type'],
                                idx_dict['ptrack'][PageNum]
                            )
                        )
            return success
            # self.assertTrue(
            #    success, 'Ptrack has failed to register changes in data files'
            # )

    def check_ptrack_recovery(self, idx_dict):
        size = idx_dict['size']
        for PageNum in range(size):
            if idx_dict['ptrack'][PageNum] != 1:
                self.assertTrue(
                    False,
                    'Recovery for Page Number {0} of Type {1}'
                    ' was conducted, but ptrack value is {2}.'
                    ' THIS IS BAD\n IDX_DICT: {3}'.format(
                        PageNum, idx_dict['type'],
                        idx_dict['ptrack'][PageNum],
                        idx_dict
                    )
                )

    def check_ptrack_clean(self, idx_dict, size):
        for PageNum in range(size):
            if idx_dict['ptrack'][PageNum] != 0:
                self.assertTrue(
                    False,
                    'Ptrack for Page Number {0} of Type {1}'
                    ' should be clean, but ptrack value is {2}.'
                    '\n THIS IS BAD\n IDX_DICT: {3}'.format(
                        PageNum,
                        idx_dict['type'],
                        idx_dict['ptrack'][PageNum],
                        idx_dict
                    )
                )

    def run_pb(self, command, asynchronous=False, gdb=False, old_binary=False):
        if not self.probackup_old_path and old_binary:
            print('PGPROBACKUPBIN_OLD is not set')
            exit(1)

        if old_binary:
            binary_path = self.probackup_old_path
        else:
            binary_path = self.probackup_path

        try:
            self.cmd = [' '.join(map(str, [binary_path] + command))]
            if self.verbose:
                print(self.cmd)
            if gdb:
                return GDBobj([binary_path] + command, self.verbose)
            if asynchronous:
                return subprocess.Popen(
                    self.cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=self.test_env
                )
            else:
                self.output = subprocess.check_output(
                    [binary_path] + command,
                    stderr=subprocess.STDOUT,
                    env=self.test_env
                    ).decode('utf-8')
                if command[0] == 'backup':
                    # return backup ID
                    for line in self.output.splitlines():
                        if 'INFO: Backup' and 'completed' in line:
                            return line.split()[2]
                else:
                    return self.output
        except subprocess.CalledProcessError as e:
            raise ProbackupException(e.output.decode('utf-8'), self.cmd)

    def run_binary(self, command, asynchronous=False):
        if self.verbose:
                print([' '.join(map(str, command))])
        try:
            if asynchronous:
                return subprocess.Popen(
                    command,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=self.test_env
                )
            else:
                self.output = subprocess.check_output(
                    command,
                    stderr=subprocess.STDOUT,
                    env=self.test_env
                    ).decode('utf-8')
                return self.output
        except subprocess.CalledProcessError as e:
            raise ProbackupException(e.output.decode('utf-8'), command)

    def init_pb(self, backup_dir, options=[], old_binary=False):

        shutil.rmtree(backup_dir, ignore_errors=True)

        # don`t forget to kill old_binary after remote ssh release
        if self.remote and not old_binary:
            options = options + [
                '--remote-proto=ssh',
                '--remote-host=localhost']

        return self.run_pb([
            'init',
            '-B', backup_dir
            ] + options,
            old_binary=old_binary
        )

    def add_instance(self, backup_dir, instance, node, old_binary=False, options=[]):

        cmd = [
            'add-instance',
            '--instance={0}'.format(instance),
            '-B', backup_dir,
            '-D', node.data_dir
            ]

        # don`t forget to kill old_binary after remote ssh release
        if self.remote and not old_binary:
            options = options + [
                '--remote-proto=ssh',
                '--remote-host=localhost']

        return self.run_pb(cmd + options, old_binary=old_binary)

    def set_config(self, backup_dir, instance, old_binary=False, options=[]):

        cmd = [
            'set-config',
            '--instance={0}'.format(instance),
            '-B', backup_dir,
            ]

        return self.run_pb(cmd + options, old_binary=old_binary)

    def del_instance(self, backup_dir, instance, old_binary=False):

        return self.run_pb([
            'del-instance',
            '--instance={0}'.format(instance),
            '-B', backup_dir
            ],
            old_binary=old_binary
        )

    def clean_pb(self, backup_dir):
        shutil.rmtree(backup_dir, ignore_errors=True)

    def backup_node(
            self, backup_dir, instance, node, data_dir=False,
            backup_type='full', options=[], asynchronous=False, gdb=False,
            old_binary=False
            ):
        if not node and not data_dir:
            print('You must provide ether node or data_dir for backup')
            exit(1)

        if node:
            pgdata = node.data_dir

        if data_dir:
            pgdata = data_dir

        cmd_list = [
            'backup',
            '-B', backup_dir,
            # "-D", pgdata,
            '-p', '%i' % node.port,
            '-d', 'postgres',
            '--instance={0}'.format(instance)
        ]

        # don`t forget to kill old_binary after remote ssh release
        if self.remote and not old_binary:
            options = options + [
                '--remote-proto=ssh',
                '--remote-host=localhost']

        if backup_type:
            cmd_list += ['-b', backup_type]

        return self.run_pb(cmd_list + options, asynchronous, gdb, old_binary)

    def merge_backup(
            self, backup_dir, instance, backup_id, asynchronous=False,
            gdb=False, old_binary=False, options=[]):
        cmd_list = [
            'merge',
            '-B', backup_dir,
            '--instance={0}'.format(instance),
            '-i', backup_id
        ]

        return self.run_pb(cmd_list + options, asynchronous, gdb, old_binary)

    def restore_node(
            self, backup_dir, instance, node=False,
            data_dir=None, backup_id=None, old_binary=False, options=[]
            ):

        if data_dir is None:
            data_dir = node.data_dir

        cmd_list = [
            'restore',
            '-B', backup_dir,
            '-D', data_dir,
            '--instance={0}'.format(instance)
        ]

        # don`t forget to kill old_binary after remote ssh release
        if self.remote and not old_binary:
            options = options + [
                '--remote-proto=ssh',
                '--remote-host=localhost']

        if backup_id:
            cmd_list += ['-i', backup_id]

        return self.run_pb(cmd_list + options, old_binary=old_binary)

    def show_pb(
            self, backup_dir, instance=None, backup_id=None,
            options=[], as_text=False, as_json=True, old_binary=False
            ):

        backup_list = []
        specific_record = {}
        cmd_list = [
            'show',
            '-B', backup_dir,
        ]
        if instance:
            cmd_list += ['--instance={0}'.format(instance)]

        if backup_id:
            cmd_list += ['-i', backup_id]

        # AHTUNG, WARNING will break json parsing
        if as_json:
            cmd_list += ['--format=json', '--log-level-console=error']

        if as_text:
            # You should print it when calling as_text=true
            return self.run_pb(cmd_list + options, old_binary=old_binary)

        # get show result as list of lines
        if as_json:
            data = json.loads(self.run_pb(cmd_list + options, old_binary=old_binary))
        #    print(data)
            for instance_data in data:
                # find specific instance if requested
                if instance and instance_data['instance'] != instance:
                    continue

                for backup in reversed(instance_data['backups']):
                    # find specific backup if requested
                    if backup_id:
                        if backup['id'] == backup_id:
                            return backup
                    else:
                        backup_list.append(backup)
            return backup_list
        else:
            show_splitted = self.run_pb(
                cmd_list + options, old_binary=old_binary).splitlines()
            if instance is not None and backup_id is None:
                # cut header(ID, Mode, etc) from show as single string
                header = show_splitted[1:2][0]
                # cut backup records from show as single list
                # with string for every backup record
                body = show_splitted[3:]
                # inverse list so oldest record come first
                body = body[::-1]
                # split string in list with string for every header element
                header_split = re.split('  +', header)
                # Remove empty items
                for i in header_split:
                    if i == '':
                        header_split.remove(i)
                        continue
                header_split = [
                    header_element.rstrip() for header_element in header_split
                    ]
                for backup_record in body:
                    backup_record = backup_record.rstrip()
                    # split list with str for every backup record element
                    backup_record_split = re.split('  +', backup_record)
                    # Remove empty items
                    for i in backup_record_split:
                        if i == '':
                            backup_record_split.remove(i)
                    if len(header_split) != len(backup_record_split):
                        print(warning.format(
                            header=header, body=body,
                            header_split=header_split,
                            body_split=backup_record_split)
                        )
                        exit(1)
                    new_dict = dict(zip(header_split, backup_record_split))
                    backup_list.append(new_dict)
                return backup_list
            else:
                # cut out empty lines and lines started with #
                # and other garbage then reconstruct it as dictionary
                # print show_splitted
                sanitized_show = [item for item in show_splitted if item]
                sanitized_show = [
                    item for item in sanitized_show if not item.startswith('#')
                ]
                # print sanitized_show
                for line in sanitized_show:
                    name, var = line.partition(' = ')[::2]
                    var = var.strip('"')
                    var = var.strip("'")
                    specific_record[name.strip()] = var
                return specific_record

    def validate_pb(
            self, backup_dir, instance=None,
            backup_id=None, options=[], old_binary=False, gdb=False
            ):

        cmd_list = [
            'validate',
            '-B', backup_dir
        ]
        if instance:
            cmd_list += ['--instance={0}'.format(instance)]
        if backup_id:
            cmd_list += ['-i', backup_id]

        return self.run_pb(cmd_list + options, old_binary=old_binary, gdb=gdb)

    def delete_pb(
            self, backup_dir, instance,
            backup_id=None, options=[], old_binary=False):
        cmd_list = [
            'delete',
            '-B', backup_dir
        ]

        cmd_list += ['--instance={0}'.format(instance)]
        if backup_id:
            cmd_list += ['-i', backup_id]

        return self.run_pb(cmd_list + options, old_binary=old_binary)

    def delete_expired(
            self, backup_dir, instance, options=[], old_binary=False):
        cmd_list = [
            'delete',
            '-B', backup_dir,
            '--instance={0}'.format(instance)
        ]
        return self.run_pb(cmd_list + options, old_binary=old_binary)

    def show_config(self, backup_dir, instance, old_binary=False):
        out_dict = {}
        cmd_list = [
            'show-config',
            '-B', backup_dir,
            '--instance={0}'.format(instance)
        ]

        res = self.run_pb(cmd_list, old_binary=old_binary).splitlines()
        for line in res:
            if not line.startswith('#'):
                name, var = line.partition(' = ')[::2]
                out_dict[name] = var
        return out_dict

    def get_recovery_conf(self, node):
        out_dict = {}
        with open(
            os.path.join(node.data_dir, 'recovery.conf'), 'r'
        ) as recovery_conf:
            for line in recovery_conf:
                try:
                    key, value = line.split('=')
                except:
                    continue
                out_dict[key.strip()] = value.strip(" '").replace("'\n", "")
        return out_dict

    def set_archiving(
            self, backup_dir, instance, node, replica=False,
            overwrite=False, compress=False, old_binary=False):

        if replica:
            archive_mode = 'always'
            node.append_conf('postgresql.auto.conf', 'hot_standby = on')
        else:
            archive_mode = 'on'

        node.append_conf(
                'postgresql.auto.conf',
                'archive_mode = {0}'.format(archive_mode)
                )
        if os.name == 'posix':
            archive_command = '"{0}" archive-push -B {1} --instance={2} '.format(
                self.probackup_path, backup_dir, instance)

        elif os.name == 'nt':
            archive_command = '"{0}" archive-push -B {1} --instance={2} '.format(
                self.probackup_path.replace("\\","\\\\"),
                backup_dir.replace("\\","\\\\"),
                instance)

        # don`t forget to kill old_binary after remote ssh release
        if self.remote and not old_binary:
            archive_command = archive_command + '--remote-proto=ssh --remote-host=localhost '

        if self.archive_compress or compress:
            archive_command = archive_command + '--compress '

        if overwrite:
            archive_command = archive_command + '--overwrite '

        if os.name == 'posix':
            archive_command = archive_command + '--wal-file-path %p --wal-file-name %f'

        elif os.name == 'nt':
            archive_command = archive_command + '--wal-file-path "%p" --wal-file-name "%f"'

        node.append_conf(
                    'postgresql.auto.conf',
                    "archive_command = '{0}'".format(
                        archive_command))

    def set_replica(
            self, master, replica,
            replica_name='replica',
            synchronous=False
            ):
        replica.append_conf(
            'postgresql.auto.conf', 'port = {0}'.format(replica.port))
        replica.append_conf('postgresql.auto.conf', 'hot_standby = on')
        replica.append_conf('recovery.conf', 'standby_mode = on')
        replica.append_conf(
            'recovery.conf',
            "primary_conninfo = 'user={0} port={1} application_name={2}"
            " sslmode=prefer sslcompression=1'".format(
                self.user, master.port, replica_name)
        )
        if synchronous:
            master.append_conf(
                'postgresql.auto.conf',
                "synchronous_standby_names='{0}'".format(replica_name)
            )
            master.append_conf(
                'postgresql.auto.conf',
                "synchronous_commit='remote_apply'"
            )
            master.reload()

    def change_backup_status(self, backup_dir, instance, backup_id, status):

        control_file_path = os.path.join(
            backup_dir, 'backups', instance, backup_id, 'backup.control')

        with open(control_file_path, 'r') as f:
            actual_control = f.read()

        new_control_file = ''
        for line in actual_control.splitlines():
            if line.startswith('status'):
                line = 'status = {0}'.format(status)
            new_control_file += line
            new_control_file += '\n'

        with open(control_file_path, 'wt') as f:
            f.write(new_control_file)
            f.flush()
            f.close()

        with open(control_file_path, 'r') as f:
            actual_control = f.read()

    def wrong_wal_clean(self, node, wal_size):
        wals_dir = os.path.join(self.backup_dir(node), 'wal')
        wals = [
            f for f in os.listdir(wals_dir) if os.path.isfile(
                os.path.join(wals_dir, f))
        ]
        wals.sort()
        file_path = os.path.join(wals_dir, wals[-1])
        if os.path.getsize(file_path) != wal_size:
            os.remove(file_path)

    def guc_wal_segment_size(self, node):
        var = node.execute(
            'postgres',
            "select setting from pg_settings where name = 'wal_segment_size'"
        )
        return int(var[0][0]) * self.guc_wal_block_size(node)

    def guc_wal_block_size(self, node):
        var = node.execute(
            'postgres',
            "select setting from pg_settings where name = 'wal_block_size'"
        )
        return int(var[0][0])

    def get_pgpro_edition(self, node):
        if node.execute(
            'postgres',
            "select exists (select 1 from"
            " pg_proc where proname = 'pgpro_edition')"
        )[0][0]:
            var = node.execute('postgres', 'select pgpro_edition()')
            return str(var[0][0])
        else:
            return False

    def get_username(self):
        """ Returns current user name """
        return getpass.getuser()

    def version_to_num(self, version):
        if not version:
            return 0
        parts = version.split('.')
        while len(parts) < 3:
            parts.append('0')
        num = 0
        for part in parts:
            num = num * 100 + int(re.sub("[^\d]", "", part))
        return num

    def switch_wal_segment(self, node):
        """
        Execute pg_switch_wal/xlog() in given node

        Args:
            node: an instance of PostgresNode or NodeConnection class
        """
        if isinstance(node, testgres.PostgresNode):
            if self.version_to_num(
                node.safe_psql('postgres', 'show server_version')
                    ) >= self.version_to_num('10.0'):
                node.safe_psql('postgres', 'select pg_switch_wal()')
            else:
                node.safe_psql('postgres', 'select pg_switch_xlog()')
        else:
            if self.version_to_num(
                node.execute('show server_version')[0][0]
                    ) >= self.version_to_num('10.0'):
                node.execute('select pg_switch_wal()')
            else:
                node.execute('select pg_switch_xlog()')

        sleep(1)

    def wait_until_replica_catch_with_master(self, master, replica):

        if self.version_to_num(
                master.safe_psql(
                    'postgres',
                    'show server_version')) >= self.version_to_num('10.0'):
            master_function = 'pg_catalog.pg_current_wal_lsn()'
            replica_function = 'pg_catalog.pg_last_wal_replay_lsn()'
        else:
            master_function = 'pg_catalog.pg_current_xlog_location()'
            replica_function = 'pg_catalog.pg_last_xlog_replay_location()'

        lsn = master.safe_psql(
            'postgres',
            'SELECT {0}'.format(master_function)).rstrip()

        # Wait until replica catch up with master
        replica.poll_query_until(
            'postgres',
            "SELECT '{0}'::pg_lsn <= {1}".format(lsn, replica_function))

    def get_version(self, node):
        return self.version_to_num(
            testgres.get_pg_config()['VERSION'].split(" ")[1])

    def get_bin_path(self, binary):
        return testgres.get_bin_path(binary)

    def del_test_dir(self, module_name, fname):
        """ Del testdir and optimistically try to del module dir"""
        try:
            testgres.clean_all()
        except:
            pass

        shutil.rmtree(
            os.path.join(
                self.tmp_path,
                module_name,
                fname
            ),
            ignore_errors=True
        )
        try:
            os.rmdir(os.path.join(self.tmp_path, module_name))
        except:
            pass

    def pgdata_content(self, pgdata, ignore_ptrack=True, exclude_dirs=None):
        """ return dict with directory content. "
        " TAKE IT AFTER CHECKPOINT or BACKUP"""
        dirs_to_ignore = [
            'pg_xlog', 'pg_wal', 'pg_log',
            'pg_stat_tmp', 'pg_subtrans', 'pg_notify'
        ]
        files_to_ignore = [
            'postmaster.pid', 'postmaster.opts',
            'pg_internal.init', 'postgresql.auto.conf',
            'backup_label', 'tablespace_map', 'recovery.conf',
            'ptrack_control', 'ptrack_init', 'pg_control'
        ]

        if exclude_dirs:
            dirs_to_ignore = dirs_to_ignore + exclude_dirs
#        suffixes_to_ignore = (
#            '_ptrack'
#        )
        directory_dict = {}
        directory_dict['pgdata'] = pgdata
        directory_dict['files'] = {}
        directory_dict['dirs'] = []
        for root, dirs, files in os.walk(pgdata, followlinks=True):
            dirs[:] = [d for d in dirs if d not in dirs_to_ignore]
            for file in files:
                if (
                    file in files_to_ignore or
                    (ignore_ptrack and file.endswith('_ptrack'))
                ):
                        continue

                file_fullpath = os.path.join(root, file)
                file_relpath = os.path.relpath(file_fullpath, pgdata)
                directory_dict['files'][file_relpath] = {'is_datafile': False}
                directory_dict['files'][file_relpath]['md5'] = hashlib.md5(
                    open(file_fullpath, 'rb').read()).hexdigest()

                # crappy algorithm
                if file.isdigit():
                    directory_dict['files'][file_relpath]['is_datafile'] = True
                    size_in_pages = os.path.getsize(file_fullpath)/8192
                    directory_dict['files'][file_relpath][
                        'md5_per_page'] = self.get_md5_per_page_for_fork(
                            file_fullpath, size_in_pages
                        )

        for root, dirs, files in os.walk(pgdata, topdown=False, followlinks=True):
            for directory in dirs:
                directory_path = os.path.join(root, directory)
                directory_relpath = os.path.relpath(directory_path, pgdata)

                found = False
                for d in dirs_to_ignore:
                    if d in directory_relpath:
                        found = True
                        break

                # check if directory already here as part of larger directory
                if not found:
                    for d in directory_dict['dirs']:
                        # print("OLD dir {0}".format(d))
                        if directory_relpath in d:
                            found = True
                            break

                if not found:
                    directory_dict['dirs'].append(directory_relpath)

        return directory_dict

    def compare_pgdata(self, original_pgdata, restored_pgdata):
        """ return dict with directory content. DO IT BEFORE RECOVERY"""
        fail = False
        error_message = 'Restored PGDATA is not equal to original!\n'

        # Compare directories
        for directory in restored_pgdata['dirs']:
            if directory not in original_pgdata['dirs']:
                fail = True
                error_message += '\nDirectory was not present'
                error_message += ' in original PGDATA: {0}\n'.format(
                    os.path.join(restored_pgdata['pgdata'], directory))

        for directory in original_pgdata['dirs']:
            if directory not in restored_pgdata['dirs']:
                fail = True
                error_message += '\nDirectory dissappeared'
                error_message += ' in restored PGDATA: {0}\n'.format(
                    os.path.join(restored_pgdata['pgdata'], directory))


        for file in restored_pgdata['files']:
            # File is present in RESTORED PGDATA
            # but not present in ORIGINAL
            # only backup_label is allowed
            if file not in original_pgdata['files']:
                fail = True
                error_message += '\nFile is not present'
                error_message += ' in original PGDATA: {0}\n'.format(
                    os.path.join(restored_pgdata['pgdata'], file))

        for file in original_pgdata['files']:
            if file in restored_pgdata['files']:

                if (
                    original_pgdata['files'][file]['md5'] !=
                    restored_pgdata['files'][file]['md5']
                ):
                    fail = True
                    error_message += (
                        '\nFile Checksumm mismatch.\n'
                        'File_old: {0}\nChecksumm_old: {1}\n'
                        'File_new: {2}\nChecksumm_new: {3}\n').format(
                        os.path.join(original_pgdata['pgdata'], file),
                        original_pgdata['files'][file]['md5'],
                        os.path.join(restored_pgdata['pgdata'], file),
                        restored_pgdata['files'][file]['md5']
                    )

                    if original_pgdata['files'][file]['is_datafile']:
                        for page in original_pgdata['files'][file]['md5_per_page']:
                            if page not in restored_pgdata['files'][file]['md5_per_page']:
                                error_message += (
                                    '\n Page {0} dissappeared.\n '
                                    'File: {1}\n').format(
                                        page,
                                        os.path.join(
                                            restored_pgdata['pgdata'],
                                            file
                                        )
                                    )
                                continue

                            if original_pgdata['files'][file][
                                'md5_per_page'][page] != restored_pgdata[
                                    'files'][file]['md5_per_page'][page]:
                                    error_message += (
                                        '\n Page checksumm mismatch: {0}\n '
                                        ' PAGE Checksumm_old: {1}\n '
                                        ' PAGE Checksumm_new: {2}\n '
                                        ' File: {3}\n'
                                    ).format(
                                        page,
                                        original_pgdata['files'][file][
                                            'md5_per_page'][page],
                                        restored_pgdata['files'][file][
                                            'md5_per_page'][page],
                                        os.path.join(
                                            restored_pgdata['pgdata'], file)
                                        )
                        for page in restored_pgdata['files'][file]['md5_per_page']:
                            if page not in original_pgdata['files'][file]['md5_per_page']:
                                error_message += '\n Extra page {0}\n File: {1}\n'.format(
                                    page,
                                    os.path.join(
                                        restored_pgdata['pgdata'], file))

            else:
                error_message += (
                    '\nFile dissappearance.\n '
                    'File: {0}\n').format(
                    os.path.join(restored_pgdata['pgdata'], file)
                    )
                fail = True
        self.assertFalse(fail, error_message)

    def get_async_connect(self, database=None, host=None, port=5432):
        if not database:
            database = 'postgres'
        if not host:
            host = '127.0.0.1'

        return psycopg2.connect(
            database='postgres',
            host='127.0.0.1',
            port=port,
            async_=True
        )

    def wait(self, connection):
        while True:
            state = connection.poll()
            if state == psycopg2.extensions.POLL_OK:
                break
            elif state == psycopg2.extensions.POLL_WRITE:
                select.select([], [connection.fileno()], [])
            elif state == psycopg2.extensions.POLL_READ:
                select.select([connection.fileno()], [], [])
            else:
                raise psycopg2.OperationalError('poll() returned %s' % state)

    def gdb_attach(self, pid):
        return GDBobj([str(pid)], self.verbose, attach=True)


class GdbException(Exception):
    def __init__(self, message=False):
        self.message = message

    def __str__(self):
        return '\n ERROR: {0}\n'.format(repr(self.message))


class GDBobj(ProbackupTest):
    def __init__(self, cmd, verbose, attach=False):
        self.verbose = verbose

        # Check gdb presense
        try:
            gdb_version, _ = subprocess.Popen(
                ['gdb', '--version'],
                stdout=subprocess.PIPE
            ).communicate()
        except OSError:
            raise GdbException("Couldn't find gdb on the path")

        self.base_cmd = [
            'gdb',
            '--interpreter',
            'mi2',
            ]

        if attach:
            self.cmd = self.base_cmd + ['--pid'] + cmd
        else:
            self.cmd = self.base_cmd + ['--args'] + cmd

        # Get version
        gdb_version_number = re.search(
            b"^GNU gdb [^\d]*(\d+)\.(\d)",
            gdb_version)
        self.major_version = int(gdb_version_number.group(1))
        self.minor_version = int(gdb_version_number.group(2))

        if self.verbose:
            print([' '.join(map(str, self.cmd))])

        self.proc = subprocess.Popen(
            self.cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
            universal_newlines=True
        )
        self.gdb_pid = self.proc.pid

        # discard data from pipe,
        # is there a way to do it a less derpy way?
        while True:
            line = self.proc.stdout.readline()

            if 'No such process' in line:
                raise GdbException(line)

            if not line.startswith('(gdb)'):
                pass
            else:
                break

    def set_breakpoint(self, location):

        result = self._execute('break ' + location)
        for line in result:
            if line.startswith('~"Breakpoint'):
                return

            elif line.startswith('=breakpoint-created'):
                return

            elif line.startswith('^error'): #or line.startswith('(gdb)'):
                break

            elif line.startswith('&"break'):
                pass

            elif line.startswith('&"Function'):
                raise GdbException(line)

            elif line.startswith('&"No line'):
                raise GdbException(line)

            elif line.startswith('~"Make breakpoint pending on future shared'):
                raise GdbException(line)

        raise GdbException(
            'Failed to set breakpoint.\n Output:\n {0}'.format(result)
        )

    def run_until_break(self):
        result = self._execute('run', False)
        for line in result:
            if line.startswith('*stopped,reason="breakpoint-hit"'):
                return
        raise GdbException(
            'Failed to run until breakpoint.\n'
        )

    def continue_execution_until_running(self):
        result = self._execute('continue')

        for line in result:
            if line.startswith('*running') or line.startswith('^running'):
                return
            if line.startswith('*stopped,reason="breakpoint-hit"'):
                continue
            if line.startswith('*stopped,reason="exited-normally"'):
                continue

        raise GdbException(
                'Failed to continue execution until running.\n'
            )

    def continue_execution_until_exit(self):
        result = self._execute('continue', False)

        for line in result:
            if line.startswith('*running'):
                continue
            if line.startswith('*stopped,reason="breakpoint-hit"'):
                continue
            if (
                line.startswith('*stopped,reason="exited') or
                line == '*stopped\n'
            ):
                return

        raise GdbException(
            'Failed to continue execution until exit.\n'
        )

    def continue_execution_until_error(self):
        result = self._execute('continue', False)

        for line in result:
            if line.startswith('^error'):
                return
            if line.startswith('*stopped,reason="exited'):
                return

        raise GdbException(
            'Failed to continue execution until error.\n')

    def continue_execution_until_break(self, ignore_count=0):
        if ignore_count > 0:
            result = self._execute(
                'continue ' + str(ignore_count),
                False
            )
        else:
            result = self._execute('continue', False)

        for line in result:
            if line.startswith('*stopped,reason="breakpoint-hit"'):
                return
            if line.startswith('*stopped,reason="exited-normally"'):
                break

        raise GdbException(
            'Failed to continue execution until break.\n')

    def stopped_in_breakpoint(self):
        output = []
        while True:
            line = self.proc.stdout.readline()
            output += [line]
            if self.verbose:
                print(line)
            if line.startswith('*stopped,reason="breakpoint-hit"'):
                return True
        return False

    # use for breakpoint, run, continue
    def _execute(self, cmd, running=True):
        output = []
        self.proc.stdin.flush()
        self.proc.stdin.write(cmd + '\n')
        self.proc.stdin.flush()

        while True:
            line = self.proc.stdout.readline()
            output += [line]
            if self.verbose:
                print(repr(line))
            if line.startswith('^done') or line.startswith('*stopped'):
                break
            if line.startswith('^error'):
                break
            if running and (line.startswith('*running') or line.startswith('^running')):
#            if running and line.startswith('*running'):
                break
        return output
