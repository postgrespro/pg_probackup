# you need os for unittest to work
import gzip
import io
import os
import threading
import unittest
import shutil
import sys

import testgres
from testgres.enums import NodeStatus
import hashlib
import time
import re
import json
import contextlib

from pg_probackup2.gdb import GDBobj
from pg_probackup2.init_helpers import init_params
from pg_probackup2.app import ProbackupApp
from pg_probackup2.storage.fs_backup import TestBackupDir, FSTestBackupDir

try:
    import lz4.frame
except ImportError:
    pass

try:
    import zstd
except ImportError:
    pass

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


def load_backup_class(fs_type):
    fs_type = os.environ.get('PROBACKUP_FS_TYPE')
    implementation = f"{__package__}.fs_backup.FSTestBackupDir"
    if fs_type:
        implementation = fs_type

    print("Using ", implementation)
    module_name, class_name = implementation.rsplit(sep='.', maxsplit=1)

    module = importlib.import_module(module_name)

    return getattr(module, class_name)


fs_backup_class = FSTestBackupDir
if os.environ.get('PROBACKUP_FS_TYPE'):
    fs_backup_class = load_backup_class(os.environ.get('PROBACKUP_FS_TYPE'))
# Run tests on s3 when we have PG_PROBACKUP_S3_TEST (minio, vk...) or PG_PROBACKUP_S3_CONFIG_FILE.
# If PG_PROBACKUP_S3_CONFIG_FILE is 'True', then using default conf file. Check config_provider.py
elif (os.environ.get('PG_PROBACKUP_S3_TEST') and os.environ.get('PG_PROBACKUP_S3_HOST') or
      os.environ.get('PG_PROBACKUP_S3_CONFIG_FILE')):
    root = os.path.realpath(os.path.join(os.path.dirname(__file__), '../..'))
    if root not in sys.path:
        sys.path.append(root)
    from tests.test_utils.s3_backup import S3TestBackupDir
    fs_backup_class = S3TestBackupDir

def dir_files(base_dir):
    out_list = []
    for dir_name, subdir_list, file_list in os.walk(base_dir):
        rel_dir = os.path.relpath(dir_name, base_dir)
        if rel_dir != '.':
            out_list.append(rel_dir)
        for fname in file_list:
            out_list.append(
                os.path.relpath(os.path.join(
                    dir_name, fname), base_dir)
            )
    out_list.sort()
    return out_list


def base36enc(number):
    """Converts an integer to a base36 string."""
    if number < 0:
        return '-' + base36enc(-number)

    alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    base36 = ''
    while number >= len(alphabet):
        number, i = divmod(number, len(alphabet))
        base36 += alphabet[i]
    base36 += alphabet[number]
    return base36[::-1]


def base36dec(id):
    return int(id, 36)


class ProbackupTest(unittest.TestCase):
    # Class attributes
    enterprise = init_params.is_enterprise
    shardman = init_params.is_shardman
    enable_nls = init_params.is_nls_enabled
    enable_lz4 = init_params.is_lz4_enabled
    pgpro = init_params.is_pgpro
    verbose = init_params.verbose
    username = init_params.username
    remote = init_params.remote
    ptrack = init_params.ptrack
    paranoia = init_params.paranoia
    tests_source_path = os.path.join(init_params.source_path, 'tests')
    archive_compress = init_params.archive_compress
    compress_suffix = init_params.compress_suffix
    pg_config_version = init_params.pg_config_version
    probackup_path = init_params.probackup_path
    probackup_old_path = init_params.probackup_old_path
    probackup_version = init_params.probackup_version
    old_probackup_version = init_params.old_probackup_version
    cfs_compress_default = init_params.cfs_compress
    EXTERNAL_DIRECTORY_DELIMITER = init_params.EXTERNAL_DIRECTORY_DELIMITER
    s3_type = os.environ.get('PG_PROBACKUP_S3_TEST')

    auto_compress_alg = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.output = None
        self.cmd = None
        self.nodes_to_cleanup = []

        if isinstance(self, unittest.TestCase):
            try:
                self.module_name = self.id().split('.')[-2]
                self.fname = self.id().split('.')[-1]
            except IndexError:
                print("Couldn't get module name and function name from self.id(): `{}`".format(self.id()))
                self.module_name = self.module_name if self.module_name else str(self).split('(')[1].split('.')[1]
                self.fname = str(self).split('(')[0]

        self.test_env = init_params.test_env()

        if self.s3_type != "minio":
            if 'PG_PROBACKUP_S3_HOST' in self.test_env:
                del(self.test_env['PG_PROBACKUP_S3_HOST'])
            if 'PG_PROBACKUP_S3_PORT' in self.test_env:
                del(self.test_env['PG_PROBACKUP_S3_PORT'])

        self.rel_path = os.path.join(self.module_name, self.fname)
        self.test_path = os.path.join(init_params.tmp_path, self.rel_path)

        self.pg_node = testgres.NodeApp(self.test_path, self.nodes_to_cleanup)
        self.pg_node.os_ops.set_env('LANGUAGE','en')

        # Cleanup FS dependent part first
        self.backup_dir = self.build_backup_dir('backup')
        self.backup_dir.cleanup()
        # Recreate the rest which should reside on local file system only
        shutil.rmtree(self.test_path, ignore_errors=True)
        os.makedirs(self.test_path)

        self.pb_log_path = os.path.join(self.test_path, "pb_log")
        self.pb = ProbackupApp(self, self.pg_node, self.pb_log_path, self.test_env,
                               self.auto_compress_alg, self.backup_dir)

    def is_test_result_ok(test_case):
        # sources of solution:
        # 1. python versions 2.7 - 3.10, verified on 3.10, 3.7, 2.7, taken from:
        # https://tousu.in/qa/?qa=555402/unit-testing-getting-pythons-unittest-results-in-a-teardown-method&show=555403#a555403
        #
        # 2. python versions 3.11+ mixin, verified on 3.11, taken from: https://stackoverflow.com/a/39606065

        if hasattr(test_case._outcome, 'errors'):
            # Python 3.4 - 3.10  (These two methods have no side effects)
            result = test_case.defaultTestResult()  # These two methods have no side effects
            test_case._feedErrorsToResult(result, test_case._outcome.errors)
        else:
            # Python 3.11+ and pytest 5.3.5+
            result = test_case._outcome.result
            if not hasattr(result, 'errors'):
                result.errors = []
            if not hasattr(result, 'failures'):
                result.failures = []

        ok = all(test != test_case for test, text in result.errors + result.failures)
        # check subtests as well
        ok = ok and all(getattr(test, 'test_case', None) != test_case
                        for test, text in result.errors + result.failures)

        # for pytest 8+
        if hasattr(result, '_excinfo'):
            if result._excinfo is not None and len(result._excinfo) > 0:
                # if test was successful, _excinfo will be None, else it will be non-empty list
                ok = False

        return ok

    def tearDown(self):
        node_crashed = None
        if self.is_test_result_ok():
            for node in self.nodes_to_cleanup:
                if node.is_started and node.status() != NodeStatus.Running:
                    node_crashed = node
                node.cleanup()
            self.del_test_dirs()

        else:
            for node in self.nodes_to_cleanup:
                # TODO make decorator with proper stop() vs cleanup()
                node._try_shutdown(max_attempts=1)
                # node.cleanup()

        self.nodes_to_cleanup.clear()

        if node_crashed:
            self.fail(f"Node '{os.path.relpath(node.base_dir, self.test_path)}' unexpectingly crashed")

    def build_backup_dir(self, backup='backup'):
        return fs_backup_class(rel_path=self.rel_path, backup=backup)

    def read_pb_log(self):
        with open(os.path.join(self.pb_log_path, 'pg_probackup.log')) as fl:
            return fl.read()

    def unlink_pg_log(self):
        os.unlink(os.path.join(self.pb_log_path, 'pg_probackup.log'))

    def simple_bootstrap(self, node, role) -> None:

        node.safe_psql(
            'postgres',
            'CREATE ROLE {0} WITH LOGIN REPLICATION'.format(role))

        # >= 10 && < 15
        if self.pg_config_version < 150000:
            node.safe_psql(
                'postgres',
                'GRANT USAGE ON SCHEMA pg_catalog TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_start_backup(text, boolean, boolean) TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_stop_backup(boolean, boolean) TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_create_restore_point(text) TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_wal() TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_last_wal_replay_lsn() TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.txid_current() TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.txid_current_snapshot() TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.txid_snapshot_xmax(txid_snapshot) TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_checkpoint() TO {0};'.format(role))
        # >= 15
        else:
            node.safe_psql(
                'postgres',
                'GRANT USAGE ON SCHEMA pg_catalog TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_backup_start(text, boolean) TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_backup_stop(boolean) TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_create_restore_point(text) TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_wal() TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_last_wal_replay_lsn() TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.txid_current() TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.txid_current_snapshot() TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.txid_snapshot_xmax(txid_snapshot) TO {0}; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_checkpoint() TO {0};'.format(role))

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

            if cfs is True and self.cfs_compress_default:
                cfs = self.cfs_compress_default
            if cfs is True or node.major_version < 12:
                cmd += ' with (compression=true)'
            else:
                cmd += ' with (compression=' + cfs + ')'

        if not os.path.exists(tblspc_path):
            os.makedirs(tblspc_path)
        res = node.safe_psql('postgres', cmd)
        # Check that tablespace was successfully created
        # self.assertEqual(
        #     res[0], 0,
        #     'Failed to create tablespace with cmd: {0}'.format(cmd))

    def drop_tblspace(self, node, tblspc_name):
        res = node.execute(
            'postgres',
            'select exists'
            " (select 1 from pg_tablespace where spcname = '{0}')".format(
                tblspc_name)
        )
        # Check that tablespace with name 'tblspc_name' do not exists already
        self.assertTrue(
            res[0][0],
            'Tablespace "{0}" do not exists'.format(tblspc_name)
        )

        rels = node.execute(
            "postgres",
            "SELECT relname FROM pg_class c "
            "LEFT JOIN pg_tablespace t ON c.reltablespace = t.oid "
            "where c.relkind = 'r' and t.spcname = '{0}'".format(tblspc_name))

        for rel in rels:
            node.safe_psql(
                'postgres',
                "DROP TABLE {0}".format(rel[0]))

        node.safe_psql(
            'postgres',
            'DROP TABLESPACE {0}'.format(tblspc_name))

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
        size_in_pages = int(size_in_pages)
        nsegments = int(size_in_pages / 131072)
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
                    file + '.{0}'.format(segment_number), os.O_RDONLY
                )
                start_page = max(md5_per_page) + 1
                end_page = end_page + pages_per_segment[segment_number]

            for page in range(start_page, end_page):
                md5_per_page[page] = hashlib.md5(
                    os.read(file_desc, 8192)).hexdigest()
                offset += 8192
                os.lseek(file_desc, offset, 0)
            os.close(file_desc)

        return md5_per_page

    def get_ptrack_bits_per_page_for_fork(self, node, file, size=None):

        if size is None:
            size = []
        header_size = 24
        ptrack_bits_for_fork = []

        # TODO: use macro instead of hard coded 8KB
        page_body_size = 8192 - header_size
        # Check that if main fork file size is 0, it`s ok
        # to not having a _ptrack fork
        if os.path.getsize(file) == 0:
            return ptrack_bits_for_fork
        byte_size = os.path.getsize(file + '_ptrack')
        npages = int(byte_size / 8192)
        if byte_size % 8192 != 0:
            print('Ptrack page is not 8k aligned')
            sys.exit(1)

        file = os.open(file + '_ptrack', os.O_RDONLY)

        for page in range(npages):
            offset = 8192 * page + header_size
            os.lseek(file, offset, 0)
            lots_of_bytes = os.read(file, page_body_size)
            byte_list = [
                lots_of_bytes[i:i + 1] for i in range(len(lots_of_bytes))
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

    def check_ptrack_map_sanity(self, node, idx_ptrack):
        success = True
        for i in idx_ptrack:
            # get new size of heap and indexes. size calculated in pages
            idx_ptrack[i]['new_size'] = self.get_fork_size(node, i)
            # update path to heap and index files in case they`ve changed
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # calculate new md5sums for pages
            idx_ptrack[i]['new_pages'] = self.get_md5_per_page_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['new_size'])
            # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                node, idx_ptrack[i]['path'],
                [idx_ptrack[i]['old_size'], idx_ptrack[i]['new_size']])

            # compare pages and check ptrack sanity
            if not self.check_ptrack_sanity(idx_ptrack[i]):
                success = False

        self.assertTrue(
            success, 'Ptrack has failed to register changes in data files')

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

    def get_backup_filelist(self, backup_dir, instance, backup_id):
        path = os.path.join('backups', instance, backup_id, 'backup_content.control')
        filelist_raw = backup_dir.read_file(path)

        filelist = {}
        for line in io.StringIO(filelist_raw):
            line = json.loads(line)
            filelist[line['path']] = line

        return filelist

    def get_backup_listdir(self, backup_dir, instance, backup_id, sub_path):
        subpath = os.path.join('backups', instance, backup_id, sub_path)
        return backup_dir.list_files(subpath)

    def get_backups_dirs(self, backup_dir, instance):
        subpath = os.path.join("backups", instance)
        return backup_dir.list_dirs(subpath)

    def read_backup_file(self, backup_dir, instance, backup_id,
                         sub_path, *, text=False):
        subpath = os.path.join('backups', instance, backup_id, sub_path)
        return backup_dir.read_file(subpath, text=text)

    def write_backup_file(self, backup_dir, instance, backup_id,
                          sub_path, content, *, text=False):
        subpath = os.path.join('backups', instance, backup_id, sub_path)
        return backup_dir.write_file(subpath, content, text=text)

    def corrupt_backup_file(self, backup_dir, instance, backup_id, sub_path, *,
                            damage: tuple = None,
                            truncate: int = None,
                            overwrite=None,
                            text=False):
        subpath = os.path.join('backups', instance, backup_id, sub_path)
        if overwrite:
            content = overwrite
        elif truncate == 0:
            content = '' if text else b''
        else:
            content = backup_dir.read_file(subpath, text=text)
            if damage:
                pos, replace = damage
                content = content[:pos] + replace + content[pos + len(replace):]
            if truncate is not None:
                content = content[:truncate]
        backup_dir.write_file(subpath, content, text=text)

    def remove_backup_file(self, backup_dir, instance, backup_id, sub_path):
        subpath = os.path.join('backups', instance, backup_id, sub_path)
        backup_dir.remove_file(subpath)

    def backup_file_exists(self, backup_dir, instance, backup_id, sub_path):
        subpath = os.path.join('backups', instance, backup_id, sub_path)
        return backup_dir.exists(subpath)

    def remove_backup_config(self, backup_dir, instance):
        subpath = os.path.join('backups', instance, 'pg_probackup.conf')
        backup_dir.remove_file(subpath)

    @contextlib.contextmanager
    def modify_backup_config(self, backup_dir, instance):
        path = os.path.join('backups', instance, 'pg_probackup.conf')
        control_file = backup_dir.read_file(path)
        cf = ProbackupTest.ControlFileContainer(control_file)
        yield cf
        if control_file != cf.data:
            backup_dir.write_file(path, cf.data)

    def remove_one_backup(self, backup_dir, instance, backup_id):
        subpath = os.path.join('backups', instance, backup_id)
        backup_dir.remove_dir(subpath)

    def remove_one_backup_instance(self, backup_dir, instance):
        subpath = os.path.join('backups', instance)
        backup_dir.remove_dir(subpath)

    # return dict of files from filelist A,
    # which are not exists in filelist_B
    def get_backup_filelist_diff(self, filelist_A, filelist_B):

        filelist_diff = {}
        for file in filelist_A:
            if file not in filelist_B:
                filelist_diff[file] = filelist_A[file]

        return filelist_diff

    def get_instance_wal_list(self, backup_dir, instance):
        files = map(str, backup_dir.list_files(os.path.join('wal', instance)))
        files = [f for f in files
                 if not any(x in f for x in ('.backup', '.history', '~tmp'))]
        files.sort()
        return files

    def read_instance_wal(self, backup_dir, instance, file, decompress=False):
        content = backup_dir.read_file(f'wal/{instance}/{file}', text=False)
        if decompress:
            content = _do_decompress(file, content)
        return content

    def write_instance_wal(self, backup_dir, instance, file, data, compress=False):
        if compress:
            data = _do_compress(file, data)
        return backup_dir.write_file(f'wal/{instance}/{file}', data, text=False)

    def corrupt_instance_wal(self, backup_dir, instance, file, pos, damage, decompressed=False):
        subpath = f'wal/{instance}/{file}'
        content = backup_dir.read_file(subpath, text=False)
        if decompressed:
            content = _do_decompress(subpath, content)
        content = content[:pos] + \
                  bytes(d^c for d, c in zip(content[pos:pos+len(damage)], damage)) + \
                  content[pos + len(damage):]
        if decompressed:
            content = _do_compress(subpath, content)
        backup_dir.write_file(subpath, content, text=False)

    def remove_instance_wal(self, backup_dir, instance, file):
        backup_dir.remove_file(f'wal/{instance}/{file}')

    def instance_wal_exists(self, backup_dir, instance, file):
        fl = f'wal/{instance}/{file}'
        return backup_dir.exists(fl)

    def wait_instance_wal_exists(self, backup_dir, instance, file, timeout=300):
        start = time.time()
        fl = f'wal/{instance}/{file}'
        while time.time() - start < timeout:
            if backup_dir.exists(fl):
                break
            time.sleep(0.25)

    def wait_server_wal_exists(self, data_dir, wal_dir, file, timeout=300):
        start = time.time()
        fl = f'{data_dir}/{wal_dir}/{file}'
        while time.time() - start < timeout:
            if os.path.exists(fl):
                return
            time.sleep(0.25)

    def remove_instance_waldir(self, backup_dir, instance):
        backup_dir.remove_dir(f'wal/{instance}')

    # used for partial restore
    def truncate_every_file_in_dir(self, path):
        for file in os.listdir(path):
            with open(os.path.join(path, file), "w") as f:
                f.close()

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

    def read_backup_content_control(self, backup_id, instance_name):
        """
        Read the content control file of a backup.
        Args: backup_id (str): The ID of the backup.
              instance_name (str): The name of the instance
        Returns: dict: The parsed JSON content of the backup_content.control file.
        Raises:
            FileNotFoundError: If the backup content control file does not exist.
            json.JSONDecodeError: If the backup content control file is not a valid JSON.
        """
        content_control_path = f'{self.backup_dir.path}/backups/{instance_name}/{backup_id}/backup_content.control'

        if not os.path.exists(content_control_path):
            raise FileNotFoundError(f"Backup content control file '{content_control_path}' does not exist.")

        try:
            with open(content_control_path) as file:
                lines = file.readlines()
                content_control_json = []
                for line in lines:
                    content_control_json.append(json.loads(line))
                return content_control_json
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Failed to parse JSON in backup content control file '{content_control_path}'",
                                       e.doc, e.pos)

    def run_pb(self, backup_dir, command, gdb=False, old_binary=False, return_id=True, env=None,
               skip_log_directory=False, expect_error=False):
        return self.pb.run(command, gdb, old_binary, return_id, env, skip_log_directory, expect_error, use_backup_dir=backup_dir)

    def clean_pb(self, backup_dir):
        fs_backup_class(backup_dir).cleanup()

    def get_recovery_conf(self, node):
        out_dict = {}

        if self.pg_config_version >= self.version_to_num('12.0'):
            recovery_conf_path = os.path.join(node.data_dir, 'postgresql.auto.conf')
            with open(recovery_conf_path, 'r') as f:
                print(f.read())
        else:
            recovery_conf_path = os.path.join(node.data_dir, 'recovery.conf')

        with open(
                recovery_conf_path, 'r'
        ) as recovery_conf:
            for line in recovery_conf:
                try:
                    key, value = line.split('=')
                except:
                    continue
                out_dict[key.strip()] = value.strip(" '").replace("'\n", "")
        return out_dict

    def get_restore_command(self, backup_dir, instance):

        # parse postgresql.auto.conf
        restore_command = " ".join([f'"{self.probackup_path}"',
                                    'archive-get', *backup_dir.pb_args])
        if os.name == 'nt':
            restore_command.replace("\\", "\\\\")
        restore_command += f' --instance={instance}'

        # don`t forget to kill old_binary after remote ssh release
        if self.remote:
            restore_command += ' --remote-proto=ssh'
            restore_command += ' --remote-host=localhost'

        if os.name == 'posix':
            restore_command += ' --wal-file-path=%p --wal-file-name=%f'

        elif os.name == 'nt':
            restore_command += ' --wal-file-path="%p" --wal-file-name="%f"'

        return restore_command

    def set_replica(
            self, master, replica,
            replica_name='replica',
            synchronous=False,
            log_shipping=False
    ):

        replica.set_auto_conf(
            options={
                'port': replica.port,
                'hot_standby': 'on'})

        if self.pg_config_version >= self.version_to_num('12.0'):
            with open(os.path.join(replica.data_dir, "standby.signal"), 'w') as f:
                f.flush()
                f.close()

            config = 'postgresql.auto.conf'

            if not log_shipping:
                replica.set_auto_conf(
                    {'primary_conninfo': 'user={0} port={1} application_name={2} '
                                         ' sslmode=prefer sslcompression=1'.format(
                        self.username, master.port, replica_name)},
                    config)
        else:
            replica.append_conf('recovery.conf', 'standby_mode = on')

            if not log_shipping:
                replica.append_conf(
                    'recovery.conf',
                    "primary_conninfo = 'user={0} port={1} application_name={2}"
                    " sslmode=prefer sslcompression=1'".format(
                        self.username, master.port, replica_name))

        if synchronous:
            master.set_auto_conf(
                options={
                    'synchronous_standby_names': replica_name,
                    'synchronous_commit': 'remote_apply'})

            master.reload()

    class ControlFileContainer(object):
        __slots__ = ('data',)

        def __init__(self, data):
            self.data = data

    @contextlib.contextmanager
    def modify_backup_control(self, backup_dir, instance, backup_id, content=False):
        file = 'backup.control'
        if content:
            file = 'backup_content.control'
        path = os.path.join('backups', instance, backup_id, file)
        control_file = backup_dir.read_file(path)
        cf = ProbackupTest.ControlFileContainer(control_file)
        yield cf
        if control_file != cf.data:
            backup_dir.write_file(path, cf.data)

    def change_backup_status(self, backup_dir, instance, backup_id, status):
        with self.modify_backup_control(backup_dir, instance, backup_id) as cf:
            cf.data = re.sub(r'status = \w+', f'status = {status}', cf.data, 1)

    def get_locks(self, backup_dir : TestBackupDir, node : str):
        path = "backups/" + node + "/locks"
        return backup_dir.list_files(path)

    def read_lock(self, backup_dir : TestBackupDir, node : str, lock : str):
        path = "backups/" + node + "/locks/" + lock
        return backup_dir.read_file(path, text=False)

    def expire_locks(self, backup_dir : TestBackupDir, node : str, seconds=1):
        path = "backups/" + node + "/locks"
        now = time.time()
        expired = base36enc(int(now) - seconds)
        for lock in backup_dir.list_files(path):
            base, ts, exclusive = lock.rsplit("_", 2)
            lock_expired = "_".join([base, expired, exclusive])
            content = backup_dir.read_file(path+"/"+lock, text = False)
            backup_dir.remove_file(path+"/"+lock)
            backup_dir.write_file(path+"/"+lock_expired, content, text = False)

    def guc_wal_segment_size(self, node):
        var = node.execute(
            'postgres',
            "select setting from pg_settings where name = 'wal_segment_size'"
        )
        print(int(var[0][0]))
        return int(var[0][0])

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

    def version_to_num(self, version):
        if not version:
            return 0
        parts = version.split('.')
        while len(parts) < 3:
            parts.append('0')
        num = 0
        for part in parts:
            num = num * 100 + int(re.sub(r"[^\d]", "", part))
        return num

    def switch_wal_segment(self, node, sleep_seconds=1, and_tx=False):
        """
        Execute pg_switch_wal() in given node

        Args:
            node: an instance of PostgresNode or NodeConnection class
        """
        if isinstance(node, testgres.PostgresNode):
            with node.connect('postgres') as con:
                if and_tx:
                    con.execute('select txid_current()')
                lsn = con.execute('select pg_switch_wal()')[0][0]
        else:
            lsn = node.execute('select pg_switch_wal()')[0][0]

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        return lsn

    @contextlib.contextmanager
    def switch_wal_after(self, node, seconds, and_tx=True):
        tm = threading.Timer(seconds, self.switch_wal_segment, [node, 0, and_tx])
        tm.start()
        try:
            yield
        finally:
            tm.cancel()
            tm.join()

    def wait_until_replica_catch_with_master(self, master, replica):
        master_function = 'pg_catalog.pg_current_wal_insert_lsn()'

        lsn = master.safe_psql(
            'postgres',
            'SELECT {0}'.format(master_function)).decode('utf-8').rstrip()

        # Wait until replica catch up with master
        self.wait_until_lsn_replayed(replica, lsn)
        return lsn

    def wait_until_lsn_replayed(self, replica, lsn):
        replica_function = 'pg_catalog.pg_last_wal_replay_lsn()'
        replica.poll_query_until(
            'postgres',
            "SELECT '{0}'::pg_lsn <= {1}".format(lsn, replica_function))

    def get_ptrack_version(self, node):
        version = node.safe_psql(
            "postgres",
            "SELECT extversion "
            "FROM pg_catalog.pg_extension WHERE extname = 'ptrack'").decode('utf-8').rstrip()
        return self.version_to_num(version)

    def get_bin_path(self, binary):
        return testgres.get_bin_path(binary)

    def del_test_dirs(self):
        """ Del testdir and optimistically try to del module dir"""
        # Remove FS dependent part first
        self.backup_dir.cleanup()
        # Remove all the rest
        if init_params.delete_logs:
            shutil.rmtree(self.test_path, ignore_errors=True)

    def pgdata_content(self, pgdata, ignore_ptrack=True, exclude_dirs=None):
        """ return dict with directory content. "
        " TAKE IT AFTER CHECKPOINT or BACKUP"""
        dirs_to_ignore = {
            'pg_xlog', 'pg_wal', 'pg_log',
            'pg_stat_tmp', 'pg_subtrans', 'pg_notify'
        }
        files_to_ignore = {
            'postmaster.pid', 'postmaster.opts',
            'pg_internal.init', 'postgresql.auto.conf',
            'backup_label', 'backup_label.old',
            'tablespace_map', 'recovery.conf',
            'ptrack_control', 'ptrack_init', 'pg_control',
            'probackup_recovery.conf', 'recovery.signal',
            'standby.signal', 'ptrack.map', 'ptrack.map.mmap',
            'ptrack.map.tmp', 'recovery.done'
        }

        if exclude_dirs:
            dirs_to_ignore |= set(exclude_dirs)
        #        suffixes_to_ignore = (
        #            '_ptrack'
        #        )
        directory_dict = {}
        directory_dict['pgdata'] = pgdata
        directory_dict['files'] = {}
        directory_dict['dirs'] = {}
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
                cfile = ContentFile(file.isdigit())
                directory_dict['files'][file_relpath] = cfile
                with open(file_fullpath, 'rb') as f:
                    # truncate cfm's content's zero tail
                    if file_relpath.endswith('.cfm'):
                        content = f.read()
                        zero64 = b"\x00" * 64
                        l = len(content)
                        while l > 64:
                            s = (l - 1) & ~63
                            if content[s:l] != zero64[:l - s]:
                                break
                            l = s
                        content = content[:l]
                        digest = hashlib.md5(content)
                    else:
                        digest = hashlib.md5()
                        while True:
                            b = f.read(64 * 1024)
                            if not b: break
                            digest.update(b)
                    cfile.md5 = digest.hexdigest()

                # crappy algorithm
                if cfile.is_datafile:
                    size_in_pages = os.path.getsize(file_fullpath) / 8192
                    cfile.md5_per_page = self.get_md5_per_page_for_fork(
                        file_fullpath, size_in_pages
                    )

            for directory in dirs:
                directory_path = os.path.join(root, directory)
                directory_relpath = os.path.relpath(directory_path, pgdata)
                parent = os.path.dirname(directory_relpath)
                if parent in directory_dict['dirs']:
                    del directory_dict['dirs'][parent]
                directory_dict['dirs'][directory_relpath] = ContentDir()

        # get permissions for every file and directory
        for dir, cdir in directory_dict['dirs'].items():
            full_path = os.path.join(pgdata, dir)
            cdir.mode = os.stat(full_path).st_mode

        for file, cfile in directory_dict['files'].items():
            full_path = os.path.join(pgdata, file)
            cfile.mode = os.stat(full_path).st_mode

        return directory_dict

    def get_known_bugs_comparision_exclusion_dict(self, node):
        """ get dict of known datafiles difference, that can be used in compare_pgdata() """
        comparision_exclusion_dict = dict()

        # bug in spgist metapage update (PGPRO-5707)
        spgist_filelist = node.safe_psql(
            "postgres",
            "SELECT pg_catalog.pg_relation_filepath(pg_class.oid) "
            "FROM pg_am, pg_class "
            "WHERE pg_am.amname = 'spgist' "
            "AND pg_class.relam = pg_am.oid"
        ).decode('utf-8').rstrip().splitlines()
        for filename in spgist_filelist:
            comparision_exclusion_dict[filename] = set([0])

        return comparision_exclusion_dict

    def compare_pgdata(self, original_pgdata, restored_pgdata, exclusion_dict=dict()):
        """
        return dict with directory content. DO IT BEFORE RECOVERY
        exclusion_dict is used for exclude files (and it block_no) from comparision
        it is a dict with relative filenames as keys and set of block numbers as values
        """
        fail = False
        error_message = 'Restored PGDATA is not equal to original!\n'

        # Compare directories
        restored_dirs = set(restored_pgdata['dirs'])
        original_dirs = set(original_pgdata['dirs'])

        for directory in sorted(restored_dirs - original_dirs):
            fail = True
            error_message += '\nDirectory was not present'
            error_message += ' in original PGDATA: {0}\n'.format(
                os.path.join(restored_pgdata['pgdata'], directory))

        for directory in sorted(original_dirs - restored_dirs):
            fail = True
            error_message += '\nDirectory dissappeared'
            error_message += ' in restored PGDATA: {0}\n'.format(
                os.path.join(restored_pgdata['pgdata'], directory))

        for directory in sorted(original_dirs & restored_dirs):
            original = original_pgdata['dirs'][directory]
            restored = restored_pgdata['dirs'][directory]
            if original.mode != restored.mode:
                fail = True
                error_message += '\nDir permissions mismatch:\n'
                error_message += ' Dir old: {0} Permissions: {1}\n'.format(
                    os.path.join(original_pgdata['pgdata'], directory),
                    original.mode)
                error_message += ' Dir new: {0} Permissions: {1}\n'.format(
                    os.path.join(restored_pgdata['pgdata'], directory),
                    restored.mode)

        restored_files = set(restored_pgdata['files'])
        original_files = set(original_pgdata['files'])

        for file in sorted(restored_files - original_files):
            # File is present in RESTORED PGDATA
            # but not present in ORIGINAL
            # only backup_label is allowed
            fail = True
            error_message += '\nFile is not present'
            error_message += ' in original PGDATA: {0}\n'.format(
                os.path.join(restored_pgdata['pgdata'], file))

        for file in sorted(original_files - restored_files):
            error_message += (
                '\nFile disappearance.\n '
                'File: {0}\n').format(
                os.path.join(restored_pgdata['pgdata'], file)
            )
            fail = True

        for file in sorted(original_files & restored_files):
            original = original_pgdata['files'][file]
            restored = restored_pgdata['files'][file]
            if restored.mode != original.mode:
                fail = True
                error_message += '\nFile permissions mismatch:\n'
                error_message += ' File_old: {0} Permissions: {1:o}\n'.format(
                    os.path.join(original_pgdata['pgdata'], file),
                    original.mode)
                error_message += ' File_new: {0} Permissions: {1:o}\n'.format(
                    os.path.join(restored_pgdata['pgdata'], file),
                    restored.mode)

            if original.md5 != restored.md5:
                if file not in exclusion_dict:
                    fail = True
                    error_message += (
                        '\nFile Checksum mismatch.\n'
                        'File_old: {0}\nChecksum_old: {1}\n'
                        'File_new: {2}\nChecksum_new: {3}\n').format(
                        os.path.join(original_pgdata['pgdata'], file),
                        original.md5,
                        os.path.join(restored_pgdata['pgdata'], file),
                        restored.md5
                    )

                if not original.is_datafile:
                    continue

                original_pages = set(original.md5_per_page)
                restored_pages = set(restored.md5_per_page)

                for page in sorted(original_pages - restored_pages):
                    error_message += '\n Page {0} dissappeared.\n File: {1}\n'.format(
                        page,
                        os.path.join(restored_pgdata['pgdata'], file)
                    )

                for page in sorted(restored_pages - original_pages):
                    error_message += '\n Extra page {0}\n File: {1}\n'.format(
                        page,
                        os.path.join(restored_pgdata['pgdata'], file))

                for page in sorted(original_pages & restored_pages):
                    if file in exclusion_dict and page in exclusion_dict[file]:
                        continue

                    if original.md5_per_page[page] != restored.md5_per_page[page]:
                        fail = True
                        error_message += (
                            '\n Page checksum mismatch: {0}\n '
                            ' PAGE Checksum_old: {1}\n '
                            ' PAGE Checksum_new: {2}\n '
                            ' File: {3}\n'
                        ).format(
                            page,
                            original.md5_per_page[page],
                            restored.md5_per_page[page],
                            os.path.join(
                                restored_pgdata['pgdata'], file)
                        )

        self.assertFalse(fail, error_message)

    def compare_instance_dir(self, original_instance, after_backup_instance, exclusion_dict=dict()):
        """
        exclusion_dict is used for exclude files (and it block_no) from comparision
        it is a dict with relative filenames as keys and set of block numbers as values
        """
        fail = False
        error_message = 'Instance directory is not equal to original!\n'

        # Compare directories
        after_backup = set(after_backup_instance['dirs'])
        original_dirs = set(original_instance['dirs'])

        for directory in sorted(after_backup - original_dirs):
            fail = True
            error_message += '\nDirectory was not present'
            error_message += ' in original instance: {0}\n'.format(directory)

        for directory in sorted(original_dirs - after_backup):
            fail = True
            error_message += '\nDirectory dissappeared'
            error_message += ' in instance after backup: {0}\n'.format(directory)

        for directory in sorted(original_dirs & after_backup):
            original = original_instance['dirs'][directory]
            after_backup = after_backup_instance['dirs'][directory]
            if original.mode != after_backup.mode:
                fail = True
                error_message += '\nDir permissions mismatch:\n'
                error_message += ' Dir old: {0} Permissions: {1}\n'.format(directory,
                                                                           original.mode)
                error_message += ' Dir new: {0} Permissions: {1}\n'.format(directory,
                                                                           after_backup.mode)

        after_backup_files = set(after_backup_instance['files'])
        original_files = set(original_instance['files'])

        for file in sorted(after_backup_files - original_files):
            # File is present in instance after backup
            # but not present in original instance
            # only backup_label is allowed
            fail = True
            error_message += '\nFile is not present'
            error_message += ' in original instance: {0}\n'.format(file)

        for file in sorted(original_files - after_backup_files):
            error_message += (
                '\nFile disappearance.\n '
                'File: {0}\n').format(file)
            fail = True

        for file in sorted(original_files & after_backup_files):
            original = original_instance['files'][file]
            after_backup = after_backup_instance['files'][file]
            if after_backup.mode != original.mode:
                fail = True
                error_message += '\nFile permissions mismatch:\n'
                error_message += ' File_old: {0} Permissions: {1:o}\n'.format(file,
                                                                              original.mode)
                error_message += ' File_new: {0} Permissions: {1:o}\n'.format(file,
                                                                              after_backup.mode)

            if original.md5 != after_backup.md5:
                if file not in exclusion_dict:
                    fail = True
                    error_message += (
                        '\nFile Checksum mismatch.\n'
                        'File_old: {0}\nChecksum_old: {1}\n'
                        'File_new: {2}\nChecksum_new: {3}\n').format(file,
                                                                     original.md5, file, after_backup.md5
                                                                     )

                if not original.is_datafile:
                    continue

                original_pages = set(original.md5_per_page)
                after_backup_pages = set(after_backup.md5_per_page)

                for page in sorted(original_pages - after_backup_pages):
                    error_message += '\n Page {0} dissappeared.\n File: {1}\n'.format(
                        page, file)


                for page in sorted(after_backup_pages - original_pages):
                    error_message += '\n Extra page {0}\n File: {1}\n'.format(
                        page,  file)

                for page in sorted(original_pages & after_backup_pages):
                    if file in exclusion_dict and page in exclusion_dict[file]:
                        continue

                    if original.md5_per_page[page] != after_backup.md5_per_page[page]:
                        fail = True
                        error_message += (
                            '\n Page checksum mismatch: {0}\n '
                            ' PAGE Checksum_old: {1}\n '
                            ' PAGE Checksum_new: {2}\n '
                            ' File: {3}\n'
                        ).format(
                            page,
                            original.md5_per_page[page],
                            after_backup.md5_per_page[page],
                            file
                        )

        self.assertFalse(fail, error_message)


    def gdb_attach(self, pid):
        return GDBobj([str(pid)], self, attach=True)

    def assertMessage(self, actual=None, *, contains=None, regex=None, has_no=None):
        if actual is None:
            actual = self.output
        if self.output and self.output != actual:  # Don't want to see this twice
            error_message = '\n Unexpected Error Message: `{0}`\n CMD: `{1}`'.format(repr(self.output),
                                                                                     self.cmd)
        else:
            error_message = '\n Unexpected Error Message. CMD: `{0}`'.format(self.cmd)
        if contains:
            self.assertIn(contains, actual, error_message)
        elif regex:
            self.assertRegex(actual, regex, error_message)
        elif has_no:
            self.assertNotIn(has_no, actual, error_message)

def get_relative_path(run_path, data_dir):
    run_path_parts = run_path.split('/')
    data_dir_parts = data_dir.split('/')

    # Find index of the first different element in the lists
    diff_index = 0
    for i in range(min(len(run_path_parts), len(data_dir_parts))):
        if run_path_parts[i] != data_dir_parts[i]:
            diff_index = i
            break

    # Build relative path
    relative_path = ['..'] * (len(run_path_parts) - diff_index) + data_dir_parts[diff_index:]

    return '/'.join(relative_path)


class ContentFile(object):
    __slots__ = ('is_datafile', 'mode', 'md5', 'md5_per_page')

    def __init__(self, is_datafile: bool):
        self.is_datafile = is_datafile


class ContentDir(object):
    __slots__ = ('mode')

def _lz4_decompress(data):
    with lz4.frame.open(io.BytesIO(data), 'rb') as fl:
        return fl.read()

def _lz4_compress(data):
    out = io.BytesIO()
    with lz4.frame.open(out, 'wb', content_checksum=True) as fl:
        fl.write(data)
    return out.getvalue()

def _do_compress(file, data):
    if file.endswith('.gz'):
        return gzip.compress(data, compresslevel=1)
    elif file.endswith('.lz4'):
        return _lz4_compress(data)
    elif file.endswith('.zst'):
        return zstd.compress(data, 1, 1)
    else:
        return data

def _do_decompress(file, data):
    if file.endswith('.gz'):
        return gzip.decompress(data)
    elif file.endswith('.lz4'):
        return _lz4_decompress(data)
    elif file.endswith('.zst'):
        return zstd.decompress(data)
    else:
        return data
