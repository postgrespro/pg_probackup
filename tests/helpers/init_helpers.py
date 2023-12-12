from functools import reduce
import getpass
import os
import re
import shutil
import subprocess
import sys
import testgres

try:
    import lz4.frame
    HAVE_LZ4 = True
except ImportError as e:
    HAVE_LZ4 = False
    LZ4_error = e

try:
    import zstd
    HAVE_ZSTD = True
except ImportError as e:
    HAVE_ZSTD = False
    ZSTD_error = e

delete_logs = os.getenv('KEEP_LOGS') not in ['1', 'y', 'Y']

try:
    testgres.configure_testgres(
        cache_initdb=False,
        cached_initdb_dir=False,
        node_cleanup_full=delete_logs)
except:
    pass


class Init(object):
    def __init__(self):
        if '-v' in sys.argv or '--verbose' in sys.argv:
            self.verbose = True
        else:
            self.verbose = False

        self._pg_config = testgres.get_pg_config()
        self.is_enterprise = self._pg_config.get('PGPRO_EDITION', None) == 'enterprise'
        self.is_shardman = self._pg_config.get('PGPRO_EDITION', None) == 'shardman'
        self.is_pgpro = 'PGPRO_EDITION' in self._pg_config
        self.is_nls_enabled = 'enable-nls' in self._pg_config['CONFIGURE']
        self.is_lz4_enabled = '-llz4' in self._pg_config['LIBS']
        version = self._pg_config['VERSION'].rstrip('develalphabetapre')
        parts = [*version.split(' ')[1].split('.'), '0', '0'][:3]
        parts[0] = re.match(r'\d+', parts[0]).group()
        self.pg_config_version = reduce(lambda v, x: v*100+int(x), parts, 0)

        test_env = os.environ.copy()
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
            test_env.pop(e, None)

        test_env['LC_MESSAGES'] = 'C'
        test_env['LC_TIME'] = 'C'
        self._test_env = test_env

        helpers_path = os.path.dirname(os.path.realpath(__file__))
        self.tests_source_path = os.path.abspath(
            os.path.join(helpers_path, os.pardir)
        )
        tmp_path = test_env.get('PGPROBACKUP_TMP_DIR')
        if tmp_path and os.path.isabs(tmp_path):
            self.tmp_path = tmp_path
        else:
            self.tmp_path = os.path.abspath(
                os.path.join(self.tests_source_path, tmp_path or 'tmp_dirs')
            )

        os.makedirs(self.tmp_path, exist_ok=True)

        self.username = getpass.getuser()

        self.probackup_path = None
        if 'PGPROBACKUPBIN' in test_env:
            if shutil.which(test_env["PGPROBACKUPBIN"]):
                self.probackup_path = test_env["PGPROBACKUPBIN"]
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
                self.tests_source_path, '../pg_probackup'))

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
            self.EXTERNAL_DIRECTORY_DELIMITER = ':'
            os.environ['PATH'] = os.path.dirname(
                self.probackup_path) + ':' + os.environ['PATH']

        elif os.name == 'nt':
            self.EXTERNAL_DIRECTORY_DELIMITER = ';'
            os.environ['PATH'] = os.path.dirname(
                self.probackup_path) + ';' + os.environ['PATH']

        self.probackup_old_path = None
        if 'PGPROBACKUPBIN_OLD' in test_env:
            if (
                    os.path.isfile(test_env['PGPROBACKUPBIN_OLD']) and
                    os.access(test_env['PGPROBACKUPBIN_OLD'], os.X_OK)
            ):
                self.probackup_old_path = test_env['PGPROBACKUPBIN_OLD']
            else:
                if self.verbose:
                    print('PGPROBACKUPBIN_OLD is not an executable file')

        self.probackup_version = None
        self.old_probackup_version = None

        probackup_version_output = subprocess.check_output(
            [self.probackup_path, "--version"],
            stderr=subprocess.STDOUT,
        ).decode('utf-8')
        self.probackup_version = re.search(r"\d+\.\d+\.\d+",
                                           probackup_version_output
                                           ).group(0)
        compressions = re.search(r"\(compressions: ([^)]*)\)",
                                 probackup_version_output).group(1)
        self.probackup_compressions = {s.strip() for s in compressions.split(',')}

        if self.probackup_old_path:
            old_probackup_version_output = subprocess.check_output(
                [self.probackup_old_path, "--version"],
                stderr=subprocess.STDOUT,
            ).decode('utf-8')
            self.old_probackup_version = re.search(r"\d+\.\d+\.\d+",
                                                   old_probackup_version_output
                                                   ).group(0)

        self.remote = test_env.get('PGPROBACKUP_SSH_REMOTE', None) == 'ON'
        self.ptrack = test_env.get('PG_PROBACKUP_PTRACK', None) == 'ON' and \
                        self.pg_config_version >= 110000

        self.paranoia = test_env.get('PG_PROBACKUP_PARANOIA', None) == 'ON'
        env_compress = test_env.get('ARCHIVE_COMPRESSION', None)
        if env_compress:
            env_compress = env_compress.lower()
        if env_compress in ('on', 'zlib'):
            self.compress_suffix = '.gz'
            self.archive_compress = 'zlib'
        elif env_compress == 'lz4':
            if not HAVE_LZ4:
                raise LZ4_error
            if 'lz4' not in self.probackup_compressions:
                raise Exception("pg_probackup is not compiled with lz4 support")
            self.compress_suffix = '.lz4'
            self.archive_compress = 'lz4'
        elif env_compress == 'zstd':
            if not HAVE_ZSTD:
                raise ZSTD_error
            if 'zstd' not in self.probackup_compressions:
                raise Exception("pg_probackup is not compiled with zstd support")
            self.compress_suffix = '.zst'
            self.archive_compress = 'zstd'
        else:
            self.compress_suffix = ''
            self.archive_compress = False

        cfs_compress = test_env.get('PG_PROBACKUP_CFS_COMPRESS', None)
        if cfs_compress:
            self.cfs_compress = cfs_compress.lower()
        else:
            self.cfs_compress = self.archive_compress

        os.environ["PGAPPNAME"] = "pg_probackup"
        self.delete_logs = delete_logs

    def test_env(self):
        return self._test_env.copy()

init_params = Init()
