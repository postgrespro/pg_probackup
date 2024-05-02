import os
import io
import sys

import minio
from minio import Minio
from minio.deleteobjects import DeleteObject
import urllib3
from pg_probackup2.storage.fs_backup import TestBackupDir
from pg_probackup2.init_helpers import init_params
from . import config_provider

root = os.path.realpath(os.path.join(os.path.dirname(__file__), '../..'))
if root not in sys.path:
    sys.path.append(root)

status_forcelist = [413,  # RequestBodyTooLarge
                    429,  # TooManyRequests
                    500,  # InternalError
                    503,  # ServerBusy
                    ]

DEFAULT_CONF_FILE = 's3/tests/s3.conf'


class S3TestBackupDir(TestBackupDir):
    is_file_based = False

    def __init__(self, *, rel_path, backup):
        self.access_key = None
        self.secret_key = None
        self.s3_type = None
        self.tmp_path = None
        self.host = None
        self.port = None
        self.bucket_name = None
        self.region = None
        self.bucket = None
        self.path_suffix = None
        self.https = None
        self.s3_config_file = None
        self.ca_certificate = None

        self.set_s3_config_file()
        self.setup_s3_env()

        path = "pg_probackup"
        if self.path_suffix:
            path += "_" + self.path_suffix
        if self.tmp_path == '' or os.path.isabs(self.tmp_path):
            self.path = f"{path}{self.tmp_path}/{rel_path}/{backup}"
        else:
            self.path = f"{path}/{self.tmp_path}/{rel_path}/{backup}"

        secure: bool = False
        self.versioning: bool = False
        if self.https in ['ON', 'HTTPS']:
            secure = True
        if self.https and self.ca_certificate:
            http_client = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',
                                              ca_certs=self.ca_certificate,
                                              retries=urllib3.Retry(total=5,
                                                                    backoff_factor=1,
                                                                    status_forcelist=status_forcelist))
        else:
            http_client = urllib3.PoolManager(retries=urllib3.Retry(total=5,
                                                                    backoff_factor=1,
                                                                    status_forcelist=status_forcelist))

        self.conn = Minio(self.host + ":" + self.port, secure=secure, access_key=self.access_key,
                          secret_key=self.secret_key, http_client=http_client)
        if not self.conn.bucket_exists(self.bucket):
            raise Exception(f"Test bucket {self.bucket} does not exist.")

        try:
            config = self.conn.get_bucket_versioning(self.bucket)
            if config.status.lower() == "enabled" or config.status.lower() == "suspended":
                self.versioning = True
            else:
                self.versioning = False
        except Exception as e:
            if "NotImplemented" in repr(e):
                self.versioning = False
            else:
                raise e
        self.pb_args = ('-B', '/' + self.path, f'--s3={init_params.s3_type}')
        if self.s3_config_file:
            self.pb_args += (f'--s3-config-file={self.s3_config_file}',)
        return

    def setup_s3_env(self, s3_config=None):
        self.tmp_path = os.environ.get('PGPROBACKUP_TMP_DIR', default='')
        self.host = os.environ.get('PG_PROBACKUP_S3_HOST', default='')

        # If environment variables are not setup, use from config
        if self.s3_config_file or s3_config:
            minio_config = config_provider.read_config(self.s3_config_file or s3_config)
            self.access_key = minio_config['access-key']
            self.secret_key = minio_config['secret-key']
            self.host = minio_config['s3-host']
            self.port = minio_config['s3-port']
            self.bucket = minio_config['s3-bucket']
            self.region = minio_config['s3-region']
            self.https = minio_config['s3-secure']
            init_params.s3_type = 'minio'
        else:
            self.access_key = os.environ.get('PG_PROBACKUP_S3_ACCESS_KEY')
            self.secret_key = os.environ.get('PG_PROBACKUP_S3_SECRET_ACCESS_KEY')
            self.host = os.environ.get('PG_PROBACKUP_S3_HOST')
            self.port = os.environ.get('PG_PROBACKUP_S3_PORT')
            self.bucket = os.environ.get('PG_PROBACKUP_S3_BUCKET_NAME')
            self.region = os.environ.get('PG_PROBACKUP_S3_REGION')
            self.https = os.environ.get('PG_PROBACKUP_S3_HTTPS')
            self.ca_certificate = os.environ.get('PG_PROBACKUP_S3_CA_CERTIFICATE')
            init_params.s3_type = os.environ.get('PG_PROBACKUP_S3_TEST')

        # multi-url case
        # remove all urls from string except the first one
        if ';' in self.host:
            self.host = self.host[:self.host.find(';')]
            if ':' in self.host:  # also change port if it was overridden in multihost string
                self.port = self.host[self.host.find(':') + 1:]
                self.host = self.host[:self.host.find(':')]

    def set_s3_config_file(self):
        s3_config = os.environ.get('PG_PROBACKUP_S3_CONFIG_FILE')
        if s3_config is not None and s3_config.strip().lower() == "true":
            self.s3_config_file = DEFAULT_CONF_FILE
        else:
            self.s3_config_file = s3_config

    def list_instance_backups(self, instance):
        full_path = os.path.join(self.path, 'backups', instance)
        candidates = self.conn.list_objects(self.bucket, prefix=full_path, recursive=True)
        return [os.path.basename(os.path.dirname(x.object_name))
                for x in candidates if x.object_name.endswith('backup.control')]

    def list_files(self, sub_dir, recursive=False):
        full_path = os.path.join(self.path, sub_dir)
        #  Need '/' in the end to find inside the folder
        full_path_dir = full_path if full_path[-1] == '/' else full_path + '/'
        object_list = self.conn.list_objects(self.bucket, prefix=full_path_dir, recursive=recursive)
        return [obj.object_name.replace(full_path_dir, '', 1)
                for obj in object_list
                if not obj.is_dir]

    def list_dirs(self, sub_dir):
        full_path = os.path.join(self.path, sub_dir)
        #  Need '/' in the end to find inside the folder
        full_path_dir = full_path if full_path[-1] == '/' else full_path + '/'
        object_list = self.conn.list_objects(self.bucket, prefix=full_path_dir, recursive=False)
        return [obj.object_name.replace(full_path_dir, '', 1).rstrip('\\/')
                for obj in object_list
                if obj.is_dir]

    def read_file(self, sub_path, *, text=True):
        full_path = os.path.join(self.path, sub_path)
        bytes = self.conn.get_object(self.bucket, full_path).read()
        if not text:
            return bytes
        return bytes.decode('utf-8')

    def write_file(self, sub_path, data, *, text=True):
        full_path = os.path.join(self.path, sub_path)
        if text:
            data = data.encode('utf-8')
        self.conn.put_object(self.bucket, full_path, io.BytesIO(data), length=len(data))

    def cleanup(self, dir=''):
        self.remove_dir(dir)

    def remove_file(self, sub_path):
        full_path = os.path.join(self.path, sub_path)
        self.conn.remove_object(self.bucket, full_path)

    def remove_dir(self, sub_path):
        if sub_path:
            full_path = os.path.join(self.path, sub_path)
        else:
            full_path = self.path
        objs = self.conn.list_objects(self.bucket, prefix=full_path, recursive=True,
                                      include_version=self.versioning)
        delobjs = (DeleteObject(o.object_name, o.version_id) for o in objs)
        errs = list(self.conn.remove_objects(self.bucket, delobjs))
        if errs:
            strerrs = "; ".join(str(err) for err in errs)
            raise Exception("There were errors: {0}".format(strerrs))

    def exists(self, sub_path):
        full_path = os.path.join(self.path, sub_path)
        try:
            self.conn.stat_object(self.bucket, full_path)
            return True
        except minio.error.S3Error as s3err:
            if s3err.code == 'NoSuchKey':
                return False
            raise s3err
        except Exception as err:
            raise err

    def __str__(self):
        return '/' + self.path

    def __repr__(self):
        return "S3TestBackupDir" + str(self.path)

    def __fspath__(self):
        return self.path
