import os
import io
import sys

import minio
from minio import Minio
from minio.deleteobjects import DeleteObject
import urllib3

root = os.path.realpath(os.path.join(os.path.dirname(__file__), '../..'))
if root not in sys.path:
    sys.path.append(root)

# Should fail if either of env vars does not exist
host = os.environ['PG_PROBACKUP_S3_HOST']
port = os.environ['PG_PROBACKUP_S3_PORT']
access = os.environ['PG_PROBACKUP_S3_ACCESS_KEY']
secret = os.environ['PG_PROBACKUP_S3_SECRET_ACCESS_KEY']
bucket = os.environ['PG_PROBACKUP_S3_BUCKET_NAME']
path_suffix = os.environ.get("PG_PROBACKUP_TEST_TMP_SUFFIX")
https = os.environ.get("PG_PROBACKUP_S3_HTTPS")

s3_type = os.environ.get('PROBACKUP_S3_TYPE_FULL_TEST')

status_forcelist = [413,  # RequestBodyTooLarge
                    429,  # TooManyRequests
                    500,  # InternalError
                    503,  # ServerBusy
                    ]


class S3TestBackupDir:
    is_file_based = False

    def __init__(self, *, rel_path, backup):
        path = "pg_probackup"
        if path_suffix:
            path += "_" + path_suffix
        self.path = f"{path}/{rel_path}/{backup}"
        secure: bool = False
        if https in ['ON', 'HTTPS']:
            secure = True
        self.conn = Minio(host + ":" + port, secure=secure, access_key=access,
                          secret_key=secret, http_client=urllib3.PoolManager(retries=urllib3.Retry(total=5,
                                                                                                   backoff_factor=1,
                                                                                                   status_forcelist=status_forcelist)))
        if not self.conn.bucket_exists(bucket):
            raise Exception(f"Test bucket {bucket} does not exist.")
        self.pb_args = ('-B', '/' + self.path, f'--s3={s3_type}')
        return

    def list_instance_backups(self, instance):
        full_path = os.path.join(self.path, 'backups', instance)
        candidates = self.conn.list_objects(bucket, prefix=full_path, recursive=True)
        return [os.path.basename(os.path.dirname(x.object_name))
                for x in candidates if x.object_name.endswith('backup.control')]

    def list_files(self, sub_dir, recursive=False):
        full_path = os.path.join(self.path, sub_dir)
        #  Need '/' in the end to find inside the folder
        full_path_dir = full_path if full_path[-1] == '/' else full_path + '/'
        object_list = self.conn.list_objects(bucket, prefix=full_path_dir, recursive=recursive)
        return [obj.object_name.replace(full_path_dir, '', 1)
                for obj in object_list
                if not obj.is_dir]

    def read_file(self, sub_path, *, text=True):
        full_path = os.path.join(self.path, sub_path)
        bytes = self.conn.get_object(bucket, full_path).read()
        if not text:
            return bytes
        return bytes.decode('utf-8')

    def write_file(self, sub_path, data, *, text=True):
        full_path = os.path.join(self.path, sub_path)
        if text:
            data = data.encode('utf-8')
        self.conn.put_object(bucket, full_path, io.BytesIO(data), length=len(data))

    def cleanup(self):
        self.remove_dir('')

    def remove_file(self, sub_path):
        full_path = os.path.join(self.path, sub_path)
        self.conn.remove_object(bucket, full_path)

    def remove_dir(self, sub_path):
        if sub_path:
            full_path = os.path.join(self.path, sub_path)
        else:
            full_path = self.path
        objs = self.conn.list_objects(bucket, prefix=full_path, recursive=True)
        delobjs = (DeleteObject(o.object_name) for o in objs)
        errs = list(self.conn.remove_objects(bucket, delobjs))
        if errs:
            strerrs = "; ".join(str(err) for err in errs)
            raise Exception("There were errors: {0}".format(strerrs))

    def exists(self, sub_path):
        full_path = os.path.join(self.path, sub_path)
        try:
            self.conn.stat_object(bucket, full_path)
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
