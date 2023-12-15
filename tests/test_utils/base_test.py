import os
import sys
import unittest

root = os.path.realpath(os.path.join(os.path.dirname(__file__), '../..'))
if root not in sys.path:
    sys.path.append(root)

from tests.helpers.ptrack_helpers import ProbackupTest
from . import config_provider


class S3BaseTest(ProbackupTest, unittest.TestCase):

    additional_options = []

    def setUp(self) -> None:
        # If env vars weren't set, think that it is a local run and set up variables
        if not os.environ.get('PG_PROBACKUP_S3_HOST'):
            minio_config = config_provider.read_config()['MINIO']
            os.environ.setdefault('PG_PROBACKUP_S3_ACCESS_KEY', minio_config['access_key'])
            os.environ.setdefault('PG_PROBACKUP_S3_SECRET_ACCESS_KEY', minio_config['secret_key'])
            os.environ.setdefault('PG_PROBACKUP_S3_HOST', minio_config['local_host'])
            os.environ.setdefault('PG_PROBACKUP_S3_PORT', minio_config['api_port'])
            os.environ.setdefault('PG_PROBACKUP_S3_BUCKET_NAME', minio_config['bucket_name'])
            os.environ.setdefault('PG_PROBACKUP_S3_REGION', minio_config['region'])
        # There will be a connector setting
        if not os.environ.get("PROBACKUP_S3_TYPE_FULL_TEST"):
            os.environ.setdefault('PROBACKUP_S3_TYPE_FULL_TEST', 'minio')
