import os
import unittest
import random

from .helpers.cfs_helpers import find_by_extensions, find_by_name, find_by_pattern, corrupt_file
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException

module_name = 'cfs_validate_backup'
tblspace_name = 'cfs_tblspace'


class CfsValidateBackupNoenc(ProbackupTest,unittest.TestCase):
    def setUp(self):
        pass

    def test_validate_fullbackup_empty_tablespace_after_delete_pg_compression(self):
        pass

    def tearDown(self):
        pass


class CfsValidateBackupNoenc(CfsValidateBackupNoenc):
    os.environ["PG_CIPHER_KEY"] = "super_secret_cipher_key"
    super(CfsValidateBackupNoenc).setUp()