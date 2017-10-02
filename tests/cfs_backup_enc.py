import os

from .cfs_backup_noenc import CfsBackupNoEncTest
from .helpers.cfs_helpers import find_by_name

module_name = 'cfs_backup_enc'
tblspace_name = 'cfs_tblspace'


class CfsBackupEncTest(CfsBackupNoEncTest):
    # --- Begin --- #
    def setUp(self):
        os.environ["PG_CIPHER_KEY"] = "super_secret_cipher_key"
        super(CfsBackupEncTest, self).setUp()
