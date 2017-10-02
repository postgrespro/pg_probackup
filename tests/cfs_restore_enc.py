import os

from .cfs_restore_noenc import CfsRestoreNoencEmptyTablespaceTest,CfsRestoreNoencTest

module_name = 'cfs_restore_enc'


class CfsRestoreEncEmptyTablespaceTest(CfsRestoreNoencEmptyTablespaceTest):
    # --- Begin --- #
    def setUp(self):
        os.environ["PG_CIPHER_KEY"] = "super_secret_cipher_key"
        super(CfsRestoreNoencEmptyTablespaceTest, self).setUp()


class CfsRestoreEncTest(CfsRestoreNoencTest):
    # --- Begin --- #
    def setUp(self):
        os.environ["PG_CIPHER_KEY"] = "super_secret_cipher_key"
        super(CfsRestoreNoencTest, self).setUp()
