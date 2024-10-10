from .helpers.ptrack_helpers import ProbackupTest


class AuthorizationTest(ProbackupTest):
    """
    Check connect to S3 via pre_start_checks() function
    calling pg_probackup init --s3

    test that s3 keys allow to connect to all types of storages
    """

    def test_s3_auth_test(self):
        console_output = self.pb.init(options=["--log-level-console=VERBOSE"])

        self.assertNotIn(': 403', console_output)  # Because we can have just '403' substring in timestamp
        self.assertMessage(console_output, contains='S3_pre_start_check successful')
        self.assertIn(
            f"INFO: Backup catalog '{self.backup_dir}' successfully initialized",
            console_output)
