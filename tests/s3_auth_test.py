from tests.test_utils.base_test import S3BaseTest


class AuthorizationTest(S3BaseTest):
    """
    Check connect to S3 via pre_start_checks() function
    calling pg_probackup init --s3

    test that s3 keys allow to connect to all types of storages
    """

    def test_s3_authorisation(self):
        console_output = self.pb.init(options=["--log-level-console=VERBOSE"])

        self.assertNotIn(': 403', console_output)  # Because we can have just '403' substring in timestamp
        self.assertMessage(console_output, contains='S3_pre_start_check successful')
        self.assertMessage(console_output, contains='HTTP response: 200')
        self.assertIn(
            f"INFO: Backup catalog '{self.backup_dir}' successfully initialized",
            console_output)

    def test_log_level_file_requires_log_directory(self):
        console_output = self.pb.init(options=["--log-level-file=VERBOSE"],
                                      skip_log_directory=True,
                                      expect_error=True)

        self.assertMessage(console_output,
                           contains='ERROR: Cannot save S3 logs to a file. You must specify --log-directory option when'
                                    ' running backup with --log-level-file option enabled')
