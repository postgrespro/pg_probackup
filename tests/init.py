import os
import stat # for chmod
import unittest
from .helpers.ptrack_helpers import dir_files, ProbackupTest, ProbackupException
import shutil

if os.name == 'nt':
    import win32security
    import ntsecuritycon as con

module_name = 'init'


class InitTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_success(self):
        """Success normal init"""
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir=os.path.join(module_name, fname, 'node'))
        self.init_pb(backup_dir)
        self.assertEqual(
            dir_files(backup_dir),
            ['backups', 'wal']
        )
        self.add_instance(backup_dir, 'node', node)
        self.assertIn(
            "INFO: Instance 'node' successfully deleted",
            self.del_instance(backup_dir, 'node'),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        # Show non-existing instance
        try:
            self.show_pb(backup_dir, 'node')
            self.assertEqual(1, 0, 'Expecting Error due to show of non-existing instance. Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Instance 'node' does not exist in this backup catalog",
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(e.message, self.cmd))

        # Delete non-existing instance
        try:
            self.del_instance(backup_dir, 'node1')
            self.assertEqual(1, 0, 'Expecting Error due to delete of non-existing instance. Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Instance 'node1' does not exist in this backup catalog",
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(e.message, self.cmd))

        # Add instance without pgdata
        try:
            self.run_pb([
                "add-instance",
                "--instance=node1",
                "-B", backup_dir
            ])
            self.assertEqual(1, 0, 'Expecting Error due to adding instance without pgdata. Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Required parameter not specified: PGDATA (-D, --pgdata)",
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(e.message, self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_already_exist(self):
        """Failure with backup catalog already existed"""
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir=os.path.join(module_name, fname, 'node'))
        self.init_pb(backup_dir)
        try:
            self.show_pb(backup_dir, 'node')
            self.assertEqual(1, 0, 'Expecting Error due to initialization in non-empty directory. Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Instance 'node' does not exist in this backup catalog",
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)
        
    # @unittest.skip("skip")
    def test_no_rights_for_directory(self):
        """Failure with backup catalog existed and empty but user has no writing permissions"""
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir=os.path.join(module_name, fname, 'node'))
        os.mkdir(backup_dir)
        # changing directory rights
        if os.name == 'nt': # windows block
            user, domain, type = win32security.LookupAccountName("", os.getlogin())
            sd = win32securityGetFileSecurity(backup_dir, win32security.DACL_SECURIRY_INFORMATION)
            dacl = sd.GetSecurityDescriptorDacl()

            dacl.AddAccessDeniedAce(win32security.ACL_REVISION, con.FILE_WRITE_DATA, user) # deny writing permission
            sd.SetSecurityDescriptorDacl(1, dacl, 0)
            win32security.SetFileSecurity(backup_dir, win32security.DACL_SECURIRY_INFORMATION, sd)
        else:
            os.chmod(backup_dir, stat.S_IREAD) # set read-only flag for current user
            assert os.access(backup_dir, os.R_OK) and not os.access(backup_dir, os.W_OK)

        self.init_pb(backup_dir)
        try:
            self.show_pb(backup_dir, 'node')
            self.assertEqual(1, 0, 'Expecting Error due to initialization in empty directory with no rithts for writing. Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Instance 'node' does not exist in this backup catalog",
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_abs_path(self):
        """failure with backup catalog should be given as absolute path"""
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir=os.path.join(module_name, fname, 'node'))
        try:
            self.run_pb(["init", "-B", os.path.relpath("%s/backup" % node.base_dir, self.dir_path)])
            self.assertEqual(1, 0, 'Expecting Error due to initialization with non-absolute path in --backup-path. Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: -B, --backup-path must be an absolute path",
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_add_instance_idempotence(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/219
        """
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir=os.path.join(module_name, fname, 'node'))
        self.init_pb(backup_dir)

        self.add_instance(backup_dir, 'node', node)
        shutil.rmtree(os.path.join(backup_dir, 'backups', 'node'))

        dir_backups = os.path.join(backup_dir, 'backups', 'node')
        dir_wal = os.path.join(backup_dir, 'wal', 'node')

        try:
            self.add_instance(backup_dir, 'node', node)
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because page backup should not be possible "
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Instance 'node' WAL archive directory already exists: ",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        try:
            self.add_instance(backup_dir, 'node', node)
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because page backup should not be possible "
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Instance 'node' WAL archive directory already exists: ",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)
