import unittest
import os
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException


module_name = 'option'


class OptionTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_help_1(self):
        """help options"""
        self.maxDiff = None
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        with open(os.path.join(self.dir_path, "expected/option_help.out"), "rb") as help_out:
            self.assertEqual(
                self.run_pb(["--help"]),
                help_out.read().decode("utf-8")
            )

    # @unittest.skip("skip")
    def test_version_2(self):
        """help options"""
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        with open(os.path.join(self.dir_path, "expected/option_version.out"), "rb") as version_out:
            self.assertIn(
                version_out.read().decode("utf-8"),
                self.run_pb(["--version"])
            )

    # @unittest.skip("skip")
    def test_without_backup_path_3(self):
        """backup command failure without backup mode option"""
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        try:
            self.run_pb(["backup", "-b", "full"])
            self.assertEqual(1, 0, "Expecting Error because '-B' parameter is not specified.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(e.message, 'ERROR: required parameter not specified: BACKUP_PATH (-B, --backup-path)\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))


    # @unittest.skip("skip")
    def test_options_4(self):
        """check options test"""
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname))

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)

        # backup command failure without instance option
        try:
            self.run_pb(["backup", "-B", backup_dir, "-D", node.data_dir, "-b", "full"])
            self.assertEqual(1, 0, "Expecting Error because 'instance' parameter is not specified.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(e.message,
                'ERROR: required parameter not specified: --instance\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # backup command failure without backup mode option
        try:
            self.run_pb(["backup", "-B", backup_dir, "--instance=node", "-D", node.data_dir])
            self.assertEqual(1, 0, "Expecting Error because '-b' parameter is not specified.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn('ERROR: required parameter not specified: BACKUP_MODE (-b, --backup-mode)',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # backup command failure with invalid backup mode option
        try:
            self.run_pb(["backup", "-B", backup_dir, "--instance=node", "-b", "bad"])
            self.assertEqual(1, 0, "Expecting Error because backup-mode parameter is invalid.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(e.message,
                'ERROR: invalid backup-mode "bad"\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # delete failure without delete options
        try:
            self.run_pb(["delete", "-B", backup_dir, "--instance=node"])
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because delete options are omitted.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(e.message,
                'ERROR: You must specify at least one of the delete options: --expired |--wal |--backup_id\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))


        # delete failure without ID
        try:
            self.run_pb(["delete", "-B", backup_dir, "--instance=node", '-i'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because backup ID is omitted.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue("option requires an argument -- 'i'" in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_options_5(self):
        """check options test"""
        fname = self.id().split(".")[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            pg_options={
                'wal_level': 'logical',
                'max_wal_senders': '2'})

        self.assertEqual("INFO: Backup catalog '{0}' successfully inited\n".format(backup_dir),
            self.init_pb(backup_dir))
        self.add_instance(backup_dir, 'node', node)

        node.start()

        # syntax error in pg_probackup.conf
        with open(os.path.join(backup_dir, "backups", "node", "pg_probackup.conf"), "a") as conf:
            conf.write(" = INFINITE\n")
        try:
            self.backup_node(backup_dir, 'node', node)
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because of garbage in pg_probackup.conf.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(e.message,
                'ERROR: syntax error in " = INFINITE"\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        self.clean_pb(backup_dir)
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)

        # invalid value in pg_probackup.conf
        with open(os.path.join(backup_dir, "backups", "node", "pg_probackup.conf"), "a") as conf:
            conf.write("BACKUP_MODE=\n")

        try:
            self.backup_node(backup_dir, 'node', node, backup_type=None),
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because of invalid backup-mode in pg_probackup.conf.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(e.message,
                'ERROR: invalid backup-mode ""\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        self.clean_pb(backup_dir)
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)

        # Command line parameters should override file values
        with open(os.path.join(backup_dir, "backups", "node", "pg_probackup.conf"), "a") as conf:
            conf.write("retention-redundancy=1\n")

        self.assertEqual(self.show_config(backup_dir, 'node')['retention-redundancy'], '1')

        # User cannot send --system-identifier parameter via command line
        try:
            self.backup_node(backup_dir, 'node', node, options=["--system-identifier", "123"]),
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because option system-identifier cannot be specified in command line.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(e.message,
                'ERROR: option system-identifier cannot be specified in command line\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # invalid value in pg_probackup.conf
        with open(os.path.join(backup_dir, "backups", "node", "pg_probackup.conf"), "a") as conf:
            conf.write("SMOOTH_CHECKPOINT=FOO\n")

        try:
            self.backup_node(backup_dir, 'node', node)
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because option -C should be boolean.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(e.message,
                "ERROR: option -C, --smooth-checkpoint should be a boolean: 'FOO'\n",
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        self.clean_pb(backup_dir)
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)

        # invalid option in pg_probackup.conf
        pbconf_path = os.path.join(backup_dir, "backups", "node", "pg_probackup.conf")
        with open(pbconf_path, "a") as conf:
            conf.write("TIMELINEID=1\n")

        try:
            self.backup_node(backup_dir, 'node', node)
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, 'Expecting Error because of invalid option "TIMELINEID".\n Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(e.message,
                'ERROR: invalid option "TIMELINEID" in file "{0}"\n'.format(pbconf_path),
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)
