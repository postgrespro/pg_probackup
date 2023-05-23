import unittest
import os
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
import locale

class OptionTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_help_1(self):
        """help options"""
        with open(os.path.join(self.dir_path, "expected/option_help.out"), "rb") as help_out:
            self.assertEqual(
                self.run_pb(["--help"]),
                help_out.read().decode("utf-8")
            )

    # @unittest.skip("skip")
    def test_without_backup_path_3(self):
        """backup command failure without backup mode option"""
        try:
            self.run_pb(["backup", "-b", "full"])
            self.assertEqual(1, 0, "Expecting Error because '-B' parameter is not specified.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: No backup catalog path specified.\n' + \
                'Please specify it either using environment variable BACKUP_DIR or\n' + \
                'command line option --backup-path (-B)',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

    # @unittest.skip("skip")
    def test_options_4(self):
        """check options test"""
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'))

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)

        # backup command failure without instance option
        try:
            self.run_pb(["backup", "-B", backup_dir, "-D", node.data_dir, "-b", "full"])
            self.assertEqual(1, 0, "Expecting Error because 'instance' parameter is not specified.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Required parameter not specified: --instance',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # backup command failure without backup mode option
        try:
            self.run_pb(["backup", "-B", backup_dir, "--instance=node", "-D", node.data_dir])
            self.assertEqual(1, 0, "Expecting Error because '-b' parameter is not specified.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: No backup mode specified.\nPlease specify it either using environment variable BACKUP_MODE or\ncommand line option --backup-mode (-b)',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # backup command failure with invalid backup mode option
        try:
            self.run_pb(["backup", "-B", backup_dir, "--instance=node", "-b", "bad"])
            self.assertEqual(1, 0, "Expecting Error because backup-mode parameter is invalid.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Invalid backup-mode "bad"',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # delete failure without delete options
        try:
            self.run_pb(["delete", "-B", backup_dir, "--instance=node"])
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because delete options are omitted.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: You must specify at least one of the delete options: '
                '--delete-expired |--delete-wal |--merge-expired |--status |(-i, --backup-id)',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))


        # delete failure without ID
        try:
            self.run_pb(["delete", "-B", backup_dir, "--instance=node", '-i'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because backup ID is omitted.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "Option '-i' requires an argument",
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

    # @unittest.skip("skip")
    def test_options_5(self):
        """check options test"""
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'))

        output = self.init_pb(backup_dir)
        self.assertIn(f"INFO: Backup catalog '{backup_dir}' successfully initialized", output)

        self.add_instance(backup_dir, 'node', node)

        node.slow_start()

        # syntax error in pg_probackup.conf
        conf_file = os.path.join(backup_dir, "backups", "node", "pg_probackup.conf")
        with open(conf_file, "a") as conf:
            conf.write(" = INFINITE\n")
        try:
            self.backup_node(backup_dir, 'node', node)
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because of garbage in pg_probackup.conf.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Syntax error in " = INFINITE',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        self.clean_pb(backup_dir)
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)

        # invalid value in pg_probackup.conf
        with open(conf_file, "a") as conf:
            conf.write("BACKUP_MODE=\n")

        try:
            self.backup_node(backup_dir, 'node', node, backup_type=None),
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because of invalid backup-mode in pg_probackup.conf.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Invalid option "BACKUP_MODE" in file',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        self.clean_pb(backup_dir)
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)

        # Command line parameters should override file values
        with open(conf_file, "a") as conf:
            conf.write("retention-redundancy=1\n")

        self.assertEqual(self.show_config(backup_dir, 'node')['retention-redundancy'], '1')

        # User cannot send --system-identifier parameter via command line
        try:
            self.backup_node(backup_dir, 'node', node, options=["--system-identifier", "123"]),
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because option system-identifier cannot be specified in command line.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Option system-identifier cannot be specified in command line',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # invalid value in pg_probackup.conf
        with open(conf_file, "a") as conf:
            conf.write("SMOOTH_CHECKPOINT=FOO\n")

        try:
            self.backup_node(backup_dir, 'node', node)
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because option -C should be boolean.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Invalid option "SMOOTH_CHECKPOINT" in file',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        self.clean_pb(backup_dir)
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)

        # invalid option in pg_probackup.conf
        with open(conf_file, "a") as conf:
            conf.write("TIMELINEID=1\n")

        try:
            self.backup_node(backup_dir, 'node', node)
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, 'Expecting Error because of invalid option "TIMELINEID".\n Output: {0} \n CMD: {1}'.format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Invalid option "TIMELINEID" in file',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

    # @unittest.skip("skip")
    def test_help_6(self):
        """help options"""
        if ProbackupTest.enable_nls:
            if check_locale('ru_RU.utf-8'):
                self.test_env['LC_ALL'] = 'ru_RU.utf-8'
                with open(os.path.join(self.dir_path, "expected/option_help_ru.out"), "rb") as help_out:
                    self.assertEqual(
                        self.run_pb(["--help"]),
                        help_out.read().decode("utf-8")
                    )
            else:
                self.skipTest(
                    "Locale ru_RU.utf-8 doesn't work. You need install ru_RU.utf-8 locale for this test")
        else:
            self.skipTest(
                'You need configure PostgreSQL with --enabled-nls option for this test')

    # @unittest.skip("skip")
    def test_options_default_units(self):
        """check --default-units option"""
        backup_dir =  os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'))
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        # check that --default-units option works correctly
        output = self.run_pb(["show-config", "--backup-path", backup_dir, "--instance=node"])
        self.assertIn(container=output, member="archive-timeout = 5min")
        output = self.run_pb(["show-config", "--backup-path", backup_dir, "--instance=node", "--default-units"])
        self.assertIn(container=output, member="archive-timeout = 300")
        self.assertNotIn(container=output, member="archive-timeout = 300s")
        # check that we have now quotes ("") in json output
        output = self.run_pb(["show-config", "--backup-path", backup_dir, "--instance=node", "--default-units", "--format=json"])
        self.assertIn(container=output, member='"archive-timeout": 300,')
        self.assertIn(container=output, member='"retention-redundancy": 0,')
        self.assertNotIn(container=output, member='"archive-timeout": "300",')

def check_locale(locale_name):
   ret=True
   old_locale = locale.setlocale(locale.LC_CTYPE,"")
   try:
      locale.setlocale(locale.LC_CTYPE, locale_name)
   except locale.Error:
      ret=False
   finally:
      locale.setlocale(locale.LC_CTYPE, old_locale)
   return ret
