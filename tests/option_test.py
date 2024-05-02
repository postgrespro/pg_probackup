import os
from .helpers.ptrack_helpers import ProbackupTest, fs_backup_class
import locale


class OptionTest(ProbackupTest):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_help_1(self):
        """help options"""
        with open(os.path.join(self.tests_source_path, "expected/option_help.out"), "rb") as help_out:
            self.assertEqual(
                self.pb.run(["--help"], use_backup_dir=None),
                help_out.read().decode("utf-8")
            )

    # @unittest.skip("skip")
    def test_without_backup_path_3(self):
        """backup command failure without backup mode option"""
        self.pb.run(["backup", "-b", "full"],
                    expect_error="because '-B' parameter is not specified", use_backup_dir=None)
        self.assertMessage(contains="No backup catalog path specified.\n"
			   "Please specify it either using environment variable BACKUP_DIR or\n"
			   "command line option --backup-path (-B)")

    def test_options_4(self):
        """check options test"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)

        # backup command failure without instance option
        self.pb.run(["backup", "-D", node.data_dir, "-b", "full"],
                    expect_error="because 'instance' parameter is not specified")
        self.assertMessage(contains='ERROR: Required parameter not specified: --instance')

        # backup command failure without backup mode option
        self.pb.run(["backup", "--instance=node", "-D", node.data_dir],
                    expect_error="Expecting Error because '-b' parameter is not specified")
        self.assertMessage(contains="Please specify it either using environment variable BACKUP_MODE or\n"
			   "command line option --backup-mode (-b)")

        # backup command failure with invalid backup mode option
        self.pb.run(["backup", "--instance=node", "-b", "bad"],
                    expect_error="because backup-mode parameter is invalid")
        self.assertMessage(contains='ERROR: Invalid backup-mode "bad"')

        # delete failure without delete options
        self.pb.run(["delete", "--instance=node"],
                    expect_error="because delete options are omitted")
        self.assertMessage(contains='ERROR: You must specify at least one of the delete options: '
                                    '--delete-expired |--delete-wal |--merge-expired |--status |(-i, --backup-id)')

        # delete failure without ID
        self.pb.run(["delete", "--instance=node", '-i'],
                    expect_error="because backup ID is omitted")
        self.assertMessage(contains="Option '-i' requires an argument")

        #init command with bad option
        self.pb.run(["init","--bad"],
              		expect_error="because unknown option")
        self.assertMessage(contains="Unknown option '--bad'")
        
        # run with bad short option
        self.pb.run(["init","-aB"],
                    expect_error="because unknown option")
        self.assertMessage(contains="Unknown option '-aB'")

    # @unittest.skip("skip")
    def test_options_5(self):
        """check options test"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node')

        output = self.pb.init()
        self.assertIn(
            f"INFO: Backup catalog '{backup_dir}' successfully initialized",
            output)

        self.pb.add_instance('node', node)

        node.slow_start()

        # syntax error in pg_probackup.conf
        with self.modify_backup_config(backup_dir, 'node') as cf:
            cf.data += " = INFINITE\n"

        self.pb.backup_node('node', node,
                         expect_error="because of garbage in pg_probackup.conf")
        self.assertMessage(regex=r'ERROR: Syntax error .* INFINITE')

        backup_dir.cleanup()
        self.pb.init()
        self.pb.add_instance('node', node)

        # invalid value in pg_probackup.conf
        with self.modify_backup_config(backup_dir, 'node') as cf:
            cf.data += "BACKUP_MODE=\n"

        self.pb.backup_node('node', node, backup_type=None,
                         expect_error="because of invalid backup-mode in pg_probackup.conf")
        self.assertMessage(contains='ERROR: Invalid option "BACKUP_MODE" in file')

        backup_dir.cleanup()
        self.pb.init()
        self.pb.add_instance('node', node)

        # Command line parameters should override file values
        with self.modify_backup_config(backup_dir, 'node') as cf:
            cf.data += "retention-redundancy=1\n"

        self.assertEqual(self.pb.show_config('node')['retention-redundancy'], '1')

        # User cannot send --system-identifier parameter via command line
        self.pb.backup_node('node', node, options=["--system-identifier", "123"],
                         expect_error="because option system-identifier cannot be specified in command line")
        self.assertMessage(contains='ERROR: Option system-identifier cannot be specified in command line')

        # invalid value in pg_probackup.conf
        with self.modify_backup_config(backup_dir, 'node') as cf:
            cf.data += "SMOOTH_CHECKPOINT=FOO\n"

        self.pb.backup_node('node', node,
                         expect_error="because option smooth-checkpoint could be specified in command-line only")
        self.assertMessage(contains='ERROR: Invalid option "SMOOTH_CHECKPOINT" in file')

        backup_dir.cleanup()
        self.pb.init()
        self.pb.add_instance('node', node)

        # invalid option in pg_probackup.conf
        with self.modify_backup_config(backup_dir, 'node') as cf:
            cf.data += "TIMELINEID=1\n"

        self.pb.backup_node('node', node,
                         expect_error='because of invalid option "TIMELINEID"')
        self.assertMessage(contains='ERROR: Invalid option "TIMELINEID" in file')

    # @unittest.skip("skip")
    def test_help_6(self):
        """help options"""
        if ProbackupTest.enable_nls:
            if check_locale('ru_RU.UTF-8'):
                env = self.test_env.copy()
                env['LC_CTYPE'] = 'ru_RU.UTF-8'
                env['LC_MESSAGES'] = 'ru_RU.UTF-8'
                env['LANGUAGE'] = 'ru_RU'
                with open(os.path.join(self.tests_source_path, "expected/option_help_ru.out"), "rb") as help_out:
                    self.assertEqual(
                        self.pb.run(["--help"], env=env, use_backup_dir=None),
                        help_out.read().decode("utf-8")
                    )
            else:
                self.skipTest(
                    "The ru_RU.UTF-8 locale is not available. You may need to install it to run this test.")
        else:
            self.skipTest(
                'You need configure PostgreSQL with --enabled-nls option for this test')

    def test_skip_if_exists(self):
        """check options --skip-if-exists"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.assertMessage(contains=f"INFO: Backup catalog '{backup_dir}' successfully initialized")
        if fs_backup_class.is_file_based:
            self.pb.init(expect_error=True)
            self.assertMessage(contains=f"ERROR: Backup catalog '{backup_dir}' already exists and is not empty")

            self.pb.init(options=['--skip-if-exists'])
            self.assertMessage(contains=f"WARNING: Backup catalog '{backup_dir}' already exists and is not empty, skipping")
            self.assertMessage(has_no="successfully initialized")

        self.pb.add_instance('node', node)
        self.assertMessage(contains="INFO: Instance 'node' successfully initialized")
        self.pb.add_instance('node', node, expect_error=True)
        self.assertMessage(contains="ERROR: Instance 'node' backup directory already exists")

        self.pb.add_instance('node', node, options=['--skip-if-exists'])
        self.assertMessage(contains=f"WARNING: Instance 'node' backup directory already exists: '{backup_dir}/backups/node'. Skipping")
        self.assertMessage(has_no="successfully initialized")

    # @unittest.skip("skip")
    def test_options_no_scale_units(self):
        """check --no-scale-units option"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node')
        self.pb.init()
        self.pb.add_instance('node', node)

        # check that --no-scale-units option works correctly
        self.pb.run(["show-config", "-D", node.data_dir, "--instance", "node"])
        self.assertMessage(contains="archive-timeout = 5min")
        self.pb.run(["show-config", "-D", node.data_dir, "--instance", "node", "--no-scale-units"])
        self.assertMessage(has_no="archive-timeout = 300s")
        self.assertMessage(contains="archive-timeout = 300")
        # check that we have now quotes ("") in json output
        self.pb.run(["show-config", "--instance", "node", "--no-scale-units", "--format=json"])
        self.assertMessage(contains='"archive-timeout": 300,')
        self.assertMessage(contains='"retention-redundancy": 0,')
        self.assertMessage(has_no='"archive-timeout": "300",')




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
