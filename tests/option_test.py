import unittest
from os import path
import six
from .ptrack_helpers import ProbackupTest, ProbackupException
from testgres import stop_all


class OptionTest(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(OptionTest, self).__init__(*args, **kwargs)

    @classmethod
    def tearDownClass(cls):
        stop_all()

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_help_1(self):
        """help options"""
        fname = self.id().split(".")[3]
        with open(path.join(self.dir_path, "expected/option_help.out"), "rb") as help_out:
            self.assertEqual(
                self.run_pb(["--help"]),
                help_out.read()
            )

    def test_version_2(self):
        """help options"""
        fname = self.id().split(".")[3]
        with open(path.join(self.dir_path, "expected/option_version.out"), "rb") as version_out:
            self.assertEqual(
                self.run_pb(["--version"]),
                version_out.read()
            )

    def test_without_backup_path_3(self):
        """backup command failure without backup mode option"""
        fname = self.id().split(".")[3]
        try:
            self.run_pb(["backup", "-b", "full"])
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                e.message,
                'ERROR: required parameter not specified: BACKUP_PATH (-B, --backup-path)\n'
                )

    def test_options_4(self):
        """check options test"""
        fname = self.id().split(".")[3]
        node = self.make_simple_node(base_dir="tmp_dirs/option/{0}".format(fname))
        try:
            node.stop()
        except:
            pass
        self.assertEqual(self.init_pb(node), six.b(""))

        # backup command failure without backup mode option
        try:
            self.run_pb(["backup", "-B", self.backup_dir(node), "-D", node.data_dir])
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
#            print e.message
            self.assertEqual(
                e.message,
                'ERROR: required parameter not specified: BACKUP_MODE (-b, --backup-mode)\n'
                )

        # backup command failure with invalid backup mode option
        try:
            self.run_pb(["backup", "-b", "bad", "-B", self.backup_dir(node)])
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                e.message,
                'ERROR: invalid backup-mode "bad"\n'
                )

        # delete failure without ID
        try:
            self.run_pb(["delete", "-B", self.backup_dir(node)])
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                e.message,
                'ERROR: required backup ID not specified\n'
                )

        node.start()

        # syntax error in pg_probackup.conf
        with open(path.join(self.backup_dir(node), "pg_probackup.conf"), "a") as conf:
            conf.write(" = INFINITE\n")

        try:
            self.backup_pb(node)
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                e.message,
                'ERROR: syntax error in " = INFINITE"\n'
                )

        self.clean_pb(node)
        self.assertEqual(self.init_pb(node), six.b(""))

        # invalid value in pg_probackup.conf
        with open(path.join(self.backup_dir(node), "pg_probackup.conf"), "a") as conf:
            conf.write("BACKUP_MODE=\n")

        try:
            self.backup_pb(node, backup_type=None),
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                e.message,
                'ERROR: invalid backup-mode ""\n'
                )

        self.clean_pb(node)

        # Command line parameters should override file values
        self.assertEqual(self.init_pb(node), six.b(""))
        with open(path.join(self.backup_dir(node), "pg_probackup.conf"), "a") as conf:
            conf.write("retention-redundancy=1\n")

        self.assertEqual(
            self.show_config(node)['retention-redundancy'],
            six.b('1')
        )

        # User cannot send --system-identifier parameter via command line
        try:
            self.backup_pb(node, options=["--system-identifier", "123"]),
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                e.message,
                'ERROR: option system-identifier cannot be specified in command line\n'
                )

        # invalid value in pg_probackup.conf
        with open(path.join(self.backup_dir(node), "pg_probackup.conf"), "a") as conf:
            conf.write("SMOOTH_CHECKPOINT=FOO\n")

        try:
            self.backup_pb(node),
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                e.message,
                "ERROR: option -C, --smooth-checkpoint should be a boolean: 'FOO'\n"
                )

        self.clean_pb(node)
        self.assertEqual(self.init_pb(node), six.b(""))

        # invalid option in pg_probackup.conf
        with open(path.join(self.backup_dir(node), "pg_probackup.conf"), "a") as conf:
            conf.write("TIMELINEID=1\n")

        try:
            self.backup_pb(node),
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                e.message,
                'ERROR: invalid option "TIMELINEID"\n'
                )

        self.clean_pb(node)
        self.assertEqual(self.init_pb(node), six.b(""))

        node.stop()
