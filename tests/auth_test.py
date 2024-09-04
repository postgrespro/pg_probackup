"""
The Test suite check behavior of pg_probackup utility, if password is required for connection to PostgreSQL instance.
 - https://confluence.postgrespro.ru/pages/viewpage.action?pageId=16777522
"""

import os
import unittest
import signal
import time

from .helpers.ptrack_helpers import ProbackupTest
from testgres import StartNodeException

skip_test = False

try:
    from pexpect import *
except ImportError:
    skip_test = True


class SimpleAuthTest(ProbackupTest):

    # @unittest.skip("skip")
    def test_backup_via_unprivileged_user(self):
        """
            Make node, create unprivileged user, try to
            run a backups without EXECUTE rights on
            certain functions
        """
        node = self.pg_node.make_simple('node',
                                        set_replication=True,
                                        ptrack_enable=self.ptrack)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        if self.ptrack:
            node.safe_psql(
                "postgres",
                "CREATE EXTENSION ptrack")

        node.safe_psql("postgres", "CREATE ROLE backup with LOGIN")

        self.pb.backup_node('node', node, options=['-U', 'backup'],
                            expect_error='due to missing grant on EXECUTE')
        if self.pg_config_version < 150000:
            self.assertMessage(contains=
                               "ERROR: Query failed: ERROR:  permission denied "
                               "for function pg_start_backup")
        else:
            self.assertMessage(contains=
                               "ERROR: Query failed: ERROR:  permission denied "
                               "for function pg_backup_start")

        if self.pg_config_version < 150000:
            node.safe_psql(
                "postgres",
                "GRANT EXECUTE ON FUNCTION"
                " pg_start_backup(text, boolean, boolean) TO backup;")
        else:
            node.safe_psql(
                "postgres",
                "GRANT EXECUTE ON FUNCTION"
                " pg_backup_start(text, boolean) TO backup;")

        node.safe_psql(
            'postgres',
            "GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_wal() TO backup")

        self.pb.backup_node('node', node,
                            options=['-U', 'backup'],
                            expect_error='due to missing grant on EXECUTE')
        self.assertMessage(contains=
                           "ERROR: Query failed: ERROR:  permission denied for function "
                           "pg_create_restore_point\nquery was: "
                           "SELECT pg_catalog.pg_create_restore_point($1)")

        node.safe_psql(
            "postgres",
            "GRANT EXECUTE ON FUNCTION"
            " pg_create_restore_point(text) TO backup;")

        self.pb.backup_node('node', node,
                            options=['-U', 'backup'],
                            expect_error='due to missing grant on EXECUTE')
        if self.pg_config_version < 150000:
            self.assertMessage(contains=
                               "ERROR: Query failed: ERROR:  permission denied "
                               "for function pg_stop_backup")
        else:
            self.assertMessage(contains=
                               "ERROR: Query failed: ERROR:  permission denied "
                               "for function pg_backup_stop")

        if self.pg_config_version < self.version_to_num('15.0'):
            node.safe_psql(
                "postgres",
                "GRANT EXECUTE ON FUNCTION pg_stop_backup() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_stop_backup(boolean, boolean) TO backup;")
        else:
            node.safe_psql(
                "postgres",
                "GRANT EXECUTE ON FUNCTION pg_backup_stop(boolean) TO backup;")

        self.pb.backup_node('node', node, options=['-U', 'backup'])

        node.safe_psql("postgres", "CREATE DATABASE test1")

        self.pb.backup_node('node', node, options=['-U', 'backup'])

        node.safe_psql(
            "test1", "create table t1 as select generate_series(0,100)")

        node.stop()
        node.slow_start()

        node.safe_psql(
            "postgres",
            "ALTER ROLE backup REPLICATION")

        # FULL
        self.pb.backup_node('node', node, options=['-U', 'backup'])

        # PTRACK
        if self.ptrack:
            self.pb.backup_node('node', node,
                                backup_type='ptrack', options=['-U', 'backup'])


class AuthTest(ProbackupTest):
    pb = None
    node = None

    # TODO move to object scope, replace module_name
    @unittest.skipIf(skip_test, "Module pexpect isn't installed. You need to install it.")
    def setUp(self):

        super().setUp()

        self.node = self.pg_node.make_simple("node",
                                             set_replication=True,
                                             initdb_params=['--auth-host=md5'],
                                             pg_options={'archive_timeout': '5s'},
                                             )

        self.modify_pg_hba(self.node)

        self.pb.init()
        self.pb.add_instance(self.node.name, self.node)
        self.pb.set_archiving(self.node.name, self.node)
        try:
            self.node.slow_start()
        except StartNodeException:
            raise unittest.skip("Node hasn't started")


        version = self.pg_config_version
        if version < 150000:
            self.node.safe_psql(
                "postgres",
                "CREATE ROLE backup WITH LOGIN PASSWORD 'password'; "
                "GRANT USAGE ON SCHEMA pg_catalog TO backup; "
                "GRANT EXECUTE ON FUNCTION current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_is_in_recovery() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_start_backup(text, boolean, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_stop_backup() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_stop_backup(boolean, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_create_restore_point(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_switch_wal() TO backup; "
                "GRANT EXECUTE ON FUNCTION txid_current() TO backup; "
                "GRANT EXECUTE ON FUNCTION txid_current_snapshot() TO backup; "
                "GRANT EXECUTE ON FUNCTION txid_snapshot_xmax(txid_snapshot) TO backup;")
        else:
            self.node.safe_psql(
                "postgres",
                "CREATE ROLE backup WITH LOGIN PASSWORD 'password'; "
                "GRANT USAGE ON SCHEMA pg_catalog TO backup; "
                "GRANT EXECUTE ON FUNCTION current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_is_in_recovery() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_backup_start(text, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_backup_stop(boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_create_restore_point(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_switch_wal() TO backup; "
                "GRANT EXECUTE ON FUNCTION txid_current() TO backup; "
                "GRANT EXECUTE ON FUNCTION txid_current_snapshot() TO backup; "
                "GRANT EXECUTE ON FUNCTION txid_snapshot_xmax(txid_snapshot) TO backup;")

        if version >= 150000:
            home_dir = os.path.join(self.test_path, "home")
            os.makedirs(home_dir, exist_ok=True)
            self.test_env['HOME'] = home_dir
            self.pgpass_file = os.path.join(home_dir, '.pgpass')
            self.pgpass_file_lock = None
        else:
            # before PGv15 only true home dir were inspected.
            # Since we can't have separate file per test, we have to serialize
            # tests.
            self.pgpass_file = os.path.join(os.path.expanduser('~'), '.pgpass')
            self.pgpass_file_lock = self.pgpass_file + '~probackup_test_lock'
            # have to lock pgpass by creating file in exclusive mode
            for i in range(120):
                try:
                    open(self.pgpass_file_lock, "x").close()
                except FileExistsError:
                    time.sleep(1)
                else:
                    break
            else:
                raise TimeoutError("can't create ~/.pgpass~probackup_test_lock for 120 seconds")

        self.pb_cmd = ['backup',
                       '--instance', self.node.name,
                       '-h', '127.0.0.1',
                       '-p', str(self.node.port),
                       '-U', 'backup',
                       '-d', 'postgres',
                       '-b', 'FULL',
                       '--no-sync'
                       ]

    def tearDown(self):
        super().tearDown()
        if not self.pgpass_file_lock:
            return
        if hasattr(self, "pgpass_line") and os.path.exists(self.pgpass_file):
            with open(self.pgpass_file, 'r') as fl:
                lines = fl.readlines()
            if self.pgpass_line in lines:
                lines.remove(self.pgpass_line)
            if len(lines) == 0:
                os.remove(self.pgpass_file)
            else:
                with open(self.pgpass_file, 'w') as fl:
                    fl.writelines(lines)
        os.remove(self.pgpass_file_lock)

    def test_empty_password(self):
        """ Test case: PGPB_AUTH03 - zero password length """
        try:
            self.assertIn("ERROR: no password supplied",
                          self.run_pb_with_auth('\0\r\n'))
        except (TIMEOUT, ExceptionPexpect) as e:
            self.fail(e.value)

    def test_wrong_password(self):
        """ Test case: PGPB_AUTH04 - incorrect password """
        self.assertIn("password authentication failed",
                      self.run_pb_with_auth('wrong_password\r\n'))

    def test_right_password(self):
        """ Test case: PGPB_AUTH01 - correct password """
        self.assertIn("completed",
                      self.run_pb_with_auth('password\r\n'))

    def test_right_password_and_wrong_pgpass(self):
        """ Test case: PGPB_AUTH05 - correct password and incorrect .pgpass (-W)"""
        line = ":".join(['127.0.0.1', str(self.node.port), 'postgres', 'backup', 'wrong_password'])
        self.create_pgpass(self.pgpass_file, line)
        self.assertIn("completed",
                      self.run_pb_with_auth('password\r\n', add_args=["-W"]))

    def test_ctrl_c_event(self):
        """ Test case: PGPB_AUTH02 - send interrupt signal """
        try:
            self.run_pb_with_auth(kill=True)
        except TIMEOUT:
            self.fail("Error: CTRL+C event ignored")

    def test_pgpassfile_env(self):
        """ Test case: PGPB_AUTH06 - set environment var PGPASSFILE """
        path = os.path.join(self.test_path, 'pgpass.conf')
        line = ":".join(['127.0.0.1', str(self.node.port), 'postgres', 'backup', 'password'])
        self.create_pgpass(path, line)
        self.test_env["PGPASSFILE"] = path
        self.assertEqual(
            "OK",
            self.pb.show(self.node.name, self.pb.run(self.pb_cmd + ['-w']))["status"],
            "ERROR: Full backup status is not valid."
        )

    def test_pgpass(self):
        """ Test case: PGPB_AUTH07 - Create file .pgpass in home dir. """
        line = ":".join(['127.0.0.1', str(self.node.port), 'postgres', 'backup', 'password'])
        self.create_pgpass(self.pgpass_file, line)
        self.assertEqual(
            "OK",
            self.pb.show(self.node.name, self.pb.run(self.pb_cmd + ['-w']))["status"],
            "ERROR: Full backup status is not valid."
        )

    def test_pgpassword(self):
        """ Test case: PGPB_AUTH08 - set environment var PGPASSWORD """
        self.test_env["PGPASSWORD"] = "password"
        self.assertEqual(
            "OK",
            self.pb.show(self.node.name, self.pb.run(self.pb_cmd + ['-w']))["status"],
            "ERROR: Full backup status is not valid."
        )

    def test_pgpassword_and_wrong_pgpass(self):
        """ Test case: PGPB_AUTH09 - Check priority between PGPASSWORD and .pgpass file"""
        line = ":".join(['127.0.0.1', str(self.node.port), 'postgres', 'backup', 'wrong_password'])
        self.create_pgpass(self.pgpass_file, line)
        self.test_env["PGPASSWORD"] = "password"
        self.assertEqual(
            "OK",
            self.pb.show(self.node.name, self.pb.run(self.pb_cmd + ['-w']))["status"],
            "ERROR: Full backup status is not valid."
        )

    def run_pb_with_auth(self, password=None, add_args = [], kill=False):
        cmd = [*self.pb_cmd, *add_args, *self.backup_dir.pb_args]
        with spawn(self.probackup_path, cmd,
                   encoding='utf-8', timeout=60, env=self.test_env) as probackup:
            result = probackup.expect(u"Password for user .*:", 10)
            if kill:
                probackup.kill(signal.SIGINT)
            elif result == 0:
                probackup.sendline(password)
                probackup.expect(EOF)
                return str(probackup.before)
            else:
                raise ExceptionPexpect("Other pexpect errors.")


    def modify_pg_hba(self, node):
        """
        Description:
            Add trust authentication for user postgres. Need for add new role and set grant.
        :param node:
        :return None:
        """
        hba_conf = os.path.join(node.data_dir, "pg_hba.conf")
        with open(hba_conf, 'r+') as fio:
            data = fio.read()
            fio.seek(0)
            fio.write('host\tall\t%s\t127.0.0.1/0\ttrust\n%s' % (self.username, data))


    def create_pgpass(self, path, line):
        self.pgpass_line = line+"\n"
        with open(path, 'a') as passfile:
            # host:port:db:username:password
            passfile.write(self.pgpass_line)
        os.chmod(path, 0o600)
