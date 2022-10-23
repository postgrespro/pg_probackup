"""
The Test suite check behavior of pg_probackup utility, if password is required for connection to PostgreSQL instance.
 - https://confluence.postgrespro.ru/pages/viewpage.action?pageId=16777522
"""

import os
import unittest
import signal
import time

from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from testgres import StartNodeException

module_name = 'auth_test'
skip_test = False


try:
    from pexpect import *
except ImportError:
    skip_test = True


class SimpleAuthTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    def test_backup_via_unprivileged_user(self):
        """
            Make node, create unprivileged user, try to
            run a backups without EXECUTE rights on
            certain functions
        """
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql("postgres", "CREATE ROLE backup with LOGIN")

        try:
            self.backup_node(
                backup_dir, 'node', node, options=['-U', 'backup'])
            self.assertEqual(
                1, 0,
                "Expecting Error due to missing grant on EXECUTE.")
        except ProbackupException as e:
            if self.get_version(node) < 150000:
                self.assertIn(
                    "ERROR: query failed: ERROR:  permission denied "
                    "for function pg_start_backup", e.message,
                    '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                        repr(e.message), self.cmd))
            else:
                self.assertIn(
                    "ERROR: query failed: ERROR:  permission denied "
                    "for function pg_backup_start", e.message,
                    '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                        repr(e.message), self.cmd))

        if self.get_version(node) < 150000:
            node.safe_psql(
                "postgres",
                "GRANT EXECUTE ON FUNCTION"
                " pg_start_backup(text, boolean, boolean) TO backup;")
        else:
            node.safe_psql(
                "postgres",
                "GRANT EXECUTE ON FUNCTION"
                " pg_backup_start(text, boolean) TO backup;")

        if self.get_version(node) < 100000:
            node.safe_psql(
                'postgres',
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_xlog() TO backup")
        else:
            node.safe_psql(
                'postgres',
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_wal() TO backup")

        try:
            self.backup_node(
                backup_dir, 'node', node, options=['-U', 'backup'])
            self.assertEqual(
                1, 0,
                "Expecting Error due to missing grant on EXECUTE.")
        except ProbackupException as e:
            self.assertIn(
                "ERROR: query failed: ERROR:  permission denied for function "
                "pg_create_restore_point\nquery was: "
                "SELECT pg_catalog.pg_create_restore_point($1)", e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        node.safe_psql(
            "postgres",
            "GRANT EXECUTE ON FUNCTION"
            " pg_create_restore_point(text) TO backup;")

        try:
            self.backup_node(
                backup_dir, 'node', node, options=['-U', 'backup'])
            self.assertEqual(
                1, 0,
                "Expecting Error due to missing grant on EXECUTE.")
        except ProbackupException as e:
            if self.get_version(node) < 150000:
                self.assertIn(
                    "ERROR: query failed: ERROR:  permission denied "
                    "for function pg_stop_backup", e.message,
                    '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                        repr(e.message), self.cmd))
            else:
                self.assertIn(
                    "ERROR: query failed: ERROR:  permission denied "
                    "for function pg_backup_stop", e.message,
                    '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                        repr(e.message), self.cmd))

        if self.get_version(node) < self.version_to_num('10.0'):
            node.safe_psql(
                "postgres",
                "GRANT EXECUTE ON FUNCTION pg_stop_backup(boolean) TO backup")
        elif self.get_vestion(node) < self.version_to_num('15.0'):
            node.safe_psql(
                "postgres",
                "GRANT EXECUTE ON FUNCTION "
                "pg_stop_backup(boolean, boolean) TO backup")
            # Do this for ptrack backups
            node.safe_psql(
                "postgres",
                "GRANT EXECUTE ON FUNCTION pg_stop_backup() TO backup")
        else:
            node.safe_psql(
                "postgres",
                "GRANT EXECUTE ON FUNCTION "
                "pg_backup_stop(boolean) TO backup")
            # Do this for ptrack backups
            node.safe_psql(
                "postgres",
                "GRANT EXECUTE ON FUNCTION pg_backup_stop() TO backup")

        self.backup_node(
                backup_dir, 'node', node, options=['-U', 'backup'])

        node.safe_psql("postgres", "CREATE DATABASE test1")

        self.backup_node(
                backup_dir, 'node', node, options=['-U', 'backup'])

        node.safe_psql(
            "test1", "create table t1 as select generate_series(0,100)")

        if self.ptrack:
            self.set_auto_conf(node, {'ptrack_enable': 'on'})
        node.stop()
        node.slow_start()

        node.safe_psql(
                "postgres",
                "ALTER ROLE backup REPLICATION")

        # FULL
        self.backup_node(
            backup_dir, 'node', node, options=['-U', 'backup'])

        # PTRACK
#        self.backup_node(
#            backup_dir, 'node', node,
#            backup_type='ptrack', options=['-U', 'backup'])


class AuthTest(unittest.TestCase):
    pb = None
    node = None

    # TODO move to object scope, replace module_name
    @classmethod
    def setUpClass(cls):

        super(AuthTest, cls).setUpClass()

        cls.pb = ProbackupTest()
        cls.backup_dir = os.path.join(cls.pb.tmp_path, module_name, 'backup')

        cls.node = cls.pb.make_simple_node(
            base_dir="{}/node".format(module_name),
            set_replication=True,
            initdb_params=['--data-checksums', '--auth-host=md5']
        )
        modify_pg_hba(cls.node)

        cls.pb.init_pb(cls.backup_dir)
        cls.pb.add_instance(cls.backup_dir, cls.node.name, cls.node)
        cls.pb.set_archiving(cls.backup_dir, cls.node.name, cls.node)
        try:
            cls.node.slow_start()
        except StartNodeException:
            raise unittest.skip("Node hasn't started")

        if cls.pb.get_version(cls.node) < 150000:
            cls.node.safe_psql(
                "postgres",
                "CREATE ROLE backup WITH LOGIN PASSWORD 'password'; "
                "GRANT USAGE ON SCHEMA pg_catalog TO backup; "
                "GRANT EXECUTE ON FUNCTION current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_is_in_recovery() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_start_backup(text, boolean, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_stop_backup() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_stop_backup(boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_create_restore_point(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_switch_xlog() TO backup; "
                "GRANT EXECUTE ON FUNCTION txid_current() TO backup; "
                "GRANT EXECUTE ON FUNCTION txid_current_snapshot() TO backup; "
                "GRANT EXECUTE ON FUNCTION txid_snapshot_xmax(txid_snapshot) TO backup;")
        else:
            cls.node.safe_psql(
                "postgres",
                "CREATE ROLE backup WITH LOGIN PASSWORD 'password'; "
                "GRANT USAGE ON SCHEMA pg_catalog TO backup; "
                "GRANT EXECUTE ON FUNCTION current_setting(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_is_in_recovery() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_backup_start(text, boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_backup_stop() TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_backup_stop(boolean) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_create_restore_point(text) TO backup; "
                "GRANT EXECUTE ON FUNCTION pg_switch_xlog() TO backup; "
                "GRANT EXECUTE ON FUNCTION txid_current() TO backup; "
                "GRANT EXECUTE ON FUNCTION txid_current_snapshot() TO backup; "
                "GRANT EXECUTE ON FUNCTION txid_snapshot_xmax(txid_snapshot) TO backup;")

        cls.pgpass_file = os.path.join(os.path.expanduser('~'), '.pgpass')

    # TODO move to object scope, replace module_name
    @classmethod
    def tearDownClass(cls):
        cls.node.cleanup()
        cls.pb.del_test_dir(module_name, '')

    @unittest.skipIf(skip_test, "Module pexpect isn't installed. You need to install it.")
    def setUp(self):
        self.cmd = ['backup',
                    '-B', self.backup_dir,
                    '--instance', self.node.name,
                    '-h', '127.0.0.1',
                    '-p', str(self.node.port),
                    '-U', 'backup',
                    '-b', 'FULL'
                    ]

    def tearDown(self):
        if "PGPASSWORD" in self.pb.test_env.keys():
            del self.pb.test_env["PGPASSWORD"]

        if "PGPASSWORD" in self.pb.test_env.keys():
            del self.pb.test_env["PGPASSFILE"]

        try:
            os.remove(self.pgpass_file)
        except OSError:
            pass

    def test_empty_password(self):
        """ Test case: PGPB_AUTH03 - zero password length """
        try:
            self.assertIn("ERROR: no password supplied",
                          str(run_pb_with_auth([self.pb.probackup_path] + self.cmd, '\0\r\n'))
                          )
        except (TIMEOUT, ExceptionPexpect) as e:
            self.fail(e.value)

    def test_wrong_password(self):
        """ Test case: PGPB_AUTH04 - incorrect password """
        try:
            self.assertIn("password authentication failed",
                          str(run_pb_with_auth([self.pb.probackup_path] + self.cmd, 'wrong_password\r\n'))
                          )
        except (TIMEOUT, ExceptionPexpect) as e:
            self.fail(e.value)

    def test_right_password(self):
        """ Test case: PGPB_AUTH01 - correct password """
        try:
            self.assertIn("completed",
                          str(run_pb_with_auth([self.pb.probackup_path] + self.cmd, 'password\r\n'))
                          )
        except (TIMEOUT, ExceptionPexpect) as e:
            self.fail(e.value)

    def test_right_password_and_wrong_pgpass(self):
        """ Test case: PGPB_AUTH05 - correct password and incorrect .pgpass (-W)"""
        line = ":".join(['127.0.0.1', str(self.node.port), 'postgres', 'backup', 'wrong_password'])
        create_pgpass(self.pgpass_file, line)
        try:
            self.assertIn("completed",
                          str(run_pb_with_auth([self.pb.probackup_path] + self.cmd + ['-W'], 'password\r\n'))
                          )
        except (TIMEOUT, ExceptionPexpect) as e:
            self.fail(e.value)

    def test_ctrl_c_event(self):
        """ Test case: PGPB_AUTH02 - send interrupt signal """
        try:
            run_pb_with_auth([self.pb.probackup_path] + self.cmd, kill=True)
        except TIMEOUT:
            self.fail("Error: CTRL+C event ignored")

    def test_pgpassfile_env(self):
        """ Test case: PGPB_AUTH06 - set environment var PGPASSFILE """
        path = os.path.join(self.pb.tmp_path, module_name, 'pgpass.conf')
        line = ":".join(['127.0.0.1', str(self.node.port), 'postgres', 'backup', 'password'])
        create_pgpass(path, line)
        self.pb.test_env["PGPASSFILE"] = path
        try:
            self.assertEqual(
                "OK",
                self.pb.show_pb(self.backup_dir, self.node.name, self.pb.run_pb(self.cmd + ['-w']))["status"],
                "ERROR: Full backup status is not valid."
            )
        except ProbackupException as e:
            self.fail(e)

    def test_pgpass(self):
        """ Test case: PGPB_AUTH07 - Create file .pgpass in home dir. """
        line = ":".join(['127.0.0.1', str(self.node.port), 'postgres', 'backup', 'password'])
        create_pgpass(self.pgpass_file, line)
        try:
            self.assertEqual(
                "OK",
                self.pb.show_pb(self.backup_dir, self.node.name, self.pb.run_pb(self.cmd + ['-w']))["status"],
                "ERROR: Full backup status is not valid."
            )
        except ProbackupException as e:
            self.fail(e)

    def test_pgpassword(self):
        """ Test case: PGPB_AUTH08 - set environment var PGPASSWORD """
        self.pb.test_env["PGPASSWORD"] = "password"
        try:
            self.assertEqual(
                "OK",
                self.pb.show_pb(self.backup_dir, self.node.name, self.pb.run_pb(self.cmd + ['-w']))["status"],
                "ERROR: Full backup status is not valid."
            )
        except ProbackupException as e:
            self.fail(e)

    def test_pgpassword_and_wrong_pgpass(self):
        """ Test case: PGPB_AUTH09 - Check priority between PGPASSWORD and .pgpass file"""
        line = ":".join(['127.0.0.1', str(self.node.port), 'postgres', 'backup', 'wrong_password'])
        create_pgpass(self.pgpass_file, line)
        self.pb.test_env["PGPASSWORD"] = "password"
        try:
            self.assertEqual(
                "OK",
                self.pb.show_pb(self.backup_dir, self.node.name, self.pb.run_pb(self.cmd + ['-w']))["status"],
                "ERROR: Full backup status is not valid."
            )
        except ProbackupException as e:
            self.fail(e)


def run_pb_with_auth(cmd, password=None, kill=False):
    try:
        with spawn(" ".join(cmd), encoding='utf-8', timeout=10) as probackup:
            result = probackup.expect(u"Password for user .*:", 5)
            if kill:
                probackup.kill(signal.SIGINT)
            elif result == 0:
                probackup.sendline(password)
                probackup.expect(EOF)
                return probackup.before
            else:
                raise ExceptionPexpect("Other pexpect errors.")
    except TIMEOUT:
        raise TIMEOUT("Timeout error.")
    except ExceptionPexpect:
        raise ExceptionPexpect("Pexpect error.")


def modify_pg_hba(node):
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
        fio.write('host\tall\tpostgres\t127.0.0.1/0\ttrust\n' + data)


def create_pgpass(path, line):
    with open(path, 'w') as passfile:
        # host:port:db:username:password
        passfile.write(line)
    os.chmod(path, 0o600)
