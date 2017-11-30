import os
import unittest
import tempfile
import signal

from .helpers.ptrack_helpers import ProbackupTest
from testgres import StartNodeException, configure_testgres

module_name = 'auth_test'
skip_test = False


try:
    import pexpect
except:
    skip_test = True


class AuthTest(unittest.TestCase):
    pb = None
    node = None

    @classmethod
    def setUpClass(cls):

        super(AuthTest, cls).setUpClass()

        cls.pb = ProbackupTest()
        cls.backup_dir = os.path.join(cls.pb.tmp_path, module_name, 'backup')

        configure_testgres(cache_pg_config=False, cache_initdb=False)
        cls.node = cls.pb.make_simple_node(
            base_dir="{}/node".format(module_name),
            set_replication=True,
            initdb_params=['--data-checksums', '--auth-host=md5'],
            pg_options={
                'wal_level': 'replica'
            }
        )
        modify_pg_hba(cls.node)

        cls.backup_dir = os.path.join(tempfile.tempdir, "backups")
        cls.pb.init_pb(cls.backup_dir)
        cls.pb.add_instance(cls.backup_dir, cls.node.name, cls.node)
        cls.pb.set_archiving(cls.backup_dir, cls.node.name, cls.node)
        try:
            cls.node.start()
        except StartNodeException:
            raise unittest.skip("Node hasn't started")

        cls.node.safe_psql("postgres",
                           "CREATE ROLE backup WITH LOGIN PASSWORD 'password'; \
                       GRANT USAGE ON SCHEMA pg_catalog TO backup; \
                       GRANT EXECUTE ON FUNCTION current_setting(text) TO backup; \
                       GRANT EXECUTE ON FUNCTION pg_is_in_recovery() TO backup; \
                       GRANT EXECUTE ON FUNCTION pg_start_backup(text, boolean, boolean) TO backup; \
                       GRANT EXECUTE ON FUNCTION pg_stop_backup() TO backup; \
                       GRANT EXECUTE ON FUNCTION pg_stop_backup(boolean) TO backup; \
                       GRANT EXECUTE ON FUNCTION pg_create_restore_point(text) TO backup; \
                       GRANT EXECUTE ON FUNCTION pg_switch_xlog() TO backup; \
                       GRANT EXECUTE ON FUNCTION txid_current() TO backup; \
                       GRANT EXECUTE ON FUNCTION txid_current_snapshot() TO backup; \
                       GRANT EXECUTE ON FUNCTION txid_snapshot_xmax(txid_snapshot) TO backup; \
                       GRANT EXECUTE ON FUNCTION pg_ptrack_clear() TO backup; \
                       GRANT EXECUTE ON FUNCTION pg_ptrack_get_and_clear(oid, oid) TO backup;")

    @classmethod
    def tearDownClass(cls):
        cls.node.cleanup()
        cls.pb.del_test_dir(module_name, '')

    @unittest.skipIf(skip_test, "Module pexpect isn't installed. You need to install it.")
    def setUp(self):
        self.cmd = [self.pb.probackup_path, 'backup',
                    '-B', self.backup_dir,
                    '--instance', self.node.name,
                    '-h', '127.0.0.1',
                    '-p', str(self.node.port),
                    '-U', 'backup',
                    '-b', 'FULL'
                    ]

    def tearDown(self):
        pass

    def test_empty_password(self):
        try:
            self.assertIn("ERROR: no password supplied",
                          "".join(map(lambda x: x.decode("utf-8"),
                                      run_pb_with_auth(self.cmd, '\0\r\n'))
                                  )
                          )
        except (pexpect.TIMEOUT, pexpect.ExceptionPexpect) as e:
            self.fail(e.value)

    def test_wrong_password(self):
        try:
            self.assertIn("password authentication failed",
                          "".join(map(lambda x: x.decode("utf-8"),
                                      run_pb_with_auth(self.cmd, 'wrong_password\r\n'))
                                  )
                          )
        except (pexpect.TIMEOUT, pexpect.ExceptionPexpect) as e:
            self.fail(e.value)

    def test_right_password(self):
        try:
            self.assertIn("completed",
                          "".join(map(lambda x: x.decode("utf-8"),
                                      run_pb_with_auth(self.cmd, 'password\r\n'))
                                  )
                          )
        except (pexpect.TIMEOUT, pexpect.ExceptionPexpect) as e:
            self.fail(e.value)

    def test_ctrl_c_event(self):
        try:
            run_pb_with_auth(self.cmd, kill=True)
        except pexpect.TIMEOUT:
            self.fail("Error: CTRL+C event ignored")


def modify_pg_hba(node):
    hba_conf = os.path.join(node.data_dir, "pg_hba.conf")
    with open(hba_conf, 'r+', encoding='utf-8') as fio:
        data = fio.read()
        fio.seek(0)
        fio.write('host\tall\tpostgres\t127.0.0.1/0\ttrust\n' + data)


def run_pb_with_auth(cmd, password=None, kill=False):
    try:
        with pexpect.spawn(" ".join(cmd), timeout=10) as probackup:
            result = probackup.expect("Password for user .*:", 5)
            if kill:
                probackup.kill(signal.SIGINT)
            elif result == 0:
                probackup.sendline(password)
                return probackup.readlines()
            else:
                raise pexpect.TIMEOUT("")
    except pexpect.TIMEOUT:
        raise pexpect.TIMEOUT("Timeout error.")
    except pexpect.ExceptionPexpect:
        raise pexpect.ExceptionPexpect("Pexpect error.")
