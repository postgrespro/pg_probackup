import os
import unittest
import tempfile
import signal

try:
    import pexpect
except:
    unittest.TestCase.fail("Error: The module pexpect not found")

from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from testgres import QueryException


module_name = 'auth_test'


class AuthTest(unittest.TestCase):
    pb = None
    node = None

    @classmethod
    def setUpClass(cls):

        super(AuthTest, cls).setUpClass()

        cls.pb = ProbackupTest()
        cls.backup_dir = os.path.join(cls.pb.tmp_path, module_name, 'backup')

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
        cls.node.start()
        try:
            add_backup_user(cls.node)
        except QueryException:
            assert False, "Query error. The backup user cannot be added."

    @classmethod
    def tearDownClass(cls):
        cls.node.cleanup()
        cls.pb.del_test_dir(module_name, '')

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

    def tearDown(self):
        pass

    def test_empty_password(self):
        print(run_pb_pexpect(self.cmd, '\0\r\n'))

    def test_wrong_password(self):
        print(run_pb_pexpect(self.cmd, 'wrong_password\r\n'))

    def test_right_password(self):
        print(run_pb_pexpect(self.cmd, 'password\r\n'))

    def test_ctrl_c_event(self):
        print(run_pb_pexpect(self.cmd, kill=True))


def modify_pg_hba(node):
    hba_conf = os.path.join(node.data_dir, "pg_hba.conf")
    with open(hba_conf, 'r+', encoding='utf-8') as fio:
        data = fio.read()
        fio.seek(0)
        fio.write('host\tall\tpostgres\t127.0.0.1/0\ttrust\n' + data)


def run_pb_pexpect(cmd, password=None, kill=False):
    try:
        with pexpect.spawn(" ".join(cmd)) as probackup:
            result = probackup.expect("Password .*:")
            if kill:
                try:
                    probackup.kill(signal.SIGINT)
                except TIMEOUT:
                    raise ProbackupException("Error: Timeout exepired", " ".join(cmd))

            elif result == 0:
                probackup.sendline(password)
                return probackup.readlines()
            else:
                raise ProbackupTest("Error: runtime error", " ".join(cmd))
    except ProbackupTest:
        raise ProbackupException("Error: pexpect error", " ".join(cmd))


def add_backup_user(node):
    query = """
    CREATE ROLE backup WITH LOGIN PASSWORD 'password';
    GRANT USAGE ON SCHEMA pg_catalog TO backup;
    GRANT EXECUTE ON FUNCTION current_setting(text) TO backup;
    GRANT EXECUTE ON FUNCTION pg_is_in_recovery() TO backup;
    GRANT EXECUTE ON FUNCTION pg_start_backup(text, boolean, boolean) TO backup;
    GRANT EXECUTE ON FUNCTION pg_stop_backup() TO backup;
    GRANT EXECUTE ON FUNCTION pg_stop_backup(boolean) TO backup;
    GRANT EXECUTE ON FUNCTION pg_create_restore_point(text) TO backup;
    GRANT EXECUTE ON FUNCTION pg_switch_xlog() TO backup;
    GRANT EXECUTE ON FUNCTION txid_current() TO backup;
    GRANT EXECUTE ON FUNCTION txid_current_snapshot() TO backup;
    GRANT EXECUTE ON FUNCTION txid_snapshot_xmax(txid_snapshot) TO backup;
    GRANT EXECUTE ON FUNCTION pg_ptrack_clear() TO backup;
    GRANT EXECUTE ON FUNCTION pg_ptrack_get_and_clear(oid, oid) TO backup;
    """
    node.safe_psql("postgres", query)
