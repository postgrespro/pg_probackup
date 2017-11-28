import os
import unittest
import tempfile

from subprocess import Popen, PIPE, TimeoutExpired
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from testgres import QueryException
module_name = 'auth_test'
ptrack = False


class AuthTest(unittest.TestCase):
    pb = None
    node = None

    @classmethod
    def setUpClass(cls):
        global ptrack

        super(AuthTest, cls).setUpClass()

        cls.pb = ProbackupTest()
        cls.backup_dir = os.path.join(cls.pb.tmp_path, module_name, 'backup')

        cls.node = cls.pb.make_simple_node(
            base_dir="{}/node".format(module_name),
            set_replication=True,
            initdb_params=['--data-checksums', '--auth-host=md5'],
        )

        cls.node.start()
        try:
            add_backup_user(cls.node)
        except QueryException:
            assert False, "Query error. The backup user cannot be added."

        cls.backup_dir = os.path.join(tempfile.tempdir, "backups")
        cls.pb.init_pb(cls.backup_dir)
        cls.pb.add_instance(cls.backup_dir, cls.node.name, cls.node)

        cls.pb.set_archiving(cls.backup_dir, cls.node.name, cls.node)

    @classmethod
    def tearDownClass(cls):
        cls.pb.del_test_dir(module_name, '')

    def setUp(self):
        self.cmd = [self.pb.probackup_path, 'pg_probackup',
            '-B', self.backup_dir,
            '--instance', self.node.name,
            '-h', '127.0.0.1',
            '-p', self.node.port,
            '-U', 'backup'
            '-b', 'FULL'
            ]

    def tearDown(self):
        pass

    @unittest.skip
    def test_empty_password(self):
        try:
            run_pb_with_auth(self.cmd,'\0')
        except:
            pass

    def test_wrong_password(self):
        try:
            run_pb_with_auth(self.cmd,'wrong_password')
        except:
            pass

    @unittest.skip
    def test_ctrl_c_event(self):
        try:
            run_pb_with_auth(self.cmd,signal='CTRL_C_EVENT')
        except:
            pass


def run_pb_with_auth(cmd, password=None, signal=None):
    probackup = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)

    if probackup.stdout.readline() == "Password:":
        if signal:
            probackup.send_signal(signal)
        try:
            out, err = probackup.communicate(password, 10)
            return probackup.returncode, out, err
        except TimeoutExpired:
            raise ProbackupException


def add_backup_user(node):
    query = '''
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
    '''
    node.psql("postgres", query.replace('\n',''))
