import unittest
import os
import six
from helpers.ptrack_helpers import ProbackupTest, ProbackupException
from datetime import datetime, timedelta
from testgres import stop_all
import subprocess
from sys import exit
import re


class ValidateTest(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(ValidateTest, self).__init__(*args, **kwargs)
        self.module_name = 'validate'

    @classmethod
    def tearDownClass(cls):
        stop_all()

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_validate_wal_unreal_values(self):
        """make node with archiving, make archive backup, validate to both real and unreal values"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        node.pgbench_init(scale=2)
        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        backup_id = self.backup_node(backup_dir, 'node', node)

        node.pgbench_init(scale=2)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )

        pgbench.wait()
        pgbench.stdout.close()

        target_time = self.show_pb(backup_dir, 'node', backup_id)['recovery-time']
        after_backup_time = datetime.now().replace(second=0, microsecond=0)

        # Validate to real time
        self.assertIn(six.b("INFO: backup validation completed successfully"),
            self.validate_pb(backup_dir, 'node', options=["--time='{0}'".format(target_time)]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        # Validate to unreal time
        unreal_time_1 = after_backup_time - timedelta(days=2)
        try:
            self.validate_pb(backup_dir, 'node', options=["--time='{0}'".format(unreal_time_1)])
            self.assertEqual(1, 0, "Expecting Error because of validation to unreal time.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertEqual(e.message, 'ERROR: Full backup satisfying target options is not found.\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Validate to unreal time #2
        unreal_time_2 = after_backup_time + timedelta(days=2)
        try:
            self.validate_pb(backup_dir, 'node', options=["--time='{0}'".format(unreal_time_2)])
            self.assertEqual(1, 0, "Expecting Error because of validation to unreal time.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertTrue('ERROR: not enough WAL records to time' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Validate to real xid
        target_xid = None
        with node.connect("postgres") as con:
            res = con.execute("INSERT INTO tbl0005 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]
        node.execute("postgres", "SELECT pg_switch_xlog()")

        self.assertIn(six.b("INFO: backup validation completed successfully"),
            self.validate_pb(backup_dir, 'node', options=["--xid={0}".format(target_xid)]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        # Validate to unreal xid
        unreal_xid = int(target_xid) + 1000
        try:
            self.validate_pb(backup_dir, 'node', options=["--xid={0}".format(unreal_xid)])
            self.assertEqual(1, 0, "Expecting Error because of validation to unreal xid.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertTrue('ERROR: not enough WAL records to xid' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Validate with backup ID
        self.assertIn(six.b("INFO: backup validation completed successfully"),
            self.validate_pb(backup_dir, 'node', backup_id),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

    # @unittest.skip("skip")
    def test_validate_corrupt_wal_1(self):
        """make archive node, make archive backup, corrupt all wal files, run validate, expect errors"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        backup_id = self.backup_node(backup_dir, 'node', node)

        # Corrupt WAL
        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        wals.sort()
        for wal in wals:
            f = open(os.path.join(wals_dir, wal), "rb+")
            f.seek(42)
            f.write(six.b("blablablaadssaaaaaaaaaaaaaaa"))
            f.close

        # Simple validate
        try:
            self.validate_pb(backup_dir, 'node')
            self.assertEqual(1, 0, "Expecting Error because of wal segments corruption.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertTrue('Possible WAL CORRUPTION' in e.message),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd)

        self.assertEqual('CORRUPT', self.show_pb(backup_dir, 'node', backup_id)['status'], 'Backup STATUS should be "CORRUPT"')
        node.stop()

    # @unittest.skip("skip")
    def test_validate_corrupt_wal_2(self):
        """make archive node, make full backup, corrupt all wal files, run validate to real xid, expect errors"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        backup_id = self.backup_node(backup_dir, 'node', node)
        target_xid = None
        with node.connect("postgres") as con:
            res = con.execute("INSERT INTO tbl0005 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]

        # Corrupt WAL
        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        wals.sort()
        for wal in wals:
            f = open(os.path.join(wals_dir, wal), "rb+")
            f.seek(0)
            f.write(six.b("blablabla"))
            f.close

        # Validate to xid
        try:
            self.validate_pb(backup_dir, 'node', backup_id, options=['--xid={0}'.format(target_xid)])
            self.assertEqual(1, 0, "Expecting Error because of wal segments corruption.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertTrue('Possible WAL CORRUPTION' in e.message),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd)

        self.assertEqual('CORRUPT', self.show_pb(backup_dir, 'node', backup_id)['status'], 'Backup STATUS should be "CORRUPT"')
        node.stop()

    # @unittest.skip("skip")
    def test_validate_wal_lost_segment_1(self):
        """make archive node, make archive full backup,
        delete from archive wal segment which belong to previous backup
        run validate, expecting error because of missing wal segment
        make sure that backup status is 'CORRUPT'
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        node.pgbench_init(scale=2)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )
        pgbench.wait()
        pgbench.stdout.close()
        backup_id = self.backup_node(backup_dir, 'node', node)

        # Delete wal segment
        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        file = os.path.join(backup_dir, 'wal', 'node', wals[1])
        os.remove(file)
        try:
            self.validate_pb(backup_dir, 'node')
            self.assertEqual(1, 0, "Expecting Error because of wal segment disappearance.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertIn('WARNING: WAL segment "{0}" is absent\nERROR: there are not enough WAL records to restore'.format(
                file), e.message, '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        self.assertEqual('CORRUPT', self.show_pb(backup_dir, 'node', backup_id)['status'], 'Backup {0} should have STATUS "CORRUPT"')

        # Be paranoid and run validate again
        try:
            self.validate_pb(backup_dir, 'node')
            self.assertEqual(1, 0, "Expecting Error because of backup corruption.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertIn('INFO: Backup {0} has status CORRUPT. Skip validation.\n'.format(backup_id), e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))
        node.stop()

    # @unittest.skip("skip")
    def test_validate_wal_lost_segment_2(self):
        """
        make node with archiving
        make archive backup
        delete from archive wal segment which DO NOT belong to previous backup
        run validate, expecting error because of missing wal segment
        make sure that backup status is 'ERROR'
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        self.backup_node(backup_dir, 'node', node)

        # make some wals
        node.pgbench_init(scale=2)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        # delete last wal segment
        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        wals = map(int, wals)
        file = os.path.join(wals_dir, '0000000' + str(max(wals)))
        os.remove(file)

        try:
            backup_id = self.backup_node(backup_dir, 'node', node, backup_type='page')
            self.assertEqual(1, 0, "Expecting Error because of wal segment disappearance.\n Output: {0} \n CMD: {1}".format(
                self.output, self.cmd))
        except ProbackupException, e:
            self.assertTrue('INFO: wait for LSN'
                and 'in archived WAL segment'
                and 'WARNING: could not read WAL record at'
                and 'ERROR: WAL segment "{0}" is absent\n'.format(file) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        self.assertEqual('ERROR', self.show_pb(backup_dir, 'node')[1]['Status'], 'Backup {0} should have STATUS "ERROR"')
        node.stop()
