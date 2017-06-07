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

    @classmethod
    def tearDownClass(cls):
        stop_all()

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_validate_wal_unreal_values(self):
        """make node with archiving, make archive backup, validate to both real and unreal values"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/validate/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )

        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        node.pgbench_init(scale=2)
        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        backup_id = self.backup_pb(node)

        node.pgbench_init(scale=2)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )

        pgbench.wait()
        pgbench.stdout.close()

        target_time = self.show_pb(node, id=backup_id)['recovery-time']
        after_backup_time = datetime.now().replace(second=0, microsecond=0)

        # Validate to real time
        self.assertIn(six.b("INFO: backup validation completed successfully on"),
            self.validate_pb(node, options=["--time='{0}'".format(target_time)]))

        # Validate to unreal time
        unreal_time_1 = after_backup_time - timedelta(days=2)
        try:
            self.validate_pb(node, options=["--time='{0}'".format(unreal_time_1)])
            self.assertEqual(1, 0, "Expecting Error because of validation to unreal time.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertEqual(e.message, 'ERROR: Full backup satisfying target options is not found.\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Validate to unreal time #2
        unreal_time_2 = after_backup_time + timedelta(days=2)
        try:
            self.validate_pb(node, options=["--time='{0}'".format(unreal_time_2)])
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

        self.assertIn(six.b("INFO: backup validation completed successfully on"),
            self.validate_pb(node, options=["--xid={0}".format(target_xid)]))

        # Validate to unreal xid
        unreal_xid = int(target_xid) + 1000
        try:
            self.validate_pb(node, options=["--xid={0}".format(unreal_xid)])
            self.assertEqual(1, 0, "Expecting Error because of validation to unreal xid.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertTrue('ERROR: not enough WAL records to xid' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Validate with backup ID
        self.assertIn(six.b("INFO: backup validation completed successfully on"),
            self.validate_pb(node, backup_id))

    # @unittest.skip("skip")
    def test_validate_corrupt_wal_1(self):
        """
        make node with archiving
        make archive backup
        corrupt all wal files
        run validate, expecting error because of wal corruption
        make sure that backup status is 'CORRUPT'
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/validate/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )

        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        backup_id = self.backup_pb(node)

        # Corrupt WAL
        wals_dir = os.path.join(self.backup_dir(node), "wal")
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        wals.sort()
        for wal in wals:
            f = open(os.path.join(wals_dir, wal), "rb+")
            f.seek(42)
            f.write(six.b("blablablaadssaaaaaaaaaaaaaaa"))
            f.close

        # Simple validate
        try:
            self.validate_pb(node)
            self.assertEqual(1, 0, "Expecting Error because of wal segments corruption.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertTrue('Possible WAL CORRUPTION' in e.message),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd)

        self.assertEqual('CORRUPT', self.show_pb(node, id=backup_id)['status'], 'Backup STATUS should be "CORRUPT"')
        node.stop()

    # @unittest.skip("skip")
    def test_validate_corrupt_wal_2(self):
        """
        make node with archiving
        make archive backup
        corrupt all wal files
        run validate to real xid, expecting error because of wal corruption
        make sure that backup status is 'CORRUPT'
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/validate/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )

        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        backup_id = self.backup_pb(node)
        target_xid = None
        with node.connect("postgres") as con:
            res = con.execute("INSERT INTO tbl0005 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]
        node.execute("postgres", "SELECT pg_switch_xlog()")

        # Corrupt WAL
        wals_dir = os.path.join(self.backup_dir(node), "wal")
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        wals.sort()
        for wal in wals:
            f = open(os.path.join(wals_dir, wal), "rb+")
            f.seek(0)
            f.write(six.b("blablabla"))
            f.close

        # Validate to xid
        try:
            self.validate_pb(node, backup_id, options=['--xid={0}'.format(target_xid)])
            self.assertEqual(1, 0, "Expecting Error because of wal segments corruption.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertTrue('Possible WAL CORRUPTION' in e.message),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd)

        self.assertEqual('CORRUPT', self.show_pb(node, id=backup_id)['status'], 'Backup STATUS should be "CORRUPT"')
        node.stop()

    # @unittest.skip("skip")
    def test_validate_wal_lost_segment_1(self):
        """
        make node with archiving
        make archive backup
        delete from archive wal segment which belong to previous backup
        run validate, expecting error because of missing wal segment
        make sure that backup status is 'CORRUPT'
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/validate/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )

        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        node.pgbench_init(scale=2)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )
        pgbench.wait()
        pgbench.stdout.close()
        backup_id = self.backup_pb(node, backup_type='full')

        # Delete wal segment
        wals_dir = os.path.join(self.backup_dir(node), "wal")
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        file = os.path.join(self.backup_dir(node), "wal", wals[1])
        os.remove(file)
        try:
            self.validate_pb(node)
            self.assertEqual(1, 0, "Expecting Error because of wal segment disappearance.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertTrue(re.match('WARNING: WAL segment "{0}" is absent\nERROR: there are not enough WAL records to restore from [0-9a-fA-F\/]+ to [0-9a-fA-F\/]+\n\Z'.format(
                file), e.message),
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        self.assertEqual('CORRUPT', self.show_pb(node, id=backup_id)['status'], 'Backup {0} should have STATUS "CORRUPT"')

        # Be paranoid and run validate again
        try:
            self.validate_pb(node)
            self.assertEqual(1, 0, "Expecting Error because of backup corruption.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException, e:
            self.assertTrue(re.match('ERROR: Backup {0} has status: CORRUPT\n\Z'.format(backup_id), e.message),
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
        node = self.make_simple_node(base_dir="tmp_dirs/validate/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )

        node.start()
        self.assertEqual(self.init_pb(node), six.b(""))
        self.backup_pb(node, backup_type='full')

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
        wals_dir = os.path.join(self.backup_dir(node), "wal")
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        wals = map(int, wals)
        file = os.path.join(self.backup_dir(node), "wal", '0000000' + str(max(wals)))
        os.remove(file)

        try:
            backup_id = self.backup_pb(node, backup_type='page')
            self.assertEqual(1, 0, "Expecting Error because of wal segment disappearance.\n Output: {0} \n CMD: {1}".format(
                self.output, self.cmd))
        except ProbackupException, e:
            self.assertTrue(re.match('INFO: wait for LSN [0-9a-fA-F\/]+ in archived WAL segment .*\nWARNING: could not read WAL record at [0-9a-fA-F\/]+\nERROR: WAL segment "{0}" is absent\n\Z'.format(
                file), e.message),
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        self.assertEqual('ERROR', self.show_pb(node)[1]['Status'], 'Backup {0} should have STATUS "ERROR"')
        node.stop()
