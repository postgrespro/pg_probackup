import unittest
import os
import six
from .ptrack_helpers import ProbackupTest, ProbackupException
from datetime import datetime, timedelta
from testgres import stop_all
import subprocess
from sys import exit


class ValidateTest(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(ValidateTest, self).__init__(*args, **kwargs)

    @classmethod
    def tearDownClass(cls):
        stop_all()

    #@unittest.skip("skip")
    #@unittest.expectedFailure
    def test_validate_wal_unreal_values(self):
        """recovery to latest from full backup"""
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

        self.backup_pb(node)

        node.pgbench_init(scale=2)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )

        pgbench.wait()
        pgbench.stdout.close()

        backup_id = self.show_pb(node)[0]['ID']
        target_time = self.show_pb(node)[0]['Recovery time']
        after_backup_time = datetime.now()

        # Validate to real time
        self.assertIn(six.b("INFO: backup validation completed successfully on"),
            self.validate_pb(node, options=["--time='{0}'".format(target_time)]))

        # Validate to unreal time
        try:
            self.validate_pb(node, options=["--time='{:%Y-%m-%d %H:%M:%S}'".format(
                after_backup_time - timedelta(days=2))])
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Error in validation is expected because of validation of unreal time")
        except ProbackupException, e:
            self.assertEqual(
                e.message,
                'ERROR: Full backup satisfying target options is not found.\n')

        # Validate to unreal time #2
        try:
            self.validate_pb(node, options=["--time='{:%Y-%m-%d %H:%M:%S}'".format(
                after_backup_time + timedelta(days=2))])
            self.assertEqual(1, 0, "Error in validation is expected because of validation unreal time")
        except ProbackupException, e:
            self.assertEqual(True, 'ERROR: not enough WAL records to time' in e.message)

        # Validate to real xid
        target_xid = None
        with node.connect("postgres") as con:
            res = con.execute("INSERT INTO tbl0005 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]
        node.execute("postgres", "SELECT pg_switch_xlog()")

        self.assertIn(six.b("INFO: backup validation completed successfully on"),
            self.validate_pb(node, options=["--xid=%s" % target_xid]))

        # Validate to unreal xid
        try:
            self.validate_pb(node, options=["--xid=%d" % (int(target_xid) + 1000)])
            self.assertEqual(1, 0, "Error in validation is expected because of validation of unreal xid")
        except ProbackupException, e:
            self.assertEqual(True, 'ERROR: not enough WAL records to xid' in e.message)

        # Validate with backup ID
        self.assertIn(six.b("INFO: backup validation completed successfully on"),
            self.validate_pb(node, backup_id))

    def test_validate_broken_wal_1(self):
        """recovery to latest from full backup"""
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
            f.seek(256)
            f.write(six.b("blablabla"))
            f.close

        # Simple validate
        try:
            self.validate_pb(node)
            self.assertEqual(1, 0, "Expecting Error because of wal corruption. THIS IS BAD")
        except ProbackupException, e:
            self.assertEqual(True, 'Possible WAL CORRUPTION' in e.message)

        self.assertEqual('CORRUPT', self.show_pb(node, id=backup_id)['status'], 'Backup STATUS should be "CORRUPT"')

        node.stop()

    def test_validate_broken_wal_2(self):
        """recovery to latest from full backup"""
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
            f.seek(256)
            f.write(six.b("blablabla"))
            f.close

        # Simple validate
        try:
            self.validate_pb(node, backup_id, options=['--xid=%s' % target_xid])
            self.assertEqual(1, 0, "Expecting Error because of wal corruption. THIS IS BAD")
        except ProbackupException, e:
            self.assertEqual(True, 'Possible WAL CORRUPTION' in e.message)

        self.assertEqual('CORRUPT', self.show_pb(node, id=backup_id)['status'], 'Backup STATUS should be "CORRUPT"')
        node.stop()

    @unittest.skip("skip")
    def test_validate_wal_lost_segment_1(self):
        """Loose segment which belong to some backup"""
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
        os.remove(os.path.join(self.backup_dir(node), "wal", wals[1]))
        try:
            self.validate_pb(node)
            self.assertEqual(1, 0, "Expecting Error because of wal segment disappearance")
        except ProbackupException, e:
            self.assertEqual(True, 'is absent' in e.message)

        self.assertEqual('CORRUPT', self.show_pb(node, id=backup_id)['status'], 'Backup STATUS should be "CORRUPT"')
        node.stop()

    def test_validate_wal_lost_segment_2(self):
        """Loose segment located between backups"""
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
        os.remove(os.path.join(self.backup_dir(node), "wal", '0000000' + str(max(wals))))

        # Need more accurate error message about loosing wal segment between backups
        try:
            self.backup_pb(node, backup_type='page')
            self.assertEqual(1, 0, "Expecting Error in PAGE backup because of wal segment disappearance")
        except ProbackupException, e:
            self.assertEqual(True, 'is absent' in e.message)
        node.stop()
