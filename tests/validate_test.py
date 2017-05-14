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

#    @classmethod
#    def tearDownClass(cls):
#        try:
#            stop_all()
#        except:
#            pass

#    @unittest.skip("123")
    def test_validate_time(self):
        """recovery to latest from full backup"""
        fname = self.id().split('.')[3]
        print '\n {0} started'.format(fname)
        node = self.make_simple_node(base_dir="tmp_dirs/validate/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        node.start()

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        self.assertEqual(self.init_pb(node), six.b(""))
        id = self.backup_pb(node)
        recovery_time = self.show_pb(node, id=id)['recovery-time']

        self.assertIn(six.b("INFO: backup validation completed successfully on"),
            self.validate_pb(node, options=["--time='{0}'".format(recovery_time)]))
        node.stop()

#    @unittest.skip("123")
    def test_validate_wal_1(self):
        """recovery to latest from full backup"""
        fname = self.id().split('.')[3]
        print '\n {0} started'.format(fname)
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

        with open(os.path.join(node.logs_dir, "backup_1.log"), "wb") as backup_log:
            backup_log.write(self.backup_pb(node, options=["--verbose"]))

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )

        pgbench.wait()
        pgbench.stdout.close()

        id_backup = self.show_pb(node)[0]['ID']
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
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                e.message,
                'ERROR: Full backup satisfying target options is not found.\n'
                )

        # Validate to unreal time #2
        try:
            self.validate_pb(node, options=["--time='{:%Y-%m-%d %H:%M:%S}'".format(
                after_backup_time + timedelta(days=2))])
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                True,
                'ERROR: not enough WAL records to time' in e.message
                )

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
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                True,
                'ERROR: not enough WAL records to xid' in e.message
                )

        # Validate with backup ID
        self.assertIn(six.b("INFO: backup validation completed successfully on"),
            self.validate_pb(node, id_backup))

        # Validate broken WAL
        wals_dir = os.path.join(self.backup_dir(node), "wal")
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        wals.sort()
        for wal in wals:
            f = open(os.path.join(wals_dir, wal), "rb+")
            f.seek(256)
            f.write(six.b("blablabla"))
            f.close

        try:
            self.validate_pb(node, id_backup, options=['--xid=%s' % target_xid])
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                True,
                'Possible WAL CORRUPTION' in e.message
                )

        try:
            self.validate_pb(node)
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                True,
                'Possible WAL CORRUPTION' in e.message
                )

        node.stop()

#    @unittest.skip("123")
    def test_validate_wal_lost_segment_1(self):
        """Loose segment which belong to some backup"""
        fname = self.id().split('.')[3]
        print '{0} started'.format(fname)
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
        self.backup_pb(node, backup_type='full')

        wals_dir = os.path.join(self.backup_dir(node), "wal")
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        os.remove(os.path.join(self.backup_dir(node), "wal", wals[1]))
        try:
            self.validate_pb(node)
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                True,
                'is absent' in e.message
                )
        node.stop()

#    @unittest.skip("123")
    def test_validate_wal_lost_segment_2(self):
        """Loose segment located between backups """
        fname = self.id().split('.')[3]
        print '{0} started'.format(fname)
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
        self.backup_pb(node, backup_type='full')

        # need to do that to find segment between(!) backups
        node.psql("postgres", "CREATE TABLE t1(a int)")
        node.psql("postgres", "SELECT pg_switch_xlog()")
        node.psql("postgres", "CREATE TABLE t2(a int)")
        node.psql("postgres", "SELECT pg_switch_xlog()")

        wals_dir = os.path.join(self.backup_dir(node), "wal")
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        wals = map(int, wals)

        # delete last wal segment
        print os.path.join(self.backup_dir(node), "wal", '0000000' + str(max(wals)))
        os.remove(os.path.join(self.backup_dir(node), "wal", '0000000' + str(max(wals))))

        # Need more accurate error message about loosing wal segment between backups
        try:
            self.backup_pb(node, backup_type='page')
            # we should die here because exception is what we expect to happen
            exit(1)
        except ProbackupException, e:
            self.assertEqual(
                True,
                'could not read WAL record' in e.message
                )
        self.delete_pb(node, id=self.show_pb(node)[1]['ID'])

        ##### Hole Smokes, Batman! We just lost a wal segment and know nothing about it
        ##### We need archive-push ASAP
        self.backup_pb(node, backup_type='full')
        self.assertEqual(False, 'validation completed successfully' in self.validate_pb(node))
        ########

        node.stop()
