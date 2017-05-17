import unittest
import os
import six
from .ptrack_helpers import ProbackupTest, ProbackupException
from datetime import datetime, timedelta
from testgres import stop_all
import subprocess
from sys import exit


class SomeTest(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(SomeTest, self).__init__(*args, **kwargs)

#    @classmethod
#    def tearDownClass(cls):
#        stop_all()

    def test_archive_node_backup_stream_restore_to_recovery_time(self):
        """
        make node with archiving, make stream backup,
        get Recovery Time, try to make pitr to Recovery Time
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/pgpro668/{0}".format(fname),
            set_archiving=True,
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        node.start()

        self.assertEqual(self.init_pb(node), six.b(""))
        id = self.backup_pb(node, backup_type='full', options=["--stream"])
        recovery_time = self.show_pb(node, id=id)['recovery-time']

        node.pg_ctl('stop', {'-m': 'immediate', '-D': '{0}'.format(node.data_dir)})
        node.cleanup()

        self.restore_pb(node, options=['--time="{0}"'.format(recovery_time)])
        node.start({"-t": "600"})
        self.assertEqual(True, node.status())

    def test_validate_to_recovery_time(self):
        """
        make node with archiving, make stream backup,
        get Recovery Time, validate to Recovery Time
        Should fail. Waiting PGPRO-688
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/pgpro668/{0}".format(fname),
            set_archiving=True,
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        node.start()

        self.assertEqual(self.init_pb(node), six.b(""))
        id = self.backup_pb(node, backup_type='full', options=["--stream"])
        recovery_time = self.show_pb(node, id=id)['recovery-time']

        # Optional
        #node.psql("postgres", "select pg_create_restore_point('123')")
        #node.psql("postgres", "select txid_current()")
        #node.psql("postgres", "select pg_switch_xlog()")
        self.assertIn(six.b("INFO: backup validation completed successfully on"),
            self.validate_pb(node, options=["--time='{0}'".format(recovery_time)]))
        ####

        node.pg_ctl('stop', {'-m': 'immediate', '-D': '{0}'.format(node.data_dir)})
        node.cleanup()

        self.restore_pb(node, options=['--time="{0}"'.format(recovery_time)])
        node.start({"-t": "600"})
        self.assertEqual(True, node.status())

    def test_archive_node_backup_stream_additional_commit_pitr(self):
        """
        make node with archiving, make stream backup, create table t_heap,
        try to make pitr to Recovery Time, check that t_heap do not exists
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/pgpro668/{0}".format(fname),
            set_archiving=True,
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        node.start()

        self.assertEqual(self.init_pb(node), six.b(""))
        id = self.backup_pb(node, backup_type='full', options=["--stream"])
        node.psql("postgres", "create table t_heap(a int)")
        node.pg_ctl('stop', {'-m': 'immediate', '-D': '{0}'.format(node.data_dir)})
        node.cleanup()
        recovery_time = self.show_pb(node, id=id)['recovery-time']
        self.restore_pb(node,
            options=["-j", "4", '--time="{0}"'.format(recovery_time)]
            )
        node.start({"-t": "600"})
        res = node.psql("postgres", 'select * from t_heap')
        self.assertEqual(True, 'does not exist' in res[2])


# Need test for validate time with autonomous backup without archiving
# We need to forbid validation of autonomous backup by time or xid
# if archiving is not set
