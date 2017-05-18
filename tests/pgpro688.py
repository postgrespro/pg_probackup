import unittest
import os
import six
from .ptrack_helpers import ProbackupTest, ProbackupException
from datetime import datetime, timedelta
from testgres import stop_all
import subprocess
from sys import exit


class ValidateTime(ProbackupTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(ValidateTime, self).__init__(*args, **kwargs)

#    @classmethod
#    def tearDownClass(cls):
#        stop_all()

    def test_validate_recovery_time(self):
        """
        make node with archiving, make backup,
        get Recovery Time, validate to Recovery Time
        EXPECT VALIDATE TO FAIL
        Waiting PGPRO-688
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/pgpro668/{0}".format(fname),
            set_archiving=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        node.start()

        self.assertEqual(self.init_pb(node), six.b(""))
        id = self.backup_pb(node, backup_type='full')
        recovery_time = self.show_pb(node, id=id)['recovery-time']

        # Optional
        #node.psql("postgres", "select pg_create_restore_point('123')")
        #node.psql("postgres", "select txid_current()")
        #node.psql("postgres", "select pg_switch_xlog()")
        self.assertIn(six.b("INFO: backup validation completed successfully on"),
            self.validate_pb(node, options=["--time='{0}'".format(recovery_time)]))
        ####
