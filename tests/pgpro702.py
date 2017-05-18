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
        make node without archiving, make stream backup,
        get Recovery Time, validate to Recovery Time
        EXPECT VALIDATE TO FAIL
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="tmp_dirs/pgpro702/{0}".format(fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        node.start()

        self.assertEqual(self.init_pb(node), six.b(""))
        id = self.backup_pb(node, backup_type='full', options=["--stream"])
        recovery_time = self.show_pb(node, id=id)['recovery-time']

        self.assertIn(six.b("INFO: backup validation completed successfully on"),
            self.validate_pb(node, options=["--time='{0}'".format(recovery_time)]))
