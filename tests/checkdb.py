import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from datetime import datetime, timedelta
import subprocess
from testgres import QueryException
import shutil
import sys
import time


module_name = 'checkdb'


class CheckdbTest(ProbackupTest, unittest.TestCase):

    def test_checkdb_index_loss(self):
        """make node, make full and ptrack stream backups,"
        " restore them and check data correctness"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
            }
        )
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i"
            " as id from generate_series(0,100) i"
        )

        node.safe_psql(
            "postgres",
            "create index on t_heap(id)"
        )

        gdb = self.checkdb_node(
        	'node', data_dir=node.data_dir,
        	gdb=True, options=['-d', 'postgres', '--amcheck', '-p', str(node.port)])

        gdb.set_breakpoint('amcheck_one_index')
        gdb.run_until_break()

        node.safe_psql(
            "postgres",
            "drop table t_heap"
        )

        gdb.continue_execution_until_exit()

        # Clean after yourself
        self.del_test_dir(module_name, fname)