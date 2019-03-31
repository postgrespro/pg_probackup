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

    @unittest.skip("skip")
    def checkdb_index_loss(self):
        """make node, make full and ptrack stream backups,"
        " restore them and check data correctness"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'])

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

    # @unittest.skip("skip")
    def test_checkdb_block_validation(self):
        """make node, corrupt some pages, check that checkdb failed"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        #self.backup_node(
        #    backup_dir, 'node', node,
        #    backup_type="full", options=["-j", "4", "--stream"])

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1000) i")
        node.safe_psql(
            "postgres",
            "CHECKPOINT;")

        heap_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").rstrip()

        with open(os.path.join(node.data_dir, heap_path), "rb+", 0) as f:
                f.seek(9000)
                f.write(b"bla")
                f.flush()
                f.close

        with open(os.path.join(node.data_dir, heap_path), "rb+", 0) as f:
                f.seek(42000)
                f.write(b"bla")
                f.flush()
                f.close

        print(self.checkdb_node('node', backup_dir,
            data_dir=node.data_dir, options=['--block-validation']))

        # Clean after yourself
        self.del_test_dir(module_name, fname)
