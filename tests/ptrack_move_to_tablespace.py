import unittest
from sys import exit
from testgres import get_new_node, stop_all
import os
from signal import SIGTERM
from .ptrack_helpers import ProbackupTest, idx_ptrack
from time import sleep


class SimpleTest(ProbackupTest, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(SimpleTest, self).__init__(*args, **kwargs)

    def teardown(self):
        # clean_all()
        stop_all()

    def test_ptrack_recovery(self):
        fname = self.id().split(".")[3]
        print '{0} started'.format(fname)
        node = self.make_simple_node(base_dir="tmp_dirs/ptrack/{0}".format(fname),
            set_replication=True,
            initdb_params=['--data-checksums', '-A trust'],
            pg_options={'ptrack_enable': 'on', 'wal_level': 'replica', 'max_wal_senders': '2'})

        node.start()
        self.create_tblspace_in_node(node, 'somedata')

        # Create table and indexes
        node.psql("postgres",
            "create table t_heap as select i as id, md5(i::text) as text,md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap':
                node.psql("postgres", "create index {0} on {1} using {2}({3})".format(
                    i, idx_ptrack[i]['relation'], idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        # Move table and indexes and make checkpoint
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] == 'heap':
                node.psql('postgres', 'alter table {0} set tablespace somedata;'.format(i))
                continue
            node.psql('postgres', 'alter index {0} set tablespace somedata'.format(i))
        node.psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get size of heap and indexes. size calculated in pages
            idx_ptrack[i]['size'] = self.get_fork_size(node, i)
            # get path to heap and index files
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_for_fork(idx_ptrack[i]['path'])
            # check that ptrack has correct bits after recovery
            self.check_ptrack_recovery(idx_ptrack[i])

        self.clean_pb(node)
        node.stop()

if __name__ == '__main__':
    unittest.main()
