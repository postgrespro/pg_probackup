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

        # Create table
        node.psql("postgres",
            "create table t_heap tablespace somedata as select i as id, md5(i::text) as text,md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")
        # Create indexes
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap':
                node.psql("postgres", "create index {0} on {1} using {2}({3}) tablespace somedata".format(
                    i, idx_ptrack[i]['relation'], idx_ptrack[i]['type'], idx_ptrack[i]['column']))

            # get size of heap and indexes. size calculated in pages
            idx_ptrack[i]['size'] = int(self.get_fork_size(node, i))
            # get path to heap and index files
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)

        print 'Killing postmaster. Losing Ptrack changes'
        node.pg_ctl('stop', {'-m': 'immediate', '-D': '{0}'.format(node.data_dir)})    
        if not node.status():
            node.start()
        else:
            print "Die! Die! Why won't you die?... Why won't you die?"
            exit(1)

        for i in idx_ptrack:
            # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['size'])
            # check that ptrack has correct bits after recovery
            self.check_ptrack_recovery(idx_ptrack[i])

        self.clean_pb(node)
        node.stop()

if __name__ == '__main__':
    unittest.main()
