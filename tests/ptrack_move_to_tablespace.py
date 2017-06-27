import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, idx_ptrack


class SimpleTest(ProbackupTest, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(SimpleTest, self).__init__(*args, **kwargs)
        self.module_name = 'ptrack_move_to_tablespace'

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_recovery(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'ptrack_enable': 'on', 'wal_level': 'replica', 'max_wal_senders': '2'})
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
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
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                node, idx_ptrack[i]['path'], idx_ptrack[i]['size'])
            # check that ptrack has correct bits after recovery
            self.check_ptrack_recovery(idx_ptrack[i])

        # Clean after yourself
        self.del_test_dir(self.module_name, fname)
