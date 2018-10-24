import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, idx_ptrack


module_name = 'ptrack_vacuum_bits_visibility'


class SimpleTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_vacuum_bits_visibility(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'ptrack_enable': 'on',
                'wal_level': 'replica',
                'max_wal_senders': '2'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'somedata')

        # Create table and indexes
        res = node.safe_psql(
            "postgres",
            "create extension bloom; create sequence t_seq; "
            "create table t_heap tablespace somedata "
            "as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")

        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                node.safe_psql(
                    "postgres",
                    "create index {0} on {1} using {2}({3}) "
                    "tablespace somedata".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        node.safe_psql('postgres', 'checkpoint')

        self.backup_node(
            backup_dir, 'node', node, options=['-j10', '--stream'])

        for i in idx_ptrack:
            # get size of heap and indexes. size calculated in pages
            idx_ptrack[i]['old_size'] = self.get_fork_size(node, i)
            # get path to heap and index files
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # calculate md5sums of pages
            idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        node.safe_psql('postgres', 'vacuum t_heap')
        node.safe_psql('postgres', 'checkpoint')

        # CHECK PTRACK SANITY
        success = True
        for i in idx_ptrack:
            # get new size of heap and indexes. size calculated in pages
            idx_ptrack[i]['new_size'] = self.get_fork_size(node, i)
            # update path to heap and index files in case they`ve changed
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # calculate new md5sums for pages
            idx_ptrack[i]['new_pages'] = self.get_md5_per_page_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['new_size'])
            # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                node, idx_ptrack[i]['path'],
                [idx_ptrack[i]['old_size'], idx_ptrack[i]['new_size']])

            # compare pages and check ptrack sanity
            if not self.check_ptrack_sanity(idx_ptrack[i]):
                success = False

        self.assertTrue(
            success, 'Ptrack has failed to register changes in data files'
        )

        # Clean after yourself
        self.del_test_dir(module_name, fname)
