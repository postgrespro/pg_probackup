import os
import unittest
from sys import exit
from .helpers.ptrack_helpers import ProbackupTest, idx_ptrack


module_name = 'ptrack_recovery'


class SimpleTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_recovery(self):
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
        node.start()

        self.create_tblspace_in_node(node, 'somedata')

        # Create table
        node.safe_psql(
            "postgres",
            "create sequence t_seq; create table t_heap tablespace somedata "
            "as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,2560) i")

        # Create indexes
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                node.safe_psql(
                    "postgres", "create index {0} on {1} using {2}({3}) "
                    "tablespace somedata".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'], idx_ptrack[i]['column']))

            # get size of heap and indexes. size calculated in pages
            idx_ptrack[i]['size'] = int(self.get_fork_size(node, i))
            # get path to heap and index files
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)

        if self.verbose:
            print('Killing postmaster. Losing Ptrack changes')
        node.stop(['-m', 'immediate', '-D', node.data_dir])
        if not node.status():
            node.start()
        else:
            print("Die! Die! Why won't you die?... Why won't you die?")
            exit(1)

        for i in idx_ptrack:
            # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                node, idx_ptrack[i]['path'], [idx_ptrack[i]['size']])
            # check that ptrack has correct bits after recovery
            self.check_ptrack_recovery(idx_ptrack[i])

        # Clean after yourself
        self.del_test_dir(module_name, fname)
