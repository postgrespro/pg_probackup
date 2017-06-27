import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, idx_ptrack


class SimpleTest(ProbackupTest, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(SimpleTest, self).__init__(*args, **kwargs)
        self.module_name = 'ptrack_clean'

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_clean(self):
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(self.module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'ptrack_enable': 'on', 'wal_level': 'replica', 'max_wal_senders': '2'})
        backup_dir = os.path.join(self.tmp_path, self.module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        self.create_tblspace_in_node(node, 'somedata')

        # Create table and indexes
        node.psql(
            "postgres",
            "create table t_heap tablespace somedata as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] == 'heap':
                continue
            node.psql("postgres", "create index {0} on {1} using {2}({3}) tablespace somedata".format(
                i, idx_ptrack[i]['relation'], idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        # Make full backup to clean every ptrack
        self.backup_node(backup_dir, 'node', node, options=['-j100', '--stream'])

        for i in idx_ptrack:
            # get fork size and calculate it in pages
            idx_ptrack[i]['size'] = self.get_fork_size(node, i)
            # get path to heap and index files
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                node, idx_ptrack[i]['path'], idx_ptrack[i]['size'])
            self.check_ptrack_clean(idx_ptrack[i], idx_ptrack[i]['size'])

        # Update everything, vacuum it and make PTRACK BACKUP
        node.psql('postgres', 'update t_heap set text = md5(text), tsvector = md5(repeat(tsvector::text, 10))::tsvector;')
        node.psql('postgres', 'vacuum t_heap')

        backup_id = self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=['-j100', '--stream'])
        node.psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get new size of heap and indexes and calculate it in pages
            idx_ptrack[i]['size'] = self.get_fork_size(node, i)
            # update path to heap and index files in case they`ve changed
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                node, idx_ptrack[i]['path'], idx_ptrack[i]['size'])
            # check that ptrack bits are cleaned
            self.check_ptrack_clean(idx_ptrack[i], idx_ptrack[i]['size'])

        # Update everything, vacuum it and make PAGE BACKUP
        node.psql('postgres', 'update t_heap set text = md5(text), tsvector = md5(repeat(tsvector::text, 10))::tsvector;')
        node.psql('postgres', 'vacuum t_heap')

        # Make page backup to clean every ptrack
        self.backup_node(backup_dir, 'node', node, backup_type='page', options=['-j100'])
        node.psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get new size of heap and indexes and calculate it in pages
            idx_ptrack[i]['size'] = self.get_fork_size(node, i)
            # update path to heap and index files in case they`ve changed
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
                node, idx_ptrack[i]['path'], idx_ptrack[i]['size'])
            # check that ptrack bits are cleaned
            self.check_ptrack_clean(idx_ptrack[i], idx_ptrack[i]['size'])

        # Clean after yourself
        self.del_test_dir(self.module_name, fname)
