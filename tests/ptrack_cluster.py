import unittest
from sys import exit
from testgres import get_new_node, stop_all
from .ptrack_helpers import ProbackupTest, idx_ptrack


class SimpleTest(ProbackupTest, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(SimpleTest, self).__init__(*args, **kwargs)

    def teardown(self):
        # clean_all()
        stop_all()

#    @unittest.skip("123")
    def test_ptrack_cluster_btree(self):
        fname = self.id().split('.')[3]
        print '{0} started'.format(fname)
        node = self.make_simple_node(base_dir="tmp_dirs/ptrack/{0}".format(fname),
            set_replication=True,
            initdb_params=['--data-checksums', '-A trust'],
            pg_options={'ptrack_enable': 'on', 'wal_level': 'replica', 'max_wal_senders': '2'})

        node.start()
        self.create_tblspace_in_node(node, 'somedata')

        # Create table and indexes
        res = node.psql(
            "postgres",
            "create table t_heap tablespace somedata as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] == 'heap':
                continue
            node.psql("postgres", "create index {0} on {1} using {2}({3}) tablespace somedata".format(
                i, idx_ptrack[i]['relation'], idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        node.psql('postgres', 'vacuum t_heap')
        node.psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get size of heap and indexes. size calculated in pages
            idx_ptrack[i]['old_size'] = self.get_fork_size(node, i)
            # get path to heap and index files
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # calculate md5sums of pages
            idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        self.init_pb(node)
        self.backup_pb(node, backup_type='full', options=['-j100', '--stream'])

        node.psql('postgres', 'delete from t_heap where id%2 = 1')
        node.psql('postgres', 'cluster t_heap using t_btree')
        node.psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get new size of heap and indexes. size calculated in pages
            idx_ptrack[i]['new_size'] = self.get_fork_size(node, i)
            # update path to heap and index files in case they`ve changed
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # calculate new md5sums for pages
            idx_ptrack[i]['new_pages'] = self.get_md5_per_page_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['new_size'])
            # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['new_size'])

            # compare pages and check ptrack sanity
            self.check_ptrack_sanity(idx_ptrack[i])

        self.clean_pb(node)
        node.stop()

    @unittest.skip("123")
    def test_ptrack_cluster_spgist(self):
        fname = self.id().split('.')[3]
        print '{0} started'.format(fname)
        node = self.make_simple_node(base_dir="tmp_dirs/ptrack/{0}".format(fname),
            set_replication=True,
            initdb_params=['--data-checksums', '-A trust'],
            pg_options={'ptrack_enable': 'on', 'wal_level': 'replica', 'max_wal_senders': '2'})

        node.start()
        self.create_tblspace_in_node(node, 'somedata')

        # Create table and indexes
        res = node.psql(
            "postgres",
            "create table t_heap tablespace somedata as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] == 'heap':
                continue
            node.psql("postgres", "create index {0} on {1} using {2}({3}) tablespace somedata".format(
                i, idx_ptrack[i]['relation'], idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        node.psql('postgres', 'vacuum t_heap')
        node.psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get size of heap and indexes. size calculated in pages
            idx_ptrack[i]['old_size'] = self.get_fork_size(node, i)
            # get path to heap and index files
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # calculate md5sums of pages
            idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        self.init_pb(node)
        self.backup_pb(node, backup_type='full', options=['-j100', '--stream'])

        node.psql('postgres', 'delete from t_heap where id%2 = 1')
        node.psql('postgres', 'cluster t_heap using t_spgist')
        node.psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get new size of heap and indexes. size calculated in pages
            idx_ptrack[i]['new_size'] = self.get_fork_size(node, i)
            # update path to heap and index files in case they`ve changed
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # calculate new md5sums for pages
            idx_ptrack[i]['new_pages'] = self.get_md5_per_page_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['new_size'])
            # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['new_size'])

            # compare pages and check ptrack sanity
            self.check_ptrack_sanity(idx_ptrack[i])

        self.clean_pb(node)
        node.stop()

    @unittest.skip("123")
    def test_ptrack_cluster_brin(self):
        fname = self.id().split('.')[3]
        print '{0} started'.format(fname)
        node = self.make_simple_node(base_dir="tmp_dirs/ptrack/{0}".format(fname),
            set_replication=True,
            initdb_params=['--data-checksums', '-A trust'],
            pg_options={'ptrack_enable': 'on', 'wal_level': 'replica', 'max_wal_senders': '2'})

        node.start()
        self.create_tblspace_in_node(node, 'somedata')

        # Create table and indexes
        res = node.psql(
            "postgres",
            "create table t_heap tablespace somedata as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] == 'heap':
                continue
            node.psql("postgres", "create index {0} on {1} using {2}({3}) tablespace somedata".format(
                i, idx_ptrack[i]['relation'], idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        node.psql('postgres', 'vacuum t_heap')
        node.psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get size of heap and indexes. size calculated in pages
            idx_ptrack[i]['old_size'] = self.get_fork_size(node, i)
            # get path to heap and index files
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # calculate md5sums of pages
            idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        self.init_pb(node)
        self.backup_pb(node, backup_type='full', options=['-j100', '--stream'])

        node.psql('postgres', 'delete from t_heap where id%2 = 1')
        node.psql('postgres', 'cluster t_heap using t_brin')
        node.psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get new size of heap and indexes. size calculated in pages
            idx_ptrack[i]['new_size'] = self.get_fork_size(node, i)
            # update path to heap and index files in case they`ve changed
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # calculate new md5sums for pages
            idx_ptrack[i]['new_pages'] = self.get_md5_per_page_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['new_size'])
            # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['new_size'])

            # compare pages and check ptrack sanity
            self.check_ptrack_sanity(idx_ptrack[i])

        self.clean_pb(node)
        node.stop()

    @unittest.skip("123")
    def test_ptrack_cluster_gist(self):
        fname = self.id().split('.')[3]
        print '{0} started'.format(fname)
        node = self.make_simple_node(base_dir="tmp_dirs/ptrack/{0}".format(fname),
            set_replication=True,
            initdb_params=['--data-checksums', '-A trust'],
            pg_options={'ptrack_enable': 'on', 'wal_level': 'replica', 'max_wal_senders': '2'})

        node.start()
        self.create_tblspace_in_node(node, 'somedata')

        # Create table and indexes
        res = node.psql(
            "postgres",
            "create table t_heap tablespace somedata as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] == 'heap':
                continue
            node.psql("postgres", "create index {0} on {1} using {2}({3}) tablespace somedata".format(
                i, idx_ptrack[i]['relation'], idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        node.psql('postgres', 'vacuum t_heap')
        node.psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get size of heap and indexes. size calculated in pages
            idx_ptrack[i]['old_size'] = self.get_fork_size(node, i)
            # get path to heap and index files
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # calculate md5sums of pages
            idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        self.init_pb(node)
        self.backup_pb(node, backup_type='full', options=['-j100', '--stream'])

        node.psql('postgres', 'delete from t_heap where id%2 = 1')
        node.psql('postgres', 'cluster t_heap using t_gist')
        node.psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get new size of heap and indexes. size calculated in pages
            idx_ptrack[i]['new_size'] = self.get_fork_size(node, i)
            # update path to heap and index files in case they`ve changed
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # calculate new md5sums for pages
            idx_ptrack[i]['new_pages'] = self.get_md5_per_page_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['new_size'])
            # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['new_size'])

            # compare pages and check ptrack sanity
            self.check_ptrack_sanity(idx_ptrack[i])

        self.clean_pb(node)
        node.stop()

    @unittest.skip("123")
    def test_ptrack_cluster_gin(self):
        fname = self.id().split('.')[3]
        print '{0} started'.format(fname)
        node = self.make_simple_node(base_dir="tmp_dirs/ptrack/{0}".format(fname),
            set_replication=True,
            initdb_params=['--data-checksums', '-A trust'],
            pg_options={'ptrack_enable': 'on', 'wal_level': 'replica', 'max_wal_senders': '2'})

        node.start()
        self.create_tblspace_in_node(node, 'somedata')

        # Create table and indexes
        res = node.psql(
            "postgres",
            "create table t_heap tablespace somedata as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] == 'heap':
                continue
            node.psql("postgres", "create index {0} on {1} using {2}({3}) tablespace somedata".format(
                i, idx_ptrack[i]['relation'], idx_ptrack[i]['type'], idx_ptrack[i]['column']))

        node.psql('postgres', 'vacuum t_heap')
        node.psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get size of heap and indexes. size calculated in pages
            idx_ptrack[i]['old_size'] = self.get_fork_size(node, i)
            # get path to heap and index files
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # calculate md5sums of pages
            idx_ptrack[i]['old_pages'] = self.get_md5_per_page_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['old_size'])

        self.init_pb(node)
        self.backup_pb(node, backup_type='full', options=['-j100', '--stream'])

        node.psql('postgres', 'delete from t_heap where id%2 = 1')
        node.psql('postgres', 'cluster t_heap using t_gin')
        node.psql('postgres', 'checkpoint')

        for i in idx_ptrack:
            # get new size of heap and indexes. size calculated in pages
            idx_ptrack[i]['new_size'] = self.get_fork_size(node, i)
            # update path to heap and index files in case they`ve changed
            idx_ptrack[i]['path'] = self.get_fork_path(node, i)
            # calculate new md5sums for pages
            idx_ptrack[i]['new_pages'] = self.get_md5_per_page_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['new_size'])
            # get ptrack for every idx
            idx_ptrack[i]['ptrack'] = self.get_ptrack_bits_per_for_fork(
                idx_ptrack[i]['path'], idx_ptrack[i]['new_size'])

            # compare pages and check ptrack sanity
            self.check_ptrack_sanity(idx_ptrack[i])

        self.clean_pb(node)
        node.stop()

if __name__ == '__main__':
    unittest.main()
