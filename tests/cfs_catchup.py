import os
import unittest
import random
import shutil

from .helpers.cfs_helpers import find_by_extensions, find_by_name, find_by_pattern, corrupt_file
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException

module_name = 'cfs_catchup'
tblspace_name = 'cfs_tblspace'


class CfsCatchupNoEncTest(ProbackupTest, unittest.TestCase):
    def setUp(self):
        self.fname = self.id().split('.')[3]

    @unittest.skipUnless(ProbackupTest.enterprise, 'skip')
    def test_full_catchup_with_tablespace(self):
        """
        Test tablespace transfers
        """
        # preparation
        src_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, self.fname, 'src'),
            set_replication = True
        )
        src_pg.slow_start()
        tblspace1_old_path = self.get_tblspace_path(src_pg, 'tblspace1_old')
        self.create_tblspace_in_node(src_pg, 'tblspace1', tblspc_path = tblspace1_old_path, cfs=True)
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question TABLESPACE tblspace1 AS SELECT 42 AS answer")
        src_query_result = src_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")
        src_pg.safe_psql(
            "postgres",
            "CHECKPOINT")

        # do full catchup with tablespace mapping
        dst_pg = self.make_empty_node(os.path.join(module_name, self.fname, 'dst'))
        tblspace1_new_path = self.get_tblspace_path(dst_pg, 'tblspace1_new')
        self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = [
                '-d', 'postgres',
                '-p', str(src_pg.port),
                '--stream',
                '-T', '{0}={1}'.format(tblspace1_old_path, tblspace1_new_path)
            ]
        )

        # 1st check: compare data directories
        self.compare_pgdata(
            self.pgdata_content(src_pg.data_dir),
            self.pgdata_content(dst_pg.data_dir)
        )

        # check cfm size
        cfms = find_by_extensions([os.path.join(dst_pg.data_dir)], ['.cfm'])
        self.assertTrue(cfms, "ERROR: .cfm files not found in backup dir")
        for cfm in cfms:
            size = os.stat(cfm).st_size
            self.assertLessEqual(size, 4096,
                                 "ERROR: {0} is not truncated (has size {1} > 4096)".format(
                                     cfm, size
                                 ))

        # make changes in master tablespace
        src_pg.safe_psql(
            "postgres",
            "UPDATE ultimate_question SET answer = -1")
        src_pg.safe_psql(
            "postgres",
            "CHECKPOINT")

        # run&recover catchup'ed instance
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start()

        # 2nd check: run verification query
        dst_query_result = dst_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # and now delta backup
        dst_pg.stop()

        self.catchup_node(
            backup_mode = 'DELTA',
            source_pgdata = src_pg.data_dir,
            destination_node = dst_pg,
            options = [
                '-d', 'postgres',
                '-p', str(src_pg.port),
                '--stream',
                '-T', '{0}={1}'.format(tblspace1_old_path, tblspace1_new_path)
            ]
        )

        # check cfm size again
        cfms = find_by_extensions([os.path.join(dst_pg.data_dir)], ['.cfm'])
        self.assertTrue(cfms, "ERROR: .cfm files not found in backup dir")
        for cfm in cfms:
            size = os.stat(cfm).st_size
            self.assertLessEqual(size, 4096,
                                 "ERROR: {0} is not truncated (has size {1} > 4096)".format(
                                     cfm, size
                                 ))

        # run&recover catchup'ed instance
        dst_options = {}
        dst_options['port'] = str(dst_pg.port)
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start()


        # 3rd check: run verification query
        src_query_result = src_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")
        dst_query_result = dst_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")
        self.assertEqual(src_query_result, dst_query_result, 'Different answer from copy')

        # Cleanup
        src_pg.stop()
        dst_pg.stop()
        self.del_test_dir(module_name, self.fname)
