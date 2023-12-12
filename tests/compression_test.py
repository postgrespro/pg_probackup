import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest
from .helpers.init_helpers import init_params
from datetime import datetime, timedelta
import subprocess

def have_alg(alg):
    return alg in init_params.probackup_compressions

class CompressionTest(ProbackupTest):

    def test_basic_compression_stream_pglz(self):
        self._test_compression_stream(compression = 'pglz')

    def test_basic_compression_stream_zlib(self):
        self._test_compression_stream(compression = 'zlib')

    @unittest.skipUnless(have_alg('lz4'), "pg_probackup is not compiled with lz4 support")
    def test_basic_compression_stream_lz4(self):
        self._test_compression_stream(compression = 'lz4')

    @unittest.skipUnless(have_alg('zstd'), "pg_probackup is not compiled with zstd support")
    def test_basic_compression_stream_zstd(self):
        self._test_compression_stream(compression = 'zstd')

    def _test_compression_stream(self, *, compression):
        """
        make archive node, make full and page stream backups,
        check data correctness in restored instance
        """
        self.maxDiff = None
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,256) i")
        full_result = node.table_checksum("t_heap")
        full_backup_id = self.pb.backup_node('node', node, backup_type='full',
            options=['--stream', f'--compress-algorithm={compression}'])

        # PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(256,512) i")
        page_result = node.table_checksum("t_heap")
        page_backup_id = self.pb.backup_node('node', node, backup_type='page',
            options=['--stream', f'--compress-algorithm={compression}'])

        # DELTA BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(512,768) i")
        delta_result = node.table_checksum("t_heap")
        delta_backup_id = self.pb.backup_node('node', node, backup_type='delta',
            options=['--stream', f'--compress-algorithm={compression}'])

        # Drop Node
        node.cleanup()

        # Check full backup
        restore_result = self.pb.restore_node('node', node, backup_id=full_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(full_backup_id))
        node.slow_start()

        full_result_new = node.table_checksum("t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check page backup
        restore_result = self.pb.restore_node('node', node, backup_id=page_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(page_backup_id))
        node.slow_start()

        page_result_new = node.table_checksum("t_heap")
        self.assertEqual(page_result, page_result_new)
        node.cleanup()

        # Check delta backup
        restore_result = self.pb.restore_node('node', node, backup_id=delta_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(delta_backup_id))
        node.slow_start()

        delta_result_new = node.table_checksum("t_heap")
        self.assertEqual(delta_result, delta_result_new)
        node.cleanup()

    def test_basic_compression_archive_pglz(self):
        self._test_compression_archive(compression = 'pglz')

    def test_basic_compression_archive_zlib(self):
        self._test_compression_archive(compression = 'zlib')

    @unittest.skipUnless(have_alg('lz4'), "pg_probackup is not compiled with lz4 support")
    def test_basic_compression_archive_lz4(self):
        self._test_compression_archive(compression = 'lz4')

    @unittest.skipUnless(have_alg('zstd'), "pg_probackup is not compiled with zstd support")
    def test_basic_compression_archive_zstd(self):
        self._test_compression_archive(compression = 'zstd')

    def _test_compression_archive(self, *, compression):
        """
        make archive node, make full and page backups,
        check data correctness in restored instance
        """
        self.maxDiff = None
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(0,100) i")
        full_result = node.table_checksum("t_heap")
        full_backup_id = self.pb.backup_node('node', node, backup_type='full',
            options=[f'--compress-algorithm={compression}'])

        # PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(100,200) i")
        page_result = node.table_checksum("t_heap")
        page_backup_id = self.pb.backup_node('node', node, backup_type='page',
            options=[f'--compress-algorithm={compression}'])

        # DELTA BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(200,300) i")
        delta_result = node.table_checksum("t_heap")
        delta_backup_id = self.pb.backup_node('node', node, backup_type='delta',
            options=[f'--compress-algorithm={compression}'])

        # Drop Node
        node.cleanup()

        # Check full backup
        restore_result = self.pb.restore_node('node', node, backup_id=full_backup_id,
            options=[
                "-j", "4", "--immediate",
                "--recovery-target-action=promote"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(full_backup_id))
        node.slow_start()

        full_result_new = node.table_checksum("t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check page backup
        restore_result = self.pb.restore_node('node', node, backup_id=page_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(page_backup_id))
        node.slow_start()

        page_result_new = node.table_checksum("t_heap")
        self.assertEqual(page_result, page_result_new)
        node.cleanup()

        # Check delta backup
        restore_result = self.pb.restore_node('node', node, backup_id=delta_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"])
        self.assertMessage(restore_result, contains="INFO: Restore of backup {0} completed.".format(delta_backup_id))
        node.slow_start()

        delta_result_new = node.table_checksum("t_heap")
        self.assertEqual(delta_result, delta_result_new)
        node.cleanup()

    def test_compression_wrong_algorithm(self):
        """
        make archive node, make full and page backups,
        check data correctness in restored instance
        """
        self.maxDiff = None
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node,
                backup_type='full', options=['--compress-algorithm=bla-blah'],
                expect_error="because compress-algorithm is invalid")
        self.assertMessage(contains='ERROR: Invalid compress algorithm value "bla-blah"')

    # @unittest.skip("skip")
    def test_incompressible_pages(self):
        """
        make archive node, create table with incompressible toast pages,
        take backup with compression, make sure that page was not compressed,
        restore backup and check data correctness
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # Full
        self.pb.backup_node('node', node,
            options=[
                '--compress-algorithm=zlib',
                '--compress-level=0'])

        node.pgbench_init(scale=3)

        self.pb.backup_node('node', node,
            backup_type='delta',
            options=[
                '--compress-algorithm=zlib',
                '--compress-level=0'])

        pgdata = self.pgdata_content(node.data_dir)

        node.cleanup()

        self.pb.restore_node('node', node=node)

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        node.slow_start()
