import unittest

from pg_probackup2.init_helpers import init_params
from .helpers.ptrack_helpers import ProbackupTest


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

    def test_compression_variant_alorithms_increment_chain(self):
        """
        If any algorithm isn't supported -> skip backup
        1. Full compressed [zlib, 3] backup -> change data
        2. Delta compressed [pglz, 5] -> change data
        3. Delta compressed [lz4, 9] -> change data
        4. Page compressed [zstd, 3] -> change data
        Restore and compare
        """

        # Initialize instance and backup directory
        node = self.pg_node.make_simple('node', set_replication=True, ptrack_enable=True)
        total_backups = 0

        self.pb.init()
        self.pb.add_instance("node", node)
        self.pb.set_archiving("node", node)
        node.slow_start()

        # Fill with data
        node.pgbench_init(scale=10)

        # Do pglz compressed FULL backup
        self.pb.backup_node("node", node, options=['--stream',
                                                            '--compress-level', '5',
                                                            '--compress-algorithm', 'pglz'])
        # Check backup
        show_backup = self.pb.show("node")[0]
        self.assertEqual(show_backup["status"], "OK")
        self.assertEqual(show_backup["backup-mode"], "FULL")

        # Do zlib compressed DELTA backup
        if have_alg('zlib'):
            total_backups += 1
            # Change data
            pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
            pgbench.wait()
            # Do backup
            self.pb.backup_node("node", node,
                backup_type="delta", options=['--compress-level', '3',
                                            '--compress-algorithm', 'zlib'])
            # Check backup
            show_backup = self.pb.show("node")[total_backups]
            self.assertEqual(show_backup["status"], "OK")
            self.assertEqual(show_backup["backup-mode"], "DELTA")

        # Do lz4 compressed DELTA backup
        if have_alg('lz4'):
            total_backups += 1
            # Change data
            pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
            pgbench.wait()
            # Do backup
            self.pb.backup_node("node", node,
                backup_type="delta", options=['--compress-level', '9',
                                            '--compress-algorithm', 'lz4'])
            # Check backup
            show_backup = self.pb.show("node")[total_backups]
            self.assertEqual(show_backup["status"], "OK")
            self.assertEqual(show_backup["backup-mode"], "DELTA")

        # Do zstd compressed PAGE backup
        if have_alg('zstd'):
            total_backups += 1
            # Change data
            pgbench = node.pgbench(options=['-T', '10', '-c', '1', '--no-vacuum'])
            pgbench.wait()
            # Do backup
            self.pb.backup_node("node", node,
                backup_type="page", options=['--compress-level', '3',
                                                                   '--compress-algorithm', 'zstd'])
            # Check backup
            show_backup = self.pb.show("node")[total_backups]
            self.assertEqual(show_backup["status"], "OK")
            self.assertEqual(show_backup["backup-mode"], "PAGE")

        pgdata = self.pgdata_content(node.data_dir)

        # Drop node and restore it
        node.cleanup()
        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        node.cleanup()
