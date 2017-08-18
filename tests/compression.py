import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException, idx_ptrack
from datetime import datetime, timedelta
import subprocess


module_name = 'compression'


class CompressionTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_compression_stream_zlib(self):
        """make archive node, make full and page stream backups, check data correctness in restored instance"""
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '30s', 'ptrack_enable': 'on'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")
        full_result = node.execute("postgres", "SELECT * FROM t_heap")
        full_backup_id = self.backup_node(backup_dir, 'node', node, backup_type='full', options=['--stream', '--compress-algorithm=zlib'])

        #PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(256,512) i")
        page_result = node.execute("postgres", "SELECT * FROM t_heap")
        page_backup_id = self.backup_node(backup_dir, 'node', node, backup_type='page', options=['--stream', '--compress-algorithm=zlib'])

        #PTRACK BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(512,768) i")
        ptrack_result = node.execute("postgres", "SELECT * FROM t_heap")
        ptrack_backup_id = self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=['--stream', '--compress-algorithm=zlib'])

        # Drop Node
        node.cleanup()

        # Check full backup
        self.assertIn("INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(backup_dir, 'node', node, backup_id=full_backup_id, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        node.start()
        full_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check page backup
        self.assertIn("INFO: Restore of backup {0} completed.".format(page_backup_id),
            self.restore_node(backup_dir, 'node', node, backup_id=page_backup_id, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        node.start()
        page_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(page_result, page_result_new)
        node.cleanup()

        # Check ptrack backup
        self.assertIn("INFO: Restore of backup {0} completed.".format(ptrack_backup_id),
            self.restore_node(backup_dir, 'node', node, backup_id=ptrack_backup_id, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        node.start()
        ptrack_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(ptrack_result, ptrack_result_new)
        node.cleanup()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_compression_archive_zlib(self):
        """make archive node, make full and page backups, check data correctness in restored instance"""
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '30s', 'ptrack_enable': 'on'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(0,1) i")
        full_result = node.execute("postgres", "SELECT * FROM t_heap")
        full_backup_id = self.backup_node(backup_dir, 'node', node, backup_type='full', options=['--compress-algorithm=zlib'])

        #PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(0,2) i")
        page_result = node.execute("postgres", "SELECT * FROM t_heap")
        page_backup_id = self.backup_node(backup_dir, 'node', node, backup_type='page', options=['--compress-algorithm=zlib'])

        #PTRACK BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(0,3) i")
        ptrack_result = node.execute("postgres", "SELECT * FROM t_heap")
        ptrack_backup_id = self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=['--compress-algorithm=zlib'])

        # Drop Node
        node.cleanup()

        # Check full backup
        self.assertIn("INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(backup_dir, 'node', node, backup_id=full_backup_id, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        node.start()
        full_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check page backup
        self.assertIn("INFO: Restore of backup {0} completed.".format(page_backup_id),
            self.restore_node(backup_dir, 'node', node, backup_id=page_backup_id, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        node.start()
        page_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(page_result, page_result_new)
        node.cleanup()

        # Check ptrack backup
        self.assertIn("INFO: Restore of backup {0} completed.".format(ptrack_backup_id),
            self.restore_node(backup_dir, 'node', node, backup_id=ptrack_backup_id, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        node.start()
        ptrack_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(ptrack_result, ptrack_result_new)
        node.cleanup()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_compression_stream_pglz(self):
        """make archive node, make full and page stream backups, check data correctness in restored instance"""
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '30s', 'ptrack_enable': 'on'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")
        full_result = node.execute("postgres", "SELECT * FROM t_heap")
        full_backup_id = self.backup_node(backup_dir, 'node', node, backup_type='full', options=['--stream', '--compress-algorithm=pglz'])

        #PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(256,512) i")
        page_result = node.execute("postgres", "SELECT * FROM t_heap")
        page_backup_id = self.backup_node(backup_dir, 'node', node, backup_type='page', options=['--stream', '--compress-algorithm=pglz'])

        #PTRACK BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(512,768) i")
        ptrack_result = node.execute("postgres", "SELECT * FROM t_heap")
        ptrack_backup_id = self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=['--stream', '--compress-algorithm=pglz'])

        # Drop Node
        node.cleanup()

        # Check full backup
        self.assertIn("INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(backup_dir, 'node', node, backup_id=full_backup_id, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        node.start()
        full_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check page backup
        self.assertIn("INFO: Restore of backup {0} completed.".format(page_backup_id),
            self.restore_node(backup_dir, 'node', node, backup_id=page_backup_id, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        node.start()
        page_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(page_result, page_result_new)
        node.cleanup()

        # Check ptrack backup
        self.assertIn("INFO: Restore of backup {0} completed.".format(ptrack_backup_id),
            self.restore_node(backup_dir, 'node', node, backup_id=ptrack_backup_id, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        node.start()
        ptrack_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(ptrack_result, ptrack_result_new)
        node.cleanup()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_compression_archive_pglz(self):
        """make archive node, make full and page backups, check data correctness in restored instance"""
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '30s', 'ptrack_enable': 'on'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(0,100) i")
        full_result = node.execute("postgres", "SELECT * FROM t_heap")
        full_backup_id = self.backup_node(backup_dir, 'node', node, backup_type='full', options=['--compress-algorithm=pglz'])

        #PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(100,200) i")
        page_result = node.execute("postgres", "SELECT * FROM t_heap")
        page_backup_id = self.backup_node(backup_dir, 'node', node, backup_type='page', options=['--compress-algorithm=pglz'])

        #PTRACK BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(200,300) i")
        ptrack_result = node.execute("postgres", "SELECT * FROM t_heap")
        ptrack_backup_id = self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=['--compress-algorithm=pglz'])

        # Drop Node
        node.cleanup()

        # Check full backup
        self.assertIn("INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(backup_dir, 'node', node, backup_id=full_backup_id, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        node.start()
        full_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check page backup
        self.assertIn("INFO: Restore of backup {0} completed.".format(page_backup_id),
            self.restore_node(backup_dir, 'node', node, backup_id=page_backup_id, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        node.start()
        page_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(page_result, page_result_new)
        node.cleanup()

        # Check ptrack backup
        self.assertIn("INFO: Restore of backup {0} completed.".format(ptrack_backup_id),
            self.restore_node(backup_dir, 'node', node, backup_id=ptrack_backup_id, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        node.start()
        ptrack_result_new = node.execute("postgres", "SELECT * FROM t_heap")
        self.assertEqual(ptrack_result, ptrack_result_new)
        node.cleanup()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_compression_wrong_algorithm(self):
        """make archive node, make full and page backups, check data correctness in restored instance"""
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '30s', 'ptrack_enable': 'on'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        try:
            self.backup_node(backup_dir, 'node', node, backup_type='full', options=['--compress-algorithm=bla-blah'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because restore destionation is not empty.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(e.message,
                'ERROR: invalid compress algorithm value "bla-blah"\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)
