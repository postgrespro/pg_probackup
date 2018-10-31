import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from datetime import datetime, timedelta
import subprocess
from sys import exit
import time


module_name = 'validate'


class ValidateTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_validate_wal_unreal_values(self):
        """
        make node with archiving, make archive backup
        validate to both real and unreal values
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        node.pgbench_init(scale=2)
        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        backup_id = self.backup_node(backup_dir, 'node', node)

        node.pgbench_init(scale=2)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )

        pgbench.wait()
        pgbench.stdout.close()

        target_time = self.show_pb(
            backup_dir, 'node', backup_id)['recovery-time']
        after_backup_time = datetime.now().replace(second=0, microsecond=0)

        # Validate to real time
        self.assertIn(
            "INFO: backup validation completed successfully",
            self.validate_pb(
                backup_dir, 'node',
                options=["--time={0}".format(target_time)]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                repr(self.output), self.cmd))

        # Validate to unreal time
        unreal_time_1 = after_backup_time - timedelta(days=2)
        try:
            self.validate_pb(
                backup_dir, 'node', options=["--time={0}".format(
                    unreal_time_1)])
            self.assertEqual(
                1, 0,
                "Expecting Error because of validation to unreal time.\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(
                e.message,
                'ERROR: Full backup satisfying target options is not found.\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Validate to unreal time #2
        unreal_time_2 = after_backup_time + timedelta(days=2)
        try:
            self.validate_pb(backup_dir, 'node', options=["--time={0}".format(unreal_time_2)])
            self.assertEqual(1, 0, "Expecting Error because of validation to unreal time.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue('ERROR: not enough WAL records to time' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Validate to real xid
        target_xid = None
        with node.connect("postgres") as con:
            res = con.execute("INSERT INTO tbl0005 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]
        self.switch_wal_segment(node)

        self.assertIn("INFO: backup validation completed successfully",
            self.validate_pb(backup_dir, 'node', options=["--xid={0}".format(target_xid)]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        # Validate to unreal xid
        unreal_xid = int(target_xid) + 1000
        try:
            self.validate_pb(backup_dir, 'node', options=["--xid={0}".format(unreal_xid)])
            self.assertEqual(1, 0, "Expecting Error because of validation to unreal xid.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue('ERROR: not enough WAL records to xid' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Validate with backup ID
        self.assertIn("INFO: Validating backup {0}".format(backup_id),
            self.validate_pb(backup_dir, 'node', backup_id),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        self.assertIn("INFO: Backup {0} data files are valid".format(backup_id),
            self.validate_pb(backup_dir, 'node', backup_id),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        self.assertIn("INFO: Backup {0} WAL segments are valid".format(backup_id),
            self.validate_pb(backup_dir, 'node', backup_id),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        self.assertIn("INFO: Backup {0} is valid".format(backup_id),
            self.validate_pb(backup_dir, 'node', backup_id),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        self.assertIn("INFO: Validate of backup {0} completed".format(backup_id),
            self.validate_pb(backup_dir, 'node', backup_id),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_validate_corrupted_intermediate_backup(self):
        """make archive node, take FULL, PAGE1, PAGE2 backups, corrupt file in PAGE1 backup,
        run validate on PAGE1, expect PAGE1 to gain status CORRUPT and PAGE2 get status ORPHAN"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # FULL
        backup_id_1 = self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,10000) i")
        file_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").rstrip()
        # PAGE1
        backup_id_2 = self.backup_node(backup_dir, 'node', node, backup_type='page')

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(10000,20000) i")
        # PAGE2
        backup_id_3 = self.backup_node(backup_dir, 'node', node, backup_type='page')

        # Corrupt some file
        file = os.path.join(backup_dir, 'backups/node', backup_id_2, 'database', file_path)
        with open(file, "rb+", 0) as f:
            f.seek(42)
            f.write(b"blah")
            f.flush()
            f.close

        # Simple validate
        try:
            self.validate_pb(backup_dir, 'node', backup_id=backup_id_2,
                options=['--log-level-file=verbose'])
            self.assertEqual(1, 0, "Expecting Error because of data files corruption.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'INFO: Validating parents for backup {0}'.format(backup_id_2) in e.message
                and 'ERROR: Backup {0} is corrupt'.format(backup_id_2) in e.message,
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        self.assertEqual('CORRUPT', self.show_pb(backup_dir, 'node', backup_id_2)['status'], 'Backup STATUS should be "CORRUPT"')
        self.assertEqual('ORPHAN', self.show_pb(backup_dir, 'node', backup_id_3)['status'], 'Backup STATUS should be "ORPHAN"')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_validate_corrupted_intermediate_backups(self):
        """make archive node, take FULL, PAGE1, PAGE2 backups,
        corrupt file in FULL and PAGE1 backupd, run validate  on PAGE1,
        expect FULL and PAGE1 to gain status CORRUPT and PAGE2 get status ORPHAN"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,10000) i")
        file_path_t_heap = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").rstrip()
        # FULL
        backup_id_1 = self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "create table t_heap_1 as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,10000) i")
        file_path_t_heap_1 = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap_1')").rstrip()
        # PAGE1
        backup_id_2 = self.backup_node(backup_dir, 'node', node, backup_type='page')

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(20000,30000) i")
        # PAGE2
        backup_id_3 = self.backup_node(backup_dir, 'node', node, backup_type='page')

        # Corrupt some file in FULL backup
        file_full = os.path.join(backup_dir, 'backups/node', backup_id_1, 'database', file_path_t_heap)
        with open(file_full, "rb+", 0) as f:
            f.seek(84)
            f.write(b"blah")
            f.flush()
            f.close

        # Corrupt some file in PAGE1 backup
        file_page1 = os.path.join(backup_dir, 'backups/node', backup_id_2, 'database', file_path_t_heap_1)
        with open(file_page1, "rb+", 0) as f:
            f.seek(42)
            f.write(b"blah")
            f.flush()
            f.close

        # Validate PAGE1
        try:
            self.validate_pb(backup_dir, 'node', backup_id=backup_id_2,
                options=['--log-level-file=verbose'])
            self.assertEqual(1, 0, "Expecting Error because of data files corruption.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue('INFO: Validating parents for backup {0}'.format(backup_id_2) in e.message,
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))
            self.assertTrue(
                'INFO: Validating backup {0}'.format(backup_id_1) in e.message
                and 'WARNING: Invalid CRC of backup file "{0}"'.format(file_full) in e.message
                and 'WARNING: Backup {0} data files are corrupted'.format(backup_id_1) in e.message,
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))
            self.assertTrue(
                'WARNING: Backup {0} is orphaned because his parent'.format(backup_id_2) in e.message
                and 'WARNING: Backup {0} is orphaned because his parent'.format(backup_id_3) in e.message
                and 'ERROR: Backup {0} is orphan.'.format(backup_id_2) in e.message,
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        self.assertEqual('CORRUPT', self.show_pb(backup_dir, 'node', backup_id_1)['status'], 'Backup STATUS should be "CORRUPT"')
        self.assertEqual('ORPHAN', self.show_pb(backup_dir, 'node', backup_id_2)['status'], 'Backup STATUS should be "ORPHAN"')
        self.assertEqual('ORPHAN', self.show_pb(backup_dir, 'node', backup_id_3)['status'], 'Backup STATUS should be "ORPHAN"')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_validate_corrupted_intermediate_backups_1(self):
        """make archive node, take FULL1, PAGE1, PAGE2, PAGE3, PAGE4, PAGE5, FULL2 backups,
        corrupt file in PAGE1 and PAGE4, run validate on PAGE3,
        expect PAGE1 to gain status CORRUPT, PAGE2, PAGE3, PAGE4 and PAGE5 to gain status ORPHAN"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # FULL1
        backup_id_1 = self.backup_node(backup_dir, 'node', node)

        # PAGE1
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")
        backup_id_2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGE2
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")
        file_page_2 = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").rstrip()
        backup_id_3 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGE3
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(10000,20000) i")
        backup_id_4 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGE4
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(20000,30000) i")
        backup_id_5 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGE5
        node.safe_psql(
            "postgres",
            "create table t_heap1 as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")
        file_page_5 = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap1')").rstrip()
        backup_id_6 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # PAGE6
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(30000,40000) i")
        backup_id_7 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # FULL2
        backup_id_8 = self.backup_node(backup_dir, 'node', node)

        # Corrupt some file in PAGE2 and PAGE5 backups
        file_page1 = os.path.join(
            backup_dir, 'backups/node', backup_id_3, 'database', file_page_2)
        with open(file_page1, "rb+", 0) as f:
            f.seek(84)
            f.write(b"blah")
            f.flush()
            f.close

        file_page4 = os.path.join(
            backup_dir, 'backups/node', backup_id_6, 'database', file_page_5)
        with open(file_page4, "rb+", 0) as f:
            f.seek(42)
            f.write(b"blah")
            f.flush()
            f.close

        # Validate PAGE3
        try:
            self.validate_pb(
                backup_dir, 'node',
                backup_id=backup_id_4,
                options=['--log-level-file=verbose'])
            self.assertEqual(
                1, 0,
                "Expecting Error because of data files corruption.\n"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'INFO: Validating parents for backup {0}'.format(
                    backup_id_4) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertTrue(
                'INFO: Validating backup {0}'.format(
                    backup_id_1) in e.message and
                'INFO: Backup {0} data files are valid'.format(
                    backup_id_1) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertTrue(
                'INFO: Validating backup {0}'.format(
                    backup_id_2) in e.message and
                'INFO: Backup {0} data files are valid'.format(
                    backup_id_2) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertTrue(
                'INFO: Validating backup {0}'.format(
                    backup_id_3) in e.message and
                'WARNING: Invalid CRC of backup file "{0}"'.format(
                    file_page1) in e.message and
                'WARNING: Backup {0} data files are corrupted'.format(
                    backup_id_3) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertTrue(
                'WARNING: Backup {0} is orphaned because '
                'his parent {1} is corrupted'.format(
                    backup_id_4, backup_id_3) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertTrue(
                'WARNING: Backup {0} is orphaned because '
                'his parent {1} is corrupted'.format(
                    backup_id_5, backup_id_3) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertTrue(
                'WARNING: Backup {0} is orphaned because '
                'his parent {1} is corrupted'.format(
                    backup_id_6, backup_id_3) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertTrue(
                'WARNING: Backup {0} is orphaned because '
                'his parent {1} is corrupted'.format(
                    backup_id_7, backup_id_3) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertTrue(
                'ERROR: Backup {0} is orphan'.format(backup_id_4) in e.message,
                '\n Unexpected Error Message: {0}\n '
                'CMD: {1}'.format(repr(e.message), self.cmd))

        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'node', backup_id_1)['status'],
            'Backup STATUS should be "OK"')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'node', backup_id_2)['status'],
            'Backup STATUS should be "OK"')
        self.assertEqual(
            'CORRUPT', self.show_pb(backup_dir, 'node', backup_id_3)['status'],
            'Backup STATUS should be "CORRUPT"')
        self.assertEqual(
            'ORPHAN', self.show_pb(backup_dir, 'node', backup_id_4)['status'],
            'Backup STATUS should be "ORPHAN"')
        self.assertEqual(
            'ORPHAN', self.show_pb(backup_dir, 'node', backup_id_5)['status'],
            'Backup STATUS should be "ORPHAN"')
        self.assertEqual(
            'ORPHAN', self.show_pb(backup_dir, 'node', backup_id_6)['status'],
            'Backup STATUS should be "ORPHAN"')
        self.assertEqual(
            'ORPHAN', self.show_pb(backup_dir, 'node', backup_id_7)['status'],
            'Backup STATUS should be "ORPHAN"')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'node', backup_id_8)['status'],
            'Backup STATUS should be "OK"')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_validate_specific_target_corrupted_intermediate_backups(self):
        """make archive node, take FULL1, PAGE1, PAGE2, PAGE3, PAGE4, PAGE5, FULL2 backups,
        corrupt file in PAGE1 and PAGE4, run validate on PAGE3 to specific xid,
        expect PAGE1 to gain status CORRUPT, PAGE2, PAGE3, PAGE4 and PAGE5 to gain status ORPHAN"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # FULL1
        backup_id_1 = self.backup_node(backup_dir, 'node', node)

        # PAGE1
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,10000) i")
        backup_id_2 = self.backup_node(backup_dir, 'node', node, backup_type='page')

        # PAGE2
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,10000) i")
        file_page_2 = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").rstrip()
        backup_id_3 = self.backup_node(backup_dir, 'node', node, backup_type='page')

        # PAGE3
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(10000,20000) i")
        backup_id_4 = self.backup_node(backup_dir, 'node', node, backup_type='page')

        # PAGE4
        target_xid = node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(20000,30000) i  RETURNING (xmin)")[0][0]
        backup_id_5 = self.backup_node(backup_dir, 'node', node, backup_type='page')

         # PAGE5
        node.safe_psql(
            "postgres",
            "create table t_heap1 as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,10000) i")
        file_page_5 = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap1')").rstrip()
        backup_id_6 = self.backup_node(backup_dir, 'node', node, backup_type='page')

        # PAGE6
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(30000,40000) i")
        backup_id_7 = self.backup_node(backup_dir, 'node', node, backup_type='page')

        # FULL2
        backup_id_8 = self.backup_node(backup_dir, 'node', node)

        # Corrupt some file in PAGE2 and PAGE5 backups
        file_page1 = os.path.join(backup_dir, 'backups/node', backup_id_3, 'database', file_page_2)
        with open(file_page1, "rb+", 0) as f:
            f.seek(84)
            f.write(b"blah")
            f.flush()
            f.close

        file_page4 = os.path.join(backup_dir, 'backups/node', backup_id_6, 'database', file_page_5)
        with open(file_page4, "rb+", 0) as f:
            f.seek(42)
            f.write(b"blah")
            f.flush()
            f.close

        # Validate PAGE3
        try:
            self.validate_pb(backup_dir, 'node',
                options=['--log-level-file=verbose', '-i', backup_id_4, '--xid={0}'.format(target_xid)])
            self.assertEqual(1, 0, "Expecting Error because of data files corruption.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'INFO: Validating parents for backup {0}'.format(backup_id_4) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))
            self.assertTrue(
                'INFO: Validating backup {0}'.format(backup_id_1) in e.message
                and 'INFO: Backup {0} data files are valid'.format(backup_id_1) in e.message,
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))
            self.assertTrue(
                'INFO: Validating backup {0}'.format(backup_id_2) in e.message
                and 'INFO: Backup {0} data files are valid'.format(backup_id_2) in e.message,
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))
            self.assertTrue(
                'INFO: Validating backup {0}'.format(backup_id_3) in e.message
                and 'WARNING: Invalid CRC of backup file "{0}"'.format(file_page1) in e.message
                and 'WARNING: Backup {0} data files are corrupted'.format(backup_id_3) in e.message,
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))
            self.assertTrue(
                'WARNING: Backup {0} is orphaned because his parent {1} is corrupted'.format(backup_id_4, backup_id_3) in e.message,
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))
            self.assertTrue(
                'WARNING: Backup {0} is orphaned because his parent {1} is corrupted'.format(backup_id_5, backup_id_3) in e.message,
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))
            self.assertTrue(
                'WARNING: Backup {0} is orphaned because his parent {1} is corrupted'.format(backup_id_6, backup_id_3) in e.message,
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))
            self.assertTrue(
                'WARNING: Backup {0} is orphaned because his parent {1} is corrupted'.format(backup_id_7, backup_id_3) in e.message,
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))
            self.assertTrue(
                'ERROR: Backup {0} is orphan'.format(backup_id_4) in e.message,
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        self.assertEqual('OK', self.show_pb(backup_dir, 'node', backup_id_1)['status'], 'Backup STATUS should be "OK"')
        self.assertEqual('OK', self.show_pb(backup_dir, 'node', backup_id_2)['status'], 'Backup STATUS should be "OK"')
        self.assertEqual('CORRUPT', self.show_pb(backup_dir, 'node', backup_id_3)['status'], 'Backup STATUS should be "CORRUPT"')
        self.assertEqual('ORPHAN', self.show_pb(backup_dir, 'node', backup_id_4)['status'], 'Backup STATUS should be "ORPHAN"')
        self.assertEqual('ORPHAN', self.show_pb(backup_dir, 'node', backup_id_5)['status'], 'Backup STATUS should be "ORPHAN"')
        self.assertEqual('ORPHAN', self.show_pb(backup_dir, 'node', backup_id_6)['status'], 'Backup STATUS should be "ORPHAN"')
        self.assertEqual('ORPHAN', self.show_pb(backup_dir, 'node', backup_id_7)['status'], 'Backup STATUS should be "ORPHAN"')
        self.assertEqual('OK', self.show_pb(backup_dir, 'node', backup_id_8)['status'], 'Backup STATUS should be "OK"')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_validate_instance_with_corrupted_page(self):
        """make archive node, take FULL, PAGE1, PAGE2, FULL2, PAGE3 backups,
        corrupt file in PAGE1 backup and run validate on instance,
        expect PAGE1 to gain status CORRUPT, PAGE2 to gain status ORPHAN"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")
        # FULL1
        backup_id_1 = self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "create table t_heap1 as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")
        file_path_t_heap1 = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap1')").rstrip()
        # PAGE1
        backup_id_2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(20000,30000) i")
        # PAGE2
        backup_id_3 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')
        # FULL1
        backup_id_4 = self.backup_node(
            backup_dir, 'node', node)
        # PAGE3
        backup_id_5 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Corrupt some file in FULL backup
        file_full = os.path.join(
            backup_dir, 'backups/node', backup_id_2,
            'database', file_path_t_heap1)
        with open(file_full, "rb+", 0) as f:
            f.seek(84)
            f.write(b"blah")
            f.flush()
            f.close

        # Validate Instance
        try:
            self.validate_pb(
                backup_dir, 'node', options=['--log-level-file=verbose'])
            self.assertEqual(
                1, 0,
                "Expecting Error because of data files corruption.\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                "INFO: Validate backups of the instance 'node'" in e.message,
                "\n Unexpected Error Message: {0}\n "
                "CMD: {1}".format(repr(e.message), self.cmd))
            self.assertTrue(
                'INFO: Validating backup {0}'.format(
                    backup_id_5) in e.message and
                'INFO: Backup {0} data files are valid'.format(
                    backup_id_5) in e.message and
                'INFO: Backup {0} WAL segments are valid'.format(
                    backup_id_5) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertTrue(
                'INFO: Validating backup {0}'.format(
                    backup_id_4) in e.message and
                'INFO: Backup {0} data files are valid'.format(
                    backup_id_4) in e.message and
                'INFO: Backup {0} WAL segments are valid'.format(
                    backup_id_4) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertTrue(
                'INFO: Validating backup {0}'.format(
                    backup_id_3) in e.message and
                'INFO: Backup {0} data files are valid'.format(
                    backup_id_3) in e.message and
                'INFO: Backup {0} WAL segments are valid'.format(
                    backup_id_3) in e.message and
                'WARNING: Backup {0} is orphaned because '
                'his parent {1} is corrupted'.format(
                    backup_id_3, backup_id_2) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertTrue(
                'INFO: Validating backup {0}'.format(
                    backup_id_2) in e.message and
                'WARNING: Invalid CRC of backup file "{0}"'.format(
                    file_full) in e.message and
                'WARNING: Backup {0} data files are corrupted'.format(
                    backup_id_2) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertTrue(
                'INFO: Validating backup {0}'.format(
                    backup_id_1) in e.message and
                'INFO: Backup {0} data files are valid'.format(
                    backup_id_1) in e.message and
                'INFO: Backup {0} WAL segments are valid'.format(
                    backup_id_1) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertTrue(
                'WARNING: Some backups are not valid' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'node', backup_id_1)['status'],
            'Backup STATUS should be "OK"')
        self.assertEqual(
            'CORRUPT', self.show_pb(backup_dir, 'node', backup_id_2)['status'],
            'Backup STATUS should be "CORRUPT"')
        self.assertEqual(
            'ORPHAN', self.show_pb(backup_dir, 'node', backup_id_3)['status'],
            'Backup STATUS should be "ORPHAN"')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'node', backup_id_4)['status'],
            'Backup STATUS should be "OK"')
        self.assertEqual(
            'OK', self.show_pb(backup_dir, 'node', backup_id_5)['status'],
            'Backup STATUS should be "OK"')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_validate_instance_with_corrupted_full_and_try_restore(self):
        """make archive node, take FULL, PAGE1, PAGE2, FULL2, PAGE3 backups,
        corrupt file in FULL backup and run validate on instance,
        expect FULL to gain status CORRUPT, PAGE1 and PAGE2 to gain status ORPHAN,
        try to restore backup with --no-validation option"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,10000) i")
        file_path_t_heap = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").rstrip()
        # FULL1
        backup_id_1 = self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "insert into t_heap     select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,10000) i")
        # PAGE1
        backup_id_2 = self.backup_node(backup_dir, 'node', node, backup_type='page')

        # PAGE2
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(20000,30000) i")
        backup_id_3 = self.backup_node(backup_dir, 'node', node, backup_type='page')

        # FULL1
        backup_id_4 = self.backup_node(backup_dir, 'node', node)

        # PAGE3
        node.safe_psql(
            "postgres",
            "insert into t_heap     select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(30000,40000) i")
        backup_id_5 = self.backup_node(backup_dir, 'node', node, backup_type='page')

        # Corrupt some file in FULL backup
        file_full = os.path.join(backup_dir, 'backups/node', backup_id_1, 'database', file_path_t_heap)
        with open(file_full, "rb+", 0) as f:
            f.seek(84)
            f.write(b"blah")
            f.flush()
            f.close

        # Validate Instance
        try:
            self.validate_pb(backup_dir, 'node', options=['--log-level-file=verbose'])
            self.assertEqual(1, 0, "Expecting Error because of data files corruption.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'INFO: Validating backup {0}'.format(backup_id_1) in e.message
                and "INFO: Validate backups of the instance 'node'" in e.message
                and 'WARNING: Invalid CRC of backup file "{0}"'.format(file_full) in e.message
                and 'WARNING: Backup {0} data files are corrupted'.format(backup_id_1) in e.message,
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        self.assertEqual('CORRUPT', self.show_pb(backup_dir, 'node', backup_id_1)['status'], 'Backup STATUS should be "CORRUPT"')
        self.assertEqual('ORPHAN', self.show_pb(backup_dir, 'node', backup_id_2)['status'], 'Backup STATUS should be "ORPHAN"')
        self.assertEqual('ORPHAN', self.show_pb(backup_dir, 'node', backup_id_3)['status'], 'Backup STATUS should be "ORPHAN"')
        self.assertEqual('OK', self.show_pb(backup_dir, 'node', backup_id_4)['status'], 'Backup STATUS should be "OK"')
        self.assertEqual('OK', self.show_pb(backup_dir, 'node', backup_id_5)['status'], 'Backup STATUS should be "OK"')

        node.cleanup()
        restore_out = self.restore_node(
                backup_dir, 'node', node,
                options=["--no-validate"])
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(backup_id_5),
            restore_out,
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                repr(self.output), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_validate_instance_with_corrupted_full(self):
        """make archive node, take FULL, PAGE1, PAGE2, FULL2, PAGE3 backups,
        corrupt file in FULL backup and run validate on instance,
        expect FULL to gain status CORRUPT, PAGE1 and PAGE2 to gain status ORPHAN"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,10000) i")
        file_path_t_heap = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").rstrip()
        # FULL1
        backup_id_1 = self.backup_node(backup_dir, 'node', node)

        node.safe_psql(
            "postgres",
            "insert into t_heap     select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,10000) i")
        # PAGE1
        backup_id_2 = self.backup_node(backup_dir, 'node', node, backup_type='page')

        # PAGE2
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(20000,30000) i")
        backup_id_3 = self.backup_node(backup_dir, 'node', node, backup_type='page')

        # FULL1
        backup_id_4 = self.backup_node(backup_dir, 'node', node)

        # PAGE3
        node.safe_psql(
            "postgres",
            "insert into t_heap     select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(30000,40000) i")
        backup_id_5 = self.backup_node(backup_dir, 'node', node, backup_type='page')

        # Corrupt some file in FULL backup
        file_full = os.path.join(backup_dir, 'backups/node', backup_id_1, 'database', file_path_t_heap)
        with open(file_full, "rb+", 0) as f:
            f.seek(84)
            f.write(b"blah")
            f.flush()
            f.close

        # Validate Instance
        try:
            self.validate_pb(backup_dir, 'node', options=['--log-level-file=verbose'])
            self.assertEqual(1, 0, "Expecting Error because of data files corruption.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'INFO: Validating backup {0}'.format(backup_id_1) in e.message
                and "INFO: Validate backups of the instance 'node'" in e.message
                and 'WARNING: Invalid CRC of backup file "{0}"'.format(file_full) in e.message
                and 'WARNING: Backup {0} data files are corrupted'.format(backup_id_1) in e.message,
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        self.assertEqual('CORRUPT', self.show_pb(backup_dir, 'node', backup_id_1)['status'], 'Backup STATUS should be "CORRUPT"')
        self.assertEqual('ORPHAN', self.show_pb(backup_dir, 'node', backup_id_2)['status'], 'Backup STATUS should be "ORPHAN"')
        self.assertEqual('ORPHAN', self.show_pb(backup_dir, 'node', backup_id_3)['status'], 'Backup STATUS should be "ORPHAN"')
        self.assertEqual('OK', self.show_pb(backup_dir, 'node', backup_id_4)['status'], 'Backup STATUS should be "OK"')
        self.assertEqual('OK', self.show_pb(backup_dir, 'node', backup_id_5)['status'], 'Backup STATUS should be "OK"')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_validate_corrupt_wal_1(self):
        """make archive node, take FULL1, PAGE1,PAGE2,FULL2,PAGE3,PAGE4 backups, corrupt all wal files, run validate, expect errors"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        backup_id_1 = self.backup_node(backup_dir, 'node', node)

        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        backup_id_2 = self.backup_node(backup_dir, 'node', node)

        # Corrupt WAL
        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        wals.sort()
        for wal in wals:
            with open(os.path.join(wals_dir, wal), "rb+", 0) as f:
                f.seek(42)
                f.write(b"blablablaadssaaaaaaaaaaaaaaa")
                f.flush()
                f.close

        # Simple validate
        try:
            self.validate_pb(backup_dir, 'node')
            self.assertEqual(
                1, 0,
                "Expecting Error because of wal segments corruption.\n"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'WARNING: Backup' in e.message and
                'WAL segments are corrupted' in e.message and
                "WARNING: There are not enough WAL "
                "records to consistenly restore backup" in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertEqual(
            'CORRUPT',
            self.show_pb(backup_dir, 'node', backup_id_1)['status'],
            'Backup STATUS should be "CORRUPT"')
        self.assertEqual(
            'CORRUPT',
            self.show_pb(backup_dir, 'node', backup_id_2)['status'],
            'Backup STATUS should be "CORRUPT"')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_validate_corrupt_wal_2(self):
        """make archive node, make full backup, corrupt all wal files, run validate to real xid, expect errors"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        backup_id = self.backup_node(backup_dir, 'node', node)
        target_xid = None
        with node.connect("postgres") as con:
            res = con.execute(
                "INSERT INTO tbl0005 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]

        # Corrupt WAL
        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        wals.sort()
        for wal in wals:
            with open(os.path.join(wals_dir, wal), "rb+", 0) as f:
                f.seek(128)
                f.write(b"blablablaadssaaaaaaaaaaaaaaa")
                f.flush()
                f.close

        # Validate to xid
        try:
            self.validate_pb(
                backup_dir,
                'node',
                backup_id,
                options=[
                    "--log-level-console=verbose",
                    "--xid={0}".format(target_xid)])
            self.assertEqual(
                1, 0,
                "Expecting Error because of wal segments corruption.\n"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'WARNING: Backup' in e.message and
                'WAL segments are corrupted' in e.message and
                "WARNING: There are not enough WAL "
                "records to consistenly restore backup" in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertEqual(
            'CORRUPT',
            self.show_pb(backup_dir, 'node', backup_id)['status'],
            'Backup STATUS should be "CORRUPT"')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_validate_wal_lost_segment_1(self):
        """make archive node, make archive full backup,
        delete from archive wal segment which belong to previous backup
        run validate, expecting error because of missing wal segment
        make sure that backup status is 'CORRUPT'
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        node.pgbench_init(scale=2)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )
        pgbench.wait()
        pgbench.stdout.close()
        backup_id = self.backup_node(backup_dir, 'node', node)

        # Delete wal segment
        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        wals.sort()
        file = os.path.join(backup_dir, 'wal', 'node', wals[-1])
        os.remove(file)

        # cut out '.gz'
        if self.archive_compress:
            file = file[:-3]

        try:
            self.validate_pb(backup_dir, 'node')
            self.assertEqual(
                1, 0,
                "Expecting Error because of wal segment disappearance.\n"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                "WARNING: WAL segment \"{0}\" is absent".format(
                    file) in e.message and
                "WARNING: There are not enough WAL records to consistenly "
                "restore backup {0}".format(backup_id) in e.message and
                "WARNING: Backup {0} WAL segments are corrupted".format(
                    backup_id) in e.message and
                "WARNING: Some backups are not valid" in e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        self.assertEqual(
            'CORRUPT',
            self.show_pb(backup_dir, 'node', backup_id)['status'],
            'Backup {0} should have STATUS "CORRUPT"')

        # Run validate again
        try:
            self.validate_pb(backup_dir, 'node', backup_id)
            self.assertEqual(
                1, 0,
                "Expecting Error because of backup corruption.\n"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'INFO: Revalidating backup {0}'.format(backup_id), e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertIn(
                'ERROR: Backup {0} is corrupt.'.format(backup_id), e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_validate_corrupt_wal_between_backups(self):
        """
        make archive node, make full backup, corrupt all wal files,
        run validate to real xid, expect errors
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        backup_id = self.backup_node(backup_dir, 'node', node)

        # make some wals
        node.pgbench_init(scale=2)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        with node.connect("postgres") as con:
            res = con.execute(
                "INSERT INTO tbl0005 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]

        if self.get_version(node) < self.version_to_num('10.0'):
            walfile = node.safe_psql(
                'postgres',
                'select pg_xlogfile_name(pg_current_xlog_location())').rstrip()
        else:
            walfile = node.safe_psql(
                'postgres',
                'select pg_walfile_name(pg_current_wal_lsn())').rstrip()

        if self.archive_compress:
            walfile = walfile + '.gz'
        self.switch_wal_segment(node)

        # generate some wals
        node.pgbench_init(scale=2)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(backup_dir, 'node', node)

        # Corrupt WAL
        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        with open(os.path.join(wals_dir, walfile), "rb+", 0) as f:
            f.seek(9000)
            f.write(b"b")
            f.flush()
            f.close

        # Validate to xid
        try:
            self.validate_pb(
                backup_dir,
                'node',
                backup_id,
                options=[
                    "--log-level-console=verbose",
                    "--xid={0}".format(target_xid)])
            self.assertEqual(
                1, 0,
                "Expecting Error because of wal segments corruption.\n"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'ERROR: not enough WAL records to xid' in e.message and
                'WARNING: recovery can be done up to time' in e.message and
                "ERROR: not enough WAL records to xid {0}\n".format(
                    target_xid),
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertEqual(
            'OK',
            self.show_pb(backup_dir, 'node')[0]['status'],
            'Backup STATUS should be "OK"')

        self.assertEqual(
            'OK',
            self.show_pb(backup_dir, 'node')[1]['status'],
            'Backup STATUS should be "OK"')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_validate_wal_lost_segment_2(self):
        """
        make node with archiving
        make archive backup
        delete from archive wal segment which DO NOT belong to this backup
        run validate, expecting error because of missing wal segment
        make sure that backup status is 'ERROR'
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        self.backup_node(backup_dir, 'node', node)

        # make some wals
        node.pgbench_init(scale=2)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        # delete last wal segment
        wals_dir = os.path.join(backup_dir, 'wal', 'node')
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(
            wals_dir, f)) and not f.endswith('.backup')]
        wals = map(str, wals)
        file = os.path.join(wals_dir, max(wals))
        os.remove(file)
        if self.archive_compress:
            file = file[:-3]

        # Try to restore
        try:
            backup_id = self.backup_node(
                backup_dir, 'node', node, backup_type='page')
            self.assertEqual(
                1, 0,
                "Expecting Error because of wal segment disappearance.\n "
                "Output: {0} \n CMD: {1}".format(
                    self.output, self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'INFO: Wait for LSN' in e.message and
                'in archived WAL segment' in e.message and
                'WARNING: could not read WAL record at' in e.message and
                'ERROR: WAL segment "{0}" is absent\n'.format(
                    file) in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertEqual(
            'ERROR',
            self.show_pb(backup_dir, 'node')[1]['status'],
            'Backup {0} should have STATUS "ERROR"')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_pgpro702_688(self):
        """make node without archiving, make stream backup, get Recovery Time, validate to Recovery Time"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.start()

        backup_id = self.backup_node(
            backup_dir, 'node', node, options=["--stream"])
        recovery_time = self.show_pb(
            backup_dir, 'node', backup_id=backup_id)['recovery-time']

        try:
            self.validate_pb(
                backup_dir, 'node',
                options=["--time={0}".format(recovery_time)])
            self.assertEqual(
                1, 0,
                "Expecting Error because of wal segment disappearance.\n "
                "Output: {0} \n CMD: {1}".format(
                    self.output, self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'WAL archive is empty. You cannot restore backup to a '
                'recovery target without WAL archive', e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_pgpro688(self):
        """make node with archiving, make backup, get Recovery Time, validate to Recovery Time. Waiting PGPRO-688. RESOLVED"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        backup_id = self.backup_node(backup_dir, 'node', node)
        recovery_time = self.show_pb(backup_dir, 'node', backup_id)['recovery-time']

        self.validate_pb(backup_dir, 'node', options=["--time={0}".format(recovery_time)])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_pgpro561(self):
        """
        make node with archiving, make stream backup,
        restore it to node1, check that archiving is not successful on node1
        """
        fname = self.id().split('.')[3]
        node1 = self.make_simple_node(
            base_dir="{0}/{1}/node1".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node1', node1)
        self.set_archiving(backup_dir, 'node1', node1)
        node1.start()

        backup_id = self.backup_node(
            backup_dir, 'node1', node1, options=["--stream"])

        node2 = self.make_simple_node(
            base_dir="{0}/{1}/node2".format(module_name, fname))
        node2.cleanup()

        node1.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,256) i")

        self.backup_node(
            backup_dir, 'node1', node1,
            backup_type='page', options=["--stream"])
        self.restore_node(backup_dir, 'node1', data_dir=node2.data_dir)
        node2.append_conf(
            'postgresql.auto.conf', 'port = {0}'.format(node2.port))
        node2.slow_start()

        timeline_node1 = node1.get_control_data()["Latest checkpoint's TimeLineID"]
        timeline_node2 = node2.get_control_data()["Latest checkpoint's TimeLineID"]
        self.assertEqual(
            timeline_node1, timeline_node2,
            "Timelines on Master and Node1 should be equal. "
            "This is unexpected")

        archive_command_node1 = node1.safe_psql(
            "postgres", "show archive_command")
        archive_command_node2 = node2.safe_psql(
            "postgres", "show archive_command")
        self.assertEqual(
            archive_command_node1, archive_command_node2,
            "Archive command on Master and Node should be equal. "
            "This is unexpected")

        # result = node2.safe_psql("postgres", "select last_failed_wal from pg_stat_get_archiver() where last_failed_wal is not NULL")
        ## self.assertEqual(res, six.b(""), 'Restored Node1 failed to archive segment {0} due to having the same archive command as Master'.format(res.rstrip()))
        # if result == "":
        #    self.assertEqual(1, 0, 'Error is expected due to Master and Node1 having the common archive and archive_command')

        self.switch_wal_segment(node1)
        self.switch_wal_segment(node2)
        time.sleep(5)

        log_file = os.path.join(node2.logs_dir, 'postgresql.log')
        with open(log_file, 'r') as f:
            log_content = f.read()
            self.assertTrue(
                'LOG:  archive command failed with exit code 1' in log_content and
                'DETAIL:  The failed archive command was:' in log_content and
                'INFO: pg_probackup archive-push from' in log_content,
                'Expecting error messages about failed archive_command'
            )
            self.assertFalse(
                'pg_probackup archive-push completed successfully' in log_content)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_validate_corrupted_full(self):
        """
        make node with archiving, take full backup, and three page backups,
        take another full backup and three page backups
        corrupt second full backup, run validate, check that
        second full backup became CORRUPT and his page backups are ORPHANs
        remove corruption and run valudate again, check that
        second full backup and his page backups are OK
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        self.backup_node(backup_dir, 'node', node)
        self.backup_node(backup_dir, 'node', node, backup_type='page')
        self.backup_node(backup_dir, 'node', node, backup_type='page')

        backup_id = self.backup_node(backup_dir, 'node', node)
        self.backup_node(backup_dir, 'node', node, backup_type='page')
        self.backup_node(backup_dir, 'node', node, backup_type='page')

        node.safe_psql(
            "postgres",
            "alter system set archive_command = 'false'")
        node.reload()
        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='page', options=['--archive-timeout=1s'])
            self.assertEqual(
                1, 0,
                "Expecting Error because of data file dissapearance.\n "
                "Output: {0} \n CMD: {1}".format(
                    self.output, self.cmd))
        except ProbackupException as e:
            pass
        self.assertTrue(
            self.show_pb(backup_dir, 'node')[6]['status'] == 'ERROR')
        self.set_archiving(backup_dir, 'node', node)
        node.reload()
        self.backup_node(backup_dir, 'node', node, backup_type='page')

        file = os.path.join(
            backup_dir, 'backups', 'node',
            backup_id, 'database', 'postgresql.auto.conf')

        file_new = os.path.join(backup_dir, 'postgresql.auto.conf')
        os.rename(file, file_new)

        try:
            self.validate_pb(backup_dir)
            self.assertEqual(
                1, 0,
                "Expecting Error because of data file dissapearance.\n "
                "Output: {0} \n CMD: {1}".format(
                    self.output, self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'Validating backup {0}'.format(backup_id), e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertIn(
                'WARNING: Backup {0} data files are corrupted'.format(
                    backup_id), e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertIn(
                'WARNING: Some backups are not valid'.format(
                    backup_id), e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertTrue(self.show_pb(backup_dir, 'node')[0]['status'] == 'OK')
        self.assertTrue(self.show_pb(backup_dir, 'node')[1]['status'] == 'OK')
        self.assertTrue(self.show_pb(backup_dir, 'node')[2]['status'] == 'OK')
        self.assertTrue(
            self.show_pb(backup_dir, 'node')[3]['status'] == 'CORRUPT')
        self.assertTrue(
            self.show_pb(backup_dir, 'node')[4]['status'] == 'ORPHAN')
        self.assertTrue(
            self.show_pb(backup_dir, 'node')[5]['status'] == 'ORPHAN')
        self.assertTrue(
            self.show_pb(backup_dir, 'node')[6]['status'] == 'ERROR')
        self.assertTrue(
            self.show_pb(backup_dir, 'node')[7]['status'] == 'ORPHAN')

        os.rename(file_new, file)
        try:
            self.validate_pb(backup_dir, options=['--log-level-file=verbose'])
        except ProbackupException as e:
            self.assertIn(
                'WARNING: Some backups are not valid'.format(
                    backup_id), e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertTrue(self.show_pb(backup_dir, 'node')[0]['status'] == 'OK')
        self.assertTrue(self.show_pb(backup_dir, 'node')[1]['status'] == 'OK')
        self.assertTrue(self.show_pb(backup_dir, 'node')[2]['status'] == 'OK')
        self.assertTrue(self.show_pb(backup_dir, 'node')[3]['status'] == 'OK')
        self.assertTrue(self.show_pb(backup_dir, 'node')[4]['status'] == 'OK')
        self.assertTrue(self.show_pb(backup_dir, 'node')[5]['status'] == 'OK')
        self.assertTrue(
            self.show_pb(backup_dir, 'node')[6]['status'] == 'ERROR')
        self.assertTrue(self.show_pb(backup_dir, 'node')[7]['status'] == 'OK')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_validate_corrupted_full_1(self):
        """
        make node with archiving, take full backup, and three page backups,
        take another full backup and four page backups
        corrupt second full backup, run validate, check that
        second full backup became CORRUPT and his page backups are ORPHANs
        remove corruption from full backup and corrupt his second page backup
        run valudate again, check that
        second full backup and his firts page backups are OK,
        second page should be CORRUPT
        third page should be ORPHAN
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        self.backup_node(backup_dir, 'node', node)
        self.backup_node(backup_dir, 'node', node, backup_type='page')
        self.backup_node(backup_dir, 'node', node, backup_type='page')

        backup_id = self.backup_node(backup_dir, 'node', node)
        self.backup_node(backup_dir, 'node', node, backup_type='page')
        backup_id_page = self.backup_node(
            backup_dir, 'node', node, backup_type='page')
        self.backup_node(backup_dir, 'node', node, backup_type='page')

        file = os.path.join(
            backup_dir, 'backups', 'node',
            backup_id, 'database', 'postgresql.auto.conf')

        file_new = os.path.join(backup_dir, 'postgresql.auto.conf')
        os.rename(file, file_new)

        try:
            self.validate_pb(backup_dir)
            self.assertEqual(
                1, 0,
                "Expecting Error because of data file dissapearance.\n "
                "Output: {0} \n CMD: {1}".format(
                    self.output, self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'Validating backup {0}'.format(backup_id), e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertIn(
                'WARNING: Backup {0} data files are corrupted'.format(
                    backup_id), e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))
            self.assertIn(
                'WARNING: Some backups are not valid'.format(
                    backup_id), e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertTrue(self.show_pb(backup_dir, 'node')[0]['status'] == 'OK')
        self.assertTrue(self.show_pb(backup_dir, 'node')[1]['status'] == 'OK')
        self.assertTrue(self.show_pb(backup_dir, 'node')[2]['status'] == 'OK')
        self.assertTrue(self.show_pb(backup_dir, 'node')[3]['status'] == 'CORRUPT')
        self.assertTrue(self.show_pb(backup_dir, 'node')[4]['status'] == 'ORPHAN')
        self.assertTrue(self.show_pb(backup_dir, 'node')[5]['status'] == 'ORPHAN')
        self.assertTrue(self.show_pb(backup_dir, 'node')[6]['status'] == 'ORPHAN')

        os.rename(file_new, file)
        file = os.path.join(
            backup_dir, 'backups', 'node',
            backup_id_page, 'database', 'postgresql.auto.conf')

        file_new = os.path.join(backup_dir, 'postgresql.auto.conf')
        os.rename(file, file_new)

        try:
            self.validate_pb(backup_dir, options=['--log-level-file=verbose'])
        except ProbackupException as e:
            self.assertIn(
                'WARNING: Some backups are not valid'.format(
                    backup_id), e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertTrue(self.show_pb(backup_dir, 'node')[0]['status'] == 'OK')
        self.assertTrue(self.show_pb(backup_dir, 'node')[1]['status'] == 'OK')
        self.assertTrue(self.show_pb(backup_dir, 'node')[2]['status'] == 'OK')
        self.assertTrue(self.show_pb(backup_dir, 'node')[3]['status'] == 'OK')
        self.assertTrue(self.show_pb(backup_dir, 'node')[4]['status'] == 'OK')
        self.assertTrue(self.show_pb(backup_dir, 'node')[5]['status'] == 'CORRUPT')
        self.assertTrue(self.show_pb(backup_dir, 'node')[6]['status'] == 'ORPHAN')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_file_size_corruption_no_validate(self):

        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            # initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica'}
        )

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)

        node.start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1000) i")
        node.safe_psql(
            "postgres",
            "CHECKPOINT;")

        heap_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").rstrip()
        heap_size = node.safe_psql(
            "postgres",
            "select pg_relation_size('t_heap')")

        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type="full",
            options=["-j", "4"], async=False, gdb=False)

        node.stop()
        node.cleanup()

        # Let`s do file corruption
        with open(os.path.join(backup_dir, "backups", 'node', backup_id, "database", heap_path), "rb+", 0) as f:
            f.truncate(int(heap_size) - 4096)
            f.flush()
            f.close

        node.cleanup()

        try:
            self.restore_node(
                backup_dir, 'node', node,
                options=["--no-validate"])
        except ProbackupException as e:
            self.assertTrue("ERROR: Data files restoring failed" in e.message, repr(e.message))
            print "\nExpected error: \n" + e.message

        # Clean after yourself
        self.del_test_dir(module_name, fname)
