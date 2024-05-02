import os
from .helpers.ptrack_helpers import ProbackupTest
from pg_probackup2.gdb import needs_gdb
from datetime import datetime, timedelta
from pathlib import Path
import time
import hashlib


class ValidateTest(ProbackupTest):

    def setUp(self):
        super().setUp()
        self.test_env["PGPROBACKUP_TESTS_SKIP_HIDDEN"] = "ON"

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_basic_validate_nullified_heap_page_backup(self):
        """
        make node with nullified heap block
        """
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        file_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('pgbench_accounts')").decode('utf-8').rstrip()

        node.safe_psql(
            "postgres",
            "CHECKPOINT")

        # Nullify some block in PostgreSQL
        file = os.path.join(node.data_dir, file_path)
        with open(file, 'r+b') as f:
            f.seek(8192)
            f.write(b"\x00"*8192)
            f.flush()

        self.pb.backup_node(
            'node', node, options=['--log-level-file=verbose'])

        pgdata = self.pgdata_content(node.data_dir)

        log_content = self.read_pb_log()
        self.assertIn(
            'File: {0} blknum 1, empty zeroed page'.format(file_path),
            log_content,
            'Failed to detect nullified block')
        if not self.remote:
            self.assertIn(
                    'File: "{0}" blknum 1, empty page'.format(Path(file).as_posix()),
                    log_content,
                    'Failed to detect nullified block')

        self.pb.validate(options=["-j", "4"])
        node.cleanup()

        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_validate_wal_unreal_values(self):
        """
        make node with archiving, make archive backup
        validate to both real and unreal values
        """
        instance_name = 'node'
        node = self.pg_node.make_simple(instance_name)

        self.pb.init()
        self.pb.add_instance(instance_name, node)
        self.pb.set_archiving(instance_name, node)
        node.slow_start()

        node.pgbench_init(scale=3)
        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        backup_id = self.pb.backup_node(instance_name, node)

        node.pgbench_init(scale=3)

        target_time = self.pb.show(
            instance_name, backup_id)['recovery-time']
        after_backup_time = datetime.now().replace(second=0, microsecond=0)

        # Validate to real time
        validate_result = self.pb.validate(instance_name, options=[f"--recovery-target-time={target_time}", "-j", "4"])
        self.assertMessage(validate_result, contains="INFO: Backup validation completed successfully")

        # Validate to unreal time
        unreal_time_1 = after_backup_time - timedelta(days=2)
        error_result = self.pb.validate(instance_name,
                                        options=[f"--time={unreal_time_1}", "-j", "4"],
                                        expect_error=True)

        self.assertMessage(error_result, contains='ERROR: Backup satisfying target options is not found')

        # Validate to unreal time #2
        unreal_time_2 = after_backup_time + timedelta(days=2)
        error_result = self.pb.validate(instance_name,
                                        options=["--time={0}".format(unreal_time_2), "-j", "4"],
                                        expect_error=True)
        self.assertMessage(error_result, contains='ERROR: Not enough WAL records to time')

        # Validate to real xid
        with node.connect("postgres") as con:
            res = con.execute(
                "INSERT INTO tbl0005 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]
        self.switch_wal_segment(node)
        time.sleep(5)

        output = self.pb.validate(instance_name,
                                  options=["--xid={0}".format(target_xid), "-j", "4"])
        self.assertMessage(output, contains="INFO: Backup validation completed successfully")

        # Validate to unreal xid
        unreal_xid = int(target_xid) + 1000
        error_result = self.pb.validate(instance_name,
                                        options=["--xid={0}".format(unreal_xid), "-j", "4"],
                                        expect_error=True)
        self.assertMessage(error_result, contains='ERROR: Not enough WAL records to xid')

        # Validate with backup ID
        output = self.pb.validate(instance_name, backup_id,
                                  options=["-j", "4"])
        self.assertMessage(output, contains=f"INFO: Validating backup {backup_id}")
        self.assertMessage(output, contains=f"INFO: Backup {backup_id} data files are valid")
        self.assertMessage(output, contains=f"INFO: Backup {backup_id} WAL segments are valid")
        self.assertMessage(output, contains=f"INFO: Backup {backup_id} is valid")
        self.assertMessage(output, contains=f"INFO: Validate of backup {backup_id} completed")

    # @unittest.skip("skip")
    def test_basic_validate_corrupted_intermediate_backup(self):
        """
        make archive node, take FULL, PAGE1, PAGE2 backups,
        corrupt file in PAGE1 backup,
        run validate on PAGE1, expect PAGE1 to gain status CORRUPT
        and PAGE2 gain status ORPHAN
        """
        instance_name = 'node'
        node = self.pg_node.make_simple(instance_name)


        self.pb.init()
        self.pb.add_instance(instance_name, node)
        self.pb.set_archiving(instance_name, node)
        node.slow_start()

        # FULL
        backup_id_full = self.pb.backup_node(instance_name, node)

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")
        file_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()
        # PAGE1
        backup_id_page = self.pb.backup_node(
            instance_name, node, backup_type='page')

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(10000,20000) i")
        # PAGE2
        backup_id_page_2 = self.pb.backup_node(
            instance_name, node, backup_type='page')

        # Corrupt some file
        self.corrupt_backup_file(self.backup_dir, instance_name, backup_id_page,
                                 f'database/{file_path}', damage=(42, b"blah"))

        # Simple validate
        error_result = self.pb.validate(instance_name, backup_id=backup_id_page,
                                        options=["-j", "4"], expect_error=True)
        self.assertMessage(error_result, contains=f'INFO: Validating parents for backup {backup_id_page}')
        self.assertMessage(error_result, contains=f'ERROR: Backup {backup_id_page} is corrupt')
        self.assertMessage(error_result, contains=f'WARNING: Backup {backup_id_page} data files are corrupted')

        page_backup_status = self.pb.show(instance_name, backup_id_page)['status']
        self.assertEqual('CORRUPT', page_backup_status, 'Backup STATUS should be "CORRUPT"')

        second_page_backup_status = self.pb.show(instance_name, backup_id_page_2)['status']
        self.assertEqual('ORPHAN', second_page_backup_status, 'Backup STATUS should be "ORPHAN"')

    # @unittest.skip("skip")
    def test_validate_corrupted_intermediate_backups(self):
        """
        make archive node, take FULL, PAGE1, PAGE2 backups,
        corrupt file in FULL and PAGE1 backups, run validate on PAGE1,
        expect FULL and PAGE1 to gain status CORRUPT and
        PAGE2 gain status ORPHAN
        """
        instance_name = 'node'
        node = self.pg_node.make_simple(instance_name)


        self.pb.init()
        self.pb.add_instance(instance_name, node)
        self.pb.set_archiving(instance_name, node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")
        file_path_t_heap = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()
        # FULL
        backup_id_1 = self.pb.backup_node(instance_name, node)

        node.safe_psql(
            "postgres",
            "create table t_heap_1 as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")
        file_path_t_heap_1 = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap_1')").decode('utf-8').rstrip()
        # PAGE1
        backup_id_2 = self.pb.backup_node(
            instance_name, node, backup_type='page')

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(20000,30000) i")
        # PAGE2
        backup_id_3 = self.pb.backup_node(
            instance_name, node, backup_type='page')

        # Corrupt some file in FULL backup
        self.corrupt_backup_file(self.backup_dir, instance_name, backup_id_1,
                                 f'database/{file_path_t_heap}',
                                 damage=(84, b"blah"))

        # Corrupt some file in PAGE1 backup
        self.corrupt_backup_file(self.backup_dir, instance_name, backup_id_2,
                                 f'database/{file_path_t_heap_1}',
                                 damage=(42, b"blah"))

        # Validate PAGE1
        error_result = self.pb.validate(instance_name, backup_id=backup_id_2,
                                        options=["-j", "4"], expect_error=True)
        self.assertMessage(error_result, contains=f'INFO: Validating parents for backup {backup_id_2}')
        self.assertMessage(error_result, contains=f'INFO: Validating backup {backup_id_1}')
        self.assertMessage(error_result, contains=f'WARNING: Invalid CRC of backup file')
        self.assertMessage(error_result, contains=f'WARNING: Backup {backup_id_1} data files are corrupted')
        self.assertMessage(error_result, contains=f'WARNING: Backup {backup_id_2} is orphaned because his parent')
        self.assertMessage(error_result, contains=f'WARNING: Backup {backup_id_3} is orphaned because his parent')
        self.assertMessage(error_result, contains=f'ERROR: Backup {backup_id_2} is orphan.')
        self.assertEqual('CORRUPT',
                         self.pb.show(instance_name, backup_id_1)['status'],
            'Backup STATUS should be "CORRUPT"')
        self.assertEqual(
            'ORPHAN',
            self.pb.show(instance_name, backup_id_2)['status'],
            'Backup STATUS should be "ORPHAN"')
        self.assertEqual(
            'ORPHAN',
            self.pb.show(instance_name, backup_id_3)['status'],
            'Backup STATUS should be "ORPHAN"')

    # @unittest.skip("skip")
    def test_validate_specific_error_intermediate_backups(self):
        """
        make archive node, take FULL, PAGE1, PAGE2 backups,
        change backup status of FULL and PAGE1 to ERROR,
        run validate on PAGE1
        purpose of this test is to be sure that not only
        CORRUPT backup descendants can be orphanized
        """
        instance_name = 'node'
        node = self.pg_node.make_simple(instance_name)


        self.pb.init()
        self.pb.add_instance(instance_name, node)
        self.pb.set_archiving(instance_name, node)
        node.slow_start()

        # FULL
        backup_id_1 = self.pb.backup_node(instance_name, node)

        # PAGE1
        backup_id_2 = self.pb.backup_node(
            instance_name, node, backup_type='page')

        # PAGE2
        backup_id_3 = self.pb.backup_node(
            instance_name, node, backup_type='page')

        # Change FULL backup status to ERROR
        self.change_backup_status(self.backup_dir, instance_name, backup_id_1, 'ERROR')

        # Validate PAGE1
        error_message = self.pb.validate(instance_name, backup_id=backup_id_2, options=["-j", "4"],
                                         expect_error=True)
        self.assertMessage(error_message, contains=f'WARNING: Backup {backup_id_2} is orphaned because his parent {backup_id_1} has status: ERROR')
        self.assertMessage(error_message, contains=f'INFO: Validating parents for backup {backup_id_2}')
        self.assertMessage(error_message, contains=f'WARNING: Backup {backup_id_1} has status ERROR. Skip validation.')
        self.assertMessage(error_message, contains=f'ERROR: Backup {backup_id_2} is orphan.')
        self.assertMessage(error_message, contains=f'ERROR: Backup {backup_id_2} is orphan.')

        self.assertEqual(
            'ERROR',
            self.pb.show(instance_name, backup_id_1)['status'],
            'Backup STATUS should be "ERROR"')
        self.assertEqual(
            'ORPHAN',
            self.pb.show(instance_name, backup_id_2)['status'],
            'Backup STATUS should be "ORPHAN"')
        self.assertEqual(
            'ORPHAN',
            self.pb.show(instance_name, backup_id_3)['status'],
            'Backup STATUS should be "ORPHAN"')

    # @unittest.skip("skip")
    def test_validate_error_intermediate_backups(self):
        """
        make archive node, take FULL, PAGE1, PAGE2 backups,
        change backup status of FULL and PAGE1 to ERROR,
        run validate on instance
        purpose of this test is to be sure that not only
        CORRUPT backup descendants can be orphanized
        """
        instance_name = 'node'
        node = self.pg_node.make_simple(instance_name)


        self.pb.init()
        self.pb.add_instance(instance_name, node)
        self.pb.set_archiving(instance_name, node)
        node.slow_start()

        # FULL
        backup_id_1 = self.pb.backup_node(instance_name, node)

        # PAGE1
        backup_id_2 = self.pb.backup_node(
            instance_name, node, backup_type='page')

        # PAGE2
        backup_id_3 = self.pb.backup_node(
            instance_name, node, backup_type='page')

        # Change FULL backup status to ERROR
        self.change_backup_status(self.backup_dir, instance_name, backup_id_1, 'ERROR')

        # Validate instance
        error_message = self.pb.validate(options=["-j", "4"], expect_error=True)
        self.assertMessage(error_message, contains=f'WARNING: Backup {backup_id_2} is orphaned because his parent {backup_id_1} has status: ERROR')
        self.assertMessage(error_message, contains=f'WARNING: Backup {backup_id_1} has status ERROR. Skip validation')

        self.assertEqual(
            'ERROR',
            self.pb.show(instance_name, backup_id_1)['status'],
            'Backup STATUS should be "ERROR"')
        self.assertEqual(
            'ORPHAN',
            self.pb.show(instance_name, backup_id_2)['status'],
            'Backup STATUS should be "ORPHAN"')
        self.assertEqual(
            'ORPHAN',
            self.pb.show(instance_name, backup_id_3)['status'],
            'Backup STATUS should be "ORPHAN"')

    # @unittest.skip("skip")
    def test_validate_corrupted_intermediate_backups_1(self):
        """
        make archive node, FULL1, PAGE1, PAGE2, PAGE3, PAGE4, PAGE5, FULL2,
        corrupt file in PAGE1 and PAGE4, run validate on PAGE3,
        expect PAGE1 to gain status CORRUPT, PAGE2, PAGE3, PAGE4 and PAGE5
        to gain status ORPHAN
        """
        node = self.pg_node.make_simple('node')


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL1
        backup_id_1 = self.pb.backup_node('node', node)

        # PAGE1
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")
        backup_id_2 = self.pb.backup_node(
            'node', node, backup_type='page')

        # PAGE2
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")
        file_page_2 = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()
        backup_id_3 = self.pb.backup_node(
            'node', node, backup_type='page')

        # PAGE3
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(10000,20000) i")
        backup_id_4 = self.pb.backup_node(
            'node', node, backup_type='page')

        # PAGE4
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(20000,30000) i")
        backup_id_5 = self.pb.backup_node(
            'node', node, backup_type='page')

        # PAGE5
        node.safe_psql(
            "postgres",
            "create table t_heap1 as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")
        file_page_5 = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap1')").decode('utf-8').rstrip()
        backup_id_6 = self.pb.backup_node(
            'node', node, backup_type='page')

        # PAGE6
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(30000,40000) i")
        backup_id_7 = self.pb.backup_node(
            'node', node, backup_type='page')

        # FULL2
        backup_id_8 = self.pb.backup_node('node', node)

        # Corrupt some file in PAGE2 and PAGE5 backups
        self.corrupt_backup_file(self.backup_dir, 'node', backup_id_3,
                                 f'database/{file_page_2}',
                                 damage=(84, b"blah"))

        self.corrupt_backup_file(self.backup_dir, 'node', backup_id_6,
                                 f'database/{file_page_5}',
                                 damage=(42, b"blah"))

        # Validate PAGE3
        self.pb.validate('node',
                         backup_id=backup_id_4, options=["-j", "4"],
                         expect_error="because of data files corruption")

        self.assertMessage(contains=f'INFO: Validating parents for backup {backup_id_4}')
        self.assertMessage(contains=f'INFO: Validating backup {backup_id_1}')
        self.assertMessage(contains=f'INFO: Backup {backup_id_1} data files are valid')
        self.assertMessage(contains=f'INFO: Backup {backup_id_1} data files are valid')
        self.assertMessage(contains=f'INFO: Validating backup {backup_id_2}')
        self.assertMessage(contains=f'INFO: Backup {backup_id_2} data files are valid')
        self.assertMessage(contains=f'INFO: Validating backup {backup_id_3}')
        self.assertMessage(contains=f'WARNING: Invalid CRC of backup file')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_3} data files are corrupted')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_4} is orphaned because his parent {backup_id_3} has status: CORRUPT')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_5} is orphaned because his parent {backup_id_3} has status: CORRUPT')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_6} is orphaned because his parent {backup_id_3} has status: CORRUPT')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_7} is orphaned because his parent {backup_id_3} has status: CORRUPT')
        self.assertMessage(contains=f'ERROR: Backup {backup_id_4} is orphan')

        self.assertEqual(
            'OK', self.pb.show('node', backup_id_1)['status'],
            'Backup STATUS should be "OK"')
        self.assertEqual(
            'OK', self.pb.show('node', backup_id_2)['status'],
            'Backup STATUS should be "OK"')
        self.assertEqual(
            'CORRUPT', self.pb.show('node', backup_id_3)['status'],
            'Backup STATUS should be "CORRUPT"')
        self.assertEqual(
            'ORPHAN', self.pb.show('node', backup_id_4)['status'],
            'Backup STATUS should be "ORPHAN"')
        self.assertEqual(
            'ORPHAN', self.pb.show('node', backup_id_5)['status'],
            'Backup STATUS should be "ORPHAN"')
        self.assertEqual(
            'ORPHAN', self.pb.show('node', backup_id_6)['status'],
            'Backup STATUS should be "ORPHAN"')
        self.assertEqual(
            'ORPHAN', self.pb.show('node', backup_id_7)['status'],
            'Backup STATUS should be "ORPHAN"')
        self.assertEqual(
            'OK', self.pb.show('node', backup_id_8)['status'],
            'Backup STATUS should be "OK"')

    # @unittest.skip("skip")
    def test_validate_specific_target_corrupted_intermediate_backups(self):
        """
        make archive node, take FULL1, PAGE1, PAGE2, PAGE3, PAGE4, PAGE5, FULL2
        corrupt file in PAGE1 and PAGE4, run validate on PAGE3 to specific xid,
        expect PAGE1 to gain status CORRUPT, PAGE2, PAGE3, PAGE4 and PAGE5 to
        gain status ORPHAN
        """
        node = self.pg_node.make_simple('node')


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL1
        backup_id_1 = self.pb.backup_node('node', node)

        # PAGE1
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")
        backup_id_2 = self.pb.backup_node(
            'node', node, backup_type='page')

        # PAGE2
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")
        file_page_2 = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()
        backup_id_3 = self.pb.backup_node(
            'node', node, backup_type='page')

        # PAGE3
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(10000,20000) i")
        backup_id_4 = self.pb.backup_node(
            'node', node, backup_type='page')

        # PAGE4
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(20000,30000) i")

        target_xid = node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(30001, 30001) i  RETURNING (xmin)").decode('utf-8').rstrip()

        backup_id_5 = self.pb.backup_node(
            'node', node, backup_type='page')

        # PAGE5
        node.safe_psql(
            "postgres",
            "create table t_heap1 as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")
        file_page_5 = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap1')").decode('utf-8').rstrip()
        backup_id_6 = self.pb.backup_node(
            'node', node, backup_type='page')

        # PAGE6
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(30000,40000) i")
        backup_id_7 = self.pb.backup_node(
            'node', node, backup_type='page')

        # FULL2
        backup_id_8 = self.pb.backup_node('node', node)

        # Corrupt some file in PAGE2 and PAGE5 backups
        self.corrupt_backup_file(self.backup_dir, 'node', backup_id_3,
                                 f'database/{file_page_2}',
                                 damage=(84, b"blah"))
        self.corrupt_backup_file(self.backup_dir, 'node', backup_id_6,
                                 f'database/{file_page_5}',
                                 damage=(42, b"blah"))

        # Validate PAGE3
        self.pb.validate('node',
                         options=['-i', backup_id_4, '--xid', target_xid, "-j", "4"],
                         expect_error="because of data files corruption")

        self.assertMessage(contains=f'INFO: Validating parents for backup {backup_id_4}')
        self.assertMessage(contains=f'INFO: Validating backup {backup_id_1}')
        self.assertMessage(contains=f'INFO: Backup {backup_id_1} data files are valid')
        self.assertMessage(contains=f'INFO: Validating backup {backup_id_2}')
        self.assertMessage(contains=f'INFO: Backup {backup_id_2} data files are valid')
        self.assertMessage(contains=f'INFO: Validating backup {backup_id_3}')
        self.assertMessage(contains='WARNING: Invalid CRC of backup file')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_3} data files are corrupted')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_4} is orphaned because his '
                                    f'parent {backup_id_3} has status: CORRUPT')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_5} is orphaned because his '
                                    f'parent {backup_id_3} has status: CORRUPT')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_6} is orphaned because his '
                                    f'parent {backup_id_3} has status: CORRUPT')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_7} is orphaned because his '
                                    f'parent {backup_id_3} has status: CORRUPT')
        self.assertMessage(contains=f'ERROR: Backup {backup_id_4} is orphan')

        self.assertEqual('OK', self.pb.show('node', backup_id_1)['status'], 'Backup STATUS should be "OK"')
        self.assertEqual('OK', self.pb.show('node', backup_id_2)['status'], 'Backup STATUS should be "OK"')
        self.assertEqual('CORRUPT', self.pb.show('node', backup_id_3)['status'], 'Backup STATUS should be "CORRUPT"')
        self.assertEqual('ORPHAN', self.pb.show('node', backup_id_4)['status'], 'Backup STATUS should be "ORPHAN"')
        self.assertEqual('ORPHAN', self.pb.show('node', backup_id_5)['status'], 'Backup STATUS should be "ORPHAN"')
        self.assertEqual('ORPHAN', self.pb.show('node', backup_id_6)['status'], 'Backup STATUS should be "ORPHAN"')
        self.assertEqual('ORPHAN', self.pb.show('node', backup_id_7)['status'], 'Backup STATUS should be "ORPHAN"')
        self.assertEqual('OK', self.pb.show('node', backup_id_8)['status'], 'Backup STATUS should be "OK"')

    # @unittest.skip("skip")
    def test_validate_instance_with_several_corrupt_backups(self):
        """
        make archive node, take FULL1, PAGE1_1, FULL2, PAGE2_1 backups, FULL3
        corrupt file in FULL and FULL2 and run validate on instance,
        expect FULL1 to gain status CORRUPT, PAGE1_1 to gain status ORPHAN
        FULL2 to gain status CORRUPT, PAGE2_1 to gain status ORPHAN
        """
        node = self.pg_node.make_simple('node')


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select generate_series(0,1) i")
        # FULL1
        backup_id_1 = self.pb.backup_node(
            'node', node, options=['--no-validate'])

        # FULL2
        backup_id_2 = self.pb.backup_node('node', node)
        rel_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()

        node.safe_psql(
            "postgres",
            "insert into t_heap values(2)")

        backup_id_3 = self.pb.backup_node(
            'node', node, backup_type='page')

        # FULL3
        backup_id_4 = self.pb.backup_node('node', node)

        node.safe_psql(
            "postgres",
            "insert into t_heap values(3)")

        backup_id_5 = self.pb.backup_node(
            'node', node, backup_type='page')

        # FULL4
        backup_id_6 = self.pb.backup_node(
            'node', node, options=['--no-validate'])

        # Corrupt some files in FULL2 and FULL3 backup
        self.remove_backup_file(self.backup_dir, 'node', backup_id_2,
                                f'database/{rel_path}')
        self.remove_backup_file(self.backup_dir, 'node', backup_id_4,
                                f'database/{rel_path}')

        # Validate Instance
        self.pb.validate('node', options=["-j", "4", "--log-level-file=LOG"],
                         expect_error="because of data files corruption")
        self.assertMessage(contains="INFO: Validate backups of the instance 'node'")
        self.assertMessage(contains='WARNING: Some backups are not valid')

        self.assertEqual(
            'OK', self.pb.show('node', backup_id_1)['status'],
            'Backup STATUS should be "OK"')
        self.assertEqual(
            'CORRUPT', self.pb.show('node', backup_id_2)['status'],
            'Backup STATUS should be "CORRUPT"')
        self.assertEqual(
            'ORPHAN', self.pb.show('node', backup_id_3)['status'],
            'Backup STATUS should be "ORPHAN"')
        self.assertEqual(
            'CORRUPT', self.pb.show('node', backup_id_4)['status'],
            'Backup STATUS should be "CORRUPT"')
        self.assertEqual(
            'ORPHAN', self.pb.show('node', backup_id_5)['status'],
            'Backup STATUS should be "ORPHAN"')
        self.assertEqual(
            'OK', self.pb.show('node', backup_id_6)['status'],
            'Backup STATUS should be "OK"')

    # @unittest.skip("skip")
    @needs_gdb
    def test_validate_instance_with_several_corrupt_backups_interrupt(self):
        """
        check that interrupt during validation is handled correctly
        """

        node = self.pg_node.make_simple('node')


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select generate_series(0,1) i")
        # FULL1
        backup_id_1 = self.pb.backup_node(
            'node', node, options=['--no-validate'])

        # FULL2
        backup_id_2 = self.pb.backup_node('node', node)
        rel_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()

        node.safe_psql(
            "postgres",
            "insert into t_heap values(2)")

        backup_id_3 = self.pb.backup_node(
            'node', node, backup_type='page')

        # FULL3
        backup_id_4 = self.pb.backup_node('node', node)

        node.safe_psql(
            "postgres",
            "insert into t_heap values(3)")

        backup_id_5 = self.pb.backup_node(
            'node', node, backup_type='page')

        # FULL4
        backup_id_6 = self.pb.backup_node(
            'node', node, options=['--no-validate'])

        # Corrupt some files in FULL2 and FULL3 backup
        self.remove_backup_file(self.backup_dir, 'node', backup_id_1,
                                f'database/{rel_path}')
        self.remove_backup_file(self.backup_dir, 'node', backup_id_3,
                                f'database/{rel_path}')

        # Validate Instance
        gdb = self.pb.validate(
            'node', options=["-j", "4", "--log-level-file=LOG"], gdb=True)

        gdb.set_breakpoint('validate_file_pages')
        gdb.run_until_break()
        gdb.continue_execution_until_break()
        gdb.signal('SIGINT')
        gdb.continue_execution_until_error()

        self.assertEqual(
            'DONE', self.pb.show('node', backup_id_1)['status'],
            'Backup STATUS should be "OK"')
        self.assertEqual(
            'OK', self.pb.show('node', backup_id_2)['status'],
            'Backup STATUS should be "OK"')
        self.assertEqual(
            'OK', self.pb.show('node', backup_id_3)['status'],
            'Backup STATUS should be "CORRUPT"')
        self.assertEqual(
            'OK', self.pb.show('node', backup_id_4)['status'],
            'Backup STATUS should be "ORPHAN"')
        self.assertEqual(
            'OK', self.pb.show('node', backup_id_5)['status'],
            'Backup STATUS should be "OK"')
        self.assertEqual(
            'DONE', self.pb.show('node', backup_id_6)['status'],
            'Backup STATUS should be "OK"')

        log_content = self.read_pb_log()
        self.assertNotIn(
                'Interrupted while locking backup', log_content)

    # @unittest.skip("skip")
    def test_validate_instance_with_corrupted_page(self):
        """
        make archive node, take FULL, PAGE1, PAGE2, FULL2, PAGE3 backups,
        corrupt file in PAGE1 backup and run validate on instance,
        expect PAGE1 to gain status CORRUPT, PAGE2 to gain status ORPHAN
        """
        node = self.pg_node.make_simple('node')


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")
        # FULL1
        backup_id_1 = self.pb.backup_node('node', node)

        node.safe_psql(
            "postgres",
            "create table t_heap1 as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")
        file_path_t_heap1 = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap1')").decode('utf-8').rstrip()
        # PAGE1
        backup_id_2 = self.pb.backup_node(
            'node', node, backup_type='page')

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(20000,30000) i")
        # PAGE2
        backup_id_3 = self.pb.backup_node(
            'node', node, backup_type='page')
        # FULL1
        backup_id_4 = self.pb.backup_node(
            'node', node)
        # PAGE3
        backup_id_5 = self.pb.backup_node(
            'node', node, backup_type='page')

        # Corrupt some file in FULL backup
        self.corrupt_backup_file(self.backup_dir, 'node', backup_id_2,
                                 f'database/{file_path_t_heap1}',
                                 damage=(84, b"blah"))

        # Validate Instance
        self.pb.validate('node', options=["-j", "4"],
                         expect_error="because of data files corruption")
        self.assertMessage(contains="INFO: Validate backups of the instance 'node'")
        self.assertMessage(contains=f'INFO: Validating backup {backup_id_5}')
        self.assertMessage(contains=f'INFO: Backup {backup_id_5} data files are valid')
        self.assertMessage(contains=f'INFO: Backup {backup_id_5} WAL segments are valid')
        self.assertMessage(contains=f'INFO: Validating backup {backup_id_4}')
        self.assertMessage(contains=f'INFO: Backup {backup_id_4} data files are valid')
        self.assertMessage(contains=f'INFO: Backup {backup_id_4} WAL segments are valid')
        self.assertMessage(contains=f'INFO: Validating backup {backup_id_3}')
        self.assertMessage(contains=f'INFO: Backup {backup_id_3} data files are valid')
        self.assertMessage(contains=f'INFO: Backup {backup_id_3} WAL segments are valid')

        self.assertMessage(contains=f'WARNING: Backup {backup_id_3} is orphaned because '
                                    f'his parent {backup_id_2} has status: CORRUPT')
        self.assertMessage(contains=f'INFO: Validating backup {backup_id_2}')
        self.assertMessage(contains='WARNING: Invalid CRC of backup file')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_2} data files are corrupted')

        self.assertMessage(contains=f'INFO: Validating backup {backup_id_1}')
        self.assertMessage(contains=f'INFO: Backup {backup_id_1} data files are valid')
        self.assertMessage(contains=f'INFO: Backup {backup_id_1} WAL segments are valid')
        self.assertMessage(contains='WARNING: Some backups are not valid')

        self.assertEqual(
            'OK', self.pb.show('node', backup_id_1)['status'],
            'Backup STATUS should be "OK"')
        self.assertEqual(
            'CORRUPT', self.pb.show('node', backup_id_2)['status'],
            'Backup STATUS should be "CORRUPT"')
        self.assertEqual(
            'ORPHAN', self.pb.show('node', backup_id_3)['status'],
            'Backup STATUS should be "ORPHAN"')
        self.assertEqual(
            'OK', self.pb.show('node', backup_id_4)['status'],
            'Backup STATUS should be "OK"')
        self.assertEqual(
            'OK', self.pb.show('node', backup_id_5)['status'],
            'Backup STATUS should be "OK"')

    # @unittest.skip("skip")
    def test_validate_instance_with_corrupted_full_and_try_restore(self):
        """make archive node, take FULL, PAGE1, PAGE2, FULL2, PAGE3 backups,
        corrupt file in FULL backup and run validate on instance,
        expect FULL to gain status CORRUPT, PAGE1 and PAGE2 to gain status ORPHAN,
        try to restore backup with --no-validation option"""
        node = self.pg_node.make_simple('node')


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")
        file_path_t_heap = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()
        # FULL1
        backup_id_1 = self.pb.backup_node('node', node)

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")
        # PAGE1
        backup_id_2 = self.pb.backup_node('node', node, backup_type='page')

        # PAGE2
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(20000,30000) i")
        backup_id_3 = self.pb.backup_node('node', node, backup_type='page')

        # FULL1
        backup_id_4 = self.pb.backup_node('node', node)

        # PAGE3
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, "
            "md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(30000,40000) i")
        backup_id_5 = self.pb.backup_node('node', node, backup_type='page')

        # Corrupt some file in FULL backup
        self.corrupt_backup_file(self.backup_dir, 'node', backup_id_1,
                                 f'database/{file_path_t_heap}',
                                 damage=(84, b"blah"))

        # Validate Instance
        self.pb.validate('node', options=["-j", "4"],
                         expect_error="because of data files corruption")
        self.assertMessage(contains=f'INFO: Validating backup {backup_id_1}')
        self.assertMessage(contains="INFO: Validate backups of the instance 'node'")
        self.assertMessage(contains='WARNING: Invalid CRC of backup file')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_1} data files are corrupted')

        self.assertEqual('CORRUPT', self.pb.show('node', backup_id_1)['status'], 'Backup STATUS should be "CORRUPT"')
        self.assertEqual('ORPHAN', self.pb.show('node', backup_id_2)['status'], 'Backup STATUS should be "ORPHAN"')
        self.assertEqual('ORPHAN', self.pb.show('node', backup_id_3)['status'], 'Backup STATUS should be "ORPHAN"')
        self.assertEqual('OK', self.pb.show('node', backup_id_4)['status'], 'Backup STATUS should be "OK"')
        self.assertEqual('OK', self.pb.show('node', backup_id_5)['status'], 'Backup STATUS should be "OK"')

        node.cleanup()
        restore_out = self.pb.restore_node(
                'node', node,
                options=["--no-validate"])
        self.assertMessage(restore_out, contains="INFO: Restore of backup {0} completed.".format(backup_id_5))

    # @unittest.skip("skip")
    def test_validate_instance_with_corrupted_full(self):
        """make archive node, take FULL, PAGE1, PAGE2, FULL2, PAGE3 backups,
        corrupt file in FULL backup and run validate on instance,
        expect FULL to gain status CORRUPT, PAGE1 and PAGE2 to gain status ORPHAN"""
        node = self.pg_node.make_simple('node')


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, "
            "md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")

        file_path_t_heap = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()
        # FULL1
        backup_id_1 = self.pb.backup_node('node', node)

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")

        # PAGE1
        backup_id_2 = self.pb.backup_node(
            'node', node, backup_type='page')

        # PAGE2
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(20000,30000) i")

        backup_id_3 = self.pb.backup_node(
            'node', node, backup_type='page')

        # FULL1
        backup_id_4 = self.pb.backup_node(
            'node', node)

        # PAGE3
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, "
            "md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(30000,40000) i")
        backup_id_5 = self.pb.backup_node('node', node, backup_type='page')

        # Corrupt some file in FULL backup
        self.corrupt_backup_file(self.backup_dir, 'node', backup_id_1,
                                 f'database/{file_path_t_heap}',
                                 damage=(84, b"blah"))

        # Validate Instance
        self.pb.validate('node', options=["-j", "4"],
                         expect_error="because of data files corruption")
        self.assertMessage(contains=f'INFO: Validating backup {backup_id_1}')
        self.assertMessage(contains="INFO: Validate backups of the instance 'node'")
        self.assertMessage(contains='WARNING: Invalid CRC of backup file')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_1} data files are corrupted')

        self.assertEqual('CORRUPT', self.pb.show('node', backup_id_1)['status'], 'Backup STATUS should be "CORRUPT"')
        self.assertEqual('ORPHAN', self.pb.show('node', backup_id_2)['status'], 'Backup STATUS should be "ORPHAN"')
        self.assertEqual('ORPHAN', self.pb.show('node', backup_id_3)['status'], 'Backup STATUS should be "ORPHAN"')
        self.assertEqual('OK', self.pb.show('node', backup_id_4)['status'], 'Backup STATUS should be "OK"')
        self.assertEqual('OK', self.pb.show('node', backup_id_5)['status'], 'Backup STATUS should be "OK"')

    # @unittest.skip("skip")
    def test_validate_corrupt_wal_1(self):
        """make archive node, take FULL1, PAGE1,PAGE2,FULL2,PAGE3,PAGE4 backups, corrupt all wal files, run validate, expect errors"""
        node = self.pg_node.make_simple('node')


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        backup_id_1 = self.pb.backup_node('node', node)

        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        backup_id_2 = self.pb.backup_node('node', node)

        # Corrupt WAL
        bla = b"blablablaadssaaaaaaaaaaaaaaa"
        for wal in self.get_instance_wal_list(self.backup_dir, 'node'):
            self.corrupt_instance_wal(self.backup_dir, 'node', wal, 42, bla)

        # Simple validate
        self.pb.validate('node', options=["-j", "4"],
                         expect_error="because of wal segments corruption")
        self.assertMessage(contains='WARNING: Backup')
        self.assertMessage(contains='WAL segments are corrupted')
        self.assertMessage(contains="WARNING: There are not enough WAL "
                                    "records to consistenly restore backup")

        self.assertEqual(
            'CORRUPT',
            self.pb.show('node', backup_id_1)['status'],
            'Backup STATUS should be "CORRUPT"')
        self.assertEqual(
            'CORRUPT',
            self.pb.show('node', backup_id_2)['status'],
            'Backup STATUS should be "CORRUPT"')

    # @unittest.skip("skip")
    def test_validate_corrupt_wal_2(self):
        """make archive node, make full backup, corrupt all wal files, run validate to real xid, expect errors"""
        node = self.pg_node.make_simple('node')


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        backup_id = self.pb.backup_node('node', node)
        target_xid = None
        with node.connect("postgres") as con:
            res = con.execute(
                "INSERT INTO tbl0005 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]

        # Corrupt WAL
        bla = b"blablablaadssaaaaaaaaaaaaaaa"
        for wal in self.get_instance_wal_list(self.backup_dir, 'node'):
            self.corrupt_instance_wal(self.backup_dir, 'node', wal, 128, bla)

        # Validate to xid
        self.pb.validate('node', backup_id,
                         options=[f"--xid={target_xid}", "-j", "4"],
                         expect_error="because of wal segments corruption")
        self.assertMessage(contains='WARNING: Backup')
        self.assertMessage(contains='WAL segments are corrupted')
        self.assertMessage(contains="WARNING: There are not enough WAL "
                                    "records to consistenly restore backup")

        self.assertEqual(
            'CORRUPT',
            self.pb.show('node', backup_id)['status'],
            'Backup STATUS should be "CORRUPT"')

    # @unittest.skip("skip")
    def test_validate_wal_lost_segment_1(self):
        """make archive node, make archive full backup,
        delete from archive wal segment which belong to previous backup
        run validate, expecting error because of missing wal segment
        make sure that backup status is 'CORRUPT'
        """
        node = self.pg_node.make_simple('node')


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        backup_id = self.pb.backup_node('node', node)

        # Delete wal segment
        wals = self.get_instance_wal_list(self.backup_dir, 'node')
        self.remove_instance_wal(self.backup_dir, 'node', max(wals))

        self.pb.validate('node', options=["-j", "4"],
                         expect_error="because of wal segment disappearance")
        self.assertMessage(contains="is absent")
        self.assertMessage(contains="WARNING: There are not enough WAL records to consistenly "
                                    f"restore backup {backup_id}")
        self.assertMessage(contains=f"WARNING: Backup {backup_id} WAL segments are corrupted")

        self.assertEqual(
            'CORRUPT',
            self.pb.show('node', backup_id)['status'],
            'Backup {0} should have STATUS "CORRUPT"')

        # Run validate again
        self.pb.validate('node', backup_id, options=["-j", "4"],
                         expect_error="because of backup corruption")
        self.assertMessage(contains=f'INFO: Revalidating backup {backup_id}')
        self.assertMessage(contains=f'ERROR: Backup {backup_id} is corrupt.')

    # @unittest.skip("skip")
    def test_validate_corrupt_wal_between_backups(self):
        """
        make archive node, make full backup, corrupt all wal files,
        run validate to real xid, expect errors
        """
        node = self.pg_node.make_simple('node')


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node)

        # make some wals
        node.pgbench_init(scale=3)

        with node.connect("postgres") as con:
            con.execute("CREATE TABLE tbl0005 (a text)")
            con.commit()

        with node.connect("postgres") as con:
            res = con.execute(
                "INSERT INTO tbl0005 VALUES ('inserted') RETURNING (xmin)")
            con.commit()
            target_xid = res[0][0]

        walfile = node.safe_psql(
            'postgres',
            'select pg_walfile_name(pg_current_wal_lsn())').decode('utf-8').rstrip()

        walfile = walfile + self.compress_suffix
        self.switch_wal_segment(node)

        # generate some wals
        node.pgbench_init(scale=3)

        self.pb.backup_node('node', node)

        # Corrupt WAL
        self.corrupt_instance_wal(self.backup_dir, 'node', walfile, 9000, b"b")

        # Validate to xid
        self.pb.validate('node', backup_id,
                         options=[f"--xid={target_xid}", "-j", "4"],
                         expect_error="because of wal segments corruption")
        self.assertMessage(contains='ERROR: Not enough WAL records to xid')
        self.assertMessage(contains='WARNING: Recovery can be done up to time')
        self.assertMessage(contains=f"ERROR: Not enough WAL records to xid {target_xid}")

        # Validate whole WAL Archive. It shouldn't be error, only warning in LOG. [PBCKP-55]
        self.pb.validate('node',
                         options=[f"--wal", "-j", "4"], expect_error=True)
        self.assertMessage(contains='ERROR: WAL archive check error')

        self.assertEqual(
            'OK',
            self.pb.show('node')[0]['status'],
            'Backup STATUS should be "OK"')

        self.assertEqual(
            'OK',
            self.pb.show('node')[1]['status'],
            'Backup STATUS should be "OK"')

    # @unittest.skip("skip")
    def test_pgpro702_688(self):
        """
        make node without archiving, make stream backup,
        get Recovery Time, validate to Recovery Time
        """
        node = self.pg_node.make_simple('node',
                                        set_replication=True)


        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node(
            'node', node, options=["--stream"])
        recovery_time = self.pb.show(
            'node', backup_id=backup_id)['recovery-time']

        self.pb.validate('node',
                         options=[f"--time={recovery_time}", "-j", "4"],
                         expect_error="because of wal segment disappearance")
        self.assertMessage(contains='WAL archive is empty. You cannot restore backup to a '
                                    'recovery target without WAL archive')

    # @unittest.skip("skip")
    def test_pgpro688(self):
        """
        make node with archiving, make backup, get Recovery Time,
        validate to Recovery Time. Waiting PGPRO-688. RESOLVED
        """
        node = self.pg_node.make_simple('node',
                                        set_replication=True)


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node)
        recovery_time = self.pb.show(
            'node', backup_id)['recovery-time']

        self.pb.validate(
            'node', options=["--time={0}".format(recovery_time),
                                         "-j", "4"])

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_pgpro561(self):
        """
        make node with archiving, make stream backup,
        restore it to node1, check that archiving is not successful on node1
        """
        node1 = self.pg_node.make_simple('node1', set_replication=True)


        self.pb.init()
        self.pb.add_instance('node1', node1)
        self.pb.set_archiving('node1', node1)
        node1.slow_start()

        backup_id = self.pb.backup_node('node1', node1, options=["--stream"])

        node2 = self.pg_node.make_simple('node2')
        node2.cleanup()

        node1.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,256) i")

        self.pb.backup_node(
            'node1', node1,
            backup_type='page', options=["--stream"])
        self.pb.restore_node('node1', node=node2)

        node2.set_auto_conf({'port': node2.port, 'archive_mode': 'off'})

        node2.slow_start()

        node2.set_auto_conf({'archive_mode': 'on'})

        node2.stop()
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
        # self.assertEqual(1, 0, 'Error is expected due to Master and Node1 having the common archive and archive_command')

        node1.psql(
            "postgres",
            "create table t_heap_1 as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10) i")

        self.switch_wal_segment(node1)

#        wals_dir = os.path.join(self.backup_dir, 'wal', 'node1')
#        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(
#            wals_dir, f)) and not f.endswith('.backup') and not f.endswith('.part')]
#        wals = map(str, wals)
#        print(wals)

        self.switch_wal_segment(node2)

#        wals_dir = os.path.join(self.backup_dir, 'wal', 'node1')
#        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(
#            wals_dir, f)) and not f.endswith('.backup') and not f.endswith('.part')]
#        wals = map(str, wals)
#        print(wals)

        time.sleep(5)

        log_file = os.path.join(node2.logs_dir, 'postgresql.log')
        with open(log_file, 'r') as f:
            log_content = f.read()
            self.assertTrue(
                'LOG:  archive command failed with exit code 1' in log_content and
                'DETAIL:  The failed archive command was:' in log_content and
                'WAL file already exists in archive with different checksum' in log_content,
                'Expecting error messages about failed archive_command'
            )
            self.assertFalse(
                'pg_probackup archive-push completed successfully' in log_content)

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
        node = self.pg_node.make_simple('node',
                                        set_replication=True,
                                        pg_options={
                'checkpoint_timeout': '30'})


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')
        self.pb.backup_node('node', node, backup_type='page')

        backup_id = self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')
        self.pb.backup_node('node', node, backup_type='page')

        node.safe_psql(
            "postgres",
            "alter system set archive_command = 'false'")
        node.reload()
        self.pb.backup_node('node', node, backup_type='page',
                            options=['--archive-timeout=1s'],
                            expect_error="because of data file dissapearance")

        self.assertTrue(
            self.pb.show('node')[6]['status'] == 'ERROR')
        self.pb.set_archiving('node', node)
        node.reload()
        self.pb.backup_node('node', node, backup_type='page')

        auto_conf = self.read_backup_file(self.backup_dir, 'node', backup_id,
                                          'database/postgresql.auto.conf')
        self.remove_backup_file(self.backup_dir, 'node', backup_id,
                                'database/postgresql.auto.conf')

        self.pb.validate(options=["-j", "4"],
                         expect_error="because of data file dissapearance")
        self.assertMessage(contains=f'Validating backup {backup_id}')
        self.assertMessage(contains=f'WARNING: Backup {backup_id} data files are corrupted')
        self.assertMessage(contains='WARNING: Some backups are not valid')

        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(
            self.pb.show('node')[3]['status'] == 'CORRUPT')
        self.assertTrue(
            self.pb.show('node')[4]['status'] == 'ORPHAN')
        self.assertTrue(
            self.pb.show('node')[5]['status'] == 'ORPHAN')
        self.assertTrue(
            self.pb.show('node')[6]['status'] == 'ERROR')
        self.assertTrue(
            self.pb.show('node')[7]['status'] == 'ORPHAN')

        self.write_backup_file(self.backup_dir, 'node', backup_id,
                               'database/postgresql.auto.conf', auto_conf)

        self.pb.validate(options=["-j", "4"],
                         expect_error=True)
        self.assertMessage(contains='WARNING: Some backups are not valid')

        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[3]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[5]['status'] == 'OK')
        self.assertTrue(
            self.pb.show('node')[6]['status'] == 'ERROR')
        self.assertTrue(self.pb.show('node')[7]['status'] == 'OK')

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
        node = self.pg_node.make_simple('node',
                                        set_replication=True)


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')
        self.pb.backup_node('node', node, backup_type='page')

        backup_id = self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')
        backup_id_page = self.pb.backup_node(
            'node', node, backup_type='page')
        self.pb.backup_node('node', node, backup_type='page')

        auto_conf = self.read_backup_file(self.backup_dir, 'node', backup_id,
                                          'database/postgresql.auto.conf')
        self.remove_backup_file(self.backup_dir, 'node', backup_id,
                                'database/postgresql.auto.conf')

        self.pb.validate(options=["-j", "4"],
                         expect_error="because of data file dissapearance")
        self.assertMessage(contains=f'Validating backup {backup_id}')
        self.assertMessage(contains=f'WARNING: Backup {backup_id} data files are corrupted')
        self.assertMessage(contains='WARNING: Some backups are not valid')

        self.assertTrue(self.pb.show('node')[6]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[5]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[3]['status'] == 'CORRUPT')
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')

        self.write_backup_file(self.backup_dir, 'node', backup_id,
                               'database/postgresql.auto.conf', auto_conf)

        self.remove_backup_file(self.backup_dir, 'node', backup_id_page,
                                'database/backup_label')

        self.pb.validate(options=["-j", "4"],
                         expect_error=True)
        self.assertMessage(contains='WARNING: Some backups are not valid')

        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[3]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[5]['status'] == 'CORRUPT')
        self.assertTrue(self.pb.show('node')[6]['status'] == 'ORPHAN')

    # @unittest.skip("skip")
    def test_validate_corrupted_full_2(self):
        """
        PAGE2_2b
        PAGE2_2a
        PAGE2_4
        PAGE2_4 <- validate
        PAGE2_3
        PAGE2_2 <- CORRUPT
        PAGE2_1
        FULL2
        PAGE1_1
        FULL1
        corrupt second page backup, run validate on PAGE2_3, check that
        PAGE2_2 became CORRUPT and his descendants are ORPHANs,
        take two more PAGE backups, which now trace their origin
        to PAGE2_1 - latest OK backup,
        run validate on PAGE2_3, check that PAGE2_2a and PAGE2_2b are OK,

        remove corruption from PAGE2_2 and run validate on PAGE2_4
        """

        node = self.pg_node.make_simple('node',
                                        set_replication=True)


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')

        backup_id_3 = self.pb.backup_node('node', node)
        backup_id_4 = self.pb.backup_node('node', node, backup_type='page')
        corrupt_id = self.pb.backup_node(
            'node', node, backup_type='page')
        backup_id_6 = self.pb.backup_node('node', node, backup_type='page')
        validate_id = self.pb.backup_node(
            'node', node, backup_type='page')
        backup_id_8 = self.pb.backup_node('node', node, backup_type='page')

        backup_label = self.read_backup_file(self.backup_dir, 'node', corrupt_id,
                                             'database/backup_label')
        self.remove_backup_file(self.backup_dir, 'node', corrupt_id,
                                'database/backup_label')

        self.pb.validate('node', validate_id, options=["-j", "4"],
                         expect_error="because of data file dissapearance")
        self.assertMessage(contains=f'INFO: Validating parents for backup {validate_id}')
        self.assertMessage(contains=f'INFO: Validating backup {backup_id_3}')
        self.assertMessage(contains=f'INFO: Validating backup {backup_id_4}')
        self.assertMessage(contains=f'INFO: Validating backup {corrupt_id}')
        self.assertMessage(contains=f'WARNING: Backup {corrupt_id} data files are corrupted')

        self.assertTrue(self.pb.show('node')[7]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[6]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[5]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'CORRUPT')
        self.assertTrue(self.pb.show('node')[3]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')

        # THIS IS GOLD!!!!
        backup_id_9 = self.pb.backup_node('node', node, backup_type='page')
        backup_id_a = self.pb.backup_node('node', node, backup_type='page')

        self.pb.validate('node', options=["-j", "4"],
                         expect_error="because of data file dissapearance")
        self.assertMessage(contains=f'Backup {backup_id_a} data files are valid')
        self.assertMessage(contains=f'Backup {backup_id_9} data files are valid')
        self.assertMessage(regex=f'WARNING: Backup {backup_id_8} .* parent {corrupt_id} .* CORRUPT')
        self.assertMessage(regex=f'WARNING: Backup {validate_id} .* parent {corrupt_id} .* CORRUPT')
        self.assertMessage(regex=f'WARNING: Backup {backup_id_6} .* parent {corrupt_id} .* CORRUPT')
        self.assertMessage(contains=f'INFO: Revalidating backup {corrupt_id}')
        self.assertMessage(contains='WARNING: Some backups are not valid')

        self.assertTrue(self.pb.show('node')[9]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[8]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[7]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[6]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[5]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'CORRUPT')
        self.assertTrue(self.pb.show('node')[3]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')

        # revalidate again

        self.pb.validate('node', validate_id, options=["-j", "4"],
                         expect_error="because of data file dissapearance")
        self.assertMessage(contains=f'WARNING: Backup {validate_id} has status: ORPHAN')
        self.assertMessage(contains=f'Backup {backup_id_8} has parent {corrupt_id} with status: CORRUPT')
        self.assertMessage(contains=f'Backup {validate_id} has parent {corrupt_id} with status: CORRUPT')
        self.assertMessage(contains=f'Backup {backup_id_6} has parent {corrupt_id} with status: CORRUPT')
        self.assertMessage(contains=f'INFO: Validating parents for backup {validate_id}')
        self.assertMessage(contains=f'INFO: Validating backup {backup_id_3}')
        self.assertMessage(contains=f'INFO: Validating backup {backup_id_4}')
        self.assertMessage(contains=f'INFO: Revalidating backup {corrupt_id}')
        self.assertMessage(contains=f'WARNING: Backup {corrupt_id} data files are corrupted')
        self.assertMessage(contains=f'ERROR: Backup {validate_id} is orphan.')

        # Fix CORRUPT
        self.write_backup_file(self.backup_dir, 'node', corrupt_id,
                               'database/backup_label', backup_label)

        self.pb.validate('node', validate_id, options=["-j", "4"])

        self.assertMessage(contains=f'WARNING: Backup {validate_id} has status: ORPHAN')
        self.assertMessage(contains=f'Backup {backup_id_8} has parent {corrupt_id} with status: CORRUPT')
        self.assertMessage(contains=f'Backup {validate_id} has parent {corrupt_id} with status: CORRUPT')
        self.assertMessage(contains=f'Backup {backup_id_6} has parent {corrupt_id} with status: CORRUPT')
        self.assertMessage(contains=f'INFO: Validating parents for backup {validate_id}')
        self.assertMessage(contains=f'INFO: Validating backup {backup_id_3}')
        self.assertMessage(contains=f'INFO: Validating backup {backup_id_4}')
        self.assertMessage(contains=f'INFO: Revalidating backup {corrupt_id}')
        self.assertMessage(contains=f'Backup {corrupt_id} data files are valid')
        self.assertMessage(contains=f'INFO: Revalidating backup {backup_id_6}')
        self.assertMessage(contains=f'Backup {backup_id_6} data files are valid')
        self.assertMessage(contains=f'INFO: Revalidating backup {validate_id}')
        self.assertMessage(contains=f'Backup {validate_id} data files are valid')
        self.assertMessage(contains=f'INFO: Backup {validate_id} WAL segments are valid')
        self.assertMessage(contains=f'INFO: Backup {validate_id} is valid.')
        self.assertMessage(contains=f'INFO: Validate of backup {validate_id} completed.')

        # Now we have two perfectly valid backup chains based on FULL2

        self.assertTrue(self.pb.show('node')[9]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[8]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[7]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[6]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[5]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[3]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')

    # @unittest.skip("skip")
    def test_validate_corrupted_full_missing(self):
        """
        make node with archiving, take full backup, and three page backups,
        take another full backup and four page backups
        corrupt second full backup, run validate, check that
        second full backup became CORRUPT and his page backups are ORPHANs
        remove corruption from full backup and remove his second page backup
        run valudate again, check that
        second full backup and his firts page backups are OK,
        third page should be ORPHAN
        """
        node = self.pg_node.make_simple('node',
                                        set_replication=True)


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')
        self.pb.backup_node('node', node, backup_type='page')
        self.pb.backup_node('node', node, backup_type='page')

        backup_id = self.pb.backup_node('node', node)
        backup_id_6 = self.pb.backup_node('node', node, backup_type='page')
        backup_id_page = self.pb.backup_node('node', node, backup_type='page')
        backup_id_8 = self.pb.backup_node('node', node, backup_type='page')
        backup_id_9 = self.pb.backup_node('node', node, backup_type='page')

        auto_conf = self.read_backup_file(self.backup_dir, 'node', backup_id,
                                          'database/postgresql.auto.conf')
        self.remove_backup_file(self.backup_dir, 'node', backup_id,
                                'database/postgresql.auto.conf')

        self.pb.validate(options=["-j", "4"],
                         expect_error="because of data file dissapearance")
        self.assertMessage(contains=f'Validating backup {backup_id}')
        self.assertMessage(contains=f'WARNING: Backup {backup_id} data files are corrupted')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_6} is orphaned because his parent {backup_id} has status: CORRUPT')

        self.assertTrue(self.pb.show('node')[8]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[7]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[6]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[5]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'CORRUPT')
        self.assertTrue(self.pb.show('node')[3]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')

        # Full backup is fixed
        self.write_backup_file(self.backup_dir, 'node', backup_id,
                               'database/postgresql.auto.conf', auto_conf)

        # break PAGE
        self.change_backup_status(self.backup_dir, 'node', backup_id_page,
                                  'THIS_BACKUP_IS_HIDDEN_FOR_TESTS')

        self.pb.validate(options=["-j", "4"],
                         expect_error="because backup in chain is removed")
        self.assertMessage(contains='WARNING: Some backups are not valid')
        self.assertMessage(regex=fr'WARNING: Backup {backup_id_9} (.*(missing|parent {backup_id_page})){{2}}')
        self.assertMessage(regex=fr'WARNING: Backup {backup_id_8} (.*(missing|parent {backup_id_page})){{2}}')
        self.assertMessage(contains=f'INFO: Backup {backup_id_6} WAL segments are valid')

        self.assertTrue(self.pb.show('node')[7]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[6]['status'] == 'ORPHAN')
        # missing backup is here
        self.assertTrue(self.pb.show('node')[5]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[3]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')

        # validate should be idempotent - user running validate
        # second time must be provided with ID of missing backup

        self.pb.validate(options=["-j", "4"],
                         expect_error=True)
        self.assertMessage(contains='WARNING: Some backups are not valid')
        self.assertMessage(regex=fr'WARNING: Backup {backup_id_9} (.*(missing|parent {backup_id_page})){{2}}')
        self.assertMessage(regex=fr'WARNING: Backup {backup_id_8} (.*(missing|parent {backup_id_page})){{2}}')

        self.assertTrue(self.pb.show('node')[7]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[6]['status'] == 'ORPHAN')
        # missing backup is here
        self.assertTrue(self.pb.show('node')[5]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[3]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')

        # fix missing PAGE backup
        self.change_backup_status(self.backup_dir, 'node', backup_id_page, 'ORPHAN')
        # exit(1)

        self.assertTrue(self.pb.show('node')[8]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[7]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[6]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[5]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[3]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')

        self.pb.validate(options=["-j", "4"])

        self.assertMessage(contains='INFO: All backups are valid')
        self.assertMessage(contains=f'Revalidating backup {backup_id_page}')
        self.assertMessage(contains=f'Revalidating backup {backup_id_8}')
        self.assertMessage(contains=f'Revalidating backup {backup_id_9}')

        self.assertTrue(self.pb.show('node')[8]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[7]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[6]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[5]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[3]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')

    def test_file_size_corruption_no_validate(self):

        node = self.pg_node.make_simple('node', checksum=False)



        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)

        node.slow_start()

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
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()
        heap_size = node.safe_psql(
            "postgres",
            "select pg_relation_size('t_heap')")

        backup_id = self.pb.backup_node(
            'node', node, backup_type="full",
            options=["-j", "4"], gdb=False)

        node.stop()
        node.cleanup()

        # Let`s do file corruption
        self.corrupt_backup_file(self.backup_dir, 'node', backup_id,
            os.path.join("database", heap_path),
            truncate=(int(heap_size) - 4096))

        node.cleanup()

        self.pb.restore_node('node', node=node, options=["--no-validate"],
                             expect_error=True)
        self.assertMessage(contains="ERROR: Backup files restoring failed")

    # @unittest.skip("skip")
    def test_validate_specific_backup_with_missing_backup(self):
        """
        PAGE3_2
        PAGE3_1
        FULL3
        PAGE2_5
        PAGE2_4 <- validate
        PAGE2_3
        PAGE2_2 <- missing
        PAGE2_1
        FULL2
        PAGE1_2
        PAGE1_1
        FULL1
        """
        node = self.pg_node.make_simple('node',
                                        set_replication=True)


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # CHAIN1
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')
        self.pb.backup_node('node', node, backup_type='page')

        # CHAIN2
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')
        missing_id = self.pb.backup_node(
            'node', node, backup_type='page')
        backup_id_6 = self.pb.backup_node('node', node, backup_type='page')
        validate_id = self.pb.backup_node(
            'node', node, backup_type='page')
        backup_id_8 = self.pb.backup_node('node', node, backup_type='page')

        # CHAIN3
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')
        self.pb.backup_node('node', node, backup_type='page')

        self.change_backup_status(self.backup_dir, 'node', missing_id,
                                  "THIS_BACKUP_IS_HIDDEN_FOR_TESTS")

        self.pb.validate('node', validate_id, options=["-j", "4"],
                         expect_error="because of backup dissapearance")
        self.assertMessage(contains=f'WARNING: Backup {backup_id_8} is orphaned '
                                    f'because his parent {missing_id} is missing')
        self.assertMessage(contains=f'WARNING: Backup {validate_id} is orphaned '
                                    f'because his parent {missing_id} is missing')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_6} is orphaned '
                                    f'because his parent {missing_id} is missing')

        self.assertTrue(self.pb.show('node')[10]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[9]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[8]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[7]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[6]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[5]['status'] == 'ORPHAN')
        # missing backup
        self.assertTrue(self.pb.show('node')[4]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[3]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')

        self.pb.validate('node', validate_id, options=["-j", "4"],
                         expect_error="because of backup dissapearance")
        self.assertMessage(contains=f'WARNING: Backup {backup_id_8} has missing '
                                    f'parent {missing_id}')
        self.assertMessage(contains=f'WARNING: Backup {validate_id} has missing '
                                    f'parent {missing_id}')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_6} has missing '
                                    f'parent {missing_id}')

        self.change_backup_status(self.backup_dir, 'node', missing_id, "OK")

        # Revalidate backup chain
        self.pb.validate('node', validate_id, options=["-j", "4"])

        self.assertTrue(self.pb.show('node')[11]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[10]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[9]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[8]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[7]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[6]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[5]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[3]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')

    # @unittest.skip("skip")
    def test_validate_specific_backup_with_missing_backup_1(self):
        """
        PAGE3_2
        PAGE3_1
        FULL3
        PAGE2_5
        PAGE2_4 <- validate
        PAGE2_3
        PAGE2_2 <- missing
        PAGE2_1
        FULL2   <- missing
        PAGE1_2
        PAGE1_1
        FULL1
        """
        node = self.pg_node.make_simple('node',
                                        set_replication=True)


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # CHAIN1
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')
        self.pb.backup_node('node', node, backup_type='page')

        # CHAIN2
        missing_full_id = self.pb.backup_node('node', node)
        backup_id_5 = self.pb.backup_node('node', node, backup_type='page')
        missing_page_id = self.pb.backup_node(
            'node', node, backup_type='page')
        backup_id_7 = self.pb.backup_node('node', node, backup_type='page')
        validate_id = self.pb.backup_node(
            'node', node, backup_type='page')
        backup_id_9 = self.pb.backup_node('node', node, backup_type='page')

        # CHAIN3
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')
        self.pb.backup_node('node', node, backup_type='page')

        self.change_backup_status(self.backup_dir, 'node', missing_page_id,
                                  "THIS_BACKUP_IS_HIDDEN_FOR_TESTS")
        self.change_backup_status(self.backup_dir, 'node', missing_full_id,
                                  "THIS_BACKUP_IS_HIDDEN_FOR_TESTS")

        self.pb.validate('node', validate_id, options=["-j", "4"],
                         expect_error="because of backup dissapearance")
        self.assertMessage(contains=f'WARNING: Backup {backup_id_9} is orphaned '
                                    f'because his parent {missing_page_id} is missing')
        self.assertMessage(contains=f'WARNING: Backup {validate_id} is orphaned '
                                    f'because his parent {missing_page_id} is missing')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_7} is orphaned '
                                    f'because his parent {missing_page_id} is missing')

        self.assertTrue(self.pb.show('node')[9]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[8]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[7]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[6]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[5]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'ORPHAN')
        # PAGE2_1
        self.assertTrue(self.pb.show('node')[3]['status'] == 'OK') # <- SHit
        # FULL2
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')

        self.change_backup_status(self.backup_dir, 'node', missing_page_id, "OK")
        self.change_backup_status(self.backup_dir, 'node', missing_full_id, "OK")

        # Revalidate backup chain
        self.pb.validate('node', validate_id, options=["-j", "4"])

        self.assertTrue(self.pb.show('node')[11]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[10]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[9]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[8]['status'] == 'ORPHAN') # <- Fail
        self.assertTrue(self.pb.show('node')[7]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[6]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[5]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[3]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')

    # @unittest.skip("skip")
    def test_validate_with_missing_backup_1(self):
        """
        PAGE3_2
        PAGE3_1
        FULL3
        PAGE2_5
        PAGE2_4 <- validate
        PAGE2_3
        PAGE2_2 <- missing
        PAGE2_1
        FULL2   <- missing
        PAGE1_2
        PAGE1_1
        FULL1
        """
        node = self.pg_node.make_simple('node',
                                        set_replication=True)


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # CHAIN1
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')
        self.pb.backup_node('node', node, backup_type='page')

        # CHAIN2
        missing_full_id = self.pb.backup_node('node', node)
        backup_id_5 = self.pb.backup_node('node', node, backup_type='page')
        missing_page_id = self.pb.backup_node(
            'node', node, backup_type='page')
        backup_id_7 = self.pb.backup_node('node', node, backup_type='page')
        validate_id = self.pb.backup_node(
            'node', node, backup_type='page')
        backup_id_9 = self.pb.backup_node('node', node, backup_type='page')

        # CHAIN3
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')
        self.pb.backup_node('node', node, backup_type='page')

        # Break PAGE
        self.change_backup_status(self.backup_dir, 'node', missing_page_id,
                                  'THIS_BACKUP_IS_HIDDEN_FOR_TESTS')

        # Break FULL
        self.change_backup_status(self.backup_dir, 'node', missing_full_id,
                                  'THIS_BACKUP_IS_HIDDEN_FOR_TESTS')

        self.pb.validate('node', validate_id, options=["-j", "4"],
                         expect_error="because of backup dissapearance")
        self.assertMessage(contains=f'WARNING: Backup {backup_id_9} is orphaned '
                                    f'because his parent {missing_page_id} is missing')
        self.assertMessage(contains=f'WARNING: Backup {validate_id} is orphaned '
                                    f'because his parent {missing_page_id} is missing')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_7} is orphaned '
                                    f'because his parent {missing_page_id} is missing')

        self.assertTrue(self.pb.show('node')[9]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[8]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[7]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[6]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[5]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'ORPHAN')
        # PAGE2_2 is missing
        self.assertTrue(self.pb.show('node')[3]['status'] == 'OK')
        # FULL1 - is missing
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')

        self.change_backup_status(self.backup_dir, 'node', missing_page_id, 'OK')

        # Revalidate backup chain
        self.pb.validate('node', validate_id, options=["-j", "4"],
                         expect_error="because of backup dissapearance")
        self.assertMessage(contains=f'WARNING: Backup {validate_id} has status: ORPHAN')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_9} has missing '
                                    f'parent {missing_full_id}')
        self.assertMessage(contains=f'WARNING: Backup {validate_id} has missing '
                                    f'parent {missing_full_id}')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_7} has missing '
                                    f'parent {missing_full_id}')
        self.assertMessage(contains=f'WARNING: Backup {missing_page_id} is orphaned '
                                    f'because his parent {missing_full_id} is missing')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_5} is orphaned '
                                    f'because his parent {missing_full_id} is missing')

        self.assertTrue(self.pb.show('node')[10]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[9]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[8]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[7]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[6]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[5]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[3]['status'] == 'ORPHAN')
        # FULL1 - is missing
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')

        self.change_backup_status(self.backup_dir, 'node', missing_full_id, 'OK')

        # Revalidate chain
        self.pb.validate('node', validate_id, options=["-j", "4"])

        self.assertTrue(self.pb.show('node')[11]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[10]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[9]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[8]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[7]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[6]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[5]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[3]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')

    # @unittest.skip("skip")
    def test_validate_with_missing_backup_2(self):
        """
        PAGE3_2
        PAGE3_1
        FULL3
        PAGE2_5
        PAGE2_4
        PAGE2_3
        PAGE2_2 <- missing
        PAGE2_1
        FULL2   <- missing
        PAGE1_2
        PAGE1_1
        FULL1
        """
        node = self.pg_node.make_simple('node',
                                        set_replication=True)


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # CHAIN1
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')
        self.pb.backup_node('node', node, backup_type='page')

        # CHAIN2
        missing_full_id = self.pb.backup_node('node', node)
        backup_id_5 = self.pb.backup_node('node', node, backup_type='page')
        missing_page_id = self.pb.backup_node(
            'node', node, backup_type='page')
        backup_id_7 = self.pb.backup_node('node', node, backup_type='page')
        backup_id_8 = self.pb.backup_node('node', node, backup_type='page')
        backup_id_9 = self.pb.backup_node('node', node, backup_type='page')

        # CHAIN3
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type='page')
        self.pb.backup_node('node', node, backup_type='page')

        self.change_backup_status(self.backup_dir, 'node', missing_page_id,
                                  'THIS_BACKUP_IS_HIDDEN_FOR_TESTS')
        self.change_backup_status(self.backup_dir, 'node', missing_full_id,
                                  'THIS_BACKUP_IS_HIDDEN_FOR_TESTS')

        self.pb.validate('node', options=["-j", "4"],
                         expect_error="because of backup dissapearance")
        self.assertMessage(contains=f'WARNING: Backup {backup_id_9} is orphaned '
                                    f'because his parent {missing_page_id} is missing')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_8} is orphaned '
                                    f'because his parent {missing_page_id} is missing')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_7} is orphaned '
                                    f'because his parent {missing_page_id} is missing')
        self.assertMessage(contains=f'WARNING: Backup {backup_id_5} is orphaned '
                                    f'because his parent {missing_full_id} is missing')

        self.assertTrue(self.pb.show('node')[9]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[8]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[7]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[6]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[5]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'ORPHAN')
        # PAGE2_2 is missing
        self.assertTrue(self.pb.show('node')[3]['status'] == 'ORPHAN')
        # FULL1 - is missing
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')

        self.change_backup_status(self.backup_dir, 'node', missing_page_id, 'OK')

        # Revalidate backup chain
        self.pb.validate('node', options=["-j", "4"],
                         expect_error="because of backup dissapearance")
        self.assertMessage(regex=fr'WARNING: Backup {backup_id_9} (.*(missing|parent {missing_full_id})){{2}}')
        self.assertMessage(regex=fr'WARNING: Backup {backup_id_8} (.*(missing|parent {missing_full_id})){{2}}')
        self.assertMessage(regex=fr'WARNING: Backup {backup_id_7} (.*(missing|parent {missing_full_id})){{2}}')
        self.assertMessage(regex=fr'WARNING: Backup {missing_page_id} (.*(missing|parent {missing_full_id})){{2}}')
        self.assertMessage(regex=fr'WARNING: Backup {backup_id_5} (.*(missing|parent {missing_full_id})){{2}}')

        self.assertTrue(self.pb.show('node')[10]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[9]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[8]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[7]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[6]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[5]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[4]['status'] == 'ORPHAN')
        self.assertTrue(self.pb.show('node')[3]['status'] == 'ORPHAN')
        # FULL1 - is missing
        self.assertTrue(self.pb.show('node')[2]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[1]['status'] == 'OK')
        self.assertTrue(self.pb.show('node')[0]['status'] == 'OK')

    # @unittest.skip("skip")
    def test_corrupt_pg_control_via_resetxlog(self):
        """ PGPRO-2096 """

        if not self.backup_dir.is_file_based:
            self.skipTest('tests uses pg_resetxlog on backup')

        node = self.pg_node.make_simple('node',
                                        set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node)

        pg_resetxlog_path = self.get_bin_path('pg_resetwal')
        wal_dir = 'pg_wal'

        os.mkdir(
            os.path.join(
                self.backup_dir, 'backups', 'node', backup_id, 'database', wal_dir, 'archive_status'))

        pg_control_path = os.path.join(
            self.backup_dir, 'backups', 'node',
            backup_id, 'database', 'global', 'pg_control')

        md5_before = hashlib.md5(
            open(pg_control_path, 'rb').read()).hexdigest()

        self.pb.run_binary(
            [
                pg_resetxlog_path,
                '-D',
                os.path.join(self.backup_dir, 'backups', 'node', backup_id, 'database'),
                '-o 42',
                '-f'
            ],
            asynchronous=False)

        md5_after = hashlib.md5(
            open(pg_control_path, 'rb').read()).hexdigest()

        if self.verbose:
            print('\n MD5 BEFORE resetxlog: {0}\n MD5 AFTER resetxlog: {1}'.format(
                md5_before, md5_after))

        # Validate backup
        self.pb.validate('node', options=["-j", "4"],
                         expect_error="because of pg_control change")
        self.assertMessage(contains='data files are corrupted')

    @needs_gdb
    def test_validation_after_backup(self):
        """"""
        node = self.pg_node.make_simple('node',
                                        set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # FULL backup
        gdb = self.pb.backup_node(
            'node', node, gdb=True, options=['--stream'])

        gdb.set_breakpoint('pgBackupValidate')
        gdb.run_until_break()

        backup_id = self.pb.show('node')[0]['id']

        self.remove_backup_file(self.backup_dir, 'node', backup_id,
                                'database/postgresql.conf')

        gdb.continue_execution_until_exit()

        self.assertEqual(
            'CORRUPT',
            self.pb.show('node', backup_id)['status'],
            'Backup STATUS should be "ERROR"')

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_validate_corrupt_tablespace_map(self):
        """
        Check that corruption in tablespace_map is detected
        """

        node = self.pg_node.make_simple('node',
                                        set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'external_dir')

        node.safe_psql(
            'postgres',
            'CREATE TABLE t_heap(a int) TABLESPACE "external_dir"')

        # FULL backup
        backup_id = self.pb.backup_node(
            'node', node, options=['--stream'])

        # Corrupt tablespace_map file in FULL backup
        self.corrupt_backup_file(self.backup_dir, 'node', backup_id,
                                 'database/tablespace_map', damage=(84,b"blah"))

        self.pb.validate('node', backup_id=backup_id,
                         expect_error="because tablespace_map is corrupted")
        self.assertMessage(contains='WARNING: Invalid CRC of backup file')

    def test_validate_target_lsn(self):
        """
        Check validation to specific LSN from "forked" backup
        """

        node = self.pg_node.make_simple('node',
                                        set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # FULL backup
        self.pb.backup_node('node', node)

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,10000) i")

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored)

        node_restored.set_auto_conf({'port': node_restored.port})

        node_restored.slow_start()

        self.switch_wal_segment(node)

        self.pb.backup_node(
            'node', node_restored,
            data_dir=node_restored.data_dir)

        target_lsn = self.pb.show('node')[1]['stop-lsn']
        
        self.pb.validate(
                'node',
                options=[
                    '--recovery-target-timeline=2',
                    '--recovery-target-lsn={0}'.format(target_lsn)])

    def test_partial_validate_empty_and_mangled_database_map(self):
        """
        """

        node = self.pg_node.make_simple('node',
                                        set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)

        node.slow_start()

        # create databases
        for i in range(1, 10, 1):
            node.safe_psql(
                'postgres',
                'CREATE database db{0}'.format(i))

        # FULL backup with database_map
        backup_id = self.pb.backup_node(
            'node', node, options=['--stream'])

        # truncate database_map
        self.corrupt_backup_file(self.backup_dir, 'node', backup_id,
                                 'database/database_map', truncate=0)

        self.pb.validate('node', backup_id,
                         options=["--db-include=db1"],
                         expect_error="because database_map is empty")
        self.assertMessage(contains=f"WARNING: Backup {backup_id} data files are corrupted")

        # mangle database_map
        self.corrupt_backup_file(self.backup_dir, 'node', backup_id,
                                 'database/database_map', overwrite=b'42')

        self.pb.validate('node', backup_id,
                         options=["--db-include=db1"],
                         expect_error="because database_map is mangled")
        self.assertMessage(contains=f"WARNING: Backup {backup_id} data files are corrupted")

    def test_partial_validate_exclude(self):
        """"""

        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        for i in range(1, 10, 1):
            node.safe_psql(
                'postgres',
                'CREATE database db{0}'.format(i))

        # FULL backup
        backup_id = self.pb.backup_node('node', node)

        self.pb.validate('node',
                         options=["--db-include=db1", "--db-exclude=db2"],
                         expect_error="because of 'db-exclude' and 'db-include'")
        self.assertMessage(contains="ERROR: You cannot specify '--db-include' "
                                    "and '--db-exclude' together")

        self.pb.validate('node',
                         options=[
                             "--db-exclude=db1",
                             "--db-exclude=db5",
                             "--log-level-console=verbose"],
                         expect_error="because of missing backup ID")
        self.assertMessage(contains="ERROR: You must specify parameter (-i, --backup-id) for partial validation")

        self.pb.validate(
            'node', backup_id,
            options=[
                "--db-exclude=db1",
                "--db-exclude=db5"])

    def test_partial_validate_include(self):
        """
        """

        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        for i in range(1, 10, 1):
            node.safe_psql(
                'postgres',
                'CREATE database db{0}'.format(i))

        # FULL backup
        backup_id = self.pb.backup_node('node', node)

        self.pb.validate('node',
                         options=["--db-include=db1", "--db-exclude=db2"],
                         expect_error="because of 'db-exclude' and 'db-include'")
        self.assertMessage(contains="ERROR: You cannot specify '--db-include' "
                                    "and '--db-exclude' together")

        self.pb.validate(
            'node', backup_id,
            options=[
                "--db-include=db1",
                "--db-include=db5",
                "--db-include=postgres"])

        self.pb.validate(
            'node', backup_id,
            options=[])

    # @unittest.skip("skip")
    def test_not_validate_diffenent_pg_version(self):
        """Do not validate backup, if binary is compiled with different PG version"""
        node = self.pg_node.make_simple('node')


        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node)

        pg_version = node.major_version

        if pg_version.is_integer():
            pg_version = int(pg_version)

        fake_new_pg_version = pg_version + 1

        with self.modify_backup_control(self.backup_dir, 'node', backup_id) as cf:
            cf.data = cf.data.replace(
                "server-version = {0}".format(str(pg_version)),
                "server-version = {0}".format(str(fake_new_pg_version))
            )

        self.pb.validate(expect_error="because validation is forbidden if server version of backup "
                                    "is different from the server version of pg_probackup.")
        self.assertMessage(contains=f"ERROR: Backup {backup_id} has server version")

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_validate_corrupt_page_header_map(self):
        """
        Check that corruption in page_header_map is detected
        """

        node = self.pg_node.make_simple('node',
                                        set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        ok_1 = self.pb.backup_node('node', node, options=['--stream'])

        # FULL backup
        backup_id = self.pb.backup_node(
            'node', node, options=['--stream'])

        ok_2 = self.pb.backup_node('node', node, options=['--stream'])

        self.corrupt_backup_file(self.backup_dir, 'node', backup_id,
                                 'page_header_map', damage=(42, b"blah"))

        self.pb.validate('node', backup_id=backup_id,
                         expect_error="because page_header_map is corrupted")

        self.assertMessage(regex=
            r'WARNING: An error occured during metadata decompression for file "[\w/]+": (data|buffer) error')

        self.assertMessage(contains=f"Backup {backup_id} is corrupt")

        self.pb.validate(expect_error="because page_header_map is corrupted")

        self.assertMessage(regex=
                           r'WARNING: An error occured during metadata decompression for file "[\w/]+": (data|buffer) error')

        self.assertMessage(contains=f"INFO: Backup {ok_1} data files are valid")
        self.assertMessage(contains=f"WARNING: Backup {backup_id} data files are corrupted")
        self.assertMessage(contains=f"INFO: Backup {ok_2} data files are valid")
        self.assertMessage(contains="WARNING: Some backups are not valid")

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_validate_truncated_page_header_map(self):
        """
        Check that corruption in page_header_map is detected
        """

        node = self.pg_node.make_simple('node',
                                        set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        ok_1 = self.pb.backup_node('node', node, options=['--stream'])

        # FULL backup
        backup_id = self.pb.backup_node(
            'node', node, options=['--stream'])

        ok_2 = self.pb.backup_node('node', node, options=['--stream'])

        self.corrupt_backup_file(self.backup_dir, 'node', backup_id,
                                 'page_header_map', truncate=121)

        self.pb.validate('node', backup_id=backup_id,
                         expect_error="because page_header_map is corrupted")
        self.assertMessage(contains=f'ERROR: Backup {backup_id} is corrupt')

        self.pb.validate(expect_error="because page_header_map is corrupted")
        self.assertMessage(contains=f"INFO: Backup {ok_1} data files are valid")
        self.assertMessage(contains=f"WARNING: Backup {backup_id} data files are corrupted")
        self.assertMessage(contains=f"INFO: Backup {ok_2} data files are valid")
        self.assertMessage(contains="WARNING: Some backups are not valid")

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_validate_missing_page_header_map(self):
        """
        Check that corruption in page_header_map is detected
        """

        node = self.pg_node.make_simple('node',
                                        set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        ok_1 = self.pb.backup_node('node', node, options=['--stream'])

        # FULL backup
        backup_id = self.pb.backup_node(
            'node', node, options=['--stream'])

        ok_2 = self.pb.backup_node('node', node, options=['--stream'])

        self.remove_backup_file(self.backup_dir, 'node', backup_id,
                                'page_header_map')

        self.pb.validate('node', backup_id=backup_id,
                         expect_error="because page_header_map is missing")
        self.assertMessage(contains=f'ERROR: Backup {backup_id} is corrupt')

        self.pb.validate(expect_error="because page_header_map is missing")
        self.assertMessage(contains=f"INFO: Backup {ok_1} data files are valid")
        self.assertMessage(contains=f"WARNING: Backup {backup_id} data files are corrupted")
        self.assertMessage(contains=f"INFO: Backup {ok_2} data files are valid")
        self.assertMessage(contains="WARNING: Some backups are not valid")

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_no_validate_tablespace_map(self):
        """
        Check that --no-validate is propagated to tablespace_map
        """

        node = self.pg_node.make_simple('node',
                                        set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        self.create_tblspace_in_node(node, 'external_dir')

        node.safe_psql(
            'postgres',
            'CREATE TABLE t_heap(a int) TABLESPACE "external_dir"')

        tblspace_new = self.get_tblspace_path(node, 'external_dir_new')

        oid = node.safe_psql(
            'postgres',
            "select oid from pg_tablespace where spcname = 'external_dir'").decode('utf-8').rstrip()

        # FULL backup
        backup_id = self.pb.backup_node(
            'node', node, options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        self.corrupt_backup_file(self.backup_dir, 'node', backup_id,
                                 'database/tablespace_map',
                                 overwrite="{0} {1}".format(oid, tblspace_new),
                                 text=True)

        node.cleanup()

        self.pb.restore_node('node', node, options=['--no-validate'])

        pgdata_restored = self.pgdata_content(node.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # check that tablespace restore as symlink
        tablespace_link = os.path.join(node.data_dir, 'pg_tblspc', oid)
        self.assertTrue(
            os.path.islink(tablespace_link),
            "'%s' is not a symlink" % tablespace_link)

        self.assertEqual(
            os.readlink(tablespace_link),
            tblspace_new,
            "Symlink '{0}' do not points to '{1}'".format(tablespace_link, tblspace_new))

    def test_custom_wal_segsize(self):
        """
        Check that we can validate a specific instance or a whole catalog
        having a custom wal segment size.
        """
        node = self.pg_node.make_simple('node',
                                        initdb_params=['--wal-segsize=64'],
                                        pg_options={'min_wal_size': '128MB'})
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        self.pb.backup_node('node', node, options=['--stream'])

        self.pb.validate('node')
        self.pb.validate()

# validate empty backup list
# page from future during validate
# page from future during backup

# corrupt block, so file become unaligned:
# 712                     Assert(header.compressed_size <= BLCKSZ);
# 713
# 714                     read_len = fread(compressed_page.data, 1,
# 715                             MAXALIGN(header.compressed_size), in);
# 716                     if (read_len != MAXALIGN(header.compressed_size))
# -> 717                             elog(ERROR, "cannot read block %u of \"%s\" read %lu of %d",
# 718                                     blknum, file->path, read_len, header.compressed_size);
