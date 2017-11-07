import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from datetime import datetime, timedelta
import subprocess
import shutil, sys, time


from testgres import QueryException


module_name = 'ptrack'


class PtrackBackupTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_enable(self):
        """make ptrack without full backup, should result in error"""
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '30s'}
            )
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # PTRACK BACKUP
        try:
            self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=["--stream"])
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because ptrack disabled.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual('ERROR: Ptrack is disabled\n', e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_disable(self):
        """Take full backup, disable ptrack restart postgresql, enable ptrack, restart postgresql, take ptrack backup which should fail"""
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
#        print('START')
#        print(node.safe_psql('postgres', "select pg_ptrack_control_lsn()"))
        self.backup_node(backup_dir, 'node', node, options=['--stream'])
#        print('AFTER FULL')
#        print(node.safe_psql('postgres', "select pg_ptrack_control_lsn()"))
        # DISABLE PTRACK
        node.safe_psql('postgres', "alter system set ptrack_enable to off")
        node.restart()
#        print('DISABLED')
#        print(node.safe_psql('postgres', "select pg_ptrack_control_lsn()"))
        # ENABLE PTRACK
        node.safe_psql('postgres', "alter system set ptrack_enable to on")
        node.restart()
#        print('ENABLED')
#        print(node.safe_psql('postgres', "select pg_ptrack_control_lsn()"))

        # PTRACK BACKUP
        try:
            self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=["--stream"])
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because ptrack_enable was set to OFF at some point after previous backup.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn('ERROR: LSN from ptrack_control', e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_stream(self):
        """make node, make full and ptrack stream backups, restore them and check data correctness"""
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '30s', 'ptrack_enable': 'on', 'autovacuum': 'off'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.start()

        # FULL BACKUP
        node.safe_psql("postgres", "create sequence t_seq")
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, nextval('t_seq') as t_seq, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(0,100) i")
        full_result = node.safe_psql("postgres", "SELECT * FROM t_heap")
        full_backup_id = self.backup_node(backup_dir, 'node', node, options=['--stream'])

        # PTRACK BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, nextval('t_seq') as t_seq, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(100,200) i")
        ptrack_result = node.safe_psql("postgres", "SELECT * FROM t_heap")
        ptrack_backup_id = self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=['--stream'])
        pgdata = self.pgdata_content(node.data_dir)

        # Drop Node
        node.cleanup()

        # Restore and check full backup
        self.assertIn("INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(backup_dir, 'node', node, backup_id=full_backup_id, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)
        full_result_new = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Restore and check ptrack backup
        self.assertIn("INFO: Restore of backup {0} completed.".format(ptrack_backup_id),
            self.restore_node(backup_dir, 'node', node, backup_id=ptrack_backup_id, options=["-j", "4"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        pgdata_restored = self.pgdata_content(node.data_dir)
        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)
        ptrack_result_new = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(ptrack_result, ptrack_result_new)

        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_archive(self):
        """make archive node, make full and ptrack backups, check data correctness in restored instance"""
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '30s', 'ptrack_enable': 'on', 'autovacuum': 'off'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(0,100) i")
        full_result = node.safe_psql("postgres", "SELECT * FROM t_heap")
        full_backup_id = self.backup_node(backup_dir, 'node', node)
        full_target_time = self.show_pb(backup_dir, 'node', full_backup_id)['recovery-time']

        # PTRACK BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(100,200) i")
        ptrack_result = node.safe_psql("postgres", "SELECT * FROM t_heap")
        ptrack_backup_id = self.backup_node(backup_dir, 'node', node, backup_type='ptrack')
        ptrack_target_time = self.show_pb(backup_dir, 'node', ptrack_backup_id)['recovery-time']
        pgdata = self.pgdata_content(node.data_dir)

        # Drop Node
        node.cleanup()

        # Check full backup
        self.assertIn("INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(backup_dir, 'node', node, backup_id=full_backup_id, options=["-j", "4", "--time={0}".format(full_target_time)]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)
        full_result_new = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check ptrack backup
        self.assertIn("INFO: Restore of backup {0} completed.".format(ptrack_backup_id),
            self.restore_node(backup_dir, 'node', node, backup_id=ptrack_backup_id, options=["-j", "4", "--time={0}".format(ptrack_target_time)]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(self.output), self.cmd))
        pgdata_restored = self.pgdata_content(node.data_dir)
        node.start()
        while node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)
        ptrack_result_new = node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.assertEqual(ptrack_result, ptrack_result_new)

        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)

        node.cleanup()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_pgpro417(self):
        """Make node, take full backup, take ptrack backup, delete ptrack backup. Try to take ptrack backup,
        which should fail """
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '30s', 'ptrack_enable': 'on', 'autovacuum': 'off'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(0,100) i")
        node.safe_psql("postgres", "SELECT * FROM t_heap")
        backup_id = self.backup_node(backup_dir, 'node', node, backup_type='full', options=["--stream"])
        start_lsn_full = self.show_pb(backup_dir, 'node', backup_id)['start-lsn']

        # PTRACK BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(100,200) i")
        node.safe_psql("postgres", "SELECT * FROM t_heap")
        backup_id = self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=["--stream"])
        start_lsn_ptrack = self.show_pb(backup_dir, 'node', backup_id)['start-lsn']

        self.delete_pb(backup_dir, 'node', backup_id)

        # SECOND PTRACK BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(200,300) i")

        try:
            self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=["--stream"])
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because of LSN mismatch from ptrack_control and previous backup start_lsn.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue('ERROR: LSN from ptrack_control' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_page_pgpro417(self):
        """Make archive node, take full backup, take page backup, delete page backup. Try to take ptrack backup, which should fail"""
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '30s', 'ptrack_enable': 'on', 'autovacuum': 'off'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(0,100) i")
        node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.backup_node(backup_dir, 'node', node)

        # PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(100,200) i")
        node.safe_psql("postgres", "SELECT * FROM t_heap")
        backup_id = self.backup_node(backup_dir, 'node', node, backup_type='page')

        self.delete_pb(backup_dir, 'node', backup_id)
#        sys.exit(1)

        # PTRACK BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(200,300) i")

        try:
            self.backup_node(backup_dir, 'node', node, backup_type='ptrack')
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because of LSN mismatch from ptrack_control and previous backup start_lsn.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue('ERROR: LSN from ptrack_control' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_full_pgpro417(self):
        """Make node, take two full backups, delete full second backup. Try to take ptrack backup, which should fail"""
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '30s', 'ptrack_enable': 'on', 'autovacuum': 'off'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(0,100) i")
        node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.backup_node(backup_dir, 'node', node, options=["--stream"])

        # SECOND FULL BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(100,200) i")
        node.safe_psql("postgres", "SELECT * FROM t_heap")
        backup_id = self.backup_node(backup_dir, 'node', node, options=["--stream"])

        self.delete_pb(backup_dir, 'node', backup_id)

        # PTRACK BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(200,300) i")
        try:
            self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=["--stream"])
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because of LSN mismatch from ptrack_control and previous backup start_lsn.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue('ERROR: LSN from ptrack_control' in e.message
                 and 'Create new full backup before an incremental one' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_create_db(self):
        """Make node, take full backup, create database db1, take ptrack backup, restore database and check it presense"""
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_size': '10GB', 'max_wal_senders': '2', 'checkpoint_timeout': '5min', 'ptrack_enable': 'on', 'autovacuum': 'off'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        # FULL BACKUP
        node.safe_psql("postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(0,100) i")
        node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.backup_node(backup_dir, 'node', node, options=["--stream"])

        # CREATE DATABASE DB1
        node.safe_psql("postgres", "create database db1")
        node.safe_psql("db1", "create table t_heap as select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(0,100) i")

        # PTRACK BACKUP
        backup_id = self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=["--stream"])
        pgdata = self.pgdata_content(node.data_dir)

        # RESTORE
        node_restored = self.make_simple_node(base_dir="{0}/{1}/node_restored".format(module_name, fname))
        node_restored.cleanup()
        # COMPARE PHYSICAL CONTENT
        self.restore_node(backup_dir, 'node', node_restored, backup_id=backup_id, options=["-j", "4"])
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)

        # START RESTORED NODE
        node_restored.append_conf("postgresql.auto.conf", "port = {0}".format(node_restored.port))
        node_restored.start()

        # DROP DATABASE DB1
        node.safe_psql(
            "postgres", "drop database db1")
        # SECOND PTRACK BACKUP
        backup_id = self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=["--stream"])
        pgdata = self.pgdata_content(node.data_dir)

        # RESTORE SECOND PTRACK BACKUP
        node_restored.cleanup()
        self.restore_node(backup_dir, 'node', node_restored, backup_id=backup_id, options=["-j", "4"])

        # START RESTORED NODE
        node_restored.append_conf("postgresql.auto.conf", "port = {0}".format(node_restored.port))
        node_restored.start()

        # COMPARE PHYSICAL CONTENT
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)

        try:
            node_restored.safe_psql('db1', 'select 1')
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because we are connecting to deleted database.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except Exception as e:
            self.assertTrue('FATAL:  database "db1" does not exist' in repr(e),
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_alter_table_set_tablespace_ptrack(self):
        """Make node, create tablespace with table, take full backup, alter tablespace location, take ptrack backup, restore database."""
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '30s', 'ptrack_enable': 'on', 'autovacuum': 'off'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.start()

        # FULL BACKUP
        self.create_tblspace_in_node(node, 'somedata')
        node.safe_psql(
            "postgres",
            "create table t_heap tablespace somedata as select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(0,100) i")
        node.safe_psql("postgres", "SELECT * FROM t_heap")
        self.backup_node(backup_dir, 'node', node, options=["--stream"])

        # ALTER TABLESPACE
        self.create_tblspace_in_node(node, 'somedata_new')
        node.safe_psql(
            "postgres", "alter table t_heap set tablespace somedata_new")

        # PTRACK BACKUP
        result =  node.safe_psql("postgres", "select * from t_heap")
        self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=["--stream"])
        pgdata = self.pgdata_content(node.data_dir)
        node.stop()
        node.cleanup()

        # RESTORE
        node_restored = self.make_simple_node(base_dir="{0}/{1}/node_restored".format(module_name, fname))
        node_restored.cleanup()

        self.restore_node(backup_dir, 'node', node_restored, options=["-j", "4",
            "-T", "{0}={1}".format(self.get_tblspace_path(node,'somedata'), self.get_tblspace_path(node_restored,'somedata')),
            "-T", "{0}={1}".format(self.get_tblspace_path(node,'somedata_new'), self.get_tblspace_path(node_restored,'somedata_new'))
            ])

        # GET RESTORED PGDATA AND COMPARE
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        # START RESTORED NODE
        node_restored.append_conf('postgresql.auto.conf', 'port = {0}'.format(node_restored.port))
        node_restored.start()
        time.sleep(5)
        while node_restored.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)
        result_new =  node_restored.safe_psql("postgres", "select * from t_heap")

        self.assertEqual(result, result_new, 'lost some data after restore')
        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_alter_database_set_tablespace_ptrack(self):
        """Make node, create tablespace with database, take full backup, alter tablespace location, take ptrack backup, restore database."""
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '30s', 'ptrack_enable': 'on', 'autovacuum': 'off'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.start()

        # FULL BACKUP
        self.backup_node(backup_dir, 'node', node, options=["--stream"])

        # CREATE TABLESPACE
        self.create_tblspace_in_node(node, 'somedata')

        # ALTER DATABASE
        node.safe_psql("template1",
            "alter database postgres set tablespace somedata")

        # PTRACK BACKUP
        self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=["--stream"])
        pgdata= self.pgdata_content(node.data_dir)
        node.stop()

        # RESTORE
        node_restored = self.make_simple_node(base_dir="{0}/{1}/node_restored".format(module_name, fname))
        node_restored.cleanup()
        self.restore_node(backup_dir, 'node', node_restored, options=["-j", "4",
            "-T", "{0}={1}".format(self.get_tblspace_path(node,'somedata'), self.get_tblspace_path(node_restored,'somedata'))])

        # GET PHYSICAL CONTENT
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        # COMPARE PHYSICAL CONTENT
        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)
        # START RESTORED NODE
        node_restored.start()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_drop_tablespace(self):
        """Make node, create table, alter table tablespace, take ptrack backup, move table from tablespace, take ptrack backup"""
        self.maxDiff = None
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2', 'checkpoint_timeout': '30s', 'ptrack_enable': 'on', 'autovacuum': 'off'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.start()

        self.create_tblspace_in_node(node, 'somedata')

        # CREATE TABLE
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(0,100) i")
        result = node.safe_psql("postgres", "select * from t_heap")
        # FULL BACKUP
        self.backup_node(backup_dir, 'node', node, options=["--stream"])

        # Move table to tablespace 'somedata'
        node.safe_psql(
            "postgres", "alter table t_heap set tablespace somedata")
        # PTRACK BACKUP
        self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=["--stream"])

        # Move table back to default tablespace
        node.safe_psql(
            "postgres", "alter table t_heap set tablespace pg_default")
        # SECOND PTRACK BACKUP
        self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=["--stream"])

        # DROP TABLESPACE 'somedata'
        node.safe_psql(
            "postgres", "drop tablespace somedata")
        # THIRD PTRACK BACKUP
        self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=["--stream"])

        tblspace = self.get_tblspace_path(node, 'somedata')
        node.cleanup()
        shutil.rmtree(tblspace, ignore_errors=True)
        self.restore_node(backup_dir, 'node', node, options=["-j", "4"])
        node.start()

        tblspc_exist = node.safe_psql("postgres", "select exists(select 1 from pg_tablespace where spcname = 'somedata')")
        if tblspc_exist.rstrip() == 't':
            self.assertEqual(1, 0, "Expecting Error because tablespace 'somedata' should not be present")

        result_new = node.safe_psql("postgres", "select * from t_heap")
        self.assertEqual(result, result_new)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_ptrack_alter_tablespace(self):
        """Make node, create table, alter table tablespace, take ptrack backup, move table from tablespace, take ptrack backup"""
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
        node.start()

        self.create_tblspace_in_node(node, 'somedata')

        # CREATE TABLE
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(i::text)::tsvector as tsvector from generate_series(0,100) i")
        result = node.safe_psql("postgres", "select * from t_heap")
        # FULL BACKUP
        self.backup_node(backup_dir, 'node', node, options=["--stream"])

        # Move table to separate tablespace
        node.safe_psql(
            "postgres", "alter table t_heap set tablespace somedata")
        # FIRTS PTRACK BACKUP
        self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=["--stream"])
        pgdata = self.pgdata_content(node.data_dir)

        # Restore ptrack backup and check table consistency
        restored_node = self.make_simple_node(base_dir="{0}/{1}/restored_node".format(module_name, fname))
        restored_node.cleanup()
        tblspc_path = self.get_tblspace_path(node, 'somedata')
        tblspc_path_new = self.get_tblspace_path(restored_node, 'somedata_restored')
        self.restore_node(backup_dir, 'node', restored_node, options=[
            "-j", "4", "-T", "{0}={1}".format(tblspc_path, tblspc_path_new)])
        result = node.safe_psql("postgres", "select * from t_heap")

        pgdata_restored = self.pgdata_content(restored_node.data_dir)

        # START RESTORED NODE
        restored_node.append_conf("postgresql.auto.conf", "port = {0}".format(restored_node.port))
        restored_node.start()
        while restored_node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        result_new = restored_node.safe_psql("postgres", "select * from t_heap")
        self.assertEqual(result, result_new)

        # COMPARE PHYSICAL CONTENT
        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)

        restored_node.cleanup()
        shutil.rmtree(tblspc_path_new, ignore_errors=True)

        # Move table to default tablespace
        node.safe_psql(
            "postgres", "alter table t_heap set tablespace pg_default")
        # SECOND PTRACK BACKUP
        self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=["--stream"])
        pgdata = self.pgdata_content(node.data_dir)

        # Restore second ptrack backup and check table consistency
        self.restore_node(backup_dir, 'node', restored_node, options=[
            "-j", "4", "-T", "{0}={1}".format(tblspc_path, tblspc_path_new)])
        restored_node.append_conf("postgresql.auto.conf", "port = {0}".format(restored_node.port))

        # GET PHYSICAL CONTENT
        pgdata_restored = self.pgdata_content(restored_node.data_dir)

        # START RESTORED NODE
        restored_node.start()
        while restored_node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        result_new = restored_node.safe_psql("postgres", "select * from t_heap")
        self.assertEqual(result, result_new)

        if self.paranoia:
            # COMPARE PHYSICAL CONTENT
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    @unittest.expectedFailure
    def test_relation_with_multiple_segments(self):
        """Make node, create table, alter table tablespace, take ptrack backup, move table from tablespace, take ptrack backup"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2',
            'ptrack_enable': 'on', 'fsync': 'off', 'shared_buffers': '1GB',
            'maintenance_work_mem': '1GB', 'autovacuum': 'off', 'full_page_writes': 'off'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        self.create_tblspace_in_node(node, 'somedata')

        # CREATE TABLE
        node.pgbench_init(scale=100, options=['--tablespace=somedata'])
        # FULL BACKUP
        #self.backup_node(backup_dir, 'node', node, options=["--stream"])
        self.backup_node(backup_dir, 'node', node)

        # PTRACK STUFF
        idx_ptrack = {'type': 'heap'}

        idx_ptrack['path'] = self.get_fork_path(node, 'pgbench_accounts')
        idx_ptrack['old_size'] = self.get_fork_size(node, 'pgbench_accounts')
        idx_ptrack['old_pages'] = self.get_md5_per_page_for_fork(
            idx_ptrack['path'], idx_ptrack['old_size'])

        pgbench = node.pgbench(options=['-T', '50', '-c', '2', '--no-vacuum'])
        pgbench.wait()
        #node.safe_psql("postgres", "update pgbench_accounts set bid = bid +1")
        node.safe_psql("postgres", "checkpoint")

        idx_ptrack['new_size'] = self.get_fork_size(node, 'pgbench_accounts')
        idx_ptrack['new_pages'] = self.get_md5_per_page_for_fork(idx_ptrack['path'], idx_ptrack['new_size'])
        idx_ptrack['ptrack'] = self.get_ptrack_bits_per_page_for_fork(
            node, idx_ptrack['path'], [idx_ptrack['old_size'], idx_ptrack['new_size']])

        self.check_ptrack_sanity(idx_ptrack)
        ## PTRACK STUFF

        # GET LOGICAL CONTENT FROM NODE
        result = node.safe_psql("postgres", "select * from pgbench_accounts")
        # FIRTS PTRACK BACKUP
        #self.backup_node(backup_dir, 'node', node, backup_type='ptrack', options=["--stream"])
        self.backup_node(backup_dir, 'node', node, backup_type='ptrack')
        # GET PHYSICAL CONTENT FROM NODE
        pgdata = self.pgdata_content(node.data_dir)
        #get_md5_per_page_for_fork

        # RESTORE NODE
        restored_node = self.make_simple_node(base_dir="{0}/{1}/restored_node".format(module_name, fname))
        restored_node.cleanup()
        tblspc_path = self.get_tblspace_path(node, 'somedata')
        tblspc_path_new = self.get_tblspace_path(restored_node, 'somedata_restored')

        self.restore_node(backup_dir, 'node', restored_node, options=[
            "-j", "4", "-T", "{0}={1}".format(tblspc_path, tblspc_path_new)])

        # GET PHYSICAL CONTENT FROM NODE_RESTORED
        pgdata_restored = self.pgdata_content(restored_node.data_dir)

        # START RESTORED NODE
        restored_node.append_conf("postgresql.auto.conf", "port = {0}".format(restored_node.port))
        restored_node.start()
        while restored_node.safe_psql("postgres", "select pg_is_in_recovery()") == 't\n':
            time.sleep(1)

        result_new = restored_node.safe_psql("postgres", "select * from pgbench_accounts")

        # COMPARE RESTORED FILES
        self.assertEqual(result, result_new, 'data is lost')

        if self.paranoia:
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        # self.del_test_dir(module_name, fname)
