import unittest
import os
import re
from time import sleep, time
from datetime import datetime

from pg_probackup2.gdb import needs_gdb

from .helpers.ptrack_helpers import base36enc, ProbackupTest
from .helpers.ptrack_helpers import fs_backup_class
import subprocess

tblspace_name = 'some_tblspace'

class Pbckp1242Test(ProbackupTest):

    def setup_node(self):
        node = self.pg_node.make_simple(
            "node",
            set_replication=True,
            initdb_params=['--data-checksums']
        )
        self.pb.init()
        self.pb.add_instance( 'node', node)
        node.slow_start()
        return node

    def jump_the_oid(self, node):
        pg_connect = node.connect("postgres", autocommit=True)
        gdb = self.gdb_attach(pg_connect.pid)
        gdb._execute('set ShmemVariableCache->nextOid=1<<31')
        gdb._execute('set ShmemVariableCache->oidCount=0')
        gdb.detach()

    @needs_gdb
    def test_table_with_giga_oid(self):
        node = self.setup_node()
        self.jump_the_oid(node)

        node.execute(f'CREATE TABLE t1 (i int)')
        node.execute('INSERT INTO t1 (i) SELECT generate_series(1, 1000)')
        node.execute('CHECKPOINT')

        table1_checksum = node.table_checksum('t1')

        backup_id = self.pb.backup_node('node', node, backup_type='full',
                                             options=['--stream'])

        node.stop()
        node.cleanup()

        self.pb.restore_node('node', node,
                             backup_id=backup_id)

        node.slow_start()

        new_table1_checksum = node.table_checksum('t1')

        self.assertEqual(new_table1_checksum, table1_checksum, "table checksums doesn't match")

    @needs_gdb
    def test_database_with_giga_oid(self):
        node = self.setup_node()
        self.jump_the_oid(node)

        node.execute(f'CREATE DATABASE db2')

        node.execute('db2', f'CREATE TABLE t1 (i int)')
        node.execute('db2', 'INSERT INTO t1 (i) SELECT generate_series(1, 1000)')
        node.execute('CHECKPOINT')

        table1_checksum = node.table_checksum('t1', 'db2')

        backup_id = self.pb.backup_node('node', node, backup_type='full',
                                             options=['--stream'])

        node.stop()
        node.cleanup()

        self.pb.restore_node('node', node,
                             backup_id=backup_id)

        node.slow_start()

        new_table1_checksum = node.table_checksum('t1', 'db2')

        self.assertEqual(new_table1_checksum, table1_checksum, "table checksums doesn't match")

    @needs_gdb
    def test_table_with_giga_oid_in_tablespace(self):
        node = self.setup_node()
        self.create_tblspace_in_node(node, tblspace_name)

        self.jump_the_oid(node)

        node.execute(f'CREATE TABLE t1 (i int) TABLESPACE {tblspace_name}')
        node.execute('INSERT INTO t1 (i) SELECT generate_series(1, 1000)')
        node.execute('CHECKPOINT')

        table1_checksum = node.table_checksum('t1')

        backup_id = self.pb.backup_node('node', node, backup_type='full',
                                             options=['--stream'])

        node.stop()
        node.cleanup()

        self.pb.restore_node('node', node,
                             backup_id=backup_id)

        node.slow_start()

        new_table1_checksum = node.table_checksum('t1')

        self.assertEqual(new_table1_checksum, table1_checksum, "table checksums doesn't match")


    @needs_gdb
    def test_database_with_giga_oid_in_tablespace(self):
        node = self.setup_node()
        self.create_tblspace_in_node(node, tblspace_name)

        self.jump_the_oid(node)

        node.execute(f'CREATE DATABASE db2 TABLESPACE {tblspace_name}')

        node.execute('db2', f'CREATE TABLE t1 (i int)')
        node.execute('db2', 'INSERT INTO t1 (i) SELECT generate_series(1, 1000)')
        node.execute('CHECKPOINT')

        table1_checksum = node.table_checksum('t1', 'db2')

        backup_id = self.pb.backup_node('node', node, backup_type='full',
                                             options=['--stream'])

        node.stop()
        node.cleanup()

        self.pb.restore_node('node', node,
                             backup_id=backup_id)

        node.slow_start()

        new_table1_checksum = node.table_checksum('t1', 'db2')

        self.assertEqual(new_table1_checksum, table1_checksum, "table checksums doesn't match")

    @needs_gdb
    def test_database_with_giga_oid_in_tablespace_2(self):
        node = self.setup_node()
        self.create_tblspace_in_node(node, tblspace_name)

        self.jump_the_oid(node)

        node.execute(f'CREATE DATABASE db2')

        node.execute('db2', f'CREATE TABLE t1 (i int) TABLESPACE {tblspace_name}')
        node.execute('db2', 'INSERT INTO t1 (i) SELECT generate_series(1, 1000)')
        node.execute('CHECKPOINT')

        table1_checksum = node.table_checksum('t1', 'db2')

        backup_id = self.pb.backup_node('node', node, backup_type='full',
                                             options=['--stream'])

        node.stop()
        node.cleanup()

        self.pb.restore_node('node', node,
                             backup_id=backup_id)

        node.slow_start()

        new_table1_checksum = node.table_checksum('t1', 'db2')

        self.assertEqual(new_table1_checksum, table1_checksum, "table checksums doesn't match")

    @needs_gdb
    def test_detect_database_with_giga_oid_in_tablespace(self):
        node = self.setup_node()
        self.create_tblspace_in_node(node, tblspace_name)

        self.jump_the_oid(node)

        node.execute(f'CREATE DATABASE db2 TABLESPACE {tblspace_name}')

        node.execute('db2', f'CREATE TABLE t1 (i int)')
        node.execute('db2', 'INSERT INTO t1 (i) SELECT generate_series(1, 1000)')
        node.execute('CHECKPOINT')

        backup_id = self.pb.backup_node('node', node, backup_type='full',
                                             options=['--stream'])

        node.stop()
        node.cleanup()

        self.prepare_backup_for_detect_missed_database(backup_id)

        self.pb.restore_node('node', node,
                             backup_id=backup_id,
                             expect_error="database with giga oid")
        self.assertMessage(contains="probably has missing files in")
        self.assertMessage(contains="were created by misbehaving")

    def test_nodetect_database_without_giga_oid_in_tablespace(self):
        node = self.setup_node()
        self.create_tblspace_in_node(node, tblspace_name)

        node.execute(f'CREATE DATABASE db2 TABLESPACE {tblspace_name}')

        node.execute('db2', f'CREATE TABLE t1 (i int)')
        node.execute('db2', 'INSERT INTO t1 (i) SELECT generate_series(1, 1000)')
        node.execute('CHECKPOINT')

        table1_checksum = node.table_checksum('t1', 'db2')

        backup_id = self.pb.backup_node('node', node, backup_type='full',
                                             options=['--stream'])

        node.stop()
        node.cleanup()

        self.prepare_backup_for_detect_missed_database(backup_id)

        self.pb.restore_node('node', node,
                             backup_id=backup_id)

        node.slow_start()

        new_table1_checksum = node.table_checksum('t1', 'db2')

        self.assertEqual(new_table1_checksum, table1_checksum, "table checksums doesn't match")

    @needs_gdb
    def test_tablespace_with_giga_oid(self):
        node = self.setup_node()
        node.execute(f'CREATE DATABASE db2')

        node.execute('db2', f'CREATE TABLE t1 (i int)')
        node.execute('db2', 'INSERT INTO t1 (i) SELECT generate_series(1, 1000)')

        table1_checksum = node.table_checksum('t1', 'db2')

        self.jump_the_oid(node)

        self.create_tblspace_in_node(node, tblspace_name)

        node.execute(f'ALTER DATABASE db2 SET TABLESPACE {tblspace_name}')
        node.execute('CHECKPOINT')

        backup_id = self.pb.backup_node('node', node, backup_type='full',
                                             options=['--stream'])

        node.stop()
        node.cleanup()

        self.pb.restore_node('node', node,
                             backup_id=backup_id)

        node.slow_start()

        new_table1_checksum = node.table_checksum('t1', 'db2')

        self.assertEqual(new_table1_checksum, table1_checksum, "table checksums doesn't match")

    @needs_gdb
    def test_detect_tablespace_with_giga_oid(self):
        node = self.setup_node()
        node.execute(f'CREATE DATABASE db2')

        node.execute('db2', f'CREATE TABLE t1 (i int)')
        node.execute('db2', 'INSERT INTO t1 (i) SELECT generate_series(1, 1000)')

        table1_checksum = node.table_checksum('t1', 'db2')

        self.jump_the_oid(node)

        self.create_tblspace_in_node(node, tblspace_name)

        node.execute(f'ALTER DATABASE db2 SET TABLESPACE {tblspace_name}')
        node.execute('CHECKPOINT')

        backup_id = self.pb.backup_node('node', node, backup_type='full',
                                             options=['--stream'])

        node.stop()
        node.cleanup()

        self.prepare_backup_for_detect_missed_tablespace(backup_id)

        self.pb.restore_node('node', node,
                             backup_id=backup_id,
                             expect_error='tablespace with gigaoid')

        self.assertMessage(contains="has missing tablespace")
        self.assertMessage(contains="were created by misbehaving")

    def test_nodetect_tablespace_without_giga_oid(self):
        node = self.setup_node()
        node.execute(f'CREATE DATABASE db2')

        node.execute('db2', f'CREATE TABLE t1 (i int)')
        node.execute('db2', 'INSERT INTO t1 (i) SELECT generate_series(1, 1000)')

        table1_checksum = node.table_checksum('t1', 'db2')

        self.create_tblspace_in_node(node, tblspace_name)

        node.execute(f'ALTER DATABASE db2 SET TABLESPACE {tblspace_name}')
        node.execute('CHECKPOINT')

        backup_id = self.pb.backup_node('node', node, backup_type='full',
                                             options=['--stream'])

        node.stop()
        node.cleanup()

        self.prepare_backup_for_detect_missed_tablespace(backup_id)

        self.pb.restore_node('node', node,
                             backup_id=backup_id)

        node.slow_start()

        new_table1_checksum = node.table_checksum('t1', 'db2')

        self.assertEqual(new_table1_checksum, table1_checksum, "table checksums doesn't match")

    @needs_gdb
    def test_detect_giga_oid_table(self):
        """Detect we couldn't increment based on backup with misdetected file type"""
        node = self.setup_node()
        self.jump_the_oid(node)

        node.execute(f'CREATE TABLE t1 (i int)')
        node.execute('INSERT INTO t1 (i) SELECT generate_series(1, 1000)')
        node.execute('CHECKPOINT')

        backup_id = self.pb.backup_node('node', node, backup_type='full',
                                             options=['--stream'])

        self.prepare_backup_for_detect_nondatafile_relation(backup_id)

        self.pb.backup_node('node', node, backup_type='delta',
                            options=['--stream'],
                            expect_error="relation is mistakenly marked as non-datafile")
        self.assertMessage(contains="were created by misbehaving")
        self.assertMessage(contains="Could not use it as a parent for increment")

    def test_nodetect_giga_oid_table(self):
        """Detect we could increment based on backup without misdetected file type"""
        node = self.setup_node()

        node.execute(f'CREATE TABLE t1 (i int)')
        node.execute('INSERT INTO t1 (i) SELECT generate_series(1, 1000)')
        node.execute('CHECKPOINT')

        backup_id = self.pb.backup_node('node', node, backup_type='full',
                                             options=['--stream'])

        self.prepare_backup_for_detect_nondatafile_relation(backup_id)

        node.execute('INSERT INTO t1 (i) SELECT generate_series(2000, 3000)')

        table1_checksum = node.table_checksum('t1')

        backup_id2 = self.pb.backup_node('node', node, backup_type='delta',
                            options=['--stream'])

        node.stop()
        node.cleanup()

        self.pb.restore_node('node', node,
                             backup_id=backup_id2)

        node.slow_start()

        new_table1_checksum = node.table_checksum('t1')

        self.assertEqual(new_table1_checksum, table1_checksum, "table checksums doesn't match")

    @needs_gdb
    def test_detect_giga_oid_table_in_merge_restore(self):
        """Detect we cann't merge/restore mixed increment chain with misdetected file type"""
        node = self.setup_node()
        self.jump_the_oid(node)

        node.execute(f'CREATE TABLE t1 (i int)')
        node.execute('INSERT INTO t1 (i) SELECT generate_series(1, 1000)')
        node.execute('CHECKPOINT')

        backup_id1 = self.pb.backup_node('node', node, backup_type='full',
                                        options=['--stream'])

        backup_id2 = self.pb.backup_node('node', node, backup_type='delta',
                                             options=['--stream'])
        self.backup_control_version_to_2_7_3(backup_id1)
        self.prepare_backup_for_detect_nondatafile_relation(backup_id2)

        self.pb.merge_backup('node', backup_id2,
                             options=['--no-validate'],
                             expect_error="due to chain of mixed bug/nobug backups")

        self.assertMessage(contains="kind reg detected is_datafile=0 stored=1")

        node.stop()
        node.cleanup()

        self.pb.restore_node('node', node,
                             backup_id=backup_id2,
                             options=['--no-validate'],
                             expect_error="due to chain of mixed bug/nobug backups")

        self.assertMessage(contains="kind reg detected is_datafile=0 stored=1")

        self.pb.merge_backup('node', backup_id2,
                             options=['--no-validate'],
                             expect_error="due to chain of mixed bug/nobug backups")

        self.assertMessage(contains="kind reg detected is_datafile=0 stored=1")


    @needs_gdb
    def test_allow_giga_oid_table_in_restore(self):
        """Detect we can restore uniform increment chain with misdetected file type"""
        node = self.setup_node()
        self.jump_the_oid(node)

        node.execute(f'CREATE TABLE t1 (i int)')
        node.execute('INSERT INTO t1 (i) SELECT generate_series(1, 1000)')
        node.execute('CHECKPOINT')

        backup_id1 = self.pb.backup_node('node', node, backup_type='full',
                                        options=['--stream'])

        backup_id2 = self.pb.backup_node('node', node, backup_type='delta',
                                             options=['--stream'])
        self.prepare_backup_for_detect_nondatafile_relation(backup_id1)
        self.prepare_backup_for_detect_nondatafile_relation(backup_id2)

        node.stop()
        node.cleanup()

        self.pb.restore_node('node', node, backup_id=backup_id2)
        # although we did restore, we could not check table checksum,
        # because we backup relations as datafiles
        # (because we backuped with fixed pbckp1242),
        # and restore relation as non-datafile (ie with probackup's headers)

        self.pb.merge_backup('node', backup_id2,
                             expect_error="because of backups with bug")
        self.assertMessage(contains='backups with 2.8.0/2.8.1')
        self.assertMessage(contains="Could not merge them.")

    @needs_gdb
    def test_nodetect_giga_oid_table_in_merge_restore(self):
        """Detect we can merge/restore mixed increment chain without misdetected file type"""
        node = self.setup_node()

        node.execute(f'CREATE TABLE t1 (i int)')
        node.execute('INSERT INTO t1 (i) SELECT generate_series(1, 1000)')
        node.execute('CHECKPOINT')

        backup_id1 = self.pb.backup_node('node', node, backup_type='full',
                                        options=['--stream'])

        node.execute('INSERT INTO t1 (i) SELECT generate_series(2000, 3000)')
        node.execute('CHECKPOINT')

        table1_checksum = node.table_checksum('t1')

        backup_id2 = self.pb.backup_node('node', node, backup_type='delta',
                                             options=['--stream'])
        self.backup_control_version_to_2_7_3(backup_id1)
        self.prepare_backup_for_detect_nondatafile_relation(backup_id2)

        node.stop()
        node.cleanup()

        self.pb.restore_node('node', node, backup_id=backup_id2)

        node.slow_start()

        new_table1_checksum = node.table_checksum('t1')

        self.assertEqual(new_table1_checksum, table1_checksum, "table checksums doesn't match")

        self.pb.merge_backup('node', backup_id2)

    @needs_gdb
    def test_detect_giga_oid_database_in_merge_restore(self):
        """Detect we cann't merge/restore mixed increment chain with misdetected file type"""
        node = self.setup_node()
        self.jump_the_oid(node)

        node.execute(f'CREATE DATABASE db2')

        node.execute('db2', f'CREATE TABLE t1 (i int)')
        node.execute('db2', 'INSERT INTO t1 (i) SELECT generate_series(1, 1000)')
        node.execute('CHECKPOINT')

        backup_id1 = self.pb.backup_node('node', node, backup_type='full',
                                        options=['--stream'])

        node.execute('db2', f'CREATE TABLE t2 (i int)')
        node.execute('db2', 'INSERT INTO t1 (i) SELECT generate_series(2000, 3000)')
        node.execute('db2', 'INSERT INTO t2 (i) SELECT generate_series(2000, 3000)')
        node.execute('CHECKPOINT')

        backup_id2 = self.pb.backup_node('node', node, backup_type='delta',
                                             options=['--stream'])
        self.backup_control_version_to_2_7_3(backup_id1)
        self.prepare_backup_for_detect_gigaoid_database(backup_id2)

        self.pb.merge_backup('node', backup_id2,
                             options=['--no-validate'],
                             expect_error="due to chain of mixed bug/nobug backups")

        self.assertMessage(contains="kind reg detected is_datafile=0 stored=1")

        node.stop()
        node.cleanup()

        self.pb.restore_node('node', node,
                             backup_id=backup_id2,
                             options=['--no-validate'],
                             expect_error="due to chain of mixed bug/nobug backups")

        self.assertMessage(contains="kind reg detected is_datafile=0 stored=1")

        self.pb.merge_backup('node', backup_id2,
                             options=['--no-validate'],
                             expect_error="due to chain of mixed bug/nobug backups")

        self.assertMessage(contains="kind reg detected is_datafile=0 stored=1")

    @needs_gdb
    def test_allow_giga_oid_database_in_restore(self):
        """Detect we can restore uniform increment chain with misdetected file type"""
        node = self.setup_node()
        self.jump_the_oid(node)

        node.execute(f'CREATE DATABASE db2')

        node.execute('db2', f'CREATE TABLE t1 (i int)')
        node.execute('db2', 'INSERT INTO t1 (i) SELECT generate_series(1, 1000)')
        node.execute('CHECKPOINT')

        backup_id1 = self.pb.backup_node('node', node, backup_type='full',
                                        options=['--stream'])

        node.execute('db2', f'CREATE TABLE t2 (i int)')
        node.execute('db2', 'INSERT INTO t1 (i) SELECT generate_series(2000, 3000)')
        node.execute('db2', 'INSERT INTO t2 (i) SELECT generate_series(2000, 3000)')
        node.execute('CHECKPOINT')

        backup_id2 = self.pb.backup_node('node', node, backup_type='delta',
                                             options=['--stream'])
        self.prepare_backup_for_detect_gigaoid_database(backup_id1)
        self.prepare_backup_for_detect_gigaoid_database(backup_id2)

        node.stop()
        node.cleanup()

        self.pb.restore_node('node', node,
                             backup_id=backup_id2)
        # although we did restore, we could not check table checksum,
        # because we backup relations as datafiles
        # (because we backuped with fixed pbckp1242),
        # and restore relation as non-datafile (ie with probackup's headers)

        self.pb.merge_backup('node', backup_id2,
                             expect_error="because of backups with bug")
        self.assertMessage(contains='backups with 2.8.0/2.8.1')
        self.assertMessage(contains="Could not merge them.")

    def backup_control_version_to(self, version, backup_id):
        with self.modify_backup_control(self.backup_dir, 'node', backup_id) as control:
            new = []
            for line in control.data.splitlines(True):
                if line.startswith('program-version'):
                    line = f'program-version = {version}\n'
                elif line.startswith('content-crc'):
                    line = 'content-crc = 0\n'
                new.append(line)
            control.data = "".join(new)

    def backup_control_version_to_2_8_1(self, backup_id):
        self.backup_control_version_to('2.8.1', backup_id)

    def backup_control_version_to_2_7_3(self, backup_id):
        self.backup_control_version_to('2.7.3', backup_id)

    def prepare_backup_for_detect_missed_database(self, backup_id):
        self.backup_control_version_to_2_8_1(backup_id)

        with self.modify_backup_control(self.backup_dir, 'node', backup_id, content=True) as content:
            new = []
            for line in content.data.splitlines(True):
                if 'pg_tblspc' in line:
                    st = line.index('pg_tblspc')
                    en = line.index('"', st)
                    path = line[st:en]
                    elems = path.split('/')
                    if len(elems) > 4 and len(elems[3]) >= 10:
                        # delete all files in database folder with giga-oid
                        continue
                new.append(line)
            content.data = "".join(new)

    def prepare_backup_for_detect_missed_tablespace(self, backup_id):
        self.backup_control_version_to_2_8_1(backup_id)

        with self.modify_backup_control(self.backup_dir, 'node', backup_id, content=True) as content:
            new = []
            for line in content.data.splitlines(True):
                if 'pg_tblspc' in line:
                    st = line.index('pg_tblspc')
                    en = line.index('"', st)
                    path = line[st:en]
                    elems = path.split('/')
                    if len(elems) >= 2 and len(elems[1]) >= 10:
                        # delete giga-oid tablespace completely
                        continue
                new.append(line)
            content.data = "".join(new)

    def prepare_backup_for_detect_nondatafile_relation(self, backup_id):
        self.backup_control_version_to_2_8_1(backup_id)

        with self.modify_backup_control(self.backup_dir, 'node', backup_id, content=True) as content:
            new = []
            for line in content.data.splitlines(True):
                if 'base/' in line:
                    st = line.index('base/')
                    en = line.index('"', st)
                    path = line[st:en]
                    elems = path.split('/')
                    if len(elems) == 3 and len(elems[2]) >= 10 and elems[2].isdecimal():
                        # pretend it is not datafile
                        line = line.replace('"is_datafile":"1"', '"is_datafile":"0"')
                new.append(line)
            content.data = "".join(new)

    def prepare_backup_for_detect_gigaoid_database(self, backup_id):
        self.backup_control_version_to_2_8_1(backup_id)

        with self.modify_backup_control(self.backup_dir, 'node', backup_id, content=True) as content:
            new = []
            for line in content.data.splitlines(True):
                if 'base/' in line:
                    st = line.index('base/')
                    en = line.index('"', st)
                    path = line[st:en]
                    elems = path.split('/')
                    if len(elems) == 3 and len(elems[1]) >= 10 and elems[2].isdecimal():
                        # 1. change dbOid = dbOid / 10
                        # 2. pretend it is not datafile
                        line = line.replace('"is_datafile":"1"', '"is_datafile":"0"')
                        line = line.replace(f'"dbOid":"{elems[1]}"', f'"dbOid":"{int(elems[1])//10}"')
                new.append(line)
            content.data = "".join(new)
