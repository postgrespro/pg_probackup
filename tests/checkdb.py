import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from datetime import datetime, timedelta
import subprocess
from testgres import QueryException
import shutil
import sys
import time


module_name = 'checkdb'


class CheckdbTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    def test_checkdb_amcheck_only_sanity(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i"
            " as id from generate_series(0,100) i")

        node.safe_psql(
            "postgres",
            "create index on t_heap(id)")

        try:
            node.safe_psql(
                "postgres",
                "create extension amcheck")
        except QueryException as e:
            node.safe_psql(
                "postgres",
                "create extension amcheck_next")

        log_file_path = os.path.join(
            backup_dir, 'log', 'pg_probackup.log')

        # simple sanity
        try:
            self.checkdb_node(
                options=['--skip-block-validation'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because --amcheck option is missing\n"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Option '--skip-block-validation' must be "
                "used with '--amcheck' option",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        # simple sanity
        output = self.checkdb_node(
            options=[
                '--amcheck',
                '--skip-block-validation',
                '-d', 'postgres', '-p', str(node.port)])

        self.assertIn(
            'INFO: checkdb --amcheck finished successfully',
            output)
        self.assertIn(
            'All checked indexes are valid',
            output)

        # logging to file sanity
        try:
            self.checkdb_node(
                options=[
                    '--amcheck',
                    '--skip-block-validation',
                    '--log-level-file=verbose',
                    '-d', 'postgres', '-p', str(node.port)])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because log_directory missing\n"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Cannot save checkdb logs to a file. "
                "You must specify --log-directory option when "
                "running checkdb with --log-level-file option enabled",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        # If backup_dir provided, then instance name must be
        # provided too
        try:
            self.checkdb_node(
                backup_dir,
                options=[
                    '--amcheck',
                    '--skip-block-validation',
                    '--log-level-file=verbose',
                    '-d', 'postgres', '-p', str(node.port)])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because log_directory missing\n"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: required parameter not specified: --instance",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        # checkdb can use default or set in config values,
        # if backup_dir and instance name are provided
        self.checkdb_node(
            backup_dir,
            'node',
            options=[
                '--amcheck',
                '--skip-block-validation',
                '--log-level-file=verbose',
                '-d', 'postgres', '-p', str(node.port)])

        # check that file present and full of messages
        os.path.isfile(log_file_path)
        with open(log_file_path) as f:
            log_file_content = f.read()
            self.assertIn(
                'INFO: checkdb --amcheck finished successfully',
                log_file_content)
            self.assertIn(
                'VERBOSE: (query)',
                log_file_content)
        os.unlink(log_file_path)

        # log-level-file and log-directory are provided
        self.checkdb_node(
            backup_dir,
            'node',
            options=[
                '--amcheck',
                '--skip-block-validation',
                '--log-level-file=verbose',
                '--log-directory={0}'.format(
                    os.path.join(backup_dir, 'log')),
                '-d', 'postgres', '-p', str(node.port)])

        # check that file present and full of messages
        os.path.isfile(log_file_path)
        with open(log_file_path) as f:
            log_file_content = f.read()
            self.assertIn(
                'INFO: checkdb --amcheck finished successfully',
                log_file_content)
            self.assertIn(
                'VERBOSE: (query)',
                log_file_content)
        os.unlink(log_file_path)

        gdb = self.checkdb_node(
            gdb=True,
            options=[
                '--amcheck',
                '--skip-block-validation',
                '--log-level-file=verbose',
                '--log-directory={0}'.format(
                    os.path.join(backup_dir, 'log')),
                '-d', 'postgres', '-p', str(node.port)])

        gdb.set_breakpoint('amcheck_one_index')
        gdb.run_until_break()

        node.safe_psql(
            "postgres",
            "drop table t_heap")

        gdb.remove_all_breakpoints()

        gdb.continue_execution_until_exit()

        # check that message about missing index is present
        with open(log_file_path) as f:
            log_file_content = f.read()
            self.assertIn(
                'ERROR: checkdb --amcheck finished with failure',
                log_file_content)
            self.assertIn(
                "WARNING: Thread [1]. Amcheck failed in database 'postgres' "
                "for index: 'public.t_heap_id_idx':",
                log_file_content)
            self.assertIn(
                'ERROR:  could not open relation with OID',
                log_file_content)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_basic_checkdb_amcheck_only_sanity(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        # create two databases
        node.safe_psql("postgres", "create database db1")
        try:
            node.safe_psql(
               "db1",
               "create extension amcheck")
        except QueryException as e:
            node.safe_psql(
                "db1",
                "create extension amcheck_next")

        node.safe_psql("postgres", "create database db2")
        try:
            node.safe_psql(
                "db2",
                "create extension amcheck")
        except QueryException as e:
            node.safe_psql(
                "db2",
                "create extension amcheck_next")

        # init pgbench in two databases and corrupt both indexes
        node.pgbench_init(scale=5, dbname='db1')
        node.pgbench_init(scale=5, dbname='db2')

        node.safe_psql(
            "db2",
            "alter index pgbench_accounts_pkey rename to some_index")

        index_path_1 = os.path.join(
            node.data_dir,
            node.safe_psql(
                "db1",
                "select pg_relation_filepath('pgbench_accounts_pkey')").decode('utf-8').rstrip())

        index_path_2 = os.path.join(
            node.data_dir,
            node.safe_psql(
                "db2",
                "select pg_relation_filepath('some_index')").decode('utf-8').rstrip())

        try:
            self.checkdb_node(
                options=[
                    '--amcheck',
                    '--skip-block-validation',
                    '-d', 'postgres', '-p', str(node.port)])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because some db was not amchecked"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Some databases were not amchecked",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        node.stop()

        # Let`s do index corruption
        with open(index_path_1, "rb+", 0) as f:
            f.seek(42000)
            f.write(b"blablahblahs")
            f.flush()
            f.close

        with open(index_path_2, "rb+", 0) as f:
            f.seek(42000)
            f.write(b"blablahblahs")
            f.flush()
            f.close

        node.slow_start()

        log_file_path = os.path.join(
            backup_dir, 'log', 'pg_probackup.log')

        try:
            self.checkdb_node(
                options=[
                    '--amcheck',
                    '--skip-block-validation',
                    '--log-level-file=verbose',
                    '--log-directory={0}'.format(
                        os.path.join(backup_dir, 'log')),
                    '-d', 'postgres', '-p', str(node.port)])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because some db was not amchecked"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: checkdb --amcheck finished with failure",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        # corruption of both indexes in db1 and db2 must be detected
        # also the that amcheck is not installed in 'postgres'
        # should be logged
        with open(log_file_path) as f:
            log_file_content = f.read()
            self.assertIn(
                "WARNING: Thread [1]. Amcheck failed in database 'db1' "
                "for index: 'public.pgbench_accounts_pkey':",
                log_file_content)

            self.assertIn(
                "WARNING: Thread [1]. Amcheck failed in database 'db2' "
                "for index: 'public.some_index':",
                log_file_content)

            self.assertIn(
                "ERROR: checkdb --amcheck finished with failure",
                log_file_content)

        # Clean after yourself
        self.del_test_dir(module_name, fname, [node])

    # @unittest.skip("skip")
    def test_checkdb_block_validation_sanity(self):
        """make node, corrupt some pages, check that checkdb failed"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
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

        # sanity
        try:
            self.checkdb_node()
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because pgdata must be specified\n"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: required parameter not specified: PGDATA (-D, --pgdata)",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        self.checkdb_node(
            data_dir=node.data_dir,
            options=['-d', 'postgres', '-p', str(node.port)])

        self.checkdb_node(
            backup_dir, 'node',
            options=['-d', 'postgres', '-p', str(node.port)])

        heap_full_path = os.path.join(node.data_dir, heap_path)

        with open(heap_full_path, "rb+", 0) as f:
                f.seek(9000)
                f.write(b"bla")
                f.flush()
                f.close

        with open(heap_full_path, "rb+", 0) as f:
                f.seek(42000)
                f.write(b"bla")
                f.flush()
                f.close

        try:
            self.checkdb_node(
                backup_dir, 'node',
                options=['-d', 'postgres', '-p', str(node.port)])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because of data corruption\n"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Checkdb failed",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

            self.assertIn(
                'WARNING: Corruption detected in file "{0}", block 1'.format(
                    os.path.normpath(heap_full_path)),
                e.message)

            self.assertIn(
                'WARNING: Corruption detected in file "{0}", block 5'.format(
                    os.path.normpath(heap_full_path)),
                e.message)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_checkdb_sigint_handling(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        try:
            node.safe_psql(
               "postgres",
               "create extension amcheck")
        except QueryException as e:
            node.safe_psql(
                "postgres",
                "create extension amcheck_next")

        # FULL backup
        gdb = self.checkdb_node(
            backup_dir, 'node', gdb=True,
            options=[
                '-d', 'postgres', '-j', '2',
                '--skip-block-validation',
                '--progress',
                '--amcheck', '-p', str(node.port)])

        gdb.set_breakpoint('amcheck_one_index')
        gdb.run_until_break()

        gdb.continue_execution_until_break(20)
        gdb.remove_all_breakpoints()

        gdb._execute('signal SIGINT')
        gdb.continue_execution_until_error()

        with open(node.pg_log_file, 'r') as f:
            output = f.read()

        self.assertNotIn('could not receive data from client', output)
        self.assertNotIn('could not send data to client', output)
        self.assertNotIn('connection to client lost', output)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_checkdb_with_least_privileges(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            'postgres',
            'CREATE DATABASE backupdb')

        try:
            node.safe_psql(
               "backupdb",
               "create extension amcheck")
        except QueryException as e:
            node.safe_psql(
                "backupdb",
                "create extension amcheck_next")

        node.safe_psql(
            'backupdb',
            "REVOKE ALL ON DATABASE backupdb from PUBLIC; "
            "REVOKE ALL ON SCHEMA public from PUBLIC; "
            "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC; "
            "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC; "
            "REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC; "
            "REVOKE ALL ON SCHEMA pg_catalog from PUBLIC; "
            "REVOKE ALL ON ALL TABLES IN SCHEMA pg_catalog FROM PUBLIC; "
            "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pg_catalog FROM PUBLIC; "
            "REVOKE ALL ON ALL SEQUENCES IN SCHEMA pg_catalog FROM PUBLIC; "
            "REVOKE ALL ON SCHEMA information_schema from PUBLIC; "
            "REVOKE ALL ON ALL TABLES IN SCHEMA information_schema FROM PUBLIC; "
            "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA information_schema FROM PUBLIC; "
            "REVOKE ALL ON ALL SEQUENCES IN SCHEMA information_schema FROM PUBLIC;")

        # PG 9.5
        if self.get_version(node) < 90600:
            node.safe_psql(
                'backupdb',
                'CREATE ROLE backup WITH LOGIN; '
                'GRANT CONNECT ON DATABASE backupdb to backup; '
                'GRANT USAGE ON SCHEMA pg_catalog TO backup; '
                'GRANT USAGE ON SCHEMA public TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_proc TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_database TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_am TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_class TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_index TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_namespace TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.nameeq(name, name) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.namene(name, name) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.int8(integer) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.oideq(oid, oid) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.charne("char", "char") TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO backup; '
                'GRANT EXECUTE ON FUNCTION bt_index_check(regclass) TO backup; '
                'GRANT EXECUTE ON FUNCTION bt_index_check(regclass, bool) TO backup;'
            )
        # PG 9.6
        elif self.get_version(node) > 90600 and self.get_version(node) < 100000:
            node.safe_psql(
                'backupdb',
                'CREATE ROLE backup WITH LOGIN; '
                'GRANT CONNECT ON DATABASE backupdb to backup; '
                'GRANT USAGE ON SCHEMA pg_catalog TO backup; '
                'GRANT USAGE ON SCHEMA public TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_proc TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_database TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_am TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_class TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_index TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_namespace TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.nameeq(name, name) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.namene(name, name) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.int8(integer) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.oideq(oid, oid) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.charne("char", "char") TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO backup; '
                'GRANT EXECUTE ON FUNCTION bt_index_check(regclass) TO backup; '
                'GRANT EXECUTE ON FUNCTION bt_index_check(regclass, bool) TO backup;'
            )
        # >= 10
        else:
            node.safe_psql(
                'backupdb',
                'CREATE ROLE backup WITH LOGIN; '
                'GRANT CONNECT ON DATABASE backupdb to backup; '
                'GRANT USAGE ON SCHEMA pg_catalog TO backup; '
                'GRANT USAGE ON SCHEMA public TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_proc TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_database TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_am TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_class TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_index TO backup; '
                'GRANT SELECT ON TABLE pg_catalog.pg_namespace TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.nameeq(name, name) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.namene(name, name) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.int8(integer) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.oideq(oid, oid) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.charne("char", "char") TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO backup; '
                'GRANT EXECUTE ON FUNCTION bt_index_check(regclass) TO backup; '
                'GRANT EXECUTE ON FUNCTION bt_index_check(regclass, bool) TO backup;'
            )

#        if ProbackupTest.enterprise:
#            node.safe_psql(
#                "backupdb",
#                "GRANT EXECUTE ON FUNCTION pg_catalog.pgpro_edition() TO backup")
#
#            node.safe_psql(
#                "backupdb",
#                "GRANT EXECUTE ON FUNCTION pg_catalog.pgpro_version() TO backup")

        # checkdb
        try:
            self.checkdb_node(
                backup_dir, 'node',
                options=[
                    '--amcheck', '-U', 'backup',
                    '-d', 'backupdb', '-p', str(node.port)])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because permissions are missing\n"
                " Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "INFO: Amcheck succeeded for database 'backupdb'",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

            self.assertIn(
                "WARNING: Extension 'amcheck' or 'amcheck_next' are "
                "not installed in database postgres",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

            self.assertIn(
                "ERROR: Some databases were not amchecked",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))
