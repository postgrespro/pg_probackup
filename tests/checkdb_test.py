import os
from .helpers.ptrack_helpers import ProbackupTest
from pg_probackup2.gdb import needs_gdb
from testgres import QueryException
from parameterized import parameterized
from .helpers.data_helpers import corrupt_data_file, validate_data_file


class CheckdbTest(ProbackupTest):

    # @unittest.skip("skip")
    @needs_gdb
    def test_checkdb_amcheck_only_sanity(self):
        """"""

        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i"
            " as id from generate_series(0,100) i")

        node.safe_psql(
            "postgres",
            "create index on t_heap(id)")

        node.safe_psql(
            "postgres",
            "create table idxpart (a int) "
            "partition by range (a)")

        # there aren't partitioned indexes on 10 and lesser versions
        if self.pg_config_version >= 110000:
            node.safe_psql(
                "postgres",
                "create index on idxpart(a)")

        try:
            node.safe_psql(
                "postgres",
                "create extension amcheck")
        except QueryException as e:
            node.safe_psql(
                "postgres",
                "create extension amcheck_next")

        # simple sanity
        self.pb.checkdb_node(options=['--skip-block-validation'],
                          expect_error="because --amcheck options is missing")
        self.assertMessage(contains="ERROR: Option '--skip-block-validation' must be "
                                    "used with '--amcheck' option")

        # simple sanity
        output = self.pb.checkdb_node(
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
        self.pb.checkdb_node(
                options=[
                    '--amcheck',
                    '--skip-block-validation',
                    '--log-level-file=verbose',
                    '-d', 'postgres', '-p', str(node.port)],
                skip_log_directory=True,
                expect_error="because log_directory missing")
        self.assertMessage(contains=
                "ERROR: Cannot save checkdb logs to a file. "
                "You must specify --log-directory option when "
                "running checkdb with --log-level-file option enabled")

        # If backup_dir provided, then instance name must be
        # provided too
        self.pb.checkdb_node(
                use_backup_dir=True,
                options=[
                    '--amcheck',
                    '--skip-block-validation',
                    '--log-level-file=verbose',
                    '-d', 'postgres', '-p', str(node.port)],
                expect_error="because instance missing"
        )
        self.assertMessage(contains="ERROR: Required parameter not specified: --instance")

        # checkdb can use default or set in config values,
        # if backup_dir and instance name are provided
        self.pb.checkdb_node(
            use_backup_dir=True,
            instance='node',
            options=[
                '--amcheck',
                '--skip-block-validation',
                '--log-level-file=verbose',
                '-d', 'postgres', '-p', str(node.port)])

        # check that file present and full of messages
        log_file_content = self.read_pb_log()
        self.assertIn(
            'INFO: checkdb --amcheck finished successfully',
            log_file_content)
        self.assertIn(
            'VERBOSE: (query)',
            log_file_content)
        self.unlink_pg_log()

        # log-level-file and log-directory are provided
        self.pb.checkdb_node(
            use_backup_dir=True,
            instance='node',
            options=[
                '--amcheck',
                '--skip-block-validation',
                '--log-level-file=verbose',
                '-d', 'postgres', '-p', str(node.port)])

        # check that file present and full of messages
        log_file_content = self.read_pb_log()
        self.assertIn(
            'INFO: checkdb --amcheck finished successfully',
            log_file_content)
        self.assertIn(
            'VERBOSE: (query)',
            log_file_content)
        self.unlink_pg_log()

        gdb = self.pb.checkdb_node(
            gdb=True,
            options=[
                '--amcheck',
                '--skip-block-validation',
                '--log-level-file=verbose',
                '-d', 'postgres', '-p', str(node.port)])

        gdb.set_breakpoint('amcheck_one_index')
        gdb.run_until_break()

        node.safe_psql(
            "postgres",
            "drop table t_heap")

        gdb.continue_execution_until_exit()

        # check that message about missing index is present
        log_file_content = self.read_pb_log()
        self.assertIn(
            'ERROR: checkdb --amcheck finished with failure',
            log_file_content)
        self.assertIn(
            "Amcheck failed in database 'postgres' "
            "for index: 'public.t_heap_id_idx':",
            log_file_content)
        self.assertIn(
            'ERROR:  could not open relation with OID',
            log_file_content)

        # Clean after yourself
        gdb.kill()
        node.stop()

    # @unittest.skip("skip")
    def test_basic_checkdb_amcheck_only_sanity(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
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

        self.pb.checkdb_node(
                options=[
                    '--amcheck',
                    '--skip-block-validation',
                    '-d', 'postgres', '-p', str(node.port)],
                expect_error="because some db was not amchecked")
        self.assertMessage(contains="ERROR: Some databases were not amchecked")

        node.stop()

        # Let`s do index corruption
        with open(index_path_1, "rb+", 0) as f:
            f.seek(42000)
            f.write(b"blablahblahs")
            f.flush()

        with open(index_path_2, "rb+", 0) as f:
            f.seek(42000)
            f.write(b"blablahblahs")
            f.flush()

        node.slow_start()

        self.pb.checkdb_node(
                options=[
                    '--amcheck',
                    '--skip-block-validation',
                    '--log-level-file=verbose',
                    '-d', 'postgres', '-p', str(node.port)],
                expect_error="because some file checks failed")
        self.assertMessage(contains="ERROR: checkdb --amcheck finished with failure")

        # corruption of both indexes in db1 and db2 must be detected
        # also the that amcheck is not installed in 'postgres'
        # should be logged
        log_file_content = self.read_pb_log()
        self.assertMessage(log_file_content, contains=
                "Amcheck failed in database 'db1' "
                "for index: 'public.pgbench_accounts_pkey':")

        self.assertMessage(log_file_content, contains=
                "Amcheck failed in database 'db2' "
                "for index: 'public.some_index':")

        self.assertMessage(log_file_content, contains=
                "ERROR: checkdb --amcheck finished with failure")

        # Clean after yourself
        node.stop()

    # @unittest.skip("skip")
    def test_checkdb_block_validation_sanity(self):
        """make node, corrupt some pages, check that checkdb failed"""
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
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
        self.pb.checkdb_node(expect_error="because pgdata must be specified")
        self.assertMessage(contains="No postgres data directory specified.\nPlease specify it either using environment variable PGDATA or\ncommand line option --pgdata (-D)")

        self.pb.checkdb_node(
            data_dir=node.data_dir,
            options=['-d', 'postgres', '-p', str(node.port)])

        self.pb.checkdb_node(use_backup_dir=True, instance='node',
            options=['-d', 'postgres', '-p', str(node.port)])

        heap_full_path = os.path.join(node.data_dir, heap_path)

        with open(heap_full_path, "rb+", 0) as f:
                f.seek(9000)
                f.write(b"bla")
                f.flush()

        with open(heap_full_path, "rb+", 0) as f:
                f.seek(42000)
                f.write(b"bla")
                f.flush()

        self.pb.checkdb_node(use_backup_dir=True, instance='node',
                options=['-d', 'postgres', '-p', str(node.port)],
                expect_error="because of data corruption")
        self.assertMessage(contains="ERROR: Checkdb failed")
        self.assertMessage(contains='WARNING: Corruption detected in file "{0}", block 1'.format(
                    os.path.normpath(heap_full_path)))

        self.assertMessage(contains='WARNING: Corruption detected in file "{0}", block 5'.format(
                    os.path.normpath(heap_full_path)))

        # Clean after yourself
        node.stop()

    # @unittest.skip("skip")
    @parameterized.expand(("vm", "fsm"))
    def test_checkdb_nondatafile_validation(self, fork_kind):
        """make node, corrupt vm file, check that checkdb failed"""
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1000) i")
        node.safe_psql(
            "postgres",
            "CHECKPOINT;")

        node.safe_psql(
            "postgres",
            "VACUUM t_heap;")

        node.safe_psql(
            "postgres",
            "CHECKPOINT;")

        heap_path = node.safe_psql(
            "postgres",
            "select pg_relation_filepath('t_heap')").decode('utf-8').rstrip()

        heap_path += "_" + fork_kind

        self.pb.checkdb_node(use_backup_dir=True, instance='node',
                             options=['-d', 'postgres', '-p', str(node.port)])

        heap_full_path = os.path.join(node.data_dir, heap_path)
        self.assertTrue(os.path.exists(heap_full_path))

        self.assertTrue(validate_data_file(heap_full_path))
        self.assertTrue(corrupt_data_file(heap_full_path), "corrupting file error")

        self.pb.checkdb_node(use_backup_dir=True, instance='node',
                options=['-d', 'postgres', '-p', str(node.port)],
                expect_error="because of data corruption")
        self.assertMessage(contains="ERROR: Checkdb failed")
        self.assertMessage(contains='WARNING: Corruption detected in file "{0}"'.format(
                    os.path.normpath(heap_full_path)))

    def test_checkdb_checkunique(self):
        """Test checkunique parameter of amcheck.bt_index_check function"""
        node = self.pg_node.make_simple('node')
        node.slow_start()

        try:
            node.safe_psql(
               "postgres",
               "create extension amcheck")
        except QueryException as e:
            node.safe_psql(
                "postgres",
                "create extension amcheck_next")

        # Part of https://commitfest.postgresql.org/32/2976/ patch test
        node.safe_psql(
                "postgres",
                "CREATE TABLE bttest_unique(a varchar(50), b varchar(1500), c bytea, d varchar(50)); "
                "ALTER TABLE bttest_unique SET (autovacuum_enabled = false); "
                "CREATE UNIQUE INDEX bttest_unique_idx ON bttest_unique(a,b); "
                "UPDATE pg_catalog.pg_index SET indisunique = false "
                "WHERE indrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = 'bttest_unique'); "
                "INSERT INTO bttest_unique "
                "        SELECT  i::text::varchar, "
                "                        array_to_string(array( "
                "                                SELECT substr('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', ((random()*(36-1)+1)::integer), 1) "
                "                        FROM generate_series(1,1300)),'')::varchar, "
                "        i::text::bytea, i::text::varchar "
                "        FROM generate_series(0,1) AS i, generate_series(0,30) AS x; "
                "UPDATE pg_catalog.pg_index SET indisunique = true "
                "WHERE indrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = 'bttest_unique'); "
                "DELETE FROM bttest_unique WHERE ctid::text='(0,2)'; "
                "DELETE FROM bttest_unique WHERE ctid::text='(4,2)'; "
                "DELETE FROM bttest_unique WHERE ctid::text='(4,3)'; "
                "DELETE FROM bttest_unique WHERE ctid::text='(9,3)';")

        # run without checkunique option (error will not detected)
        output = self.pb.checkdb_node(
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

        # run with checkunique option
        if (ProbackupTest.enterprise and
                (self.pg_config_version >= 111300 and self.pg_config_version < 120000
                 or self.pg_config_version >= 120800 and self.pg_config_version < 130000
                 or self.pg_config_version >= 130400 and self.pg_config_version < 160000
                 or self.pg_config_version > 160000)):
            self.pb.checkdb_node(
                options=[
                    '--amcheck',
                    '--skip-block-validation',
                    '--checkunique',
                    '-d', 'postgres', '-p', str(node.port)],
                expect_error="because of index corruption")
            self.assertMessage(contains=
                "ERROR: checkdb --amcheck finished with failure. Not all checked indexes are valid. All databases were amchecked.")

            self.assertMessage(contains=
                "Amcheck failed in database 'postgres' for index: 'public.bttest_unique_idx'")

            self.assertMessage(regex=
                r"ERROR:[^\n]*(violating UNIQUE constraint|uniqueness is violated)")
        else:
            self.pb.checkdb_node(
                options=[
                    '--amcheck',
                    '--skip-block-validation',
                    '--checkunique',
                    '-d', 'postgres', '-p', str(node.port)])
            self.assertMessage(regex=
                r"WARNING: Extension 'amcheck(|_next)' version [\d.]* in schema 'public' do not support 'checkunique' parameter")

        # Clean after yourself
        node.stop()

    # @unittest.skip("skip")
    @needs_gdb
    def test_checkdb_sigint_handling(self):
        """"""
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
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
        gdb = self.pb.checkdb_node(use_backup_dir=True, instance='node', gdb=True,
            options=[
                '-d', 'postgres', '-j', '2',
                '--skip-block-validation',
                '--progress',
                '--amcheck', '-p', str(node.port)])

        gdb.set_breakpoint('amcheck_one_index')
        gdb.run_until_break()
        gdb.continue_execution_until_break(20)

        gdb.signal('SIGINT')
        gdb.continue_execution_until_error()

        with open(node.pg_log_file, 'r') as f:
            output = f.read()

        self.assertNotIn('could not receive data from client', output)
        self.assertNotIn('could not send data to client', output)
        self.assertNotIn('connection to client lost', output)

        # Clean after yourself
        gdb.kill()
        node.stop()

    # @unittest.skip("skip")
    def test_checkdb_with_least_privileges(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
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

        # PG 10
        if self.pg_config_version < 110000:
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
                'GRANT EXECUTE ON FUNCTION pg_catalog.set_config(text, text, boolean) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.texteq(text, text) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.nameeq(name, name) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.namene(name, name) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.int8(integer) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.oideq(oid, oid) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.charne("char", "char") TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.string_to_array(text, text) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.array_position(anyarray, anyelement) TO backup;'
                'GRANT EXECUTE ON FUNCTION bt_index_check(regclass) TO backup;'
            )
            if ProbackupTest.enterprise:
                # amcheck-1.1
                node.safe_psql(
                    'backupdb',
                    'GRANT EXECUTE ON FUNCTION bt_index_check(regclass, bool) TO backup')
        # >= 11 < 14
        elif self.pg_config_version > 110000 and self.pg_config_version < 140000:
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
                'GRANT EXECUTE ON FUNCTION pg_catalog.set_config(text, text, boolean) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.texteq(text, text) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.nameeq(name, name) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.namene(name, name) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.int8(integer) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.oideq(oid, oid) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.charne("char", "char") TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.string_to_array(text, text) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.array_position(anyarray, anyelement) TO backup; '
                'GRANT EXECUTE ON FUNCTION bt_index_check(regclass) TO backup; '
                'GRANT EXECUTE ON FUNCTION bt_index_check(regclass, bool) TO backup;'
            )
            # checkunique parameter
            if ProbackupTest.enterprise:
                if (self.pg_config_version >= 111300 and self.pg_config_version < 120000
                        or self.pg_config_version >= 120800 and self.pg_config_version < 130000
                        or self.pg_config_version >= 130400):
                    node.safe_psql(
                        "backupdb",
                        "GRANT EXECUTE ON FUNCTION bt_index_check(regclass, bool, bool) TO backup")
        # >= 14
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
                'GRANT EXECUTE ON FUNCTION pg_catalog.set_config(text, text, boolean) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.texteq(text, text) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.nameeq(name, name) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.namene(name, name) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.int8(integer) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.oideq(oid, oid) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.charne("char", "char") TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.string_to_array(text, text) TO backup; '
                'GRANT EXECUTE ON FUNCTION pg_catalog.array_position(anycompatiblearray, anycompatible) TO backup; '
                'GRANT EXECUTE ON FUNCTION bt_index_check(regclass) TO backup; '
                'GRANT EXECUTE ON FUNCTION bt_index_check(regclass, bool) TO backup;'
            )
            # checkunique parameter
            if ProbackupTest.enterprise and self.pg_config_version != 160000:
                node.safe_psql(
                    "backupdb",
                    "GRANT EXECUTE ON FUNCTION bt_index_check(regclass, bool, bool) TO backup")

        if ProbackupTest.pgpro:
            node.safe_psql(
                "backupdb",
                "GRANT EXECUTE ON FUNCTION pg_catalog.pgpro_version() TO backup;"
                "GRANT EXECUTE ON FUNCTION pg_catalog.pgpro_edition() TO backup;")

        # checkdb
        self.pb.checkdb_node(use_backup_dir=True, instance='node',
                          options=[
                              '--amcheck', '-U', 'backup',
                              '-d', 'backupdb', '-p', str(node.port)],
                          expect_error="because permissions are missing")
        self.assertMessage(contains="INFO: Amcheck succeeded for database 'backupdb'")
        self.assertMessage(contains="WARNING: Extension 'amcheck' or 'amcheck_next' "
                                    "are not installed in database postgres")
        self.assertMessage(contains="ERROR: Some databases were not amchecked")

        # Clean after yourself
        node.stop()
