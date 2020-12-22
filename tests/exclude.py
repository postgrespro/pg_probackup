import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException


module_name = 'exclude'


class ExcludeTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    def test_exclude_temp_files(self):
        """
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'logging_collector': 'on',
                'log_filename': 'postgresql.log'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        oid = node.safe_psql(
            'postgres',
            "select oid from pg_database where datname = 'postgres'").rstrip()

        pgsql_tmp_dir = os.path.join(node.data_dir, 'base', 'pgsql_tmp')

        os.mkdir(pgsql_tmp_dir)

        file = os.path.join(pgsql_tmp_dir, 'pgsql_tmp7351.16')
        with open(file, 'w') as f:
            f.write("HELLO")
            f.flush()
            f.close

        full_id = self.backup_node(
            backup_dir, 'node', node, backup_type='full', options=['--stream'])

        file = os.path.join(
            backup_dir, 'backups', 'node', full_id,
            'database', 'base', 'pgsql_tmp', 'pgsql_tmp7351.16')

        self.assertFalse(
            os.path.exists(file),
            "File must be excluded: {0}".format(file))

        # TODO check temporary tablespaces

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_exclude_temp_tables(self):
        """
        make node without archiving, create temp table, take full backup,
        check that temp table not present in backup catalogue
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        with node.connect("postgres") as conn:

            conn.execute(
                "create temp table test as "
                "select generate_series(0,50050000)::text")
            conn.commit()

            temp_schema_name = conn.execute(
                "SELECT nspname FROM pg_namespace "
                "WHERE oid = pg_my_temp_schema()")[0][0]
            conn.commit()

            temp_toast_schema_name = "pg_toast_" + temp_schema_name.replace(
                "pg_", "")
            conn.commit()

            conn.execute("create index test_idx on test (generate_series)")
            conn.commit()

            heap_path = conn.execute(
                "select pg_relation_filepath('test')")[0][0]
            conn.commit()

            index_path = conn.execute(
                "select pg_relation_filepath('test_idx')")[0][0]
            conn.commit()

            heap_oid = conn.execute("select 'test'::regclass::oid")[0][0]
            conn.commit()

            toast_path = conn.execute(
                "select pg_relation_filepath('{0}.{1}')".format(
                    temp_toast_schema_name, "pg_toast_" + str(heap_oid)))[0][0]
            conn.commit()

            toast_idx_path = conn.execute(
                "select pg_relation_filepath('{0}.{1}')".format(
                    temp_toast_schema_name,
                    "pg_toast_" + str(heap_oid) + "_index"))[0][0]
            conn.commit()

        temp_table_filename = os.path.basename(heap_path)
        temp_idx_filename = os.path.basename(index_path)
        temp_toast_filename = os.path.basename(toast_path)
        temp_idx_toast_filename = os.path.basename(toast_idx_path)

        self.backup_node(
            backup_dir, 'node', node, backup_type='full', options=['--stream'])

        for root, dirs, files in os.walk(backup_dir):
            for file in files:
                if file in [
                    temp_table_filename, temp_table_filename + ".1",
                    temp_idx_filename,
                    temp_idx_filename + ".1",
                    temp_toast_filename,
                    temp_toast_filename + ".1",
                    temp_idx_toast_filename,
                    temp_idx_toast_filename + ".1"
                        ]:
                    self.assertEqual(
                        1, 0,
                        "Found temp table file in backup catalogue.\n "
                        "Filepath: {0}".format(file))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_exclude_unlogged_tables_1(self):
        """
        make node without archiving, create unlogged table, take full backup,
        alter table to unlogged, take delta backup, restore delta backup,
        check that PGDATA`s are physically the same
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off',
                "shared_buffers": "10MB"})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        conn = node.connect()
        with node.connect("postgres") as conn:

            conn.execute(
                "create unlogged table test as "
                "select generate_series(0,5005000)::text")
            conn.commit()

            conn.execute("create index test_idx on test (generate_series)")
            conn.commit()

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='full', options=['--stream'])

        node.safe_psql('postgres', "alter table test set logged")

        self.backup_node(
            backup_dir, 'node', node, backup_type='delta',
            options=['--stream']
        )

        pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_exclude_log_dir(self):
        """
        check that by default 'log' and 'pg_log' directories are not backed up
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'logging_collector': 'on',
                'log_filename': 'postgresql.log'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='full', options=['--stream'])

        log_dir = node.safe_psql(
            'postgres',
            'show log_directory').decode('utf-8').rstrip()

        node.cleanup()

        self.restore_node(
            backup_dir, 'node', node, options=["-j", "4"])

        # check that PGDATA/log or PGDATA/pg_log do not exists
        path = os.path.join(node.data_dir, log_dir)
        log_file = os.path.join(path, 'postgresql.log')
        self.assertTrue(os.path.exists(path))
        self.assertFalse(os.path.exists(log_file))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_exclude_log_dir_1(self):
        """
        check that "--backup-pg-log" works correctly
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'logging_collector': 'on',
                'log_filename': 'postgresql.log'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        log_dir = node.safe_psql(
            'postgres',
            'show log_directory').decode('utf-8').rstrip()

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='full', options=['--stream', '--backup-pg-log'])

        node.cleanup()

        self.restore_node(
            backup_dir, 'node', node, options=["-j", "4"])

        # check that PGDATA/log or PGDATA/pg_log do not exists
        path = os.path.join(node.data_dir, log_dir)
        log_file = os.path.join(path, 'postgresql.log')
        self.assertTrue(os.path.exists(path))
        self.assertTrue(os.path.exists(log_file))

        # Clean after yourself
        self.del_test_dir(module_name, fname)
