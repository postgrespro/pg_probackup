import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest


class ExcludeTest(ProbackupTest):

    # @unittest.skip("skip")
    def test_exclude_temp_files(self):
        """
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'logging_collector': 'on',
                'log_filename': 'postgresql.log'})

        self.pb.init()
        self.pb.add_instance('node', node)
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

        full_id = self.pb.backup_node('node', node, backup_type='full', options=['--stream'])

        self.assertFalse(
            self.backup_file_exists(backup_dir, 'node', full_id,
                                    'database/base/pgsql_tmp/pgsql_tmp7351.16'),
            "File must be excluded: database/base/pgsql_tmp/pgsql_tmp7351.16"
        )

        # TODO check temporary tablespaces

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_exclude_temp_tables(self):
        """
        make node without archiving, create temp table, take full backup,
        check that temp table not present in backup catalogue
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
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

        self.pb.backup_node('node', node, backup_type='full', options=['--stream'])

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

    # @unittest.skip("skip")
    def test_exclude_unlogged_tables_1(self):
        """
        make node without archiving, create unlogged table, take full backup,
        alter table to unlogged, take delta backup, restore delta backup,
        check that PGDATA`s are physically the same
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                "shared_buffers": "10MB"})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        conn = node.connect()
        with node.connect("postgres") as conn:

            conn.execute(
                "create unlogged table test as "
                "select generate_series(0,5005000)::text")
            conn.commit()

            conn.execute("create index test_idx on test (generate_series)")
            conn.commit()

        self.pb.backup_node('node', node,
            backup_type='full', options=['--stream'])

        node.safe_psql('postgres', "alter table test set logged")

        self.pb.backup_node('node', node, backup_type='delta',
            options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.pg_node.make_simple('node_restored')

        node_restored.cleanup()

        self.pb.restore_node('node', node_restored, options=["-j", "4"])

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_exclude_unlogged_tables_2(self):
        """
        1. make node, create unlogged, take FULL, DELTA, PAGE,
            check that unlogged table files was not backed up
        2. restore FULL, DELTA, PAGE to empty db,
            ensure unlogged table exist and is epmty
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                "shared_buffers": "10MB"})

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        backup_ids = []

        for backup_type in ['full', 'delta', 'page']:

            if backup_type == 'full':
                node.safe_psql(
                    'postgres',
                    'create unlogged table test as select generate_series(0,20050000)::text')
            else:
                node.safe_psql(
                    'postgres',
                    'insert into test select generate_series(0,20050000)::text')

            rel_path = node.execute(
                'postgres',
                "select pg_relation_filepath('test')")[0][0]

            backup_id = self.pb.backup_node('node', node,
                backup_type=backup_type, options=['--stream'])

            backup_ids.append(backup_id)

            filelist = self.get_backup_filelist(backup_dir, 'node', backup_id)

            self.assertNotIn(
                rel_path, filelist,
                "Unlogged table was not excluded")

            self.assertNotIn(
                rel_path + '.1', filelist,
                "Unlogged table was not excluded")

            self.assertNotIn(
                rel_path + '.2', filelist,
                "Unlogged table was not excluded")

            self.assertNotIn(
                rel_path + '.3', filelist,
                "Unlogged table was not excluded")

        # ensure restoring retrieves back only empty unlogged table
        for backup_id in backup_ids:
            node.stop()
            node.cleanup()

            self.pb.restore_node('node', node=node, backup_id=backup_id)

            node.slow_start()

            self.assertEqual(
                node.execute(
                    'postgres',
                    'select count(*) from test')[0][0],
                0)

    # @unittest.skip("skip")
    def test_exclude_log_dir(self):
        """
        check that by default 'log' and 'pg_log' directories are not backed up
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'logging_collector': 'on',
                'log_filename': 'postgresql.log'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        self.pb.backup_node('node', node,
            backup_type='full', options=['--stream'])

        log_dir = node.safe_psql(
            'postgres',
            'show log_directory').decode('utf-8').rstrip()

        node.cleanup()

        self.pb.restore_node('node', node, options=["-j", "4"])

        # check that PGDATA/log or PGDATA/pg_log do not exists
        path = os.path.join(node.data_dir, log_dir)
        log_file = os.path.join(path, 'postgresql.log')
        self.assertTrue(os.path.exists(path))
        self.assertFalse(os.path.exists(log_file))

    # @unittest.skip("skip")
    def test_exclude_log_dir_1(self):
        """
        check that "--backup-pg-log" works correctly
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'logging_collector': 'on',
                'log_filename': 'postgresql.log'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        log_dir = node.safe_psql(
            'postgres',
            'show log_directory').decode('utf-8').rstrip()

        self.pb.backup_node('node', node,
            backup_type='full', options=['--stream', '--backup-pg-log'])

        node.cleanup()

        self.pb.restore_node('node', node, options=["-j", "4"])

        # check that PGDATA/log or PGDATA/pg_log do not exists
        path = os.path.join(node.data_dir, log_dir)
        log_file = os.path.join(path, 'postgresql.log')
        self.assertTrue(os.path.exists(path))
        self.assertTrue(os.path.exists(log_file))
