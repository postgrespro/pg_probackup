import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException


module_name = 'exclude'


class ExcludeTest(ProbackupTest, unittest.TestCase):

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
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica', 'max_wal_senders': '2',
                'shared_buffers': '1GB', 'fsync': 'off', 'ptrack_enable': 'on'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.start()

        conn = node.connect()
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
        alter table to unlogged, take ptrack backup, restore ptrack backup,
        check that PGDATA`s are physically the same
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2',
                "shared_buffers": "10MB",
                "fsync": "off",
                'ptrack_enable': 'on'}
            )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.start()

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
            backup_dir, 'node', node, backup_type='ptrack',
            options=['--stream']
        )

        pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.make_simple_node(
            base_dir="{0}/{1}/node_restored".format(module_name, fname),
        )
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)
