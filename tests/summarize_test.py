import os
import unittest
from time import sleep, time
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException


class SummarizeTest(ProbackupTest, unittest.TestCase):
    def _skip_unless_pg17(self):
        if self.pg_config_version < self.version_to_num('17.0'):
            self.skipTest('SUMMARIZE mode requires PostgreSQL 17 or higher')

    def _settle(self, node, backup_type):
        target = node.safe_psql(
            "postgres", "SELECT pg_current_wal_lsn()").decode('utf-8').strip()
        node.safe_psql("postgres", "SELECT pg_switch_wal()")
        node.safe_psql("postgres", "CHECKPOINT")
        if backup_type == 'summarize':
            start = time()
            while time() - start < 60:
                if node.safe_psql(
                        "postgres",
                        "SELECT summarized_lsn >= '{0}'::pg_lsn "
                        "FROM pg_get_wal_summarizer_state()".format(target)
                ).decode('utf-8').strip() == 't':
                    return
                sleep(0.3)
        else:
            sleep(1)

    def _incr_options(self, backup_type):
        return ['--stream'] if backup_type == 'summarize' else []

    def _make_node(self, extra_options=None):
        options = {'summarize_wal': 'on'}
        if extra_options:
            options.update(extra_options)
        return self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options=options)

    def test_summarize_basic(self):
        """
        1. make node with summarize_wal
        2. FULL
        3. update a small table
        4. take wal summarize backup
        5. restore
        """
        self._skip_unless_pg17()
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self._make_node()
        node_restored = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node_restored'))
        node_restored.cleanup()

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text "
            "from generate_series(0, 20000) i")
        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        node.safe_psql(
            "postgres", "update t_heap set text = md5((id + 1)::text) where id % 3 = 0")
        node.safe_psql("postgres", "delete from t_heap where id % 7 = 0")
        node.safe_psql(
            "postgres",
            "insert into t_heap select i, md5(i::text) from generate_series(20001, 25000) i")

        self._settle(node, 'summarize')
        self.backup_node(backup_dir, 'node', node,
                         backup_type='summarize', options=['--stream'])

        pgdata = self.pgdata_content(node.data_dir)
        before = node.table_checksum("t_heap")

        self.restore_node(backup_dir, 'node', node_restored)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()
        self.assertEqual(before, node_restored.table_checksum("t_heap"))

    def test_summarize_incremental_is_small(self):
        """
        a summarize backup must be far smaller than the FULL
        """
        self._skip_unless_pg17()
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self._make_node({'wal_keep_size': '1GB', 'max_wal_size': '2GB'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t (id int primary key, payload text) "
            "with (autovacuum_enabled=off)")
        node.safe_psql(
            "postgres",
            "insert into t select i, repeat('a', 1000) from generate_series(1, 250000) i")
        node.safe_psql("postgres", "vacuum (freeze, analyze) t")
        node.safe_psql("postgres", "checkpoint")
        self._settle(node, 'summarize')

        full_id = self.backup_node(backup_dir, 'node', node, options=['--stream'])

        node.safe_psql("postgres", "update t set payload = repeat('b', 1000) where id <= 100")
        self._settle(node, 'summarize')
        summ_id = self.backup_node(backup_dir, 'node', node,
                                   backup_type='summarize', options=['--stream'])

        full_bytes = self.show_pb(backup_dir, 'node', backup_id=full_id)["data-bytes"]
        summ_bytes = self.show_pb(backup_dir, 'node', backup_id=summ_id)["data-bytes"]
        self.assertLess(
            summ_bytes, full_bytes // 2,
            "summarize backup of a 100-row change ({0} B) is not << full ({1} B)".format(
                summ_bytes, full_bytes))

    def test_summarize_requires_summarize_wal(self):
        """summarize backup must fail with a clear error when summarize_wal=off"""
        self._skip_unless_pg17()
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'summarize_wal': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id from generate_series(0, 1000) i")
        self.backup_node(backup_dir, 'node', node, options=['--stream'])
        node.safe_psql("postgres", "insert into t_heap select generate_series(1001, 2000)")

        try:
            self.backup_node(backup_dir, 'node', node,
                             backup_type='summarize', options=['--stream'])
            self.fail("summarize backup should fail when summarize_wal=off")
        except ProbackupException as e:
            self.assertIn('summarize_wal', e.message, e.message)

    # ------------------------------------------------------------------ #
    # data integrity: a >1GB (multi-segment) relation, changes in segment .1
    # ------------------------------------------------------------------ #
    def _segmented_relation(self, backup_type):
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self._make_node(
            {'wal_keep_size': '2GB', 'max_wal_size': '4GB', 'checkpoint_timeout': '1h'})
        node_restored = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node_restored'))
        node_restored.cleanup()

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create table t (id int primary key, payload text) "
            "with (autovacuum_enabled=off)")
        node.safe_psql(
            "postgres",
            "insert into t select i, repeat('a', 1000) from generate_series(1, 1300000) i")
        size = int(node.safe_psql(
            "postgres", "select pg_relation_size('t')").decode('utf-8').strip())
        self.assertGreater(size, 1024 ** 3, "relation must exceed 1GB to span segments")

        node.safe_psql("postgres", "vacuum (freeze, analyze) t")
        node.safe_psql("postgres", "checkpoint")
        self._settle(node, backup_type)

        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        node.safe_psql("postgres", "update t set payload = repeat('Z', 1000) where id > 1299900")
        expected = int(node.safe_psql(
            "postgres", "select count(*) from t where payload like 'Z%'").decode().strip())
        self._settle(node, backup_type)
        self.backup_node(backup_dir, 'node', node,
                         backup_type=backup_type, options=self._incr_options(backup_type))

        self.restore_node(backup_dir, 'node', node_restored)
        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()
        got = int(node_restored.safe_psql(
            "postgres", "select count(*) from t where payload like 'Z%'").decode().strip())
        self.assertEqual(
            expected, got,
            "[{0}] segment .1: {1}/{2} changed rows restored".format(backup_type, got, expected))

    def test_page_segmented_relation(self):
        """PAGE with segments .1 of a >1GB db"""
        self._skip_unless_pg17()
        self._segmented_relation('page')

    def test_summarize_segmented_relation(self):
        """SUMMARIZE with segments .1 of a >1GB db"""
        self._skip_unless_pg17()
        self._segmented_relation('summarize')

    # ------------------------------------------------------------------ #
    # data integrity: same relations changed across several WAL summary files
    # ------------------------------------------------------------------ #
    NT = 12
    ROUNDS = [
        (1, [6, 7, 8, 9, 10, 11]),
        (2, [0, 1, 2, 3, 4, 5, 6, 7]),
        (3, [8, 9, 10, 11, 0, 2]),
        (4, [1, 3, 5, 7, 9, 11]),
        (5, [10, 8, 6, 4, 2, 0]),
        (6, [11, 9, 7, 5, 3, 1]),
    ]

    def _changed_rows(self, node):
        return sum(int(node.safe_psql(
            "postgres", "select count(*) from t{0} where payload like 'r%'".format(t)
        ).decode('utf-8').strip()) for t in range(self.NT))

    def _overlapping_changes(self, backup_type):
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self._make_node()
        node_restored = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node_restored'))
        node_restored.cleanup()

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        for t in range(self.NT):
            node.safe_psql(
                "postgres",
                "create table t{0} (id int primary key, payload text) "
                "with (autovacuum_enabled=off)".format(t))
            node.safe_psql(
                "postgres",
                "insert into t{0} select i, repeat('a', 200) "
                "from generate_series(1, 8000) i".format(t))
        node.safe_psql("postgres", "vacuum (freeze, analyze)")
        node.safe_psql("postgres", "checkpoint")
        self._settle(node, backup_type)

        self.backup_node(backup_dir, 'node', node, options=['--stream'])

        # each change lands in a distinct block (id = round*1000 + 1)
        for rnd, tables in self.ROUNDS:
            for t in tables:
                node.safe_psql(
                    "postgres",
                    "update t{0} set payload = 'r{1}' where id = {2}".format(
                        t, rnd, rnd * 1000 + 1))
            self._settle(node, backup_type)

        expected = self._changed_rows(node)
        self.backup_node(backup_dir, 'node', node,
                         backup_type=backup_type, options=self._incr_options(backup_type))

        self.restore_node(backup_dir, 'node', node_restored)
        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()
        got = self._changed_rows(node_restored)
        self.assertEqual(
            expected, got,
            "[{0}] overlapping changes across summary files: {1}/{2} rows restored".format(
                backup_type, got, expected))

    def test_page_overlapping_changes(self):
        self._skip_unless_pg17()
        self._overlapping_changes('page')

    def test_summarize_overlapping_changes(self):
        self._skip_unless_pg17()
        self._overlapping_changes('summarize')

    def test_summarize_catchup(self):
        self._skip_unless_pg17()
        src = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'src'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'summarize_wal': 'on', 'wal_keep_size': '2GB',
                        'max_wal_size': '4GB', 'checkpoint_timeout': '1h'})
        src.slow_start()

        src.safe_psql(
            "postgres",
            "create table big (id int primary key, payload text) "
            "with (autovacuum_enabled=off)")
        src.safe_psql(
            "postgres",
            "insert into big select i, repeat('a', 1000) from generate_series(1, 1300000) i")
        size = int(src.safe_psql(
            "postgres", "select pg_relation_size('big')").decode('utf-8').strip())
        self.assertGreater(size, 1024 ** 3)
        src.safe_psql("postgres", "vacuum (freeze, analyze) big")
        src.safe_psql("postgres", "checkpoint")

        dst = self.make_empty_node(os.path.join(self.module_name, self.fname, 'dst'))
        self.catchup_node(
            backup_mode='FULL', source_pgdata=src.data_dir, destination_node=dst,
            options=['-d', 'postgres', '-p', str(src.port), '--stream'])
        self.set_replica(src, dst)
        self.set_auto_conf(dst, {'port': str(dst.port)})
        dst.slow_start(replica=True)
        dst.stop()

        # change blocks in segment .1 on the source
        src.safe_psql("postgres", "update big set payload = repeat('Z', 1000) where id > 1299900")
        expected = int(src.safe_psql(
            "postgres", "select count(*) from big where payload like 'Z%'").decode('utf-8').strip())
        self._settle(src, 'summarize')

        self.catchup_node(
            backup_mode='summarize', source_pgdata=src.data_dir, destination_node=dst,
            options=['-d', 'postgres', '-p', str(src.port), '--stream'])

        self.compare_pgdata(self.pgdata_content(src.data_dir), self.pgdata_content(dst.data_dir))

        src.stop()
        self.set_replica(master=src, replica=dst)
        self.set_auto_conf(dst, {'port': str(dst.port)})
        dst.slow_start(replica=True)
        got = int(dst.safe_psql(
            "postgres", "select count(*) from big where payload like 'Z%'").decode('utf-8').strip())
        dst.stop()
        self.assertEqual(
            expected, got,
            "catchup summarize: {0}/{1} changed rows in segment .1".format(got, expected))
