import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException

module_name = 'catchup'

class CatchupTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    def dummy(self):
        """
        dummy test
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir = os.path.join(module_name, fname, 'node')
            )
        node.slow_start()

        # Clean after yourself
        node.stop()
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_multithread_local_transfer(self):
        """
        Test 'multithreaded basebackup' mode
        create node, insert some test data, catchup into other dir, start, select test data
        """
        fname = self.id().split('.')[3]

        source_pg = self.make_simple_node(base_dir = os.path.join(module_name, fname, 'src'))
        source_pg.slow_start()
        source_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question AS SELECT 42 AS answer")
        result = source_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")

        dest_pg = self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = source_pg.data_dir,
            destination_base_dir = os.path.join(module_name, fname, 'dst'),
            options = ['-d', 'postgres', '-p', str(source_pg.port), '--stream', '-j', '4']
            )
        source_pg.stop()

        dest_pg.slow_start()
        self.assertEqual(
            result,
            dest_pg.safe_psql("postgres", "SELECT * FROM ultimate_question"),
            'Different answer from copy')
        dest_pg.stop()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_multithread_remote_transfer(self):
        """
        Test 'multithreaded basebackup' mode
        create node, insert some test data, catchup into other dir, start, select test data
        """
        fname = self.id().split('.')[3]

        source_pg = self.make_simple_node(base_dir = os.path.join(module_name, fname, 'src'))
        source_pg.slow_start()
        source_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question AS SELECT 42 AS answer")
        result = source_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")

        dest_pg = self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = source_pg.data_dir,
            destination_base_dir = os.path.join(module_name, fname, 'dst'),
            options = ['-d', 'postgres', '-p', str(source_pg.port), '--stream', '-j', '4'])
        source_pg.stop()

        dest_pg.slow_start()
        self.assertEqual(
            result,
            dest_pg.safe_psql("postgres", "SELECT * FROM ultimate_question"),
            'Different answer from copy')
        dest_pg.stop()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_remote_catchup(self):
        """
        Test 'catchup' mode
        create node,
        make a copy with replication, start copy, stop copy,
        generate some load on master, insert some test data on master,
        catchup copy, start and select test data
        """
        fname = self.id().split('.')[3]

        # prepare master
        source_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, fname, 'src'),
            set_replication = True,
            ptrack_enable = True,
            initdb_params = ['--data-checksums']
            )
        source_pg.slow_start()
        source_pg.safe_psql("postgres", "CREATE EXTENSION ptrack")
        source_pg.safe_psql("postgres", "CREATE TABLE ultimate_question(answer int)")

        # make clean shutdowned lagging behind replica
        dest_pg = self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = source_pg.data_dir,
            destination_base_dir = os.path.join(module_name, fname, 'dst'),
            options = ['-d', 'postgres', '-p', str(source_pg.port), '--stream'])
        self.set_replica(source_pg, dest_pg)
        dest_pg.slow_start(replica = True)
        dest_pg.stop()

        # make changes on master
        source_pg.pgbench_init(scale=10)
        pgbench = source_pg.pgbench(options=['-T', '10', '--no-vacuum'])
        pgbench.wait()
        source_pg.safe_psql("postgres", "INSERT INTO ultimate_question VALUES(42)")
        result = source_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")

        # catchup
        self.catchup_node(
            backup_mode = 'PTRACK',
            source_pgdata = source_pg.data_dir,
            destination_base_dir = os.path.join(module_name, fname, 'dst'),
            options = ['-d', 'postgres', '-p', str(source_pg.port), '--stream'],
            node = dest_pg)

        # stop replication
        source_pg.stop()

        # check latest changes
        self.set_replica(source_pg, dest_pg)
        dest_pg.slow_start(replica = True)
        self.assertEqual(
            result,
            dest_pg.safe_psql("postgres", "SELECT * FROM ultimate_question"),
            'Different answer from copy')
        dest_pg.stop()

        # Clean after yourself
        self.del_test_dir(module_name, fname)


