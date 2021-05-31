import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException

module_name = 'catchup'

class CatchupTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    def test_multithread_local_transfer(self):
        """
        Test 'multithreaded basebackup' mode
        create node, insert some test data, catchup into other dir, start, select test data
        """
        fname = self.id().split('.')[3]

        source_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, fname, 'src'),
            set_replication=True)
        source_pg.slow_start()
        source_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question AS SELECT 42 AS answer")
        result = source_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")

        dest_pg = self.make_empty_node(os.path.join(module_name, fname, 'dst'))
        dest_pg = self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = source_pg.data_dir,
            destination_node = dest_pg,
            options = ['-d', 'postgres', '-p', str(source_pg.port), '--stream', '-j', '4']
            )
        source_pg.stop()

        dest_options = {}
        dest_options['port'] = str(dest_pg.port)
        self.set_auto_conf(dest_pg, dest_options)
        dest_pg.slow_start()
        self.assertEqual(
            result,
            dest_pg.safe_psql("postgres", "SELECT * FROM ultimate_question"),
            'Different answer from copy')
        dest_pg.stop()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_local_simple_transfer_with_tablespace(self):
        fname = self.id().split('.')[3]

        source_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, fname, 'src'),
            initdb_params = ['--data-checksums'])
        source_pg.slow_start()

        tblspace1_old_path = self.get_tblspace_path(source_pg, 'tblspace1_old')
        self.create_tblspace_in_node(
            source_pg, 'tblspace1',
            tblspc_path = tblspace1_old_path)

        source_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question TABLESPACE tblspace1 AS SELECT 42 AS answer")
        result = source_pg.safe_psql("postgres", "SELECT * FROM ultimate_question")

        dest_pg = self.make_empty_node(os.path.join(module_name, fname, 'dst'))
        tblspace1_new_path = self.get_tblspace_path(dest_pg, 'tblspace1_new')
        dest_pg = self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = source_pg.data_dir,
            destination_node = dest_pg,
            options = [
                '-d', 'postgres',
                '-p', str(source_pg.port),
                '--stream',
                '-T', '{0}={1}'.format(tblspace1_old_path, tblspace1_new_path)
                ]
            )

        source_pgdata = self.pgdata_content(source_pg.data_dir)
        dest_pgdata = self.pgdata_content(dest_pg.data_dir)
        self.compare_pgdata(source_pgdata, dest_pgdata)

        source_pg.stop()

        dest_options = {}
        dest_options['port'] = str(dest_pg.port)
        self.set_auto_conf(dest_pg, dest_options)
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

        dest_pg = self.make_empty_node(os.path.join(module_name, fname, 'dst'))
        dest_pg = self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = source_pg.data_dir,
            destination_node = dest_pg,
            options = ['-d', 'postgres', '-p', str(source_pg.port), '--stream', '-j', '4'])
        source_pg.stop()

        dest_options = {}
        dest_options['port'] = str(dest_pg.port)
        self.set_auto_conf(dest_pg, dest_options)
        dest_pg.slow_start()
        self.assertEqual(
            result,
            dest_pg.safe_psql("postgres", "SELECT * FROM ultimate_question"),
            'Different answer from copy')
        dest_pg.stop()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_remote_ptrack_catchup(self):
        """
        Test 'catchup' mode
        create node,
        make a copy with replication, start copy, stop copy,
        generate some load on master, insert some test data on master,
        catchup copy, start and select test data
        """
        if not self.ptrack:
            return unittest.skip('Skipped because ptrack support is disabled')

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
        dest_pg = self.make_empty_node(os.path.join(module_name, fname, 'dst'))
        dest_pg = self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = source_pg.data_dir,
            destination_node = dest_pg,
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
            destination_node = dest_pg,
            options = ['-d', 'postgres', '-p', str(source_pg.port), '--stream'])

        # stop replication
        source_pg.stop()

        # check latest changes
        dest_options = {}
        dest_options['port'] = str(dest_pg.port)
        self.set_auto_conf(dest_pg, dest_options)
        self.set_replica(source_pg, dest_pg)
        dest_pg.slow_start(replica = True)
        self.assertEqual(
            result,
            dest_pg.safe_psql("postgres", "SELECT * FROM ultimate_question"),
            'Different answer from copy')
        dest_pg.stop()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_remote_delta_catchup(self):
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
            )
        source_pg.slow_start()
        source_pg.safe_psql("postgres", "CREATE TABLE ultimate_question(answer int)")

        # make clean shutdowned lagging behind replica
        dest_pg = self.make_empty_node(os.path.join(module_name, fname, 'dst'))
        dest_pg = self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = source_pg.data_dir,
            destination_node = dest_pg,
            options = ['-d', 'postgres', '-p', str(source_pg.port), '--stream'])
        self.set_replica(source_pg, dest_pg)
        dest_options = {}
        dest_options['port'] = str(dest_pg.port)
        self.set_auto_conf(dest_pg, dest_options)
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
            backup_mode = 'DELTA',
            source_pgdata = source_pg.data_dir,
            destination_node = dest_pg,
            options = ['-d', 'postgres', '-p', str(source_pg.port), '--stream'])

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

    # @unittest.skip("skip")
    def test_table_drop(self):
        """
        """
        if not self.ptrack:
            return unittest.skip('Skipped because ptrack support is disabled')

        fname = self.id().split('.')[3]

        source_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, fname, 'src'),
            ptrack_enable = True,
            initdb_params = ['--data-checksums'])
        source_pg.slow_start()

        source_pg.safe_psql("postgres", "CREATE EXTENSION ptrack")
        source_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question AS SELECT 42 AS answer")

        dest_pg = self.make_empty_node(os.path.join(module_name, fname, 'dst'))
        dest_pg = self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = source_pg.data_dir,
            destination_node = dest_pg,
            options = [
                '-d', 'postgres',
                '-p', str(source_pg.port),
                '--stream'
                ]
            )

        dest_options = {}
        dest_options['port'] = str(dest_pg.port)
        self.set_auto_conf(dest_pg, dest_options)
        dest_pg.slow_start()
        dest_pg.stop()

        source_pg.safe_psql("postgres", "DROP TABLE ultimate_question")
        source_pg.safe_psql("postgres", "CHECKPOINT")
        source_pg.safe_psql("postgres", "CHECKPOINT")

        # catchup
        self.catchup_node(
            backup_mode = 'PTRACK',
            source_pgdata = source_pg.data_dir,
            destination_node = dest_pg,
            options = ['-d', 'postgres', '-p', str(source_pg.port), '--stream'])

        source_pgdata = self.pgdata_content(source_pg.data_dir)
        dest_pgdata = self.pgdata_content(dest_pg.data_dir)
        self.compare_pgdata(source_pgdata, dest_pgdata)

        # Clean after yourself
        source_pg.stop()
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_tablefile_truncation(self):
        """
        """
        if not self.ptrack:
            return unittest.skip('Skipped because ptrack support is disabled')

        fname = self.id().split('.')[3]

        source_pg = self.make_simple_node(
            base_dir = os.path.join(module_name, fname, 'src'),
            ptrack_enable = True,
            initdb_params = ['--data-checksums'])
        source_pg.slow_start()

        source_pg.safe_psql("postgres", "CREATE EXTENSION ptrack")
        source_pg.safe_psql(
            "postgres",
            "CREATE SEQUENCE t_seq; "
            "CREATE TABLE t_heap AS SELECT i AS id, "
            "md5(i::text) AS text, "
            "md5(repeat(i::text, 10))::tsvector AS tsvector "
            "FROM generate_series(0, 1024) i")
        source_pg.safe_psql("postgres", "VACUUM t_heap")

        dest_pg = self.make_empty_node(os.path.join(module_name, fname, 'dst'))
        dest_pg = self.catchup_node(
            backup_mode = 'FULL',
            source_pgdata = source_pg.data_dir,
            destination_node = dest_pg,
            options = [
                '-d', 'postgres',
                '-p', str(source_pg.port),
                '--stream'
                ]
            )

        dest_options = {}
        dest_options['port'] = str(dest_pg.port)
        self.set_auto_conf(dest_pg, dest_options)
        dest_pg.slow_start()
        dest_pg.stop()

        source_pg.safe_psql("postgres", "DELETE FROM t_heap WHERE ctid >= '(11,0)'")
        source_pg.safe_psql("postgres", "VACUUM t_heap")

        # catchup
        self.catchup_node(
            backup_mode = 'PTRACK',
            source_pgdata = source_pg.data_dir,
            destination_node = dest_pg,
            options = ['-d', 'postgres', '-p', str(source_pg.port), '--stream'])

        source_pgdata = self.pgdata_content(source_pg.data_dir)
        dest_pgdata = self.pgdata_content(dest_pg.data_dir)
        self.compare_pgdata(source_pgdata, dest_pgdata)

        # Clean after yourself
        source_pg.stop()
        self.del_test_dir(module_name, fname)
