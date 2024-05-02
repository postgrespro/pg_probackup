import os
from .helpers.ptrack_helpers import ProbackupTest
from pg_probackup2.gdb import needs_gdb
import datetime

class LogTest(ProbackupTest):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    # PGPRO-2154
    @needs_gdb
    def test_log_rotation(self):
        """
        """

        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        self.pb.set_config('node',
            options=['--log-rotation-age=1s', '--log-rotation-size=1MB'])

        self.pb.backup_node('node', node,
            options=['--stream', '--log-level-file=verbose'])

        gdb = self.pb.backup_node('node', node,
            options=['--stream', '--log-level-file=verbose'], gdb=True)

        gdb.set_breakpoint('open_logfile')
        gdb.run_until_break()
        gdb.continue_execution_until_exit()

    def test_log_filename_strftime(self):
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        self.pb.set_config('node',
            options=['--log-rotation-age=1d'])

        self.pb.backup_node('node', node,
            options=[
                '--stream',
                '--log-level-file=VERBOSE',
                '--log-filename=pg_probackup-%a.log'])

        day_of_week = datetime.datetime.today().strftime("%a")

        path = os.path.join(self.pb_log_path, 'pg_probackup-{0}.log'.format(day_of_week))

        self.assertTrue(os.path.isfile(path))

    def test_truncate_rotation_file(self):
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        self.pb.set_config('node',
            options=['--log-rotation-age=1d'])

        self.pb.backup_node('node', node,
            options=[
                '--stream',
                '--log-level-file=VERBOSE'])

        rotation_file_path = os.path.join(self.pb_log_path, 'pg_probackup.log.rotation')

        log_file_path = os.path.join(self.pb_log_path, 'pg_probackup.log')

        log_file_size = os.stat(log_file_path).st_size

        self.assertTrue(os.path.isfile(rotation_file_path))

        # truncate .rotation file
        with open(rotation_file_path, "rb+", 0) as f:
            f.truncate()
            f.flush()
            f.close

        output = self.pb.backup_node('node', node,
            options=[
                '--stream',
                '--log-level-file=LOG'],
            return_id=False)

        # check that log file wasn`t rotated
        self.assertGreater(
            os.stat(log_file_path).st_size,
            log_file_size)

        self.assertIn(
            'WARNING: cannot read creation timestamp from rotation file',
            output)

        output = self.pb.backup_node('node', node,
            options=[
                '--stream',
                '--log-level-file=LOG'],
            return_id=False)

        # check that log file wasn`t rotated
        self.assertGreater(
            os.stat(log_file_path).st_size,
            log_file_size)

        self.assertNotIn(
            'WARNING: cannot read creation timestamp from rotation file',
            output)

        self.assertTrue(os.path.isfile(rotation_file_path))

    def test_unlink_rotation_file(self):
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        self.pb.set_config('node',
            options=['--log-rotation-age=1d'])

        self.pb.backup_node('node', node,
            options=[
                '--stream',
                '--log-level-file=VERBOSE'])

        rotation_file_path = os.path.join(self.pb_log_path, 'pg_probackup.log.rotation')

        log_file_path = os.path.join(self.pb_log_path, 'pg_probackup.log')

        log_file_size = os.stat(log_file_path).st_size

        self.assertTrue(os.path.isfile(rotation_file_path))

        # unlink .rotation file
        os.unlink(rotation_file_path)

        output = self.pb.backup_node('node', node,
            options=[
                '--stream',
                '--log-level-file=LOG'],
            return_id=False)

        # check that log file wasn`t rotated
        self.assertGreater(
            os.stat(log_file_path).st_size,
            log_file_size)

        self.assertIn(
            'WARNING: missing rotation file:',
            output)

        self.assertTrue(os.path.isfile(rotation_file_path))

        output = self.pb.backup_node('node', node,
            options=[
                '--stream',
                '--log-level-file=VERBOSE'],
            return_id=False)

        self.assertNotIn(
            'WARNING: missing rotation file:',
            output)

        # check that log file wasn`t rotated
        self.assertGreater(
            os.stat(log_file_path).st_size,
            log_file_size)

    def test_garbage_in_rotation_file(self):
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        self.pb.set_config('node',
            options=['--log-rotation-age=1d'])

        self.pb.backup_node('node', node,
            options=[
                '--stream',
                '--log-level-file=VERBOSE'])

        rotation_file_path = os.path.join(self.pb_log_path, 'pg_probackup.log.rotation')

        log_file_path = os.path.join(self.pb_log_path, 'pg_probackup.log')

        log_file_size = os.stat(log_file_path).st_size

        self.assertTrue(os.path.isfile(rotation_file_path))

        # mangle .rotation file
        with open(rotation_file_path, "w+b", 0) as f:
            f.write(b"blah")
        output = self.pb.backup_node('node', node,
            options=[
                '--stream',
                '--log-level-file=LOG'],
            return_id=False)

        # check that log file wasn`t rotated
        self.assertGreater(
            os.stat(log_file_path).st_size,
            log_file_size)

        self.assertIn(
            'WARNING: rotation file',
            output)

        self.assertIn(
            'has wrong creation timestamp',
            output)

        self.assertTrue(os.path.isfile(rotation_file_path))

        output = self.pb.backup_node('node', node,
            options=[
                '--stream',
                '--log-level-file=LOG'],
            return_id=False)

        self.assertNotIn(
            'WARNING: rotation file',
            output)

        # check that log file wasn`t rotated
        self.assertGreater(
            os.stat(log_file_path).st_size,
            log_file_size)

    def test_issue_274(self):
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        replica = self.pg_node.make_simple('replica')
        replica.cleanup()

        self.pb.backup_node('node', node, options=['--stream'])
        self.pb.restore_node('node', node=replica)

        # Settings for Replica
        self.set_replica(node, replica, synchronous=True)
        self.pb.set_archiving('node', replica, replica=True)
        replica.set_auto_conf({'port': replica.port})

        replica.slow_start(replica=True)

        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,45600) i")

        log_dir = os.path.join(self.test_path, "somedir")

        self.pb.backup_node('node', replica, backup_type='page',
                options=[
                    '--log-level-console=verbose', '--log-level-file=verbose',
                    '--log-directory={0}'.format(log_dir), '-j1',
                    '--log-filename=somelog.txt', '--archive-timeout=5s',
                    '--no-validate', '--log-rotation-size=100KB'],
                expect_error="because of archiving timeout")
        self.assertMessage(contains='ERROR: WAL segment')

        log_file_path = os.path.join(
            log_dir, 'somelog.txt')

        self.assertTrue(os.path.isfile(log_file_path))

        with open(log_file_path, "r+") as f:
            log_content = f.read()

        self.assertIn('INFO: command:', log_content)
