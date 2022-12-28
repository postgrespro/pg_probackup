import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException, idx_ptrack
from .helpers.ptrack_helpers import test_needs_gdb
from datetime import datetime, timedelta
import subprocess
from time import sleep
import shutil
import signal
from testgres import ProcessType


class BugTest(ProbackupTest, unittest.TestCase):

    @test_needs_gdb
    def test_minrecpoint_on_replica(self):
        """
        https://jira.postgrespro.ru/browse/PGPRO-2068
        """

        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                # 'checkpoint_timeout': '60min',
                'checkpoint_completion_target': '0.9',
                'bgwriter_delay': '10ms',
                'bgwriter_lru_maxpages': '1000',
                'bgwriter_lru_multiplier': '4.0',
                'max_wal_size': '256MB'})

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # take full backup and restore it as replica
        self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        # start replica
        replica = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'replica'))
        replica.cleanup()

        self.restore_node(backup_dir, 'node', replica, options=['-R'])
        self.set_replica(node, replica)
        self.add_instance(backup_dir, 'replica', replica)
        self.set_archiving(backup_dir, 'replica', replica, replica=True)

        self.set_auto_conf(
            replica,
            {'port': replica.port, 'restart_after_crash': 'off'})

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION pageinspect")

        replica.slow_start(replica=True)

        # generate some data
        node.pgbench_init(scale=10)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "20"])
        pgbench.wait()
        pgbench.stdout.close()

        # generate some more data and leave it in background
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-j 4", "-T", "100"])

        # wait for shared buffer on replica to be filled with dirty data
        sleep(20)

        # get pids of replica background workers
        startup_pid = replica.auxiliary_pids[ProcessType.Startup][0]
        checkpointer_pid = replica.auxiliary_pids[ProcessType.Checkpointer][0]

        # break checkpointer on UpdateLastRemovedPtr
        gdb_checkpointer = self.gdb_attach(checkpointer_pid)
        gdb_checkpointer._execute('handle SIGINT noprint nostop pass')
        gdb_checkpointer._execute('handle SIGUSR1 noprint nostop pass')
        gdb_checkpointer.set_breakpoint('UpdateLastRemovedPtr')
        gdb_checkpointer.continue_execution_until_break()

        # break recovery on UpdateControlFile
        gdb_recovery = self.gdb_attach(startup_pid)
        gdb_recovery._execute('handle SIGINT noprint nostop pass')
        gdb_recovery._execute('handle SIGUSR1 noprint nostop pass')
        gdb_recovery.set_breakpoint('UpdateMinRecoveryPoint')
        gdb_recovery.continue_execution_until_break()
        gdb_recovery.set_breakpoint('UpdateControlFile')
        gdb_recovery.continue_execution_until_break()

        # stop data generation
        pgbench.wait()
        pgbench.stdout.close()

        # kill someone, we need a crash
        replica.kill(someone=ProcessType.BackgroundWriter)
        gdb_recovery._execute('detach')
        gdb_checkpointer._execute('detach')

        # just to be sure
        try:
            replica.stop(['-m', 'immediate', '-D', replica.data_dir])
        except:
            pass

        # MinRecLSN = replica.get_control_data()['Minimum recovery ending location']

        # Promote replica with 'immediate' target action
        if self.get_version(replica) >= self.version_to_num('12.0'):
            recovery_config = 'postgresql.auto.conf'
        else:
            recovery_config = 'recovery.conf'

        replica.append_conf(
            recovery_config, "recovery_target = 'immediate'")
        replica.append_conf(
            recovery_config, "recovery_target_action = 'pause'")
        replica.slow_start(replica=True)

        script = f'''
DO
$$
DECLARE
    roid oid;
    current_xlog_lsn  pg_lsn;
    pages_from_future RECORD;
    found_corruption  bool := false;
BEGIN
    SELECT pg_last_wal_replay_lsn() INTO current_xlog_lsn;
    RAISE NOTICE 'CURRENT LSN: %', current_xlog_lsn;
    FOR roid IN select oid from pg_class class where relkind IN ('r', 'i', 't', 'm') and relpersistence = 'p' LOOP
        FOR pages_from_future IN
                with number_of_blocks as (select blknum from generate_series(0, pg_relation_size(roid) / 8192 -1) as blknum )
                select blknum, lsn, checksum, flags, lower, upper, special, pagesize, version, prune_xid
                from number_of_blocks, page_header(get_raw_page(roid::regclass::text, number_of_blocks.blknum::int))
                where lsn > current_xlog_lsn LOOP
            RAISE NOTICE 'Found page from future. OID: %, BLKNUM: %, LSN: %', roid, pages_from_future.blknum, pages_from_future.lsn;
            found_corruption := true;
        END LOOP;
    END LOOP;
    IF found_corruption THEN
        RAISE 'Found Corruption';
    END IF;
END;
$$ LANGUAGE plpgsql;
'''

        # Find blocks from future
        replica.safe_psql(
            'postgres',
            script)

        # error is expected if version < 10.6
        # gdb_backup.continue_execution_until_exit()

        # do basebackup

        # do pg_probackup, expect error
