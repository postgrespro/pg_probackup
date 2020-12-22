import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException, idx_ptrack
from datetime import datetime, timedelta
import subprocess
from time import sleep
import shutil
import signal
from testgres import ProcessType


module_name = '2068'


class BugTest(ProbackupTest, unittest.TestCase):

    def test_minrecpoint_on_replica(self):
        """
        https://jira.postgrespro.ru/browse/PGPRO-2068
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                # 'checkpoint_timeout': '60min',
                'checkpoint_completion_target': '0.9',
                'bgwriter_delay': '10ms',
                'bgwriter_lru_maxpages': '1000',
                'bgwriter_lru_multiplier': '4.0',
                'max_wal_size': '256MB'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # take full backup and restore it as replica
        self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        # start replica
        replica = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'replica'))
        replica.cleanup()

        self.restore_node(backup_dir, 'node', replica, options=['-R'])
        self.set_replica(node, replica)
        self.add_instance(backup_dir, 'replica', replica)
        self.set_archiving(backup_dir, 'replica', replica, replica=True)

        self.set_auto_conf(
            replica,
            {'port': replica.port, 'restart_after_crash': 'off'})

        # we need those later
        node.safe_psql(
            "postgres",
            "CREATE EXTENSION plpython3u")

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
        bgwriter_pid = replica.auxiliary_pids[ProcessType.BackgroundWriter][0]

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
        os.kill(int(bgwriter_pid), 9)
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

        if self.get_version(node) < 100000:
            script = '''
DO
$$
relations = plpy.execute("select class.oid from pg_class class WHERE class.relkind IN ('r', 'i', 't', 'm')  and class.relpersistence = 'p'")
current_xlog_lsn = plpy.execute("SELECT min_recovery_end_lsn as lsn FROM pg_control_recovery()")[0]['lsn']
plpy.notice('CURRENT LSN: {0}'.format(current_xlog_lsn))
found_corruption = False
for relation in relations:
    pages_from_future = plpy.execute("with number_of_blocks as (select blknum from generate_series(0, pg_relation_size({0}) / 8192 -1) as blknum) select blknum, lsn, checksum, flags, lower, upper, special, pagesize, version, prune_xid from number_of_blocks, page_header(get_raw_page('{0}'::oid::regclass::text, number_of_blocks.blknum::int)) where lsn > '{1}'::pg_lsn".format(relation['oid'], current_xlog_lsn))

    if pages_from_future.nrows() == 0:
        continue

    for page in pages_from_future:
        plpy.notice('Found page from future. OID: {0}, BLKNUM: {1}, LSN: {2}'.format(relation['oid'], page['blknum'], page['lsn']))
        found_corruption = True
if found_corruption:
    plpy.error('Found Corruption')
$$ LANGUAGE plpythonu;
'''
        else:
            script = '''
DO
$$
relations = plpy.execute("select class.oid from pg_class class WHERE class.relkind IN ('r', 'i', 't', 'm')  and class.relpersistence = 'p'")
current_xlog_lsn = plpy.execute("select pg_last_wal_replay_lsn() as lsn")[0]['lsn']
plpy.notice('CURRENT LSN: {0}'.format(current_xlog_lsn))
found_corruption = False
for relation in relations:
    pages_from_future = plpy.execute("with number_of_blocks as (select blknum from generate_series(0, pg_relation_size({0}) / 8192 -1) as blknum) select blknum, lsn, checksum, flags, lower, upper, special, pagesize, version, prune_xid from number_of_blocks, page_header(get_raw_page('{0}'::oid::regclass::text, number_of_blocks.blknum::int)) where lsn > '{1}'::pg_lsn".format(relation['oid'], current_xlog_lsn))

    if pages_from_future.nrows() == 0:
        continue

    for page in pages_from_future:
        plpy.notice('Found page from future. OID: {0}, BLKNUM: {1}, LSN: {2}'.format(relation['oid'], page['blknum'], page['lsn']))
        found_corruption = True
if found_corruption:
    plpy.error('Found Corruption')
$$ LANGUAGE plpython3u;
'''

        # Find blocks from future
        replica.safe_psql(
            'postgres',
            script)

        # error is expected if version < 10.6
        # gdb_backup.continue_execution_until_exit()

        # do basebackup

        # do pg_probackup, expect error

        # Clean after yourself
        self.del_test_dir(module_name, fname)
