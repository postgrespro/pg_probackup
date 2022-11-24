import unittest
import subprocess
import os
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from sys import exit
import shutil


def check_manual_tests_enabled():
    return 'PGPROBACKUP_MANUAL' in os.environ and os.environ['PGPROBACKUP_MANUAL'] == 'ON'


def check_ssh_agent_path_exists():
    return 'PGPROBACKUP_SSH_AGENT_PATH' in os.environ


class CompatibilityTest(ProbackupTest, unittest.TestCase):

    def setUp(self):
        self.fname = self.id().split('.')[3]

    # @unittest.expectedFailure
    @unittest.skipUnless(check_manual_tests_enabled(), 'skip manual test')
    @unittest.skipUnless(check_ssh_agent_path_exists(), 'skip no ssh agent path exist')
    # @unittest.skip("skip")
    def test_catchup_with_different_remote_major_pg(self):
        """
        Decription in jira issue PBCKP-236
        This test exposures ticket error using pg_probackup builds for both PGPROEE11 and PGPROEE9_6

        Prerequisites:
        - pg_probackup git tag for PBCKP 2.5.1
        - master pg_probackup build should be made for PGPROEE11
        - agent pg_probackup build should be made for PGPROEE9_6

        Calling probackup PGPROEE9_6 pg_probackup agent from PGPROEE11 pg_probackup master for DELTA backup causes
        the PBCKP-236 problem

        Please give env variables PROBACKUP_MANUAL=ON;PGPROBACKUP_SSH_AGENT_PATH=<pg_probackup_ssh_agent_path>
        for the test

        Please make path for agent's pgprobackup_ssh_agent_path = '/home/avaness/postgres/postgres.build.ee.9.6/bin/'
        without pg_probackup executable
        """

        self.verbose = True
        self.remote = True
        # please use your own local path like
        # pgprobackup_ssh_agent_path = '/home/avaness/postgres/postgres.build.clean/bin/'
        pgprobackup_ssh_agent_path = os.environ['PGPROBACKUP_SSH_AGENT_PATH']

        src_pg = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'src'),
            set_replication=True,
            )
        src_pg.slow_start()
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question AS SELECT 42 AS answer")

        # do full catchup
        dst_pg = self.make_empty_node(os.path.join(self.module_name, self.fname, 'dst'))
        self.catchup_node(
            backup_mode='FULL',
            source_pgdata=src_pg.data_dir,
            destination_node=dst_pg,
            options=['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        dst_options = {'port': str(dst_pg.port)}
        self.set_auto_conf(dst_pg, dst_options)
        dst_pg.slow_start()
        dst_pg.stop()

        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question2 AS SELECT 42 AS answer")

        # do delta catchup with remote pg_probackup agent with another postgres major version
        # this DELTA backup should fail without PBCKP-236 patch.
        self.catchup_node(
            backup_mode='DELTA',
            source_pgdata=src_pg.data_dir,
            destination_node=dst_pg,
            # here's substitution of --remoge-path pg_probackup agent compiled with another postgres version
            options=['-d', 'postgres', '-p', str(src_pg.port), '--stream', '--remote-path=' + pgprobackup_ssh_agent_path]
            )

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_backward_compatibility_page(self):
        """Description in jira issue PGPRO-434"""
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir, old_binary=True)
        self.show_pb(backup_dir)

        self.add_instance(backup_dir, 'node', node, old_binary=True)
        self.show_pb(backup_dir)

        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=10)

        # FULL backup with old binary
        self.backup_node(
            backup_dir, 'node', node, old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.show_pb(backup_dir)

        self.validate_pb(backup_dir)

        # RESTORE old FULL with new binary
        node_restored = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node_restored'))

        node_restored.cleanup()

        self.restore_node(
                backup_dir, 'node', node_restored, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Page BACKUP with old binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "20"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node, backup_type='page',
            old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Page BACKUP with new binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "20"])

        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        node.safe_psql(
            'postgres',
            'create table tmp as select * from pgbench_accounts where aid < 1000')

        node.safe_psql(
            'postgres',
            'delete from pgbench_accounts')

        node.safe_psql(
            'postgres',
            'VACUUM')

        self.backup_node(backup_dir, 'node', node, backup_type='page')

        pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        node.safe_psql(
            'postgres',
            'insert into pgbench_accounts select * from pgbench_accounts')

        self.backup_node(backup_dir, 'node', node, backup_type='page')

        pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_backward_compatibility_delta(self):
        """Description in jira issue PGPRO-434"""
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir, old_binary=True)
        self.show_pb(backup_dir)

        self.add_instance(backup_dir, 'node', node, old_binary=True)
        self.show_pb(backup_dir)

        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=10)

        # FULL backup with old binary
        self.backup_node(
            backup_dir, 'node', node, old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.show_pb(backup_dir)

        self.validate_pb(backup_dir)

        # RESTORE old FULL with new binary
        node_restored = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node_restored'))

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Delta BACKUP with old binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "20"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node, backup_type='delta',
            old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Delta BACKUP with new binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "20"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(backup_dir, 'node', node, backup_type='delta')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        node.safe_psql(
            'postgres',
            'create table tmp as select * from pgbench_accounts where aid < 1000')

        node.safe_psql(
            'postgres',
            'delete from pgbench_accounts')

        node.safe_psql(
            'postgres',
            'VACUUM')

        self.backup_node(backup_dir, 'node', node, backup_type='delta')

        pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        node.safe_psql(
            'postgres',
            'insert into pgbench_accounts select * from pgbench_accounts')

        self.backup_node(backup_dir, 'node', node, backup_type='delta')

        pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_backward_compatibility_ptrack(self):
        """Description in jira issue PGPRO-434"""

        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir, old_binary=True)
        self.show_pb(backup_dir)

        self.add_instance(backup_dir, 'node', node, old_binary=True)
        self.show_pb(backup_dir)

        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        node.pgbench_init(scale=10)

        # FULL backup with old binary
        self.backup_node(
            backup_dir, 'node', node, old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.show_pb(backup_dir)

        self.validate_pb(backup_dir)

        # RESTORE old FULL with new binary
        node_restored = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node_restored'))

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # ptrack BACKUP with old binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "20"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack',
            old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.restore_node(
            backup_dir, 'node', node_restored,
            options=[
                "-j", "4",
                "--recovery-target=latest",
                "--recovery-target-action=promote"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Ptrack BACKUP with new binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "20"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=[
                "-j", "4",
                "--recovery-target=latest",
                "--recovery-target-action=promote"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_backward_compatibility_compression(self):
        """Description in jira issue PGPRO-434"""
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir, old_binary=True)
        self.add_instance(backup_dir, 'node', node, old_binary=True)

        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=10)

        # FULL backup with OLD binary
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            old_binary=True,
            options=['--compress'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # restore OLD FULL with new binary
        node_restored = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node_restored'))

        node_restored.cleanup()

        self.restore_node(
                backup_dir, 'node', node_restored,
                options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # PAGE backup with OLD binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"])
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='page',
            old_binary=True,
            options=['--compress'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.restore_node(
            backup_dir, 'node', node_restored,
            options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # PAGE backup with new binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"])
        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='page',
            options=['--compress'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Delta backup with old binary
        self.delete_pb(backup_dir, 'node', backup_id)

        self.backup_node(
            backup_dir, 'node', node,
            old_binary=True,
            options=['--compress'])

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"])

        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta',
            options=['--compress'],
            old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Delta backup with new binary
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"])

        pgbench.wait()
        pgbench.stdout.close()

        self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta',
            options=['--compress'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_backward_compatibility_merge(self):
        """
        Create node, take FULL and PAGE backups with old binary,
        merge them with new binary
        """
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir, old_binary=True)
        self.add_instance(backup_dir, 'node', node, old_binary=True)

        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        # FULL backup with OLD binary
        self.backup_node(
            backup_dir, 'node', node,
            old_binary=True)

        node.pgbench_init(scale=1)

        # PAGE backup with OLD binary
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.merge_backup(backup_dir, "node", backup_id)

        self.show_pb(backup_dir, as_text=True, as_json=False)

        # restore OLD FULL with new binary
        node_restored = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node_restored'))

        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_backward_compatibility_merge_1(self):
        """
        Create node, take FULL and PAGE backups with old binary,
        merge them with new binary.
        old binary version =< 2.2.7
        """
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir, old_binary=True)
        self.add_instance(backup_dir, 'node', node, old_binary=True)

        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=20)

        # FULL backup with OLD binary
        self.backup_node(backup_dir, 'node', node, old_binary=True)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "1", "-T", "10", "--no-vacuum"])
        pgbench.wait()
        pgbench.stdout.close()

        # PAGE1 backup with OLD binary
        self.backup_node(
            backup_dir, 'node', node, backup_type='page', old_binary=True)

        node.safe_psql(
            'postgres',
            'DELETE from pgbench_accounts')

        node.safe_psql(
            'postgres',
            'VACUUM pgbench_accounts')

        # PAGE2 backup with OLD binary
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page', old_binary=True)

        pgdata = self.pgdata_content(node.data_dir)

        # merge chain created by old binary with new binary
        output = self.merge_backup(backup_dir, "node", backup_id)

        # check that in-place is disabled
        self.assertIn(
            "WARNING: In-place merge is disabled "
            "because of storage format incompatibility", output)

        # restore merged backup
        node_restored = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(backup_dir, 'node', node_restored)

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_backward_compatibility_merge_2(self):
        """
        Create node, take FULL and PAGE backups with old binary,
        merge them with new binary.
        old binary version =< 2.2.7
        """
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir, old_binary=True)
        self.add_instance(backup_dir, 'node', node, old_binary=True)

        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=50)

        node.safe_psql(
            'postgres',
            'VACUUM pgbench_accounts')

        node_restored = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node_restored'))

        # FULL backup with OLD binary
        self.backup_node(backup_dir, 'node', node, old_binary=True)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "1", "-T", "10", "--no-vacuum"])
        pgbench.wait()
        pgbench.stdout.close()

        # PAGE1 backup with OLD binary
        page1 = self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', old_binary=True)

        pgdata1 = self.pgdata_content(node.data_dir)

        node.safe_psql(
            'postgres',
            "DELETE from pgbench_accounts where ctid > '(10,1)'")

        # PAGE2 backup with OLD binary
        page2 = self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', old_binary=True)

        pgdata2 = self.pgdata_content(node.data_dir)

        # PAGE3 backup with OLD binary
        page3 = self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', old_binary=True)

        pgdata3 = self.pgdata_content(node.data_dir)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "1", "-T", "10", "--no-vacuum"])
        pgbench.wait()
        pgbench.stdout.close()

        # PAGE4 backup with NEW binary
        page4 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')
        pgdata4 = self.pgdata_content(node.data_dir)

        # merge backups one by one and check data correctness
        # merge PAGE1
        self.merge_backup(
            backup_dir, "node", page1, options=['--log-level-file=VERBOSE'])

        # check data correctness for PAGE1
        node_restored.cleanup()
        self.restore_node(
            backup_dir, 'node', node_restored, backup_id=page1,
            options=['--log-level-file=VERBOSE'])
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata1, pgdata_restored)

        # merge PAGE2
        self.merge_backup(backup_dir, "node", page2)

        # check data correctness for PAGE2
        node_restored.cleanup()
        self.restore_node(backup_dir, 'node', node_restored, backup_id=page2)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata2, pgdata_restored)

        # merge PAGE3
        self.show_pb(backup_dir, 'node', page3)
        self.merge_backup(backup_dir, "node", page3)
        self.show_pb(backup_dir, 'node', page3)

        # check data correctness for PAGE3
        node_restored.cleanup()
        self.restore_node(backup_dir, 'node', node_restored, backup_id=page3)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata3, pgdata_restored)

        # merge PAGE4
        self.merge_backup(backup_dir, "node", page4)

        # check data correctness for PAGE4
        node_restored.cleanup()
        self.restore_node(backup_dir, 'node', node_restored, backup_id=page4)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata4, pgdata_restored)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_backward_compatibility_merge_3(self):
        """
        Create node, take FULL and PAGE backups with old binary,
        merge them with new binary.
        old binary version =< 2.2.7
        """
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir, old_binary=True)
        self.add_instance(backup_dir, 'node', node, old_binary=True)

        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=50)

        node.safe_psql(
            'postgres',
            'VACUUM pgbench_accounts')

        node_restored = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node_restored'))

        # FULL backup with OLD binary
        self.backup_node(
            backup_dir, 'node', node, old_binary=True, options=['--compress'])

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "1", "-T", "10", "--no-vacuum"])
        pgbench.wait()
        pgbench.stdout.close()

        # PAGE1 backup with OLD binary
        page1 = self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', old_binary=True, options=['--compress'])

        pgdata1 = self.pgdata_content(node.data_dir)

        node.safe_psql(
            'postgres',
            "DELETE from pgbench_accounts where ctid > '(10,1)'")

        # PAGE2 backup with OLD binary
        page2 = self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', old_binary=True, options=['--compress'])

        pgdata2 = self.pgdata_content(node.data_dir)

        # PAGE3 backup with OLD binary
        page3 = self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', old_binary=True, options=['--compress'])

        pgdata3 = self.pgdata_content(node.data_dir)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "1", "-T", "10", "--no-vacuum"])
        pgbench.wait()
        pgbench.stdout.close()

        # PAGE4 backup with NEW binary
        page4 = self.backup_node(
            backup_dir, 'node', node, backup_type='page', options=['--compress'])
        pgdata4 = self.pgdata_content(node.data_dir)

        # merge backups one by one and check data correctness
        # merge PAGE1
        self.merge_backup(
            backup_dir, "node", page1, options=['--log-level-file=VERBOSE'])

        # check data correctness for PAGE1
        node_restored.cleanup()
        self.restore_node(
            backup_dir, 'node', node_restored, backup_id=page1,
            options=['--log-level-file=VERBOSE'])
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata1, pgdata_restored)

        # merge PAGE2
        self.merge_backup(backup_dir, "node", page2)

        # check data correctness for PAGE2
        node_restored.cleanup()
        self.restore_node(backup_dir, 'node', node_restored, backup_id=page2)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata2, pgdata_restored)

        # merge PAGE3
        self.show_pb(backup_dir, 'node', page3)
        self.merge_backup(backup_dir, "node", page3)
        self.show_pb(backup_dir, 'node', page3)

        # check data correctness for PAGE3
        node_restored.cleanup()
        self.restore_node(backup_dir, 'node', node_restored, backup_id=page3)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata3, pgdata_restored)

        # merge PAGE4
        self.merge_backup(backup_dir, "node", page4)

        # check data correctness for PAGE4
        node_restored.cleanup()
        self.restore_node(backup_dir, 'node', node_restored, backup_id=page4)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata4, pgdata_restored)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_backward_compatibility_merge_4(self):
        """
        Start merge between minor version, crash and retry it.
        old binary version =< 2.4.0
        """
        if self.version_to_num(self.old_probackup_version) > self.version_to_num('2.4.0'):
            self.assertTrue(
                False, 'You need pg_probackup old_binary =< 2.4.0 for this test')

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir, old_binary=True)
        self.add_instance(backup_dir, 'node', node, old_binary=True)

        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=20)

        node.safe_psql(
            'postgres',
            'VACUUM pgbench_accounts')

        node_restored = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node_restored'))

        # FULL backup with OLD binary
        self.backup_node(
            backup_dir, 'node', node, old_binary=True, options=['--compress'])

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "1", "-T", "20", "--no-vacuum"])
        pgbench.wait()
        pgbench.stdout.close()

        # PAGE backup with NEW binary
        page_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page', options=['--compress'])
        pgdata = self.pgdata_content(node.data_dir)

        # merge PAGE4
        gdb = self.merge_backup(backup_dir, "node", page_id, gdb=True)

        gdb.set_breakpoint('rename')
        gdb.run_until_break()
        gdb.continue_execution_until_break(500)
        gdb._execute('signal SIGKILL')

        try:
            self.merge_backup(backup_dir, "node", page_id)
            self.assertEqual(
                1, 0,
                "Expecting Error because of format changes.\n "
                "Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Retry of failed merge for backups with different "
                "between minor versions is forbidden to avoid data corruption "
                "because of storage format changes introduced in 2.4.0 version, "
                "please take a new full backup",
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_backward_compatibility_merge_5(self):
        """
        Create node, take FULL and PAGE backups with old binary,
        merge them with new binary.
        old binary version >= STORAGE_FORMAT_VERSION (2.4.4)
        """
        if self.version_to_num(self.old_probackup_version) < self.version_to_num('2.4.4'):
            self.assertTrue(
                False, 'OLD pg_probackup binary must be >= 2.4.4 for this test')

        self.assertNotEqual(
            self.version_to_num(self.old_probackup_version),
            self.version_to_num(self.probackup_version))

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir, old_binary=True)
        self.add_instance(backup_dir, 'node', node, old_binary=True)

        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=20)

        # FULL backup with OLD binary
        self.backup_node(backup_dir, 'node', node, old_binary=True)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "1", "-T", "10", "--no-vacuum"])
        pgbench.wait()
        pgbench.stdout.close()

        # PAGE1 backup with OLD binary
        self.backup_node(
            backup_dir, 'node', node, backup_type='page', old_binary=True)

        node.safe_psql(
            'postgres',
            'DELETE from pgbench_accounts')

        node.safe_psql(
            'postgres',
            'VACUUM pgbench_accounts')

        # PAGE2 backup with OLD binary
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page', old_binary=True)

        pgdata = self.pgdata_content(node.data_dir)

        # merge chain created by old binary with new binary
        output = self.merge_backup(backup_dir, "node", backup_id)

        # check that in-place is disabled
        self.assertNotIn(
            "WARNING: In-place merge is disabled "
            "because of storage format incompatibility", output)

        # restore merged backup
        node_restored = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(backup_dir, 'node', node_restored)

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_page_vacuum_truncate(self):
        """
        make node, create table, take full backup,
        delete all data, vacuum relation,
        take page backup, insert some data,
        take second page backup,
        restore latest page backup using new binary
        and check data correctness
        old binary should be 2.2.x version
        """
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir, old_binary=True)
        self.add_instance(backup_dir, 'node', node, old_binary=True)
        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create sequence t_seq; "
            "create table t_heap as select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1024) i")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        id1 = self.backup_node(backup_dir, 'node', node, old_binary=True)
        pgdata1 = self.pgdata_content(node.data_dir)

        node.safe_psql(
            "postgres",
            "delete from t_heap")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        id2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page', old_binary=True)
        pgdata2 = self.pgdata_content(node.data_dir)

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1) i")

        id3 = self.backup_node(
            backup_dir, 'node', node, backup_type='page', old_binary=True)
        pgdata3 = self.pgdata_content(node.data_dir)

        node_restored = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored,
            data_dir=node_restored.data_dir, backup_id=id1)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata1, pgdata_restored)

        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored,
            data_dir=node_restored.data_dir, backup_id=id2)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata2, pgdata_restored)

        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored,
            data_dir=node_restored.data_dir, backup_id=id3)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata3, pgdata_restored)

        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()
        node_restored.cleanup()

    # @unittest.skip("skip")
    def test_page_vacuum_truncate_compression(self):
        """
        make node, create table, take full backup,
        delete all data, vacuum relation,
        take page backup, insert some data,
        take second page backup,
        restore latest page backup using new binary
        and check data correctness
        old binary should be 2.2.x version
        """
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir, old_binary=True)
        self.add_instance(backup_dir, 'node', node, old_binary=True)
        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create sequence t_seq; "
            "create table t_heap as select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1024) i")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.backup_node(
            backup_dir, 'node',node, old_binary=True, options=['--compress'])

        node.safe_psql(
            "postgres",
            "delete from t_heap")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.backup_node(
            backup_dir, 'node', node, backup_type='page',
            old_binary=True, options=['--compress'])

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1) i")

        self.backup_node(
            backup_dir, 'node', node, backup_type='page',
            old_binary=True, options=['--compress'])

        pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(backup_dir, 'node', node_restored)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()

    # @unittest.skip("skip")
    def test_page_vacuum_truncate_compressed_1(self):
        """
        make node, create table, take full backup,
        delete all data, vacuum relation,
        take page backup, insert some data,
        take second page backup,
        restore latest page backup using new binary
        and check data correctness
        old binary should be 2.2.x version
        """
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir, old_binary=True)
        self.add_instance(backup_dir, 'node', node, old_binary=True)
        self.set_archiving(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "create sequence t_seq; "
            "create table t_heap as select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1024) i")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        id1 = self.backup_node(
            backup_dir, 'node', node,
            old_binary=True, options=['--compress'])
        pgdata1 = self.pgdata_content(node.data_dir)

        node.safe_psql(
            "postgres",
            "delete from t_heap")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        id2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page',
            old_binary=True, options=['--compress'])
        pgdata2 = self.pgdata_content(node.data_dir)

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1) i")

        id3 = self.backup_node(
            backup_dir, 'node', node, backup_type='page',
            old_binary=True, options=['--compress'])
        pgdata3 = self.pgdata_content(node.data_dir)

        node_restored = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node_restored'))
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored,
            data_dir=node_restored.data_dir, backup_id=id1)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata1, pgdata_restored)

        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored,
            data_dir=node_restored.data_dir, backup_id=id2)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata2, pgdata_restored)

        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()
        node_restored.cleanup()

        self.restore_node(
            backup_dir, 'node', node_restored,
            data_dir=node_restored.data_dir, backup_id=id3)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata3, pgdata_restored)

        self.set_auto_conf(node_restored, {'port': node_restored.port})
        node_restored.slow_start()
        node_restored.cleanup()

    # @unittest.skip("skip")
    def test_hidden_files(self):
        """
        old_version should be < 2.3.0
        Create hidden file in pgdata, take backup
        with old binary, then try to delete backup
        with new binary
        """
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir, old_binary=True)
        self.add_instance(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        open(os.path.join(node.data_dir, ".hidden_stuff"), 'a').close()

        backup_id = self.backup_node(
            backup_dir, 'node',node, old_binary=True, options=['--stream'])

        self.delete_pb(backup_dir, 'node', backup_id)

    # @unittest.skip("skip")
    def test_compatibility_tablespace(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/348
        """
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type="full",
            options=["-j", "4", "--stream"], old_binary=True)

        tblspace_old_path = self.get_tblspace_path(node, 'tblspace_old')

        self.create_tblspace_in_node(
            node, 'tblspace',
            tblspc_path=tblspace_old_path)

        node.safe_psql(
            "postgres",
            "create table t_heap_lame tablespace tblspace "
            "as select 1 as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1000) i")

        tblspace_new_path = self.get_tblspace_path(node, 'tblspace_new')

        node_restored = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node_restored'))
        node_restored.cleanup()

        try:
            self.restore_node(
                backup_dir, 'node', node_restored,
                options=[
                    "-j", "4",
                    "-T", "{0}={1}".format(
                        tblspace_old_path, tblspace_new_path)])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because tablespace mapping is incorrect"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: Backup {0} has no tablespaceses, '
                'nothing to remap'.format(backup_id),
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.backup_node(
            backup_dir, 'node', node, backup_type="delta",
            options=["-j", "4", "--stream"], old_binary=True)

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=[
                "-j", "4",
                "-T", "{0}={1}".format(
                    tblspace_old_path, tblspace_new_path)])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)
