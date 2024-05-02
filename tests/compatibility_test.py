import unittest
import subprocess
import os
from .helpers.ptrack_helpers import ProbackupTest, fs_backup_class
from sys import exit
import shutil


def check_manual_tests_enabled():
    return 'PGPROBACKUP_MANUAL' in os.environ and os.environ['PGPROBACKUP_MANUAL'] == 'ON'


def check_ssh_agent_path_exists():
    return 'PGPROBACKUP_SSH_AGENT_PATH' in os.environ


class CrossCompatibilityTest(ProbackupTest):
    auto_compress_alg = False

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

        src_pg = self.pg_node.make_simple('src',
            set_replication=True,
            )
        src_pg.slow_start()
        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question AS SELECT 42 AS answer")

        # do full catchup
        dst_pg = self.pg_node.make_empty('dst')
        self.pb.catchup_node(
            backup_mode='FULL',
            source_pgdata=src_pg.data_dir,
            destination_node=dst_pg,
            options=['-d', 'postgres', '-p', str(src_pg.port), '--stream']
            )

        dst_options = {'port': str(dst_pg.port)}
        dst_pg.set_auto_conf(dst_options)
        dst_pg.slow_start()
        dst_pg.stop()

        src_pg.safe_psql(
            "postgres",
            "CREATE TABLE ultimate_question2 AS SELECT 42 AS answer")

        # do delta catchup with remote pg_probackup agent with another postgres major version
        # this DELTA backup should fail without PBCKP-236 patch.
        self.pb.catchup_node(
            backup_mode='DELTA',
            source_pgdata=src_pg.data_dir,
            destination_node=dst_pg,
            # here's substitution of --remoge-path pg_probackup agent compiled with another postgres version
            options=['-d', 'postgres', '-p', str(src_pg.port), '--stream', '--remote-path=' + pgprobackup_ssh_agent_path]
            )


class CompatibilityTest(ProbackupTest):

    def setUp(self):
        super().setUp()
        if not self.probackup_old_path:
            self.skipTest('An old version of pg_probackup is not set up')

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_backward_compatibility_page(self):
        """Description in jira issue PGPRO-434"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init(old_binary=True)
        self.pb.show()

        self.pb.add_instance('node', node, old_binary=True)
        self.pb.show()

        self.pb.set_archiving('node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=10)

        # FULL backup with old binary
        self.pb.backup_node('node', node, old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.pb.show()

        self.pb.validate()

        # RESTORE old FULL with new binary
        node_restored = self.pg_node.make_simple('node_restored')

        node_restored.cleanup()

        self.pb.restore_node('node', node_restored, options=["-j", "4"])

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

        self.pb.backup_node('node', node, backup_type='page',
            old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.pb.restore_node('node', node_restored, options=["-j", "4"])

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

        self.pb.backup_node('node', node, backup_type='page')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.pb.restore_node('node', node_restored, options=["-j", "4"])

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

        self.pb.backup_node('node', node, backup_type='page')

        pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.pb.restore_node('node', node_restored, options=["-j", "4"])

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        node.safe_psql(
            'postgres',
            'insert into pgbench_accounts select * from pgbench_accounts')

        self.pb.backup_node('node', node, backup_type='page')

        pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.pb.restore_node('node', node_restored, options=["-j", "4"])

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_backward_compatibility_delta(self):
        """Description in jira issue PGPRO-434"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init(old_binary=True)
        self.pb.show()

        self.pb.add_instance('node', node, old_binary=True)
        self.pb.show()

        self.pb.set_archiving('node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=10)

        # FULL backup with old binary
        self.pb.backup_node('node', node, old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.pb.show()

        self.pb.validate()

        # RESTORE old FULL with new binary
        node_restored = self.pg_node.make_simple('node_restored')

        node_restored.cleanup()

        self.pb.restore_node('node', node_restored, options=["-j", "4"])

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

        self.pb.backup_node('node', node, backup_type='delta',
            old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.pb.restore_node('node', node_restored, options=["-j", "4"])

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

        self.pb.backup_node('node', node, backup_type='delta')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.pb.restore_node('node', node_restored, options=["-j", "4"])

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

        self.pb.backup_node('node', node, backup_type='delta')

        pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.pb.restore_node('node', node_restored, options=["-j", "4"])

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        node.safe_psql(
            'postgres',
            'insert into pgbench_accounts select * from pgbench_accounts')

        self.pb.backup_node('node', node, backup_type='delta')

        pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.pb.restore_node('node', node_restored, options=["-j", "4"])

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_backward_compatibility_ptrack(self):
        """Description in jira issue PGPRO-434"""

        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            ptrack_enable=True)

        self.pb.init(old_binary=True)
        self.pb.show()

        self.pb.add_instance('node', node, old_binary=True)
        self.pb.show()

        self.pb.set_archiving('node', node, old_binary=True)
        node.slow_start()

        node.safe_psql(
            "postgres",
            "CREATE EXTENSION ptrack")

        node.pgbench_init(scale=10)

        # FULL backup with old binary
        self.pb.backup_node('node', node, old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.pb.show()

        self.pb.validate()

        # RESTORE old FULL with new binary
        node_restored = self.pg_node.make_simple('node_restored')

        node_restored.cleanup()

        self.pb.restore_node('node', node_restored, options=["-j", "4"])

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

        self.pb.backup_node('node', node, backup_type='ptrack',
            old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.pb.restore_node('node', node_restored,
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

        self.pb.backup_node('node', node, backup_type='ptrack')

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.pb.restore_node('node', node_restored,
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
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init(old_binary=True)
        self.pb.add_instance('node', node, old_binary=True)

        self.pb.set_archiving('node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=10)

        # FULL backup with OLD binary
        backup_id = self.pb.backup_node('node', node,
            old_binary=True,
            options=['--compress'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # restore OLD FULL with new binary
        node_restored = self.pg_node.make_simple('node_restored')

        node_restored.cleanup()

        self.pb.restore_node('node', node_restored,
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

        self.pb.backup_node('node', node,
            backup_type='page',
            old_binary=True,
            options=['--compress'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()
        self.pb.restore_node('node', node_restored,
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

        self.pb.backup_node('node', node,
            backup_type='page',
            options=['--compress'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.pb.restore_node('node', node_restored,
            options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Delta backup with old binary
        self.pb.delete('node', backup_id)

        self.pb.backup_node('node', node,
            old_binary=True,
            options=['--compress'])

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"])

        pgbench.wait()
        pgbench.stdout.close()

        self.pb.backup_node('node', node,
            backup_type='delta',
            options=['--compress'],
            old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.pb.restore_node('node', node_restored,
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

        self.pb.backup_node('node', node,
            backup_type='delta',
            options=['--compress'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        node_restored.cleanup()

        self.pb.restore_node('node', node_restored,
            options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    def test_backward_compatibility_merge(self):
        """
        Create node, take FULL and PAGE backups with old binary,
        merge them with new binary
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init(old_binary=True)
        self.pb.add_instance('node', node, old_binary=True)

        self.pb.set_archiving('node', node, old_binary=True)
        node.slow_start()

        # FULL backup with OLD binary
        self.pb.backup_node('node', node,
            old_binary=True)

        node.pgbench_init(scale=1)

        # PAGE backup with OLD binary
        backup_id = self.pb.backup_node('node', node,
            backup_type='page', old_binary=True)

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.pb.merge_backup("node", backup_id)

        self.pb.show(as_text=True, as_json=False)

        # restore OLD FULL with new binary
        node_restored = self.pg_node.make_simple('node_restored')

        node_restored.cleanup()

        self.pb.restore_node('node', node_restored, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    def test_backward_compatibility_merge_1(self):
        """
        Create node, take FULL and PAGE backups with old binary,
        merge them with new binary.
        old binary version =< 2.2.7
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init(old_binary=True)
        self.pb.add_instance('node', node, old_binary=True)

        self.pb.set_archiving('node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=20)

        # FULL backup with OLD binary
        self.pb.backup_node('node', node, old_binary=True)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "1", "-T", "10", "--no-vacuum"])
        pgbench.wait()
        pgbench.stdout.close()

        # PAGE1 backup with OLD binary
        self.pb.backup_node('node', node, backup_type='page', old_binary=True)

        node.safe_psql(
            'postgres',
            'DELETE from pgbench_accounts')

        node.safe_psql(
            'postgres',
            'VACUUM pgbench_accounts')

        # PAGE2 backup with OLD binary
        backup_id = self.pb.backup_node('node', node, backup_type='page', old_binary=True)

        pgdata = self.pgdata_content(node.data_dir)

        # merge chain created by old binary with new binary
        output = self.pb.merge_backup("node", backup_id)

        # check that in-place is disabled
        self.assertIn(
            "WARNING: In-place merge is disabled "
            "because of storage format incompatibility", output)

        # restore merged backup
        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node=node_restored)

        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    def test_backward_compatibility_merge_2(self):
        """
        Create node, take FULL and PAGE backups with old binary,
        merge them with new binary.
        old binary version =< 2.2.7
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init(old_binary=True)
        self.pb.add_instance('node', node, old_binary=True)

        self.pb.set_archiving('node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=50)

        node.safe_psql(
            'postgres',
            'VACUUM pgbench_accounts')

        node_restored = self.pg_node.make_simple('node_restored')

        # FULL backup with OLD binary
        self.pb.backup_node('node', node, old_binary=True)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "1", "-T", "10", "--no-vacuum"])
        pgbench.wait()
        pgbench.stdout.close()

        # PAGE1 backup with OLD binary
        page1 = self.pb.backup_node('node', node,
            backup_type='page', old_binary=True)

        pgdata1 = self.pgdata_content(node.data_dir)

        node.safe_psql(
            'postgres',
            "DELETE from pgbench_accounts where ctid > '(10,1)'")

        # PAGE2 backup with OLD binary
        page2 = self.pb.backup_node('node', node,
            backup_type='page', old_binary=True)

        pgdata2 = self.pgdata_content(node.data_dir)

        # PAGE3 backup with OLD binary
        page3 = self.pb.backup_node('node', node,
            backup_type='page', old_binary=True)

        pgdata3 = self.pgdata_content(node.data_dir)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "1", "-T", "10", "--no-vacuum"])
        pgbench.wait()
        pgbench.stdout.close()

        # PAGE4 backup with NEW binary
        page4 = self.pb.backup_node('node', node, backup_type='page')
        pgdata4 = self.pgdata_content(node.data_dir)

        # merge backups one by one and check data correctness
        # merge PAGE1
        self.pb.merge_backup("node", page1, options=['--log-level-file=VERBOSE'])

        # check data correctness for PAGE1
        node_restored.cleanup()
        self.pb.restore_node('node', node_restored, backup_id=page1,
            options=['--log-level-file=VERBOSE'])
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata1, pgdata_restored)

        # merge PAGE2
        self.pb.merge_backup("node", page2)

        # check data correctness for PAGE2
        node_restored.cleanup()
        self.pb.restore_node('node', node=node_restored, backup_id=page2)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata2, pgdata_restored)

        # merge PAGE3
        self.pb.show('node', page3)
        self.pb.merge_backup("node", page3)
        self.pb.show('node', page3)

        # check data correctness for PAGE3
        node_restored.cleanup()
        self.pb.restore_node('node', node=node_restored, backup_id=page3)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata3, pgdata_restored)

        # merge PAGE4
        self.pb.merge_backup("node", page4)

        # check data correctness for PAGE4
        node_restored.cleanup()
        self.pb.restore_node('node', node_restored, backup_id=page4)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata4, pgdata_restored)

    # @unittest.expectedFailure
    def test_backward_compatibility_merge_3(self):
        """
        Create node, take FULL and PAGE backups with old binary,
        merge them with new binary.
        old binary version =< 2.2.7
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init(old_binary=True)
        self.pb.add_instance('node', node, old_binary=True)

        self.pb.set_archiving('node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=50)

        node.safe_psql(
            'postgres',
            'VACUUM pgbench_accounts')

        node_restored = self.pg_node.make_simple('node_restored')

        # FULL backup with OLD binary
        self.pb.backup_node('node', node, old_binary=True, options=['--compress'])

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "1", "-T", "10", "--no-vacuum"])
        pgbench.wait()
        pgbench.stdout.close()

        # PAGE1 backup with OLD binary
        page1 = self.pb.backup_node('node', node,
            backup_type='page', old_binary=True, options=['--compress'])

        pgdata1 = self.pgdata_content(node.data_dir)

        node.safe_psql(
            'postgres',
            "DELETE from pgbench_accounts where ctid > '(10,1)'")

        # PAGE2 backup with OLD binary
        page2 = self.pb.backup_node('node', node,
            backup_type='page', old_binary=True, options=['--compress'])

        pgdata2 = self.pgdata_content(node.data_dir)

        # PAGE3 backup with OLD binary
        page3 = self.pb.backup_node('node', node,
            backup_type='page', old_binary=True, options=['--compress'])

        pgdata3 = self.pgdata_content(node.data_dir)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "1", "-T", "10", "--no-vacuum"])
        pgbench.wait()
        pgbench.stdout.close()

        # PAGE4 backup with NEW binary
        page4 = self.pb.backup_node('node', node, backup_type='page', options=['--compress'])
        pgdata4 = self.pgdata_content(node.data_dir)

        # merge backups one by one and check data correctness
        # merge PAGE1
        self.pb.merge_backup("node", page1, options=['--log-level-file=VERBOSE'])

        # check data correctness for PAGE1
        node_restored.cleanup()
        self.pb.restore_node('node', node_restored, backup_id=page1,
            options=['--log-level-file=VERBOSE'])
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata1, pgdata_restored)

        # merge PAGE2
        self.pb.merge_backup("node", page2)

        # check data correctness for PAGE2
        node_restored.cleanup()
        self.pb.restore_node('node', node_restored, backup_id=page2)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata2, pgdata_restored)

        # merge PAGE3
        self.pb.show('node', page3)
        self.pb.merge_backup("node", page3)
        self.pb.show('node', page3)

        # check data correctness for PAGE3
        node_restored.cleanup()
        self.pb.restore_node('node', node_restored, backup_id=page3)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata3, pgdata_restored)

        # merge PAGE4
        self.pb.merge_backup("node", page4)

        # check data correctness for PAGE4
        node_restored.cleanup()
        self.pb.restore_node('node', node_restored, backup_id=page4)
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata4, pgdata_restored)

    # @unittest.expectedFailure
    def test_backward_compatibility_merge_4(self):
        """
        Start merge between minor version, crash and retry it.
        old binary version =< 2.4.0
        """
        if self.version_to_num(self.old_probackup_version) > self.version_to_num('2.4.0'):
            self.assertTrue(
                False, 'You need pg_probackup old_binary =< 2.4.0 for this test')

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init(old_binary=True)
        self.pb.add_instance('node', node, old_binary=True)

        self.pb.set_archiving('node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=20)

        node.safe_psql(
            'postgres',
            'VACUUM pgbench_accounts')

        node_restored = self.pg_node.make_simple('node_restored')

        # FULL backup with OLD binary
        self.pb.backup_node('node', node, old_binary=True, options=['--compress'])

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "1", "-T", "20", "--no-vacuum"])
        pgbench.wait()
        pgbench.stdout.close()

        # PAGE backup with NEW binary
        page_id = self.pb.backup_node('node', node, backup_type='page', options=['--compress'])
        pgdata = self.pgdata_content(node.data_dir)

        # merge PAGE4
        gdb = self.pb.merge_backup("node", page_id, gdb=True)

        gdb.set_breakpoint('rename')
        gdb.run_until_break()
        gdb.continue_execution_until_break(500)
        gdb.signal('SIGKILL')

        self.expire_locks(backup_dir, "node")

        self.pb.merge_backup("node", page_id,
                          expect_error="because of format changes")
        self.assertMessage(contains=
                "ERROR: Retry of failed merge for backups with different "
                "between minor versions is forbidden to avoid data corruption "
                "because of storage format changes introduced in 2.4.0 version, "
                "please take a new full backup")

    # @unittest.expectedFailure
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

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init(old_binary=True)
        self.pb.add_instance('node', node, old_binary=True)

        self.pb.set_archiving('node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=20)

        # FULL backup with OLD binary
        self.pb.backup_node('node', node, old_binary=True)

        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "1", "-T", "10", "--no-vacuum"])
        pgbench.wait()
        pgbench.stdout.close()

        # PAGE1 backup with OLD binary
        self.pb.backup_node('node', node, backup_type='page', old_binary=True)

        node.safe_psql(
            'postgres',
            'DELETE from pgbench_accounts')

        node.safe_psql(
            'postgres',
            'VACUUM pgbench_accounts')

        # PAGE2 backup with OLD binary
        backup_id = self.pb.backup_node('node', node, backup_type='page', old_binary=True)

        pgdata = self.pgdata_content(node.data_dir)

        # merge chain created by old binary with new binary
        output = self.pb.merge_backup("node", backup_id)

        # check that in-place is disabled
        self.assertNotIn(
            "WARNING: In-place merge is disabled "
            "because of storage format incompatibility", output)

        # restore merged backup
        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored)

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
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init(old_binary=True)
        self.pb.add_instance('node', node, old_binary=True)
        self.pb.set_archiving('node', node, old_binary=True)
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

        id1 = self.pb.backup_node('node', node, old_binary=True)
        pgdata1 = self.pgdata_content(node.data_dir)

        node.safe_psql(
            "postgres",
            "delete from t_heap")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        id2 = self.pb.backup_node('node', node, backup_type='page', old_binary=True)
        pgdata2 = self.pgdata_content(node.data_dir)

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1) i")

        id3 = self.pb.backup_node('node', node, backup_type='page', old_binary=True)
        pgdata3 = self.pgdata_content(node.data_dir)

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored, backup_id=id1)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata1, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored, backup_id=id2)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata2, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored, backup_id=id3)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata3, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})
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
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init(old_binary=True)
        self.pb.add_instance('node', node, old_binary=True)
        self.pb.set_archiving('node', node, old_binary=True)
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

        self.pb.backup_node('node',node, old_binary=True, options=['--compress'])

        node.safe_psql(
            "postgres",
            "delete from t_heap")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        self.pb.backup_node('node', node, backup_type='page',
            old_binary=True, options=['--compress'])

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1) i")

        self.pb.backup_node('node', node, backup_type='page',
            old_binary=True, options=['--compress'])

        pgdata = self.pgdata_content(node.data_dir)

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})
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
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init(old_binary=True)
        self.pb.add_instance('node', node, old_binary=True)
        self.pb.set_archiving('node', node, old_binary=True)
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

        id1 = self.pb.backup_node('node', node,
            old_binary=True, options=['--compress'])
        pgdata1 = self.pgdata_content(node.data_dir)

        node.safe_psql(
            "postgres",
            "delete from t_heap")

        node.safe_psql(
            "postgres",
            "vacuum t_heap")

        id2 = self.pb.backup_node('node', node, backup_type='page',
            old_binary=True, options=['--compress'])
        pgdata2 = self.pgdata_content(node.data_dir)

        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, "
            "md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1) i")

        id3 = self.pb.backup_node('node', node, backup_type='page',
            old_binary=True, options=['--compress'])
        pgdata3 = self.pgdata_content(node.data_dir)

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored, backup_id=id1)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata1, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored, backup_id=id2)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata2, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})
        node_restored.slow_start()
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored, backup_id=id3)

        # Physical comparison
        pgdata_restored = self.pgdata_content(node_restored.data_dir)
        self.compare_pgdata(pgdata3, pgdata_restored)

        node_restored.set_auto_conf({'port': node_restored.port})
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
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init(old_binary=True)
        self.pb.add_instance('node', node, old_binary=True)
        node.slow_start()

        open(os.path.join(node.data_dir, ".hidden_stuff"), 'a').close()

        backup_id = self.pb.backup_node('node',node, old_binary=True, options=['--stream'])

        self.pb.delete('node', backup_id)

    # @unittest.skip("skip")
    def test_compatibility_tablespace(self):
        """
        https://github.com/postgrespro/pg_probackup/issues/348
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir

        self.pb.init()
        self.pb.add_instance('node', node, old_binary=True)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node, backup_type="full",
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

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        self.pb.restore_node('node', node_restored,
                          options=[
                              "-j", "4",
                              "-T", "{0}={1}".format(
                                  tblspace_old_path, tblspace_new_path)],
                          expect_error="because tablespace mapping is incorrect")
        self.assertMessage(contains=
                'ERROR: Backup {0} has no tablespaceses, '
                'nothing to remap'.format(backup_id))

        self.pb.backup_node('node', node, backup_type="delta",
            options=["-j", "4", "--stream"], old_binary=True)

        self.pb.restore_node('node', node_restored,
            options=[
                "-j", "4",
                "-T", "{0}={1}".format(
                    tblspace_old_path, tblspace_new_path)])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_compatibility_master_options(self):
        """
        Test correctness of handling of removed master-db, master-host, master-port,
        master-user and replica-timeout options
        """
        self.assertTrue(
            self.version_to_num(self.old_probackup_version) <= self.version_to_num('2.6.0'),
            'You need pg_probackup old_binary =< 2.6.0 for this test')

        node = self.pg_node.make_simple('node')
        backup_dir = self.backup_dir

        self.pb.init(old_binary=True)
        self.pb.add_instance('node', node, old_binary=True)

        # add deprecated options (using probackup< 2.6) into pg_probackup.conf
        # don't care about option values, we can use random values here
        self.pb.set_config('node',
            options=[
                '--master-db=postgres',
                '--master-host=localhost',
                '--master-port=5432',
                '--master-user={0}'.format(self.username),
                '--replica-timeout=100500'],
            old_binary=True)

        # and try to show config with new binary (those options must be silently skipped)
        self.pb.show_config('node', old_binary=False)

        # store config with new version (those options must disappear from config)
        self.pb.set_config('node',
            options=[],
            old_binary=False)

        # and check absence
        config_options = self.pb.show_config('node', old_binary=False)
        self.assertFalse(
            ['master-db', 'master-host', 'master-port', 'master-user', 'replica-timeout'] & config_options.keys(),
            'Obsolete options found in new config')