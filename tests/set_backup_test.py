import unittest
import subprocess
import os

from .helpers.data_helpers import tail_file
from .helpers.ptrack_helpers import ProbackupTest
from sys import exit
from datetime import datetime, timedelta
from .helpers.enums.date_time_enum import DateTimePattern

class SetBackupTest(ProbackupTest):

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_set_backup_sanity(self):
        """general sanity for set-backup command"""
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        backup_id = self.pb.backup_node('node', node, options=['--stream'])

        recovery_time = self.pb.show('node', backup_id=backup_id)['recovery-time']
         # Remove microseconds
        recovery_time = datetime.strptime(recovery_time + '00', DateTimePattern.Y_m_d_H_M_S_f_z_dash.value)
        recovery_time = recovery_time.strftime(DateTimePattern.Y_m_d_H_M_S_z_dash.value)

        expire_time_1 = "{:%Y-%m-%d %H:%M:%S}".format(
            datetime.now() + timedelta(days=5))

        self.pb.set_backup(False, options=['--ttl=30d'],
                        expect_error="because of missing instance")
        self.assertMessage(contains='ERROR: Required parameter not specified: --instance')

        self.pb.set_backup('node',
                        options=["--ttl=30d", f"--expire-time='{expire_time_1}'"],
                        expect_error="because options cannot be mixed")
        self.assertMessage(contains="ERROR: You cannot specify '--expire-time' "
                                    "and '--ttl' options together")

        self.pb.set_backup('node', options=["--ttl=30d"],
                        expect_error="because of missing backup_id")
        self.assertMessage(contains="ERROR: You must specify parameter (-i, "
                                    "--backup-id) for 'set-backup' command")

        self.pb.set_backup('node', backup_id, options=["--ttl=30d"])

        actual_expire_time = self.pb.show('node', backup_id=backup_id)['expire-time']

        self.assertNotEqual(expire_time_1, actual_expire_time)

        expire_time_2 = "{:%Y-%m-%d %H:%M:%S}".format(
            datetime.now() + timedelta(days=6))

        self.pb.set_backup('node', backup_id,
            options=["--expire-time={0}".format(expire_time_2)])

        actual_expire_time = self.pb.show('node', backup_id=backup_id)['expire-time']

        self.assertIn(expire_time_2, actual_expire_time)

        # unpin backup
        self.pb.set_backup('node', backup_id, options=["--ttl=0"])

        attr_list = self.pb.show('node', backup_id=backup_id)

        self.assertNotIn('expire-time', attr_list)

        self.pb.set_backup('node', backup_id, options=["--expire-time={0}".format(recovery_time)])

        # parse string to datetime object
        #new_expire_time = datetime.strptime(new_expire_time, '%Y-%m-%d %H:%M:%S%z')

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_retention_redundancy_pinning(self):
        """"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        self.pb.set_config('node', options=['--retention-redundancy=1'])

        # Make backups to be purged
        full_id = self.pb.backup_node('node', node)
        page_id = self.pb.backup_node('node', node, backup_type="page")
        # Make backups to be keeped
        self.pb.backup_node('node', node)
        self.pb.backup_node('node', node, backup_type="page")

        self.assertEqual(len(self.pb.show('node')), 4)

        self.pb.set_backup('node', page_id, options=['--ttl=5d'])

        # Purge backups
        log = self.pb.delete_expired(
            'node',
            options=['--delete-expired', '--log-level-console=LOG'])
        self.assertEqual(len(self.pb.show('node')), 4)

        self.assertIn('Time Window: 0d/5d', log)
        self.assertIn(
            'LOG: Backup {0} is pinned until'.format(page_id),
            log)
        self.assertIn(
            'LOG: Retain backup {0} because his descendant '
            '{1} is guarded by retention'.format(full_id, page_id),
            log)

    # @unittest.skip("skip")
    def test_retention_window_pinning(self):
        """purge all backups using window-based retention policy"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # take FULL BACKUP
        backup_id_1 = self.pb.backup_node('node', node)
        page1 = self.pb.backup_node('node', node, backup_type='page')

        # Take second FULL BACKUP
        backup_id_2 = self.pb.backup_node('node', node)
        page2 = self.pb.backup_node('node', node, backup_type='page')

        # Take third FULL BACKUP
        backup_id_3 = self.pb.backup_node('node', node)
        page2 = self.pb.backup_node('node', node, backup_type='page')

        for backup in backup_dir.list_instance_backups('node'):
            with self.modify_backup_control(backup_dir, 'node', backup) as cf:
                cf.data += "\nrecovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=3))

        self.pb.set_backup('node', page1, options=['--ttl=30d'])

        # Purge backups
        out = self.pb.delete_expired(
            'node',
            options=[
                '--log-level-console=LOG',
                '--retention-window=1',
                '--delete-expired'])

        self.assertEqual(len(self.pb.show('node')), 2)

        self.assertIn(
            'LOG: Backup {0} is pinned until'.format(page1), out)

        self.assertIn(
            'LOG: Retain backup {0} because his descendant '
            '{1} is guarded by retention'.format(backup_id_1, page1),
            out)

    # @unittest.skip("skip")
    def test_wal_retention_and_pinning(self):
        """
        B1---B2---P---B3--->
        wal-depth=2
        P - pinned backup

        expected result after WAL purge:
        B1   B2---P---B3--->

        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # take FULL BACKUP
        self.pb.backup_node('node', node, options=['--stream'])

        node.pgbench_init(scale=1)

        # Take PAGE BACKUP
        self.pb.backup_node('node', node,
            backup_type='page', options=['--stream'])
        
        node.pgbench_init(scale=1)

        # Take DELTA BACKUP and pin it
        expire_time = "{:%Y-%m-%d %H:%M:%S}".format(
            datetime.now() + timedelta(days=6))
        backup_id_pinned = self.pb.backup_node('node', node,
            backup_type='delta',
            options=[
                '--stream',
                '--expire-time={0}'.format(expire_time)])
        
        node.pgbench_init(scale=1)

        # Take second PAGE BACKUP
        self.pb.backup_node('node', node, backup_type='delta', options=['--stream'])

        node.pgbench_init(scale=1)

        tailer = tail_file(os.path.join(node.logs_dir, 'postgresql.log'))
        tailer.wait(contains='LOG: pushing file "000000010000000000000004"')

        # Purge backups
        out = self.pb.delete_expired(
            'node',
            options=[
                '--log-level-console=LOG',
                '--delete-wal', '--wal-depth=2'])

        # print(out)
        self.assertIn(
            'Pinned backup {0} is ignored for the '
            'purpose of WAL retention'.format(backup_id_pinned),
            out)

        for instance in self.pb.show_archive():
            timelines = instance['timelines']
            for timeline in timelines:
                self.assertEqual(
                    timeline['min-segno'],
                    '000000010000000000000004')
                self.assertEqual(timeline['status'], 'OK')

    # @unittest.skip("skip")
    def test_wal_retention_and_pinning_1(self):
        """
        P---B1--->
        wal-depth=2
        P - pinned backup

        expected result after WAL purge:
        P---B1--->

        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        expire_time = "{:%Y-%m-%d %H:%M:%S}".format(
            datetime.now() + timedelta(days=6))

        # take FULL BACKUP
        backup_id_pinned = self.pb.backup_node('node', node,
            options=['--expire-time={0}'.format(expire_time)])

        node.pgbench_init(scale=2)

        # Take second PAGE BACKUP
        self.pb.backup_node('node', node, backup_type='delta')

        node.pgbench_init(scale=2)

        self.wait_instance_wal_exists(backup_dir, 'node',
                                      "000000010000000000000001.gz")

        tailer = tail_file(os.path.join(node.logs_dir, 'postgresql.log'))
        tailer.wait(contains='LOG: pushing file "000000010000000000000002"')

        # Purge backups
        out = self.pb.delete_expired(
            'node',
            options=[
                '--log-level-console=verbose',
                '--delete-wal', '--wal-depth=2'])

        print(out)
        self.assertIn(
            'Pinned backup {0} is ignored for the '
            'purpose of WAL retention'.format(backup_id_pinned),
            out)

        for instance in self.pb.show_archive():
            timelines = instance['timelines']
            for timeline in timelines:
                self.assertEqual(
                    timeline['min-segno'],
                    '000000010000000000000002')
                self.assertEqual(timeline['status'], 'OK')

        self.pb.validate()

    # @unittest.skip("skip")
    def test_add_note_newlines(self):
        """"""
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # FULL
        backup_id = self.pb.backup_node('node', node,
            options=['--stream', '--note={0}'.format('hello\nhello')])

        backup_meta = self.pb.show('node', backup_id)
        self.assertEqual(backup_meta['note'], "hello")

        self.pb.set_backup('node', backup_id, options=['--note=hello\nhello'])

        backup_meta = self.pb.show('node', backup_id)
        self.assertEqual(backup_meta['note'], "hello")

        self.pb.set_backup('node', backup_id, options=['--note=none'])

        backup_meta = self.pb.show('node', backup_id)
        self.assertNotIn('note', backup_meta)

    # @unittest.skip("skip")
    def test_add_big_note(self):
        """"""
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

#        note = node.safe_psql(
#            "postgres",
#            "SELECT repeat('hello', 400)").rstrip() # TODO: investigate

        note = node.safe_psql(
            "postgres",
            "SELECT repeat('hello', 210)").rstrip()

        # FULL
        self.pb.backup_node('node', node,
                         options=['--stream', '--note', note],
                         expect_error="because note is too large")
        self.assertMessage(contains="ERROR: Backup note cannot exceed 1024 bytes")

        note = node.safe_psql(
            "postgres",
            "SELECT repeat('hello', 200)").decode('utf-8').rstrip()

        backup_id = self.pb.backup_node('node', node,
            options=['--stream', '--note={0}'.format(note)])

        backup_meta = self.pb.show('node', backup_id)
        self.assertEqual(backup_meta['note'], note)


    # @unittest.skip("skip")
    def test_add_big_note_1(self):
        """"""
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        note = node.safe_psql(
            "postgres",
            "SELECT repeat('q', 1024)").decode('utf-8').rstrip()

        # FULL
        backup_id = self.pb.backup_node('node', node, options=['--stream'])

        self.pb.set_backup('node', backup_id,
            options=['--note={0}'.format(note)])

        backup_meta = self.pb.show('node', backup_id)

        print(backup_meta)
        self.assertEqual(backup_meta['note'], note)

####################################################################
#                           dry-run
####################################################################

    def test_basic_dry_run_set_backup(self):
        """"""
        node = self.pg_node.make_simple('node',
                                        set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        note = node.safe_psql(
            "postgres",
            "SELECT repeat('q', 1024)").decode('utf-8').rstrip()

        backup_id = self.pb.backup_node('node', node, options=['--stream'])

        expire_time = "{:%Y-%m-%d %H:%M:%S}".format(
            datetime.now() + timedelta(days=6))

        self.pb.set_backup('node', backup_id,
                           options=['--expire-time={}'.format(expire_time),
                                    '--dry-run',
                                    '--note={0}'.format(note)])

        backup_meta = self.pb.show('node', backup_id)

        print(backup_meta)
        self.assertFalse(any('expire-time' in d for d in backup_meta))
        self.assertFalse(any('note' in d for d in backup_meta))

        self.pb.set_backup('node', backup_id,
                           options=['--ttl=30d',
                                    '--dry-run',
                                    '--note={0}'.format(note)])

        backup_meta = self.pb.show('node', backup_id)

        print(backup_meta)
        self.assertFalse(any('ttl' in d for d in backup_meta))
        self.assertFalse(any('note' in d for d in backup_meta))
