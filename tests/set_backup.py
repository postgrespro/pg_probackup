import unittest
import subprocess
import os
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from sys import exit
from datetime import datetime, timedelta

module_name = 'set_backup'


class SetBackupTest(ProbackupTest, unittest.TestCase):

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_set_backup_sanity(self):
        """general sanity for set-backup command"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        backup_id = self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        recovery_time = self.show_pb(
            backup_dir, 'node', backup_id=backup_id)['recovery-time']

        expire_time_1 = "{:%Y-%m-%d %H:%M:%S}".format(
            datetime.now() + timedelta(days=5))

        try:
            self.set_backup(backup_dir, False, options=['--ttl=30d'])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because of missing instance. "
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: required parameter not specified: --instance',
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        try:
            self.set_backup(
                backup_dir, 'node',
                options=[
                    "--ttl=30d",
                    "--expire-time='{0}'".format(expire_time_1)])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because options cannot be mixed. "
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: You cannot specify '--expire-time' "
                "and '--ttl' options together",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        try:
            self.set_backup(backup_dir, 'node', options=["--ttl=30d"])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because of missing backup_id. "
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: You must specify parameter (-i, --backup-id) "
                "for 'set-backup' command",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        self.set_backup(
            backup_dir, 'node', backup_id, options=["--ttl=30d"])

        actual_expire_time = self.show_pb(
            backup_dir, 'node', backup_id=backup_id)['expire-time']

        self.assertNotEqual(expire_time_1, actual_expire_time)

        expire_time_2 = "{:%Y-%m-%d %H:%M:%S}".format(
            datetime.now() + timedelta(days=6))

        self.set_backup(
            backup_dir, 'node', backup_id,
            options=["--expire-time={0}".format(expire_time_2)])

        actual_expire_time = self.show_pb(
            backup_dir, 'node', backup_id=backup_id)['expire-time']

        self.assertIn(expire_time_2, actual_expire_time)

        # unpin backup
        self.set_backup(
            backup_dir, 'node', backup_id, options=["--ttl=0"])

        attr_list = self.show_pb(
            backup_dir, 'node', backup_id=backup_id)

        self.assertNotIn('expire-time', attr_list)

        self.set_backup(
            backup_dir, 'node', backup_id, options=["--expire-time={0}".format(recovery_time)])

        # parse string to datetime object
        #new_expire_time = datetime.strptime(new_expire_time, '%Y-%m-%d %H:%M:%S%z')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_retention_redundancy_pinning(self):
        """"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        with open(os.path.join(
                backup_dir, 'backups', 'node',
                "pg_probackup.conf"), "a") as conf:
            conf.write("retention-redundancy = 1\n")

        self.set_config(
            backup_dir, 'node', options=['--retention-redundancy=1'])

        # Make backups to be purged
        full_id = self.backup_node(backup_dir, 'node', node)
        page_id = self.backup_node(
            backup_dir, 'node', node, backup_type="page")
        # Make backups to be keeped
        self.backup_node(backup_dir, 'node', node)
        self.backup_node(backup_dir, 'node', node, backup_type="page")

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 4)

        self.set_backup(
            backup_dir, 'node', page_id, options=['--ttl=5d'])

        # Purge backups
        log = self.delete_expired(
            backup_dir, 'node',
            options=['--delete-expired', '--log-level-console=LOG'])
        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 4)

        self.assertIn('Time Window: 0d/5d', log)
        self.assertIn(
            'LOG: Backup {0} is pinned until'.format(page_id),
            log)
        self.assertIn(
            'LOG: Retain backup {0} because his descendant '
            '{1} is guarded by retention'.format(full_id, page_id),
            log)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_retention_window_pinning(self):
        """purge all backups using window-based retention policy"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # take FULL BACKUP
        backup_id_1 = self.backup_node(backup_dir, 'node', node)
        page1 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Take second FULL BACKUP
        backup_id_2 = self.backup_node(backup_dir, 'node', node)
        page2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # Take third FULL BACKUP
        backup_id_3 = self.backup_node(backup_dir, 'node', node)
        page2 = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        backups = os.path.join(backup_dir, 'backups', 'node')
        for backup in os.listdir(backups):
            if backup == 'pg_probackup.conf':
                continue
            with open(
                    os.path.join(
                        backups, backup, "backup.control"), "a") as conf:
                conf.write("recovery_time='{:%Y-%m-%d %H:%M:%S}'\n".format(
                    datetime.now() - timedelta(days=3)))

        self.set_backup(
            backup_dir, 'node', page1, options=['--ttl=30d'])

        # Purge backups
        out = self.delete_expired(
            backup_dir, 'node',
            options=[
                '--log-level-console=LOG',
                '--retention-window=1',
                '--delete-expired'])

        self.assertEqual(len(self.show_pb(backup_dir, 'node')), 2)

        self.assertIn(
            'LOG: Backup {0} is pinned until'.format(page1), out)

        self.assertIn(
            'LOG: Retain backup {0} because his descendant '
            '{1} is guarded by retention'.format(backup_id_1, page1),
            out)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_wal_retention_and_pinning(self):
        """
        B1---B2---P---B3--->
        wal-depth=2
        P - pinned backup

        expected result after WAL purge:
        B1   B2---P---B3--->

        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # take FULL BACKUP
        self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        node.pgbench_init(scale=1)

        # Take PAGE BACKUP
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='page', options=['--stream'])
        
        node.pgbench_init(scale=1)

        # Take DELTA BACKUP and pin it
        expire_time = "{:%Y-%m-%d %H:%M:%S}".format(
            datetime.now() + timedelta(days=6))
        backup_id_pinned = self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta',
            options=[
                '--stream',
                '--expire-time={0}'.format(expire_time)])
        
        node.pgbench_init(scale=1)

        # Take second PAGE BACKUP
        self.backup_node(
            backup_dir, 'node', node, backup_type='delta', options=['--stream'])

        node.pgbench_init(scale=1)

        # Purge backups
        out = self.delete_expired(
            backup_dir, 'node',
            options=[
                '--log-level-console=LOG',
                '--delete-wal', '--wal-depth=2'])

        # print(out)
        self.assertIn(
            'Pinned backup {0} is ignored for the '
            'purpose of WAL retention'.format(backup_id_pinned),
            out)

        for instance in self.show_archive(backup_dir):
            timelines = instance['timelines']

        # sanity
        for timeline in timelines:
            self.assertEqual(
                timeline['min-segno'],
                '000000010000000000000004')
            self.assertEqual(timeline['status'], 'OK')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_wal_retention_and_pinning_1(self):
        """
        P---B1--->
        wal-depth=2
        P - pinned backup

        expected result after WAL purge:
        P---B1--->

        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        expire_time = "{:%Y-%m-%d %H:%M:%S}".format(
            datetime.now() + timedelta(days=6))

        # take FULL BACKUP
        backup_id_pinned = self.backup_node(
            backup_dir, 'node', node,
            options=['--expire-time={0}'.format(expire_time)])

        node.pgbench_init(scale=2)

        # Take second PAGE BACKUP
        self.backup_node(
            backup_dir, 'node', node, backup_type='delta')

        node.pgbench_init(scale=2)

        # Purge backups
        out = self.delete_expired(
            backup_dir, 'node',
            options=[
                '--log-level-console=verbose',
                '--delete-wal', '--wal-depth=2'])

        print(out)
        self.assertIn(
            'Pinned backup {0} is ignored for the '
            'purpose of WAL retention'.format(backup_id_pinned),
            out)

        for instance in self.show_archive(backup_dir):
            timelines = instance['timelines']

        # sanity
        for timeline in timelines:
            self.assertEqual(
                timeline['min-segno'],
                '000000010000000000000002')
            self.assertEqual(timeline['status'], 'OK')

        self.validate_pb(backup_dir)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_add_note_newlines(self):
        """"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        # FULL
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            options=['--stream', '--note={0}'.format('hello\nhello')])

        backup_meta = self.show_pb(backup_dir, 'node', backup_id)
        self.assertEqual(backup_meta['note'], "hello")

        self.set_backup(backup_dir, 'node', backup_id, options=['--note=hello\nhello'])

        backup_meta = self.show_pb(backup_dir, 'node', backup_id)
        self.assertEqual(backup_meta['note'], "hello")

        self.set_backup(backup_dir, 'node', backup_id, options=['--note=none'])

        backup_meta = self.show_pb(backup_dir, 'node', backup_id)
        self.assertNotIn('note', backup_meta)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_add_big_note(self):
        """"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

#        note = node.safe_psql(
#            "postgres",
#            "SELECT repeat('hello', 400)").rstrip() # TODO: investigate

        note = node.safe_psql(
            "postgres",
            "SELECT repeat('hello', 210)").rstrip()

        # FULL
        try:
            self.backup_node(
                backup_dir, 'node', node,
                options=['--stream', '--note={0}'.format(note)])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because note is too large "
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                "ERROR: Backup note cannot exceed 1024 bytes",
                e.message,
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(
                    repr(e.message), self.cmd))

        note = node.safe_psql(
            "postgres",
            "SELECT repeat('hello', 200)").decode('utf-8').rstrip()

        backup_id = self.backup_node(
            backup_dir, 'node', node,
            options=['--stream', '--note={0}'.format(note)])

        backup_meta = self.show_pb(backup_dir, 'node', backup_id)
        self.assertEqual(backup_meta['note'], note)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_add_big_note_1(self):
        """"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        note = node.safe_psql(
            "postgres",
            "SELECT repeat('q', 1024)").decode('utf-8').rstrip()

        # FULL
        backup_id = self.backup_node(backup_dir, 'node', node, options=['--stream'])

        self.set_backup(
            backup_dir, 'node', backup_id,
            options=['--note={0}'.format(note)])

        backup_meta = self.show_pb(backup_dir, 'node', backup_id)

        print(backup_meta)
        self.assertEqual(backup_meta['note'], note)

        # Clean after yourself
        self.del_test_dir(module_name, fname)
