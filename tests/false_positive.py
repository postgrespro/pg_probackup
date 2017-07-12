import unittest
import os
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from datetime import datetime, timedelta
import subprocess


module_name = 'false_positive'


class FalsePositive(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_pgpro561(self):
        """
        make node with archiving, make stream backup, restore it to node1,
        check that archiving is not successful on node1
        """
        fname = self.id().split('.')[3]
        node1 = self.make_simple_node(base_dir="{0}/{1}/node1".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node1', node1)
        self.set_archiving(backup_dir, 'node1', node1)
        node1.start()

        backup_id = self.backup_node(backup_dir, 'node1', node1, options=["--stream"])

        node2 = self.make_simple_node(base_dir="{0}/{1}/node2".format(module_name, fname))
        node2.cleanup()

        node1.psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, md5(repeat(i::text,10))::tsvector as tsvector from generate_series(0,256) i")

        self.backup_node(backup_dir, 'node1', node1, backup_type='page', options=["--stream"])
        self.restore_node(backup_dir, 'node1', data_dir=node2.data_dir)
        node2.append_conf('postgresql.auto.conf', 'port = {0}'.format(node2.port))
        node2.start({"-t": "600"})

        timeline_node1 = node1.get_control_data()["Latest checkpoint's TimeLineID"]
        timeline_node2 = node2.get_control_data()["Latest checkpoint's TimeLineID"]
        self.assertEqual(timeline_node1, timeline_node2, "Timelines on Master and Node1 should be equal. This is unexpected")

        archive_command_node1 = node1.safe_psql("postgres", "show archive_command")
        archive_command_node2 = node2.safe_psql("postgres", "show archive_command")
        self.assertEqual(archive_command_node1, archive_command_node2, "Archive command on Master and Node should be equal. This is unexpected")

        result = node2.safe_psql("postgres", "select last_failed_wal from pg_stat_get_archiver() where last_failed_wal is not NULL")
        # self.assertEqual(res, six.b(""), 'Restored Node1 failed to archive segment {0} due to having the same archive command as Master'.format(res.rstrip()))
        if result == "":
            self.assertEqual(1, 0, 'Error is expected due to Master and Node1 having the common archive and archive_command')

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def pgpro688(self):
        """make node with archiving, make backup, get Recovery Time, validate to Recovery Time. Waiting PGPRO-688. RESOLVED"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        backup_id = self.backup_node(backup_dir, 'node', node)
        recovery_time = self.show_pb(backup_dir, 'node', backup_id)['recovery-time']

        # Uncommenting this section will make this test True Positive
        #node.safe_psql("postgres", "select pg_create_restore_point('123')")
        #node.safe_psql("postgres", "select txid_current()")
        #node.safe_psql("postgres", "select pg_switch_xlog()")
        ####

        #try:
        self.validate_pb(backup_dir, 'node', options=["--time='{0}'".format(recovery_time)])
        # we should die here because exception is what we expect to happen
        # self.assertEqual(1, 0, "Expecting Error because it should not be possible safely validate 'Recovery Time' without wal record with timestamp.\n Output: {0} \n CMD: {1}".format(
        #    repr(self.output), self.cmd))
        # except ProbackupException as e:
        # self.assertTrue('WARNING: recovery can be done up to time {0}'.format(recovery_time) in e.message,
        #    '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def pgpro702_688(self):
        """make node without archiving, make stream backup, get Recovery Time, validate to Recovery Time"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.start()

        backup_id = self.backup_node(backup_dir, 'node', node, options=["--stream"])
        recovery_time = self.show_pb(backup_dir, 'node', backup_id)['recovery-time']

        self.assertIn(six.b("INFO: backup validation completed successfully on"),
            self.validate_pb(backup_dir, 'node', node, options=["--time='{0}'".format(recovery_time)]))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    @unittest.expectedFailure
    def test_validate_wal_lost_segment(self):
        """Loose segment located between backups. ExpectedFailure. This is BUG """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'max_wal_senders': '2'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        self.backup_node(backup_dir, 'node', node)

        # make some wals
        node.pgbench_init(scale=2)
        pgbench = node.pgbench(
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            options=["-c", "4", "-T", "10"]
        )
        pgbench.wait()
        pgbench.stdout.close()

        # delete last wal segment
        wals_dir = os.path.join(backup_dir, "wal", 'node')
        wals = [f for f in os.listdir(wals_dir) if os.path.isfile(os.path.join(wals_dir, f)) and not f.endswith('.backup')]
        wals = map(int, wals)
        os.remove(os.path.join(wals_dir, '0000000' + str(max(wals))))


        ##### Hole Smokes, Batman! We just lost a wal segment and know nothing about it
        ##### We need archive-push ASAP
        self.backup_node(backup_dir, 'node', node)
        self.assertFalse('validation completed successfully' in self.validate_pb(backup_dir, 'node'))
        ########

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    @unittest.expectedFailure
    # Need to force validation of ancestor-chain
    def test_incremental_backup_corrupt_full_1(self):
        """page-level backup with corrupted full backup"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(base_dir="{0}/{1}/node".format(module_name, fname),
            initdb_params=['--data-checksums'],
            pg_options={'wal_level': 'replica', 'ptrack_enable': 'on'}
            )
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        backup_id = self.backup_node(backup_dir, 'node', node)
        file = os.path.join(backup_dir, "backups", "node", backup_id.decode("utf-8"), "database", "postgresql.conf")
        os.remove(file)

        try:
            self.backup_node(backup_dir, 'node', node, backup_type="page")
            # we should die here because exception is what we expect to happen
            self.assertEqual(1, 0, "Expecting Error because page backup should not be possible without valid full backup.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(e.message,
                'ERROR: Valid backup on current timeline is not found. Create new FULL backup before an incremental one.\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

            sleep(1)
            self.assertEqual(1, 0, "Expecting Error because page backup should not be possible without valid full backup.\n Output: {0} \n CMD: {1}".format(
                repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertEqual(e.message,
                'ERROR: Valid backup on current timeline is not found. Create new FULL backup before an incremental one.\n',
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(repr(e.message), self.cmd))

        self.assertEqual(self.show_pb(backup_dir, 'node')[0]['Status'], "ERROR")

        # Clean after yourself
        self.del_test_dir(module_name, fname)
