import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, idx_ptrack
import time


module_name = 'ptrack_clean'


class SimpleTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_clean(self):
        """Take backups of every available types and check that PTRACK is clean"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'ptrack_enable': 'on',
                'wal_level': 'replica',
                'max_wal_senders': '2',
                'autovacuum': 'off'})
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        self.create_tblspace_in_node(node, 'somedata')

        # Create table
        node.safe_psql(
            "postgres",
            "create sequence t_seq; create table t_heap "
            "(id int DEFAULT nextval('t_seq'), text text, tsvector tsvector) "
            "tablespace somedata")

        # Take FULL backup to clean every ptrack
        self.backup_node(
            backup_dir, 'node', node,
            options=['-j10', '--stream'])

        # Create indexes
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                node.safe_psql(
                    "postgres",
                    "create index {0} on {1} using {2}({3}) "
                    "tablespace somedata".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'],
                        idx_ptrack[i]['column']))

        node.safe_psql('postgres', 'checkpoint')

        node_restored = self.make_simple_node(
            base_dir="{0}/{1}/node_restored".format(module_name, fname))
        node_restored.cleanup()

        tblspace1 = self.get_tblspace_path(node, 'somedata')
        tblspace2 = self.get_tblspace_path(node_restored, 'somedata')

        # Take PTRACK backup
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='ptrack',
            options=['-j10', '--log-level-file=verbose'])

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        self.restore_node(
            backup_dir, 'node', node_restored,
            backup_id=backup_id,
            options=[
                "-j", "4",
                "-T{0}={1}".format(tblspace1, tblspace2)]
        )

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_ptrack_clean_replica(self):
        """Take backups of every available types from master and check that PTRACK on replica is clean"""
        fname = self.id().split('.')[3]
        master = self.make_simple_node(
            base_dir="{0}/{1}/master".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'ptrack_enable': 'on',
                'wal_level': 'replica',
                'max_wal_senders': '2'})
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'master', master)
        master.start()

        self.backup_node(backup_dir, 'master', master, options=['--stream'])

        replica = self.make_simple_node(
            base_dir="{0}/{1}/replica".format(module_name, fname))
        replica.cleanup()

        self.restore_node(backup_dir, 'master', replica)

        self.add_instance(backup_dir, 'replica', replica)
        self.set_replica(master, replica, synchronous=True)
        self.set_archiving(backup_dir, 'replica', replica, replica=True)
        replica.start()

        # Create table
        master.safe_psql(
            "postgres",
            "create sequence t_seq; create table t_heap "
            "(id int DEFAULT nextval('t_seq'), text text, tsvector tsvector)")
        self.wait_until_replica_catch_with_master(master, replica)

        # Take FULL backup
        self.backup_node(
            backup_dir,
            'replica',
            replica,
            options=[
                '-j10', '--stream',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port)])

        # Create indexes
        for i in idx_ptrack:
            if idx_ptrack[i]['type'] != 'heap' and idx_ptrack[i]['type'] != 'seq':
                master.safe_psql(
                    "postgres",
                    "create index {0} on {1} using {2}({3})".format(
                        i, idx_ptrack[i]['relation'],
                        idx_ptrack[i]['type'],
                        idx_ptrack[i]['column']))


        self.wait_until_replica_catch_with_master(master, replica)

        node_restored = self.make_simple_node(
            base_dir="{0}/{1}/node_restored".format(module_name, fname))
        node_restored.cleanup()

        # Take PTRACK backup
        backup_id = self.backup_node(
            backup_dir,
            'replica',
            replica,
            backup_type='ptrack',
            options=[
                '-j10', '--stream',
                '--master-host=localhost',
                '--master-db=postgres',
                '--master-port={0}'.format(master.port)])
        
        if self.paranoia:
            pgdata = self.pgdata_content(replica.data_dir)

        self.restore_node(
            backup_dir, 'replica', node_restored,
            backup_id=backup_id,
            options=["-j", "4"]
        )

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node_restored.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)
