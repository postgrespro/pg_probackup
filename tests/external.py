import unittest
import os
from time import sleep
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from .helpers.cfs_helpers import find_by_name
import shutil


module_name = 'external'


class ExternalTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_external_simple(self):
        """
        make node, create external directory, take backup
        with external directory, restore backup, check that
        external directory was successfully copied
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        external_dir = self.get_tblspace_path(node, 'somedirectory')

        # create directory in external_directory
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # FULL backup
        self.backup_node(
            backup_dir, 'node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        # Fill external directories
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir, options=["-j", "4"])

        # Full backup with external dir
        self.backup_node(
            backup_dir, 'node', node,
            options=[
                '--external-dirs={0}'.format(external_dir)])

        if self.paranoia:
            pgdata = self.pgdata_content(
                node.base_dir, exclude_dirs=['logs'])

        node.cleanup()
        shutil.rmtree(external_dir, ignore_errors=True)

        self.restore_node(
            backup_dir, 'node', node, options=["-j", "4"])

        if self.paranoia:
            pgdata_restored = self.pgdata_content(
                node.base_dir, exclude_dirs=['logs'])
            self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_external_none(self):
        """
        make node, create external directory, take backup
        with external directory, take delta backup with --external-dirs=none,
        restore delta backup, check that
        external directory was not copied
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        external_dir = self.get_tblspace_path(node, 'somedirectory')

        # create directory in external_directory
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # FULL backup
        self.backup_node(
            backup_dir, 'node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        # Fill external directories with data
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir, options=["-j", "4"])

        # Full backup with external dir
        self.backup_node(
            backup_dir, 'node', node,
            options=[
                '--external-dirs={0}'.format(external_dir)])

        # Delta backup without external directory
        self.backup_node(
            backup_dir, 'node', node, backup_type="delta",
            options=['--external-dirs=none'])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs', 'somedirectory'])

        node.cleanup()
        shutil.rmtree(external_dir, ignore_errors=True)

        self.restore_node(
            backup_dir, 'node', node, options=["-j", "4"])

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        # self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_external_dir_mapping(self):
        """
        make node, take full backup, check that restore with
        external-dir mapping will end with error, take page backup,
        check that restore with external-dir mapping will end with
        success
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica',
                'max_wal_senders': '2'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(
            backup_dir, 'node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        external_dir1_old = self.get_tblspace_path(node, 'external_dir1')
        external_dir2_old = self.get_tblspace_path(node, 'external_dir2')

        # copy postgresql.conf to external_directory
        os.mkdir(external_dir1_old)
        os.mkdir(external_dir2_old)
        shutil.copyfile(
            os.path.join(node.data_dir, 'postgresql.conf'),
            os.path.join(external_dir1_old, 'postgresql.conf'))
        shutil.copyfile(
            os.path.join(node.data_dir, 'postgresql.conf'),
            os.path.join(external_dir2_old, 'postgresql.conf'))

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))
        node_restored.cleanup()

        external_dir1_new = self.get_tblspace_path(node_restored, 'external_dir1')
        external_dir2_new = self.get_tblspace_path(node_restored, 'external_dir2')

        try:
            self.restore_node(
                backup_dir, 'node', node_restored,
                options=[
                    "-j", "4",
                    "--external-mapping={0}={1}".format(
                        external_dir1_old, external_dir1_new),
                    "--external-mapping={0}={1}".format(
                        external_dir2_old, external_dir2_new)])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because tablespace mapping is incorrect"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'ERROR: --external-mapping option' in e.message and
                'have an entry in list of external directories' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.backup_node(
            backup_dir, 'node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}:{1}".format(external_dir1_old, external_dir2_old)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=[
                "-j", "4",
                "--external-mapping={0}={1}".format(
                    external_dir1_old, external_dir1_new),
                "--external-mapping={0}={1}".format(
                    external_dir2_old, external_dir2_new)])

        pgdata_restored = self.pgdata_content(
            node_restored.base_dir, exclude_dirs=['logs'])
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_backup_multiple_external(self):
        """make node, """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={
                'wal_level': 'replica'})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        external_dir1_old = self.get_tblspace_path(node, 'external_dir1')
        external_dir2_old = self.get_tblspace_path(node, 'external_dir2')

        # FULL backup
        self.backup_node(
            backup_dir, 'node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        # fill external directories with data
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1_old, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2_old, options=["-j", "4"])

        self.set_config(
            backup_dir, 'node',
            options=[
                '-E', external_dir1_old])

        # cmdline option MUST override options in config
        self.backup_node(
            backup_dir, 'node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}".format(external_dir2_old)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs', 'external_dir1'])

        node.cleanup()

        shutil.rmtree(external_dir1_old, ignore_errors=True)
        shutil.rmtree(external_dir2_old, ignore_errors=True)

        self.restore_node(
            backup_dir, 'node', node,
            options=["-j", "4"])
        
        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_external_backward_compatibility(self):
        """Description in jira issue PGPRO-434"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'max_wal_senders': '2',
                'autovacuum': 'off'})

        self.init_pb(backup_dir, old_binary=True)
        self.show_pb(backup_dir)

        self.add_instance(backup_dir, 'node', node, old_binary=True)
        self.show_pb(backup_dir)

        node.slow_start()

        node.pgbench_init(scale=10)

        # FULL backup with old binary without external dirs support
        self.backup_node(
            backup_dir, 'node', node,
            old_binary=True, options=["-j", "4", "--stream"])

        external_dir1_old = self.get_tblspace_path(node, 'external_dir1')
        external_dir2_old = self.get_tblspace_path(node, 'external_dir2')

        # fill external directories with data
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1_old, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2_old, options=["-j", "4"])

        # delta backup with external directories using new binary
        self.backup_node(
            backup_dir, 'node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}:{1}".format(
                    external_dir1_old,
                    external_dir2_old)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # RESTORE chain with new binary
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))

        node_restored.cleanup()

        external_dir1_new = self.get_tblspace_path(node_restored, 'external_dir1')
        external_dir2_new = self.get_tblspace_path(node_restored, 'external_dir2')

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=[
                "-j", "4",
                "--external-mapping={0}={1}".format(external_dir1_old, external_dir1_new),
                "--external-mapping={0}={1}".format(external_dir2_old, external_dir2_new)])

        pgdata_restored = self.pgdata_content(
            node_restored.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

        # external directory contain symlink to file
        # external directory contain symlink to directory
        # latest page backup without external_dir
        # multiple external directories +
        # --external-dirs=none +
        # --external-dirs point to a file
        # external directory in config and in command line +
        # external directory contain multuple directories, some of them my be empty +
        # forbid to external-dirs to point to tablespace directories
        # check that not changed files are not copied by next backup +
        # merge
        # complex merge
