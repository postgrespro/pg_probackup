import unittest
import os
from time import sleep
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from .helpers.cfs_helpers import find_by_name
import shutil


module_name = 'external'

# TODO: add some ptrack tests
class ExternalTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_basic_external(self):
        """
        make node, create external directory, take backup
        with external directory, restore backup, check that
        external directory was successfully copied
        """
        fname = self.id().split('.')[3]
        core_dir = os.path.join(self.tmp_path, module_name, fname)
        shutil.rmtree(core_dir, ignore_errors=True)
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            set_replication=True)

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        external_dir = self.get_tblspace_path(node, 'somedirectory')

        # create directory in external_directory
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # take FULL backup with external directory pointing to a file
        file_path = os.path.join(core_dir, 'file')
        with open(file_path, "w+") as f:
            pass

        try:
            self.backup_node(
                backup_dir, 'node', node, backup_type="full",
                options=[
                    '--external-dirs={0}'.format(file_path)])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because external dir point to a file"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'ERROR:  --external-dirs option' in e.message and
                'directory or symbolic link expected' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        sleep(1)

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

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        self.restore_node(
            backup_dir, 'node', node, options=["-j", "4"])

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
            set_replication=True)

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        external_dir = self.get_tblspace_path(node, 'somedirectory')

        # create directory in external_directory
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
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
                '--stream',
                '--external-dirs={0}'.format(external_dir)])

        # Delta backup without external directory
        self.backup_node(
            backup_dir, 'node', node, backup_type="delta",
            options=['--external-dirs=none', '--stream'])

        shutil.rmtree(external_dir, ignore_errors=True)
        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        self.restore_node(
            backup_dir, 'node', node, options=["-j", "4"])

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_external_dirs_overlapping(self):
        """
        make node, create directory,
        take backup with two external directories pointing to
        the same directory, backup should fail
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            set_replication=True)

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # create directory in external_directory
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        os.mkdir(external_dir1)
        os.mkdir(external_dir2)

        # Full backup with external dirs
        try:
            self.backup_node(
                backup_dir, 'node', node,
                options=[
                    "-j", "4", "--stream",
                    "-E", "{0}{1}{2}{1}{0}".format(
                        external_dir1,
                        self.EXTERNAL_DIRECTORY_DELIMITER,
                        external_dir2,
                        self.EXTERNAL_DIRECTORY_DELIMITER,
                        external_dir1)])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because tablespace mapping is incorrect"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertTrue(
                'ERROR: External directory path (-E option)' in e.message and
                'contain another external directory' in e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

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
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        self.backup_node(
            backup_dir, 'node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # Fill external directories with data
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2, options=["-j", "4"])

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
                        external_dir1, external_dir1_new),
                    "--external-mapping={0}={1}".format(
                        external_dir2, external_dir2_new)])
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
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=[
                "-j", "4",
                "--external-mapping={0}={1}".format(
                    external_dir1, external_dir1_new),
                "--external-mapping={0}={1}".format(
                    external_dir2, external_dir2_new)])

        pgdata_restored = self.pgdata_content(
            node_restored.base_dir, exclude_dirs=['logs'])
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_backup_multiple_external(self):
        """check that cmdline has priority over config"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # FULL backup
        self.backup_node(
            backup_dir, 'node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        # fill external directories with data
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2, options=["-j", "4"])

        self.set_config(
            backup_dir, 'node',
            options=['-E', external_dir1])

        # cmdline option MUST override options in config
        self.backup_node(
            backup_dir, 'node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", external_dir2])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs', 'external_dir1'])

        node.cleanup()
        shutil.rmtree(external_dir1, ignore_errors=True)
        shutil.rmtree(external_dir2, ignore_errors=True)

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
        """
        take backup with old binary without external dirs support
        take delta backup with new binary and 2 external directories
        restore delta backup, check that incremental chain
        restored correctly
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir, old_binary=True)
        self.show_pb(backup_dir)

        self.add_instance(backup_dir, 'node', node, old_binary=True)
        self.show_pb(backup_dir)

        node.slow_start()

        node.pgbench_init(scale=3)

        # FULL backup with old binary without external dirs support
        self.backup_node(
            backup_dir, 'node', node,
            old_binary=True, options=["-j", "4", "--stream"])

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # fill external directories with data
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2, options=["-j", "4"])

        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # FULL backup
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            old_binary=True, options=["-j", "4", "--stream"])

        # fill external directories with changed data
        shutil.rmtree(external_dir1, ignore_errors=True)
        shutil.rmtree(external_dir2, ignore_errors=True)

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2, options=["-j", "4"])

        self.delete_pb(backup_dir, 'node', backup_id=backup_id)

        # delta backup with external directories using new binary
        self.backup_node(
            backup_dir, 'node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

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
                "--external-mapping={0}={1}".format(external_dir1, external_dir1_new),
                "--external-mapping={0}={1}".format(external_dir2, external_dir2_new)])

        pgdata_restored = self.pgdata_content(
            node_restored.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_external_backward_compatibility_merge_1(self):
        """
        take backup with old binary without external dirs support
        take delta backup with new binary and 2 external directories
        merge delta backup ajd restore it
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir, old_binary=True)
        self.show_pb(backup_dir)

        self.add_instance(backup_dir, 'node', node, old_binary=True)
        self.show_pb(backup_dir)

        node.slow_start()

        node.pgbench_init(scale=3)

        # tmp FULL backup with old binary
        tmp_id = self.backup_node(
            backup_dir, 'node', node,
            old_binary=True, options=["-j", "4", "--stream"])

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # fill external directories with data
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2, options=["-j", "4"])

        self.delete_pb(backup_dir, 'node', backup_id=tmp_id)

        # FULL backup with old binary without external dirs support
        self.backup_node(
            backup_dir, 'node', node,
            old_binary=True, options=["-j", "4", "--stream"])

        pgbench = node.pgbench(options=['-T', '30', '-c', '1'])
        pgbench.wait()

        # delta backup with external directories using new binary
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # Merge chain chain with new binary
        self.merge_backup(backup_dir, 'node', backup_id=backup_id)

        # Restore merged backup
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))

        node_restored.cleanup()

        external_dir1_new = self.get_tblspace_path(node_restored, 'external_dir1')
        external_dir2_new = self.get_tblspace_path(node_restored, 'external_dir2')

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=[
                "-j", "4",
                "--external-mapping={0}={1}".format(external_dir1, external_dir1_new),
                "--external-mapping={0}={1}".format(external_dir2, external_dir2_new)])

        pgdata_restored = self.pgdata_content(
            node_restored.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_external_backward_compatibility_merge_2(self):
        """
        take backup with old binary without external dirs support
        take delta backup with new binary and 2 external directories
        merge delta backup and restore it
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir, old_binary=True)
        self.show_pb(backup_dir)

        self.add_instance(backup_dir, 'node', node, old_binary=True)
        self.show_pb(backup_dir)

        node.slow_start()

        node.pgbench_init(scale=3)

        # tmp FULL backup with old binary
        tmp_id = self.backup_node(
            backup_dir, 'node', node,
            old_binary=True, options=["-j", "4", "--stream"])

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # fill external directories with data
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2, options=["-j", "4"])

        self.delete_pb(backup_dir, 'node', backup_id=tmp_id)

        # FULL backup with old binary without external dirs support
        self.backup_node(
            backup_dir, 'node', node,
            old_binary=True, options=["-j", "4", "--stream"])

        pgbench = node.pgbench(options=['-T', '30', '-c', '1'])
        pgbench.wait()

        # delta backup with external directories using new binary
        self.backup_node(
            backup_dir, 'node', node,
            backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        pgbench = node.pgbench(options=['-T', '30', '-c', '1'])
        pgbench.wait()

        # Fill external dirs with changed data
        shutil.rmtree(external_dir1, ignore_errors=True)
        shutil.rmtree(external_dir2, ignore_errors=True)

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1,
            options=['-j', '4', '--skip-external-dirs'])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2,
            options=['-j', '4', '--skip-external-dirs'])

        # delta backup without external directories using old binary
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # Merge chain using new binary
        self.merge_backup(backup_dir, 'node', backup_id=backup_id)

        # Restore merged backup
        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))

        node_restored.cleanup()

        external_dir1_new = self.get_tblspace_path(
            node_restored, 'external_dir1')
        external_dir2_new = self.get_tblspace_path(
            node_restored, 'external_dir2')

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=[
                "-j", "4",
                "--external-mapping={0}={1}".format(
                    external_dir1, external_dir1_new),
                "--external-mapping={0}={1}".format(
                    external_dir2, external_dir2_new)])

        pgdata_restored = self.pgdata_content(
            node_restored.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_external_merge(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=3)

        # take temp FULL backup
        tmp_id = self.backup_node(
            backup_dir, 'node', node, options=["-j", "4", "--stream"])

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # fill external directories with data
        self.restore_node(
            backup_dir, 'node', node, backup_id=tmp_id,
            data_dir=external_dir1, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node, backup_id=tmp_id,
            data_dir=external_dir2, options=["-j", "4"])

        self.delete_pb(backup_dir, 'node', backup_id=tmp_id)

        # FULL backup with old binary without external dirs support
        self.backup_node(
            backup_dir, 'node', node,
            old_binary=True, options=["-j", "4", "--stream"])

        # change data a bit
        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # delta backup with external directories using new binary
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        print(self.show_pb(backup_dir, 'node', as_json=False, as_text=True))

        # Merge
        print(self.merge_backup(backup_dir, 'node', backup_id=backup_id,
            options=['--log-level-file=VERBOSE']))

        # RESTORE
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        external_dir1_new = self.get_tblspace_path(node, 'external_dir1')
        external_dir2_new = self.get_tblspace_path(node, 'external_dir2')

        self.restore_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4",
                "--external-mapping={0}={1}".format(
                    external_dir1, external_dir1_new),
                "--external-mapping={0}={1}".format(
                    external_dir2, external_dir2_new)])

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_external_merge_skip_external_dirs(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        # FULL backup with old data
        tmp_id = self.backup_node(
            backup_dir, 'node', node, options=["-j", "4", "--stream"])

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # fill external directories with old data
        self.restore_node(
            backup_dir, 'node', node, backup_id=tmp_id,
            data_dir=external_dir1, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node, backup_id=tmp_id,
            data_dir=external_dir2, options=["-j", "4"])

        self.delete_pb(backup_dir, 'node', backup_id=tmp_id)

        # change data a bit
        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # FULL backup with external directories
        self.backup_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        # drop old external data
        shutil.rmtree(external_dir1, ignore_errors=True)
        shutil.rmtree(external_dir2, ignore_errors=True)

        # fill external directories with new data
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1,
            options=["-j", "4", "--skip-external-dirs"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2,
            options=["-j", "4", "--skip-external-dirs"])

        # DELTA backup with external directories
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # merge backups without external directories
        self.merge_backup(
            backup_dir, 'node',
            backup_id=backup_id, options=['--skip-external-dirs'])

        # RESTORE
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

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
    def test_external_merge_1(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        # FULL backup
        self.backup_node(
            backup_dir, 'node', node, options=["-j", "4", "--stream"])

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # FULL backup  with changed data
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            options=["-j", "4", "--stream"])

        # fill external directories with changed data
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2, options=["-j", "4"])

        self.delete_pb(backup_dir, 'node', backup_id=backup_id)

        # delta backup with external directories using new binary
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        self.merge_backup(backup_dir, 'node', backup_id=backup_id)

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # RESTORE
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        external_dir1_new = self.get_tblspace_path(node, 'external_dir1')
        external_dir2_new = self.get_tblspace_path(node, 'external_dir2')

        self.restore_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4",
                "--external-mapping={0}={1}".format(external_dir1, external_dir1_new),
                "--external-mapping={0}={1}".format(external_dir2, external_dir2_new)])

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_external_merge_3(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        # FULL backup
        self.backup_node(backup_dir, 'node', node, options=["-j", "4"])

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # FULL backup
        backup_id = self.backup_node(
            backup_dir, 'node', node)

        # fill external directories with changed data
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1)

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2)

        self.delete_pb(backup_dir, 'node', backup_id=backup_id)

        # page backup with external directories
        self.backup_node(
            backup_dir, 'node', node, backup_type="page",
            options=[
                "-j", "4",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        # page backup with external directories
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type="page",
            options=[
                "-j", "4",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.merge_backup(
            backup_dir, 'node', backup_id=backup_id,
            options=['--log-level-file=verbose'])

        # RESTORE
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        external_dir1_new = self.get_tblspace_path(node, 'external_dir1')
        external_dir2_new = self.get_tblspace_path(node, 'external_dir2')

        self.restore_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4",
                "--external-mapping={0}={1}".format(
                    external_dir1, external_dir1_new),
                "--external-mapping={0}={1}".format(
                    external_dir2, external_dir2_new)])

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_external_merge_2(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        # FULL backup
        self.backup_node(
            backup_dir, 'node', node, options=["-j", "4", "--stream"])

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # FULL backup
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            options=["-j", "4", "--stream"])

        # fill external directories with changed data
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2, options=["-j", "4"])

        self.delete_pb(backup_dir, 'node', backup_id=backup_id)

        # delta backup with external directories
        self.backup_node(
            backup_dir, 'node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        # delta backup with external directories
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        shutil.rmtree(external_dir1, ignore_errors=True)
        shutil.rmtree(external_dir2, ignore_errors=True)

        # delta backup without external directories
        self.merge_backup(backup_dir, 'node', backup_id=backup_id)

        # RESTORE
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        external_dir1_new = self.get_tblspace_path(node, 'external_dir1')
        external_dir2_new = self.get_tblspace_path(node, 'external_dir2')

        self.restore_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4",
                "--external-mapping={0}={1}".format(external_dir1, external_dir1_new),
                "--external-mapping={0}={1}".format(external_dir2, external_dir2_new)])

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_restore_external_changed_data(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=2)

        # set externals
        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # FULL backup
        tmp_id = self.backup_node(
            backup_dir, 'node',
            node, options=["-j", "4", "--stream"])

        # fill external directories with data
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2, options=["-j", "4"])

        self.delete_pb(backup_dir, 'node', backup_id=tmp_id)

        # change data a bit
        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # FULL backup
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        # fill external directories with changed data
        shutil.rmtree(external_dir1, ignore_errors=True)
        shutil.rmtree(external_dir2, ignore_errors=True)

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1, backup_id=backup_id,
            options=["-j", "4", "--skip-external-dirs"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2, backup_id=backup_id,
            options=["-j", "4", "--skip-external-dirs"])

        # change data a bit more
        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Delta backup with external directories
        self.backup_node(
            backup_dir, 'node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # Restore
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

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
    def test_restore_external_changed_data_1(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off',
                'max_wal_size': '32MB'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=1)

        # set externals
        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # FULL backup
        tmp_id = self.backup_node(
            backup_dir, 'node',
            node, options=["-j", "4", "--stream"])

        # fill external directories with data
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2, options=["-j", "4"])

        self.delete_pb(backup_dir, 'node', backup_id=tmp_id)

        # change data a bit
        pgbench = node.pgbench(options=['-T', '5', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # FULL backup
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        # fill external directories with changed data
        shutil.rmtree(external_dir1, ignore_errors=True)
        shutil.rmtree(external_dir2, ignore_errors=True)

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1, backup_id=backup_id,
            options=["-j", "4", "--skip-external-dirs"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2, backup_id=backup_id,
            options=["-j", "4", "--skip-external-dirs"])

        # change data a bit more
        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Delta backup with only one external directory
        self.backup_node(
            backup_dir, 'node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", external_dir1])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs', 'external_dir2'])

        # Restore
        node.cleanup()
        shutil.rmtree(node._base_dir)

        # create empty file in external_dir2
        os.mkdir(node._base_dir)
        os.mkdir(external_dir2)
        with open(os.path.join(external_dir2, 'file'), 'w+') as f:
            f.close()

        output = self.restore_node(
            backup_dir, 'node', node,
            options=["-j", "4"])

        self.assertNotIn(
            'externaldir2',
            output)

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs', 'external_dir2'])

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_merge_external_changed_data(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off',
                'max_wal_size': '32MB'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        node.pgbench_init(scale=2)

        # set externals
        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # FULL backup
        tmp_id = self.backup_node(
            backup_dir, 'node',
            node, options=["-j", "4", "--stream"])

        # fill external directories with data
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2, options=["-j", "4"])

        self.delete_pb(backup_dir, 'node', backup_id=tmp_id)

        # change data a bit
        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # FULL backup
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        # fill external directories with changed data
        shutil.rmtree(external_dir1, ignore_errors=True)
        shutil.rmtree(external_dir2, ignore_errors=True)

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1, backup_id=backup_id,
            options=["-j", "4", "--skip-external-dirs"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2, backup_id=backup_id,
            options=["-j", "4", "--skip-external-dirs"])

        # change data a bit more
        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Delta backup with external directories
        backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # Merge
        self.merge_backup(backup_dir, 'node', backup_id)

        # Restore
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

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
    def test_restore_skip_external(self):
        """
        Check that --skip-external-dirs works correctly
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # temp FULL backup
        backup_id = self.backup_node(
            backup_dir, 'node', node, options=["-j", "4", "--stream"])

        # fill external directories with data
        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir1, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=external_dir2, options=["-j", "4"])

        self.delete_pb(backup_dir, 'node', backup_id=backup_id)

        # FULL backup with external directories
        self.backup_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        # delete first externals, so pgdata_compare
        # will be capable of detecting redundant
        # external files after restore
        shutil.rmtree(external_dir1, ignore_errors=True)
        shutil.rmtree(external_dir2, ignore_errors=True)

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # RESTORE
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        self.restore_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4", "--skip-external-dirs"])

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_external_dir_is_symlink(self):
        """
        Check that backup works correctly if external dir is symlink,
        symlink pointing to external dir should be followed,
        but restored as directory
        """
        if os.name == 'nt':
            return unittest.skip('Skipped for Windows')

        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        core_dir = os.path.join(self.tmp_path, module_name, fname)
        shutil.rmtree(core_dir, ignore_errors=True)
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        external_dir = self.get_tblspace_path(node, 'external_dir')

        # temp FULL backup
        backup_id = self.backup_node(
            backup_dir, 'node', node, options=["-j", "4", "--stream"])

        # fill some directory with data
        core_dir = os.path.join(self.tmp_path, module_name, fname)
        symlinked_dir = os.path.join(core_dir, 'symlinked')

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=symlinked_dir, options=["-j", "4"])

        # drop temp FULL backup
        self.delete_pb(backup_dir, 'node', backup_id=backup_id)

        # create symlink to directory in external directory
        os.symlink(symlinked_dir, external_dir)

        # FULL backup with external directories
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))

        # RESTORE
        node_restored.cleanup()

        external_dir_new = self.get_tblspace_path(
            node_restored, 'external_dir')

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=[
                "-j", "4", "--external-mapping={0}={1}".format(
                    external_dir, external_dir_new)])

        pgdata_restored = self.pgdata_content(
            node_restored.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

        self.assertEqual(
            external_dir,
            self.show_pb(
                backup_dir, 'node',
                backup_id=backup_id)['external-dirs'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_external_dir_contain_symlink_on_dir(self):
        """
        Check that backup works correctly if external dir is symlink,
        symlink pointing to external dir should be followed,
        but restored as directory
        """
        if os.name == 'nt':
            return unittest.skip('Skipped for Windows')

        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        core_dir = os.path.join(self.tmp_path, module_name, fname)
        shutil.rmtree(core_dir, ignore_errors=True)
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        external_dir = self.get_tblspace_path(node, 'external_dir')
        dir_in_external_dir = os.path.join(external_dir, 'dir')

        # temp FULL backup
        backup_id = self.backup_node(
            backup_dir, 'node', node, options=["-j", "4", "--stream"])

        # fill some directory with data
        core_dir = os.path.join(self.tmp_path, module_name, fname)
        symlinked_dir = os.path.join(core_dir, 'symlinked')

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=symlinked_dir, options=["-j", "4"])

        # drop temp FULL backup
        self.delete_pb(backup_dir, 'node', backup_id=backup_id)

        # create symlink to directory in external directory
        os.mkdir(external_dir)
        os.symlink(symlinked_dir, dir_in_external_dir)

        # FULL backup with external directories
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))

        # RESTORE
        node_restored.cleanup()

        external_dir_new = self.get_tblspace_path(
            node_restored, 'external_dir')

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=[
                "-j", "4", "--external-mapping={0}={1}".format(
                    external_dir, external_dir_new)])

        pgdata_restored = self.pgdata_content(
            node_restored.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

        self.assertEqual(
            external_dir,
            self.show_pb(
                backup_dir, 'node',
                backup_id=backup_id)['external-dirs'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_external_dir_contain_symlink_on_file(self):
        """
        Check that backup works correctly if external dir is symlink,
        symlink pointing to external dir should be followed,
        but restored as directory
        """
        if os.name == 'nt':
            return unittest.skip('Skipped for Windows')

        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        core_dir = os.path.join(self.tmp_path, module_name, fname)
        shutil.rmtree(core_dir, ignore_errors=True)
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        external_dir = self.get_tblspace_path(node, 'external_dir')
        file_in_external_dir = os.path.join(external_dir, 'file')

        # temp FULL backup
        backup_id = self.backup_node(
            backup_dir, 'node', node, options=["-j", "4", "--stream"])

        # fill some directory with data
        core_dir = os.path.join(self.tmp_path, module_name, fname)
        symlinked_dir = os.path.join(core_dir, 'symlinked')

        self.restore_node(
            backup_dir, 'node', node,
            data_dir=symlinked_dir, options=["-j", "4"])

        # drop temp FULL backup
        self.delete_pb(backup_dir, 'node', backup_id=backup_id)

        # create symlink to directory in external directory
        src_file = os.path.join(symlinked_dir, 'postgresql.conf')
        os.mkdir(external_dir)
        os.chmod(external_dir, 0o0700)
        os.symlink(src_file, file_in_external_dir)

        # FULL backup with external directories
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        node_restored = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node_restored'))

        # RESTORE
        node_restored.cleanup()

        external_dir_new = self.get_tblspace_path(
            node_restored, 'external_dir')

        self.restore_node(
            backup_dir, 'node', node_restored,
            options=[
                "-j", "4", "--external-mapping={0}={1}".format(
                    external_dir, external_dir_new)])

        pgdata_restored = self.pgdata_content(
            node_restored.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

        self.assertEqual(
            external_dir,
            self.show_pb(
                backup_dir, 'node',
                backup_id=backup_id)['external-dirs'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_external_dir_is_tablespace(self):
        """
        Check that backup fails with error
        if external directory points to tablespace
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        core_dir = os.path.join(self.tmp_path, module_name, fname)
        shutil.rmtree(core_dir, ignore_errors=True)
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        external_dir = self.get_tblspace_path(node, 'external_dir')

        self.create_tblspace_in_node(
            node, 'tblspace1', tblspc_path=external_dir)

        node.pgbench_init(scale=1, tablespace='tblspace1')

        # FULL backup with external directories
        try:
            backup_id = self.backup_node(
                backup_dir, 'node', node,
                options=[
                    "-j", "4", "--stream",
                    "-E", external_dir])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because external dir points to the tablespace"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'External directory path (-E option)',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_restore_external_dir_not_empty(self):
        """
        Check that backup fails with error
        if external directory point to not empty tablespace and
        if remapped directory also isn`t empty
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        core_dir = os.path.join(self.tmp_path, module_name, fname)
        shutil.rmtree(core_dir, ignore_errors=True)
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        external_dir = self.get_tblspace_path(node, 'external_dir')

        # create empty file in external directory
        # open(os.path.join(external_dir, 'file'), 'a').close()
        os.mkdir(external_dir)
        with open(os.path.join(external_dir, 'file'), 'w+') as f:
            f.close()

        # FULL backup with external directory
        self.backup_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        node.cleanup()

        try:
            self.restore_node(backup_dir, 'node', node)
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because external dir is not empty"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'External directory is not empty',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        external_dir_new = self.get_tblspace_path(node, 'external_dir_new')

        # create empty file in directory, which will be a target of
        # remapping
        os.mkdir(external_dir_new)
        with open(os.path.join(external_dir_new, 'file1'), 'w+') as f:
            f.close()

        try:
            self.restore_node(
                backup_dir, 'node', node,
                options=['--external-mapping={0}={1}'.format(
                        external_dir, external_dir_new)])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because remapped external dir is not empty"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'External directory is not empty',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_restore_external_dir_is_missing(self):
        """
        take FULL backup with not empty external directory
        delete external directory
        take DELTA backup with external directory, which
        should fail
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        core_dir = os.path.join(self.tmp_path, module_name, fname)
        shutil.rmtree(core_dir, ignore_errors=True)
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        external_dir = self.get_tblspace_path(node, 'external_dir')

        # create empty file in external directory
        # open(os.path.join(external_dir, 'file'), 'a').close()
        os.mkdir(external_dir)
        with open(os.path.join(external_dir, 'file'), 'w+') as f:
            f.close()

        # FULL backup with external directory
        self.backup_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        # drop external directory
        shutil.rmtree(external_dir, ignore_errors=True)

        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='delta',
                options=[
                    "-j", "4", "--stream",
                    "-E", external_dir])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because external dir is missing"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: External directory is not found:',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        sleep(1)

        # take DELTA without external directories
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta',
            options=["-j", "4", "--stream"])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # Restore Delta backup
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_merge_external_dir_is_missing(self):
        """
        take FULL backup with not empty external directory
        delete external directory
        take DELTA backup with external directory, which
        should fail,
        take DELTA backup without external directory,
        merge it into FULL, restore and check
        data correctness
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        core_dir = os.path.join(self.tmp_path, module_name, fname)
        shutil.rmtree(core_dir, ignore_errors=True)
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        external_dir = self.get_tblspace_path(node, 'external_dir')

        # create empty file in external directory
        # open(os.path.join(external_dir, 'file'), 'a').close()
        os.mkdir(external_dir)
        with open(os.path.join(external_dir, 'file'), 'w+') as f:
            f.close()

        # FULL backup with external directory
        self.backup_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        # drop external directory
        shutil.rmtree(external_dir, ignore_errors=True)

        try:
            self.backup_node(
                backup_dir, 'node', node,
                backup_type='delta',
                options=[
                    "-j", "4", "--stream",
                    "-E", external_dir])
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because external dir is missing"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'ERROR: External directory is not found:',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        sleep(1)

        # take DELTA without external directories
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta',
            options=["-j", "4", "--stream"])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # Merge
        self.merge_backup(backup_dir, 'node', backup_id=backup_id)

        # Restore
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_restore_external_dir_is_empty(self):
        """
        take FULL backup with not empty external directory
        drop external directory content
        take DELTA backup with the same external directory
        restore DELRA backup, check that restored
        external directory is empty
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        core_dir = os.path.join(self.tmp_path, module_name, fname)
        shutil.rmtree(core_dir, ignore_errors=True)
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        external_dir = self.get_tblspace_path(node, 'external_dir')

        # create empty file in external directory
        # open(os.path.join(external_dir, 'file'), 'a').close()
        os.mkdir(external_dir)
        os.chmod(external_dir, 0o0700)
        with open(os.path.join(external_dir, 'file'), 'w+') as f:
            f.close()

        # FULL backup with external directory
        self.backup_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        # make external directory empty
        os.remove(os.path.join(external_dir, 'file'))

        # take DELTA backup with empty external directory
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta',
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # Restore Delta backup
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_merge_external_dir_is_empty(self):
        """
        take FULL backup with not empty external directory
        drop external directory content
        take DELTA backup with the same external directory
        merge backups and restore FULL, check that restored
        external directory is empty
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        core_dir = os.path.join(self.tmp_path, module_name, fname)
        shutil.rmtree(core_dir, ignore_errors=True)
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        external_dir = self.get_tblspace_path(node, 'external_dir')

        # create empty file in external directory
        # open(os.path.join(external_dir, 'file'), 'a').close()
        os.mkdir(external_dir)
        os.chmod(external_dir, 0o0700)
        with open(os.path.join(external_dir, 'file'), 'w+') as f:
            f.close()

        # FULL backup with external directory
        self.backup_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        # make external directory empty
        os.remove(os.path.join(external_dir, 'file'))

        # take DELTA backup with empty external directory
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta',
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # Merge
        self.merge_backup(backup_dir, 'node', backup_id=backup_id)

        # Restore
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])
        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_restore_external_dir_string_order(self):
        """
        take FULL backup with not empty external directory
        drop external directory content
        take DELTA backup with the same external directory
        restore DELRA backup, check that restored
        external directory is empty
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        core_dir = os.path.join(self.tmp_path, module_name, fname)
        shutil.rmtree(core_dir, ignore_errors=True)
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        external_dir_1 = self.get_tblspace_path(node, 'external_dir_1')
        external_dir_2 = self.get_tblspace_path(node, 'external_dir_2')

        # create empty file in external directory
        os.mkdir(external_dir_1)
        os.chmod(external_dir_1, 0o0700)
        with open(os.path.join(external_dir_1, 'fileA'), 'w+') as f:
            f.close()

        os.mkdir(external_dir_2)
        os.chmod(external_dir_2, 0o0700)
        with open(os.path.join(external_dir_2, 'fileZ'), 'w+') as f:
            f.close()

        # FULL backup with external directory
        self.backup_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir_1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir_2)])

        with open(os.path.join(external_dir_1, 'fileB'), 'w+') as f:
            f.close()

        with open(os.path.join(external_dir_2, 'fileY'), 'w+') as f:
            f.close()

        # take DELTA backup and swap external_dir_2 and external_dir_1
        # in external_dir_str
        self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta',
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir_2,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir_1)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # Restore Delta backup
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_merge_external_dir_string_order(self):
        """
        take FULL backup with not empty external directory
        drop external directory content
        take DELTA backup with the same external directory
        restore DELRA backup, check that restored
        external directory is empty
        """
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        core_dir = os.path.join(self.tmp_path, module_name, fname)
        shutil.rmtree(core_dir, ignore_errors=True)
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        external_dir_1 = self.get_tblspace_path(node, 'external_dir_1')
        external_dir_2 = self.get_tblspace_path(node, 'external_dir_2')

        # create empty file in external directory
        os.mkdir(external_dir_1)
        os.chmod(external_dir_1, 0o0700)
        with open(os.path.join(external_dir_1, 'fileA'), 'w+') as f:
            f.close()

        os.mkdir(external_dir_2)
        os.chmod(external_dir_2, 0o0700)
        with open(os.path.join(external_dir_2, 'fileZ'), 'w+') as f:
            f.close()

        # FULL backup with external directory
        self.backup_node(
            backup_dir, 'node', node,
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir_1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir_2)])

        with open(os.path.join(external_dir_1, 'fileB'), 'w+') as f:
            f.close()

        with open(os.path.join(external_dir_2, 'fileY'), 'w+') as f:
            f.close()

        # take DELTA backup and swap external_dir_2 and external_dir_1
        # in external_dir_str
        backup_id = self.backup_node(
            backup_dir, 'node', node,
            backup_type='delta',
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir_2,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir_1)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # Merge backups
        self.merge_backup(backup_dir, 'node', backup_id=backup_id)

        # Restore
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        self.restore_node(backup_dir, 'node', node)

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_smart_restore_externals(self):
        """
        make node, create database, take full backup with externals,
        take incremental backup without externals and restore it,
        make sure that files from externals are not copied during restore
        https://github.com/postgrespro/pg_probackup/issues/63
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

        # fill external directories with data
        tmp_id = self.backup_node(backup_dir, 'node', node)

        external_dir_1 = self.get_tblspace_path(node, 'external_dir_1')
        external_dir_2 = self.get_tblspace_path(node, 'external_dir_2')

        self.restore_node(
            backup_dir, 'node', node, backup_id=tmp_id,
            data_dir=external_dir_1, options=["-j", "4"])

        self.restore_node(
            backup_dir, 'node', node, backup_id=tmp_id,
            data_dir=external_dir_2, options=["-j", "4"])

        self.delete_pb(backup_dir, 'node', backup_id=tmp_id)

        # create database
        node.safe_psql(
            "postgres",
            "CREATE DATABASE testdb")

        # take FULL backup
        full_id = self.backup_node(backup_dir, 'node', node)

        # drop database
        node.safe_psql(
            "postgres",
            "DROP DATABASE testdb")

        # take PAGE backup
        page_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page')

        # restore PAGE backup
        node.cleanup()
        self.restore_node(
            backup_dir, 'node', node, backup_id=page_id,
            options=['--no-validate', '--log-level-file=VERBOSE'])

        logfile = os.path.join(backup_dir, 'log', 'pg_probackup.log')
        with open(logfile, 'r') as f:
            logfile_content = f.read()

        # get delta between FULL and PAGE filelists
        filelist_full = self.get_backup_filelist(
            backup_dir, 'node', full_id)

        filelist_page = self.get_backup_filelist(
            backup_dir, 'node', page_id)

        filelist_diff = self.get_backup_filelist_diff(
            filelist_full, filelist_page)

        for file in filelist_diff:
            self.assertNotIn(file, logfile_content)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_external_validation(self):
        """
        make node, create database,
        take full backup with external directory,
        corrupt external file in backup,
        run validate which should fail
        """
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        # take temp FULL backup
        tmp_id = self.backup_node(
            backup_dir, 'node', node, options=['--stream'])

        external_dir = self.get_tblspace_path(node, 'external_dir')

        # fill external directories with data
        self.restore_node(
            backup_dir, 'node', node, backup_id=tmp_id,
            data_dir=external_dir, options=["-j", "4"])

        self.delete_pb(backup_dir, 'node', backup_id=tmp_id)

        # take FULL backup
        full_id = self.backup_node(
            backup_dir, 'node', node,
            options=[
                '--stream', '-E', "{0}".format(external_dir)])

        # Corrupt file
        file = os.path.join(
            backup_dir, 'backups', 'node', full_id,
            'external_directories', 'externaldir1', 'postgresql.auto.conf')

        with open(file, "r+b", 0) as f:
            f.seek(42)
            f.write(b"blah")
            f.flush()
            f.close

        try:
            self.validate_pb(backup_dir)
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1, 0,
                "Expecting Error because file in external dir is corrupted"
                "\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd))
        except ProbackupException as e:
            self.assertIn(
                'WARNING: Invalid CRC of backup file',
                e.message,
                '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                    repr(e.message), self.cmd))

        self.assertEqual(
            'CORRUPT',
            self.show_pb(backup_dir, 'node', full_id)['status'],
            'Backup STATUS should be "CORRUPT"')

        # Clean after yourself
        self.del_test_dir(module_name, fname)
