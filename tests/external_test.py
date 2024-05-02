import unittest
import os
from time import sleep
from .helpers.ptrack_helpers import ProbackupTest, fs_backup_class
import shutil


# TODO: add some ptrack tests
class ExternalTest(ProbackupTest):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_basic_external(self):
        """
        make node, create external directory, take backup
        with external directory, restore backup, check that
        external directory was successfully copied
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        external_dir = self.get_tblspace_path(node, 'somedirectory')

        # create directory in external_directory
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # take FULL backup with external directory pointing to a file
        file_path = os.path.join(self.test_path, 'file')
        with open(file_path, "w+") as f:
            pass

        self.pb.backup_node('node', node, backup_type="full",
                         options=[
                             '--external-dirs={0}'.format(file_path)],
                         expect_error="because external dir point to a file")
        self.assertMessage(contains='ERROR:  --external-dirs option')
        self.assertMessage(contains='directory or symbolic link expected')

        sleep(1)

        # FULL backup
        self.pb.backup_node('node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        # Fill external directories
        self.pb.restore_node('node', restore_dir=external_dir, options=["-j", "4"])

        # Full backup with external dir
        self.pb.backup_node('node', node,
            options=[
                '--external-dirs={0}'.format(external_dir)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        self.pb.restore_node('node', node=node, options=["-j", "4"])

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_external_none(self):
        """
        make node, create external directory, take backup
        with external directory, take delta backup with --external-dirs=none,
        restore delta backup, check that
        external directory was not copied
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        external_dir = self.get_tblspace_path(node, 'somedirectory')

        # create directory in external_directory
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # FULL backup
        self.pb.backup_node('node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        # Fill external directories with data
        self.pb.restore_node('node', restore_dir=external_dir, options=["-j", "4"])

        # Full backup with external dir
        self.pb.backup_node('node', node,
            options=[
                '--stream',
                '--external-dirs={0}'.format(external_dir)])

        # Delta backup without external directory
        self.pb.backup_node('node', node, backup_type="delta",
            options=['--external-dirs=none', '--stream'])

        shutil.rmtree(external_dir, ignore_errors=True)
        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        self.pb.restore_node('node', node=node, options=["-j", "4"])

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_external_dirs_overlapping(self):
        """
        make node, create directory,
        take backup with two external directories pointing to
        the same directory, backup should fail
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # create directory in external_directory
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        os.mkdir(external_dir1)
        os.mkdir(external_dir2)

        # Full backup with external dirs
        self.pb.backup_node('node', node,
                         options=[
                             "-j", "4", "--stream",
                             "-E", "{0}{1}{2}{1}{0}".format(
                                 external_dir1,
                                 self.EXTERNAL_DIRECTORY_DELIMITER,
                                 external_dir2,
                                 self.EXTERNAL_DIRECTORY_DELIMITER,
                                 external_dir1)],
                         expect_error="because tablespace mapping is incorrect")
        self.assertMessage(regex=r'ERROR: External directory path \(-E option\) ".*" '
                                 r'contain another external directory')

    # @unittest.skip("skip")
    def test_external_dir_mapping(self):
        """
        make node, take full backup, check that restore with
        external-dir mapping will end with error, take page backup,
        check that restore with external-dir mapping will end with
        success
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        self.pb.backup_node('node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # Fill external directories with data
        self.pb.restore_node('node', restore_dir=external_dir1, options=["-j", "4"])

        self.pb.restore_node('node', restore_dir=external_dir2, options=["-j", "4"])

        node_restored = self.pg_node.make_simple('node_restored')
        node_restored.cleanup()

        external_dir1_new = self.get_tblspace_path(node_restored, 'external_dir1')
        external_dir2_new = self.get_tblspace_path(node_restored, 'external_dir2')

        self.pb.restore_node('node', node=node_restored,
                          options=[
                              "-j", "4",
                              "--external-mapping={0}={1}".format(
                                  external_dir1, external_dir1_new),
                              "--external-mapping={0}={1}".format(
                                  external_dir2, external_dir2_new)],
                          expect_error="because tablespace mapping is incorrect")
        self.assertMessage(contains=r"ERROR: --external-mapping option's old directory "
                                    r"doesn't have an entry in list of external directories")

        self.pb.backup_node('node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.pb.restore_node('node', node=node_restored,
            options=[
                "-j", "4",
                "--external-mapping={0}={1}".format(
                    external_dir1, external_dir1_new),
                "--external-mapping={0}={1}".format(
                    external_dir2, external_dir2_new)])

        pgdata_restored = self.pgdata_content(
            node_restored.base_dir, exclude_dirs=['logs'])
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_backup_multiple_external(self):
        """check that cmdline has priority over config"""
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # FULL backup
        self.pb.backup_node('node', node, backup_type="full",
            options=["-j", "4", "--stream"])

        # fill external directories with data
        self.pb.restore_node('node', restore_dir=external_dir1, options=["-j", "4"])

        self.pb.restore_node('node', restore_dir=external_dir2, options=["-j", "4"])

        self.pb.set_config('node',
            options=['-E', external_dir1])

        # cmdline option MUST override options in config
        self.pb.backup_node('node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", external_dir2])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs', 'external_dir1'])

        node.cleanup()
        shutil.rmtree(external_dir1, ignore_errors=True)
        shutil.rmtree(external_dir2, ignore_errors=True)

        self.pb.restore_node('node', node=node,
            options=["-j", "4"])

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])
        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_external_backward_compatibility(self):
        """
        take backup with old binary without external dirs support
        take delta backup with new binary and 2 external directories
        restore delta backup, check that incremental chain
        restored correctly
        """
        if not self.probackup_old_path:
            self.skipTest("You must specify PGPROBACKUPBIN_OLD"
                          " for run this test")
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init(old_binary=True)
        self.pb.show()

        self.pb.add_instance('node', node, old_binary=True)
        self.pb.show()

        node.slow_start()

        node.pgbench_init(scale=3)

        # FULL backup with old binary without external dirs support
        self.pb.backup_node('node', node,
            old_binary=True, options=["-j", "4", "--stream"])

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # fill external directories with data
        self.pb.restore_node('node', external_dir1, options=["-j", "4"])

        self.pb.restore_node('node', external_dir2, options=["-j", "4"])

        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # FULL backup
        backup_id = self.pb.backup_node('node', node,
            old_binary=True, options=["-j", "4", "--stream"])

        # fill external directories with changed data
        shutil.rmtree(external_dir1, ignore_errors=True)
        shutil.rmtree(external_dir2, ignore_errors=True)

        self.pb.restore_node('node', external_dir1, options=["-j", "4"])

        self.pb.restore_node('node', external_dir2, options=["-j", "4"])

        self.pb.delete('node', backup_id=backup_id)

        # delta backup with external directories using new binary
        self.pb.backup_node('node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # RESTORE chain with new binary
        node_restored = self.pg_node.make_simple('node_restored')

        node_restored.cleanup()

        external_dir1_new = self.get_tblspace_path(node_restored, 'external_dir1')
        external_dir2_new = self.get_tblspace_path(node_restored, 'external_dir2')

        self.pb.restore_node('node', node_restored,
            options=[
                "-j", "4",
                "--external-mapping={0}={1}".format(external_dir1, external_dir1_new),
                "--external-mapping={0}={1}".format(external_dir2, external_dir2_new)])

        pgdata_restored = self.pgdata_content(
            node_restored.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    def test_external_backward_compatibility_merge_1(self):
        """
        take backup with old binary without external dirs support
        take delta backup with new binary and 2 external directories
        merge delta backup ajd restore it
        """
        if not self.probackup_old_path:
            self.skipTest("You must specify PGPROBACKUPBIN_OLD"
                          " for run this test")
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init(old_binary=True)
        self.pb.show()

        self.pb.add_instance('node', node, old_binary=True)
        self.pb.show()

        node.slow_start()

        node.pgbench_init(scale=3)

        # tmp FULL backup with old binary
        tmp_id = self.pb.backup_node('node', node,
            old_binary=True, options=["-j", "4", "--stream"])

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # fill external directories with data
        self.pb.restore_node('node', external_dir1, options=["-j", "4"])

        self.pb.restore_node('node', external_dir2, options=["-j", "4"])

        self.pb.delete('node', backup_id=tmp_id)

        # FULL backup with old binary without external dirs support
        self.pb.backup_node('node', node,
            old_binary=True, options=["-j", "4", "--stream"])

        pgbench = node.pgbench(options=['-T', '30', '-c', '1'])
        pgbench.wait()

        # delta backup with external directories using new binary
        backup_id = self.pb.backup_node('node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # Merge chain chain with new binary
        self.pb.merge_backup('node', backup_id=backup_id)

        # Restore merged backup
        node_restored = self.pg_node.make_simple('node_restored')

        node_restored.cleanup()

        external_dir1_new = self.get_tblspace_path(node_restored, 'external_dir1')
        external_dir2_new = self.get_tblspace_path(node_restored, 'external_dir2')

        self.pb.restore_node('node', node_restored,
            options=[
                "-j", "4",
                "--external-mapping={0}={1}".format(external_dir1, external_dir1_new),
                "--external-mapping={0}={1}".format(external_dir2, external_dir2_new)])

        pgdata_restored = self.pgdata_content(
            node_restored.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    def test_external_backward_compatibility_merge_2(self):
        """
        take backup with old binary without external dirs support
        take delta backup with new binary and 2 external directories
        merge delta backup and restore it
        """
        if not self.probackup_old_path:
            self.skipTest("You must specify PGPROBACKUPBIN_OLD"
                          " for run this test")
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init(old_binary=True)
        self.pb.show()

        self.pb.add_instance('node', node, old_binary=True)
        self.pb.show()

        node.slow_start()

        node.pgbench_init(scale=3)

        # tmp FULL backup with old binary
        tmp_id = self.pb.backup_node('node', node,
            old_binary=True, options=["-j", "4", "--stream"])

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # fill external directories with data
        self.pb.restore_node('node', external_dir1, options=["-j", "4"])

        self.pb.restore_node('node', external_dir2, options=["-j", "4"])

        self.pb.delete('node', backup_id=tmp_id)

        # FULL backup with old binary without external dirs support
        self.pb.backup_node('node', node,
            old_binary=True, options=["-j", "4", "--stream"])

        pgbench = node.pgbench(options=['-T', '30', '-c', '1'])
        pgbench.wait()

        # delta backup with external directories using new binary
        self.pb.backup_node('node', node,
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

        self.pb.restore_node('node', external_dir1,
            options=['-j', '4', '--skip-external-dirs'])

        self.pb.restore_node('node', external_dir2,
            options=['-j', '4', '--skip-external-dirs'])

        # delta backup without external directories using old binary
        backup_id = self.pb.backup_node('node', node,
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
        self.pb.merge_backup('node', backup_id=backup_id)

        # Restore merged backup
        node_restored = self.pg_node.make_simple('node_restored')

        node_restored.cleanup()

        external_dir1_new = self.get_tblspace_path(
            node_restored, 'external_dir1')
        external_dir2_new = self.get_tblspace_path(
            node_restored, 'external_dir2')

        self.pb.restore_node('node', node_restored,
            options=[
                "-j", "4",
                "--external-mapping={0}={1}".format(
                    external_dir1, external_dir1_new),
                "--external-mapping={0}={1}".format(
                    external_dir2, external_dir2_new)])

        pgdata_restored = self.pgdata_content(
            node_restored.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    def test_external_merge(self):
        """"""
        if not self.probackup_old_path:
            self.skipTest("You must specify PGPROBACKUPBIN_OLD"
                          " for run this test")
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node, old_binary=True)
        node.slow_start()

        node.pgbench_init(scale=3)

        # take temp FULL backup
        tmp_id = self.pb.backup_node('node', node, options=["-j", "4", "--stream"])

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        self.create_tblspace_in_node(node, 'tblsp_1')
        node.safe_psql(
            "postgres",
            "create table t_heap_lame tablespace tblsp_1 "
            "as select 1 as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(0,100) i")

        # fill external directories with data
        self.pb.restore_node('node', external_dir1, backup_id=tmp_id,
            options=["-j", "4"])

        self.pb.restore_node('node', external_dir2, backup_id=tmp_id,
            options=["-j", "4"])

        self.pb.delete('node', backup_id=tmp_id)

        # FULL backup with old binary without external dirs support
        self.pb.backup_node('node', node,
            old_binary=True, options=["-j", "4", "--stream"])

        # change data a bit
        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # delta backup with external directories using new binary
        backup_id = self.pb.backup_node('node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        print(self.pb.show('node', as_json=False, as_text=True))

        # Merge
        print(self.pb.merge_backup('node', backup_id=backup_id,
            options=['--log-level-file=VERBOSE']))

        # RESTORE
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        external_dir1_new = self.get_tblspace_path(node, 'external_dir1')
        external_dir2_new = self.get_tblspace_path(node, 'external_dir2')

        self.pb.restore_node('node', node,
            options=[
                "-j", "4",
                "--external-mapping={0}={1}".format(
                    external_dir1, external_dir1_new),
                "--external-mapping={0}={1}".format(
                    external_dir2, external_dir2_new)])

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_external_merge_skip_external_dirs(self):
        """"""
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        # FULL backup with old data
        tmp_id = self.pb.backup_node('node', node, options=["-j", "4", "--stream"])

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # fill external directories with old data
        self.pb.restore_node('node', restore_dir=external_dir1, backup_id=tmp_id,
            options=["-j", "4"])

        self.pb.restore_node('node', restore_dir=external_dir2, backup_id=tmp_id,
            options=["-j", "4"])

        self.pb.delete('node', backup_id=tmp_id)

        # change data a bit
        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # FULL backup with external directories
        self.pb.backup_node('node', node,
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
        self.pb.restore_node('node', restore_dir=external_dir1,
            options=["-j", "4", "--skip-external-dirs"])

        self.pb.restore_node('node', restore_dir=external_dir2,
            options=["-j", "4", "--skip-external-dirs"])

        # DELTA backup with external directories
        backup_id = self.pb.backup_node('node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # merge backups without external directories
        self.pb.merge_backup('node',
            backup_id=backup_id, options=['--skip-external-dirs'])

        # RESTORE
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        self.pb.restore_node('node', node=node,
            options=["-j", "4"])

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    def test_external_merge_1(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        # FULL backup
        self.pb.backup_node('node', node, options=["-j", "4", "--stream"])

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # FULL backup  with changed data
        backup_id = self.pb.backup_node('node', node,
            options=["-j", "4", "--stream"])

        # fill external directories with changed data
        self.pb.restore_node('node', restore_dir=external_dir1, options=["-j", "4"])

        self.pb.restore_node('node', restore_dir=external_dir2, options=["-j", "4"])

        self.pb.delete('node', backup_id=backup_id)

        # delta backup with external directories using new binary
        backup_id = self.pb.backup_node('node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        self.pb.merge_backup('node', backup_id=backup_id)

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # RESTORE
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        external_dir1_new = self.get_tblspace_path(node, 'external_dir1')
        external_dir2_new = self.get_tblspace_path(node, 'external_dir2')

        self.pb.restore_node('node', node=node,
            options=[
                "-j", "4",
                "--external-mapping={0}={1}".format(external_dir1, external_dir1_new),
                "--external-mapping={0}={1}".format(external_dir2, external_dir2_new)])

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_external_merge_3(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        # FULL backup
        self.pb.backup_node('node', node, options=["-j", "4"])

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # FULL backup
        backup_id = self.pb.backup_node('node', node)

        # fill external directories with changed data
        self.pb.restore_node('node', restore_dir=external_dir1)

        self.pb.restore_node('node', restore_dir=external_dir2)

        self.pb.delete('node', backup_id=backup_id)

        # page backup with external directories
        self.pb.backup_node('node', node, backup_type="page",
            options=[
                "-j", "4",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        # page backup with external directories
        backup_id = self.pb.backup_node('node', node, backup_type="page",
            options=[
                "-j", "4",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.pb.merge_backup('node', backup_id=backup_id,
            options=['--log-level-file=verbose'])

        # RESTORE
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        external_dir1_new = self.get_tblspace_path(node, 'external_dir1')
        external_dir2_new = self.get_tblspace_path(node, 'external_dir2')

        self.pb.restore_node('node', node,
            options=[
                "-j", "4",
                "--external-mapping={0}={1}".format(
                    external_dir1, external_dir1_new),
                "--external-mapping={0}={1}".format(
                    external_dir2, external_dir2_new)])

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    def test_external_merge_2(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.pgbench_init(scale=3)

        # FULL backup
        self.pb.backup_node('node', node, options=["-j", "4", "--stream"])

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # FULL backup
        backup_id = self.pb.backup_node('node', node,
            options=["-j", "4", "--stream"])

        # fill external directories with changed data
        self.pb.restore_node('node', restore_dir=external_dir1, options=["-j", "4"])

        self.pb.restore_node('node', restore_dir=external_dir2, options=["-j", "4"])

        self.pb.delete('node', backup_id=backup_id)

        # delta backup with external directories
        self.pb.backup_node('node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        # delta backup with external directories
        backup_id = self.pb.backup_node('node', node, backup_type="delta",
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
        self.pb.merge_backup('node', backup_id=backup_id)

        # RESTORE
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        external_dir1_new = self.get_tblspace_path(node, 'external_dir1')
        external_dir2_new = self.get_tblspace_path(node, 'external_dir2')

        self.pb.restore_node('node', node=node,
            options=[
                "-j", "4",
                "--external-mapping={0}={1}".format(external_dir1, external_dir1_new),
                "--external-mapping={0}={1}".format(external_dir2, external_dir2_new)])

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_restore_external_changed_data(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.pgbench_init(scale=2)

        # set externals
        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # FULL backup
        tmp_id = self.pb.backup_node('node',
            node, options=["-j", "4", "--stream"])

        # fill external directories with data
        self.pb.restore_node('node', restore_dir=external_dir1, options=["-j", "4"])

        self.pb.restore_node('node', restore_dir=external_dir2, options=["-j", "4"])

        self.pb.delete('node', backup_id=tmp_id)

        # change data a bit
        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # FULL backup
        backup_id = self.pb.backup_node('node', node,
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        # fill external directories with changed data
        shutil.rmtree(external_dir1, ignore_errors=True)
        shutil.rmtree(external_dir2, ignore_errors=True)

        self.pb.restore_node('node', restore_dir=external_dir1, backup_id=backup_id,
            options=["-j", "4", "--skip-external-dirs"])

        self.pb.restore_node('node', restore_dir=external_dir2, backup_id=backup_id,
            options=["-j", "4", "--skip-external-dirs"])

        # change data a bit more
        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Delta backup with external directories
        self.pb.backup_node('node', node, backup_type="delta",
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

        self.pb.restore_node('node', node,
            options=["-j", "4"])

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_restore_external_changed_data_1(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'max_wal_size': '32MB'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.pgbench_init(scale=1)

        # set externals
        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # FULL backup
        tmp_id = self.pb.backup_node('node',
            node, options=["-j", "4", "--stream"])

        # fill external directories with data
        self.pb.restore_node('node', restore_dir=external_dir1, options=["-j", "4"])

        self.pb.restore_node('node', restore_dir=external_dir2, options=["-j", "4"])

        self.pb.delete('node', backup_id=tmp_id)

        # change data a bit
        pgbench = node.pgbench(options=['-T', '5', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # FULL backup
        backup_id = self.pb.backup_node('node', node,
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        # fill external directories with changed data
        shutil.rmtree(external_dir1, ignore_errors=True)
        shutil.rmtree(external_dir2, ignore_errors=True)

        self.pb.restore_node('node', restore_dir=external_dir1, backup_id=backup_id,
            options=["-j", "4", "--skip-external-dirs"])

        self.pb.restore_node('node', restore_dir=external_dir2, backup_id=backup_id,
            options=["-j", "4", "--skip-external-dirs"])

        # change data a bit more
        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Delta backup with only one external directory
        self.pb.backup_node('node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", external_dir1])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs', 'external_dir2'])

        # Restore
        node.stop()
        shutil.rmtree(node.base_dir)

        # create empty file in external_dir2
        os.makedirs(external_dir2)
        with open(os.path.join(external_dir2, 'file'), 'w+') as f:
            f.close()

        output = self.pb.restore_node('node', node=node,
            options=["-j", "4"])

        self.assertNotIn(
            'externaldir2',
            output)

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs', 'external_dir2'])

        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    def test_merge_external_changed_data(self):
        """"""
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={
                'max_wal_size': '32MB'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.pgbench_init(scale=2)

        # set externals
        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # FULL backup
        tmp_id = self.pb.backup_node('node',
            node, options=["-j", "4", "--stream"])

        # fill external directories with data
        self.pb.restore_node('node', restore_dir=external_dir1, options=["-j", "4"])

        self.pb.restore_node('node', restore_dir=external_dir2, options=["-j", "4"])

        self.pb.delete('node', backup_id=tmp_id)

        # change data a bit
        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # FULL backup
        backup_id = self.pb.backup_node('node', node,
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        # fill external directories with changed data
        shutil.rmtree(external_dir1, ignore_errors=True)
        shutil.rmtree(external_dir2, ignore_errors=True)

        self.pb.restore_node('node', restore_dir=external_dir1, backup_id=backup_id,
            options=["-j", "4", "--skip-external-dirs"])

        self.pb.restore_node('node', restore_dir=external_dir2, backup_id=backup_id,
            options=["-j", "4", "--skip-external-dirs"])

        # change data a bit more
        pgbench = node.pgbench(options=['-T', '30', '-c', '1', '--no-vacuum'])
        pgbench.wait()

        # Delta backup with external directories
        backup_id = self.pb.backup_node('node', node, backup_type="delta",
            options=[
                "-j", "4", "--stream",
                "-E", "{0}{1}{2}".format(
                    external_dir1,
                    self.EXTERNAL_DIRECTORY_DELIMITER,
                    external_dir2)])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # Merge
        self.pb.merge_backup('node', backup_id)

        # Restore
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        self.pb.restore_node('node', node=node,
            options=["-j", "4"])

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_restore_skip_external(self):
        """
        Check that --skip-external-dirs works correctly
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        external_dir1 = self.get_tblspace_path(node, 'external_dir1')
        external_dir2 = self.get_tblspace_path(node, 'external_dir2')

        # temp FULL backup
        backup_id = self.pb.backup_node('node', node, options=["-j", "4", "--stream"])

        # fill external directories with data
        self.pb.restore_node('node', restore_dir=external_dir1, options=["-j", "4"])

        self.pb.restore_node('node', restore_dir=external_dir2, options=["-j", "4"])

        self.pb.delete('node', backup_id=backup_id)

        # FULL backup with external directories
        self.pb.backup_node('node', node,
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

        self.pb.restore_node('node', node=node,
            options=[
                "-j", "4", "--skip-external-dirs"])

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_external_dir_is_symlink(self):
        """
        Check that backup works correctly if external dir is symlink,
        symlink pointing to external dir should be followed,
        but restored as directory
        """
        if os.name == 'nt':
            self.skipTest('Skipped for Windows')

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        external_dir = self.get_tblspace_path(node, 'external_dir')

        # temp FULL backup
        backup_id = self.pb.backup_node('node', node, options=["-j", "4", "--stream"])

        # fill some directory with data
        symlinked_dir = os.path.join(self.test_path, 'symlinked')

        self.pb.restore_node('node', restore_dir=symlinked_dir, options=["-j", "4"])

        # drop temp FULL backup
        self.pb.delete('node', backup_id=backup_id)

        # create symlink to directory in external directory
        os.symlink(symlinked_dir, external_dir)

        # FULL backup with external directories
        backup_id = self.pb.backup_node('node', node,
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        node_restored = self.pg_node.make_simple('node_restored')

        # RESTORE
        node_restored.cleanup()

        external_dir_new = self.get_tblspace_path(
            node_restored, 'external_dir')

        self.pb.restore_node('node', node=node_restored,
            options=[
                "-j", "4", "--external-mapping={0}={1}".format(
                    external_dir, external_dir_new)])

        pgdata_restored = self.pgdata_content(
            node_restored.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

        self.assertEqual(
            external_dir,
            self.pb.show('node',
                backup_id=backup_id)['external-dirs'])

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_external_dir_contain_symlink_on_dir(self):
        """
        Check that backup works correctly if external dir is symlink,
        symlink pointing to external dir should be followed,
        but restored as directory
        """
        if os.name == 'nt':
            self.skipTest('Skipped for Windows')

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        external_dir = self.get_tblspace_path(node, 'external_dir')
        dir_in_external_dir = os.path.join(external_dir, 'dir')

        # temp FULL backup
        backup_id = self.pb.backup_node('node', node, options=["-j", "4", "--stream"])

        # fill some directory with data
        symlinked_dir = os.path.join(self.test_path, 'symlinked')

        self.pb.restore_node('node', restore_dir=symlinked_dir, options=["-j", "4"])

        # drop temp FULL backup
        self.pb.delete('node', backup_id=backup_id)

        # create symlink to directory in external directory
        os.mkdir(external_dir)
        os.symlink(symlinked_dir, dir_in_external_dir)

        # FULL backup with external directories
        backup_id = self.pb.backup_node('node', node=node,
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        node_restored = self.pg_node.make_simple('node_restored')

        # RESTORE
        node_restored.cleanup()

        external_dir_new = self.get_tblspace_path(
            node_restored, 'external_dir')

        self.pb.restore_node('node', node=node_restored,
            options=[
                "-j", "4", "--external-mapping={0}={1}".format(
                    external_dir, external_dir_new)])

        pgdata_restored = self.pgdata_content(
            node_restored.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

        self.assertEqual(
            external_dir,
            self.pb.show('node',
                backup_id=backup_id)['external-dirs'])

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_external_dir_contain_symlink_on_file(self):
        """
        Check that backup works correctly if external dir is symlink,
        symlink pointing to external dir should be followed,
        but restored as directory
        """
        if os.name == 'nt':
            self.skipTest('Skipped for Windows')

        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        external_dir = self.get_tblspace_path(node, 'external_dir')
        file_in_external_dir = os.path.join(external_dir, 'file')

        # temp FULL backup
        backup_id = self.pb.backup_node('node', node, options=["-j", "4", "--stream"])

        # fill some directory with data
        symlinked_dir = os.path.join(self.test_path, 'symlinked')

        self.pb.restore_node('node', restore_dir=symlinked_dir, options=["-j", "4"])

        # drop temp FULL backup
        self.pb.delete('node', backup_id=backup_id)

        # create symlink to directory in external directory
        src_file = os.path.join(symlinked_dir, 'postgresql.conf')
        os.mkdir(external_dir)
        os.chmod(external_dir, 0o0700)
        os.symlink(src_file, file_in_external_dir)

        # FULL backup with external directories
        backup_id = self.pb.backup_node('node', node,
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        node_restored = self.pg_node.make_simple('node_restored')

        # RESTORE
        node_restored.cleanup()

        external_dir_new = self.get_tblspace_path(
            node_restored, 'external_dir')

        self.pb.restore_node('node', node=node_restored,
            options=[
                "-j", "4", "--external-mapping={0}={1}".format(
                    external_dir, external_dir_new)])

        pgdata_restored = self.pgdata_content(
            node_restored.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

        self.assertEqual(
            external_dir,
            self.pb.show('node',
                backup_id=backup_id)['external-dirs'])

    # @unittest.expectedFailure
    # @unittest.skip("skip")
    def test_external_dir_is_tablespace(self):
        """
        Check that backup fails with error
        if external directory points to tablespace
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        external_dir = self.get_tblspace_path(node, 'external_dir')

        self.create_tblspace_in_node(
            node, 'tblspace1', tblspc_path=external_dir)

        node.pgbench_init(scale=1, tablespace='tblspace1')

        # FULL backup with external directories
        self.pb.backup_node('node', node,
                         options=["-j", "4", "--stream", "-E", external_dir],
                         expect_error="because external dir points to the tablespace")
        self.assertMessage(contains='External directory path (-E option)')

    def test_restore_external_dir_not_empty(self):
        """
        Check that backup fails with error
        if external directory point to not empty tablespace and
        if remapped directory also isn`t empty
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        external_dir = self.get_tblspace_path(node, 'external_dir')

        # create empty file in external directory
        # open(os.path.join(external_dir, 'file'), 'a').close()
        os.mkdir(external_dir)
        with open(os.path.join(external_dir, 'file'), 'w+') as f:
            f.close()

        # FULL backup with external directory
        self.pb.backup_node('node', node,
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        node.stop()
        shutil.rmtree(node.data_dir)

        self.pb.restore_node('node', node=node,
                          expect_error="because external dir is not empty")
        self.assertMessage(contains='External directory is not empty')

        external_dir_new = self.get_tblspace_path(node, 'external_dir_new')

        # create empty file in directory, which will be a target of
        # remapping
        os.mkdir(external_dir_new)
        with open(os.path.join(external_dir_new, 'file1'), 'w+') as f:
            f.close()

        self.pb.restore_node('node', node=node,
                          options=[f'--external-mapping',
                                   f'{external_dir}={external_dir_new}'],
                          expect_error="because remapped external dir is not empty")
        self.assertMessage(contains='External directory is not empty')

    def test_restore_external_dir_is_missing(self):
        """
        take FULL backup with not empty external directory
        delete external directory
        take DELTA backup with external directory, which
        should fail
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        external_dir = self.get_tblspace_path(node, 'external_dir')

        # create empty file in external directory
        # open(os.path.join(external_dir, 'file'), 'a').close()
        os.mkdir(external_dir)
        with open(os.path.join(external_dir, 'file'), 'w+') as f:
            f.close()

        # FULL backup with external directory
        self.pb.backup_node('node', node,
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        # drop external directory
        shutil.rmtree(external_dir, ignore_errors=True)

        self.pb.backup_node('node', node, backup_type='delta',
                         options=["-j", "4", "--stream", "-E", external_dir],
                         expect_error="because external dir is missing")
        self.assertMessage(contains='ERROR: External directory is not found:')

        sleep(1)

        # take DELTA without external directories
        self.pb.backup_node('node', node,
            backup_type='delta',
            options=["-j", "4", "--stream"])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # Restore Delta backup
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])
        self.compare_pgdata(pgdata, pgdata_restored)

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
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        external_dir = self.get_tblspace_path(node, 'external_dir')

        # create empty file in external directory
        # open(os.path.join(external_dir, 'file'), 'a').close()
        os.mkdir(external_dir)
        with open(os.path.join(external_dir, 'file'), 'w+') as f:
            f.close()

        # FULL backup with external directory
        self.pb.backup_node('node', node,
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        # drop external directory
        shutil.rmtree(external_dir, ignore_errors=True)

        self.pb.backup_node('node', node, backup_type='delta',
                         options=["-j", "4", "--stream", "-E", external_dir],
                         expect_error="because external dir is missing")
        self.assertMessage(contains='ERROR: External directory is not found:')

        sleep(1)

        # take DELTA without external directories
        backup_id = self.pb.backup_node('node', node,
            backup_type='delta',
            options=["-j", "4", "--stream"])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # Merge
        self.pb.merge_backup('node', backup_id=backup_id)

        # Restore
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])
        self.compare_pgdata(pgdata, pgdata_restored)

    def test_restore_external_dir_is_empty(self):
        """
        take FULL backup with not empty external directory
        drop external directory content
        take DELTA backup with the same external directory
        restore DELRA backup, check that restored
        external directory is empty
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        external_dir = self.get_tblspace_path(node, 'external_dir')

        # create empty file in external directory
        # open(os.path.join(external_dir, 'file'), 'a').close()
        os.mkdir(external_dir)
        os.chmod(external_dir, 0o0700)
        with open(os.path.join(external_dir, 'file'), 'w+') as f:
            f.close()

        # FULL backup with external directory
        self.pb.backup_node('node', node,
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        # make external directory empty
        os.remove(os.path.join(external_dir, 'file'))

        # take DELTA backup with empty external directory
        self.pb.backup_node('node', node,
            backup_type='delta',
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # Restore Delta backup
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])
        self.compare_pgdata(pgdata, pgdata_restored)

    def test_merge_external_dir_is_empty(self):
        """
        take FULL backup with not empty external directory
        drop external directory content
        take DELTA backup with the same external directory
        merge backups and restore FULL, check that restored
        external directory is empty
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        external_dir = self.get_tblspace_path(node, 'external_dir')

        # create empty file in external directory
        # open(os.path.join(external_dir, 'file'), 'a').close()
        os.mkdir(external_dir)
        os.chmod(external_dir, 0o0700)
        with open(os.path.join(external_dir, 'file'), 'w+') as f:
            f.close()

        # FULL backup with external directory
        self.pb.backup_node('node', node,
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        # make external directory empty
        os.remove(os.path.join(external_dir, 'file'))

        # take DELTA backup with empty external directory
        backup_id = self.pb.backup_node('node', node,
            backup_type='delta',
            options=[
                "-j", "4", "--stream",
                "-E", external_dir])

        pgdata = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        # Merge
        self.pb.merge_backup('node', backup_id=backup_id)

        # Restore
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])
        self.compare_pgdata(pgdata, pgdata_restored)

    def test_restore_external_dir_string_order(self):
        """
        take FULL backup with not empty external directory
        drop external directory content
        take DELTA backup with the same external directory
        restore DELRA backup, check that restored
        external directory is empty
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
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
        self.pb.backup_node('node', node,
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
        self.pb.backup_node('node', node,
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

        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

    def test_merge_external_dir_string_order(self):
        """
        take FULL backup with not empty external directory
        drop external directory content
        take DELTA backup with the same external directory
        restore DELRA backup, check that restored
        external directory is empty
        """
        backup_dir = self.backup_dir
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
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
        self.pb.backup_node('node', node,
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
        backup_id = self.pb.backup_node('node', node,
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
        self.pb.merge_backup('node', backup_id=backup_id)

        # Restore
        node.cleanup()
        shutil.rmtree(node.base_dir, ignore_errors=True)

        self.pb.restore_node('node', node=node)

        pgdata_restored = self.pgdata_content(
            node.base_dir, exclude_dirs=['logs'])

        self.compare_pgdata(pgdata, pgdata_restored)

    # @unittest.skip("skip")
    def test_smart_restore_externals(self):
        """
        make node, create database, take full backup with externals,
        take incremental backup without externals and restore it,
        make sure that files from externals are not copied during restore
        https://github.com/postgrespro/pg_probackup/issues/63
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.slow_start()

        # fill external directories with data
        tmp_id = self.pb.backup_node('node', node)

        external_dir_1 = self.get_tblspace_path(node, 'external_dir_1')
        external_dir_2 = self.get_tblspace_path(node, 'external_dir_2')

        self.pb.restore_node('node', restore_dir=external_dir_1, backup_id=tmp_id,
            options=["-j", "4"])

        self.pb.restore_node('node', restore_dir=external_dir_2, backup_id=tmp_id,
            options=["-j", "4"])

        self.pb.delete('node', backup_id=tmp_id)

        # create database
        node.safe_psql(
            "postgres",
            "CREATE DATABASE testdb")

        # take FULL backup
        full_id = self.pb.backup_node('node', node)

        # drop database
        node.safe_psql(
            "postgres",
            "DROP DATABASE testdb")

        # take PAGE backup
        page_id = self.pb.backup_node('node', node=node, backup_type='page')

        # restore PAGE backup
        node.cleanup()
        self.pb.restore_node('node', node=node, backup_id=page_id,
            options=['--no-validate', '--log-level-file=VERBOSE'])

        logfile_content = self.read_pb_log()

        # get delta between FULL and PAGE filelists
        filelist_full = self.get_backup_filelist(backup_dir, 'node', full_id)

        filelist_page = self.get_backup_filelist(backup_dir, 'node', page_id)

        filelist_diff = self.get_backup_filelist_diff(
            filelist_full, filelist_page)

        self.assertTrue(filelist_diff, 'There should be deleted files')
        for file in filelist_diff:
            self.assertNotIn(file, logfile_content)

    # @unittest.skip("skip")
    def test_external_validation(self):
        """
        make node, create database,
        take full backup with external directory,
        corrupt external file in backup,
        run validate which should fail
        """
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        # take temp FULL backup
        tmp_id = self.pb.backup_node('node', node, options=['--stream'])

        external_dir = self.get_tblspace_path(node, 'external_dir')

        # fill external directories with data
        self.pb.restore_node('node', restore_dir=external_dir, backup_id=tmp_id,
            options=["-j", "4"])

        self.pb.delete('node', backup_id=tmp_id)

        # take FULL backup
        full_id = self.pb.backup_node('node', node,
            options=[
                '--stream', '-E', "{0}".format(external_dir)])

        # Corrupt file
        file = os.path.join(
            backup_dir, 'backups', 'node', full_id,
            'external_directories', 'externaldir1', 'postgresql.auto.conf')

        to_corrupt = 'external_directories/externaldir1/postgresql.auto.conf'
        self.corrupt_backup_file(backup_dir, 'node', full_id, to_corrupt,
                                 damage=(42, b"blah"))

        self.pb.validate(
                         expect_error="because file in external dir is corrupted")
        self.assertMessage(contains='WARNING: Invalid CRC of backup file')

        self.assertEqual(
            'CORRUPT',
            self.pb.show('node', full_id)['status'],
            'Backup STATUS should be "CORRUPT"')
