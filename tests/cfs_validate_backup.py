import os
import unittest
import random

from .helpers.cfs_helpers import find_by_extensions, find_by_name, find_by_pattern, corrupt_file
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException

tblspace_name = 'cfs_tblspace'


class CfsValidateBackupNoenc(ProbackupTest, unittest.TestCase):

    def test_validate_fullbackup_empty_tablespace_after_delete_pg_compression(self):
        pass

    @unittest.skipUnless(ProbackupTest.enterprise, 'skip')
    # TODO replace all ProbackupTest._check_gdb_flag_or_skip_test() by this check
    @unittest.skipUnless(ProbackupTest.gdb, 'skip')
    # TODO rename
    def test_crash_on_postgres_cfs_gc_and_running_bad_backup_on_postgres_restart_before_postgres_cfs_recovery(self):

        # TODO extract to setUp()
        self.backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')

        self.node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(self.module_name, self.fname),
            set_replication=True, #TODO make set_replication=False?????
            initdb_params=['--data-checksums'],
            pg_options={
                #                'ptrack_enable': 'on',
                'cfs_encryption': 'off',
            }
        )

        self.init_pb(self.backup_dir)
        self.add_instance(self.backup_dir, 'node', self.node)
        self.set_archiving(self.backup_dir, 'node', self.node)

        self.node.slow_start()
        self.create_tblspace_in_node(self.node, tblspace_name, cfs=True)

        # TODO END OF EXTRACTION to setUp() method

        self.backup_id = None
        try:
            # TODO remove backup here?
            self.backup_id = self.backup_node(self.backup_dir, 'node', self.node, backup_type='full')
        except ProbackupException as e:
            self.fail(
                "ERROR: Full backup failed \n {0} \n {1}".format(
                    repr(self.cmd),
                    repr(e.message)
                )
            )

        self.node.safe_psql(
            "postgres",
            'CREATE TABLE {0} TABLESPACE {1} \
                AS SELECT i AS {0}_pkey, MD5(i::text) AS text, \
                MD5(repeat(i::text,10))::tsvector AS tsvector \
                FROM generate_series(0,1e4) i'.format('t1', tblspace_name)
                # FROM generate_series(0,1e5) i'.format('t1', tblspace_name)
        )

        cfs_gc_pid = self.node.safe_psql(
            "postgres",
            "SELECT pid "
            "FROM pg_stat_activity "
            "WHERE application_name LIKE 'CFS GC worker%'")

        self.verbose = True
        gdb = self.gdb_attach(cfs_gc_pid)
        gdb.set_breakpoint('cfs_gc_file')

        self.node.safe_psql(
            "postgres",
            # "WITH i AS (select generate_series(1, 1e5) AS i) "
            "WITH i AS (select generate_series(1, 1e4) AS i) "
            "UPDATE {0}"
            "   SET text = MD5(MD5(i::text))"
            "   FROM i"
            "   WHERE {0}_pkey = i"
            .format("t1")
        )

        gdb.continue_execution_until_break()
        gdb.remove_all_breakpoints()
        ## gdb.continue_execution_until_break()
        # gdb.continue_execution_until_running()
        ## gdb.continue_execution_until_exit()
        ## gdb.kill()
        gdb._execute("detach")
        self.node.cleanup()
        print("hurray ")


#class CfsValidateBackupNoenc(CfsValidateBackupNoenc):
#    os.environ["PG_CIPHER_KEY"] = "super_secret_cipher_key"
#    super(CfsValidateBackupNoenc).setUp()
