import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
from datetime import datetime, timedelta
import subprocess
from testgres import QueryException
import shutil
import sys
import time

module_name = 'CVE-2018-1058'

class CVE_2018_1058(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    def test_default_search_path(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'))

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            'postgres',
            "CREATE FUNCTION public.pgpro_edition() "
            "RETURNS text "
            "AS $$ "
            "BEGIN "
            "  RAISE 'pg_probackup vulnerable!'; "
            "END "
            "$$ LANGUAGE plpgsql")

        self.backup_node(backup_dir, 'node', node, backup_type='full', options=['--stream'])

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_backup_modified_search_path(self):
        """"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'))
        self.set_auto_conf(node, options={'search_path': 'public,pg_catalog'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        node.safe_psql(
            'postgres',
            "CREATE FUNCTION public.pg_control_checkpoint(OUT timeline_id integer, OUT dummy integer) "
            "RETURNS record "
            "AS $$ "
            "BEGIN "
            "  RAISE 'pg_probackup vulnerable!'; "
            "END "
            "$$ LANGUAGE plpgsql")

        node.safe_psql(
            'postgres',
            "CREATE FUNCTION public.pg_proc(OUT proname name, OUT dummy integer) "
            "RETURNS record "
            "AS $$ "
            "BEGIN "
            "  RAISE 'pg_probackup vulnerable!'; "
            "END "
            "$$ LANGUAGE plpgsql; "
            "CREATE VIEW public.pg_proc AS SELECT proname FROM public.pg_proc()")

        self.backup_node(backup_dir, 'node', node, backup_type='full', options=['--stream'])

        log_file = os.path.join(node.logs_dir, 'postgresql.log')
        with open(log_file, 'r') as f:
            log_content = f.read()
            self.assertFalse(
                'pg_probackup vulnerable!' in log_content)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    # @unittest.skip("skip")
    def test_checkdb_modified_search_path(self):
        """"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'])
        self.set_auto_conf(node, options={'search_path': 'public,pg_catalog'})
        node.slow_start()

        node.safe_psql(
            'postgres',
            "CREATE FUNCTION public.pg_database(OUT datname name, OUT oid oid, OUT dattablespace oid) "
            "RETURNS record "
            "AS $$ "
            "BEGIN "
            "  RAISE 'pg_probackup vulnerable!'; "
            "END "
            "$$ LANGUAGE plpgsql; "
            "CREATE VIEW public.pg_database AS SELECT * FROM public.pg_database()")
        
        node.safe_psql(
            'postgres',
            "CREATE FUNCTION public.pg_extension(OUT extname name, OUT extnamespace oid, OUT extversion text) "
            "RETURNS record "
            "AS $$ "
            "BEGIN "
            "  RAISE 'pg_probackup vulnerable!'; "
            "END "
            "$$ LANGUAGE plpgsql; "
            "CREATE FUNCTION public.pg_namespace(OUT oid oid, OUT nspname name) "
            "RETURNS record "
            "AS $$ "
            "BEGIN "
            "  RAISE 'pg_probackup vulnerable!'; "
            "END "
            "$$ LANGUAGE plpgsql; "
            "CREATE VIEW public.pg_extension AS SELECT * FROM public.pg_extension();"
            "CREATE VIEW public.pg_namespace AS SELECT * FROM public.pg_namespace();"
            )

        self.checkdb_node(
            options=[
                '--amcheck',
                '--skip-block-validation',
                '-d', 'postgres', '-p', str(node.port)])

        # Clean after yourself
        self.del_test_dir(module_name, fname)
