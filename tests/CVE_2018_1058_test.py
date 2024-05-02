import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest

class CVE_2018_1058(ProbackupTest):

    # @unittest.skip("skip")
    def test_basic_default_search_path(self):
        """"""
        node = self.pg_node.make_simple('node', checksum=False, set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
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

        self.pb.backup_node('node', node, backup_type='full', options=['--stream'])

    # @unittest.skip("skip")
    def test_basic_backup_modified_search_path(self):
        """"""
        node = self.pg_node.make_simple('node', checksum=False, set_replication=True)
        node.set_auto_conf(options={'search_path': 'public,pg_catalog'})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        node.safe_psql(
            'postgres',
            "CREATE FUNCTION public.pg_control_checkpoint(OUT timeline_id integer, OUT dummy integer) "
            "RETURNS record "
            "AS $$ "
            "BEGIN "
            "  RAISE '% vulnerable!', 'pg_probackup'; "
            "END "
            "$$ LANGUAGE plpgsql")

        node.safe_psql(
            'postgres',
            "CREATE FUNCTION public.pg_proc(OUT proname name, OUT dummy integer) "
            "RETURNS record "
            "AS $$ "
            "BEGIN "
            "  RAISE '% vulnerable!', 'pg_probackup'; "
            "END "
            "$$ LANGUAGE plpgsql; "
            "CREATE VIEW public.pg_proc AS SELECT proname FROM public.pg_proc()")

        self.pb.backup_node('node', node, backup_type='full', options=['--stream'])

        log_file = os.path.join(node.logs_dir, 'postgresql.log')
        with open(log_file, 'r') as f:
            log_content = f.read()
            self.assertFalse(
                'pg_probackup vulnerable!' in log_content)

    # @unittest.skip("skip")
    def test_basic_checkdb_modified_search_path(self):
        """"""
        node = self.pg_node.make_simple('node')
        node.set_auto_conf(options={'search_path': 'public,pg_catalog'})
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

        self.pb.checkdb_node(
                options=[
                    '--amcheck',
                    '--skip-block-validation',
                    '-d', 'postgres', '-p', str(node.port)],
                expect_error="because amcheck{,_next} not installed")
        self.assertMessage(contains=
                "WARNING: Extension 'amcheck' or 'amcheck_next' are not installed in database postgres")
