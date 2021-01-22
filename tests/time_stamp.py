import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException
import subprocess
from time import sleep


module_name = 'time_stamp'

class TimeStamp(ProbackupTest, unittest.TestCase):

    def test_start_time_format(self):
        """Test backup ID changing after start-time editing in backup.control.
        We should convert local time in UTC format"""
        # Create simple node
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'])


        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.start()

        backup_id = self.backup_node(backup_dir, 'node', node, options=['--stream', '-j 2'])
        show_backup = self.show_pb(backup_dir, 'node')

        i = 0
        while i < 2:
            with open(os.path.join(backup_dir, "backups", "node", backup_id, "backup.control"), "r+") as f:
                output = ""
                for line in f:
                    if line.startswith('start-time') is True:
                        if i == 0:
                            output = output + str(line[:-5])+'+00\''+'\n'
                        else:
                            output = output + str(line[:-5]) + '\'' + '\n'
                    else:
                        output = output + str(line)
                f.close()

            with open(os.path.join(backup_dir, "backups", "node", backup_id, "backup.control"), "w") as fw:
                fw.write(output)
                fw.flush()
            show_backup = show_backup + self.show_pb(backup_dir, 'node')
            i += 1

        print(show_backup[1]['id'])
        print(show_backup[2]['id'])

        self.assertTrue(show_backup[1]['id'] == show_backup[2]['id'], "ERROR: Localtime format using instead of UTC")

        output = self.show_pb(backup_dir, as_json=False, as_text=True)
        self.assertNotIn("backup ID in control file", output)

        node.stop()
        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_server_date_style(self):
        """Issue #112"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'],
            pg_options={"datestyle": "GERMAN, DMY"})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.start()

        self.backup_node(
            backup_dir, 'node', node, options=['--stream', '-j 2'])
        
        # Clean after yourself
        self.del_test_dir(module_name, fname)

    def test_handling_of_TZ_env_variable(self):
        """Issue #284"""
        fname = self.id().split('.')[3]
        node = self.make_simple_node(
            base_dir="{0}/{1}/node".format(module_name, fname),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.start()

        my_env = os.environ.copy()
        my_env["TZ"] = "America/Detroit"

        self.backup_node(
            backup_dir, 'node', node, options=['--stream', '-j 2'], env=my_env)

        output = self.show_pb(backup_dir, 'node', as_json=False, as_text=True, env=my_env)

        self.assertNotIn("backup ID in control file", output)

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_dst_timezone_handling(self):
        """for manual testing"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        print(subprocess.Popen(
            ['sudo', 'timedatectl', 'set-timezone', 'America/Detroit'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate())

        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-ntp', 'false'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-time', '2020-05-25 12:00:00'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        # FULL
        output = self.backup_node(backup_dir, 'node', node, return_id=False)
        self.assertNotIn("backup ID in control file", output)

        # move to dst
        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-time', '2020-10-25 12:00:00'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        # DELTA
        output = self.backup_node(
            backup_dir, 'node', node, backup_type='delta', return_id=False)
        self.assertNotIn("backup ID in control file", output)

        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-time', '2020-12-01 12:00:00'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        # DELTA
        self.backup_node(backup_dir, 'node', node, backup_type='delta')

        output = self.show_pb(backup_dir, as_json=False, as_text=True)
        self.assertNotIn("backup ID in control file", output)

        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-ntp', 'true'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        sleep(10)

        self.backup_node(backup_dir, 'node', node, backup_type='delta')

        output = self.show_pb(backup_dir, as_json=False, as_text=True)
        self.assertNotIn("backup ID in control file", output)

        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-timezone', 'US/Moscow'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        # Clean after yourself
        self.del_test_dir(module_name, fname)

    @unittest.skip("skip")
    def test_dst_timezone_handling_backward_compatibilty(self):
        """for manual testing"""
        fname = self.id().split('.')[3]
        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(module_name, fname, 'node'),
            initdb_params=['--data-checksums'],
            pg_options={'autovacuum': 'off'})

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-timezone', 'America/Detroit'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-ntp', 'false'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-time', '2020-05-25 12:00:00'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        # FULL
        self.backup_node(backup_dir, 'node', node, old_binary=True, return_id=False)

        # move to dst
        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-time', '2020-10-25 12:00:00'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        # DELTA
        output = self.backup_node(
            backup_dir, 'node', node, backup_type='delta', old_binary=True, return_id=False)

        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-time', '2020-12-01 12:00:00'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        # DELTA
        self.backup_node(backup_dir, 'node', node, backup_type='delta')

        output = self.show_pb(backup_dir, as_json=False, as_text=True)
        self.assertNotIn("backup ID in control file", output)

        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-ntp', 'true'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        sleep(10)

        self.backup_node(backup_dir, 'node', node, backup_type='delta')

        output = self.show_pb(backup_dir, as_json=False, as_text=True)
        self.assertNotIn("backup ID in control file", output)

        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-timezone', 'US/Moscow'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        # Clean after yourself
        self.del_test_dir(module_name, fname)
