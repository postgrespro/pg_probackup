import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest
import subprocess
from time import sleep


class TimeStamp(ProbackupTest):

    def test_start_time_format(self):
        """Test backup ID changing after start-time editing in backup.control.
        We should convert local time in UTC format"""
        # Create simple node
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
        node.start()

        backup_id = self.pb.backup_node('node', node, options=['--stream', '-j 2'])
        show_backup = self.pb.show('node')

        for i in range(2):
            with self.modify_backup_control(backup_dir, 'node', backup_id) as cf:
                lines = cf.data.splitlines(keepends=True)
                for j, line in enumerate(lines):
                    if not line.startswith('start-time'):
                        continue
                    if i == 0:
                        lines[j] = line[:-5] + "+00'\n"
                    else:
                        lines[j] = line[:-5] + "'\n"
                cf.data = "".join(lines)
            show_backup = show_backup + self.pb.show('node')

        print(show_backup[1]['id'])
        print(show_backup[2]['id'])

        self.assertTrue(show_backup[1]['id'] == show_backup[2]['id'], "ERROR: Localtime format using instead of UTC")

        output = self.pb.show(as_json=False, as_text=True)
        self.assertNotIn("backup ID in control file", output)

        node.stop()

    def test_server_date_style(self):
        """Issue #112"""
        node = self.pg_node.make_simple('node',
            set_replication=True,
            pg_options={"datestyle": "GERMAN, DMY"})

        self.pb.init()
        self.pb.add_instance('node', node)
        node.start()

        self.pb.backup_node('node', node, options=['--stream', '-j 2'])
        
    def test_handling_of_TZ_env_variable(self):
        """Issue #284"""
        node = self.pg_node.make_simple('node',
            set_replication=True)

        self.pb.init()
        self.pb.add_instance('node', node)
        node.start()

        my_env = os.environ.copy()
        my_env["TZ"] = "America/Detroit"

        self.pb.backup_node('node', node, options=['--stream', '-j 2'], env=my_env)

        output = self.pb.show('node', as_json=False, as_text=True, env=my_env)

        self.assertNotIn("backup ID in control file", output)

    @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_dst_timezone_handling(self):
        """for manual testing"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
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
        output = self.pb.backup_node('node', node, return_id=False)
        self.assertNotIn("backup ID in control file", output)

        # move to dst
        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-time', '2020-10-25 12:00:00'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        # DELTA
        output = self.pb.backup_node('node', node, backup_type='delta', return_id=False)
        self.assertNotIn("backup ID in control file", output)

        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-time', '2020-12-01 12:00:00'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        # DELTA
        self.pb.backup_node('node', node, backup_type='delta')

        output = self.pb.show(as_json=False, as_text=True)
        self.assertNotIn("backup ID in control file", output)

        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-ntp', 'true'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        sleep(10)

        self.pb.backup_node('node', node, backup_type='delta')

        output = self.pb.show(as_json=False, as_text=True)
        self.assertNotIn("backup ID in control file", output)

        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-timezone', 'US/Moscow'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

    @unittest.skip("skip")
    def test_dst_timezone_handling_backward_compatibilty(self):
        """for manual testing"""
        node = self.pg_node.make_simple('node')

        self.pb.init()
        self.pb.add_instance('node', node)
        self.pb.set_archiving('node', node)
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
        self.pb.backup_node('node', node, old_binary=True, return_id=False)

        # move to dst
        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-time', '2020-10-25 12:00:00'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        # DELTA
        output = self.pb.backup_node('node', node, backup_type='delta', old_binary=True, return_id=False)

        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-time', '2020-12-01 12:00:00'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        # DELTA
        self.pb.backup_node('node', node, backup_type='delta')

        output = self.pb.show(as_json=False, as_text=True)
        self.assertNotIn("backup ID in control file", output)

        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-ntp', 'true'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()

        sleep(10)

        self.pb.backup_node('node', node, backup_type='delta')

        output = self.pb.show(as_json=False, as_text=True)
        self.assertNotIn("backup ID in control file", output)

        subprocess.Popen(
            ['sudo', 'timedatectl', 'set-timezone', 'US/Moscow'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()
