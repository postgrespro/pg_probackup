import os
import unittest
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException


module_name = 'time_stamp'

class CheckTimeStamp(ProbackupTest, unittest.TestCase):

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

        self.assertTrue(show_backup[1]['id'] == show_backup[2]['id'], "ERROR: Localtime format using instead of UTC")

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
            pg_options={"datestyle": "'GERMAN, DMY'"})

        backup_dir = os.path.join(self.tmp_path, module_name, fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.start()

        self.backup_node(
            backup_dir, 'node', node, options=['--stream', '-j 2'])
        
        # Clean after yourself
        self.del_test_dir(module_name, fname)
