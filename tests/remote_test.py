import unittest
import os
from time import sleep
from .helpers.ptrack_helpers import ProbackupTest, ProbackupException


class RemoteTest(ProbackupTest, unittest.TestCase):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_remote_sanity(self):
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        node.slow_start()

        output = self.backup_node(
            backup_dir, 'node', node,
            options=['--stream'], no_remote=True, return_id=False)
        self.assertIn('remote: false', output)

        # try:
        #     self.backup_node(
        #         backup_dir, 'node',
        #         node, options=['--remote-proto=ssh', '--stream'], no_remote=True)
        #     # we should die here because exception is what we expect to happen
        #     self.assertEqual(
        #         1, 0,
        #         "Expecting Error because remote-host option is missing."
        #         "\n Output: {0} \n CMD: {1}".format(
        #             repr(self.output), self.cmd))
        # except ProbackupException as e:
        #     self.assertIn(
        #         "Insert correct error",
        #         e.message,
        #         "\n Unexpected Error Message: {0}\n CMD: {1}".format(
        #             repr(e.message), self.cmd))
