from .helpers.ptrack_helpers import ProbackupTest


class RemoteTest(ProbackupTest):

    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_remote_sanity(self):
        node = self.pg_node.make_simple('node',
            set_replication=True)

        backup_dir = self.backup_dir
        self.pb.init()
        self.pb.add_instance('node', node)
        node.slow_start()

        output = self.pb.backup_node('node', node,
            options=['--stream'], no_remote=True, return_id=False)
        self.assertIn('remote: false', output)
