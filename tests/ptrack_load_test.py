import os
from .helpers.ptrack_helpers import ProbackupTest

PAGE_SIZE = 8192
ZEROES = b"\x00" * PAGE_SIZE


class PtrackLoadTest(ProbackupTest):
    def setUp(self):
        if not self.ptrack:
            self.skipTest('Skipped because ptrack support is disabled')

    def find_zero_pages(self, node, pagemapset, file_path):
        """
        Find zero pages in a file using pagemapset.

        Args:
            node (Node): The PostgreSQL node instance.
            pagemapset (dict): The pagemapset obtained from fetch_ptrack.
            file_path (str): Path to the file to analyze.

        Returns:
            list: List of missed pages.
        """
        missed_pages = []

        if os.path.isfile(file_path):
            rel_path = file_path.replace(f"{node.data_dir}/", "")
            with open(file_path, "rb") as f:
                bno = 0
                while True:
                    page_data = f.read(PAGE_SIZE)
                    if not page_data:
                        break

                    if page_data == ZEROES:
                        if not self.check_ptrack(pagemapset, rel_path, bno):
                            print(f"Missed page: {rel_path}|{bno}")
                            missed_pages.append(f'{rel_path}|{bno}')
                        else:
                            print(f"Found page: {rel_path}|{bno}")

                    bno += 1

        return missed_pages

    @staticmethod
    def fetch_ptrack(node, lsn):
        """
        Fetch pagemapset using ptrack_get_pagemapset function.

        Args:
            node (Node): The PostgreSQL node instance.
            lsn (str): The LSN (Log Sequence Number).

        Returns:
            dict: Dictionary containing pagemapset data.
        """
        result_map = {}
        ptrack_out = node.execute(
            "postgres",
            f"select (ptrack_get_pagemapset('{lsn}')).*;")
        for row in ptrack_out:
            path, pagecount, pagemap = row
            result_map[path] = bytearray(pagemap)
        return result_map

    def check_ptrack(self, page_map, file, bno):
        """
        Check if the given block number has changes in pagemap.

        Args:
            page_map (dict): Pagemapset data.
            file (str): File name.
            bno (int): Block number.

        Returns:
            bool: True if changes are detected, False otherwise.
        """
        self.assertNotEqual(page_map, {})
        bits = page_map.get(file)

        if bits and bno // 8 < len(bits):
            return (bits[bno // 8] & (1 << (bno & 7))) != 0
        else:
            return False

    def test_load_ptrack_zero_pages(self):
        """
        An error too many clients already for some clients is usual for this test
        """
        pg_options = {'max_connections': 1024,
                      'ptrack.map_size': 1024,
                      'shared_buffers': '8GB',
                      'checkpoint_timeout': '1d',
                      'synchronous_commit': 'off',
                      'fsync': 'off',
                      'shared_preload_libraries': 'ptrack',
                      'wal_buffers': '128MB',
                      'wal_writer_delay': '5s',
                      'wal_writer_flush_after': '16MB',
                      'commit_delay': 100,
                      'checkpoint_flush_after': '2MB',
                      'max_wal_size': '10GB',
                      'autovacuum': 'off'}
        if self.pg_config_version >= 120000:
            pg_options['wal_recycle'] = 'off'

        node = self.pg_node.make_simple('node',
                                        set_replication=True,
                                        ptrack_enable=self.ptrack,
                                        )
        node.slow_start()

        start_lsn = node.execute(
            "postgres",
            "select pg_current_wal_lsn()")[0][0]

        self.pb.init()
        self.pb.add_instance('node', node)

        node.execute(
            "postgres",
            "CREATE EXTENSION ptrack")

        # Initialize and start pgbench
        node.pgbench_init(scale=100)

        pgbench = node.pgbench(options=['-T', '20', '-c', '150', '-j', '150'])
        pgbench.wait()

        node.execute(
            "postgres",
            "CHECKPOINT;select txid_current();")

        missed_pages = []
        # Process each file in the data directory
        for root, dirs, files in os.walk(node.data_dir):
            # Process only the files in the 'global' and 'base' directories
            if 'data/global' in root or 'data/base' in root or 'data/pg_tblspc' in root:
                for file in files:
                    if file in ['ptrack.map']:
                        continue
                    file_path = os.path.join(root, file)
                    pagemapset = self.fetch_ptrack(node, start_lsn)
                    pages = self.find_zero_pages(node, pagemapset, file_path)
                    if pages:
                        missed_pages.extend(pages)
        # Check that no missed pages
        self.assertEqual(missed_pages, [])
