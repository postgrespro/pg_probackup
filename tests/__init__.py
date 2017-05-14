import unittest

from . import init_test, option_test, show_test, \
    backup_test, delete_test, restore_test, validate_test, \
    retention_test, ptrack_clean, ptrack_cluster, \
    ptrack_move_to_tablespace, ptrack_recovery, ptrack_vacuum, \
    ptrack_vacuum_bits_frozen, ptrack_vacuum_bits_visibility, \
    ptrack_vacuum_full, ptrack_vacuum_truncate, pgpro668


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromModule(pgpro668))
    suite.addTests(loader.loadTestsFromModule(init_test))
    suite.addTests(loader.loadTestsFromModule(option_test))
    suite.addTests(loader.loadTestsFromModule(show_test))
    suite.addTests(loader.loadTestsFromModule(backup_test))
    suite.addTests(loader.loadTestsFromModule(delete_test))
    suite.addTests(loader.loadTestsFromModule(restore_test))
    suite.addTests(loader.loadTestsFromModule(validate_test))
    suite.addTests(loader.loadTestsFromModule(retention_test))
    suite.addTests(loader.loadTestsFromModule(ptrack_clean))
    suite.addTests(loader.loadTestsFromModule(ptrack_cluster))
    suite.addTests(loader.loadTestsFromModule(ptrack_move_to_tablespace))
    suite.addTests(loader.loadTestsFromModule(ptrack_recovery))
    suite.addTests(loader.loadTestsFromModule(ptrack_vacuum))
    suite.addTests(loader.loadTestsFromModule(ptrack_vacuum_bits_frozen))
    suite.addTests(loader.loadTestsFromModule(ptrack_vacuum_bits_visibility))
    suite.addTests(loader.loadTestsFromModule(ptrack_vacuum_full))
    suite.addTests(loader.loadTestsFromModule(ptrack_vacuum_truncate))

    return suite
