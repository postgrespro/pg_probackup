import unittest

from . import init_test, option_test, show_test, \
    backup_test, delete_test, restore_test, validate_test, \
    retention_test, ptrack_clean, ptrack_cluster, \
    ptrack_move_to_tablespace, ptrack_recovery, ptrack_vacuum, \
    ptrack_vacuum_bits_frozen, ptrack_vacuum_bits_visibility, \
    ptrack_vacuum_full, ptrack_vacuum_truncate, pgpro560, pgpro589, \
    false_positive, replica, compression, page, ptrack, archive, \
    exclude, cfs_backup, cfs_restore, cfs_validate_backup


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromModule(archive))
    suite.addTests(loader.loadTestsFromModule(backup_test))
#    suite.addTests(loader.loadTestsFromModule(cfs_backup))
#    suite.addTests(loader.loadTestsFromModule(cfs_restore))
#    suite.addTests(loader.loadTestsFromModule(cfs_validate_backup))
#    suite.addTests(loader.loadTestsFromModule(logging))
    suite.addTests(loader.loadTestsFromModule(compression))
    suite.addTests(loader.loadTestsFromModule(delete_test))
    suite.addTests(loader.loadTestsFromModule(exclude))
    suite.addTests(loader.loadTestsFromModule(false_positive))
    suite.addTests(loader.loadTestsFromModule(init_test))
    suite.addTests(loader.loadTestsFromModule(option_test))
    suite.addTests(loader.loadTestsFromModule(page))
    suite.addTests(loader.loadTestsFromModule(ptrack))
    suite.addTests(loader.loadTestsFromModule(ptrack_clean))
    suite.addTests(loader.loadTestsFromModule(ptrack_cluster))
    suite.addTests(loader.loadTestsFromModule(ptrack_move_to_tablespace))
    suite.addTests(loader.loadTestsFromModule(ptrack_recovery))
    suite.addTests(loader.loadTestsFromModule(ptrack_vacuum))
    suite.addTests(loader.loadTestsFromModule(ptrack_vacuum_bits_frozen))
    suite.addTests(loader.loadTestsFromModule(ptrack_vacuum_bits_visibility))
    suite.addTests(loader.loadTestsFromModule(ptrack_vacuum_full))
    suite.addTests(loader.loadTestsFromModule(ptrack_vacuum_truncate))
    suite.addTests(loader.loadTestsFromModule(replica))
    suite.addTests(loader.loadTestsFromModule(restore_test))
    suite.addTests(loader.loadTestsFromModule(retention_test))
    suite.addTests(loader.loadTestsFromModule(show_test))
    suite.addTests(loader.loadTestsFromModule(validate_test))
    suite.addTests(loader.loadTestsFromModule(pgpro560))
    suite.addTests(loader.loadTestsFromModule(pgpro589))

    return suite

#   test_pgpro434_2 unexpected success
# ToDo:
#  archive:
#    discrepancy of instance`s SYSTEMID and node`s SYSTEMID should lead to archive-push refusal to work
#  replica:
#    backup should exit with correct error message if some master* option is missing
#    --master* options shoukd not work when backuping master
#  logging:
#     https://jira.postgrespro.ru/browse/PGPRO-584
#     https://jira.postgrespro.ru/secure/attachment/20420/20420_doc_logging.md
#  ptrack:
#      ptrack backup on replica should work correctly
# archive:
#      immediate recovery and full recovery
# 10vanilla_1.3ptrack +
# 10vanilla+
# 9.6vanilla_1.3ptrack +
