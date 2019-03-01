import unittest

from . import init_test, merge, option_test, show_test, compatibility, \
    backup_test, delete_test, delta, restore_test, validate_test, \
    retention_test, pgpro560, pgpro589, pgpro2068, false_positive, replica, \
    compression, page, ptrack, archive, exclude, cfs_backup, cfs_restore, \
    cfs_validate_backup, auth_test, time_stamp, snapfs, logging, \
    locking, remote


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
#    suite.addTests(loader.loadTestsFromModule(auth_test))
    suite.addTests(loader.loadTestsFromModule(archive))
    suite.addTests(loader.loadTestsFromModule(backup_test))
    suite.addTests(loader.loadTestsFromModule(compatibility))
#    suite.addTests(loader.loadTestsFromModule(cfs_backup))
#    suite.addTests(loader.loadTestsFromModule(cfs_restore))
#    suite.addTests(loader.loadTestsFromModule(cfs_validate_backup))
#    suite.addTests(loader.loadTestsFromModule(logging))
    suite.addTests(loader.loadTestsFromModule(compression))
    suite.addTests(loader.loadTestsFromModule(delete_test))
    suite.addTests(loader.loadTestsFromModule(delta))
    suite.addTests(loader.loadTestsFromModule(exclude))
    suite.addTests(loader.loadTestsFromModule(false_positive))
    suite.addTests(loader.loadTestsFromModule(init_test))
    suite.addTests(loader.loadTestsFromModule(locking))
    suite.addTests(loader.loadTestsFromModule(logging))
    suite.addTests(loader.loadTestsFromModule(merge))
    suite.addTests(loader.loadTestsFromModule(option_test))
    suite.addTests(loader.loadTestsFromModule(page))
#    suite.addTests(loader.loadTestsFromModule(ptrack))
    suite.addTests(loader.loadTestsFromModule(remote))
    suite.addTests(loader.loadTestsFromModule(replica))
    suite.addTests(loader.loadTestsFromModule(restore_test))
    suite.addTests(loader.loadTestsFromModule(retention_test))
    suite.addTests(loader.loadTestsFromModule(show_test))
    suite.addTests(loader.loadTestsFromModule(snapfs))
    suite.addTests(loader.loadTestsFromModule(validate_test))
    suite.addTests(loader.loadTestsFromModule(pgpro560))
    suite.addTests(loader.loadTestsFromModule(pgpro589))
    suite.addTests(loader.loadTestsFromModule(time_stamp))

    return suite

#   test_pgpro434_2 unexpected success
# ToDo:
#  archive:
#    discrepancy of instance`s SYSTEMID and node`s SYSTEMID should lead to archive-push refusal to work
#  logging:
#     https://jira.postgrespro.ru/browse/PGPRO-584
#     https://jira.postgrespro.ru/secure/attachment/20420/20420_doc_logging.md
# archive:
#      immediate recovery and full recovery
