import unittest
import os

from . import init_test, merge_test, option_test, show_test, compatibility_test, \
    backup_test, delete_test, delta_test, restore_test, validate_test, \
    retention_test, pgpro560_test, pgpro589_test, false_positive_test, replica_test, \
    compression_test, page_test, ptrack_test, archive_test, exclude_test, \
    auth_test, time_stamp_test, logging_test, \
    locking_test, remote_test, external_test, config_test, checkdb_test, set_backup_test, incr_restore_test, \
    catchup_test, CVE_2018_1058_test, time_consuming_test


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()

    if 'PG_PROBACKUP_TEST_BASIC' in os.environ:
        if os.environ['PG_PROBACKUP_TEST_BASIC'] == 'ON':
            loader.testMethodPrefix = 'test_basic'

    if 'PG_PROBACKUP_PTRACK' in os.environ:
        if os.environ['PG_PROBACKUP_PTRACK'] == 'ON':
            suite.addTests(loader.loadTestsFromModule(ptrack_test))

    # PG_PROBACKUP_LONG section for tests that are long
    # by design e.g. they contain loops, sleeps and so on
    if 'PG_PROBACKUP_LONG' in os.environ:
        if os.environ['PG_PROBACKUP_LONG'] == 'ON':
            suite.addTests(loader.loadTestsFromModule(time_consuming_test))

    suite.addTests(loader.loadTestsFromModule(auth_test))
    suite.addTests(loader.loadTestsFromModule(archive_test))
    suite.addTests(loader.loadTestsFromModule(backup_test))
    suite.addTests(loader.loadTestsFromModule(catchup_test))
    if 'PGPROBACKUPBIN_OLD' in os.environ and os.environ['PGPROBACKUPBIN_OLD']:
        suite.addTests(loader.loadTestsFromModule(compatibility_test))
    suite.addTests(loader.loadTestsFromModule(checkdb_test))
    suite.addTests(loader.loadTestsFromModule(config_test))
    suite.addTests(loader.loadTestsFromModule(compression_test))
    suite.addTests(loader.loadTestsFromModule(delete_test))
    suite.addTests(loader.loadTestsFromModule(delta_test))
    suite.addTests(loader.loadTestsFromModule(exclude_test))
    suite.addTests(loader.loadTestsFromModule(external_test))
    suite.addTests(loader.loadTestsFromModule(false_positive_test))
    suite.addTests(loader.loadTestsFromModule(init_test))
    suite.addTests(loader.loadTestsFromModule(incr_restore_test))
    suite.addTests(loader.loadTestsFromModule(locking_test))
    suite.addTests(loader.loadTestsFromModule(logging_test))
    suite.addTests(loader.loadTestsFromModule(merge_test))
    suite.addTests(loader.loadTestsFromModule(option_test))
    suite.addTests(loader.loadTestsFromModule(page_test))
    suite.addTests(loader.loadTestsFromModule(pgpro560_test))
    suite.addTests(loader.loadTestsFromModule(pgpro589_test))
    suite.addTests(loader.loadTestsFromModule(remote_test))
    suite.addTests(loader.loadTestsFromModule(replica_test))
    suite.addTests(loader.loadTestsFromModule(restore_test))
    suite.addTests(loader.loadTestsFromModule(retention_test))
    suite.addTests(loader.loadTestsFromModule(set_backup_test))
    suite.addTests(loader.loadTestsFromModule(show_test))
    suite.addTests(loader.loadTestsFromModule(time_stamp_test))
    suite.addTests(loader.loadTestsFromModule(validate_test))
    suite.addTests(loader.loadTestsFromModule(CVE_2018_1058_test))

    return suite

#   test_pgpro434_2 unexpected success
# ToDo:
#  logging:
#     https://jira.postgrespro.ru/secure/attachment/20420/20420_doc_logging.md
# archive:
#      immediate recovery and full recovery
