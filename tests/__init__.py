import unittest
import os

from . import init, merge, option, show, compatibility, \
    backup, delete, delta, restore, validate, \
    retention, pgpro560, pgpro589, pgpro2068, false_positive, replica, \
    compression, page, ptrack, archive, exclude, cfs_backup, cfs_restore, \
    cfs_validate_backup, auth_test, time_stamp, snapfs, logging, \
    locking, remote, external, config, checkdb, set_backup, incr_restore, \
    CVE_2018_1058


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()

    if 'PG_PROBACKUP_TEST_BASIC' in os.environ:
        if os.environ['PG_PROBACKUP_TEST_BASIC'] == 'ON':
            loader.testMethodPrefix = 'test_basic'

    if 'PG_PROBACKUP_PTRACK' in os.environ:
        if os.environ['PG_PROBACKUP_PTRACK'] == 'ON':
            suite.addTests(loader.loadTestsFromModule(ptrack))

    suite.addTests(loader.loadTestsFromModule(auth_test))
    suite.addTests(loader.loadTestsFromModule(archive))
    suite.addTests(loader.loadTestsFromModule(backup))
    if 'PGPROBACKUPBIN_OLD' in os.environ and os.environ['PGPROBACKUPBIN_OLD']:
        suite.addTests(loader.loadTestsFromModule(compatibility))
    suite.addTests(loader.loadTestsFromModule(checkdb))
    suite.addTests(loader.loadTestsFromModule(config))
   # suite.addTests(loader.loadTestsFromModule(cfs_backup))
   # suite.addTests(loader.loadTestsFromModule(cfs_restore))
   # suite.addTests(loader.loadTestsFromModule(cfs_validate_backup))
    suite.addTests(loader.loadTestsFromModule(compression))
    suite.addTests(loader.loadTestsFromModule(delete))
    suite.addTests(loader.loadTestsFromModule(delta))
    suite.addTests(loader.loadTestsFromModule(exclude))
    suite.addTests(loader.loadTestsFromModule(external))
    suite.addTests(loader.loadTestsFromModule(false_positive))
    suite.addTests(loader.loadTestsFromModule(init))
    suite.addTests(loader.loadTestsFromModule(incr_restore))
    suite.addTests(loader.loadTestsFromModule(locking))
    suite.addTests(loader.loadTestsFromModule(logging))
    # suite.addTests(loader.loadTestsFromModule(merge))
    suite.addTests(loader.loadTestsFromModule(option))
    suite.addTests(loader.loadTestsFromModule(page))
    suite.addTests(loader.loadTestsFromModule(pgpro560))
    suite.addTests(loader.loadTestsFromModule(pgpro589))
    suite.addTests(loader.loadTestsFromModule(pgpro2068))
    suite.addTests(loader.loadTestsFromModule(remote))
    suite.addTests(loader.loadTestsFromModule(replica))
    suite.addTests(loader.loadTestsFromModule(restore))
    suite.addTests(loader.loadTestsFromModule(retention))
    suite.addTests(loader.loadTestsFromModule(set_backup))
    suite.addTests(loader.loadTestsFromModule(show))
    suite.addTests(loader.loadTestsFromModule(snapfs))
    suite.addTests(loader.loadTestsFromModule(time_stamp))
    suite.addTests(loader.loadTestsFromModule(validate))
    suite.addTests(loader.loadTestsFromModule(CVE_2018_1058))

    return suite

#   test_pgpro434_2 unexpected success
# ToDo:
#  logging:
#     https://jira.postgrespro.ru/secure/attachment/20420/20420_doc_logging.md
# archive:
#      immediate recovery and full recovery
