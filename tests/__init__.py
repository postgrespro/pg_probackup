import unittest

from . import init_test, option_test, show_test, backup_test, delete_test, restore_test


def load_tests(loader, tests, pattern):
	suite = unittest.TestSuite()
	suite.addTests(loader.loadTestsFromModule(init_test))
	suite.addTests(loader.loadTestsFromModule(option_test))
	suite.addTests(loader.loadTestsFromModule(show_test))
	suite.addTests(loader.loadTestsFromModule(backup_test))
	suite.addTests(loader.loadTestsFromModule(delete_test))
	suite.addTests(loader.loadTestsFromModule(restore_test))

	return suite
