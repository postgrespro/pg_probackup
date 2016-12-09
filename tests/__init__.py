import unittest

from . import init_test, option_test, show_test


def load_tests(loader, tests, pattern):
	suite = unittest.TestSuite()
	suite.addTests(loader.loadTestsFromModule(init_test))
	suite.addTests(loader.loadTestsFromModule(option_test))
	suite.addTests(loader.loadTestsFromModule(show_test))

	return suite
