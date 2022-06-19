import unittest
from tests.helpers.ptrack_helpers import ProbackupTest


class CCallsTests(ProbackupTest, unittest.TestCase):
    #    """ tests for internal C functions calls"""
    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_get_control_value_buffer_overflow(self):
        pass


