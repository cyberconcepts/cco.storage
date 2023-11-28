#! /usr/bin/python

"""
Tests for the 'cco.storage' package.
"""

import unittest, doctest
import warnings
import cco.storage

#warnings.filterwarnings('ignore', category=ResourceWarning)

class Test(unittest.TestCase):
    "Basic tests for the cco.storage package."

    def testBasicStuff(self):
        pass


def test_suite():
    flags = doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS
    return unittest.TestSuite((
        unittest.TestLoader().loadTestsFromTestCase(Test),
        doctest.DocFileSuite('README.rst', optionflags=flags),
        ))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
