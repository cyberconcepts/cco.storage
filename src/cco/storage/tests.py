#! /usr/bin/python

"""Tests for the 'cco.storage' package.
"""

import transaction
import unittest, doctest
import warnings

from cco.storage.common import getEngine, sessionFactory
from cco.storage.tracking import record

warnings.filterwarnings('ignore', category=ResourceWarning)

record.engine = getEngine('postgresql+psycopg', 'ccotest', 'ccotest', 'cco')
record.Session = sessionFactory(record.engine)


class Test(unittest.TestCase):
    "Basic tests for the cco.storage package."

    def testBasicStuff(self):
        storage = record.Storage()
        tr01 = record.Track('t01', 'john')
        self.assertEqual(tr01.head,{'taskId': 't01', 'userName': 'john'})

        self.assertTrue(storage.getTable() is not None)

        trackId = storage.save(tr01)
        self.assertTrue(trackId > 0)

        tr01a = storage.get(trackId)
        print(tr01.head)

        transaction.commit()


def test_suite():
    flags = doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS
    return unittest.TestSuite((
        unittest.TestLoader().loadTestsFromTestCase(Test),
        #doctest.DocFileSuite('README.rst', optionflags=flags),
    ))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
