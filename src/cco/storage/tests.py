#! /usr/bin/python

"""Tests for the 'cco.storage' package.
"""

from datetime import datetime
import transaction
import unittest, doctest
import warnings

from cco.storage.common import Storage, getEngine, sessionFactory
from cco.storage.proxy import core
from cco.storage.tracking import record

#warnings.filterwarnings('ignore', category=ResourceWarning)

storage = Storage(getEngine('postgresql', 'ccotest', 'ccotest', 'cco'),
                  schema='testing')


class Test(unittest.TestCase):
    "Basic tests for the cco.storage package."

    def testBasicStuff(self):
        tracks = storage.create(record.Container)

        tr01 = record.Track('t01', 'john')
        tr01.update(dict(activity='testing'))
        self.assertEqual(tr01.head, {'taskId': 't01', 'userName': 'john'})

        self.assertTrue(tracks.getTable() is not None)

        trid01 = tracks.save(tr01)
        self.assertTrue(trid01 > 0)

        tr01a = tracks.get(trid01)
        self.assertEqual(tr01a.head, tr01.head)
        self.assertEqual(tr01a.trackId, trid01)
        self.assertEqual(tr01a.data.get('activity'), 'testing')

        tr01a.update(dict(text='Set up unit tests.'))
        tr01a.timeStamp = None
        self.assertTrue(tracks.save(tr01a) > 0)

        tr01b = tracks.queryLast(taskId='t01')
        self.assertEqual(tr01b.head, tr01.head)
        self.assertNotEqual(tr01b.trackId, trid01)
        self.assertEqual(tr01b.data.get('activity'), 'testing')

        tr02 = record.Track('t02', 'jim', trackId=31, timeStamp=datetime(2023, 11, 30),
                            data=dict(activity='concept'))
        trid02 = tracks.upsert(tr02)
        self.assertEqual(trid02, 31)
        self.assertEqual(tr02.uid, 'rec-31')
        tr02.trackId = trid01
        trid021 = tracks.upsert(tr02)
        self.assertEqual(trid021, trid01)
        self.assertEqual(tr02.uid, 'rec-' + str(trid01))

        tr03 = storage.getItem('rec-31')
        self.assertEqual(tr03.trackId, 31)

        n = tracks.remove(31)
        self.assertEqual(n, 1)
        self.assertEqual(tracks.get(31), None)

        transaction.commit()


def test_suite():
    #flags = doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS
    return unittest.TestSuite((
        unittest.TestLoader().loadTestsFromTestCase(Test),
        #doctest.DocFileSuite('README.rst', optionflags=flags),
    ))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
