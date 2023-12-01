========================================================
SQL-based Storage for Records (Tracks) and Other Objects
========================================================

Test Prerequisite: PostgreSQL database ccotest (user ccotest with password cco).

  >>> from cco.storage.common import getEngine, sessionFactory
  >>> from cco.storage.tracking import record

  >>> record.engine = getEngine('postgresql+psycopg', 'ccotest', 'ccotest', 'cco')
  >>> record.Session = sessionFactory(record.engine)


Tracking Storage
================

  >>> storage = record.Storage(doCommit=True)

  >>> tr01 = record.Track('t01', 'john')
  >>> tr01.head
  {'taskId': 't01', 'userName': 'john'}

  >>> storage.getTable()
  Table(...)

  >>> trackId = storage.save(tr01)
  >>> trackId > 0
  True

  >>> tr01a = storage.get(trackId)
  >>> tr01a.head

 Fin
 ===

  >>> storage.conn.close()

