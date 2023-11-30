========================================================
SQL-based Storage for Records (Tracks) and Other Objects
========================================================

Test Prerequisite: PostgreSQL database ccotest (user ccotest with password cco).

  >>> import transaction
  >>> from cco.storage.common import getEngine

  >>> engine = getEngine('postgresql+psycopg', 'ccotest', 'ccotest', 'cco')


Tracking Storage
================

  >>> from cco.storage.tracking import record
  >>> storage = record.Storage(engine, doCommit=True)

  >>> tr01 = record.Track('t01', 'john')
  >>> tr01.head
  {'taskId': 't01', 'userName': 'john'}

  >>> storage.getTable()
  Table(...)

  >>> trackId = storage.save(tr01)
  >>> trackId > 0
  True

 Fin
 ===

  >>> storage.conn.close()

