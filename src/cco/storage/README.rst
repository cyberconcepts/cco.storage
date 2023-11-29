========================================================
SQL-based Storage for Records (Tracks) and Other Objects
========================================================

Test Prerequisite: PostgreSQL database ccotest (user ccotest with password cco).

  >>> from cco.storage.common import getEngine

  >>> engine = getEngine('postgresql+psycopg', 'ccotest', 'ccotest', 'cco')


Tracking Storage
================

  >>> from cco.storage.tracking import record
  >>> storage = record.Storage(engine)

  >>> tr01 = record.Track('t01', 'john')
  >>> tr01.keydata
  {'taskId': 't01', 'userName': 'john'}

  >>> storage.getTable()
  Table(...)

 Fin
 ===

  >>> storage.conn.close()

