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

 Fin
 ===

  >>> storage.conn.close()

