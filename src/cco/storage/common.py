# cco.storage.common

"""Common utility stuff for the cco.storage packages.
"""

from sqlalchemy import create_engine

def getEngine(dbtype, dbname, user, pw, host='localhost', port=5432, **kw):
    return create_engine('%s://%s:%s@%s:%s/%s' % (
        dbtype, user, pw, host, port, dbname), **kw)

