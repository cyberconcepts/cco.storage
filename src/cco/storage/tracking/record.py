# cco.storage.tracking.record

"""SQL-based storage for simple tracks (records). 

A track consists of a head (index data, metadata)  with a fixed set of fields and 
data (payload) represented as a dict.
"""

from sqlalchemy import MetaData, Table
from sqlalchemy.orm import scoped_session, sessionmaker
from zope.sqlalchemy import register, mark_changed


class Track(object):
    
    head_fields = ['task', 'user']


class Storage(object):

    conn = None

    def __init__(self, engine=None, conn=None):
        if conn is None:
            self.conn = engine.connect()
        else:
            self.conn = conn

    def save(self, track):
        pass
