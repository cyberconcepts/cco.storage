# cco.storage.tracking.record
"""SQL-based storage for simple tracks (records). 

A track consists of a head (index data, metadata)  with a fixed set of fields and 
data (payload) represented as a dict.
"""

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import scoped_session, sessionmaker
from zope.sqlalchemy import register, mark_changed


class Track(object):
    
    head_fields = ['task', 'user']

class Storage(object):

    def save(self, track):
        pass
