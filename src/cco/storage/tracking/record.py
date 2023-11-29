# cco.storage.tracking.record

"""SQL-based storage for simple tracks (records). 

A track consists of a head (index data, metadata)  with a fixed set of fields and 
data (payload) represented as a dict.
"""

from sqlalchemy import MetaData, Table, Column, Index
from sqlalchemy import BigInteger, DateTime, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import scoped_session, sessionmaker
from zope.sqlalchemy import register, mark_changed

from cybertools.util.date import getTimeStamp, timeStamp2ISO


class Track(object):
    
    keydataFields = ['taskId', 'userName']

    def __init__(self, *keys, data=None):
        self.keydata = {}
        for ix, k in enumerate(keys):
            self.keydata[self.keydataFields[ix]] = k
        self.data = data or {}


class Storage(object):

    trackClass = Track
    tableName = 'tracks'

    conn = None

    def __init__(self, engine=None, conn=None):
        self.engine = engine
        if conn is None:
            self.conn = engine.connect()
        else:
            self.conn = conn
        self.metadata = MetaData()

    def save(self, track):
        pass

    def getTable(self):
        fields = [f.lower() for f in self.trackClass.keydataFields]
        cols = []
        indexes = []
        cols.append(Column('trackid', BigInteger, primary_key=True))
        for ix, f in enumerate(fields):
            cols.append(Column(f.lower(), Text))
            indexes.append(Index(f, *fields[ix:]))
        cols.append(Column('timestamp', DateTime(timezone=True), 
                           server_default=func.now()))
        cols.append(Column('data', JSONB))
        table = Table(self.tableName, self.metadata, 
                      *(cols+indexes), extend_existing=True)
        self.metadata.create_all(self.engine)
        return table

