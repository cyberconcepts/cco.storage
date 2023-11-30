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
    
    headFields = ['taskId', 'userName']

    def __init__(self, *keys, data=None):
        self.head = {}
        for ix, k in enumerate(keys):
            self.head[self.headFields[ix]] = k
        self.data = data or {}


class Storage(object):

    trackFactory = Track
    tableName = 'tracks'
    indexes = None  # create indexes automatically
    #indexes = [('username',), ('taskid', 'username')]

    conn = None
    table = None

    def __init__(self, engine=None, conn=None):
        self.engine = engine
        if conn is None:
            self.conn = engine.connect()
        else:
            self.conn = conn
        self.metadata = MetaData()
        self.table = self.getTable()

    def save(self, track):
        values = {}
        t = self.table
        trackId = 0
        for k, v in track.head.items():
            values[k.lower()] = v
        values['data'] = track.data
        stmt = t.insert().values(**values).returning(t.c.trackid)
        for v in self.conn.execute(stmt):
            trackId = v[0]
            break
        self.conn.commit()
        return trackId

    def getTable(self):
        if self.table is not None:
            return self.table
        if self.tableName in self.metadata.tables:
            self.table = Table(self.tableName, self.metadata, autoload_with=self.engine)
            return self.table
        fields = [f.lower() for f in self.trackFactory.headFields]
        return createTable(self.engine, self.metadata, self.tableName,
                           fields, self.indexes)


def createTable(engine, metadata, tableName, headcols, indexes=None):
    cols = []
    idxs = []
    cols.append(Column('trackid', BigInteger, primary_key=True))
    for ix, f in enumerate(headcols):
        cols.append(Column(f.lower(), Text))
        if indexes is None:
            indexName = 'idx_%s_%d' % (tableName, (ix + 1))
            idxs.append(Index(indexName, *headcols[ix:]))
    cols.append(Column('timestamp', DateTime(timezone=True), server_default=func.now()))
    if indexes is not None:
        for ix, idef in enumerate(indexes):
            indexName = 'idx_%s_%d' % (tableName, (ix + 1))
            idxs.append(Index(indexName, *idef))
    idxs.append(Index('idx_%s_ts' % tableName, 'timestamp'))
    cols.append(Column('data', JSONB))
    table = Table(tableName, metadata, *(cols+idxs), extend_existing=True)
    metadata.create_all(engine)
    return table

