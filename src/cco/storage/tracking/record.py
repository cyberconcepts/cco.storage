# cco.storage.tracking.record

"""SQL-based storage for simple tracks (records). 

A track consists of a head (index data, metadata)  with a fixed set of fields and 
data (payload) represented as a dict.
"""

from datetime import datetime
from sqlalchemy import MetaData, Table, Column, Index
from sqlalchemy import BigInteger, DateTime, Text, func
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import JSONB
import transaction
from zope.sqlalchemy import register, mark_changed

from cybertools.util.date import getTimeStamp, timeStamp2ISO

Session = None
engine = None

def defaultIndexes(cols):
    return [cols[i:] for i in range(len(cols))]


class Track(object):
    
    headFields = ['taskId', 'userName']

    def __init__(self, *keys, data=None, timeStamp=None, trackId=None):
        self.head = {}
        for ix, k in enumerate(keys):
            self.head[self.headFields[ix]] = k
        self.data = data or {}
        self.timeStamp = timeStamp
        self.trackId = trackId

    def update(self, data, overwrite=False):
        if data is None:
            return
        if overwrite:
            self.data = data
        else:
            self.data.update(data)


class Storage(object):

    trackFactory = Track
    headCols = tuple(f.lower() for f in trackFactory.headFields)
    indexes = defaultIndexes(headCols)
    #indexes = [('username',), ('taskid', 'username')]
    tableName = 'tracks'

    session = None
    conn = None
    table = None

    def __init__(self):
        self.session = Session()
        self.conn = self.session.connection()
        self.metadata = MetaData()
        self.table = self.getTable()

    def get(self, trackId):
        t = self.table
        stmt = select(t).where(t.c.trackid == trackId)
        for r in self.conn.execute(stmt):
            return Track(*r[1:-2], trackId=r[0],timeStamp=r[-2], data=r[-1])

    def save(self, track, update=False, overwrite=False, newTimeStamp=True):
        if not update:
            return self.insert(track)
        if track.trackId:
            if self.update(track, newTimeStamp) > 0:
                return track.trackId
        found = self.queryLast(track)
        if found is None:
            return self.insert(track)
        if overwrite:
            found.data.update(track.data)
        else:
            found.data = track.data
        self.update(found, newTimeStamp)
        return found.trackId

    def insert(self, track):
        t = self.table
        trackId = 0
        values = self.setupValues(track)
        stmt = t.insert().values(**values).returning(t.c.trackid)
        for v in self.conn.execute(stmt):
            trackId = v[0]
            break
        mark_changed(self.session)
        return trackId

    def update(self, track, newTimeStamp=True):
        t = self.table
        values = self.setupValues(track)
        if newTimeStamp:  # CHECK: and track.timeStamp is None:
            values['timestamp'] = datetime.now()
        stmt = t.update().values(**values).where(t.c.trackid == track.trackId)
        n = self.conn.execute(stmt).rowcount
        if n > 0:
            mark_changed(self.session)
        return n

    def setupValues(self, track):
        values = {}
        hf = self.trackFactory.headFields
        for i, c in enumerate(self.headCols):
            values[c] = track.head[hf[i]]
        values['data'] = track.data
        if track.timeStamp is not None:
            values['timestamp'] = track.timeStamp
        return values

    def getTable(self):
        if self.table is not None:
            return self.table
        if self.tableName in self.metadata.tables:
            self.table = Table(self.tableName, self.metadata, autoload_with=self.engine)
            return self.table
        return createTable(engine, self.metadata, self.tableName,
                           self.headCols, self.indexes)


def createTable(engine, metadata, tableName, headcols, indexes=None):
    cols = [Column('trackid', BigInteger, primary_key=True)]
    idxs = []
    for ix, f in enumerate(headcols):
        cols.append(Column(f.lower(), Text, nullable=False))
    cols.append(Column('timestamp', DateTime(timezone=True), server_default=func.now()))
    for ix, idef in enumerate(indexes):
        indexName = 'idx_%s_%d' % (tableName, (ix + 1))
        idxs.append(Index(indexName, *idef))
    idxs.append(Index('idx_%s_ts' % tableName, 'timestamp'))
    cols.append(Column('data', JSONB))
    table = Table(tableName, metadata, *(cols+idxs), extend_existing=True)
    metadata.create_all(engine)
    return table

