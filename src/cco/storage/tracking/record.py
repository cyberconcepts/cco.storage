# cco.storage.tracking.record

"""SQL-based storage for simple tracks (records). 

A track consists of a head (index data, metadata)  with a fixed set of fields and 
data (payload) represented as a dict.
"""

from datetime import datetime
from sqlalchemy import MetaData, Table, Column, Index
from sqlalchemy import BigInteger, DateTime, Text, func
from sqlalchemy import and_
from sqlalchemy.dialects.postgresql import JSONB
import transaction
from zope.sqlalchemy import register, mark_changed

from cybertools.util.date import getTimeStamp, timeStamp2ISO

def defaultIndexes(cols):
    return [cols[i:] for i in range(len(cols))]


class Track(object):
    
    headFields = ['taskId', 'userName']

    def __init__(self, *keys, **kw):
        self.head = {}
        for ix, k in enumerate(keys):
            self.head[self.headFields[ix]] = k
        for k in self.headFields:
            if self.head.get(k) is None:
                self.heaad[k] = ''
        self.data = kw.get('data') or {}
        self.timeStamp = kw.get('timeStamp')
        self.trackId = kw.get('trackId')
        self.storage = kw.get('storage')

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

    table = None

    def __init__(self, context, recordChanges=True):
        self.context = context
        self.recordChanges = recordChanges  # insert new track on change of data
        self.session = context.Session()
        self.engine = context.engine
        self.metadata = MetaData(schema=context.schema)
        self.table = self.getTable(context.schema)

    def get(self, trackId):
        stmt = self.table.select().where(self.table.c.trackid == trackId)
        return self.makeTrack(self.session.execute(stmt).first())

    def query(self, **crit):
        stmt = self.table.select().where(
                and_(*self.setupWhere(crit))).order_by(t.c.trackId)
        for r in self.session.execute(stmt):
            yield self.makeTrack(r)

    def queryLast(self, **crit):
        stmt = (self.table.select().where(and_(*self.setupWhere(crit))).
                order_by(self.table.c.trackid.desc()).limit(1))
        return self.makeTrack(self.session.execute(stmt).first())

    def save(self, track):
        crit = dict((hf, track.head[hf]) for hf in track.headFields)
        found = self.queryLast(**crit)
        if found is None:
            return self.insert(track)
        if self.recordChanges and found.data != track.data:
            return self.insert(track)
        if found.data != track.data or found.timeStamp != track.timeStamp:
            found.update(track.data)
            found.timeStamp = track.timeStamp
            self.update(found)
        return found.trackId

    def insert(self, track):
        t = self.table
        values = self.setupValues(track)
        stmt = t.insert().values(**values).returning(t.c.trackid)
        trackId = self.session.execute(stmt).first()[0]
        mark_changed(self.session)
        return trackId

    def update(self, track):
        t = self.table
        values = self.setupValues(track)
        if track.timeStamp is None:
            values['timestamp'] = datetime.now()
        stmt = t.update().values(**values).where(t.c.trackid == track.trackId)
        n = self.session.execute(stmt).rowcount
        if n > 0:
            mark_changed(self.session)
        return n

    def makeTrack(self, r):
        if r is None:
            return None
        return Track(*r[1:-2], trackId=r[0],timeStamp=r[-2], data=r[-1], storage=self)

    def setupWhere(self, crit):
        return [self.table.c[k.lower()] == v for k, v in crit.items()]

    def setupValues(self, track):
        values = {}
        hf = self.trackFactory.headFields
        for i, c in enumerate(self.headCols):
            values[c] = track.head[hf[i]]
        values['data'] = track.data
        if track.timeStamp is not None:
            values['timestamp'] = track.timeStamp
        return values

    def getTable(self, schema=None):
        if self.table is not None:
            return self.table
        #table = getExistingTable(self.engine, self.metadata, self.tableName)
        #if table is None:
        return createTable(self.engine, self.metadata, self.tableName, self.headCols, 
                           indexes=self.indexes)


def getExistingTable(engine, metadata, tableName):
        metadata.reflect(engine)
        return metadata.tables.get((schema and schema + '.' or '') + tableName)


def createTable(engine, metadata, tableName, headcols, indexes=None):
    cols = [Column('trackid', BigInteger, primary_key=True)]
    idxs = []
    for ix, f in enumerate(headcols):
        cols.append(Column(f.lower(), Text, nullable=False, server_default=''))
    cols.append(Column('timestamp', DateTime(timezone=True), 
                       nullable=False, server_default=func.now()))
    for ix, idef in enumerate(indexes):
        indexName = 'idx_%s_%d' % (tableName, (ix + 1))
        idxs.append(Index(indexName, *idef))
    idxs.append(Index('idx_%s_ts' % tableName, 'timestamp'))
    cols.append(Column('data', JSONB, nullable=False, server_default='{}'))
    table = Table(tableName, metadata, *(cols+idxs), extend_existing=True)
    metadata.create_all(engine)
    return table

