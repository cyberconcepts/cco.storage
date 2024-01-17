# cco.storage.tracking

"""SQL-based storage for simple tracks (records). 

A track consists of a head (index data, metadata)  with a fixed set of fields and 
data (payload) represented as a dict.
"""

from datetime import datetime
from sqlalchemy import Table, Column, Index
from sqlalchemy import BigInteger, DateTime, Text, func
from sqlalchemy import and_
from sqlalchemy.dialects.postgresql import JSONB
import transaction
from zope.sqlalchemy import register, mark_changed

from cco.storage.common import registerContainerClass


class Track(object):
    
    headFields = ['taskId', 'userName']
    prefix = 'rec'

    def __init__(self, *keys, **kw):
        self.head = {}
        for ix, k in enumerate(keys):
            self.head[self.headFields[ix]] = k
        for k in self.headFields:
            if self.head.get(k) is None:
                self.heaad[k] = ''
            setattr(self, k, self.head[k])
        self.data = kw.get('data') or {}
        self.timeStamp = kw.get('timeStamp')
        self.trackId = kw.get('trackId')
        self.container = kw.get('container')

    def update(self, data, overwrite=False):
        if data is None:
            return
        if overwrite:
            self.data = data
        else:
            self.data.update(data)

    @property
    def uid(self):
        if self.trackId is None:
            return None
        return '%s-%d' % (self.prefix, self.trackId)


@registerContainerClass
class Container(object):

    itemFactory = Track
    tableName = 'tracks'
    insertOnChange = True # always insert new track when data are changed
    indexes = None  # default, will be overwritten by registerContainerClass()
    #indexes = [('username',), ('taskid', 'username')] # or put explicitly in class

    table = None

    def __init__(self, storage):
        self.storage = storage
        self.session = storage.Session()
        self.table = self.getTable()

    def get(self, trackId):
        stmt = self.table.select().where(self.table.c.trackid == trackId)
        return self.makeTrack(self.session.execute(stmt).first())

    def query(self, **crit):
        stmt = self.table.select().where(
                and_(*self.setupWhere(crit))).order_by(self.table.c.trackid)
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
        if self.insertOnChange and found.data != track.data:
            return self.insert(track)
        if found.data != track.data or found.timeStamp != track.timeStamp:
            found.update(track.data)
            found.timeStamp = track.timeStamp
            self.update(found)
        return found.trackId

    def insert(self, track, withTrackId=False):
        t = self.table
        values = self.setupValues(track, withTrackId)
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

    def upsert(self, track):
        """Try to update the record identified by the trackId given with ``track``. 
        If not found insert new record without generating a new trackId.
        Use this method for migration and other bulk insert/update tasks.
        Don't forget to update the trackid sequence afterwards: 
        ``select setval('<schema>.tracks_trackid_seq', <max>);``"""
        if track.trackId is not None:
            if self.update(track) > 0:
                return track.trackId
        return self.insert(track, withTrackId=True)

    def remove(self, trackId):
        stmt = self.table.delete().where(self.table.c.trackid == trackId)
        n = self.session.execute(stmt).rowcount
        if n > 0:
            mark_changed(self.session)
        return n

    def makeTrack(self, r):
        if r is None:
            return None
        return self.itemFactory(
                *r[1:-2], trackId=r[0],timeStamp=r[-2], data=r[-1], container=self)

    def setupWhere(self, crit):
        return [self.table.c[k.lower()] == v for k, v in crit.items()]

    def setupValues(self, track, withTrackId=False):
        values = {}
        hf = self.itemFactory.headFields
        for i, c in enumerate(self.headCols):
            values[c] = track.head[hf[i]]
        values['data'] = track.data
        if track.timeStamp is not None:
            values['timestamp'] = track.timeStamp
        if withTrackId and track.trackId is not None:
            values['trackid'] = track.trackId
        return values

    def getTable(self):
        #table = getExistingTable(self.storage, self.tableName)
        #if table is None:
        return createTable(self.storage, self.tableName, self.headCols, 
                           indexes=self.indexes)


def getExistingTable(storage, tableName):
    metadata = storage.metadata
    schema = storage.schema
    metadata.reflect(storage.engine)
    return metadata.tables.get((schema and schema + '.' or '') + tableName)


def createTable(storage, tableName, headcols, indexes=None):
    metadata = storage.metadata
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
    metadata.create_all(storage.engine)
    return table

