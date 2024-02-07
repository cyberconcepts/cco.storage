# cco.storage.common

"""Common utility stuff for the cco.storage packages.
"""

from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.orm import scoped_session, sessionmaker
import threading
import zope.sqlalchemy


def getEngine(dbtype, dbname, user, pw, host='localhost', port=5432, **kw):
    return create_engine('%s://%s:%s@%s:%s/%s' % (
        dbtype, user, pw, host, port, dbname), **kw)

def sessionFactory(engine):
    Session = scoped_session(sessionmaker(bind=engine, twophase=True))
    zope.sqlalchemy.register(Session)
    return Session

# put something like this in code before first creating a Storage object
#engine = getEngine('postgresql+psycopg', 'ccotest', 'ccotest', 'cco')
#cco.storage.common.engine = engine
#cco.storage.common.Session = sessionFactory(engine)


class Storage(object):

    def __init__(self, schema=None):
        self.engine = engine
        self.session = Session()
        self.schema = schema
        self.metadata = MetaData(schema=schema)
        self.containers = {}

    def create(self, cls):
        container = cls(self)
        self.add(container)
        return container

    def add(self, container):
        self.containers[container.itemFactory.prefix] = container

    def getItem(self, uid):
        prefix, id = uid.split('-')
        id = int(id)
        container = self.containers.get(prefix)
        if container is None:
            container = self.create(registry[prefix])
        return container.get(id)

    def getExistingTable(self, tableName):
        metadata = self.metadata
        schema = self.schema
        metadata.reflect(self.engine)
        return metadata.tables.get((schema and schema + '.' or '') + tableName)

    def dropTable(self, tableName):
        with self.engine.begin() as conn:
            conn.execute(text('drop table if exists %s.%s' % (self.schema, tableName)))

    def resetSequence(self, tableName, colName, v):
        sq = ('alter sequence %s.%s_%s_seq restart %i' % 
                (self.schema, tableName, colName, v))
        with self.engine.begin() as conn:
            conn.execute(text(sq))


# store information about container implementations, identified by a uid prefix.

registry = {}

def registerContainerClass(cls):
    # TODO: error on duplicate key
    registry[cls.itemFactory.prefix] = cls
    cls.headCols = cols = tuple(f.lower() for f in cls.itemFactory.headFields)
    if cls.indexes is None:
        cls.indexes = [cols[i:] for i in range(len(cols))]
    return cls

