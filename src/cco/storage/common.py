# cco.storage.common

"""Common utility stuff for the cco.storage packages.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import threading
from zope.sqlalchemy import register


def getEngine(dbtype, dbname, user, pw, host='localhost', port=5432, **kw):
    return create_engine('%s://%s:%s@%s:%s/%s' % (
        dbtype, user, pw, host, port, dbname), **kw)

def sessionFactory(engine):
    Session = scoped_session(sessionmaker(bind=engine, twophase=True))
    register(Session)
    return Session


class Context(object):

    def __init__(self, engine, schema=None):
        self.engine = engine
        self.Session = sessionFactory(engine)
        self.schema = schema
        self.storages = {}

    def create(self, cls):
        storage = cls(self)
        self.add(storage)
        return storage

    def add(self, storage):
        self.storages[storage.itemFactory.prefix] = storage

    def getItem(self, uid):
        prefix, id = uid.split('-')
        id = int(id)
        storage = self.storages.get(prefix)
        if storage is None:
            storage = self.create(storageRegistry[prefix])
        return storage.get(id)


# store information about storage implementations, identified by a uid prefix.

storageRegistry = {}

def registerStorage(cls):
    # TODO: error on duplicate key
    storageRegistry[cls.itemFactory.prefix] = cls
    return cls

