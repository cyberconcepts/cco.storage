# cco.storage.proxy.core

"""Core classes and helper functions for creating proxy and adapter objects
for storing attribute values in a SQL database.
"""

import transaction

_not_found = object()


def loadData(obj):
    print ('getData ***', obj.context.__name__, obj.context.__parent__.__name__)
    return dict(dummy='dummy')


def storeData(obj, data):
    print ('storeData ***', obj.context.__name__, obj.context.__parent__.__name__, data)


class AdapterBase(object):

    _old_data = None
    _cont = None
    _id = None

    def __init__(self, context):
        super(AdapterBase, self).__init__(context)
        object.__setattr__(self, '_new_data', {})

    def __getattr__(self, attr):
        value = self._new_data.get(attr, _not_found)
        if value is _not_found:
            if self._old_data is None:
                object.__setattr__(self, '_old_data', loadData(self))
            value = self._old_data.get(attr, _not_found)
        if value is _not_found:
            return super(AdapterBase, self).__getattr__(attr)
        return value

    def __setattr__(self, attr, value):
        super(AdapterBase, self).__setattr__(attr, value)
        if attr.startswith('__') or attr in self._adapterAttributes:
            return
        if not self._new_data:
            tr = transaction.manager.get()
            tr.addBeforeCommitHook(storeData, [self, self._new_data], {})
        self._new_data[attr] = value
