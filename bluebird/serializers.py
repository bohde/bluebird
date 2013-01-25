try:
    from cPickle import dumps, loads
except ImportError:
    from pickle import dumps, loads


class Pickle(object):
    def serialize(self, obj):
        return dumps(obj, -1)

    def deserialize(self, obj):
        return loads(obj)

