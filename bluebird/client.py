from contextlib import contextmanager
from Queue import LifoQueue

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol

from .thrift_kestrel import Kestrel
from .serializers import Pickle
from .exceptions import NoneFound


def maybe_seconds_to_millis(t):
    return t and int(t * 1000)


class Queue(object):
    def __init__(self, name, client):
        self.client = client
        self.name = name


    def put(self, item, **kwargs):
        """
        Put an item on the queue.
        Optionally, expire this item after `expiration` seconds.
        """
        return self.extend(item, **kwargs)


    def extend(self, items, **kwargs):
        """
        Extend the queue with a list of items.
        Optionally, expire these items after `expiration` seconds.
        """
        expiration = kwargs.get('expiration', None)
        expiration = maybe_seconds_to_millis(expiration)

        serialized = map(self.client.serialize, items)

        with self.client.conn() as conn:
            return conn.put(self.name, serialized, expiration)


    def get(self, timeout=None, abort_after=None):
        """
        Get an item from the queue.
        """
        return self.multi_get(1, timeout=timeout, abort_after=abort_after)


    def multi_get(self, n, timeout=None, abort_after=None):
        """
        Get up to n items from the queue.
        """
        timeout = maybe_seconds_to_millis(timeout)
        abort_after = maybe_seconds_to_millis(abort_after)

        with self.client.conn() as conn:
            results = conn.get(self.name, n, timeout, abort_after)

        if not results:
            raise NoneFound(self.name)

        return map(self.client.deserialize, results)


    def confirm(self, *ids):
        """
        Confirm the items as processed.
        """
        with self.client.conn() as conn:
            conn.confirm(self.name, ids)


    def abort(self, *ids):
        """
        Abort the processing of those items, allowing other consumers
        to process them.
        """
        with self.client.conn() as conn:
            conn.abort(self.name, ids)


    def peek(self):
        """
        Look at the next item in the queue, without reserving it.
        """
        with self.client.conn() as conn:
            obj = conn.peek(self.name)
        return self.client.serialize(obj)


    def flush(self):
        """
        Delete all data in this queue.
        """
        with self.client.conn() as conn:
            return conn.flush_queue(self.name)


    def delete(self):
        """
        Delete this queue and all data in it.
        """
        with self.client.conn() as conn:
            return conn.delete_queue(self.name)


class Client(object):
    def __init__(self, host='localhost', port=2229, serializer=None, size=1):
        self.serializer = serializer or Pickle()
        self.host = host
        self.port = port
        self.size = size
        self._socket_queue = None


    def __unicode__(self):
        return 'kestrel://%(host)s:%(port)s/' % {
            'host': self.host,
            'port': self.port
        }


    def __repr__(self):
        return 'Client(host=%(host), port=%(port))' % {
            'host': self.host,
            'port': self.port
        }

    @property
    def socket_queue(self):
        if self._socket_queue is None:
          self._socket_queue = LifoQueue()

          for i in xrange(self.size):
              socket = TSocket.TSocket(self.host, self.port)
              transport = TTransport.TFramedTransport(socket)
              self._socket_queue.put(transport)
        return self._socket_queue


    def queue(self, name):
        return Queue(name, self)


    def __getitem__(self, name):
        return self.queue(name)


    @contextmanager
    def conn(self):
        transport = self.socket_queue.get()
        if not transport.isOpen():
            transport.open()

        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        yield Kestrel.Client(protocol)

        self.socket_queue.put(transport)


    def serialize(self, obj):
        return self.serializer.serialize(obj)


    def deserialize(self, item):
        item.data = self.serializer.deserialize(item.data)
        return item


    def status(self):
        """
        Get the status code of the server
        """
        with self.conn() as conn:
            return conn.current_status()


    def version(self):
        """
        Get the server version
        """
        with self.conn() as conn:
            return conn.get_version()


    def flush_all(self):
        """
        Delete all data in all queues
        """
        with self.conn() as conn:
            return conn.flush_all_queues()
