from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol

from .thrift_kestrel import Kestrel
from .serializers import Pickle


def maybe_seconds_to_millis(t):
    return t and int(t * 1000)


class Client(object):
    def __init__(self, host, port, serializer=None):
        self.serializer = serializer or Pickle()
        self.host = host
        self.port = port


    def open_connection(self, host, port):
        socket = TSocket.TSocket(host, port)
        transport = TTransport.TFramedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        transport.open()
        return Kestrel.Client(protocol)


    @property
    def conn(self):
        if not hasattr(self, '_conn'):
            self._conn = self.open_connection(self.host, self.port)
        return self._conn


    def serialize(self, obj):
        return self.serializer.serialize(obj)


    def deserialize(self, item):
        item.data = self.serializer.deserialize(item.data)
        return item


    def put(self, queue_name, *items, **kwargs):
        """
        Parameters:
         - queue_name
         - items
         - expiration_msec
        """
        expiration = kwargs.get('expiration', None)
        expiration = maybe_seconds_to_millis(expiration)

        serialized = map(self.serialize, items)

        return self.conn.put(queue_name, serialized, expiration)


    def get(self, queue_name, max_items=1, timeout=None, abort_after=None):
        """
        Parameters:
         - queue_name
         - max_items
         - timeout_msec
         - auto_abort_msec
        """
        timeout = maybe_seconds_to_millis(timeout)
        abort_after = maybe_seconds_to_millis(abort_after)

        results = self.conn.get(queue_name, max_items, timeout, abort_after)

        return map(self.deserialize, results)


    def confirm(self, queue_name, *ids):
        """
        Parameters:
         - queue_name
         - ids
        """
        self.conn.confirm(queue_name, ids)


    def abort(self, queue_name, *ids):
        """
        Parameters:
         - queue_name
         - ids
        """
        self.conn.abort(queue_name, ids)


    def peek(self, queue_name):
        """
        Parameters:
         - queue_name
        """
        return self.serialize(self.conn.peek(queue_name))


    def flush(self, queue_name):
        """
        Parameters:
         - queue_name
        """
        return self.conn.flush_queue(queue_name)


    def flush_all(self):
        return self.conn.flush_all_queues()


    def delete(self, queue_name):
        """
        Parameters:
        - queue_name
        """
        return self.conn.delete_queue(queue_name)


    def status(self):
        return self.conn.current_status()


    def version(self):
        return self.conn.get_version()
