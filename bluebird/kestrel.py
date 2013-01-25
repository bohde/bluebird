from nydus.db.backends import BaseConnection
from thrift.transport.TTransport import TTransportException

from .client import Client

class Kestrel(BaseConnection):
    retryable_exceptions = frozenset([TTransportException])
    support_pipelines = False


    def __init__(self, num, host='localhost', port=2229, **options):
        self.host = host
        self.port = port
        super(Kestrel, self).__init__(num)


    @property
    def identifier(self):
        mapping = vars(self)
        return "kestrel://%(host)s:%(port)s/" % mapping


    def connect(self):
        return Client(self.host, self.port)


    def disconnect(self):
        pass

