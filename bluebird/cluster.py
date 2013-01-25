
from nydus.db.base import BaseCluster

from contextlib import contextmanager

class Cluster(BaseCluster):
    @contextmanager
    def pipe(self):
        yield self.get_conn(self, None)


