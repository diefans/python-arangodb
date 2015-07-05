import inspect
from itertools import chain

from six import with_metaclass

from . import db, api, exc, meta


import logging
logging.basicConfig(level=logging.DEBUG)


class GraphEdge(with_metaclass(meta.MetaGraphEdge, meta.GraphBase)):
    pass


class GraphVertex(with_metaclass(meta.MetaGraphVertex, meta.GraphBase)):
    pass


class Graph(with_metaclass(meta.MetaGraph)):
    pass
