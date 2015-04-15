import inspect
from itertools import chain

from . import db, api, exc, meta


import logging
logging.basicConfig(level=logging.DEBUG)


class GraphEdge(meta.GraphBase):
    __metaclass__ = meta.MetaGraphEdge


class GraphVertex(meta.GraphBase):
    __metaclass__ = meta.MetaGraphVertex


class Graph(object):
    __metaclass__ = meta.MetaGraph
