import inspect
from itertools import chain

from . import db, api, exc, meta


import logging
logging.basicConfig(level=logging.DEBUG)


class GraphBase(object):

    """The base class for edges and vertices."""

    __graph__ = None


class GraphEdge(GraphBase):
    __metaclass__ = meta.MetaGraphEdge


class GraphVertex(GraphBase):
    __metaclass__ = meta.MetaGraphVertex


class Graph(object):
    __metaclass__ = meta.MetaGraph
