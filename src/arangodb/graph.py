import inspect
from itertools import chain

from . import db, api, exc


import logging
logging.basicConfig(level=logging.DEBUG)


class GraphBase(object):

    """The base class for edges and vertices."""

    __graph__ = None


def find_collection_name(bases, document_cls):
    for base in bases:
        if issubclass(base, document_cls):
            # add collection name from "real" edge
            return base.__collection_name__

    raise TypeError("You have to specify an Edge base class!")


# we have to subclass from MetaEdgeBase
class MetaGraphEdge(db.MetaEdgeBase):
    __graph_edge_base__ = None
    __graph_edges__ = {}

    __from_vertices__ = []
    __to_vertices__ = []

    def __init__(cls, name, bases, dct):
        super(MetaGraphEdge, cls).__init__(name, bases, dct)

        # init vertices
        cls.__from_vertices__ = []
        cls.__to_vertices__ = []

        # resolve collection name
        if cls != cls.__graph_edge_base__:
            cls.__collection_name__ = find_collection_name(bases, db.Edge)

    @classmethod
    def _register_class(mcs, cls):
        if mcs.__graph_edge_base__ is None:
            mcs.__graph_edge_base__ = cls

        else:
            mcs.__graph_edges__[cls.__collection_name__] = cls

    @property
    def api(cls):
        client = cls.client

        return api.Edges(client.api(client.database, 'gharial', cls.__graph__.__graph_name__, 'edge'))

    def from_vertex(cls, vertex):
        if vertex not in cls.__from_vertices__:
            cls.__from_vertices__.append(vertex)

        return vertex

    def to_vertex(cls, vertex):
        if vertex not in cls.__to_vertices__:
            cls.__to_vertices__.append(vertex)

        return vertex

    @property
    def __definition__(cls):
        return {
            "collection": cls.__collection_name__,
            "from": [col.__collection_name__ for col in cls.__from_vertices__],
            "to": [col.__collection_name__ for col in cls.__to_vertices__]
        }


class GraphEdge(GraphBase):
    __metaclass__ = MetaGraphEdge


class MetaGraphVertex(db.MetaDocumentBase):
    __graph_vertex_base__ = None
    __graph_vertices__ = {}

    def __init__(cls, name, bases, dct):
        super(MetaGraphVertex, cls).__init__(name, bases, dct)

        # resolve collection name
        if cls != cls.__graph_vertex_base__:
            cls.__collection_name__ = find_collection_name(bases, db.Document)

    @classmethod
    def _register_class(mcs, cls):
        if mcs.__graph_vertex_base__ is None:
            mcs.__graph_vertex_base__ = cls

        else:
            mcs.__graph_vertices__[cls.__collection_name__] = cls

    @property
    def api(cls):
        client = cls.client

        return api.Edges(client.api(client.database, 'gharial', cls.__graph__.__graph_name__, 'vertex'))


class GraphVertex(GraphBase):
    __metaclass__ = MetaGraphVertex


class MetaGraph(db.MetaBase):
    __graph_base__ = None
    __graphs__ = {}

    __graph_name__ = None

    def __new__(mcs, name, bases, dct):

        cls = type.__new__(mcs, name, bases, dct)

        if mcs.__graph_base__ is None:
            mcs.__graph_base__ = cls

        else:
            mcs.__graphs__[cls.__name__] = cls

        return cls

    def __init__(cls, name, bases, dct):
        super(MetaGraph, cls).__init__(name, bases, dct)

        cls.__graph_edges__ = {}
        cls.__graph_vertices__ = {}

        # graph name is always the first none base
        if cls.__graph_base__ is not cls and cls.__graph_name__ is None:
            cls.__graph_name__ = name

        # lookup graph classes to attach graph specific api proxy
        for name in dir(cls):

            attr = getattr(cls, name)
            if inspect.isclass(attr) and issubclass(attr, GraphBase):
                attr.__graph__ = cls

                # collect graph edges and vertices
                if isinstance(attr, MetaGraphEdge):
                    cls.__graph_edges__[attr.__collection_name__] = attr

                else:
                    cls.__graph_vertices__[attr.__collection_name__] = attr

    @property
    def __definition__(cls):
        """ArangoDB graph definition."""

        # orphans are not in an edge definition
        in_edge = set()

        edge_definitions = []
        for edge in cls.__graph_edges__.itervalues():
            edge_definitions.append(edge.__definition__)
            in_edge.update(set(edge.__from_vertices__) | set(edge.__to_vertices__))

        orphans = [
            name
            for name, vertice in cls.__graph_vertices__.iteritems()
            if vertice not in in_edge
        ]

        return {
            'name': cls.__graph_name__,
            'edgeDefinitions': edge_definitions,
            'orphanCollections': orphans
        }

    @property
    def api(cls):
        return cls.client.graphs

    def _create_graph(cls):
        # first create all graph collections
        for edge in chain(
                cls.__graph_edges__.itervalues(),
                cls.__graph_vertices__.itervalues()
        ):
            edge._create_collection()       # pylint: disable=W0212

        # check for existing graph
        try:
            definition = cls.api.get(cls.__graph_name__)

            if definition != cls.__definition__:
                # TODO replace old with new definition
                pass

        except exc.GraphNotFound:
            # post graph definition
            return cls.api.create(cls.__definition__)


class Graph(object):

    __metaclass__ = MetaGraph
