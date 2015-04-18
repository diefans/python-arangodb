"""
Some meta classes
"""

from collections import OrderedDict
from functools import wraps
from itertools import chain

from . import api, exc

import logging

LOG = logging.getLogger(__name__)


class MetaBase(type):

    """Collects and registers all classes, which need a session resp. an api.

    Provides a session property to the class, for that arangodb server connection.
    Provides also an api property mapping to the specific arango server url.
    """

    # global client factory
    __client_factory__ = None

    @classmethod
    def _set_global_client_factory(mcs, factory):
        """Set the global client factory."""

        # we take the method wrapper here
        mcs.__client_factory__ = factory.__get__

    def _set_client_factory(cls, factory):
        """Set the local client factory.
        This will only be available to subclasses of this class.
        """

        # we take the method wrapper here
        cls.__client_factory__ = factory.__get__

    @property
    def client(cls):
        """Resolves the client for this class."""

        if callable(cls.__client_factory__):
            return cls.__client_factory__(cls)()

        # just the default
        return api.SystemClient()

    @property
    def api(cls):
        raise NotImplementedError("There is no api implemented for the general meta class!")


class MetaDocumentBase(MetaBase):

    """Document type.

    Edges are also documents, but a little bit special...
    """

    # the collection name may deviate from class name
    __collection_name__ = None

    __document_base__ = None
    __documents__ = OrderedDict()

    def __init__(cls, name, bases, dct):
        super(MetaDocumentBase, cls).__init__(name, bases, dct)

        # stores an optional de/serializer
        cls.__objective__ = None

    def __new__(mcs, name, bases, dct):

        # we set our collection name to class name if not already done
        if '__collection_name__' not in dct:
            dct['__collection_name__'] = name

        cls = type.__new__(mcs, name, bases, dct)

        # register our base class or a document class
        mcs._register_class(cls)

        return cls

    @classmethod
    def _register_class(mcs, cls):
        if mcs.__document_base__ is None:
            mcs.__document_base__ = cls

        elif cls.__collection_name__ in mcs.__documents__:
            raise TypeError(
                "A collection with the same name `{0}`"
                " is already registered by {1}".format(
                    cls.__collection_name__,
                    mcs.__documents__[cls.__collection_name__]
                )
            )

        else:
            mcs.__documents__[cls.__collection_name__] = cls

    def _create_collection(cls):
        """Try to create a collection."""

        col_type = api.EDGE_COLLECTION if isinstance(cls, MetaEdgeBase) else api.DOCUMENT_COLLECTION

        col = cls.client.collections.get(cls.__collection_name__)
        if col is None:
            cls.client.collections.create(cls.__collection_name__, type=col_type)
            LOG.info("Created collection: %s", cls)

        else:
            # check if type is good
            if col['type'] != col_type:
                raise exc.ArangoException(
                    "An existing collection has the wrong type, solve this manually!",
                    col, cls)

        LOG.info("Collection in use: %s", cls)

    def _polymorph(cls, dct):
        """Just create a proper instance for this dct."""

        # dct must have an _id to infer collection
        if '_id' not in dct or '/' not in dct['_id']:
            raise TypeError("Cannot infer document type!", dct)

        collection, _ = dct['_id'].split('/')

        # lookup type
        if collection not in cls.__documents__:
            raise TypeError("Unknown document type!", collection)

        return cls.__documents__[collection](dct)

    @property
    def api(cls):
        return cls.client.documents


class MetaEdgeBase(MetaDocumentBase):

    """The edge type."""

    __edge_base__ = None
    __edges__ = OrderedDict()

    @classmethod
    def _register_class(mcs, cls):
        """Register edge base and classes."""

        if mcs.__edge_base__ is None:
            mcs.__edge_base__ = cls

        else:
            super(MetaEdgeBase, mcs)._register_class(cls)
            mcs.__edges__[cls.__collection_name__] = cls

    @property
    def api(cls):
        return cls.client.edges


class MetaCursorBase(MetaBase):

    @property
    def api(cls):
        return cls.client.cursors


class MetaIndexBase(MetaBase):

    @property
    def api(cls):
        return cls.client.indexes


class MetaQueryBase(MetaBase):

    @property
    def api(cls):
        return cls.client.queries


def find_collection_name(bases, document_cls):
    for base in bases:
        if issubclass(base, document_cls):
            # add collection name from "real" edge
            return base.__collection_name__

    raise TypeError("You have to specify an Edge base class!")


# we have to subclass from MetaEdgeBase
class MetaGraphEdge(MetaEdgeBase):
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
            cls.__collection_name__ = find_collection_name(bases, EdgeBase)

    @classmethod
    def _register_class(mcs, cls):
        if mcs.__graph_edge_base__ is None:
            mcs.__graph_edge_base__ = cls

        else:
            mcs.__graph_edges__[cls.__collection_name__] = cls

    @property
    def graph_api(cls):
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


class MetaGraphVertex(MetaDocumentBase):
    __graph_vertex_base__ = None
    __graph_vertices__ = {}

    def __init__(cls, name, bases, dct):
        super(MetaGraphVertex, cls).__init__(name, bases, dct)

        # resolve collection name
        if cls != cls.__graph_vertex_base__:
            cls.__collection_name__ = find_collection_name(bases, DocumentBase)

    @classmethod
    def _register_class(mcs, cls):
        if mcs.__graph_vertex_base__ is None:
            mcs.__graph_vertex_base__ = cls

        else:
            mcs.__graph_vertices__[cls.__collection_name__] = cls

    @property
    def graph_api(cls):
        client = cls.client

        return api.Edges(client.api(client.database, 'gharial', cls.__graph__.__graph_name__, 'vertex'))


class MetaGraph(MetaBase):
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
            if isinstance(attr, MetaGraphEdge) or isinstance(attr, MetaGraphVertex):
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


def polymorph(wrapped):
    """Decorate a function which returns a raw ArangoDB document to create a document class instance."""

    # TODO
    # polymorph should deal with graph context
    # so that a document gets becomes a vertex

    @wraps(wrapped)
    def decorator(*args, **kwargs):
        doc = wrapped(*args, **kwargs)

        return BaseDocument._polymorph(doc)         # pylint: disable=W0212

    return decorator


class BaseDocument(object):

    """Is an object, which is able to connect to a session."""

    __metaclass__ = MetaDocumentBase

    def __init__(self, *args, **kwargs):
        self.__data__ = {}

        # we have to call __setitem__
        self.update(*args, **kwargs)

    def __setitem__(self, key, value):
        self.__data__[key] = value

    def __getitem__(self, key):
        return self.__data__[key]

    def __delitem__(self, key):
        del self.__data__[key]

    def __contains__(self, key):
        return key in self.__data__

    def __iter__(self):
        return self.__data__.__iter__()

    def keys(self):
        return self.__data__.keys()

    def iteritems(self):
        return self.__data__.iteritems()

    def iterkeys(self):
        return self.__data__.iterkeys()

    def itervalues(self):
        return self.__data__.itervalues()

    def copy(self):
        clone = self.__class__()
        clone.__data__ = self.__data__.copy()
        return clone

    def update(self, *args, **kwargs):
        for key, value in dict(*args, **kwargs).iteritems():
            self[key] = value

    def get(self, key, default=None):
        if key in self:
            return self[key]

        return default

    @property
    def _id(self):
        return self['_id']

    @_id.setter
    def _id(self, value):
        self['_id'] = value

    @property
    def _key(self):
        return self['_key']

    @_key.setter
    def _key(self, value):
        self['_key'] = value

    @property
    def _rev(self):
        return self['_rev']

    @_rev.setter
    def _rev(self, value):
        self['_rev'] = value

    @classmethod
    def _iter_all(cls):
        """query all documents and create instances.

        This can be very big, use this just for debugging!
        """
        for _id in cls.api.get(cls.__collection_name__)['documents']:
            yield cls.load(_id)

    @classmethod
    def _create(cls, doc):
        """Create a db instance on the server."""

        return cls.api.create(cls.__collection_name__, doc)

    @classmethod
    def deserialize(cls, doc):
        """Take the deserializer to validate the document."""

        if cls.__objective__ is not None:
            return cls.__objective__.deserialize(doc)

        return doc.copy()

    def serialize(self):
        """Take the serializer to adjust the document."""

        if self.__objective__ is not None:
            return self.__objective__.serialize(self.__data__)

        return self.__data__.copy()

    @classmethod
    @polymorph
    def load(cls, key):
        """Just create a fresh instance by requesting the document by key."""

        if '/' in key:
            name, key = key.split('/')

        else:
            name = cls.__collection_name__

        doc = cls.api.get(name, key)

        return cls.deserialize(doc)

    def save(self):
        """Save the document to db or update."""

        serialized = self.serialize()

        # test for existing key
        if '_id' in self:
            # replace
            doc = self.__class__.api.replace(serialized, self['_id'])

        else:
            # create
            doc = self._create(serialized)

        # update self
        for k in ('_id', '_key', '_rev'):
            self[k] = doc[k]

    def delete(self):
        """Delete a document."""

        return self.__class__.api.delete(self['_id'])

    def __str__(self):
        return self.get(
            '_id',
            '/'.join(
                (self.__class__.__collection_name__, self.get('key', ''))
            )
        )


class DocumentBase(BaseDocument):

    """Just an arangodb document."""

    __metaclass__ = MetaDocumentBase


class EdgeBase(BaseDocument):

    __metaclass__ = MetaEdgeBase


class CursorBase(object):

    __metaclass__ = MetaCursorBase


class IndexBase(object):

    """An index representation."""

    __metaclass__ = MetaIndexBase


class GraphBase(object):

    """The base class for edges and vertices."""

    __graph__ = None

    @classmethod
    def _create(cls, doc):
        """Create a db instance on the server."""

        return cls.graph_api.create(cls.__collection_name__, doc)

    def save(self):
        """Save the document to db or update."""

        serialized = self.serialize()

        # test for existing key
        if '_id' in self:
            # replace
            doc = self.__class__.graph_api.replace(serialized, self['_id'])

        else:
            # create
            doc = self._create(serialized)

        # update self
        for k in ('_id', '_key', '_rev'):
            self[k] = doc[k]

    def delete(self):
        """Delete a document."""

        return self.__class__.graph_api.delete(self['_id'])
