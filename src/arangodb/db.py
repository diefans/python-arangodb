"""Some classes to easy work with arangodb."""

from functools import partial
from itertools import starmap

from collections import OrderedDict

import logging

LOG = logging.getLogger(__name__)

from . import api, exc


class MetaBase(type):

    """Collects and registers all classes, which need a session resp. an api.

    Provides a session property to the class, for that arangodb server connection.
    Provides also an api property mapping to the specific arango server url.
    """

    # just to identify the root of all roots
    __base__ = None

    # a collection of all classes we will deal with
    __classes__ = OrderedDict()

    # global client factory
    __client_factory__ = None

    # the collection name may deviate from class name
    __collection_name__ = None

    def __new__(mcs, name, bases, dct):

        # we set our collection name to class name if not already done
        if '__collection_name__' not in dct:
            dct['__collection_name__'] = name

        cls = type.__new__(mcs, name, bases, dct)

        mcs._register_class(cls)

        return cls

    @classmethod
    def _register_class(mcs, cls):
        if mcs.__base__ is None:
            mcs.__base__ = cls

        else:
            mcs.__classes__[cls.__collection_name__] = cls

    @classmethod
    def set_client_factory(mcs, factory):
        """Set the client factory."""

        mcs.__client_factory__ = factory

    @property
    def client(cls):
        """Resolves the client for this class."""

        if callable(cls.__client_factory__):
            return cls.__client_factory__()

        # just the default
        return api.SystemClient()

    @property
    def api(cls):
        raise NotImplementedError("There is no api implemented for the general meta class!")

    def __polymorph__(cls, dct):
        """Just create a proper instance for this dct."""

        # dct must have an _id to infer collection
        if '_id' not in dct or '/' not in dct['_id']:
            raise TypeError("Cannot infer document type!", dct)

        collection, _ = dct['_id'].split('/')

        # lookup type
        if collection not in cls.__classes__:
            raise TypeError("Unknown document type!", collection)

        return cls.__classes__[collection](dct)


class MetaDocumentBase(MetaBase):

    """Document type.

    Edges are also documents, but a little bit special...
    """

    documents = {}
    document_base = None

    objective = None

    @classmethod
    def _register_class(mcs, cls):
        if mcs.document_base is None:
            mcs.document_base = cls

        else:
            MetaBase._register_class(cls)
            mcs.documents[cls.__collection_name__] = cls

    @property
    def api(cls):
        return cls.client.documents

    def deserialize(cls, doc):
        if cls.objective is not None:
            return cls.objective.deserialize(doc)

        return doc

    def serialize(cls, doc):
        if cls.objective is not None:
            return cls.objective.serialize(doc)

        return doc

    def create_collection(cls):
        """Try to create a collection."""

        col_type = api.DOCUMENT_COLLECTION if issubclass(cls, Document) else api.EDGE_COLLECTION

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


class MetaEdgeBase(MetaDocumentBase):

    """The edge type."""

    edges = {}
    edge_base = None

    @classmethod
    def _register_class(mcs, cls):
        if mcs.edge_base is None:
            mcs.edge_base = cls

        else:
            MetaDocumentBase._register_class(cls)
            mcs.edges[cls.__collection_name__] = cls

    @property
    def api(cls):
        return cls.client.edges


class BaseDocument(dict):

    """Is an object, which is able to connect to a session."""

    __metaclass__ = MetaBase

    @classmethod
    def load(cls, key):
        """Just create a fresh instance by requesting the document by key."""

        if '/' in key:
            name, key = key.split('/')

        else:
            name = cls.__collection_name__

        raw_doc = cls.api.get(name, key)
        doc = cls.deserialize(raw_doc)

        # lookup of document collection to identify proper class
        return cls.__polymorph__(doc)

    @classmethod
    def find(cls, **kwargs):
        return Cursor.find(cls, **kwargs).iter_documents()

    @classmethod
    def _iter_all(cls):
        """query all documents and create instances.

        This can be very big, use this just for debugging!
        """
        for _id in cls.api.get(cls.__collection_name__)['documents']:
            yield cls.load(_id)

    def _create(self):
        """Create a db instance."""

        doc = self.__class__.serialize(self)

        return self.__class__.api.create(self.__class__.__collection_name__, doc)

    def save(self):
        """Save the document to db or update."""

        # test for existing key
        if '_id' in self:
            # replace
            ser_doc = self.__class__.serialize(self)
            doc = self.__class__.api.replace(ser_doc, self['_id'])

        else:
            # create
            doc = self._create()

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


class Edge(BaseDocument):

    """An edge between two documents.

    When the edge is loaded the two dowuments are also loaded.
    """

    class Direction:
        any = 'any'
        inbound = 'inbound'
        outbound = 'outbound'

    __metaclass__ = MetaEdgeBase

    def __init__(self, *args, **kwargs):
        """
        call scheme:

            Edge([_from, _to,] [iterable,] **kwargs)

        If _from and _to are not given, they have to be set later before saving!
        """

        # split args
        args_len = len(args)

        if args_len < 2:
            # _to and _from must be in kwargs or args
            pass

        else:
            self['_from'], self['_to'] = args[:2]
            args = args[2:]

        # else:
            # pass

        super(Edge, self).__init__(*args, **kwargs)

    @classmethod
    def load(cls, key):
        """Load the edge and connected documents."""

        edge = super(Edge, cls).load(key)

        edge['_from'] = Document.load(edge['_from'])
        edge['_to'] = Document.load(edge['_to'])

        return edge

    def _create(self):
        """Create a db instance."""

        if '_from' not in self or '_to' not in self:
            # _from and _to must have been set now
            raise TypeError("You must create an edge ether by calling __init__ "
                            "with _from and _to args or an appropriate dict!")

        return self.__class__.api.create(
            self.__class__.__collection_name__,
            self['_from'],
            self['_to'],
            self)

    @classmethod
    def connections(cls, document, direction=None):
        query = """
            FOR p IN PATHS(@@doc, @@edge, @direction)
                FILTER p.source._id == @doc_id && LENGTH(p.edges) == 1
                RETURN
                    p.destination
        """
        params = {

            "@doc": document.__class__.__collection_name__,
            "@edge": cls.__collection_name__,
            "direction": direction or Edge.Direction.any,
            "doc_id": document['_id']
        }

        return Cursor(query, params).iter_documents()

    @classmethod
    def inbounds(cls, document):
        return cls.connections(document, direction=Edge.Direction.inbound)

    @classmethod
    def outbounds(cls, document):
        return cls.connections(document, direction=Edge.Direction.outbound)


class Document(BaseDocument):

    """Just an arangodb document."""

    __metaclass__ = MetaDocumentBase


class MetaCursorBase(MetaBase):

    @property
    def api(cls):
        return cls.client.cursors


class Cursor(object):

    """A cursor is created to perform queries."""

    __metaclass__ = MetaCursorBase

    def __init__(self, query, bind=None, **kwargs):
        self.query = query
        self.bind = bind
        self.kwargs = kwargs

    @classmethod
    def find(cls, doc, **kwargs):
        """Very simple filter query for a collection.

        :param filter: concatenated by AND
        """
        if isinstance(doc, basestring):
            collection = doc

        elif issubclass(doc, BaseDocument):
            collection = doc.__collection_name__

        else:
            raise TypeError(":param doc: must be a string or a BaseDocument.")

        obj = 'obj'

        tmpl = """FOR {obj} IN {collection} {filter} RETURN {obj}""".format
        tmpl_bind = partial('{obj}.`{0}` == @param_{0}'.format, obj=obj)

        filter_str = ''

        if kwargs:

            filter_str = ' '.join(
                (
                    'FILTER',
                    # we sort attributes by name
                    ' AND '.join(starmap(tmpl_bind, sorted(kwargs.iteritems())))
                )
            )

        return cls(tmpl(collection=collection, filter=filter_str, obj=obj),
                   bind={'param_{}'.format(k): v for k, v in kwargs.iteritems()})

    def iter_result(self):
        """Iterate over all batches of result."""

        cursor = self.__class__.api.create(self.query, bind=self.bind, **self.kwargs)

        while cursor['result']:
            for result in cursor['result']:
                yield result

            if not cursor['hasMore']:
                # step out
                break

            # fetch next batch
            cursor = self.__class__.api.pursue(cursor['id'])

    def iter_documents(self):
        """If you expect document instances to be returned from the cursor, call this to instantiate them."""

        for doc in self.iter_result():
            yield self.__class__.__polymorph__(doc)
