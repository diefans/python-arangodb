"""Some classes to easy work with arangodb."""

from functools import partial, wraps
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


def polymorph(wrapped):
    """Decorate a function which returns a raw ArangoDB document to create a document class instance."""

    @wraps(wrapped)
    def decorator(*args, **kwargs):
        doc = wrapped(*args, **kwargs)

        return BaseDocument._polymorph(doc)         # pylint: disable=W0212

    return decorator


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


class BaseDocument(dict):

    """Is an object, which is able to connect to a session."""

    __metaclass__ = MetaDocumentBase

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
            return self.__objective__.serialize(self)

        return self.copy()

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

    @classmethod
    def find(cls, **kwargs):
        return Cursor.find(cls, **kwargs).iter_documents()

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


EDGE_DIRECTION_ANY = 'any'
EDGE_DIRECTION_INBOUND = 'inbound'
EDGE_DIRECTION_OUTBOUND = 'outbound'


class Edge(BaseDocument):

    """An edge between two documents.

    When the edge is loaded the two dowuments are also loaded.
    """

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

    def __setitem__(self, key, value):
        if key in ('_from', '_to') and isinstance(value, BaseDocument):
            # check for documents and reduce to _id
            if '_id' not in value:
                raise TypeError(
                    "The document for setting `{0}` has no `_id`: {1}"
                    .format(key, value)
                )

            super(Edge, self).__setitem__(key, value['_id'])

        else:
            super(Edge, self).__setitem__(key, value)

    @classmethod
    def _create(cls, doc):
        """Create a db instance."""

        if '_from' not in doc or '_to' not in doc:
            # _from and _to must have been set now
            raise TypeError("You must create an edge ether by calling __init__ "
                            "with _from and _to args or an appropriate dict!")

        return cls.api.create(
            cls.__collection_name__,
            doc['_from'],
            doc['_to'],
            doc)

    @classmethod
    def load(cls, key):
        """Load the edge and connected documents."""

        edge = super(Edge, cls).load(key)

        edge['_from'] = Document.load(edge['_from'])
        edge['_to'] = Document.load(edge['_to'])

        return edge

    @classmethod
    def connections(cls, document, direction=EDGE_DIRECTION_ANY):
        assert isinstance(document, BaseDocument), "document is Document or Edge"

        query = """
            FOR p IN PATHS(@@doc, @@edge, @direction)
                FILTER p.source._id == @doc_id && LENGTH(p.edges) == 1
                RETURN
                    p.destination
        """
        params = {

            "@doc": document.__class__.__collection_name__,
            "@edge": cls.__collection_name__,
            "direction": direction,
            "doc_id": document['_id']
        }

        return Cursor(query, params).iter_documents()

    @classmethod
    def inbounds(cls, document):
        return cls.connections(document, direction=EDGE_DIRECTION_INBOUND)

    @classmethod
    def outbounds(cls, document):
        return cls.connections(document, direction=EDGE_DIRECTION_OUTBOUND)


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
            yield BaseDocument._polymorph(doc)
