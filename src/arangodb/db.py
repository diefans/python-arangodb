"""Some classes to easy work with arangodb."""

from functools import partial, wraps
from itertools import starmap
from collections import OrderedDict

from . import api, exc

import logging

LOG = logging.getLogger(__name__)


EDGE_DIRECTION_ANY = 'any'
EDGE_DIRECTION_INBOUND = 'inbound'
EDGE_DIRECTION_OUTBOUND = 'outbound'


class MetaEdgeBase(api.MetaDocumentBase):

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


class Edge(api.BaseDocument):

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
            kwargs['_from'], kwargs['_to'] = args[:2]
            args = args[2:]

        # else:
            # pass

        super(Edge, self).__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        if key in ('_from', '_to') and isinstance(value, api.BaseDocument):
            # check for documents and reduce to _id
            if '_id' not in value:
                raise TypeError(
                    "The document for setting `{0}` has no `_id`: {1}"
                    .format(key, value)
                )

            super(Edge, self).__setitem__(key, value['_id'])

        else:
            super(Edge, self).__setitem__(key, value)

    @property
    def _from(self):
        return self['_from']

    @_from.setter
    def _from(self, value):
        self['_from'] = value

    @property
    def _to(self):
        return self['_to']

    @_to.setter
    def _to(self, value):
        self['_to'] = value

    @classmethod
    def _create(cls, doc):
        """Create a db instance."""

        assert '_from' in doc and '_to' in doc, \
            "You must create an edge ether by calling __init__ " \
            "with _from and _to args or an appropriate dict!"

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
    def connections(cls, document, collection=None, direction=EDGE_DIRECTION_ANY):
        assert isinstance(document, api.BaseDocument), "document is Document or Edge"

        filters = [
            "p.source._id == @doc_id",
            "LENGTH(p.edges) == 1",
        ]

        params = {

            "@doc": document.__class__.__collection_name__,
            "@edge": cls.__collection_name__,
            "direction": direction,
            "doc_id": document['_id']
        }

        if collection:
            params['collection'] = collection.__collection_name__
            filters.append("FIND_FIRST(p.destination._id, @collection) == 0")

        query = """
            FOR p IN PATHS(@@doc, @@edge, @direction)
                FILTER  {filters}
                RETURN
                    p.destination
        """.format(filters=' && '.join(filters))

        return Cursor(query, params).iter_documents()

    @classmethod
    def inbounds(cls, document, collection=None):
        return cls.connections(document, collection, direction=EDGE_DIRECTION_INBOUND)

    @classmethod
    def outbounds(cls, document, collection=None):
        return cls.connections(document, collection, direction=EDGE_DIRECTION_OUTBOUND)


class Document(api.BaseDocument):

    """Just an arangodb document."""

    __metaclass__ = api.MetaDocumentBase


class MetaCursorBase(api.MetaBase):

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

        elif issubclass(doc, api.BaseDocument):
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
            yield api.BaseDocument._polymorph(doc)

    def first_document(self):
        for doc in self.iter_documents():
            return doc


class MetaIndexBase(api.MetaBase):

    @property
    def api(cls):
        return cls.client.indexes


class Index(object):

    """An index representation."""

    __metaclass__ = MetaIndexBase

    collection = None
    index_type = None
    unique = False


INDEX_TYPE_HASH = "hash"


class Hash(Index):

    """A hash index."""

    collection = None
    index_type = INDEX_TYPE_HASH

    # unique can be setted at class level
    unique = False

    def __init__(self, *fields, **kwargs):
        self.fields = fields

        if "unique" in kwargs:
            self.unique = kwargs['unique']

        if "collection" in kwargs:
            self.collection = kwargs['collection']

        if self.collection is None:
            raise TypeError("No index collection specified!")

    def save(self):
        if isinstance(self.collection, api.BaseDocument):
            collection = self.collection.__collection_name__

        else:
            collection = self.collection

        result = self.api.create(collection, self.index_type, fields=self.fields, unique=self.unique)

        return result


class UniqueHash(Hash):

    """A unique hash index."""

    unique = True
