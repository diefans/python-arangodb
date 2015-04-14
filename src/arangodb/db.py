"""Some classes to easy work with arangodb."""

from . import meta, util, query, cursor

import logging

LOG = logging.getLogger(__name__)


class Document(meta.DocumentBase):
    pass


EDGE_DIRECTION_ANY = 'any'
EDGE_DIRECTION_INBOUND = 'inbound'
EDGE_DIRECTION_OUTBOUND = 'outbound'


class QueryMixin(object):
    # pylint: disable=E0213

    @util.classproperty
    def alias(cls):
        """A query alias for this collection."""

        return query.Alias(cls.__collection_name__)

    @util.classproperty
    def query(cls):
        """Prepare a query against this collection."""

        return query.Query(cls.alias, query.Collection(cls))


class Edge(meta.EdgeBase):

    """An edge between two documents.

    When the edge is loaded the two dowuments are also loaded.
    """

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
        if key in ('_from', '_to') and isinstance(value, meta.BaseDocument):
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
        assert isinstance(document, meta.BaseDocument), "document is Document or Edge"

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

        q = "FOR p IN PATHS(@@doc, @@edge, @direction) "\
            "FILTER {filters} RETURN p.destination".format(filters=' && '.join(filters))

        return cursor.Cursor(q, params).iter_documents()

    @classmethod
    def inbounds(cls, document, collection=None):
        return cls.connections(document, collection, direction=EDGE_DIRECTION_INBOUND)

    @classmethod
    def outbounds(cls, document, collection=None):
        return cls.connections(document, collection, direction=EDGE_DIRECTION_OUTBOUND)


class Index(meta.IndexBase):

    """An index representation."""

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
        if isinstance(self.collection, meta.BaseDocument):
            collection = self.collection.__collection_name__

        else:
            collection = self.collection

        result = self.api.create(collection, self.index_type, fields=self.fields, unique=self.unique)

        return result


class UniqueHash(Hash):

    """A unique hash index."""

    unique = True
