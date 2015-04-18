"""Some classes to easy work with arangodb."""

from . import meta, util, query

import logging

LOG = logging.getLogger(__name__)


class QueryMixin(object):
    # pylint: disable=E0213

    @util.classproperty
    def alias(cls):
        """A query alias for this collection."""

        return query.Alias(cls.__collection_name__)

    @util.classproperty
    def query(cls):
        """Prepare a query against this collection.

        The default action is to return the alias.
        """

        return query.Query(cls.alias, query.Collection(cls)).action(cls.alias)


class Document(meta.DocumentBase, QueryMixin):
    pass


EDGE_DIRECTION_ANY = 'any'
EDGE_DIRECTION_INBOUND = 'inbound'
EDGE_DIRECTION_OUTBOUND = 'outbound'


class Edge(meta.EdgeBase, QueryMixin):

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
    def connections_query(cls, alias, document, direction=EDGE_DIRECTION_ANY):
        assert isinstance(document, meta.BaseDocument), "document is Document or Edge"

        # pylint: disable=W0212
        q = query.Query(
            alias,
            query.PATHS(
                query.Collection(document),
                query.Collection(cls),
                direction)
        )\
            .filter(alias.source._id == document._id)\
            .filter(query.LENGTH(alias.edges) == 1)\
            .action(alias.destination)

        return q

    @classmethod
    def connections(cls, document, collection=None, direction=EDGE_DIRECTION_ANY):
        alias = query.Alias('p')

        q = cls.connections_query(alias, document, direction)

        if collection is not None:
            # pylint: disable=W0212
            q = q\
                .filter(query.FIND_FIRST(alias.destination._id, collection.__collection_name__))

        return q.cursor.iter_documents()

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
