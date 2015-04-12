"""ArangoDB api."""


from functools import wraps, partial
from itertools import imap, chain
from collections import OrderedDict

import requests
import logging

LOG = logging.getLogger(__name__)

from . import exc


def json_result():
    """Decorate an arango response call to extract json and perform error handling."""

    def decorator(func):

        @wraps(func)
        def wrapped(*args, **kwargs):
            response = func(*args, **kwargs)

            headers = dict(response.headers.lower_items())
            if headers.get('content-type', '').startswith('application/json'):
                json_content = response.json()

                # inspect response
                if "error" in json_content and json_content.get('error', False):
                    # create a polymorphic exception just by errorNum
                    code = json_content.get('code')
                    num = json_content.get('errorNum')
                    message = json_content.get('errorMessage')

                    raise exc.ApiError(
                        code=code,
                        num=num,
                        message=message,
                        func=func,
                        args=args,
                        kwargs=kwargs,
                    )

                # no error
                return json_content

            raise exc.ContentTypeException("No json content-type", response)

        return wrapped

    return decorator


class Client(object):

    """A client for arangodb server."""

    def __init__(self, database, endpoint="http://localhost:8529", session=None):
        # default database
        self.database = database

        self.endpoint = endpoint.rstrip("/")

        # use an external session
        self.session = session or requests.Session()

        # arango specific api
        self.collections = Collections(self.api(self.database, 'collection'))
        self.documents = Documents(self.api(self.database, 'document'))
        self.edges = Edges(self.api(self.database, 'edge'))
        self.cursors = Cursors(self.api(self.database, 'cursor'))
        self.graphs = Graphs(self.api(self.database, 'gharial'))
        self.indexes = Indexes(self.api(self.database, 'index'))

    def url(self, *path):
        """Return a full url to the arangodb server."""

        return '/'.join(imap(str, chain((self.endpoint, ), path)))

    @json_result()
    def get(self, *path, **kwargs):
        return self.session.get(self.url(*path), **kwargs)

    @json_result()
    def post(self, *path, **kwargs):
        return self.session.post(self.url(*path), **kwargs)

    @json_result()
    def put(self, *path, **kwargs):
        return self.session.put(self.url(*path), **kwargs)

    @json_result()
    def patch(self, *path, **kwargs):
        return self.session.patch(self.url(*path), **kwargs)

    def head(self, *path, **kwargs):
        return self.session.head(self.url(*path), **kwargs)

    @json_result()
    def delete(self, *path, **kwargs):
        return self.session.delete(self.url(*path), **kwargs)

    def api(self, database, *path, **kwargs):
        """Just expose the HTTP methods to this session, by partially pre binding the path."""

        if database is None:
            prefix = ('_api', )

        else:
            prefix = ('_db', database, '_api')

        return ApiProxy(self, *chain(prefix, path), **kwargs)


class SystemClient(Client):

    """A client to the system database of an arangodb server."""

    def __init__(self, endpoint="http://localhost:8529", session=None):
        super(SystemClient, self).__init__(None, endpoint=endpoint, session=session)

        # database api is only allowed for system database
        self.databases = Databases(self.api(None, 'database'))

    def create_database(self, database):
        """Just create the actual database if not exists."""

        if database not in self.databases.databases:
            self.databases.create(database)


class ApiProxy(object):

    """A Proxy to the session, partially preselect parts of the url and request parameter."""

    def __init__(self, session, *path, **kwargs):
        # wrap the session and preselect api
        for method in ('get', 'post', 'put', 'patch', 'delete', 'head'):
            setattr(self, method, partial(getattr(session, method), *path, **kwargs))


class Api(object):
    def __init__(self, api_proxy):
        self.api = api_proxy


class Databases(Api):
    """Database stuff."""

    @property
    def databases(self):
        return self.api.get('user')['result']

    def create(self, name, *users):
        """Create a database."""

        data = dict(name=name)

        if users:
            # TODO validation of users
            data['users'] = users

        return self.api.post(json=data).get('result', False)

    def drop(self, name):
        """Drop a database."""

        return self.api.delete(name).get('result', False)


DOCUMENT_COLLECTION = 2
EDGE_COLLECTION = 3


class Collections(Api):
    """Collection stuff."""

    def create(self, name, **kwargs):
        """Create a collection."""

        body = dict(name=name, **kwargs)

        return self.api.post(json=body)

    def get(self, *name, **kwargs):
        """Get one or all collection/s."""

        params = {}

        if 'no_system' in kwargs:
            params['excludeSystem'] = kwargs['no_system']

        try:
            return self.api.get(*name, params=params)

        except exc.CollectionNotFound:
            return None


class DocumentsMixin(Api):

    def create(self, collection, doc, createCollection=None):
        """Create a document."""

        params = {'collection': collection}

        if createCollection is not None:
            params['createCollection'] = createCollection

        return self.api.post(json=doc, params=params)

    def get(self, *handle, **kwargs):
        """Get a document or all documents.

        :param handle: the document handle or the collection name

        """

        params = {}

        if len(handle) == 1 and '/' not in handle[0]:
            params = dict(collection=handle[0])

            # default to document handle
            params['type'] = kwargs.get('type', 'id')

            handle = ()

        return self.api.get(*handle, params=params)

    def delete(self, *handle):
        """Delete a document."""

        return self.api.delete(*handle)

    def replace(self, doc, *handle, **kwargs):
        """Replace a document."""

        return self.api.put(*handle, json=doc)

    def update(self, doc, *handle, **kwargs):
        """Partially update a document."""

        params = {}

        params['keepNull'] = kwargs.get('keep', False)
        params['mergeObjects'] = kwargs.get('merge', True)

        return self.api.patch(*handle, json=doc, params=params)


class Documents(DocumentsMixin):
    pass


class Edges(DocumentsMixin):
    """Edge stuff."""

    def create(self, collection, _from, _to, edge):
        params = {
            'from': str(_from),
            'to': str(_to),
            'collection': collection
        }

        return self.api.post(json=edge, params=params)


class Graphs(Api):

    def get(self, *name):
        result = self.api.get(*name)

        if name:
            return result['graph']

        return result['graphs']

    def create(self, definition):
        return self.api.post(json=definition)

    def drop(self, name):
        return self.api.delete(name)

    def vertex(self, name):
        return self.api.get(name, "vertex")['collections']


def remap_fields(dct, *include, **mapping):
    """Remap certain fields of a dict by yielding (key, value)."""

    for k, v in dct.iteritems():
        # just define all possible keys
        # this is to prevent wrong ones
        if include and k not in include:
            continue

        yield mapping.get(k, k), v


class Cursors(Api):

    """
    see https://github.com/arangodb/arangodb/issues/1285

    no underscore in query bind var
    """

    def create(self, query, bind=None, **kwargs):
        # https://docs.arangodb.com/HttpAqlQueryCursor/AccessingCursors.html
        body = dict(
            query=query,
            bindVars=bind or {}
        )

        # optional fields
        body.update(
            remap_fields(
                kwargs,
                'batch', 'ttl', 'count',
                batch='batchSize'
            )
        )

        return self.api.post(json=body)

    def pursue(self, cursor_id):
        """Just continue to load a batch from a previous call."""

        return self.api.put(cursor_id)

    def delete(self, name):
        return self.api.delete(name)


class Indexes(Api):
    def get(self, *handle, **kwargs):
        """Get a document or all documents.

        :param handle: the document handle or the collection name

        """

        params = {}

        if len(handle) == 1 and '/' not in handle[0]:
            params = dict(collection=handle[0])

            # default to document handle
            params['type'] = kwargs.get('type', 'id')

            handle = ()

        return self.api.get(*handle, params=params)

    def create(self, collection, index_type, fields=None, **kwargs):
        """Create an index."""

        params = {'collection': collection}

        doc = {
            "type": index_type,
            "fields": fields is not None and fields or []
        }

        doc.update(kwargs)

        return self.api.post(json=doc, params=params)


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
        return SystemClient()

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

        col_type = DOCUMENT_COLLECTION if issubclass(cls, BaseDocument) else EDGE_COLLECTION

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

        return doc.__data__.copy()

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
