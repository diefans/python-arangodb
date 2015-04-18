"""ArangoDB api."""


from functools import wraps, partial
from itertools import imap, chain

import requests
import requests.adapters

from . import exc

import logging

LOG = logging.getLogger(__name__)


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

        # may use an external session
        if session is None:
            adapter = requests.adapters.HTTPAdapter(
                pool_maxsize=100,
                pool_connections=100
            )

            session = requests.Session()
            session.mount(self.endpoint, adapter)

        self.session = session

        # arango specific api
        self.collections = Collections(self.api(self.database, 'collection'))
        self.documents = Documents(self.api(self.database, 'document'))
        self.edges = Edges(self.api(self.database, 'edge'))
        self.cursors = Cursors(self.api(self.database, 'cursor'))
        self.graphs = Graphs(self.api(self.database, 'gharial'))
        self.indexes = Indexes(self.api(self.database, 'index'))
        self.queries = Queries(self.api(self.database, 'query'))

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


class DocumentsMixin(object):

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


class Documents(Api, DocumentsMixin):
    pass


class Edges(Api, DocumentsMixin):
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


class Queries(Api):
    def parse(self, query):
        """Parse a query and validate it by the server."""

        return self.api.post(
            json={
                'query': query
            }
        )
