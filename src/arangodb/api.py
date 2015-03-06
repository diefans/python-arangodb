"""ArangoDB api."""


from functools import wraps, partial
from itertools import imap, chain

import requests
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

                    raise exc.ApiError(code=code, num=num, message=message)

                # no error
                return json_content

            raise exc.ContentTypeException("No json content-type", response)

        return wrapped

    return decorator


class Session(object):
    """A request session to save connects on upcomming requests."""

    def __init__(self, endpoint="http://localhost:8529", session=None):
        self.endpoint = endpoint.rstrip("/")

        # use an external session
        self.session = session or requests.Session()

        # arango specific api
        self.databases = Databases(self)
        self.collections = Collections(self)
        self.documents = Documents(self)
        self.edges = Edges(self)
        self.cursors = Cursors(self)
        self.graphs = Graphs(self)

    def url(self, *path):
        """Joins the path to the url."""

        return '/'.join(imap(str, chain((self.endpoint, '_api'), path)))

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

    def __call__(self, *path, **kwargs):
        """Just expose the HTTP methods to this session, by partially pre binding the path."""

        return Api(self, *path, **kwargs)


class Api(object):

    """Partially preselect parts of the session methods."""

    def __init__(self, session, *api, **kwargs):
        # wrap the session and preselect api
        for method in ('get', 'post', 'put', 'patch', 'delete', 'head'):
            setattr(self, method, partial(getattr(session, method), *api, **kwargs))


class Databases(object):
    """Database stuff."""

    def __init__(self, session):
        self.api = session("database")

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


class Collections(object):
    """Collection stuff."""

    def __init__(self, session):
        self.api = session('collection')

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

        except exc.CollectionNotFoundError:
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


class Documents(DocumentsMixin):
    def __init__(self, session):
        self.api = session("document")


class Edges(DocumentsMixin):
    """Edge stuff."""

    def __init__(self, session):
        self.api = session('edge')

    def create(self, collection, _from, _to, edge):
        params = {
            'from': str(_from),
            'to': str(_to),
            'collection': collection
        }

        return self.api.post(json=edge, params=params)


class Graphs(object):

    def __init__(self, session):
        self.api = session('gharial')

    def get(self, *name):
        result = self.api.get(*name)

        if name:
            return result['graphs']

        return result

    def create(self, name):
        return self.api.post(name)

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


class Cursors(object):
    def __init__(self, session):
        self.api = session('cursor')

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
