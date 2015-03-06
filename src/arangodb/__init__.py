""" I was before trying orientdb, but was disappointed by bad coding zen and
the smell of broken or incomplete implementation.


"""
from itertools import chain

import requests.adapters

from zope.interface import Interface, implementer, classImplements
from zope.interface.verify import verifyObject
from zope.interface.exceptions import DoesNotImplement

import venusian

import logging

LOG = logging.getLogger(__name__)

from . import db, api, exc


class IArangoSession(Interface):    # pylint: disable=E0239
    pass


class IObjective(Interface):        # pylint: disable=E0239

    """An objective must implement this interface."""

    def serialize(data):            # pylint: disable=E0213
        """Serialize data into JSON."""

    def deserialize(data):          # pylint: disable=E0213
        """Deserialize data from JSON."""


import objective
# we declare a Field to be implementer of IObjective
classImplements(objective.Field, IObjective)


class register_objective(object):

    """Instantiate the objective with pyramid specific environment on venusian scan."""

    def __init__(self, wrapped):
        self.objective = None
        self.objective_class = wrapped
        venusian.attach(wrapped, self.register)

    def register(self, scanner, name, wrapped):
        # wrapped is the container and not the objective!
        # create the instance here
        self.objective = self.objective_class(
            name=name,
            config=scanner.config,
            registry=scanner.config.registry)

        # verify interface
        try:
            verifyObject(IObjective, self.objective)

        except DoesNotImplement:
            LOG.error("Objective of %s does not implement IObjective interface!", wrapped)
            raise

    def __get__(self, inst, cls):
        return self.objective


class DocumentObjective(objective.Mapping):

    """ArangoDB's objective for documents."""

    _id = objective.Item(objective.Field, missing=objective.Ignore)
    _key = objective.Item(objective.Field, missing=objective.Ignore)
    _rev = objective.Item(objective.Field, missing=objective.Ignore)


class EdgeObjective(DocumentObjective):

    """ArangoDB's objective for edges."""

    _from = objective.Item(objective.Field, missing=objective.Ignore)
    _to = objective.Item(objective.Field, missing=objective.Ignore)


import datetime


class User(db.Document):

    # this is pyramid specific
    @register_objective
    class objective(DocumentObjective):

        email = objective.Item(objective.Unicode, missing=objective.Ignore)
        password = objective.Item(objective.Unicode, missing=objective.Ignore)
        since = objective.Item(objective.UtcDateTime, missing=datetime.datetime.utcnow)


class foo(db.Document):
    pass


class offers(db.Edge):
    pass


# pyramid specific


@implementer(IArangoSession)
class SessionPool(api.Session):

    """Creates a customizable connection pool for arangodb."""

    def __init__(self, config):
        settings = config.registry.settings

        self.pool_size = int(settings.get('arangodb.pool.size', 10))
        self.pool_max_size = int(settings.get('arangodb.pool.max_size', 1000))

        endpoint = settings.get('arangodb.endpoint', 'http://localhost:8529')
        adapter = requests.adapters.HTTPAdapter(
            pool_maxsize=self.pool_max_size,
            pool_connections=self.pool_size
        )

        session = requests.Session()
        session.mount(endpoint, adapter)

        super(SessionPool, self).__init__(endpoint, session=session)


def setup_arango_collections(config):
    """Just create all collections defined by all classes."""

    arango = config.registry.getUtility(IArangoSession)

    # create all Documents
    for k, v in chain(db.Document.documents.iteritems(),
                      db.Edge.edges.iteritems()):

        col_type = api.DOCUMENT_COLLECTION if issubclass(v, db.Document) else api.EDGE_COLLECTION

        col = arango.collections.get(k)
        if col is None:
            arango.collections.create(k, type=col_type)
            LOG.info("Created collection: %s", v)

        else:
            # check if type is good
            if col['type'] != col_type:
                raise exc.ArangoException(
                    "An existing collection has the wrong type, solve this manually!",
                    col, v)


def setup_session_factory(config):
    """Attach a session_factory to the MetaBase."""

    session = config.registry.getUtility(IArangoSession)

    def factory(cls):
        """ Return a session for the class.

        :param cls: the class which is requesting a session

        """

        # we ignore cls for the moment
        return session

    db.MetaBase.session_factory = factory


def includeme(config):
    """I will try to implement my idea of the rest api.
    """

    # global session
    config.registry.registerUtility(SessionPool(config))

    # setup for pyramid
    setup_session_factory(config)
    setup_arango_collections(config)
