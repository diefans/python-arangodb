"""Query module.

See https://docs.arangodb.com/Aql/Basics.html for details

"""

import inspect
from itertools import izip, chain

from collections import defaultdict, OrderedDict

from . import api


KEYWORDS = [
    "FOR",
    "RETURN",
    "FILTER",
    "SORT",
    "LIMIT",
    "LET",
    "COLLECT",
    "INSERT",
    "UPDATE",
    "REPLACE",
    "REMOVE",
    "WITH",
    "ASC",
    "DESC",
    "IN",
    "INTO",
    "NOT",
    "AND",
    "OR",
    "NULL",
    "TRUE",
    "FALSE",
]




class Expression(object):
    def __init__(self):

        # empty bind param map
        self.params = {}

    def __iter__(self):
        """Iterate over all terms and return a tuple of query string part and related bind params."""

        yield self

    def _get_params(self, index):
        """:returns: indexed params"""

        return {'_'.join((key, str(index))): value for key, value in self.params.iteritems()}

    def _get_term(self, index):
        return ""

    def assemble(self):
        # hold index for each type and instance
        expressions = defaultdict(list)

        def register_expr(expr):
            types = expressions[type(expr)]

            if expr not in types:
                types.append(expr)

            return types.index(expr)

        terms, params = [], []

        for expr in self:
            index = register_expr(expr)

            yield expr._get_term(index), expr._get_params(index)

    def join(self):

        terms, binds = izip(*list(self.assemble()))

        # TODO take care that no param has an ambigous meaning
        joined_params = {}
        for i, params in enumerate(binds):
            joined_params.update(params)

        return ' '.join(terms), joined_params


class KeyWord(Expression):
    term = ''

    def __init__(self, term):
        super(KeyWord, self).__init__()

        self.term = term

    def __iter__(self):
        yield self

    def _get_params(self, index):
        return {}

    def _get_term(self, index):
        return self.term


FOR = KeyWord("FOR")
RETURN = KeyWord("RETURN")
FILTER = KeyWord("FILTER")
SORT = KeyWord("SORT")
LIMIT = KeyWord("LIMIT")
LET = KeyWord("LET")
COLLECT = KeyWord("COLLECT")
INSERT = KeyWord("INSERT")
UPDATE = KeyWord("UPDATE")
REPLACE = KeyWord("REPLACE")
REMOVE = KeyWord("REMOVE")
WITH = KeyWord("WITH")
ASC = KeyWord("ASC")
DESC = KeyWord("DESC")
IN = KeyWord("IN")
INTO = KeyWord("INTO")
NOT = KeyWord("NOT")
AND = KeyWord("AND")
OR = KeyWord("OR")
NULL = KeyWord("NULL")
TRUE = KeyWord("TRUE")
FALSE = KeyWord("FALSE")


class Alias(Expression):
    def __init__(self, name):
        super(Alias, self).__init__()
        self.name = name

    def _get_term(self, index):
        return self.name

    def __getattr__(self, name):



class AliasAttr(Alias):
    def __init__(self, parent, name):
        super(AliasAttr, self).__init__(name)
        self.parent = parent


class ListExpression(Expression):
    pass


class ReturnExpression(Expression):
    def __init__(self, alias_or_object):
        self.alias = alias_or_object

    def __iter__(self):
        yield RETURN

        for expr in self.alias:
            yield expr


class Collection(ListExpression):
    def __init__(self, collection):
        super(Collection, self).__init__()

        if inspect.isclass(collection) and issubclass(collection, api.BaseDocument):
            self.collection = collection.__collection_name__

        else:
            self.collection = collection

        self.params["@collection"] = self.collection

    def _get_term(self, index):
        return "@@collection_{}".format(index)


class ForExpression(Expression):
    def __init__(self, alias, list_expr):
        super(ForExpression, self).__init__()

        self.alias = alias
        self.list_expr = list_expr

    def __iter__(self):
        yield FOR

        for expr in self.alias:
            yield expr

        yield IN

        for expr in self.list_expr:
            yield expr


class Query(ForExpression):

    """A query will join into an arango query plus bind params."""

    def __init__(self, alias, list_expr, retr_expr):
        super(Query, self).__init__(alias, list_expr)

        self.retr_expr = retr_expr

    def collection(self, collection):
        self._collection = collection

    def __iter__(self):
        for expr in super(Query, self).__iter__():
            yield expr

        for expr in self.retr_expr:
            yield expr


