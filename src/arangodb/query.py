"""Query module.

See https://docs.arangodb.com/Aql/Basics.html for details

"""

import inspect
from functools import wraps
from itertools import izip, izip_longest, chain, repeat

import ujson

from collections import defaultdict, OrderedDict

from . import api


class Expression(object):

    """Everything is an expression.

    The only thing to remenber is, that you should only directly yield a
    subexpression, if it implements :py:meth:`Expression._get_term`, if that is
    not the case, just yield from it.

    """

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
        raise NotImplementedError("If this expression should have a term, you hav to implement it")

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


class Value(Expression):
    def __init__(self, value):
        super(Value, self).__init__()
        self.value = value

    def _get_term(self, index):
        return ujson.dumps(self.value)

    @classmethod
    def iter_fixed(cls, sequence):
        """:returns: a generator which is wrapping none-expressions into :py:class:`Value`s"""

        for value in sequence:
            if not isinstance(value, Expression):
                yield cls(value)

            else:
                yield value


def iter_fix_value(wrapped):

    @wraps(wrapped)
    def decorator(*args, **kwargs):
        return Value.iter_fixed(wrapped(*args, **kwargs))

    return decorator


class Term(Expression):
    term = ''

    def __init__(self, term):
        super(Term, self).__init__()

        self.term = term

    def __iter__(self):
        yield self

    def _get_params(self, index):
        return {}

    def _get_term(self, index):
        return self.term



class KeyWord(Term):
    pass


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
        attr = AliasAttr(self, name)

        self.__dict__[name] = attr
        return attr

    def __eq__(self, other):
        return Operator(self, other, _EQ)

    def __lt__(self, other):
        return Operator(self, other, _LT)

    def __le__(self, other):
        return Operator(self, other, _LE)

    def __gt__(self, other):
        return Operator(self, other, _GT)

    def __ge__(self, other):
        return Operator(self, other, _GE)


class AliasAttr(Alias):
    def __init__(self, parent, name):
        super(AliasAttr, self).__init__(name)
        self.parent = parent

    def _get_term(self, index):
        parent_term = self.parent._get_term(index)

        return "{}.`{}`".format(parent_term, self.name)


class Chain(Expression):
    sep = None

    def __init__(self, exprs):
        super(Chain, self).__init__()

        self.exprs = exprs

    @iter_fix_value
    def __iter__(self):
        c = len(self.exprs)

        for i, expr in enumerate(Value.iter_fixed(self.exprs)):
            if expr is not None:
                for subexpr in expr:
                    yield subexpr

            if i < c - 1 and self.sep is not None:
                yield self.sep


class ListExpression(Chain):
    sep = Term(', ')


class And(ListExpression):
    sep = AND

    def __init__(self, *exprs):
        super(And, self).__init__(exprs)


class Filter(And):

    def __iter__(self):
        yield FILTER

        for expr in super(Filter, self).__iter__():
            yield expr


_EQ = Term("==")
_LT = Term("<")
_LE = Term("<=")
_GT = Term(">")
_GE = Term(">=")
_NE = Term("!=")
_IN = Term("IN")
_NOT_IN = Term("NOT IN")

_AND = Term("AND")
_OR = Term("OR")
_NOT = Term("NOT")


class Operator(Expression):
    op = _EQ

    def __init__(self, a, b, op=None):
        super(Operator, self).__init__()

        if op is not None:
            self.op = op

        if not isinstance(a, Expression):
            a = Value(a)

        if not isinstance(b, Expression):
            b = Value(b)

        self.a = a
        self.b = b

    @iter_fix_value
    def __iter__(self):
        for expr in self.a:
            yield expr

        for expr in self.op:
            yield expr

        for expr in self.b:
            yield expr


class Return(Expression):
    def __init__(self, alias_or_object):
        super(Return, self).__init__()

        self.alias = alias_or_object

    def __iter__(self):
        yield RETURN

        for expr in self.alias:
            yield expr


class Collection(Expression):
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
    def __init__(self, alias, from_list):
        super(ForExpression, self).__init__()

        self.alias_expr = alias
        self.list_expr = from_list

    def __iter__(self):
        yield FOR

        for expr in self.alias_expr:
            yield expr

        yield IN

        for expr in self.list_expr:
            yield expr

    def alias(self, alias):
        self.alias_expr = alias

        return self

    def from_list(self, from_list):
        self.list_expr = from_list

        return self


class Query(ForExpression):

    """A query will join into an arango query plus bind params."""

    def __init__(self, alias=None, from_list=None, action=None, filter=None):
        super(Query, self).__init__(alias, from_list)

        self.action_expr = action

        self.filter_expr = filter

    def filter(self, *filters):
        self.filter_expr = Filter(*filters)

        return self

    def result(self, action):
        self.action_expr = Return(action)

        return self

    def __iter__(self):
        for expr in super(Query, self).__iter__():
            yield expr

        if self.filter_expr is not None:
            for expr in self.filter_expr:
                yield expr

        for expr in self.action_expr:
            yield expr
