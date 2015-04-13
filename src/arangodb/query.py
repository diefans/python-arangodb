"""Query module.

See https://docs.arangodb.com/Aql/Basics.html for details

"""

from functools import wraps
from itertools import izip

from collections import defaultdict

from . import meta, util, cursor


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

        for expr in self:
            index = register_expr(expr)

            yield expr._get_term(index), expr._get_params(index)            # pylint: disable=w0212

    def query(self):
        """Create a query with its bind params assembled."""

        terms, binds = izip(*list(self.assemble()))

        joined_params = {}
        for params in binds:
            joined_params.update(params)

        return ' '.join(terms), joined_params


class Param(object):
    def __init__(self, name):
        self.name = name

    def __call__(self, index, declare=True):
        return "{declare}{name}_{index}"\
            .format(declare=declare and "@" or "", name=self.name, index=index)


class Value(Expression):
    param = Param('value')

    def __init__(self, value):
        super(Value, self).__init__()
        self.value = value

    def _get_term(self, index):
        return self.param(index)

    def _get_params(self, index):
        return {
            self.param(index, False): self.value
        }

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
        parent_term = self.parent._get_term(index)          # pylint: disable=W0212

        return "{}.`{}`".format(parent_term, self.name)


# pylint: disable=W0223

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


class List(Chain):
    sep = Term(',')


class And(List):
    sep = AND

    def __init__(self, *exprs):
        super(And, self).__init__(exprs)


class Filter(And):

    def __iter__(self):
        yield FILTER

        for expr in super(Filter, self).__iter__():
            yield expr


class Function(List):

    def __init__(self, *exprs):
        super(Function, self).__init__(exprs)

    def __iter__(self):
        yield Term(self.name + "(")

        for expr in super(Function, self).__iter__():
            yield expr

        yield Term(")")

    @util.classproperty
    def name(cls):          # pylint: disable=E0213
        return cls.__name__


# See https://docs.arangodb.com/Aql/Functions.html
# pylama:ignore=E302,E701,C0321
# TYPES
class TO_BOOL(Function): pass
class TO_NUMBER(Function): pass
class TO_STRING(Function): pass
class TO_ARRAY(Function): pass
class TO_LIST(Function): pass
class IS_NULL(Function): pass
class IS_BOOL(Function): pass
class IS_NUMBER(Function): pass
class IS_STRING(Function): pass
class IS_ARRAY(Function): pass
class IS_LIST(Function): pass
class IS_OBJECT(Function): pass
class IS_DOCUMENT(Function): pass

# STRING
class CONCAT(Function): pass
class CONCAT_SEPARATOR(Function): pass
class CHAR_LENGTH(Function): pass
class LOWER(Function): pass
class UPPER(Function): pass
class SUBSTITUTE(Function): pass
class SUBSTRING(Function): pass
class LEFT(Function): pass
class RIGHT(Function): pass
class TRIM(Function): pass
class LTRIM(Function): pass
class RTRIM(Function): pass
class SPLIT(Function): pass
class REVERSE(Function): pass
class CONTAINS(Function): pass
class FIND_FIRST(Function): pass
class fIND_LAST(Function): pass
class LIKE(Function): pass
class MD5(Function): pass
class SHA1(Function): pass
class RANDOM_TOKEN(Function): pass

# NUMERIC
class FLOOR(Function): pass
class CEIL(Function): pass
class ROUND(Function): pass
class ABS(Function): pass
class SQRT(Function): pass
class RAND(Function): pass

# DATE
class DATE_TIMESTAMP(Function): pass
class DATE_ISO8601(Function): pass
class DATE_DAYOFWEEK(Function): pass
class DATE_YEAR(Function): pass
class DATE_MONTH(Function): pass
class DATE_DAY(Function): pass
class DATE_HOUR(Function): pass
class DATE_MINUTE(Function): pass
class DATE_SECOND(Function): pass
class DATE_MILLISECOND(Function): pass
class DATE_NOW(Function): pass

# ARRAY
class LENGTH(Function): pass
class FLATTEN(Function): pass
class MIN(Function): pass
class MAX(Function): pass
class AVERAGE(Function): pass
class SUM(Function): pass
class MEDIAN(Function): pass
class PERCENTILE(Function): pass
class VARIANCE_POPULATION(Function): pass
class STDDEV_POPULATION(Function): pass
class SDTDEV_SAMPLE(Function): pass
# class REVERSE(Function): pass
class FIRST(Function): pass
class LAST(Function): pass
class NTH(Function): pass
class POSITION(Function): pass
class SLICE(Function): pass
class UNIQUE(Function): pass
class UNION(Function): pass
class UNION_DISTINCT(Function): pass
class MINUS(Function): pass
class INTERSECTION(Function): pass
class APPEND(Function): pass
class PUSH(Function): pass
class UNSHIFT(Function): pass
class POP(Function): pass
class SHIFT(Function): pass
class REMOVE_VALUE(Function): pass
class REMOVE_VALUES(Function): pass
class REMOVE_NTH(Function): pass

# OBJECT/DOCUMENT
class MATCHES(Function): pass
class MERGE(Function): pass
class MERGE_RECURSIVE(Function): pass
class TRANSLATE(Function): pass
class HAS(Function): pass
class ATTRIBUTES(Function): pass
class VALUES(Function): pass
class ZIP(Function): pass
class UNSET(Function): pass
class KEEP(Function): pass
class PARSE_IDENTIFIER(Function): pass

# GEO
class NEAR(Function): pass
class WITHIN(Function): pass
class WITHIN_RECTANGLE(Function): pass
class IS_IN_POLYGON(Function): pass

# FULLTEXT
class FULLTEXT(Function): pass

# GRAPH
class EDGES(Function): pass
class NEIGHBORS(Function): pass
class TRAVERSAL(Function): pass
class TRAVERSAL_TREE(Function): pass
class SHORTEST_PATH(Function): pass
class PATHS(Function): pass

# MISC
class NOT_NULL(Function): pass
class FIRST_LIST(Function): pass
class FIRST_DOCUMENT(Function): pass
class COLLECTIONS(Function): pass
class CURRENT_USER(Function): pass
class DOCUMENT(Function): pass
class SKIPLIST(Function): pass
class CALL(Function): pass
class APPLY(Function): pass


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


class Action(Expression):
    """A Kind of mandatory command for a query to perform.

    RETURN, REMOVE, INSERT, UPDATE, REPLACE
    """


class Return(Action):
    def __init__(self, alias_or_object):
        super(Return, self).__init__()

        self.alias = alias_or_object

    def __iter__(self):
        yield RETURN

        for expr in self.alias:
            yield expr


class Collection(Expression):
    param = Param('@c')

    def __init__(self, collection):
        super(Collection, self).__init__()

        self.collection = getattr(collection, "__collection_name__", collection)

    def _get_params(self, index):
        return {
            self.param(index, False): self.collection
        }

    def _get_term(self, index):
        return self.param(index)


class For(Expression):
    def __init__(self, alias, from_list):
        super(For, self).__init__()

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


class QueryBase(Expression):

    __metaclass__ = meta.MetaQueryBase


class Query(QueryBase):

    """A query will join into an arango query plus bind params."""

    def __init__(self, alias, from_list, action=None, filter=None):         # pylint: disable=W0622
        super(Query, self).__init__()

        self.for_exprs = [For(alias, from_list)]

        self.action_expr = action

        self.filter_expr = filter

    def __iter__(self):
        for for_expr in self.for_exprs:
            for expr in for_expr:
                yield expr

        if self.filter_expr is not None:
            for expr in self.filter_expr:
                yield expr

        for expr in self.action_expr:
            yield expr

    def filter(self, *filters):
        self.filter_expr = Filter(*filters)

        return self

    def join(self, alias, from_list):
        self.for_exprs.append(For(alias, from_list))

        return self

    def action(self, action):
        """Replace the action of that query."""

        if isinstance(action, Action):
            self.action_expr = action

        else:
            self.action_expr = Return(action)

        return self

    def validate(self):
        """Will ask the server to parse the query without executing it."""

        query, _ = self.query()

        return self.__class__.api.parse(query)

    @property
    def cursor(self):
        return cursor.Cursor(*self.query())
