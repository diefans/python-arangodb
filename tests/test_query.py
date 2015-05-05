def test_iter_expr():
    from arangodb import query

    q = query.Expression()

    assert list(q) == [q]


def test_chain():
    from arangodb import query

    q = query.Chain((query.SPACE, query.Term("foo"), query.Term("bar"), query.SPACE))

    qstr, params = q.query()

    assert qstr == " foobar "
    assert params == {}


def test_and():
    from arangodb import query

    q = query.And(query.Term("foo"), query.Term("bar"))

    qstr, params = q.query()

    assert qstr == "foo AND bar"
    assert params == {}


def test_filter():
    from arangodb import query

    q = query.Filter(query.Term("foo"), query.Term("bar"))

    qstr, params = q.query()

    assert qstr == "FILTER foo AND bar"
    assert params == {}


def test_filter_values():
    from arangodb import query

    q = query.Filter(1, 2, 3)

    qstr, params = q.query()

    assert qstr == "FILTER @value_0 AND @value_1 AND @value_2"
    assert params == {
        "value_0": 1,
        "value_1": 2,
        "value_2": 3,
    }


def test_query_filter_and():
    from arangodb import query

    alias = query.Alias("foo")
    alias2 = query.Alias("bar")
    q = query.Query(
        alias,
        query.Collection("bar"),
        query.Return(alias.bar),
        query.Filter(alias == "1", alias2 <= 1)
    )

    qstr, params = q.query()

    assert qstr == 'FOR foo IN @@c_0 FILTER foo == @value_0 AND bar <= @value_1 RETURN foo.`bar`'
    assert params == {
        "@c_0": "bar",
        "value_0": "1",
        "value_1": 1
    }


def test_for_expr():
    from arangodb import query

    q = query.For(query.Alias("foo"), query.Collection("bar"))

    assert list(q) == [
        query.FOR,
        query.SPACE,
        q.alias,
        query.SPACE,
        query.IN,
        query.SPACE,
        q.list_expr,
    ]


def test_query():
    from arangodb import query

    alias = query.Alias("foo")
    q = query.Query(alias, query.Collection("bar"), query.Return(alias))

    qstr, params = q.query()

    assert qstr == "FOR foo IN @@c_0 RETURN foo"
    assert params == {
        "@c_0": "bar",
    }


def test_query_alias_attr():
    from arangodb import query

    alias = query.Alias("foo")
    q = query.Query(alias, query.Collection("bar"), query.Return(alias.bar))

    qstr, params = q.query()

    assert qstr == "FOR foo IN @@c_0 RETURN foo.`bar`"
    assert params == {
        "@c_0": "bar",
    }


def test_alias_relations_eq():
    from arangodb import query

    a = query.Alias("foo")

    x = a == 1
    assert isinstance(x, query.Operator)

    x = a > 1
    assert isinstance(x, query.Operator)

    x = a >= 1
    assert isinstance(x, query.Operator)

    x = a < 1
    assert isinstance(x, query.Operator)

    x = a <= 1
    assert isinstance(x, query.Operator)


def test_list():
    from arangodb import query

    q = query.List([1, 2, 3])

    qstr, params = q.query()

    # so we have some artifacts ...?
    assert qstr == "@value_0, @value_1, @value_2"
    assert params == {'value_0': 1, 'value_1': 2, 'value_2': 3}


def test_operator():
    from arangodb import query

    q = query.Operator(query.Alias("foo"), query.Alias("bar"))

    qstr, params = q.query()

    assert qstr == "foo == bar"
    assert params == {}


def test_fast_query():
    from arangodb import query

    alias = query.Alias("foo")
    alias2 = query.Alias("bar")
    q = query.Query(alias, query.Collection("bar"))\
        .filter(alias == "1", alias2 <= 1)\
        .filter(alias != 1, alias > 1, alias < 2, alias >= 2)\
        .action(alias.bar)

    qstr, params = q.query()

    assert qstr == 'FOR foo IN @@c_0 FILTER foo == @value_0 AND bar <= @value_1'\
        ' AND foo != @value_2 AND foo > @value_3 AND foo < @value_4 AND foo >= @value_5 RETURN foo.`bar`'
    assert params == {
        "@c_0": "bar",
        "value_0": "1",
        "value_1": 1,
        'value_2': 1,
        'value_3': 1,
        'value_4': 2,
        'value_5': 2
    }


def test_function():
    from arangodb import query

    alias = query.Alias("foo")

    q = query.PATHS(alias, 1)

    qstr, params = q.query()

    assert qstr == "PATHS(foo, @value_0)"
    assert params == {
        "value_0": 1
    }


def test_let():
    from arangodb import query

    a = query.Alias("foo")
    q = query.Let(a, "bar")

    qstr, params = q.query()

    assert qstr == "LET foo = @value_0"
    assert params == {
        "value_0": "bar"
    }


def test_sort():
    from arangodb import query

    a = query.Alias("foo")
    q = query.Sort(a.foo, query.Desc(a.bar))

    qstr, params = q.query()

    assert qstr == "SORT foo.`foo`, foo.`bar` DESC"
    assert params == {}


def test_sort_query():
    from arangodb import query

    a = query.Alias("foo")
    q = query.Query(a, query.Collection("bar")).action(a).sort(a.foo, query.Desc(a.bar)).limit(10)

    qstr, params = q.query()

    assert qstr == "FOR foo IN @@c_0 SORT foo.`foo`, foo.`bar` DESC LIMIT 10 RETURN foo"
    assert params == {
        "@c_0": "bar"
    }


def test_limit():
    from arangodb import query

    q = query.Limit(1, 10)

    qstr, params = q.query()

    assert qstr == "LIMIT 1, 10"

    q = query.Limit(10)

    qstr, params = q.query()

    assert qstr == "LIMIT 10"
    assert params == {}


def test_in():
    from arangodb import query

    a = query.Alias("foo")
    q = query.In(a, [1, 2, 3])

    qstr, params = q.query()

    assert qstr == "foo IN @value_0"
    assert params == {
        'value_0': [1, 2, 3]
    }
