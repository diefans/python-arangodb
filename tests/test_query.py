def test_iter_expr():
    from arangodb import query

    q = query.Expression()

    assert list(q) == [q]


def test_for_expr():
    from arangodb import query

    q = query.For(query.Alias("foo"), query.Collection("bar"))

    assert list(q) == [
        (query.FOR),
        (q.alias),
        (query.IN),
        (q.list_expr),
    ]


def test_query():
    from arangodb import query

    alias = query.Alias("foo")
    q = query.Query(alias, query.Collection("bar"), query.Return(alias))

    query, params = q.query()

    assert query == "FOR foo IN @@c_0 RETURN foo"
    assert params == {
        "@c_0": "bar",
    }


def test_query_alias_attr():
    from arangodb import query

    alias = query.Alias("foo")
    q = query.Query(alias, query.Collection("bar"), query.Return(alias.bar))

    query, params = q.query()

    assert query == "FOR foo IN @@c_0 RETURN foo.`bar`"
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


def test_filter():
    from arangodb import query

    q = query.Filter(1, 2, 3)

    query, params = q.query()

    assert query == "FILTER @value_0 AND @value_1 AND @value_2"
    assert params == {
        "value_0": 1,
        "value_1": 2,
        "value_2": 3,
    }


def test_filter_and():
    from arangodb import query

    alias = query.Alias("foo")
    alias2 = query.Alias("bar")
    q = query.Query(
        alias,
        query.Collection("bar"),
        query.Return(alias.bar),
        query.Filter(alias == "1", alias2 <= 1)
    )

    query, params = q.query()

    assert query == 'FOR foo IN @@c_0 FILTER foo == @value_0 AND bar <= @value_1 RETURN foo.`bar`'
    assert params == {
        "@c_0": "bar",
        "value_0": "1",
        "value_1": 1
    }


def test_list():
    from arangodb import query

    q = query.Chain([1, 2, 3])

    query, params = q.query()

    # so we have some artifacts ...?
    assert query == "@value_0 @value_1 @value_2"
    assert params == {'value_0': 1, 'value_1': 2, 'value_2': 3}


def test_operator():
    from arangodb import query

    q = query.Operator(query.Alias("foo"), query.Alias("bar"))

    query, params = q.query()

    assert query == "foo == bar"
    assert params == {}


def test_fast_query():
    from arangodb import query

    alias = query.Alias("foo")
    alias2 = query.Alias("bar")
    q = query.Query(alias, query.Collection("bar"))\
        .filter(alias == "1", alias2 <= 1)\
        .result(alias.bar)

    query, params = q.query()

    assert query == 'FOR foo IN @@c_0 FILTER foo == @value_0 AND bar <= @value_1 RETURN foo.`bar`'
    assert params == {
        "@c_0": "bar",
        "value_0": "1",
        "value_1": 1
    }


def test_function():
    from arangodb import query

    alias = query.Alias("foo")

    q = query.PATHS(alias, 1)

    query, params = q.query()

    assert query == "PATHS( foo , @value_0 )"
    assert params == {
        "value_0": 1
    }
