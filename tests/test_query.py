def test_iter_expr():
    from arangodb import query

    q = query.Expression()

    assert list(q) == [q]


def test_for_expr():
    from arangodb import query

    q = query.ForExpression(query.Alias("foo"), query.Collection("bar"))

    assert list(q) == [
        (query.FOR),
        (q.alias),
        (query.IN),
        (q.list_expr),
    ]


def test_join():
    from arangodb import query

    alias = query.Alias("foo")
    q = query.Query(alias, query.Collection("bar"), query.Return(alias))

    query, params = q.join()

    assert query == "FOR foo IN @@collection_0 RETURN foo"
    assert params == {
        "@collection_0": "bar",
    }


def test_join_alias_attr():
    from arangodb import query

    alias = query.Alias("foo")
    q = query.Query(alias, query.Collection("bar"), query.Return(alias.bar))

    query, params = q.join()

    assert query == "FOR foo IN @@collection_0 RETURN foo.`bar`"
    assert params == {
        "@collection_0": "bar",
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

    query, params = q.join()

    assert query == "FILTER 1 AND 2 AND 3"


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

    query, params = q.join()

    assert query == 'FOR foo IN @@collection_0 FILTER foo == "1" AND bar <= 1 RETURN foo.`bar`'
    assert params == {
        "@collection_0": "bar",
    }


def test_list():
    from arangodb import query

    q = query.ListExpression([1, 2, 3])

    query, params = q.join()

    # TODO at the moment every expression is separated with space ' '
    # so we have some artifacts ...?
    assert query == "1 ,  2 ,  3"


def test_operator():
    from arangodb import query

    q = query.Operator(query.Alias("foo"), query.Alias("bar"))

    query, params = q.join()

    assert query == "foo == bar"


def test_fast_query():
    from arangodb import query

    alias = query.Alias("foo")
    alias2 = query.Alias("bar")
    q = query.Query(alias)\
        .from_list(query.Collection("bar"))\
        .filter(alias == "1", alias2 <= 1)\
        .result(alias.bar)

    query, params = q.join()

    assert query == 'FOR foo IN @@collection_0 FILTER foo == "1" AND bar <= 1 RETURN foo.`bar`'
    assert params == {
        "@collection_0": "bar",
    }
