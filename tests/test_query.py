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

    q = query.ForExpression(query.Alias("foo"), query.Collection("bar"))

    query, params = q.join()

    assert query == "FOR foo IN @@collection_0"
    assert params == {
        "@collection_0": "bar",
    }
