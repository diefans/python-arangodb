import mock
import pytest


def test_init():
    from arangodb import db

    class provides(db.Edge):
        pass

    e = provides("foo", "bar")

    with mock.patch.object(e, '_create') as patched_create:

        patched_create.return_value = {
            '_id': "id",
            '_key': "key",
            '_rev': "rev"
        }
        e.save()
        patched_create.assert_called_with({'_from': "foo", '_to': "bar"})


def test_document_from_to():
    from arangodb import db

    d = db.Document(_id='foo/bar')

    db.Edge(d, d)

    with pytest.raises(TypeError):
        db.Edge(db.Document(), "foo")

    with pytest.raises(TypeError):
        db.Edge("foo", db.Document())


@mock.patch("arangodb.cursor.Cursor")
def test_connections(Cursor):
    from arangodb import db, query

    db.Edge.connections(db.Document(_id="foo"))

    Cursor.assert_called_with(
        "FOR p IN PATHS(@@doc, @@edge, @direction) FILTER "
        "p.source._id == @doc_id "
        "&& LENGTH(p.edges) == 1 "
        "RETURN p.destination",
        {'@edge': 'Edge', 'doc_id': 'foo', 'direction': 'any', '@doc': 'Document'}
    )

    alias = query.Alias("p")
    q = query.Query(
        alias,
        query.PATHS(
            query.Collection(db.Document),
            query.Collection(db.Edge),
            "any")
    ).filter(alias.source._id == 1).filter(query.LENGTH(alias.edges) == 1).action(alias.destination)

    qstr, params = q.query()

    assert (qstr, params) == (
        'FOR p IN PATHS(@@c_0, @@c_1, @value_0) '
        'FILTER p.`source`.`_id` == @value_1 AND LENGTH(p.`edges`) == @value_2 '
        'RETURN p.`destination`',
        {
            'value_2': 1,
            'value_1': 1,
            'value_0': 'any',
            '@c_0': 'Document',
            '@c_1': 'Edge'
        }
    )
