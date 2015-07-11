try:
    import unittest.mock as mock
except ImportError:
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
    from arangodb import db

    db.Edge.connections(db.Document(_id="foo"))

    Cursor.assert_called_with(
        'FOR p IN PATHS(@@c_0, @@c_1, @value_0) '
        'FILTER p.`source`.`_id` == @value_1 AND LENGTH(p.`edges`) == @value_2 '
        # 'AND FIND_FIRST(p.`destination`.`_id`, @value_3) '
        'RETURN p.`destination`',
        {
            'value_1': "foo",
            'value_2': 1,
            # 'value_3': 'Foo',
            'value_0': 'any',
            '@c_0': 'Document',
            '@c_1': 'Edge'
        }
    )
