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
