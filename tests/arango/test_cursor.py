import pytest
import mock


def test_find():
    from collections import OrderedDict

    from arangodb.db import Cursor

    col = "User"

    query = OrderedDict()
    query["foo"] = 123
    query["bar"] = "baz"

    with mock.patch.object(Cursor, "__init__") as cursor_mock:
        cursor_mock.return_value = None

        Cursor.find(col, **query)

        cursor_mock.assert_called_with(
            "FOR obj IN User FILTER obj.bar == @bar AND obj.foo == @foo RETURN obj",
            bind=query
        )
