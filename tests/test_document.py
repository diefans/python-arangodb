class TestDocumentDict(object):
    def test_document_dict(self):
        from arangodb import db

        data = {'foo': 'bar', 1: 2}
        doc = db.Document(data)

        assert dict(doc) == data
