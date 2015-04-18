from . import meta

import logging

LOG = logging.getLogger(__name__)


class Cursor(meta.CursorBase):

    """A cursor is created to perform queries."""

    def __init__(self, query, bind=None, **kwargs):
        self.query = query
        self.bind = bind
        self.kwargs = kwargs

    def iter_result(self):
        """Iterate over all batches of result."""

        LOG.debug("Create cursor: `%s`, %s, %s", self.query, self.bind, self.kwargs)
        cursor = self.__class__.api.create(self.query, bind=self.bind, **self.kwargs)

        while cursor['result']:
            for result in cursor['result']:
                yield result

            if not cursor['hasMore']:
                # step out
                break

            # fetch next batch
            cursor = self.__class__.api.pursue(cursor['id'])

    def iter_documents(self):

        """If you expect document instances to be returned from the cursor,
        call this to instantiate them."""

        for doc in self.iter_result():
            yield meta.BaseDocument._polymorph(doc)         # pylint: disable=W0212

    def first_document(self):
        for doc in self.iter_documents():
            return doc
