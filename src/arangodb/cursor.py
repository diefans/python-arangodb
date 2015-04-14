from functools import partial
from itertools import starmap

from . import meta

import logging

LOG = logging.getLogger(__name__)


class Cursor(meta.CursorBase):

    """A cursor is created to perform queries."""

    def __init__(self, query, bind=None, **kwargs):
        self.query = query
        self.bind = bind
        self.kwargs = kwargs

    @classmethod
    def find(cls, doc, **kwargs):
        """Very simple filter query for a collection.

        :param filter: concatenated by AND
        """
        if isinstance(doc, basestring):
            collection = doc

        elif issubclass(doc, meta.BaseDocument):
            collection = doc.__collection_name__

        else:
            raise TypeError(":param doc: must be a string or a BaseDocument.")

        obj = 'obj'

        tmpl = """FOR {obj} IN {collection} {filter} RETURN {obj}""".format
        tmpl_bind = partial('{obj}.`{0}` == @param_{0}'.format, obj=obj)

        filter_str = ''

        if kwargs:

            filter_str = ' '.join(
                (
                    'FILTER',
                    # we sort attributes by name
                    ' AND '.join(starmap(tmpl_bind, sorted(kwargs.iteritems())))
                )
            )

        return cls(tmpl(collection=collection, filter=filter_str, obj=obj),
                   bind={'param_{}'.format(k): v for k, v in kwargs.iteritems()})

    def iter_result(self):
        """Iterate over all batches of result."""

        LOG.debug("Create query: `%s`, %s, %s", self.query, self.bind, self.kwargs)
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
        """If you expect document instances to be returned from the cursor, call this to instantiate them."""

        for doc in self.iter_result():
            yield meta.BaseDocument._polymorph(doc)         # pylint: disable=W0212

    def first_document(self):
        for doc in self.iter_documents():
            return doc
