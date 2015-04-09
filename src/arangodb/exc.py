"""
The arangodb errors.
"""


class ArangoException(Exception):
    pass


class ContentTypeException(ArangoException):
    pass


class MetaApiError(type):
    """Just for polymorphic instantiation."""

    base = None
    classes = {}

    def __new__(mcs, name, bases, dct):
        cls = type.__new__(mcs, name, bases, dct)

        if mcs.base is None:
            mcs.base = cls

        else:
            error_num = dct.get('error_num')
            if error_num is not None:

                # this should not happen
                if error_num in mcs.classes:
                    raise ArangoException("You defined an arango error ({}) twice!".format(error_num),
                                          error_num, mcs.classes[error_num], cls)

                mcs.classes[error_num] = cls

        return cls

    def __call__(cls, code, num, message, *args, **kwargs):
        # polymorphic part
        # lookup exception type
        if num in cls.classes:
            cls = cls.classes[num]

        return type.__call__(cls, code, num, message, *args, **kwargs)


class ApiError(ArangoException):
    """Raise when an api error occurs."""

    __metaclass__ = MetaApiError

    error_num = None

    def __init__(self, code, num, message, func=None, args=None, kwargs=None):
        self.code = code
        self.num = num
        self.message = message

        self.func = func
        self.args = args
        self.kwargs = kwargs

        super(ApiError, self).__init__(code, num, message, func, args, kwargs)


# basically an implementation of https://docs.arangodb.com/ErrorCodes/README.html

class CollectionNotFound(ApiError):
    error_num = 1203


class CollectionTypeInvalid(ApiError):
    error_num = 1218


class DatabaseNotFound(ApiError):
    error_num = 1228


class UniqueConstraintViolated(ApiError):
    error_num = 1210


class GraphError(ApiError):
    pass


class EdgeCollectionAlreadyUsed(GraphError):
    error_num = 1921


class GraphNotFound(GraphError):
    error_num = 1924
