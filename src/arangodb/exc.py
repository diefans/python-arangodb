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

    def __call__(cls, code, num, message):
        # polymorphic part
        # lookup exception type
        if num in cls.classes:
            cls = cls.classes[num]

        return type.__call__(cls, code, num, message)


class ApiError(ArangoException):
    """Raise when an api error occurs."""

    __metaclass__ = MetaApiError

    error_num = None

    def __init__(self, code, num, message):
        self.code = code
        self.num = num
        self.message = message

        super(ApiError, self).__init__(code, num, message)


# basically an implementation of https://docs.arangodb.com/ErrorCodes/README.html

class CollectionNotFound(ApiError):
    error_num = 1203


class CollectionTypeInvalid(ApiError):
    error_num = 1218


class DatabaseNotFound(ApiError):
    error_num = 1228


class UniqueConstraintViolated(ApiError):
    error_num = 1210
