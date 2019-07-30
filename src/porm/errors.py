__all__ = (
    "EmptyError", "DatabaseError", "InterfaceError", "OperationalError", "ValidationError", "ParamError",
    "__exception_wrapper__"
)


class BaseError(Exception):
    """
    Raised when a error occur.
    """

    SUBCODE = 10501
    STATUS = u'Porm Error'

    def __init__(self, message="", subcode=SUBCODE, status=STATUS, *args, **kwargs):
        self.status = status or self.STATUS
        self.subcode = subcode or self.SUBCODE
        Exception.__init__(self, message, *args, **kwargs)


class DatabaseError(BaseError):
    SUBCODE = 10503
    STATUS = u'Porm Database Error'


class ValidationError(ValueError):
    SUBCODE = 10503
    STATUS = u'Porm Validation Error'


class EmptyError(BaseError):
    SUBCODE = 10404
    STATUS = u'Porm Empty Error'


class NotSupportError(BaseError):
    SUBCODE = 12404
    STATUS = u'Porm Not Supported Error'


class InterfaceError(BaseError):
    SUBCODE = 11404
    STATUS = u'Porm Interface Error'


class OperationalError(BaseError):
    SUBCODE = 10403
    STATUS = u'Porm Operational Error'


class ParamError(BaseError):
    SUBCODE = 10422
    STATUS = u'Porm Invalid Parameter Error'



def reraise(tp, value, tb=None):
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value


class ExceptionWrapper(object):
    __slots__ = ('exceptions',)

    def __init__(self, exceptions):
        self.exceptions = exceptions

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            return
        # psycopg2.8 shits out a million cute error types. Try to catch em all.
        # TODO
        # deal with pg
        # if pg_errors is not None and exc_type.__name__ not in self.exceptions \
        #    and issubclass(exc_type, pg_errors.Error):
        #     exc_type = exc_type.__bases__[0]
        if exc_type.__name__ in self.exceptions:
            new_type = self.exceptions[exc_type.__name__]
            exc_args = exc_value.args
            reraise(new_type, new_type(*exc_args), traceback)


EXCEPTIONS = {
    'DatabaseError': DatabaseError,
    'InterfaceError': InterfaceError,
    'OperationalError': OperationalError
}

__exception_wrapper__ = ExceptionWrapper(EXCEPTIONS)
