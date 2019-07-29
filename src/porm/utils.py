from datetime import datetime, date
from functools import wraps
from json import JSONEncoder

from porm.errors import EmptyError, ValidationError


def field_exception(func):
    """
    Add field information to the exception
    :return:
    """

    @wraps(func)
    def wrapped_func(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except ValidationError as ex:
            ex.STATUS = 'FIELD NAME: {}, INFO: '.format(self.name) + ex.STATUS
            raise ex

    return wrapped_func


def param_notempty(func):
    """
    Validate the params of func are not empty like 0, '', None, [], {}

    :param func:
    :return:
    """

    @wraps(func)
    def wrapped_func(self, *args, **kwargs):
        for param_idx, param_val in enumerate(args):
            if not param_val:
                raise EmptyError(u'PARAM: idx of {} is empty: {}'.format(param_idx, param_val))
        for param_key, param_val in kwargs.items():
            if not param_val:
                raise EmptyError(u'PARAM: {} is empty: {}'.format(param_key, param_val))
        return func(self, *args, **kwargs)

    return wrapped_func


def type_check(*args_type, **kwargs_type):
    def type_checker(func):
        """
        Validate the params of func are not empty like 0, '', None, [], {}

        :param func:
        :return:
        """

        @wraps(func)
        def wrapped_func(*args, **kwargs):
            args_type_count = len(args_type) + len(kwargs_type)
            args_count = len(args) + len(kwargs)
            if args_type_count > args_count:
                raise ValidationError(u'Param Types Count: {} != Params Count: {}'.format(args_type_count, args_count))
            for idx, type_arg in enumerate(zip(args_type, args)):
                arg_type, arg = type_arg
                if not isinstance(arg, arg_type):
                    raise ValidationError(
                        u'Param Type: {} != Given Param Type: {}, Arg Index: {}'.format(
                            arg_type, type(arg), format(idx)))
            for kwarg_name, kwarg_type in kwargs_type.items():
                kwarg = kwargs.get(kwarg_name)
                if not isinstance(kwarg, kwarg_type):
                    raise ValidationError(
                        u'Param Type: {} != Given Param Type: {}, Key Arg Name: {}'.format(
                            kwarg_type, type(kwarg), kwarg_name))
            return func(*args, **kwargs)

        return wrapped_func

    return type_checker


class DTJsonEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return JSONEncoder.default(self, obj)
