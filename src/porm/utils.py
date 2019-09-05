import decimal
from datetime import datetime, date, time, timedelta
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


class PormJsonEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        elif isinstance(obj, time):
            return obj.strftime('%H:%M:%S')
        elif isinstance(obj, timedelta):
            sec = int(obj.total_seconds())
            return '{H:02d}:{M:02d}:{S:02d}'.format(H=int(sec / 3600), M=int(sec / 24 % 60), S=sec % 60)
        else:
            return JSONEncoder.default(self, obj)


def param_notnone(func):
    """
    Validate the params of func are not none like None

    :param func:
    :return:
    """

    @wraps(func)
    def wrapped_func(self, *args, **kwargs):
        for param_idx, param_val in enumerate(args):
            if param_val is None:
                raise EmptyError(u'PARAM: idx of {} is empty: {}'.format(param_idx, param_val))
        for param_key, param_val in kwargs.items():
            if param_val is None:
                raise EmptyError(u'PARAM: {} is empty: {}'.format(param_key, param_val))
        return func(self, *args, **kwargs)

    return wrapped_func


def notnone_check(*arg_names):
    def notnone_checker(func):
        """
        Validate the params of func are not none like None

        :param func:
        :return:
        """

        @wraps(func)
        def wrapped_func(self, *args, **kwargs):
            for arg_name in arg_names:
                if isinstance(arg_name, int):
                    if args[arg_name] is None:
                        raise EmptyError(u'PARAM: param idx of {} is None'.format(arg_name))
                elif isinstance(arg_name, str):
                    if kwargs[arg_name] is None:
                        raise EmptyError(u'PARAM: param name of {} is None'.format(arg_name))
            return func(self, *args, **kwargs)

        return wrapped_func

    return notnone_checker
