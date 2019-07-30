# -*- coding: utf-8 -*-
import datetime
import sys
from abc import ABCMeta, abstractmethod
from dateutil import parser

import six

from porm.utils import field_exception
from porm.errors import ValidationError

__all__ = (
    "BaseType",
    "IntegerType",
    "VarcharType",
    "TextType",
    "DateType",
    "DatetimeType",
    "TimestampType"
)


class BaseType(metaclass=ABCMeta):
    __metaclass__ = ABCMeta

    __baseattrs__ = {
        '_required': '_REQUIRED',
        '_type': '_TYPE',
        '_default': '_DEFAULT',
        '_pk': '_PK'
    }

    _REQUIRED = False
    _TYPE = None
    _DEFAULT = None
    _PK = False

    def __init__(self, *args, **kwargs):
        for key, attr_name in self.__baseattrs__.items():
            setattr(self, key, kwargs.get(key[1:], getattr(self, attr_name, None)))
        self._value = None
        self._name = None
        self._has_value = False

    def set_name(self, name):
        self._name = name or self._name

    @property
    def name(self):
        return self._name

    def set_value(self, val):
        self._value = val
        self._value = self.validate(self._value)
        self._has_value = True

    @property
    def value(self):
        if self._has_value:
            return self._value
        else:
            self._value = self.validate(self.default)
            self._has_value = True
            return self._value

    @property
    def default(self):
        return self._default() if callable(self._default) else self._default

    @property
    def required(self):
        return self._required

    @property
    def not_null(self):
        return self.required

    @property
    def type(self):
        return self._type

    def ispk(self):
        return self._pk

    def set_pk(self, ispk=True):
        self._pk = ispk

    @abstractmethod
    def validate(self, val: object) -> object:
        if self.required and val is None:
            raise ValidationError(u'{}: is not null but got null'.format(self.name or 'value'))
        return val


class VarcharType(BaseType):
    # if py2 _TYPE = unicode
    # if py3 _TYPE = str
    # 4 both None

    _TYPE = str
    _DEFAULT = u''
    _LENGTH = 255

    def __init__(self, *args, **kwargs):
        super(VarcharType, self).__init__(*args, **kwargs)
        self.length = kwargs.get('length', self._LENGTH)

    @field_exception
    def validate(self, val):
        super().validate(val)
        val = self.type(val)
        if not isinstance(val, six.string_types):
            raise ValidationError(u'{}: {} is not string type'.format(self.name or 'value', val))
        if len(val) > self.length:
            raise ValidationError(u'{}: {} is not over size: {}'.format(self.name or 'value', val, self.length))
        return val


class TextType(VarcharType):
    _LENGTH = sys.maxsize


class IntegerType(BaseType):
    _TYPE = int
    _DEFAULT = -sys.maxsize
    _MIN = -sys.maxsize
    _MAX = sys.maxsize

    def __init__(self, *args, **kwargs):
        super(IntegerType, self).__init__(*args, **kwargs)
        self.min = kwargs.get('min', self._MIN)
        self.max = kwargs.get('max', self._MAX)

    @field_exception
    def validate(self, val):
        super().validate(val)
        val = self.type(val)
        if val is not None:
            if val > self.max or val < self.min:
                raise ValidationError(u'{}: {} not in [{}, {}]'.format(self.name, val, self.min, self.max))
        else:
            pass
        return val


class DatetimeType(BaseType):
    _TYPE = datetime.datetime
    _DEFAULT = datetime.datetime.now()
    _FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, *args, **kwargs):
        super(DatetimeType, self).__init__(*args, **kwargs)
        self.format = kwargs.get('format', self._FORMAT)

    @field_exception
    def validate(self, val):
        super().validate(val)
        if isinstance(val, six.string_types):
            try:
                val = self.type(val, self.format)
            except Exception:
                val = parser.parse(val)
        elif isinstance(val, self.type):
            pass
        else:
            raise ValidationError(u'{}: {} is not string type or {} type'.format(self.name or 'value', val, self.type))
        return val


class DateType(BaseType):
    _TYPE = datetime.date
    _DEFAULT = datetime.date.today()
    _FORMAT = '%Y-%m-%d'

    def __init__(self, *args, **kwargs):
        super(DateType, self).__init__(*args, **kwargs)
        self.format = kwargs.get('format', self._FORMAT)

    @field_exception
    def validate(self, val):
        super().validate(val)
        if isinstance(val, six.string_types):
            try:
                val = self.type(val, self.format)
            except Exception:
                val = parser.parse(val)
        elif isinstance(val, self.type):
            pass
        else:
            raise ValidationError(u'{}: {} is not string type or {} type'.format(self.name or 'value', val, self.type))
        return val


class TimestampType(BaseType):
    _TYPE = datetime.time
    _DEFAULT = datetime.datetime.now().time()

    @field_exception
    def validate(self, val):
        super().validate(val)
        if isinstance(val, six.string_types):
            val = datetime.datetime.fromtimestamp(float(val))
        elif isinstance(val, self.type):
            pass
        elif isinstance(val, datetime.datetime):
            val = val.time()
        else:
            raise ValidationError(u'{}: {} is not valid'.format(self.name or 'value', val, self.type))
        return val
