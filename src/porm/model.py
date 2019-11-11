from __future__ import annotations
import json
import logging
from collections import OrderedDict
from contextlib import contextmanager
from copy import deepcopy
from typing import List, Union, Dict

from porm.databases.api import _transaction
from porm.databases.api.mysql import MyDBApi
from porm.errors import ValidationError, EmptyError, ParamError
from porm.orms import Field, Join, SQL
from porm.parsers.mysql import parse, parse_join, ParsedResult
from porm.types.core import VarcharType, BaseType, IntegerType, DictType
from porm.utils import param_notempty, type_check, PormJsonEncoder, notnone_check

__all__ = ("DBModel",)
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logger = logging.getLogger('porm')
logger.addHandler(NullHandler())


class Table(object):

    @param_notempty
    def __init__(self, database_name: str, table_name: str):
        """

        :param database_name:
        :param table_name:
        """
        self.database = database_name
        self.name = table_name
        self._columns = set()
        self._primary_keys = set()

    @property
    def full_name(self):
        return u'{}.{}'.format(self.database, self.name)

    @property
    def primary_keys(self) -> tuple:
        return tuple(self._primary_keys)

    @property
    def columns(self) -> tuple:
        return tuple(self._columns)

    def add_primary_key(self, primary_key: str):
        return self._primary_keys.add(primary_key)

    def add_column(self, column: str):
        return self._columns.add(column)


class DBModelMetaData(object):
    """
    Meta data holding the information that will not changed after defined
    """

    def __init__(self, database_name: str, table_name: str, **connection_config):
        self.database_name = database_name
        self.table_name = table_name
        self._fields: Dict[str, Field] = OrderedDict()
        self._database = None
        self._connection_config = connection_config
        self._table = None
        self._init_table()

    def _init_table(self):
        if self._table is None:
            self._table = Table(self.database_name, self.table_name)
            for field_name, field_obj in self._fields.items():
                if field_obj.type.ispk():
                    self._table.add_primary_key(field_name)
                else:
                    self._table.add_column(field_name)

        return self._table

    @property
    def table(self) -> Table:
        if self._table is None:
            self._init_table()
        return self._table

    @property
    def config(self):
        return self._connection_config.copy()

    @property
    def fields(self) -> List[str]:
        return list(self._fields.keys())

    @property
    def fields_with_tablename(self) -> List[str]:
        tablename = self.get_full_table_name()
        return ['{}.{}'.format(tablename, field) for field in self._fields.keys()]

    def has_field(self, field_name) -> bool:
        return field_name in self._fields

    @type_check(field_type=BaseType)
    def add_field(self, field_name: str, field_type: BaseType = VarcharType()):
        tablename = self.get_full_table_name() + '.'
        field_name_with_table = field_name if field_name.startswith(tablename) else tablename + field_name
        field_type.set_name(field_name_with_table)
        self._fields[field_name] = Field(field_name_with_table, field_type)
        if field_type.ispk():
            self.table.add_primary_key(field_name)
        else:
            self.table.add_column(field_name)

    def get_field(self, field_name) -> Field:
        return self._fields[field_name]

    def get_fields(self) -> List[Field]:
        return list(self._fields.values())

    def get_field_type(self, field_name: str) -> BaseType:
        return self._fields[field_name].type

    def get_insert_sql_tpl(self, db: str = None, table: str = None, ignore: bool = False):
        """
        Get an insert sql template
        :param db:
        :param table:
        :param ignore:
        :return:
        """
        dbtb = self._full_table_name(db=db, table=table)
        ignore_str = 'IGNORE' if ignore else ''
        return """INSERT {ignore} INTO {dbtb} ({{col}}) VALUES ({{col_param}})""".format(
            ignore=ignore_str,
            dbtb=dbtb
        )

    def get_select_sql_tpl(self, db: str = None, table: str = None):
        dbtb = self._full_table_name(db=db, table=table)
        return """SELECT {{return_columns}} FROM {dbtb} WHERE {{filter}}""".format(dbtb=dbtb)

    def get_upsert_sql_tpl(self, db: str = None, table: str = None):
        insert_sql_tpl = self.get_insert_sql_tpl(db=db, table=table)
        return insert_sql_tpl + "\t ON DUPLICATE KEY UPDATE {update_fields}"

    def get_for_update_sql_tpl(self, db: str = None, table: str = None):
        get_sql_tpl = self.get_select_sql_tpl(db=db, table=table)
        return get_sql_tpl + "\tFOR UPDATE"

    def get_update_sql_tpl(self, db: str = None, table: str = None):
        dbtb = self._full_table_name(db=db, table=table)
        return """UPDATE {dbtb} SET {{update_columns}} WHERE {{filter}}""".format(dbtb=dbtb)

    def get_delete_sql_tpl(self, db: str = None, table: str = None):
        dbtb = self._full_table_name(db=db, table=table)
        return """DELETE FROM {dbtb} WHERE {{filter}}""".format(dbtb=dbtb)

    def get_drop_sql_tpl(self, db: str = None, table: str = None, ifexists: bool = False):
        dbtb = self._full_table_name(db=db, table=table)
        ifexists_str = 'IF EXISTS' if ifexists else ''
        return """DROP TABLE {ifexists} {dbtb}""".format(ifexists=ifexists_str, dbtb=dbtb)

    def _full_table_name(self, db: str = None, table: str = None):
        table = table.strip() if table is not None else table
        if table:
            if db:
                db_name = db
            else:
                db_name = self.table.database
            db_name += '.'
            if table.startswith(db_name):
                dbtb = table
            else:
                dbtb = db_name + table
        else:
            dbtb = self.table.full_name
        return dbtb

    def get_full_table_name(self, db: str = None, table: str = None):
        return self._full_table_name(db=db, table=table)


class DBModelMeta(type):
    """
    The metaclass for `DBModel` and any subclasses of `DBModel`.
    `DBModelMeta`'s responsibility is to create the `_unbound_fields` list, which
    is a list of `UnboundField` instances sorted by their order of
    instantiation.  The list is created at the first instantiation of the dbmodel.
    If any types are added/removed from the form, the list is cleared to be
    re-generated on the next instantiation.
    Any properties which begin with an underscore or are not `UnboundField`
    instances are ignored by the metaclass.
    """

    CUSTOM_ATTR = {'__DATABASE__', '__TABLE__', '__CONFIG__', '__DB__'}

    def __new__(mcs, name: str, bases: tuple, attrs: dict):
        """
        Meta class of search form
        :param name: name of current decorated class
        :param bases: base classes of current decorated class
        :param attrs: attributes of decorated class
        :return: an instance of the type class
        """
        mcs.init_meta_data(name, bases, attrs)
        mcs.set_columns(bases, attrs)
        return super().__new__(mcs, name, bases, attrs)

    @staticmethod
    def init_meta_data(name: str, bases: tuple, attrs: dict):
        database_name = attrs.get('__DATABASE__', attrs.get('__DB__', None))
        if database_name:
            setattr(bases[0], '__DATABASE__', database_name)
        table_name = attrs.get('__TABLE__', name)
        attrs['__TABLE__'] = table_name
        connection_config = attrs.get('__CONFIG__', None)
        if connection_config:
            setattr(bases[0], '__CONFIG__', connection_config)
        # metadata = DBModelMetaData(database_name=database_name, table_name=table_name, **connection_config)
        # attrs['__META__'] = metadata
        # return metadata

    @staticmethod
    def set_columns(bases: tuple, attrs: dict, col_types: tuple = ('__FIELDS__', '__PK__')):
        _pks = OrderedDict()
        _fields = OrderedDict()
        if bases[0].__name__ not in ('BaseDBModel', 'DBModel'):
            for field_name, field_type in getattr(bases[0], '__PK__', []):
                _pks[field_name] = field_type
            for field_name, field_type in getattr(bases[0], '__FIELDS__', []):
                _fields[field_name] = field_type
        else:
            pass
        for col_type in col_types:
            if col_type in attrs:
                field_attrs = attrs[col_type]
                del attrs[col_type]
                for field in field_attrs:
                    if isinstance(field, tuple):
                        # support tuple define like: [('name', TextField), ('info', TextField)]
                        field_name = field[0]
                        field_type = field[1]
                        if callable(field_type):
                            if field == '__PK__':
                                field_type = field_type(ispk=True)
                            else:
                                field_type = field_type(ispk=False)
                        elif isinstance(field_type, BaseType):
                            if field == '__PK__' and field_type.ispk() is False:
                                field_type.set_pk(ispk=True)
                        else:
                            raise ParamError(u'Error Field Type: {}'.format(field_type))
                    else:
                        # support string define like: ['name', 'info']
                        field_name = field
                        if field == '__PK__':
                            field_type = IntegerType(ispk=True)
                        else:
                            field_type = VarcharType(ispk=False)
                    field_type.set_name(field_name)
                    if field_type.ispk():
                        _pks[field_name] = field_type
                    else:
                        _fields[field_name] = field_type
                    # metadata.add_field(field_name=field_name, field_type=field_type)
        for field_name, field_type in list(attrs.items()):
            if isinstance(field_type, BaseType):
                del attrs[field_name]
                field_type.set_name(field_name)
                if field_type.ispk():
                    _pks[field_name] = field_type
                else:
                    _fields[field_name] = field_type
            else:
                continue
            # metadata.add_field(field_name=field_name, field_type=field_type)
        _pklist = [(field_name, field_type) for field_name, field_type in _pks.items()]
        _fieldlist = [(field_name, field_type) for field_name, field_type in _fields.items()]
        if bases[0].__name__ in ('BaseDBModel', 'DBModel'):
            setattr(bases[0], '__PK__', _pklist)
            setattr(bases[0], '__FIELDS__', _fieldlist)
        attrs['__PK__'] = _pklist
        attrs['__FIELDS__'] = _fieldlist
        # return metadata


class BaseDBModel(dict):
    """
    Base DBModel Class.  Provides core behaviour like field construction,
    validation, and data and error proxying.
    """

    __META__: DBModelMetaData = None
    __DATABASE__: str = 'PORM_DATABASE'
    __DB__: str = 'PORM_DATABASE'
    __TABLE__: str = 'BaseDBModel'
    __CONFIG__: dict = None

    def __new__(cls, *args, **kwargs):
        _metadata = cls._init_cls_meta_data()
        cls._set_cls_columns(_metadata)
        return dict.__new__(cls, *args, **kwargs)

    @classmethod
    def _init_cls_meta_data(cls):
        database_name = getattr(cls, '__DATABASE__', getattr(cls, '__DB__', None))
        table_name = getattr(cls, '__TABLE__', cls.__class__.__name__)
        connection_config = getattr(cls, '__CONFIG__', None)
        metadata = DBModelMetaData(database_name=database_name, table_name=table_name, **connection_config)
        setattr(cls, '__META__', metadata)
        return metadata

    @classmethod
    def _set_cls_columns(cls, metadata: DBModelMetaData, col_types: tuple = ('__FIELDS__', '__PK__')):
        cols = OrderedDict()
        attr_names = dir(cls)

        for col_type in col_types:
            if col_type in attr_names:
                fields = getattr(cls, col_type)
                for field in fields:
                    if isinstance(field, tuple):
                        # support tuple define like: [('name', TextField), ('info', TextField)]
                        field_name = field[0]
                        field_type = field[1]
                        if callable(field_type):
                            if field == '__PK__':
                                field_type = field_type(ispk=True)
                            else:
                                field_type = field_type(ispk=False)
                        elif isinstance(field_type, BaseType):
                            if field == '__PK__' and field_type.ispk() is False:
                                field_type.set_pk(ispk=True)
                        else:
                            raise ParamError(u'Error Field Type: {}'.format(field_type))
                    else:
                        # support string define like: ['name', 'info']
                        field_name = field
                        if field == '__PK__':
                            field_type = IntegerType(ispk=True)
                        else:
                            field_type = VarcharType(ispk=False)
                    field_type.set_name(field_name)
                    cols[field_name] = field_type
                    metadata.add_field(field_name, field_type=field_type)
        return metadata

    def __init__(self, **kwargs):
        super(BaseDBModel, self).__init__(**kwargs)
        self._data = dict()
        self._actived_fields = dict()
        self._init_data(**kwargs)

    def _init_data(self, **kwargs):
        for field_name, field_val in kwargs.items():
            field = self.__META__.get_field_type(field_name)
            valid_val = field.validate(field_val)
            self._data[field_name] = valid_val
            self._actived_fields[field_name] = True

    def __len__(self) -> int:
        return len(self._data)

    def __getitem__(self, field_name: str):
        if self.__META__.has_field(field_name) and field_name in self._actived_fields:
            if field_name in self._data:
                return self._data[field_name]
            else:
                raise EmptyError(u'Empty Value: {}'.format(field_name))
        else:
            raise ValidationError(u'Unkown Field: {} In Valid Fields: {}'.format(field_name, self.__META__.fields))

    def __setitem__(self, field_name: str, field_val):
        if self.__META__.has_field(field_name):
            self._data[field_name] = self.__META__.get_field_type(field_name).validate(field_val)
            self._actived_fields[field_name] = True
        else:
            raise ValidationError(u'Unkown Field: {} In Valid Fields: {}'.format(field_name, self.__META__.fields))

    def __delitem__(self, key: str) -> None:
        del self._data[key]
        del self._actived_fields[key]

    def __iter__(self):
        return self._data.__iter__()

    def __contains__(self, field_name: object) -> bool:
        return self.__META__.has_field(field_name) and field_name in self._actived_fields

    def copy(self):
        _d = {}
        for actived_field in self._actived_fields.keys():
            _d[actived_field] = self._data[actived_field]
        return self.__class__(**deepcopy(_d))

    def __str__(self):
        return json.dumps(self._data, cls=PormJsonEncoder)

    def __repr__(self):
        ret = {}
        for primary_key in self.__META__.table.primary_keys and self._data.keys():
            ret[primary_key] = self._data[primary_key]
        for column in self.__META__.table.columns and self._data.keys():
            ret[column] = self._data[column]
        return repr(ret)

    def __getattr__(self, item):
        # if item == '__DB__':
        #     return self.__DB__
        # elif item == '__DATABASE__':
        #     return self.__DATABASE__
        # elif item == '__CONFIG__':
        #     return self.__CONFIG__
        # elif item == '__TABLE__':
        #     return self.__TABLE__
        # elif item == 'dbi':
        #     return self.dbi
        # el
        if item in self.__META__.fields:
            if self.is_valid_field(item):
                return self._data[item]
            else:
                raise EmptyError(u'Field: {} is Not Valid'.format(item))
        else:
            a = super(BaseDBModel, self).__getattribute__(item)
            return a
            # raise NotSupportError(
            #     u'Field: {} Not in Defined Field: {} of {}'.format(
            #         item, self.__META__.fields, self.__META__.get_full_table_name()))

    def is_valid_field(self, field_name: str) -> bool:
        """
        Check field is valid or not:
        1. A get method generated object
        2. A new method generated object
        3. A set method activated object field
        including pk and columns
        :return:
        """
        return self.__META__.has_field(field_name) and field_name in self._actived_fields

    def get_valid_fields(self, for_save=True) -> OrderedDict:
        """
        Get the valid fields of:
        1. A get method generated object
        2. A new method generated object
        3. A set method activated object field
        including pk and columns
        :return:
        """
        ret = OrderedDict()
        for _fn, val in self._data.items():
            if self.__META__.has_field(_fn) and _fn in self._actived_fields:
                if for_save:
                    _ft = self.__META__.get_field_type(field_name=_fn)
                    if isinstance(_ft, DictType):
                        ret[_fn] = _ft.dumps(val)
                    else:
                        ret[_fn] = _ft.dumps(val)
                else:
                    ret[_fn] = val
            else:
                continue
        return ret

    @property
    def valid_fields(self) -> OrderedDict:
        """
        Get the valid fields of:
        1. A get method generated object
        2. A new method generated object
        3. A set method activated object field
        including pk and columns
        :return:
        """
        ret = OrderedDict()
        for _fn, val in self._data.items():
            if self.__META__.has_field(_fn) and _fn in self._actived_fields:
                ret[_fn] = val
            else:
                continue
        return ret

    @property
    def pk_fields(self) -> OrderedDict:
        """
        Get the valid pk fields of:
        1. A get method generated object
        2. A new method generated object
        3. A set method activated object field
        :return:
        """
        ret = OrderedDict()
        for pk in self.__META__.table.primary_keys:
            if pk in self._actived_fields:
                ret[pk] = self._data.get(pk, self.__META__.get_field_type(pk).default)
        return ret

    def get_column_fields(self, for_save=True) -> OrderedDict:
        ret = OrderedDict()
        for col in self.__META__.table.columns:
            if self.is_valid_field(col):
                _ft = self.__META__.get_field_type(col)
                if for_save:
                    ret[col] = _ft.dumps(self._data.get(col, _ft.default))
                else:
                    ret[col] = self._data.get(col, _ft.default)
        return ret

    @property
    def column_fields(self) -> OrderedDict:
        return self.get_column_fields(for_save=False)

    def _render_insert(self, ignore=False) -> SQL:
        _valid_fields = self.get_valid_fields(for_save=True)
        fields = list(_valid_fields.keys())
        _sql_tpl = self.__META__.get_insert_sql_tpl(ignore=ignore)
        _sql = _sql_tpl.format(
            col=', '.join(fields),
            col_param=', '.join(['%({f})s'.format(f=field) for field in fields]),
        )
        return SQL(_sql, _valid_fields)

    @property
    def _insert_sql(self) -> SQL:
        return self._render_insert(ignore=False)

    @property
    def _insert_ignore_sql(self) -> SQL:
        return self._render_insert(ignore=True)

    @property
    def _upsert_sql(self) -> SQL:
        _valid_fields = self.get_valid_fields(for_save=True)
        _sql_tpl = self.__META__.get_upsert_sql_tpl()
        _sql = _sql_tpl.format(
            col=', '.join(self.valid_fields.keys()),
            col_param=', '.join(['%({f})s'.format(f=field) for field in _valid_fields.keys()]),
            update_fields='{update_fields}'
        )
        return SQL(_sql, _valid_fields)

    @property
    def _update_sql(self) -> SQL:
        _valid_fields = self.get_column_fields(for_save=True)
        _sql = self.__META__.get_update_sql_tpl().format(
            update_columns=', '.join(['{f}=%({f})s'.format(f=field) for field in _valid_fields.keys()]),
            filter='{filter}'
        )
        return SQL(_sql, _valid_fields)


class DBModel(BaseDBModel, metaclass=DBModelMeta):
    """
    Declarative DBModel base class. Extends BaseDBModel's core behaviour allowing
    types to be defined on DBModel subclasses as class attributes.
    """
    __metaclass__ = DBModelMeta

    @classmethod
    def _get_db_conf(cls, db=None):
        if db:
            _db_conf = cls.__META__.config
            _db_conf['db'] = db
        else:
            _db_conf = cls.__META__.config
        return _db_conf

    @classmethod
    def _check_meta(cls):
        if cls.__META__ is None:
            _metadata = cls._init_cls_meta_data()
            cls._set_cls_columns(_metadata)

    @classmethod
    def new(cls, **kwargs) -> DBModel:
        obj = cls(**kwargs)
        return obj

    @classmethod
    @contextmanager
    def start_transaction(cls, db: Union[str, dict]=None) -> _transaction:
        cls._check_meta()
        if isinstance(db, dict):
            config = db
        else:
            config = cls._get_db_conf(db=db)
        mydb = MyDBApi(config=config)
        with mydb.start_transaction() as _t:
            yield _t

    @classmethod
    def count(
            cls, return_columns='COUNT(1) as cnt', db=None, table=None, join_table=None, t: _transaction = None,
            **terms) -> int:
        cls._check_meta()
        cnt_table = cls.__META__.get_full_table_name(db=db, table=table)
        cnt_parsed = parse(**terms)
        if join_table:
            for join_t in join_table.keys():
                join_parsed = parse_join(**join_table[join_t])
                cnt_table += ' JOIN {} ON ({}) '.format(join_t, join_parsed['filter'])
                cnt_parsed['param'].update(join_parsed['param'])
        _get_sql_tpl = cls.__META__.get_select_sql_tpl(db=db, table=cnt_table)
        cnt_sql = _get_sql_tpl.format(
            return_columns=return_columns,
            filter=cnt_parsed.filter
        )
        config = cls._get_db_conf(db=db)
        mydb = MyDBApi(config=config, t=t)
        total_cnt = mydb.query(cnt_sql, cnt_parsed.param)[0]['cnt']
        return int(total_cnt)

    @classmethod
    def search(
            cls, return_columns=None, order_by=None, db=None, table=None, t: _transaction = None,
            **terms) -> SearchResult:
        """
        分页查询接口
        :param return_columns:
        :param order_by:
        :param db:
        :param table:
        :param t: transaction
        :param terms: {'key': ('value', 'LIKE')}
        :return:
        :rtype SearchResult
        """
        cls._check_meta()
        page = max(terms.pop('page', 1) or 1, 1)
        size = terms.pop('size', 10)
        total_cnt = cls.count(db=db, table=table, t=t, **terms)
        rets = cls.get_many(
            return_columns=return_columns, order_by=order_by, db=db, table=table, page=page, size=size, t=t, **terms)
        return SearchResult(total=total_cnt, index=page - 1, size=size, result=rets)

    @classmethod
    def search_and_join(
            cls, return_columns=None, order_by=None, db=None, table=None, t: _transaction = None, join_table=None,
            **terms) -> SearchResult:
        """
        分页查询接口
        :param return_columns:
        :param order_by:
        :param db:
        :param table:
        :param t: transaction
        :param join_table: {'join_tablename': {'base_tablename.field1': ('value', 'LIKE'), 'base_tablename.field1': ('\\join_tablename.field2\\', '=')}}
        :param terms: {'key': ('value', 'LIKE')}
        :return:
        """
        cls._check_meta()
        if not join_table:
            return cls.search(return_columns=return_columns, order_by=order_by, db=db, table=table, t=t, **terms)
        else:
            page = max(terms.pop('page', 1) or 1, 1)
            size = terms.pop('size', 10)
            total_cnt = cls.count(db=db, table=table, join_table=join_table, t=t, **terms)
            rets = cls.get_many_and_join(
                return_columns=return_columns, order_by=order_by, db=db, get_tablename=table, page=page, size=size, t=t,
                join_table=join_table, **terms)
            return SearchResult(total=total_cnt, index=page - 1, size=size, result=rets)

    @classmethod
    def _query_by_parsed_terms(
            cls, return_columns=None, db=None, table=None, t=None, for_update=False, parsed: ParsedResult = None):
        cls._check_meta()
        if not return_columns:
            return_columns = cls.__META__.fields_with_tablename
        if not for_update:
            sql = cls.__META__.get_select_sql_tpl(db=db, table=table).format(
                return_columns=', '.join(return_columns),
                filter=parsed['filter']
            )
        else:
            sql = cls.__META__.get_for_update_sql_tpl(db=db, table=table).format(
                return_columns=', '.join(return_columns),
                filter=parsed['filter']
            )
        param = parsed['param']
        mydb = MyDBApi(config=cls._get_db_conf(db=db), t=t)
        return mydb.query_many(sql, param)

    @classmethod
    def _get_by_parsed_terms(
            cls, return_columns=None, db=None, table=None, t=None, for_update=False, parsed: ParsedResult = None):
        rets = [
            cls.new(**json.loads(json.dumps(obj, cls=PormJsonEncoder))) for obj in cls._query_by_parsed_terms(
                return_columns=return_columns, db=db, table=table, t=t, for_update=for_update, parsed=parsed
            )]
        return rets

    @classmethod
    def _join_get_by_parsed_terms(
            cls, return_columns=None, db=None, table=None, t=None, for_update=False, parsed: ParsedResult = None):
        return cls._query_by_parsed_terms(
            return_columns=return_columns, db=db, table=table, t=t, for_update=for_update, parsed=parsed)

    @classmethod
    def get_many(
            cls, return_columns=None, order_by=None, db=None, table=None, t: _transaction = None, for_update=False,
            **terms) -> list:
        """
        全量查询接口
        :param return_columns:
        :param order_by:
        :param db:
        :param table:
        :param t:
        :param for_update:
        :param terms:
        :return:
        """
        cls._check_meta()
        parsed = parse(order_by=order_by, **terms)
        rets = cls._get_by_parsed_terms(
            return_columns=return_columns, db=db, table=table, t=t, for_update=for_update, parsed=parsed)
        return rets

    @classmethod
    def get_many_and_join(
            cls, return_columns=None, order_by=None, db=None, get_tablename=None, t: _transaction = None,
            for_update=False, join_table=None, parse_with_tablename=False, **terms) -> List[DBModel]:
        """
        全量连接查询接口
        :param return_columns:
        :param order_by:
        :param db:
        :param get_tablename:
        :param t:
        :param for_update:
        :param terms:
        :param join_table:
        :param parse_with_tablename:
        :return:
        """
        cls._check_meta()
        get_tablename = cls.__META__.get_full_table_name(db=db, table=get_tablename)
        term_tablename = get_tablename if parse_with_tablename else None
        parsed = parse(tablename=term_tablename, order_by=order_by, **terms)
        for join_t in join_table.keys():
            join_parsed = parse_join(**join_table[join_t])
            get_tablename += ' JOIN {} ON ({}) '.format(join_t, join_parsed['filter'])
            parsed['param'].update(join_parsed['param'])
        rets = cls._join_get_by_parsed_terms(
            return_columns=return_columns, db=db, table=get_tablename, t=t, for_update=for_update, parsed=parsed)
        return rets

    @classmethod
    def get_one(cls, return_columns=None, t: _transaction = None, for_update=False, **kwargs) -> Union[None, DBModel]:
        _l = cls.get_many(return_columns=return_columns, t=t, for_update=for_update, page=0, size=1, **kwargs)
        if _l:
            return _l[0]
        else:
            return None

    @classmethod
    def delete_many(cls, t: _transaction = None, **terms):
        """
        批量查询接口
        :param t:
        :param terms: 删除过滤条件
        :return:
        """
        cls._check_meta()
        if not terms:
            raise Exception(u'ERROR: Unknow delete terms')
        parsed = parse(**terms)
        param = parsed['param']
        delete_tpl = cls.__META__.get_delete_sql_tpl()
        sql = delete_tpl.format(filter=parsed['filter'])
        mydb = MyDBApi(config=cls._get_db_conf(), t=t)
        return mydb.delete(sql, param)

    @classmethod
    def insert_many(cls, objs: List[BaseDBModel], t: _transaction = None, ignore=False):
        """
        批量插入接口
        :param objs:
        :param t:
        :param ignore: 执行insert ignore语义
        :return:
        """
        cls._check_meta()
        if not objs:
            return None
        _sql_tpls = set()
        _params = []
        for obj in objs:
            if not isinstance(obj, cls):
                raise Exception(u'ERROR: Unknown type: {} in valid type: {}'.format(type(obj), cls))
            else:
                _sql_obj = obj._insert_ignore_sql if ignore else obj._insert_sql
                _sql_tpls.add(_sql_obj.sql)
                _params.append(_sql_obj.param)
        if len(_sql_tpls) == 1:
            # 如果所有生成的sql一样，说明插入的对象一样
            _sql_tpl = _sql_tpls.pop()
        else:
            # 如果生成的sql不一样，说明存在不同的插入对象
            # 这种情况可能会存在插入Null值，请注意
            _sql_tpl = cls._insert_ignore_sql if ignore else cls._insert_sql
            _params = [obj.get_valid_fields(for_save=True) for obj in objs]
        mydb = MyDBApi(config=cls._get_db_conf(), t=t)
        return mydb.insert_many(_sql_tpl, _params)

    @classmethod
    def get_tablename(cls, db: str = None) -> str:
        """

        :param db:
        :return: full table name string like DB1.Table2
        """
        cls._check_meta()
        return cls.__META__.get_full_table_name(db=db)

    @classmethod
    def get_field(cls, name: str) -> Field:
        """

        :param name:
        :return:
        """
        cls._check_meta()
        return cls.__META__.get_field(name)

    @classmethod
    def get_fields(cls) -> List[Field]:
        """
        :return:
        """
        cls._check_meta()
        return cls.__META__.get_fields()

    @classmethod
    def join(cls, join_table: DBModel.__class__, **eq_terms) -> Join:
        """

        :param join_table: DBModel class ob table to join
        :param eq_terms:
        :return:
        """
        cls._check_meta()
        ret = Join(cls.__META__.get_full_table_name(), join_table.get_tablename(), **eq_terms)
        return ret

    @classmethod
    def drop(cls, ifexists: bool = False, t: _transaction = None):
        """

        :param ifexists:
        :param t:
        :return:
        """
        cls._check_meta()
        mydb = MyDBApi(config=cls._get_db_conf(), t=t)
        _sql = cls.__META__.get_drop_sql_tpl(ifexists=ifexists)
        return mydb.delete(_sql)

    @classmethod
    @notnone_check('sql')
    def create(cls, sql: SQL = None, t: _transaction = None):
        """

        :param sql:
        :param t:
        :return:
        """
        cls._check_meta()
        mydb = MyDBApi(config=cls._get_db_conf(), t=t)
        return mydb.insert_one(sql.sql)

    @property
    def dbi(self) -> MyDBApi:
        return MyDBApi(config=self._get_db_conf())

    def upsert(self, t: _transaction = None, *update_fields):
        _valid_fields = self.get_valid_fields(for_save=True)
        if not update_fields:
            update_fields = list(_valid_fields.keys())
        sql = self.__META__.get_upsert_sql_tpl().format(
            update_fields=', '.join('{field}=%({field})s'.format(field=field) for field in update_fields))
        param = _valid_fields
        mydb = MyDBApi(config=self._get_db_conf(), t=t)
        return mydb.insert_one(sql, param)

    def insert(self, t: _transaction = None):
        mydb = MyDBApi(config=self._get_db_conf(), t=t)
        sql_obj = self._insert_sql
        return mydb.insert_one(sql_obj.sql, param=sql_obj.param)

    def reset(self, **reset_fields):
        """
        使用新的列值更新当前对象
        :param reset_fields:
        :return:
        """
        for f, v in reset_fields.items():
            if f in self.__META__.table.columns:
                self[f] = v

    def update(self, t: _transaction = None, **filters):
        """
        使用当前对象的值去更新数据库记录
        :param t:
        :param filters: 当filters参数非空，函数表示用当前的值去更新所有符合filter条件的数据
        :return:
        """

        if not filters:
            filters = self.pk_fields
        f = []
        sql_obj = self._update_sql
        sql = sql_obj.sql
        param = sql_obj.param
        for key, val in filters.items():
            f.append(u'{key}=%(f_{key})s'.format(key=key))
            param[u'f_' + key] = val
        sql = sql.format(filter=' AND '.join(f))
        mydb = MyDBApi(config=self._get_db_conf(), t=t)
        return mydb.insert_one(sql, param)

    def delete(self, t: _transaction = None):
        """
        删除当前对象对应的数据库记录
        :param t:
        :return:
        """
        filters = self.pk_fields
        f = []
        param = {}
        for key, val in filters.items():
            f.append(u'{key}=%(f_{key})s'.format(key=key))
            param[u'f_' + key] = val
        sql = self.__META__.get_delete_sql_tpl().format(
            filter=' AND '.join(f))
        mydb = MyDBApi(config=self._get_db_conf(), t=t)
        return mydb.delete(sql, param)


class SearchResult(dict):
    """
    查询返回结果
    """

    def __init__(self, total=0, index=0, size=10, result=None, *args, **kwargs):
        super(SearchResult, self).__init__(*args, **kwargs)
        self['total'] = total
        self['index'] = index
        self['page'] = index + 1
        self['size'] = size
        self['result'] = result

    def pagination(self):
        """
        转换成前端分页模式数据
        :return:
        """
        return {
            'data': self['result'],
            'pagination': {
                'page': self['page'],
                'index': self['index'],
                'total': self['total'],
                'size': self['size']
            }
        }

    @property
    def total(self):
        return self['total']

    @property
    def index(self):
        return self['index']

    @property
    def size(self):
        return self['size']

    @property
    def page(self):
        return self['page']

    @property
    def result(self):
        return self['result']
