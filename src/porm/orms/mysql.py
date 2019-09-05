# -*- coding: utf-8 -*-
from __future__ import annotations

from collections import defaultdict
from enum import Enum, unique
from functools import partial
from typing import Union, Dict

from porm import BaseType, VarcharType
from porm.utils import param_notnone


@unique
class Operand(Enum):
    EQ = '='
    NEQ = '<>'
    GT = '>'
    GTE = '>='
    LT = '<'
    LTE = '<='
    IN = 'IN'
    NIN = 'NOT IN'
    LIKE = 'LIKE'
    NLIKE = 'NOT LIKE'


@unique
class Relation(Enum):
    AND = 'AND'
    OR = 'OR'


@unique
class Leaf(Enum):
    LEFT = '_left'
    RIGHT = '_right'


class ConditionNode(object):
    @classmethod
    def new(cls, field_name: str, operand: Operand = Operand.EQ, field_val=None) -> ConditionNode:
        return cls(field_name, operand, field_val)

    def __init__(self, field_name: str, operand: Operand = Operand.EQ, field_val=None):
        self._operand = operand
        self._field_name = field_name
        self._field_val = field_val

    def __str__(self):
        return u"{} {} {}".format(self._field_name, self._operand.value, self._field_val)

    def __repr__(self):
        return self.__str__()


class ConditionTree(object):
    @classmethod
    def new(cls, left: Union[ConditionNode, ConditionTree], relation: Relation,
            right: Union[ConditionNode, ConditionTree]) -> ConditionTree:
        return cls(left, relation, right)

    def __init__(
            self, left: Union[ConditionNode, ConditionTree], relation: Relation,
            right: Union[ConditionNode, ConditionTree]):
        self._relation: Relation = relation
        self._left: Union[ConditionNode, ConditionTree] = left
        self._right: Union[ConditionNode, ConditionTree] = right

    def __str__(self):
        left = str(self._left)
        right = str(self._right)
        return u'({} {} {})'.format(left, self._relation.value, right)

    def __repr__(self):
        return self.__str__()

    def or_condition(self, condition: Union[ConditionNode, ConditionTree], leaf: Leaf = Leaf.RIGHT) -> ConditionTree:
        leaf_obj = getattr(self, leaf.value)
        _t = ConditionTree.new(leaf_obj, Relation.OR, condition)
        setattr(self, leaf.value, _t)
        return self

    def and_condition(self, condition: Union[ConditionNode, ConditionTree], leaf: Leaf = Leaf.RIGHT) -> ConditionTree:
        leaf_obj = getattr(self, leaf.value)
        _t = ConditionTree.new(leaf_obj, Relation.AND, condition)
        setattr(self, leaf.value, _t)
        return self


class Condition(object):

    def __init__(self):
        self._condition: Union[ConditionTree, None] = None

    def __str__(self):
        return str(self._condition)

    def __repr__(self):
        return self.__str__()

    def _init(self, right: Union[ConditionTree, ConditionNode]):
        self._condition = ConditionTree.new(ConditionNode.new('1', Operand.EQ, '1'), Relation.AND, right)

    def _add(self, node: Union[ConditionTree, ConditionNode], relation: Relation = Relation.AND):
        if self._condition is None:
            self._init(node)
        else:
            self._condition = ConditionTree.new(self._condition, relation, node)
        return self

    def and_like(self, field_name: str, like_val: str) -> Condition:
        _node = ConditionNode.new(field_name, Operand.LIKE, like_val)
        self._add(_node)
        return self

    def and_nlike(self, field_name: str, nlike_val: str) -> Condition:
        _node = ConditionNode.new(field_name, Operand.NLIKE, nlike_val)
        self._add(_node)
        return self

    def and_eq(self, field_name: str, eq_val: str) -> Condition:
        _node = ConditionNode.new(field_name, Operand.EQ, eq_val)
        self._add(_node)
        return self

    def and_neq(self, field_name: str, neq_val: str) -> Condition:
        _node = ConditionNode.new(field_name, Operand.NEQ, neq_val)
        self._add(_node)
        return self

    def and_gt(self, field_name: str, gt_val: Union[str, int, float], equal: bool = False) -> Condition:
        _node = ConditionNode.new(field_name, Operand.GTE if equal else Operand.GT, gt_val)
        self._add(_node)
        return self

    def and_gte(self, field_name: str, gte_val: Union[str, int, float]) -> Condition:
        return self.and_gt(field_name, gte_val, equal=True)

    def and_lt(self, field_name: str, lt_val: Union[str, int, float], equal: bool = False) -> Condition:
        _node = ConditionNode.new(field_name, Operand.LTE if equal else Operand.LT, lt_val)
        self._add(_node)
        return self

    def and_lte(self, field_name: str, lte_val: Union[str, int, float]) -> Condition:
        return self.and_lt(field_name, lte_val, equal=True)

    def and_in_these(self, field_name: str, in_vals: tuple) -> Condition:
        _node = ConditionNode.new(field_name, Operand.IN, in_vals)
        self._add(_node)
        return self

    def and_nin_these(self, field_name: str, nin_vals: tuple) -> Condition:
        _node = ConditionNode.new(field_name, Operand.IN, nin_vals)
        self._add(_node)
        return self

    @classmethod
    def like(cls, field_name: str, like_val: str) -> ConditionNode:
        return ConditionNode.new(field_name, Operand.LIKE, like_val)

    @classmethod
    def nlike(cls, field_name: str, nlike_val: str) -> ConditionNode:
        return ConditionNode.new(field_name, Operand.NLIKE, nlike_val)

    @classmethod
    def eq(cls, field_name: str, eq_val: str) -> ConditionNode:
        return ConditionNode.new(field_name, Operand.EQ, eq_val)

    @classmethod
    def neq(cls, field_name: str, neq_val: Union[str, int, float]) -> ConditionNode:
        return ConditionNode.new(field_name, Operand.NEQ, neq_val)

    @classmethod
    def gt(cls, field_name: str, gt_val: Union[str, int, float], equal: bool = False) -> ConditionNode:
        return ConditionNode.new(field_name, Operand.GTE if equal else Operand.GT, gt_val)

    @classmethod
    def gte(cls, field_name: str, gte_val: Union[str, int, float]) -> ConditionNode:
        return cls.gt(field_name, gte_val, equal=True)

    @classmethod
    def lt(cls, field_name: str, lt_val: Union[str, int, float], equal: bool = False) -> ConditionNode:
        return ConditionNode.new(field_name, Operand.LTE if equal else Operand.LT, lt_val)

    @classmethod
    def lte(cls, field_name: str, lte_val: Union[str, int, float]) -> ConditionNode:
        return cls.lt(field_name, lte_val, equal=True)

    @classmethod
    def in_these(cls, field_name: str, in_vals: tuple) -> ConditionNode:
        return ConditionNode.new(field_name, Operand.IN, in_vals)

    @classmethod
    def nin_these(cls, field_name: str, nin_vals: tuple) -> ConditionNode:
        return ConditionNode.new(field_name, Operand.NIN, nin_vals)

    @classmethod
    def and_condition(
            cls, c1: Union[ConditionNode, ConditionTree], c2: Union[ConditionNode, ConditionTree]) -> ConditionTree:
        return ConditionTree.new(c1, Relation.AND, c2)

    @classmethod
    def or_condition(
            cls, c1: Union[ConditionNode, ConditionTree], c2: Union[ConditionNode, ConditionTree]) -> ConditionTree:
        return ConditionTree.new(c1, Relation.OR, c2)


FIELD_NONE_VALUE = 'FieldNoneValue'


class Field(object):
    def __init__(self, column_name: str, column_type: BaseType = VarcharType(), column_value=FIELD_NONE_VALUE):
        self._name = column_name
        self._type = column_type
        self._value = column_value if column_value != FIELD_NONE_VALUE else column_type.default

    @property
    def name(self) -> str:
        """
        column name
        :return:
        """
        return self._name

    @property
    def type(self) -> BaseType:
        """
        column type
        :return:
        """
        return self._type

    @property
    def value(self) -> any:
        """
        column value
        :return:
        """
        return self._value


class Join(object):
    @param_notnone
    def __init__(self, base_table: str, join_table: str, **eq_terms):
        """
        :param base_table: base table to join like A in select * from A join B
        :param join_table: join table to join like B in select * from A join B
        :param eq_terms: join conditions like id=Field('id') in select * from  A join B on (A.id=B.id)
        """
        # join_terms: {'join_tablename': {'key': ('value', 'LIKE'), 'field1': ('\\field2\\', '=')}}
        self._join_terms: Dict[str, Dict[str, tuple]] = {join_table: {}}
        self._set_terms(join_table, base_table, **eq_terms)

    def _set_terms(self, join_table: str, base_table: str, **eq_terms):
        join_term = self._join_terms.get(join_table, {})
        for base_key, join_key in eq_terms.items():
            if '.' not in base_key:
                base_column = '{}.{}'.format(base_table, base_key)
            else:
                base_column = base_key
            if isinstance(join_key, Field):
                if '.' not in join_key.name:
                    join_column = '{}.{}'.format(join_table, join_key.name)
                else:
                    join_column = join_key.name
                join_column = '\\{}\\'.format(join_column)
            else:
                join_column = join_key
            join_term[base_column] = (join_column, '=')
        self._join_terms[join_table] = join_term

    @param_notnone
    def join(self, base_table, join_table, **eq_terms) -> Join:
        self._set_terms(join_table, base_table, **eq_terms)
        return self

    def to_json(self) -> Dict[str, Dict[str, tuple]]:
        """

        :return: {'join_tablename': {'base_tablename.field1': ('value', 'LIKE'), 'base_tablename.field1': ('\\join_tablename.field2\\', '=')}}
        """
        return self._join_terms.copy()


class SQL(object):

    def __init__(self, sql: str, params: dict = None):
        self._sql = sql
        self._params = params

    @property
    def sql(self) -> str:
        return self._sql

    @property
    def param(self) -> dict:
        return self._params
