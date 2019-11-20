import datetime
import json

import pymysql

from porm import IntegerType, VarcharType, TextType, DatetimeType, FloatType, BooleanType
from porm.model import DBModel
from porm.orms import SQL
from porm.types.core import TimeType, DictType
from tests.test_common import DatabaseTestCase


class TestModel(DBModel):
    __DATABASE__ = 'porm_database_test'
    __CONFIG__ = {
        'host': 'localhost',
        'user': 'root',
        'password': 'root',
        'db': 'porm_database_test',
        'charset': 'utf8',
        'autocommit': 0,  # default 0
        'cursorclass': pymysql.cursors.DictCursor
    }


class UserInfoBase(TestModel):
    userid = IntegerType(pk=True, required=True)
    username = VarcharType(required=True)
    email = VarcharType(required=True)
    descr = TextType(required=False, default=None)
    createtime = DatetimeType(required=False, default=None)
    updatetime = DatetimeType(required=False, default=None)
    is_active = IntegerType(required=False, default=1)
    start_time = TimeType(required=True, default=datetime.time.fromisoformat('08:00:00'))
    properties = DictType(required=True)


class AnotherInfoBase(TestModel):
    weight = FloatType(required=True, default=180)


class UserInfo(UserInfoBase):
    height = FloatType(required=True, default=180)


class UserInfo2(UserInfoBase):
    height = FloatType(required=True, default=180)


class AnotherInfo(AnotherInfoBase):
    score = FloatType(required=True, default=180)


class BodyInfoBase(TestModel):
    id = IntegerType(pk=True, required=True)
    createtime = DatetimeType(required=False, default=None)
    updatetime = DatetimeType(required=False, default=None)


class UserBodyInfo(BodyInfoBase):
    weight = FloatType(required=True)
    userid = IntegerType(required=True)
    someone = BooleanType(required=True)


class TestDatabase(DatabaseTestCase):
    user_info = None

    def test_00_ormobj_createtable(self):
        with UserInfo.start_transaction() as _t:
            UserInfo.drop(ifexists=True, t=_t)
            sql = SQL("""
            CREATE TABLE `UserInfo` (
              `userid` bigint NOT NULL AUTO_INCREMENT,
              `email` varchar(255) NOT NULL COMMENT '电子邮箱',
              `username` varchar(255) NOT NULL COMMENT '用户名',
              `descr` text DEFAULT NULL COMMENT '用户说明',
              `createtime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
              `updatetime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
              `is_active` tinyint(1) DEFAULT '1' COMMENT '用户有效标志',
              `height` DECIMAL(10,4) NOT NULL comment '售卖价格',
              `start_time` time default '08:00:00' comment '开始时间',
              `properties` JSON NOT NULL COMMENT '添加json',
              PRIMARY KEY (`userid`),
              UNIQUE KEY `email` (`email`),
              UNIQUE KEY `username` (`username`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;""")
            UserInfo.create(sql=sql, t=_t)

    def test_01_ormobj_crud(self):
        with UserInfo.start_transaction() as _t:
            self.user_info = UserInfo.new(
                email='dennias.chiu@gmail.com', username='dennias', height=188,
                start_time=datetime.time.fromisoformat('08:00:00'), properties={'abc': 'yoyo'})
            self.user_info.insert(t=_t)
            obj = UserInfo.get_one(email='dennias.chiu@gmail.com', username='dennias')
            self.assertIsNone(obj)
            obj = UserInfo.get_one(email='dennias.chiu@gmail.com', username='dennias', t=_t)
            self.assertEqual(obj['start_time'], datetime.time(8, 0))
            self.assertIsNotNone(obj)
            obj.reset(email='123@123.com')
            obj.update(t=_t)
            obj = UserInfo.get_one(email='123@123.com', t=_t)
            self.assertIsNotNone(obj)
            self.assertEqual(obj.email, '123@123.com')
            obj.delete(t=_t)
            obj = UserInfo.get_one(email='123@123.com', t=_t)
            self.assertIsNone(obj)
            u1 = UserInfo.new(email='dennias.chiu@gmail.com1', username='dennias1', height=188,
                              properties={"yooyo": "hahaha"})
            u2 = UserInfo.new(email='dennias.chiu@gmail.com2', username='dennias2', height=188,
                              properties={"yooyo": "hahaha"})
            u3 = UserInfo.new(email='dennias.chiu@gmail.com3', username='dennias3', height=188,
                              properties={"yooyo": "hahaha"})
            UserInfo.insert_many([u1, u2, u3])
            ua1 = UserInfo.get_one(email='dennias.chiu@gmail.com1')
            self.assertIsNotNone(ua1)
            self.assertEqual(ua1['email'], 'dennias.chiu@gmail.com1')
            ua2 = UserInfo.get_one(email='dennias.chiu@gmail.com2')
            self.assertIsNotNone(ua2)
            self.assertEqual(ua2['email'], 'dennias.chiu@gmail.com2')

    def test_02_serielization(self):
        self.user_info = UserInfo.new(email='dennias.chiu@gmail.com', username='dennias', height=180,
                                      properties={"yooyo": "hahaha"})
        with self.user_info.dbi.start_transaction() as _t:
            self.user_info.insert(t=_t)
            obj = UserInfo.get_one(email='dennias.chiu@gmail.com', username='dennias', t=_t)
            json_objstr = json.dumps(obj)
            json_obj = json.loads(json_objstr)
            self.assertIsNotNone(json_obj)
            self.assertEqual(json_obj['email'], 'dennias.chiu@gmail.com')
            obj.delete(t=_t)

    def test_03_copy(self):
        uis = UserInfo.get_many(email='dennias.chiu@gmail.com2')
        a = dict(uis[0])
        self.assertEqual(a['email'], 'dennias.chiu@gmail.com2')
        a['abc'] = 'hhh'
        self.assertEqual(a['abc'], 'hhh')

    def test_04_transaction(self):
        with UserInfo.start_transaction() as _t:
            ui2 = UserInfo.get_one(email='dennias.chiu@gmail.com1', for_update=True, t=_t)
            ui3 = UserInfo.get_one(email='dennias.chiu@gmail.com1', for_update=True, t=_t)
            self.assertEqual(ui2.email, ui3.email)
            from time import sleep
            sleep(2)
            uis = UserInfo.get_many(userid=([1, 2, 3, 4, 5, 6], 'IN'), for_update=True, t=_t)
            sleep(2)

    def test_05_join(self):
        with UserBodyInfo.start_transaction() as _t:
            UserBodyInfo.drop(ifexists=True, t=_t)
            sql = SQL("""
            CREATE TABLE `UserBodyInfo` (
              `id` bigint NOT NULL AUTO_INCREMENT,
              `userid` bigint NOT NULL COMMENT '用户id',
              `weight` DECIMAL(10,4) NOT NULL comment '售卖价格',
              `createtime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
              `updatetime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
              `someone` boolean NOT NULL COMMENT '布尔类型',
              PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;""")
            UserBodyInfo.create(sql=sql, t=_t)
            ui = UserInfo.get_one(email=('dennias.chiu@gmail.com1', 'LIKE'), for_update=True, t=_t)
            ubi = UserBodyInfo.new(userid=ui.userid, weight=ui.height, someone=True)
            ubi.insert(t=_t)
            join_table = UserInfo.join(UserBodyInfo, userid=UserBodyInfo.get_field('userid')).to_json()
            ret = UserInfo.search_and_join(join_table=join_table, email='dennias.chiu@gmail.com1', t=_t)
            self.assertEqual(ret.result[0]['username'], 'dennias1')
            ret = UserInfo.search_and_join(
                return_columns=['username', 'weight', 'height'], join_table=join_table, email='dennias.chiu@gmail.com1',
                t=_t)
            self.assertEqual(ret.result[0]['weight'], 188.0)
            ret = UserInfo.search_and_join(
                return_columns=[field.name for field in UserBodyInfo.get_fields()], join_table=join_table,
                email='dennias.chiu@gmail.com1', t=_t)
            self.assertEqual(ret.result[0]['weight'], 188.0)

    def test_06_transaction_failed(self):
        ui = UserInfo.new(
            email='312dennias.chiu@gmail.com', username='dennias', height=180,
            properties={"yooyo": "hahaha"})
        try:
            with ui.start_transaction() as _t:
                ui.insert(t=_t)
                raise Exception
        except Exception as ex:
            pass
        self.assertIsNone(UserInfo.get_one(email='312dennias.chiu@gmail.com'))

    def test_99_drop_table(self):
        with UserInfo.start_transaction() as _t:
            UserInfo.drop(t=_t)
            UserBodyInfo.drop(t=_t)
