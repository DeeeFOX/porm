import datetime
import json

import pymysql

from porm import IntegerType, VarcharType, TextType, DatetimeType, FloatType
from porm.model import DBModel
from porm.orms import SQL
from porm.types.core import TimeType
from tests.test_common import DatabaseTestCase


class TestDatabase(DatabaseTestCase):
    class TestModel(DBModel):
        __DATABASE__ = 'PORM_DATABASE_TEST'
        __CONFIG__ = {
            'host': 'localhost',
            'user': 'root',
            'db': 'PORM_DATABASE_TEST',
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

    class AnotherInfoBase(TestModel):
        weight = FloatType(required=True, default=180)

    class UserInfo(UserInfoBase):
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

    user_info = None

    def test_00_ormobj_createtable(self):
        with self.UserInfo.start_transaction() as _t:
            self.UserInfo.drop(ifexists=True, t=_t)
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
              PRIMARY KEY (`userid`),
              UNIQUE KEY `email` (`email`),
              UNIQUE KEY `username` (`username`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;""")
            self.UserInfo.create(sql=sql, t=_t)

    def test_01_ormobj_crud(self):
        with self.UserInfo.start_transaction() as _t:
            self.user_info = self.UserInfo.new(
                email='dennias.chiu@gmail.com', username='dennias', height=188,
                start_time=datetime.time.fromisoformat('08:00:00'))
            self.user_info.insert(t=_t)
            obj = self.UserInfo.get_one(email='dennias.chiu@gmail.com', username='dennias', t=_t)
            self.assertEqual(obj['start_time'], datetime.time(8, 0))
            self.assertIsNotNone(obj)
            obj.reset(email='123@123.com')
            obj.update(t=_t)
            obj = self.UserInfo.get_one(email='123@123.com')
            self.assertIsNotNone(obj)
            self.assertEqual(obj.email, '123@123.com')
            obj.delete(t=_t)
            obj = self.UserInfo.get_one(email='123@123.com')
            self.assertIsNone(obj)
            u1 = self.UserInfo.new(email='dennias.chiu@gmail.com1', username='dennias1', height=188)
            u2 = self.UserInfo.new(email='dennias.chiu@gmail.com2', username='dennias2', height=188)
            u3 = self.UserInfo.new(email='dennias.chiu@gmail.com3', username='dennias3', height=188)
            self.UserInfo.insert_many([u1, u2, u3])
            ua1 = self.UserInfo.get_one(email='dennias.chiu@gmail.com1')
            self.assertIsNotNone(ua1)
            self.assertEqual(ua1['email'], 'dennias.chiu@gmail.com1')
            ua2 = self.UserInfo.get_one(email='dennias.chiu@gmail.com2')
            self.assertIsNotNone(ua2)
            self.assertEqual(ua2['email'], 'dennias.chiu@gmail.com2')

    def test_02_serielization(self):
        self.user_info = self.UserInfo.new(email='dennias.chiu@gmail.com', username='dennias', height=180)
        with self.user_info.dbi.start_transaction() as _t:
            self.user_info.insert(t=_t)
            obj = self.UserInfo.get_one(email='dennias.chiu@gmail.com', username='dennias')
            json_objstr = json.dumps(obj)
            json_obj = json.loads(json_objstr)
            self.assertIsNotNone(json_obj)
            self.assertEqual(json_obj['email'], 'dennias.chiu@gmail.com')
            obj.delete(t=_t)

    def test_03_copy(self):
        uis = self.UserInfo.get_many(email='dennias.chiu@gmail.com2')
        a = dict(uis[0])
        self.assertEqual(a['email'], 'dennias.chiu@gmail.com2')
        a['abc'] = 'hhh'
        self.assertEqual(a['abc'], 'hhh')

    def test_04_transaction(self):
        with self.UserInfo.start_transaction() as _t:
            ui2 = self.UserInfo.get_one(email='dennias.chiu@gmail.com1', for_update=True, t=_t)
            ui3 = self.UserInfo.get_one(email='dennias.chiu@gmail.com1', for_update=True, t=_t)
            self.assertEqual(ui2.email, ui3.email)
            from time import sleep
            sleep(2)
            uis = self.UserInfo.get_many(userid=([1, 2, 3, 4, 5, 6], 'IN'), for_update=True, t=_t)
            sleep(2)

    def test_05_join(self):
        with self.UserBodyInfo.start_transaction() as _t:
            self.UserBodyInfo.drop(ifexists=True, t=_t)
            sql = SQL("""
            CREATE TABLE `UserBodyInfo` (
              `id` bigint NOT NULL AUTO_INCREMENT,
              `userid` bigint NOT NULL COMMENT '用户id',
              `weight` DECIMAL(10,4) NOT NULL comment '售卖价格',
              `createtime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
              `updatetime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
              PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;""")
            self.UserBodyInfo.create(sql=sql, t=_t)
            ui = self.UserInfo.get_one(email=('dennias.chiu@gmail.com1', 'LIKE'), for_update=True, t=_t)
            ubi = self.UserBodyInfo.new(userid=ui.userid, weight=ui.height)
            ubi.insert(t=_t)
            join_table = self.UserInfo.join(self.UserBodyInfo, userid=self.UserBodyInfo.get_field('userid')).to_json()
            ret = self.UserInfo.search_and_join(join_table=join_table, email='dennias.chiu@gmail.com1', t=_t)
            self.assertEqual(ret.result[0]['username'], 'dennias1')
            ret = self.UserInfo.search_and_join(
                return_columns=['username', 'weight', 'height'], join_table=join_table, email='dennias.chiu@gmail.com1',
                t=_t)
            self.assertEqual(ret.result[0]['weight'], 188.0)

    def test_06_drop_table(self):
        with self.UserInfo.start_transaction() as _t:
            self.UserInfo.drop(t=_t)
            self.UserBodyInfo.drop(t=_t)
