import json

import pymysql

from porm import IntegerType, VarcharType, TextType, DatetimeType, FloatType
from porm.databases.api.mysql import CONN_CONF
from porm.model import DBModel
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

    class UserInfo(TestModel):
        userid = IntegerType(pk=True, required=True)
        username = VarcharType(required=True)
        email = VarcharType(required=True)
        descr = TextType(required=False, default=None)
        createtime = DatetimeType(required=False, default=None)
        updatetime = DatetimeType(required=False, default=None)
        is_active = IntegerType(required=False, default=1)
        height = FloatType(required=True, default=180)

    user_info = None

    def test_01_ormobj_crud(self):
        self.user_info = self.UserInfo.new(email='dennias.chiu@gmail.com', username='dennias', height=188)
        with self.user_info.dbi.start_transaction() as _t:
            self.user_info.dbi.execute_sql("""DROP TABLE IF EXISTS UserInfo;""")
            self.user_info.dbi.execute_sql("""
            CREATE TABLE `UserInfo` (
              `userid` bigint NOT NULL AUTO_INCREMENT,
              `email` varchar(255) NOT NULL COMMENT '电子邮箱',
              `username` varchar(255) NOT NULL COMMENT '用户名',
              `descr` text DEFAULT NULL COMMENT '用户说明',
              `createtime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
              `updatetime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
              `is_active` tinyint(1) DEFAULT '1' COMMENT '用户有效标志',
              `height` DECIMAL(10,4) NOT NULL comment '售卖价格',
              PRIMARY KEY (`userid`),
              UNIQUE KEY `email` (`email`),
              UNIQUE KEY `username` (`username`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;""")
            self.user_info.insert(t=_t)
            obj = self.UserInfo.get_one(email='dennias.chiu@gmail.com', username='dennias', t=_t)
            self.assertIsNotNone(obj)
            obj.reset(email='123@123.com')
            obj.update(t=_t)
            obj = self.UserInfo.get_one(email='123@123.com')
            self.assertIsNotNone(obj)
            self.assertEqual(obj.email, '123@123.com')
            obj.delete(t=_t)
            obj = self.UserInfo.get_one(email='123@123.com')
            self.assertIsNone(obj)

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

    def test_03_drop_table(self):
        self.user_info = self.UserInfo.new(email='dennias.chiu@gmail.com', username='dennias')
        self.user_info.dbi.execute_sql('DROP TABLE UserInfo;')
