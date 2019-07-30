from porm import IntegerType, VarcharType, TextType, DatetimeType
from porm.databases.api.mysql import CONN_CONF
from porm.model import DBModel
from tests.test_common import DatabaseTestCase


class TestDatabase(DatabaseTestCase):
    class UserInfo(DBModel):
        __CONFIG__ = CONN_CONF

        userid = IntegerType(pk=True, required=True)
        username = VarcharType(required=True)
        email = VarcharType(required=True)
        descr = TextType(required=False, default=None)
        createtime = DatetimeType(required=False, default=None)
        updatetime = DatetimeType(required=False, default=None)
        is_active = IntegerType(required=False, default=1)

    user_info = None

    def test_ormobj_crud(self):
        self.user_info = self.UserInfo.new(email='dennias.chiu@gmail.com', username='dennias')
        with self.user_info.dbi.start_transaction() as _t:
            self.user_info.dbi.execute_sql("""
            CREATE TABLE `UserInfo` (
              `userid` bigint NOT NULL AUTO_INCREMENT,
              `email` varchar(255) NOT NULL COMMENT '电子邮箱',
              `username` varchar(255) NOT NULL COMMENT '用户名',
              `descr` text DEFAULT NULL COMMENT '用户说明',
              `createtime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
              `updatetime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
              `is_active` tinyint(1) DEFAULT '1' COMMENT '用户有效标志',
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

    def test_drop_table(self):
        self.user_info = self.UserInfo.new(email='dennias.chiu@gmail.com', username='dennias')
        self.user_info.dbi.delete('DROP TABLE UserInfo;')
