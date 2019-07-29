import contextlib
import logging

import pymysql

from porm.databases.api import DBApi
from porm.databases.api.drivers import mysql as driver
from porm.errors import EmptyError

try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logger = logging.getLogger('porm')
logger.addHandler(NullHandler())

CONN_CONF = {
    'host': 'localhost',
    'user': 'root',
    'db': 'PORM_DATABASE',
    'charset': 'utf8',
    'autocommit': 0,  # default 0
    'cursorclass': pymysql.cursors.DictCursor
}


class MyDBApi(DBApi):

    def __init__(self, database_name=None, thread_safe=True, autorollback=False, autocommit=None, autoconnect=True,
                 t=None, **config):
        config.update(config.pop('config', {}))
        config['database_name'] = database_name
        config['thread_safe'] = thread_safe
        config['autorollback'] = autorollback
        config['autocommit'] = autocommit
        config['autoconnect'] = autoconnect
        config['t'] = t
        super(MyDBApi, self).__init__(**config)

    def _connect(self):
        if driver is None:
            raise EmptyError('MySQL driver not installed!')
        conn = driver.connect(db=self.database_name, **self.connect_params)
        return conn

    def _initialize_connection(self, conn):
        pass

    @classmethod
    def _log(cls, sql, param, level='info'):
        exec_sql = sql % param
        getattr(logger, level, 'info')(exec_sql)

    @classmethod
    @contextlib.contextmanager
    def create_transaction(cls, config=None):
        api_obj = cls(**config)
        with api_obj.start_transaction() as _t:
            yield _t

    @staticmethod
    def _get_autocommit(conn):
        """
        Get if connection is auto commit
        :param conn:
        :return:
        """
        try:
            _auto = conn.get_autocommit()
        except AttributeError:
            try:
                conn.ping(reconnect=True)
                with conn.cursor() as cursor:
                    cursor.execute("SHOW VARIABLES WHERE Variable_name='autocommit';")
                    results = cursor.fetchall()
                    _auto = True if results[0]['Value'] == 'ON' else False
            except Exception as ex:
                raise ex
        return _auto

    @staticmethod
    def _set_autocommit(conn, auto):
        """
        Get connection auto commit attribute
        :param conn:
        :param auto:
        :return:
        """
        try:
            conn.autocommit(auto)
        except AttributeError:
            with conn.cursor() as cursor:
                cursor.execute("SET AUTOCOMMIT=%s;" % auto)

    @contextlib.contextmanager
    def start_transaction(self):
        """
        Start a new transaction of this connection
        :return:
        """
        _auto = self._get_autocommit(self.conn)
        try:
            self._set_autocommit(self.conn, False)
            self.session_start()
            yield self.conn
        except Exception as ex:
            self._log(str(ex), {}, level='error')
            self.session_rollback()
            raise ex
        else:
            self.session_commit()
        finally:
            self._set_autocommit(self.conn, _auto)

    def query_many(self, sql, param=None):
        try:
            cursor = self.execute_sql(sql, params=param)
            results = cursor.fetchall()
        except Exception as ex:
            self._log(sql, param, level='error')
            raise ex
        return results

    def query_one(self, sql, param=None):
        try:
            cursor = self.execute_sql(sql, params=param)
            result = cursor.fetchone()
        except Exception as ex:
            self._log(sql, param, level='error')
            raise ex
        return result

    def query(self, sql, param=None):
        return self.query_many(sql, param)

    def insert_one(self, sql, param=None):
        try:
            self.execute_sql(sql, params=param)
        except Exception as ex:
            self._log(sql, param, level='error')
            raise ex

    def insert_many(self, sql, param=None):
        try:
            self.execute_sql(sql, params=param)
        except Exception as ex:
            self._log(sql, param, level='error')
            raise ex

    def delete(self, sql, param=None):
        try:
            self.execute_sql(sql, params=param)
        except Exception as ex:
            self._log(sql, param, level='error')
            raise ex
