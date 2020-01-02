from typing import Union

import pymysql

__all__ = (
    'mysql'
)

mysql_passwd = False
mysql: pymysql = None
mysql_constants: pymysql.constants = None

try:
    import pymysql as mysql
    from pymysql import constants as mysql_constants
except ImportError:
    try:
        import MySQLdb as mysql

        mysql_passwd = True
    except ImportError:
        mysql = None
