__all__ = (
    'mysql'
)

mysql_passwd = False
mysql = None

try:
    import pymysql as mysql
except ImportError:
    try:
        import MySQLdb as mysql

        mysql_passwd = True
    except ImportError:
        mysql = None
