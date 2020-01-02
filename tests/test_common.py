import logging
import unittest
from contextlib import contextmanager

from tests.context import logger, QueryLogHandler, db_loader

db = db_loader('mysql', name='porm_database_test', user='root', password='root', host='localhost', port=3306)


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self._qh = QueryLogHandler()
        logger.setLevel(logging.DEBUG)
        logger.addHandler(self._qh)

    def tearDown(self):
        logger.removeHandler(self._qh)

    def assertIsNone(self, value):
        self.assertTrue(value is None, '%r is not None' % value)

    def assertIsNotNone(self, value):
        self.assertTrue(value is not None, '%r is None' % value)

    def assertValueEqual(self, val1, val2):
        self.assertTrue(str(val1) == str(val2), u'{} != {}'.format(str(val1), str(val2)))

    @contextmanager
    def assertRaisesCtx(self, exceptions):
        try:
            yield
        except Exception as exc:
            if not isinstance(exc, exceptions):
                raise AssertionError('Got %s, expected %s' % (exc, exceptions))
        else:
            raise AssertionError('No exception was raised.')

    def assertSQL(self, to_assert_sql, sql, params=None, **state):
        database = getattr(self, 'database', None) or db
        state.setdefault('conflict_statement', database.conflict_statement)
        state.setdefault('conflict_update', database.conflict_update)
        self.assertEqual(to_assert_sql, sql % params)

    @property
    def history(self):
        return self._qh.queries

    def reset_sql_history(self):
        self._qh.queries = []

    @contextmanager
    def assertQueryCount(self, num):
        qc = len(self.history)
        yield
        self.assertEqual(len(self.history) - qc, num)


class DatabaseTestCase(BaseTestCase):
    database = db

    def setUp(self):
        if not self.database.is_closed():
            self.database.close()
        self.database.connect()
        super(DatabaseTestCase, self).setUp()

    def tearDown(self):
        super(DatabaseTestCase, self).tearDown()
        self.database.close()

    def execute(self, sql, params=None):
        return self.database.execute_sql(sql, params)
