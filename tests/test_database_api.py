from tests.context import db_loader
from tests.test_common import DatabaseTestCase


class TestDatabase(DatabaseTestCase):
    api = db_loader('mysql', name='porm_database_test', user='root', password='root', host='localhost', port=3306)

    def test_execute_sql(self):
        self.api.execute_sql('CREATE TABLE register (val INTEGER);')
        self.api.execute_sql(
            'INSERT INTO register (val) VALUES (%s), (%s)', params=(1337, 31337))
        cursor = self.api.execute_sql('SELECT val FROM register ORDER BY val')
        self.assertValueEqual(cursor.fetchall(), ((1337,), (31337,)))
        self.api.execute_sql('DROP TABLE register;')

    def test_mydbapi_sql(self):
        self.api.execute_sql('CREATE TABLE register (val INTEGER);')
        self.api.insert_one(
            'INSERT INTO register (val) VALUES (%s), (%s)', param=(1337, 31337))
        results = self.api.query_many('SELECT val FROM register ORDER BY val')
        self.assertValueEqual(results, ((1337,), (31337,)))
        self.api.execute_sql('DROP TABLE register;')
