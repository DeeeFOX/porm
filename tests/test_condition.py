from porm.orms import Condition
from tests.test_common import DatabaseTestCase


class TestDatabase(DatabaseTestCase):

    def test_condition(self):
        con_obj = Condition.lt('abc', 123)
        self.assertEqual('abc < 123', str(con_obj))
        con_obj = Condition().and_lt('abc', 123)
        self.assertEqual('(1 = 1 AND abc < 123)', str(con_obj))
        con_obj.and_lt('bca', 321)
        self.assertEqual('((1 = 1 AND abc < 123) AND bca < 321)', str(con_obj))