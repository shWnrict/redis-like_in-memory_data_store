import unittest
from src.datatypes.lists import Lists

class TestLists(unittest.TestCase):

    def setUp(self):
        self.store = {}
        self.lists = Lists()

    def test_lpush(self):
        result = self.lists.lpush(self.store, "mylist", "value1", "value2")
        self.assertEqual(result, 2)
        self.assertEqual(self.store["mylist"], ["value2", "value1"])

    def test_rpush(self):
        result = self.lists.rpush(self.store, "mylist", "value1", "value2")
        self.assertEqual(result, 2)
        self.assertEqual(self.store["mylist"], ["value1", "value2"])

    def test_lpop(self):
        self.lists.lpush(self.store, "mylist", "value1", "value2")
        result = self.lists.lpop(self.store, "mylist")
        self.assertEqual(result, "value2")
        self.assertEqual(self.store["mylist"], ["value1"])

    def test_rpop(self):
        self.lists.rpush(self.store, "mylist", "value1", "value2")
        result = self.lists.rpop(self.store, "mylist")
        self.assertEqual(result, "value2")
        self.assertEqual(self.store["mylist"], ["value1"])

    def test_lrange(self):
        self.lists.rpush(self.store, "mylist", "value1", "value2", "value3")
        result = self.lists.lrange(self.store, "mylist", 0, 1)
        self.assertEqual(result, ["value1", "value2"])

if __name__ == '__main__':
    unittest.main()
