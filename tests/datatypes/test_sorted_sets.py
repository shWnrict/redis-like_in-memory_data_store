import unittest
from src.datatypes.sorted_sets import SortedSets

class TestSortedSets(unittest.TestCase):

    def setUp(self):
        self.store = {}
        self.sorted_sets = SortedSets()

    def test_zadd(self):
        result = self.sorted_sets.zadd(self.store, "myzset", 1, "one", 2, "two")
        self.assertEqual(result, 2)
        self.assertIn("one", self.store["myzset"])
        self.assertIn("two", self.store["myzset"])

    def test_zrange(self):
        self.sorted_sets.zadd(self.store, "myzset", 1, "one", 2, "two")
        result = self.sorted_sets.zrange(self.store, "myzset", 0, -1)
        self.assertEqual(result, ["one", "two"])

    def test_zrank(self):
        self.sorted_sets.zadd(self.store, "myzset", 1, "one", 2, "two")
        result = self.sorted_sets.zrank(self.store, "myzset", "one")
        self.assertEqual(result, 0)

    def test_zrem(self):
        self.sorted_sets.zadd(self.store, "myzset", 1, "one", 2, "two")
        result = self.sorted_sets.zrem(self.store, "myzset", "one")
        self.assertEqual(result, 1)
        self.assertNotIn("one", self.store["myzset"])

    def test_zrangebyscore(self):
        self.sorted_sets.zadd(self.store, "myzset", 1, "one", 2, "two", 3, "three")
        result = self.sorted_sets.zrangebyscore(self.store, "myzset", 1, 2)
        self.assertEqual(result, ["one", "two"])

if __name__ == '__main__':
    unittest.main()
