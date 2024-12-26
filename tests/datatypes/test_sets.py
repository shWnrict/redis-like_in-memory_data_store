import unittest
from src.datatypes.sets import Sets

class TestSets(unittest.TestCase):

    def setUp(self):
        self.store = {}
        self.sets = Sets()

    def test_sadd(self):
        result = self.sets.sadd(self.store, "myset", "member1", "member2")
        self.assertEqual(result, 2)
        self.assertIn("member1", self.store["myset"])
        self.assertIn("member2", self.store["myset"])

    def test_srem(self):
        self.sets.sadd(self.store, "myset", "member1", "member2")
        result = self.sets.srem(self.store, "myset", "member1")
        self.assertEqual(result, 1)
        self.assertNotIn("member1", self.store["myset"])

    def test_sismember(self):
        self.sets.sadd(self.store, "myset", "member1")
        result = self.sets.sismember(self.store, "myset", "member1")
        self.assertEqual(result, 1)
        result = self.sets.sismember(self.store, "myset", "member2")
        self.assertEqual(result, 0)

    def test_smembers(self):
        self.sets.sadd(self.store, "myset", "member1", "member2")
        result = self.sets.smembers(self.store, "myset")
        self.assertEqual(set(result), {"member1", "member2"})

if __name__ == '__main__':
    unittest.main()
