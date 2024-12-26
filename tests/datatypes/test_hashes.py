import unittest
from src.datatypes.hashes import Hashes

class TestHashes(unittest.TestCase):

    def setUp(self):
        self.store = {}
        self.hashes = Hashes()

    def test_hset(self):
        result = self.hashes.hset(self.store, "myhash", "field1", "value1")
        self.assertEqual(result, 1)
        self.assertIn("field1", self.store["myhash"])

    def test_hget(self):
        self.hashes.hset(self.store, "myhash", "field1", "value1")
        result = self.hashes.hget(self.store, "myhash", "field1")
        self.assertEqual(result, "value1")

    def test_hdel(self):
        self.hashes.hset(self.store, "myhash", "field1", "value1")
        result = self.hashes.hdel(self.store, "myhash", "field1")
        self.assertEqual(result, 1)
        self.assertNotIn("field1", self.store["myhash"])

    def test_hexists(self):
        self.hashes.hset(self.store, "myhash", "field1", "value1")
        result = self.hashes.hexists(self.store, "myhash", "field1")
        self.assertEqual(result, 1)
        result = self.hashes.hexists(self.store, "myhash", "field2")
        self.assertEqual(result, 0)

    def test_hgetall(self):
        self.hashes.hset(self.store, "myhash", "field1", "value1")
        self.hashes.hset(self.store, "myhash", "field2", "value2")
        result = self.hashes.hgetall(self.store, "myhash")
        self.assertEqual(result, {"field1": "value1", "field2": "value2"})

if __name__ == '__main__':
    unittest.main()
