import unittest
from src.core.keyspace_manager import KeyspaceManager

class TestKeyspaceManager(unittest.TestCase):

    def setUp(self):
        self.keyspace_manager = KeyspaceManager()

    def test_add_key(self):
        result = self.keyspace_manager.add_key("key", "value")
        self.assertEqual(result, "OK")

    def test_get_key(self):
        self.keyspace_manager.add_key("key", "value")
        result = self.keyspace_manager.get_key("key")
        self.assertEqual(result, "value")

    def test_delete_key(self):
        self.keyspace_manager.add_key("key", "value")
        result = self.keyspace_manager.delete_key("key")
        self.assertEqual(result, 1)
        result = self.keyspace_manager.get_key("key")
        self.assertEqual(result, "(nil)")

    def test_key_exists(self):
        self.keyspace_manager.add_key("key", "value")
        result = self.keyspace_manager.key_exists("key")
        self.assertEqual(result, 1)
        self.keyspace_manager.delete_key("key")
        result = self.keyspace_manager.key_exists("key")
        self.assertEqual(result, 0)

if __name__ == '__main__':
    unittest.main()
