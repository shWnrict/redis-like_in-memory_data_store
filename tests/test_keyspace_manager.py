import unittest
from src.core.keyspace_manager import KeyspaceManager

class TestKeyspaceManager(unittest.TestCase):
    def setUp(self):
        self.keyspace_manager = KeyspaceManager(num_databases=4)

    def test_select_valid_db(self):
        response = self.keyspace_manager.select(2)
        self.assertEqual(response, "OK - Switched to database 2")
        self.assertEqual(self.keyspace_manager.current_db, 2)

    def test_select_invalid_db(self):
        with self.assertRaises(ValueError):
            self.keyspace_manager.select(10)

    def test_flushdb(self):
        current_store = self.keyspace_manager.get_current_store()
        current_store.set("key1", "value1")
        self.assertEqual(current_store.get("key1"), "value1")

        response = self.keyspace_manager.flushdb()
        self.assertEqual(response, "OK")
        self.assertIsNone(current_store.get("key1"))

    def test_randomkey(self):
        current_store = self.keyspace_manager.get_current_store()
        current_store.set("key1", "value1")
        current_store.set("key2", "value2")

        random_key = self.keyspace_manager.randomkey()
        self.assertIn(random_key, ["key1", "key2"])

        # Test for empty database
        self.keyspace_manager.flushdb()
        self.assertIsNone(self.keyspace_manager.randomkey())
