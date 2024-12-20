import unittest
from src.core.data_store import KeyValueStore

class TestKeyValueStore(unittest.TestCase):
    def setUp(self):
        self.kv_store = KeyValueStore()

    def test_set_and_get(self):
        self.kv_store.set("key1", "value1")
        self.assertEqual(self.kv_store.get("key1"), "value1")

    def test_delete(self):
        self.kv_store.set("key1", "value1")
        self.assertEqual(self.kv_store.delete("key1"), 1)
        self.assertIsNone(self.kv_store.get("key1"))

    def test_exists(self):
        self.kv_store.set("key1", "value1")
        self.assertTrue(self.kv_store.exists("key1"))
        self.kv_store.delete("key1")
        self.assertFalse(self.kv_store.exists("key1"))
