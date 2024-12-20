import unittest
import time
from src.datatypes.base import BaseDataType

class TestBaseDataType(unittest.TestCase):
    def setUp(self):
        self.base_data = BaseDataType()

    def test_set_and_get(self):
        self.base_data.set("key1", "value1")
        self.assertEqual(self.base_data.get("key1"), "value1")

    def test_delete(self):
        self.base_data.set("key1", "value1")
        self.assertTrue(self.base_data.delete("key1"))
        self.assertIsNone(self.base_data.get("key1"))

    def test_exists(self):
        self.base_data.set("key1", "value1")
        self.assertTrue(self.base_data.exists("key1"))
        self.base_data.delete("key1")
        self.assertFalse(self.base_data.exists("key1"))

    def test_ttl(self):
        self.base_data.set("key1", "value1")
        self.base_data.set_ttl("key1", 2)  # 2 seconds TTL
        time.sleep(1)
        self.assertGreaterEqual(self.base_data.get_ttl("key1"), 1)
        time.sleep(2)
        self.assertIsNone(self.base_data.get("key1"))  # Expired key

    def test_persist(self):
        self.base_data.set("key1", "value1")
        self.base_data.set_ttl("key1", 2)
        self.base_data.persist("key1")
        self.assertEqual(self.base_data.get_ttl("key1"), -1)  # TTL removed

    def test_get_ttl_no_key(self):
        self.assertEqual(self.base_data.get_ttl("key1"), -1)  # Key doesn't exist
