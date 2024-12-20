import unittest
import time
from src.core.data_store import KeyValueStore

class TestExpiryManager(unittest.TestCase):
    def setUp(self):
        self.kv_store = KeyValueStore()

    def test_expire(self):
        self.kv_store.set("key1", "value1")
        self.kv_store.expiry_manager.set_expiry("key1", 2)  # 2 seconds TTL
        time.sleep(3)
        self.assertFalse(self.kv_store.exists("key1"))

    def test_ttl(self):
        self.kv_store.set("key1", "value1")
        self.kv_store.expiry_manager.set_expiry("key1", 2)  # 2 seconds TTL
        ttl = self.kv_store.expiry_manager.ttl("key1")
        self.assertGreaterEqual(ttl, 1)

    def test_persist(self):
        self.kv_store.set("key1", "value1")
        self.kv_store.expiry_manager.set_expiry("key1", 2)
        self.kv_store.expiry_manager.persist("key1")
        self.assertEqual(self.kv_store.expiry_manager.ttl("key1"), -1)
