import unittest
import time
from src.core.expiry_manager import ExpiryManager

class TestExpiryManager(unittest.TestCase):

    def setUp(self):
        self.expiry_manager = ExpiryManager()

    def test_set_expiry(self):
        result = self.expiry_manager.set_expiry("key", 10)
        self.assertTrue(result)

    def test_check_expiry(self):
        self.expiry_manager.set_expiry("key", 1)
        time.sleep(1.1)
        result = self.expiry_manager.check_expiry("key")
        self.assertFalse(result)

    def test_remove_expiry(self):
        self.expiry_manager.set_expiry("key", 10)
        result = self.expiry_manager.remove_expiry("key")
        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()
