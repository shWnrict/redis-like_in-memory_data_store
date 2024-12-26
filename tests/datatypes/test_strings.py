import unittest
from src.datatypes.strings import Strings

class TestStrings(unittest.TestCase):

    def setUp(self):
        self.store = {}
        self.strings = Strings()

    def test_set(self):
        result = self.strings.set(self.store, "mykey", "myvalue")
        self.assertEqual(result, "OK")
        self.assertIn("mykey", self.store)

    def test_get(self):
        self.strings.set(self.store, "mykey", "myvalue")
        result = self.strings.get(self.store, "mykey")
        self.assertEqual(result, "myvalue")

    def test_delete(self):
        self.strings.set(self.store, "mykey", "myvalue")
        result = self.strings.delete(self.store, "mykey")
        self.assertEqual(result, 1)
        self.assertNotIn("mykey", self.store)

    def test_exists(self):
        self.strings.set(self.store, "mykey", "myvalue")
        result = self.strings.exists(self.store, "mykey")
        self.assertEqual(result, 1)
        self.strings.delete(self.store, "mykey")
        result = self.strings.exists(self.store, "mykey")
        self.assertEqual(result, 0)

if __name__ == '__main__':
    unittest.main()
