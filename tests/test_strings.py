import unittest
from src.datatypes.strings import Strings

class TestStrings(unittest.TestCase):
    def setUp(self):
        self.strings = Strings()

    def test_set_and_get(self):
        self.strings.set("key1", "value1")
        self.assertEqual(self.strings.get("key1"), "value1")

    def test_append(self):
        self.strings.set("key1", "Hello")
        length = self.strings.append("key1", " World")
        self.assertEqual(length, 11)
        self.assertEqual(self.strings.get("key1"), "Hello World")

    def test_strlen(self):
        self.strings.set("key1", "Hello")
        self.assertEqual(self.strings.strlen("key1"), 5)
        self.strings.set("key2", "")
        self.assertEqual(self.strings.strlen("key2"), 0)

    def test_incr_and_decr(self):
        self.strings.set("counter", "10")
        self.assertEqual(self.strings.incr("counter"), 11)
        self.assertEqual(self.strings.decr("counter"), 10)

    def test_incrby_and_decrby(self):
        self.strings.set("counter", "5")
        self.assertEqual(self.strings.incrby("counter", 10), 15)
        self.assertEqual(self.strings.decrby("counter", 5), 10)

    def test_getrange(self):
        self.strings.set("key1", "Hello World")
        self.assertEqual(self.strings.getrange("key1", 0, 4), "Hello")
        self.assertEqual(self.strings.getrange("key1", 6, 10), "World")

    def test_setrange(self):
        self.strings.set("key1", "Hello World")
        length = self.strings.setrange("key1", 6, "Redis")
        self.assertEqual(length, 11)
        self.assertEqual(self.strings.get("key1"), "Hello Redis")

    def test_invalid_operations(self):
        self.strings.set("key1", "Hello")
        with self.assertRaises(ValueError):
            self.strings.incr("key1")
        with self.assertRaises(ValueError):
            self.strings.append("key1", 123)  # Non-string value
