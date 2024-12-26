import unittest
from src.datatypes.json_type import JSONType

class TestJSONType(unittest.TestCase):

    def setUp(self):
        self.store = {}
        self.json_type = JSONType()

    def test_json_set(self):
        result = self.json_type.json_set(self.store, "myjson", "path", '{"name": "John"}')
        self.assertEqual(result, "OK")
        self.assertIn("path", self.store["myjson"])

    def test_json_get(self):
        self.json_type.json_set(self.store, "myjson", "path", '{"name": "John"}')
        result = self.json_type.json_get(self.store, "myjson", "path")
        self.assertEqual(result, '{"name": "John"}')

    def test_json_del(self):
        self.json_type.json_set(self.store, "myjson", "path", '{"name": "John"}')
        result = self.json_type.json_del(self.store, "myjson", "path")
        self.assertEqual(result, 1)
        self.assertNotIn("path", self.store["myjson"])

    def test_json_arrappend(self):
        self.json_type.json_set(self.store, "myjson", "path", '[]')
        result = self.json_type.json_arrappend(self.store, "myjson", "path", '{"name": "John"}')
        self.assertEqual(result, 1)
        self.assertIn({"name": "John"}, self.store["myjson"]["path"])

if __name__ == '__main__':
    unittest.main()
