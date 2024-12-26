import unittest
from src.datatypes.documents import Documents

class TestDocuments(unittest.TestCase):

    def setUp(self):
        self.store = {}
        self.documents = Documents()

    def test_insert(self):
        result = self.documents.insert(self.store, "mycollection", '{"name": "John"}')
        self.assertEqual(result, "OK")
        self.assertIn({"name": "John"}, self.store["mycollection"])

    def test_find(self):
        self.documents.insert(self.store, "mycollection", '{"name": "John"}')
        result = self.documents.find(self.store, "mycollection", '{"name": "John"}')
        self.assertEqual(result, [{"name": "John"}])

    def test_update(self):
        self.documents.insert(self.store, "mycollection", '{"name": "John"}')
        result = self.documents.update(self.store, "mycollection", '{"name": "John"}', '{"age": 30}')
        self.assertEqual(result, 1)
        self.assertIn({"name": "John", "age": 30}, self.store["mycollection"])

    def test_delete(self):
        self.documents.insert(self.store, "mycollection", '{"name": "John"}')
        result = self.documents.delete(self.store, "mycollection", '{"name": "John"}')
        self.assertEqual(result, 1)
        self.assertNotIn({"name": "John"}, self.store["mycollection"])

    def test_aggregate_count(self):
        self.documents.insert(self.store, "mycollection", '{"name": "John"}')
        self.documents.insert(self.store, "mycollection", '{"name": "Jane"}')
        result = self.documents.aggregate(self.store, "mycollection", "count", "name")
        self.assertEqual(result, 2)

    def test_aggregate_sum(self):
        self.documents.insert(self.store, "mycollection", '{"age": 30}')
        self.documents.insert(self.store, "mycollection", '{"age": 40}')
        result = self.documents.aggregate(self.store, "mycollection", "sum", "age")
        self.assertEqual(result, 70)

    def test_aggregate_avg(self):
        self.documents.insert(self.store, "mycollection", '{"age": 30}')
        self.documents.insert(self.store, "mycollection", '{"age": 40}')
        result = self.documents.aggregate(self.store, "mycollection", "avg", "age")
        self.assertEqual(result, 35)

if __name__ == '__main__':
    unittest.main()
