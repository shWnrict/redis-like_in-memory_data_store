import unittest
from src.datatypes.vectors import Vectors

class TestVectors(unittest.TestCase):

    def setUp(self):
        self.store = {}
        self.vectors = Vectors()

    def test_add_vector(self):
        result = self.vectors.add_vector(self.store, "myvector", [1, 2, 3])
        self.assertEqual(result, "OK")
        self.assertIn("myvector", self.store)

    def test_similarity_search(self):
        self.vectors.add_vector(self.store, "vector1", [1, 2, 3])
        self.vectors.add_vector(self.store, "vector2", [4, 5, 6])
        result = self.vectors.similarity_search(self.store, [1, 2, 3])
        self.assertEqual(result[0][0], "vector1")

    def test_vector_operation_add(self):
        result = self.vectors.vector_operation(self.store, "add", [1, 2, 3], [4, 5, 6])
        self.assertEqual(result, [5, 7, 9])

    def test_vector_operation_sub(self):
        result = self.vectors.vector_operation(self.store, "sub", [4, 5, 6], [1, 2, 3])
        self.assertEqual(result, [3, 3, 3])

    def test_vector_operation_dot(self):
        result = self.vectors.vector_operation(self.store, "dot", [1, 2, 3], [4, 5, 6])
        self.assertEqual(result, 32)

if __name__ == '__main__':
    unittest.main()
