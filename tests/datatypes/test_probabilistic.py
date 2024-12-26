import unittest
from src.datatypes.probabilistic import HyperLogLog

class TestHyperLogLog(unittest.TestCase):

    def setUp(self):
        self.hll = HyperLogLog()

    def test_add(self):
        result = self.hll.add("value1")
        self.assertEqual(result, 1)

    def test_count(self):
        self.hll.add("value1")
        self.hll.add("value2")
        result = self.hll.count()
        self.assertTrue(result >= 2)

    def test_merge(self):
        hll2 = HyperLogLog()
        hll2.add("value3")
        result = self.hll.merge(hll2)
        self.assertEqual(result, "OK")

if __name__ == '__main__':
    unittest.main()
