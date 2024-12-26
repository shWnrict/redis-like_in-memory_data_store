import unittest
from src.persistence.snapshot import Snapshot

class TestSnapshot(unittest.TestCase):

    def setUp(self):
        self.snapshot = Snapshot()

    def test_save_snapshot(self):
        result = self.snapshot.save({"key": "value"})
        self.assertTrue(result)

    def test_load_snapshot(self):
        self.snapshot.save({"key": "value"})
        result = self.snapshot.load()
        self.assertEqual(result, {"key": "value"})

if __name__ == '__main__':
    unittest.main()
