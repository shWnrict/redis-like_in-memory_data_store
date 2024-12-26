import unittest
from src.core.memory_manager import MemoryManager

class TestMemoryManager(unittest.TestCase):

    def setUp(self):
        self.memory_manager = MemoryManager()

    def test_allocate_memory(self):
        result = self.memory_manager.allocate(1024)
        self.assertTrue(result)

    def test_free_memory(self):
        self.memory_manager.allocate(1024)
        result = self.memory_manager.free(512)
        self.assertTrue(result)

    def test_memory_usage(self):
        self.memory_manager.allocate(1024)
        result = self.memory_manager.memory_usage()
        self.assertEqual(result, 1024)

if __name__ == '__main__':
    unittest.main()
