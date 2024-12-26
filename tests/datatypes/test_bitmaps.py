import unittest
from src.datatypes.bitmaps import Bitmaps

class TestBitmaps(unittest.TestCase):

    def setUp(self):
        self.store = {}
        self.bitmaps = Bitmaps()

    def test_setbit(self):
        result = self.bitmaps.setbit(self.store, "mybitmap", 0, 1)
        self.assertEqual(result, 0)
        self.assertEqual(self.store["mybitmap"], "\x80")

    def test_getbit(self):
        self.bitmaps.setbit(self.store, "mybitmap", 0, 1)
        result = self.bitmaps.getbit(self.store, "mybitmap", 0)
        self.assertEqual(result, 1)

    def test_bitcount(self):
        self.bitmaps.setbit(self.store, "mybitmap", 0, 1)
        self.bitmaps.setbit(self.store, "mybitmap", 1, 1)
        result = self.bitmaps.bitcount(self.store, "mybitmap")
        self.assertEqual(result, 2)

    def test_bitop_and(self):
        self.bitmaps.setbit(self.store, "bitmap1", 0, 1)
        self.bitmaps.setbit(self.store, "bitmap2", 0, 1)
        result = self.bitmaps.bitop(self.store, "AND", "destbitmap", "bitmap1", "bitmap2")
        self.assertEqual(result, 1)
        self.assertEqual(self.store["destbitmap"], "\x80")

    def test_bitop_or(self):
        self.bitmaps.setbit(self.store, "bitmap1", 0, 1)
        self.bitmaps.setbit(self.store, "bitmap2", 1, 1)
        result = self.bitmaps.bitop(self.store, "OR", "destbitmap", "bitmap1", "bitmap2")
        self.assertEqual(result, 1)
        self.assertEqual(self.store["destbitmap"], "\xC0")

if __name__ == '__main__':
    unittest.main()
