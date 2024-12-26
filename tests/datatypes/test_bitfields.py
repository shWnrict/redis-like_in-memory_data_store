import unittest
from src.datatypes.bitfields import Bitfields

class TestBitfields(unittest.TestCase):

    def setUp(self):
        self.store = {"mybitfield": bytearray(1)}
        self.bitfields = Bitfields(self)

    def test_get(self):
        self.store["mybitfield"][0] = 0b10101010
        result = self.bitfields.get("mybitfield", 0, 4)
        self.assertEqual(result, ":10\r\n")

    def test_set(self):
        result = self.bitfields.set("mybitfield", 0, 4, 10)
        self.assertEqual(result, ":10\r\n")
        self.assertEqual(self.store["mybitfield"][0], 0b10100000)

    def test_incrby(self):
        self.bitfields.set("mybitfield", 0, 4, 10)
        result = self.bitfields.incrby("mybitfield", 0, 4, 5)
        self.assertEqual(result, ":15\r\n")
        self.assertEqual(self.store["mybitfield"][0], 0b11110000)

if __name__ == '__main__':
    unittest.main()
