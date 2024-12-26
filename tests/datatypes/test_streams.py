import unittest
from src.datatypes.streams import Streams

class TestStreams(unittest.TestCase):

    def setUp(self):
        self.store = {}
        self.streams = Streams()

    def test_xadd(self):
        result = self.streams.xadd(self.store, "mystream", "*", field1="value1")
        self.assertTrue(result)
        self.assertIn(result, self.store["mystream"]["entries"])

    def test_xread(self):
        entry_id = self.streams.xadd(self.store, "mystream", "*", field1="value1")
        result = self.streams.xread(self.store, "mystream")
        self.assertIn((entry_id, {"field1": "value1"}), result)

    def test_xrange(self):
        entry_id1 = self.streams.xadd(self.store, "mystream", "*", field1="value1")
        entry_id2 = self.streams.xadd(self.store, "mystream", "*", field2="value2")
        result = self.streams.xrange(self.store, "mystream", entry_id1, entry_id2)
        self.assertIn((entry_id1, {"field1": "value1"}), result)
        self.assertIn((entry_id2, {"field2": "value2"}), result)

    def test_xlen(self):
        self.streams.xadd(self.store, "mystream", "*", field1="value1")
        self.streams.xadd(self.store, "mystream", "*", field2="value2")
        result = self.streams.xlen(self.store, "mystream")
        self.assertEqual(result, 2)

    def test_xgroup_create(self):
        self.streams.xadd(self.store, "mystream", "*", field1="value1")
        result = self.streams.xgroup_create(self.store, "mystream", "mygroup")
        self.assertEqual(result, "OK")
        self.assertIn("mygroup", self.store["mystream"]["group_data"])

    def test_xreadgroup(self):
        self.streams.xadd(self.store, "mystream", "*", field1="value1")
        self.streams.xgroup_create(self.store, "mystream", "mygroup")
        result = self.streams.xreadgroup(self.store, "mygroup", "consumer1", "mystream")
        self.assertTrue(result)

    def test_xack(self):
        entry_id = self.streams.xadd(self.store, "mystream", "*", field1="value1")
        self.streams.xgroup_create(self.store, "mystream", "mygroup")
        self.streams.xreadgroup(self.store, "mygroup", "consumer1", "mystream")
        result = self.streams.xack(self.store, "mystream", "mygroup", entry_id)
        self.assertEqual(result, 1)

if __name__ == '__main__':
    unittest.main()
