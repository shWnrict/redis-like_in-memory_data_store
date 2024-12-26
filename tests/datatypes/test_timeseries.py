import unittest
from src.datatypes.timeseries import TimeSeries

class TestTimeSeries(unittest.TestCase):

    def setUp(self):
        self.store = {}
        self.timeseries = TimeSeries()

    def test_create(self):
        result = self.timeseries.create(self.store, "myts")
        self.assertEqual(result, "OK")
        self.assertIn("myts", self.store)

    def test_add(self):
        self.timeseries.create(self.store, "myts")
        result = self.timeseries.add(self.store, "myts", 1627849200, 25.5)
        self.assertEqual(result, "OK")
        self.assertIn((1627849200, 25.5), self.store["myts"])

    def test_get(self):
        self.timeseries.create(self.store, "myts")
        self.timeseries.add(self.store, "myts", 1627849200, 25.5)
        result = self.timeseries.get(self.store, "myts")
        self.assertEqual(result, (1627849200, 25.5))

    def test_range(self):
        self.timeseries.create(self.store, "myts")
        self.timeseries.add(self.store, "myts", 1627849200, 25.5)
        self.timeseries.add(self.store, "myts", 1627849300, 26.0)
        result = self.timeseries.range(self.store, "myts", 1627849200, 1627849300)
        self.assertEqual(result, [(1627849200, 25.5), (1627849300, 26.0)])

    def test_range_with_aggregation(self):
        self.timeseries.create(self.store, "myts")
        self.timeseries.add(self.store, "myts", 1627849200, 25.5)
        self.timeseries.add(self.store, "myts", 1627849300, 26.0)
        result = self.timeseries.range(self.store, "myts", 1627849200, 1627849300, aggregation="SUM")
        self.assertEqual(result, 51.5)

if __name__ == '__main__':
    unittest.main()
