import unittest
from src.datatypes.geospatial import Geospatial

class TestGeospatial(unittest.TestCase):

    def setUp(self):
        self.store = {}
        self.geospatial = Geospatial()

    def test_geoadd(self):
        result = self.geospatial.geoadd(self.store, "mygeo", 13.361389, 38.115556, "Palermo")
        self.assertEqual(result, 1)
        self.assertIn("Palermo", self.store["mygeo"])

    def test_geodist(self):
        self.geospatial.geoadd(self.store, "mygeo", 13.361389, 38.115556, "Palermo")
        self.geospatial.geoadd(self.store, "mygeo", 15.087269, 37.502669, "Catania")
        result = self.geospatial.geodist(self.store, "mygeo", "Palermo", "Catania")
        self.assertTrue(result > 0)

    def test_geosearch(self):
        self.geospatial.geoadd(self.store, "mygeo", 13.361389, 38.115556, "Palermo")
        self.geospatial.geoadd(self.store, "mygeo", 15.087269, 37.502669, "Catania")
        result = self.geospatial.geosearch(self.store, "mygeo", 13.361389, 38.115556, 200)
        self.assertIn(("Palermo", 0), result)

if __name__ == '__main__':
    unittest.main()
