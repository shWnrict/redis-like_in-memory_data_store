# src/datatypes/geospatial.py
from src.logger import setup_logger
import threading
import math

logger = setup_logger("geospatial")

class Geospatial:
    EARTH_RADIUS_KM = 6371  # Radius of the Earth in kilometers

    def __init__(self):
        self.lock = threading.Lock()

    def __calculate_distance(self, lat1, lon1, lat2, lon2):
        """
        Calculate the Haversine distance between two points.
        """
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return self.EARTH_RADIUS_KM * c

    def geoadd(self, store, key, *args):
        """
        Add geospatial data to the store.
        GEOADD key lon lat member
        """
        if len(args) % 3 != 0:
            return "ERR Invalid number of arguments"
        with self.lock:
            if key not in store:
                store[key] = {}
            if not isinstance(store[key], dict):
                return "ERR Key is not a geospatial index"

            count = 0
            for i in range(0, len(args), 3):
                lon, lat, member = float(args[i]), float(args[i + 1]), args[i + 2]
                store[key][member] = (lat, lon)
                count += 1
            logger.info(f"GEOADD {key} -> {count} locations added")
            return count

    def geodist(self, store, key, member1, member2, unit="m"):
        """
        Calculate the distance between two members.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], dict):
                return "ERR Key is not a geospatial index"

            loc1 = store[key].get(member1)
            loc2 = store[key].get(member2)
            if not loc1 or not loc2:
                return "(nil)"

            distance_km = self.__calculate_distance(loc1[0], loc1[1], loc2[0], loc2[1])

            if unit == "m":
                return distance_km * 1000
            elif unit == "km":
                return distance_km
            elif unit == "mi":
                return distance_km * 0.621371
            else:
                return "ERR Unknown unit"

    def geosearch(self, store, key, lat, lon, radius, unit="km"):
        """
        Search for locations within a radius of the given coordinates.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], dict):
                return []

            radius_km = radius if unit == "km" else radius / 1000 if unit == "m" else radius * 1.60934 if unit == "mi" else 0
            if radius_km == 0:
                return "ERR Unknown unit"

            result = []
            for member, (lat2, lon2) in store[key].items():
                distance_km = self.__calculate_distance(lat, lon, lat2, lon2)
                if distance_km <= radius_km:
                    result.append((member, distance_km))
            logger.info(f"GEOSEARCH {key} -> Found {len(result)} locations")
            return result
