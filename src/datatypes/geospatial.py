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

    def _validate_unit(self, unit):
        """
        Validate the distance unit.
        """
        if unit not in {"m", "km", "mi", "ft"}:
            return "ERR Unknown unit"
        return None

    def handle_command(self, cmd, store, *args):
        if cmd == "GEOADD":
            return self.geoadd(store, *args)
        elif cmd == "GEODIST":
            return self.geodist(store, *args)
        elif cmd == "GEOSEARCH":
            return self.geosearch(store, *args)
        return "ERR Unknown geospatial command"

    def geoadd(self, store, key, *args):
        """
        Add geospatial data to the store.
        GEOADD key [NX | XX] [CH] lon lat member [lon lat member ...]
        """
        if len(args) < 3 or len(args) % 3 != 0:
            return "ERR Invalid number of arguments"
        
        options = {"NX": False, "XX": False, "CH": False}
        while args and args[0] in options:
            options[args[0]] = True
            args = args[1:]

        with self.lock:
            if key not in store:
                store[key] = {}
            if not isinstance(store[key], dict):
                return "ERR Key is not a geospatial index"

            count = 0
            changed = 0
            for i in range(0, len(args), 3):
                try:
                    lon, lat = float(args[i]), float(args[i + 1])
                    member = args[i + 2]
                    if lon < -180 or lon > 180 or lat < -85.05112878 or lat > 85.05112878:
                        return "ERR Invalid longitude or latitude"

                    if options["NX"] and member in store[key]:
                        continue
                    if options["XX"] and member not in store[key]:
                        continue

                    if member in store[key]:
                        changed += 1
                    else:
                        count += 1

                    store[key][member] = (lat, lon)
                except ValueError:
                    return "ERR Invalid longitude or latitude"

            logger.info(f"GEOADD {key} -> {count} locations added, {changed} locations changed")
            return changed if options["CH"] else count

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

            unit_error = self._validate_unit(unit)
            if unit_error:
                return unit_error

            if unit == "m":
                return distance_km * 1000
            elif unit == "km":
                return distance_km
            elif unit == "mi":
                return distance_km * 0.621371
            elif unit == "ft":
                return distance_km * 3280.84

    def geosearch(self, store, key, *args):
        """
        Search for locations within a specified area.
        """
        if len(args) < 5:
            return "ERR Invalid number of arguments"

        with self.lock:
            if key not in store or not isinstance(store[key], dict):
                return []

            from_member = None
            from_lonlat = None
            by_radius = None
            by_box = None
            unit = "km"
            sort_order = None
            count = None
            with_coord = False
            with_dist = False
            with_hash = False

            i = 0
            while i < len(args):
                arg = args[i].upper()
                if arg == "FROMMEMBER":
                    from_member = args[i + 1]
                    i += 2
                elif arg == "FROMLONLAT":
                    from_lonlat = (float(args[i + 1]), float(args[i + 2]))
                    i += 3
                elif arg == "BYRADIUS":
                    by_radius = float(args[i + 1])
                    unit = args[i + 2].lower()
                    i += 3
                elif arg == "BYBOX":
                    by_box = (float(args[i + 1]), float(args[i + 2]))
                    unit = args[i + 3].lower()
                    i += 4
                elif arg == "ASC":
                    sort_order = "ASC"
                    i += 1
                elif arg == "DESC":
                    sort_order = "DESC"
                    i += 1
                elif arg == "COUNT":
                    count = int(args[i + 1])
                    i += 2
                    if i < len(args) and args[i].upper() == "ANY":
                        i += 1
                elif arg == "WITHCOORD":
                    with_coord = True
                    i += 1
                elif arg == "WITHDIST":
                    with_dist = True
                    i += 1
                elif arg == "WITHHASH":
                    with_hash = True
                    i += 1
                else:
                    return "ERR Invalid argument"

            if from_member:
                if from_member not in store[key]:
                    return []
                from_lonlat = store[key][from_member]

            if not from_lonlat:
                return "ERR Missing FROMMEMBER or FROMLONLAT"

            result = []
            if by_radius:
                radius_km = by_radius if unit == "km" else by_radius / 1000 if unit == "m" else by_radius * 1.60934 if unit == "mi" else 0
                unit_error = self._validate_unit(unit)
                if unit_error:
                    return unit_error

                for member, (lat2, lon2) in store[key].items():
                    distance_km = self.__calculate_distance(from_lonlat[1], from_lonlat[0], lon2, lat2)
                    if distance_km <= radius_km:
                        result.append((member, distance_km, (lat2, lon2)))

            elif by_box:
                width_km = by_box[0] if unit == "km" else by_box[0] / 1000 if unit == "m" else by_box[0] * 1.60934 if unit == "mi" else 0
                height_km = by_box[1] if unit == "km" else by_box[1] / 1000 if unit == "m" else by_box[1] * 1.60934 if unit == "mi" else 0
                unit_error = self._validate_unit(unit)
                if unit_error:
                    return unit_error

                for member, (lat2, lon2) in store[key].items():
                    if abs(lat2 - from_lonlat[1]) <= height_km / 2 and abs(lon2 - from_lonlat[0]) <= width_km / 2:
                        result.append((member, self.__calculate_distance(from_lonlat[1], from_lonlat[0], lon2, lat2), (lat2, lon2)))

            else:
                return "ERR Missing BYRADIUS or BYBOX"

            if sort_order == "ASC":
                result.sort(key=lambda x: x[1])
            elif sort_order == "DESC":
                result.sort(key=lambda x: -x[1])

            if count is not None:
                result = result[:count]

            final_result = []
            for item in result:
                entry = [item[0]]
                if with_dist:
                    entry.append(item[1])
                if with_coord:
                    entry.append(item[2])
                if with_hash:
                    entry.append(hash(item[0]))
                final_result.append(entry)

            return final_result
