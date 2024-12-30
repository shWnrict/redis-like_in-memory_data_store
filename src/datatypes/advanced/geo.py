import math
from typing import Dict, List, Optional, Tuple, Union

class Geohash:
    BITS_PER_CHAR = 5
    BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    
    @staticmethod
    def encode(lat: float, lon: float, precision: int = 12) -> str:
        """More accurate geohash encoding."""
        lat_range = (-90.0, 90.0)
        lon_range = (-180.0, 180.0)
        geohash = []
        bits = 0
        bit_count = 0

        while len(geohash) < precision:
            if bit_count % 2 == 0:
                mid = (lon_range[0] + lon_range[1]) / 2
                if lon >= mid:
                    bits = (bits << 1) | 1
                    lon_range = (mid, lon_range[1])
                else:
                    bits = bits << 1
                    lon_range = (lon_range[0], mid)
            else:
                mid = (lat_range[0] + lat_range[1]) / 2
                if lat >= mid:
                    bits = (bits << 1) | 1
                    lat_range = (mid, lat_range[1])
                else:
                    bits = bits << 1
                    lat_range = (lat_range[0], mid)

            bit_count += 1
            if bit_count == Geohash.BITS_PER_CHAR:
                geohash.append(Geohash.BASE32[bits])
                bits = 0
                bit_count = 0

        return ''.join(geohash)

    @staticmethod
    def get_neighbors(geohash: str) -> List[str]:
        """Get neighboring geohashes for radius search."""
        # Implementation for finding neighboring cells
        # This would be used for optimizing radius searches
        pass

class GeoDataType:
    """
    GeoDataType is a class that provides geospatial data storage and querying capabilities similar to Redis' GEO commands.
    Attributes:
        EARTH_RADIUS_METERS (float): The Earth's radius in meters, used for distance calculations.
    Methods:
        __init__(database):
            Initializes the GeoDataType instance with a reference to the database.
        _ensure_geo(key: str) -> Dict:
            Ensures that the given key in the database is a valid geo index. If the key does not exist, it creates a new geo index.
        geoadd(key: str, *args) -> int:
            Adds geospatial items to the geo index. Each item is specified by longitude, latitude, and a member name. Returns the number of new elements added.
        geosearch(key: str, from_member: Optional[str] = None, lon: Optional[float] = None, lat: Optional[float] = None,
                  radius: Optional[float] = None, width: Optional[float] = None, height: Optional[float] = None, unit: str = 'm',
            Searches for members within a specified radius or bounding box from a given point or member. Returns a list of matching members, optionally including their coordinates and distances.
        geodist(key: str, member1: str, member2: str, unit: str = 'm') -> Optional[str]:
            Calculates the distance between two members in the specified unit. Returns the distance as a string formatted to four decimal places.
        _haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
            Calculates the Haversine distance between two points specified by their latitude and longitude. Returns the distance in meters.
    """
    EARTH_RADIUS_METERS = 6372797.560856  # More precise Earth radius in meters
    
    def __init__(self, database):
        self.db = database

    def _ensure_geo(self, key: str) -> Dict:
        """Create or validate geo index."""
        if not self.db.exists(key):
            value = {
                'points': {},   # member -> (lat, lon, hash)
            }
            self.db.store[key] = value
            return value
        
        value = self.db.store.get(key)
        if not isinstance(value, dict) or 'points' not in value:
            raise ValueError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return value

    def geoadd(self, key: str, *args) -> int:
        """Add geospatial items."""
        if len(args) % 3 != 0:
            raise ValueError("ERR wrong number of arguments")
            
        try:
            geo_index = self._ensure_geo(key)
            added = 0
            
            for i in range(0, len(args), 3):
                try:
                    lon = float(args[i])
                    lat = float(args[i+1])
                    member = str(args[i+2])
                    
                    # Validate coordinates
                    if not (-180 <= lon <= 180 and -85.05112878 <= lat <= 85.05112878):
                        continue
                        
                    # Generate geohash
                    geohash = Geohash.encode(lat, lon)
                    
                    if member not in geo_index['points']:
                        added += 1

                    # Update entry
                    geo_index['points'][member] = (lat, lon, geohash)
                    
                except (ValueError, TypeError):
                    continue
                    
            if added and not self.db.replaying:
                args_str = ' '.join(str(arg) for arg in args)
                self.db.persistence_manager.log_command(f"GEOADD {key} {args_str}")
                
            return added
            
        except ValueError as e:
            raise ValueError(f"ERR {str(e)}")

    def geosearch(self, key: str, from_member: Optional[str] = None, 
                  lon: Optional[float] = None, lat: Optional[float] = None,
                  radius: Optional[float] = None, width: Optional[float] = None, 
                  height: Optional[float] = None, unit: str = 'm',
                  with_coord: bool = False, with_dist: bool = False) -> List:
        """Search within radius or box."""
        try:
            geo_index = self._ensure_geo(key)
            if not geo_index['points']:
                return []

            # Get center coordinates
            if from_member:
                if from_member not in geo_index['points']:
                    return []
                center_lat, center_lon, _ = geo_index['points'][from_member]
            else:
                if lon is None or lat is None:
                    return []
                center_lat, center_lon = lat, lon

            # Convert units to meters for radius/box
            factor = 1
            if unit == 'km':
                factor = 1000
            elif unit == 'mi':
                factor = 1609.344
            elif unit == 'ft':
                factor = 0.3048

            results = []
            for member, (mlat, mlon, _) in geo_index['points'].items():
                dist = self._haversine_distance(center_lat, center_lon, mlat, mlon)
                
                # Check if point is within search area
                include = False
                if radius is not None:
                    include = dist <= radius * factor
                elif width is not None and height is not None:
                    lat_dist = abs(mlat - center_lat) * 111319.9
                    lon_dist = abs(mlon - center_lon) * 111319.9 * math.cos(math.radians(center_lat))
                    include = lat_dist <= height/2 * factor and lon_dist <= width/2 * factor

                if include:
                    result = [member]
                    if with_dist:
                        dist_converted = dist / factor
                        result.append("{:.4f}".format(dist_converted))
                    if with_coord:
                        result.append(["{:.6f}".format(mlon), "{:.6f}".format(mlat)])
                    results.append((dist, result))

            results.sort(key=lambda x: x[0])
            return [r[1] for r in results]

        except ValueError:
            return []

    def geodist(self, key: str, member1: str, member2: str, unit: str = 'm') -> Optional[str]:
        """Get distance between members."""
        try:
            geo_index = self._ensure_geo(key)
            if member1 not in geo_index['points'] or member2 not in geo_index['points']:
                return None
                
            lat1, lon1, _ = geo_index['points'][member1]
            lat2, lon2, _ = geo_index['points'][member2]
            
            dist = self._haversine_distance(lat1, lon1, lat2, lon2)
            
            # Convert to requested unit
            if unit == 'km':
                dist /= 1000
            elif unit == 'mi':
                dist /= 1609.344
            elif unit == 'ft':
                dist *= 3.28084
                
            return "{:.4f}".format(dist)
            
        except ValueError:
            return None

    def _haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Precise haversine distance calculation."""
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        
        a = math.sin(dphi/2.0)**2 + \
            math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2.0)**2
        c = 2 * math.asin(math.sqrt(a))
        
        return self.EARTH_RADIUS_METERS * c
