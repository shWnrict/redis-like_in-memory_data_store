import math
from typing import Dict, List, Optional, Tuple

class Geohash:
    BITS_PER_CHAR = 5
    BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    
    @staticmethod
    def encode(lat: float, lon: float, precision: int = 12) -> str:
        """Encode latitude and longitude to geohash."""
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

class GeoDataType:
    EARTH_RADIUS_KM = 6371.0

    def __init__(self, database):
        self.db = database

    def _ensure_geo(self, key: str) -> Dict:
        """Ensure the value at key is a geo index."""
        if not self.db.exists(key):
            value = {
                'points': {},  # member -> (lat, lon, geohash)
                'hashes': {}   # geohash -> set(members)
            }
            self.db.store[key] = value
            return value
        value = self.db.get(key)
        if not isinstance(value, dict) or 'points' not in value:
            raise ValueError("Value is not a geo index")
        return value

    def _haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate the great circle distance between two points in kilometers."""
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = (math.sin(delta_phi/2) * math.sin(delta_phi/2) +
             math.cos(phi1) * math.cos(phi2) *
             math.sin(delta_lambda/2) * math.sin(delta_lambda/2))
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        return self.EARTH_RADIUS_KM * c

    def geoadd(self, key: str, *args, nx: bool = False, xx: bool = False, ch: bool = False) -> int:
        """Add geo points to the index with options."""
        if len(args) % 3 != 0:
            raise ValueError("Wrong number of arguments")

        try:
            geo_index = self._ensure_geo(key)
            added = 0
            changed = 0

            # Process arguments in groups of three: longitude, latitude, member
            for i in range(0, len(args), 3):
                try:
                    lon = float(args[i])
                    lat = float(args[i+1])
                    member = args[i+2]

                    # Validate coordinates
                    if not (-180 <= lon <= 180):
                        continue
                    if not (-85.05112878 <= lat <= 85.05112878):  # EPSG:900913 limits
                        continue

                    exists = member in geo_index['points']
                    # Skip if XX and doesn't exist, or if NX and exists
                    if (xx and not exists) or (nx and exists):
                        continue

                    # Generate geohash
                    geohash = Geohash.encode(lat, lon)
                    old_coords = None
                    
                    # Check if updating existing member
                    if exists:
                        old_coords = geo_index['points'][member]
                        old_hash = old_coords[2]
                        if old_hash in geo_index['hashes']:
                            geo_index['hashes'][old_hash].remove(member)
                            if not geo_index['hashes'][old_hash]:
                                del geo_index['hashes'][old_hash]
                    else:
                        added += 1

                    # Add new entry
                    new_coords = (lat, lon, geohash)
                    if old_coords != new_coords:
                        changed += 1
                    
                    geo_index['points'][member] = new_coords
                    if geohash not in geo_index['hashes']:
                        geo_index['hashes'][geohash] = set()
                    geo_index['hashes'][geohash].add(member)

                except (ValueError, TypeError):
                    continue

            if (added or changed) and not self.db.replaying:
                options = []
                if nx:
                    options.append("NX")
                if xx:
                    options.append("XX")
                if ch:
                    options.append("CH")
                opt_str = ' '.join(options)
                cmd_args = ' '.join(map(str, args))
                self.db.persistence_manager.log_command(
                    f"GEOADD {key}{' ' + opt_str if opt_str else ''} {cmd_args}")

            return changed if ch else added

        except ValueError as e:
            raise ValueError(f"GEOADD error: {str(e)}")

    def geosearch(self, key: str, from_member: str = None, lon: float = None, lat: float = None,
                  radius: float = None, width: float = None, height: float = None,
                  with_coord: bool = False, with_dist: bool = False, with_hash: bool = False,
                  count: int = None, any_order: bool = False, sort: str = None) -> List:
        """Enhanced search for geo points."""
        try:
            geo_index = self._ensure_geo(key)
            if not geo_index['points']:
                return []

            # Get center coordinates
            if from_member:
                if from_member not in geo_index['points']:
                    raise ValueError("Member not found")
                center_lat, center_lon, _ = geo_index['points'][from_member]
            else:
                if lon is None or lat is None:
                    raise ValueError("Either FROMMEMBER or FROMLONLAT must be specified")
                center_lat, center_lon = lat, lon

            results = []
            for member, (mlat, mlon, geohash) in geo_index['points'].items():
                distance = self._haversine_distance(center_lat, center_lon, mlat, mlon)
                
                # Check if point is within search area
                if radius is not None:
                    if distance > radius:
                        continue
                elif width is not None and height is not None:
                    # Check if point is within rectangle
                    lat_diff = abs(mlat - center_lat)
                    lon_diff = abs(mlon - center_lon)
                    if lat_diff > height/2 or lon_diff > width/2:
                        continue
                else:
                    raise ValueError("Either BYRADIUS or BYBOX must be specified")

                # Build result based on WITH* options
                result = [member]
                if with_dist:
                    result.append(str(round(distance, 4)))
                if with_coord:
                    result.append([str(mlon), str(mlat)])
                if with_hash:
                    result.append(geohash)
                
                results.append((distance, result))

            # Sort results
            if sort:
                results.sort(key=lambda x: x[0], reverse=(sort == 'DESC'))
            elif not any_order:
                results.sort(key=lambda x: x[0])  # Default ASC

            # Apply count limit
            if count:
                results = results[:count]

            # Return just the formatted results without distances used for sorting
            return [r[1] for r in results]

        except ValueError as e:
            raise ValueError(f"GEOSEARCH error: {str(e)}")

    def geodist(self, key: str, member1: str, member2: str, unit: str = 'm') -> Optional[float]:
        """
        Calculate distance between two members.
        Units: m (meters), km (kilometers), mi (miles), ft (feet)
        Returns None if either member doesn't exist
        """
        try:
            geo_index = self._ensure_geo(key)
            if member1 not in geo_index['points'] or member2 not in geo_index['points']:
                return None

            lat1, lon1, _ = geo_index['points'][member1]
            lat2, lon2, _ = geo_index['points'][member2]
            
            # Calculate base distance in meters
            distance = self._haversine_distance(lat1, lon1, lat2, lon2) * 1000  # Convert to meters
            
            # Convert to requested unit
            if unit == 'km':
                distance /= 1000
            elif unit == 'mi':
                distance /= 1609.344
            elif unit == 'ft':
                distance *= 3.28084
            # else 'm' is default, no conversion needed
            
            # Format with 4 decimal places, matching Redis behavior
            return round(distance, 4)
        except ValueError:
            return None
