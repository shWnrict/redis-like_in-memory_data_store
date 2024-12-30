from .base_handler import BaseCommandHandler

class GeoCommandHandler(BaseCommandHandler):
    def get_commands(self):
        return {
            "GEOADD": self.geoadd_command,
            "GEOSEARCH": self.geosearch_command,
            "GEODIST": self.geodist_command,
        }

    def geoadd_command(self, client_id, *args):
        """Add geo points."""
        if len(args) < 4:  # Need at least key + lon + lat + member
            return "ERR wrong number of arguments for 'geoadd' command"

        key = args[0]
        coords_and_members = args[1:]

        if len(coords_and_members) % 3 != 0:
            return "ERR wrong number of arguments for 'geoadd' command"

        try:
            result = self.db.geo.geoadd(key, *coords_and_members)
            return result
        except ValueError as e:
            return str(e)

    def geosearch_command(self, client_id, *args):
        """Search within radius or box."""
        if len(args) < 4:
            return "ERR wrong number of arguments for 'geosearch' command"

        try:
            key = args[0]
            pos = 1
            lat = lon = None
            from_member = None
            radius = width = height = None
            unit = 'm'
            with_coord = with_dist = False

            # Parse FROMMEMBER or FROMLONLAT
            if args[pos].upper() == "FROMMEMBER":
                from_member = args[pos + 1]
                pos += 2
            elif args[pos].upper() == "FROMLONLAT":
                lon = float(args[pos + 1])
                lat = float(args[pos + 2])
                pos += 3

            # Parse BYRADIUS or BYBOX
            if args[pos].upper() == "BYRADIUS":
                radius = float(args[pos + 1])
                unit = args[pos + 2].lower()
                pos += 3
            elif args[pos].upper() == "BYBOX":
                width = float(args[pos + 1])
                height = float(args[pos + 2])
                unit = args[pos + 3].lower()
                pos += 4

            # Parse optional arguments
            while pos < len(args):
                arg = args[pos].upper()
                if arg == "WITHCOORD":
                    with_coord = True
                elif arg == "WITHDIST":
                    with_dist = True
                pos += 1

            result = self.db.geo.geosearch(
                key=key,
                from_member=from_member,
                lon=lon,
                lat=lat,
                radius=radius,
                width=width,
                height=height,
                unit=unit,
                with_coord=with_coord,
                with_dist=with_dist
            )
            return result if result else []
        except ValueError as e:
            return f"ERR {str(e)}"

    def geodist_command(self, client_id, *args):
        """Get distance between points."""
        if len(args) < 3:
            return "ERR wrong number of arguments for 'geodist' command"

        key = args[0]
        member1 = args[1]
        member2 = args[2]
        unit = args[3].lower() if len(args) > 3 else 'm'

        result = self.db.geo.geodist(key, member1, member2, unit)
        return result if result is not None else "(nil)"
