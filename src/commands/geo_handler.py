from .base_handler import BaseCommandHandler

class GeoCommandHandler(BaseCommandHandler):
    def get_commands(self):
        return {
            "GEOADD": self.geoadd_command,
            "GEOSEARCH": self.geosearch_command,
            "GEODIST": self.geodist_command,
        }

    def geoadd_command(self, client_id, key, *args):
        """Add geo points. Format: GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]"""
        if not args:
            return "ERROR: Wrong number of arguments for GEOADD"

        # Parse options
        nx = False
        xx = False
        ch = False
        pos = 0

        while pos < len(args):
            arg = str(args[pos]).upper()
            if arg == "NX":
                nx = True
                pos += 1
            elif arg == "XX":
                xx = True
                pos += 1
            elif arg == "CH":
                ch = True
                pos += 1
            else:
                break

        # Check for mutually exclusive options
        if nx and xx:
            return "ERROR: XX and NX options are mutually exclusive"

        # Ensure remaining arguments are in sets of 3
        remaining = args[pos:]
        if len(remaining) < 3 or len(remaining) % 3 != 0:
            return "ERROR: Wrong number of arguments for GEOADD"

        try:
            # Pass remaining arguments and options separately
            return str(self.db.geo.geoadd(key, *remaining, nx=nx, xx=xx, ch=ch))
        except ValueError as e:
            return f"ERROR: {str(e)}"

    def geosearch_command(self, client_id, key, *args):
        """Enhanced GEOSEARCH command."""
        if len(args) < 2:  # At least need FROMLONLAT + coordinates or FROMMEMBER + member
            return "ERROR: Wrong number of arguments for GEOSEARCH"

        try:
            # Parse search origin
            pos = 0
            from_member = None
            lon = lat = None

            first_arg = str(args[pos]).upper()
            if first_arg == "FROMMEMBER":
                if pos + 1 >= len(args):
                    return "ERROR: Missing member name for FROMMEMBER"
                from_member = args[pos + 1]
                pos += 2
            elif first_arg == "FROMLONLAT":
                if pos + 2 >= len(args):
                    return "ERROR: Missing coordinates for FROMLONLAT"
                lon = float(args[pos + 1])
                lat = float(args[pos + 2])
                pos += 3
            else:
                # If no FROMMEMBER/FROMLONLAT specified, treat first two args as lon/lat
                try:
                    lon = float(args[pos])
                    lat = float(args[pos + 1])
                    pos += 2
                except (ValueError, IndexError):
                    return "ERROR: Invalid coordinates"

            # Parse search area
            radius = width = height = None
            unit = 'km'

            if pos < len(args):
                if args[pos].upper() == "BYRADIUS":
                    if pos + 2 >= len(args):
                        return "ERROR: Missing radius or unit"
                    radius = float(args[pos + 1])
                    unit = args[pos + 2].lower()
                    pos += 3
                elif args[pos].upper() == "BYBOX":
                    if pos + 3 >= len(args):
                        return "ERROR: Missing box dimensions or unit"
                    width = float(args[pos + 1])
                    height = float(args[pos + 2])
                    unit = args[pos + 3].lower()
                    pos += 4
                else:
                    return "ERROR: Missing BYRADIUS or BYBOX"

            # Parse optional arguments
            sort = None
            count = None
            any_order = False
            with_coord = False
            with_dist = False
            with_hash = False

            while pos < len(args):
                arg = args[pos].upper()
                if arg in ("ASC", "DESC"):
                    sort = arg
                    pos += 1
                elif arg == "COUNT":
                    if pos + 1 >= len(args):
                        return "ERROR: Missing count value"
                    count = int(args[pos + 1])
                    pos += 2
                    if pos < len(args) and args[pos].upper() == "ANY":
                        any_order = True
                        pos += 1
                elif arg == "WITHCOORD":
                    with_coord = True
                    pos += 1
                elif arg == "WITHDIST":
                    with_dist = True
                    pos += 1
                elif arg == "WITHHASH":
                    with_hash = True
                    pos += 1
                else:
                    return f"ERROR: Unknown option {arg}"

            # Execute search
            result = self.db.geo.geosearch(
                key=key,
                from_member=from_member,
                lon=lon,
                lat=lat,
                radius=radius,
                width=width,
                height=height,
                with_coord=with_coord,
                with_dist=with_dist,
                with_hash=with_hash,
                count=count,
                any_order=any_order,
                sort=sort
            )

            # Format result based on options
            if not result:
                return []

            formatted_result = []
            for item in result:
                if with_coord or with_dist or with_hash:
                    # If any WITH* option is used, keep the nested array format
                    formatted_result.append(item)
                else:
                    # For simple queries without options, just return member names
                    formatted_result.append(item[0])

            return formatted_result

        except ValueError as e:
            return f"ERROR: {str(e)}"

    def geodist_command(self, client_id, key, member1, member2, *args):
        """Get distance between members. Format: GEODIST key member1 member2 [unit]"""
        # Parse unit if provided
        unit = args[0].lower() if args else 'm'
        
        # Validate unit
        if unit not in ['m', 'km', 'mi', 'ft']:
            return "ERROR: unsupported unit provided. please use M, KM, FT, MI"
        
        result = self.db.geo.geodist(key, member1, member2, unit)
        if result is None:
            return "(nil)"
            
        # Format result as string with exact precision
        return f"{result:f}".rstrip('0').rstrip('.')
