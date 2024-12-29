from .base_handler import BaseCommandHandler

class BitMapCommandHandler(BaseCommandHandler):
    def get_commands(self):
        return {
            "SETBIT": self.setbit_command,
            "GETBIT": self.getbit_command,
            "BITCOUNT": self.bitcount_command,
            "BITOP": self.bitop_command,
        }

    def setbit_command(self, client_id, key, offset, value):
        """Set or clear a bit in string. Format: SETBIT key offset value"""
        try:
            offset = int(offset)
            value = int(value)
            if offset < 0:
                return "ERROR: bit offset is not an integer or out of range"
            if value not in (0, 1):
                return "ERROR: bit value must be 0 or 1"
            return str(self.db.bitmap.setbit(key, offset, value))
        except ValueError as e:
            return f"ERROR: {str(e)}"

    def getbit_command(self, client_id, key, offset):
        """Get bit value from string. Format: GETBIT key offset"""
        try:
            offset = int(offset)
            if offset < 0:
                return "ERROR: bit offset is not an integer or out of range"
            return str(self.db.bitmap.getbit(key, offset))
        except ValueError as e:
            return f"ERROR: {str(e)}"

    def bitcount_command(self, client_id, key, *args):
        """Count set bits in string. Format: BITCOUNT key [start end [BYTE | BIT]]"""
        try:
            if len(args) == 0:
                return str(self.db.bitmap.bitcount(key))
            elif len(args) == 2:
                start, end = map(int, args)
                return str(self.db.bitmap.bitcount(key, start, end))
            elif len(args) == 3:
                start, end, unit = map(str, args)
                unit = unit.upper()
                if unit not in ['BYTE', 'BIT']:
                    return "ERROR: Unit must be either BYTE or BIT"
                return str(self.db.bitmap.bitcount(key, int(start), int(end), unit))
            return "ERROR: Wrong number of arguments for BITCOUNT"
        except ValueError as e:
            return f"ERROR: {str(e)}"

    def bitop_command(self, client_id, operation, destkey, *sourcekeys):
        """Perform bitwise operation. Format: BITOP operation destkey sourcekey [sourcekey ...]"""
        if not sourcekeys:
            return "ERROR: Wrong number of arguments for BITOP"
        try:
            return str(self.db.bitmap.bitop(operation, destkey, *sourcekeys))
        except ValueError as e:
            return f"ERROR: {str(e)}"
