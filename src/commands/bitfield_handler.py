from .base_handler import BaseCommandHandler

class BitFieldCommandHandler(BaseCommandHandler):
    def get_commands(self):
        return {
            "BITFIELD": self.bitfield_command,
        }

    def bitfield_command(self, client_id, key, *args):
        """Handle BITFIELD commands. Format: BITFIELD key GET encoding offset | SET encoding offset value | INCRBY encoding offset increment"""
        if not args:
            return "ERROR: Wrong number of arguments for BITFIELD"

        results = []
        pos = 0

        while pos < len(args):
            if pos >= len(args):
                break

            subcommand = args[pos].upper()
            pos += 1

            try:
                if subcommand == "GET":
                    if pos + 2 > len(args):
                        return "ERROR: Wrong number of arguments for GET"
                    type_spec = args[pos]
                    offset = args[pos + 1]
                    result = self.db.bitfield.get(key, type_spec, offset)
                    results.append(int(result) if result is not None else "(nil)")
                    pos += 2

                elif subcommand == "SET":
                    if pos + 3 > len(args):
                        return "ERROR: Wrong number of arguments for SET"
                    type_spec = args[pos]
                    offset = args[pos + 1]
                    try:
                        value = int(args[pos + 2])
                    except ValueError:
                        return f"ERROR: Invalid value: {args[pos + 2]}"
                    result = self.db.bitfield.set(key, type_spec, offset, value)
                    results.append(int(result) if result is not None else "(nil)")
                    pos += 3

                elif subcommand == "INCRBY":
                    if pos + 3 > len(args):
                        return "ERROR: Wrong number of arguments for INCRBY"
                    type_spec = args[pos]
                    offset = args[pos + 1]
                    try:
                        increment = int(args[pos + 2])
                    except ValueError:
                        return f"ERROR: Invalid increment: {args[pos + 2]}"
                    result = self.db.bitfield.incrby(key, type_spec, offset, increment)
                    results.append(int(result) if result is not None else "(nil)")
                    pos += 3

                else:
                    return f"ERROR: Unknown BITFIELD subcommand {subcommand}"

            except ValueError as e:
                return f"ERROR: {str(e)}"

        return results
