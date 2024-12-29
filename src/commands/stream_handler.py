from .base_handler import BaseCommandHandler

class StreamCommandHandler(BaseCommandHandler):
    def get_commands(self):
        return {
            "XADD": self.xadd_command,
            "XREAD": self.xread_command,
            "XRANGE": self.xrange_command,
            "XLEN": self.xlen_command,
            "XGROUP": self.xgroup_command,
            "XREADGROUP": self.xreadgroup_command,
            "XACK": self.xack_command,
        }

    def _format_entry(self, entry):
        """Format a stream entry into Redis protocol format."""
        id, fields = entry
        # Convert fields dict to flat list of alternating keys and values
        field_list = []
        for k, v in fields.items():
            field_list.extend([k, v])
        return [id, field_list]

    def xadd_command(self, client_id, key, *args):
        """Add entry to stream. Format: XADD key [ID] field value [field value ...]"""
        if len(args) < 3 or len(args) % 2 == 0:
            return "ERROR: Wrong number of arguments for XADD"
        
        try:
            id = args[0]
            fields = {}
            for i in range(1, len(args), 2):
                fields[args[i]] = args[i + 1]
            
            return self.db.stream.xadd(key, fields, id)
        except ValueError as e:
            return f"ERROR: {str(e)}"

    def xread_command(self, client_id, *args):
        """Read from streams. Format: XREAD [COUNT count] STREAMS key [key ...] ID [ID ...]"""
        if len(args) < 3:
            return "ERROR: Wrong number of arguments for XREAD"
        
        count = None
        arg_pos = 0

        if args[0].upper() == "COUNT":
            if len(args) < 5:
                return "ERROR: Wrong number of arguments for XREAD with COUNT"
            try:
                count = int(args[1])
                arg_pos = 2
            except ValueError:
                return "ERROR: Invalid COUNT value"

        if args[arg_pos].upper() != "STREAMS":
            return "ERROR: Missing STREAMS keyword"
        
        arg_pos += 1
        split_point = len(args)
        for i in range(arg_pos, len(args)):
            try:
                float(args[i])
                split_point = i
                break
            except ValueError:
                continue

        keys = args[arg_pos:split_point]
        ids = args[split_point:]

        if len(keys) != len(ids):
            return "ERROR: Unbalanced keys and IDs"

        result = self.db.stream.xread(keys, ids, count)
        if not result:
            return "(nil)"
        
        # Format result for protocol
        formatted = []
        for key, entries in result.items():
            formatted.append(key)
            formatted.append([self._format_entry(entry) for entry in entries])
        return formatted

    def xrange_command(self, client_id, key, start='-', end='+', *args):
        """Return range of entries. Format: XRANGE key start end [COUNT count]"""
        count = None
        if len(args) >= 2 and args[0].upper() == "COUNT":
            try:
                count = int(args[1])
            except ValueError:
                return "ERROR: Invalid COUNT value"

        result = self.db.stream.xrange(key, start, end, count)
        return [self._format_entry(entry) for entry in result] if result else "(empty list)"

    def xlen_command(self, client_id, key):
        """Return length of stream."""
        return str(self.db.stream.xlen(key))

    def xgroup_command(self, client_id, subcommand, *args):
        """Handle consumer group commands."""
        if not args:
            return "ERROR: Wrong number of arguments for XGROUP"
        
        subcommand = subcommand.upper()
        if subcommand == "CREATE":
            if len(args) < 2:
                return "ERROR: Wrong number of arguments for XGROUP CREATE"
            key, group = args[0], args[1]
            id = args[2] if len(args) > 2 else "$"
            mkstream = len(args) > 3 and args[3].upper() == "MKSTREAM"
            return "OK" if self.db.stream.xgroup_create(key, group, id, mkstream) else "ERROR: Group already exists"
        
        return f"ERROR: Unknown XGROUP subcommand {subcommand}"

    def xreadgroup_command(self, client_id, *args):
        """Read from stream as part of consumer group."""
        if len(args) < 5:  # minimum: GROUP group-name consumer-name STREAMS key id
            return "ERROR: Wrong number of arguments for XREADGROUP"

        if args[0].upper() != "GROUP":
            return "ERROR: Missing GROUP keyword"

        group = args[1]
        consumer = args[2]
        arg_pos = 3

        # Parse optional COUNT
        count = None
        if len(args) > arg_pos and args[arg_pos].upper() == "COUNT":
            if len(args) < arg_pos + 2:
                return "ERROR: Missing COUNT value"
            try:
                count = int(args[arg_pos + 1])
                arg_pos += 2
            except ValueError:
                return "ERROR: Invalid COUNT value"

        # Verify STREAMS keyword
        if arg_pos >= len(args) or args[arg_pos].upper() != "STREAMS":
            return "ERROR: Missing STREAMS keyword"
        
        arg_pos += 1
        if arg_pos >= len(args):
            return "ERROR: Missing stream key"

        # Split remaining args into keys and IDs
        remaining = args[arg_pos:]
        mid = len(remaining) // 2
        if mid == 0 or len(remaining) % 2 != 0:
            return "ERROR: Unbalanced STREAMS input"

        keys = remaining[:mid]
        ids = remaining[mid:]

        result = self.db.stream.xreadgroup(group, consumer, keys, ids, count)
        if not result:
            return "(nil)"
        
        # Format result for protocol
        formatted = []
        for key, entries in result.items():
            formatted.append(key)
            formatted.append([self._format_entry(entry) for entry in entries])
        return formatted

    def xack_command(self, client_id, key, group, *ids):
        """Acknowledge consumed messages."""
        if not ids:
            return "ERROR: Wrong number of arguments for XACK"
        return str(self.db.stream.xack(key, group, *ids))
