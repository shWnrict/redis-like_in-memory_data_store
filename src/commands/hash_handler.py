from .base_handler import BaseCommandHandler

class HashCommandHandler(BaseCommandHandler):
    def get_commands(self):
        return {
            "HSET": self.hset_command,
            "HGET": self.hget_command,
            "HMSET": self.hmset_command,
            "HGETALL": self.hgetall_command,
            "HDEL": self.hdel_command,
            "HEXISTS": self.hexists_command,
        }

    def hset_command(self, client_id, key, *args):
        """Set field-value pairs in hash."""
        if len(args) < 2 or len(args) % 2 != 0:
            return "ERROR: Wrong number of arguments for HSET"
        
        # Convert args into field-value pairs
        field_values = {}
        for i in range(0, len(args), 2):
            field_values[args[i]] = args[i + 1]
        
        try:
            return str(self.db.hash.hmset(key, field_values))
        except ValueError as e:
            return f"ERROR: {str(e)}"

    def hget_command(self, client_id, key, field):
        """Get value of field in hash."""
        value = self.db.hash.hget(key, field)
        return value if value is not None else "(nil)"

    def hmset_command(self, client_id, key, *args):
        """Set multiple field-value pairs in hash."""
        if len(args) < 2 or len(args) % 2 != 0:
            return "ERROR: Wrong number of arguments for HMSET"
        
        # Convert args into field-value pairs
        field_values = {}
        for i in range(0, len(args), 2):
            field_values[args[i]] = args[i + 1]
        
        try:
            return "OK" if self.db.hash.hmset(key, field_values) else "ERROR"
        except ValueError as e:
            return f"ERROR: {str(e)}"

    def hgetall_command(self, client_id, key):
        """Get all fields and values in hash."""
        result = self.db.hash.hgetall(key)
        return result if result else "(empty hash)"

    def hdel_command(self, client_id, key, *fields):
        if not fields:
            return "ERROR: Wrong number of arguments for HDEL"
        return str(self.db.hash.hdel(key, *fields))

    def hexists_command(self, client_id, key, field):
        return "1" if self.db.hash.hexists(key, field) else "0"
