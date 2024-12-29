from .base_handler import BaseCommandHandler

class ListCommandHandler(BaseCommandHandler):
    def get_commands(self):
        return {
            "LPUSH": self.lpush_command,
            "RPUSH": self.rpush_command,
            "LPOP": self.lpop_command,
            "RPOP": self.rpop_command,
            "LRANGE": self.lrange_command,
            "LINDEX": self.lindex_command,
            "LSET": self.lset_command,
        }

    def lpush_command(self, client_id, key, *values):
        return str(self.db.list.lpush(key, *values))

    def rpush_command(self, client_id, key, *values):
        return str(self.db.list.rpush(key, *values))

    def lpop_command(self, client_id, key):
        value = self.db.list.lpop(key)
        return value if value is not None else "(nil)"

    def rpop_command(self, client_id, key):
        value = self.db.list.rpop(key)
        return value if value is not None else "(nil)"

    def lrange_command(self, client_id, key, start, stop):
        try:
            result = self.db.list.lrange(key, int(start), int(stop))
            return result if result else "(empty list)"
        except ValueError:
            return "ERROR: Invalid integer value"

    def lindex_command(self, client_id, key, index):
        try:
            value = self.db.list.lindex(key, int(index))
            return value if value is not None else "(nil)"
        except ValueError:
            return "ERROR: Invalid integer value"

    def lset_command(self, client_id, key, index, value):
        try:
            if self.db.list.lset(key, int(index), value):
                return "OK"
            return "ERROR: Index out of range"
        except ValueError:
            return "ERROR: Invalid integer value"
