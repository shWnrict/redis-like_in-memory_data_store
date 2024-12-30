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
        if not values:
            return "ERR wrong number of arguments for 'lpush' command"
        result = self.db.list.lpush(key, *values)
        return result

    def rpush_command(self, client_id, key, *values):
        if not values:
            return "ERR wrong number of arguments for 'rpush' command"
        result = self.db.list.rpush(key, *values)
        return result

    def lpop_command(self, client_id, key, *args):
        if args:
            return "ERR wrong number of arguments for 'lpop' command"
        return self.db.list.lpop(key)

    def rpop_command(self, client_id, key, *args):
        if args:
            return "ERR wrong number of arguments for 'rpop' command"
        return self.db.list.rpop(key)

    def lrange_command(self, client_id, key, start, stop):
        try:
            result = self.db.list.lrange(key, start, stop)
            if isinstance(result, str) and result.startswith("ERR"):
                return result
            return result if result else []  # Return empty list instead of None
        except Exception as e:
            return f"ERR {str(e)}"

    def lindex_command(self, client_id, key, index):
        return self.db.list.lindex(key, index)

    def lset_command(self, client_id, key, index, value):
        return self.db.list.lset(key, index, value)
