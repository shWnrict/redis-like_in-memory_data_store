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

    def hset_command(self, client_id, *args):
        if len(args) != 3:
            return "ERR wrong number of arguments for 'hset' command"
        key, field, value = args
        result = self.db.hash.hset(key, field, value)
        if isinstance(result, str) and result.startswith("WRONGTYPE"):
            return result
        return str(result)

    def hget_command(self, client_id, *args):
        if len(args) != 2:
            return "ERR wrong number of arguments for 'hget' command"
        key, field = args
        result = self.db.hash.hget(key, field)
        if isinstance(result, str) and result.startswith("WRONGTYPE"):
            return result
        return result if result is not None else "(nil)"

    def hmset_command(self, client_id, *args):
        if len(args) < 3 or len(args) % 2 == 0:
            return "ERR wrong number of arguments for 'hmset' command"
        key = args[0]
        field_values = {}
        for i in range(1, len(args), 2):
            field_values[args[i]] = args[i + 1]
        result = self.db.hash.hmset(key, field_values)
        if isinstance(result, str) and result.startswith("WRONGTYPE"):
            return result
        return "OK" if result else "ERR"

    def hgetall_command(self, client_id, *args):
        if len(args) != 1:
            return "ERR wrong number of arguments for 'hgetall' command"
        result = self.db.hash.hgetall(args[0])
        if isinstance(result, str) and result.startswith("WRONGTYPE"):
            return result
        return result if result else "(empty hash)"

    def hdel_command(self, client_id, *args):
        if len(args) < 2:
            return "ERR wrong number of arguments for 'hdel' command"
        key, *fields = args
        result = self.db.hash.hdel(key, *fields)
        if isinstance(result, str) and result.startswith("WRONGTYPE"):
            return result
        return str(result)

    def hexists_command(self, client_id, *args):
        if len(args) != 2:
            return "ERR wrong number of arguments for 'hexists' command"
        key, field = args
        return "1" if self.db.hash.hexists(key, field) else "0"
