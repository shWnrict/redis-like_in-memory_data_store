from .base_handler import BaseCommandHandler

class HashCommandHandler(BaseCommandHandler):
    def get_commands(self):
        return {
            "HSET": self.hset_command,
            "HMSET": self.hmset_command,
            "HGET": self.hget_command,
            "HGETALL": self.hgetall_command,
            "HDEL": self.hdel_command,
            "HEXISTS": self.hexists_command,
        }

    def hset_command(self, client_id, key, field, value):
        return str(self.db.hash.hset(key, field, value))

    def hmset_command(self, client_id, key, *args):
        if len(args) % 2 != 0:
            return "ERROR: Wrong number of arguments for HMSET"
        mapping = dict(zip(args[::2], args[1::2]))
        return "OK" if self.db.hash.hmset(key, mapping) else "ERROR"

    def hget_command(self, client_id, key, field):
        value = self.db.hash.hget(key, field)
        return value if value is not None else "(nil)"

    def hgetall_command(self, client_id, key):
        result = self.db.hash.hgetall(key)
        return result if result else "(empty hash)"

    def hdel_command(self, client_id, key, *fields):
        if not fields:
            return "ERROR: Wrong number of arguments for HDEL"
        return str(self.db.hash.hdel(key, *fields))

    def hexists_command(self, client_id, key, field):
        return "1" if self.db.hash.hexists(key, field) else "0"
