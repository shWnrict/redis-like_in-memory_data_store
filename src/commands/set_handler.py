from .base_handler import BaseCommandHandler

class SetCommandHandler(BaseCommandHandler):
    def get_commands(self):
        return {
            "SADD": self.sadd_command,
            "SREM": self.srem_command,
            "SISMEMBER": self.sismember_command,
            "SMEMBERS": self.smembers_command,
            "SINTER": self.sinter_command,
            "SUNION": self.sunion_command,
            "SDIFF": self.sdiff_command,
        }

    def sadd_command(self, client_id, key, *members):
        if not members:
            return "ERROR: Wrong number of arguments"
        return str(self.db.set.sadd(key, *members))

    def srem_command(self, client_id, key, *members):
        if not members:
            return "ERROR: Wrong number of arguments"
        return str(self.db.set.srem(key, *members))

    def sismember_command(self, client_id, key, member):
        return "1" if self.db.set.sismember(key, member) else "0"

    def smembers_command(self, client_id, key):
        result = self.db.set.smembers(key)
        return result if result else "(empty set)"

    def sinter_command(self, client_id, *keys):
        if not keys:
            return "ERROR: Wrong number of arguments"
        return self.db.set.sinter(*keys)

    def sunion_command(self, client_id, *keys):
        if not keys:
            return "ERROR: Wrong number of arguments"
        return self.db.set.sunion(*keys)

    def sdiff_command(self, client_id, *keys):
        if not keys:
            return "ERROR: Wrong number of arguments"
        return self.db.set.sdiff(*keys)
