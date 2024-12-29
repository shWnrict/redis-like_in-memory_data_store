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
        return str(self.db.sets.sadd(key, *members))

    def srem_command(self, client_id, key, *members):
        if not members:
            return "ERROR: Wrong number of arguments"
        return str(self.db.sets.srem(key, *members))

    def sismember_command(self, client_id, key, member):
        return "1" if self.db.sets.sismember(key, member) else "0"

    def smembers_command(self, client_id, key):
        result = self.db.sets.smembers(key)
        return result if result else "(empty set)"

    def sinter_command(self, client_id, *keys):
        if not keys:
            return "ERROR: Wrong number of arguments"
        return self.db.sets.sinter(*keys)

    def sunion_command(self, client_id, *keys):
        if not keys:
            return "ERROR: Wrong number of arguments"
        return self.db.sets.sunion(*keys)

    def sdiff_command(self, client_id, *keys):
        if not keys:
            return "ERROR: Wrong number of arguments"
        return self.db.sets.sdiff(*keys)
