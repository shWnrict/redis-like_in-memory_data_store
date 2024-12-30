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

    def sadd_command(self, client_id, *args):
        if len(args) < 2:
            return "ERR wrong number of arguments for 'sadd' command"
        key, *members = args
        result = self.db.sets.sadd(key, *members)
        if isinstance(result, str) and result.startswith("WRONGTYPE"):
            return result
        return result

    def srem_command(self, client_id, *args):
        if len(args) < 2:
            return "ERR wrong number of arguments for 'srem' command"
        key, *members = args
        result = self.db.sets.srem(key, *members)
        if isinstance(result, str) and result.startswith("WRONGTYPE"):
            return result
        return result

    def sismember_command(self, client_id, *args):
        if len(args) != 2:
            return "ERR wrong number of arguments for 'sismember' command"
        key, member = args
        return 1 if self.db.sets.sismember(key, member) else 0

    def smembers_command(self, client_id, *args):
        if len(args) != 1:
            return "ERR wrong number of arguments for 'smembers' command"
        result = self.db.sets.smembers(args[0])
        if isinstance(result, str):
            return result
        return list(result) if result else []

    def sinter_command(self, client_id, *args):
        if not args:
            return "ERR wrong number of arguments for 'sinter' command"
        result = self.db.sets.sinter(*args)
        if isinstance(result, str):
            return result
        return list(result) if result else []

    def sunion_command(self, client_id, *args):
        if not args:
            return "ERR wrong number of arguments for 'sunion' command"
        result = self.db.sets.sunion(*args)
        if isinstance(result, str):
            return result
        return list(result) if result else []

    def sdiff_command(self, client_id, *args):
        if not args:
            return "ERR wrong number of arguments for 'sdiff' command"
        result = self.db.sets.sdiff(*args)
        if isinstance(result, str):
            return result
        return list(result) if result else []
