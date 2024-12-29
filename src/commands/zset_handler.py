from .base_handler import BaseCommandHandler

class ZSetCommandHandler(BaseCommandHandler):
    def get_commands(self):
        return {
            "ZADD": self.zadd_command,
            "ZRANGE": self.zrange_command,
            "ZRANK": self.zrank_command,
            "ZREM": self.zrem_command,
            "ZRANGEBYSCORE": self.zrangebyscore_command,
        }

    def zadd_command(self, client_id, key, *args):
        """Add members to sorted set."""
        if len(args) < 2:
            return "ERROR: Wrong number of arguments for ZADD"
        try:
            return str(self.db.zset.zadd(key, *args))
        except ValueError as e:
            return f"ERROR: {str(e)}"

    def zrange_command(self, client_id, key, start, stop, *args):
        """Return range of members by index."""
        try:
            withscores = len(args) > 0 and args[0].upper() == "WITHSCORES"
            result = self.db.zset.zrange(key, int(start), int(stop), withscores)
            if not result:
                return "(empty list)"
            if withscores:
                # Flatten the list of tuples into alternating members and scores
                return [item for pair in result for item in pair]
            return result
        except ValueError:
            return "ERROR: Invalid integer value"

    def zrank_command(self, client_id, key, member):
        """Return rank of member in sorted set."""
        result = self.db.zset.zrank(key, member)
        return str(result) if result is not None else "(nil)"

    def zrem_command(self, client_id, key, *members):
        """Remove members from sorted set."""
        if not members:
            return "ERROR: Wrong number of arguments for ZREM"
        return str(self.db.zset.zrem(key, *members))

    def zrangebyscore_command(self, client_id, key, min_score, max_score, *args):
        """Return members with scores within range."""
        withscores = len(args) > 0 and args[0].upper() == "WITHSCORES"
        result = self.db.zset.zrangebyscore(key, min_score, max_score, withscores)
        if not result:
            return "(empty list)"
        if withscores:
            # Flatten the list of tuples into alternating members and scores
            return [item for pair in result for item in pair]
        return result
