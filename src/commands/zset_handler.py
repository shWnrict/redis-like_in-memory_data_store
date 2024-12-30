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

    def zadd_command(self, client_id, *args):
        """Add one or more members to a sorted set."""
        if len(args) < 3:
            return "ERR wrong number of arguments for 'zadd' command"
        
        key = args[0]
        score_members = []
        
        # Parse score-member pairs
        for i in range(1, len(args), 2):
            if i + 1 >= len(args):
                return "ERR syntax error"
            try:
                score = float(args[i])
                member = args[i + 1]
                score_members.extend([score, member])
            except ValueError:
                return "ERR value is not a valid float"
        
        try:
            result = self.db.zset.zadd(key, *score_members)
            return result
        except ValueError as e:
            return f"ERR {str(e)}"

    def zrange_command(self, client_id, *args):
        """Return a range of members from sorted set."""
        if len(args) < 3:
            return "ERR wrong number of arguments for 'zrange' command"
            
        key = args[0]
        try:
            start = int(args[1])
            stop = int(args[2])
            withscores = len(args) > 3 and args[3].upper() == "WITHSCORES"
        except ValueError:
            return "ERR value is not an integer or out of range"
            
        result = self.db.zset.zrange(key, start, stop, withscores)
        return result if result else []

    def zrank_command(self, client_id, *args):
        """Get the rank of a member in the sorted set."""
        if len(args) != 2:
            return "ERR wrong number of arguments for 'zrank' command"
        
        key, member = args
        result = self.db.zset.zrank(key, member)
        return result if result is not None else "(nil)"

    def zrem_command(self, client_id, *args):
        """Remove one or more members from the sorted set."""
        if len(args) < 2:
            return "ERR wrong number of arguments for 'zrem' command"
            
        key = args[0]
        members = args[1:]
        return self.db.zset.zrem(key, *members)

    def zrangebyscore_command(self, client_id, *args):
        """Return members with scores within the specified range."""
        if len(args) < 3:
            return "ERR wrong number of arguments for 'zrangebyscore' command"
            
        key = args[0]
        min_score = args[1]
        max_score = args[2]
        withscores = len(args) > 3 and args[3].upper() == "WITHSCORES"
        
        result = self.db.zset.zrangebyscore(key, min_score, max_score, withscores)
        return result if result else []
