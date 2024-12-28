# src/datatypes/sorted_sets.py
from src.logger import setup_logger
import threading  # Import threading
from sortedcontainers import SortedDict
from src.datatypes.base import BaseDataType  # Import BaseDataType
from src.protocol import RESPProtocol

logger = setup_logger("sorted_sets")

class SortedSets(BaseDataType):
    def __init__(self, store, expiry_manager=None):
        super().__init__(store, expiry_manager)
        self.lock = threading.Lock()

    def zadd(self, store, key, scores_and_members):
        """
        Add members with scores to sorted set
        """
        with self.lock:
            if key not in store:
                store[key] = SortedDict()
            sorted_set = store[key]
            
            added = 0
            for member, score in scores_and_members.items():
                if member not in sorted_set:
                    added += 1
                sorted_set[member] = float(score)
            
            return added

    def zrange(self, store, key, start, end, with_scores=False):
        """
        Returns a range of members in the sorted set by rank.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], SortedDict):
                return []
            try:
                start, end = int(start), int(end)
            except ValueError:
                return "ERR start or end is not an integer"
            members = list(store[key].keys())
            if end == -1 or end >= len(members):
                end = len(members) - 1
            result = members[start:end + 1]
            if with_scores:
                result = [(member, store[key][member]) for member in result]
            logger.info(f"ZRANGE {key} [{start}:{end}] -> {result}")
            return result

    def zrank(self, store, key, member):
        """
        Returns the rank of the member in the sorted set.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], SortedDict):
                return "(nil)"
            members = list(store[key].keys())
            rank = members.index(member) if member in members else None
            logger.info(f"ZRANK {key} {member} -> {rank}")
            return rank if rank is not None else "(nil)"

    def zrem(self, store, key, *members):
        """
        Removes members from the sorted set.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], SortedDict):
                return 0
            removed = 0
            for member in members:
                if member in store[key]:
                    del store[key][member]
                    removed += 1
            logger.info(f"ZREM {key} -> {removed} members removed")
            return removed

    def zrangebyscore(self, store, key, min_score, max_score, with_scores=False):
        """
        Returns members in the sorted set within the specified score range.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], SortedDict):
                return []
            try:
                min_score, max_score = float(min_score), float(max_score)
            except ValueError:
                return "ERR min_score or max_score is not a valid float"
            result = [(member, score) for member, score in store[key].items()
                      if min_score <= score <= max_score]
            if not with_scores:
                result = [member for member, _ in result]
            logger.info(f"ZRANGEBYSCORE {key} [{min_score}:{max_score}] -> {result}")
            return result

    def zscan(self, store, key, cursor=0, match=None, count=None):
        """
        Incrementally iterate sorted set
        """
        with self.lock:
            if key not in store:
                return 0, []
            
            sorted_set = store[key]
            members = list(sorted_set.items())
            
            # Implement cursor-based scanning
            next_cursor = 0
            if cursor >= len(members):
                return next_cursor, []
                
            end = min(cursor + (count or 10), len(members))
            result = members[cursor:end]
            
            if end < len(members):
                next_cursor = end
                
            return next_cursor, result

    def handle_command(self, cmd, store, *args):
        """
        Dispatch sorted set commands to the appropriate methods.
        """
        cmd = cmd.upper()
        if cmd == "ZADD":
            key = args[0]
            score_member_pairs = args[1:]
            if len(score_member_pairs) % 2 != 0:
                return "-ERR wrong number of arguments for 'ZADD' command\r\n"
            mapping = {score_member_pairs[i+1]: float(score_member_pairs[i]) for i in range(0, len(score_member_pairs), 2)}
            result = self.zadd(store, key, mapping)
            return f":{result}\r\n"
        elif cmd == "ZRANGE":
            key, start, end = args[:3]
            with_scores = False
            if len(args) > 3 and args[3].upper() == "WITHSCORES":
                with_scores = True
            result = self.zrange(store, key, int(start), int(end), with_scores)
            return RESPProtocol.format_response(result)
        elif cmd == "ZRANK":
            key, member = args[:2]
            rank = self.zrank(store, key, member)
            return f":{rank}\r\n" if rank is not None else "$-1\r\n"
        elif cmd == "ZREM":
            key, *members = args
            removed = self.zrem(store, key, *members)
            return f":{removed}\r\n"
        elif cmd == "ZRANGEBYSCORE":
            key, min_score, max_score = args[:3]
            with_scores = False
            if len(args) > 3 and args[3].upper() == "WITHSCORES":
                with_scores = True
            result = self.zrangebyscore(store, key, float(min_score), float(max_score), with_scores)
            return RESPProtocol.format_response(result)
        else:
            return "-ERR Unknown sorted set command\r\n"
