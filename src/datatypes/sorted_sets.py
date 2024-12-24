# src/datatypes/sorted_sets.py
from src.logger import setup_logger
import threading
from sortedcontainers import SortedDict

logger = setup_logger("sorted_sets")

class SortedSets:
    def __init__(self):
        self.lock = threading.Lock()

    def zadd(self, store, key, *args):
        """
        Adds elements with their scores to the sorted set.
        """
        if len(args) % 2 != 0:
            return "ERR Invalid number of arguments"
        
        with self.lock:
            if key not in store:
                store[key] = SortedDict()
            if not isinstance(store[key], SortedDict):
                return "ERR Key is not a sorted set"

            added = 0
            for i in range(0, len(args), 2):
                score = float(args[i])
                member = args[i + 1]
                if member not in store[key]:
                    added += 1
                store[key][member] = score
            logger.info(f"ZADD {key} -> {added} members added")
            return added

    def zrange(self, store, key, start, end, with_scores=False):
        """
        Returns a range of members in the sorted set by rank.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], SortedDict):
                return []
            start, end = int(start), int(end)
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
            min_score, max_score = float(min_score), float(max_score)
            result = [(member, score) for member, score in store[key].items()
                      if min_score <= score <= max_score]
            if not with_scores:
                result = [member for member, _ in result]
            logger.info(f"ZRANGEBYSCORE {key} [{min_score}:{max_score}] -> {result}")
            return result
