# src/datatypes/sets.py
from src.logger import setup_logger
import threading

logger = setup_logger("sets")

class Sets:
    def __init__(self):
        self.lock = threading.Lock()

    def sadd(self, store, key, *members):
        """
        Adds members to the set stored at the key.
        """
        with self.lock:
            if key not in store:
                store[key] = set()
            if not isinstance(store[key], set):
                return "ERR Key is not a set"
            added = 0
            for member in members:
                if member not in store[key]:
                    store[key].add(member)
                    added += 1
            logger.info(f"SADD {key} -> {added} members added")
            return added

    def srem(self, store, key, *members):
        """
        Removes members from the set stored at the key.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], set):
                return "ERR Key is not a set"
            removed = 0
            for member in members:
                if member in store[key]:
                    store[key].remove(member)
                    removed += 1
            logger.info(f"SREM {key} -> {removed} members removed")
            return removed

    def sismember(self, store, key, member):
        """
        Checks if a member is part of the set stored at the key.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], set):
                return 0
            exists = 1 if member in store[key] else 0
            logger.info(f"SISMEMBER {key} {member} -> {exists}")
            return exists

    def smembers(self, store, key):
        """
        Returns all members of the set stored at the key.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], set):
                return "(nil)"
            members = list(store[key])
            logger.info(f"SMEMBERS {key} -> {members}")
            return members

    def sinter(self, store, *keys):
        """
        Returns the intersection of sets stored at the specified keys.
        """
        with self.lock:
            sets = [store[key] for key in keys if key in store and isinstance(store[key], set)]
            if not sets:
                return []
            result = list(set.intersection(*sets))
            logger.info(f"SINTER {keys} -> {result}")
            return result

    def sunion(self, store, *keys):
        """
        Returns the union of sets stored at the specified keys.
        """
        with self.lock:
            sets = [store[key] for key in keys if key in store and isinstance(store[key], set)]
            result = list(set.union(*sets)) if sets else []
            logger.info(f"SUNION {keys} -> {result}")
            return result

    def sdiff(self, store, key1, *keys):
        """
        Returns the difference of sets stored at the specified keys.
        """
        with self.lock:
            if key1 not in store or not isinstance(store[key1], set):
                return []
            base_set = store[key1]
            other_sets = [store[key] for key in keys if key in store and isinstance(store[key], set)]
            result = list(base_set.difference(*other_sets)) if other_sets else list(base_set)
            logger.info(f"SDIFF {key1} {keys} -> {result}")
            return result

    def handle_command(self, cmd, store, *args):
        if cmd == "SADD":
            return self.sadd(store, args[0], *args[1:])
        elif cmd == "SREM":
            return self.srem(store, args[0], *args[1:])
        elif cmd == "SISMEMBER":
            return self.sismember(store, args[0], args[1])
        elif cmd == "SMEMBERS":
            return self.smembers(store, args[0])
        elif cmd == "SINTER":
            return self.sinter(store, *args)
        elif cmd == "SUNION":
            return self.sunion(store, *args)
        elif cmd == "SDIFF":
            return self.sdiff(store, args[0], *args[1:])
        return "ERR Unknown command"
