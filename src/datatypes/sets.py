# src/datatypes/sets.py
from src.logger import setup_logger
import threading
from src.datatypes.base import BaseDataType

logger = setup_logger("sets")

class Sets(BaseDataType):
    def __init__(self, store, expiry_manager=None):
        super().__init__(store, expiry_manager)

    def sadd(self, key, *members):
        """
        Adds members to the set stored at the key.
        """
        with self.lock:
            if key not in self.store:
                self.store[key] = set()
            if not isinstance(self.store[key], set):
                logger.error("ERR Key is not a set")
                return "ERR Key is not a set"
            added = 0
            for member in members:
                if member not in self.store[key]:
                    self.store[key].add(member)
                    added += 1
            logger.info(f"SADD {key} -> {added} members added")
            return added

    def srem(self, key, *members):
        """
        Removes members from the set stored at the key.
        """
        with self.lock:
            if key not in self.store or not isinstance(self.store[key], set):
                return "ERR Key is not a set"
            removed = 0
            for member in members:
                if member in self.store[key]:
                    self.store[key].remove(member)
                    removed += 1
            logger.info(f"SREM {key} -> {removed} members removed")
            return removed

    def sismember(self, key, member):
        """
        Checks if a member is part of the set stored at the key.
        """
        with self.lock:
            if key not in self.store or not isinstance(self.store[key], set):
                return 0
            exists = 1 if member in self.store[key] else 0
            logger.info(f"SISMEMBER {key} {member} -> {exists}")
            return exists

    def smembers(self, key):
        """
        Returns all members of the set stored at the key.
        """
        with self.lock:
            if key not in self.store or not isinstance(self.store[key], set):
                return "(nil)"
            members = list(self.store[key])
            logger.info(f"SMEMBERS {key} -> {members}")
            return members

    def sinter(self, *keys):
        """
        Returns the intersection of sets stored at the specified keys.
        """
        with self.lock:
            sets = [self.store[key] for key in keys if key in self.store and isinstance(self.store[key], set)]
            if not sets:
                return []
            result = list(set.intersection(*sets))
            logger.info(f"SINTER {keys} -> {result}")
            return result

    def sunion(self, *keys):
        """
        Returns the union of sets stored at the specified keys.
        """
        with self.lock:
            sets = [self.store[key] for key in keys if key in self.store and isinstance(self.store[key], set)]
            result = list(set.union(*sets)) if sets else []
            logger.info(f"SUNION {keys} -> {result}")
            return result

    def sdiff(self, key1, *keys):
        """
        Returns the difference of sets stored at the specified keys.
        """
        with self.lock:
            if key1 not in self.store or not isinstance(self.store[key1], set):
                return []
            base_set = self.store[key1]
            other_sets = [self.store[key] for key in keys if key in self.store and isinstance(self.store[key], set)]
            result = list(base_set.difference(*other_sets)) if other_sets else list(base_set)
            logger.info(f"SDIFF {key1} {keys} -> {result}")
            return result

    def handle_command(self, cmd, store, *args):
        if cmd == "SADD":
            return self.sadd(args[0], *args[1:])
        elif cmd == "SREM":
            return self.srem(args[0], *args[1:])
        elif cmd == "SISMEMBER":
            return self.sismember(args[0], args[1])
        elif cmd == "SMEMBERS":
            return self.smembers(args[0])
        elif cmd == "SINTER":
            return self.sinter(*args)
        elif cmd == "SUNION":
            return self.sunion(*args)
        elif cmd == "SDIFF":
            return self.sdiff(args[0], *args[1:])
        return "ERR Unknown command"
