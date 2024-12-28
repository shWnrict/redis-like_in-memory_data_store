# src/datatypes/sets.py
from src.logger import setup_logger
from src.datatypes.base import BaseDataType
from src.protocol import RESPProtocol
import threading

logger = setup_logger("sets")

class Sets(BaseDataType):
    def __init__(self, store, expiry_manager=None):
        super().__init__(store, expiry_manager)
        self.lock = threading.Lock()

    def sadd(self, store, key, *members):
        """Add members to set"""
        with self.lock:
            if key not in store:
                store[key] = set()
            elif not isinstance(store[key], set):
                return "ERR Key exists but is not a set"
            
            added = 0
            for member in members:
                if member not in store[key]:
                    store[key].add(member)
                    added += 1
            return added

    def srem(self, store, key, *members):
        """Remove members from set"""
        with self.lock:
            if key not in store or not isinstance(store[key], set):
                return 0
            
            removed = 0
            for member in members:
                if member in store[key]:
                    store[key].remove(member)
                    removed += 1
            return removed

    def sinter(self, store, *keys):
        """Return intersection of multiple sets"""
        with self.lock:
            sets = []
            for key in keys:
                if key not in store or not isinstance(store[key], set):
                    return set()
                sets.append(store[key])
            return set.intersection(*sets)

    def sunion(self, store, *keys):
        """Return union of multiple sets"""
        with self.lock:
            result = set()
            for key in keys:
                if key in store and isinstance(store[key], set):
                    result.update(store[key])
            return result

    def sdiff(self, store, *keys):
        """Return difference between first set and all others"""
        with self.lock:
            if not keys or keys[0] not in store:
                return set()
            result = store[keys[0]].copy()
            for key in keys[1:]:
                if key in store and isinstance(store[key], set):
                    result -= store[key]
            return result

    def sismember(self, store, key, member):
        """Check if member exists in set"""
        with self.lock:
            if key not in store or not isinstance(store[key], set):
                return 0
            return 1 if member in store[key] else 0

    def smembers(self, store, key):
        """Return all members of set"""
        with self.lock:
            if key not in store or not isinstance(store[key], set):
                return set()
            return store[key].copy()

    def handle_command(self, cmd, store, *args):
        """Handle set commands"""
        cmd = cmd.upper()
        if cmd == "SADD":
            if len(args) < 2:
                return "-ERR wrong number of arguments\r\n"
            key, *members = args
            return f":{self.sadd(store, key, *members)}\r\n"
        elif cmd == "SREM":
            if len(args) < 2:
                return "-ERR wrong number of arguments\r\n"
            key, *members = args
            return f":{self.srem(store, key, *members)}\r\n"
        elif cmd == "SISMEMBER":
            if len(args) != 2:
                return "-ERR wrong number of arguments\r\n"
            key, member = args
            return f":{self.sismember(store, key, member)}\r\n"
        elif cmd == "SMEMBERS":
            if len(args) != 1:
                return "-ERR wrong number of arguments\r\n"
            key = args[0]
            return RESPProtocol.format_array(self.smembers(store, key))
        elif cmd == "SINTER":
            if len(args) < 2:
                return "-ERR wrong number of arguments\r\n"
            return RESPProtocol.format_array(self.sinter(store, *args))
        elif cmd == "SUNION":
            if len(args) < 2:
                return "-ERR wrong number of arguments\r\n"
            return RESPProtocol.format_array(self.sunion(store, *args))
        elif cmd == "SDIFF":
            if len(args) < 2:
                return "-ERR wrong number of arguments\r\n"
            return RESPProtocol.format_array(self.sdiff(store, *args))
        else:
            return "-ERR Unknown set command\r\n"
