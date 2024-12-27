# src/datatypes/hashes.py
from src.logger import setup_logger
import threading
from src.datatypes.base import BaseDataType  # Import BaseDataType

logger = setup_logger("hashes")

class Hashes(BaseDataType):
    def __init__(self, store, expiry_manager=None):
        super().__init__(store, expiry_manager)
        self.lock = threading.Lock()

    def hset(self, store, key, field, value):
        """
        Sets a field in the hash stored at the key.
        """
        with self.lock:
            if key not in store:
                store[key] = {}
            if not isinstance(store[key], dict):
                return "ERR Key is not a hash"
            new_field = field not in store[key]
            store[key][field] = value
            logger.info(f"HSET {key} {field} -> {value}")
            return 1 if new_field else 0

    def hmset(self, store, key, *field_value_pairs):
        """
        Sets multiple fields in the hash stored at the key.
        """
        if len(field_value_pairs) % 2 != 0:
            return "ERR Invalid number of arguments for HMSET"
        
        with self.lock:
            if key not in store:
                store[key] = {}
            if not isinstance(store[key], dict):
                return "ERR Key is not a hash"
            for i in range(0, len(field_value_pairs), 2):
                field = field_value_pairs[i]
                value = field_value_pairs[i + 1]
                store[key][field] = value
            logger.info(f"HMSET {key} -> {field_value_pairs}")
            return "OK"

    def hget(self, store, key, field):
        """
        Gets the value of a field in the hash stored at the key.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], dict):
                return "(nil)"
            value = store[key].get(field)
            logger.info(f"HGET {key} {field} -> {value}")
            return value if value is not None else "(nil)"

    def hgetall(self, store, key):
        """
        Gets all fields and values of the hash stored at the key.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], dict):
                return "(nil)"
            items = []
            for field, value in store[key].items():
                items.append(field)
                items.append(value)
            logger.info(f"HGETALL {key} -> {items}")
            return items

    def hdel(self, store, key, *fields):
        """
        Deletes one or more fields from the hash stored at the key.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], dict):
                return 0
            deleted = 0
            for field in fields:
                if field in store[key]:
                    del store[key][field]
                    deleted += 1
            logger.info(f"HDEL {key} -> {deleted} fields deleted")
            return deleted

    def hexists(self, store, key, field):
        """
        Checks if a field exists in the hash stored at the key.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], dict):
                return 0
            exists = 1 if field in store[key] else 0
            logger.info(f"HEXISTS {key} {field} -> {exists}")
            return exists

    def handle_command(self, cmd, store, *args):
        if cmd == "HSET":
            return self.hset(store, args[0], args[1], args[2])
        elif cmd == "HMSET":
            return self.hmset(store, args[0], *args[1:])
        elif cmd == "HGET":
            return self.hget(store, args[0], args[1])
        elif cmd == "HGETALL":
            return self.hgetall(store, args[0])
        elif cmd == "HDEL":
            return self.hdel(store, args[0], *args[1:])
        elif cmd == "HEXISTS":
            return self.hexists(store, args[0], args[1])
        return "ERR Unknown command"
