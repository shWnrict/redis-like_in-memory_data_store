# src/datatypes/hashes.py
from src.logger import setup_logger
import threading

logger = setup_logger("hashes")

class Hashes:
    def __init__(self):
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

    def hmset(self, store, key, field_value_pairs):
        """
        Sets multiple fields in the hash stored at the key.
        """
        with self.lock:
            if key not in store:
                store[key] = {}
            if not isinstance(store[key], dict):
                return "ERR Key is not a hash"
            for field, value in field_value_pairs.items():
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
            items = list(store[key].items())
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
