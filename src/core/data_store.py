# src/core/data_store.py
from threading import Lock
import time
from src.logger import setup_logger
from src.config import Config
from typing import Dict, Any
from src.pubsub.publisher import PubSubPublisher  # Import PubSubPublisher

logger = setup_logger("data_store", level=Config.LOG_LEVEL)

class DataStore:
    def __init__(self):
        self.store: Dict[str, Any] = {}
        self.expiry: Dict[str, float] = {}  # Store expiry timestamps for keys
        self.lock = Lock()
        self.publisher = PubSubPublisher()  # Initialize Publisher

    def _is_expired(self, key):
        """
        Check if a key is expired and remove it if so.
        """
        if key in self.expiry and time.time() > self.expiry[key]:
            del self.store[key]
            del self.expiry[key]
            logger.info(f"Key {key} expired and removed.")
            return True
        return False

    def set(self, key, value, ttl=None):
        """
        Set a key-value pair with an optional TTL (in seconds).
        """
        # Basic memory guard (placeholder)
        if self._exceeds_memory_threshold():
            return "-ERR Not enough memory"

        with self.lock:
            self.store[key] = value
            if ttl:
                self.expiry[key] = time.time() + ttl
            logger.info(f"SET {key} -> {value} (TTL={ttl})")
            result = "OK"
            if result == "OK":
                self.publisher.publish_invalidation(key)
            return result

    def _exceeds_memory_threshold(self):
        # Dummy example check
        MAX_MEMORY = 1024 * 1024  # 1 MB for illustration
        current_usage = sum(len(str(v)) for v in self.store.values())
        return current_usage > MAX_MEMORY

    def get(self, key):
        """
        Get the value of a key. Returns (nil) if the key doesn't exist or is expired.
        """
        with self.lock:
            if self._is_expired(key):
                return "(nil)"
            value = self.store.get(key)
            logger.info(f"GET {key} -> {value}")
            return value if value is not None else "(nil)"

    def delete(self, key):
        """
        Delete a key. Returns 1 if the key was removed, 0 otherwise.
        """
        with self.lock:
            if key in self.store:
                del self.store[key]
                self.expiry.pop(key, None)
                logger.info(f"DEL {key}")
                result = 1
                if result == 1:
                    self.publisher.publish_invalidation(key)
                return result
            logger.info(f"DEL {key} -> Key not found.")
            return 0

    def exists(self, key):
        """
        Check if a key exists and is not expired.
        """
        with self.lock:
            if self._is_expired(key):
                return 0
            exists = key in self.store
            logger.info(f"EXISTS {key} -> {exists}")
            return 1 if exists else 0

    def incr(self, key, amount=1):
        """
        Increment the integer value of a key by the given amount.
        """
        with self.lock:
            if self._is_expired(key):
                return "-ERR Key expired"
            if key not in self.store:
                self.store[key] = 0
            try:
                self.store[key] = int(self.store[key]) + amount
                logger.info(f"INCR {key} -> {self.store[key]} (by {amount})")
                return self.store[key]
            except ValueError:
                logger.error(f"INCR {key} -> ERR Value is not an integer")
                return "-ERR Value is not an integer"

    def decr(self, key, amount=1):
        """
        Decrement the integer value of a key by the given amount.
        """
        return self.incr(key, -amount)

    def lpush(self, key, value):
        with self.lock:
            self.store.setdefault(key, []).insert(0, value)
            logger.info(f"LPUSH {key} -> {value}")
            return "OK"

    def rpop(self, key):
        with self.lock:
            if self._is_expired(key):
                return "(nil)"
            lst = self.store.get(key, [])
            if lst:
                value = lst.pop()
                logger.info(f"RPOP {key} -> {value}")
                return value
            logger.info(f"RPOP {key} -> (nil)")
            return "(nil)"

    def lindex(self, key, index):
        with self.lock:
            if self._is_expired(key):
                return "(nil)"
            lst = self.store.get(key, [])
            if -len(lst) <= index < len(lst):
                value = lst[index]
                logger.info(f"LINDEX {key} {index} -> {value}")
                return value
            logger.info(f"LINDEX {key} {index} -> (nil)")
            return "(nil)"

    def zadd(self, key, mapping):
        with self.lock:
            sorted_set = self.store.setdefault(key, {})
            for member, score in mapping.items():
                sorted_set[member] = score
            logger.info(f"ZADD {key} -> {mapping}")
            return "OK"

    def zremrangebyscore(self, key, min_score, max_score):
        with self.lock:
            sorted_set = self.store.get(key, {})
            to_remove = [member for member, score in sorted_set.items() if float(min_score) <= score <= float(max_score)]
            for member in to_remove:
                del sorted_set[member]
            logger.info(f"ZREMRANGEBYSCORE {key} {min_score} {max_score} -> Removed {len(to_remove)} members")
            return len(to_remove)

    def zcard(self, key):
        with self.lock:
            sorted_set = self.store.get(key, {})
            count = len(sorted_set)
            logger.info(f"ZCARD {key} -> {count}")
            return count
