# src/core/data_store.py
from threading import Lock
import time
from src.logger import setup_logger
from src.config import Config

logger = setup_logger("data_store", level=Config.LOG_LEVEL)

class DataStore:
    def __init__(self):
        self.store = {}
        self.expiry = {}  # Store expiry timestamps for keys
        self.lock = Lock()

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
        with self.lock:
            self.store[key] = value
            if ttl:
                self.expiry[key] = time.time() + ttl
            logger.info(f"SET {key} -> {value} (TTL={ttl})")
            return "OK"

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
                return 1
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
