# src/core/data_store.py
import threading
import time
from src.logger import setup_logger
from src.config import Config

logger = setup_logger("data_store", level=Config.LOG_LEVEL)

class DataStore:
    def __init__(self):
        self.store = {}
        self.lock = threading.Lock()

    def set(self, key, value):
        with self.lock:
            self.store[key] = value
            logger.info(f"SET {key} -> {value}")
            return "OK"

    def get(self, key):
        with self.lock:
            value = self.store.get(key)
            logger.info(f"GET {key} -> {value}")
            return value if value is not None else "(nil)"

    def delete(self, key):
        with self.lock:
            if key in self.store:
                del self.store[key]
                logger.info(f"DEL {key}")
                return 1
            return 0

    def exists(self, key):
        with self.lock:
            exists = key in self.store
            logger.info(f"EXISTS {key} -> {exists}")
            return exists


# src/core/memory_manager.py
import sys

class MemoryManager:
    def __init__(self, max_memory=Config.DATA_LIMIT):
        self.max_memory = max_memory
        self.current_memory = 0

    def calculate_size(self, value):
        return sys.getsizeof(value)

    def can_store(self, value):
        return self.current_memory + self.calculate_size(value) <= self.max_memory

    def add(self, value):
        size = self.calculate_size(value)
        if not self.can_store(value):
            raise MemoryError("Not enough memory to store the value")
        self.current_memory += size

    def remove(self, value):
        self.current_memory -= self.calculate_size(value)


# src/core/expiry_manager.py
class ExpiryManager:
    def __init__(self, store):
        self.expiry = {}
        self.store = store
        self.lock = threading.Lock()
        self.cleanup_thread = threading.Thread(target=self.cleanup_expired_keys, daemon=True)
        self.cleanup_thread.start()

    def set_expiry(self, key, ttl):
        with self.lock:
            self.expiry[key] = time.time() + ttl

    def is_expired(self, key):
        with self.lock:
            if key not in self.expiry:
                return False
            if time.time() > self.expiry[key]:
                self.remove_expired_key(key)
                return True
            return False

    def remove_expired_key(self, key):
        with self.lock:
            if key in self.expiry:
                del self.expiry[key]
            if key in self.store.store:
                del self.store.store[key]

    def cleanup_expired_keys(self):
        while True:
            with self.lock:
                keys_to_remove = [key for key, expiry in self.expiry.items() if time.time() > expiry]
            for key in keys_to_remove:
                self.remove_expired_key(key)
            time.sleep(1)  # Cleanup interval
