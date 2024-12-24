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