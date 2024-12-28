# src/datatypes/base.py

import threading
from abc import ABC, abstractmethod
from src.logger import setup_logger

logger = setup_logger("base_datatype")

class BaseDataType(ABC):
    def __init__(self, store, expiry_manager=None):
        self.store = store
        self.expiry_manager = expiry_manager
        self.lock = threading.Lock()

    def validate_key(self, key, expected_type):
        with self.lock:
            if key in self.store:
                if not isinstance(self.store[key], expected_type):
                    logger.error(f"ERR Operation against a key holding the wrong kind of value")
                    return False
            return True

    def handle_ttl(self, key):
        if self.expiry_manager:
            return self.expiry_manager.handle_ttl(key)
        return -1

    def handle_persist(self, key):
        if self.expiry_manager:
            return self.expiry_manager.handle_persist(key)
        return 0

    @abstractmethod
    def handle_command(self, cmd, store, *args):
        pass

    # ...other common methods...