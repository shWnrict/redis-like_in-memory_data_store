# core/database.py

from core.expiry import ExpiryManager
from core.transaction import TransactionManager
from core.persistence import AOFLogger
from datatypes.string import StringDataType
import threading
import time

class KeyValueStore:
    def __init__(self):
        self.store = {}
        self.expiry = {}
        self.expiry_manager = ExpiryManager(self)
        self.transaction_manager = TransactionManager(self)
        self.aof_logger = AOFLogger()
        self.string = StringDataType(self)  # Initialize string operations

        # Disable logging during replay
        self.replaying = True
        self.aof_logger.replay(self)
        self.replaying = False

        # Start background cleaner
        self.expiry_cleaner_thread = threading.Thread(target=self.expiry_manager.clean_expired_keys, daemon=True)
        self.expiry_cleaner_thread.start()

    def set(self, key, value):
        """Set a key-value pair and log the operation."""
        self.store[key] = value
        if key in self.expiry:
            del self.expiry[key]
        if not self.replaying:
            self.aof_logger.log_command(f"SET {key} {value}")

    def get(self, key):
        """Retrieve a key's value, considering expiry."""
        if key in self.expiry and self.expiry[key] <= time.time():
            self.delete(key)
            return None
        return self.store.get(key)

    def delete(self, key):
        """Delete a key and log the operation."""
        if key in self.store:
            del self.store[key]
            self.expiry.pop(key, None)
            if not self.replaying:
                self.aof_logger.log_command(f"DEL {key}")
            return True
        return False

    def exists(self, key):
        """Check if a key exists, considering expiry."""
        return key in self.store and not (key in self.expiry and self.expiry[key] <= time.time())

    def stop(self):
        """Stop the expiry manager and AOF logging."""
        self.expiry_manager.stop()
        self.aof_logger.close()
