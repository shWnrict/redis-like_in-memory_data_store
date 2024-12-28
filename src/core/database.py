# core/database.py

from core.expiry import ExpiryManager
import threading
import time

class KeyValueStore:
    def __init__(self):
        self.store = {}
        self.expiry = {}
        self.expiry_manager = ExpiryManager(self)

        # Start background cleaner
        self.expiry_cleaner_thread = threading.Thread(target=self.expiry_manager.clean_expired_keys, daemon=True)
        self.expiry_cleaner_thread.start()

    def set(self, key, value):
        self.store[key] = value
        if key in self.expiry:
            del self.expiry[key]

    def get(self, key):
        if key in self.expiry and self.expiry[key] <= time.time():
            self.delete(key)
            return None
        return self.store.get(key)

    def delete(self, key):
        if key in self.store:
            del self.store[key]
            self.expiry.pop(key, None)
            return True
        return False

    def exists(self, key):
        return key in self.store and not (key in self.expiry and self.expiry[key] <= time.time())

    def stop(self):
        """Stop the expiry manager."""
        self.expiry_manager.stop()


# core/database.py

from core.transaction import TransactionManager

class KeyValueStore:
    def __init__(self):
        self.store = {}
        self.expiry = {}
        self.transaction_manager = TransactionManager(self)

    def set(self, key, value):
        self.store[key] = value
        if key in self.expiry:
            del self.expiry[key]

    def get(self, key):
        if key in self.expiry and self.expiry[key] <= time.time():
            self.delete(key)
            return None
        return self.store.get(key)

    def delete(self, key):
        if key in self.store:
            del self.store[key]
            self.expiry.pop(key, None)
            return True
        return False

    def exists(self, key):
        return key in self.store and not (key in self.expiry and self.expiry[key] <= time.time())
