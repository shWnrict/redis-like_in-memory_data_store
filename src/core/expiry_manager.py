# src/core/expiry_manager.py
import threading
import time
from src.logger import setup_logger
from src.config import Config

logger = setup_logger("expiry_manager")

class ExpiryManager:
    def __init__(self, data_store):
        self.data_store = data_store
        self.running = False
        self.thread = threading.Thread(target=self.manage_expirations)
        self.thread.daemon = True
        self.logger = setup_logger("expiry_manager")

    def start(self):
        self.running = True
        self.thread.start()
        self.logger.info("ExpiryManager started.")

    def stop(self):
        self.running = False
        self.thread.join()
        self.logger.info("ExpiryManager stopped.")

    def manage_expirations(self):
        while self.running:
            keys_to_expire = []
            with self.data_store.lock:
                current_time = time.time()
                for key, expiry in list(self.data_store.expiry.items()):
                    if current_time > expiry:
                        keys_to_expire.append(key)
                for key in keys_to_expire:
                    del self.data_store.store[key]
                    del self.data_store.expiry[key]
                    self.logger.info(f"Key '{key}' expired and removed.")
            time.sleep(1)  # Check every second

    def handle_expire(self, key, ttl):
        """
        Set a TTL for a key.
        """
        with self.data_store.lock:
            if key in self.data_store.store:
                self.data_store.expiry[key] = time.time() + ttl
                return 1
            return 0

    def handle_ttl(self, key):
        """
        Get the TTL of a key.
        """
        with self.data_store.lock:
            if key in self.data_store.expiry:
                ttl = int(self.data_store.expiry[key] - time.time())
                return ttl if ttl > 0 else -2  # -2 if key does not exist
            return -1  # -1 if key exists but has no associated expire

    def handle_persist(self, key):
        """
        Remove the expiration from a key.
        """
        with self.data_store.lock:
            if key in self.data_store.expiry:
                del self.data_store.expiry[key]
                return 1
            return 0
