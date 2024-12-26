# src/core/expiry_manager.py
import threading
import time
from src.logger import setup_logger

logger = setup_logger("expiry_manager")

class ExpiryManager:
    def __init__(self, store, cleanup_interval=1):
        self.expiry = {}
        self.store = store
        self.lock = threading.Lock()
        self.cleanup_interval = cleanup_interval
        self.cleanup_thread = threading.Thread(target=self.cleanup_expired_keys, daemon=True)
        self.cleanup_thread.start()

    def set_expiry(self, key, ttl):
        """
        Set a TTL (time-to-live) for a key.
        """
        expiry_time = time.time() + ttl
        with self.lock:
            self.expiry[key] = expiry_time
        logger.info(f"Set expiry for key '{key}' to {expiry_time}")

    def is_expired(self, key):
        """
        Check if a key is expired. Removes the key if expired.
        """
        with self.lock:
            if key not in self.expiry:
                return False
            if time.time() > self.expiry[key]:
                logger.info(f"Key '{key}' has expired.")
                self.remove_expired_key(key)
                return True
        return False

    def remove_expired_key(self, key):
        """
        Remove the key from both expiry and the store if it exists.
        """
        with self.lock:
            self.expiry.pop(key, None)
            if key in self.store.store:
                del self.store.store[key]
                logger.info(f"Removed expired key '{key}' from store.")

    def cleanup_expired_keys(self):
        """
        Background task to periodically clean up expired keys.
        """
        while True:
            try:
                now = time.time()
                with self.lock:
                    keys_to_remove = [key for key, expiry in self.expiry.items() if now > expiry]
                for key in keys_to_remove:
                    self.remove_expired_key(key)
                time.sleep(self.cleanup_interval)
            except Exception as e:
                logger.error(f"Error in cleanup thread: {e}")
