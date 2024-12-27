# src/core/expiry_manager.py
import threading
import time
from src.logger import setup_logger
from src.config import Config

logger = setup_logger("expiry_manager")

class ExpiryManager:
    def __init__(self, store, cleanup_interval=Config.CLEANUP_INTERVAL):
        self.expiry = {}
        self.store = store
        self.lock = threading.Lock()
        self.cleanup_interval = cleanup_interval
        self.running = False
        self.thread = threading.Thread(target=self.cleanup_expired_keys)
        self.thread.daemon = True

    def start(self):
        self.running = True
        self.thread.start()
        logger.info("ExpiryManager started.")

    def stop(self):
        self.running = False
        self.thread.join()
        logger.info("ExpiryManager stopped.")

    def set_expiry(self, key, ttl):
        with self.lock:
            self.expiry[key] = time.time() + ttl
            logger.info(f"Set expiry for key '{key}' to TTL={ttl} seconds.")

    def is_expired(self, key):
        with self.lock:
            if key in self.expiry:
                if time.time() > self.expiry[key]:
                    logger.info(f"Key '{key}' has expired.")
                    return True
            return False

    def remove_expired_key(self, key):
        with self.lock:
            if key in self.expiry:
                del self.expiry[key]
                if key in self.store:
                    del self.store[key]
                logger.info(f"Removed expired key '{key}'.")

    def cleanup_expired_keys(self):
        while self.running:
            time.sleep(self.cleanup_interval)
            with self.lock:
                keys_to_remove = [key for key, expiry_time in self.expiry.items() if time.time() > expiry_time]
            for key in keys_to_remove:
                self.remove_expired_key(key)

    def handle_expire(self, key, ttl):
        if key not in self.store:
            return 0
        self.set_expiry(key, ttl)
        return 1

    def handle_ttl(self, key):
        with self.lock:
            if key not in self.expiry:
                return -1
            ttl = int(self.expiry[key] - time.time())
            return ttl if ttl > 0 else -1

    def handle_persist(self, key):
        with self.lock:
            if key in self.expiry:
                del self.expiry[key]
                logger.info(f"Persisted key '{key}', removing expiry.")
                return 1
            return 0
