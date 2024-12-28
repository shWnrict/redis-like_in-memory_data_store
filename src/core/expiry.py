# core/expiry.py

import time
import threading

class ExpiryManager:
    def __init__(self, database):
        self.database = database
        self.running = True

    def set_expiry(self, key, ttl):
        """Set expiration time for a key."""
        if self.database.exists(key):
            self.database.expiry[key] = time.time() + ttl
            return True
        return False

    def ttl(self, key):
        """Get time-to-live for a key."""
        if key not in self.database.expiry:
            return -1
        ttl = self.database.expiry[key] - time.time()
        return int(ttl) if ttl > 0 else -2

    def persist(self, key):
        """Remove the expiration time from a key."""
        return self.database.expiry.pop(key, None) is not None

    def clean_expired_keys(self):
        """Background task to remove expired keys."""
        while self.running:
            now = time.time()
            expired_keys = [key for key, exp_time in self.database.expiry.items() if exp_time <= now]
            for key in expired_keys:
                self.database.delete(key)
            time.sleep(1)

    def stop(self):
        """Stop the background cleaner."""
        self.running = False
