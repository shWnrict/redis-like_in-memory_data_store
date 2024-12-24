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
