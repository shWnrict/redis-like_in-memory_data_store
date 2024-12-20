import time
import threading
import heapq

class ExpiryManager:
    """Manages expiration for keys in the KeyValueStore."""

    def __init__(self, kv_store):
        self.kv_store = kv_store  # Reference to the KeyValueStore
        self.expiry_heap = []  # Min-heap for TTL management
        self.lock = threading.Lock()  # Thread-safe operations
        self.running = True
        self.cleanup_thread = threading.Thread(target=self._background_cleanup)
        self.cleanup_thread.daemon = True
        self.cleanup_thread.start()

    def set_expiry(self, key, ttl_seconds):
        """Set an expiry for a key."""
        with self.lock:
            expiry_time = time.time() + ttl_seconds
            heapq.heappush(self.expiry_heap, (expiry_time, key))

    def clear_expiry(self, key):
        """Clear expiry for a key."""
        with self.lock:
            self.expiry_heap = [(t, k) for t, k in self.expiry_heap if k != key]
            heapq.heapify(self.expiry_heap)

    def is_expired(self, key):
        """Check if a key has expired."""
        with self.lock:
            current_time = time.time()
            while self.expiry_heap and self.expiry_heap[0][0] <= current_time:
                _, expired_key = heapq.heappop(self.expiry_heap)
                if expired_key in self.kv_store.store:
                    del self.kv_store.store[expired_key]
            return False

    def stop(self):
        """Stop the background cleanup thread."""
        self.running = False
        self.cleanup_thread.join()

    def _background_cleanup(self):
        """Background thread to remove expired keys."""
        while self.running:
            with self.lock:
                self.is_expired(None)  # Trigger cleanup
            time.sleep(1)  # Run cleanup every second

    # TTL Commands
    def ttl(self, key):
        """Get the remaining TTL for a key."""
        with self.lock:
            for expiry_time, k in self.expiry_heap:
                if k == key:
                    remaining = max(0, int(expiry_time - time.time()))
                    return remaining
        return -1  # Key does not exist or has no TTL

    def persist(self, key):
        """Remove the TTL for a key."""
        self.clear_expiry(key)
        return 1 if key in self.kv_store.store else 0
