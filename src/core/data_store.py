# src/core/data_store.py
from threading import Lock
import time
import random
from typing import Dict, Any, Optional, List
from src.logger import setup_logger
from collections import OrderedDict

logger = setup_logger("data_store")

class HashTable:
    """Custom hash table implementation for better control"""
    def __init__(self, size=1024):
        self.size = size
        self.buckets = [[] for _ in range(size)]
        self.count = 0
        
    def _hash(self, key: str) -> int:
        """FNV-1a hash function"""
        h = 14695981039346656037
        for byte in key.encode():
            h = h ^ byte
            h = h * 1099511628211
        return h % self.size
    
    def get(self, key: str) -> Optional[Any]:
        bucket = self._hash(key)
        for k, v in self.buckets[bucket]:
            if k == key:
                return v
        return None

    def put(self, key: str, value: Any) -> None:
        bucket = self._hash(key)
        for i, (k, _) in enumerate(self.buckets[bucket]):
            if k == key:
                self.buckets[bucket][i] = (key, value)
                return
        self.buckets[bucket].append((key, value))
        self.count += 1
        
        # Resize if load factor exceeds 0.75
        if self.count > self.size * 0.75:
            self._resize()

    def delete(self, key: str) -> bool:
        bucket = self._hash(key)
        for i, (k, _) in enumerate(self.buckets[bucket]):
            if k == key:
                self.buckets[bucket].pop(i)
                self.count -= 1
                return True
        return False

    def _resize(self):
        """Double the hash table size when load factor is too high"""
        old_buckets = self.buckets
        self.size *= 2
        self.buckets = [[] for _ in range(self.size)]
        self.count = 0
        for bucket in old_buckets:
            for key, value in bucket:
                self.put(key, value)

class DataStore:
    def __init__(self):
        self.store = HashTable()
        self.expiry: Dict[str, float] = {}
        self.lock = Lock()
        self.access_time = OrderedDict()  # For LRU
        self.access_count = {}  # For LFU
        self.databases = {0: HashTable()}  # Multiple database support
        self.current_db = 0

    def select(self, db_index: int) -> bool:
        """Select a database"""
        with self.lock:
            if db_index < 0:
                return False
            if db_index not in self.databases:
                self.databases[db_index] = HashTable()
            self.current_db = db_index
            return True

    def flushdb(self) -> bool:
        """Clear current database"""
        with self.lock:
            self.databases[self.current_db] = HashTable()
            return True

    def randomkey(self) -> Optional[str]:
        """Get a random key from current database"""
        with self.lock:
            store = self.databases[self.current_db]
            non_empty_buckets = [b for b in store.buckets if b]
            if not non_empty_buckets:
                return None
            bucket = random.choice(non_empty_buckets)
            return random.choice(bucket)[0]

    def set(self, key: str, value: Any, ex: Optional[int] = None) -> str:
        """Set key with optional expiration"""
        with self.lock:
            store = self.databases[self.current_db]
            store.put(key, value)
            if ex is not None:
                self.expiry[key] = time.time() + ex
            self._record_access(key)
            return "OK"

    def get(self, key: str) -> Optional[Any]:
        """Get value checking for expiration"""
        with self.lock:
            if self._is_expired(key):
                return None
            store = self.databases[self.current_db]
            value = store.get(key)
            if value is not None:
                self._record_access(key)
            return value

    def delete(self, key: str) -> int:
        """Delete key and associated data"""
        with self.lock:
            store = self.databases[self.current_db]
            if store.delete(key):
                self.expiry.pop(key, None)
                self.access_time.pop(key, None)
                self.access_count.pop(key, None)
                return 1
            return 0

    def exists(self, key: str) -> int:
        """Check if key exists and not expired"""
        with self.lock:
            if self._is_expired(key):
                return 0
            store = self.databases[self.current_db]
            return 1 if store.get(key) is not None else 0

    def _is_expired(self, key: str) -> bool:
        """Check and handle expired keys"""
        if key in self.expiry and time.time() > self.expiry[key]:
            self.delete(key)
            return True
        return False

    def _record_access(self, key: str):
        """Record access for eviction policies"""
        current_time = time.time()
        self.access_time[key] = current_time
        self.access_count[key] = self.access_count.get(key, 0) + 1

    def evict_lru(self):
        """Evict least recently used key"""
        with self.lock:
            if not self.access_time:
                return
            key = next(iter(self.access_time))
            self._remove_key(key)
            logger.info(f"LRU eviction: removed key {key}")

    def evict_lfu(self):
        """Evict least frequently used key"""
        with self.lock:
            if not self.access_count:
                return
            key = min(self.access_count.items(), key=lambda x: x[1])[0]
            self._remove_key(key)
            logger.info(f"LFU eviction: removed key {key}")

    def _remove_key(self, key):
        """Remove key and its metadata"""
        store = self.databases[self.current_db]
        store.delete(key)
        if key in self.expiry:
            del self.expiry[key]
        if key in self.access_time:
            del self.access_time[key]
        if key in self.access_count:
            del self.access_count[key]
