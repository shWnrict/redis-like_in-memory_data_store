#src/core/keyspace_manager.py
import random
from threading import Lock
from src.core.data_store import KeyValueStore
from src.logger import setup_logger  # Import setup_logger
from typing import Dict, Set, Optional, List
import threading
import re

logger = setup_logger("keyspace_manager")  # Initialize logger

class KeyspaceManager:
    """
    Manages multiple logical keyspaces (databases).
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.databases: Dict[int, Dict] = {0: {}}
        self.current_db = 0
        self.key_patterns: Dict[str, Set[str]] = {}  # For pattern matching
        self.db_sizes: Dict[int, int] = {0: 0}

    def select_db(self, db_index: int) -> bool:
        """Select a database"""
        with self.lock:
            if db_index < 0:
                return False
            if db_index not in self.databases:
                self.databases[db_index] = {}
                self.db_sizes[db_index] = 0
            self.current_db = db_index
            logger.info(f"Selected database {db_index}")
            return True

    def flush_db(self, db_index: Optional[int] = None) -> bool:
        """Clear specified database or current if none specified"""
        with self.lock:
            target_db = db_index if db_index is not None else self.current_db
            if target_db in self.databases:
                self.databases[target_db].clear()
                self.db_sizes[target_db] = 0
                logger.info(f"Flushed database {target_db}")
                return True
            return False

    def flush_all(self) -> bool:
        """Clear all databases"""
        with self.lock:
            self.databases.clear()
            self.db_sizes.clear()
            self.databases[0] = {}
            self.db_sizes[0] = 0
            self.current_db = 0
            logger.info("Flushed all databases")
            return True

    def get_random_key(self, db_index: Optional[int] = None) -> Optional[str]:
        """Get random key from specified database or current if none specified"""
        import random
        with self.lock:
            target_db = db_index if db_index is not None else self.current_db
            if target_db in self.databases and self.databases[target_db]:
                return random.choice(list(self.databases[target_db].keys()))
            return None

    def scan(self, cursor: int, match_pattern: Optional[str] = None, count: int = 10) -> tuple[int, List[str]]:
        """Incrementally iterate over keys"""
        with self.lock:
            all_keys = list(self.databases[self.current_db].keys())
            if match_pattern:
                pattern = re.compile(match_pattern.replace("*", ".*"))
                all_keys = [k for k in all_keys if pattern.match(k)]
            
            start_idx = min(cursor, len(all_keys))
            end_idx = min(start_idx + count, len(all_keys))
            next_cursor = end_idx if end_idx < len(all_keys) else 0
            
            return next_cursor, all_keys[start_idx:end_idx]

    def get_db_size(self, db_index: Optional[int] = None) -> int:
        """Get number of keys in specified database or current if none specified"""
        with self.lock:
            target_db = db_index if db_index is not None else self.current_db
            return self.db_sizes.get(target_db, 0)

    def track_key_pattern(self, pattern: str, key: str):
        """Track key for pattern matching notifications"""
        with self.lock:
            if pattern not in self.key_patterns:
                self.key_patterns[pattern] = set()
            self.key_patterns[pattern].add(key)

    def untrack_key_pattern(self, pattern: str, key: str):
        """Remove key from pattern tracking"""
        with self.lock:
            if pattern in self.key_patterns:
                self.key_patterns[pattern].discard(key)
                if not self.key_patterns[pattern]:
                    del self.key_patterns[pattern]

    def notify_key_change(self, key: str, operation: str):
        """Notify subscribers about key changes"""
        with self.lock:
            for pattern, keys in self.key_patterns.items():
                if key in keys:
                    logger.info(f"Key {key} {operation}: pattern {pattern}")
                    # Notification logic here
