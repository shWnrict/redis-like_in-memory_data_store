# core/database.py

from core.expiry import ExpiryManager
from core.transaction import TransactionManager
from core.persistence import PersistenceManager
from datatypes.string import StringDataType
from datatypes.list import ListDataType
from datatypes.set import SetDataType
from datatypes.hash import HashDataType
from datatypes.zset import ZSetDataType
from datatypes.advanced.stream import StreamDataType
from datatypes.advanced.geo import GeoDataType
from datatypes.bitmap import BitMapDataType
import threading
import time

class KeyValueStore:
    def __init__(self):
        self.store = {}
        self.expiry = {}
        self.expiry_manager = ExpiryManager(self)
        self.transaction_manager = TransactionManager(self)
        self.persistence_manager = PersistenceManager(self)
        self.string = StringDataType(self)  # Initialize string operations
        self.list = ListDataType(self)  # Initialize list operations
        self.sets = SetDataType(self)  # Rename from 'set' to 'sets' to avoid conflict
        self.hash = HashDataType(self)  # Initialize hash operations
        self.zset = ZSetDataType(self)  # Initialize sorted set operations
        self.stream = StreamDataType(self)  # Initialize stream operations
        self.geo = GeoDataType(self)  # Initialize geo operations
        self.bitmap = BitMapDataType(self)  # Initialize bitmap operations
        self.command_map = None  # Will be set by server

        # Disable logging during replay
        self.replaying = True
        self.persistence_manager.restore()
        self.replaying = False

        # Start background cleaner
        self.expiry_cleaner_thread = threading.Thread(target=self.expiry_manager.clean_expired_keys, daemon=True)
        self.expiry_cleaner_thread.start()

    def set_command_map(self, command_map):
        """Set the command map for transaction handling."""
        self.command_map = command_map

    def set(self, key, value):
        """Set a key-value pair and log the operation."""
        self.store[key] = value
        if key in self.expiry:
            del self.expiry[key]
        if not self.replaying:
            if isinstance(value, list):
                log_value = ' '.join(str(x) for x in value)
            elif isinstance(value, set):
                log_value = ' '.join(sorted(str(x) for x in value))  # Sort for consistent logging
            elif isinstance(value, dict):
                if 'entries' in value or 'points' in value:  # Stream or Geo type
                    return  # These types handle their own persistence
                log_value = ' '.join(f"{k} {v}" for k, v in sorted(value.items()))
            else:
                log_value = value
            self.persistence_manager.log_command(f"SET {key} {log_value}")

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
                self.persistence_manager.log_command(f"DEL {key}")
            return True
        return False

    def exists(self, key):
        """Check if a key exists, considering expiry."""
        return key in self.store and not (key in self.expiry and self.expiry[key] <= time.time())

    def stop(self):
        """Stop the managers and persistence."""
        self.expiry_manager.stop()
        self.persistence_manager.close()

    def get_snapshot(self):
        """Get current database state for replication."""
        return {
            'store': dict(self.store),
            'expiry': dict(self.expiry)
        }

    def restore_from_master(self, data):
        """Restore database state from master."""
        self.replaying = True  # Prevent logging during restore
        try:
            self.store = data['store']
            self.expiry = data['expiry']
        finally:
            self.replaying = False
