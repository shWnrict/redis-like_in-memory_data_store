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
from datatypes.advanced.bitmap import BitMapDataType
from datatypes.advanced.bitfield import BitFieldDataType
from datatypes.advanced.probabilistic import ProbabilisticDataType
from datatypes.advanced.timeseries import TimeSeriesDataType
from datatypes.advanced.json import JSONDataType
import threading
import time

class KeyValueStore:
    """
    KeyValueStore is an in-memory key-value database that supports various data types and operations, similar to Redis. 
    It includes features such as persistence, transactions, and expiry management. 
    
    Methods:
        set_command_map(command_map): Sets the command map for transaction handling.
        set(key, value): Sets a key-value pair and logs the operation.
        get(key): Retrieves a key's value, considering expiry.
        delete(key): Deletes a key and logs the operation.
        exists(key): Checks if a key exists, considering expiry.
        flush(): Clears all keys from the database.
        stop(): Stops the managers and persistence.
        get_snapshot(): Gets the current database state for replication.
        restore_from_master(data): Restores the database state from a master.
    """
    def __init__(self):
        self.store = {}
        self.expiry = {}
        self.expiry_manager = ExpiryManager(self)
        self.transaction_manager = TransactionManager(self)
        self.persistence_manager = PersistenceManager(self)
        #initialize operations
        self.string = StringDataType(self) 
        self.list = ListDataType(self)  
        self.sets = SetDataType(self) 
        self.hash = HashDataType(self)  
        self.zset = ZSetDataType(self) 
        self.stream = StreamDataType(self) 
        self.geo = GeoDataType(self)  
        self.bitmap = BitMapDataType(self)  
        self.bitfield = BitFieldDataType(self) 
        self.probabilistic = ProbabilisticDataType(self) 
        self.timeseries = TimeSeriesDataType(self) 
        self.json = JSONDataType(self)
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
                if ('entries' in value or 'points' in value or  # For Stream/Geo
                    isinstance(value.get('points'), dict)):  # For JSON objects
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
            
        value = self.store.get(key)
        if value is None:
            return None
        # Return type error if trying to GET a non-string value
        if isinstance(value, (list, dict, set)):
            return "WRONGTYPE Operation against a key holding the wrong kind of value"
        return value

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

    def flush(self):
        """Clear all keys from the database."""
        self.store.clear()
        self.expiry.clear()
        if not self.replaying:
            self.persistence_manager.log_command("FLUSHDB")

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
