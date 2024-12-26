#src/core/keyspace_manager.py
import random
from threading import Lock
from src.core.data_store import KeyValueStore

class KeyspaceManager:
    """
    Manages multiple logical keyspaces (databases).
    """

    def __init__(self, num_databases=16):
        self.databases = [KeyValueStore() for _ in range(num_databases)]
        self.current_db = 0
        self.lock = Lock()

    def select(self, db_index):
        """
        Switch to a different database (keyspace).
        """
        with self.lock:
            if 0 <= db_index < len(self.databases):
                self.current_db = db_index
                return f"OK - Switched to database {db_index}"
            raise ValueError("Invalid database index.")

    def flushdb(self):
        """
        Clear all keys in the current database.
        """
        with self.lock:
            self.databases[self.current_db] = KeyValueStore()  # Reset the current database
            return "OK"

    def randomkey(self):
        """
        Retrieve a random key from the current database.
        """
        with self.lock:
            keys = list(self.databases[self.current_db].store.keys())
            if keys:
                return random.choice(keys)
            return "(nil)"  # Align with Redis convention

    def get_current_store(self):
        """
        Get the current database (keyspace).
        """
        with self.lock:
            return self.databases[self.current_db]

    def add_database(self):
        """
        Dynamically add a new database (keyspace).
        """
        with self.lock:
            self.databases.append(KeyValueStore())
            return f"OK - Added database {len(self.databases) - 1}"

    def remove_database(self, db_index):
        """
        Dynamically remove a database (keyspace).
        """
        with self.lock:
            if 0 <= db_index < len(self.databases):
                if db_index == self.current_db:
                    self.current_db = 0  # Reset to the first database if the current one is removed
                del self.databases[db_index]
                return f"OK - Removed database {db_index}"
            raise ValueError("Invalid database index.")
