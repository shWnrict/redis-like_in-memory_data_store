import random
from src.core.data_store import KeyValueStore

class KeyspaceManager:
    """Manages multiple logical keyspaces (databases)."""

    def __init__(self, num_databases=16):
        self.databases = [KeyValueStore() for _ in range(num_databases)]
        self.current_db = 0

    def select(self, db_index):
        """Switch to a different database (keyspace)."""
        if 0 <= db_index < len(self.databases):
            self.current_db = db_index
            return f"OK - Switched to database {db_index}"
        else:
            raise ValueError("Invalid database index.")

    def flushdb(self):
        """Clear all keys in the current database."""
        self.databases[self.current_db] = KeyValueStore()  # Reset the current database
        return "OK"

    def randomkey(self):
        """Retrieve a random key from the current database."""
        keys = list(self.databases[self.current_db].store.keys())
        if keys:
            return random.choice(keys)
        return None

    def get_current_store(self):
        """Get the current database (keyspace)."""
        return self.databases[self.current_db]
