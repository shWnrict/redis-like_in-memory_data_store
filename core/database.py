# core/database.py

class KeyValueStore:
    def __init__(self):
        self.store = {}

    def set(self, key, value):
        """Store a key-value pair."""
        self.store[key] = value

    def get(self, key):
        """Retrieve a value by key."""
        return self.store.get(key)

    def delete(self, key):
        """Delete a key from the store."""
        return self.store.pop(key, None) is not None

    def exists(self, key):
        """Check if a key exists in the store."""
        return key in self.store
