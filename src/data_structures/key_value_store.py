class KeyValueStore:
    def __init__(self):
        self._store = {}
    
    def set(self, key, value):
        """Set a key-value pair in the store"""
        self._store[key] = value
    
    def get(self, key):
        """Retrieve a value by key"""
        return self._store.get(key)
    
    def delete(self, key):
        """Delete a key-value pair"""
        if key in self._store:
            del self._store[key]
    
    def exists(self, key):
        """Check if a key exists"""
        return key in self._store