import time

class BaseDataType:
    """Base class for all Redis-like data types."""
    
    def __init__(self):
        """Initialize the base data type with storage and metadata."""
        self.store = {}  # Internal storage for the data
        self.metadata = {}  # Metadata storage (e.g., TTL)

    # Core Methods
    def set(self, key, value):
        """Set a key-value pair."""
        self.store[key] = value
        self._clear_metadata(key)  # Clear TTL when value is updated
        return "OK"

    def get(self, key):
        """Get the value of a key, if it exists and has not expired."""
        if not self._key_exists(key):
            return None
        return self.store[key]

    def delete(self, key):
        """Delete a key from the data store."""
        if key in self.store:
            del self.store[key]
            self._clear_metadata(key)
            return True
        return False

    def exists(self, key):
        """Check if a key exists and has not expired."""
        return self._key_exists(key)

    # TTL Methods
    def set_ttl(self, key, ttl_seconds):
        """Set a TTL for a key."""
        if key not in self.store:
            raise KeyError(f"Key '{key}' does not exist.")
        expiry_time = time.time() + ttl_seconds
        self.metadata[key] = {"ttl": expiry_time}

    def get_ttl(self, key):
        """Get the remaining TTL for a key."""
        if key not in self.metadata or "ttl" not in self.metadata[key]:
            return -1  # No TTL set
        remaining = int(self.metadata[key]["ttl"] - time.time())
        return max(0, remaining)

    def persist(self, key):
        """Remove the TTL for a key."""
        if key in self.metadata:
            self.metadata[key].pop("ttl", None)
            if not self.metadata[key]:  # Remove key if no metadata is left
                del self.metadata[key]
            return True
        return False

    # Internal Utility Methods
    def _key_exists(self, key):
        """Check if a key exists and is not expired."""
        if key not in self.store:
            return False
        if key in self.metadata and "ttl" in self.metadata[key]:
            if time.time() >= self.metadata[key]["ttl"]:
                self.delete(key)  # Remove expired key
                return False
        return True

    def _clear_metadata(self, key):
        """Clear metadata for a key."""
        if key in self.metadata:
            del self.metadata[key]

    # Serialization/Deserialization Placeholder
    def serialize(self):
        """Serialize the data store for persistence (placeholder)."""
        pass

    def deserialize(self, data):
        """Deserialize the data store from persistence (placeholder)."""
        pass
