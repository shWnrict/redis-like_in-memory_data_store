# Base class for all data types
# datatypes/base.py

class BaseDataType:
    def __init__(self):
        self.data = {}

    def set(self, key, value):
        """Set a key-value pair."""
        self.data[key] = value
        return "OK"

    def get(self, key):
        """Get the value of a key."""
        return self.data.get(key)

    def delete(self, key):
        """Delete a key."""
        return self.data.pop(key, None) is not None
