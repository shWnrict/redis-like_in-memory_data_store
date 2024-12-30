# Base class for all data types that repesents simple in-memory key-value store.
# datatypes/base.py

class BaseDataType:
    """
    BaseDataType is a simple in-memory data store that mimics basic functionalities of a key-value store.
    Attributes:
        data (dict): A dictionary to store key-value pairs.
    Methods:
        set(key, value):
            Sets a key-value pair in the data store.
        get(key):
            Retrieves the value associated with the given key.
        delete(key):
            Deletes the key-value pair from the data store.
    """
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
