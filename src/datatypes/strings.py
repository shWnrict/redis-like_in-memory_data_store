from src.datatypes.base import BaseDataType

class Strings(BaseDataType):
    """Implements string-specific operations for the data store."""

    def append(self, key, value):
        """Append a value to the existing string at key."""
        if not self._key_exists(key):
            self.set(key, value)
        else:
            current_value = self.get(key)
            if not isinstance(current_value, str):
                raise ValueError(f"Key '{key}' does not contain a string.")
            new_value = current_value + value
            self.set(key, new_value)
        return len(self.get(key))

    def strlen(self, key):
        """Get the length of the string at key."""
        value = self.get(key)
        if value is None:
            return 0
        if not isinstance(value, str):
            raise ValueError(f"Key '{key}' does not contain a string.")
        return len(value)

    def incr(self, key):
        """Increment the integer value of a key by 1."""
        return self.incrby(key, 1)

    def decr(self, key):
        """Decrement the integer value of a key by 1."""
        return self.decrby(key, 1)

    def incrby(self, key, amount):
        """Increment the integer value of a key by the given amount."""
        value = self.get(key)
        if value is None:
            value = 0
        if not isinstance(value, (int, str)) or not str(value).isdigit():
            raise ValueError(f"Key '{key}' does not contain an integer.")
        new_value = int(value) + amount
        self.set(key, str(new_value))
        return new_value

    def decrby(self, key, amount):
        """Decrement the integer value of a key by the given amount."""
        return self.incrby(key, -amount)

    def getrange(self, key, start, end):
        """Get a substring of the string value at key."""
        value = self.get(key)
        if value is None:
            return ""
        if not isinstance(value, str):
            raise ValueError(f"Key '{key}' does not contain a string.")
        return value[start:end + 1]

    def setrange(self, key, offset, value):
        """Overwrite part of the string at key starting at offset."""
        current_value = self.get(key) or ""
        if not isinstance(current_value, str):
            raise ValueError(f"Key '{key}' does not contain a string.")
        if offset < 0:
            raise ValueError("Offset cannot be negative.")
        new_value = current_value[:offset] + value + current_value[offset + len(value):]
        self.set(key, new_value)
        return len(new_value)
