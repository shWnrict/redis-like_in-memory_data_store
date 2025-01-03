class StringDataType:
    """
    A class to represent string data type operations in an in-memory data store.
    Attributes:
    ----------
    database : object
        An instance of the database where the data is stored.
    Methods:
    -------
    append(key, value):
        Appends a value to the string stored at the specified key.
    strlen(key):
        Returns the length of the string stored at the specified key.
    incr(key):
        Increments the integer value of the specified key by 1.
    decr(key):
        Decrements the integer value of the specified key by 1.
    incrby(key, increment):
        Increments the integer value of the specified key by a given amount.
    decrby(key, decrement):
        Decrements the integer value of the specified key by a given amount.
    getrange(key, start, end):
        Returns a substring of the string stored at the specified key, based on the provided start and end indices.
    setrange(key, offset, value):
        Overwrites part of the string stored at the specified key, starting at the given offset.
    """
    def __init__(self, database):
        self.database = database

    def append(self, key, value):
        """Append a value to the string stored at key."""
        existing_value = self.database.get(key) or ""
        if not isinstance(existing_value, str):
            return "ERROR: Value at key is not a string"
        new_value = existing_value + value
        self.database.set(key, new_value)
        return len(new_value)

    def strlen(self, key):
        """Get the length of the string stored at key."""
        value = self.database.get(key)
        if value is None:
            return 0
        if not isinstance(value, str):
            return "ERROR: Value at key is not a string"
        return len(value)

    def incr(self, key):
        """Increment the integer value of a key by 1."""
        return self.incrby(key, 1)

    def decr(self, key):
        """Decrement the integer value of a key by 1."""
        return self.incrby(key, -1)

    def incrby(self, key, increment):
        """Increment the integer value of a key by a given amount."""
        try:
            value = int(self.database.get(key) or 0)
            new_value = value + increment
            self.database.set(key, str(new_value))
            return new_value
        except ValueError:
            return "ERROR: Value at key is not an integer"

    def decrby(self, key, decrement):
        """Decrement the integer value of a key by a given amount."""
        return self.incrby(key, -decrement)

    def getrange(self, key, start, end):
        """Get a substring of the string stored at key."""
        try:
            value = self.database.get(key)
            if value is None:
                return ""
            if not isinstance(value, str):
                return "ERROR: Value at key is not a string"
            
            start = int(start)
            end = int(end)
            
            # Handle negative indices
            if start < 0:
                start = len(value) + start
            if end < 0:
                end = len(value) + end
            
            end += 1
            
            # Ensure we don't go out of bounds
            start = max(0, start)
            end = min(len(value), end)
            
            return value[start:end]
        except ValueError:
            return ""

    def setrange(self, key, offset, value):
        """Overwrite part of the string stored at key."""
        try:
            offset = int(offset)
            if offset < 0:
                return "ERROR: Offset cannot be negative"
            
            current = self.database.get(key)
            if current is None:
                current = "\0" * offset  # Initialize with null bytes if key doesn't exist
            elif not isinstance(current, str):
                return "ERROR: Value at key is not a string"
            
            # Extend string with null bytes if needed
            if offset > len(current):
                current = current + "\0" * (offset - len(current))
            
            new_value = current[:offset] + str(value)
            if offset + len(str(value)) < len(current):
                new_value += current[offset + len(str(value)):]
                
            self.database.set(key, new_value)
            return len(new_value)
        except ValueError:
            return "ERROR: Invalid offset value"
