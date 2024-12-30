class ListDataType:
    """
    ListDataType is a class that provides list-like operations for an in-memory data store.
    It ensures that the values associated with keys are lists and provides methods to manipulate these lists.
    Methods:
        __init__(database):
            Initializes the ListDataType with a reference to the database.
        _ensure_list(key):
            Ensures that the value at the given key is a list. If the key does not exist, it initializes it with an empty list.
            Raises a ValueError if the value at the key is not a list.
        lpush(key, *values):
            Pushes values to the head of the list at the given key. Returns the length of the list after the operation.
        rpush(key, *values):
            Pushes values to the tail of the list at the given key. Returns the length of the list after the operation.
        lpop(key):
            Removes and returns the first element of the list at the given key. If the list becomes empty, the key is deleted.
        rpop(key):
            Removes and returns the last element of the list at the given key. If the list becomes empty, the key is deleted.
        lrange(key, start, stop):
            Returns a range of elements from the list at the given key, from the start index to the stop index (inclusive).
            Handles negative indices and ensures bounds. If the value at the key is not a list or indices are invalid, returns an error message.
        lindex(key, index):
            Returns the element at the given index from the list at the given key. Handles negative indices.
        lset(key, index, value):
            Sets the element at the given index to the specified value in the list at the given key. Handles negative indices.
    """
    def __init__(self, database):
        self.db = database

    def _ensure_list(self, key):
        """Ensure the value at key is a list."""
        value = self.db.store.get(key) 
        if value is None:
            value = []
            self.db.store[key] = value
            return value
        if not isinstance(value, list):
            raise ValueError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return value

    def lpush(self, key, *values):
        """Push values to the head of the list."""
        if not values:
            return 0
        try:
            current = self._ensure_list(key)
            for value in reversed(values):
                current.insert(0, value)  # Store as-is without str() conversion
            return len(current)
        except ValueError as e:
            return str(e)

    def rpush(self, key, *values):
        """Push values to the tail of the list."""
        if not values:
            return 0
        try:
            current = self._ensure_list(key)
            for value in values:
                current.append(value)  # Store as-is without str() conversion
            return len(current)
        except ValueError as e:
            return str(e)

    def lpop(self, key):
        """Remove and return the first element of the list."""
        try:
            current = self._ensure_list(key)
            if not current:
                return None
            value = current.pop(0)
            if len(current) == 0:
                self.db.delete(key)  # Delete key if list becomes empty
            return value
        except ValueError as e:
            return str(e)

    def rpop(self, key):
        """Remove and return the last element of the list."""
        try:
            current = self._ensure_list(key)
            if not current:
                return None
            value = current.pop()
            if len(current) == 0:
                self.db.delete(key)  # Delete key if list becomes empty
            return value
        except ValueError as e:
            return str(e)

    def lrange(self, key, start, stop):
        """Get a range of elements from the list."""
        try:
            current = self._ensure_list(key)
            start = int(start)
            stop = int(stop)
            
            # Handle negative indices
            length = len(current)
            if start < 0:
                start = length + start
            if stop < 0:
                stop = length + stop
            
            # Adjust indices to include stop
            stop += 1
            
            # Ensure bounds
            start = max(0, start)
            stop = min(length, stop)
            
            # Return empty list if invalid range
            if start > stop or start >= length:
                return []
                
            return current[start:stop]
        except (ValueError, TypeError):
            return "ERR value is not an integer or out of range"

    def lindex(self, key, index):
        """Get an element from the list by its index."""
        try:
            current = self._ensure_list(key)
            index = int(index)
            # Handle negative indices
            if index < 0:
                index = len(current) + index
            if 0 <= index < len(current):
                return current[index]
            return None
        except ValueError as e:
            return str(e)

    def lset(self, key, index, value):
        """Set the list element at index to value."""
        try:
            current = self._ensure_list(key)
            index = int(index)
            # Handle negative indices
            if index < 0:
                index = len(current) + index
            if 0 <= index < len(current):
                current[index] = value  # Store as-is without str() conversion
                return "OK"
            return "ERR index out of range"
        except ValueError as e:
            return str(e)
