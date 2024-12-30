class ListDataType:
    def __init__(self, database):
        self.db = database

    def _ensure_list(self, key):
        """Ensure the value at key is a list."""
        value = self.db.store.get(key)  # Direct store access to bypass type check
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
            for value in reversed(values):  # Reverse to match Redis behavior
                current.insert(0, str(value))
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
                current.append(str(value))
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
                current[index] = str(value)
                return "OK"
            return "ERR index out of range"
        except ValueError as e:
            return str(e)
