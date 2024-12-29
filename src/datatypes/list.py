class ListDataType:
    def __init__(self, database):
        self.db = database

    def _ensure_list(self, key):
        """Ensure the value at key is a list."""
        value = self.db.get(key)
        if value is None:
            # Initialize new list
            value = []
            self.db.store[key] = value  # Direct store access to avoid string conversion
            return value
        if not isinstance(value, list):
            raise ValueError("Value is not a list")
        return value

    def lpush(self, key, *values):
        """Push values to the head of the list."""
        if not values:
            return 0
        try:
            current = self._ensure_list(key)
            for value in values:
                current.insert(0, str(value))  # Convert value to string
            if not self.db.replaying:
                self.db.persistence_manager.log_command(f"LPUSH {key} {' '.join(map(str, values))}")
            self.db.set(key, current)
            return len(current)
        except ValueError:
            return 0

    def rpush(self, key, *values):
        """Push values to the tail of the list."""
        try:
            current = self._ensure_list(key)
            for value in values:
                current.append(str(value))  # Convert value to string
            if not self.db.replaying:
                self.db.persistence_manager.log_command(f"RPUSH {key} {' '.join(map(str, values))}")
            self.db.set(key, current)
            return len(current)
        except ValueError:
            return 0

    def lpop(self, key):
        """Remove and return the first element of the list."""
        try:
            current = self._ensure_list(key)
            if not current:
                return None
            value = current.pop(0)
            if not self.db.replaying:
                self.db.persistence_manager.log_command(f"LPOP {key}")
            self.db.set(key, current)
            return value
        except ValueError:
            return None

    def rpop(self, key):
        """Remove and return the last element of the list."""
        try:
            current = self._ensure_list(key)
            if not current:
                return None
            value = current.pop()
            if not self.db.replaying:
                self.db.persistence_manager.log_command(f"RPOP {key}")
            self.db.set(key, current)
            return value
        except ValueError:
            return None

    def lrange(self, key, start, stop):
        """Get a range of elements from the list."""
        try:
            current = self._ensure_list(key)
            start = max(0, start if start >= 0 else len(current) + start)
            stop = min(len(current), stop if stop >= 0 else len(current) + stop + 1)
            return current[start:stop]
        except ValueError:
            return []

    def lindex(self, key, index):
        """Get an element from the list by its index."""
        try:
            current = self._ensure_list(key)
            if 0 <= index < len(current) or -len(current) <= index < 0:
                return current[index]
            return None
        except ValueError:
            return None

    def lset(self, key, index, value):
        """Set the list element at index to value."""
        try:
            current = self._ensure_list(key)
            if 0 <= index < len(current) or -len(current) <= index < 0:
                current[index] = str(value)
                if not self.db.replaying:
                    self.db.persistence_manager.log_command(f"LSET {key} {index} {value}")
                self.db.set(key, current)
                return True
            return False
        except ValueError:
            return False
