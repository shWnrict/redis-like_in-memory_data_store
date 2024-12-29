class HashDataType:
    def __init__(self, database):
        self.db = database

    def _ensure_hash(self, key):
        """Ensure the value at key is a hash (dict)."""
        value = self.db.get(key)
        if value is None:
            value = {}
            self.db.store[key] = value
            return value
        if not isinstance(value, dict):
            raise ValueError("Value is not a hash")
        return value

    def hset(self, key, field, value):
        """Set field in hash stored at key to value."""
        try:
            current = self._ensure_hash(key)
            is_new = field not in current
            current[field] = str(value)
            if not self.db.replaying:
                self.db.persistence_manager.log_command(f"HSET {key} {field} {value}")
            return 1 if is_new else 0
        except ValueError:
            return 0

    def hmset(self, key, mapping):
        """Set multiple field-value pairs in hash stored at key."""
        try:
            current = self._ensure_hash(key)
            current.update({k: str(v) for k, v in mapping.items()})
            if not self.db.replaying:
                fields_values = ' '.join(f"{k} {v}" for k, v in mapping.items())
                self.db.persistence_manager.log_command(f"HMSET {key} {fields_values}")
            return True
        except ValueError:
            return False

    def hget(self, key, field):
        """Get value of field in hash stored at key."""
        try:
            current = self._ensure_hash(key)
            return current.get(field)
        except ValueError:
            return None

    def hgetall(self, key):
        """Get all field-value pairs in hash stored at key."""
        try:
            current = self._ensure_hash(key)
            # Return flattened list of alternating fields and values
            result = []
            for field in sorted(current.keys()):  # Sort for consistent ordering
                result.extend([field, current[field]])
            return result
        except ValueError:
            return []

    def hdel(self, key, *fields):
        """Delete fields from hash stored at key."""
        try:
            current = self._ensure_hash(key)
            count = 0
            for field in fields:
                if field in current:
                    del current[field]
                    count += 1
            if count and not self.db.replaying:
                self.db.persistence_manager.log_command(f"HDEL {key} {' '.join(fields)}")
            return count
        except ValueError:
            return 0

    def hexists(self, key, field):
        """Check if field exists in hash stored at key."""
        try:
            current = self._ensure_hash(key)
            return field in current
        except ValueError:
            return False
