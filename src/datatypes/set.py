class SetDataType:
    def __init__(self, database):
        self.db = database

    def _ensure_set(self, key):
        """Ensure the value at key is a set."""
        if not self.db.exists(key):
            self.db.store[key] = set()  # Direct store access to avoid conversion
            return self.db.store[key]
        value = self.db.get(key)
        if not isinstance(value, set):
            raise ValueError("Value is not a set")
        return value

    def sadd(self, key, *members):
        """Add members to set stored at key."""
        try:
            current = self._ensure_set(key)
            count = 0
            for member in members:
                if member not in current:
                    current.add(str(member))
                    count += 1
            if count > 0:
                self.db.store[key] = current
            return count
        except ValueError:
            return 0

    def srem(self, key, *members):
        """Remove members from set stored at key."""
        try:
            current = self._ensure_set(key)
            count = 0
            for member in members:
                if member in current:
                    current.remove(member)
                    count += 1
            if count > 0:
                self.db.store[key] = current
            return count
        except ValueError:
            return 0

    def sismember(self, key, member):
        """Return if member is a member of set stored at key."""
        try:
            current = self._ensure_set(key)
            return member in current
        except ValueError:
            return False

    def smembers(self, key):
        """Return all members of set stored at key."""
        try:
            return sorted(self._ensure_set(key))
        except ValueError:
            return []

    def sinter(self, *keys):
        """Return intersection of all sets specified by keys."""
        try:
            sets = [self._ensure_set(key) for key in keys]
            if not sets:
                return []
            return sorted(sets[0].intersection(*sets[1:]))
        except ValueError:
            return []

    def sunion(self, *keys):
        """Return union of all sets specified by keys."""
        try:
            sets = [self._ensure_set(key) for key in keys]
            if not sets:
                return []
            return sorted(sets[0].union(*sets[1:]))
        except ValueError:
            return []

    def sdiff(self, *keys):
        """Return difference between first set and all successive sets."""
        try:
            if not keys:
                return []
            sets = [self._ensure_set(key) for key in keys]
            return sorted(sets[0].difference(*sets[1:]))
        except ValueError:
            return []
