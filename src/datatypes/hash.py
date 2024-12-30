from collections import OrderedDict

class HashDataType:
    """
    HashDataType is a class that provides a Redis-like in-memory data store for hash data structures.
    It allows for storing, retrieving, and manipulating hash data types, which are essentially dictionaries.
    Attributes:
        db (Database): The database instance where the hash data is stored.
    Methods:
        _create_hash(key):
            Create a new hash at the specified key.
        _validate_hash(key):
            Validate if the key exists and holds a hash. Raises ValueError if the key holds a different type.
        hset(key, field, value):
            Set the field in the hash stored at the key to the specified value. Returns 1 if the field is new, 0 if it was updated.
        hget(key, field):
            Get the value of the field in the hash stored at the key. Returns the value or None if the field does not exist.
        hmset(key, mapping):
            Set multiple field-value pairs in the hash stored at the key. Returns True if successful.
        hgetall(key):
            Get all field-value pairs in the hash stored at the key. Returns a list of alternating fields and values.
        hdel(key, *fields):
            Delete the specified fields from the hash stored at the key. Returns the number of fields that were removed.
        hexists(key, field):
            Check if the field exists in the hash stored at the key. Returns True if the field exists, False otherwise.
    """
    def __init__(self, database):
        self.db = database

    def _create_hash(self, key):
        """Create a new hash at key."""
        hash_dict = {}
        self.db.store[key] = hash_dict
        return hash_dict

    def _validate_hash(self, key):
        """Validate if key exists and holds a hash."""
        if not self.db.exists(key):
            return None
        value = self.db.store.get(key)
        if not isinstance(value, dict):
            raise ValueError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return value

    def hset(self, key, field, value):
        """Set field in hash stored at key to value."""
        try:
            hash_dict = self._validate_hash(key) or self._create_hash(key)
            is_new = field not in hash_dict
            hash_dict[field] = str(value)
            if not self.db.replaying:
                self.db.persistence_manager.log_command(f"HSET {key} {field} {value}")
            return 1 if is_new else 0
        except ValueError as e:
            return str(e)

    def hget(self, key, field):
        """Get value of field in hash stored at key."""
        try:
            hash_dict = self._validate_hash(key)
            return hash_dict.get(field) if hash_dict else None
        except ValueError as e:
            return str(e)

    def hmset(self, key, mapping):
        """Set multiple field-value pairs in hash stored at key."""
        try:
            hash_dict = self._validate_hash(key) or self._create_hash(key)
            for field, value in mapping.items():
                hash_dict[field] = str(value)
            if not self.db.replaying:
                fields_values = ' '.join(f"{k} {v}" for k, v in mapping.items())
                self.db.persistence_manager.log_command(f"HMSET {key} {fields_values}")
            return True
        except ValueError as e:
            return str(e)

    def hgetall(self, key):
        """Get all field-value pairs in hash stored at key."""
        try:
            hash_dict = self._validate_hash(key)
            if not hash_dict:
                return []
            # Return flattened list of alternating fields and values
            result = []
            for field, value in hash_dict.items():
                result.extend([field, value])
            return result
        except ValueError as e:
            return str(e)

    def hdel(self, key, *fields):
        """Delete fields from hash stored at key."""
        try:
            hash_dict = self._validate_hash(key)
            if not hash_dict:
                return 0
            count = 0
            for field in fields:
                if field in hash_dict:
                    del hash_dict[field]
                    count += 1
            if count > 0:
                if len(hash_dict) == 0:
                    self.db.delete(key)
                if not self.db.replaying:
                    self.db.persistence_manager.log_command(f"HDEL {key} {' '.join(fields)}")
            return count
        except ValueError as e:
            return str(e)

    def hexists(self, key, field):
        """Check if field exists in hash stored at key."""
        try:
            hash_dict = self._validate_hash(key)
            return bool(hash_dict and field in hash_dict)
        except ValueError:
            return False
