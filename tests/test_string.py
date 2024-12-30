import pytest

class TestStringBasicOperations:
    def test_set_and_get(self, db):
        # No need to flush manually anymore
        assert db.set("key", "value") is None
        assert db.get("key") == "value"
        assert db.get("nonexistent") is None

    def test_append(self, db):
        """Test string APPEND operation"""
        # Starting with empty key
        assert db.string.append("key", "Hello") == 5  # First append
        assert db.get("key") == "Hello"
        assert db.string.append("key", " World") == 11  # Second append
        assert db.get("key") == "Hello World"

    def test_strlen(self, db):
        """Test string length operation"""
        db.set("key", "Hello")
        assert db.string.strlen("key") == 5
        assert db.string.strlen("nonexistent") == 0
        
        # Test with empty string
        db.set("empty", "")
        assert db.string.strlen("empty") == 0
        
        # Test with ASCII-only unicode characters
        db.set("ascii", "hello")
        assert db.string.strlen("ascii") == 5

class TestStringIncrementalOperations:
    def test_incr(self, db):
        # No need to flush manually anymore
        assert db.string.incr("counter") == 1
        assert db.string.incr("counter") == 2
        
        # Reset counter
        db.set("counter", "0")
        assert db.string.incr("counter") == 1

    def test_decr(self, db):
        """Test DECR operation"""
        # Test with new key
        assert db.string.decr("counter") == -1
        assert db.string.decr("counter") == -2
        
        # Reset counter
        db.set("counter", "0")
        assert db.string.decr("counter") == -1

    def test_incrby(self, db):
        """Test INCRBY operation"""
        # Test with new key
        assert db.string.incrby("counter", 10) == 10
        assert db.string.incrby("counter", 5) == 15
        
        # Reset and test negative increment
        db.set("counter", "20")
        assert db.string.incrby("counter", -5) == 15

    def test_decrby(self, db):
        """Test DECRBY operation"""
        # Test with new key
        assert db.string.decrby("counter", 10) == -10
        assert db.string.decrby("counter", 5) == -15
        
        # Reset and test negative decrement
        db.set("counter", "0")
        assert db.string.decrby("counter", 10) == -10

class TestStringRangeOperations:
    def test_getrange(self, db):
        # No need to flush manually anymore
        db.set("key", "Hello World")
        assert db.string.getrange("key", 0, 4) == "Hello"
        assert db.string.getrange("key", 6, 10) == "World"
        assert db.string.getrange("nonexistent", 0, 5) == ""

    def test_setrange(self, db):
        """Test SETRANGE operation"""
        # Test with existing string
        db.set("key", "Hello World")
        assert db.string.setrange("key", 6, "Redis") == 11
        assert db.get("key") == "Hello Redis"
        
        # Test with offset beyond string length
        db.set("key", "test")
        assert db.string.setrange("key", 5, "test") == 9
        result = db.get("key")
        assert len(result) == 9
        assert result[4] == '\0'  # Check null byte padding

    def test_edge_cases(self, db):
        """Test edge cases for string operations"""
        # Test with non-string values
        db.sets.sadd("set", "member")  # Create a set
        assert db.string.append("set", "test") == "ERROR: Value at key is not a string"
        assert db.string.strlen("set") == "ERROR: Value at key is not a string"
        assert db.string.getrange("set", 0, 5) == "ERROR: Value at key is not a string"
        
        # Test with very large strings
        large_string = "x" * 1000000
        db.set("large", large_string)
        assert db.string.strlen("large") == 1000000
        assert db.string.getrange("large", 0, 5) == "xxxxxx"

if __name__ == '__main__':
    pytest.main([__file__])
