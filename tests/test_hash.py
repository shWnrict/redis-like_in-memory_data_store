import pytest

class TestHashBasicOperations:
    def test_hset_hget(self, db):
        """Test basic HSET and HGET operations"""
        # Test single field set
        assert db.hash.hset("myhash", "field1", "value1") == 1
        assert db.hash.hget("myhash", "field1") == "value1"
        
        # Test updating existing field
        assert db.hash.hset("myhash", "field1", "newvalue1") == 0
        assert db.hash.hget("myhash", "field1") == "newvalue1"
        
        # Test non-existent field
        assert db.hash.hget("myhash", "nonexistent") is None
        assert db.hash.hget("nonexistent", "field") is None

    def test_hmset(self, db):
        """Test HMSET operation"""
        # Test setting multiple fields
        mapping = {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        }
        assert db.hash.hmset("myhash", mapping) == 3
        
        # Verify all fields were set
        assert db.hash.hget("myhash", "field1") == "value1"
        assert db.hash.hget("myhash", "field2") == "value2"
        assert db.hash.hget("myhash", "field3") == "value3"
        
        # Test updating existing fields
        update = {
            "field1": "newvalue1",
            "field4": "value4"
        }
        assert db.hash.hmset("myhash", update) == 1  # Only field4 is new

    def test_hgetall(self, db):
        """Test HGETALL operation"""
        # Test empty hash
        assert db.hash.hgetall("nonexistent") == []
        
        # Test with multiple fields
        db.hash.hmset("myhash", {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        })
        
        result = db.hash.hgetall("myhash")
        # Convert flat list to dict for easier comparison
        result_dict = {result[i]: result[i+1] for i in range(0, len(result), 2)}
        assert result_dict == {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        }

class TestHashDeletion:
    def test_hdel(self, db):
        """Test HDEL operation"""
        # Setup test data
        db.hash.hmset("myhash", {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        })
        
        # Test single field deletion
        assert db.hash.hdel("myhash", "field1") == 1
        assert db.hash.hget("myhash", "field1") is None
        
        # Test multiple field deletion
        assert db.hash.hdel("myhash", "field2", "field3", "nonexistent") == 2
        assert db.hash.hgetall("myhash") == []
        
        # Test deleting from non-existent hash
        assert db.hash.hdel("nonexistent", "field") == 0

class TestHashExistence:
    def test_hexists(self, db):
        """Test HEXISTS operation"""
        db.hash.hset("myhash", "field1", "value1")
        
        # Test existing field
        assert db.hash.hexists("myhash", "field1") == True
        
        # Test non-existent field
        assert db.hash.hexists("myhash", "nonexistent") == False
        
        # Test non-existent hash
        assert db.hash.hexists("nonexistent", "field") == False

class TestHashEdgeCases:
    def test_type_handling(self, db):
        """Test handling of different value types"""
        # Test numeric values
        assert db.hash.hset("myhash", "num", 123) == 1
        assert db.hash.hget("myhash", "num") == "123"
        
        # Test boolean values
        assert db.hash.hset("myhash", "bool", True) == 1
        assert db.hash.hget("myhash", "bool") == "True"
        
        # Test empty string
        assert db.hash.hset("myhash", "empty", "") == 1
        assert db.hash.hget("myhash", "empty") == ""

    def test_large_hash(self, db):
        """Test operations on large hash"""
        # Create large hash
        large_mapping = {f"field{i}": f"value{i}" for i in range(1000)}
        assert db.hash.hmset("largehash", large_mapping) == 1000
        
        # Verify random access
        assert db.hash.hget("largehash", "field500") == "value500"
        
        # Verify deletion
        assert db.hash.hdel("largehash", "field500") == 1
        assert db.hash.hexists("largehash", "field500") == False

    def test_error_conditions(self, db):
        """Test error conditions"""
        # Test operations on wrong type
        db.string.append("string", "test")  # Create a string
        assert db.hash.hset("string", "field", "value") == 0  # Should fail
        assert db.hash.hget("string", "field") is None  # Should fail
        assert db.hash.hexists("string", "field") == False  # Should fail

if __name__ == '__main__':
    pytest.main([__file__])
