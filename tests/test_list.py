import pytest

class TestListBasicOperations:
    def test_push_operations(self, db):
        """Test LPUSH and RPUSH operations"""
        # Test LPUSH
        assert db.list.lpush("mylist", "first") == 1
        assert db.list.lpush("mylist", "second") == 2
        assert db.list.lrange("mylist", 0, -1) == ["second", "first"]
        
        # Test RPUSH
        assert db.list.rpush("mylist", "last") == 3
        assert db.list.lrange("mylist", 0, -1) == ["second", "first", "last"]
        
        # Test multiple values
        assert db.list.lpush("mylist", "a", "b", "c") == 6
        assert db.list.lrange("mylist", 0, -1) == ["c", "b", "a", "second", "first", "last"]

    def test_pop_operations(self, db):
        """Test LPOP and RPOP operations"""
        # Setup test data
        db.list.rpush("mylist", "one", "two", "three", "four")
        
        # Test LPOP
        assert db.list.lpop("mylist") == "one"
        assert db.list.lrange("mylist", 0, -1) == ["two", "three", "four"]
        
        # Test RPOP
        assert db.list.rpop("mylist") == "four"
        assert db.list.lrange("mylist", 0, -1) == ["two", "three"]
        
        # Test popping until empty
        assert db.list.lpop("mylist") == "two"
        assert db.list.rpop("mylist") == "three"
        assert db.list.lpop("mylist") is None
        assert db.list.rpop("mylist") is None

class TestListRangeOperations:
    def test_lrange(self, db):
        """Test LRANGE operation"""
        # Setup test data
        db.list.rpush("mylist", "one", "two", "three", "four", "five")
        
        # Test positive indices
        assert db.list.lrange("mylist", 0, 2) == ["one", "two", "three"]
        assert db.list.lrange("mylist", 1, 3) == ["two", "three", "four"]
        
        # Test negative indices
        assert db.list.lrange("mylist", -3, -1) == ["three", "four", "five"]
        assert db.list.lrange("mylist", -100, 100) == ["one", "two", "three", "four", "five"]
        
        # Test empty ranges
        assert db.list.lrange("mylist", 5, 10) == []
        assert db.list.lrange("nonexistent", 0, -1) == []

    def test_lindex(self, db):
        """Test LINDEX operation"""
        # Setup test data
        db.list.rpush("mylist", "one", "two", "three", "four", "five")
        
        # Test positive indices
        assert db.list.lindex("mylist", 0) == "one"
        assert db.list.lindex("mylist", 4) == "five"
        
        # Test negative indices
        assert db.list.lindex("mylist", -1) == "five"
        assert db.list.lindex("mylist", -5) == "one"
        
        # Test out of range indices
        assert db.list.lindex("mylist", 5) is None
        assert db.list.lindex("mylist", -6) is None
        assert db.list.lindex("nonexistent", 0) is None

    def test_lset(self, db):
        """Test LSET operation"""
        # Setup test data
        db.list.rpush("mylist", "one", "two", "three")
        
        # Test setting at valid indices
        assert db.list.lset("mylist", 0, "ONE") is True
        assert db.list.lset("mylist", -1, "THREE") is True
        assert db.list.lrange("mylist", 0, -1) == ["ONE", "two", "THREE"]
        
        # Test setting at invalid indices
        assert db.list.lset("mylist", 3, "four") is False
        assert db.list.lset("mylist", -4, "zero") is False
        assert db.list.lset("nonexistent", 0, "value") is False

class TestListEdgeCases:
    def test_type_handling(self, db):
        """Test handling of different value types"""
        # Test numeric values
        assert db.list.rpush("numlist", 1, 2, 3) == 3
        assert db.list.lrange("numlist", 0, -1) == ["1", "2", "3"]
        
        # Test mixed types
        assert db.list.rpush("mixedlist", 1, "two", 3.14, True) == 4
        assert db.list.lrange("mixedlist", 0, -1) == ["1", "two", "3.14", "True"]
        
        # Test empty strings
        assert db.list.rpush("emptystrings", "", " ", "  ") == 3
        assert db.list.lrange("emptystrings", 0, -1) == ["", " ", "  "]

    def test_large_lists(self, db):
        """Test operations on large lists"""
        # Create large list
        large_size = 1000
        values = [str(i) for i in range(large_size)]
        assert db.list.rpush("largelist", *values) == large_size
        
        # Test range queries on large list
        assert len(db.list.lrange("largelist", 0, -1)) == large_size
        assert db.list.lrange("largelist", 100, 200) == [str(i) for i in range(100, 201)]
        
        # Test index access on large list
        assert db.list.lindex("largelist", 500) == "500"
        assert db.list.lindex("largelist", -1) == str(large_size - 1)

    def test_error_conditions(self, db):
        """Test error conditions and edge cases"""
        # Test operations on non-list types
        db.string.append("string", "test")  # Create a string
        assert db.list.lpush("string", "value") == 0  # Should fail
        assert db.list.rpop("string") is None  # Should fail
        assert db.list.lrange("string", 0, -1) == []  # Should return empty list
        
        # Test boundary conditions
        db.list.rpush("mylist", "one")
        assert db.list.lrange("mylist", -1, -2) == []  # Invalid range
        assert db.list.lrange("mylist", 1, 0) == []  # Invalid range

if __name__ == '__main__':
    pytest.main([__file__])
