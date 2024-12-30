import pytest

class TestSetBasicOperations:
    def test_sadd(self, db):
        """Test SADD operation"""
        # Add single member
        assert db.sets.sadd("myset", "one") == 1
        assert db.sets.smembers("myset") == ["one"]
        
        # Add multiple members
        assert db.sets.sadd("myset", "two", "three") == 2
        assert sorted(db.sets.smembers("myset")) == ["one", "three", "two"]
        
        # Add duplicate members
        assert db.sets.sadd("myset", "one", "two") == 0
        assert sorted(db.sets.smembers("myset")) == ["one", "three", "two"]

    def test_srem(self, db):
        """Test SREM operation"""
        db.sets.sadd("myset", "one", "two", "three")
        
        # Remove single member
        assert db.sets.srem("myset", "one") == 1
        assert sorted(db.sets.smembers("myset")) == ["three", "two"]
        
        # Remove multiple members
        assert db.sets.srem("myset", "two", "three") == 2
        assert db.sets.smembers("myset") == []
        
        # Remove non-existent members
        assert db.sets.srem("myset", "four") == 0

    def test_sismember(self, db):
        """Test SISMEMBER operation"""
        db.sets.sadd("myset", "one", "two")
        
        assert db.sets.sismember("myset", "one") == True
        assert db.sets.sismember("myset", "two") == True
        assert db.sets.sismember("myset", "three") == False
        assert db.sets.sismember("nonexistent", "one") == False

    def test_smembers(self, db):
        """Test SMEMBERS operation"""
        # Test empty set
        assert db.sets.smembers("empty") == []
        
        # Test set with members
        db.sets.sadd("myset", "one", "two", "three")
        assert sorted(db.sets.smembers("myset")) == ["one", "three", "two"]
        
        # Test after removing members
        db.sets.srem("myset", "two")
        assert sorted(db.sets.smembers("myset")) == ["one", "three"]

class TestSetOperations:
    def test_sinter(self, db):
        """Test SINTER operation"""
        # Setup test sets
        db.sets.sadd("set1", "a", "b", "c", "d")
        db.sets.sadd("set2", "c", "d", "e")
        db.sets.sadd("set3", "d", "e", "f")
        
        # Test intersection of two sets
        assert set(db.sets.sinter("set1", "set2")) == {"c", "d"}
        
        # Test intersection of three sets
        assert set(db.sets.sinter("set1", "set2", "set3")) == {"d"}
        
        # Test with empty set
        db.sets.sadd("empty", "")
        assert db.sets.sinter("set1", "empty") == []
        
        # Test with non-existent set
        assert db.sets.sinter("set1", "nonexistent") == []

    def test_sunion(self, db):
        """Test SUNION operation"""
        # Setup test sets
        db.sets.sadd("set1", "a", "b", "c")
        db.sets.sadd("set2", "c", "d", "e")
        
        # Test union of two sets
        assert set(db.sets.sunion("set1", "set2")) == {"a", "b", "c", "d", "e"}
        
        # Test union with empty set
        assert set(db.sets.sunion("set1", "empty")) == {"a", "b", "c"}
        
        # Test union with non-existent set
        assert set(db.sets.sunion("set1", "nonexistent")) == {"a", "b", "c"}

    def test_sdiff(self, db):
        """Test SDIFF operation"""
        # Setup test sets
        db.sets.sadd("set1", "a", "b", "c", "d")
        db.sets.sadd("set2", "c", "d", "e")
        db.sets.sadd("set3", "d", "e", "f")
        
        # Test difference between two sets
        assert set(db.sets.sdiff("set1", "set2")) == {"a", "b"}
        
        # Test difference between three sets
        assert set(db.sets.sdiff("set1", "set2", "set3")) == {"a", "b"}
        
        # Test with empty set
        assert set(db.sets.sdiff("set1", "empty")) == {"a", "b", "c", "d"}
        
        # Test with non-existent set
        assert set(db.sets.sdiff("set1", "nonexistent")) == {"a", "b", "c", "d"}

    def test_edge_cases(self, db):
        """Test edge cases for set operations"""
        # Test operations on non-set types
        db.string.append("string", "test")  # Create a string
        assert db.sets.sadd("string", "value") == 0  # Should fail
        assert db.sets.srem("string", "value") == 0  # Should fail
        assert db.sets.sismember("string", "value") == False  # Should fail
        assert db.sets.smembers("string") == []  # Should fail
        
        # Test with empty members
        assert db.sets.sadd("myset", "") == 1  # Empty string is valid
        assert db.sets.sismember("myset", "") == True
        
        # Test with large sets
        large_set = set(str(i) for i in range(1000))
        assert db.sets.sadd("large", *large_set) == 1000
        assert len(db.sets.smembers("large")) == 1000

if __name__ == '__main__':
    pytest.main([__file__])
