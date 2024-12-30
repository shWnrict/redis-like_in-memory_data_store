import pytest

class TestZSetBasicOperations:
    def test_zadd(self, db):
        """Test ZADD operation"""
        # Add single member
        assert db.zset.zadd("myzset", "1.5", "member1") == 1
        
        # Add multiple members
        assert db.zset.zadd("myzset", "2.0", "member2", "3.5", "member3") == 2
        
        # Update existing member (should return 0 for no new additions)
        assert db.zset.zadd("myzset", "4.0", "member1") == 0
        
        # Verify order with scores
        result = db.zset.zrange("myzset", 0, -1, withscores=True)
        assert result == [("member2", "2"), ("member3", "3.5"), ("member1", "4")]

    def test_zadd_edge_cases(self, db):
        """Test ZADD edge cases"""
        # Test invalid score format
        assert db.zset.zadd("myzset", "invalid", "member1") == 0
        
        # Test integer scores
        assert db.zset.zadd("myzset", "1", "member1") == 1
        result = db.zset.zrange("myzset", 0, -1, withscores=True)
        assert result == [("member1", "1")]
        
        # Test negative scores
        assert db.zset.zadd("myzset", "-1.5", "member2") == 1
        result = db.zset.zrange("myzset", 0, -1, withscores=True)
        assert result == [("member2", "-1.5"), ("member1", "1")]

class TestZSetRangeOperations:
    def test_zrange(self, db):
        """Test ZRANGE operation"""
        # Setup test data
        db.zset.zadd("myzset", "1", "one", "2", "two", "3", "three", "4", "four", "5", "five")
        
        # Test positive indices
        assert db.zset.zrange("myzset", 0, 2) == ["one", "two", "three"]
        assert db.zset.zrange("myzset", 1, 3) == ["two", "three", "four"]
        
        # Test negative indices
        assert db.zset.zrange("myzset", -3, -1) == ["three", "four", "five"]
        
        # Test with scores
        result = db.zset.zrange("myzset", 0, -1, withscores=True)
        assert result == [("one", "1"), ("two", "2"), ("three", "3"), ("four", "4"), ("five", "5")]
        
        # Test out of range
        assert db.zset.zrange("myzset", 5, 10) == []
        assert db.zset.zrange("nonexistent", 0, -1) == []

    def test_zrangebyscore(self, db):
        """Test ZRANGEBYSCORE operation"""
        # Setup test data
        db.zset.zadd("myzset", "1", "one", "2", "two", "3", "three", "4", "four", "5", "five")
        
        # Test score ranges
        assert db.zset.zrangebyscore("myzset", "2", "4") == ["two", "three", "four"]
        assert db.zset.zrangebyscore("myzset", "-inf", "2") == ["one", "two"]
        assert db.zset.zrangebyscore("myzset", "4", "+inf") == ["four", "five"]
        
        # Test with scores
        result = db.zset.zrangebyscore("myzset", "3", "5", withscores=True)
        assert result == [("three", "3"), ("four", "4"), ("five", "5")]
        
        # Test invalid ranges
        assert db.zset.zrangebyscore("myzset", "invalid", "5") == []
        assert db.zset.zrangebyscore("nonexistent", "1", "5") == []

class TestZSetRankOperations:
    def test_zrank(self, db):
        """Test ZRANK operation"""
        # Setup test data
        db.zset.zadd("myzset", "1", "one", "2", "two", "3", "three", "4", "four", "5", "five")
        
        # Test ranks
        assert db.zset.zrank("myzset", "one") == 0
        assert db.zset.zrank("myzset", "three") == 2
        assert db.zset.zrank("myzset", "five") == 4
        
        # Test non-existent member
        assert db.zset.zrank("myzset", "nonexistent") is None
        assert db.zset.zrank("nonexistent", "one") is None

class TestZSetRemoveOperations:
    def test_zrem(self, db):
        """Test ZREM operation"""
        # Setup test data
        db.zset.zadd("myzset", "1", "one", "2", "two", "3", "three")
        
        # Test single member removal
        assert db.zset.zrem("myzset", "two") == 1
        assert db.zset.zrange("myzset", 0, -1) == ["one", "three"]
        
        # Test multiple member removal
        assert db.zset.zrem("myzset", "one", "three", "nonexistent") == 2
        assert db.zset.zrange("myzset", 0, -1) == []
        
        # Test non-existent set
        assert db.zset.zrem("nonexistent", "member") == 0

class TestZSetEdgeCases:
    def test_type_handling(self, db):
        """Test handling of different types"""
        # Create string value
        db.string.append("string", "test")
        
        # Test operations on wrong type
        assert db.zset.zadd("string", "1", "member") == 0
        assert db.zset.zrange("string", 0, -1) == []
        assert db.zset.zrank("string", "member") is None
        
        # Test with special characters in member names
        assert db.zset.zadd("myzset", "1", "test:123", "2", "test:456") == 2
        assert db.zset.zrange("myzset", 0, -1) == ["test:123", "test:456"]

    def test_large_sorted_sets(self, db):
        """Test operations on large sorted sets"""
        # Add many members
        members = {str(i): str(i/10.0) for i in range(1000)}
        for member, score in members.items():
            db.zset.zadd("largezset", score, member)
        
        # Test range queries
        assert len(db.zset.zrange("largezset", 0, -1)) == 1000
        
        # Test score range queries
        range_result = db.zset.zrangebyscore("largezset", "10", "20")
        assert len(range_result) == 101  # 10.0 to 20.0 inclusive
        
        # Test random access
        assert db.zset.zrank("largezset", "500") == 500

if __name__ == '__main__':
    pytest.main([__file__])
