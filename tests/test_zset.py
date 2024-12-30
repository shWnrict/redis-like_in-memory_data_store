import pytest

def test_zset_add_remove(db):
    assert db.zset.zadd("myzset", "1.5", "member1", "2.0", "member2") == 2
    assert db.zset.zrem("myzset", "member1") == 1
    result = db.zset.zrange("myzset", 0, -1, withscores=True)
    assert result == [("member2", "2")]

def test_zset_range_operations(db):
    db.zset.zadd("myzset", "1", "one", "2", "two", "3", "three")
    assert db.zset.zrange("myzset", 0, -1) == ["one", "two", "three"]
    assert db.zset.zrangebyscore("myzset", "2", "3") == ["two", "three"]

def test_zset_rank(db):
    db.zset.zadd("myzset", "1", "one", "2", "two", "3", "three")
    assert db.zset.zrank("myzset", "two") == 1

if __name__ == '__main__':
    pytest.main([__file__])
