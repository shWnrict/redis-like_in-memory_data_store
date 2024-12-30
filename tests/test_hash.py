import pytest

def test_hash_set_get(db):
    assert db.hash.hset("myhash", "field1", "value1") == 1
    assert db.hash.hget("myhash", "field1") == "value1"
    assert db.hash.hexists("myhash", "field1") == True

def test_hash_multiple_operations(db):
    mapping = {"field1": "value1", "field2": "value2"}
    assert db.hash.hmset("myhash", mapping) == 2
    assert sorted(db.hash.hgetall("myhash")) == ["field1", "field2", "value1", "value2"]
    
def test_hash_delete(db):
    db.hash.hset("myhash", "field1", "value1")
    assert db.hash.hdel("myhash", "field1") == 1
    assert db.hash.hget("myhash", "field1") is None

if __name__ == '__main__':
    pytest.main([__file__])
