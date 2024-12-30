import pytest

def test_set_add_remove(db):
    assert db.sets.sadd("myset", "one", "two") == 2
    assert db.sets.sadd("myset", "two") == 0
    assert db.sets.srem("myset", "one") == 1
    assert db.sets.smembers("myset") == ["two"]

def test_set_operations(db):
    db.sets.sadd("set1", "a", "b", "c")
    db.sets.sadd("set2", "b", "c", "d")
    assert set(db.sets.sinter("set1", "set2")) == {"b", "c"}
    assert set(db.sets.sunion("set1", "set2")) == {"a", "b", "c", "d"}
    assert set(db.sets.sdiff("set1", "set2")) == {"a"}

def test_set_membership(db):
    db.sets.sadd("myset", "one", "two")
    assert db.sets.sismember("myset", "one") == True
    assert db.sets.sismember("myset", "three") == False

if __name__ == '__main__':
    pytest.main([__file__])
