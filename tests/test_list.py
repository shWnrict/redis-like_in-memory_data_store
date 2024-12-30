import pytest

def test_list_push_pop(db):
    assert db.list.lpush("mylist", "world", "hello") == 2
    assert db.list.rpush("mylist", "!") == 3
    assert db.list.lrange("mylist", 0, -1) == ["hello", "world", "!"]
    assert db.list.lpop("mylist") == "hello"
    assert db.list.rpop("mylist") == "!"
    assert db.list.lrange("mylist", 0, -1) == ["world"]

def test_list_range_and_index(db):
    db.list.rpush("mylist", "one", "two", "three", "four", "five")
    assert db.list.lrange("mylist", 1, 3) == ["two", "three", "four"]
    assert db.list.lindex("mylist", 1) == "two"
    assert db.list.lindex("mylist", -1) == "five"

def test_list_set(db):
    db.list.rpush("mylist", "one", "two", "three")
    assert db.list.lset("mylist", 1, "TWO") == True
    assert db.list.lrange("mylist", 0, -1) == ["one", "TWO", "three"]

if __name__ == '__main__':
    pytest.main([__file__])
