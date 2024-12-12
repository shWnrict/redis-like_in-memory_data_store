from src.data_structures.data_types import RedisDataTypes

def test_string_operations():
    store = RedisDataTypes()
    
    # Test SET and GET
    store.string_set('key1', 'value1')
    assert store.string_get('key1') == 'value1'
    
    # Test APPEND
    store.string_append('key1', ' more')
    assert store.string_get('key1') == 'value1 more'

def test_list_operations():
    store = RedisDataTypes()
    
    # Test RPUSH
    store.list_rpush('mylist', 'a')
    store.list_rpush('mylist', 'b')
    assert store.list_range('mylist', 0, -1) == ['a', 'b']
    
    # Test LPUSH
    store.list_lpush('mylist', 'c')
    assert store.list_range('mylist', 0, -1) == ['c', 'a', 'b']

def test_set_operations():
    store = RedisDataTypes()
    
    store.set_add('myset', 'a')
    store.set_add('myset', 'b')
    store.set_add('myset', 'a')  # Duplicate
    
    assert store.set_members('myset') == {'a', 'b'}