import pytest
from key_value_store import KeyValueStore  # Import KeyValueStore

@pytest.fixture
def kv_store():
    """Fixture for creating a new KeyValueStore instance."""
    return KeyValueStore()

def test_set_get(kv_store):
    """Test the SET and GET operations."""
    kv_store.set("name", "John Doe")
    assert kv_store.get("name") == "John Doe"

def test_delete(kv_store):
    """Test the DEL operation."""
    kv_store.set("age", 30)
    assert kv_store.get("age") == 30
    kv_store.delete("age")
    assert kv_store.get("age") is None  # Should be None after deletion

def test_exists(kv_store):
    """Test the EXISTS operation."""
    kv_store.set("city", "New York")
    assert kv_store.exists("city") is True
    kv_store.delete("city")
    assert kv_store.exists("city") is False
