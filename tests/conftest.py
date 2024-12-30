import pytest
import sys
import os

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from core.database import KeyValueStore

@pytest.fixture
def db():
    """Fixture to provide a fresh database instance for each test."""
    store = KeyValueStore()
    store.flush()  # Clear database before each test
    return store

@pytest.fixture(autouse=True)
def clean_db(db):
    """Automatically flush database before each test."""
    db.flush()
    yield
    db.flush()  # Clean up after test
