import sys
import os
from pathlib import Path

# Add the root directory of the project to the system path so that 'server' can be found
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from server import app  # Import the app from the server.py file

import pytest

@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

def test_home(client):
    response = client.get('/')
    assert response.status_code == 200
    assert response.json['message'] == "Welcome to the Redis-like In-Memory Data Store!"
