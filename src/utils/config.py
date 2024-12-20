# src/utils/config.py

class Config:
    """Centralized configuration for the Redis-like server."""
    HOST = "127.0.0.1"  # Localhost
    PORT = 6379         # Default Redis port
    MAX_CONNECTIONS = 5  # Max pending client connections
    LOG_LEVEL = "INFO"   # Logging level
