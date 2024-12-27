class Config:
    HOST = "127.0.0.1"
    PORT = 6379
    LOG_LEVEL = "INFO"
    MAX_CLIENTS = 1000
    DATA_LIMIT = 1024 * 1024  # 1 MB per key
    SNAPSHOT_THRESHOLD = 1000  # Number of changes before triggering a snapshot
    TIMEOUT = 60  # Timeout in seconds
    RETRY_ATTEMPTS = 3  # Number of retry attempts
    BACKUP_PATH = "/var/backups"  # Path to store backups
    SAVE_INTERVAL = 300  # Interval in seconds for periodic snapshot saving
    CLEANUP_INTERVAL = 1  # Interval in seconds for cleaning up expired keys